"""Phase 6C durable lease, invocation, health, and safe-log tests."""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from app.storage import (
    HealthStatus,
    InvocationStatus,
    OperationLogLevel,
    SQLiteOperationsRepository,
    SQLiteResearchRepository,
)


NOW = datetime(2026, 8, 6, 10, 0, tzinfo=timezone.utc)


def _repository(tmp_path: Path) -> SQLiteOperationsRepository:
    research = SQLiteResearchRepository(tmp_path / "operations.db")
    operations = SQLiteOperationsRepository(research)
    operations.initialize()
    return operations


def _invocation(operations: SQLiteOperationsRepository, suffix: str):
    return operations.create_invocation(
        invocation_id=f"inv-{suffix}",
        job_name="test-eod",
        trigger="manual",
        mode="eod",
        requested_date=date(2026, 8, 6),
        scheduled_for=NOW,
        created_at=NOW,
    )


def test_v10_schema_and_atomic_overlap_protection(tmp_path: Path) -> None:
    operations = _repository(tmp_path)
    first = _invocation(operations, "first")
    second = _invocation(operations, "second")

    acquired = operations.acquire_lease(
        lease_key="eod:test-eod",
        invocation_id=first.invocation_id,
        lease_id="lease-first",
        acquired_at=NOW,
        expires_at=NOW + timedelta(minutes=30),
    )
    blocked = operations.acquire_lease(
        lease_key="eod:test-eod",
        invocation_id=second.invocation_id,
        lease_id="lease-second",
        acquired_at=NOW + timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=31),
    )

    assert acquired.acquired is True
    assert acquired.lease is not None
    assert blocked.acquired is False
    assert blocked.blocked_by_lease_id == "lease-first"

    with sqlite3.connect(tmp_path / "operations.db") as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0] == 10
        assert sorted(connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('scheduler_invocations', 'operation_leases', 'operation_logs')"
        ).fetchall()) == [
            ("operation_leases",),
            ("operation_logs",),
            ("scheduler_invocations",),
        ]


def test_expired_lease_is_replaced_and_old_invocation_is_resumable(
    tmp_path: Path,
) -> None:
    operations = _repository(tmp_path)
    old = _invocation(operations, "crashed")
    operations.start_invocation(old.invocation_id, started_at=NOW, attempt_count=1)
    operations.acquire_lease(
        lease_key="eod:test-eod",
        invocation_id=old.invocation_id,
        lease_id="lease-expired",
        acquired_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
    )

    resumed = _invocation(operations, "resumed")
    acquired = operations.acquire_lease(
        lease_key="eod:test-eod",
        invocation_id=resumed.invocation_id,
        lease_id="lease-resumed",
        acquired_at=NOW + timedelta(minutes=2),
        expires_at=NOW + timedelta(minutes=32),
    )

    assert acquired.acquired is True
    assert acquired.expired_lease_id == "lease-expired"
    assert acquired.expired_invocation_id == old.invocation_id
    stored_old = operations.get_invocation(old.invocation_id)
    assert stored_old is not None
    assert stored_old.status == InvocationStatus.WARNING
    assert stored_old.batch_run_id is None
    assert operations.get_lease("eod:test-eod").lease_id == "lease-resumed"
    with sqlite3.connect(tmp_path / "operations.db") as connection:
        assert connection.execute(
            "SELECT status FROM operation_leases WHERE lease_id = ?",
            ("lease-expired",),
        ).fetchone()[0] == "expired"


def test_two_scheduler_threads_only_one_acquires_same_lease(tmp_path: Path) -> None:
    operations = _repository(tmp_path)
    first = _invocation(operations, "thread-first")
    second = _invocation(operations, "thread-second")

    def acquire(invocation_id: str, lease_id: str):
        repo = SQLiteOperationsRepository(operations.research_repository)
        return repo.acquire_lease(
            lease_key="eod:test-eod",
            invocation_id=invocation_id,
            lease_id=lease_id,
            acquired_at=NOW,
            expires_at=NOW + timedelta(minutes=30),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                acquire,
                (first.invocation_id, second.invocation_id),
                ("lease-thread-first", "lease-thread-second"),
            )
        )

    assert sum(result.acquired for result in results) == 1
    assert sum(result.acquired is False for result in results) == 1


def test_safe_operation_log_redacts_secret_and_config_values(tmp_path: Path) -> None:
    operations = _repository(tmp_path)
    invocation = _invocation(operations, "log")
    fake_key = "test-" + "only-key"
    fake_bearer = "test-" + "only-bearer"
    entry = operations.log(
        invocation.invocation_id,
        event="child_failed",
        message=(
            f"api_key={fake_key} config_path=<USER_HOME>/provider/client.p12 "
            f"--env-file <USER_HOME>/research.env Bearer {fake_bearer}"
        ),
        level=OperationLogLevel.ERROR,
        created_at=NOW,
    )

    assert fake_key not in entry.message
    assert "client.p12" not in entry.message
    assert "research.env" not in entry.message
    assert fake_bearer not in entry.message
    assert "REDACTED" in entry.message


def test_health_status_distinguishes_requested_operation_outcomes(
    tmp_path: Path,
) -> None:
    operations = _repository(tmp_path)
    expected = {
        InvocationStatus.SUCCESS: HealthStatus.SUCCESS,
        InvocationStatus.WARNING: HealthStatus.WARNING,
        InvocationStatus.PARTIAL_FAILURE: HealthStatus.PARTIAL_FAILURE,
        InvocationStatus.HARD_FAILURE: HealthStatus.HARD_FAILURE,
        InvocationStatus.SKIP: HealthStatus.SKIP,
    }
    for index, (operation_status, health_status) in enumerate(expected.items()):
        invocation = _invocation(operations, f"health-{index}")
        operations.finish_invocation(
            invocation.invocation_id,
            status=operation_status,
            finished_at=NOW + timedelta(seconds=index),
            error_message=(None if operation_status == InvocationStatus.SUCCESS else "state"),
        )
        assert operations.health_for_invocation(
            invocation.invocation_id, checked_at=NOW
        ).health_status == health_status
