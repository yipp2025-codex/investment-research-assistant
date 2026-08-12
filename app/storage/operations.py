"""Durable operations state for Phase 6C scheduler orchestration.

This module is intentionally an operations layer.  It does not fetch market
data, execute analysis, or replace the frozen Phase 6A batch runner.  Scheduler
invocations, leases, and redacted operation messages are persisted so a
Windows process can be restarted without losing ownership or resume context.
"""

from __future__ import annotations

import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator
from uuid import uuid4

from app.storage.daily_report import SQLiteDailyReportRepository
from app.storage.sqlite import SQLiteResearchRepository


class OperationsError(RuntimeError):
    """Base class for scheduler operations state errors."""


class LeaseUnavailableError(OperationsError):
    """The requested operation lease is currently held by another run."""


class OperationsStateError(OperationsError):
    """An operations row or transition violates the Phase 6C contract."""


class InvocationStatus:
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    WARNING = "warning"
    PARTIAL_FAILURE = "partial_failure"
    HARD_FAILURE = "hard_failure"
    SKIP = "skip"
    DEFERRED = "deferred"


class OperationLogLevel:
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class HealthStatus:
    SUCCESS = "success"
    WARNING = "warning"
    PARTIAL_FAILURE = "partial_failure"
    HARD_FAILURE = "hard_failure"
    SKIP = "skip"


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password|authorization|client[_-]?secret)\b"
    r"\s*[:=]\s*[^\s,;]+"
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_CONFIG_ARGUMENT = re.compile(
    r"(?i)(--(?:env-file|config(?:-path)?|certificate(?:-path)?)(?:=|\s+))"
    r"(?:\"[^\"]+\"|[^\s,;]+)"
)
_CONFIG_PATH = re.compile(
    r"(?i)(?:\"(?:[A-Za-z]:[\\/]|/)[^\"]+\.(?:env|ini|p12|pfx|pem|key|crt|cer)\"|"
    r"(?:[A-Za-z]:[\\/]|/)[^\s\"']+\.(?:env|ini|p12|pfx|pem|key|crt|cer)\b)"
)


def redact_operation_message(message: str) -> str:
    """Remove secret assignments and local credential/config paths from a log."""

    safe = str(message).replace("\r", " ").replace("\n", " ").strip()
    safe = _SECRET_ASSIGNMENT.sub("[REDACTED_SECRET]", safe)
    safe = _BEARER_TOKEN.sub("Bearer [REDACTED]", safe)
    safe = _CONFIG_ARGUMENT.sub(r"\1[REDACTED_CONFIG]", safe)
    safe = _CONFIG_PATH.sub("[REDACTED_CONFIG_PATH]", safe)
    return safe[:1000] or "operation event"


@dataclass(frozen=True, slots=True)
class SchedulerInvocation:
    invocation_id: str
    job_name: str
    trigger: str
    mode: str
    requested_date: date
    scheduled_for: datetime
    attempt_count: int
    status: str
    batch_run_id: str | None
    process_id: int | None
    started_at: datetime | None
    finished_at: datetime | None
    next_retry_at: datetime | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class OperationLease:
    lease_key: str
    lease_id: str
    invocation_id: str
    status: str
    acquired_at: datetime
    expires_at: datetime
    heartbeat_at: datetime
    released_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class LeaseAcquireResult:
    acquired: bool
    lease: OperationLease | None
    blocked_by_lease_id: str | None = None
    expired_lease_id: str | None = None
    expired_invocation_id: str | None = None


@dataclass(frozen=True, slots=True)
class OperationLogEntry:
    operation_log_id: int
    invocation_id: str
    level: str
    event: str
    message: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    invocation_id: str | None
    job_name: str
    operation_status: str
    health_status: str
    batch_run_id: str | None
    message: str | None
    checked_at: datetime


class SQLiteOperationsRepository:
    """Persist scheduler invocations, leases, and safe operational events."""

    SCHEMA_VERSION = 9

    def __init__(self, research_repository: SQLiteResearchRepository) -> None:
        self.research_repository = research_repository

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self.research_repository._transaction() as connection:
            yield connection

    def initialize(self) -> None:
        """Ensure v1-v8 exist, then apply the v9 operations migration."""

        SQLiteDailyReportRepository(self.research_repository).initialize()
        migration_path = (
            Path(__file__).with_name("migrations") / "0009_scheduler_operations.sql"
        )
        with self._transaction() as connection:
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?",
                (self.SCHEMA_VERSION,),
            ).fetchone()
            required_tables = {
                "scheduler_invocations",
                "operation_leases",
                "operation_logs",
            }
            present_tables = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if applied is not None:
                missing = required_tables - present_tables
                if missing:
                    raise OperationsStateError(
                        "schema migration 9 is recorded but tables are missing: "
                        + ", ".join(sorted(missing))
                    )
                return
            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            connection.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
                (self.SCHEMA_VERSION, "phase 6c scheduler operations"),
            )

    @staticmethod
    def _execute_sql_statements(
        connection: sqlite3.Connection, script: str
    ) -> None:
        pending = ""
        for line in script.splitlines(keepends=True):
            pending += line
            if sqlite3.complete_statement(pending):
                statement = pending.strip()
                if statement:
                    connection.execute(statement)
                pending = ""
        if pending.strip():
            raise sqlite3.OperationalError("incomplete migration SQL statement")

    def create_invocation(
        self,
        *,
        job_name: str,
        trigger: str,
        mode: str,
        requested_date: date,
        scheduled_for: datetime,
        process_id: int | None = None,
        invocation_id: str | None = None,
        attempt_count: int = 0,
        created_at: datetime | None = None,
    ) -> SchedulerInvocation:
        self._validate_common(job_name, trigger, mode, scheduled_for)
        if attempt_count < 0:
            raise ValueError("attempt_count must be non-negative")
        now = created_at or datetime.now(timezone.utc)
        self._require_aware(now)
        invocation_id = invocation_id or str(uuid4())
        timestamp = now.isoformat()
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO scheduler_invocations ("
                "invocation_id, job_name, trigger, mode, requested_date, "
                "scheduled_for, attempt_count, status, process_id, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
                (
                    invocation_id,
                    job_name.strip(),
                    trigger,
                    mode,
                    requested_date.isoformat(),
                    scheduled_for.isoformat(),
                    attempt_count,
                    process_id,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                "SELECT * FROM scheduler_invocations WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
        return self._invocation_from_row(row)

    def get_invocation(self, invocation_id: str) -> SchedulerInvocation | None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM scheduler_invocations WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
        return None if row is None else self._invocation_from_row(row)

    def list_invocations(self, job_name: str | None = None, limit: int = 50) -> list[SchedulerInvocation]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._transaction() as connection:
            if job_name is None:
                rows = connection.execute(
                    "SELECT * FROM scheduler_invocations "
                    "ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM scheduler_invocations WHERE job_name = ? "
                    "ORDER BY created_at DESC LIMIT ?",
                    (job_name.strip(), limit),
                ).fetchall()
        return [self._invocation_from_row(row) for row in rows]

    def start_invocation(
        self,
        invocation_id: str,
        *,
        started_at: datetime,
        attempt_count: int | None = None,
    ) -> SchedulerInvocation:
        self._require_aware(started_at)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM scheduler_invocations WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
            if row is None:
                raise OperationsStateError(f"unknown invocation {invocation_id}")
            current = self._invocation_from_row(row)
            next_attempt = (
                current.attempt_count + 1
                if attempt_count is None
                else attempt_count
            )
            if next_attempt < 1:
                raise ValueError("running invocation must have attempt_count >= 1")
            connection.execute(
                "UPDATE scheduler_invocations SET status = 'running', "
                "attempt_count = ?, started_at = COALESCE(started_at, ?), "
                "finished_at = NULL, error_message = NULL, next_retry_at = NULL, "
                "updated_at = ? WHERE invocation_id = ?",
                (
                    next_attempt,
                    started_at.isoformat(),
                    started_at.isoformat(),
                    invocation_id,
                ),
            )
            updated = connection.execute(
                "SELECT * FROM scheduler_invocations WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
        return self._invocation_from_row(updated)

    def set_batch_run_id(
        self, invocation_id: str, batch_run_id: str, *, updated_at: datetime | None = None
    ) -> SchedulerInvocation:
        now = updated_at or datetime.now(timezone.utc)
        self._require_aware(now)
        with self._transaction() as connection:
            connection.execute(
                "UPDATE scheduler_invocations SET batch_run_id = ?, updated_at = ? "
                "WHERE invocation_id = ?",
                (batch_run_id, now.isoformat(), invocation_id),
            )
            row = connection.execute(
                "SELECT * FROM scheduler_invocations WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
        if row is None:
            raise OperationsStateError(f"unknown invocation {invocation_id}")
        return self._invocation_from_row(row)

    def finish_invocation(
        self,
        invocation_id: str,
        *,
        status: str,
        finished_at: datetime,
        error_message: str | None = None,
        next_retry_at: datetime | None = None,
    ) -> SchedulerInvocation:
        if status not in {
            InvocationStatus.SUCCESS,
            InvocationStatus.WARNING,
            InvocationStatus.PARTIAL_FAILURE,
            InvocationStatus.HARD_FAILURE,
            InvocationStatus.SKIP,
            InvocationStatus.DEFERRED,
        }:
            raise OperationsStateError(f"invalid final invocation status: {status}")
        self._require_aware(finished_at)
        if next_retry_at is not None:
            self._require_aware(next_retry_at)
        safe_error = (
            None
            if error_message is None
            else redact_operation_message(error_message)
        )
        with self._transaction() as connection:
            connection.execute(
                "UPDATE scheduler_invocations SET status = ?, finished_at = ?, "
                "error_message = ?, next_retry_at = ?, updated_at = ? "
                "WHERE invocation_id = ?",
                (
                    status,
                    finished_at.isoformat(),
                    safe_error,
                    None if next_retry_at is None else next_retry_at.isoformat(),
                    finished_at.isoformat(),
                    invocation_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM scheduler_invocations WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
        if row is None:
            raise OperationsStateError(f"unknown invocation {invocation_id}")
        return self._invocation_from_row(row)

    def acquire_lease(
        self,
        *,
        lease_key: str,
        invocation_id: str,
        acquired_at: datetime,
        expires_at: datetime,
        lease_id: str | None = None,
    ) -> LeaseAcquireResult:
        if not lease_key.strip():
            raise ValueError("lease_key must not be empty")
        if expires_at <= acquired_at:
            raise ValueError("expires_at must be after acquired_at")
        self._require_aware(acquired_at)
        self._require_aware(expires_at)
        lease_id = lease_id or str(uuid4())
        now_iso = acquired_at.isoformat()
        with self._transaction() as connection:
            # A write lock makes the check-and-acquire atomic across scheduler
            # processes.  The surrounding repository transaction commits it.
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM operation_leases WHERE lease_key = ? "
                "AND status = 'active' ORDER BY acquired_at DESC LIMIT 1",
                (lease_key.strip(),),
            ).fetchone()
            expired_lease_id = None
            expired_invocation_id = None
            if row is not None and row["status"] == "active":
                existing_expiry = datetime.fromisoformat(row["expires_at"])
                if existing_expiry > acquired_at:
                    return LeaseAcquireResult(
                        acquired=False,
                        lease=None,
                        blocked_by_lease_id=row["lease_id"],
                    )
                expired_lease_id = row["lease_id"]
                expired_invocation_id = row["invocation_id"]
                connection.execute(
                    "UPDATE scheduler_invocations SET status = 'warning', "
                    "finished_at = COALESCE(finished_at, ?), "
                    "error_message = COALESCE(error_message, ?), updated_at = ? "
                    "WHERE invocation_id = ? AND status = 'running'",
                    (
                        now_iso,
                        "operation lease expired; batch is resumable",
                        now_iso,
                        expired_invocation_id,
                    ),
                )
                connection.execute(
                    "UPDATE operation_leases SET status = 'expired', "
                    "released_at = ?, updated_at = ? WHERE lease_id = ?",
                    (now_iso, now_iso, row["lease_id"]),
                )
            connection.execute(
                "INSERT INTO operation_leases ("
                "lease_id, lease_key, invocation_id, status, acquired_at, expires_at, "
                "heartbeat_at, released_at, updated_at"
                ") VALUES (?, ?, ?, 'active', ?, ?, ?, NULL, ?)",
                (
                    lease_id,
                    lease_key.strip(),
                    invocation_id,
                    acquired_at.isoformat(),
                    expires_at.isoformat(),
                    acquired_at.isoformat(),
                    now_iso,
                ),
            )
            inserted = connection.execute(
                "SELECT * FROM operation_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
        return LeaseAcquireResult(
            acquired=True,
            lease=self._lease_from_row(inserted),
            expired_lease_id=expired_lease_id,
            expired_invocation_id=expired_invocation_id,
        )

    def heartbeat(
        self,
        *,
        lease_key: str,
        lease_id: str,
        heartbeat_at: datetime,
        expires_at: datetime,
    ) -> OperationLease | None:
        self._require_aware(heartbeat_at)
        self._require_aware(expires_at)
        with self._transaction() as connection:
            connection.execute(
                "UPDATE operation_leases SET heartbeat_at = ?, expires_at = ?, "
                "updated_at = ? WHERE lease_key = ? AND lease_id = ? "
                "AND status = 'active'",
                (
                    heartbeat_at.isoformat(),
                    expires_at.isoformat(),
                    heartbeat_at.isoformat(),
                    lease_key.strip(),
                    lease_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM operation_leases WHERE lease_id = ?",
                (lease_id,),
            ).fetchone()
        return None if row is None else self._lease_from_row(row)

    def release_lease(
        self, *, lease_key: str, lease_id: str, released_at: datetime
    ) -> bool:
        self._require_aware(released_at)
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE operation_leases SET status = 'released', released_at = ?, "
                "updated_at = ? WHERE lease_key = ? AND lease_id = ? "
                "AND status = 'active'",
                (
                    released_at.isoformat(),
                    released_at.isoformat(),
                    lease_key.strip(),
                    lease_id,
                ),
            )
        return cursor.rowcount == 1

    def get_lease(self, lease_key: str) -> OperationLease | None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM operation_leases WHERE lease_key = ? "
                "ORDER BY acquired_at DESC LIMIT 1",
                (lease_key.strip(),),
            ).fetchone()
        return None if row is None else self._lease_from_row(row)

    def log(
        self,
        invocation_id: str,
        *,
        event: str,
        message: str = "",
        level: str = OperationLogLevel.INFO,
        created_at: datetime | None = None,
    ) -> OperationLogEntry:
        if level not in {
            OperationLogLevel.INFO,
            OperationLogLevel.WARNING,
            OperationLogLevel.ERROR,
        }:
            raise ValueError(f"invalid operation log level: {level}")
        if not event.strip():
            raise ValueError("event must not be empty")
        now = created_at or datetime.now(timezone.utc)
        self._require_aware(now)
        safe_message = redact_operation_message(message)
        with self._transaction() as connection:
            cursor = connection.execute(
                "INSERT INTO operation_logs (invocation_id, level, event, message, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    invocation_id,
                    level,
                    redact_operation_message(event)[:120],
                    safe_message,
                    now.isoformat(),
                ),
            )
            row = connection.execute(
                "SELECT operation_log_id, invocation_id, level, event, message, created_at "
                "FROM operation_logs WHERE operation_log_id = ?",
                (cursor.lastrowid,),
            ).fetchone()
        return self._log_from_row(row)

    def list_logs(self, invocation_id: str, limit: int = 100) -> list[OperationLogEntry]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        with self._transaction() as connection:
            rows = connection.execute(
                "SELECT operation_log_id, invocation_id, level, event, message, created_at "
                "FROM operation_logs WHERE invocation_id = ? "
                "ORDER BY created_at, operation_log_id LIMIT ?",
                (invocation_id, limit),
            ).fetchall()
        return [self._log_from_row(row) for row in rows]

    def health_for_invocation(
        self, invocation_id: str, *, checked_at: datetime | None = None
    ) -> HealthSnapshot:
        invocation = self.get_invocation(invocation_id)
        if invocation is None:
            raise OperationsStateError(f"unknown invocation {invocation_id}")
        return HealthSnapshot(
            invocation_id=invocation.invocation_id,
            job_name=invocation.job_name,
            operation_status=invocation.status,
            health_status=health_status_for_operation(invocation.status),
            batch_run_id=invocation.batch_run_id,
            message=invocation.error_message,
            checked_at=checked_at or datetime.now(timezone.utc),
        )

    def latest_health(
        self, job_name: str, *, checked_at: datetime | None = None
    ) -> HealthSnapshot:
        invocations = self.list_invocations(job_name=job_name, limit=1)
        now = checked_at or datetime.now(timezone.utc)
        if not invocations:
            return HealthSnapshot(
                invocation_id=None,
                job_name=job_name,
                operation_status="none",
                health_status=HealthStatus.WARNING,
                batch_run_id=None,
                message="no scheduler invocation recorded",
                checked_at=now,
            )
        return self.health_for_invocation(invocations[0].invocation_id, checked_at=now)

    @staticmethod
    def _validate_common(
        job_name: str, trigger: str, mode: str, scheduled_for: datetime
    ) -> None:
        if not job_name.strip():
            raise ValueError("job_name must not be empty")
        if trigger not in {"manual", "scheduled"}:
            raise ValueError("trigger must be manual or scheduled")
        if mode != "eod":
            raise ValueError("Phase 6C supports EOD mode only")
        SQLiteOperationsRepository._require_aware(scheduled_for)

    @staticmethod
    def _require_aware(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")

    @staticmethod
    def _invocation_from_row(row: sqlite3.Row) -> SchedulerInvocation:
        if row is None:
            raise OperationsStateError("operations row was not persisted")
        return SchedulerInvocation(
            invocation_id=row["invocation_id"],
            job_name=row["job_name"],
            trigger=row["trigger"],
            mode=row["mode"],
            requested_date=date.fromisoformat(row["requested_date"]),
            scheduled_for=datetime.fromisoformat(row["scheduled_for"]),
            attempt_count=int(row["attempt_count"]),
            status=row["status"],
            batch_run_id=row["batch_run_id"],
            process_id=None if row["process_id"] is None else int(row["process_id"]),
            started_at=(
                None
                if row["started_at"] is None
                else datetime.fromisoformat(row["started_at"])
            ),
            finished_at=(
                None
                if row["finished_at"] is None
                else datetime.fromisoformat(row["finished_at"])
            ),
            next_retry_at=(
                None
                if row["next_retry_at"] is None
                else datetime.fromisoformat(row["next_retry_at"])
            ),
            error_message=row["error_message"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> OperationLease:
        if row is None:
            raise OperationsStateError("lease row was not persisted")
        return OperationLease(
            lease_key=row["lease_key"],
            lease_id=row["lease_id"],
            invocation_id=row["invocation_id"],
            status=row["status"],
            acquired_at=datetime.fromisoformat(row["acquired_at"]),
            expires_at=datetime.fromisoformat(row["expires_at"]),
            heartbeat_at=datetime.fromisoformat(row["heartbeat_at"]),
            released_at=(
                None
                if row["released_at"] is None
                else datetime.fromisoformat(row["released_at"])
            ),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _log_from_row(row: sqlite3.Row) -> OperationLogEntry:
        if row is None:
            raise OperationsStateError("operation log row was not persisted")
        return OperationLogEntry(
            operation_log_id=int(row["operation_log_id"]),
            invocation_id=row["invocation_id"],
            level=row["level"],
            event=row["event"],
            message=row["message"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )


def health_status_for_operation(status: str) -> str:
    """Map durable operation state to the small public health vocabulary."""

    if status == InvocationStatus.SUCCESS:
        return HealthStatus.SUCCESS
    if status in {
        InvocationStatus.WARNING,
        InvocationStatus.RUNNING,
        InvocationStatus.PENDING,
        InvocationStatus.DEFERRED,
    }:
        return HealthStatus.WARNING
    if status == InvocationStatus.PARTIAL_FAILURE:
        return HealthStatus.PARTIAL_FAILURE
    if status == InvocationStatus.HARD_FAILURE:
        return HealthStatus.HARD_FAILURE
    if status == InvocationStatus.SKIP:
        return HealthStatus.SKIP
    return HealthStatus.WARNING


__all__ = [
    "HealthSnapshot",
    "HealthStatus",
    "InvocationStatus",
    "LeaseAcquireResult",
    "LeaseUnavailableError",
    "OperationLease",
    "OperationLogEntry",
    "OperationLogLevel",
    "OperationsError",
    "OperationsStateError",
    "SQLiteOperationsRepository",
    "SchedulerInvocation",
    "health_status_for_operation",
    "redact_operation_message",
]
