from __future__ import annotations

import ast
import hashlib
import json
import sqlite3
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.daily_runner import (
    EXECUTION_ALREADY_RUNNING,
    EXECUTION_FAILED,
    EXECUTION_SKIPPED_NON_MARKET_DAY,
    EXECUTION_SUCCESS,
    EXECUTION_SUCCESS_REPLAY,
    LOCK_ALREADY_RUNNING,
    LOCK_RELEASED,
    MARKET_DAY,
    MARKET_DAY_UNKNOWN,
    NON_MARKET_DAY,
    DailyRunLock,
    DailyRunner,
    DeterministicMarketDatePolicy,
)
from app.market_calendar import MarketDayState
from app.reporting.screener_report import ScreenerReportCollisionError
from app.storage.screener_replay import ScreenerReplayIntegrityError


MARKET_DATE = date(2026, 8, 7)
RUN_ID = "a" * 64
SCREENER_SHA = "b" * 64
REPORT_SHA = "c" * 64


def _db(tmp_path: Path) -> Path:
    path = tmp_path / "explicit database.db"
    path.write_bytes(b"synthetic persisted database")
    return path


def _success_result(*, replayed: bool = False) -> SimpleNamespace:
    return SimpleNamespace(
        status="success",
        replayed=replayed,
        screener_run_id=RUN_ID,
        canonical_sha256=SCREENER_SHA,
    )


def _partial_result() -> SimpleNamespace:
    return SimpleNamespace(
        status="partial_success",
        replayed=False,
        screener_run_id=RUN_ID,
        canonical_sha256=None,
    )


def _failed_result() -> SimpleNamespace:
    return SimpleNamespace(
        status="failed",
        replayed=False,
        screener_run_id=RUN_ID,
        canonical_sha256=None,
    )


class SequencedS5:
    def __init__(self, *results: object) -> None:
        self.results = list(results)
        self.calls = 0

    def __call__(self, target_market_date: date) -> object:
        assert target_market_date == MARKET_DATE
        self.calls += 1
        result = self.results.pop(0) if self.results else _success_result()
        if isinstance(result, BaseException):
            raise result
        return result


class FakeReportGenerator:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def generate(self, screener_run_id: str) -> SimpleNamespace:
        self.calls.append(screener_run_id)
        return SimpleNamespace(
            report=SimpleNamespace(
                screener_run_id=screener_run_id,
                screener_canonical_sha256=SCREENER_SHA,
            ),
            report_sha256=REPORT_SHA,
        )


class FakeReportWriter:
    def __init__(self, *, collision: bool = False, no_op: bool = False) -> None:
        self.calls = 0
        self.collision = collision
        self.no_op = no_op

    def write(self, generation, *, output_directory, **unused_paths):
        self.calls += 1
        if self.collision:
            raise ScreenerReportCollisionError("synthetic collision")
        output_directory = Path(output_directory)
        output_directory.mkdir(parents=True, exist_ok=True)
        json_path = output_directory / "daily-screener-2026-08-07.json"
        markdown_path = output_directory / "daily-screener-2026-08-07.md"
        return SimpleNamespace(
            json_artifact=SimpleNamespace(path=json_path, written=not self.no_op),
            markdown_artifact=SimpleNamespace(path=markdown_path, written=not self.no_op),
            no_op=self.no_op,
        )


def _runner(
    tmp_path: Path,
    s5: object,
    *,
    report_generator: object | None = None,
    report_writer: object | None = None,
    policy: object | None = None,
    lock_factory=None,
) -> tuple[DailyRunner, object, object]:
    database_path = _db(tmp_path)
    generator = report_generator or FakeReportGenerator()
    writer = report_writer or FakeReportWriter()
    runner = DailyRunner(
        database_path=database_path,
        report_output_directory=tmp_path / "reports with spaces",
        lock_directory=tmp_path / "locks with spaces",
        screener_runner=s5,
        report_generator=generator,
        report_writer=writer,
        market_date_policy=policy
        or DeterministicMarketDatePolicy(latest_published_date=MARKET_DATE),
        lock_factory=lock_factory,
    )
    return runner, generator, writer


def test_s6b_gate1_market_day_fresh_s5_then_s6a(tmp_path: Path) -> None:
    s5 = SequencedS5(_success_result(replayed=False))
    runner, generator, writer = _runner(tmp_path, s5)

    result = runner.run(MARKET_DATE)

    assert result.execution_status == EXECUTION_SUCCESS
    assert result.market_day_status == MARKET_DAY
    assert result.lock_status == LOCK_RELEASED
    assert result.s5_status == "success"
    assert result.s5_replayed is False
    assert result.report_sha256 == REPORT_SHA
    assert result.report_no_op is False
    assert result.exit_code == 0
    assert s5.calls == 1
    assert generator.calls == [RUN_ID]
    assert writer.calls == 1


def test_s6b_gate2_second_execution_is_s5_replay_and_s6a_noop(
    tmp_path: Path,
) -> None:
    s5 = SequencedS5(
        _success_result(replayed=False),
        _success_result(replayed=True),
    )
    generator = FakeReportGenerator()
    writer = FakeReportWriter(no_op=False)
    runner, unused_generator, unused_writer = _runner(
        tmp_path,
        s5,
        report_generator=generator,
        report_writer=writer,
    )

    first = runner.run(MARKET_DATE)
    writer.no_op = True
    second = runner.run(MARKET_DATE)

    assert first.execution_status == EXECUTION_SUCCESS
    assert second.execution_status == EXECUTION_SUCCESS_REPLAY
    assert second.s5_replayed is True
    assert second.screener_run_id == first.screener_run_id
    assert second.report_sha256 == first.report_sha256
    assert second.report_no_op is True
    assert s5.calls == 2
    assert generator.calls == [RUN_ID, RUN_ID]
    assert writer.calls == 2


def test_s6b_gate3_non_market_day_skips_without_hooks_or_files(tmp_path: Path) -> None:
    s5 = SequencedS5(_success_result())
    policy = DeterministicMarketDatePolicy(latest_published_date=None)
    runner, generator, writer = _runner(tmp_path, s5, policy=policy)

    result = runner.run(date(2026, 8, 8))

    assert result.execution_status == EXECUTION_SKIPPED_NON_MARKET_DAY
    assert result.market_day_status == NON_MARKET_DAY
    assert result.market_date_evidence_state == MarketDayState.WEEKEND.value
    assert result.exit_code == 0
    assert s5.calls == 0
    assert generator.calls == []
    assert writer.calls == 0
    assert not (tmp_path / "reports with spaces").exists()


def test_s6b_unknown_market_date_fails_closed_without_hooks(tmp_path: Path) -> None:
    s5 = SequencedS5(_success_result())
    runner, generator, writer = _runner(
        tmp_path,
        s5,
        policy=DeterministicMarketDatePolicy(latest_published_date=None),
    )

    result = runner.run(MARKET_DATE)

    assert result.execution_status == EXECUTION_FAILED
    assert result.market_day_status == MARKET_DAY_UNKNOWN
    assert result.error_code == "market_date_unresolved"
    assert result.exit_code != 0
    assert s5.calls == 0
    assert generator.calls == []
    assert writer.calls == 0


def test_s6b_gate4_concurrent_lock_blocks_second_runner(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingS5:
        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, target_market_date: date) -> object:
            self.calls += 1
            entered.set()
            assert release.wait(5)
            return _success_result()

    first_s5 = BlockingS5()
    first, first_generator, first_writer = _runner(tmp_path, first_s5)
    second_s5 = SequencedS5(_success_result())
    second, second_generator, second_writer = _runner(tmp_path, second_s5)
    holder: list[object] = []

    thread = threading.Thread(target=lambda: holder.append(first.run(MARKET_DATE)))
    thread.start()
    assert entered.wait(5)
    second_result = second.run(MARKET_DATE)
    release.set()
    thread.join(timeout=5)

    assert not thread.is_alive()
    assert second_result.execution_status == EXECUTION_ALREADY_RUNNING
    assert second_result.lock_status == LOCK_ALREADY_RUNNING
    assert second_result.exit_code == 0
    assert second_s5.calls == 0
    assert second_generator.calls == []
    assert second_writer.calls == 0
    assert holder[0].execution_status == EXECUTION_SUCCESS


def test_s6b_gate5_lock_releases_after_success(tmp_path: Path) -> None:
    s5 = SequencedS5(_success_result(), _success_result(replayed=True))
    runner, unused_generator, unused_writer = _runner(tmp_path, s5)

    first = runner.run(MARKET_DATE)
    second = runner.run(MARKET_DATE)

    assert first.lock_status == LOCK_RELEASED
    assert second.lock_status == LOCK_RELEASED
    assert s5.calls == 2


def test_s6b_gate6_lock_releases_after_s5_failure(tmp_path: Path) -> None:
    s5 = SequencedS5(RuntimeError("synthetic S5 failure"), _success_result())
    runner, generator, writer = _runner(tmp_path, s5)

    failed = runner.run(MARKET_DATE)
    succeeded = runner.run(MARKET_DATE)

    assert failed.execution_status == EXECUTION_FAILED
    assert failed.error_code == "s5_unexpected_error"
    assert failed.lock_status == LOCK_RELEASED
    assert succeeded.execution_status == EXECUTION_SUCCESS
    assert succeeded.lock_status == LOCK_RELEASED
    assert generator.calls == [RUN_ID]
    assert writer.calls == 1


def test_s6b_gate7_partial_s5_does_not_create_report(tmp_path: Path) -> None:
    s5 = SequencedS5(_partial_result())
    runner, generator, writer = _runner(tmp_path, s5)

    result = runner.run(MARKET_DATE)

    assert result.execution_status == EXECUTION_FAILED
    assert result.error_code == "s5_partial_success"
    assert result.s5_status == "partial_success"
    assert result.screener_run_id == RUN_ID
    assert generator.calls == []
    assert writer.calls == 0
    assert not (tmp_path / "reports with spaces").exists()


def test_s6b_gate8_resume_success_then_report(tmp_path: Path) -> None:
    s5 = SequencedS5(_partial_result(), _success_result(replayed=True))
    runner, generator, writer = _runner(tmp_path, s5)

    first = runner.run(MARKET_DATE)
    second = runner.run(MARKET_DATE)

    assert first.execution_status == EXECUTION_FAILED
    assert second.execution_status == EXECUTION_SUCCESS_REPLAY
    assert second.s5_status == "success"
    assert second.s5_replayed is True
    assert generator.calls == [RUN_ID]
    assert writer.calls == 1


def test_s6b_gate9_tampered_s5_replay_blocks_s6a(tmp_path: Path) -> None:
    s5 = SequencedS5(ScreenerReplayIntegrityError("tampered"))
    runner, generator, writer = _runner(tmp_path, s5)

    result = runner.run(MARKET_DATE)

    assert result.execution_status == EXECUTION_FAILED
    assert result.error_code == "s5_integrity_failed"
    assert generator.calls == []
    assert writer.calls == 0


def test_s6b_gate10_report_collision_fails_closed(tmp_path: Path) -> None:
    s5 = SequencedS5(_success_result())
    writer = FakeReportWriter(collision=True)
    runner, generator, unused_writer = _runner(
        tmp_path,
        s5,
        report_writer=writer,
    )

    result = runner.run(MARKET_DATE)

    assert result.execution_status == EXECUTION_FAILED
    assert result.error_code == "report_artifact_collision"
    assert generator.calls == [RUN_ID]
    assert writer.calls == 1
    assert result.lock_status == LOCK_RELEASED


def test_s6b_gate11_explicit_db_isolation_and_operational_metadata(tmp_path: Path) -> None:
    production = tmp_path / "production.db"
    production.write_bytes(b"production unchanged")
    production_before = hashlib.sha256(production.read_bytes()).hexdigest()
    s5 = SequencedS5(_success_result())
    runner, unused_generator, unused_writer = _runner(tmp_path, s5)

    result = runner.run(MARKET_DATE)

    assert result.execution_status == EXECUTION_SUCCESS
    assert result.started_at.tzinfo is not None
    assert result.finished_at.tzinfo is not None
    assert result.duration_seconds >= 0
    assert result.lock_acquisition_seconds is not None
    assert result.s5_duration_seconds is not None
    assert result.s6a_duration_seconds is not None
    assert result.runner_overhead_seconds is not None
    assert hashlib.sha256(production.read_bytes()).hexdigest() == production_before
    assert result.as_dict()["runner_contract_version"] == "s6b-daily-runner-v1"
    json.loads(result.as_json())


def test_s6b_gate12_runner_has_no_windows_or_legacy_scheduler_dependency() -> None:
    source_path = Path(__file__).parents[1] / "app" / "daily_runner.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "app.operations.scheduler" not in imported
    assert "app.windows_scheduler_adapter" not in imported
    assert "winreg" not in source_path.read_text(encoding="utf-8")


def test_s6b_market_policy_only_accepts_exact_published_date() -> None:
    policy = DeterministicMarketDatePolicy(latest_published_date=MARKET_DATE)
    assert policy.resolve(MARKET_DATE).status == MARKET_DAY
    assert policy.resolve(date(2026, 8, 8)).status == NON_MARKET_DAY
    assert DeterministicMarketDatePolicy(
        latest_published_date=date(2026, 8, 10)
    ).resolve(MARKET_DATE).status == MARKET_DAY_UNKNOWN
    assert DeterministicMarketDatePolicy(
        latest_published_date=MARKET_DATE,
        explicit_non_market_dates=frozenset({MARKET_DATE}),
    ).resolve(MARKET_DATE).status == NON_MARKET_DAY


def test_s6b_lock_identity_is_per_date_and_database_and_crash_releases(
    tmp_path: Path,
) -> None:
    database = _db(tmp_path)
    lock_dir = tmp_path / "lock"
    first = DailyRunLock(database, MARKET_DATE, lock_directory=lock_dir)
    same = DailyRunLock(database, MARKET_DATE, lock_directory=lock_dir)
    other_date = DailyRunLock(database, date(2026, 8, 10), lock_directory=lock_dir)
    other_db = DailyRunLock(tmp_path / "other.db", MARKET_DATE, lock_directory=lock_dir)

    assert first.identity_sha256 == same.identity_sha256
    assert first.identity_sha256 != other_date.identity_sha256
    assert first.identity_sha256 != other_db.identity_sha256
    assert first.try_acquire() is True
    assert same.try_acquire() is False
    first.release()
    metadata = same.owner_metadata()
    assert metadata is not None
    assert metadata["owner_token"] == first.owner_token

    assert same.try_acquire() is True
    same.release()

    # Simulate process death: the OS closes/releases the handle, while stale
    # metadata remains.  The next process may acquire based on the OS lock.
    crashed = DailyRunLock(database, MARKET_DATE, lock_directory=lock_dir)
    recovered = DailyRunLock(database, MARKET_DATE, lock_directory=lock_dir)
    assert crashed.try_acquire() is True
    assert crashed._stream is not None
    crashed._stream.close()
    crashed._stream = None
    assert recovered.try_acquire() is True
    recovered.release()
