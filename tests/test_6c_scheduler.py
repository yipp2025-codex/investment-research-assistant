"""Phase 6C EOD scheduler acceptance tests without external network access."""

from __future__ import annotations

import io
import os
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from app.models import Symbol
from app.operations.scheduler import (
    MarketReadiness,
    SchedulerConfig,
    SchedulerRunner,
)
from app.operations.task_scheduler import WindowsTaskConfig, build_task_scheduler_xml
from app.pipelines.batch_runner import DailyBatchRunner
from app.pipelines.daily_research import DailyResearchPipeline
from app.pipelines.retry import RetryPolicy
from app.providers import MockMarketDataProvider
from app.storage import SQLiteOperationsRepository, SQLiteResearchRepository
from app.storage.batch_run import BatchRunStatus, SQLiteBatchRunRepository
from scripts import run_daily_batch


NOW = datetime(2026, 8, 6, 10, 0, tzinfo=timezone.utc)
SYMBOLS = ("2330", "2317", "2454")
TARGET = date(2026, 8, 6)


class _Clock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


@dataclass
class _SequenceProbe:
    values: list[MarketReadiness]

    def __post_init__(self) -> None:
        self.calls = 0

    def check(self, target_date: date, *, timeout_seconds: float) -> MarketReadiness:
        del target_date, timeout_seconds
        self.calls += 1
        return self.values[min(self.calls - 1, len(self.values) - 1)]


def _setup(tmp_path: Path) -> tuple[SQLiteResearchRepository, Path]:
    database = tmp_path / "scheduler.db"
    repository = SQLiteResearchRepository(database)
    repository.initialize()
    for symbol in SYMBOLS:
        repository.upsert_symbol(
            Symbol(
                symbol=symbol,
                name=f"Synthetic {symbol}",
                market="TWSE",
                currency="TWD",
            )
        )
    batch_repository = SQLiteBatchRunRepository(repository)
    batch_repository.initialize()
    watchlist_id = batch_repository.get_or_create_watchlist("acceptance")
    batch_repository.set_watchlist_members(watchlist_id, SYMBOLS)
    env_file = tmp_path / "empty.env"
    env_file.write_text("# deterministic test environment\n", encoding="utf-8")
    return repository, env_file


def _config(
    tmp_path: Path,
    env_file: Path,
    *,
    probe: _SequenceProbe | None = None,
    max_attempts: int = 3,
) -> SchedulerConfig:
    return SchedulerConfig(
        job_name="test-scheduler",
        watchlist="acceptance",
        provider="mock",
        project_root=Path.cwd(),
        database_path=tmp_path / "scheduler.db",
        env_file=env_file,
        max_deferred_attempts=max_attempts,
        retry_backoff_seconds=0,
        lease_ttl_seconds=60,
        cli_timeout_seconds=30,
    )


def _execute_existing_6a(
    commands: list[tuple[str, ...]],
    command,
    cwd: Path,
    environment,
    timeout_seconds: float,
):
    del cwd, timeout_seconds
    commands.append(tuple(command))
    stdout = io.StringIO()
    stderr = io.StringIO()
    with patch.dict(os.environ, dict(environment), clear=False):
        with redirect_stdout(stdout), redirect_stderr(stderr):
            returncode = run_daily_batch.main(list(command[2:]))
    from app.operations.scheduler import _CliResult

    return _CliResult(returncode, stdout.getvalue(), stderr.getvalue())


def test_manual_and_scheduled_triggers_replay_same_batch_and_reports(
    tmp_path: Path,
) -> None:
    repository, env_file = _setup(tmp_path)
    commands: list[tuple[str, ...]] = []
    config = _config(tmp_path, env_file)
    runner = SchedulerRunner(
        config,
        repository=repository,
        execute_cli=lambda command, cwd, environment, timeout: _execute_existing_6a(
            commands, command, cwd, environment, timeout
        ),
        clock=_Clock(),
        sleep=lambda _: None,
    )

    first = runner.run_once(trigger="manual", target_date=TARGET)
    second = runner.run_once(trigger="scheduled", target_date=TARGET)

    assert first.health_status == "success"
    assert second.health_status == "success"
    assert first.batch_run_id == second.batch_run_id
    assert second.idempotent_replay is True
    assert len(commands) == 2
    assert commands[0][2:] == commands[1][2:]
    with repository._transaction() as connection:
        canonical_hashes = [
            row[0]
            for row in connection.execute(
                "SELECT payload_sha256 FROM daily_research_results "
                "ORDER BY symbol"
            ).fetchall()
        ]
        report_hashes = [
            row[0]
            for row in connection.execute(
                "SELECT markdown_sha256 FROM daily_research_reports "
                "ORDER BY symbol"
            ).fetchall()
        ]
        assert connection.execute(
            "SELECT COUNT(*) FROM daily_research_results"
        ).fetchone()[0] == len(SYMBOLS)
        assert connection.execute(
            "SELECT COUNT(*) FROM daily_research_reports"
        ).fetchone()[0] == len(SYMBOLS)
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert all(len(value) == 64 for value in canonical_hashes)
        assert all(len(value) == 64 for value in report_hashes)


def test_weekend_skips_without_probe_or_child_cli(tmp_path: Path) -> None:
    repository, env_file = _setup(tmp_path)
    probe = _SequenceProbe(
        [MarketReadiness(MarketReadiness.READY, date(2026, 8, 1), "unexpected")]
    )
    child_calls: list[tuple[str, ...]] = []
    runner = SchedulerRunner(
        _config(tmp_path, env_file),
        repository=repository,
        readiness_probe=probe,
        execute_cli=lambda *args: child_calls.append(tuple(args[0])),
        clock=_Clock(),
        sleep=lambda _: None,
    )

    result = runner.run_once(target_date=date(2026, 8, 1))

    assert result.health_status == "skip"
    assert result.probe_attempts == 0
    assert probe.calls == 0
    assert child_calls == []


def test_market_data_deferred_retry_is_bounded_and_does_not_run_6a(
    tmp_path: Path,
) -> None:
    repository, env_file = _setup(tmp_path)
    probe = _SequenceProbe(
        [
            MarketReadiness(
                MarketReadiness.DEFERRED, date(2026, 8, 5), "not published"
            )
        ]
    )
    sleeps: list[float] = []
    child_calls: list[tuple[str, ...]] = []
    runner = SchedulerRunner(
        _config(tmp_path, env_file, max_attempts=3),
        repository=repository,
        readiness_probe=probe,
        execute_cli=lambda *args: child_calls.append(tuple(args[0])),
        clock=_Clock(),
        sleep=sleeps.append,
    )

    result = runner.run_once(trigger="scheduled", target_date=TARGET)

    assert result.operation_status == "deferred"
    assert result.health_status == "warning"
    assert result.probe_attempts == 3
    assert probe.calls == 3
    assert len(sleeps) == 2
    assert child_calls == []
    with repository._transaction() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM daily_research_results"
        ).fetchone()[0] == 0


def test_permanent_readiness_failure_is_hard_failure_without_retry(
    tmp_path: Path,
) -> None:
    repository, env_file = _setup(tmp_path)
    probe = _SequenceProbe(
        [MarketReadiness(MarketReadiness.PERMANENT_FAILURE, None, "invalid payload")]
    )
    sleeps: list[float] = []
    runner = SchedulerRunner(
        _config(tmp_path, env_file),
        repository=repository,
        readiness_probe=probe,
        execute_cli=lambda *args: (_ for _ in ()).throw(
            AssertionError("6A must not run after permanent probe failure")
        ),
        clock=_Clock(),
        sleep=sleeps.append,
    )

    result = runner.run_once(target_date=TARGET)

    assert result.operation_status == "hard_failure"
    assert result.health_status == "hard_failure"
    assert result.probe_attempts == 1
    assert probe.calls == 1
    assert sleeps == []


def test_partial_success_resume_passes_original_batch_and_only_fills_failed_symbol(
    tmp_path: Path,
) -> None:
    repository, env_file = _setup(tmp_path)
    batch_repository = SQLiteBatchRunRepository(repository)
    pipeline = DailyResearchPipeline(
        MockMarketDataProvider(),
        repository,
        retry_policy=RetryPolicy(max_attempts=1, initial_backoff_seconds=0),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    class Fail2330:
        def run(self, symbol, start_date, end_date, **kwargs):
            if symbol == "2330":
                raise RuntimeError("temporary symbol failure")
            return pipeline.run(symbol, start_date, end_date, **kwargs)

    partial = DailyBatchRunner(
        batch_repository=batch_repository,
        research_pipeline=Fail2330(),  # type: ignore[arg-type]
        watchlist_name="acceptance",
        latest_market_date_fn=lambda: TARGET,
        clock=lambda: NOW,
        sleep=lambda _: None,
    ).run(TARGET)
    assert partial.batch_status == BatchRunStatus.PARTIAL_SUCCESS

    commands: list[tuple[str, ...]] = []
    runner = SchedulerRunner(
        _config(tmp_path, env_file),
        repository=repository,
        execute_cli=lambda command, cwd, environment, timeout: _execute_existing_6a(
            commands, command, cwd, environment, timeout
        ),
        clock=_Clock(),
        sleep=lambda _: None,
    )
    result = runner.run_once(target_date=TARGET)

    assert result.health_status == "success"
    assert result.batch_run_id == partial.batch_run_id
    assert "--resume-batch-id" in commands[0]
    assert commands[0][commands[0].index("--resume-batch-id") + 1] == partial.batch_run_id
    with repository._transaction() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM daily_research_results"
        ).fetchone()[0] == len(SYMBOLS)
        assert connection.execute(
            "SELECT COUNT(*) FROM daily_research_reports"
        ).fetchone()[0] == len(SYMBOLS)


def test_expired_scheduler_lease_resumes_original_partial_batch(
    tmp_path: Path,
) -> None:
    repository, env_file = _setup(tmp_path)
    batch_repository = SQLiteBatchRunRepository(repository)
    pipeline = DailyResearchPipeline(
        MockMarketDataProvider(),
        repository,
        retry_policy=RetryPolicy(max_attempts=1, initial_backoff_seconds=0),
        sleep=lambda _: None,
        clock=lambda: NOW,
    )

    class Fail2330:
        def run(self, symbol, start_date, end_date, **kwargs):
            if symbol == "2330":
                raise RuntimeError("crash simulation before resume")
            return pipeline.run(symbol, start_date, end_date, **kwargs)

    partial = DailyBatchRunner(
        batch_repository=batch_repository,
        research_pipeline=Fail2330(),  # type: ignore[arg-type]
        watchlist_name="acceptance",
        latest_market_date_fn=lambda: TARGET,
        clock=lambda: NOW,
        sleep=lambda _: None,
    ).run(TARGET)
    assert partial.batch_status == BatchRunStatus.PARTIAL_SUCCESS

    operations = SQLiteOperationsRepository(repository)
    operations.initialize()
    old = operations.create_invocation(
        invocation_id="crashed-scheduler-invocation",
        job_name="test-scheduler",
        trigger="scheduled",
        mode="eod",
        requested_date=TARGET,
        scheduled_for=NOW - timedelta(minutes=5),
        created_at=NOW - timedelta(minutes=5),
    )
    operations.start_invocation(
        old.invocation_id, started_at=NOW - timedelta(minutes=5), attempt_count=1
    )
    operations.set_batch_run_id(old.invocation_id, partial.batch_run_id, updated_at=NOW)
    operations.acquire_lease(
        lease_key="eod:test-scheduler",
        invocation_id=old.invocation_id,
        lease_id="crashed-scheduler-lease",
        acquired_at=NOW - timedelta(minutes=5),
        expires_at=NOW - timedelta(minutes=1),
    )

    commands: list[tuple[str, ...]] = []
    runner = SchedulerRunner(
        _config(tmp_path, env_file),
        repository=repository,
        execute_cli=lambda command, cwd, environment, timeout: _execute_existing_6a(
            commands, command, cwd, environment, timeout
        ),
        clock=_Clock(),
        sleep=lambda _: None,
    )
    result = runner.run_once(trigger="scheduled", target_date=TARGET)

    assert result.health_status == "success"
    assert result.batch_run_id == partial.batch_run_id
    assert "--resume-batch-id" in commands[0]
    assert operations.get_invocation(old.invocation_id).status == "warning"


def test_task_scheduler_xml_is_eod_only_and_ignores_overlapping_runs(
    tmp_path: Path,
) -> None:
    xml = build_task_scheduler_xml(
        WindowsTaskConfig(
            task_name="IRA-Test",
            project_root=tmp_path,
            python_path="python.exe",
            watchlist="acceptance",
            schedule_time="19:15",
            grace_period_minutes=45,
        )
    )

    assert "CalendarTrigger" in xml
    assert "ScheduleByDay" in xml
    assert "19:15" in xml
    assert "IgnoreNew" in xml
    assert "LeastPrivilege" in xml
    assert "scripts\\scheduler.py" in xml
    assert "--scheduled" in xml
    assert "stream" not in xml.lower()
    assert "trade" not in xml.lower()
