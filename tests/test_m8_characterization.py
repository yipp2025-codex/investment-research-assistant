"""M8.0 characterization freeze for calendar and as-of behavior.

These tests intentionally describe Phase 6A/6B/6C behavior as it exists before
the M8 MarketCalendar contract.  They are compatibility barriers, including
edge cases that are not necessarily the desired future policy.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from app.models import CompanyMetric, DailyPrice, Symbol
from app.operations.scheduler import (
    MarketReadiness,
    SchedulerConfig,
    SchedulerRunner,
    TwseMarketReadinessProbe,
    _CliResult,
)
from app.pipelines.batch_runner import BatchRunSummary
from app.pipelines.trading_day import TradingDayStatus, resolve_trading_day
from app.providers import (
    EsunMarketDataProvider,
    MarketDataBatch,
    MockMarketDataProvider,
    ProviderInvalidPayloadError,
)
from app.reports.daily_research import METHODOLOGY_VERSION, DailyResearchReportService
from app.storage import (
    SQLiteDailyReportRepository,
    SQLiteOperationsRepository,
    SQLiteResearchRepository,
)
from app.storage.batch_run import BatchRunStatus, SQLiteBatchRunRepository
from scripts import run_daily_batch


TARGET = date(2026, 8, 6)
MARKET_DATE = date(2026, 8, 5)
UTC = timezone.utc
TAIPEI = timezone(timedelta(hours=8))
FIXED_UTC = datetime(2026, 8, 6, 10, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    (
        "case",
        "requested_date",
        "latest_market_date",
        "expected_status",
        "expected_resolved_date",
        "expected_calls",
    ),
    [
        (
            "weekend",
            date(2026, 8, 8),
            date(2026, 8, 8),
            TradingDayStatus.SKIPPED_NON_TRADING_DAY,
            None,
            0,
        ),
        (
            "latest-none",
            TARGET,
            None,
            TradingDayStatus.SKIPPED_NO_NEW_MARKET_DATE,
            None,
            1,
        ),
        (
            "latest-before",
            TARGET,
            date(2026, 8, 5),
            TradingDayStatus.DEFERRED_AWAITING_MARKET_DATA,
            None,
            1,
        ),
        (
            "latest-on",
            TARGET,
            TARGET,
            TradingDayStatus.RESOLVED,
            TARGET,
            1,
        ),
        (
            "latest-after",
            TARGET,
            date(2026, 8, 7),
            TradingDayStatus.RESOLVED,
            date(2026, 8, 7),
            1,
        ),
    ],
    ids=("weekend", "latest-none", "latest-before", "latest-on", "latest-after"),
)
def test_6a_trading_day_resolution_frozen_matrix(
    case: str,
    requested_date: date,
    latest_market_date: date | None,
    expected_status: TradingDayStatus,
    expected_resolved_date: date | None,
    expected_calls: int,
) -> None:
    del case
    calls = 0

    def latest_market_date_fn() -> date | None:
        nonlocal calls
        calls += 1
        return latest_market_date

    result = resolve_trading_day(
        requested_date,
        latest_market_date_fn=latest_market_date_fn,
    )

    assert result.requested_date == requested_date
    assert result.status is expected_status
    assert result.resolved_market_date == expected_resolved_date
    assert calls == expected_calls


@dataclass
class _LatestDateProvider:
    latest_market_date: date | None

    def __post_init__(self) -> None:
        self.calls: list[tuple[str, date, date, float]] = []

    def fetch_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        timeout_seconds: float,
    ) -> MarketDataBatch:
        self.calls.append((symbol, start_date, end_date, timeout_seconds))
        prices = (
            ()
            if self.latest_market_date is None
            else ({"trade_date": self.latest_market_date.isoformat()},)
        )
        return MarketDataBatch(
            source="twse",
            symbol={"symbol": symbol},
            daily_prices=prices,
            company_metrics=(),
            market_date=self.latest_market_date,
        )


@pytest.mark.parametrize(
    ("latest_market_date", "expected_status"),
    [
        (None, MarketReadiness.DEFERRED),
        (date(2026, 8, 5), MarketReadiness.DEFERRED),
        (TARGET, MarketReadiness.READY),
        (date(2026, 8, 7), MarketReadiness.READY),
    ],
    ids=("latest-none", "latest-before", "latest-on", "latest-after"),
)
def test_6c_twse_readiness_frozen_publication_matrix(
    latest_market_date: date | None,
    expected_status: str,
) -> None:
    provider = _LatestDateProvider(latest_market_date)
    probe = TwseMarketReadinessProbe(provider=provider)  # type: ignore[arg-type]

    result = probe.check(TARGET, timeout_seconds=7.5)

    assert result.status == expected_status
    assert result.latest_market_date == latest_market_date
    assert provider.calls == [
        ("2330", TARGET - timedelta(days=14), TARGET, 7.5)
    ]


@dataclass
class _CountingProbe:
    readiness: MarketReadiness

    def __post_init__(self) -> None:
        self.calls = 0

    def check(self, target_date: date, *, timeout_seconds: float) -> MarketReadiness:
        del target_date, timeout_seconds
        self.calls += 1
        return self.readiness


def _unexpected_child(*args, **kwargs):
    del args, kwargs
    raise AssertionError("Phase 6A child CLI must not run")


def test_6c_weekend_short_circuits_readiness_and_child_cli(tmp_path: Path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "weekend.db")
    probe = _CountingProbe(
        MarketReadiness(MarketReadiness.READY, date(2026, 8, 8), "unexpected")
    )
    runner = SchedulerRunner(
        SchedulerConfig(
            provider="mock",
            project_root=Path.cwd(),
            database_path=repository.database_path,
        ),
        repository=repository,
        readiness_probe=probe,
        execute_cli=_unexpected_child,
        clock=lambda: FIXED_UTC,
        sleep=lambda _: None,
    )

    result = runner.run_once(target_date=date(2026, 8, 8))

    assert result.operation_status == "skip"
    assert result.health_status == "skip"
    assert result.probe_attempts == 0
    assert probe.calls == 0


def test_standalone_6a_twse_cli_passes_no_latest_date_callback(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "standalone-6a.db"
    env_file = tmp_path / "empty.env"
    env_file.write_text("# characterization only\n", encoding="utf-8")
    summary = BatchRunSummary(
        batch_run_id="characterization-batch",
        watchlist_revision_id="",
        requested_date=TARGET,
        resolved_market_date=None,
        trading_day_status=TradingDayStatus.SKIPPED_NO_NEW_MARKET_DATE,
        batch_status=BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE,
        symbol_results=(),
        idempotent_replay=False,
    )
    output = io.StringIO()

    with patch.dict(
        os.environ,
        {"IRA_DATABASE_PATH": str(database_path)},
        clear=False,
    ), patch.object(
        run_daily_batch,
        "build_market_data_provider",
        return_value=MockMarketDataProvider(),
    ), patch.object(run_daily_batch, "DailyBatchRunner") as runner_type:
        runner_type.return_value.run.return_value = summary
        with redirect_stdout(output):
            exit_code = run_daily_batch.main(
                [
                    "--date",
                    TARGET.isoformat(),
                    "--provider",
                    "twse",
                    "--env-file",
                    str(env_file),
                ]
            )

    assert exit_code == 0
    assert runner_type.call_args.kwargs["latest_market_date_fn"] is None
    runner_type.return_value.run.assert_called_once_with(
        TARGET, resume_batch_run_id=None
    )


def test_6c_child_command_keeps_requested_date_and_skip_flag(tmp_path: Path) -> None:
    runner = SchedulerRunner(
        SchedulerConfig(
            provider="twse",
            project_root=tmp_path,
            database_path=tmp_path / "command.db",
            env_file=tmp_path / "empty.env",
            provider_timeout_seconds=12.5,
        ),
        clock=lambda: FIXED_UTC,
    )

    command = runner.build_6a_command(TARGET)

    assert command[command.index("--date") + 1] == TARGET.isoformat()
    assert "--skip-trading-day-check" in command
    assert command[command.index("--provider") + 1] == "twse"
    assert command[command.index("--provider-timeout-seconds") + 1] == "12.5"


def _epoch_microseconds(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000)


def test_taipei_market_date_switches_at_1600_utc() -> None:
    before_boundary = datetime(2026, 8, 5, 15, 59, 59, tzinfo=UTC)
    on_boundary = datetime(2026, 8, 5, 16, 0, 0, tzinfo=UTC)

    _, before = EsunMarketDataProvider._source_timestamp(
        _epoch_microseconds(before_boundary), date(2026, 8, 5), "lastUpdated"
    )
    _, after = EsunMarketDataProvider._source_timestamp(
        _epoch_microseconds(on_boundary), date(2026, 8, 6), "lastUpdated"
    )

    assert before == before_boundary
    assert after == on_boundary
    with pytest.raises(
        ProviderInvalidPayloadError,
        match="does not match the payload market date",
    ):
        EsunMarketDataProvider._source_timestamp(
            _epoch_microseconds(on_boundary), date(2026, 8, 5), "lastUpdated"
        )


class _TaipeiLocalDateTime(datetime):
    """Make no-argument astimezone deterministic for this characterization."""

    def astimezone(self, tz=None):
        if tz is None:
            return self
        return super().astimezone(tz)


def test_scheduler_default_requested_date_switches_at_taipei_midnight(
    tmp_path: Path,
) -> None:
    instants = (
        _TaipeiLocalDateTime(2026, 8, 5, 23, 59, 59, tzinfo=TAIPEI),
        _TaipeiLocalDateTime(2026, 8, 6, 0, 0, 0, tzinfo=TAIPEI),
    )
    requested_dates = []
    for index, instant in enumerate(instants):
        repository = SQLiteResearchRepository(tmp_path / f"date-switch-{index}.db")
        probe = _CountingProbe(
            MarketReadiness(
                MarketReadiness.PERMANENT_FAILURE,
                None,
                "stop after date resolution",
            )
        )
        runner = SchedulerRunner(
            SchedulerConfig(
                provider="mock",
                project_root=Path.cwd(),
                database_path=repository.database_path,
            ),
            repository=repository,
            readiness_probe=probe,
            execute_cli=_unexpected_child,
            clock=lambda instant=instant: instant,
            sleep=lambda _: None,
        )

        result = runner.run_once(trigger="manual")
        requested_dates.append(result.requested_date)
        assert result.operation_status == "hard_failure"
        assert probe.calls == 1

    assert requested_dates == [date(2026, 8, 5), date(2026, 8, 6)]


class _AdvancingClock:
    def __init__(self, value: _TaipeiLocalDateTime) -> None:
        self.value = value
        self.sleeps: list[float] = []

    def __call__(self) -> _TaipeiLocalDateTime:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value = self.value + timedelta(seconds=seconds)


def test_scheduled_wall_clock_and_grace_deadline_are_frozen(tmp_path: Path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "scheduled-grace.db")
    clock = _AdvancingClock(
        _TaipeiLocalDateTime(2026, 8, 6, 18, 29, 0, tzinfo=TAIPEI)
    )
    probe = _CountingProbe(
        MarketReadiness(
            MarketReadiness.DEFERRED,
            date(2026, 8, 5),
            "not published",
        )
    )
    runner = SchedulerRunner(
        SchedulerConfig(
            provider="mock",
            schedule_time="18:00",
            grace_period_seconds=30 * 60,
            max_deferred_attempts=3,
            retry_backoff_seconds=120,
            project_root=Path.cwd(),
            database_path=repository.database_path,
        ),
        repository=repository,
        readiness_probe=probe,
        execute_cli=_unexpected_child,
        clock=clock,
        sleep=clock.sleep,
    )

    result = runner.run_once(trigger="scheduled", target_date=TARGET)
    invocation = runner.operations.get_invocation(result.invocation_id)

    assert result.operation_status == "deferred"
    assert result.probe_attempts == 2
    assert probe.calls == 2
    assert clock.sleeps == [60.0]
    assert invocation is not None
    assert invocation.scheduled_for == datetime(
        2026, 8, 6, 18, 0, 0, tzinfo=TAIPEI
    )


def _new_report_service(
    database_path: Path,
    *,
    methodology_version: str = METHODOLOGY_VERSION,
) -> tuple[
    SQLiteResearchRepository,
    SQLiteDailyReportRepository,
    DailyResearchReportService,
]:
    repository = SQLiteResearchRepository(database_path)
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()
    repository.upsert_symbol(Symbol("2330", "Test 2330", "TWSE", "TWD"))
    service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        methodology_version=methodology_version,
        clock=lambda: datetime(2026, 8, 8, 3, 0, tzinfo=UTC),
    )
    return repository, report_repository, service


def _price(trade_date: date, close: float) -> DailyPrice:
    return DailyPrice(
        symbol="2330",
        trade_date=trade_date,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1_000,
        source="twse-historical",
    )


def test_6b_price_requires_exact_date_and_excludes_future_data(
    tmp_path: Path,
) -> None:
    missing_repo, _, missing_service = _new_report_service(
        tmp_path / "price-missing.db"
    )
    missing_repo.upsert_daily_prices(
        [
            _price(date(2026, 8, 4), 100.0),
            _price(date(2026, 8, 6), 999.0),
        ]
    )

    missing_payload = missing_service.generate(
        "2330", MARKET_DATE
    ).canonical.payload

    assert missing_payload["data_quality"]["price_status"] == "market_date_mismatch"
    assert missing_payload["data_quality"]["latest_price_date"] == "2026-08-04"
    assert missing_payload["metrics"]["latest_close"] == {
        "status": "market_date_mismatch",
        "value": None,
        "unit": "TWD",
        "as_of_date": None,
    }

    exact_repo, _, exact_service = _new_report_service(tmp_path / "price-exact.db")
    exact_repo.upsert_daily_prices(
        [
            _price(date(2026, 8, 4), 100.0),
            _price(MARKET_DATE, 105.0),
            _price(date(2026, 8, 6), 999.0),
        ]
    )

    exact_payload = exact_service.generate("2330", MARKET_DATE).canonical.payload

    assert exact_payload["data_quality"]["price_status"] == "available"
    assert exact_payload["data_quality"]["latest_price_date"] == "2026-08-05"
    assert exact_payload["metrics"]["latest_close"] == {
        "status": "available",
        "value": 105.0,
        "unit": "TWD",
        "as_of_date": "2026-08-05",
    }


def test_6b_valuation_carries_forward_and_excludes_future_metric(
    tmp_path: Path,
) -> None:
    repository, _, service = _new_report_service(tmp_path / "valuation.db")
    repository.upsert_daily_prices([_price(MARKET_DATE, 105.0)])
    repository.upsert_company_metrics(
        [
            CompanyMetric(
                "2330",
                date(2026, 8, 4),
                "price_earnings_ratio",
                20.0,
                "ratio",
                "twse",
            ),
            CompanyMetric(
                "2330",
                date(2026, 8, 6),
                "price_earnings_ratio",
                99.0,
                "ratio",
                "twse",
            ),
        ]
    )

    payload = service.generate("2330", MARKET_DATE).canonical.payload

    assert payload["valuation"]["pe_ratio"] == {
        "status": "available",
        "value": 20.0,
        "unit": "ratio",
        "as_of_date": "2026-08-04",
    }


def test_6b_previous_successful_result_can_cross_report_gap(tmp_path: Path) -> None:
    repository, _, service = _new_report_service(tmp_path / "previous-gap.db")
    first_date = date(2026, 8, 3)
    current_date = date(2026, 8, 7)
    repository.upsert_daily_prices(
        [_price(first_date, 100.0), _price(current_date, 110.0)]
    )

    previous = service.generate("2330", first_date)
    current = service.generate("2330", current_date)
    comparison = current.canonical.payload["comparison"]

    assert comparison["status"] == "available"
    assert comparison["previous_result_id"] == previous.canonical.result_id
    assert comparison["previous_market_date"] == "2026-08-03"


def test_6b_methodology_mismatch_does_not_compare(tmp_path: Path) -> None:
    database_path = tmp_path / "methodology-mismatch.db"
    repository, report_repository, old_service = _new_report_service(
        database_path,
        methodology_version="old-method-v0",
    )
    first_date = date(2026, 8, 3)
    current_date = date(2026, 8, 7)
    repository.upsert_daily_prices(
        [_price(first_date, 100.0), _price(current_date, 110.0)]
    )
    old = old_service.generate("2330", first_date)
    current_service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        methodology_version=METHODOLOGY_VERSION,
        clock=lambda: datetime(2026, 8, 8, 3, 0, tzinfo=UTC),
    )

    current = current_service.generate("2330", current_date)
    comparison = current.canonical.payload["comparison"]

    assert old.canonical.methodology_version == "old-method-v0"
    assert comparison == {
        "status": "methodology_incompatible",
        "previous_result_id": None,
        "previous_market_date": None,
        "changes": [],
    }


def test_6b_canonical_json_and_sha256_frozen_baseline(tmp_path: Path) -> None:
    repository, report_repository, service = _new_report_service(
        tmp_path / "canonical-baseline.db"
    )
    repository.upsert_daily_prices([_price(MARKET_DATE, 105.0)])
    repository.upsert_company_metrics(
        [
            CompanyMetric(
                "2330",
                date(2026, 8, 4),
                "price_earnings_ratio",
                20.0,
                "ratio",
                "twse",
            )
        ]
    )

    first = service.generate("2330", MARKET_DATE, requested_date=MARKET_DATE)
    replay = service.generate("2330", MARKET_DATE, requested_date=MARKET_DATE)
    expected_json = json.dumps(
        first.canonical.payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    assert first.canonical.payload_json == expected_json
    assert first.canonical.payload_sha256 == hashlib.sha256(
        expected_json.encode("utf-8")
    ).hexdigest()
    assert first.canonical.payload_sha256 == (
        "8730b97cb0f744d0041f1ffde7cb4baf92848ed7318722326625c2f0e356ac64"
    )
    assert replay.idempotent_replay is True
    assert replay.canonical.payload_json == first.canonical.payload_json
    assert replay.canonical.payload_sha256 == first.canonical.payload_sha256
    assert report_repository.count_results("2330") == 1
    assert report_repository.count_reports("2330") == 1


def _execute_existing_6a(
    commands: list[tuple[str, ...]],
    command,
    cwd: Path,
    environment,
    timeout_seconds: float,
) -> _CliResult:
    del cwd, timeout_seconds
    commands.append(tuple(command))
    stdout = io.StringIO()
    stderr = io.StringIO()
    with patch.dict(os.environ, dict(environment), clear=False):
        with redirect_stdout(stdout), redirect_stderr(stderr):
            returncode = run_daily_batch.main(list(command[2:]))
    return _CliResult(returncode, stdout.getvalue(), stderr.getvalue())


def test_6a_6b_6c_end_to_end_frozen_checkpoint_baseline(tmp_path: Path) -> None:
    database_path = tmp_path / "e2e-baseline.db"
    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    symbols = ("2330", "2317")
    for symbol in symbols:
        repository.upsert_symbol(
            Symbol(symbol, f"Synthetic {symbol}", "MOCK", "TWD")
        )
    batch_repository = SQLiteBatchRunRepository(repository)
    batch_repository.initialize()
    watchlist_id = batch_repository.get_or_create_watchlist("m8-freeze")
    batch_repository.set_watchlist_members(watchlist_id, symbols)
    env_file = tmp_path / "empty.env"
    env_file.write_text("# deterministic characterization\n", encoding="utf-8")
    commands: list[tuple[str, ...]] = []
    runner = SchedulerRunner(
        SchedulerConfig(
            job_name="m8-freeze",
            watchlist="m8-freeze",
            provider="mock",
            project_root=Path.cwd(),
            database_path=database_path,
            env_file=env_file,
            max_deferred_attempts=1,
            retry_backoff_seconds=0,
            cli_timeout_seconds=30,
        ),
        repository=repository,
        execute_cli=lambda command, cwd, environment, timeout: _execute_existing_6a(
            commands, command, cwd, environment, timeout
        ),
        clock=lambda: FIXED_UTC,
        sleep=lambda _: None,
    )

    first = runner.run_once(trigger="manual", target_date=TARGET)
    replay = runner.run_once(trigger="scheduled", target_date=TARGET)

    assert first.operation_status == "success"
    assert replay.operation_status == "success"
    assert first.batch_run_id is not None
    assert replay.batch_run_id == first.batch_run_id
    assert replay.idempotent_replay is True
    assert len(commands) == 2
    for command in commands:
        assert command[command.index("--date") + 1] == TARGET.isoformat()
        assert "--skip-trading-day-check" in command

    expected_counts = {
        "daily_batch_runs": 1,
        "daily_symbol_runs": 2,
        "pipeline_runs": 2,
        "research_notes": 2,
        "source_artifacts": 2,
        "daily_research_results": 2,
        "daily_research_reports": 2,
        "scheduler_invocations": 2,
        "operation_leases": 2,
    }
    with sqlite3.connect(database_path) as connection:
        actual_counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in expected_counts
        }
        invocation_statuses = connection.execute(
            "SELECT status FROM scheduler_invocations ORDER BY created_at, rowid"
        ).fetchall()
        stored_batch_ids = connection.execute(
            "SELECT batch_run_id FROM daily_batch_runs"
        ).fetchall()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_violations = connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()

    assert actual_counts == expected_counts
    assert invocation_statuses == [("success",), ("success",)]
    assert stored_batch_ids == [(first.batch_run_id,)]
    assert integrity == "ok"
    assert foreign_key_violations == []
