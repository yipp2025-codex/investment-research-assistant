"""Phase 6A tests: watchlist, trading-day resolution, batch runner, idempotency."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable
import pytest

from app.pipelines.batch_runner import (
    BatchRunSummary,
    DailyBatchRunner,
)
from app.pipelines.daily_research import DailyResearchPipeline
from app.pipelines.retry import RetryPolicy
from app.pipelines.trading_day import (
    TradingDayStatus,
    resolve_trading_day,
)
from app.providers import MockMarketDataProvider
from app.storage import SQLiteResearchRepository
from app.storage.batch_run import (
    BatchRunError,
    BatchRunStatus,
    SQLiteBatchRunRepository,
    SymbolRunStatus,
)
from app.models import Symbol


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_repo(tmp_path: Path) -> SQLiteResearchRepository:
    repo = SQLiteResearchRepository(tmp_path / "test.db")
    repo.initialize()
    return repo


def _make_batch_repo(repo: SQLiteResearchRepository) -> SQLiteBatchRunRepository:
    batch_repo = SQLiteBatchRunRepository(repo)
    batch_repo.initialize()
    return batch_repo


def _fixed_clock(dt: datetime | None = None) -> Callable[[], datetime]:
    ts = dt or datetime(2026, 8, 6, 8, 0, 0, tzinfo=timezone.utc)
    return lambda: ts


def _make_pipeline(repo: SQLiteResearchRepository) -> DailyResearchPipeline:
    return DailyResearchPipeline(
        MockMarketDataProvider(),
        repo,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.0),
        sleep=lambda _: None,
        clock=_fixed_clock(),
    )


def _seed_symbols(repo: SQLiteResearchRepository, symbols: list[str]) -> None:
    for sym in symbols:
        repo.upsert_symbol(Symbol(
            symbol=sym.strip().upper(),
            name=f"Test {sym}",
            market="TWSE",
            currency="TWD",
            is_active=True,
        ))


def _populate_watchlist(
    batch_repo: SQLiteBatchRunRepository,
    symbols: list[str],
    watchlist_name: str = "test",
) -> str:
    watchlist_id = batch_repo.get_or_create_watchlist(watchlist_name)
    batch_repo.set_watchlist_members(watchlist_id, symbols)
    return watchlist_id


def _make_runner(
    batch_repo: SQLiteBatchRunRepository,
    research_pipeline: DailyResearchPipeline,
    *,
    latest_market_date: date | None = date(2026, 8, 6),
    watchlist_name: str = "test",
) -> DailyBatchRunner:
    return DailyBatchRunner(
        batch_repository=batch_repo,
        research_pipeline=research_pipeline,
        watchlist_name=watchlist_name,
        latest_market_date_fn=lambda: latest_market_date,
        clock=_fixed_clock(),
        sleep=lambda _: None,
    )


# ---------------------------------------------------------------------------
# Trading-day resolution tests
# ---------------------------------------------------------------------------

class TestTradingDayResolution:
    def test_saturday_is_skipped(self) -> None:
        saturday = date(2026, 8, 8)
        result = resolve_trading_day(saturday, latest_market_date_fn=lambda: saturday)
        assert result.status == TradingDayStatus.SKIPPED_NON_TRADING_DAY
        assert result.resolved_market_date is None

    def test_sunday_is_skipped(self) -> None:
        sunday = date(2026, 8, 9)
        result = resolve_trading_day(sunday, latest_market_date_fn=lambda: sunday)
        assert result.status == TradingDayStatus.SKIPPED_NON_TRADING_DAY

    def test_weekend_does_not_call_provider(self) -> None:
        called = []
        def fn() -> date | None:
            called.append(True)
            return date(2026, 8, 8)
        resolve_trading_day(date(2026, 8, 8), latest_market_date_fn=fn)
        assert called == [], "provider should not be called for weekends"

    def test_weekday_resolved_when_provider_returns_same_date(self) -> None:
        target = date(2026, 8, 6)
        result = resolve_trading_day(target, latest_market_date_fn=lambda: target)
        assert result.status == TradingDayStatus.RESOLVED
        assert result.resolved_market_date == target

    def test_weekday_deferred_when_provider_returns_earlier_date(self) -> None:
        target = date(2026, 8, 6)
        result = resolve_trading_day(
            target, latest_market_date_fn=lambda: date(2026, 8, 5)
        )
        assert result.status == TradingDayStatus.DEFERRED_AWAITING_MARKET_DATA
        assert result.resolved_market_date is None

    def test_weekday_skipped_when_provider_returns_none(self) -> None:
        target = date(2026, 8, 6)
        result = resolve_trading_day(target, latest_market_date_fn=lambda: None)
        assert result.status == TradingDayStatus.SKIPPED_NO_NEW_MARKET_DATE


# ---------------------------------------------------------------------------
# Watchlist management
# ---------------------------------------------------------------------------

class TestWatchlistManagement:
    def test_get_or_create_watchlist_is_idempotent(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        batch_repo = _make_batch_repo(repo)
        id1 = batch_repo.get_or_create_watchlist("my-list")
        id2 = batch_repo.get_or_create_watchlist("my-list")
        assert id1 == id2

    def test_empty_watchlist_name_raises(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        batch_repo = _make_batch_repo(repo)
        with pytest.raises(ValueError, match="must not be empty"):
            batch_repo.get_or_create_watchlist("  ")

    def test_set_and_retrieve_watchlist_members(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330", "2317"])
        batch_repo = _make_batch_repo(repo)
        wid = batch_repo.get_or_create_watchlist("test")
        batch_repo.set_watchlist_members(wid, ["2330", "2317"])
        members = batch_repo.get_active_watchlist_symbols(wid)
        assert set(members) == {"2330", "2317"}

    def test_deactivated_symbols_are_excluded(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330", "2317", "2454"])
        batch_repo = _make_batch_repo(repo)
        wid = batch_repo.get_or_create_watchlist("test")
        batch_repo.set_watchlist_members(wid, ["2330", "2317", "2454"])
        batch_repo.set_watchlist_members(wid, ["2330", "2317"])
        members = batch_repo.get_active_watchlist_symbols(wid)
        assert "2454" not in members

    def test_revision_freezes_member_set(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330", "2317"])
        batch_repo = _make_batch_repo(repo)
        wid = batch_repo.get_or_create_watchlist("test")
        batch_repo.set_watchlist_members(wid, ["2330", "2317"])
        revision = batch_repo.create_revision(wid)
        _seed_symbols(repo, ["2454"])
        batch_repo.set_watchlist_members(wid, ["2454"])
        fetched = batch_repo.get_revision(revision.revision_id)
        assert fetched is not None
        assert set(fetched.symbols) == {"2330", "2317"}

    def test_revision_requires_active_members(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        batch_repo = _make_batch_repo(repo)
        wid = batch_repo.get_or_create_watchlist("empty")
        with pytest.raises(BatchRunError, match="no active members"):
            batch_repo.create_revision(wid)


# ---------------------------------------------------------------------------
# Batch runner: success path
# ---------------------------------------------------------------------------

class TestDailyBatchRunnerSuccess:
    def test_batch_succeeds_for_mock_symbols(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330", "2317"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330", "2317"])
        pipeline = _make_pipeline(repo)
        runner = _make_runner(batch_repo, pipeline)
        summary = runner.run(date(2026, 8, 6))
        assert summary.batch_status == BatchRunStatus.SUCCESS
        assert len(summary.symbol_results) == 2
        assert all(
            sr.status == SymbolRunStatus.SUCCESS for sr in summary.symbol_results
        )
        assert summary.resolved_market_date == date(2026, 8, 6)

    def test_batch_idempotent_replay_on_second_call(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330"])
        pipeline = _make_pipeline(repo)
        runner = _make_runner(batch_repo, pipeline)
        s1 = runner.run(date(2026, 8, 6))
        s2 = runner.run(date(2026, 8, 6))
        assert s1.batch_run_id == s2.batch_run_id
        assert s2.idempotent_replay is True
        assert s2.batch_status == BatchRunStatus.SUCCESS

    def test_successful_symbol_not_rerun_on_second_call(
        self, tmp_path: Path
    ) -> None:
        """Symbol that succeeded must not be re-executed on idempotent replay."""
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330", "2317"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330", "2317"])

        run_counts: dict[str, int] = {}
        real_pipeline = _make_pipeline(repo)

        class CountingPipeline:
            source = real_pipeline.provider.source

            def run(self, symbol: str, start: date, end: date, **kw):
                run_counts[symbol] = run_counts.get(symbol, 0) + 1
                return real_pipeline.run(symbol, start, end, **kw)

        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=CountingPipeline(),  # type: ignore[arg-type]
            watchlist_name="test",
            latest_market_date_fn=lambda: date(2026, 8, 6),
            clock=_fixed_clock(),
            sleep=lambda _: None,
        )
        s1 = runner.run(date(2026, 8, 6))
        assert s1.batch_status == BatchRunStatus.SUCCESS
        s2 = runner.run(date(2026, 8, 6))
        assert s2.idempotent_replay is True
        # Each symbol ran at most once (idempotent replay does not re-run pipeline).
        assert run_counts.get("2330", 0) <= 1
        assert run_counts.get("2317", 0) <= 1


# ---------------------------------------------------------------------------
# Batch runner: skip / defer
# ---------------------------------------------------------------------------

class TestTradingDaySkip:
    def test_saturday_batch_skipped(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330"])
        pipeline = _make_pipeline(repo)
        # For weekends, trading_day resolution ignores latest_market_date_fn
        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=pipeline,
            watchlist_name="test",
            latest_market_date_fn=lambda: date(2026, 8, 6),
            clock=_fixed_clock(),
        )
        saturday = date(2026, 8, 8)
        summary = runner.run(saturday)
        assert summary.batch_status == BatchRunStatus.SKIPPED_NON_TRADING_DAY
        assert summary.symbol_results == ()

    def test_no_market_date_batch_skipped(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330"])
        pipeline = _make_pipeline(repo)
        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=pipeline,
            watchlist_name="test",
            latest_market_date_fn=lambda: None,
            clock=_fixed_clock(),
        )
        summary = runner.run(date(2026, 8, 6))
        assert summary.batch_status == BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE

    def test_deferred_when_market_data_not_yet_published(
        self, tmp_path: Path
    ) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330"])
        pipeline = _make_pipeline(repo)
        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=pipeline,
            watchlist_name="test",
            latest_market_date_fn=lambda: date(2026, 8, 5),
            clock=_fixed_clock(),
        )
        summary = runner.run(date(2026, 8, 6))
        assert summary.batch_status == BatchRunStatus.DEFERRED_AWAITING_MARKET_DATA


# ---------------------------------------------------------------------------
# Batch runner: per-symbol failure isolation
# ---------------------------------------------------------------------------

class TestSymbolFailureIsolation:
    def test_one_symbol_failure_does_not_stop_others(
        self, tmp_path: Path
    ) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330", "2317"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330", "2317"])

        real_pipeline = _make_pipeline(repo)

        # Raise for 2330, succeed for others.
        class FailSpecificPipeline:
            source = real_pipeline.provider.source

            def run(self, symbol: str, start: date, end: date, **kw):
                if symbol == "2330":
                    raise RuntimeError("simulated permanent failure for 2330")
                return real_pipeline.run(symbol, start, end, **kw)

        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=FailSpecificPipeline(),  # type: ignore[arg-type]
            watchlist_name="test",
            latest_market_date_fn=lambda: date(2026, 8, 6),
            clock=_fixed_clock(),
        )
        summary = runner.run(date(2026, 8, 6))
        assert summary.batch_status in (
            BatchRunStatus.PARTIAL_SUCCESS,
            BatchRunStatus.FAILED,
        )
        statuses = {sr.symbol: sr.status for sr in summary.symbol_results}
        assert statuses.get("2317") == SymbolRunStatus.SUCCESS
        assert statuses.get("2330") == SymbolRunStatus.FAILED

    def test_failed_symbol_has_safe_error_message(
        self, tmp_path: Path
    ) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330"])
        fake_secret = "test-" + "only-secret"
        fake_token = "test-" + "only-token"

        class AlwaysFail:
            source = "mock"

            def run(self, *a, **kw):
                raise RuntimeError(f"api_key={fake_secret} token={fake_token}")

        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=AlwaysFail(),  # type: ignore[arg-type]
            watchlist_name="test",
            latest_market_date_fn=lambda: date(2026, 8, 6),
            clock=_fixed_clock(),
        )
        summary = runner.run(date(2026, 8, 6))
        sr = summary.symbol_results[0]
        assert sr.status == SymbolRunStatus.FAILED
        assert sr.error_message is not None
        assert fake_secret not in sr.error_message
        assert fake_token not in sr.error_message
        assert "[REDACTED]" in sr.error_message

    def test_all_failures_give_failed_status(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330", "2317"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330", "2317"])

        class AlwaysFail:
            source = "mock"
            def run(self, *a, **kw):
                raise RuntimeError("always fails")

        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=AlwaysFail(),  # type: ignore[arg-type]
            watchlist_name="test",
            latest_market_date_fn=lambda: date(2026, 8, 6),
            clock=_fixed_clock(),
        )
        summary = runner.run(date(2026, 8, 6))
        assert summary.batch_status == BatchRunStatus.FAILED


# ---------------------------------------------------------------------------
# Batch runner: interrupt and resume
# ---------------------------------------------------------------------------

class TestInterruptAndResume:
    def test_interrupted_batch_resumes_with_original_id(
        self, tmp_path: Path
    ) -> None:
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330", "2317"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330", "2317"])

        real_pipeline = _make_pipeline(repo)

        class FailFor2330:
            source = real_pipeline.provider.source
            def run(self, symbol: str, start: date, end: date, **kw):
                if symbol == "2330":
                    raise RuntimeError("crash")
                return real_pipeline.run(symbol, start, end, **kw)

        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=FailFor2330(),  # type: ignore[arg-type]
            watchlist_name="test",
            latest_market_date_fn=lambda: date(2026, 8, 6),
            clock=_fixed_clock(),
        )
        s1 = runner.run(date(2026, 8, 6))
        # First run: 2317 OK, 2330 failed -> partial or failed.
        assert s1.batch_status in (BatchRunStatus.PARTIAL_SUCCESS, BatchRunStatus.FAILED)

        # Resume with fixed pipeline.
        real_runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=real_pipeline,
            watchlist_name="test",
            latest_market_date_fn=lambda: date(2026, 8, 6),
            clock=_fixed_clock(),
        )
        s2 = real_runner.run(
            date(2026, 8, 6),
            resume_batch_run_id=s1.batch_run_id,
        )
        assert s2.batch_run_id == s1.batch_run_id
        statuses = {sr.symbol: sr.status for sr in s2.symbol_results}
        # 2317 was already success -> skipped or still success.
        assert statuses.get("2317") == SymbolRunStatus.SUCCESS
        # 2330 should now succeed.
        assert statuses.get("2330") == SymbolRunStatus.SUCCESS


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------

class TestSchemaMigration:
    def test_migration_7_applied_once(self, tmp_path: Path) -> None:
        repo = _make_repo(tmp_path)
        batch_repo = SQLiteBatchRunRepository(repo)
        batch_repo.initialize()
        batch_repo.initialize()
        version = repo.get_schema_version()
        assert version >= 7

    def test_tables_created(self, tmp_path: Path) -> None:
        import sqlite3
        repo = _make_repo(tmp_path)
        batch_repo = SQLiteBatchRunRepository(repo)
        batch_repo.initialize()
        conn = sqlite3.connect(repo.database_path)
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        conn.close()
        for expected in (
            "watchlists",
            "watchlist_members",
            "watchlist_revisions",
            "watchlist_revision_members",
            "daily_batch_runs",
            "daily_symbol_runs",
        ):
            assert expected in tables, f"missing table: {expected}"


# ---------------------------------------------------------------------------
# SQLite integrity check
# ---------------------------------------------------------------------------

class TestSQLiteIntegrity:
    def test_fk_integrity_after_successful_batch(self, tmp_path: Path) -> None:
        import sqlite3
        repo = _make_repo(tmp_path)
        _seed_symbols(repo, ["2330"])
        batch_repo = _make_batch_repo(repo)
        _populate_watchlist(batch_repo, ["2330"])
        pipeline = _make_pipeline(repo)
        runner = _make_runner(batch_repo, pipeline)
        runner.run(date(2026, 8, 6))

        conn = sqlite3.connect(repo.database_path)
        conn.execute("PRAGMA foreign_keys = ON")
        fk_result = conn.execute("PRAGMA foreign_key_check").fetchall()
        ic = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        assert fk_result == [], f"FK violations: {fk_result}"
        assert ic[0] == "ok"
