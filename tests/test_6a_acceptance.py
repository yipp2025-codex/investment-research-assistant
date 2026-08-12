"""Phase 6A acceptance test: 5 consecutive trading days, idempotency, partial resume.

This test is deterministic and uses only MockMarketDataProvider.
No network access required. Run with ``python -m pytest tests/test_6a_acceptance.py -v``.
"""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

import pytest

from app.models import Symbol
from app.pipelines.batch_runner import DailyBatchRunner
from app.pipelines.daily_research import DailyResearchPipeline
from app.pipelines.retry import RetryPolicy
from app.providers import MockMarketDataProvider
from app.providers.mock import MockFailureMode
from app.storage import SQLiteResearchRepository
from app.storage.batch_run import BatchRunStatus, SQLiteBatchRunRepository, SymbolRunStatus


# ---------------------------------------------------------------------------
# 5 consecutive weekday dates (all Monday-Friday, no public holidays assumed)
# ---------------------------------------------------------------------------
TRADING_DAYS = [
    date(2026, 7, 27),  # Monday
    date(2026, 7, 28),  # Tuesday
    date(2026, 7, 29),  # Wednesday
    date(2026, 7, 30),  # Thursday
    date(2026, 7, 31),  # Friday
]
SYMBOLS = ["2330", "2317", "2454"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fixed_clock(dt: datetime | None = None) -> Callable[[], datetime]:
    ts = dt or datetime(2026, 7, 27, 8, 0, 0, tzinfo=timezone.utc)
    return lambda: ts


def _setup(tmp_path: Path) -> tuple[SQLiteResearchRepository, SQLiteBatchRunRepository]:
    repo = SQLiteResearchRepository(tmp_path / "test_accept.db")
    repo.initialize()
    for sym in SYMBOLS:
        repo.upsert_symbol(Symbol(symbol=sym, name=f"Test {sym}", market="TWSE",
                                  currency="TWD", is_active=True))
    batch_repo = SQLiteBatchRunRepository(repo)
    batch_repo.initialize()
    wid = batch_repo.get_or_create_watchlist("acceptance")
    batch_repo.set_watchlist_members(wid, SYMBOLS)
    return repo, batch_repo


def _make_runner(
    batch_repo: SQLiteBatchRunRepository,
    pipeline: DailyResearchPipeline,
    target_date: date,
) -> DailyBatchRunner:
    return DailyBatchRunner(
        batch_repository=batch_repo,
        research_pipeline=pipeline,
        watchlist_name="acceptance",
        latest_market_date_fn=lambda: target_date,  # always resolved for this date
        clock=_fixed_clock(),
        sleep=lambda _: None,
    )


def _make_pipeline(repo: SQLiteResearchRepository,
                   provider: MockMarketDataProvider | None = None) -> DailyResearchPipeline:
    return DailyResearchPipeline(
        provider or MockMarketDataProvider(),
        repo,
        retry_policy=RetryPolicy(max_attempts=3, initial_backoff_seconds=0.0),
        sleep=lambda _: None,
        clock=_fixed_clock(),
    )


def _count_symbol_runs(db_path: Path, batch_run_id: str) -> int:
    conn = sqlite3.connect(db_path)
    count = conn.execute(
        "SELECT COUNT(*) FROM daily_symbol_runs WHERE batch_run_id = ?",
        (batch_run_id,),
    ).fetchone()[0]
    conn.close()
    return count

def _count_pipeline_runs(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
    conn.close()
    return count

def _count_research_notes(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM research_notes").fetchone()[0]
    conn.close()
    return count


# ---------------------------------------------------------------------------
# Test 1: 5-day run x2 — idempotency
# ---------------------------------------------------------------------------

class TestFiveDayIdempotency:
    """Each of 5 consecutive trading days runs twice; second run must replay."""

    def test_five_days_two_runs_each(self, tmp_path: Path) -> None:
        repo, batch_repo = _setup(tmp_path)
        pipeline = _make_pipeline(repo)

        batch_ids: dict[date, str] = {}

        for trading_date in TRADING_DAYS:
            runner = _make_runner(batch_repo, pipeline, trading_date)

            # --- First run ---
            s1 = runner.run(trading_date)
            assert s1.batch_status == BatchRunStatus.SUCCESS, (
                f"Day {trading_date}: first run failed with {s1.batch_status}"
            )
            assert not s1.idempotent_replay
            assert len(s1.symbol_results) == len(SYMBOLS)
            assert all(r.status == SymbolRunStatus.SUCCESS for r in s1.symbol_results), (
                f"Day {trading_date}: not all symbols succeeded: "
                + str([(r.symbol, r.status) for r in s1.symbol_results])
            )
            batch_ids[trading_date] = s1.batch_run_id
            runs_after_first = _count_symbol_runs(repo.database_path, s1.batch_run_id)
            notes_after_first = _count_research_notes(repo.database_path)

            # --- Second run (idempotent replay) ---
            s2 = runner.run(trading_date)
            assert s2.batch_run_id == s1.batch_run_id, (
                f"Day {trading_date}: second run got different batch_run_id"
            )
            assert s2.idempotent_replay is True, (
                f"Day {trading_date}: second run not flagged as idempotent replay"
            )
            assert s2.batch_status == BatchRunStatus.SUCCESS

            # Verify no new rows were inserted.
            runs_after_second = _count_symbol_runs(repo.database_path, s1.batch_run_id)
            notes_after_second = _count_research_notes(repo.database_path)
            assert runs_after_second == runs_after_first, (
                f"Day {trading_date}: symbol_runs count grew from "
                f"{runs_after_first} to {runs_after_second} on replay"
            )
            assert notes_after_second == notes_after_first, (
                f"Day {trading_date}: research_notes count grew from "
                f"{notes_after_first} to {notes_after_second} on replay"
            )

        # Each day must have a distinct batch_run_id.
        assert len(set(batch_ids.values())) == len(TRADING_DAYS), (
            "Different trading days share a batch_run_id"
        )

    def test_pipeline_runs_not_duplicated_across_days(self, tmp_path: Path) -> None:
        """Total pipeline_runs == SYMBOLS × TRADING_DAYS after all 5 days run x2."""
        repo, batch_repo = _setup(tmp_path)
        pipeline = _make_pipeline(repo)
        for trading_date in TRADING_DAYS:
            runner = _make_runner(batch_repo, pipeline, trading_date)
            runner.run(trading_date)
            runner.run(trading_date)  # replay

        expected = len(SYMBOLS) * len(TRADING_DAYS)
        actual = _count_pipeline_runs(repo.database_path)
        # pipeline_runs are per (symbol, target_date) and idempotent by design.
        # MockProvider uses same date for all 5 days, so each symbol gets 1 run/day.
        assert actual == expected, (
            f"Expected {expected} pipeline_runs, got {actual}"
        )


# ---------------------------------------------------------------------------
# Test 2: Day 3 — partial_success → resume → success
# ---------------------------------------------------------------------------

class TestPartialSuccessResume:
    """Day 3: inject 2330 permanent failure → partial_success → resume → success."""

    def test_partial_then_resume(self, tmp_path: Path) -> None:
        repo, batch_repo = _setup(tmp_path)
        target_date = TRADING_DAYS[2]  # Day 3: 2026-07-29

        # First, run days 1 and 2 cleanly so the DB has prior history.
        good_pipeline = _make_pipeline(repo)
        for d in TRADING_DAYS[:2]:
            runner = _make_runner(batch_repo, good_pipeline, d)
            s = runner.run(d)
            assert s.batch_status == BatchRunStatus.SUCCESS

        # Day 3: 2330 always fails, 2317 and 2454 succeed.
        class Fail2330Pipeline:
            source = good_pipeline.provider.source
            def run(self, symbol: str, start: date, end: date, **kw):
                if symbol == "2330":
                    raise RuntimeError("simulated temporary outage for 2330")
                return good_pipeline.run(symbol, start, end, **kw)

        failing_runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=Fail2330Pipeline(),  # type: ignore[arg-type]
            watchlist_name="acceptance",
            latest_market_date_fn=lambda: target_date,
            clock=_fixed_clock(),
            sleep=lambda _: None,
        )
        s_partial = failing_runner.run(target_date)
        assert s_partial.batch_status in (
            BatchRunStatus.PARTIAL_SUCCESS, BatchRunStatus.FAILED
        ), f"Expected partial/failed, got {s_partial.batch_status}"

        statuses = {r.symbol: r.status for r in s_partial.symbol_results}
        assert statuses.get("2330") == SymbolRunStatus.FAILED
        assert statuses.get("2317") == SymbolRunStatus.SUCCESS
        assert statuses.get("2454") == SymbolRunStatus.SUCCESS

        rows_before_resume = _count_symbol_runs(
            repo.database_path, s_partial.batch_run_id
        )

        # Resume with good pipeline — 2330 now succeeds, others already succeeded.
        good_runner = _make_runner(batch_repo, good_pipeline, target_date)
        s_resume = good_runner.run(
            target_date, resume_batch_run_id=s_partial.batch_run_id
        )
        assert s_resume.batch_run_id == s_partial.batch_run_id, (
            "resume must reuse the original batch_run_id"
        )
        assert s_resume.batch_status == BatchRunStatus.SUCCESS, (
            f"resume result: {s_resume.batch_status}"
        )

        statuses_resumed = {r.symbol: r.status for r in s_resume.symbol_results}
        assert statuses_resumed.get("2330") == SymbolRunStatus.SUCCESS
        assert statuses_resumed.get("2317") == SymbolRunStatus.SUCCESS
        assert statuses_resumed.get("2454") == SymbolRunStatus.SUCCESS

        # symbol_run row count must not have grown (same 3 rows, no new inserts).
        rows_after_resume = _count_symbol_runs(
            repo.database_path, s_partial.batch_run_id
        )
        assert rows_after_resume == rows_before_resume, (
            f"symbol_run row count grew from {rows_before_resume} "
            f"to {rows_after_resume} after resume"
        )

    def test_already_succeeded_symbol_not_re_executed_on_resume(
        self, tmp_path: Path
    ) -> None:
        """2317 and 2454 must not execute again during resume of a partial batch."""
        repo, batch_repo = _setup(tmp_path)
        target_date = TRADING_DAYS[2]

        good_pipeline = _make_pipeline(repo)
        executed: list[str] = []

        class TrackingPipeline:
            source = good_pipeline.provider.source

            def run(self, symbol: str, start: date, end: date, **kw):
                executed.append(symbol)
                if symbol == "2330" and executed.count("2330") == 1:
                    raise RuntimeError("first 2330 call fails")
                return good_pipeline.run(symbol, start, end, **kw)

        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=TrackingPipeline(),  # type: ignore[arg-type]
            watchlist_name="acceptance",
            latest_market_date_fn=lambda: target_date,
            clock=_fixed_clock(),
            sleep=lambda _: None,
        )
        s1 = runner.run(target_date)
        assert s1.batch_status in (BatchRunStatus.PARTIAL_SUCCESS, BatchRunStatus.FAILED)

        first_run_count = len(executed)

        # Resume: only 2330 should re-execute.
        s2 = runner.run(target_date, resume_batch_run_id=s1.batch_run_id)
        assert s2.batch_status == BatchRunStatus.SUCCESS

        # Everything appended after the first-run boundary is from the resume phase.
        resume_executions = executed[first_run_count:]
        assert "2330" in resume_executions, (
            f"Expected 2330 to re-execute on resume, got: {resume_executions}"
        )
        non_2330 = [e for e in resume_executions if e != "2330"]
        assert non_2330 == [], (
            f"Symbols other than 2330 executed on resume: {non_2330}"
        )


# ---------------------------------------------------------------------------
# Test 3: SQLite integrity across all 5 days
# ---------------------------------------------------------------------------

class TestSQLiteIntegrityFiveDays:
    def test_fk_and_integrity_after_five_days(self, tmp_path: Path) -> None:
        repo, batch_repo = _setup(tmp_path)
        pipeline = _make_pipeline(repo)
        for trading_date in TRADING_DAYS:
            runner = _make_runner(batch_repo, pipeline, trading_date)
            runner.run(trading_date)

        conn = sqlite3.connect(repo.database_path)
        conn.execute("PRAGMA foreign_keys = ON")
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        conn.close()

        assert fk_violations == [], f"FK violations after 5-day run: {fk_violations}"
        assert integrity == "ok", f"integrity_check: {integrity}"


# ---------------------------------------------------------------------------
# Test 4: weekend is always skipped, never produces pipeline runs
# ---------------------------------------------------------------------------

class TestWeekendSkip:
    def test_saturday_produces_no_pipeline_runs(self, tmp_path: Path) -> None:
        repo, batch_repo = _setup(tmp_path)
        pipeline = _make_pipeline(repo)
        saturday = date(2026, 8, 1)  # Saturday after our 5-day window
        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=pipeline,
            watchlist_name="acceptance",
            latest_market_date_fn=lambda: saturday,
            clock=_fixed_clock(),
        )
        s = runner.run(saturday)
        assert s.batch_status == BatchRunStatus.SKIPPED_NON_TRADING_DAY
        assert _count_pipeline_runs(repo.database_path) == 0
