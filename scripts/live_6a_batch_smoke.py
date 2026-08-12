"""Phase 6A Live Smoke Test: TWSE batch runner (read-only, no pytest required).

Run manually:
    .venv/Scripts/python.exe scripts/live_6a_batch_smoke.py

Requires internet access to openapi.twse.com.tw.
Network failure does NOT cause pytest failure; this script is standalone.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.models import Symbol
from app.pipelines.batch_runner import DailyBatchRunner
from app.pipelines.daily_research import DailyResearchPipeline
from app.pipelines.retry import RetryPolicy
from app.providers import TwseMarketDataProvider
from app.storage import SQLiteResearchRepository
from app.storage.batch_run import BatchRunStatus, SQLiteBatchRunRepository


SYMBOLS = ["2330", "2317", "2454"]
WATCHLIST = "live-smoke-6a"


def main() -> int:
    today = date.today()
    print(f"Phase 6A Live Smoke — {today}")
    print(f"Symbols: {SYMBOLS}")
    print()

    # ------------------------------------------------------------------
    # Probe TWSE to resolve the latest market date.
    # ------------------------------------------------------------------
    print("Probing TWSE for latest market date...")
    try:
        provider = TwseMarketDataProvider()
        probe = provider.fetch_market_data(
            "2330",
            today - timedelta(days=14),
            today,
            timeout_seconds=15.0,
        )
    except Exception as exc:
        print(f"TWSE probe failed: {exc}")
        print("Live smoke SKIPPED (network unavailable).")
        return 0

    market_date = probe.market_date
    if market_date is None:
        print("TWSE returned no market_date — likely a holiday or off-hours.")
        print("Live smoke SKIPPED.")
        return 0

    print(f"Latest TWSE market date: {market_date}")

    # ------------------------------------------------------------------
    # Run batch in a temp DB so live smoke never touches the real DB.
    # ------------------------------------------------------------------
    with tempfile.TemporaryDirectory(prefix="ira-live-smoke-") as tmp_dir:
        db_path = Path(tmp_dir) / "smoke.db"
        repo = SQLiteResearchRepository(db_path)
        repo.initialize()

        # Seed symbol records.
        for sym in SYMBOLS:
            repo.upsert_symbol(Symbol(
                symbol=sym, name=f"Live {sym}", market="TWSE",
                currency="TWD", is_active=True,
            ))

        batch_repo = SQLiteBatchRunRepository(repo)
        batch_repo.initialize()
        wid = batch_repo.get_or_create_watchlist(WATCHLIST)
        batch_repo.set_watchlist_members(wid, SYMBOLS)

        pipeline = DailyResearchPipeline(
            provider,
            repo,
            retry_policy=RetryPolicy(
                max_attempts=3,
                initial_backoff_seconds=1.0,
                backoff_multiplier=2.0,
            ),
            provider_timeout_seconds=15.0,
        )

        runner = DailyBatchRunner(
            batch_repository=batch_repo,
            research_pipeline=pipeline,
            watchlist_name=WATCHLIST,
            latest_market_date_fn=lambda: market_date,
        )

        print(f"\nRunning batch for market_date={market_date}...")
        try:
            s = runner.run(market_date)
        except Exception as exc:
            print(f"Batch run raised: {exc}")
            return 1

        print(f"batch_run_id : {s.batch_run_id}")
        print(f"status       : {s.batch_status.value}")
        print(f"market_date  : {s.resolved_market_date}")
        print(f"idempotent   : {s.idempotent_replay}")
        print()

        for sr in s.symbol_results:
            tag = "OK" if sr.status.value == "success" else sr.status.value.upper()
            err = f" ({sr.error_message})" if sr.error_message else ""
            print(f"  [{tag}] {sr.symbol}  pipeline_run_id={sr.pipeline_run_id}{err}")

        # ------------------------------------------------------------------
        # Run again — must be idempotent replay.
        # ------------------------------------------------------------------
        print("\nRunning again (idempotency check)...")
        s2 = runner.run(market_date)
        if s2.batch_run_id != s.batch_run_id:
            print(f"FAIL: second run got new batch_run_id {s2.batch_run_id}")
            return 1
        if not s2.idempotent_replay:
            print("FAIL: second run not flagged as idempotent replay")
            return 1
        print(f"OK — same batch_run_id, idempotent_replay=True")

        # ------------------------------------------------------------------
        # SQLite integrity check.
        # ------------------------------------------------------------------
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()
        ic = conn.execute("PRAGMA integrity_check").fetchone()[0]
        conn.close()
        if fk:
            print(f"FAIL: FK violations: {fk}")
            return 1
        if ic != "ok":
            print(f"FAIL: integrity_check: {ic}")
            return 1
        print(f"SQLite integrity: {ic}")

        if s.batch_status not in (BatchRunStatus.SUCCESS, BatchRunStatus.PARTIAL_SUCCESS):
            print(f"\nWARN: batch_status={s.batch_status.value} (not all symbols succeeded)")
        else:
            print(f"\nPhase 6A Live Smoke PASSED")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
