"""Manual, network-dependent, read-only TWSE historical smoke test."""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

from app.pipelines import DailyResearchPipeline, HistoricalSyncPipeline, RetryPolicy
from app.providers import TwseHistoricalMarketDataProvider, TwseMarketDataProvider
from app.storage import SQLiteHistoricalSyncRepository, SQLiteResearchRepository


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a manual read-only TWSE snapshot + historical research smoke test."
        )
    )
    parser.add_argument("--symbols", nargs="+", default=["2330", "2317", "2454"])
    parser.add_argument("--database", type=Path)
    parser.add_argument("--target-date", type=_iso_date)
    parser.add_argument("--target-observations", type=int, default=250)
    parser.add_argument("--max-months", type=int, default=18)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=float, default=0.5)
    parser.add_argument("--lookback-days", type=int, default=14)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    retry_policy = RetryPolicy(
        max_attempts=args.max_attempts,
        initial_backoff_seconds=args.backoff_seconds,
    )
    snapshot_provider = TwseMarketDataProvider()
    target_date = args.target_date
    if target_date is None:
        today = date.today()
        probe = snapshot_provider.fetch_market_data(
            args.symbols[0],
            today - timedelta(days=args.lookback_days),
            today,
            timeout_seconds=args.timeout_seconds,
        )
        if probe.market_date is None:
            raise RuntimeError("TWSE snapshot probe did not return market_date")
        target_date = probe.market_date

    database_path = args.database or Path(
        f"data/live_twse_history_smoke_{target_date.isoformat()}.db"
    )
    repository = SQLiteResearchRepository(database_path)
    snapshot_pipeline = DailyResearchPipeline(
        snapshot_provider,
        repository,
        retry_policy=retry_policy,
        provider_timeout_seconds=args.timeout_seconds,
    )
    historical_pipeline = HistoricalSyncPipeline(
        TwseHistoricalMarketDataProvider(),
        repository,
        retry_policy=retry_policy,
        provider_timeout_seconds=args.timeout_seconds,
    )
    history_repository = SQLiteHistoricalSyncRepository(repository)

    results: list[dict[str, object]] = []
    for symbol in args.symbols:
        snapshot = snapshot_pipeline.run(symbol, target_date, target_date)
        first = historical_pipeline.run(
            symbol,
            target_date,
            target_observations=args.target_observations,
            max_months=args.max_months,
        )
        replay = historical_pipeline.run(
            symbol,
            target_date,
            target_observations=args.target_observations,
            max_months=args.max_months,
        )
        if replay.run_id != first.run_id or not replay.idempotent_replay:
            raise RuntimeError(f"historical idempotency verification failed for {symbol}")
        run = history_repository.get(first.run_id)
        if run is None:
            raise RuntimeError(f"historical checkpoint disappeared for {symbol}")
        results.append(
            {
                "symbol": symbol,
                "target_market_date": target_date.isoformat(),
                "snapshot_market_date": (
                    snapshot.market_date.isoformat() if snapshot.market_date else None
                ),
                "historical_period_start": first.analysis.period_start.isoformat(),
                "historical_period_end": first.analysis.period_end.isoformat(),
                "observations": first.observation_count,
                "months_completed": first.months_completed,
                "run_id": first.run_id,
                "research_note_id": first.research_note_id,
                "second_execution_replayed": replay.idempotent_replay,
                "latest_source_endpoint": run.source_endpoint,
                "latest_fetched_at": (
                    run.fetched_at.isoformat() if run.fetched_at else None
                ),
            }
        )

    print(
        json.dumps(
            {
                "ok": True,
                "provider": "twse-historical",
                "database": str(database_path),
                "target_observations": args.target_observations,
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
