"""Manual, network-dependent, read-only TWSE OpenAPI smoke test."""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

from app.pipelines import DailyResearchPipeline, RetryPolicy
from app.providers import TwseMarketDataProvider
from app.storage import SQLiteResearchRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a manual read-only TWSE OpenAPI research smoke test."
    )
    parser.add_argument("--symbols", nargs="+", default=["2330", "2317", "2454"])
    parser.add_argument("--database", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=float, default=0.5)
    parser.add_argument("--lookback-days", type=int, default=14)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    today = date.today()
    provider = TwseMarketDataProvider()
    probe = provider.fetch_market_data(
        args.symbols[0],
        today - timedelta(days=args.lookback_days),
        today,
        timeout_seconds=args.timeout_seconds,
    )
    if probe.market_date is None:
        raise RuntimeError("TWSE probe did not return market_date")

    database_path = args.database or Path(
        f"data/live_twse_smoke_{probe.market_date.isoformat()}.db"
    )
    repository = SQLiteResearchRepository(database_path)
    pipeline = DailyResearchPipeline(
        provider,
        repository,
        retry_policy=RetryPolicy(
            max_attempts=args.max_attempts,
            initial_backoff_seconds=args.backoff_seconds,
        ),
        provider_timeout_seconds=args.timeout_seconds,
    )

    results: list[dict[str, object]] = []
    for symbol in args.symbols:
        first = pipeline.run(symbol, probe.market_date, probe.market_date)
        replay = pipeline.run(symbol, probe.market_date, probe.market_date)
        if replay.run_id != first.run_id or not replay.idempotent_replay:
            raise RuntimeError(f"idempotency verification failed for {symbol}")
        results.append(
            {
                "symbol": symbol,
                "market_date": first.market_date.isoformat()
                if first.market_date
                else None,
                "run_id": first.run_id,
                "research_note_id": first.research_note_id,
                "first_execution_replayed": first.idempotent_replay,
                "second_execution_replayed": replay.idempotent_replay,
                "source_endpoints": list(first.source_endpoints),
            }
        )

    print(
        json.dumps(
            {
                "ok": True,
                "provider": "twse",
                "database": str(database_path),
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
