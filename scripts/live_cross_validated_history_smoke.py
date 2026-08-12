"""Manual read-only 60-250 day TWSE/E.SUN historical research smoke test."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from app.config import Settings
from app.pipelines import (
    CrossValidatedHistoricalResearchPipeline,
    DailyResearchPipeline,
    RetryPolicy,
)
from app.providers import (
    EsunHistoricalMarketDataProvider,
    TwseHistoricalMarketDataProvider,
    TwseMarketDataProvider,
)
from app.storage import SQLiteHistoricalSyncRepository, SQLiteResearchRepository


SYMBOLS = ("2330", "2317", "2454")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run read-only cross-validated TWSE/E.SUN historical research."
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--database", type=Path)
    parser.add_argument("--symbols", nargs="+", default=list(SYMBOLS))
    parser.add_argument("--target-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--target-observations", type=int, default=60)
    parser.add_argument("--max-months", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=float, default=0.5)
    parser.add_argument("--lookback-days", type=int, default=14)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not 60 <= args.target_observations <= 250:
        raise SystemExit("target-observations must be between 60 and 250")
    settings = Settings.from_env(args.env_file)
    config_path = args.config or settings.esun_marketdata_config_path
    if config_path is None:
        raise SystemExit(
            "provide --config or set ESUN_MARKETDATA_CONFIG_PATH in an ignored .env"
        )
    database_path = args.database or Path(
        "data/"
        f"live_cross_validated_history_{args.target_date.isoformat()}_"
        f"{args.target_observations}.db"
    )
    repository = SQLiteResearchRepository(database_path)
    retry_policy = RetryPolicy(
        max_attempts=args.max_attempts,
        initial_backoff_seconds=args.backoff_seconds,
    )

    twse_seed_provider = TwseMarketDataProvider()
    probe = twse_seed_provider.fetch_market_data(
        args.symbols[0],
        args.target_date - timedelta(days=args.lookback_days),
        args.target_date,
        timeout_seconds=args.timeout_seconds,
    )
    if probe.market_date is None:
        raise RuntimeError("TWSE symbol seed probe did not return market_date")
    seed_pipeline = DailyResearchPipeline(
        twse_seed_provider,
        repository,
        retry_policy=retry_policy,
        provider_timeout_seconds=args.timeout_seconds,
    )
    for symbol in args.symbols:
        seed_pipeline.run(symbol, probe.market_date, probe.market_date)

    pipeline = CrossValidatedHistoricalResearchPipeline(
        TwseHistoricalMarketDataProvider(),
        EsunHistoricalMarketDataProvider(config_path=config_path),
        repository,
        retry_policy=retry_policy,
        provider_timeout_seconds=args.timeout_seconds,
    )
    history_repository = SQLiteHistoricalSyncRepository(repository)
    results: list[dict[str, object]] = []
    for symbol in args.symbols:
        first = pipeline.run(
            symbol,
            args.target_date,
            target_observations=args.target_observations,
            max_months=args.max_months,
        )
        replay = pipeline.run(
            symbol,
            args.target_date,
            target_observations=args.target_observations,
            max_months=args.max_months,
        )
        if replay.run_id != first.run_id or not replay.idempotent_replay:
            raise RuntimeError(f"historical validation replay failed for {symbol}")
        esun_rows = history_repository.list_source_observations(
            first.right_sync.run_id,
            end_date=args.target_date,
            limit=args.target_observations,
        )
        if len(esun_rows) != args.target_observations:
            raise RuntimeError(f"E.SUN source observations are incomplete for {symbol}")
        canonical = repository.list_daily_prices(symbol, end_date=args.target_date)
        if any(price.source == "esun-historical" for price in canonical):
            raise RuntimeError(f"E.SUN overwrote canonical prices for {symbol}")
        results.append(
            {
                "symbol": symbol,
                "validation_run_id": first.run_id,
                "twse_run_id": first.left_sync.run_id,
                "esun_run_id": first.right_sync.run_id,
                "outcome": first.outcome.value,
                "target_observations": first.target_observations,
                "analysis_period": {
                    "start": first.analysis.period_start.isoformat(),
                    "end": first.analysis.period_end.isoformat(),
                },
                "latest_market_dates": {
                    "twse": first.left_latest_date.isoformat(),
                    "esun": first.right_latest_date.isoformat(),
                },
                "common_dates": first.common_date_count,
                "fully_matched_dates": first.matched_date_count,
                "twse_only_dates": first.left_only_date_count,
                "esun_only_dates": first.right_only_date_count,
                "field_discrepancies": first.field_discrepancy_count,
                "discrepancy_preview": [
                    {
                        "date": item.trade_date.isoformat(),
                        "field": item.field,
                        "left": item.left_value,
                        "right": item.right_value,
                        "reason": item.reason,
                    }
                    for item in first.discrepancies[:20]
                ],
                "research_note_id": first.research_note_id,
                "second_execution_replayed": replay.idempotent_replay,
                "canonical_sources": sorted({price.source for price in canonical}),
            }
        )

    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_violations = connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()
    if integrity != "ok" or foreign_key_violations:
        raise RuntimeError("historical live smoke database integrity failed")

    print(
        json.dumps(
            {
                "ok": True,
                "mode": "read-only",
                "schema_version": repository.get_schema_version(),
                "database": str(database_path),
                "target_date": args.target_date.isoformat(),
                "twse_seed_market_date": probe.market_date.isoformat(),
                "target_observations": args.target_observations,
                "results": results,
                "sqlite_integrity": integrity,
                "foreign_key_violations": len(foreign_key_violations),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
