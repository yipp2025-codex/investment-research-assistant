"""Manual, network-dependent, read-only E.SUN/TWSE Phase 5 smoke test."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

from app.config import Settings
from app.pipelines import (
    DailyResearchPipeline,
    MarketDataCrossValidationPipeline,
    RetryPolicy,
)
from app.providers import (
    EsunMarketDataProvider,
    ProviderNotImplementedError,
    ProviderPermanentError,
    TwseMarketDataProvider,
)
from app.storage import SQLiteResearchRepository


SYMBOLS = ("2330", "2317", "2454")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a manual read-only E.SUN market-data and TWSE comparison smoke test."
        )
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--database", type=Path)
    parser.add_argument("--symbols", nargs="+", default=list(SYMBOLS))
    parser.add_argument("--target-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=float, default=0.5)
    return parser


def _iso(value):
    return None if value is None else value.isoformat()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.lookback_days < 1:
        raise SystemExit("lookback-days must be at least 1")
    settings = Settings.from_env(args.env_file)
    config_path = args.config or settings.esun_marketdata_config_path
    if config_path is None:
        raise SystemExit(
            "provide --config or set ESUN_MARKETDATA_CONFIG_PATH in an ignored .env"
        )

    target_date = args.target_date
    start_date = target_date - timedelta(days=args.lookback_days)
    database_path = args.database or Path(
        f"data/live_esun_phase5_{target_date.isoformat()}.db"
    )
    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    preexisting = repository.list_daily_prices("2330")
    if any(price.source != "esun" for price in preexisting):
        raise RuntimeError(
            "live Phase 5 database already contains non-E.SUN canonical prices"
        )

    esun = EsunMarketDataProvider(config_path=config_path)
    twse = TwseMarketDataProvider()
    retry_policy = RetryPolicy(
        max_attempts=args.max_attempts,
        initial_backoff_seconds=args.backoff_seconds,
    )
    validator = MarketDataCrossValidationPipeline(
        twse,
        esun,
        repository,
        retry_policy=retry_policy,
        provider_timeout_seconds=args.timeout_seconds,
    )

    validation_results: list[dict[str, object]] = []
    quote_results: list[dict[str, object]] = []
    stats_results: list[dict[str, object]] = []
    for symbol in args.symbols:
        first = validator.run(symbol, start_date, target_date)
        replay = validator.run(symbol, start_date, target_date)
        if replay.run_id != first.run_id or not replay.idempotent_replay:
            raise RuntimeError(f"cross-validation idempotency failed for {symbol}")
        validation_results.append(
            {
                "symbol": symbol,
                "run_id": first.run_id,
                "outcome": first.outcome.value,
                "second_execution_replayed": replay.idempotent_replay,
                "observations": [
                    {
                        "provider": item.provider,
                        "market_date": item.market_date.isoformat(),
                        "open": item.open,
                        "high": item.high,
                        "low": item.low,
                        "close": item.close,
                        "volume": item.volume,
                        "source_timestamp": _iso(item.source_timestamp),
                        "fetched_at": item.fetched_at.isoformat(),
                        "source_endpoints": list(item.source_endpoints),
                    }
                    for item in first.observations
                ],
                "discrepancies": [
                    {
                        "field": item.field,
                        "left_value": item.left_value,
                        "right_value": item.right_value,
                        "reason": item.reason,
                        "absolute_difference": item.absolute_difference,
                        "relative_difference_pct": item.relative_difference_pct,
                    }
                    for item in first.discrepancies
                ],
            }
        )

        quote = esun.fetch_quote(symbol, timeout_seconds=args.timeout_seconds)
        quote_results.append(
            {
                "symbol": symbol,
                "market_date": quote.market_date.isoformat(),
                "source_timestamp": _iso(quote.source_timestamp),
                "is_close": quote.is_close,
                "ohlc_present": quote.open is not None,
                "volume_semantics": "raw_intraday_value_not_canonical_daily_volume",
            }
        )
        stats = esun.fetch_historical_stats(
            symbol, timeout_seconds=args.timeout_seconds
        )
        stats_results.append(
            {
                "symbol": symbol,
                "market_date": stats.market_date.isoformat(),
                "close": stats.close,
                "volume": stats.volume,
            }
        )

    snapshot_result: dict[str, object]
    try:
        esun.fetch_snapshot_quotes(
            market="TSE", symbols=args.symbols, timeout_seconds=args.timeout_seconds
        )
    except ProviderNotImplementedError as error:
        snapshot_result = {
            "available": False,
            "classification": type(error).__name__,
            "reason": "successful_live_fixture_required_before_mapping",
        }
    except ProviderPermanentError as error:
        snapshot_result = {
            "available": False,
            "classification": type(error).__name__,
            "reason": "basic_plan_403",
        }
    else:  # pragma: no cover - mapping is intentionally fail closed.
        raise RuntimeError("snapshot mapping unexpectedly bypassed its evidence gate")

    research_pipeline = DailyResearchPipeline(
        esun,
        repository,
        retry_policy=retry_policy,
        provider_timeout_seconds=args.timeout_seconds,
    )
    research = research_pipeline.run("2330", start_date, target_date)
    research_replay = research_pipeline.run("2330", start_date, target_date)
    if (
        research_replay.run_id != research.run_id
        or not research_replay.idempotent_replay
    ):
        raise RuntimeError("E.SUN research pipeline idempotency failed for 2330")

    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_violations = connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()
    if integrity != "ok" or foreign_key_violations:
        raise RuntimeError("live smoke database integrity verification failed")

    print(
        json.dumps(
            {
                "ok": True,
                "mode": "read-only",
                "schema_version": repository.get_schema_version(),
                "database": str(database_path),
                "target_date": target_date.isoformat(),
                "cross_validation": validation_results,
                "intraday_quotes": quote_results,
                "historical_stats": stats_results,
                "snapshot": snapshot_result,
                "research_summary": {
                    "symbol": "2330",
                    "market_date": _iso(research.market_date),
                    "run_id": research.run_id,
                    "research_note_id": research.research_note_id,
                    "observations": research.analysis.observations,
                    "second_execution_replayed": research_replay.idempotent_replay,
                },
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
