"""Phase 6A: Daily batch runner CLI.

Usage examples:
  python scripts/run_daily_batch.py --date 2026-08-06 --provider mock --symbols 2330 2317 2454
  python scripts/run_daily_batch.py --date 2026-08-06 --provider mock --watchlist default
  python scripts/run_daily_batch.py --resume-batch-id <batch_run_id>

This script does NOT connect to real brokers, does NOT place orders,
and does NOT generate buy/sell signals.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Sequence

# Allow running directly from the repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.config import Settings
from app.pipelines.batch_runner import DailyBatchRunner
from app.pipelines.daily_research import DailyResearchPipeline
from app.pipelines.normalization import MarketDataNormalizer
from app.pipelines.retry import RetryPolicy
from app.providers import (
    EsunMarketDataProvider,
    MockMarketDataProvider,
    TwseMarketDataProvider,
)
from app.providers.base import MarketDataProvider
from app.storage import SQLiteResearchRepository
from app.storage.batch_run import BatchRunStatus, SQLiteBatchRunRepository


@dataclass(frozen=True, slots=True)
class WatchlistBootstrapResult:
    """Result of the one-time watchlist member bootstrap operation."""

    watchlist_id: str
    symbols: tuple[str, ...]
    changed: bool


def build_market_data_provider(
    provider_name: str, settings: Settings
) -> MarketDataProvider:
    """Build the existing Phase 6A provider contract for CLI/bootstrap use."""
    if provider_name == "mock":
        return MockMarketDataProvider()
    if provider_name == "twse":
        return TwseMarketDataProvider()
    if provider_name == "esun":
        if settings.esun_marketdata_config_path is None:
            raise ValueError("ESUN_MARKETDATA_CONFIG_PATH is required for the E.SUN provider")
        return EsunMarketDataProvider(
            config_path=settings.esun_marketdata_config_path
        )
    raise ValueError(f"unsupported market-data provider: {provider_name}")


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    today = date.today()
    parser = argparse.ArgumentParser(
        description="Phase 6A: Run a daily research batch for a watchlist."
    )
    parser.add_argument(
        "--date",
        type=_iso_date,
        default=today,
        help="Market date to process (YYYY-MM-DD, default: today).",
    )
    parser.add_argument(
        "--watchlist",
        default="default",
        help="Watchlist name to use (default: 'default').",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        default=None,
        help="Symbols to add to the watchlist before running (space-separated).",
    )
    parser.add_argument(
        "--provider",
        choices=("mock", "twse", "esun"),
        default="mock",
        help="Market data provider.",
    )
    parser.add_argument(
        "--resume-batch-id",
        default=None,
        help="Resume an interrupted batch run by its batch_run_id.",
    )
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=float, default=0.5)
    parser.add_argument("--backoff-multiplier", type=float, default=2.0)
    parser.add_argument("--provider-timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--skip-trading-day-check",
        action="store_true",
        help="Bypass the trading-day check (for testing with mock provider).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    settings = Settings.from_env(args.env_file)
    repository = SQLiteResearchRepository(settings.database_path)
    repository.initialize()

    # Build provider through the same factory used by the one-time bootstrap.
    try:
        provider = build_market_data_provider(args.provider, settings)
    except ValueError as exc:
        parser.error(str(exc))

    research_pipeline = DailyResearchPipeline(
        provider,
        repository,
        normalizer=MarketDataNormalizer(),
        retry_policy=RetryPolicy(
            max_attempts=args.max_attempts,
            initial_backoff_seconds=args.backoff_seconds,
            backoff_multiplier=args.backoff_multiplier,
        ),
        provider_timeout_seconds=args.provider_timeout_seconds,
    )

    batch_repo = SQLiteBatchRunRepository(repository)
    batch_repo.initialize()

    # Optionally populate the watchlist before running.
    if args.symbols:
        bootstrap = bootstrap_watchlist(
            repository=repository,
            batch_repository=batch_repo,
            provider=provider,
            normalizer=research_pipeline.normalizer,
            watchlist_name=args.watchlist,
            symbols=args.symbols,
            target_date=args.date,
            timeout_seconds=args.provider_timeout_seconds,
        )
        print(
            f"Watchlist '{args.watchlist}' updated: "
            + ", ".join(bootstrap.symbols)
        )

    # For mock provider, skip the real trading-day check (always returns today).
    if args.skip_trading_day_check or args.provider == "mock":
        requested_date_for_check = args.date
        latest_fn = lambda: requested_date_for_check  # noqa: E731
    else:
        latest_fn = None  # real check done inside DailyBatchRunner

    runner = DailyBatchRunner(
        batch_repository=batch_repo,
        research_pipeline=research_pipeline,
        watchlist_name=args.watchlist,
        latest_market_date_fn=latest_fn,
    )

    print(f"\nStarting daily batch for {args.date} (provider={args.provider})...")
    summary = runner.run(args.date, resume_batch_run_id=args.resume_batch_id)

    # ------------------------------------------------------------------
    # Print results.
    # ------------------------------------------------------------------
    print(f"\nBatch run:  {summary.batch_run_id}")
    print(f"Status:     {summary.batch_status.value}")
    print(f"Trading day: {summary.trading_day_status.value}")
    print(f"Market date: {summary.resolved_market_date}")
    print(f"Idempotent:  {summary.idempotent_replay}")
    print()

    if summary.symbol_results:
        print("Per-symbol results:")
        for sr in summary.symbol_results:
            tag = "OK" if sr.status.value == "success" else sr.status.value.upper()
            detail = f"  pipeline_run_id={sr.pipeline_run_id}" if sr.pipeline_run_id else ""
            err = f"  error={sr.error_message}" if sr.error_message else ""
            print(f"  [{tag}] {sr.symbol}{detail}{err}")

    if summary.batch_status in (BatchRunStatus.FAILED, BatchRunStatus.PARTIAL_SUCCESS):
        print("\nSome symbols failed. Re-run with --resume-batch-id to retry.")
        print(f"  --resume-batch-id {summary.batch_run_id}")
        return 1

    return 0


def _resolve_symbol_master_records(
    provider: MarketDataProvider,
    *,
    normalizer: MarketDataNormalizer,
    symbols: Sequence[str],
    target_date: date,
    timeout_seconds: float,
):
    """Resolve symbols through the selected provider's canonical payload.

    Mock uses the provider's explicitly synthetic ``Synthetic <symbol>``
    identity.  TWSE and E.SUN use their own validated identity mapping.  The
    CLI never invents names, markets, currencies, or active flags.
    """
    normalized_symbols = tuple(
        dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip())
    )
    if not normalized_symbols:
        raise ValueError("--symbols must contain at least one non-empty symbol")
    start_date = target_date - timedelta(days=14)
    records = []
    for requested_symbol in normalized_symbols:
        batch = provider.fetch_market_data(
            requested_symbol,
            start_date,
            target_date,
            timeout_seconds=timeout_seconds,
        )
        canonical = normalizer.normalize(batch)
        if canonical.symbol.symbol != requested_symbol:
            raise ValueError(
                f"provider returned a different symbol than requested: {requested_symbol}"
            )
        records.append(canonical.symbol)
    return records


def bootstrap_watchlist(
    *,
    repository: SQLiteResearchRepository,
    batch_repository: SQLiteBatchRunRepository,
    provider: MarketDataProvider,
    normalizer: MarketDataNormalizer,
    watchlist_name: str,
    symbols: Sequence[str],
    target_date: date,
    timeout_seconds: float,
) -> WatchlistBootstrapResult:
    """Create/update active watchlist members without running a batch.

    This is the one-time activation path.  It reuses the existing provider
    payload and canonical normalizer, then the repository's atomic
    ``upsert_symbols`` API.  The existing 6A runner creates the immutable
    watchlist revision when the first normal batch is executed.
    """
    if not watchlist_name.strip():
        raise ValueError("watchlist name must not be empty")
    canonical_symbols = _resolve_symbol_master_records(
        provider,
        normalizer=normalizer,
        symbols=symbols,
        target_date=target_date,
        timeout_seconds=timeout_seconds,
    )
    desired_symbols = tuple(sorted(symbol.symbol for symbol in canonical_symbols))

    # All provider identity validation happens before this atomic master
    # upsert.  No synthetic/live metadata is constructed in this path.
    repository.upsert_symbols(canonical_symbols)
    watchlist_id = batch_repository.get_or_create_watchlist(watchlist_name)
    previous_symbols = tuple(
        sorted(batch_repository.get_active_watchlist_symbols(watchlist_id))
    )
    batch_repository.set_watchlist_members(watchlist_id, desired_symbols)
    return WatchlistBootstrapResult(
        watchlist_id=watchlist_id,
        symbols=desired_symbols,
        changed=previous_symbols != desired_symbols,
    )


if __name__ == "__main__":
    raise SystemExit(main())
