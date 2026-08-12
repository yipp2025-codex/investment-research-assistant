"""Command-line entry point for the synthetic research pipeline."""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from app.config import Settings
from app.pipelines import DailyResearchPipeline, RetryPolicy
from app.providers import (
    EsunMarketDataProvider,
    MockMarketDataProvider,
    TwseMarketDataProvider,
)
from app.storage import SQLiteResearchRepository


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def build_parser() -> argparse.ArgumentParser:
    today = date.today()
    parser = argparse.ArgumentParser(
        description="Run the broker-independent synthetic research pipeline."
    )
    parser.add_argument("--symbol", default="MOCK1")
    parser.add_argument(
        "--provider", choices=("mock", "twse", "esun"), default="mock"
    )
    parser.add_argument("--start", type=_iso_date, default=today - timedelta(days=6))
    parser.add_argument("--end", type=_iso_date, default=today)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--backoff-seconds", type=float, default=0.5)
    parser.add_argument("--backoff-multiplier", type=float, default=2.0)
    parser.add_argument("--provider-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--resume-run-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_env(args.env_file)
    repository = SQLiteResearchRepository(settings.database_path)
    if args.provider == "mock":
        provider = MockMarketDataProvider()
    elif args.provider == "twse":
        provider = TwseMarketDataProvider()
    else:
        if settings.esun_marketdata_config_path is None:
            parser.error(
                "ESUN_MARKETDATA_CONFIG_PATH is required for the E.SUN provider"
            )
        provider = EsunMarketDataProvider(
            config_path=settings.esun_marketdata_config_path
        )
    pipeline = DailyResearchPipeline(
        provider,
        repository,
        retry_policy=RetryPolicy(
            max_attempts=args.max_attempts,
            initial_backoff_seconds=args.backoff_seconds,
            backoff_multiplier=args.backoff_multiplier,
        ),
        provider_timeout_seconds=args.provider_timeout_seconds,
    )
    result = pipeline.run(
        args.symbol,
        args.start,
        args.end,
        resume_run_id=args.resume_run_id,
    )

    print(result.summary)
    print()
    print(
        f"Run {result.run_id} ({result.run_status.value}, "
        f"attempt {result.run_attempt_count}) stored "
        f"{result.daily_prices_written} prices, "
        f"{result.company_metrics_written} metrics, note #{result.research_note_id} "
        f"in {settings.database_path}. "
        f"Market date: {result.market_date}. "
        f"Idempotent replay: {result.idempotent_replay}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
