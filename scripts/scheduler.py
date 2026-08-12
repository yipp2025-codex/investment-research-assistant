"""Phase 6C scheduler CLI.

This wrapper is intentionally small: ``bootstrap`` is a one-time watchlist
activation path, while ``run`` delegates to
``SchedulerRunner``, which invokes the existing Phase 6A CLI.  It never
contains a second market-data or research pipeline.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from app.operations.scheduler import SchedulerConfig, SchedulerRunner, result_as_json
from app.operations.task_scheduler import WindowsTaskConfig, build_task_scheduler_xml
from app.config import Settings
from app.pipelines.normalization import MarketDataNormalizer
from app.storage import SQLiteOperationsRepository, SQLiteResearchRepository
from app.storage.batch_run import SQLiteBatchRunRepository
from scripts.run_daily_batch import bootstrap_watchlist, build_market_data_provider


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Phase 6C EOD operations wrapper for the Phase 6A CLI."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="run one EOD operation")
    trigger = run.add_mutually_exclusive_group()
    trigger.add_argument("--manual", action="store_true", help="manual trigger")
    trigger.add_argument(
        "--scheduled", action="store_true", help="Windows Task Scheduler trigger"
    )
    run.add_argument("--target-date", type=_iso_date, default=None)
    run.add_argument("--project-root", type=Path, default=_REPO_ROOT)
    run.add_argument("--database", type=Path, default=None)
    run.add_argument("--watchlist", default="default")
    run.add_argument("--provider", choices=("twse", "mock"), default="twse")
    run.add_argument("--schedule-time", default="18:00")
    run.add_argument("--grace-minutes", type=int, default=30)
    run.add_argument("--deferred-attempts", type=int, default=3)
    run.add_argument("--retry-backoff-seconds", type=float, default=60.0)
    run.add_argument("--retry-backoff-multiplier", type=float, default=2.0)
    run.add_argument("--lease-ttl-seconds", type=int, default=1800)
    run.add_argument("--provider-timeout-seconds", type=float, default=10.0)
    run.add_argument("--cli-timeout-seconds", type=float, default=900.0)
    run.add_argument("--python-path", default=sys.executable)
    run.add_argument("--env-file", type=Path, default=None)
    run.add_argument("--probe-symbol", default="2330")
    run.add_argument("--job-name", default="investment-research-eod")

    bootstrap = subparsers.add_parser(
        "bootstrap",
        help="one-time watchlist activation; does not run a research batch",
    )
    bootstrap.add_argument("--target-date", type=_iso_date, default=date.today())
    bootstrap.add_argument("--project-root", type=Path, default=_REPO_ROOT)
    bootstrap.add_argument("--database", type=Path, default=None)
    bootstrap.add_argument("--watchlist", default="default")
    bootstrap.add_argument(
        "--symbols", nargs="+", required=True,
        help="canonical symbols to activate once for this watchlist",
    )
    bootstrap.add_argument("--provider", choices=("mock", "twse", "esun"), default="twse")
    bootstrap.add_argument("--env-file", type=Path, default=Path(".env"))
    bootstrap.add_argument("--provider-timeout-seconds", type=float, default=10.0)

    health = subparsers.add_parser("health", help="show the latest safe health state")
    health.add_argument("--project-root", type=Path, default=_REPO_ROOT)
    health.add_argument("--database", type=Path, default=None)
    health.add_argument("--job-name", default="investment-research-eod")

    task_xml = subparsers.add_parser("task-xml", help="write a Windows Task Scheduler XML")
    task_xml.add_argument("--output", type=Path, required=True)
    task_xml.add_argument("--project-root", type=Path, default=_REPO_ROOT)
    task_xml.add_argument("--database", type=Path, default=None)
    task_xml.add_argument("--task-name", default="ResearchAssistant-EOD")
    task_xml.add_argument("--python-path", default=sys.executable)
    task_xml.add_argument("--watchlist", default="default")
    task_xml.add_argument("--provider", choices=("twse", "mock"), default="twse")
    task_xml.add_argument("--schedule-time", default="18:00")
    task_xml.add_argument("--grace-minutes", type=int, default=30)
    return parser


def _scheduler_config(args: argparse.Namespace) -> SchedulerConfig:
    return SchedulerConfig(
        job_name=args.job_name,
        watchlist=args.watchlist,
        provider=args.provider,
        schedule_time=args.schedule_time,
        grace_period_seconds=args.grace_minutes * 60,
        max_deferred_attempts=args.deferred_attempts,
        retry_backoff_seconds=args.retry_backoff_seconds,
        retry_backoff_multiplier=args.retry_backoff_multiplier,
        lease_ttl_seconds=args.lease_ttl_seconds,
        provider_timeout_seconds=args.provider_timeout_seconds,
        cli_timeout_seconds=args.cli_timeout_seconds,
        project_root=args.project_root,
        database_path=args.database,
        python_path=args.python_path,
        env_file=args.env_file,
        probe_symbol=args.probe_symbol,
    )


def _run(args: argparse.Namespace) -> int:
    config = _scheduler_config(args)
    runner = SchedulerRunner(config)
    trigger = "scheduled" if args.scheduled else "manual"
    result = runner.run_once(trigger=trigger, target_date=args.target_date)
    print(result_as_json(result))
    if result.health_status in {"hard_failure", "partial_failure"}:
        return 1
    return 0


def _bootstrap(args: argparse.Namespace) -> int:
    settings = Settings.from_env(args.env_file)
    database_path = args.database or settings.database_path
    repository = SQLiteResearchRepository(database_path)
    # Bootstrap must produce a v9-compatible database, but it does not run a
    # batch or create daily result/report rows.
    SQLiteOperationsRepository(repository).initialize()
    batch_repository = SQLiteBatchRunRepository(repository)
    batch_repository.initialize()
    try:
        provider = build_market_data_provider(args.provider, settings)
        result = bootstrap_watchlist(
            repository=repository,
            batch_repository=batch_repository,
            provider=provider,
            normalizer=MarketDataNormalizer(),
            watchlist_name=args.watchlist,
            symbols=args.symbols,
            target_date=args.target_date,
            timeout_seconds=args.provider_timeout_seconds,
        )
    except (ValueError, RuntimeError) as exc:
        raise SystemExit(f"watchlist bootstrap failed: {type(exc).__name__}") from exc
    print(
        json.dumps(
            {
                "status": "bootstrapped",
                "watchlist_id": result.watchlist_id,
                "symbols": list(result.symbols),
                "changed": result.changed,
                "idempotent": not result.changed,
                "revision_policy": (
                    "first normal scheduler run creates the immutable 6A revision"
                ),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def _health(args: argparse.Namespace) -> int:
    config = SchedulerConfig(
        job_name=args.job_name,
        project_root=args.project_root,
        database_path=args.database,
    )
    repository = SQLiteResearchRepository(config.effective_database_path)
    operations = SQLiteOperationsRepository(repository)
    operations.initialize()
    snapshot = operations.latest_health(config.job_name)
    print(
        json.dumps(
            {
                "invocation_id": snapshot.invocation_id,
                "job_name": snapshot.job_name,
                "operation_status": snapshot.operation_status,
                "health_status": snapshot.health_status,
                "batch_run_id": snapshot.batch_run_id,
                "message": snapshot.message,
                "checked_at": snapshot.checked_at.isoformat(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


def _task_xml(args: argparse.Namespace) -> int:
    config = WindowsTaskConfig(
        task_name=args.task_name,
        project_root=args.project_root,
        python_path=args.python_path,
        watchlist=args.watchlist,
        provider=args.provider,
        schedule_time=args.schedule_time,
        grace_period_minutes=args.grace_minutes,
        database_path=args.database,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(build_task_scheduler_xml(config), encoding="utf-8")
    print(json.dumps({"status": "written", "output": str(args.output)}))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "run":
        return _run(args)
    if args.command == "bootstrap":
        return _bootstrap(args)
    if args.command == "health":
        return _health(args)
    return _task_xml(args)


if __name__ == "__main__":
    raise SystemExit(main())
