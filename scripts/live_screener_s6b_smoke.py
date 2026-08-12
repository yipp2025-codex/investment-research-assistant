"""Manual S6B smoke: actual S5 orchestration followed by actual S6A report.

The database must contain official TWSE historical source data prepared by the
existing historical smoke.  This script applies the already-frozen S4
migration 11 as setup, then runs only the application-level S6B runner twice.
It never registers a Windows task and never touches the production database.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import Counter
from datetime import date
from pathlib import Path
from time import perf_counter

from app.daily_runner import DailyRunner, DeterministicMarketDatePolicy
from app.reporting.screener_report import (
    ScreenerReportArtifactWriter,
    ScreenerReportGenerator,
)
from app.research_dataset import ResearchDatasetRequest
from app.screener.orchestration import (
    DailyScreenerOrchestrator,
    Stage1Execution,
    Stage2CandidateExecution,
)
from app.screener.stage1_dataset import scan_stage1_from_dataset
from app.screener.stage2 import research_stage2_candidate
from app.screener.universe import MarketUniverseSnapshot
from app.sqlite_research_dataset import SQLiteResearchDataset
from app.storage import SQLiteResearchRepository
from app.storage.candidate_persistence import CandidateInputLocator
from app.storage.screener_migration import SQLiteScreenerMigrationRunner
try:
    from scripts.live_screener_s5_smoke import (
        DEFAULT_FIXTURE,
        _hash,
        _load_subset_universe,
        _snapshot_evidence_sha256,
    )
except ModuleNotFoundError as error:
    if error.name != "scripts":
        raise
    from live_screener_s5_smoke import (
        DEFAULT_FIXTURE,
        _hash,
        _load_subset_universe,
        _snapshot_evidence_sha256,
    )


DEFAULT_SYMBOLS = ("2330", "2317", "2454")


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from error


def _fingerprint(path: Path) -> str | None:
    if not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _row_counts(path: Path) -> dict[str, int]:
    tables = (
        "market_universe_runs",
        "market_universe_members",
        "screener_runs",
        "screener_candidates",
        "candidate_reasons",
        "candidate_metrics",
        "screener_source_artifacts",
    )
    connection = sqlite3.connect(path)
    try:
        return {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables
        }
    finally:
        connection.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the S6B runner twice over an official TWSE subset."
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--target-date", type=_iso_date, default=date(2026, 8, 7))
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    parser.add_argument("--production-database", type=Path, default=Path("data/research.db"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    database_path = args.database.resolve()
    report_directory = args.report_dir.resolve()
    target_date = args.target_date
    symbols = tuple(sorted({str(item).strip().upper() for item in args.symbols}))
    if not symbols:
        raise ValueError("at least one symbol is required")
    production_before = _fingerprint(args.production_database)

    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    if repository.get_schema_version() != 10:
        raise RuntimeError("S6B smoke database must start at frozen schema v10")
    source_dataset = SQLiteResearchDataset(database_path)
    SQLiteScreenerMigrationRunner(database_path).migrate()

    universe = _load_subset_universe(
        args.fixture,
        market_date=target_date,
        symbols=symbols,
    )
    hook_calls: Counter[str] = Counter()

    def universe_provider(market_date: date) -> MarketUniverseSnapshot:
        hook_calls["universe"] += 1
        return universe

    def stage1_runner(
        current_universe: MarketUniverseSnapshot, market_date: date
    ) -> Stage1Execution:
        hook_calls["stage1"] += 1
        evidence = scan_stage1_from_dataset(
            universe=current_universe,
            dataset=source_dataset,
            as_of_date=market_date,
            candidate_limit=30,
        )
        return Stage1Execution(
            result=evidence.result,
            candidate_locators=tuple(
                CandidateInputLocator(
                    candidate.symbol,
                    _hash(
                        {
                            "symbol": candidate.symbol,
                            "market_date": market_date.isoformat(),
                            "history_observations": 250,
                            "source_policy": "twse_baseline",
                        }
                    ),
                )
                for candidate in evidence.result.candidates
            ),
        )

    def stage2_runner(request) -> Stage2CandidateExecution:
        hook_calls[f"stage2:{request.candidate.symbol}"] += 1
        snapshot = source_dataset.read(
            ResearchDatasetRequest(
                symbol=request.candidate.symbol,
                as_of_date=request.market_date,
                history_observations=250,
            )
        )
        candidate = research_stage2_candidate(
            stage1_candidate=request.candidate,
            dataset_snapshot=snapshot,
            market_date=request.market_date,
        )
        return Stage2CandidateExecution(
            candidate=candidate,
            research_locator_sha256=request.locator.research_locator_sha256,
            snapshot_sha256=_snapshot_evidence_sha256(snapshot),
        )

    s5 = DailyScreenerOrchestrator(
        database_path,
        universe_provider=universe_provider,
        stage1_runner=stage1_runner,
        stage2_runner=stage2_runner,
    )
    runner = DailyRunner(
        database_path=database_path,
        report_output_directory=report_directory,
        lock_directory=database_path.parent / ".s6b-live-locks",
        screener_runner=s5.run,
        report_generator=ScreenerReportGenerator(database_path),
        report_writer=ScreenerReportArtifactWriter(),
        market_date_policy=DeterministicMarketDatePolicy(
            latest_published_date=target_date,
            evidence_source="official_twse_historical_smoke",
        ),
    )

    database_before = _fingerprint(database_path)
    first_started = perf_counter()
    first = runner.run(target_date)
    first_elapsed = perf_counter() - first_started
    calls_after_first = dict(hook_calls)
    database_after_first = _fingerprint(database_path)

    second_started = perf_counter()
    second = runner.run(target_date)
    second_elapsed = perf_counter() - second_started
    database_after_second = _fingerprint(database_path)
    production_after = _fingerprint(args.production_database)

    if first.execution_status != "success":
        raise RuntimeError(f"first S6B execution did not succeed: {first.error_code}")
    if second.execution_status != "success_replay":
        raise RuntimeError(f"second S6B execution did not replay: {second.error_code}")
    if first.report_sha256 != second.report_sha256:
        raise RuntimeError("S6A report SHA changed on runner replay")
    if first.report_json_path is None or second.report_json_path is None:
        raise RuntimeError("S6B report paths are missing")
    if _fingerprint(first.report_json_path) != _fingerprint(second.report_json_path):
        raise RuntimeError("S6A JSON fingerprint changed on runner replay")
    if hook_calls != Counter(calls_after_first):
        raise RuntimeError("second runner invocation re-executed research hooks")
    if production_before != production_after:
        raise RuntimeError("production database changed during S6B smoke")

    print(
        json.dumps(
            {
                "ok": True,
                "scope": "official_twse_historical_data_plus_frozen_s1_universe_subset",
                "subset": True,
                "target_market_date": target_date.isoformat(),
                "database": str(database_path),
                "report_directory": str(report_directory),
                "schema_version": 11,
                "first": first.as_dict(),
                "second": second.as_dict(),
                "first_duration_seconds": round(first_elapsed, 6),
                "second_duration_seconds": round(second_elapsed, 6),
                "screener_run_id": first.screener_run_id,
                "report_sha256": first.report_sha256,
                "report_json_fingerprint": _fingerprint(first.report_json_path),
                "report_markdown_fingerprint": _fingerprint(first.report_markdown_path),
                "hook_calls_after_first": calls_after_first,
                "hook_calls_total": dict(hook_calls),
                "database_fingerprint_before": database_before,
                "database_fingerprint_after_first": database_after_first,
                "database_fingerprint_after_second": database_after_second,
                "row_counts": _row_counts(database_path),
                "production_database_before": production_before,
                "production_database_after": production_after,
                "production_database_unchanged": production_before == production_after,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
