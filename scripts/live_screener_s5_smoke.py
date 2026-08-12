"""Manual S5 live smoke over an explicit official TWSE subset.

This script is intentionally not a scheduler or production entrypoint.  It
uses one isolated temporary v10 database as the M9 source, then applies the
existing S4 migration 11 in that same database so frozen provenance foreign
keys remain resolvable.  The Universe is a three-symbol subset of the frozen
official TWSE S1 evidence fixture because S1 currently owns normalized-input
construction rather than live acquisition.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import time
from collections import Counter
from datetime import date
from pathlib import Path

from app.research_dataset import ResearchDatasetRequest
from app.screener.orchestration import (
    DailyScreenerOrchestrator,
    Stage1Execution,
    Stage2CandidateExecution,
)
from app.screener.stage1_dataset import scan_stage1_from_dataset
from app.screener.stage2 import research_stage2_candidate
from app.screener.universe import (
    UNIVERSE_METHODOLOGY_VERSION,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)
from app.sqlite_research_dataset import SQLiteResearchDataset
from app.storage import SQLiteResearchRepository
from app.storage.candidate_persistence import CandidateInputLocator
from app.storage.screener_migration import SQLiteScreenerMigrationRunner


DEFAULT_SYMBOLS = ("2330", "2317", "2454")
DEFAULT_FIXTURE = Path("tests/fixtures/screener/twse_universe_20260807.json")


def _hash(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from error


def _load_subset_universe(
    fixture_path: Path,
    *,
    market_date: date,
    symbols: tuple[str, ...],
) -> MarketUniverseSnapshot:
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if payload.get("market_date") != market_date.isoformat():
        raise ValueError("S5 smoke fixture market_date differs from requested date")
    if payload.get("methodology_version") != UNIVERSE_METHODOLOGY_VERSION:
        raise ValueError("S5 smoke fixture methodology changed")

    evidence = tuple(
        sorted(
            {
                SourceEvidence(
                    source=item["source"],
                    dataset=item["dataset"],
                    source_ref=item["source_ref"],
                    contract_version=item["contract_version"],
                    payload_sha256=item["payload_sha256"],
                    payload_size_bytes=item["payload_size_bytes"],
                    hash_basis=item["hash_basis"],
                )
                for item in payload["source_snapshots"].values()
            },
            key=lambda item: (
                item.source,
                item.dataset,
                item.source_ref,
                item.contract_version,
                item.payload_sha256,
            ),
        )
    )
    eligible = {
        str(item[0]): (str(item[1]), date.fromisoformat(str(item[2])))
        for item in payload["eligible"]
    }
    missing = set(symbols) - set(eligible)
    if missing:
        raise ValueError("S5 smoke fixture lacks symbols: " + ", ".join(sorted(missing)))

    members = tuple(
        MarketUniverseMember(
            symbol=symbol,
            name=eligible[symbol][0],
            market="TWSE",
            status=UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
            listing_date=eligible[symbol][1],
            delisting_date=None,
            exclusion_reason=None,
            source_evidence=evidence,
        )
        for symbol in sorted(symbols)
    )
    return MarketUniverseSnapshot(
        market_date=market_date,
        methodology_version=UNIVERSE_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=len(members),
        scan_eligible_count=len(members),
        scan_unavailable_count=0,
        excluded_count=0,
        inactive_count=0,
        unresolved_count=0,
        members=members,
    )


def _snapshot_evidence_sha256(snapshot) -> str:
    """Hash only deterministic M9 read evidence for the S4 snapshot field."""

    return _hash(
        {
            "symbol": snapshot.symbol.symbol,
            "as_of_date": snapshot.as_of.as_of_date.isoformat(),
            "history_observations": snapshot.as_of.history_observations,
            "returned_history_observations": snapshot.as_of.returned_history_observations,
            "total_history_observations": snapshot.as_of.total_history_observations,
            "price_rows": [
                {
                    "trade_date": item.trade_date.isoformat(),
                    "open": item.open,
                    "high": item.high,
                    "low": item.low,
                    "close": item.close,
                    "volume": item.volume,
                    "source": item.source,
                }
                for item in snapshot.price_history.observations
            ],
            "valuation": [
                {
                    "metric_date": item.metric_date.isoformat(),
                    "name": item.name,
                    "value": item.value,
                    "unit": item.unit,
                    "source": item.source,
                }
                for item in snapshot.valuation.metrics
            ],
            "canonical_sources": snapshot.provenance.canonical_sources,
            "validation_sources": snapshot.provenance.validation_sources,
            "artifacts": [item.payload_sha256 for item in snapshot.provenance.artifact_refs],
        }
    )


def _row_counts(database_path: Path) -> dict[str, int]:
    tables = (
        "market_universe_runs",
        "market_universe_members",
        "screener_runs",
        "screener_candidates",
        "candidate_reasons",
        "candidate_metrics",
        "screener_source_artifacts",
    )
    connection = sqlite3.connect(database_path)
    try:
        return {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables
        }
    finally:
        connection.close()


def _production_db_fingerprint(path: Path) -> str | None:
    if not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the S5 orchestration smoke over an official TWSE subset."
    )
    parser.add_argument("--source-database", type=Path, required=True)
    parser.add_argument("--screener-database", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--target-date", type=_iso_date, default=date(2026, 8, 7))
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    parser.add_argument("--candidate-limit", type=int, default=30)
    parser.add_argument("--production-database", type=Path, default=Path("data/research.db"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = tuple(sorted({str(item).strip().upper() for item in args.symbols}))
    if not symbols:
        raise ValueError("at least one symbol is required")
    target_date = args.target_date
    if args.source_database.resolve() != args.screener_database.resolve():
        raise ValueError(
            "S5 live smoke requires one isolated database for M9 source and S4 "
            "persistence so frozen provenance foreign keys remain resolvable"
        )
    production_before = _production_db_fingerprint(args.production_database)

    source_dataset = SQLiteResearchDataset(args.source_database)
    universe = _load_subset_universe(
        args.fixture,
        market_date=target_date,
        symbols=symbols,
    )

    repository = SQLiteResearchRepository(args.screener_database)
    repository.initialize()
    if repository.get_schema_version() != 10:
        raise RuntimeError("S5 smoke base database is not frozen v10")
    SQLiteScreenerMigrationRunner(args.screener_database).migrate()

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
            candidate_limit=args.candidate_limit,
        )
        return Stage1Execution(
            result=evidence.result,
            candidate_locators=tuple(
                # The locator describes the stable research request, never a
                # database path, timestamp, process, or retry attempt.
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

    orchestrator = DailyScreenerOrchestrator(
        args.screener_database,
        universe_provider=universe_provider,
        stage1_runner=stage1_runner,
        stage2_runner=stage2_runner,
    )
    started = time.perf_counter()
    first = orchestrator.run(target_date)
    first_seconds = time.perf_counter() - started
    calls_after_first = dict(hook_calls)
    started = time.perf_counter()
    second = orchestrator.run(
        target_date,
        expected_screener_run_id=first.screener_run_id,
    )
    second_seconds = time.perf_counter() - started
    production_after = _production_db_fingerprint(args.production_database)

    if not second.replayed or second.written or hook_calls != Counter(calls_after_first):
        raise RuntimeError("S5 strict replay smoke failed")
    if first.status != "success":
        raise RuntimeError(f"S5 live subset did not reach success: {first.status}")
    if production_before != production_after:
        raise RuntimeError("production database changed during S5 smoke")

    final_candidates = [
        {
            "rank": item.rank,
            "symbol": item.symbol,
            "candidate_kind": item.candidate_kind.value,
            "analysis_status": item.analysis_status.value,
            "stage2_reason_codes": [reason.code for reason in item.stage2_reasons],
        }
        for item in first.candidates
    ]
    print(
        json.dumps(
            {
                "ok": True,
                "scope": "official_twse_live_historical_data_plus_frozen_s1_universe_subset",
                "subset": True,
                "market_date": target_date.isoformat(),
                "source_database": str(args.source_database),
                "screener_database": str(args.screener_database),
                "schema_version": 11,
                "universe_count": first.universe_count,
                "screened_count": first.screened_count,
                "triggered_count": first.triggered_count,
                "candidate_count": first.candidate_count,
                "truncated": first.truncated,
                "final_candidates": final_candidates,
                "persistence_status": first.status,
                "universe_run_id": first.universe_run_id,
                "screener_run_id": first.screener_run_id,
                "canonical_sha256": first.canonical_sha256,
                "first_execution": {
                    "replayed": first.replayed,
                    "written": first.written,
                    "seconds": round(first_seconds, 6),
                },
                "second_execution": {
                    "replayed": second.replayed,
                    "written": second.written,
                    "seconds": round(second_seconds, 6),
                },
                "hook_calls_after_first": calls_after_first,
                "hook_calls_total": dict(hook_calls),
                "row_counts": _row_counts(args.screener_database),
                "production_database_unchanged": production_before == production_after,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
