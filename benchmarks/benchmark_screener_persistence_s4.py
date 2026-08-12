"""Local S4 persistence/replay/read-model capacity evidence.

The benchmark uses the frozen 1,095-member S1 fixture shape and a 30-candidate
shortlist.  Fixture and Stage 2 object construction are excluded from timed
persistence; no network, provider, or production database is used.
"""

from __future__ import annotations

import gc
import json
import sqlite3
import statistics
import tempfile
from dataclasses import replace
from datetime import date
from pathlib import Path
from time import perf_counter

try:
    from benchmarks.benchmark_screener_stage2_s3 import (
        IndexedDataset,
        _snapshots,
        _stage1_result,
    )
except ModuleNotFoundError:  # Direct ``python benchmarks/<script>.py`` execution.
    from benchmark_screener_stage2_s3 import (  # type: ignore[no-redef]
        IndexedDataset,
        _snapshots,
        _stage1_result,
    )
from app.screener.stage1 import Stage1ScanResult
from app.screener.stage2 import Stage2ArtifactRef, Stage2Candidate
from app.screener.stage2_dataset import research_stage2_from_dataset
from app.screener.universe import (
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)
from app.storage import SQLiteResearchRepository
from app.storage.candidate_persistence import (
    CandidateInputLocator,
    SQLiteScreenerCheckpointRepository,
)
from app.storage.screener_history import (
    MethodologySelector,
    SQLiteScreenerHistoryReader,
)
from app.storage.screener_migration import SQLiteScreenerMigrationRunner
from app.storage.screener_replay import SQLiteScreenerReplayRepository
from app.storage.universe_persistence import SQLiteMarketUniverseRepository


MARKET_DATE = date(2026, 8, 7)
UNIVERSE_SIZE = 1_095
CANDIDATE_COUNT = 30
REPLAY_REPEATS = 3
QUERY_REPEATS = 3
FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "screener"
    / "twse_universe_20260807.json"
)
ROW_TABLES = (
    "market_universe_members",
    "screener_candidates",
    "candidate_reasons",
    "candidate_metrics",
    "screener_source_artifacts",
)


def run_benchmark(database_path: Path | None = None) -> dict[str, object]:
    """Run one deterministic local capacity measurement and return evidence."""

    temporary: tempfile.TemporaryDirectory[str] | None = None
    if database_path is None:
        temporary = tempfile.TemporaryDirectory(prefix="screener-s4-benchmark-")
        path = Path(temporary.name) / "screener-s4.db"
    else:
        path = Path(database_path)
        if path.exists():
            raise FileExistsError(f"benchmark database already exists: {path}")

    try:
        universe, stage1_result, candidates, locator_hashes = _fixtures()
        research = SQLiteResearchRepository(path)
        research.initialize()
        SQLiteScreenerMigrationRunner(path).migrate()
        base_size = path.stat().st_size

        gc.collect()
        started = perf_counter()
        universe_result = SQLiteMarketUniverseRepository(path).persist(universe)
        universe_seconds = perf_counter() - started

        checkpoint = SQLiteScreenerCheckpointRepository(path)
        gc.collect()
        started = perf_counter()
        run = checkpoint.create_run(
            universe_run_id=universe_result.universe_run_id,
            stage1_result=stage1_result,
            candidate_locators=tuple(
                CandidateInputLocator(symbol, locator_hashes[symbol])
                for symbol in locator_hashes
            ),
        )
        for candidate in candidates:
            checkpoint.persist_candidate(
                screener_run_id=run.screener_run_id,
                candidate=candidate,
                research_locator_sha256=locator_hashes[candidate.symbol],
                snapshot_sha256=_sha256(f"snapshot-{candidate.symbol}"),
            )
        candidate_persistence_seconds = perf_counter() - started

        replay_repository = SQLiteScreenerReplayRepository(path)
        gc.collect()
        started = perf_counter()
        finalized = replay_repository.finalize_run(run.screener_run_id)
        finalization_seconds = perf_counter() - started
        final_size = path.stat().st_size

        replay_times = []
        replay_hashes = set()
        for _ in range(REPLAY_REPEATS):
            gc.collect()
            started = perf_counter()
            replayed = replay_repository.replay_run(run.screener_run_id)
            replay_times.append(perf_counter() - started)
            replay_hashes.add(replayed.result.payload_sha256)
        if replay_hashes != {finalized.result.payload_sha256}:
            raise AssertionError("canonical replay hash changed")

        history = SQLiteScreenerHistoryReader(path)
        selector = MethodologySelector(
            stage1_result.methodology_version,
            finalized.result.stage2_methodology_version,
        )
        first_symbol = candidates[0].symbol
        query_functions = {
            "selection_count": lambda: history.selection_count(first_symbol),
            "reason_frequency": history.reason_frequency,
            "daily_candidate_set": lambda: history.daily_candidate_set(MARKET_DATE),
            "reason_history": lambda: history.reason_history(
                first_symbol,
                code="price_change_1d_threshold",
            ),
            "methodology_comparison": lambda: history.compare_methodologies(
                MARKET_DATE,
                left=selector,
                right=selector,
            ),
        }
        query_times: dict[str, list[float]] = {}
        for name, query in query_functions.items():
            values = []
            for _ in range(QUERY_REPEATS):
                gc.collect()
                started = perf_counter()
                query()
                values.append(perf_counter() - started)
            query_times[name] = values

        connection = sqlite3.connect(path)
        try:
            row_counts = {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in ROW_TABLES
            }
            schema_version = connection.execute(
                "SELECT MAX(version) FROM schema_migrations"
            ).fetchone()[0]
            daily_price_rows = connection.execute(
                "SELECT COUNT(*) FROM daily_prices"
            ).fetchone()[0]
            schema_objects = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        finally:
            connection.close()

        expected_rows = {
            "market_universe_members": UNIVERSE_SIZE,
            "screener_candidates": CANDIDATE_COUNT,
            "candidate_metrics": CANDIDATE_COUNT * 16,
        }
        for table, expected in expected_rows.items():
            if row_counts[table] != expected:
                raise AssertionError(f"unexpected {table} row count")
        if schema_version != 11:
            raise AssertionError("benchmark schema version changed")
        if daily_price_rows != 0 or "market_scan_observations" in schema_objects:
            raise AssertionError("S4 benchmark persisted forbidden scan history")

        total_persistence = (
            universe_seconds
            + candidate_persistence_seconds
            + finalization_seconds
        )
        return {
            "benchmark_version": "screener-persistence-s4-v1",
            "measurement_mode": (
                "one local v11 SQLite run; immutable fixture construction excluded; "
                "three query-only replay/query measurements"
            ),
            "market_date": MARKET_DATE.isoformat(),
            "schema_version": schema_version,
            "universe_count": UNIVERSE_SIZE,
            "candidate_count": CANDIDATE_COUNT,
            "row_counts": row_counts,
            "database_size_bytes": {
                "v11_empty": base_size,
                "final": final_size,
                "delta": final_size - base_size,
            },
            "persistence_elapsed_seconds": {
                "universe": round(universe_seconds, 6),
                "run_and_candidate_checkpoints": round(
                    candidate_persistence_seconds, 6
                ),
                "finalization": round(finalization_seconds, 6),
                "total": round(total_persistence, 6),
            },
            "replay_elapsed_seconds": _timing_summary(replay_times),
            "historical_query_elapsed_seconds": {
                name: _timing_summary(values)
                for name, values in sorted(query_times.items())
            },
            "canonical_sha256": finalized.result.payload_sha256,
            "dataset_reads": 0,
            "full_time_series_rows_persisted": daily_price_rows,
            "raw_provider_responses_persisted": False,
            "market_scan_observations_table_present": (
                "market_scan_observations" in schema_objects
            ),
            "performance_budget": None,
        }
    finally:
        if temporary is not None:
            temporary.cleanup()


def _fixtures() -> tuple[
    MarketUniverseSnapshot,
    Stage1ScanResult,
    tuple[Stage2Candidate, ...],
    dict[str, str],
]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    universe = _universe_from_fixture(payload)
    eligible = payload["eligible"]
    shortlist = tuple(eligible[:CANDIDATE_COUNT])
    symbols = tuple(item[0] for item in shortlist)
    names = {item[0]: item[1] for item in shortlist}

    base_stage1 = _stage1_result(symbols)
    stage1_candidates = tuple(
        replace(candidate, name=names[candidate.symbol])
        for candidate in base_stage1.candidates
    )
    stage1_result = replace(
        base_stage1,
        universe_count=UNIVERSE_SIZE,
        candidates=stage1_candidates,
    )
    dataset = IndexedDataset(_snapshots(symbols))
    researched = research_stage2_from_dataset(
        stage1_result=stage1_result,
        dataset=dataset,
        market_date=MARKET_DATE,
    ).result
    candidates = tuple(_with_artifact(candidate) for candidate in researched.candidates)
    locator_hashes = {
        symbol: _sha256(f"s4-benchmark-locator-{symbol}") for symbol in symbols
    }
    return universe, stage1_result, candidates, locator_hashes


def _universe_from_fixture(payload: dict[str, object]) -> MarketUniverseSnapshot:
    source_values = payload["source_snapshots"]
    if not isinstance(source_values, dict):
        raise ValueError("invalid S1 source fixture")
    evidence = tuple(
        sorted(
            (
                SourceEvidence(
                    source=value["source"],
                    dataset=value["dataset"],
                    source_ref=value["source_ref"],
                    contract_version=value["contract_version"],
                    payload_sha256=value["payload_sha256"],
                    payload_size_bytes=value["payload_size_bytes"],
                    hash_basis=value["hash_basis"],
                )
                for value in source_values.values()
            ),
            key=lambda item: (item.source, item.dataset, item.source_ref),
        )
    )
    members = []
    for symbol, name, listing_date in payload["eligible"]:
        members.append(
            _member(
                symbol,
                name,
                UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
                listing_date,
                None,
                evidence,
            )
        )
    for symbol, name, listing_date in payload["active_unavailable"]:
        members.append(
            _member(
                symbol,
                name,
                UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE,
                listing_date,
                "stock_day_all_missing",
                evidence,
            )
        )
    for symbol, name, classification, listing_date in payload["excluded"]:
        members.append(
            _member(
                symbol,
                name,
                UniverseMemberStatus.EXCLUDED_NON_COMMON_EQUITY,
                listing_date,
                f"non_common_equity_{classification}",
                evidence,
            )
        )
    ordered = tuple(sorted(members, key=lambda item: item.symbol))
    if len(ordered) != UNIVERSE_SIZE:
        raise ValueError("frozen S1 fixture no longer has 1,095 members")
    return MarketUniverseSnapshot(
        market_date=MARKET_DATE,
        methodology_version=str(payload["methodology_version"]),
        source_policy=str(payload["source_policy"]),
        universe_count=len(ordered),
        scan_eligible_count=1_082,
        scan_unavailable_count=1,
        excluded_count=12,
        inactive_count=0,
        unresolved_count=0,
        members=ordered,
    )


def _member(
    symbol: str,
    name: str,
    status: UniverseMemberStatus,
    listing_date: str | None,
    exclusion_reason: str | None,
    evidence: tuple[SourceEvidence, ...],
) -> MarketUniverseMember:
    return MarketUniverseMember(
        symbol=symbol,
        name=name,
        market="TWSE",
        status=status,
        listing_date=date.fromisoformat(listing_date) if listing_date else None,
        delisting_date=None,
        exclusion_reason=exclusion_reason,
        source_evidence=evidence,
    )


def _with_artifact(candidate: Stage2Candidate) -> Stage2Candidate:
    artifact = Stage2ArtifactRef(
        owner_kind="historical",
        owner_run_id=f"benchmark-{candidate.symbol}",
        provider="mock-synthetic",
        dataset="s4-persistence-benchmark",
        endpoint="contract://screener-s4-benchmark/synthetic-candidate",
        contract_version="screener-s4-benchmark-v1",
        payload_sha256=_sha256(f"artifact-{candidate.symbol}"),
        payload_size_bytes=250,
        hash_basis="canonical-json-v1",
    )
    return replace(
        candidate,
        provenance=replace(candidate.provenance, artifact_refs=(artifact,)),
    )


def _timing_summary(values: list[float]) -> dict[str, object]:
    rounded = [round(value, 6) for value in values]
    return {
        "measurements": rounded,
        "minimum": min(rounded),
        "median": statistics.median(rounded),
        "maximum": max(rounded),
    }


def _sha256(value: str) -> str:
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    print(json.dumps(run_benchmark(), ensure_ascii=False, indent=2))
