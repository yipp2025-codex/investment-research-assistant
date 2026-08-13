from __future__ import annotations

import ast
import hashlib
import sqlite3
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pytest

from app.screener.orchestration import (
    DailyScreenerOrchestrator,
    DailyScreenerStateError,
    Stage1Execution,
    Stage2CandidateExecution,
)
from app.screener.stage1 import (
    PRICE_CHANGE_1D,
    STAGE1_METHODOLOGY_VERSION,
    DataQualityStatus,
    MetricStatus,
    Stage1Candidate,
    Stage1DataQuality,
    Stage1Metric,
    Stage1Reason,
    Stage1ScanResult,
)
from app.screener.stage2 import (
    MA_DISTANCE_120D,
    MA_DISTANCE_20D,
    MA_DISTANCE_250D,
    MA_DISTANCE_60D,
    MAX_DRAWDOWN_120D,
    MAX_DRAWDOWN_20D,
    MAX_DRAWDOWN_250D,
    MAX_DRAWDOWN_60D,
    RETURN_120D,
    RETURN_20D,
    RETURN_60D,
    STAGE2_METHODOLOGY_VERSION,
    VALUATION_PB,
    VALUATION_PE,
    VALUATION_YIELD,
    VOLATILITY_60D,
    VOLUME_RATIO_20D,
    Stage2AnalysisStatus,
    Stage2Candidate,
    Stage2CandidateKind,
    Stage2DataQuality,
    Stage2Metric,
    Stage2MetricStatus,
    Stage2Provenance,
    Stage2QualityStatus,
    Stage2Reason,
)
from app.screener.universe import (
    UNIVERSE_METHODOLOGY_VERSION,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)
from app.storage import SQLiteResearchRepository
from app.storage.candidate_persistence import CandidateInputLocator
from app.storage.screener_migration import SQLiteScreenerMigrationRunner
from app.storage.screener_replay import ScreenerReplayIntegrityError


MARKET_DATE = date(2026, 8, 7)
SYMBOLS = ("1001", "1002", "1003")
METRIC_ORDER = (
    RETURN_20D,
    RETURN_60D,
    RETURN_120D,
    VOLATILITY_60D,
    MAX_DRAWDOWN_20D,
    MAX_DRAWDOWN_60D,
    MAX_DRAWDOWN_120D,
    MAX_DRAWDOWN_250D,
    VOLUME_RATIO_20D,
    MA_DISTANCE_20D,
    MA_DISTANCE_60D,
    MA_DISTANCE_120D,
    MA_DISTANCE_250D,
    VALUATION_PE,
    VALUATION_PB,
    VALUATION_YIELD,
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _initialize(database_path: Path) -> None:
    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    assert repository.get_schema_version() == 10
    SQLiteScreenerMigrationRunner(database_path).migrate()


def _universe(symbols: tuple[str, ...] = SYMBOLS) -> MarketUniverseSnapshot:
    evidence = (
        SourceEvidence(
            source="twse",
            dataset="s5-test-universe",
            source_ref="contract://s5-tests/universe",
            contract_version="s5-test-v1",
            payload_sha256=_hash("s5-universe"),
            payload_size_bytes=12,
            hash_basis="canonical-json-v1",
        ),
    )
    members = tuple(
        MarketUniverseMember(
            symbol=symbol,
            name=f"Company {symbol}",
            market="TWSE",
            status=UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
            listing_date=date(2000, 1, 1),
            delisting_date=None,
            exclusion_reason=None,
            source_evidence=evidence,
        )
        for symbol in symbols
    )
    return MarketUniverseSnapshot(
        market_date=MARKET_DATE,
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


def _stage1_candidate(symbol: str, rank: int) -> Stage1Candidate:
    reason = Stage1Reason(
        code="price_change_1d_threshold",
        metric=PRICE_CHANGE_1D,
        component=None,
        previous=1.0,
        current=4.0,
        delta=3.0,
        unit="percent",
        operator="abs_current_gte",
        threshold=3.0,
        rule_version=STAGE1_METHODOLOGY_VERSION,
        role="primary",
        trigger_class="price_change",
        threshold_multiple=4.0 / 3.0,
    )
    metric = Stage1Metric(
        metric=PRICE_CHANGE_1D,
        component=None,
        status=MetricStatus.AVAILABLE,
        previous=1.0,
        current=4.0,
        delta=3.0,
        value_unit="percent",
        delta_unit="percentage_point",
        previous_as_of_date=MARKET_DATE - timedelta(days=1),
        current_as_of_date=MARKET_DATE,
    )
    return Stage1Candidate(
        symbol=symbol,
        name=f"Company {symbol}",
        rank=rank,
        reasons=(reason,),
        metrics=(metric,),
        data_quality=Stage1DataQuality(
            status=DataQualityStatus.CLEAN,
            issues=(),
        ),
    )


def _stage1_execution(
    universe: MarketUniverseSnapshot,
    symbols: tuple[str, ...] = SYMBOLS,
) -> Stage1Execution:
    candidates = tuple(
        _stage1_candidate(symbol, rank)
        for rank, symbol in enumerate(symbols, 1)
    )
    result = Stage1ScanResult(
        market_date=MARKET_DATE,
        methodology_version=STAGE1_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=universe.universe_count,
        screened_count=universe.scan_eligible_count,
        triggered_count=len(candidates),
        candidate_count=len(candidates),
        candidate_limit=len(candidates),
        truncated=False,
        candidates=candidates,
    )
    return Stage1Execution(
        result=result,
        candidate_locators=tuple(
            CandidateInputLocator(symbol, _hash(f"locator-{symbol}"))
            for symbol in symbols
        ),
    )


def _metric_unit(name: str) -> str:
    if name in {VOLUME_RATIO_20D, VALUATION_PE, VALUATION_PB}:
        return "ratio"
    return "percent"


def _stage2_candidate(stage1: Stage1Candidate) -> Stage2Candidate:
    metrics = tuple(
        Stage2Metric(
            name=name,
            status=Stage2MetricStatus.AVAILABLE,
            value=float(index + 1),
            previous_value=float(index),
            delta=1.0,
            unit=_metric_unit(name),
            as_of_date=MARKET_DATE,
            previous_as_of_date=MARKET_DATE - timedelta(days=1),
            observations=250,
            previous_observations=249,
        )
        for index, name in enumerate(METRIC_ORDER)
    )
    reason = Stage2Reason(
        code="return_60d_change",
        metric=RETURN_60D,
        previous=2.0,
        current=4.0,
        delta=2.0,
        unit="percentage_point",
        operator="abs_delta_gte",
        threshold=1.0,
        rule_version=STAGE2_METHODOLOGY_VERSION,
        reason_kind="research_change",
        reason_class="return_change",
        threshold_multiple=2.0,
    )
    return Stage2Candidate(
        rank=stage1.rank,
        stage1_rank=stage1.rank,
        symbol=stage1.symbol,
        name=stage1.name,
        market="TWSE",
        candidate_kind=Stage2CandidateKind.RESEARCH_CANDIDATE,
        analysis_status=Stage2AnalysisStatus.AVAILABLE,
        stage1_reasons=stage1.reasons,
        stage2_reasons=(reason,),
        metrics=metrics,
        data_quality=Stage2DataQuality(
            status=Stage2QualityStatus.CLEAN,
            validation_status="available",
            discrepancies=(),
        ),
        provenance=Stage2Provenance(
            pipeline_run_id=None,
            historical_run_id=None,
            validation_run_id=None,
            canonical_sources=("twse",),
            validation_sources=(),
            artifact_refs=(),
        ),
    )


def _orchestrator(
    tmp_path: Path,
    *,
    failure_budget: dict[str, int] | None = None,
    symbols: tuple[str, ...] = SYMBOLS,
) -> tuple[DailyScreenerOrchestrator, dict[str, int]]:
    database_path = tmp_path / "s5.db"
    _initialize(database_path)
    calls = Counter()
    remaining_failures = dict(failure_budget or {})

    def universe_provider(market_date: date) -> MarketUniverseSnapshot:
        calls["universe"] += 1
        assert market_date == MARKET_DATE
        return _universe(symbols)

    def stage1_runner(
        universe: MarketUniverseSnapshot, market_date: date
    ) -> Stage1Execution:
        calls["stage1"] += 1
        assert market_date == MARKET_DATE
        return _stage1_execution(universe, symbols)

    def stage2_runner(request) -> Stage2CandidateExecution:
        symbol = request.candidate.symbol
        calls[f"stage2:{symbol}"] += 1
        if remaining_failures.get(symbol, 0) > 0:
            remaining_failures[symbol] -= 1
            raise RuntimeError("synthetic candidate failure")
        return Stage2CandidateExecution(
            candidate=_stage2_candidate(request.candidate),
            research_locator_sha256=request.locator.research_locator_sha256,
            snapshot_sha256=_hash(f"snapshot-{symbol}"),
        )

    return (
        DailyScreenerOrchestrator(
            database_path,
            universe_provider=universe_provider,
            stage1_runner=stage1_runner,
            stage2_runner=stage2_runner,
        ),
        calls,
    )


def _database_path(orchestrator: DailyScreenerOrchestrator) -> Path:
    return orchestrator.database_path


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


def _candidate_timestamps(database_path: Path) -> dict[str, tuple[object, ...]]:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            "SELECT symbol, created_at, started_at, finished_at, updated_at "
            "FROM screener_candidates ORDER BY symbol"
        ).fetchall()
        return {
            row["symbol"]: tuple(
                row[field]
                for field in ("created_at", "started_at", "finished_at", "updated_at")
            )
            for row in rows
        }
    finally:
        connection.close()


def test_s5_full_run_and_strict_replay(tmp_path: Path) -> None:
    orchestrator, calls = _orchestrator(tmp_path)

    first = orchestrator.run(MARKET_DATE)
    assert first.status == "success"
    assert first.market_date == MARKET_DATE
    assert first.universe_count == 3
    assert first.screened_count == 3
    assert first.triggered_count == 3
    assert first.candidate_count == 3
    assert first.truncated is False
    assert first.canonical_sha256 is not None
    assert first.replayed is False
    assert first.written is True
    assert calls == Counter({"universe": 1, "stage1": 1, **{
        f"stage2:{symbol}": 1 for symbol in SYMBOLS
    }})

    database_path = _database_path(orchestrator)
    counts_before = _row_counts(database_path)
    timestamps_before = _candidate_timestamps(database_path)

    second = orchestrator.run(MARKET_DATE)
    assert second.status == "success"
    assert second.screener_run_id == first.screener_run_id
    assert second.universe_run_id == first.universe_run_id
    assert second.canonical_sha256 == first.canonical_sha256
    assert second.replayed is True
    assert second.written is False
    assert calls == Counter({"universe": 1, "stage1": 1, **{
        f"stage2:{symbol}": 1 for symbol in SYMBOLS
    }})
    assert _row_counts(database_path) == counts_before
    assert _candidate_timestamps(database_path) == timestamps_before

    # A new process-like orchestrator can replay without invoking any hook when
    # the deterministic S4 run id is supplied explicitly.
    second_root = tmp_path / "second"
    second_root.mkdir()
    replay_orchestrator, replay_calls = _orchestrator(second_root)
    # Use the first database and hooks only; repositories are intentionally
    # constructed by a fresh orchestrator over the existing v11 database.
    replay_orchestrator = DailyScreenerOrchestrator(
        database_path,
        universe_provider=replay_orchestrator.universe_provider,
        stage1_runner=replay_orchestrator.stage1_runner,
        stage2_runner=replay_orchestrator.stage2_runner,
        replay_key_resolver=lambda market_date: first.screener_run_id,
    )
    replay = replay_orchestrator.run(MARKET_DATE)
    assert replay.replayed is True
    assert replay.canonical_sha256 == first.canonical_sha256
    assert replay_calls == Counter()


def test_s5_replay_runs_optional_candidate_validation_hook(
    tmp_path: Path,
) -> None:
    orchestrator, _calls = _orchestrator(tmp_path)
    validated: list[tuple[str, date]] = []
    orchestrator.candidate_validation_hook = (
        lambda symbol, market_date: validated.append((symbol, market_date))
    )

    first = orchestrator.run(MARKET_DATE)
    assert first.status == "success"
    assert validated == []

    second = orchestrator.run(MARKET_DATE)
    assert second.replayed is True
    assert validated == [(symbol, MARKET_DATE) for symbol in SYMBOLS]


def test_s5_partial_failure_retries_only_failed_candidate(tmp_path: Path) -> None:
    orchestrator, calls = _orchestrator(tmp_path, failure_budget={"1003": 1})

    first = orchestrator.run(MARKET_DATE)
    assert first.status == "partial_success"
    assert first.canonical_sha256 is None
    assert first.failed_candidates == ("1003",)
    assert calls["stage2:1001"] == 1
    assert calls["stage2:1002"] == 1
    assert calls["stage2:1003"] == 1
    timestamps_before = _candidate_timestamps(_database_path(orchestrator))
    counts_before = _row_counts(_database_path(orchestrator))

    second = orchestrator.run(MARKET_DATE)
    assert second.status == "success"
    assert second.screener_run_id == first.screener_run_id
    assert second.failed_candidates == ()
    assert second.candidate_replayed_count == 2
    assert calls["stage2:1001"] == 1
    assert calls["stage2:1002"] == 1
    assert calls["stage2:1003"] == 2
    assert _row_counts(_database_path(orchestrator))["screener_candidates"] == counts_before[
        "screener_candidates"
    ]
    timestamps_after = _candidate_timestamps(_database_path(orchestrator))
    assert timestamps_after["1001"] == timestamps_before["1001"]
    assert timestamps_after["1002"] == timestamps_before["1002"]


def test_s5_exact_29_success_1_failure_resume_gate(tmp_path: Path) -> None:
    symbols = tuple(f"{2000 + index}" for index in range(30))
    failed_symbol = symbols[-1]
    orchestrator, calls = _orchestrator(
        tmp_path,
        failure_budget={failed_symbol: 1},
        symbols=symbols,
    )

    first = orchestrator.run(MARKET_DATE)
    assert first.status == "partial_success"
    assert first.candidate_count == 30
    assert first.failed_candidates == (failed_symbol,)
    assert first.canonical_sha256 is None
    assert calls["universe"] == 1
    assert calls["stage1"] == 1
    assert all(calls[f"stage2:{symbol}"] == 1 for symbol in symbols)

    database_path = _database_path(orchestrator)
    timestamps_before = _candidate_timestamps(database_path)
    counts_before = _row_counts(database_path)

    second = orchestrator.run(MARKET_DATE)
    assert second.status == "success"
    assert second.screener_run_id == first.screener_run_id
    assert second.candidate_count == 30
    assert second.failed_candidates == ()
    assert second.candidate_replayed_count == 29
    assert second.canonical_sha256 is not None
    assert calls["universe"] == 1
    assert calls["stage1"] == 1
    assert all(calls[f"stage2:{symbol}"] == 1 for symbol in symbols[:-1])
    assert calls[f"stage2:{failed_symbol}"] == 2

    timestamps_after = _candidate_timestamps(database_path)
    assert all(
        timestamps_after[symbol] == timestamps_before[symbol]
        for symbol in symbols[:-1]
    )
    assert _row_counts(database_path)["screener_candidates"] == counts_before[
        "screener_candidates"
    ]


def test_s5_retry_failure_preserves_partial_checkpoint(tmp_path: Path) -> None:
    orchestrator, calls = _orchestrator(tmp_path, failure_budget={"1003": 2})

    first = orchestrator.run(MARKET_DATE)
    assert first.status == "partial_success"
    counts_before = _row_counts(_database_path(orchestrator))
    timestamps_before = _candidate_timestamps(_database_path(orchestrator))

    second = orchestrator.run(MARKET_DATE)
    assert second.status == "partial_success"
    assert second.screener_run_id == first.screener_run_id
    assert second.failed_candidates == ("1003",)
    assert calls["stage2:1003"] == 2
    assert _row_counts(_database_path(orchestrator)) == counts_before
    timestamps_after = _candidate_timestamps(_database_path(orchestrator))
    assert timestamps_after["1001"] == timestamps_before["1001"]
    assert timestamps_after["1002"] == timestamps_before["1002"]
    assert timestamps_after["1003"] == timestamps_before["1003"]


def test_s5_tamper_fails_closed_without_reanalysis(tmp_path: Path) -> None:
    orchestrator, calls = _orchestrator(tmp_path)
    first = orchestrator.run(MARKET_DATE)
    database_path = _database_path(orchestrator)
    counts_before = _row_counts(database_path)

    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "UPDATE screener_runs SET canonical_sha256 = ? WHERE screener_run_id = ?",
            ("0" * 64, first.screener_run_id),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ScreenerReplayIntegrityError):
        orchestrator.run(MARKET_DATE)
    assert calls == Counter({"universe": 1, "stage1": 1, **{
        f"stage2:{symbol}": 1 for symbol in SYMBOLS
    }})
    assert _row_counts(database_path) == counts_before


def test_s5_schema_and_import_boundary_has_no_scheduler_or_new_migration() -> None:
    source_path = Path(__file__).parents[1] / "app" / "screener" / "orchestration.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not any(
        module.startswith(("app.operations", "app.reports", "app.pipelines"))
        for module in imported
    )
    assert "SQLiteScreenerMigrationRunner" not in source_path.read_text(
        encoding="utf-8"
    )
    migration_names = {
        path.name
        for path in (source_path.parents[1] / "storage" / "migrations").glob("*.sql")
    }
    assert "0011_market_screener_persistence.sql" in migration_names
    assert "0012_dual_source_dataset_versions.sql" in migration_names
    source_text = source_path.read_text(encoding="utf-8")
    assert "DatasetVersionRepository" not in source_text
    assert "DatasetVersionMigrationRunner" not in source_text
