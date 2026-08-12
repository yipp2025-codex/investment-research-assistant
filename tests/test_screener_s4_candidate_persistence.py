from __future__ import annotations

import ast
import hashlib
import sqlite3
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

import app.storage.candidate_persistence as candidate_persistence_module
from app.screener.stage1 import (
    PRICE_CHANGE_1D,
    DataQualityStatus,
    MetricStatus,
    Stage1Candidate,
    Stage1DataQuality,
    Stage1Metric,
    Stage1Reason,
    Stage1ScanResult,
)
from app.screener.stage2 import (
    MA_DISTANCE_20D,
    MA_DISTANCE_60D,
    MA_DISTANCE_120D,
    MA_DISTANCE_250D,
    MAX_DRAWDOWN_20D,
    MAX_DRAWDOWN_60D,
    MAX_DRAWDOWN_120D,
    MAX_DRAWDOWN_250D,
    RETURN_20D,
    RETURN_60D,
    RETURN_120D,
    VALUATION_PB,
    VALUATION_PE,
    VALUATION_YIELD,
    VOLATILITY_60D,
    VOLUME_RATIO_20D,
    Stage2AnalysisStatus,
    Stage2ArtifactRef,
    Stage2Candidate,
    Stage2CandidateKind,
    Stage2DataQuality,
    Stage2Metric,
    Stage2MetricStatus,
    Stage2Provenance,
    Stage2QualityStatus,
    Stage2Reason,
    failed_stage2_candidate,
)
from app.screener.universe import (
    UNIVERSE_METHODOLOGY_VERSION,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)
from app.storage import SQLiteResearchRepository
from app.storage.candidate_persistence import (
    CandidateInputLocator,
    CandidateSourcePolicyError,
    SQLiteScreenerCheckpointRepository,
    ScreenerCheckpointConflictError,
    ScreenerCheckpointStateError,
)
from app.storage.screener_migration import SQLiteScreenerMigrationRunner
from app.storage.universe_persistence import SQLiteMarketUniverseRepository


MARKET_DATE = date(2026, 8, 7)
PREVIOUS_DATE = date(2026, 8, 6)
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


def _evidence() -> tuple[SourceEvidence, ...]:
    value = SourceEvidence(
        source="twse",
        dataset="s4-candidate-test-universe",
        source_ref="contract://screener-s4-candidate-tests/universe",
        contract_version="screener-s4-candidate-test-v1",
        payload_sha256=_hash("universe-evidence"),
        payload_size_bytes=17,
        hash_basis="canonical-json-v1",
    )
    return (value,)


def _universe(symbols: tuple[str, ...]) -> MarketUniverseSnapshot:
    evidence = _evidence()
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
        code="price_change_1d",
        metric=PRICE_CHANGE_1D,
        component=None,
        previous=1.0,
        current=4.0,
        delta=3.0,
        unit="percent",
        operator="abs_current_gte",
        threshold=3.0,
        rule_version="screener-stage1-v1",
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
        previous_as_of_date=PREVIOUS_DATE,
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


def _stage1_result(candidates: tuple[Stage1Candidate, ...]) -> Stage1ScanResult:
    count = len(candidates)
    return Stage1ScanResult(
        market_date=MARKET_DATE,
        methodology_version="screener-stage1-v1",
        source_policy="twse_baseline",
        universe_count=count,
        screened_count=count,
        triggered_count=count,
        candidate_count=count,
        candidate_limit=max(count, 1),
        truncated=False,
        candidates=candidates,
    )


def _unit(name: str) -> str:
    if name == VOLUME_RATIO_20D:
        return "ratio"
    if name in {VALUATION_PE, VALUATION_PB}:
        return "ratio"
    return "percent"


def _metrics() -> tuple[Stage2Metric, ...]:
    return tuple(
        Stage2Metric(
            name=name,
            status=Stage2MetricStatus.AVAILABLE,
            value=float(index + 1),
            previous_value=float(index),
            delta=1.0,
            unit=_unit(name),
            as_of_date=MARKET_DATE,
            previous_as_of_date=PREVIOUS_DATE,
            observations=250,
            previous_observations=249,
        )
        for index, name in enumerate(METRIC_ORDER)
    )


def _artifacts(symbol: str) -> tuple[Stage2ArtifactRef, ...]:
    return (
        Stage2ArtifactRef(
            owner_kind="historical",
            owner_run_id=f"historical-{symbol}",
            provider="twse",
            dataset="STOCK_DAY",
            endpoint="contract://screener-s4-candidate-tests/twse-history",
            contract_version="twse-history-v1",
            payload_sha256=_hash(f"twse-history-{symbol}"),
            payload_size_bytes=250,
            hash_basis="canonical-json-v1",
        ),
        Stage2ArtifactRef(
            owner_kind="validation",
            owner_run_id=f"validation-{symbol}",
            provider="esun",
            dataset="daily-validation",
            endpoint="contract://screener-s4-candidate-tests/esun-validation",
            contract_version="validation-v1",
            payload_sha256=_hash(f"esun-validation-{symbol}"),
            payload_size_bytes=1,
            hash_basis="canonical-json-v1",
        ),
        Stage2ArtifactRef(
            owner_kind="validation",
            owner_run_id=f"validation-{symbol}",
            provider="twse",
            dataset="daily-validation",
            endpoint="contract://screener-s4-candidate-tests/twse-validation",
            contract_version="validation-v1",
            payload_sha256=_hash(f"twse-validation-{symbol}"),
            payload_size_bytes=1,
            hash_basis="canonical-json-v1",
        ),
    )


def _success_candidate(stage1: Stage1Candidate) -> Stage2Candidate:
    reason = Stage2Reason(
        code="return_60d_change",
        metric=RETURN_60D,
        previous=2.0,
        current=4.0,
        delta=2.0,
        unit="percentage_point",
        operator="abs_delta_gte",
        threshold=1.0,
        rule_version="screener-stage2-v1",
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
        metrics=_metrics(),
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
            validation_sources=("esun", "twse"),
            artifact_refs=_artifacts(stage1.symbol),
        ),
    )


def _setup_run(
    tmp_path: Path,
    *,
    count: int,
    name: str,
) -> tuple[
    Path,
    SQLiteScreenerCheckpointRepository,
    str,
    tuple[Stage1Candidate, ...],
    dict[str, str],
]:
    database_path = tmp_path / name
    research = SQLiteResearchRepository(database_path)
    research.initialize()
    SQLiteScreenerMigrationRunner(database_path).migrate()
    symbols = tuple(f"{1000 + index:04d}" for index in range(1, count + 1))
    universe_result = SQLiteMarketUniverseRepository(database_path).persist(
        _universe(symbols)
    )
    stage1_candidates = tuple(
        _stage1_candidate(symbol, rank)
        for rank, symbol in enumerate(symbols, 1)
    )
    stage1_result = _stage1_result(stage1_candidates)
    locator_hashes = {symbol: _hash(f"locator-{symbol}") for symbol in symbols}
    locators = tuple(
        CandidateInputLocator(symbol, locator_hashes[symbol]) for symbol in symbols
    )
    repository = SQLiteScreenerCheckpointRepository(database_path)
    run = repository.create_run(
        universe_run_id=universe_result.universe_run_id,
        stage1_result=stage1_result,
        candidate_locators=locators,
    )
    return database_path, repository, run.screener_run_id, stage1_candidates, locator_hashes


def _candidate_state(
    database_path: Path, candidate_ids: tuple[str, ...]
) -> dict[str, tuple[tuple[object, ...], ...]]:
    state: dict[str, tuple[tuple[object, ...], ...]] = {}
    with sqlite3.connect(database_path) as connection:
        for candidate_id in candidate_ids:
            rows: list[tuple[object, ...]] = []
            rows.extend(
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM screener_candidates WHERE candidate_id = ?",
                    (candidate_id,),
                )
            )
            rows.extend(
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM candidate_reasons WHERE candidate_id = ? "
                    "ORDER BY stage, ordinal",
                    (candidate_id,),
                )
            )
            rows.extend(
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM candidate_metrics WHERE candidate_id = ? "
                    "ORDER BY ordinal",
                    (candidate_id,),
                )
            )
            rows.extend(
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM screener_source_artifacts WHERE candidate_id = ? "
                    "ORDER BY ordinal",
                    (candidate_id,),
                )
            )
            state[candidate_id] = tuple(rows)
    return state


def _insert_provenance_runs(database_path: Path, symbol: str) -> tuple[str, str, str]:
    pipeline_run_id = f"pipeline-{symbol}"
    historical_run_id = f"historical-provenance-{symbol}"
    validation_run_id = f"validation-provenance-{symbol}"
    timestamp = "2026-08-07T00:00:00+00:00"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO pipeline_runs ("
            "run_id, symbol, target_date, requested_start_date, requested_end_date, "
            "status, provider, created_at, updated_at"
            ") VALUES (?, ?, '2026-08-07', '2026-08-07', '2026-08-07', "
            "'pending', 'twse', ?, ?)",
            (pipeline_run_id, symbol, timestamp, timestamp),
        )
        connection.execute(
            "INSERT INTO historical_sync_runs ("
            "run_id, symbol, target_date, target_observations, provider, status, "
            "next_month, created_at, updated_at"
            ") VALUES (?, ?, '2026-08-07', 250, 'twse', 'pending', "
            "'2026-08', ?, ?)",
            (historical_run_id, symbol, timestamp, timestamp),
        )
        connection.execute(
            "INSERT INTO market_data_validation_runs ("
            "run_id, symbol, target_date, requested_start_date, left_provider, "
            "right_provider, status, created_at, updated_at"
            ") VALUES (?, ?, '2026-08-07', '2026-08-07', 'twse', 'esun', "
            "'pending', ?, ?)",
            (validation_run_id, symbol, timestamp, timestamp),
        )
        connection.commit()
    return pipeline_run_id, historical_run_id, validation_run_id


def test_candidate_bundle_round_trip_preserves_reasons_metrics_and_provenance(
    tmp_path: Path,
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=1,
        name="candidate-round-trip.db",
    )
    candidate = _success_candidate(stage1_candidates[0])
    provenance_ids = _insert_provenance_runs(database_path, candidate.symbol)
    candidate = replace(
        candidate,
        provenance=replace(
            candidate.provenance,
            pipeline_run_id=provenance_ids[0],
            historical_run_id=provenance_ids[1],
            validation_run_id=provenance_ids[2],
        ),
    )

    result = repository.persist_candidate(
        screener_run_id=run_id,
        candidate=candidate,
        research_locator_sha256=locators[candidate.symbol],
        snapshot_sha256=_hash("snapshot-1001"),
    )
    reconstructed = repository.load_candidate(result.candidate_id)

    assert result.written is True
    assert reconstructed == result.checkpoint
    assert reconstructed.stage1_reasons == candidate.stage1_reasons
    assert reconstructed.stage2_reasons == candidate.stage2_reasons
    assert reconstructed.metrics == candidate.metrics
    assert len(reconstructed.metrics) == 16
    assert reconstructed.provenance == candidate.provenance
    assert (
        reconstructed.provenance.pipeline_run_id,
        reconstructed.provenance.historical_run_id,
        reconstructed.provenance.validation_run_id,
    ) == provenance_ids
    assert "rank" not in reconstructed.as_dict()
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "SELECT rank, status, metric_count, payload_sha256 "
            "FROM screener_candidates WHERE candidate_id = ?",
            (result.candidate_id,),
        ).fetchone()
        reason_counts = dict(
            connection.execute(
                "SELECT stage, COUNT(*) FROM candidate_reasons "
                "WHERE candidate_id = ? GROUP BY stage",
                (result.candidate_id,),
            )
        )
        artifact_roles = tuple(
            connection.execute(
                "SELECT provider, source_role FROM screener_source_artifacts "
                "WHERE candidate_id = ? ORDER BY ordinal",
                (result.candidate_id,),
            )
        )
        run = connection.execute(
            "SELECT status, canonical_sha256 FROM screener_runs "
            "WHERE screener_run_id = ?",
            (run_id,),
        ).fetchone()
    assert row == (None, "success", 16, reconstructed.payload_sha256)
    assert reason_counts == {"stage1": 1, "stage2": 1}
    assert artifact_roles == (
        ("twse", "canonical"),
        ("esun", "validation"),
        ("twse", "validation"),
    )
    assert run == ("running", None)


@pytest.mark.parametrize(
    "method_name",
    (
        "_prepare_candidate",
        "_insert_reasons",
        "_insert_metrics",
        "_insert_artifacts",
        "_complete_candidate",
        "_refresh_run_status",
    ),
)
def test_candidate_bundle_fault_at_any_stage_rolls_back_to_pending_shell(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=1,
        name=f"bundle-rollback-{method_name}.db",
    )
    original = getattr(repository, method_name)

    def execute_then_fail(*args: object, **kwargs: object) -> None:
        original(*args, **kwargs)
        raise RuntimeError(f"fault after {method_name}")

    monkeypatch.setattr(repository, method_name, execute_then_fail)
    candidate = _success_candidate(stage1_candidates[0])
    with pytest.raises(RuntimeError, match=method_name):
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=candidate,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash("snapshot-1001"),
        )

    with sqlite3.connect(database_path) as connection:
        candidate_row = connection.execute(
            "SELECT status, rank, attempt_count, payload_sha256 "
            "FROM screener_candidates"
        ).fetchone()
        child_counts = tuple(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "candidate_reasons",
                "candidate_metrics",
            )
        )
        artifact_count = connection.execute(
            "SELECT COUNT(*) FROM screener_source_artifacts "
            "WHERE candidate_id IS NOT NULL"
        ).fetchone()[0]
        run_status = connection.execute(
            "SELECT status FROM screener_runs"
        ).fetchone()[0]
    assert candidate_row == ("pending", None, 0, None)
    assert child_counts == (0, 0)
    assert artifact_count == 0
    assert run_status == "running"


def test_partial_success_retry_changes_only_failed_candidate_bundle(
    tmp_path: Path,
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=30,
        name="partial-retry.db",
    )
    successful_ids = []
    for stage1 in stage1_candidates[:29]:
        candidate = _success_candidate(stage1)
        result = repository.persist_candidate(
            screener_run_id=run_id,
            candidate=candidate,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash(f"snapshot-{candidate.symbol}"),
        )
        successful_ids.append(result.candidate_id)
    failed_stage1 = stage1_candidates[-1]
    failed = failed_stage2_candidate(
        stage1_candidate=failed_stage1,
        error=RuntimeError("isolated"),
        market_date=MARKET_DATE,
    )
    failed_result = repository.persist_candidate(
        screener_run_id=run_id,
        candidate=failed,
        research_locator_sha256=locators[failed.symbol],
        snapshot_sha256=None,
    )

    before_success = _candidate_state(database_path, tuple(successful_ids))
    with sqlite3.connect(database_path) as connection:
        run_before = connection.execute(
            "SELECT status, canonical_sha256 FROM screener_runs"
        ).fetchone()
        statuses_before = dict(
            connection.execute(
                "SELECT status, COUNT(*) FROM screener_candidates GROUP BY status"
            )
        )
        ranks_before = connection.execute(
            "SELECT COUNT(*) FROM screener_candidates WHERE rank IS NOT NULL"
        ).fetchone()[0]
    assert run_before == ("partial_success", None)
    assert statuses_before == {"failed": 1, "success": 29}
    assert ranks_before == 0

    recovered = _success_candidate(failed_stage1)
    retry = repository.persist_candidate(
        screener_run_id=run_id,
        candidate=recovered,
        research_locator_sha256=locators[recovered.symbol],
        snapshot_sha256=_hash(f"snapshot-{recovered.symbol}"),
        retry_failed=True,
    )

    after_success = _candidate_state(database_path, tuple(successful_ids))
    assert after_success == before_success
    assert retry.candidate_id == failed_result.candidate_id
    with sqlite3.connect(database_path) as connection:
        run_after = connection.execute(
            "SELECT status, canonical_sha256 FROM screener_runs"
        ).fetchone()
        statuses_after = dict(
            connection.execute(
                "SELECT status, COUNT(*) FROM screener_candidates GROUP BY status"
            )
        )
        retried = connection.execute(
            "SELECT status, attempt_count, rank, failure_code, failure_type "
            "FROM screener_candidates WHERE candidate_id = ?",
            (failed_result.candidate_id,),
        ).fetchone()
        ranks_after = connection.execute(
            "SELECT COUNT(*) FROM screener_candidates WHERE rank IS NOT NULL"
        ).fetchone()[0]
    assert run_after == ("running", None)
    assert statuses_after == {"success": 30}
    assert retried == ("success", 2, None, None, None)
    assert ranks_after == 0


def test_success_candidate_replay_is_zero_write_and_conflict_never_rebuilds(
    tmp_path: Path,
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=1,
        name="success-immutable.db",
    )
    candidate = _success_candidate(stage1_candidates[0])
    first = repository.persist_candidate(
        screener_run_id=run_id,
        candidate=candidate,
        research_locator_sha256=locators[candidate.symbol],
        snapshot_sha256=_hash("snapshot-1001"),
    )
    before = _candidate_state(database_path, (first.candidate_id,))

    replay = repository.persist_candidate(
        screener_run_id=run_id,
        candidate=candidate,
        research_locator_sha256=locators[candidate.symbol],
        snapshot_sha256=_hash("snapshot-1001"),
    )
    assert replay.written is False
    assert _candidate_state(database_path, (first.candidate_id,)) == before
    with pytest.raises(ScreenerCheckpointStateError, match="successful candidate"):
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=candidate,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash("snapshot-1001"),
            retry_failed=True,
        )
    assert _candidate_state(database_path, (first.candidate_id,)) == before

    changed_reason = replace(
        candidate.stage2_reasons[0],
        current=5.0,
        delta=3.0,
        threshold_multiple=3.0,
    )
    changed = replace(candidate, stage2_reasons=(changed_reason,))
    with pytest.raises(ScreenerCheckpointConflictError, match="immutable"):
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=changed,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash("snapshot-1001"),
        )
    assert _candidate_state(database_path, (first.candidate_id,)) == before


def test_failed_retry_fault_restores_original_partial_checkpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=2,
        name="retry-rollback.db",
    )
    first = _success_candidate(stage1_candidates[0])
    first_result = repository.persist_candidate(
        screener_run_id=run_id,
        candidate=first,
        research_locator_sha256=locators[first.symbol],
        snapshot_sha256=_hash(f"snapshot-{first.symbol}"),
    )
    failed_stage1 = stage1_candidates[1]
    failed = failed_stage2_candidate(
        stage1_candidate=failed_stage1,
        error=RuntimeError("isolated"),
        market_date=MARKET_DATE,
    )
    failed_result = repository.persist_candidate(
        screener_run_id=run_id,
        candidate=failed,
        research_locator_sha256=locators[failed.symbol],
        snapshot_sha256=None,
    )
    candidate_ids = (first_result.candidate_id, failed_result.candidate_id)
    before = _candidate_state(database_path, candidate_ids)
    with sqlite3.connect(database_path) as connection:
        run_before = tuple(
            connection.execute(
                "SELECT * FROM screener_runs WHERE screener_run_id = ?",
                (run_id,),
            ).fetchone()
        )
    original = repository._insert_metrics

    def metrics_then_fail(*args: object, **kwargs: object) -> None:
        original(*args, **kwargs)
        raise RuntimeError("retry persistence fault")

    monkeypatch.setattr(repository, "_insert_metrics", metrics_then_fail)
    recovered = _success_candidate(failed_stage1)
    with pytest.raises(RuntimeError, match="retry persistence fault"):
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=recovered,
            research_locator_sha256=locators[recovered.symbol],
            snapshot_sha256=_hash(f"snapshot-{recovered.symbol}"),
            retry_failed=True,
        )

    assert _candidate_state(database_path, candidate_ids) == before
    with sqlite3.connect(database_path) as connection:
        run_after = tuple(
            connection.execute(
                "SELECT * FROM screener_runs WHERE screener_run_id = ?",
                (run_id,),
            ).fetchone()
        )
    assert run_after == run_before


def test_external_success_child_tamper_fails_closed_without_repair(
    tmp_path: Path,
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=1,
        name="success-child-tamper.db",
    )
    candidate = _success_candidate(stage1_candidates[0])
    first = repository.persist_candidate(
        screener_run_id=run_id,
        candidate=candidate,
        research_locator_sha256=locators[candidate.symbol],
        snapshot_sha256=_hash("snapshot-1001"),
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE candidate_metrics SET value = value + 1, delta = delta + 1 "
            "WHERE candidate_id = ? AND ordinal = 1",
            (first.candidate_id,),
        )
        connection.commit()
    tampered = _candidate_state(database_path, (first.candidate_id,))

    with pytest.raises(ScreenerCheckpointConflictError, match="hash"):
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=candidate,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash("snapshot-1001"),
        )
    assert _candidate_state(database_path, (first.candidate_id,)) == tampered


def test_stage1_reason_change_is_rejected_by_candidate_input_identity(
    tmp_path: Path,
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=1,
        name="stage1-handoff.db",
    )
    candidate = _success_candidate(stage1_candidates[0])
    altered_reason = replace(
        candidate.stage1_reasons[0],
        current=6.0,
        delta=5.0,
        threshold_multiple=2.0,
    )
    altered = replace(candidate, stage1_reasons=(altered_reason,))

    with pytest.raises(ScreenerCheckpointConflictError, match="handoff"):
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=altered,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash("snapshot-1001"),
        )
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "SELECT status, attempt_count FROM screener_candidates"
        ).fetchone()
        children = connection.execute(
            "SELECT COUNT(*) FROM candidate_reasons"
        ).fetchone()[0]
    assert row == ("pending", 0)
    assert children == 0


@pytest.mark.parametrize("source", ("esun", "esun-historical"))
def test_esun_canonical_source_is_hard_rejected_without_writes(
    tmp_path: Path,
    source: str,
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=1,
        name=f"canonical-{source}.db",
    )
    candidate = _success_candidate(stage1_candidates[0])
    invalid = replace(
        candidate,
        provenance=replace(
            candidate.provenance,
            canonical_sources=(source,),
            validation_sources=(source,),
        ),
    )

    with pytest.raises(CandidateSourcePolicyError, match="canonical"):
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=invalid,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash("snapshot-1001"),
        )
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "SELECT status, attempt_count FROM screener_candidates"
        ).fetchone()
        children = tuple(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("candidate_reasons", "candidate_metrics")
        )
    assert row == ("pending", 0)
    assert children == (0, 0)


def test_candidate_artifact_credential_ref_is_rejected_before_write(
    tmp_path: Path,
) -> None:
    database_path, repository, run_id, stage1_candidates, locators = _setup_run(
        tmp_path,
        count=1,
        name="artifact-credential.db",
    )
    candidate = _success_candidate(stage1_candidates[0])
    unsafe_artifact = replace(
        candidate.provenance.artifact_refs[0],
        endpoint="https://openapi.twse.com.tw/data?token=secret",
    )
    unsafe = replace(
        candidate,
        provenance=replace(
            candidate.provenance,
            artifact_refs=(
                unsafe_artifact,
                *candidate.provenance.artifact_refs[1:],
            ),
        ),
    )

    with pytest.raises(CandidateSourcePolicyError, match="credential"):
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=unsafe,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash("snapshot-1001"),
        )
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT status FROM screener_candidates"
        ).fetchone()[0] == "pending"
        assert connection.execute(
            "SELECT COUNT(*) FROM screener_source_artifacts "
            "WHERE candidate_id IS NOT NULL"
        ).fetchone()[0] == 0


def test_create_run_replay_is_zero_write_for_progressed_candidate_shells(
    tmp_path: Path,
) -> None:
    database_path, repository, run_id, stage1_candidates, locator_hashes = _setup_run(
        tmp_path,
        count=1,
        name="run-replay.db",
    )
    candidate = _success_candidate(stage1_candidates[0])
    repository.persist_candidate(
        screener_run_id=run_id,
        candidate=candidate,
        research_locator_sha256=locator_hashes[candidate.symbol],
        snapshot_sha256=_hash("snapshot-1001"),
    )
    before = _candidate_state(
        database_path,
        (
            repository.persist_candidate(
                screener_run_id=run_id,
                candidate=candidate,
                research_locator_sha256=locator_hashes[candidate.symbol],
                snapshot_sha256=_hash("snapshot-1001"),
            ).candidate_id,
        ),
    )
    with sqlite3.connect(database_path) as connection:
        universe_run_id = connection.execute(
            "SELECT universe_run_id FROM screener_runs WHERE screener_run_id = ?",
            (run_id,),
        ).fetchone()[0]
    replay = repository.create_run(
        universe_run_id=universe_run_id,
        stage1_result=_stage1_result(stage1_candidates),
        candidate_locators=(
            CandidateInputLocator(candidate.symbol, locator_hashes[candidate.symbol]),
        ),
    )

    assert replay.created is False
    assert replay.screener_run_id == run_id
    candidate_id = next(iter(before))
    assert _candidate_state(database_path, (candidate_id,)) == before


def test_checkpoint_repository_has_no_research_provider_or_operations_dependency() -> None:
    tree = ast.parse(
        Path(candidate_persistence_module.__file__).read_text(encoding="utf-8")
    )
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not any(
        name.startswith(
            (
                "app.analysis",
                "app.pipelines",
                "app.providers",
                "app.scheduler",
                "app.sqlite_research_dataset",
            )
        )
        for name in imported
    )
