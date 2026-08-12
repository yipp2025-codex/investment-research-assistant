from __future__ import annotations

import ast
import hashlib
import json
import sqlite3
from dataclasses import FrozenInstanceError, replace
from datetime import date, timedelta
from pathlib import Path

import pytest

import app.storage.screener_history as history_module
import app.storage.screener_replay as replay_module
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
    STAGE2_METHODOLOGY_VERSION,
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
    finalize_stage2_result,
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
    SQLiteScreenerCheckpointRepository,
)
from app.storage.screener_history import (
    MethodologySelector,
    SQLiteScreenerHistoryReader,
)
from app.storage.screener_migration import SQLiteScreenerMigrationRunner
from app.storage.screener_replay import (
    FINALIZATION_FAULT_POINTS,
    SQLiteScreenerReplayRepository,
    ScreenerFinalizationStateError,
    ScreenerReplayIntegrityError,
)
from app.storage.universe_persistence import SQLiteMarketUniverseRepository


MARKET_DATE = date(2026, 8, 7)
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
PERSISTENCE_TABLES = (
    "market_universe_runs",
    "market_universe_members",
    "screener_runs",
    "screener_candidates",
    "candidate_reasons",
    "candidate_metrics",
    "screener_source_artifacts",
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _initialize(database_path: Path) -> None:
    research = SQLiteResearchRepository(database_path)
    research.initialize()
    assert research.get_schema_version() == 10
    SQLiteScreenerMigrationRunner(database_path).migrate()


def _evidence(market_date: date) -> tuple[SourceEvidence, ...]:
    payload = f"universe-{market_date.isoformat()}"
    return (
        SourceEvidence(
            source="twse",
            dataset="s4-replay-test-universe",
            source_ref="contract://screener-s4-replay-tests/universe",
            contract_version="screener-s4-replay-test-v1",
            payload_sha256=_hash(payload),
            payload_size_bytes=len(payload.encode("utf-8")),
            hash_basis="canonical-json-v1",
        ),
    )


def _universe(
    symbols: tuple[str, ...], market_date: date
) -> MarketUniverseSnapshot:
    evidence = _evidence(market_date)
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


def _stage1_candidate(
    symbol: str,
    rank: int,
    market_date: date,
    *,
    previous_reason_is_null: bool = False,
) -> Stage1Candidate:
    reason = Stage1Reason(
        code="price_change_1d",
        metric=PRICE_CHANGE_1D,
        component=None,
        previous=None if previous_reason_is_null else 1.0,
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
        previous_as_of_date=market_date - timedelta(days=1),
        current_as_of_date=market_date,
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


def _stage1_result(
    candidates: tuple[Stage1Candidate, ...], market_date: date
) -> Stage1ScanResult:
    count = len(candidates)
    return Stage1ScanResult(
        market_date=market_date,
        methodology_version=STAGE1_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=count,
        screened_count=count,
        triggered_count=count,
        candidate_count=count,
        candidate_limit=max(count, 1),
        truncated=False,
        candidates=candidates,
    )


def _metric_unit(name: str) -> str:
    if name in {VOLUME_RATIO_20D, VALUATION_PE, VALUATION_PB}:
        return "ratio"
    return "percent"


def _metrics(market_date: date) -> tuple[Stage2Metric, ...]:
    return tuple(
        Stage2Metric(
            name=name,
            status=Stage2MetricStatus.AVAILABLE,
            value=float(index + 1),
            previous_value=float(index),
            delta=1.0,
            unit=_metric_unit(name),
            as_of_date=market_date,
            previous_as_of_date=market_date - timedelta(days=1),
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
            endpoint="contract://screener-s4-replay-tests/twse-history",
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
            endpoint="contract://screener-s4-replay-tests/esun-validation",
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
            endpoint="contract://screener-s4-replay-tests/twse-validation",
            contract_version="validation-v1",
            payload_sha256=_hash(f"twse-validation-{symbol}"),
            payload_size_bytes=1,
            hash_basis="canonical-json-v1",
        ),
    )


def _reason(code: str) -> Stage2Reason:
    if code == "source_discrepancy":
        return Stage2Reason(
            code=code,
            metric="validation_state",
            previous="available",
            current="source_discrepancy",
            delta=None,
            unit="state",
            operator="status_equals",
            threshold="source_discrepancy",
            rule_version=STAGE2_METHODOLOGY_VERSION,
            reason_kind="data_quality",
            reason_class="data_quality_transition",
            threshold_multiple=1.0,
        )
    if code == "volume_anomaly":
        return Stage2Reason(
            code=code,
            metric=VOLUME_RATIO_20D,
            previous=1.0,
            current=3.0,
            delta=2.0,
            unit="ratio",
            operator="current_gte",
            threshold=2.0,
            rule_version=STAGE2_METHODOLOGY_VERSION,
            reason_kind="research_change",
            reason_class="liquidity_change",
            threshold_multiple=1.5,
        )
    if code != "return_60d_change":
        raise ValueError(f"unsupported test reason {code}")
    return Stage2Reason(
        code=code,
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


def _success_candidate(
    stage1: Stage1Candidate,
    market_date: date,
    *,
    reason_codes: tuple[str, ...] = ("return_60d_change",),
    candidate_kind: Stage2CandidateKind = Stage2CandidateKind.RESEARCH_CANDIDATE,
    quality_status: Stage2QualityStatus = Stage2QualityStatus.CLEAN,
    validation_status: str = "available",
) -> Stage2Candidate:
    return Stage2Candidate(
        rank=stage1.rank,
        stage1_rank=stage1.rank,
        symbol=stage1.symbol,
        name=stage1.name,
        market="TWSE",
        candidate_kind=candidate_kind,
        analysis_status=Stage2AnalysisStatus.AVAILABLE,
        stage1_reasons=stage1.reasons,
        stage2_reasons=tuple(_reason(code) for code in reason_codes),
        metrics=_metrics(market_date),
        data_quality=Stage2DataQuality(
            status=quality_status,
            validation_status=validation_status,
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


def _create_run(
    database_path: Path,
    *,
    market_date: date,
    symbols: tuple[str, ...],
    previous_reason_is_null: bool = False,
) -> tuple[
    SQLiteScreenerCheckpointRepository,
    str,
    tuple[Stage1Candidate, ...],
    dict[str, str],
]:
    universe = SQLiteMarketUniverseRepository(database_path).persist(
        _universe(symbols, market_date)
    )
    candidates = tuple(
        _stage1_candidate(
            symbol,
            rank,
            market_date,
            previous_reason_is_null=previous_reason_is_null,
        )
        for rank, symbol in enumerate(symbols, 1)
    )
    locator_hashes = {
        symbol: _hash(f"locator-{market_date.isoformat()}-{symbol}")
        for symbol in symbols
    }
    repository = SQLiteScreenerCheckpointRepository(database_path)
    run = repository.create_run(
        universe_run_id=universe.universe_run_id,
        stage1_result=_stage1_result(candidates, market_date),
        candidate_locators=tuple(
            CandidateInputLocator(symbol, locator_hashes[symbol])
            for symbol in symbols
        ),
    )
    return repository, run.screener_run_id, candidates, locator_hashes


def _setup_running_run(
    tmp_path: Path,
    *,
    count: int = 3,
    name: str = "s4-replay.db",
) -> tuple[
    Path,
    SQLiteScreenerCheckpointRepository,
    SQLiteScreenerReplayRepository,
    str,
    tuple[Stage1Candidate, ...],
    dict[str, str],
]:
    database_path = tmp_path / name
    _initialize(database_path)
    symbols = tuple(f"{1000 + index:04d}" for index in range(1, count + 1))
    checkpoint, run_id, stage1_candidates, locators = _create_run(
        database_path,
        market_date=MARKET_DATE,
        symbols=symbols,
    )
    return (
        database_path,
        checkpoint,
        SQLiteScreenerReplayRepository(database_path),
        run_id,
        stage1_candidates,
        locators,
    )


def _persist_success_candidates(
    repository: SQLiteScreenerCheckpointRepository,
    run_id: str,
    stage1_candidates: tuple[Stage1Candidate, ...],
    locators: dict[str, str],
    *,
    market_date: date = MARKET_DATE,
    candidates: tuple[Stage2Candidate, ...] | None = None,
) -> tuple[Stage2Candidate, ...]:
    values = candidates or tuple(
        _success_candidate(item, market_date) for item in stage1_candidates
    )
    for candidate in values:
        repository.persist_candidate(
            screener_run_id=run_id,
            candidate=candidate,
            research_locator_sha256=locators[candidate.symbol],
            snapshot_sha256=_hash(
                f"snapshot-{market_date.isoformat()}-{candidate.symbol}"
            ),
        )
    return values


def _table_state(database_path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    ordering = {
        "market_universe_runs": "universe_run_id",
        "market_universe_members": "universe_run_id, symbol",
        "screener_runs": "screener_run_id",
        "screener_candidates": "candidate_id",
        "candidate_reasons": "candidate_id, stage, ordinal",
        "candidate_metrics": "candidate_id, ordinal",
        "screener_source_artifacts": "artifact_ref_id",
    }
    with sqlite3.connect(database_path) as connection:
        return {
            table: tuple(
                tuple(row)
                for row in connection.execute(
                    f"SELECT * FROM {table} ORDER BY {ordering[table]}"
                )
            )
            for table in PERSISTENCE_TABLES
        }


def _candidate_children(
    database_path: Path,
) -> tuple[tuple[tuple[object, ...], ...], ...]:
    with sqlite3.connect(database_path) as connection:
        return tuple(
            tuple(tuple(row) for row in connection.execute(sql))
            for sql in (
                "SELECT * FROM candidate_reasons ORDER BY candidate_id, stage, ordinal",
                "SELECT * FROM candidate_metrics ORDER BY candidate_id, ordinal",
                "SELECT * FROM screener_source_artifacts "
                "WHERE candidate_id IS NOT NULL ORDER BY candidate_id, ordinal",
            )
        )


def test_run_finalization_uses_frozen_s3_ranking_and_seals_one_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        database_path,
        checkpoint,
        replay,
        run_id,
        stage1_candidates,
        locators,
    ) = _setup_running_run(tmp_path)
    candidates = (
        _success_candidate(stage1_candidates[0], MARKET_DATE),
        _success_candidate(
            stage1_candidates[1],
            MARKET_DATE,
            reason_codes=("source_discrepancy",),
            candidate_kind=Stage2CandidateKind.DATA_QUALITY_CANDIDATE,
            quality_status=Stage2QualityStatus.WARNING,
            validation_status="source_discrepancy",
        ),
        _success_candidate(
            stage1_candidates[2],
            MARKET_DATE,
            reason_codes=("return_60d_change", "volume_anomaly"),
        ),
    )
    _persist_success_candidates(
        checkpoint,
        run_id,
        stage1_candidates,
        locators,
        candidates=candidates,
    )
    expected = finalize_stage2_result(candidates=candidates, market_date=MARKET_DATE)
    children_before = _candidate_children(database_path)
    with sqlite3.connect(database_path) as connection:
        checkpoint_state_before = tuple(
            connection.execute(
                "SELECT candidate_id, created_at, started_at, finished_at, updated_at, "
                "attempt_count, snapshot_sha256, payload_sha256 "
                "FROM screener_candidates ORDER BY candidate_id"
            )
        )

    original_finalizer = replay_module.finalize_stage2_result
    calls = 0

    def counted_finalizer(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        return original_finalizer(*args, **kwargs)

    monkeypatch.setattr(replay_module, "finalize_stage2_result", counted_finalizer)
    finalized = replay.finalize_run(run_id)

    assert finalized.written is True
    assert calls == 1
    assert finalized.result.stage2_result == expected
    assert tuple(item.symbol for item in finalized.result.candidates) == tuple(
        item.symbol for item in expected.candidates
    )
    assert finalized.result.payload_sha256 == hashlib.sha256(
        finalized.result.canonical_json().encode("utf-8")
    ).hexdigest()
    assert _candidate_children(database_path) == children_before
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        run = connection.execute(
            "SELECT status, canonical_sha256, finished_at FROM screener_runs "
            "WHERE screener_run_id = ?",
            (run_id,),
        ).fetchone()
        ranks = tuple(
            connection.execute(
                "SELECT symbol, rank FROM screener_candidates "
                "WHERE screener_run_id = ? ORDER BY rank",
                (run_id,),
            )
        )
        checkpoint_state_after = tuple(
            tuple(row)
            for row in connection.execute(
                "SELECT candidate_id, created_at, started_at, finished_at, updated_at, "
                "attempt_count, snapshot_sha256, payload_sha256 "
                "FROM screener_candidates ORDER BY candidate_id"
            )
        )
    assert run["status"] == "success"
    assert run["canonical_sha256"] == finalized.result.payload_sha256
    assert run["finished_at"] is not None
    assert tuple(rank for unused_symbol, rank in ranks) == (1, 2, 3)
    assert checkpoint_state_after == checkpoint_state_before

    replayed = replay.replay_run(run_id)
    assert replayed.result == finalized.result
    assert replayed.written is False
    assert calls == 1, "success replay must not re-rank"
    payload = replayed.result.canonical_json().casefold()
    for forbidden in (
        "price_history",
        "daily_prices",
        "buy_score",
        "sell_score",
        "expected_return",
        "price_target",
        "recommendation",
    ):
        assert forbidden not in payload


@pytest.mark.parametrize("fault_point", FINALIZATION_FAULT_POINTS)
def test_every_finalization_fault_rolls_back_all_ranks_and_run_state(
    tmp_path: Path,
    fault_point: str,
) -> None:
    (
        database_path,
        checkpoint,
        replay,
        run_id,
        stage1_candidates,
        locators,
    ) = _setup_running_run(tmp_path, name=f"fault-{fault_point}.db")
    _persist_success_candidates(
        checkpoint,
        run_id,
        stage1_candidates,
        locators,
    )
    before = _table_state(database_path)

    def inject(point: str) -> None:
        if point == fault_point:
            raise RuntimeError(f"injected {point}")

    with pytest.raises(RuntimeError, match=fault_point):
        replay.finalize_run(run_id, fault_injector=inject)

    assert _table_state(database_path) == before
    with sqlite3.connect(database_path) as connection:
        run = connection.execute(
            "SELECT status, canonical_sha256, finished_at FROM screener_runs "
            "WHERE screener_run_id = ?",
            (run_id,),
        ).fetchone()
        ranks = connection.execute(
            "SELECT COUNT(*) FROM screener_candidates "
            "WHERE screener_run_id = ? AND rank IS NOT NULL",
            (run_id,),
        ).fetchone()[0]
    assert run == ("running", None, None)
    assert ranks == 0


def test_success_replay_is_strict_zero_write_and_preserves_every_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        database_path,
        checkpoint,
        replay,
        run_id,
        stage1_candidates,
        locators,
    ) = _setup_running_run(tmp_path)
    _persist_success_candidates(
        checkpoint,
        run_id,
        stage1_candidates,
        locators,
    )
    first = replay.finalize_run(run_id)
    before = _table_state(database_path)
    bytes_before = database_path.read_bytes()
    identity_connection = sqlite3.connect(database_path)
    try:
        expected_candidate_ids = tuple(
            row[0]
            for row in identity_connection.execute(
                "SELECT candidate_id FROM screener_candidates ORDER BY rank"
            )
        )
        universe_run_id = identity_connection.execute(
            "SELECT universe_run_id FROM screener_runs WHERE screener_run_id = ?",
            (run_id,),
        ).fetchone()[0]
    finally:
        identity_connection.close()
    dml_attempts: list[tuple[int, str | None]] = []
    original_connect = sqlite3.connect

    def guarded_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = original_connect(*args, **kwargs)

        def authorizer(
            action: int,
            arg1: str | None,
            unused_arg2: str | None,
            unused_database: str | None,
            unused_trigger: str | None,
        ) -> int:
            if action in {
                sqlite3.SQLITE_INSERT,
                sqlite3.SQLITE_UPDATE,
                sqlite3.SQLITE_DELETE,
            }:
                dml_attempts.append((action, arg1))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorizer)
        return connection

    monkeypatch.setattr(replay_module.sqlite3, "connect", guarded_connect)
    results = tuple(replay.replay_run(run_id) for _ in range(3))
    finalized_replay = replay.finalize_run(run_id)
    input_replay = checkpoint.create_run(
        universe_run_id=universe_run_id,
        stage1_result=_stage1_result(stage1_candidates, MARKET_DATE),
        candidate_locators=tuple(
            CandidateInputLocator(candidate.symbol, locators[candidate.symbol])
            for candidate in stage1_candidates
        ),
    )

    assert dml_attempts == []
    assert input_replay.screener_run_id == run_id
    assert input_replay.created is False
    assert all(item.written is False for item in (*results, finalized_replay))
    assert all(item.result == first.result for item in (*results, finalized_replay))
    assert _table_state(database_path) == before
    assert database_path.read_bytes() == bytes_before
    assert tuple(
        item.candidate_id
        for item in SQLiteScreenerHistoryReader(database_path).daily_candidate_set(
            MARKET_DATE
        )
    ) == expected_candidate_ids


TAMPER_CASES = (
    "candidate_rank",
    "candidate_delete",
    "metric_delete",
    "stage1_reason",
    "reason_ordinal",
    "artifact_metadata",
    "candidate_payload_hash",
    "run_canonical_hash",
)


@pytest.mark.parametrize("tamper", TAMPER_CASES)
def test_success_tamper_always_fails_closed_without_repair(
    tmp_path: Path,
    tamper: str,
) -> None:
    (
        database_path,
        checkpoint,
        replay,
        run_id,
        stage1_candidates,
        locators,
    ) = _setup_running_run(tmp_path, name=f"tamper-{tamper}.db")
    _persist_success_candidates(
        checkpoint,
        run_id,
        stage1_candidates,
        locators,
    )
    replay.finalize_run(run_id)

    with sqlite3.connect(database_path) as connection:
        if tamper == "candidate_rank":
            connection.execute(
                "UPDATE screener_candidates SET rank = 99 "
                "WHERE screener_run_id = ? AND rank = 1",
                (run_id,),
            )
        elif tamper == "candidate_delete":
            connection.execute(
                "DELETE FROM screener_candidates "
                "WHERE screener_run_id = ? AND rank = 1",
                (run_id,),
            )
        elif tamper == "metric_delete":
            connection.execute(
                "DELETE FROM candidate_metrics WHERE candidate_id = ("
                "SELECT candidate_id FROM screener_candidates "
                "WHERE screener_run_id = ? AND rank = 1) AND ordinal = 1",
                (run_id,),
            )
        elif tamper == "stage1_reason":
            connection.execute(
                "UPDATE candidate_reasons SET current_json = '9.0' "
                "WHERE candidate_id = (SELECT candidate_id FROM screener_candidates "
                "WHERE screener_run_id = ? AND rank = 1) "
                "AND stage = 'stage1' AND ordinal = 1",
                (run_id,),
            )
        elif tamper == "reason_ordinal":
            connection.execute(
                "UPDATE candidate_reasons SET ordinal = 2 "
                "WHERE candidate_id = (SELECT candidate_id FROM screener_candidates "
                "WHERE screener_run_id = ? AND rank = 1) "
                "AND stage = 'stage1' AND ordinal = 1",
                (run_id,),
            )
        elif tamper == "artifact_metadata":
            connection.execute(
                "UPDATE screener_source_artifacts SET payload_size_bytes = "
                "payload_size_bytes + 1 WHERE candidate_id = ("
                "SELECT candidate_id FROM screener_candidates "
                "WHERE screener_run_id = ? AND rank = 1) AND ordinal = 1",
                (run_id,),
            )
        elif tamper == "candidate_payload_hash":
            connection.execute(
                "UPDATE screener_candidates SET payload_sha256 = ? "
                "WHERE screener_run_id = ? AND rank = 1",
                ("f" * 64, run_id),
            )
        elif tamper == "run_canonical_hash":
            connection.execute(
                "UPDATE screener_runs SET canonical_sha256 = ? "
                "WHERE screener_run_id = ?",
                ("e" * 64, run_id),
            )
        connection.commit()

    tampered = _table_state(database_path)
    with pytest.raises(ScreenerReplayIntegrityError):
        replay.replay_run(run_id)
    assert _table_state(database_path) == tampered


@pytest.mark.parametrize(
    ("mutation", "error_type"),
    (
        ("pending_candidate", ScreenerFinalizationStateError),
        ("reason_count", ScreenerReplayIntegrityError),
        ("metric_count", ScreenerReplayIntegrityError),
        ("candidate_count", ScreenerReplayIntegrityError),
    ),
)
def test_finalization_preconditions_fail_before_any_rank_write(
    tmp_path: Path,
    mutation: str,
    error_type: type[Exception],
) -> None:
    (
        database_path,
        checkpoint,
        replay,
        run_id,
        stage1_candidates,
        locators,
    ) = _setup_running_run(tmp_path, count=2, name=f"precondition-{mutation}.db")
    persisted = stage1_candidates if mutation != "pending_candidate" else stage1_candidates[:1]
    _persist_success_candidates(
        checkpoint,
        run_id,
        persisted,
        locators,
    )
    with sqlite3.connect(database_path) as connection:
        if mutation == "reason_count":
            connection.execute(
                "UPDATE screener_candidates SET stage1_reason_count = 2 "
                "WHERE screener_run_id = ? AND stage1_rank = 1",
                (run_id,),
            )
        elif mutation == "metric_count":
            connection.execute(
                "UPDATE screener_candidates SET metric_count = 15 "
                "WHERE screener_run_id = ? AND stage1_rank = 1",
                (run_id,),
            )
        elif mutation == "candidate_count":
            connection.execute(
                "UPDATE screener_runs SET candidate_count = 1, truncated = 1 "
                "WHERE screener_run_id = ?",
                (run_id,),
            )
        connection.commit()
    before = _table_state(database_path)

    with pytest.raises(error_type):
        replay.finalize_run(run_id)
    assert _table_state(database_path) == before
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM screener_candidates WHERE rank IS NOT NULL"
        ).fetchone()[0] == 0


def test_frozen_result_is_immutable_and_has_no_full_history_payload(
    tmp_path: Path,
) -> None:
    (
        unused_database_path,
        checkpoint,
        replay,
        run_id,
        stage1_candidates,
        locators,
    ) = _setup_running_run(tmp_path, count=1)
    _persist_success_candidates(
        checkpoint,
        run_id,
        stage1_candidates,
        locators,
    )
    result = replay.finalize_run(run_id).result
    with pytest.raises(FrozenInstanceError):
        result.candidate_count = 99  # type: ignore[misc]
    payload = json.loads(result.canonical_json())
    assert len(payload["candidates"][0]["metrics"]) == 16
    assert "price_history" not in payload["candidates"][0]
    assert "observations" in payload["candidates"][0]["metrics"][0]


def test_historical_read_models_count_only_success_and_preserve_json_null(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.db"
    _initialize(database_path)
    replay = SQLiteScreenerReplayRepository(database_path)

    checkpoint1, run1, stage1_day1, locators1 = _create_run(
        database_path,
        market_date=MARKET_DATE,
        symbols=("1001",),
        previous_reason_is_null=True,
    )
    _persist_success_candidates(
        checkpoint1,
        run1,
        stage1_day1,
        locators1,
        market_date=MARKET_DATE,
    )
    replay.finalize_run(run1)

    day2 = MARKET_DATE + timedelta(days=1)
    checkpoint2, run2, stage1_day2, locators2 = _create_run(
        database_path,
        market_date=day2,
        symbols=("1001", "1002"),
    )
    _persist_success_candidates(
        checkpoint2,
        run2,
        stage1_day2,
        locators2,
        market_date=day2,
    )
    replay.finalize_run(run2)

    day3 = MARKET_DATE + timedelta(days=2)
    checkpoint3, run3, stage1_day3, locators3 = _create_run(
        database_path,
        market_date=day3,
        symbols=("1001", "1003"),
    )
    checkpoint3.persist_candidate(
        screener_run_id=run3,
        candidate=_success_candidate(stage1_day3[0], day3),
        research_locator_sha256=locators3["1001"],
        snapshot_sha256=_hash("partial-success-1001"),
    )
    checkpoint3.persist_candidate(
        screener_run_id=run3,
        candidate=failed_stage2_candidate(
            stage1_candidate=stage1_day3[1],
            error=RuntimeError("isolated fixture failure"),
            market_date=day3,
        ),
        research_locator_sha256=locators3["1003"],
        snapshot_sha256=None,
    )

    reader = SQLiteScreenerHistoryReader(database_path)
    assert reader.selection_count("1001") == 2
    assert reader.selection_count("1002") == 1
    assert reader.selection_count("1003") == 0

    frequencies = reader.reason_frequency()
    assert tuple((item.stage, item.code, item.count) for item in frequencies) == (
        ("stage1", "price_change_1d", 3),
        ("stage2", "return_60d_change", 3),
    )
    assert reader.reason_frequency(stage="stage1")[0].count == 3
    assert reader.reason_frequency(stage="stage2")[0].count == 3

    daily = reader.daily_candidate_set(day2)
    assert tuple(item.symbol for item in daily) == ("1001", "1002")
    assert tuple(item.rank for item in daily) == (1, 2)
    assert all(item.screener_run_id == run2 for item in daily)

    history = reader.reason_history("1001", code="price_change_1d")
    assert tuple(item.market_date for item in history) == (MARKET_DATE, day2)
    assert history[0].previous is None
    assert history[0].current == 4.0
    assert history[0].threshold == 3.0
    assert all(item.stage == "stage1" for item in history)
    stage2_history = reader.reason_history(
        "1001",
        metric=RETURN_60D,
        stage="stage2",
    )
    assert len(stage2_history) == 2
    assert all(item.stage == "stage2" for item in stage2_history)

    frozen_v1 = MethodologySelector(
        STAGE1_METHODOLOGY_VERSION,
        STAGE2_METHODOLOGY_VERSION,
    )
    future_v2 = MethodologySelector(
        "screener-stage1-v2",
        "screener-stage2-v2",
    )
    comparison = reader.compare_methodologies(
        day2,
        left=frozen_v1,
        right=future_v2,
    )
    assert comparison.left.run_ids == (run2,)
    assert comparison.left.candidate_symbols == ("1001", "1002")
    assert comparison.left.selection_count == 2
    assert comparison.right.run_ids == ()
    assert comparison.selection_overlap == ()
    assert comparison.left_only == ("1001", "1002")
    assert comparison.right_only == ()
    same = reader.compare_methodologies(day2, left=frozen_v1, right=frozen_v1)
    assert same.selection_overlap == ("1001", "1002")
    assert same.left_only == same.right_only == ()


def test_historical_queries_are_authorized_read_only_and_architecturally_isolated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        database_path,
        checkpoint,
        replay,
        run_id,
        stage1_candidates,
        locators,
    ) = _setup_running_run(tmp_path, count=1)
    _persist_success_candidates(
        checkpoint,
        run_id,
        stage1_candidates,
        locators,
    )
    replay.finalize_run(run_id)
    before = _table_state(database_path)
    original_connect = sqlite3.connect
    dml_attempts: list[tuple[int, str | None]] = []
    forbidden_reads: list[str] = []

    def guarded_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = original_connect(*args, **kwargs)

        def authorizer(
            action: int,
            arg1: str | None,
            unused_arg2: str | None,
            unused_database: str | None,
            unused_trigger: str | None,
        ) -> int:
            if action in {
                sqlite3.SQLITE_INSERT,
                sqlite3.SQLITE_UPDATE,
                sqlite3.SQLITE_DELETE,
            }:
                dml_attempts.append((action, arg1))
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and arg1 in {"symbols", "watchlist"}:
                forbidden_reads.append(str(arg1))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorizer)
        return connection

    monkeypatch.setattr(history_module.sqlite3, "connect", guarded_connect)
    reader = SQLiteScreenerHistoryReader(database_path)
    selector = MethodologySelector(
        STAGE1_METHODOLOGY_VERSION,
        STAGE2_METHODOLOGY_VERSION,
    )
    assert reader.selection_count("1001") == 1
    assert reader.reason_frequency()
    assert reader.daily_candidate_set(MARKET_DATE)
    assert reader.reason_history("1001", code="price_change_1d")
    assert reader.compare_methodologies(
        MARKET_DATE,
        left=selector,
        right=selector,
    ).selection_overlap == ("1001",)
    assert dml_attempts == []
    assert forbidden_reads == []
    assert _table_state(database_path) == before

    source = Path(history_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported.update(
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    assert not any(
        value.startswith(
            (
                "app.providers",
                "app.research_dataset",
                "app.screener.stage1",
                "app.screener.stage2",
                "app.storage.screener_migration",
                "app.storage.sqlite",
            )
        )
        for value in imported
    )


def test_s4_4_modules_do_not_expose_operations_or_trading_dependencies() -> None:
    forbidden_import_prefixes = (
        "app.providers",
        "app.operations",
        "app.scheduler",
        "app.reporting",
        "app.trading",
    )
    for module in (replay_module, history_module):
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported.update(
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        assert not any(
            value.startswith(forbidden_import_prefixes) for value in imported
        )
