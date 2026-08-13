from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
import hashlib
from pathlib import Path
import sqlite3

import pytest

from app.data_contracts.dataset_persistence import (
    CoverageBasis,
    DatasetArtifactRef,
    DatasetObservation,
    DatasetPersistenceContractError,
    build_canonical_dataset_version,
    build_provisional_dataset_version,
)
from app.data_contracts.dual_source import FailureClassification, SourceRole
from app.data_contracts.reconciliation import (
    RECONCILIATION_RELATION_DATASET,
    ReconciliationContractError,
    ReconciliationInput,
    reconcile_dataset_version,
)
from app.data_contracts.supplemental_eligibility import (
    InstrumentIdentity,
    OHLCVObservation,
    SecurityBoundaryEvidence,
    SupplementalCandidateInput,
    evaluate_supplemental,
)
from app.storage import SQLiteResearchRepository
from app.storage.dataset_versions import (
    DatasetMigrationStateError,
    DatasetVersionMigrationRunner,
    DatasetVersionRepository,
)
from app.storage.reconciliation import (
    DatasetReconciliationRepository,
    ReconciliationPersistenceError,
)
from app.storage.screener_migration import SQLiteScreenerMigrationRunner


BASE_DATE = date(2026, 1, 1)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identity(symbol: str) -> InstrumentIdentity:
    return InstrumentIdentity(
        symbol=symbol,
        market="TSE",
        exchange="TWSE",
        currency="TWD",
        security_type="EQUITY",
    )


def _eligibility_row(offset: int, symbol: str) -> OHLCVObservation:
    return OHLCVObservation(
        trade_date=BASE_DATE + timedelta(days=offset),
        open=100.0,
        high=105.0,
        low=95.0,
        close=102.0,
        volume=1000 + offset,
        symbol=symbol,
    )


def _candidate_6108(
    esun_count: int = 22, symbol: str = "6108"
) -> SupplementalCandidateInput:
    missing = tuple(BASE_DATE + timedelta(days=offset) for offset in range(228, 250))
    return SupplementalCandidateInput(
        requested_symbol=symbol,
        target_date=BASE_DATE + timedelta(days=249),
        requested_start_date=BASE_DATE,
        missing_twse_dates=missing,
        twse_failure_class=FailureClassification.PACING_BLOCKED,
        source_run_id=f"esun-run-{symbol}",
        artifact_sha256=_hash(f"esun-artifact-{symbol}"),
        twse_observations=tuple(
            _eligibility_row(offset, symbol) for offset in range(228)
        ),
        esun_requested_identity=_identity(symbol),
        esun_returned_identity=_identity(symbol),
        esun_observations=tuple(
            _eligibility_row(offset, symbol)
            for offset in range(228, 228 + esun_count)
        ),
        security_boundary=SecurityBoundaryEvidence.passed(),
    )


def _observation(
    offset: int,
    *,
    symbol: str,
    provider: str,
    role: SourceRole,
    run_id: str,
    selected: bool,
    price_delta: float = 0.0,
    volume_delta: int = 0,
) -> DatasetObservation:
    trade_date = BASE_DATE + timedelta(days=offset)
    open_price = 100.0 + price_delta
    high = 105.0 + price_delta
    low = 95.0 + price_delta
    close = 102.0 + price_delta
    volume = 1000 + offset + volume_delta
    return DatasetObservation(
        symbol=symbol,
        trade_date=trade_date,
        provider=provider,
        source_role=role,
        source_run_id=run_id,
        selected=selected,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        observation_sha256=_hash(
            f"{symbol}|{trade_date.isoformat()}|{provider}|{role.value}|"
            f"{run_id}|{selected}|{price_delta}|{volume}"
        ),
    )


def _artifact(ordinal: int, provider: str, symbol: str) -> DatasetArtifactRef:
    return DatasetArtifactRef(
        ordinal=ordinal,
        provider=provider,
        dataset=f"{provider}-historical",
        source_ref=f"https://example.invalid/{provider}/{symbol}/{ordinal}",
        contract_version=f"{provider}-contract.v1",
        payload_sha256=_hash(f"artifact|{provider}|{symbol}|{ordinal}"),
        payload_size_bytes=128 + ordinal,
        hash_basis="raw-response-bytes-v1",
    )


def _parent_6108(
    *, methodology_version: str = "dataset-v1.1", symbol: str = "6108"
):
    eligibility = evaluate_supplemental(_candidate_6108(symbol=symbol))
    twse = tuple(
        _observation(
            offset,
            symbol=symbol,
            provider="twse",
            role=SourceRole.CANONICAL,
            run_id="twse-run-6108",
            selected=True,
        )
        for offset in range(228)
    )
    esun = tuple(
        _observation(
            offset,
            symbol=symbol,
            provider="esun",
            role=SourceRole.SUPPLEMENTAL,
            run_id="esun-run-6108",
            selected=True,
        )
        for offset in range(228, 250)
    )
    return build_provisional_dataset_version(
        symbol=symbol,
        as_of_date=BASE_DATE + timedelta(days=249),
        required_observation_count=250,
        twse_observations=twse,
        eligibility_result=eligibility,
        esun_observations=esun,
        artifacts=(_artifact(1, "twse", symbol), _artifact(2, "esun", symbol)),
        methodology_version=methodology_version,
    )


def _reconciliation_input(
    parent,
    offsets: tuple[int, ...],
    *,
    run_id: str = "twse-later-6108",
    discrepant_offsets: tuple[int, ...] = (),
) -> ReconciliationInput:
    rows = tuple(
        _observation(
            offset,
            symbol="6108",
            provider="twse-historical",
            role=SourceRole.CANONICAL,
            run_id=run_id,
            selected=True,
            volume_delta=7 if offset in discrepant_offsets else 0,
        )
        for offset in offsets
    )
    return ReconciliationInput(
        parent=parent,
        parent_dataset_version_id=parent.identity.dataset_version_id,
        parent_canonical_sha256=parent.canonical_sha256,
        twse_observations=rows,
        twse_source_run_id=run_id,
        twse_artifact=_artifact(99, "twse-historical", "6108"),
        target_dates=tuple(BASE_DATE + timedelta(days=offset) for offset in offsets),
    )


def _v12_database(tmp_path: Path, symbols: tuple[str, ...] = ("6108",)) -> Path:
    database_path = tmp_path / "reconciliation-v12.db"
    SQLiteResearchRepository(database_path).initialize()
    with sqlite3.connect(database_path) as connection:
        connection.executemany(
            "INSERT INTO symbols (symbol, name, market, currency, is_active) "
            "VALUES (?, ?, 'TSE', 'TWD', 1)",
            [(symbol, f"Company {symbol}") for symbol in symbols],
        )
    SQLiteScreenerMigrationRunner(database_path).migrate()
    DatasetVersionMigrationRunner(database_path).migrate(isolated=True)
    return database_path


def _stored_created_at(database_path: Path, dataset_id: str) -> str:
    with sqlite3.connect(database_path) as connection:
        return connection.execute(
            "SELECT created_at FROM research_dataset_versions "
            "WHERE dataset_version_id=?",
            (dataset_id,),
        ).fetchone()[0]


def test_equal_reconciliation_creates_new_child_and_preserves_parent() -> None:
    parent = _parent_6108()
    result = reconcile_dataset_version(
        _reconciliation_input(parent, tuple(range(228, 250)))
    )

    child = result.new_dataset_version
    assert result.result_kind == "equal"
    assert result.reconciliation_status.value == "reconciled_equal"
    assert result.equal_dates == tuple(BASE_DATE + timedelta(days=i) for i in range(228, 250))
    assert result.discrepant_dates == ()
    assert result.still_pending_dates == ()
    assert child.identity.source_status.value == "reconciled"
    assert child.identity.authority_status.value == "reconciled"
    assert child.identity.coverage.twse_observation_count == 250
    assert child.identity.coverage.esun_supplemental_count == 0
    assert child.identity.parent_dataset_version_id == parent.identity.dataset_version_id
    assert child.identity.source_policy == "twse_dual_source_v1"
    assert child.identity.dataset_version_id != parent.identity.dataset_version_id
    assert child.provenance_summary.validation_sources == ("esun",)
    assert parent.identity.coverage.esun_supplemental_count == 22
    assert all(
        not item.selected
        for item in child.observations
        if item.source_role is SourceRole.VALIDATION
    )


def test_discrepant_reconciliation_prefers_twse_and_retains_esun_validation() -> None:
    parent = _parent_6108()
    result = reconcile_dataset_version(
        _reconciliation_input(
            parent,
            tuple(range(228, 250)),
            discrepant_offsets=(233, 240),
        )
    )

    assert result.result_kind == "discrepant"
    assert result.reconciliation_status.value == "reconciled_discrepant"
    assert result.discrepancy_count == 2
    assert result.discrepant_dates == (
        BASE_DATE + timedelta(days=233),
        BASE_DATE + timedelta(days=240),
    )
    child = result.new_dataset_version
    assert child.identity.coverage.esun_supplemental_count == 0
    assert sum(item.selected for item in child.observations if item.source_role is SourceRole.CANONICAL) == 250
    assert sum(item.source_role is SourceRole.VALIDATION for item in child.observations) == 22


def test_partial_reconciliation_is_immutable_provisional_child() -> None:
    parent = _parent_6108()
    result = reconcile_dataset_version(
        _reconciliation_input(parent, tuple(range(228, 238)))
    )
    child = result.new_dataset_version

    assert result.result_kind == "partial"
    assert result.reconciliation_status.value == "pending"
    assert len(result.still_pending_dates) == 12
    assert child.identity.source_status.value == "provisional_mixed"
    assert child.identity.authority_status.value == "incomplete"
    assert child.identity.coverage.twse_observation_count == 238
    assert child.identity.coverage.esun_supplemental_count == 12
    assert child.identity.parent_dataset_version_id == parent.identity.dataset_version_id
    assert child.identity.dataset_version_id != parent.identity.dataset_version_id


def test_partial_then_final_reconciliation_chain_is_deterministic(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path)
    parent = _parent_6108()
    repository = DatasetReconciliationRepository(database_path)
    DatasetVersionRepository(database_path).save(parent)

    first_input = _reconciliation_input(parent, tuple(range(228, 238)))
    first = repository.reconcile_and_save(first_input)
    partial = first.reconciliation.new_dataset_version
    second_input = _reconciliation_input(
        partial,
        tuple(range(238, 250)),
        run_id="twse-later-6108-final",
    )
    second = repository.reconcile_and_save(second_input)
    final = second.reconciliation.new_dataset_version

    assert first.written is True
    assert second.written is True
    assert final.identity.source_status.value == "reconciled"
    assert final.identity.reconciliation_status.value == "reconciled_equal"
    assert final.identity.parent_dataset_version_id == partial.identity.dataset_version_id
    assert partial.identity.parent_dataset_version_id == parent.identity.dataset_version_id
    assert final.identity.dataset_version_id not in {
        parent.identity.dataset_version_id,
        partial.identity.dataset_version_id,
    }
    assert len(final.artifacts) == len(parent.artifacts) + 4


def test_cross_symbol_parent_lineage_fails_closed(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path, symbols=("6108", "3044"))
    parent = _parent_6108()
    different_symbol_parent = _parent_6108(symbol="3044")
    dataset_repository = DatasetVersionRepository(database_path)
    dataset_repository.save(parent)
    dataset_repository.save(different_symbol_parent)

    mismatched_parent = replace(
        parent,
        identity=replace(
            parent.identity,
            parent_dataset_version_id=different_symbol_parent.identity.dataset_version_id,
        ),
        canonical_sha256=None,
    )
    dataset_repository.save(mismatched_parent)

    with pytest.raises(ReconciliationPersistenceError, match="incompatible"):
        DatasetReconciliationRepository(database_path).reconcile_and_save(
            _reconciliation_input(mismatched_parent, (228,))
        )


def test_discrepant_reconciliation_replay_is_zero_write(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path)
    parent = _parent_6108()
    DatasetVersionRepository(database_path).save(parent)
    repository = DatasetReconciliationRepository(database_path)
    input_value = _reconciliation_input(
        parent,
        tuple(range(228, 250)),
        discrepant_offsets=(233, 240),
    )

    first = repository.reconcile_and_save(input_value)
    before = database_path.read_bytes()
    replay = repository.reconcile_and_save(input_value)

    assert first.reconciliation.result_kind == "discrepant"
    assert replay.written is False
    assert replay.child_dataset_version_id == first.child_dataset_version_id
    assert replay.reconciliation_evidence_sha256 == first.reconciliation_evidence_sha256
    assert database_path.read_bytes() == before


@pytest.mark.parametrize("offsets", [(228,), (228, 229, 230), tuple(range(228, 250))])
def test_partial_reconciliation_replay_is_zero_write(
    tmp_path: Path, offsets: tuple[int, ...]
) -> None:
    database_path = _v12_database(tmp_path)
    parent = _parent_6108()
    dataset_repository = DatasetVersionRepository(database_path)
    dataset_repository.save(parent)
    repository = DatasetReconciliationRepository(database_path)
    input_value = _reconciliation_input(parent, offsets)

    first = repository.reconcile_and_save(input_value)
    before = database_path.read_bytes()
    replay = repository.reconcile_and_save(input_value)

    assert replay.written is False
    assert replay.child_dataset_version_id == first.child_dataset_version_id
    assert replay.reconciliation_evidence_sha256 == first.reconciliation_evidence_sha256
    assert replay.created_at == first.created_at
    assert database_path.read_bytes() == before


def test_parent_fingerprint_and_created_at_are_unchanged_after_child_persist(
    tmp_path: Path,
) -> None:
    database_path = _v12_database(tmp_path)
    parent = _parent_6108()
    dataset_repository = DatasetVersionRepository(database_path)
    dataset_repository.save(parent)
    before = dataset_repository.replay(parent.identity.dataset_version_id)
    before_created_at = _stored_created_at(database_path, parent.identity.dataset_version_id)

    DatasetReconciliationRepository(database_path).reconcile_and_save(
        _reconciliation_input(parent, tuple(range(228, 250)))
    )
    after = dataset_repository.replay(parent.identity.dataset_version_id)

    assert after.as_dict() == before.as_dict()
    assert _stored_created_at(database_path, parent.identity.dataset_version_id) == before_created_at


def test_reconciliation_evidence_is_deterministic_and_excludes_runtime_values() -> None:
    parent = _parent_6108()
    first = reconcile_dataset_version(_reconciliation_input(parent, (228, 229)))
    second = reconcile_dataset_version(_reconciliation_input(parent, (228, 229)))

    assert first.reconciliation_evidence_sha256 == second.reconciliation_evidence_sha256
    assert first.canonical_evidence_json() == second.canonical_evidence_json()
    assert "created_at" not in first.canonical_evidence_json()
    assert "timestamp" not in first.canonical_evidence_json()
    assert "temp_path" not in first.canonical_evidence_json()


def test_only_parent_supplemental_dates_are_in_scope() -> None:
    parent = _parent_6108()
    with pytest.raises(ReconciliationContractError, match="target_dates"):
        _reconciliation_input(parent, (227,))


def test_new_twse_validation_rejects_esun_future_duplicate_and_symbol_mismatch() -> None:
    parent = _parent_6108()
    base = _reconciliation_input(parent, (228,))
    with pytest.raises(ReconciliationContractError, match="twse_observations"):
        ReconciliationInput(
            parent=parent,
            parent_dataset_version_id=parent.identity.dataset_version_id,
            parent_canonical_sha256=parent.canonical_sha256,
            twse_observations=(
                _observation(
                    228,
                    symbol="6108",
                    provider="esun",
                    role=SourceRole.SUPPLEMENTAL,
                    run_id="twse-later-6108",
                    selected=True,
                ),
            ),
            twse_source_run_id=base.twse_source_run_id,
            twse_artifact=base.twse_artifact,
            target_dates=(BASE_DATE + timedelta(days=228),),
        )
    with pytest.raises(ReconciliationContractError, match="twse_artifact.provider"):
        ReconciliationInput(
            parent=parent,
            parent_dataset_version_id=parent.identity.dataset_version_id,
            parent_canonical_sha256=parent.canonical_sha256,
            twse_observations=base.twse_observations,
            twse_source_run_id=base.twse_source_run_id,
            twse_artifact=_artifact(99, "twse", "6108"),
            target_dates=(BASE_DATE + timedelta(days=228),),
        )
    with pytest.raises(ReconciliationContractError, match="source_ref"):
        ReconciliationInput(
            parent=parent,
            parent_dataset_version_id=parent.identity.dataset_version_id,
            parent_canonical_sha256=parent.canonical_sha256,
            twse_observations=base.twse_observations,
            twse_source_run_id=base.twse_source_run_id,
            twse_artifact=replace(
                base.twse_artifact,
                source_ref=(
                    f"reconciliation://{parent.identity.dataset_version_id}/child"
                ),
            ),
            target_dates=(BASE_DATE + timedelta(days=228),),
        )
    duplicate = _observation(
        228,
        symbol="6108",
        provider="twse-historical",
        role=SourceRole.CANONICAL,
        run_id="twse-later-6108",
        selected=True,
    )
    with pytest.raises(ReconciliationContractError, match="at most one"):
        ReconciliationInput(
            parent=parent,
            parent_dataset_version_id=parent.identity.dataset_version_id,
            parent_canonical_sha256=parent.canonical_sha256,
            twse_observations=(base.twse_observations[0], duplicate),
            twse_source_run_id=base.twse_source_run_id,
            twse_artifact=base.twse_artifact,
            target_dates=(BASE_DATE + timedelta(days=228),),
        )
    with pytest.raises(ReconciliationContractError, match="twse_observations"):
        ReconciliationInput(
            parent=parent,
            parent_dataset_version_id=parent.identity.dataset_version_id,
            parent_canonical_sha256=parent.canonical_sha256,
            twse_observations=(
                _observation(
                    228,
                    symbol="3044",
                    provider="twse-historical",
                    role=SourceRole.CANONICAL,
                    run_id="twse-later-6108",
                    selected=True,
                ),
            ),
            twse_source_run_id=base.twse_source_run_id,
            twse_artifact=base.twse_artifact,
            target_dates=(BASE_DATE + timedelta(days=228),),
        )


@pytest.mark.parametrize("symbol", ["4590", "6589", "7740"])
def test_identity_mismatch_symbols_have_no_legal_reconciliation_parent(symbol: str) -> None:
    result = evaluate_supplemental(
        SupplementalCandidateInput(
            requested_symbol=symbol,
            target_date=BASE_DATE,
            requested_start_date=BASE_DATE,
            missing_twse_dates=(BASE_DATE,),
            twse_failure_class=FailureClassification.PACING_BLOCKED,
            source_run_id=f"esun-run-{symbol}",
            artifact_sha256=_hash(f"artifact-{symbol}"),
            esun_requested_identity=_identity(symbol),
            esun_returned_identity=_identity("2330"),
            esun_observations=(_eligibility_row(0, symbol),),
            security_boundary=SecurityBoundaryEvidence.passed(),
        )
    )
    assert result.eligible is False
    with pytest.raises(DatasetPersistenceContractError):
        build_provisional_dataset_version(
            symbol=symbol,
            as_of_date=BASE_DATE,
            required_observation_count=1,
            twse_observations=(),
            eligibility_result=result,
            esun_observations=(),
            artifacts=(_artifact(1, "esun", symbol),),
            methodology_version="dataset-v1.1",
        )


def test_3044_canonical_complete_validation_discrepancy_is_not_reconcilable() -> None:
    twse = tuple(
        _observation(
            offset,
            symbol="3044",
            provider="twse",
            role=SourceRole.CANONICAL,
            run_id="twse-run-3044",
            selected=True,
        )
        for offset in range(250)
    )
    validation = (
        _observation(
            249,
            symbol="3044",
            provider="esun",
            role=SourceRole.VALIDATION,
            run_id="esun-validation-3044",
            selected=False,
            volume_delta=7,
        ),
    )
    parent = build_canonical_dataset_version(
        symbol="3044",
        as_of_date=BASE_DATE + timedelta(days=249),
        required_observation_count=250,
        twse_observations=twse,
        validation_observations=validation,
        artifacts=(_artifact(1, "twse", "3044"), _artifact(2, "esun", "3044")),
        methodology_version="dataset-v1.1",
        discrepancy_count=1,
    )
    with pytest.raises(ReconciliationContractError, match="source_status"):
        _reconciliation_input(parent, (249,))


def test_legal_short_reconciliation_uses_actual_scope_not_250() -> None:
    candidate = SupplementalCandidateInput(
        requested_symbol="6589",
        target_date=BASE_DATE + timedelta(days=1),
        requested_start_date=BASE_DATE,
        missing_twse_dates=(BASE_DATE + timedelta(days=1),),
        twse_failure_class=FailureClassification.TIMEOUT,
        source_run_id="esun-run-6589",
        artifact_sha256=_hash("esun-artifact-6589"),
        twse_observations=(_eligibility_row(0, "6589"),),
        esun_requested_identity=_identity("6589"),
        esun_returned_identity=_identity("6589"),
        esun_observations=(_eligibility_row(1, "6589"),),
        security_boundary=SecurityBoundaryEvidence.passed(),
    )
    eligibility = evaluate_supplemental(candidate)
    parent = build_provisional_dataset_version(
        symbol="6589",
        as_of_date=BASE_DATE + timedelta(days=1),
        required_observation_count=2,
        twse_observations=(
            _observation(
                0,
                symbol="6589",
                provider="twse",
                role=SourceRole.CANONICAL,
                run_id="twse-run-6589",
                selected=True,
            ),
        ),
        eligibility_result=eligibility,
        esun_observations=(
            _observation(
                1,
                symbol="6589",
                provider="esun",
                role=SourceRole.SUPPLEMENTAL,
                run_id="esun-run-6589",
                selected=True,
            ),
        ),
        artifacts=(_artifact(1, "twse", "6589"), _artifact(2, "esun", "6589")),
        methodology_version="dataset-v1.1",
        coverage_basis=CoverageBasis.LEGAL_SHORT_LISTING_HISTORY,
    )
    input_value = ReconciliationInput(
        parent=parent,
        parent_dataset_version_id=parent.identity.dataset_version_id,
        parent_canonical_sha256=parent.canonical_sha256,
        twse_observations=(
            _observation(
                1,
                symbol="6589",
                provider="twse-historical",
                role=SourceRole.CANONICAL,
                run_id="twse-later-6589",
                selected=True,
            ),
        ),
        twse_source_run_id="twse-later-6589",
        twse_artifact=_artifact(99, "twse-historical", "6589"),
        target_dates=(BASE_DATE + timedelta(days=1),),
    )
    result = reconcile_dataset_version(input_value)
    assert result.new_dataset_version.coverage_basis is CoverageBasis.LEGAL_SHORT_LISTING_HISTORY
    assert result.new_dataset_version.identity.coverage.required_observation_count == 2
    assert result.new_dataset_version.identity.coverage.twse_observation_count == 2


def test_relation_artifact_is_hash_only_and_replay_detects_relation_tamper(
    tmp_path: Path,
) -> None:
    database_path = _v12_database(tmp_path)
    parent = _parent_6108()
    dataset_repository = DatasetVersionRepository(database_path)
    dataset_repository.save(parent)
    repository = DatasetReconciliationRepository(database_path)
    saved = repository.reconcile_and_save(
        _reconciliation_input(parent, tuple(range(228, 250)))
    )
    child_id = saved.child_dataset_version_id
    with sqlite3.connect(database_path) as connection:
        relation = connection.execute(
            "SELECT ordinal, provider, dataset, source_ref, payload_sha256 "
            "FROM research_dataset_artifacts WHERE dataset_version_id=? "
            "AND dataset=?",
            (child_id, RECONCILIATION_RELATION_DATASET),
        ).fetchone()
        assert relation is not None
        connection.execute(
            "UPDATE research_dataset_artifacts SET payload_sha256=? "
            "WHERE dataset_version_id=? AND dataset=?",
            ("f" * 64, child_id, RECONCILIATION_RELATION_DATASET),
        )
    with pytest.raises(ReconciliationPersistenceError):
        repository.replay(child_id)


@pytest.mark.parametrize(
    "source_ref",
    [
        "https://example.invalid/data?access_token=redacted",
        "https://example.invalid/data?X-Amz-Signature=redacted",
    ],
)
def test_artifact_refs_reject_credential_like_locator(source_ref: str) -> None:
    with pytest.raises(DatasetPersistenceContractError, match="source_ref"):
        DatasetArtifactRef(
            ordinal=1,
            provider="twse",
            dataset="twse-historical",
            source_ref=source_ref,
            contract_version="twse-contract.v1",
            payload_sha256=_hash("payload"),
            payload_size_bytes=1,
            hash_basis="raw-response-bytes-v1",
        )


def test_self_parent_lineage_is_rejected_before_persistence(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path)
    parent = _parent_6108()
    DatasetVersionRepository(database_path).save(parent)
    repository = DatasetReconciliationRepository(database_path)
    child_id = repository.reconcile_and_save(
        _reconciliation_input(parent, tuple(range(228, 250)))
    ).child_dataset_version_id

    with sqlite3.connect(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="parent_dataset_version_id"):
            connection.execute(
                "UPDATE research_dataset_versions SET parent_dataset_version_id=? "
                "WHERE dataset_version_id=?",
                (child_id, child_id),
            )


@pytest.mark.parametrize("field", ["parent_hash", "child_hash", "twse_observation", "esun_observation", "source_run", "artifact_sha", "counts", "lineage"])
def test_tamper_vectors_fail_closed_without_repair(tmp_path: Path, field: str) -> None:
    database_path = _v12_database(tmp_path)
    parent = _parent_6108()
    dataset_repository = DatasetVersionRepository(database_path)
    dataset_repository.save(parent)
    repository = DatasetReconciliationRepository(database_path)
    saved = repository.reconcile_and_save(
        _reconciliation_input(parent, tuple(range(228, 250)))
    )
    child_id = saved.child_dataset_version_id
    parent_id = parent.identity.dataset_version_id
    before_counts = tuple(
        sqlite3.connect(database_path).execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        for table in (
            "research_dataset_versions",
            "research_dataset_observations",
            "research_dataset_artifacts",
        )
    )
    with sqlite3.connect(database_path) as connection:
        if field == "parent_hash":
            connection.execute(
                "UPDATE research_dataset_versions SET canonical_sha256=? WHERE dataset_version_id=?",
                ("a" * 64, parent_id),
            )
        elif field == "child_hash":
            connection.execute(
                "UPDATE research_dataset_versions SET canonical_sha256=? WHERE dataset_version_id=?",
                ("b" * 64, child_id),
            )
        elif field == "twse_observation":
            connection.execute(
                "UPDATE research_dataset_observations SET open_price=101 WHERE dataset_version_id=? AND provider='twse-historical'",
                (child_id,),
            )
        elif field == "esun_observation":
            connection.execute(
                "UPDATE research_dataset_observations SET volume=999999 WHERE dataset_version_id=? AND source_role='validation'",
                (child_id,),
            )
        elif field == "source_run":
            connection.execute(
                "UPDATE research_dataset_observations SET source_run_id='tampered-run' WHERE dataset_version_id=? AND provider='twse-historical'",
                (child_id,),
            )
        elif field == "artifact_sha":
            connection.execute(
                "UPDATE research_dataset_artifacts SET payload_sha256=? WHERE dataset_version_id=? AND dataset=?",
                ("c" * 64, child_id, RECONCILIATION_RELATION_DATASET),
            )
        elif field == "counts":
            connection.execute(
                "UPDATE research_dataset_versions SET discrepancy_count=1 WHERE dataset_version_id=?",
                (child_id,),
            )
        elif field == "lineage":
            connection.execute(
                "UPDATE research_dataset_versions SET parent_dataset_version_id=? WHERE dataset_version_id=?",
                ("d" * 64, child_id),
            )
    with pytest.raises((ReconciliationPersistenceError, sqlite3.IntegrityError)):
        repository.replay(child_id)
    after_counts = tuple(
        sqlite3.connect(database_path).execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        for table in (
            "research_dataset_versions",
            "research_dataset_observations",
            "research_dataset_artifacts",
        )
    )
    assert after_counts == before_counts


def test_reconciliation_does_not_create_or_update_s4_rows(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path)
    parent = _parent_6108()
    dataset_repository = DatasetVersionRepository(database_path)
    dataset_repository.save(parent)
    with sqlite3.connect(database_path) as connection:
        before = connection.execute("SELECT COUNT(*) FROM screener_runs").fetchone()[0]
    DatasetReconciliationRepository(database_path).reconcile_and_save(
        _reconciliation_input(parent, (228,))
    )
    with sqlite3.connect(database_path) as connection:
        after = connection.execute("SELECT COUNT(*) FROM screener_runs").fetchone()[0]
    assert after == before


def test_no_migration_beyond_0012_and_production_db_is_not_a_reconciliation_target() -> None:
    migration_files = sorted(
        path.name
        for path in Path("app/storage/migrations").glob("*.sql")
    )
    assert not any(name.startswith("0013_") for name in migration_files)
    production = Path("data/research.db").resolve()
    with pytest.raises(DatasetMigrationStateError, match="frozen production database"):
        DatasetVersionMigrationRunner(production).migrate(isolated=True)
