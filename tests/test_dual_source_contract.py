from __future__ import annotations

import ast
import hashlib
from dataclasses import FrozenInstanceError
from datetime import date, timedelta
from pathlib import Path

import pytest

from app.data_contracts.dual_source import (
    DUAL_SOURCE_DATASET_CONTRACT_VERSION,
    DUAL_SOURCE_SECURITY_INVARIANTS,
    AuthorityStatus,
    DatasetCoverage,
    DatasetProvenanceSummary,
    DatasetSourceStatus,
    DatasetVersionIdentity,
    DualSourceContractError,
    FailureClassification,
    ObservationProvenance,
    ReconciliationStatus,
    ResearchDataQuality,
    SourceIdentityValidation,
    SourceRole,
    SupplementalEligibilityReason,
    TWSE_BASELINE_SOURCE_POLICY,
    TWSE_CANONICAL_AUTHORITY,
    TWSE_DUAL_SOURCE_POLICY,
    canonical_provenance_json,
    canonicalize_provenance,
    is_supplemental_eligible_failure,
    is_supplemental_forbidden_failure,
    legacy_v1_dataset_identity,
    legacy_v1_provenance_summary,
    provenance_map_sha256,
    validate_dataset_provenance,
)


BASE_DATE = date(2025, 1, 2)


def _hash(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _observation(
    offset: int,
    *,
    provider: str,
    role: SourceRole,
    selected: bool,
    run_id: str,
    symbol: str,
    label: str,
) -> ObservationProvenance:
    return ObservationProvenance(
        trade_date=BASE_DATE + timedelta(days=offset),
        provider=provider,
        source_role=role,
        source_run_id=run_id,
        selected=selected,
        observation_sha256=_hash(label),
        symbol=symbol,
    )


def _canonical_complete_summary(
    *, required: int = 1, discrepancy_count: int = 0, validation_sources: tuple[str, ...] = ()
) -> DatasetProvenanceSummary:
    observations = [
        _observation(
            offset,
            provider="twse",
            role=SourceRole.CANONICAL,
            selected=True,
            run_id="twse-canonical-run",
            symbol="2330",
            label=f"twse-{offset}",
        )
        for offset in range(required)
    ]
    observations.extend(
        _observation(
            offset,
            provider=provider,
            role=SourceRole.VALIDATION,
            selected=False,
            run_id="validation-run",
            symbol="2330",
            label=f"validation-{provider}-{offset}",
        )
        for provider in validation_sources
        for offset in range(required)
    )
    coverage = DatasetCoverage(
        required_observation_count=required,
        twse_observation_count=required,
        esun_supplemental_count=0,
        missing_twse_count=0,
        discrepancy_count=discrepancy_count,
    )
    return DatasetProvenanceSummary.from_observations(
        observations,
        source_status=DatasetSourceStatus.CANONICAL_COMPLETE,
        authority_status=AuthorityStatus.COMPLETE,
        reconciliation_status=ReconciliationStatus.NOT_APPLICABLE,
        coverage=coverage,
        symbol="2330",
    )


def _provisional_6108() -> tuple[list[ObservationProvenance], DatasetProvenanceSummary]:
    observations = [
        _observation(
            offset,
            provider="twse",
            role=SourceRole.CANONICAL,
            selected=True,
            run_id="twse-6108-run",
            symbol="6108",
            label=f"6108-twse-{offset}",
        )
        for offset in range(228)
    ]
    observations.extend(
        _observation(
            offset,
            provider="esun",
            role=SourceRole.SUPPLEMENTAL,
            selected=True,
            run_id="esun-6108-run",
            symbol="6108",
            label=f"6108-esun-{offset}",
        )
        for offset in range(228, 250)
    )
    coverage = DatasetCoverage(
        required_observation_count=250,
        twse_observation_count=228,
        esun_supplemental_count=22,
        missing_twse_count=22,
        discrepancy_count=0,
    )
    summary = DatasetProvenanceSummary.from_observations(
        observations,
        source_status=DatasetSourceStatus.PROVISIONAL_MIXED,
        authority_status=AuthorityStatus.INCOMPLETE,
        reconciliation_status=ReconciliationStatus.PENDING,
        coverage=coverage,
        symbol="6108",
    )
    return observations, summary


def _reconciled_6108(
    provisional: DatasetProvenanceSummary,
) -> tuple[
    list[ObservationProvenance],
    DatasetProvenanceSummary,
    DatasetVersionIdentity,
    DatasetVersionIdentity,
]:
    provisional_identity = DatasetVersionIdentity.from_summary(
        provisional,
        symbol="6108",
        as_of_date=BASE_DATE + timedelta(days=249),
        methodology_version="research-v1",
        source_policy=TWSE_DUAL_SOURCE_POLICY,
    )
    observations = [
        _observation(
            offset,
            provider="twse",
            role=SourceRole.CANONICAL,
            selected=True,
            run_id="twse-6108-reconciled-run",
            symbol="6108",
            label=f"6108-reconciled-twse-{offset}",
        )
        for offset in range(250)
    ]
    observations.extend(
        _observation(
            offset,
            provider="esun",
            role=SourceRole.VALIDATION,
            selected=False,
            run_id="esun-6108-validation-run",
            symbol="6108",
            label=f"6108-reconciled-validation-{offset}",
        )
        for offset in range(22)
    )
    coverage = DatasetCoverage(
        required_observation_count=250,
        twse_observation_count=250,
        esun_supplemental_count=0,
        missing_twse_count=0,
        discrepancy_count=0,
        latest_reconciled_date=BASE_DATE + timedelta(days=249),
    )
    summary = DatasetProvenanceSummary.from_observations(
        observations,
        source_status=DatasetSourceStatus.RECONCILED,
        authority_status=AuthorityStatus.RECONCILED,
        reconciliation_status=ReconciliationStatus.RECONCILED_EQUAL,
        coverage=coverage,
        symbol="6108",
    )
    identity = DatasetVersionIdentity.from_summary(
        summary,
        symbol="6108",
        as_of_date=BASE_DATE + timedelta(days=249),
        methodology_version="research-v1",
        source_policy=TWSE_DUAL_SOURCE_POLICY,
        parent_dataset_version_id=provisional_identity.dataset_version_id,
    )
    return observations, summary, identity, provisional_identity


def test_contract_version_enums_and_security_vocabulary_are_fixed() -> None:
    assert DUAL_SOURCE_DATASET_CONTRACT_VERSION == "dual-source-dataset-contract.v1"
    assert [item.value for item in SourceRole] == [
        "canonical",
        "validation",
        "supplemental",
    ]
    assert [item.value for item in DatasetSourceStatus] == [
        "canonical_complete",
        "provisional_mixed",
        "reconciled",
    ]
    assert [item.value for item in AuthorityStatus] == [
        "complete",
        "incomplete",
        "reconciled",
    ]
    assert [item.value for item in ReconciliationStatus] == [
        "not_applicable",
        "pending",
        "reconciled_equal",
        "reconciled_discrepant",
    ]
    assert {item.value for item in ResearchDataQuality} == {
        "canonical",
        "provisional",
        "reconciled",
    }
    assert DUAL_SOURCE_SECURITY_INVARIANTS == (
        "redirect_authority_verification",
        "credential_forwarding_protection",
        "response_byte_cap",
        "total_deadline",
        "bounded_retry_after",
        "identity_validation",
    )


def test_valid_canonical_complete_state() -> None:
    summary = _canonical_complete_summary()
    assert summary.source_status is DatasetSourceStatus.CANONICAL_COMPLETE
    assert summary.authority_status is AuthorityStatus.COMPLETE
    assert summary.reconciliation_status is ReconciliationStatus.NOT_APPLICABLE
    assert summary.canonical_authority == TWSE_CANONICAL_AUTHORITY
    assert summary.coverage.selected_observation_count == 1
    assert summary.coverage.coverage_complete is True


def test_6108_provisional_mixed_preserves_22_esun_trade_dates() -> None:
    observations, summary = _provisional_6108()
    esun_dates = {
        item.trade_date
        for item in observations
        if item.source_role is SourceRole.SUPPLEMENTAL
    }
    assert len(esun_dates) == 22
    assert summary.source_status is DatasetSourceStatus.PROVISIONAL_MIXED
    assert summary.authority_status is AuthorityStatus.INCOMPLETE
    assert summary.reconciliation_status is ReconciliationStatus.PENDING
    assert summary.coverage.twse_observation_count == 228
    assert summary.coverage.esun_supplemental_count == 22
    assert summary.coverage.missing_twse_count == 22
    assert summary.coverage.coverage_complete is True
    validate_dataset_provenance(summary, observations, symbol="6108")


def test_6108_reconciled_is_new_version_and_keeps_old_provisional_unchanged() -> None:
    provisional_observations, provisional = _provisional_6108()
    old_hash = provisional.provenance_map_sha256
    old_state = (
        provisional.source_status,
        provisional.authority_status,
        provisional.reconciliation_status,
        provisional.coverage,
    )
    observations, reconciled, identity, provisional_identity = _reconciled_6108(provisional)

    assert reconciled.source_status is DatasetSourceStatus.RECONCILED
    assert reconciled.authority_status is AuthorityStatus.RECONCILED
    assert reconciled.reconciliation_status is ReconciliationStatus.RECONCILED_EQUAL
    assert reconciled.coverage.twse_observation_count == 250
    assert reconciled.coverage.esun_supplemental_count == 0
    assert reconciled.validation_sources == ("esun",)
    assert identity.parent_dataset_version_id == provisional_identity.dataset_version_id
    assert identity.dataset_version_id != provisional_identity.dataset_version_id
    assert provisional.provenance_map_sha256 == old_hash
    assert (
        provisional.source_status,
        provisional.authority_status,
        provisional.reconciliation_status,
        provisional.coverage,
    ) == old_state
    validate_dataset_provenance(reconciled, observations, symbol="6108")


def test_valid_reconciled_discrepant_state_allows_validation_evidence() -> None:
    twse = _observation(
        0,
        provider="twse",
        role=SourceRole.CANONICAL,
        selected=True,
        run_id="twse-3044-run",
        symbol="3044",
        label="3044-twse-volume-7170943",
    )
    esun = _observation(
        0,
        provider="esun",
        role=SourceRole.VALIDATION,
        selected=False,
        run_id="esun-3044-run",
        symbol="3044",
        label="3044-esun-volume-6989943",
    )
    observations = [twse, esun]
    summary = DatasetProvenanceSummary.from_observations(
        observations,
        source_status=DatasetSourceStatus.RECONCILED,
        authority_status=AuthorityStatus.RECONCILED,
        reconciliation_status=ReconciliationStatus.RECONCILED_DISCREPANT,
        coverage=DatasetCoverage(1, 1, 0, 0, 1),
        symbol="3044",
    )
    assert summary.coverage.discrepancy_count == 1
    assert summary.coverage.esun_supplemental_count == 0
    assert all(not item.selected for item in [esun])
    validate_dataset_provenance(summary, observations, symbol="3044")


def test_3044_canonical_complete_can_have_validation_discrepancy() -> None:
    twse = _observation(
        0,
        provider="twse",
        role=SourceRole.CANONICAL,
        selected=True,
        run_id="twse-3044-run",
        symbol="3044",
        label="3044-twse-volume-7170943",
    )
    esun = _observation(
        0,
        provider="esun",
        role=SourceRole.VALIDATION,
        selected=False,
        run_id="esun-3044-run",
        symbol="3044",
        label="3044-esun-volume-6989943",
    )
    summary = DatasetProvenanceSummary.from_observations(
        [twse, esun],
        source_status=DatasetSourceStatus.CANONICAL_COMPLETE,
        authority_status=AuthorityStatus.COMPLETE,
        reconciliation_status=ReconciliationStatus.NOT_APPLICABLE,
        coverage=DatasetCoverage(1, 1, 0, 0, 1),
        symbol="3044",
    )
    assert summary.source_status is DatasetSourceStatus.CANONICAL_COMPLETE
    assert summary.coverage.discrepancy_count > 0
    assert summary.coverage.esun_supplemental_count == 0
    assert summary.validation_sources == ("esun",)


@pytest.mark.parametrize(
    ("provider", "role"),
    [
        ("esun", SourceRole.CANONICAL),
        ("esun-historical", SourceRole.CANONICAL),
        ("twse", SourceRole.SUPPLEMENTAL),
        ("twse-historical", SourceRole.SUPPLEMENTAL),
    ],
)
def test_cross_authority_source_roles_are_rejected(
    provider: str, role: SourceRole
) -> None:
    with pytest.raises(DualSourceContractError):
        _observation(
            0,
            provider=provider,
            role=role,
            selected=True,
            run_id="invalid-run",
            symbol="2330",
            label="invalid",
        )


def test_validation_rows_are_not_selected() -> None:
    with pytest.raises(DualSourceContractError):
        _observation(
            0,
            provider="esun",
            role=SourceRole.VALIDATION,
            selected=True,
            run_id="invalid-validation-run",
            symbol="2330",
            label="invalid-validation",
        )


def test_illegal_state_matrix_is_rejected() -> None:
    with pytest.raises(DualSourceContractError):
        DatasetProvenanceSummary(
            "twse",
            ("esun",),
            (),
            DatasetSourceStatus.CANONICAL_COMPLETE,
            AuthorityStatus.COMPLETE,
            ReconciliationStatus.NOT_APPLICABLE,
            DatasetCoverage(2, 1, 1, 1, 0),
            _hash("canonical-with-supplemental"),
        )

    with pytest.raises(DualSourceContractError):
        DatasetProvenanceSummary(
            "twse",
            (),
            (),
            DatasetSourceStatus.PROVISIONAL_MIXED,
            AuthorityStatus.INCOMPLETE,
            ReconciliationStatus.PENDING,
            DatasetCoverage(1, 1, 0, 0, 0),
            _hash("provisional-without-supplemental"),
        )

    with pytest.raises(DualSourceContractError):
        DatasetProvenanceSummary(
            "twse",
            ("esun",),
            (),
            DatasetSourceStatus.PROVISIONAL_MIXED,
            AuthorityStatus.COMPLETE,
            ReconciliationStatus.PENDING,
            DatasetCoverage(2, 1, 1, 1, 0),
            _hash("provisional-complete-authority"),
        )

    with pytest.raises(DualSourceContractError):
        DatasetProvenanceSummary(
            "twse",
            ("esun",),
            (),
            DatasetSourceStatus.PROVISIONAL_MIXED,
            AuthorityStatus.INCOMPLETE,
            ReconciliationStatus.RECONCILED_EQUAL,
            DatasetCoverage(2, 1, 1, 1, 0),
            _hash("provisional-not-pending"),
        )

    with pytest.raises(DualSourceContractError):
        DatasetProvenanceSummary(
            "twse",
            (),
            (),
            DatasetSourceStatus.RECONCILED,
            AuthorityStatus.RECONCILED,
            ReconciliationStatus.PENDING,
            DatasetCoverage(1, 1, 0, 0, 0),
            _hash("reconciled-pending"),
        )


def test_coverage_rejects_invalid_counts_and_supports_legal_short() -> None:
    with pytest.raises(DualSourceContractError):
        DatasetCoverage(0, 0, 0, 0, 0)
    with pytest.raises(DualSourceContractError):
        DatasetCoverage(2, 2, 1, 0, 0)
    with pytest.raises(DualSourceContractError):
        DatasetCoverage(2, 1, 0, 0, 0)
    with pytest.raises(DualSourceContractError):
        DatasetCoverage(2, 1, 0, 1, 0, selected_observation_count=2)

    legal_short = DatasetCoverage(3, 2, 0, 1, 0)
    assert legal_short.coverage_complete is False
    assert legal_short.selected_observation_count == 2


def test_provenance_ordering_hash_and_json_are_insertion_order_independent() -> None:
    first = _observation(
        1,
        provider="esun",
        role=SourceRole.VALIDATION,
        selected=False,
        run_id="esun-run",
        symbol="2330",
        label="esun-1",
    )
    second = _observation(
        0,
        provider="twse",
        role=SourceRole.CANONICAL,
        selected=True,
        run_id="twse-run",
        symbol="2330",
        label="twse-0",
    )
    third = _observation(
        1,
        provider="twse",
        role=SourceRole.CANONICAL,
        selected=True,
        run_id="twse-run",
        symbol="2330",
        label="twse-1",
    )
    ordered = canonicalize_provenance([first, second, third])
    reversed_order = canonicalize_provenance([third, first, second])
    assert ordered == reversed_order
    assert [item.provider for item in ordered] == ["twse", "twse", "esun"]
    assert provenance_map_sha256([first, second, third]) == provenance_map_sha256(
        [third, first, second]
    )
    assert canonical_provenance_json([first, second, third]) == canonical_provenance_json(
        [third, first, second]
    )


def test_duplicate_selected_trade_date_is_rejected() -> None:
    first = _observation(
        0,
        provider="twse",
        role=SourceRole.CANONICAL,
        selected=True,
        run_id="twse-run-a",
        symbol="2330",
        label="first",
    )
    second = _observation(
        0,
        provider="twse",
        role=SourceRole.CANONICAL,
        selected=True,
        run_id="twse-run-b",
        symbol="2330",
        label="second",
    )
    with pytest.raises(DualSourceContractError):
        canonicalize_provenance([first, second])


def test_dataset_version_identity_is_deterministic() -> None:
    summary = _canonical_complete_summary()
    first = DatasetVersionIdentity.from_summary(
        summary,
        symbol="2330",
        as_of_date=BASE_DATE,
        methodology_version="research-v1",
        source_policy=TWSE_BASELINE_SOURCE_POLICY,
    )
    second = DatasetVersionIdentity.from_summary(
        summary,
        symbol="2330",
        as_of_date=BASE_DATE,
        methodology_version="research-v1",
        source_policy=TWSE_BASELINE_SOURCE_POLICY,
    )
    assert first.dataset_version_id == second.dataset_version_id
    assert first.canonical_json() == second.canonical_json()
    assert first.as_dict()["dataset_version_id"] == first.dataset_version_id


def test_reconciled_identity_requires_parent_and_initial_versions_reject_parent() -> None:
    summary = DatasetProvenanceSummary(
        "twse",
        (),
        (),
        DatasetSourceStatus.RECONCILED,
        AuthorityStatus.RECONCILED,
        ReconciliationStatus.RECONCILED_EQUAL,
        DatasetCoverage(1, 1, 0, 0, 0),
        _hash("reconciled-summary"),
    )
    with pytest.raises(DualSourceContractError):
        DatasetVersionIdentity.from_summary(
            summary,
            symbol="2330",
            as_of_date=BASE_DATE,
            methodology_version="research-v1",
            source_policy=TWSE_DUAL_SOURCE_POLICY,
        )

    provisional = _canonical_complete_summary()
    candidate = DatasetVersionIdentity.from_summary(
        provisional,
        symbol="2330",
        as_of_date=BASE_DATE,
        methodology_version="research-v1",
        source_policy=TWSE_BASELINE_SOURCE_POLICY,
    )
    with pytest.raises(DualSourceContractError):
        DatasetVersionIdentity(
            symbol="2330",
            as_of_date=BASE_DATE,
            methodology_version="research-v1",
            source_policy=TWSE_BASELINE_SOURCE_POLICY,
            provenance_map_sha256=summary.provenance_map_sha256,
            source_status=DatasetSourceStatus.CANONICAL_COMPLETE,
            authority_status=AuthorityStatus.COMPLETE,
            reconciliation_status=ReconciliationStatus.NOT_APPLICABLE,
            coverage=summary.coverage,
            parent_dataset_version_id=candidate.dataset_version_id,
        )


@pytest.mark.parametrize("symbol", ["4590", "6589", "7740"])
def test_identity_mismatch_is_hard_reject_without_symbol_mapping(symbol: str) -> None:
    result = SourceIdentityValidation(requested_symbol=symbol, returned_symbol="2330")
    assert result.is_match is False
    assert result.supplemental_eligible is False
    assert result.reason is SupplementalEligibilityReason.REJECTED_IDENTITY_MISMATCH
    assert result.eligibility_reason is SupplementalEligibilityReason.REJECTED_IDENTITY_MISMATCH
    assert result.requested_symbol == symbol
    assert result.returned_symbol == "2330"


def test_matching_identity_is_verified() -> None:
    result = SourceIdentityValidation(requested_symbol="2330", returned_symbol="2330")
    assert result.is_match is True
    assert result.supplemental_eligible is True
    assert result.reason is None
    assert result.eligibility_reason is SupplementalEligibilityReason.VERIFIED


def test_failure_classification_eligibility_boundary() -> None:
    assert set(FailureClassification.supplemental_eligible()) == {
        FailureClassification.TEMPORARY,
        FailureClassification.DELAYED,
        FailureClassification.PACING_BLOCKED,
        FailureClassification.TIMEOUT,
    }
    assert set(FailureClassification.supplemental_forbidden()) == {
        FailureClassification.MALFORMED,
        FailureClassification.PERMANENT,
        FailureClassification.IDENTITY_MISMATCH,
        FailureClassification.UNAVAILABLE,
    }
    for value in FailureClassification:
        assert is_supplemental_eligible_failure(value) != is_supplemental_forbidden_failure(value)


def test_legacy_v1_mapping_is_pure_and_keeps_twse_baseline_semantics() -> None:
    summary = legacy_v1_provenance_summary(
        250, discrepancy_count=1, validation_sources=("esun",)
    )
    assert summary.source_status is DatasetSourceStatus.CANONICAL_COMPLETE
    assert summary.authority_status is AuthorityStatus.COMPLETE
    assert summary.reconciliation_status is ReconciliationStatus.NOT_APPLICABLE
    assert summary.coverage.twse_observation_count == 250
    assert summary.coverage.esun_supplemental_count == 0
    assert summary.validation_sources == ("esun",)
    identity = legacy_v1_dataset_identity(
        symbol="2330",
        as_of_date=BASE_DATE,
        methodology_version="research-v1",
        required_observation_count=250,
        discrepancy_count=1,
        validation_sources=("esun",),
    )
    assert identity.source_policy == TWSE_BASELINE_SOURCE_POLICY
    assert identity.parent_dataset_version_id is None


def test_models_are_immutable() -> None:
    coverage = DatasetCoverage(1, 1, 0, 0, 0)
    with pytest.raises(FrozenInstanceError):
        coverage.twse_observation_count = 2  # type: ignore[misc]


def test_dual_source_module_has_no_production_or_network_imports() -> None:
    module_path = Path(__file__).parents[1] / "app" / "data_contracts" / "dual_source.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    forbidden_roots = (
        "app.providers",
        "app.storage",
        "app.operations",
        "app.reporting",
        "app.config",
        "sqlite3",
        "requests",
        "httpx",
        "urllib",
        "socket",
    )
    assert not any(
        name == root or name.startswith(root + ".")
        for name in imported
        for root in forbidden_roots
    )
