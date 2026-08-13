from __future__ import annotations

import ast
import hashlib
from dataclasses import FrozenInstanceError
from datetime import date, timedelta
from pathlib import Path

import pytest

from app.data_contracts.dual_source import (
    DUAL_SOURCE_DATASET_CONTRACT_VERSION,
    FailureClassification,
)
from app.data_contracts.supplemental_eligibility import (
    SUPPLEMENTAL_ELIGIBILITY_CONTRACT_VERSION,
    EligibilityReason,
    IdentityVerificationStatus,
    InstrumentIdentity,
    OHLCVObservation,
    OverlapValidationStatus,
    PayloadStructuralStatus,
    SecurityBoundaryEvidence,
    SecurityBoundaryStatus,
    SupplementalCandidateInput,
    SupplementalEligibilityResult,
    TwseFailureEvidence,
    evaluate_supplemental,
)


BASE_DATE = date(2026, 1, 1)
SYMBOL = "6108"
_UNSET = object()


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identity(
    symbol: str = SYMBOL,
    *,
    market: str = "TSE",
    exchange: str = "TWSE",
    currency: str = "TWD",
    security_type: str = "EQUITY",
) -> InstrumentIdentity:
    return InstrumentIdentity(
        symbol=symbol,
        market=market,
        exchange=exchange,
        currency=currency,
        security_type=security_type,
    )


def _row(
    offset: int,
    *,
    symbol: str | None = SYMBOL,
    open_price: object = 100.0,
    high: object = 105.0,
    low: object = 95.0,
    close: object = 102.0,
    volume: object = 1000,
    trade_date: object | None = None,
) -> OHLCVObservation:
    return OHLCVObservation(
        trade_date=BASE_DATE + timedelta(days=offset)
        if trade_date is None
        else trade_date,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        symbol=symbol,
    )


def _candidate(
    *,
    missing: tuple[date, ...] = (BASE_DATE,),
    failure: FailureClassification | None = FailureClassification.PACING_BLOCKED,
    twse: tuple[OHLCVObservation, ...] = (),
    esun: tuple[OHLCVObservation, ...] = (),
    requested_identity: InstrumentIdentity | None | object = _UNSET,
    returned_identity: InstrumentIdentity | None | object = _UNSET,
    security: SecurityBoundaryEvidence | SecurityBoundaryStatus = SecurityBoundaryEvidence.passed(),
    failure_evidence: TwseFailureEvidence | None = TwseFailureEvidence(
        endpoint_identity_verified=True,
        classification_basis="official-boundary-classification",
    ),
    symbol: str = SYMBOL,
    start_date: date = BASE_DATE,
    target_date: date = BASE_DATE,
    source_run_id: str = "esun-run-1",
) -> SupplementalCandidateInput:
    return SupplementalCandidateInput(
        requested_symbol=symbol,
        target_date=target_date,
        requested_start_date=start_date,
        missing_twse_dates=missing,
        twse_failure_class=failure,
        source_run_id=source_run_id,
        artifact_sha256=_hash(source_run_id),
        twse_observations=twse,
        esun_requested_identity=(
            _identity(symbol) if requested_identity is _UNSET else requested_identity
        ),
        esun_returned_identity=(
            _identity(symbol) if returned_identity is _UNSET else returned_identity
        ),
        esun_observations=esun,
        security_boundary=security,
        twse_failure_evidence=failure_evidence,
    )


def _valid_single_candidate(**kwargs: object) -> SupplementalCandidateInput:
    return _candidate(
        missing=(BASE_DATE,),
        esun=(_row(0),),
        **kwargs,
    )


def _valid_6108_candidate(esun_count: int = 22) -> SupplementalCandidateInput:
    missing = tuple(BASE_DATE + timedelta(days=offset) for offset in range(228, 250))
    twse = tuple(_row(offset) for offset in range(228))
    esun = tuple(_row(offset) for offset in range(228, 228 + esun_count))
    return _candidate(
        missing=missing,
        twse=twse,
        esun=esun,
        target_date=BASE_DATE + timedelta(days=249),
    )


@pytest.mark.parametrize(
    "failure",
    [
        FailureClassification.PACING_BLOCKED,
        FailureClassification.TIMEOUT,
        FailureClassification.DELAYED,
        FailureClassification.TEMPORARY,
    ],
)
def test_eligible_twse_failure_classes_can_pass(
    failure: FailureClassification,
) -> None:
    result = evaluate_supplemental(
        _valid_single_candidate(failure=failure)
    )
    assert result.eligible is True
    assert result.source_eligible is True
    assert result.reason is EligibilityReason.ELIGIBLE_VERIFIED_SUPPLEMENTAL
    assert result.coverage_complete is True


@pytest.mark.parametrize(
    "failure",
    [
        FailureClassification.MALFORMED,
        FailureClassification.PERMANENT,
        FailureClassification.IDENTITY_MISMATCH,
        FailureClassification.UNAVAILABLE,
    ],
)
def test_forbidden_twse_failure_classes_fail_closed(
    failure: FailureClassification,
) -> None:
    result = evaluate_supplemental(_valid_single_candidate(failure=failure))
    assert result.eligible is False
    assert result.reason is EligibilityReason.TWSE_FAILURE_NOT_ELIGIBLE
    assert result.eligible_observation_dates == ()


def test_missing_failure_class_is_not_inferred_from_missing_data() -> None:
    result = evaluate_supplemental(_valid_single_candidate(failure=None))
    assert result.eligible is False
    assert result.reason is EligibilityReason.TWSE_FAILURE_NOT_ELIGIBLE


def test_untrusted_twse_failure_evidence_is_unknown_fail_closed() -> None:
    candidate = _valid_single_candidate(
        failure_evidence=TwseFailureEvidence(
            endpoint_identity_verified=False,
            classification_basis="unsafe-redirect",
        )
    )
    result = evaluate_supplemental(candidate)
    assert result.eligible is False
    assert result.reason is EligibilityReason.UNKNOWN_FAIL_CLOSED


@pytest.mark.parametrize(
    ("symbol", "returned_symbol"),
    [("4590", "2330"), ("6589", "2330"), ("7740", "2330")],
)
def test_identity_mismatch_fixtures_are_hard_rejected(
    symbol: str, returned_symbol: str
) -> None:
    candidate = _valid_single_candidate(
        symbol=symbol,
        requested_identity=_identity(symbol),
        returned_identity=_identity(returned_symbol),
    )
    result = evaluate_supplemental(candidate)
    assert result.eligible is False
    assert result.reason is EligibilityReason.IDENTITY_MISMATCH
    assert result.esun_identity_status is IdentityVerificationStatus.SYMBOL_MISMATCH
    assert result.eligible_observation_dates == ()


def test_unavailable_instrument_is_not_identity_verified() -> None:
    candidate = _valid_single_candidate(returned_identity=None)
    result = evaluate_supplemental(candidate)
    assert result.eligible is False
    assert result.reason is EligibilityReason.INSTRUMENT_UNAVAILABLE
    assert result.esun_identity_status is IdentityVerificationStatus.UNAVAILABLE


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("market", "OTC", EligibilityReason.MARKET_MISMATCH),
        ("currency", "USD", EligibilityReason.CURRENCY_MISMATCH),
        ("security_type", "ETF", EligibilityReason.SECURITY_TYPE_MISMATCH),
    ],
)
def test_identity_dimensions_are_checked(
    field: str, value: str, reason: EligibilityReason
) -> None:
    values = {"market": "TSE", "exchange": "TWSE", "currency": "TWD", "security_type": "EQUITY"}
    values[field] = value
    candidate = _valid_single_candidate(returned_identity=_identity(**values))
    result = evaluate_supplemental(candidate)
    assert result.eligible is False
    assert result.reason is reason


def test_security_boundary_failure_blocks_eligibility() -> None:
    result = evaluate_supplemental(
        _valid_single_candidate(security=SecurityBoundaryEvidence())
    )
    assert result.eligible is False
    assert result.reason is EligibilityReason.SECURITY_BOUNDARY_FAILED
    assert result.security_boundary_status is SecurityBoundaryStatus.FAILED


def test_malformed_esun_payload_rejects_entire_source_run() -> None:
    result = evaluate_supplemental(
        _candidate(esun=(_row(0, open_price="not-a-number"),))
    )
    assert result.eligible is False
    assert result.reason is EligibilityReason.MALFORMED_PAYLOAD
    assert result.structural_validation_status is PayloadStructuralStatus.MALFORMED_PAYLOAD
    assert result.eligible_observation_dates == ()


def test_partial_ohlc_rejects_entire_source_run() -> None:
    result = evaluate_supplemental(
        _candidate(esun=(_row(0, high=None),))
    )
    assert result.eligible is False
    assert result.reason is EligibilityReason.PARTIAL_OHLC
    assert result.structural_validation_status is PayloadStructuralStatus.PARTIAL_OHLC


def test_duplicate_trade_date_rejects_entire_source_run() -> None:
    result = evaluate_supplemental(
        _candidate(esun=(_row(0), _row(0)))
    )
    assert result.eligible is False
    assert result.reason is EligibilityReason.DUPLICATE_TRADE_DATE
    assert result.rejected_observation_dates == (BASE_DATE,)


def test_future_trade_date_rejects_entire_source_run() -> None:
    result = evaluate_supplemental(
        _candidate(esun=(_row(1),))
    )
    assert result.eligible is False
    assert result.reason is EligibilityReason.FUTURE_TRADE_DATE


def test_invalid_ohlc_relationship_rejects_payload() -> None:
    result = evaluate_supplemental(
        _candidate(esun=(_row(0, high=90.0),))
    )
    assert result.eligible is False
    assert result.reason is EligibilityReason.MALFORMED_PAYLOAD


def test_exact_overlap_is_validation_only_and_not_selected() -> None:
    result = evaluate_supplemental(
        _candidate(
            missing=(BASE_DATE + timedelta(days=1),),
            twse=(_row(0),),
            esun=(_row(0), _row(1)),
            target_date=BASE_DATE + timedelta(days=1),
        )
    )
    assert result.eligible is True
    assert result.reason is EligibilityReason.ELIGIBLE_VERIFIED_SUPPLEMENTAL
    assert result.overlap_status is OverlapValidationStatus.EXACT_MATCH
    assert result.eligible_observation_dates == (BASE_DATE + timedelta(days=1),)
    assert result.rejected_observation_dates == (BASE_DATE,)


def test_overlap_discrepancy_rejects_entire_source_run() -> None:
    twse = tuple(_row(offset) for offset in range(249))
    esun = tuple(
        _row(offset, volume=9999 if offset == 100 else 1000)
        for offset in range(250)
    )
    result = evaluate_supplemental(
        _candidate(
            missing=(BASE_DATE + timedelta(days=249),),
            twse=twse,
            esun=esun,
            target_date=BASE_DATE + timedelta(days=249),
        )
    )
    assert result.eligible is False
    assert result.reason is EligibilityReason.OVERLAP_DISCREPANCY
    assert result.overlap_status is OverlapValidationStatus.DISCREPANCY
    assert result.eligible_observation_dates == ()
    assert len(result.rejected_observation_dates) == 250


def test_unexpected_extra_esun_date_is_fail_closed() -> None:
    result = evaluate_supplemental(
        _candidate(
            missing=(BASE_DATE,),
            esun=(_row(0), _row(1)),
            target_date=BASE_DATE + timedelta(days=1),
        )
    )
    assert result.eligible is False
    assert result.reason is EligibilityReason.UNEXPECTED_OBSERVATION_DATE


def test_6108_full_228_plus_22_is_eligible_without_mixed_dataset() -> None:
    candidate = _valid_6108_candidate(22)
    result = evaluate_supplemental(candidate)
    assert result.eligible is True
    assert result.source_eligible is True
    assert result.reason is EligibilityReason.ELIGIBLE_VERIFIED_SUPPLEMENTAL
    assert len(result.missing_twse_dates) == 22
    assert len(result.eligible_observation_dates) == 22
    assert result.remaining_uncovered_dates == ()
    assert result.coverage_complete is True
    assert not hasattr(result, "mixed_dataset")


def test_6108_partial_18_of_22_is_source_eligible_but_not_complete() -> None:
    result = evaluate_supplemental(_valid_6108_candidate(18))
    assert result.eligible is True
    assert result.source_eligible is True
    assert result.reason is EligibilityReason.INSUFFICIENT_SUPPLEMENTAL_COVERAGE
    assert len(result.eligible_observation_dates) == 18
    assert len(result.remaining_uncovered_dates) == 4
    assert result.coverage_complete is False


def test_known_good_complete_twse_means_supplemental_not_needed() -> None:
    candidate = _candidate(
        missing=(),
        failure=None,
        esun=(),
        requested_identity=None,
        returned_identity=None,
        security=SecurityBoundaryStatus.NOT_CHECKED,
    )
    result = evaluate_supplemental(candidate)
    assert result.eligible is False
    assert result.reason is EligibilityReason.NOT_NEEDED
    assert result.esun_identity_status is IdentityVerificationStatus.NOT_CHECKED
    assert result.security_boundary_status is SecurityBoundaryStatus.NOT_CHECKED
    assert result.coverage_complete is True


def test_deterministic_evidence_hash_excludes_runtime_fields() -> None:
    first = _valid_single_candidate()
    second = _valid_single_candidate()
    first_result = evaluate_supplemental(first)
    second_result = evaluate_supplemental(second)
    assert first_result.eligibility_evidence_sha256 == second_result.eligibility_evidence_sha256
    assert "timestamp" not in first.as_dict()
    assert "retry_count" not in first.as_dict()
    assert "pid" not in first.as_dict()
    changed = evaluate_supplemental(_valid_single_candidate(source_run_id="other-run"))
    assert changed.eligibility_evidence_sha256 != first_result.eligibility_evidence_sha256


def test_candidate_and_result_are_immutable_and_serializable() -> None:
    candidate = _valid_single_candidate()
    result = evaluate_supplemental(candidate)
    with pytest.raises(FrozenInstanceError):
        candidate.requested_symbol = "2330"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        result.eligible = False  # type: ignore[misc]
    assert result.as_dict()["contract_version"] == SUPPLEMENTAL_ELIGIBILITY_CONTRACT_VERSION
    assert candidate.as_dict()["ds1_contract_version"] == DUAL_SOURCE_DATASET_CONTRACT_VERSION


def test_ds1_failure_vocabulary_is_reused_without_changing_its_values() -> None:
    assert FailureClassification.PACING_BLOCKED.value == "pacing_blocked"
    assert FailureClassification.IDENTITY_MISMATCH.value == "identity_mismatch"


def test_security_boundary_evidence_requires_all_invariants_for_pass() -> None:
    evidence = SecurityBoundaryEvidence.passed()
    assert evidence.status is SecurityBoundaryStatus.PASS
    assert evidence.failed_invariants == ()
    failed = SecurityBoundaryEvidence(
        redirect_authority_verified=True,
        credential_forwarding_protected=True,
        https_verified=True,
        approved_endpoint_verified=True,
        response_byte_cap_verified=False,
        total_deadline_verified=True,
        bounded_retry_after_verified=True,
        bounded_attempts_verified=True,
    )
    assert failed.status is SecurityBoundaryStatus.FAILED
    assert failed.failed_invariants == ("response_byte_cap_verified",)


def test_pure_evaluator_has_no_network_sqlite_or_production_imports() -> None:
    module_path = (
        Path(__file__).parents[1]
        / "app"
        / "data_contracts"
        / "supplemental_eligibility.py"
    )
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    forbidden = (
        "sqlite3",
        "requests",
        "httpx",
        "urllib",
        "socket",
        "app.providers",
        "app.storage",
        "app.scheduler",
        "app.reporting",
        "app.config",
    )
    assert not any(
        name == root or name.startswith(root + ".")
        for name in imported
        for root in forbidden
    )
