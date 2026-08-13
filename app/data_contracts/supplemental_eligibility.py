"""DS2 pure E.SUN supplemental eligibility contract.

The provider layer is responsible for acquisition and transport hardening.  This
module consumes only normalized values and an already classified TWSE failure;
it never performs network I/O, reads SQLite, consults a scheduler, or mutates a
mixed dataset.  DS3 owns persistence and dataset composition.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
import math
import re
from typing import Any, Iterable

from .dual_source import (
    DUAL_SOURCE_DATASET_CONTRACT_VERSION,
    FailureClassification,
    canonical_json,
    is_supplemental_eligible_failure,
    sha256_text,
)


SUPPLEMENTAL_ELIGIBILITY_CONTRACT_VERSION = "supplemental-eligibility-contract.v1"
_SYMBOL_RE = re.compile(r"^[0-9A-Z]{2,12}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class SupplementalEligibilityContractError(ValueError):
    """Raised when normalized DS2 input or output violates its contract."""


class EligibilityReason(str, Enum):
    ELIGIBLE_VERIFIED_SUPPLEMENTAL = "eligible_verified_supplemental"
    TWSE_FAILURE_NOT_ELIGIBLE = "twse_failure_not_eligible"
    IDENTITY_MISMATCH = "identity_mismatch"
    INSTRUMENT_UNAVAILABLE = "instrument_unavailable"
    MARKET_MISMATCH = "market_mismatch"
    CURRENCY_MISMATCH = "currency_mismatch"
    SECURITY_TYPE_MISMATCH = "security_type_mismatch"
    MALFORMED_PAYLOAD = "malformed_payload"
    PARTIAL_OHLC = "partial_ohlc"
    DUPLICATE_TRADE_DATE = "duplicate_trade_date"
    FUTURE_TRADE_DATE = "future_trade_date"
    OVERLAP_DISCREPANCY = "overlap_discrepancy"
    SECURITY_BOUNDARY_FAILED = "security_boundary_failed"
    INSUFFICIENT_SUPPLEMENTAL_COVERAGE = "insufficient_supplemental_coverage"
    UNKNOWN_FAIL_CLOSED = "unknown_fail_closed"
    NOT_NEEDED = "not_needed"
    UNEXPECTED_OBSERVATION_DATE = "unexpected_observation_date"


class IdentityVerificationStatus(str, Enum):
    NOT_CHECKED = "not_checked"
    VERIFIED = "verified"
    UNAVAILABLE = "unavailable"
    SYMBOL_MISMATCH = "symbol_mismatch"
    MARKET_MISMATCH = "market_mismatch"
    EXCHANGE_MISMATCH = "exchange_mismatch"
    CURRENCY_MISMATCH = "currency_mismatch"
    SECURITY_TYPE_MISMATCH = "security_type_mismatch"


class OverlapValidationStatus(str, Enum):
    NOT_APPLICABLE = "not_applicable"
    EXACT_MATCH = "exact_match"
    DISCREPANCY = "discrepancy"


class PayloadStructuralStatus(str, Enum):
    NOT_CHECKED = "not_checked"
    PASS = "pass"
    MALFORMED_PAYLOAD = "malformed_payload"
    PARTIAL_OHLC = "partial_ohlc"
    DUPLICATE_TRADE_DATE = "duplicate_trade_date"
    FUTURE_TRADE_DATE = "future_trade_date"
    UNEXPECTED_OBSERVATION_DATE = "unexpected_observation_date"


class SecurityBoundaryStatus(str, Enum):
    NOT_CHECKED = "not_checked"
    PASS = "pass"
    FAILED = "failed"


# Short aliases make the status vocabulary convenient without changing its
# serialized values.
ReasonCode = EligibilityReason
IdentityStatus = IdentityVerificationStatus
OverlapStatus = OverlapValidationStatus
StructuralValidationStatus = PayloadStructuralStatus


def _error(field_name: str, message: str) -> SupplementalEligibilityContractError:
    return SupplementalEligibilityContractError(f"{field_name}: {message}")


def _non_blank(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _error(field_name, "must be a non-blank string")
    return value.strip()


def _normalize_symbol(value: Any, field_name: str) -> str:
    normalized = _non_blank(value, field_name).upper()
    if _SYMBOL_RE.fullmatch(normalized) is None:
        raise _error(field_name, "must contain 2-12 ASCII letters or digits")
    return normalized


def _normalize_date(value: Any, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise _error(field_name, "must be a date")
    return value


def _normalize_sha256(value: Any, field_name: str) -> str:
    normalized = _non_blank(value, field_name)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise _error(field_name, "must be 64 lowercase hexadecimal characters")
    return normalized


def _coerce_enum(enum_type: type[Enum], value: Any, field_name: str) -> Any:
    if isinstance(value, enum_type):
        return value
    if not isinstance(value, str):
        raise _error(field_name, "must be a fixed enum value")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise _error(field_name, "contains an unsupported enum value") from exc


def _bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise _error(field_name, "must be boolean")
    return value


def _optional_bool(value: Any, field_name: str) -> bool | None:
    if value is not None and not isinstance(value, bool):
        raise _error(field_name, "must be boolean or null")
    return value


def _stable_value(value: Any) -> Any:
    """Convert normalized input values to deterministic JSON-safe values."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if value is None or isinstance(value, (str, bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            return {"non_finite_float": str(value)}
        return value
    if isinstance(value, (tuple, list)):
        return [_stable_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _stable_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    return {"unsupported_type": f"{type(value).__module__}.{type(value).__qualname__}"}


@dataclass(frozen=True, slots=True)
class InstrumentIdentity:
    """Requested or returned E.SUN instrument identity."""

    symbol: str
    market: str
    exchange: str
    currency: str
    security_type: str

    def __post_init__(self) -> None:
        symbol = _normalize_symbol(self.symbol, "symbol")
        values = {
            field_name: _non_blank(getattr(self, field_name), field_name).upper()
            for field_name in ("market", "exchange", "currency", "security_type")
        }
        object.__setattr__(self, "symbol", symbol)
        for field_name, value in values.items():
            object.__setattr__(self, field_name, value)

    def as_dict(self) -> dict[str, str]:
        return {
            "symbol": self.symbol,
            "market": self.market,
            "exchange": self.exchange,
            "currency": self.currency,
            "security_type": self.security_type,
        }


@dataclass(frozen=True, slots=True)
class OHLCVObservation:
    """Raw normalized-shape row; evaluator performs the structural checks."""

    trade_date: Any
    open: Any
    high: Any
    low: Any
    close: Any
    volume: Any
    symbol: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "trade_date": _stable_value(self.trade_date),
            "open": _stable_value(self.open),
            "high": _stable_value(self.high),
            "low": _stable_value(self.low),
            "close": _stable_value(self.close),
            "volume": _stable_value(self.volume),
            "symbol": _stable_value(self.symbol),
        }


@dataclass(frozen=True, slots=True)
class TwseFailureEvidence:
    """Already-classified TWSE failure evidence consumed by DS2."""

    endpoint_identity_verified: bool
    classification_basis: str = ""
    safe_location_verified: bool | None = None
    bounded_pacing_exhausted: bool | None = None
    publication_incomplete: bool | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "endpoint_identity_verified",
            _bool(self.endpoint_identity_verified, "endpoint_identity_verified"),
        )
        object.__setattr__(
            self,
            "classification_basis",
            "" if self.classification_basis is None else _non_blank(self.classification_basis, "classification_basis"),
        )
        object.__setattr__(
            self,
            "safe_location_verified",
            _optional_bool(self.safe_location_verified, "safe_location_verified"),
        )
        object.__setattr__(
            self,
            "bounded_pacing_exhausted",
            _optional_bool(self.bounded_pacing_exhausted, "bounded_pacing_exhausted"),
        )
        object.__setattr__(
            self,
            "publication_incomplete",
            _optional_bool(self.publication_incomplete, "publication_incomplete"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "endpoint_identity_verified": self.endpoint_identity_verified,
            "classification_basis": self.classification_basis,
            "safe_location_verified": self.safe_location_verified,
            "bounded_pacing_exhausted": self.bounded_pacing_exhausted,
            "publication_incomplete": self.publication_incomplete,
        }


@dataclass(frozen=True, slots=True)
class SecurityBoundaryEvidence:
    """Result of the hardened provider boundary, with no transport logic here."""

    redirect_authority_verified: bool = False
    credential_forwarding_protected: bool = False
    https_verified: bool = False
    approved_endpoint_verified: bool = False
    response_byte_cap_verified: bool = False
    total_deadline_verified: bool = False
    bounded_retry_after_verified: bool = False
    bounded_attempts_verified: bool = False

    def __post_init__(self) -> None:
        for field_name in (
            "redirect_authority_verified",
            "credential_forwarding_protected",
            "https_verified",
            "approved_endpoint_verified",
            "response_byte_cap_verified",
            "total_deadline_verified",
            "bounded_retry_after_verified",
            "bounded_attempts_verified",
        ):
            object.__setattr__(self, field_name, _bool(getattr(self, field_name), field_name))

    @classmethod
    def passed(cls) -> "SecurityBoundaryEvidence":
        return cls(
            redirect_authority_verified=True,
            credential_forwarding_protected=True,
            https_verified=True,
            approved_endpoint_verified=True,
            response_byte_cap_verified=True,
            total_deadline_verified=True,
            bounded_retry_after_verified=True,
            bounded_attempts_verified=True,
        )

    @property
    def status(self) -> SecurityBoundaryStatus:
        return (
            SecurityBoundaryStatus.PASS
            if not self.failed_invariants
            else SecurityBoundaryStatus.FAILED
        )

    @property
    def failed_invariants(self) -> tuple[str, ...]:
        return tuple(
            field_name
            for field_name in (
                "redirect_authority_verified",
                "credential_forwarding_protected",
                "https_verified",
                "approved_endpoint_verified",
                "response_byte_cap_verified",
                "total_deadline_verified",
                "bounded_retry_after_verified",
                "bounded_attempts_verified",
            )
            if not getattr(self, field_name)
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "redirect_authority_verified": self.redirect_authority_verified,
            "credential_forwarding_protected": self.credential_forwarding_protected,
            "https_verified": self.https_verified,
            "approved_endpoint_verified": self.approved_endpoint_verified,
            "response_byte_cap_verified": self.response_byte_cap_verified,
            "total_deadline_verified": self.total_deadline_verified,
            "bounded_retry_after_verified": self.bounded_retry_after_verified,
            "bounded_attempts_verified": self.bounded_attempts_verified,
        }


@dataclass(frozen=True, slots=True)
class SupplementalCandidateInput:
    """Pure normalized candidate input consumed by :func:`evaluate_supplemental`."""

    requested_symbol: str
    target_date: date
    requested_start_date: date
    missing_twse_dates: tuple[date, ...]
    twse_failure_class: FailureClassification | None
    source_run_id: str
    artifact_sha256: str
    requested_end_date: date | None = None
    twse_observations: tuple[OHLCVObservation, ...] = ()
    esun_requested_identity: InstrumentIdentity | None = None
    esun_returned_identity: InstrumentIdentity | None = None
    esun_observations: tuple[OHLCVObservation, ...] = ()
    security_boundary: SecurityBoundaryEvidence | SecurityBoundaryStatus = (
        SecurityBoundaryStatus.NOT_CHECKED
    )
    twse_failure_evidence: TwseFailureEvidence | None = None

    def __post_init__(self) -> None:
        requested_symbol = _normalize_symbol(self.requested_symbol, "requested_symbol")
        target_date = _normalize_date(self.target_date, "target_date")
        requested_start_date = _normalize_date(
            self.requested_start_date, "requested_start_date"
        )
        requested_end_date = (
            target_date
            if self.requested_end_date is None
            else _normalize_date(self.requested_end_date, "requested_end_date")
        )
        if requested_start_date > requested_end_date:
            raise _error("requested_date_range", "start must not be after end")
        if not requested_start_date <= target_date <= requested_end_date:
            raise _error("target_date", "must be inside requested date range")

        missing_dates = tuple(
            _normalize_date(value, "missing_twse_dates")
            for value in self.missing_twse_dates
        )
        if len(set(missing_dates)) != len(missing_dates):
            raise _error("missing_twse_dates", "must not contain duplicates")
        if any(
            value < requested_start_date or value > target_date for value in missing_dates
        ):
            raise _error("missing_twse_dates", "must be inside the requested range")
        missing_dates = tuple(sorted(missing_dates))

        failure_class = self.twse_failure_class
        if failure_class is not None:
            failure_class = _coerce_enum(
                FailureClassification, failure_class, "twse_failure_class"
            )
        source_run_id = _non_blank(self.source_run_id, "source_run_id")
        artifact_sha256 = _normalize_sha256(self.artifact_sha256, "artifact_sha256")

        for field_name in ("twse_observations", "esun_observations"):
            values = getattr(self, field_name)
            if isinstance(values, (str, bytes)):
                raise _error(field_name, "must be an iterable of OHLCVObservation values")
            normalized_values = tuple(values)
            if any(not isinstance(item, OHLCVObservation) for item in normalized_values):
                raise _error(field_name, "must contain only OHLCVObservation values")
            object.__setattr__(self, field_name, normalized_values)

        if self.esun_requested_identity is not None and not isinstance(
            self.esun_requested_identity, InstrumentIdentity
        ):
            raise _error("esun_requested_identity", "must be an InstrumentIdentity")
        if self.esun_returned_identity is not None and not isinstance(
            self.esun_returned_identity, InstrumentIdentity
        ):
            raise _error("esun_returned_identity", "must be an InstrumentIdentity")
        if (
            self.esun_requested_identity is not None
            and self.esun_requested_identity.symbol != requested_symbol
        ):
            raise _error(
                "esun_requested_identity", "symbol must match requested_symbol"
            )

        security_boundary = self.security_boundary
        if not isinstance(security_boundary, (SecurityBoundaryEvidence, SecurityBoundaryStatus)):
            security_boundary = _coerce_enum(
                SecurityBoundaryStatus, security_boundary, "security_boundary"
            )
        if self.twse_failure_evidence is not None and not isinstance(
            self.twse_failure_evidence, TwseFailureEvidence
        ):
            raise _error("twse_failure_evidence", "must be TwseFailureEvidence or null")

        object.__setattr__(self, "requested_symbol", requested_symbol)
        object.__setattr__(self, "target_date", target_date)
        object.__setattr__(self, "requested_start_date", requested_start_date)
        object.__setattr__(self, "requested_end_date", requested_end_date)
        object.__setattr__(self, "missing_twse_dates", missing_dates)
        object.__setattr__(self, "twse_failure_class", failure_class)
        object.__setattr__(self, "source_run_id", source_run_id)
        object.__setattr__(self, "artifact_sha256", artifact_sha256)
        object.__setattr__(self, "security_boundary", security_boundary)

    def as_dict(self) -> dict[str, Any]:
        security_boundary: Any
        if isinstance(self.security_boundary, SecurityBoundaryEvidence):
            security_boundary = self.security_boundary.as_dict()
        else:
            security_boundary = self.security_boundary.value
        return {
            "contract_version": SUPPLEMENTAL_ELIGIBILITY_CONTRACT_VERSION,
            "ds1_contract_version": DUAL_SOURCE_DATASET_CONTRACT_VERSION,
            "requested_symbol": self.requested_symbol,
            "target_date": self.target_date.isoformat(),
            "requested_start_date": self.requested_start_date.isoformat(),
            "requested_end_date": self.requested_end_date.isoformat(),
            "missing_twse_dates": [item.isoformat() for item in self.missing_twse_dates],
            "twse_failure_class": (
                self.twse_failure_class.value
                if self.twse_failure_class is not None
                else None
            ),
            "twse_observations": [item.as_dict() for item in self.twse_observations],
            "esun_requested_identity": (
                self.esun_requested_identity.as_dict()
                if self.esun_requested_identity is not None
                else None
            ),
            "esun_returned_identity": (
                self.esun_returned_identity.as_dict()
                if self.esun_returned_identity is not None
                else None
            ),
            "esun_observations": [item.as_dict() for item in self.esun_observations],
            "security_boundary": security_boundary,
            "twse_failure_evidence": (
                self.twse_failure_evidence.as_dict()
                if self.twse_failure_evidence is not None
                else None
            ),
            "source_run_id": self.source_run_id,
            "artifact_sha256": self.artifact_sha256,
        }


@dataclass(frozen=True, slots=True)
class SupplementalEligibilityResult:
    """Deterministic DS2 decision; it contains no mixed dataset."""

    eligible: bool
    source_eligible: bool
    reason: EligibilityReason
    twse_failure_class: FailureClassification | None
    esun_identity_status: IdentityVerificationStatus
    overlap_status: OverlapValidationStatus
    structural_validation_status: PayloadStructuralStatus
    security_boundary_status: SecurityBoundaryStatus
    eligible_observation_dates: tuple[date, ...]
    rejected_observation_dates: tuple[date, ...]
    missing_twse_dates: tuple[date, ...]
    remaining_uncovered_dates: tuple[date, ...]
    coverage_complete: bool
    eligibility_evidence_sha256: str

    def __post_init__(self) -> None:
        for field_name in ("eligible", "source_eligible", "coverage_complete"):
            object.__setattr__(self, field_name, _bool(getattr(self, field_name), field_name))
        object.__setattr__(
            self,
            "reason",
            _coerce_enum(EligibilityReason, self.reason, "reason"),
        )
        if self.twse_failure_class is not None:
            object.__setattr__(
                self,
                "twse_failure_class",
                _coerce_enum(
                    FailureClassification,
                    self.twse_failure_class,
                    "twse_failure_class",
                ),
            )
        for field_name, enum_type in (
            ("esun_identity_status", IdentityVerificationStatus),
            ("overlap_status", OverlapValidationStatus),
            ("structural_validation_status", PayloadStructuralStatus),
            ("security_boundary_status", SecurityBoundaryStatus),
        ):
            object.__setattr__(
                self,
                field_name,
                _coerce_enum(enum_type, getattr(self, field_name), field_name),
            )
        for field_name in (
            "eligible_observation_dates",
            "rejected_observation_dates",
            "missing_twse_dates",
            "remaining_uncovered_dates",
        ):
            values = tuple(
                _normalize_date(item, field_name) for item in getattr(self, field_name)
            )
            if len(set(values)) != len(values):
                raise _error(field_name, "must not contain duplicate dates")
            object.__setattr__(self, field_name, tuple(sorted(values)))
        object.__setattr__(
            self,
            "eligibility_evidence_sha256",
            _normalize_sha256(
                self.eligibility_evidence_sha256, "eligibility_evidence_sha256"
            ),
        )
        if self.eligible != self.source_eligible:
            raise _error("eligible", "must equal source_eligible in DS2")
        if self.coverage_complete != (not self.remaining_uncovered_dates):
            raise _error(
                "coverage_complete", "must equal the absence of remaining uncovered dates"
            )

    @property
    def identity_status(self) -> IdentityVerificationStatus:
        return self.esun_identity_status

    @property
    def failure_class(self) -> FailureClassification | None:
        return self.twse_failure_class

    @property
    def reason_code(self) -> EligibilityReason:
        return self.reason

    @property
    def structural_status(self) -> PayloadStructuralStatus:
        return self.structural_validation_status

    @property
    def security_status(self) -> SecurityBoundaryStatus:
        return self.security_boundary_status

    @property
    def eligible_dates(self) -> tuple[date, ...]:
        return self.eligible_observation_dates

    @property
    def rejected_dates(self) -> tuple[date, ...]:
        return self.rejected_observation_dates

    @property
    def missing_twse_dates_count(self) -> int:
        return len(self.missing_twse_dates)

    @property
    def eligible_esun_dates_count(self) -> int:
        return len(self.eligible_observation_dates)

    @property
    def remaining_uncovered_dates_count(self) -> int:
        return len(self.remaining_uncovered_dates)

    @property
    def evidence_hash(self) -> str:
        return self.eligibility_evidence_sha256

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": SUPPLEMENTAL_ELIGIBILITY_CONTRACT_VERSION,
            "eligible": self.eligible,
            "source_eligible": self.source_eligible,
            "reason": self.reason.value,
            "twse_failure_class": (
                self.twse_failure_class.value
                if self.twse_failure_class is not None
                else None
            ),
            "esun_identity_status": self.esun_identity_status.value,
            "overlap_status": self.overlap_status.value,
            "structural_validation_status": self.structural_validation_status.value,
            "security_boundary_status": self.security_boundary_status.value,
            "eligible_observation_dates": [
                item.isoformat() for item in self.eligible_observation_dates
            ],
            "rejected_observation_dates": [
                item.isoformat() for item in self.rejected_observation_dates
            ],
            "missing_twse_dates": [item.isoformat() for item in self.missing_twse_dates],
            "remaining_uncovered_dates": [
                item.isoformat() for item in self.remaining_uncovered_dates
            ],
            "coverage_complete": self.coverage_complete,
            "eligibility_evidence_sha256": self.eligibility_evidence_sha256,
        }


@dataclass(frozen=True, slots=True)
class _NormalizedObservation:
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    symbol: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "symbol": self.symbol,
        }


def _parse_observation_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError:
            return None
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    converted = float(value)
    return converted if math.isfinite(converted) else None


def _normalize_observations(
    observations: tuple[OHLCVObservation, ...],
    *,
    requested_symbol: str,
    start_date: date,
    target_date: date,
) -> tuple[PayloadStructuralStatus, tuple[_NormalizedObservation, ...], EligibilityReason | None]:
    if not observations:
        return (
            PayloadStructuralStatus.MALFORMED_PAYLOAD,
            (),
            EligibilityReason.INSUFFICIENT_SUPPLEMENTAL_COVERAGE,
        )

    parsed_dates: list[date] = []
    for item in observations:
        parsed = _parse_observation_date(item.trade_date)
        if parsed is None:
            return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.MALFORMED_PAYLOAD
        parsed_dates.append(parsed)
    if parsed_dates != sorted(parsed_dates):
        return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.MALFORMED_PAYLOAD
    if len(set(parsed_dates)) != len(parsed_dates):
        return (
            PayloadStructuralStatus.DUPLICATE_TRADE_DATE,
            (),
            EligibilityReason.DUPLICATE_TRADE_DATE,
        )
    if any(item > target_date for item in parsed_dates):
        return PayloadStructuralStatus.FUTURE_TRADE_DATE, (), EligibilityReason.FUTURE_TRADE_DATE
    if any(item < start_date for item in parsed_dates):
        return (
            PayloadStructuralStatus.UNEXPECTED_OBSERVATION_DATE,
            (),
            EligibilityReason.UNEXPECTED_OBSERVATION_DATE,
        )

    normalized: list[_NormalizedObservation] = []
    for item, parsed_date in zip(observations, parsed_dates):
        if item.symbol is not None:
            if not isinstance(item.symbol, str):
                return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.MALFORMED_PAYLOAD
            try:
                row_symbol = _normalize_symbol(item.symbol, "observation.symbol")
            except SupplementalEligibilityContractError:
                return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.MALFORMED_PAYLOAD
            if row_symbol != requested_symbol:
                return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.IDENTITY_MISMATCH
        else:
            row_symbol = None

        values = (item.open, item.high, item.low, item.close)
        missing = tuple(value is None for value in values)
        if any(missing) and not all(missing):
            return PayloadStructuralStatus.PARTIAL_OHLC, (), EligibilityReason.PARTIAL_OHLC
        if all(missing):
            return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.MALFORMED_PAYLOAD
        prices = tuple(_number(value) for value in values)
        if any(value is None or value <= 0 for value in prices):
            return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.MALFORMED_PAYLOAD
        volume = item.volume
        if isinstance(volume, bool) or not isinstance(volume, int) or volume < 0:
            return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.MALFORMED_PAYLOAD
        open_price, high, low, close = prices  # type: ignore[misc]
        if high < max(open_price, low, close) or low > min(open_price, high, close):
            return PayloadStructuralStatus.MALFORMED_PAYLOAD, (), EligibilityReason.MALFORMED_PAYLOAD
        normalized.append(
            _NormalizedObservation(
                trade_date=parsed_date,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume,
                symbol=row_symbol,
            )
        )
    return PayloadStructuralStatus.PASS, tuple(normalized), None


def _same_observation(left: _NormalizedObservation, right: _NormalizedObservation) -> bool:
    return (
        left.open == right.open
        and left.high == right.high
        and left.low == right.low
        and left.close == right.close
        and left.volume == right.volume
    )


def _identity_status(
    requested: InstrumentIdentity | None,
    returned: InstrumentIdentity | None,
    requested_symbol: str,
) -> IdentityVerificationStatus:
    if requested is None or returned is None:
        return IdentityVerificationStatus.UNAVAILABLE
    if requested.symbol != requested_symbol or returned.symbol != requested.symbol:
        return IdentityVerificationStatus.SYMBOL_MISMATCH
    if returned.market != requested.market:
        return IdentityVerificationStatus.MARKET_MISMATCH
    if returned.exchange != requested.exchange:
        return IdentityVerificationStatus.EXCHANGE_MISMATCH
    if returned.currency != requested.currency:
        return IdentityVerificationStatus.CURRENCY_MISMATCH
    if returned.security_type != requested.security_type:
        return IdentityVerificationStatus.SECURITY_TYPE_MISMATCH
    return IdentityVerificationStatus.VERIFIED


def _identity_reason(status: IdentityVerificationStatus) -> EligibilityReason:
    return {
        IdentityVerificationStatus.UNAVAILABLE: EligibilityReason.INSTRUMENT_UNAVAILABLE,
        IdentityVerificationStatus.SYMBOL_MISMATCH: EligibilityReason.IDENTITY_MISMATCH,
        IdentityVerificationStatus.MARKET_MISMATCH: EligibilityReason.MARKET_MISMATCH,
        IdentityVerificationStatus.EXCHANGE_MISMATCH: EligibilityReason.IDENTITY_MISMATCH,
        IdentityVerificationStatus.CURRENCY_MISMATCH: EligibilityReason.CURRENCY_MISMATCH,
        IdentityVerificationStatus.SECURITY_TYPE_MISMATCH: EligibilityReason.SECURITY_TYPE_MISMATCH,
    }.get(status, EligibilityReason.UNKNOWN_FAIL_CLOSED)


def _security_status(
    value: SecurityBoundaryEvidence | SecurityBoundaryStatus,
) -> SecurityBoundaryStatus:
    if isinstance(value, SecurityBoundaryEvidence):
        return value.status
    return value


def _candidate_dates(observations: Iterable[OHLCVObservation]) -> tuple[date, ...]:
    dates = {
        parsed
        for item in observations
        if (parsed := _parse_observation_date(item.trade_date)) is not None
    }
    return tuple(sorted(dates))


def _evidence_hash(
    candidate: SupplementalCandidateInput,
    *,
    eligible: bool,
    reason: EligibilityReason,
    identity_status: IdentityVerificationStatus,
    overlap_status: OverlapValidationStatus,
    structural_status: PayloadStructuralStatus,
    security_status: SecurityBoundaryStatus,
    eligible_dates: tuple[date, ...],
    rejected_dates: tuple[date, ...],
    remaining_dates: tuple[date, ...],
    coverage_complete: bool,
) -> str:
    payload = {
        "candidate": candidate.as_dict(),
        "decision": {
            "eligible": eligible,
            "reason": reason.value,
            "identity_status": identity_status.value,
            "overlap_status": overlap_status.value,
            "structural_status": structural_status.value,
            "security_status": security_status.value,
            "eligible_dates": [item.isoformat() for item in eligible_dates],
            "rejected_dates": [item.isoformat() for item in rejected_dates],
            "remaining_dates": [item.isoformat() for item in remaining_dates],
            "coverage_complete": coverage_complete,
        },
    }
    return sha256_text(canonical_json(payload))


def _result(
    candidate: SupplementalCandidateInput,
    *,
    eligible: bool,
    reason: EligibilityReason,
    identity_status: IdentityVerificationStatus,
    overlap_status: OverlapValidationStatus,
    structural_status: PayloadStructuralStatus,
    security_status: SecurityBoundaryStatus,
    eligible_dates: tuple[date, ...] = (),
    rejected_dates: tuple[date, ...] = (),
    remaining_dates: tuple[date, ...] | None = None,
    coverage_complete: bool | None = None,
) -> SupplementalEligibilityResult:
    if remaining_dates is None:
        remaining_dates = tuple(
            sorted(set(candidate.missing_twse_dates) - set(eligible_dates))
        )
    if coverage_complete is None:
        coverage_complete = not remaining_dates
    evidence_hash = _evidence_hash(
        candidate,
        eligible=eligible,
        reason=reason,
        identity_status=identity_status,
        overlap_status=overlap_status,
        structural_status=structural_status,
        security_status=security_status,
        eligible_dates=tuple(sorted(set(eligible_dates))),
        rejected_dates=tuple(sorted(set(rejected_dates))),
        remaining_dates=tuple(sorted(set(remaining_dates))),
        coverage_complete=coverage_complete,
    )
    return SupplementalEligibilityResult(
        eligible=eligible,
        source_eligible=eligible,
        reason=reason,
        twse_failure_class=candidate.twse_failure_class,
        esun_identity_status=identity_status,
        overlap_status=overlap_status,
        structural_validation_status=structural_status,
        security_boundary_status=security_status,
        eligible_observation_dates=tuple(sorted(set(eligible_dates))),
        rejected_observation_dates=tuple(sorted(set(rejected_dates))),
        missing_twse_dates=candidate.missing_twse_dates,
        remaining_uncovered_dates=tuple(sorted(set(remaining_dates))),
        coverage_complete=coverage_complete,
        eligibility_evidence_sha256=evidence_hash,
    )


def evaluate_supplemental(
    candidate: SupplementalCandidateInput,
) -> SupplementalEligibilityResult:
    """Evaluate one normalized candidate with fail-closed deterministic semantics."""

    if not isinstance(candidate, SupplementalCandidateInput):
        raise _error("candidate", "must be a SupplementalCandidateInput")

    # A complete TWSE source does not need a supplemental path.  E.SUN may be
    # consumed elsewhere as validation evidence, but DS2 does not inspect it.
    if not candidate.missing_twse_dates:
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.NOT_NEEDED,
            identity_status=IdentityVerificationStatus.NOT_CHECKED,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=PayloadStructuralStatus.NOT_CHECKED,
            security_status=SecurityBoundaryStatus.NOT_CHECKED,
            remaining_dates=(),
            coverage_complete=True,
        )

    failure_class = candidate.twse_failure_class
    if failure_class is None or not is_supplemental_eligible_failure(failure_class):
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.TWSE_FAILURE_NOT_ELIGIBLE,
            identity_status=IdentityVerificationStatus.NOT_CHECKED,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=PayloadStructuralStatus.NOT_CHECKED,
            security_status=SecurityBoundaryStatus.NOT_CHECKED,
            rejected_dates=_candidate_dates(candidate.esun_observations),
            coverage_complete=False,
        )

    failure_evidence = candidate.twse_failure_evidence
    if failure_evidence is not None and not failure_evidence.endpoint_identity_verified:
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.UNKNOWN_FAIL_CLOSED,
            identity_status=IdentityVerificationStatus.NOT_CHECKED,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=PayloadStructuralStatus.NOT_CHECKED,
            security_status=SecurityBoundaryStatus.NOT_CHECKED,
            rejected_dates=_candidate_dates(candidate.esun_observations),
            coverage_complete=False,
        )

    identity_status = _identity_status(
        candidate.esun_requested_identity,
        candidate.esun_returned_identity,
        candidate.requested_symbol,
    )
    if identity_status is not IdentityVerificationStatus.VERIFIED:
        return _result(
            candidate,
            eligible=False,
            reason=_identity_reason(identity_status),
            identity_status=identity_status,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=PayloadStructuralStatus.NOT_CHECKED,
            security_status=SecurityBoundaryStatus.NOT_CHECKED,
            rejected_dates=_candidate_dates(candidate.esun_observations),
            coverage_complete=False,
        )

    security_status = _security_status(candidate.security_boundary)
    if security_status is not SecurityBoundaryStatus.PASS:
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.SECURITY_BOUNDARY_FAILED,
            identity_status=identity_status,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=PayloadStructuralStatus.NOT_CHECKED,
            security_status=security_status,
            rejected_dates=_candidate_dates(candidate.esun_observations),
            coverage_complete=False,
        )

    structural_status, esun_rows, structural_reason = _normalize_observations(
        candidate.esun_observations,
        requested_symbol=candidate.requested_symbol,
        start_date=candidate.requested_start_date,
        target_date=candidate.target_date,
    )
    if structural_status is not PayloadStructuralStatus.PASS:
        return _result(
            candidate,
            eligible=False,
            reason=structural_reason or EligibilityReason.MALFORMED_PAYLOAD,
            identity_status=identity_status,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=structural_status,
            security_status=security_status,
            rejected_dates=_candidate_dates(candidate.esun_observations),
            coverage_complete=False,
        )

    twse_status, twse_rows, _ = _normalize_observations(
        candidate.twse_observations,
        requested_symbol=candidate.requested_symbol,
        start_date=candidate.requested_start_date,
        target_date=candidate.target_date,
    ) if candidate.twse_observations else (PayloadStructuralStatus.PASS, (), None)
    if twse_status is not PayloadStructuralStatus.PASS:
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.UNKNOWN_FAIL_CLOSED,
            identity_status=identity_status,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=twse_status,
            security_status=security_status,
            rejected_dates=_candidate_dates(candidate.esun_observations),
            coverage_complete=False,
        )
    twse_by_date = {item.trade_date: item for item in twse_rows}
    if set(twse_by_date).intersection(candidate.missing_twse_dates):
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.UNKNOWN_FAIL_CLOSED,
            identity_status=identity_status,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=PayloadStructuralStatus.PASS,
            security_status=security_status,
            rejected_dates=_candidate_dates(candidate.esun_observations),
            coverage_complete=False,
        )

    esun_by_date = {item.trade_date: item for item in esun_rows}
    overlap_dates = set(esun_by_date).intersection(twse_by_date)
    unexpected_dates = set(esun_by_date) - set(candidate.missing_twse_dates) - overlap_dates
    if unexpected_dates:
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.UNEXPECTED_OBSERVATION_DATE,
            identity_status=identity_status,
            overlap_status=OverlapValidationStatus.NOT_APPLICABLE,
            structural_status=structural_status,
            security_status=security_status,
            rejected_dates=tuple(esun_by_date),
            coverage_complete=False,
        )

    discrepancy_dates = {
        trade_date
        for trade_date in overlap_dates
        if not _same_observation(esun_by_date[trade_date], twse_by_date[trade_date])
    }
    if discrepancy_dates:
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.OVERLAP_DISCREPANCY,
            identity_status=identity_status,
            overlap_status=OverlapValidationStatus.DISCREPANCY,
            structural_status=structural_status,
            security_status=security_status,
            rejected_dates=tuple(esun_by_date),
            coverage_complete=False,
        )

    overlap_status = (
        OverlapValidationStatus.EXACT_MATCH
        if overlap_dates
        else OverlapValidationStatus.NOT_APPLICABLE
    )
    eligible_dates = tuple(
        sorted(set(candidate.missing_twse_dates).intersection(esun_by_date))
    )
    rejected_dates = tuple(sorted(overlap_dates))
    remaining_dates = tuple(sorted(set(candidate.missing_twse_dates) - set(eligible_dates)))
    if not eligible_dates:
        return _result(
            candidate,
            eligible=False,
            reason=EligibilityReason.INSUFFICIENT_SUPPLEMENTAL_COVERAGE,
            identity_status=identity_status,
            overlap_status=overlap_status,
            structural_status=structural_status,
            security_status=security_status,
            rejected_dates=rejected_dates,
            remaining_dates=remaining_dates,
            coverage_complete=False,
        )
    if remaining_dates:
        return _result(
            candidate,
            eligible=True,
            reason=EligibilityReason.INSUFFICIENT_SUPPLEMENTAL_COVERAGE,
            identity_status=identity_status,
            overlap_status=overlap_status,
            structural_status=structural_status,
            security_status=security_status,
            eligible_dates=eligible_dates,
            rejected_dates=rejected_dates,
            remaining_dates=remaining_dates,
            coverage_complete=False,
        )
    return _result(
        candidate,
        eligible=True,
        reason=EligibilityReason.ELIGIBLE_VERIFIED_SUPPLEMENTAL,
        identity_status=identity_status,
        overlap_status=overlap_status,
        structural_status=structural_status,
        security_status=security_status,
        eligible_dates=eligible_dates,
        rejected_dates=rejected_dates,
        remaining_dates=(),
        coverage_complete=True,
    )


evaluate_supplemental_eligibility = evaluate_supplemental


__all__ = [
    "DUAL_SOURCE_DATASET_CONTRACT_VERSION",
    "EligibilityReason",
    "IdentityStatus",
    "IdentityVerificationStatus",
    "InstrumentIdentity",
    "OHLCVObservation",
    "OverlapStatus",
    "OverlapValidationStatus",
    "PayloadStructuralStatus",
    "ReasonCode",
    "SecurityBoundaryEvidence",
    "SecurityBoundaryStatus",
    "StructuralValidationStatus",
    "SUPPLEMENTAL_ELIGIBILITY_CONTRACT_VERSION",
    "SupplementalCandidateInput",
    "SupplementalEligibilityContractError",
    "SupplementalEligibilityResult",
    "TwseFailureEvidence",
    "evaluate_supplemental",
    "evaluate_supplemental_eligibility",
]
