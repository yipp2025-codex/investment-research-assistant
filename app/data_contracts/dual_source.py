"""DS1 pure contract for dual-source dataset status and provenance.

This module deliberately contains only immutable domain values, deterministic
serialization, and validation.  It must remain independent of providers,
network clients, SQLite, schedulers, production configuration, and report
renderers.  DS2 owns supplemental eligibility and provider integration.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, ClassVar, Iterable, Mapping, Sequence


DUAL_SOURCE_DATASET_CONTRACT_VERSION = "dual-source-dataset-contract.v1"
TWSE_CANONICAL_AUTHORITY = "twse"
TWSE_BASELINE_SOURCE_POLICY = "twse_baseline"
TWSE_DUAL_SOURCE_POLICY = "twse_dual_source_v1"

TWSE_PROVIDER_IDENTITIES = frozenset({"twse", "twse-historical"})
ESUN_PROVIDER_IDENTITIES = frozenset({"esun", "esun-historical"})
FORMAL_PROVIDER_IDENTITIES = frozenset(
    TWSE_PROVIDER_IDENTITIES | ESUN_PROVIDER_IDENTITIES
)

DUAL_SOURCE_SECURITY_INVARIANTS = (
    "redirect_authority_verification",
    "credential_forwarding_protection",
    "response_byte_cap",
    "total_deadline",
    "bounded_retry_after",
    "identity_validation",
)


class DualSourceContractError(ValueError):
    """Raised when a DS1 value or state violates the pure contract."""


class SourceRole(str, Enum):
    CANONICAL = "canonical"
    VALIDATION = "validation"
    SUPPLEMENTAL = "supplemental"


class DatasetSourceStatus(str, Enum):
    CANONICAL_COMPLETE = "canonical_complete"
    PROVISIONAL_MIXED = "provisional_mixed"
    RECONCILED = "reconciled"


class AuthorityStatus(str, Enum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    RECONCILED = "reconciled"


class ReconciliationStatus(str, Enum):
    NOT_APPLICABLE = "not_applicable"
    PENDING = "pending"
    RECONCILED_EQUAL = "reconciled_equal"
    RECONCILED_DISCREPANT = "reconciled_discrepant"


class ResearchDataQuality(str, Enum):
    """DS1 vocabulary only; this does not alter screener terminal statuses."""

    CANONICAL = "canonical"
    PROVISIONAL = "provisional"
    RECONCILED = "reconciled"


class FailureClassification(str, Enum):
    TEMPORARY = "temporary"
    DELAYED = "delayed"
    PACING_BLOCKED = "pacing_blocked"
    TIMEOUT = "timeout"
    MALFORMED = "malformed"
    PERMANENT = "permanent"
    IDENTITY_MISMATCH = "identity_mismatch"
    UNAVAILABLE = "unavailable"

    @classmethod
    def supplemental_eligible(cls) -> frozenset["FailureClassification"]:
        return SUPPLEMENTAL_ELIGIBLE_FAILURES

    @classmethod
    def supplemental_forbidden(cls) -> frozenset["FailureClassification"]:
        return SUPPLEMENTAL_FORBIDDEN_FAILURES


class SupplementalEligibilityReason(str, Enum):
    VERIFIED = "verified"
    REJECTED_IDENTITY_MISMATCH = "rejected_identity_mismatch"
    REJECTED_MALFORMED = "rejected_malformed"
    REJECTED_PERMANENT = "rejected_permanent"
    REJECTED_UNAVAILABLE = "rejected_unavailable"


SUPPLEMENTAL_ELIGIBLE_FAILURES = frozenset(
    {
        FailureClassification.TEMPORARY,
        FailureClassification.DELAYED,
        FailureClassification.PACING_BLOCKED,
        FailureClassification.TIMEOUT,
    }
)
SUPPLEMENTAL_FORBIDDEN_FAILURES = frozenset(
    {
        FailureClassification.MALFORMED,
        FailureClassification.PERMANENT,
        FailureClassification.IDENTITY_MISMATCH,
        FailureClassification.UNAVAILABLE,
    }
)


_SYMBOL_RE = re.compile(r"^[0-9A-Z]{2,12}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PROVIDER_ORDER = {
    "twse": 0,
    "twse-historical": 1,
    "esun": 2,
    "esun-historical": 3,
}
_ROLE_ORDER = {
    SourceRole.CANONICAL: 0,
    SourceRole.SUPPLEMENTAL: 1,
    SourceRole.VALIDATION: 2,
}


def _contract_error(field_name: str, message: str) -> DualSourceContractError:
    return DualSourceContractError(f"{field_name}: {message}")


def _coerce_enum(enum_type: type[Enum], value: Any, field_name: str) -> Any:
    if isinstance(value, enum_type):
        return value
    if not isinstance(value, str):
        raise _contract_error(field_name, "must be a fixed enum value")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise _contract_error(field_name, "contains an unsupported enum value") from exc


def _require_non_blank(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _contract_error(field_name, "must be a non-blank string")
    return value.strip()


def _normalize_date(value: Any, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise _contract_error(field_name, "must be a date, not a datetime or string")
    return value


def _normalize_symbol(value: Any, field_name: str) -> str:
    normalized = _require_non_blank(value, field_name).upper()
    if _SYMBOL_RE.fullmatch(normalized) is None:
        raise _contract_error(field_name, "must contain 2-12 ASCII letters or digits")
    return normalized


def _normalize_provider(value: Any, field_name: str = "provider") -> str:
    normalized = _require_non_blank(value, field_name).lower()
    if normalized not in FORMAL_PROVIDER_IDENTITIES:
        raise _contract_error(field_name, "is not an approved provider identity")
    return normalized


def _normalize_sha256(value: Any, field_name: str) -> str:
    normalized = _require_non_blank(value, field_name)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise _contract_error(field_name, "must be 64 lowercase hexadecimal characters")
    return normalized


def _require_non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _contract_error(field_name, "must be a non-negative integer")
    return value


def _require_positive_int(value: Any, field_name: str) -> int:
    normalized = _require_non_negative_int(value, field_name)
    if normalized == 0:
        raise _contract_error(field_name, "must be greater than zero")
    return normalized


def canonical_json(value: Any) -> str:
    """Serialize JSON values with the DS1 stable ordering and null semantics."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise DualSourceContractError("value is not deterministically JSON serializable") from exc


def sha256_text(value: str) -> str:
    if not isinstance(value, str):
        raise _contract_error("value", "must be text")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_provider_tuple(
    values: Iterable[str], field_name: str, *, esun_only: bool = False
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise _contract_error(field_name, "must be an iterable of provider identities")
    try:
        normalized = tuple(_normalize_provider(value, field_name) for value in values)
    except TypeError as exc:
        raise _contract_error(field_name, "must be an iterable of provider identities") from exc
    if len(set(normalized)) != len(normalized):
        raise _contract_error(field_name, "must not contain duplicate providers")
    if esun_only and any(value not in ESUN_PROVIDER_IDENTITIES for value in normalized):
        raise _contract_error(field_name, "may contain only E.SUN provider identities")
    return tuple(sorted(normalized, key=lambda value: _PROVIDER_ORDER[value]))


@dataclass(frozen=True, slots=True)
class DatasetCoverage:
    """Counts for required research observations and selected source rows."""

    required_observation_count: int
    twse_observation_count: int
    esun_supplemental_count: int
    missing_twse_count: int
    discrepancy_count: int
    latest_reconciled_date: date | None = None
    selected_observation_count: int | None = None
    coverage_complete: bool | None = None

    def __post_init__(self) -> None:
        required = _require_positive_int(
            self.required_observation_count, "required_observation_count"
        )
        twse = _require_non_negative_int(
            self.twse_observation_count, "twse_observation_count"
        )
        supplemental = _require_non_negative_int(
            self.esun_supplemental_count, "esun_supplemental_count"
        )
        missing = _require_non_negative_int(self.missing_twse_count, "missing_twse_count")
        discrepancy = _require_non_negative_int(
            self.discrepancy_count, "discrepancy_count"
        )
        if twse + supplemental > required:
            raise _contract_error(
                "coverage", "TWSE and E.SUN selected counts exceed required coverage"
            )
        if missing != required - twse:
            raise _contract_error(
                "missing_twse_count", "must equal required_observation_count - twse_observation_count"
            )
        if self.latest_reconciled_date is not None:
            _normalize_date(self.latest_reconciled_date, "latest_reconciled_date")

        selected = self.selected_observation_count
        if selected is None:
            selected = twse + supplemental
        else:
            selected = _require_non_negative_int(selected, "selected_observation_count")
            if selected != twse + supplemental:
                raise _contract_error(
                    "selected_observation_count",
                    "must equal TWSE plus E.SUN supplemental counts",
                )

        complete = selected == required
        if self.coverage_complete is None:
            coverage_complete = complete
        elif not isinstance(self.coverage_complete, bool):
            raise _contract_error("coverage_complete", "must be a boolean or null")
        else:
            coverage_complete = self.coverage_complete
            if coverage_complete != complete:
                raise _contract_error(
                    "coverage_complete", "must match selected_observation_count == required_observation_count"
                )

        object.__setattr__(self, "required_observation_count", required)
        object.__setattr__(self, "twse_observation_count", twse)
        object.__setattr__(self, "esun_supplemental_count", supplemental)
        object.__setattr__(self, "missing_twse_count", missing)
        object.__setattr__(self, "discrepancy_count", discrepancy)
        object.__setattr__(self, "selected_observation_count", selected)
        object.__setattr__(self, "coverage_complete", coverage_complete)

    @property
    def required_count(self) -> int:
        return self.required_observation_count

    @property
    def selected_count(self) -> int:
        return self.selected_observation_count  # type: ignore[return-value]

    def as_dict(self) -> dict[str, Any]:
        return {
            "required_observation_count": self.required_observation_count,
            "twse_observation_count": self.twse_observation_count,
            "esun_supplemental_count": self.esun_supplemental_count,
            "missing_twse_count": self.missing_twse_count,
            "discrepancy_count": self.discrepancy_count,
            "latest_reconciled_date": (
                self.latest_reconciled_date.isoformat()
                if self.latest_reconciled_date is not None
                else None
            ),
            "selected_observation_count": self.selected_observation_count,
            "coverage_complete": self.coverage_complete,
        }


@dataclass(frozen=True, slots=True)
class ObservationProvenance:
    """Per-observation source and selection evidence."""

    trade_date: date
    provider: str
    source_role: SourceRole
    source_run_id: str
    selected: bool
    observation_sha256: str
    symbol: str | None = None

    def __post_init__(self) -> None:
        trade_date = _normalize_date(self.trade_date, "trade_date")
        provider = _normalize_provider(self.provider)
        source_role = _coerce_enum(SourceRole, self.source_role, "source_role")
        source_run_id = _require_non_blank(self.source_run_id, "source_run_id")
        if not isinstance(self.selected, bool):
            raise _contract_error("selected", "must be a boolean")
        observation_sha256 = _normalize_sha256(
            self.observation_sha256, "observation_sha256"
        )
        symbol = (
            None if self.symbol is None else _normalize_symbol(self.symbol, "symbol")
        )

        if source_role is SourceRole.CANONICAL:
            if provider not in TWSE_PROVIDER_IDENTITIES:
                raise _contract_error(
                    "source_role", "canonical observations must use a TWSE provider"
                )
            if not self.selected:
                raise _contract_error(
                    "selected", "canonical observations must be selected"
                )
        elif source_role is SourceRole.SUPPLEMENTAL:
            if provider not in ESUN_PROVIDER_IDENTITIES:
                raise _contract_error(
                    "source_role", "supplemental observations must use an E.SUN provider"
                )
            if not self.selected:
                raise _contract_error(
                    "selected", "supplemental observations must be selected"
                )
        elif self.selected:
            raise _contract_error(
                "selected", "validation observations must not be selected"
            )

        object.__setattr__(self, "trade_date", trade_date)
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "source_role", source_role)
        object.__setattr__(self, "source_run_id", source_run_id)
        object.__setattr__(self, "observation_sha256", observation_sha256)
        object.__setattr__(self, "symbol", symbol)

    def as_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date.isoformat(),
            "provider": self.provider,
            "source_role": self.source_role.value,
            "source_run_id": self.source_run_id,
            "selected": self.selected,
            "observation_sha256": self.observation_sha256,
            "symbol": self.symbol,
        }


def _provenance_sort_key(observation: ObservationProvenance) -> tuple[Any, ...]:
    return (
        observation.trade_date,
        _PROVIDER_ORDER[observation.provider],
        _ROLE_ORDER[observation.source_role],
        observation.source_run_id,
        observation.selected,
        observation.symbol or "",
        observation.observation_sha256,
    )


def canonicalize_provenance(
    observations: Iterable[ObservationProvenance],
) -> tuple[ObservationProvenance, ...]:
    """Validate and return provenance in a stable, insertion-order-free order."""

    if isinstance(observations, (str, bytes)):
        raise _contract_error("observations", "must be an iterable of provenance values")
    try:
        normalized = tuple(observations)
    except TypeError as exc:
        raise _contract_error("observations", "must be an iterable of provenance values") from exc
    if any(not isinstance(item, ObservationProvenance) for item in normalized):
        raise _contract_error(
            "observations", "must contain only ObservationProvenance values"
        )
    full_identity = {
        (
            item.trade_date,
            item.provider,
            item.source_role,
            item.source_run_id,
            item.selected,
            item.observation_sha256,
            item.symbol,
        )
        for item in normalized
    }
    if len(full_identity) != len(normalized):
        raise _contract_error("observations", "contains duplicate provenance identity")

    selected_dates = [item.trade_date for item in normalized if item.selected]
    if len(set(selected_dates)) != len(selected_dates):
        raise _contract_error(
            "observations", "may contain at most one selected observation per trade date"
        )
    return tuple(sorted(normalized, key=_provenance_sort_key))


def canonical_provenance_json(
    observations: Iterable[ObservationProvenance],
) -> str:
    return canonical_json([item.as_dict() for item in canonicalize_provenance(observations)])


def provenance_map_sha256(
    observations: Iterable[ObservationProvenance],
) -> str:
    return sha256_text(canonical_provenance_json(observations))


def validate_provenance_map(
    observations: Iterable[ObservationProvenance], *, symbol: str | None = None
) -> tuple[ObservationProvenance, ...]:
    normalized = canonicalize_provenance(observations)
    if symbol is not None:
        expected_symbol = _normalize_symbol(symbol, "symbol")
        if any(item.symbol != expected_symbol for item in normalized):
            raise _contract_error("observations", "symbol does not match the requested symbol")
    return normalized


def validate_dataset_state(
    source_status: DatasetSourceStatus,
    authority_status: AuthorityStatus,
    reconciliation_status: ReconciliationStatus,
    coverage: DatasetCoverage,
) -> None:
    """Validate the DS1 legal state matrix without any external side effects."""

    source_status = _coerce_enum(DatasetSourceStatus, source_status, "source_status")
    authority_status = _coerce_enum(AuthorityStatus, authority_status, "authority_status")
    reconciliation_status = _coerce_enum(
        ReconciliationStatus, reconciliation_status, "reconciliation_status"
    )
    if not isinstance(coverage, DatasetCoverage):
        raise _contract_error("coverage", "must be a DatasetCoverage value")

    if source_status is DatasetSourceStatus.CANONICAL_COMPLETE:
        if authority_status is not AuthorityStatus.COMPLETE:
            raise _contract_error(
                "authority_status", "canonical_complete requires complete authority"
            )
        if reconciliation_status is not ReconciliationStatus.NOT_APPLICABLE:
            raise _contract_error(
                "reconciliation_status",
                "canonical_complete requires not_applicable reconciliation",
            )
        if coverage.esun_supplemental_count != 0:
            raise _contract_error(
                "esun_supplemental_count", "canonical_complete cannot select supplemental rows"
            )
        if coverage.twse_observation_count != coverage.required_observation_count:
            raise _contract_error(
                "twse_observation_count", "canonical_complete requires complete TWSE coverage"
            )
    elif source_status is DatasetSourceStatus.PROVISIONAL_MIXED:
        if coverage.esun_supplemental_count <= 0:
            raise _contract_error(
                "esun_supplemental_count", "provisional_mixed requires supplemental rows"
            )
        if authority_status is not AuthorityStatus.INCOMPLETE:
            raise _contract_error(
                "authority_status", "provisional_mixed requires incomplete authority"
            )
        if reconciliation_status is not ReconciliationStatus.PENDING:
            raise _contract_error(
                "reconciliation_status", "provisional_mixed requires pending reconciliation"
            )
    else:
        if authority_status is not AuthorityStatus.RECONCILED:
            raise _contract_error(
                "authority_status", "reconciled requires reconciled authority"
            )
        if reconciliation_status not in {
            ReconciliationStatus.RECONCILED_EQUAL,
            ReconciliationStatus.RECONCILED_DISCREPANT,
        }:
            raise _contract_error(
                "reconciliation_status",
                "reconciled requires equal or discrepant reconciliation",
            )
        if coverage.esun_supplemental_count != 0:
            raise _contract_error(
                "esun_supplemental_count", "reconciled cannot select supplemental rows"
            )
        if coverage.twse_observation_count != coverage.required_observation_count:
            raise _contract_error(
                "twse_observation_count", "reconciled requires complete TWSE coverage"
            )


@dataclass(frozen=True, slots=True)
class DatasetProvenanceSummary:
    """Dataset-level status plus the hash of its per-observation provenance."""

    canonical_authority: str
    supplemental_sources: tuple[str, ...]
    validation_sources: tuple[str, ...]
    source_status: DatasetSourceStatus
    authority_status: AuthorityStatus
    reconciliation_status: ReconciliationStatus
    coverage: DatasetCoverage
    provenance_map_sha256: str

    def __post_init__(self) -> None:
        canonical_authority = _normalize_provider(
            self.canonical_authority, "canonical_authority"
        )
        if canonical_authority != TWSE_CANONICAL_AUTHORITY:
            raise _contract_error(
                "canonical_authority", "DS1 fixes canonical authority to twse"
            )
        supplemental_sources = _normalize_provider_tuple(
            self.supplemental_sources, "supplemental_sources", esun_only=True
        )
        validation_sources = _normalize_provider_tuple(
            self.validation_sources, "validation_sources"
        )
        source_status = _coerce_enum(
            DatasetSourceStatus, self.source_status, "source_status"
        )
        authority_status = _coerce_enum(
            AuthorityStatus, self.authority_status, "authority_status"
        )
        reconciliation_status = _coerce_enum(
            ReconciliationStatus,
            self.reconciliation_status,
            "reconciliation_status",
        )
        if not isinstance(self.coverage, DatasetCoverage):
            raise _contract_error("coverage", "must be a DatasetCoverage value")
        provenance_hash = _normalize_sha256(
            self.provenance_map_sha256, "provenance_map_sha256"
        )
        validate_dataset_state(
            source_status, authority_status, reconciliation_status, self.coverage
        )

        if self.coverage.esun_supplemental_count > 0 and not supplemental_sources:
            raise _contract_error(
                "supplemental_sources", "must identify selected E.SUN sources"
            )
        if self.coverage.esun_supplemental_count == 0 and supplemental_sources:
            raise _contract_error(
                "supplemental_sources", "cannot be present without selected supplemental rows"
            )

        object.__setattr__(self, "canonical_authority", canonical_authority)
        object.__setattr__(self, "supplemental_sources", supplemental_sources)
        object.__setattr__(self, "validation_sources", validation_sources)
        object.__setattr__(self, "source_status", source_status)
        object.__setattr__(self, "authority_status", authority_status)
        object.__setattr__(self, "reconciliation_status", reconciliation_status)
        object.__setattr__(self, "provenance_map_sha256", provenance_hash)

    @classmethod
    def from_observations(
        cls,
        observations: Iterable[ObservationProvenance],
        *,
        source_status: DatasetSourceStatus,
        authority_status: AuthorityStatus,
        reconciliation_status: ReconciliationStatus,
        coverage: DatasetCoverage,
        canonical_authority: str = TWSE_CANONICAL_AUTHORITY,
        symbol: str | None = None,
    ) -> "DatasetProvenanceSummary":
        normalized = validate_provenance_map(observations, symbol=symbol)
        supplemental_sources = tuple(
            sorted(
                {
                    item.provider
                    for item in normalized
                    if item.source_role is SourceRole.SUPPLEMENTAL
                },
                key=lambda value: _PROVIDER_ORDER[value],
            )
        )
        validation_sources = tuple(
            sorted(
                {
                    item.provider
                    for item in normalized
                    if item.source_role is SourceRole.VALIDATION
                },
                key=lambda value: _PROVIDER_ORDER[value],
            )
        )
        summary = cls(
            canonical_authority=canonical_authority,
            supplemental_sources=supplemental_sources,
            validation_sources=validation_sources,
            source_status=source_status,
            authority_status=authority_status,
            reconciliation_status=reconciliation_status,
            coverage=coverage,
            provenance_map_sha256=provenance_map_sha256(normalized),
        )
        validate_dataset_provenance(summary, normalized, symbol=symbol)
        return summary

    def as_dict(self) -> dict[str, Any]:
        return {
            "canonical_authority": self.canonical_authority,
            "supplemental_sources": list(self.supplemental_sources),
            "validation_sources": list(self.validation_sources),
            "source_status": self.source_status.value,
            "authority_status": self.authority_status.value,
            "reconciliation_status": self.reconciliation_status.value,
            "coverage": self.coverage.as_dict(),
            "provenance_map_sha256": self.provenance_map_sha256,
        }


def validate_dataset_provenance(
    summary: DatasetProvenanceSummary,
    observations: Iterable[ObservationProvenance],
    *,
    symbol: str | None = None,
) -> tuple[ObservationProvenance, ...]:
    """Validate summary counts, source roles, and hash against per-row evidence."""

    if not isinstance(summary, DatasetProvenanceSummary):
        raise _contract_error("summary", "must be a DatasetProvenanceSummary value")
    normalized = validate_provenance_map(observations, symbol=symbol)
    if provenance_map_sha256(normalized) != summary.provenance_map_sha256:
        raise _contract_error(
            "provenance_map_sha256", "does not match the canonical provenance map"
        )

    selected_canonical = tuple(
        item for item in normalized if item.selected and item.source_role is SourceRole.CANONICAL
    )
    selected_supplemental = tuple(
        item
        for item in normalized
        if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
    )
    validation = tuple(item for item in normalized if item.source_role is SourceRole.VALIDATION)
    coverage = summary.coverage
    if len(selected_canonical) != coverage.twse_observation_count:
        raise _contract_error(
            "twse_observation_count", "does not match selected canonical provenance rows"
        )
    if len(selected_supplemental) != coverage.esun_supplemental_count:
        raise _contract_error(
            "esun_supplemental_count", "does not match selected supplemental provenance rows"
        )
    if len(selected_canonical) + len(selected_supplemental) != coverage.selected_observation_count:
        raise _contract_error(
            "selected_observation_count", "does not match selected provenance rows"
        )
    if set(item.provider for item in selected_supplemental) != set(
        summary.supplemental_sources
    ):
        raise _contract_error(
            "supplemental_sources", "does not match selected supplemental provenance providers"
        )
    if set(item.provider for item in validation) != set(summary.validation_sources):
        raise _contract_error(
            "validation_sources", "does not match validation provenance providers"
        )
    return normalized


@dataclass(frozen=True, slots=True)
class SourceIdentityValidation:
    """Pure identity check used before any future supplemental eligibility decision."""

    requested_symbol: str
    returned_symbol: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "requested_symbol", _normalize_symbol(self.requested_symbol, "requested_symbol")
        )
        object.__setattr__(
            self, "returned_symbol", _normalize_symbol(self.returned_symbol, "returned_symbol")
        )

    @property
    def is_match(self) -> bool:
        return self.requested_symbol == self.returned_symbol

    @property
    def supplemental_eligible(self) -> bool:
        return self.is_match

    @property
    def reason(self) -> SupplementalEligibilityReason | None:
        if self.is_match:
            return None
        return SupplementalEligibilityReason.REJECTED_IDENTITY_MISMATCH

    @property
    def eligibility_reason(self) -> SupplementalEligibilityReason:
        return (
            SupplementalEligibilityReason.VERIFIED
            if self.is_match
            else SupplementalEligibilityReason.REJECTED_IDENTITY_MISMATCH
        )


@dataclass(frozen=True, slots=True)
class DatasetVersionIdentity:
    """Deterministic dataset identity, including status and parent lineage."""

    symbol: str
    as_of_date: date
    methodology_version: str
    source_policy: str
    provenance_map_sha256: str
    source_status: DatasetSourceStatus
    authority_status: AuthorityStatus
    reconciliation_status: ReconciliationStatus
    coverage: DatasetCoverage
    parent_dataset_version_id: str | None = None
    contract_version: str = DUAL_SOURCE_DATASET_CONTRACT_VERSION

    _ALLOWED_POLICIES: ClassVar[frozenset[str]] = frozenset(
        {TWSE_BASELINE_SOURCE_POLICY, TWSE_DUAL_SOURCE_POLICY}
    )

    def __post_init__(self) -> None:
        symbol = _normalize_symbol(self.symbol, "symbol")
        as_of_date = _normalize_date(self.as_of_date, "as_of_date")
        methodology_version = _require_non_blank(
            self.methodology_version, "methodology_version"
        )
        source_policy = _require_non_blank(self.source_policy, "source_policy")
        if source_policy not in self._ALLOWED_POLICIES:
            raise _contract_error("source_policy", "is not an approved DS1 source policy")
        provenance_hash = _normalize_sha256(
            self.provenance_map_sha256, "provenance_map_sha256"
        )
        source_status = _coerce_enum(
            DatasetSourceStatus, self.source_status, "source_status"
        )
        authority_status = _coerce_enum(
            AuthorityStatus, self.authority_status, "authority_status"
        )
        reconciliation_status = _coerce_enum(
            ReconciliationStatus,
            self.reconciliation_status,
            "reconciliation_status",
        )
        if not isinstance(self.coverage, DatasetCoverage):
            raise _contract_error("coverage", "must be a DatasetCoverage value")
        contract_version = _require_non_blank(self.contract_version, "contract_version")
        if contract_version != DUAL_SOURCE_DATASET_CONTRACT_VERSION:
            raise _contract_error("contract_version", "is not the DS1 contract version")
        validate_dataset_state(
            source_status, authority_status, reconciliation_status, self.coverage
        )
        if source_status in {
            DatasetSourceStatus.PROVISIONAL_MIXED,
            DatasetSourceStatus.RECONCILED,
        } and source_policy != TWSE_DUAL_SOURCE_POLICY:
            raise _contract_error(
                "source_policy", "provisional and reconciled versions require the dual-source policy"
            )

        parent = self.parent_dataset_version_id
        if parent is not None:
            parent = _normalize_sha256(parent, "parent_dataset_version_id")
        if source_status is DatasetSourceStatus.CANONICAL_COMPLETE and parent is not None:
            raise _contract_error(
                "parent_dataset_version_id", "canonical_complete versions must have no parent"
            )
        if source_status is DatasetSourceStatus.RECONCILED and parent is None:
            raise _contract_error(
                "parent_dataset_version_id", "reconciled versions require the provisional parent id"
            )

        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "as_of_date", as_of_date)
        object.__setattr__(self, "methodology_version", methodology_version)
        object.__setattr__(self, "source_policy", source_policy)
        object.__setattr__(self, "provenance_map_sha256", provenance_hash)
        object.__setattr__(self, "source_status", source_status)
        object.__setattr__(self, "authority_status", authority_status)
        object.__setattr__(self, "reconciliation_status", reconciliation_status)
        object.__setattr__(self, "parent_dataset_version_id", parent)
        object.__setattr__(self, "contract_version", contract_version)

        if parent is not None and parent == self.dataset_version_id:
            raise _contract_error(
                "parent_dataset_version_id", "a dataset version cannot parent itself"
            )

    def _identity_payload(self) -> dict[str, Any]:
        return {
            "as_of_date": self.as_of_date.isoformat(),
            "authority_status": self.authority_status.value,
            "contract_version": self.contract_version,
            "coverage": self.coverage.as_dict(),
            "methodology_version": self.methodology_version,
            "parent_dataset_version_id": self.parent_dataset_version_id,
            "provenance_map_sha256": self.provenance_map_sha256,
            "reconciliation_status": self.reconciliation_status.value,
            "source_policy": self.source_policy,
            "source_status": self.source_status.value,
            "symbol": self.symbol,
        }

    @property
    def dataset_version_id(self) -> str:
        return sha256_text(canonical_json(self._identity_payload()))

    def canonical_json(self) -> str:
        return canonical_json(self._identity_payload())

    def as_dict(self) -> dict[str, Any]:
        payload = dict(self._identity_payload())
        payload["dataset_version_id"] = self.dataset_version_id
        return payload

    @classmethod
    def from_summary(
        cls,
        summary: DatasetProvenanceSummary,
        *,
        symbol: str,
        as_of_date: date,
        methodology_version: str,
        source_policy: str,
        parent_dataset_version_id: str | None = None,
    ) -> "DatasetVersionIdentity":
        if not isinstance(summary, DatasetProvenanceSummary):
            raise _contract_error("summary", "must be a DatasetProvenanceSummary value")
        return cls(
            symbol=symbol,
            as_of_date=as_of_date,
            methodology_version=methodology_version,
            source_policy=source_policy,
            provenance_map_sha256=summary.provenance_map_sha256,
            source_status=summary.source_status,
            authority_status=summary.authority_status,
            reconciliation_status=summary.reconciliation_status,
            coverage=summary.coverage,
            parent_dataset_version_id=parent_dataset_version_id,
        )


def legacy_v1_provenance_summary(
    required_observation_count: int,
    *,
    discrepancy_count: int = 0,
    validation_sources: Iterable[str] = (),
) -> DatasetProvenanceSummary:
    """Map frozen v1 ``twse_baseline`` semantics without rewriting old rows.

    v1 did not have a DS1 per-row provenance map.  The hash therefore records
    an explicit compatibility sentinel, rather than pretending to reconstruct
    row-level evidence that was never stored.
    """

    coverage = DatasetCoverage(
        required_observation_count=required_observation_count,
        twse_observation_count=required_observation_count,
        esun_supplemental_count=0,
        missing_twse_count=0,
        discrepancy_count=discrepancy_count,
    )
    normalized_validation = _normalize_provider_tuple(
        validation_sources, "validation_sources"
    )
    compatibility_marker = canonical_json(
        {
            "legacy_source_policy": TWSE_BASELINE_SOURCE_POLICY,
            "legacy_semantics": "canonical_complete",
            "validation_sources": list(normalized_validation),
            "coverage": coverage.as_dict(),
        }
    )
    return DatasetProvenanceSummary(
        canonical_authority=TWSE_CANONICAL_AUTHORITY,
        supplemental_sources=(),
        validation_sources=normalized_validation,
        source_status=DatasetSourceStatus.CANONICAL_COMPLETE,
        authority_status=AuthorityStatus.COMPLETE,
        reconciliation_status=ReconciliationStatus.NOT_APPLICABLE,
        coverage=coverage,
        provenance_map_sha256=sha256_text(compatibility_marker),
    )


def legacy_v1_dataset_identity(
    *,
    symbol: str,
    as_of_date: date,
    methodology_version: str,
    required_observation_count: int,
    discrepancy_count: int = 0,
    validation_sources: Iterable[str] = (),
) -> DatasetVersionIdentity:
    summary = legacy_v1_provenance_summary(
        required_observation_count,
        discrepancy_count=discrepancy_count,
        validation_sources=validation_sources,
    )
    return DatasetVersionIdentity.from_summary(
        summary,
        symbol=symbol,
        as_of_date=as_of_date,
        methodology_version=methodology_version,
        source_policy=TWSE_BASELINE_SOURCE_POLICY,
    )


def is_supplemental_eligible_failure(
    failure: FailureClassification | str,
) -> bool:
    normalized = _coerce_enum(FailureClassification, failure, "failure")
    return normalized in SUPPLEMENTAL_ELIGIBLE_FAILURES


def is_supplemental_forbidden_failure(
    failure: FailureClassification | str,
) -> bool:
    normalized = _coerce_enum(FailureClassification, failure, "failure")
    return normalized in SUPPLEMENTAL_FORBIDDEN_FAILURES


__all__ = [
    "AuthorityStatus",
    "DUAL_SOURCE_DATASET_CONTRACT_VERSION",
    "DUAL_SOURCE_SECURITY_INVARIANTS",
    "DatasetCoverage",
    "DatasetProvenanceSummary",
    "DatasetSourceStatus",
    "DatasetVersionIdentity",
    "DualSourceContractError",
    "ESUN_PROVIDER_IDENTITIES",
    "FailureClassification",
    "FORMAL_PROVIDER_IDENTITIES",
    "ObservationProvenance",
    "ResearchDataQuality",
    "ReconciliationStatus",
    "SourceIdentityValidation",
    "SourceRole",
    "SupplementalEligibilityReason",
    "SUPPLEMENTAL_ELIGIBLE_FAILURES",
    "SUPPLEMENTAL_FORBIDDEN_FAILURES",
    "TWSE_BASELINE_SOURCE_POLICY",
    "TWSE_CANONICAL_AUTHORITY",
    "TWSE_DUAL_SOURCE_POLICY",
    "TWSE_PROVIDER_IDENTITIES",
    "canonical_json",
    "canonical_provenance_json",
    "canonicalize_provenance",
    "is_supplemental_eligible_failure",
    "is_supplemental_forbidden_failure",
    "legacy_v1_dataset_identity",
    "legacy_v1_provenance_summary",
    "provenance_map_sha256",
    "sha256_text",
    "validate_dataset_provenance",
    "validate_dataset_state",
    "validate_provenance_map",
]
