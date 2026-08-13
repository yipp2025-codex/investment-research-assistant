"""DS3 pure mixed-dataset construction and canonical persistence values.

This module contains no SQLite or provider imports.  The storage repository
serializes these immutable values; DS4 owns reconciliation and DS5 owns any
consumer integration.
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
    DatasetCoverage,
    DatasetProvenanceSummary,
    DatasetSourceStatus,
    DatasetVersionIdentity,
    DualSourceContractError,
    ObservationProvenance,
    ReconciliationStatus,
    SourceRole,
    TWSE_DUAL_SOURCE_POLICY,
    TWSE_PROVIDER_IDENTITIES,
    ESUN_PROVIDER_IDENTITIES,
    FORMAL_PROVIDER_IDENTITIES,
    AuthorityStatus,
    canonical_json,
    sha256_text,
    validate_dataset_provenance,
)
from .supplemental_eligibility import (
    SupplementalEligibilityResult,
)


DATASET_PERSISTENCE_CONTRACT_VERSION = "mixed-dataset-persistence-contract.v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[0-9A-Z]{2,12}$")
_SECRET_LIKE_SOURCE_RUN_FRAGMENTS = (
    "authorization=",
    "authorization:",
    "api_key=",
    "apikey=",
    "api-key:",
    "password=",
    "secret=",
    "token=",
    "token:",
    "bearer ",
)


class DatasetPersistenceContractError(DualSourceContractError):
    """Raised when a DS3 pure dataset cannot be constructed safely."""


class IncompleteDatasetCoverageError(DatasetPersistenceContractError):
    """A DS2 source is valid but cannot become a formal DS3 dataset."""


class CoverageBasis(str, Enum):
    STANDARD = "standard"
    LEGAL_SHORT_LISTING_HISTORY = "legal_short_listing_history"


def _error(field_name: str, message: str) -> DatasetPersistenceContractError:
    return DatasetPersistenceContractError(f"{field_name}: {message}")


def _non_blank(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _error(field_name, "must be a non-blank string")
    return value.strip()


def _date(value: Any, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise _error(field_name, "must be a date")
    return value


def _symbol(value: Any, field_name: str) -> str:
    normalized = _non_blank(value, field_name).upper()
    if _SYMBOL_RE.fullmatch(normalized) is None:
        raise _error(field_name, "must contain 2-12 ASCII letters or digits")
    return normalized


def _sha256(value: Any, field_name: str) -> str:
    normalized = _non_blank(value, field_name)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise _error(field_name, "must be 64 lowercase hexadecimal characters")
    return normalized


def _positive_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _error(field_name, "must be numeric")
    converted = float(value)
    if not math.isfinite(converted) or converted <= 0:
        raise _error(field_name, "must be finite and greater than zero")
    return converted


def _volume(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _error(field_name, "must be a non-negative integer")
    return value


def _reject_secret_like(value: str, field_name: str) -> str:
    lowered = value.lower()
    if any(fragment in lowered for fragment in _SECRET_LIKE_SOURCE_RUN_FRAGMENTS):
        raise _error(field_name, "must not contain credential-like material")
    return value


def _coerce_basis(value: CoverageBasis | str) -> CoverageBasis:
    if isinstance(value, CoverageBasis):
        return value
    try:
        return CoverageBasis(value)
    except (TypeError, ValueError) as exc:
        raise _error("coverage_basis", "contains an unsupported value") from exc


@dataclass(frozen=True, slots=True)
class DatasetObservation:
    """One immutable row in a DS3 dataset-version snapshot."""

    symbol: str
    trade_date: date
    provider: str
    source_role: SourceRole
    source_run_id: str
    selected: bool
    open: float
    high: float
    low: float
    close: float
    volume: int
    observation_sha256: str

    def __post_init__(self) -> None:
        symbol = _symbol(self.symbol, "symbol")
        trade_date = _date(self.trade_date, "trade_date")
        source_run_id = _reject_secret_like(
            _non_blank(self.source_run_id, "source_run_id"), "source_run_id"
        )
        if not isinstance(self.selected, bool):
            raise _error("selected", "must be boolean")
        try:
            provenance = ObservationProvenance(
                trade_date=trade_date,
                provider=self.provider,
                source_role=self.source_role,
                source_run_id=source_run_id,
                selected=self.selected,
                observation_sha256=self.observation_sha256,
                symbol=symbol,
            )
        except DualSourceContractError:
            raise
        open_price = _positive_number(self.open, "open")
        high = _positive_number(self.high, "high")
        low = _positive_number(self.low, "low")
        close = _positive_number(self.close, "close")
        if high < max(open_price, low, close) or low > min(open_price, high, close):
            raise _error("ohlc", "high/low relationship is invalid")
        volume = _volume(self.volume, "volume")

        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "trade_date", trade_date)
        object.__setattr__(self, "provider", provenance.provider)
        object.__setattr__(self, "source_role", provenance.source_role)
        object.__setattr__(self, "source_run_id", source_run_id)
        object.__setattr__(self, "open", open_price)
        object.__setattr__(self, "high", high)
        object.__setattr__(self, "low", low)
        object.__setattr__(self, "close", close)
        object.__setattr__(self, "volume", volume)
        object.__setattr__(self, "observation_sha256", provenance.observation_sha256)

    def as_provenance(self) -> ObservationProvenance:
        return ObservationProvenance(
            trade_date=self.trade_date,
            provider=self.provider,
            source_role=self.source_role,
            source_run_id=self.source_run_id,
            selected=self.selected,
            observation_sha256=self.observation_sha256,
            symbol=self.symbol,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "trade_date": self.trade_date.isoformat(),
            "provider": self.provider,
            "source_role": self.source_role.value,
            "source_run_id": self.source_run_id,
            "selected": self.selected,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "observation_sha256": self.observation_sha256,
        }


@dataclass(frozen=True, slots=True)
class DatasetArtifactRef:
    """Hash-only upstream artifact reference; raw provider payload is excluded."""

    ordinal: int
    provider: str
    dataset: str
    source_ref: str
    contract_version: str
    payload_sha256: str
    payload_size_bytes: int
    hash_basis: str

    def __post_init__(self) -> None:
        if isinstance(self.ordinal, bool) or not isinstance(self.ordinal, int) or self.ordinal <= 0:
            raise _error("ordinal", "must be a positive integer")
        provider = _non_blank(self.provider, "provider").lower()
        if provider not in FORMAL_PROVIDER_IDENTITIES:
            raise _error("provider", "is not an approved provider identity")
        dataset = _non_blank(self.dataset, "dataset")
        source_ref = _non_blank(self.source_ref, "source_ref")
        lowered = source_ref.lower()
        forbidden_fragments = (
            "authorization=",
            "api_key=",
            "apikey=",
            "access_token=",
            "api-token=",
            "api_token=",
            "password=",
            "secret=",
            "client_secret=",
            "x-api-key=",
            "x_api_key=",
            "credential=",
            "sig=",
            "signature=",
            "x-amz-signature=",
            "token=",
            "bearer=",
        )
        if any(fragment in lowered for fragment in forbidden_fragments):
            raise _error("source_ref", "must not contain credential-like query values")
        if "authorization" in lowered and "authorization" in lowered.split("?")[0]:
            raise _error("source_ref", "must not contain authorization material")
        contract_version = _non_blank(self.contract_version, "contract_version")
        payload_sha256 = _sha256(self.payload_sha256, "payload_sha256")
        if (
            isinstance(self.payload_size_bytes, bool)
            or not isinstance(self.payload_size_bytes, int)
            or self.payload_size_bytes < 0
        ):
            raise _error("payload_size_bytes", "must be a non-negative integer")
        if self.hash_basis not in {"raw-response-bytes-v1", "canonical-json-v1"}:
            raise _error("hash_basis", "is unsupported")
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "dataset", dataset)
        object.__setattr__(self, "source_ref", source_ref)
        object.__setattr__(self, "contract_version", contract_version)
        object.__setattr__(self, "payload_sha256", payload_sha256)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ordinal": self.ordinal,
            "provider": self.provider,
            "dataset": self.dataset,
            "source_ref": self.source_ref,
            "contract_version": self.contract_version,
            "payload_sha256": self.payload_sha256,
            "payload_size_bytes": self.payload_size_bytes,
            "hash_basis": self.hash_basis,
        }


def _observation_sort_key(item: DatasetObservation) -> tuple[Any, ...]:
    return (
        item.trade_date,
        item.provider,
        item.source_role.value,
        item.source_run_id,
        item.selected,
        item.observation_sha256,
    )


@dataclass(frozen=True, slots=True)
class MixedDatasetVersion:
    """Immutable pure dataset version ready for an explicit repository."""

    identity: DatasetVersionIdentity
    provenance_summary: DatasetProvenanceSummary
    observations: tuple[DatasetObservation, ...]
    artifacts: tuple[DatasetArtifactRef, ...]
    coverage_basis: CoverageBasis
    canonical_sha256: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.identity, DatasetVersionIdentity):
            raise _error("identity", "must be DatasetVersionIdentity")
        if not isinstance(self.provenance_summary, DatasetProvenanceSummary):
            raise _error("provenance_summary", "must be DatasetProvenanceSummary")
        observations = tuple(self.observations)
        artifacts = tuple(sorted(self.artifacts, key=lambda item: item.ordinal))
        if not observations:
            raise _error("observations", "must not be empty")
        if any(not isinstance(item, DatasetObservation) for item in observations):
            raise _error("observations", "must contain DatasetObservation values")
        if any(not isinstance(item, DatasetArtifactRef) for item in artifacts):
            raise _error("artifacts", "must contain DatasetArtifactRef values")
        ordinals = tuple(item.ordinal for item in artifacts)
        if ordinals and ordinals != tuple(range(1, len(ordinals) + 1)):
            raise _error("artifacts", "ordinals must be contiguous from one")
        if self.identity.coverage != self.provenance_summary.coverage:
            raise _error("coverage", "identity and provenance summary differ")
        if (
            self.identity.provenance_map_sha256
            != self.provenance_summary.provenance_map_sha256
        ):
            raise _error("provenance_map_sha256", "identity and summary differ")
        if self.identity.symbol not in {item.symbol for item in observations}:
            raise _error("observations", "must belong to the dataset symbol")
        provenance = tuple(item.as_provenance() for item in observations)
        validate_dataset_provenance(
            self.provenance_summary,
            provenance,
            symbol=self.identity.symbol,
        )
        observation_providers = {item.provider for item in observations}
        artifact_providers = {item.provider for item in artifacts}
        if not observation_providers <= artifact_providers:
            raise _error("artifacts", "must cover every observation provider")
        basis = _coerce_basis(self.coverage_basis)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "coverage_basis", basis)
        computed = sha256_text(canonical_json(self._canonical_payload()))
        if self.canonical_sha256 is not None:
            stored = _sha256(self.canonical_sha256, "canonical_sha256")
            if stored != computed:
                raise _error("canonical_sha256", "does not match canonical serialization")
        object.__setattr__(self, "canonical_sha256", computed)

    def _canonical_payload(self) -> dict[str, Any]:
        return {
            "contract_version": DATASET_PERSISTENCE_CONTRACT_VERSION,
            "identity": self.identity.as_dict(),
            "provenance_summary": self.provenance_summary.as_dict(),
            "coverage_basis": self.coverage_basis.value,
            "observations": [
                item.as_dict() for item in sorted(self.observations, key=_observation_sort_key)
            ],
            "artifacts": [item.as_dict() for item in self.artifacts],
        }

    def canonical_json(self) -> str:
        return canonical_json(self._canonical_payload())

    def as_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity.as_dict(),
            "provenance_summary": self.provenance_summary.as_dict(),
            "coverage_basis": self.coverage_basis.value,
            "observations": [
                item.as_dict() for item in sorted(self.observations, key=_observation_sort_key)
            ],
            "artifacts": [item.as_dict() for item in self.artifacts],
            "canonical_sha256": self.canonical_sha256,
        }


def _validate_selected_dates(
    observations: Iterable[DatasetObservation],
) -> tuple[DatasetObservation, ...]:
    items = tuple(observations)
    selected_dates = [item.trade_date for item in items if item.selected]
    if len(selected_dates) != len(set(selected_dates)):
        raise _error("observations", "must contain at most one selected row per trade date")
    return items


def _build_version(
    *,
    symbol: str,
    as_of_date: date,
    required_observation_count: int,
    observations: Iterable[DatasetObservation],
    artifacts: Iterable[DatasetArtifactRef],
    source_status: DatasetSourceStatus,
    authority_status: AuthorityStatus,
    reconciliation_status: ReconciliationStatus,
    methodology_version: str,
    source_policy: str,
    coverage_basis: CoverageBasis | str,
    discrepancy_count: int,
    parent_dataset_version_id: str | None = None,
) -> MixedDatasetVersion:
    normalized_symbol = _symbol(symbol, "symbol")
    items = _validate_selected_dates(observations)
    canonical = tuple(
        item for item in items if item.source_role is SourceRole.CANONICAL and item.selected
    )
    supplemental = tuple(
        item
        for item in items
        if item.source_role is SourceRole.SUPPLEMENTAL and item.selected
    )
    coverage = DatasetCoverage(
        required_observation_count=required_observation_count,
        twse_observation_count=len(canonical),
        esun_supplemental_count=len(supplemental),
        missing_twse_count=required_observation_count - len(canonical),
        discrepancy_count=discrepancy_count,
    )
    if coverage.selected_observation_count != required_observation_count:
        raise IncompleteDatasetCoverageError(
            "formal dataset version requires selected coverage to equal required coverage"
        )
    summary = DatasetProvenanceSummary.from_observations(
        tuple(item.as_provenance() for item in items),
        source_status=source_status,
        authority_status=authority_status,
        reconciliation_status=reconciliation_status,
        coverage=coverage,
        symbol=normalized_symbol,
    )
    identity = DatasetVersionIdentity.from_summary(
        summary,
        symbol=normalized_symbol,
        as_of_date=_date(as_of_date, "as_of_date"),
        methodology_version=methodology_version,
        source_policy=source_policy,
        parent_dataset_version_id=parent_dataset_version_id,
    )
    return MixedDatasetVersion(
        identity=identity,
        provenance_summary=summary,
        observations=items,
        artifacts=tuple(artifacts),
        coverage_basis=_coerce_basis(coverage_basis),
    )


def build_provisional_dataset_version(
    *,
    symbol: str,
    as_of_date: date,
    required_observation_count: int,
    twse_observations: Iterable[DatasetObservation],
    eligibility_result: SupplementalEligibilityResult,
    esun_observations: Iterable[DatasetObservation],
    artifacts: Iterable[DatasetArtifactRef],
    methodology_version: str,
    coverage_basis: CoverageBasis | str = CoverageBasis.STANDARD,
    source_policy: str = TWSE_DUAL_SOURCE_POLICY,
) -> MixedDatasetVersion:
    """Build a formal provisional version only from an approved complete DS2 set."""

    if not isinstance(eligibility_result, SupplementalEligibilityResult):
        raise _error("eligibility_result", "must be SupplementalEligibilityResult")
    if not eligibility_result.eligible:
        raise DatasetPersistenceContractError(
            "DS2 eligibility result is rejected; provisional dataset is forbidden"
        )
    if not eligibility_result.coverage_complete:
        raise IncompleteDatasetCoverageError(
            "DS2 source is eligible but supplemental coverage is incomplete"
        )
    twse = tuple(twse_observations)
    esun = tuple(esun_observations)
    if any(item.source_role is not SourceRole.CANONICAL or not item.selected for item in twse):
        raise _error("twse_observations", "must be selected canonical observations")
    if any(
        item.source_role is not SourceRole.SUPPLEMENTAL or not item.selected
        for item in esun
    ):
        raise _error("esun_observations", "must be selected supplemental observations")
    missing_dates = set(eligibility_result.missing_twse_dates)
    esun_dates = {item.trade_date for item in esun}
    if esun_dates != set(eligibility_result.eligible_observation_dates):
        raise _error(
            "esun_observations", "dates must equal the DS2 eligible observation dates"
        )
    if esun_dates - missing_dates:
        raise _error("esun_observations", "must not cover a TWSE-present date")
    if {item.trade_date for item in twse} & esun_dates:
        raise _error("observations", "selected TWSE and E.SUN dates must not overlap")
    return _build_version(
        symbol=symbol,
        as_of_date=as_of_date,
        required_observation_count=required_observation_count,
        observations=twse + esun,
        artifacts=artifacts,
        source_status=DatasetSourceStatus.PROVISIONAL_MIXED,
        authority_status=AuthorityStatus.INCOMPLETE,
        reconciliation_status=ReconciliationStatus.PENDING,
        methodology_version=methodology_version,
        source_policy=source_policy,
        coverage_basis=coverage_basis,
        discrepancy_count=0,
    )


def build_canonical_dataset_version(
    *,
    symbol: str,
    as_of_date: date,
    required_observation_count: int,
    twse_observations: Iterable[DatasetObservation],
    validation_observations: Iterable[DatasetObservation],
    artifacts: Iterable[DatasetArtifactRef],
    methodology_version: str,
    discrepancy_count: int = 0,
    coverage_basis: CoverageBasis | str = CoverageBasis.STANDARD,
    source_policy: str = TWSE_DUAL_SOURCE_POLICY,
) -> MixedDatasetVersion:
    """Build canonical-complete fixture without creating a supplemental path."""

    twse = tuple(twse_observations)
    validation = tuple(validation_observations)
    if any(item.source_role is not SourceRole.CANONICAL or not item.selected for item in twse):
        raise _error("twse_observations", "must be selected canonical observations")
    if any(
        item.source_role is not SourceRole.VALIDATION or item.selected
        for item in validation
    ):
        raise _error("validation_observations", "must be unselected validation observations")
    return _build_version(
        symbol=symbol,
        as_of_date=as_of_date,
        required_observation_count=required_observation_count,
        observations=twse + validation,
        artifacts=artifacts,
        source_status=DatasetSourceStatus.CANONICAL_COMPLETE,
        authority_status=AuthorityStatus.COMPLETE,
        reconciliation_status=ReconciliationStatus.NOT_APPLICABLE,
        methodology_version=methodology_version,
        source_policy=source_policy,
        coverage_basis=coverage_basis,
        discrepancy_count=discrepancy_count,
    )


__all__ = [
    "CoverageBasis",
    "DATASET_PERSISTENCE_CONTRACT_VERSION",
    "DatasetArtifactRef",
    "DatasetObservation",
    "DatasetPersistenceContractError",
    "IncompleteDatasetCoverageError",
    "MixedDatasetVersion",
    "build_canonical_dataset_version",
    "build_provisional_dataset_version",
]
