"""Pure read-only research dataset contract for M9.

The contract owns immutable read models and source-policy validation only.  It
has no provider, network, SQLite, migration, or report dependency.  The
in-memory fake exists for contract tests and future consumer composition.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from typing import ClassVar, Protocol, runtime_checkable
from urllib.parse import parse_qsl, urlsplit


TWSE_BASELINE_SOURCE_POLICY = "twse_baseline"
TWSE_BASELINE_SOURCES = frozenset({"twse", "twse-historical"})
ESUN_VALIDATION_SOURCES = frozenset({"esun", "esun-historical"})
MOCK_SYNTHETIC_SOURCE = "mock-synthetic"

_FORMAL_VALIDATION_SOURCES = TWSE_BASELINE_SOURCES | ESUN_VALIDATION_SOURCES
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SENSITIVE_QUERY_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "access_token",
        "api_token",
        "api-token",
        "x-api-key",
        "x_api_key",
        "authorization",
        "auth",
        "bearer",
        "credential",
        "credentials",
        "key",
        "password",
        "secret",
        "signature",
        "token",
        "client_secret",
        "private_key",
        "x-amz-credential",
        "x-amz-signature",
    }
)


class DatasetSourcePolicyError(ValueError):
    """Dataset evidence violates the frozen ``twse_baseline`` source policy."""


@dataclass(frozen=True, slots=True)
class ResearchDatasetRequest:
    """One immutable request for a symbol snapshot as of a market date."""

    symbol: str
    as_of_date: date
    history_observations: int | None = None
    pipeline_run_id: str | None = None
    historical_run_id: str | None = None
    validation_run_id: str | None = None
    dataset_version_id: str | None = None
    source_policy: str = field(
        default=TWSE_BASELINE_SOURCE_POLICY,
        init=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(
            self,
            "as_of_date",
            _require_date(self.as_of_date, "as_of_date"),
        )
        if self.history_observations is not None and (
            isinstance(self.history_observations, bool)
            or not isinstance(self.history_observations, int)
            or self.history_observations < 1
        ):
            raise ValueError("history_observations must be a positive integer or None")
        for name in (
            "pipeline_run_id",
            "historical_run_id",
            "validation_run_id",
            "dataset_version_id",
        ):
            object.__setattr__(
                self,
                name,
                _normalize_optional_text(getattr(self, name), name),
            )


@dataclass(frozen=True, slots=True)
class DatasetSymbol:
    """Provider-neutral symbol identity included in every snapshot."""

    symbol: str
    name: str | None
    market: str
    currency: str
    is_active: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(
            self,
            "name",
            _normalize_optional_text(self.name, "name"),
        )
        object.__setattr__(self, "market", _require_text(self.market, "market"))
        object.__setattr__(
            self,
            "currency",
            _require_text(self.currency, "currency").upper(),
        )
        if not isinstance(self.is_active, bool):
            raise TypeError("is_active must be a bool")


@dataclass(frozen=True, slots=True)
class DatasetPrice:
    """One canonical price observation; absence is represented by no row."""

    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str
    source_role: str = "canonical"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(
            self,
            "trade_date",
            _require_date(self.trade_date, "trade_date"),
        )
        for name in ("open", "high", "low", "close"):
            object.__setattr__(
                self,
                name,
                _require_finite_number(getattr(self, name), name, positive=True),
            )
        if (
            isinstance(self.volume, bool)
            or not isinstance(self.volume, int)
            or self.volume < 0
        ):
            raise ValueError("volume must be a non-negative integer")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("high must be at least open, low, and close")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("low must be at most open, high, and close")
        object.__setattr__(self, "source", _normalize_source(self.source))
        if self.source_role not in {"canonical", "supplemental", "validation"}:
            raise ValueError("source_role is unsupported")


@dataclass(frozen=True, slots=True)
class DatasetValuationMetric:
    """One canonical valuation fact selected independently by metric name."""

    symbol: str
    metric_date: date
    name: str
    value: float
    unit: str | None
    source: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(
            self,
            "metric_date",
            _require_date(self.metric_date, "metric_date"),
        )
        object.__setattr__(self, "name", _require_text(self.name, "name"))
        object.__setattr__(
            self,
            "value",
            _require_finite_number(self.value, "value"),
        )
        object.__setattr__(
            self,
            "unit",
            _normalize_optional_text(self.unit, "unit"),
        )
        object.__setattr__(self, "source", _normalize_source(self.source))


@dataclass(frozen=True, slots=True)
class DatasetValidationObservation:
    """Independent provider observation retained as validation evidence."""

    symbol: str
    provider: str
    market_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source_endpoints: tuple[str, ...]
    fetched_at: datetime
    source_timestamp: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "provider", _normalize_source(self.provider))
        object.__setattr__(
            self,
            "market_date",
            _require_date(self.market_date, "market_date"),
        )
        for name in ("open", "high", "low", "close"):
            object.__setattr__(
                self,
                name,
                _require_finite_number(getattr(self, name), name, positive=True),
            )
        if (
            isinstance(self.volume, bool)
            or not isinstance(self.volume, int)
            or self.volume < 0
        ):
            raise ValueError("volume must be a non-negative integer")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("high must be at least open, low, and close")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("low must be at most open, high, and close")
        endpoints = tuple(
            sorted({_safe_endpoint(item, "source_endpoints") for item in self.source_endpoints})
        )
        if not endpoints:
            raise ValueError("validation observation requires source_endpoints")
        object.__setattr__(self, "source_endpoints", endpoints)
        object.__setattr__(
            self,
            "fetched_at",
            _require_aware_datetime(self.fetched_at, "fetched_at"),
        )
        if self.source_timestamp is not None:
            object.__setattr__(
                self,
                "source_timestamp",
                _require_aware_datetime(
                    self.source_timestamp,
                    "source_timestamp",
                ),
            )


@dataclass(frozen=True, slots=True)
class DatasetDiscrepancy:
    """Source-preserving discrepancy without sentinel values."""

    field: str
    left_value: str | int | float | None
    right_value: str | int | float | None
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "field", _require_text(self.field, "field"))
        object.__setattr__(self, "reason", _require_text(self.reason, "reason"))
        if self.left_value is None and self.right_value is None:
            raise ValueError("discrepancy must retain at least one source value")
        for name in ("left_value", "right_value"):
            value = getattr(self, name)
            if isinstance(value, str) and not value.strip():
                raise ValueError(f"{name} must be None rather than an empty string")
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"{name} must be finite")


@dataclass(frozen=True, slots=True)
class PriceHistoryReadModel:
    """Sorted canonical history and exact-date current-price evidence."""

    symbol: str
    as_of_date: date
    status: str
    observations: tuple[DatasetPrice, ...]
    current: DatasetPrice | None
    total_observations_as_of: int
    requested_observations: int | None
    is_truncated: bool

    def __post_init__(self) -> None:
        symbol = _normalize_symbol(self.symbol)
        as_of_date = _require_date(self.as_of_date, "as_of_date")
        observations = tuple(self.observations)
        if self.status not in {
            "available",
            "missing_source",
            "market_date_mismatch",
        }:
            raise ValueError("unsupported price history status")
        if any(item.symbol != symbol for item in observations):
            raise ValueError("price history contains another symbol")
        if any(item.trade_date > as_of_date for item in observations):
            raise ValueError("price history must not contain future rows")
        dates = tuple(item.trade_date for item in observations)
        if dates != tuple(sorted(dates)) or len(set(dates)) != len(dates):
            raise ValueError("price history must be uniquely sorted by trade_date")
        if (
            isinstance(self.total_observations_as_of, bool)
            or not isinstance(self.total_observations_as_of, int)
            or self.total_observations_as_of < len(observations)
        ):
            raise ValueError("total_observations_as_of is inconsistent")
        if self.requested_observations is not None and (
            isinstance(self.requested_observations, bool)
            or not isinstance(self.requested_observations, int)
            or self.requested_observations < 1
            or len(observations) > self.requested_observations
        ):
            raise ValueError("requested_observations is inconsistent")
        expected_truncated = self.total_observations_as_of > len(observations)
        if self.is_truncated is not expected_truncated:
            raise ValueError("is_truncated does not match observation counts")
        if self.requested_observations is None and self.is_truncated:
            raise ValueError("None history_observations must return complete history")
        latest = observations[-1] if observations else None
        if self.status == "missing_source":
            if latest is not None or self.current is not None:
                raise ValueError("missing price history cannot expose a current value")
        elif self.status == "available":
            if latest is None or latest.trade_date != as_of_date or self.current != latest:
                raise ValueError("available current price must be exact as_of_date")
        elif latest is None or latest.trade_date >= as_of_date or self.current is not None:
            raise ValueError("market_date_mismatch requires only earlier history")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "as_of_date", as_of_date)
        object.__setattr__(self, "observations", observations)


@dataclass(frozen=True, slots=True)
class ValuationReadModel:
    """Nearest canonical valuation value per metric at or before as-of."""

    symbol: str
    as_of_date: date
    status: str
    metrics: tuple[DatasetValuationMetric, ...]

    def __post_init__(self) -> None:
        symbol = _normalize_symbol(self.symbol)
        as_of_date = _require_date(self.as_of_date, "as_of_date")
        metrics = tuple(self.metrics)
        if self.status not in {"available", "missing_source"}:
            raise ValueError("unsupported valuation status")
        if any(item.symbol != symbol for item in metrics):
            raise ValueError("valuation contains another symbol")
        if any(item.metric_date > as_of_date for item in metrics):
            raise ValueError("valuation must not contain future rows")
        names = tuple(item.name for item in metrics)
        if names != tuple(sorted(names)) or len(set(names)) != len(names):
            raise ValueError("valuation metrics must be unique and sorted by name")
        if self.status == "missing_source" and metrics:
            raise ValueError("missing valuation cannot expose values")
        if self.status == "available" and not metrics:
            raise ValueError("available valuation requires at least one value")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "as_of_date", as_of_date)
        object.__setattr__(self, "metrics", metrics)


@dataclass(frozen=True, slots=True)
class ValidationReadModel:
    """Selected independent validation run and source-preserving evidence."""

    symbol: str
    status: str = "missing_source"
    run_id: str | None = None
    target_date: date | None = None
    left_provider: str | None = None
    right_provider: str | None = None
    outcome: str | None = None
    created_at: datetime | None = None
    observations: tuple[DatasetValidationObservation, ...] = ()
    discrepancies: tuple[DatasetDiscrepancy, ...] = ()

    def __post_init__(self) -> None:
        symbol = _normalize_symbol(self.symbol)
        if self.status not in {
            "available",
            "missing_source",
            "market_date_mismatch",
            "source_discrepancy",
        }:
            raise ValueError("unsupported validation status")
        observations = tuple(self.observations)
        discrepancies = tuple(
            sorted(self.discrepancies, key=lambda item: (item.field, item.reason))
        )
        if self.status == "missing_source":
            if any(
                value is not None
                for value in (
                    self.run_id,
                    self.target_date,
                    self.left_provider,
                    self.right_provider,
                    self.outcome,
                    self.created_at,
                )
            ) or observations or discrepancies:
                raise ValueError("missing validation cannot expose run evidence")
        else:
            run_id = _require_text(self.run_id, "run_id")
            target_date = _require_date(self.target_date, "target_date")
            left = _normalize_source(self.left_provider)
            right = _normalize_source(self.right_provider)
            created_at = _require_aware_datetime(self.created_at, "created_at")
            if left == right:
                raise ValueError("validation providers must be different")
            if self.outcome not in {"match", "discrepancy"}:
                raise ValueError("validation outcome must be match or discrepancy")
            if any(item.symbol != symbol for item in observations):
                raise ValueError("validation contains another symbol")
            providers = {item.provider for item in observations}
            if observations and providers != {left, right}:
                raise ValueError("validation observations do not match provider pair")
            order = {left: 0, right: 1}
            observations = tuple(
                sorted(
                    observations,
                    key=lambda item: (order.get(item.provider, 2), item.provider),
                )
            )
            if self.status == "available" and (
                self.outcome != "match" or discrepancies
            ):
                raise ValueError("available validation must be a match")
            if self.status == "source_discrepancy" and (
                self.outcome != "discrepancy"
                or not discrepancies
                or any(item.field == "market_date" for item in discrepancies)
            ):
                raise ValueError("source_discrepancy requires non-date discrepancies")
            if self.status == "market_date_mismatch" and (
                self.outcome != "discrepancy"
                or not any(item.field == "market_date" for item in discrepancies)
            ):
                raise ValueError("market_date_mismatch requires date discrepancy")
            object.__setattr__(self, "run_id", run_id)
            object.__setattr__(self, "target_date", target_date)
            object.__setattr__(self, "left_provider", left)
            object.__setattr__(self, "right_provider", right)
            object.__setattr__(self, "created_at", created_at)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "discrepancies", discrepancies)


@dataclass(frozen=True, slots=True)
class DatasetArtifactRef:
    """Hash-only M7 evidence reference; raw responses and headers are absent."""

    owner_kind: str
    owner_run_id: str
    provider: str
    dataset: str
    endpoint: str
    contract_version: str
    content_type: str
    payload_sha256: str
    payload_size_bytes: int
    hash_basis: str
    fetched_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.owner_kind not in {"pipeline", "historical", "validation", "dataset_version"}:
            raise ValueError("unsupported artifact owner_kind")
        object.__setattr__(
            self,
            "owner_run_id",
            _require_text(self.owner_run_id, "owner_run_id"),
        )
        object.__setattr__(self, "provider", _normalize_source(self.provider))
        for name in ("dataset", "contract_version", "content_type"):
            object.__setattr__(
                self,
                name,
                _require_text(getattr(self, name), name),
            )
        object.__setattr__(self, "endpoint", _safe_endpoint(self.endpoint, "endpoint"))
        if not isinstance(self.payload_sha256, str) or not _SHA256.fullmatch(
            self.payload_sha256
        ):
            raise ValueError("payload_sha256 must be lowercase SHA-256")
        if (
            isinstance(self.payload_size_bytes, bool)
            or not isinstance(self.payload_size_bytes, int)
            or self.payload_size_bytes < 0
        ):
            raise ValueError("payload_size_bytes must be non-negative")
        if self.hash_basis not in {"raw-response-bytes-v1", "canonical-json-v1"}:
            raise ValueError("unsupported artifact hash_basis")
        if self.fetched_at is not None:
            object.__setattr__(
                self,
                "fetched_at",
                _require_aware_datetime(self.fetched_at, "fetched_at"),
            )


@dataclass(frozen=True, slots=True)
class DatasetProvenance:
    """Run ids, safe endpoints, source roles, and optional hash-only artifacts."""

    symbol: str
    pipeline_run_id: str | None = None
    historical_run_id: str | None = None
    validation_run_id: str | None = None
    canonical_sources: tuple[str, ...] = ()
    validation_sources: tuple[str, ...] = ()
    source_endpoints: tuple[str, ...] = ()
    artifact_refs: tuple[DatasetArtifactRef, ...] = ()
    fetched_at: datetime | None = None
    source_policy: str = TWSE_BASELINE_SOURCE_POLICY
    dataset_version_id: str | None = None
    source_status: str = "canonical_complete"
    authority_status: str = "complete"
    reconciliation_status: str = "not_applicable"
    research_data_quality: str = "canonical"
    canonical_authority: str = "twse"
    supplemental_sources: tuple[str, ...] = ()
    twse_observation_count: int = 0
    esun_supplemental_count: int = 0
    missing_twse_count: int = 0
    discrepancy_count: int = 0
    provenance_map_sha256: str | None = None
    parent_dataset_version_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        if self.source_policy not in {
            TWSE_BASELINE_SOURCE_POLICY,
            "twse_dual_source_v1",
        }:
            raise DatasetSourcePolicyError("unsupported dataset source policy")
        if self.dataset_version_id is not None:
            if not _SHA256.fullmatch(self.dataset_version_id):
                raise ValueError("dataset_version_id must be lowercase SHA-256")
            if self.source_policy != "twse_dual_source_v1":
                raise DatasetSourcePolicyError(
                    "dataset_version_id requires the dual-source policy"
                )
        if self.source_status not in {
            "canonical_complete",
            "provisional_mixed",
            "reconciled",
        }:
            raise ValueError("unsupported source_status")
        if self.authority_status not in {"complete", "incomplete", "reconciled"}:
            raise ValueError("unsupported authority_status")
        if self.reconciliation_status not in {
            "not_applicable",
            "pending",
            "reconciled_equal",
            "reconciled_discrepant",
        }:
            raise ValueError("unsupported reconciliation_status")
        if self.research_data_quality not in {"canonical", "provisional", "reconciled"}:
            raise ValueError("unsupported research_data_quality")
        if self.canonical_authority != "twse":
            raise DatasetSourcePolicyError("canonical authority must remain twse")
        for name in (
            "twse_observation_count",
            "esun_supplemental_count",
            "missing_twse_count",
            "discrepancy_count",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.provenance_map_sha256 is not None and not _SHA256.fullmatch(
            self.provenance_map_sha256
        ):
            raise ValueError("provenance_map_sha256 must be lowercase SHA-256")
        if self.parent_dataset_version_id is not None and not _SHA256.fullmatch(
            self.parent_dataset_version_id
        ):
            raise ValueError("parent_dataset_version_id must be lowercase SHA-256")
        for name in (
            "pipeline_run_id",
            "historical_run_id",
            "validation_run_id",
        ):
            object.__setattr__(
                self,
                name,
                _normalize_optional_text(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "canonical_sources",
            tuple(sorted({_normalize_source(item) for item in self.canonical_sources})),
        )
        object.__setattr__(
            self,
            "validation_sources",
            tuple(sorted({_normalize_source(item) for item in self.validation_sources})),
        )
        object.__setattr__(
            self,
            "supplemental_sources",
            tuple(sorted({_normalize_source(item) for item in self.supplemental_sources})),
        )
        object.__setattr__(
            self,
            "source_endpoints",
            tuple(
                sorted(
                    {
                        _safe_endpoint(item, "source_endpoints")
                        for item in self.source_endpoints
                    }
                )
            ),
        )
        object.__setattr__(
            self,
            "artifact_refs",
            tuple(
                sorted(
                    self.artifact_refs,
                    key=lambda item: (
                        item.owner_kind,
                        item.owner_run_id,
                        item.provider,
                        item.dataset,
                        item.endpoint,
                        item.payload_sha256,
                    ),
                )
            ),
        )
        if self.fetched_at is not None:
            object.__setattr__(
                self,
                "fetched_at",
                _require_aware_datetime(self.fetched_at, "fetched_at"),
            )


@dataclass(frozen=True, slots=True)
class DatasetAsOf:
    """Request cutoff and explicit history-window accounting."""

    as_of_date: date
    history_observations: int | None
    total_history_observations: int
    returned_history_observations: int
    history_is_truncated: bool
    source_policy: str = TWSE_BASELINE_SOURCE_POLICY

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "as_of_date",
            _require_date(self.as_of_date, "as_of_date"),
        )
        if self.source_policy not in {
            TWSE_BASELINE_SOURCE_POLICY,
            "twse_dual_source_v1",
        }:
            raise DatasetSourcePolicyError("unsupported as_of source policy")
        if self.history_observations is not None and (
            isinstance(self.history_observations, bool)
            or not isinstance(self.history_observations, int)
            or self.history_observations < 1
        ):
            raise ValueError("history_observations must be positive or None")
        for name in (
            "total_history_observations",
            "returned_history_observations",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.returned_history_observations > self.total_history_observations:
            raise ValueError("returned history cannot exceed total history")
        expected = (
            self.returned_history_observations < self.total_history_observations
        )
        if self.history_is_truncated is not expected:
            raise ValueError("history_is_truncated is inconsistent")
        if self.history_observations is None and self.history_is_truncated:
            raise ValueError("None history_observations must not truncate history")


class TwseBaselineSourcePolicy:
    """Pure role-aware source allowlist for formal and synthetic snapshots."""

    __slots__ = ()

    name: ClassVar[str] = TWSE_BASELINE_SOURCE_POLICY
    canonical_family: ClassVar[frozenset[str]] = TWSE_BASELINE_SOURCES
    validation_family: ClassVar[frozenset[str]] = _FORMAL_VALIDATION_SOURCES
    synthetic_source: ClassVar[str] = MOCK_SYNTHETIC_SOURCE

    @classmethod
    def validate_sources(
        cls,
        *,
        canonical_sources: Iterable[str],
        validation_sources: Iterable[str],
        artifact_sources: Iterable[str],
        has_canonical_history: bool,
    ) -> None:
        canonical = {_normalize_source(item) for item in canonical_sources}
        validation = {_normalize_source(item) for item in validation_sources}
        artifacts = {_normalize_source(item) for item in artifact_sources}
        all_sources = canonical | validation | artifacts

        if cls.synthetic_source in all_sources:
            mixed = all_sources - {cls.synthetic_source}
            if mixed:
                raise DatasetSourcePolicyError(
                    "mock-synthetic must not mix with formal sources: "
                    + ", ".join(sorted(mixed))
                )
            if canonical != {cls.synthetic_source} or not has_canonical_history:
                raise DatasetSourcePolicyError(
                    "mock-synthetic requires a complete synthetic canonical history"
                )
            return

        invalid_canonical = canonical - cls.canonical_family
        if invalid_canonical:
            raise DatasetSourcePolicyError(
                "canonical history/valuation source is outside TWSE baseline: "
                + ", ".join(sorted(invalid_canonical))
            )
        invalid_validation = validation - cls.validation_family
        if invalid_validation:
            raise DatasetSourcePolicyError(
                "validation source is not an allowed TWSE/E.SUN family: "
                + ", ".join(sorted(invalid_validation))
            )
        invalid_artifacts = artifacts - cls.validation_family
        if invalid_artifacts:
            raise DatasetSourcePolicyError(
                "provenance artifact source is unknown: "
                + ", ".join(sorted(invalid_artifacts))
            )

    @classmethod
    def validate_models(
        cls,
        *,
        history: PriceHistoryReadModel,
        valuation: ValuationReadModel,
        validation: ValidationReadModel,
        provenance: DatasetProvenance,
    ) -> None:
        canonical_sources = {
            *(item.source for item in history.observations),
            *(item.source for item in valuation.metrics),
            *provenance.canonical_sources,
        }
        validation_sources = {
            *(item.provider for item in validation.observations),
            *(
                item
                for item in (
                    validation.left_provider,
                    validation.right_provider,
                )
                if item is not None
            ),
            *provenance.validation_sources,
        }
        cls.validate_sources(
            canonical_sources=canonical_sources,
            validation_sources=validation_sources,
            artifact_sources=(item.provider for item in provenance.artifact_refs),
            has_canonical_history=bool(history.observations),
        )


def _validate_dual_source_models(
    *,
    history: PriceHistoryReadModel,
    valuation: ValuationReadModel,
    validation: ValidationReadModel,
    provenance: DatasetProvenance,
) -> None:
    """Validate the DS5 read model without changing the frozen M9 math."""

    if provenance.dataset_version_id is None:
        raise DatasetSourcePolicyError(
            "dual-source snapshots require an explicit dataset_version_id"
        )
    canonical = {
        *provenance.canonical_sources,
        *(item.source for item in history.observations if item.source_role == "canonical"),
        *(item.source for item in valuation.metrics),
    }
    supplemental = {
        *provenance.supplemental_sources,
        *(item.source for item in history.observations if item.source_role == "supplemental"),
    }
    validation_sources = {
        *provenance.validation_sources,
        *(item.provider for item in validation.observations),
        *(item for item in (validation.left_provider, validation.right_provider) if item),
    }
    if not canonical or not canonical <= TWSE_BASELINE_SOURCES:
        raise DatasetSourcePolicyError(
            "dual-source canonical observations must remain TWSE-authoritative"
        )
    if supplemental and not supplemental <= {"esun", "esun-historical"}:
        raise DatasetSourcePolicyError(
            "dual-source supplemental observations must remain E.SUN-only"
        )
    if not validation_sources <= (TWSE_BASELINE_SOURCES | _FORMAL_VALIDATION_SOURCES):
        raise DatasetSourcePolicyError("dual-source validation source is unsupported")
    if any(item.source_role == "validation" for item in history.observations):
        raise DatasetSourcePolicyError("validation rows must not enter selected history")
    if provenance.esun_supplemental_count != len(
        [item for item in history.observations if item.source_role == "supplemental"]
    ):
        raise DatasetSourcePolicyError("supplemental coverage count is inconsistent")
    expected_quality = {
        "canonical_complete": ("complete", "not_applicable", "canonical"),
        "provisional_mixed": ("incomplete", "pending", "provisional"),
        "reconciled": ("reconciled", None, "reconciled"),
    }
    expected = expected_quality[provenance.source_status]
    if provenance.authority_status != expected[0] or provenance.research_data_quality != expected[2]:
        raise DatasetSourcePolicyError("dataset status and research data quality disagree")
    if expected[1] is not None and provenance.reconciliation_status != expected[1]:
        raise DatasetSourcePolicyError("dataset status and reconciliation status disagree")
    if provenance.source_status == "reconciled" and provenance.reconciliation_status not in {
        "reconciled_equal",
        "reconciled_discrepant",
    }:
        raise DatasetSourcePolicyError("reconciled dataset requires a reconciliation result")
    if provenance.source_status == "canonical_complete" and supplemental:
        raise DatasetSourcePolicyError("canonical-complete dataset cannot contain supplemental rows")
    if provenance.source_status == "reconciled" and supplemental:
        raise DatasetSourcePolicyError("reconciled dataset cannot select supplemental rows")


@dataclass(frozen=True, slots=True)
class ResearchDatasetSnapshot:
    """Complete immutable read result for one symbol and as-of request."""

    symbol: DatasetSymbol
    as_of: DatasetAsOf
    price_history: PriceHistoryReadModel
    valuation: ValuationReadModel
    validation: ValidationReadModel
    provenance: DatasetProvenance

    def __post_init__(self) -> None:
        symbol = self.symbol.symbol
        if any(
            item != symbol
            for item in (
                self.price_history.symbol,
                self.valuation.symbol,
                self.validation.symbol,
                self.provenance.symbol,
            )
        ):
            raise ValueError("snapshot read models must share one symbol")
        if (
            self.price_history.as_of_date != self.as_of.as_of_date
            or self.valuation.as_of_date != self.as_of.as_of_date
        ):
            raise ValueError("snapshot read models must share one as_of_date")
        if self.as_of.total_history_observations != (
            self.price_history.total_observations_as_of
        ) or self.as_of.returned_history_observations != len(
            self.price_history.observations
        ):
            raise ValueError("snapshot history accounting is inconsistent")
        if self.as_of.history_observations != self.price_history.requested_observations:
            raise ValueError("snapshot history request is inconsistent")
        if self.as_of.history_is_truncated != self.price_history.is_truncated:
            raise ValueError("snapshot truncation state is inconsistent")
        if (
            self.validation.status != "missing_source"
            and self.validation.target_date != self.as_of.as_of_date
        ):
            raise ValueError("validation target must equal snapshot as_of_date")
        if any(
            item.market_date > self.as_of.as_of_date
            for item in self.validation.observations
        ):
            raise ValueError("validation must not contain future observations")
        if self.provenance.source_policy == TWSE_BASELINE_SOURCE_POLICY:
            TwseBaselineSourcePolicy.validate_models(
                history=self.price_history,
                valuation=self.valuation,
                validation=self.validation,
                provenance=self.provenance,
            )
        else:
            _validate_dual_source_models(
                history=self.price_history,
                valuation=self.valuation,
                validation=self.validation,
                provenance=self.provenance,
            )
        if self.as_of.source_policy != self.provenance.source_policy:
            raise DatasetSourcePolicyError(
                "snapshot as_of and provenance source policies must match"
            )


@runtime_checkable
class ResearchDataset(Protocol):
    """Read-only dataset boundary; no persistence or acquisition surface."""

    def read(
        self,
        request: ResearchDatasetRequest,
        /,
    ) -> ResearchDatasetSnapshot:
        """Return one immutable snapshot for the supplied request."""


class FakeResearchDataset:
    """Deterministic in-memory implementation used to verify the contract."""

    __slots__ = (
        "_symbols",
        "_prices",
        "_valuations",
        "_validations",
        "_provenance",
    )

    def __init__(
        self,
        *,
        symbols: Iterable[DatasetSymbol],
        prices: Iterable[DatasetPrice] = (),
        valuations: Iterable[DatasetValuationMetric] = (),
        validations: Iterable[ValidationReadModel] = (),
        provenance: Iterable[DatasetProvenance] = (),
    ) -> None:
        symbol_items = tuple(symbols)
        self._symbols = {item.symbol: item for item in symbol_items}
        if len(self._symbols) != len(symbol_items):
            raise ValueError("fake dataset symbols must be unique")
        self._prices = tuple(prices)
        self._valuations = tuple(valuations)
        self._validations = tuple(validations)
        provenance_items = tuple(provenance)
        self._provenance = {item.symbol: item for item in provenance_items}
        if len(self._provenance) != len(provenance_items):
            raise ValueError("fake dataset provenance must be unique by symbol")
        known = set(self._symbols)
        referenced = {
            *(item.symbol for item in self._prices),
            *(item.symbol for item in self._valuations),
            *(item.symbol for item in self._validations),
            *self._provenance,
        }
        unknown = referenced - known
        if unknown:
            raise ValueError(
                "fake dataset rows reference unknown symbols: "
                + ", ".join(sorted(unknown))
            )

    def read(
        self,
        request: ResearchDatasetRequest,
        /,
    ) -> ResearchDatasetSnapshot:
        if not isinstance(request, ResearchDatasetRequest):
            raise TypeError("request must be a ResearchDatasetRequest")
        symbol = self._symbols.get(request.symbol)
        if symbol is None:
            raise KeyError(f"unknown dataset symbol {request.symbol}")

        eligible_prices = tuple(
            sorted(
                (
                    item
                    for item in self._prices
                    if item.symbol == request.symbol
                    and item.trade_date <= request.as_of_date
                ),
                key=lambda item: item.trade_date,
            )
        )
        eligible_valuations = tuple(
            item
            for item in self._valuations
            if item.symbol == request.symbol
            and item.metric_date <= request.as_of_date
        )
        validation = self._select_validation(request)
        provenance = self._select_provenance(request, validation)

        all_canonical_sources = {
            *(item.source for item in eligible_prices),
            *(item.source for item in eligible_valuations),
            *provenance.canonical_sources,
        }
        all_validation_sources = {
            *(item.provider for item in validation.observations),
            *(
                item
                for item in (
                    validation.left_provider,
                    validation.right_provider,
                )
                if item is not None
            ),
            *provenance.validation_sources,
        }
        TwseBaselineSourcePolicy.validate_sources(
            canonical_sources=all_canonical_sources,
            validation_sources=all_validation_sources,
            artifact_sources=(item.provider for item in provenance.artifact_refs),
            has_canonical_history=bool(eligible_prices),
        )

        requested_count = request.history_observations
        returned_prices = (
            eligible_prices
            if requested_count is None
            else eligible_prices[-requested_count:]
        )
        history_status = "missing_source"
        current = None
        if eligible_prices:
            if eligible_prices[-1].trade_date == request.as_of_date:
                history_status = "available"
                current = eligible_prices[-1]
            else:
                history_status = "market_date_mismatch"
        history = PriceHistoryReadModel(
            symbol=request.symbol,
            as_of_date=request.as_of_date,
            status=history_status,
            observations=returned_prices,
            current=current,
            total_observations_as_of=len(eligible_prices),
            requested_observations=requested_count,
            is_truncated=len(returned_prices) < len(eligible_prices),
        )

        latest_by_name: dict[str, DatasetValuationMetric] = {}
        for metric in eligible_valuations:
            current_metric = latest_by_name.get(metric.name)
            if current_metric is None or (
                metric.metric_date,
                metric.source,
            ) > (
                current_metric.metric_date,
                current_metric.source,
            ):
                latest_by_name[metric.name] = metric
        selected_metrics = tuple(
            latest_by_name[name] for name in sorted(latest_by_name)
        )
        valuation = ValuationReadModel(
            symbol=request.symbol,
            as_of_date=request.as_of_date,
            status="available" if selected_metrics else "missing_source",
            metrics=selected_metrics,
        )

        canonical_sources = tuple(
            sorted(
                {
                    *provenance.canonical_sources,
                    *(item.source for item in history.observations),
                    *(item.source for item in valuation.metrics),
                }
            )
        )
        validation_sources = tuple(
            sorted(
                {
                    *provenance.validation_sources,
                    *(item.provider for item in validation.observations),
                    *(
                        item
                        for item in (
                            validation.left_provider,
                            validation.right_provider,
                        )
                        if item is not None
                    ),
                }
            )
        )
        provenance = replace(
            provenance,
            validation_run_id=(
                None if validation.status == "missing_source" else validation.run_id
            ),
            canonical_sources=canonical_sources,
            validation_sources=validation_sources,
        )
        as_of = DatasetAsOf(
            as_of_date=request.as_of_date,
            history_observations=requested_count,
            total_history_observations=len(eligible_prices),
            returned_history_observations=len(returned_prices),
            history_is_truncated=len(returned_prices) < len(eligible_prices),
            source_policy=provenance.source_policy,
        )
        return ResearchDatasetSnapshot(
            symbol=symbol,
            as_of=as_of,
            price_history=history,
            valuation=valuation,
            validation=validation,
            provenance=provenance,
        )

    def _select_validation(
        self,
        request: ResearchDatasetRequest,
    ) -> ValidationReadModel:
        candidates = [
            item for item in self._validations if item.symbol == request.symbol
        ]
        if request.validation_run_id is not None:
            candidates = [
                item
                for item in candidates
                if item.run_id == request.validation_run_id
            ]
        candidates = [
            item
            for item in candidates
            if item.status != "missing_source"
            and item.target_date == request.as_of_date
        ]
        if not candidates:
            return ValidationReadModel(symbol=request.symbol)
        return max(
            candidates,
            key=lambda item: (item.created_at, item.run_id),
        )

    def _select_provenance(
        self,
        request: ResearchDatasetRequest,
        validation: ValidationReadModel,
    ) -> DatasetProvenance:
        provenance = self._provenance.get(
            request.symbol,
            DatasetProvenance(symbol=request.symbol),
        )
        selected: dict[str, str | None] = {}
        for name in ("pipeline_run_id", "historical_run_id"):
            requested = getattr(request, name)
            available = getattr(provenance, name)
            if requested is not None and available not in {None, requested}:
                raise LookupError(f"requested {name} is not available")
            selected[name] = requested or available
        return replace(
            provenance,
            pipeline_run_id=selected["pipeline_run_id"],
            historical_run_id=selected["historical_run_id"],
            validation_run_id=(
                None if validation.status == "missing_source" else validation.run_id
            ),
        )


def _normalize_symbol(value: object) -> str:
    return _require_text(value, "symbol").upper()


def _normalize_source(value: object) -> str:
    return _require_text(value, "source").lower()


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be blank")
    return value.strip()


def _normalize_optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"{field_name} must be a date, not a datetime")
    return value


def _require_aware_datetime(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be a timezone-aware datetime")
    return value


def _require_finite_number(
    value: object,
    field_name: str,
    *,
    positive: bool = False,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be numeric")
    normalized = float(value)
    if not math.isfinite(normalized) or (positive and normalized <= 0):
        requirement = "positive and finite" if positive else "finite"
        raise ValueError(f"{field_name} must be {requirement}")
    return normalized


def _safe_endpoint(value: object, field_name: str) -> str:
    endpoint = _require_text(value, field_name)
    if "\r" in endpoint or "\n" in endpoint:
        raise ValueError(f"{field_name} must be one line")
    parsed = urlsplit(endpoint)
    if not parsed.scheme:
        raise ValueError(f"{field_name} must include a URI scheme")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(f"{field_name} must not contain credentials")
    sensitive = {
        key.lower()
        for key, _ in parse_qsl(parsed.query, keep_blank_values=True)
    } & _SENSITIVE_QUERY_KEYS
    if sensitive:
        raise ValueError(
            f"{field_name} must not contain credential query parameters"
        )
    return endpoint


__all__ = [
    "DatasetArtifactRef",
    "DatasetAsOf",
    "DatasetDiscrepancy",
    "DatasetPrice",
    "DatasetProvenance",
    "DatasetSourcePolicyError",
    "DatasetSymbol",
    "DatasetValidationObservation",
    "DatasetValuationMetric",
    "ESUN_VALIDATION_SOURCES",
    "FakeResearchDataset",
    "MOCK_SYNTHETIC_SOURCE",
    "PriceHistoryReadModel",
    "ResearchDataset",
    "ResearchDatasetRequest",
    "ResearchDatasetSnapshot",
    "TWSE_BASELINE_SOURCE_POLICY",
    "TWSE_BASELINE_SOURCES",
    "TwseBaselineSourcePolicy",
    "ValidationReadModel",
    "ValuationReadModel",
]
