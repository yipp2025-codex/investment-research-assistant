"""DS6 isolated v12 daily preparation and explicit M9 consumption.

This module is an application orchestration boundary, not a new provider or
storage contract.  It composes the frozen DS1--DS5 values in this order:

``latest date -> TWSE preparation -> bounded retry/grace -> DS2 -> DS3``
``-> optional DS4 reconciliation -> explicit v12 id -> S1/S2 M9 reads``.

The source preparers are injected.  A preparer owns the already-hardened
provider boundary and returns normalized, hash-only evidence; this module
never guesses an E.SUN endpoint and never writes the v11 production tables.
The query consumer below accepts one explicit dataset version id and has no
provider, preparation, or mutation side effect.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
import hashlib
import math
from pathlib import Path
from typing import Callable, Protocol, Sequence

from app.data_contracts.dataset_persistence import (
    CoverageBasis,
    DatasetArtifactRef,
    DatasetObservation,
    DatasetPersistenceContractError,
    MixedDatasetVersion,
    build_canonical_dataset_version,
    build_provisional_dataset_version,
)
from app.data_contracts.dual_source import (
    AuthorityStatus,
    DatasetSourceStatus,
    FailureClassification,
    ReconciliationStatus,
    SourceRole,
    TWSE_DUAL_SOURCE_POLICY,
)
from app.data_contracts.reconciliation import ReconciliationInput
from app.data_contracts.supplemental_eligibility import (
    InstrumentIdentity,
    OHLCVObservation,
    SecurityBoundaryEvidence,
    SecurityBoundaryStatus,
    SupplementalCandidateInput,
    TwseFailureEvidence,
    evaluate_supplemental,
)
from app.providers.base import (
    ProviderError,
    ProviderInvalidPayloadError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)
from app.research_dataset import (
    ResearchDataset,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
)
from app.screener.stage1_dataset import (
    Stage1DatasetScanEvidence,
    scan_stage1_from_dataset,
)
from app.screener.stage2_dataset import (
    Stage2DatasetResearchEvidence,
    research_stage2_from_dataset,
)
from app.screener.universe import MarketUniverseSnapshot, UniverseMemberStatus
from app.sqlite_dataset_version import SQLiteV12ResearchDataset
from app.storage.dataset_versions import DatasetVersionRepository
from app.storage.reconciliation import (
    DatasetReconciliationRepository,
    ReconciliationPersistenceError,
)


DS6_DAILY_PREPARATION_CONTRACT_VERSION = "ds6-daily-preparation-integration.v1"
DS6_METHODOLOGY_VERSION = "dataset-v1.1"
DS6_DAILY_PREPARATION_FACTORY_LOCATOR = (
    "app.deployment.ds6_daily_preparation:DS6DailyPreparationCoordinator"
)
DEFAULT_DS6_TARGET_OBSERVATIONS = 250
DEFAULT_DS6_CANDIDATE_LIMIT = 30
DEFAULT_DS6_MAX_TWSE_ATTEMPTS = 3
DEFAULT_DS6_RETRY_DELAY_SECONDS = 1.0
DEFAULT_DS6_GRACE_END = time(hour=20, minute=0)
_UTC = timezone.utc
_SHA256 = set("0123456789abcdef")


class DS6PreparationError(RuntimeError):
    """A daily preparation input or immutable v12 result is unsafe."""


class DS6LatestDateError(DS6PreparationError):
    """The authoritative latest-date gate did not resolve a date."""


class DS6AmbiguousVersionError(DS6PreparationError):
    """More than one immutable version matches without an explicit selector."""


class DS6SourcePreparer(Protocol):
    """Prepare one source without writing the v11 production data tables."""

    def prepare(
        self,
        symbol: str,
        target_date: date,
        *,
        target_observation_count: int,
        requested_dates: tuple[date, ...] | None,
        resume_state: "DS6ResumeState | None",
    ) -> "DS6SourcePreparation":
        ...


class DS6LatestDateProvider(Protocol):
    """Resolve a market date from the authoritative provider only."""

    def __call__(self, anchor_date: date, /) -> date | None:
        ...


class DS6UniverseProvider(Protocol):
    """Return one immutable S1 Universe snapshot for the resolved date."""

    def __call__(self, market_date: date, /) -> MarketUniverseSnapshot:
        ...


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise DS6PreparationError(f"{field_name} must be a date")
    return value


def _non_blank(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DS6PreparationError(f"{field_name} must be a non-blank string")
    return value.strip()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _is_sha256(value: str | None) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and set(value) <= _SHA256
    )


def _coerce_failure(value: FailureClassification | str | None) -> FailureClassification | None:
    if value is None:
        return None
    try:
        return value if isinstance(value, FailureClassification) else FailureClassification(value)
    except ValueError as error:
        raise DS6PreparationError("unsupported source failure classification") from error


def _provider_failure_class(error: BaseException) -> FailureClassification:
    if isinstance(error, ProviderInvalidPayloadError):
        return FailureClassification.MALFORMED
    if isinstance(error, ProviderTimeoutError):
        return FailureClassification.TIMEOUT
    if isinstance(error, ProviderTemporaryError):
        return FailureClassification.TEMPORARY
    if isinstance(error, ProviderPermanentError):
        return FailureClassification.PERMANENT
    if isinstance(error, ProviderError):
        return FailureClassification.UNAVAILABLE
    return FailureClassification.UNAVAILABLE


def _safe_error_type(error: BaseException) -> str:
    return type(error).__name__[:120]


def _failure_artifact(
    *,
    provider: str,
    symbol: str,
    target_date: date,
    failure_class: FailureClassification,
    error_type: str,
) -> DatasetArtifactRef:
    evidence = (
        f"{DS6_DAILY_PREPARATION_CONTRACT_VERSION}|{provider}|{symbol}|"
        f"{target_date.isoformat()}|{failure_class.value}|{error_type}"
    )
    payload_sha256 = _sha256(evidence)
    return DatasetArtifactRef(
        ordinal=1,
        provider=provider,
        dataset="ds6-source-failure-evidence",
        source_ref=(
            f"evidence://ds6/{provider}/{symbol}/{target_date.isoformat()}"
        ),
        contract_version=DS6_DAILY_PREPARATION_CONTRACT_VERSION,
        payload_sha256=payload_sha256,
        payload_size_bytes=len(evidence.encode("utf-8")),
        hash_basis="canonical-json-v1",
    )


def _failure_run_id(provider: str, symbol: str, target_date: date) -> str:
    return "ds6-failure-" + _sha256(
        f"{DS6_DAILY_PREPARATION_CONTRACT_VERSION}|{provider}|"
        f"{symbol}|{target_date.isoformat()}"
    )


def _ordered_artifacts(*artifacts: DatasetArtifactRef) -> tuple[DatasetArtifactRef, ...]:
    """Assign the DS3 contiguous ordinals without changing artifact identity."""

    return tuple(replace(item, ordinal=index) for index, item in enumerate(artifacts, 1))


@dataclass(frozen=True, slots=True)
class DS6ResumeState:
    """Explicit source checkpoint handoff for a resumable preparation."""

    dataset_version_id: str
    source_run_ids: tuple[str, ...]
    remaining_dates: tuple[date, ...]
    checkpoint_reused: bool = True

    def __post_init__(self) -> None:
        if not _is_sha256(self.dataset_version_id):
            raise DS6PreparationError("resume dataset_version_id must be SHA-256")
        if not self.source_run_ids or any(not str(item).strip() for item in self.source_run_ids):
            raise DS6PreparationError("resume source_run_ids must be non-empty")
        dates = tuple(sorted(set(_require_date(item, "remaining_dates") for item in self.remaining_dates)))
        if not isinstance(self.checkpoint_reused, bool):
            raise DS6PreparationError("checkpoint_reused must be boolean")
        object.__setattr__(self, "source_run_ids", tuple(str(item).strip() for item in self.source_run_ids))
        object.__setattr__(self, "remaining_dates", dates)


@dataclass(frozen=True, slots=True)
class DS6SourcePreparation:
    """Normalized source output consumed by DS6 and DS2.

    ``observations`` are not market-data sentinels.  They are the normalized
    provider rows returned by an injected, already-hardened source preparer.
    A failed attempt still carries a hash-only evidence artifact so DS2 can
    make a deterministic fail-closed decision.
    """

    provider: str
    symbol: str
    target_date: date
    requested_dates: tuple[date, ...]
    observations: tuple[OHLCVObservation, ...]
    source_run_id: str
    artifact: DatasetArtifactRef
    status: str = "complete"
    failure_class: FailureClassification | str | None = None
    failure_evidence: TwseFailureEvidence | None = None
    requested_identity: InstrumentIdentity | None = None
    returned_identity: InstrumentIdentity | None = None
    security_boundary: SecurityBoundaryEvidence | SecurityBoundaryStatus = (
        SecurityBoundaryStatus.NOT_CHECKED
    )
    checkpoint_reused: bool = False
    provider_request_count: int = 1
    remaining_dates: tuple[date, ...] = ()

    def __post_init__(self) -> None:
        provider = _non_blank(self.provider, "provider").lower()
        if provider not in {"twse", "twse-historical", "esun", "esun-historical"}:
            raise DS6PreparationError("source provider is not a formal DS1 provider")
        symbol = _non_blank(self.symbol, "symbol").upper()
        target_date = _require_date(self.target_date, "target_date")
        requested_dates = tuple(sorted(set(_require_date(item, "requested_dates") for item in self.requested_dates)))
        if not requested_dates:
            raise DS6PreparationError("requested_dates must not be empty")
        if (
            provider.startswith("twse")
            and target_date not in requested_dates
            and not self.checkpoint_reused
        ):
            raise DS6PreparationError("TWSE requested_dates must include target_date")
        observations = tuple(self.observations)
        if any(not isinstance(item, OHLCVObservation) for item in observations):
            raise DS6PreparationError("observations must contain OHLCVObservation values")
        parsed_dates: list[date] = []
        for item in observations:
            trade_date = item.trade_date
            if isinstance(trade_date, datetime):
                raise DS6PreparationError("observation trade_date must not be datetime")
            if isinstance(trade_date, str):
                try:
                    trade_date = date.fromisoformat(trade_date.strip())
                except ValueError as error:
                    raise DS6PreparationError("observation trade_date is invalid") from error
            if not isinstance(trade_date, date):
                raise DS6PreparationError("observation trade_date is invalid")
            if trade_date not in requested_dates:
                raise DS6PreparationError("observation date is outside requested dates")
            if item.symbol is not None and str(item.symbol).strip().upper() != symbol:
                raise DS6PreparationError("observation symbol differs from request")
            parsed_dates.append(trade_date)
        if len(parsed_dates) != len(set(parsed_dates)):
            raise DS6PreparationError("source observations must have unique dates")
        source_run_id = _non_blank(self.source_run_id, "source_run_id")
        if not isinstance(self.artifact, DatasetArtifactRef):
            raise DS6PreparationError("artifact must be a DatasetArtifactRef")
        if self.artifact.provider != provider:
            raise DS6PreparationError("artifact provider differs from source provider")
        status = _non_blank(self.status, "status").lower()
        if status not in {"complete", "incomplete", "failed"}:
            raise DS6PreparationError("unsupported source preparation status")
        failure_class = _coerce_failure(self.failure_class)
        if status == "complete" and set(parsed_dates) != set(requested_dates):
            raise DS6PreparationError("complete source preparation has incomplete coverage")
        if status != "complete" and set(parsed_dates) == set(requested_dates):
            raise DS6PreparationError("incomplete source preparation has complete coverage")
        if not isinstance(self.checkpoint_reused, bool):
            raise DS6PreparationError("checkpoint_reused must be boolean")
        if isinstance(self.provider_request_count, bool) or not isinstance(self.provider_request_count, int) or self.provider_request_count < 0:
            raise DS6PreparationError("provider_request_count must be non-negative")
        remaining = tuple(sorted(set(_require_date(item, "remaining_dates") for item in self.remaining_dates)))
        if not remaining:
            remaining = tuple(sorted(set(requested_dates) - set(parsed_dates)))
        if set(remaining) != set(requested_dates) - set(parsed_dates):
            raise DS6PreparationError("remaining_dates does not match source coverage")
        if self.requested_identity is not None and not isinstance(self.requested_identity, InstrumentIdentity):
            raise DS6PreparationError("requested_identity must be InstrumentIdentity")
        if self.returned_identity is not None and not isinstance(self.returned_identity, InstrumentIdentity):
            raise DS6PreparationError("returned_identity must be InstrumentIdentity")
        if not isinstance(self.security_boundary, (SecurityBoundaryEvidence, SecurityBoundaryStatus)):
            raise DS6PreparationError("security_boundary is invalid")
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "target_date", target_date)
        object.__setattr__(self, "requested_dates", requested_dates)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "source_run_id", source_run_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "failure_class", failure_class)
        object.__setattr__(self, "remaining_dates", remaining)


@dataclass(frozen=True, slots=True)
class DailyDatasetPreparationResult:
    """Immutable per-symbol DS6 preparation outcome."""

    symbol: str
    target_date: date
    dataset_version_id: str | None
    source_policy: str
    source_status: str | None
    authority_status: str | None
    research_data_quality: str | None
    twse_coverage: int
    esun_count: int
    discrepancy_count: int
    preparation_outcome: str
    provenance_sha256: str | None
    replayed: bool
    provider_request_count: int = 0
    checkpoint_reused: bool = False
    remaining_dates: tuple[date, ...] = ()
    twse_failure_class: str | None = None
    supplemental_source_eligible: bool | None = None
    validation_status: str = "not_requested"
    reconciliation_status: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _non_blank(self.symbol, "symbol").upper())
        object.__setattr__(self, "target_date", _require_date(self.target_date, "target_date"))
        if self.dataset_version_id is not None and not _is_sha256(self.dataset_version_id):
            raise DS6PreparationError("dataset_version_id must be a SHA-256 or null")
        _non_blank(self.source_policy, "source_policy")
        _non_blank(self.preparation_outcome, "preparation_outcome")
        if self.provenance_sha256 is not None and not _is_sha256(self.provenance_sha256):
            raise DS6PreparationError("provenance_sha256 must be a SHA-256 or null")
        for field_name in ("twse_coverage", "esun_count", "discrepancy_count", "provider_request_count"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise DS6PreparationError(f"{field_name} must be non-negative")
        if not isinstance(self.replayed, bool) or not isinstance(self.checkpoint_reused, bool):
            raise DS6PreparationError("replayed/checkpoint_reused must be boolean")
        object.__setattr__(self, "remaining_dates", tuple(sorted(set(_require_date(item, "remaining_dates") for item in self.remaining_dates))))

    @property
    def dataset_id(self) -> str | None:
        """Compatibility spelling for callers that use the shorter name."""

        return self.dataset_version_id


@dataclass(frozen=True, slots=True)
class DailyPreparationIntegrationResult:
    """One complete DS6 daily entrypoint result."""

    target_date: date | None
    preparation_results: tuple[DailyDatasetPreparationResult, ...]
    dataset_version_ids: tuple[tuple[str, str], ...]
    stage1: Stage1DatasetScanEvidence | None
    stage2: Stage2DatasetResearchEvidence | None
    status: str
    replayed: bool
    provider_request_count: int
    checkpoint_reused_count: int
    latest_date_source: str = "twse-latest-date-provider"

    def __post_init__(self) -> None:
        if self.target_date is not None:
            _require_date(self.target_date, "target_date")
        results = tuple(self.preparation_results)
        if any(not isinstance(item, DailyDatasetPreparationResult) for item in results):
            raise DS6PreparationError("preparation_results contains an invalid value")
        ids = tuple(sorted(self.dataset_version_ids))
        if any(not _is_sha256(value) for _, value in ids):
            raise DS6PreparationError("dataset_version_ids must contain SHA-256 values")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (self.provider_request_count, self.checkpoint_reused_count)):
            raise DS6PreparationError("integration counters must be non-negative")
        if not isinstance(self.replayed, bool):
            raise DS6PreparationError("replayed must be boolean")
        object.__setattr__(self, "preparation_results", results)
        object.__setattr__(self, "dataset_version_ids", ids)


class ExplicitDatasetVersionConsumer:
    """M9 query-only consumer that requires one explicit dataset version id."""

    def __init__(self, database_path: str | Path) -> None:
        self._dataset = SQLiteV12ResearchDataset(database_path)

    def read(
        self,
        *,
        symbol: str,
        target_date: date,
        dataset_version_id: str,
        history_observations: int | None = None,
    ) -> ResearchDatasetSnapshot:
        if not _is_sha256(dataset_version_id):
            raise DS6PreparationError("M9 consumer requires an explicit dataset_version_id")
        return self._dataset.read(
            ResearchDatasetRequest(
                symbol=symbol,
                as_of_date=target_date,
                history_observations=history_observations,
                dataset_version_id=dataset_version_id,
            )
        )


@dataclass(frozen=True, slots=True)
class DS6Configuration:
    """Isolated v12 configuration; it is never used by the production factory."""

    database_path: str | Path
    target_observations: int = DEFAULT_DS6_TARGET_OBSERVATIONS
    candidate_limit: int = DEFAULT_DS6_CANDIDATE_LIMIT
    max_twse_attempts: int = DEFAULT_DS6_MAX_TWSE_ATTEMPTS
    retry_delay_seconds: float = DEFAULT_DS6_RETRY_DELAY_SECONDS
    grace_end: time = DEFAULT_DS6_GRACE_END

    def __post_init__(self) -> None:
        path = Path(self.database_path).expanduser()
        if not path.is_absolute():
            raise DS6PreparationError("isolated v12 database_path must be absolute")
        if isinstance(self.target_observations, bool) or not isinstance(self.target_observations, int) or self.target_observations < 1:
            raise DS6PreparationError("target_observations must be positive")
        if isinstance(self.candidate_limit, bool) or not isinstance(self.candidate_limit, int) or self.candidate_limit < 1:
            raise DS6PreparationError("candidate_limit must be positive")
        if isinstance(self.max_twse_attempts, bool) or not isinstance(self.max_twse_attempts, int) or self.max_twse_attempts < 1:
            raise DS6PreparationError("max_twse_attempts must be positive")
        if isinstance(self.retry_delay_seconds, bool) or not isinstance(self.retry_delay_seconds, (int, float)) or not math.isfinite(float(self.retry_delay_seconds)) or self.retry_delay_seconds < 0:
            raise DS6PreparationError("retry_delay_seconds must be finite and non-negative")
        if not isinstance(self.grace_end, time) or self.grace_end.tzinfo is not None:
            raise DS6PreparationError("grace_end must be a timezone-free time")
        object.__setattr__(self, "database_path", path.resolve())
        object.__setattr__(self, "retry_delay_seconds", float(self.retry_delay_seconds))


class DS6DailyPreparationCoordinator:
    """Compose isolated v12 preparation, reconciliation, S1 and S2."""

    def __init__(
        self,
        configuration: DS6Configuration,
        *,
        latest_date_provider: DS6LatestDateProvider,
        universe_provider: DS6UniverseProvider,
        twse_source: DS6SourcePreparer,
        esun_source: DS6SourcePreparer | None = None,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        if not isinstance(configuration, DS6Configuration):
            raise TypeError("configuration must be DS6Configuration")
        for name, value in (
            ("latest_date_provider", latest_date_provider),
            ("universe_provider", universe_provider),
            ("twse_source", twse_source),
        ):
            if not callable(value) and not hasattr(value, "prepare"):
                raise TypeError(f"{name} must be callable")
        if esun_source is not None and not callable(esun_source) and not hasattr(esun_source, "prepare"):
            raise TypeError("esun_source must be callable")
        self.configuration = configuration
        self.latest_date_provider = latest_date_provider
        self.universe_provider = universe_provider
        self.twse_source = twse_source
        self.esun_source = esun_source
        self.clock = clock or (lambda: datetime.now(_UTC))
        self.sleep = sleep or (lambda _seconds: None)
        self.dataset_repository = DatasetVersionRepository(configuration.database_path)
        self.reconciliation_repository = DatasetReconciliationRepository(configuration.database_path)
        self.dataset = SQLiteV12ResearchDataset(configuration.database_path)
        self._cache: dict[tuple[str, date], DailyDatasetPreparationResult] = {}

    def prepare(self, anchor_date: date, /) -> DailyPreparationIntegrationResult:
        """Resolve a date, prepare explicit versions, then consume S1/S2."""

        anchor = _require_date(anchor_date, "anchor_date")
        resolved = self.latest_date_provider(anchor)
        if resolved is None:
            return DailyPreparationIntegrationResult(
                target_date=None,
                preparation_results=(),
                dataset_version_ids=(),
                stage1=None,
                stage2=None,
                status="latest_date_unavailable",
                replayed=False,
                provider_request_count=0,
                checkpoint_reused_count=0,
            )
        target_date = _require_date(resolved, "latest_date_provider result")
        universe = self.universe_provider(target_date)
        if not isinstance(universe, MarketUniverseSnapshot):
            raise DS6PreparationError("universe_provider must return MarketUniverseSnapshot")
        if universe.market_date != target_date:
            raise DS6PreparationError("Universe market_date differs from authoritative latest date")

        eligible = tuple(
            item
            for item in universe.members
            if item.status is UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE
        )
        results = tuple(self._prepare_symbol(item.symbol, target_date) for item in eligible)
        ids = tuple(
            (item.symbol, item.dataset_version_id)
            for item in results
            if item.dataset_version_id is not None
        )
        provider_requests = sum(item.provider_request_count for item in results)
        checkpoint_reused = sum(item.checkpoint_reused for item in results)
        if len(ids) != len(eligible):
            return DailyPreparationIntegrationResult(
                target_date=target_date,
                preparation_results=results,
                dataset_version_ids=ids,
                stage1=None,
                stage2=None,
                status="preparation_incomplete",
                replayed=bool(results) and all(item.replayed for item in results),
                provider_request_count=provider_requests,
                checkpoint_reused_count=checkpoint_reused,
            )

        dataset_version_ids = {symbol: version_id for symbol, version_id in ids}
        stage1 = scan_stage1_from_dataset(
            universe=universe,
            dataset=self.dataset,
            as_of_date=target_date,
            candidate_limit=self.configuration.candidate_limit,
            dataset_version_ids=dataset_version_ids,
        )
        shortlist_ids = {
            candidate.symbol: dataset_version_ids[candidate.symbol]
            for candidate in stage1.result.candidates
        }
        stage2 = research_stage2_from_dataset(
            stage1_result=stage1.result,
            dataset=self.dataset,
            market_date=target_date,
            dataset_version_ids=shortlist_ids,
        )
        return DailyPreparationIntegrationResult(
            target_date=target_date,
            preparation_results=results,
            dataset_version_ids=ids,
            stage1=stage1,
            stage2=stage2,
            status="success",
            replayed=bool(results) and all(item.replayed for item in results),
            provider_request_count=provider_requests,
            checkpoint_reused_count=checkpoint_reused,
        )

    def _prepare_symbol(self, symbol: str, target_date: date) -> DailyDatasetPreparationResult:
        key = (symbol.strip().upper(), target_date)
        cached = self._cache.get(key)
        if cached is not None:
            return self._copy_result(cached, replayed=True, provider_request_count=0)

        existing_complete, existing_provisional = self._existing_versions(key[0], target_date)
        if existing_complete is not None:
            result = self._result_from_version(existing_complete, outcome="replay")
            self._cache[key] = result
            return result

        resume_state = None
        if existing_provisional is not None:
            resume_state = self._resume_state(existing_provisional)

        twse, twse_attempts = self._prepare_twse(
            key[0],
            target_date,
            requested_dates=(
                tuple(
                    sorted(
                        item.trade_date
                        for item in existing_provisional.observations
                        if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
                    )
                )
                if existing_provisional is not None
                else None
            ),
            resume_state=resume_state,
        )
        if twse.target_date != target_date:
            raise DS6PreparationError("TWSE preparation target date differs")

        if twse.status == "complete":
            if existing_provisional is not None:
                result = self._reconcile_existing(
                    existing_provisional,
                    twse,
                    twse_attempts,
                    resume_state is not None,
                )
            else:
                result = self._build_canonical(
                    twse,
                    twse_attempts,
                    resume_state is not None,
                )
        else:
            result = self._build_supplemental_or_fail(
                twse,
                twse_attempts,
                existing_provisional,
                resume_state is not None,
            )
        if result.dataset_version_id is not None:
            self._cache[key] = result
        return result

    def _prepare_twse(
        self,
        symbol: str,
        target_date: date,
        *,
        requested_dates: tuple[date, ...] | None,
        resume_state: DS6ResumeState | None,
    ) -> tuple[DS6SourcePreparation, int]:
        attempt_count = 0
        provider_requests = 0
        current_resume = resume_state
        current = self._call_source(
            self.twse_source,
            symbol,
            target_date,
            requested_dates=requested_dates,
            resume_state=current_resume,
            expected_provider="twse",
        )
        attempt_count += 1
        provider_requests += max(1, current.provider_request_count)
        while (
            current.status != "complete"
            and current.failure_class in FailureClassification.supplemental_eligible()
            and attempt_count < self.configuration.max_twse_attempts
            and self._within_grace()
        ):
            remaining_seconds = self._grace_remaining_seconds()
            if remaining_seconds <= 0:
                break
            delay = min(self.configuration.retry_delay_seconds, remaining_seconds)
            if delay > 0:
                self.sleep(delay)
            current_resume = self._resume_from_source(current, resume_state)
            current = self._call_source(
                self.twse_source,
                symbol,
                target_date,
                requested_dates=current.requested_dates,
                resume_state=current_resume,
                expected_provider="twse",
            )
            attempt_count += 1
            provider_requests += max(1, current.provider_request_count)
        return current, provider_requests

    def _build_canonical(
        self,
        twse: DS6SourcePreparation,
        twse_attempts: int,
        resumed: bool,
    ) -> DailyDatasetPreparationResult:
        try:
            twse_rows = self._to_dataset_rows(twse, SourceRole.CANONICAL)
        except DS6PreparationError:
            return self._failed_result(
                twse,
                outcome="twse_malformed",
                provider_request_count=twse_attempts,
                checkpoint_reused=resumed or twse.checkpoint_reused,
            )

        validation: DS6SourcePreparation | None = None
        validation_status = "not_requested"
        if self.esun_source is not None:
            validation = self._call_source(
                self.esun_source,
                twse.symbol,
                twse.target_date,
                requested_dates=twse.requested_dates,
                resume_state=None,
                expected_provider="esun",
            )
            validation_status = self._validation_status(validation)

        validation_rows: tuple[DatasetObservation, ...] = ()
        if (
            validation is not None
            and self._identity_matches(validation)
            and self._security_boundary_passed(validation)
        ):
            try:
                validation_rows = self._to_dataset_rows(validation, SourceRole.VALIDATION)
            except DS6PreparationError:
                validation_rows = ()
                validation_status = "invalid"
        discrepancy_count = self._discrepancy_count(twse_rows, validation_rows)
        artifacts = _ordered_artifacts(
            twse.artifact,
            *(() if validation is None else (validation.artifact,)),
        )
        try:
            version = build_canonical_dataset_version(
                symbol=twse.symbol,
                as_of_date=twse.target_date,
                required_observation_count=len(twse.requested_dates),
                twse_observations=twse_rows,
                validation_observations=validation_rows,
                artifacts=artifacts,
                methodology_version=DS6_METHODOLOGY_VERSION,
                discrepancy_count=discrepancy_count,
                coverage_basis=CoverageBasis.STANDARD,
                source_policy=TWSE_DUAL_SOURCE_POLICY,
            )
        except (DatasetPersistenceContractError, ValueError) as error:
            return self._failed_result(
                twse,
                outcome="canonical_construction_failed",
                provider_request_count=twse_attempts,
                checkpoint_reused=resumed or twse.checkpoint_reused,
                validation_status=validation_status,
                error=error,
            )
        saved = self.dataset_repository.save(version)
        return self._result_from_version(
            version,
            outcome="canonical_complete" if saved.written else "replay",
            replayed=not saved.written,
            provider_request_count=twse_attempts + (0 if validation is None else max(1, validation.provider_request_count)),
            checkpoint_reused=resumed or twse.checkpoint_reused or bool(validation and validation.checkpoint_reused),
            validation_status=validation_status,
        )

    def _build_supplemental_or_fail(
        self,
        twse: DS6SourcePreparation,
        twse_attempts: int,
        existing_provisional: MixedDatasetVersion | None,
        resumed: bool,
    ) -> DailyDatasetPreparationResult:
        failure_evidence = twse.failure_evidence or TwseFailureEvidence(
            endpoint_identity_verified=False,
            classification_basis="missing_twse_failure_evidence",
        )
        failure_class = twse.failure_class
        should_request_esun = (
            failure_class in FailureClassification.supplemental_eligible()
            and failure_evidence.endpoint_identity_verified
            and bool(twse.remaining_dates)
        )
        if should_request_esun and self.esun_source is not None:
            esun = self._call_source(
                self.esun_source,
                twse.symbol,
                twse.target_date,
                requested_dates=twse.remaining_dates,
                resume_state=(
                    self._resume_state(existing_provisional)
                    if existing_provisional is not None
                    else None
                ),
                expected_provider="esun",
            )
        else:
            esun = self._failure_preparation(
                provider="esun-historical",
                symbol=twse.symbol,
                target_date=twse.target_date,
                requested_dates=twse.remaining_dates or twse.requested_dates,
                failure_class=(
                    FailureClassification.UNAVAILABLE
                    if self.esun_source is None
                    else FailureClassification.PERMANENT
                ),
                error_type="supplemental_not_requested",
            )

        candidate = SupplementalCandidateInput(
            requested_symbol=twse.symbol,
            target_date=twse.target_date,
            requested_start_date=min(twse.requested_dates),
            requested_end_date=max(twse.requested_dates),
            missing_twse_dates=twse.remaining_dates,
            twse_failure_class=failure_class,
            source_run_id=esun.source_run_id,
            artifact_sha256=esun.artifact.payload_sha256,
            twse_observations=twse.observations,
            esun_requested_identity=esun.requested_identity,
            esun_returned_identity=esun.returned_identity,
            esun_observations=esun.observations,
            security_boundary=esun.security_boundary,
            twse_failure_evidence=failure_evidence,
        )
        eligibility = evaluate_supplemental(candidate)
        esun_requests = max(0, esun.provider_request_count)
        if eligibility.eligible and eligibility.coverage_complete:
            try:
                twse_rows = self._to_dataset_rows(twse, SourceRole.CANONICAL)
                esun_rows = self._to_dataset_rows(esun, SourceRole.SUPPLEMENTAL)
                esun_rows = tuple(
                    item
                    for item in esun_rows
                    if item.trade_date in set(eligibility.eligible_observation_dates)
                )
                version = build_provisional_dataset_version(
                    symbol=twse.symbol,
                    as_of_date=twse.target_date,
                    required_observation_count=len(twse.requested_dates),
                    twse_observations=twse_rows,
                    eligibility_result=eligibility,
                    esun_observations=esun_rows,
                    artifacts=_ordered_artifacts(twse.artifact, esun.artifact),
                    methodology_version=DS6_METHODOLOGY_VERSION,
                    coverage_basis=CoverageBasis.STANDARD,
                    source_policy=TWSE_DUAL_SOURCE_POLICY,
                )
            except (DatasetPersistenceContractError, ValueError) as error:
                return self._failed_result(
                    twse,
                    outcome="provisional_construction_failed",
                    provider_request_count=twse_attempts + esun_requests,
                    checkpoint_reused=resumed or twse.checkpoint_reused or esun.checkpoint_reused,
                    remaining_dates=eligibility.remaining_uncovered_dates,
                    failure_class=failure_class,
                    source_eligible=eligibility.source_eligible,
                    error=error,
                )
            saved = self.dataset_repository.save(version)
            return self._result_from_version(
                version,
                outcome="provisional_mixed" if saved.written else "replay",
                replayed=not saved.written,
                provider_request_count=twse_attempts + esun_requests,
                checkpoint_reused=resumed or twse.checkpoint_reused or esun.checkpoint_reused,
                remaining_dates=eligibility.remaining_uncovered_dates,
                twse_failure_class=failure_class,
                source_eligible=eligibility.source_eligible,
            )

        return self._failed_result(
            twse,
            outcome=(
                "supplemental_incomplete"
                if eligibility.eligible
                else "supplemental_rejected"
            ),
            provider_request_count=twse_attempts + esun_requests,
            checkpoint_reused=resumed or twse.checkpoint_reused or esun.checkpoint_reused,
            remaining_dates=eligibility.remaining_uncovered_dates,
            failure_class=failure_class,
            source_eligible=eligibility.source_eligible,
        )

    def _reconcile_existing(
        self,
        parent: MixedDatasetVersion,
        twse: DS6SourcePreparation,
        twse_attempts: int,
        resumed: bool,
    ) -> DailyDatasetPreparationResult:
        try:
            all_rows = self._to_dataset_rows(twse, SourceRole.CANONICAL)
            parent_supplemental_dates = {
                item.trade_date
                for item in parent.observations
                if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
            }
            target_dates = tuple(
                sorted(parent_supplemental_dates & {item.trade_date for item in all_rows})
            )
            if not target_dates:
                raise DS6PreparationError("TWSE complete result has no provisional dates to reconcile")
            input_value = ReconciliationInput(
                parent=parent,
                parent_dataset_version_id=parent.identity.dataset_version_id,
                parent_canonical_sha256=parent.canonical_sha256 or "",
                twse_observations=tuple(
                    item for item in all_rows if item.trade_date in set(target_dates)
                ),
                twse_source_run_id=twse.source_run_id,
                twse_artifact=twse.artifact,
                target_dates=target_dates,
            )
            persisted = self.reconciliation_repository.reconcile_and_save(input_value)
            child = persisted.reconciliation.new_dataset_version
            if persisted.reconciliation.reconciliation_status is ReconciliationStatus.RECONCILED_DISCREPANT:
                outcome = "reconciled_discrepant"
            elif persisted.reconciliation.reconciliation_status is ReconciliationStatus.RECONCILED_EQUAL:
                outcome = "reconciled_equal"
            else:
                outcome = "reconciliation_partial"
            return self._result_from_version(
                child,
                outcome=outcome if persisted.written else "replay",
                replayed=not persisted.written,
                provider_request_count=twse_attempts,
                checkpoint_reused=resumed or twse.checkpoint_reused,
                remaining_dates=persisted.reconciliation.still_pending_dates,
                twse_failure_class=None,
                source_eligible=None,
            )
        except (
            DS6PreparationError,
            DatasetPersistenceContractError,
            ReconciliationPersistenceError,
            ValueError,
        ) as error:
            return self._failed_result(
                twse,
                outcome="reconciliation_failed",
                provider_request_count=twse_attempts,
                checkpoint_reused=resumed or twse.checkpoint_reused,
                error=error,
            )

    def _existing_versions(
        self, symbol: str, target_date: date
    ) -> tuple[MixedDatasetVersion | None, MixedDatasetVersion | None]:
        versions = self.dataset_repository.list_versions(symbol=symbol, as_of_date=target_date)
        complete = tuple(
            item
            for item in versions
            if item.identity.source_status
            in {DatasetSourceStatus.CANONICAL_COMPLETE, DatasetSourceStatus.RECONCILED}
        )
        if len(complete) > 1:
            raise DS6AmbiguousVersionError(
                f"{symbol} {target_date.isoformat()} has multiple consumable versions"
            )
        if complete:
            return complete[0], None
        provisional = tuple(
            item
            for item in versions
            if item.identity.source_status is DatasetSourceStatus.PROVISIONAL_MIXED
        )
        child_parent_ids = {
            item.identity.parent_dataset_version_id
            for item in provisional
            if item.identity.parent_dataset_version_id is not None
        }
        leaves = tuple(
            item for item in provisional if item.identity.dataset_version_id not in child_parent_ids
        )
        if len(leaves) > 1:
            raise DS6AmbiguousVersionError(
                f"{symbol} {target_date.isoformat()} has multiple open provisional lineages"
            )
        return None, (leaves[0] if leaves else None)

    def _resume_state(self, version: MixedDatasetVersion) -> DS6ResumeState:
        source_runs = tuple(sorted({item.source_run_id for item in version.observations}))
        remaining = tuple(
            sorted(
                item.trade_date
                for item in version.observations
                if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
            )
        )
        return DS6ResumeState(
            dataset_version_id=version.identity.dataset_version_id,
            source_run_ids=source_runs,
            remaining_dates=remaining,
        )

    @staticmethod
    def _resume_from_source(
        current: DS6SourcePreparation,
        prior: DS6ResumeState | None,
    ) -> DS6ResumeState | None:
        source_runs = (current.source_run_id,) if prior is None else tuple(sorted(set(prior.source_run_ids + (current.source_run_id,))))
        if prior is None:
            return None
        return DS6ResumeState(
            dataset_version_id=prior.dataset_version_id,
            source_run_ids=source_runs,
            remaining_dates=current.remaining_dates,
        )

    def _call_source(
        self,
        source: DS6SourcePreparer,
        symbol: str,
        target_date: date,
        *,
        requested_dates: tuple[date, ...] | None,
        resume_state: DS6ResumeState | None,
        expected_provider: str,
    ) -> DS6SourcePreparation:
        try:
            prepare = getattr(source, "prepare", source)
            result = prepare(
                symbol,
                target_date,
                target_observation_count=self.configuration.target_observations,
                requested_dates=requested_dates,
                resume_state=resume_state,
            )
            if not isinstance(result, DS6SourcePreparation):
                raise DS6PreparationError("source preparer returned an invalid result")
            if not result.provider.startswith(expected_provider):
                raise DS6PreparationError("source preparer returned an unexpected provider")
            return result
        except BaseException as error:
            if isinstance(error, KeyboardInterrupt):
                raise
            failure_class = _provider_failure_class(error)
            fallback_dates = requested_dates or (target_date,)
            return self._failure_preparation(
                provider=("twse-historical" if expected_provider == "twse" else "esun-historical"),
                symbol=symbol,
                target_date=target_date,
                requested_dates=fallback_dates,
                failure_class=failure_class,
                error_type=_safe_error_type(error),
            )

    def _failure_preparation(
        self,
        *,
        provider: str,
        symbol: str,
        target_date: date,
        requested_dates: tuple[date, ...],
        failure_class: FailureClassification,
        error_type: str,
    ) -> DS6SourcePreparation:
        dates = tuple(sorted(set(requested_dates)))
        return DS6SourcePreparation(
            provider=provider,
            symbol=symbol,
            target_date=target_date,
            requested_dates=dates,
            observations=(),
            source_run_id=_failure_run_id(provider, symbol, target_date),
            artifact=_failure_artifact(
                provider=provider,
                symbol=symbol,
                target_date=target_date,
                failure_class=failure_class,
                error_type=error_type,
            ),
            status="failed",
            failure_class=failure_class,
            provider_request_count=0,
        )

    @staticmethod
    def _identity_matches(source: DS6SourcePreparation) -> bool:
        return (
            source.requested_identity is not None
            and source.returned_identity is not None
            and source.requested_identity == source.returned_identity
            and source.requested_identity.symbol == source.symbol
        )

    @staticmethod
    def _security_boundary_passed(source: DS6SourcePreparation) -> bool:
        boundary = source.security_boundary
        if isinstance(boundary, SecurityBoundaryEvidence):
            return boundary.status is SecurityBoundaryStatus.PASS
        return boundary is SecurityBoundaryStatus.PASS

    @staticmethod
    def _validation_status(source: DS6SourcePreparation) -> str:
        if source.status == "failed":
            return (source.failure_class.value if source.failure_class is not None else "unavailable")
        if not DS6DailyPreparationCoordinator._identity_matches(source):
            return "identity_mismatch"
        if not DS6DailyPreparationCoordinator._security_boundary_passed(source):
            return "security_boundary_failed"
        if source.status != "complete":
            return "incomplete"
        return "available"

    @staticmethod
    def _to_dataset_rows(
        source: DS6SourcePreparation,
        role: SourceRole,
    ) -> tuple[DatasetObservation, ...]:
        rows: list[DatasetObservation] = []
        seen: set[date] = set()
        for raw in source.observations:
            trade_date = raw.trade_date
            if isinstance(trade_date, str):
                try:
                    trade_date = date.fromisoformat(trade_date.strip())
                except ValueError as error:
                    raise DS6PreparationError("source observation date is malformed") from error
            if isinstance(trade_date, datetime) or not isinstance(trade_date, date):
                raise DS6PreparationError("source observation date is malformed")
            if trade_date in seen:
                raise DS6PreparationError("source observation dates are duplicated")
            seen.add(trade_date)
            try:
                open_price = float(raw.open)
                high = float(raw.high)
                low = float(raw.low)
                close = float(raw.close)
                volume = int(raw.volume)
            except (TypeError, ValueError) as error:
                raise DS6PreparationError("source OHLCV is malformed") from error
            if any(not math.isfinite(value) or value <= 0 for value in (open_price, high, low, close)):
                raise DS6PreparationError("source OHLCV contains a non-positive value")
            if high < max(open_price, low, close) or low > min(open_price, high, close):
                raise DS6PreparationError("source OHLCV relationship is invalid")
            if isinstance(raw.volume, bool) or volume < 0 or str(raw.volume).strip() not in {str(volume), f"{volume}.0"}:
                raise DS6PreparationError("source volume is malformed")
            observation_sha256 = _sha256(
                "|".join(
                    (
                        source.symbol,
                        trade_date.isoformat(),
                        source.provider,
                        role.value,
                        source.source_run_id,
                        str(open_price),
                        str(high),
                        str(low),
                        str(close),
                        str(volume),
                    )
                )
            )
            rows.append(
                DatasetObservation(
                    symbol=source.symbol,
                    trade_date=trade_date,
                    provider=source.provider,
                    source_role=role,
                    source_run_id=source.source_run_id,
                    selected=role is not SourceRole.VALIDATION,
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                    observation_sha256=observation_sha256,
                )
            )
        return tuple(sorted(rows, key=lambda item: item.trade_date))

    @staticmethod
    def _discrepancy_count(
        twse_rows: Sequence[DatasetObservation],
        validation_rows: Sequence[DatasetObservation],
    ) -> int:
        canonical = {item.trade_date: item for item in twse_rows}
        validation = {item.trade_date: item for item in validation_rows}
        count = 0
        for trade_date in sorted(set(canonical) & set(validation)):
            left = canonical[trade_date]
            right = validation[trade_date]
            count += sum(
                getattr(left, field) != getattr(right, field)
                for field in ("open", "high", "low", "close", "volume")
            )
        return count

    def _result_from_version(
        self,
        version: MixedDatasetVersion,
        *,
        outcome: str,
        replayed: bool = True,
        provider_request_count: int = 0,
        checkpoint_reused: bool = False,
        remaining_dates: tuple[date, ...] = (),
        twse_failure_class: FailureClassification | str | None = None,
        source_eligible: bool | None = None,
        validation_status: str | None = None,
    ) -> DailyDatasetPreparationResult:
        identity = version.identity
        quality = {
            DatasetSourceStatus.CANONICAL_COMPLETE: "canonical",
            DatasetSourceStatus.PROVISIONAL_MIXED: "provisional",
            DatasetSourceStatus.RECONCILED: "reconciled",
        }[identity.source_status]
        if validation_status is None:
            validation_status = (
                "available"
                if identity.source_status is DatasetSourceStatus.CANONICAL_COMPLETE
                and version.provenance_summary.validation_sources
                else "not_recorded"
            )
        failure = _coerce_failure(twse_failure_class)
        return DailyDatasetPreparationResult(
            symbol=identity.symbol,
            target_date=identity.as_of_date,
            dataset_version_id=identity.dataset_version_id,
            source_policy=identity.source_policy,
            source_status=identity.source_status.value,
            authority_status=identity.authority_status.value,
            research_data_quality=quality,
            twse_coverage=identity.coverage.twse_observation_count,
            esun_count=identity.coverage.esun_supplemental_count,
            discrepancy_count=identity.coverage.discrepancy_count,
            preparation_outcome=outcome,
            provenance_sha256=identity.provenance_map_sha256,
            replayed=replayed,
            provider_request_count=provider_request_count,
            checkpoint_reused=checkpoint_reused,
            remaining_dates=remaining_dates,
            twse_failure_class=None if failure is None else failure.value,
            supplemental_source_eligible=source_eligible,
            validation_status=validation_status,
            reconciliation_status=identity.reconciliation_status.value,
        )

    @staticmethod
    def _copy_result(
        result: DailyDatasetPreparationResult,
        *,
        replayed: bool,
        provider_request_count: int,
    ) -> DailyDatasetPreparationResult:
        return DailyDatasetPreparationResult(
            symbol=result.symbol,
            target_date=result.target_date,
            dataset_version_id=result.dataset_version_id,
            source_policy=result.source_policy,
            source_status=result.source_status,
            authority_status=result.authority_status,
            research_data_quality=result.research_data_quality,
            twse_coverage=result.twse_coverage,
            esun_count=result.esun_count,
            discrepancy_count=result.discrepancy_count,
            preparation_outcome="replay",
            provenance_sha256=result.provenance_sha256,
            replayed=replayed,
            provider_request_count=provider_request_count,
            checkpoint_reused=result.checkpoint_reused,
            remaining_dates=result.remaining_dates,
            twse_failure_class=result.twse_failure_class,
            supplemental_source_eligible=result.supplemental_source_eligible,
            validation_status=result.validation_status,
            reconciliation_status=result.reconciliation_status,
        )

    @staticmethod
    def _failed_result(
        source: DS6SourcePreparation,
        *,
        outcome: str,
        provider_request_count: int,
        checkpoint_reused: bool,
        remaining_dates: tuple[date, ...] = (),
        failure_class: FailureClassification | str | None = None,
        source_eligible: bool | None = None,
        validation_status: str = "not_requested",
        error: BaseException | None = None,
    ) -> DailyDatasetPreparationResult:
        # ``error`` is intentionally represented only by the outcome; raw
        # provider messages never enter the immutable preparation result.
        del error
        failure = _coerce_failure(failure_class if failure_class is not None else source.failure_class)
        return DailyDatasetPreparationResult(
            symbol=source.symbol,
            target_date=source.target_date,
            dataset_version_id=None,
            source_policy=TWSE_DUAL_SOURCE_POLICY,
            source_status=None,
            authority_status=None,
            research_data_quality=None,
            twse_coverage=len(source.observations),
            esun_count=0,
            discrepancy_count=0,
            preparation_outcome=outcome,
            provenance_sha256=None,
            replayed=False,
            provider_request_count=provider_request_count,
            checkpoint_reused=checkpoint_reused,
            remaining_dates=remaining_dates or source.remaining_dates,
            twse_failure_class=None if failure is None else failure.value,
            supplemental_source_eligible=source_eligible,
            validation_status=validation_status,
            reconciliation_status=None,
        )

    def _within_grace(self) -> bool:
        return self._grace_remaining_seconds() > 0

    def _grace_remaining_seconds(self) -> float:
        now = self.clock()
        if not isinstance(now, datetime):
            raise DS6PreparationError("clock must return datetime")
        if now.tzinfo is None:
            raise DS6PreparationError("clock datetime must be timezone-aware")
        end = datetime.combine(now.date(), self.configuration.grace_end, tzinfo=now.tzinfo)
        return max(0.0, (end - now).total_seconds())


__all__ = [
    "DEFAULT_DS6_CANDIDATE_LIMIT",
    "DEFAULT_DS6_GRACE_END",
    "DEFAULT_DS6_MAX_TWSE_ATTEMPTS",
    "DEFAULT_DS6_RETRY_DELAY_SECONDS",
    "DEFAULT_DS6_TARGET_OBSERVATIONS",
    "DS6AmbiguousVersionError",
    "DS6Configuration",
    "DS6DailyPreparationCoordinator",
    "DS6LatestDateError",
    "DS6PreparationError",
    "DS6SourcePreparation",
    "DS6ResumeState",
    "DS6_DAILY_PREPARATION_CONTRACT_VERSION",
    "DS6_DAILY_PREPARATION_FACTORY_LOCATOR",
    "DS6_METHODOLOGY_VERSION",
    "DailyDatasetPreparationResult",
    "DailyPreparationIntegrationResult",
    "ExplicitDatasetVersionConsumer",
]
