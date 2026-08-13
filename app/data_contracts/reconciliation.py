"""DS4 pure reconciliation and immutable dataset-lineage contracts.

The functions in this module consume already-normalized DS3 values.  They do
not fetch providers, inspect a database, run a screener stage, or mutate the
parent dataset.  A reconciliation always produces a new child version.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
import re
from typing import Any, Iterable

from .dataset_persistence import (
    CoverageBasis,
    DatasetArtifactRef,
    DatasetObservation,
    DatasetPersistenceContractError,
    MixedDatasetVersion,
)
from .dual_source import (
    AuthorityStatus,
    DatasetCoverage,
    DatasetProvenanceSummary,
    DatasetSourceStatus,
    DatasetVersionIdentity,
    ReconciliationStatus,
    SourceRole,
    TWSE_DUAL_SOURCE_POLICY,
    TWSE_PROVIDER_IDENTITIES,
    canonical_json,
    sha256_text,
)


RECONCILIATION_CONTRACT_VERSION = "reconciliation-lineage-contract.v1"
RECONCILIATION_RELATION_DATASET = "reconciliation_relation_v1"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SECRET_FRAGMENTS = (
    "authorization=",
    "authorization:",
    "api_key=",
    "apikey=",
    "password=",
    "secret=",
    "token=",
    "bearer ",
)


class ReconciliationContractError(DatasetPersistenceContractError):
    """The normalized DS4 input or lineage state is invalid."""


class ReconciliationIntegrityError(RuntimeError):
    """A parent/child lineage or reconciliation relation fails closed."""


def _error(field_name: str, message: str) -> ReconciliationContractError:
    return ReconciliationContractError(f"{field_name}: {message}")


def _sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise _error(field_name, "must be a lowercase SHA-256 hex string")
    return value


def _text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _error(field_name, "must be a non-blank string")
    normalized = value.strip()
    lowered = normalized.lower()
    if any(fragment in lowered for fragment in _SECRET_FRAGMENTS):
        raise _error(field_name, "must not contain credential-like material")
    return normalized


def _dates(values: Iterable[date], field_name: str) -> tuple[date, ...]:
    if isinstance(values, (str, bytes)):
        raise _error(field_name, "must be an iterable of dates")
    try:
        normalized = tuple(values)
    except TypeError as exc:
        raise _error(field_name, "must be an iterable of dates") from exc
    if any(not isinstance(value, date) for value in normalized):
        raise _error(field_name, "must contain date values")
    if len(set(normalized)) != len(normalized):
        raise _error(field_name, "must not contain duplicate dates")
    return tuple(sorted(normalized))


def _ohlcv(item: DatasetObservation) -> dict[str, Any]:
    return {
        "trade_date": item.trade_date.isoformat(),
        "open": item.open,
        "high": item.high,
        "low": item.low,
        "close": item.close,
        "volume": item.volume,
    }


def _same_ohlcv(left: DatasetObservation, right: DatasetObservation) -> bool:
    return (
        left.open,
        left.high,
        left.low,
        left.close,
        left.volume,
    ) == (
        right.open,
        right.high,
        right.low,
        right.close,
        right.volume,
    )


def _validate_parent(parent: MixedDatasetVersion) -> None:
    if not isinstance(parent, MixedDatasetVersion):
        raise _error("parent", "must be a MixedDatasetVersion")
    if parent.identity.source_policy != TWSE_DUAL_SOURCE_POLICY:
        raise _error("parent.source_policy", "must be twse_dual_source_v1")
    if parent.identity.source_status is not DatasetSourceStatus.PROVISIONAL_MIXED:
        raise _error("parent.source_status", "must be provisional_mixed")
    if parent.identity.authority_status is not AuthorityStatus.INCOMPLETE:
        raise _error("parent.authority_status", "must be incomplete")
    if parent.identity.reconciliation_status is not ReconciliationStatus.PENDING:
        raise _error("parent.reconciliation_status", "must be pending")
    if parent.identity.coverage.esun_supplemental_count <= 0:
        raise _error("parent.coverage", "must contain selected supplemental rows")
    if parent.identity.parent_dataset_version_id == parent.identity.dataset_version_id:
        raise _error("parent_dataset_version_id", "parent cannot self-reference")
    if parent.identity.dataset_version_id != sha256_text(parent.identity.canonical_json()):
        raise _error("parent.dataset_version_id", "does not match identity serialization")
    if parent.canonical_sha256 != sha256_text(parent.canonical_json()):
        raise _error("parent.canonical_sha256", "does not match canonical serialization")


@dataclass(frozen=True, slots=True)
class ReconciliationInput:
    """Pure, normalized input for one bounded reconciliation operation."""

    parent: MixedDatasetVersion
    parent_dataset_version_id: str
    parent_canonical_sha256: str
    twse_observations: tuple[DatasetObservation, ...]
    twse_source_run_id: str
    twse_artifact: DatasetArtifactRef
    target_dates: tuple[date, ...]

    def __post_init__(self) -> None:
        _validate_parent(self.parent)
        parent_id = _sha256(self.parent_dataset_version_id, "parent_dataset_version_id")
        parent_sha = _sha256(self.parent_canonical_sha256, "parent_canonical_sha256")
        if parent_id != self.parent.identity.dataset_version_id:
            raise _error("parent_dataset_version_id", "does not match parent identity")
        if parent_sha != self.parent.canonical_sha256:
            raise _error("parent_canonical_sha256", "does not match parent canonical SHA")

        run_id = _text(self.twse_source_run_id, "twse_source_run_id")
        if not isinstance(self.twse_artifact, DatasetArtifactRef):
            raise _error("twse_artifact", "must be a DatasetArtifactRef")
        if self.twse_artifact.provider not in TWSE_PROVIDER_IDENTITIES:
            raise _error("twse_artifact.provider", "must be a TWSE provider")
        if self.twse_artifact.dataset == RECONCILIATION_RELATION_DATASET:
            raise _error("twse_artifact.dataset", "must be a source artifact")
        if self.twse_artifact.source_ref.lower().startswith("reconciliation://"):
            raise _error("twse_artifact.source_ref", "must not point to a relation")

        observations = tuple(self.twse_observations)
        if not observations:
            raise _error("twse_observations", "must not be empty")
        if any(not isinstance(item, DatasetObservation) for item in observations):
            raise _error("twse_observations", "must contain DatasetObservation values")
        if any(
            item.symbol != self.parent.identity.symbol
            or item.provider not in TWSE_PROVIDER_IDENTITIES
            or item.source_role is not SourceRole.CANONICAL
            or not item.selected
            or item.source_run_id != run_id
            for item in observations
        ):
            raise _error(
                "twse_observations",
                "must be selected canonical TWSE rows from the declared source run",
            )
        if any(item.provider != self.twse_artifact.provider for item in observations):
            raise _error(
                "twse_artifact.provider",
                "must match the provider of every TWSE observation",
            )
        observation_dates = tuple(item.trade_date for item in observations)
        if len(set(observation_dates)) != len(observation_dates):
            raise _error("twse_observations", "must contain at most one row per date")

        target_dates = _dates(self.target_dates, "target_dates")
        if not target_dates:
            raise _error("target_dates", "must not be empty")
        if set(target_dates) != set(observation_dates):
            raise _error("target_dates", "must exactly match TWSE observation dates")
        if any(item > self.parent.identity.as_of_date for item in target_dates):
            raise _error("target_dates", "must not contain future dates")

        parent_supplemental_dates = {
            item.trade_date
            for item in self.parent.observations
            if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
        }
        if not set(target_dates) <= parent_supplemental_dates:
            raise _error(
                "target_dates", "may contain only dates originally selected as supplemental"
            )

        object.__setattr__(self, "parent_dataset_version_id", parent_id)
        object.__setattr__(self, "parent_canonical_sha256", parent_sha)
        object.__setattr__(self, "twse_observations", tuple(sorted(observations, key=lambda item: item.trade_date)))
        object.__setattr__(self, "twse_source_run_id", run_id)
        object.__setattr__(self, "target_dates", target_dates)

    @property
    def twse_artifact_sha256(self) -> str:
        return self.twse_artifact.payload_sha256


@dataclass(frozen=True, slots=True)
class ReconciliationComparison:
    trade_date: date
    twse_observation: DatasetObservation
    prior_supplemental_observation: DatasetObservation
    classification: str

    def __post_init__(self) -> None:
        if not isinstance(self.trade_date, date):
            raise _error("trade_date", "must be a date")
        if self.twse_observation.trade_date != self.trade_date:
            raise _error("twse_observation", "date does not match comparison date")
        if self.prior_supplemental_observation.trade_date != self.trade_date:
            raise _error("prior_supplemental_observation", "date does not match comparison date")
        if (
            self.twse_observation.source_role is not SourceRole.CANONICAL
            or not self.twse_observation.selected
            or self.twse_observation.provider not in TWSE_PROVIDER_IDENTITIES
        ):
            raise _error("twse_observation", "must be selected canonical TWSE")
        if (
            self.prior_supplemental_observation.source_role is not SourceRole.SUPPLEMENTAL
            or not self.prior_supplemental_observation.selected
        ):
            raise _error(
                "prior_supplemental_observation",
                "must be the parent's selected supplemental row",
            )
        if self.classification not in {"equal", "discrepant"}:
            raise _error("classification", "must be equal or discrepant")
        expected = "equal" if _same_ohlcv(self.twse_observation, self.prior_supplemental_observation) else "discrepant"
        if self.classification != expected:
            raise _error("classification", "does not match OHLCV comparison")

    def as_dict(self) -> dict[str, Any]:
        return {
            "trade_date": self.trade_date.isoformat(),
            "twse": self.twse_observation.as_dict(),
            "prior_esun_supplemental": self.prior_supplemental_observation.as_dict(),
            "classification": self.classification,
        }


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Pure result containing a new immutable child and deterministic evidence."""

    parent_dataset_version_id: str
    parent_canonical_sha256: str
    new_dataset_version: MixedDatasetVersion
    comparisons: tuple[ReconciliationComparison, ...]
    compared_dates: tuple[date, ...]
    equal_dates: tuple[date, ...]
    discrepant_dates: tuple[date, ...]
    still_pending_dates: tuple[date, ...]
    reconciliation_status: ReconciliationStatus
    discrepancy_count: int
    twse_source_run_id: str
    twse_artifact_sha256: str
    reconciliation_evidence_sha256: str

    def __post_init__(self) -> None:
        parent_id = _sha256(self.parent_dataset_version_id, "parent_dataset_version_id")
        parent_sha = _sha256(self.parent_canonical_sha256, "parent_canonical_sha256")
        if self.new_dataset_version.identity.parent_dataset_version_id != parent_id:
            raise _error("new_dataset_version", "does not point to the parent")
        comparisons = tuple(sorted(self.comparisons, key=lambda item: item.trade_date))
        compared = _dates(self.compared_dates, "compared_dates")
        equal = _dates(self.equal_dates, "equal_dates")
        discrepant = _dates(self.discrepant_dates, "discrepant_dates")
        pending = _dates(self.still_pending_dates, "still_pending_dates")
        if tuple(item.trade_date for item in comparisons) != compared:
            raise _error("comparisons", "must match compared_dates")
        if set(equal) | set(discrepant) != set(compared):
            raise _error("comparison_dates", "equal/discrepant dates must partition compared dates")
        if set(equal) & set(discrepant):
            raise _error("comparison_dates", "equal and discrepant dates must not overlap")
        if set(compared) & set(pending):
            raise _error("pending_dates", "pending dates must not be compared")
        if not isinstance(self.reconciliation_status, ReconciliationStatus):
            try:
                status = ReconciliationStatus(self.reconciliation_status)
            except (TypeError, ValueError) as exc:
                raise _error("reconciliation_status", "contains an unsupported value") from exc
        else:
            status = self.reconciliation_status
        if isinstance(self.discrepancy_count, bool) or not isinstance(self.discrepancy_count, int) or self.discrepancy_count < 0:
            raise _error("discrepancy_count", "must be a non-negative integer")
        if self.discrepancy_count != self.new_dataset_version.identity.coverage.discrepancy_count:
            raise _error("discrepancy_count", "must match child coverage")
        if status is ReconciliationStatus.PENDING and not pending:
            raise _error("reconciliation_status", "pending requires remaining dates")
        if status is not ReconciliationStatus.PENDING and pending:
            raise _error("reconciliation_status", "final status cannot have pending dates")
        if status is ReconciliationStatus.RECONCILED_EQUAL and self.discrepancy_count != 0:
            raise _error("reconciliation_status", "reconciled_equal cannot have discrepancies")
        if status is ReconciliationStatus.RECONCILED_DISCREPANT and self.discrepancy_count <= 0:
            raise _error("reconciliation_status", "reconciled_discrepant requires discrepancy")
        evidence = self._evidence_payload()
        computed_evidence = sha256_text(canonical_json(evidence))
        if _sha256(self.reconciliation_evidence_sha256, "reconciliation_evidence_sha256") != computed_evidence:
            raise _error("reconciliation_evidence_sha256", "does not match deterministic evidence")
        object.__setattr__(self, "parent_dataset_version_id", parent_id)
        object.__setattr__(self, "parent_canonical_sha256", parent_sha)
        object.__setattr__(self, "comparisons", comparisons)
        object.__setattr__(self, "compared_dates", compared)
        object.__setattr__(self, "equal_dates", equal)
        object.__setattr__(self, "discrepant_dates", discrepant)
        object.__setattr__(self, "still_pending_dates", pending)
        object.__setattr__(self, "reconciliation_status", status)

    @property
    def result_kind(self) -> str:
        if self.still_pending_dates:
            return "partial"
        if self.discrepant_dates:
            return "discrepant"
        return "equal"

    def _evidence_payload(self) -> dict[str, Any]:
        return {
            "contract_version": RECONCILIATION_CONTRACT_VERSION,
            "parent_dataset_version_id": self.parent_dataset_version_id,
            "parent_canonical_sha256": self.parent_canonical_sha256,
            "twse_source_run_id": self.twse_source_run_id,
            "twse_artifact_sha256": self.twse_artifact_sha256,
            "compared_dates": [item.isoformat() for item in self.compared_dates],
            "equal_dates": [item.isoformat() for item in self.equal_dates],
            "discrepant_dates": [item.isoformat() for item in self.discrepant_dates],
            "still_pending_dates": [item.isoformat() for item in self.still_pending_dates],
            "comparisons": [item.as_dict() for item in self.comparisons],
            "reconciliation_status": self.reconciliation_status.value,
            "discrepancy_count": self.discrepancy_count,
        }

    def canonical_evidence_json(self) -> str:
        return canonical_json(self._evidence_payload())

    def as_dict(self) -> dict[str, Any]:
        return {
            "parent_dataset_version_id": self.parent_dataset_version_id,
            "parent_canonical_sha256": self.parent_canonical_sha256,
            "new_dataset_version_id": self.new_dataset_version.identity.dataset_version_id,
            "new_dataset_canonical_sha256": self.new_dataset_version.canonical_sha256,
            "result_kind": self.result_kind,
            "compared_dates": [item.isoformat() for item in self.compared_dates],
            "equal_dates": [item.isoformat() for item in self.equal_dates],
            "discrepant_dates": [item.isoformat() for item in self.discrepant_dates],
            "still_pending_dates": [item.isoformat() for item in self.still_pending_dates],
            "reconciliation_status": self.reconciliation_status.value,
            "discrepancy_count": self.discrepancy_count,
            "twse_source_run_id": self.twse_source_run_id,
            "twse_artifact_sha256": self.twse_artifact_sha256,
            "reconciliation_evidence_sha256": self.reconciliation_evidence_sha256,
        }


def _relation_artifact(
    *,
    provider: str,
    ordinal: int,
    parent_id: str,
    child_id: str,
    evidence_json: str,
    evidence_sha256: str,
) -> DatasetArtifactRef:
    return DatasetArtifactRef(
        ordinal=ordinal,
        provider=provider,
        dataset=RECONCILIATION_RELATION_DATASET,
        source_ref=f"reconciliation://{parent_id}/{child_id}",
        contract_version=RECONCILIATION_CONTRACT_VERSION,
        payload_sha256=evidence_sha256,
        payload_size_bytes=len(evidence_json.encode("utf-8")),
        hash_basis="canonical-json-v1",
    )


def _build_child(
    input_value: ReconciliationInput,
    comparisons: tuple[ReconciliationComparison, ...],
    pending_dates: tuple[date, ...],
) -> tuple[MixedDatasetVersion, str, str]:
    parent = input_value.parent
    compared_dates = {item.trade_date for item in comparisons}
    comparison_by_date = {item.trade_date: item for item in comparisons}
    child_observations: list[DatasetObservation] = []
    for item in parent.observations:
        if item.trade_date in compared_dates and item.source_role is SourceRole.SUPPLEMENTAL and item.selected:
            comparison = comparison_by_date[item.trade_date]
            child_observations.append(
                DatasetObservation(
                    symbol=item.symbol,
                    trade_date=item.trade_date,
                    provider=item.provider,
                    source_role=SourceRole.VALIDATION,
                    source_run_id=item.source_run_id,
                    selected=False,
                    open=item.open,
                    high=item.high,
                    low=item.low,
                    close=item.close,
                    volume=item.volume,
                    observation_sha256=item.observation_sha256,
                )
            )
            if comparison.prior_supplemental_observation != item:
                raise ReconciliationContractError(
                    "parent supplemental observation changed during reconciliation"
                )
        else:
            child_observations.append(item)
    child_observations.extend(input_value.twse_observations)

    current_discrepancies = sum(
        1 for item in comparisons if item.classification == "discrepant"
    )
    discrepancy_count = parent.identity.coverage.discrepancy_count + current_discrepancies
    latest_dates = list(input_value.target_dates)
    if parent.identity.coverage.latest_reconciled_date is not None:
        latest_dates.append(parent.identity.coverage.latest_reconciled_date)
    latest_reconciled_date = max(latest_dates) if latest_dates else None
    coverage = DatasetCoverage(
        required_observation_count=parent.identity.coverage.required_observation_count,
        twse_observation_count=parent.identity.coverage.twse_observation_count + len(comparisons),
        esun_supplemental_count=parent.identity.coverage.esun_supplemental_count - len(comparisons),
        missing_twse_count=(
            parent.identity.coverage.required_observation_count
            - (parent.identity.coverage.twse_observation_count + len(comparisons))
        ),
        discrepancy_count=discrepancy_count,
        latest_reconciled_date=latest_reconciled_date,
    )
    pending = bool(pending_dates)
    if pending:
        source_status = DatasetSourceStatus.PROVISIONAL_MIXED
        authority_status = AuthorityStatus.INCOMPLETE
        reconciliation_status = ReconciliationStatus.PENDING
    else:
        source_status = DatasetSourceStatus.RECONCILED
        authority_status = AuthorityStatus.RECONCILED
        reconciliation_status = (
            ReconciliationStatus.RECONCILED_DISCREPANT
            if discrepancy_count > 0
            else ReconciliationStatus.RECONCILED_EQUAL
        )
    summary = DatasetProvenanceSummary.from_observations(
        tuple(item.as_provenance() for item in child_observations),
        source_status=source_status,
        authority_status=authority_status,
        reconciliation_status=reconciliation_status,
        coverage=coverage,
        symbol=parent.identity.symbol,
    )
    identity = DatasetVersionIdentity.from_summary(
        summary,
        symbol=parent.identity.symbol,
        as_of_date=parent.identity.as_of_date,
        methodology_version=parent.identity.methodology_version,
        source_policy=TWSE_DUAL_SOURCE_POLICY,
        parent_dataset_version_id=parent.identity.dataset_version_id,
    )

    evidence_payload = {
        "contract_version": RECONCILIATION_CONTRACT_VERSION,
        "parent_dataset_version_id": parent.identity.dataset_version_id,
        "parent_canonical_sha256": parent.canonical_sha256,
        "twse_source_run_id": input_value.twse_source_run_id,
        "twse_artifact_sha256": input_value.twse_artifact_sha256,
        "compared_dates": [item.trade_date.isoformat() for item in comparisons],
        "equal_dates": [
            item.trade_date.isoformat()
            for item in comparisons
            if item.classification == "equal"
        ],
        "discrepant_dates": [
            item.trade_date.isoformat()
            for item in comparisons
            if item.classification == "discrepant"
        ],
        "still_pending_dates": [item.isoformat() for item in pending_dates],
        "comparisons": [item.as_dict() for item in comparisons],
        "reconciliation_status": reconciliation_status.value,
        "discrepancy_count": discrepancy_count,
    }
    evidence_json = canonical_json(evidence_payload)
    evidence_sha256 = sha256_text(evidence_json)
    twse_artifact = replace(
        input_value.twse_artifact,
        ordinal=len(parent.artifacts) + 1,
    )
    relation_artifact = _relation_artifact(
        provider=input_value.twse_artifact.provider,
        ordinal=len(parent.artifacts) + 2,
        parent_id=parent.identity.dataset_version_id,
        child_id=identity.dataset_version_id,
        evidence_json=evidence_json,
        evidence_sha256=evidence_sha256,
    )
    child = MixedDatasetVersion(
        identity=identity,
        provenance_summary=summary,
        observations=tuple(child_observations),
        artifacts=parent.artifacts + (twse_artifact, relation_artifact),
        coverage_basis=CoverageBasis(parent.coverage_basis),
    )
    return child, evidence_json, evidence_sha256


def reconcile_dataset_version(input_value: ReconciliationInput) -> ReconciliationResult:
    """Compare only the parent's selected supplemental dates and create a child."""

    if not isinstance(input_value, ReconciliationInput):
        raise _error("input", "must be a ReconciliationInput")
    parent = input_value.parent
    parent_supplemental = {
        item.trade_date: item
        for item in parent.observations
        if item.selected and item.source_role is SourceRole.SUPPLEMENTAL
    }
    twse_by_date = {item.trade_date: item for item in input_value.twse_observations}
    comparisons = tuple(
        ReconciliationComparison(
            trade_date=trade_date,
            twse_observation=twse_by_date[trade_date],
            prior_supplemental_observation=parent_supplemental[trade_date],
            classification=(
                "equal"
                if _same_ohlcv(twse_by_date[trade_date], parent_supplemental[trade_date])
                else "discrepant"
            ),
        )
        for trade_date in input_value.target_dates
    )
    compared_dates = tuple(item.trade_date for item in comparisons)
    equal_dates = tuple(
        item.trade_date for item in comparisons if item.classification == "equal"
    )
    discrepant_dates = tuple(
        item.trade_date for item in comparisons if item.classification == "discrepant"
    )
    still_pending_dates = tuple(
        sorted(set(parent_supplemental) - set(compared_dates))
    )
    child, evidence_json, evidence_sha256 = _build_child(
        input_value, comparisons, still_pending_dates
    )
    result = ReconciliationResult(
        parent_dataset_version_id=parent.identity.dataset_version_id,
        parent_canonical_sha256=parent.canonical_sha256,
        new_dataset_version=child,
        comparisons=comparisons,
        compared_dates=compared_dates,
        equal_dates=equal_dates,
        discrepant_dates=discrepant_dates,
        still_pending_dates=still_pending_dates,
        reconciliation_status=child.identity.reconciliation_status,
        discrepancy_count=child.identity.coverage.discrepancy_count,
        twse_source_run_id=input_value.twse_source_run_id,
        twse_artifact_sha256=input_value.twse_artifact_sha256,
        reconciliation_evidence_sha256=evidence_sha256,
    )
    if result.canonical_evidence_json() != evidence_json:
        raise ReconciliationIntegrityError(
            "reconciliation evidence changed during result construction"
        )
    return result


__all__ = [
    "RECONCILIATION_CONTRACT_VERSION",
    "RECONCILIATION_RELATION_DATASET",
    "ReconciliationComparison",
    "ReconciliationContractError",
    "ReconciliationInput",
    "ReconciliationIntegrityError",
    "ReconciliationResult",
    "reconcile_dataset_version",
]
