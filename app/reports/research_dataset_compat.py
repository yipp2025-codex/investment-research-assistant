"""Compatibility reads that project a ResearchDataset snapshot into Phase 6B."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from app.models import CrossValidationOutcome, PipelineRunStatus
from app.research_dataset import ResearchDatasetSnapshot


class ResearchDatasetCompatibilityError(RuntimeError):
    """A snapshot cannot satisfy the frozen Phase 6B input contract."""


@dataclass(frozen=True, slots=True)
class ProjectedPipelineRun:
    run_id: str
    source_endpoints: tuple[str, ...]
    fetched_at: datetime | None


@dataclass(frozen=True, slots=True)
class ProjectedValidationRun:
    run_id: str
    symbol: str
    target_date: date
    requested_start_date: date
    left_provider: str
    right_provider: str
    status: PipelineRunStatus
    outcome: CrossValidationOutcome
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    attempt_count: int = 1


class SnapshotResearchReadAdapter:
    """Expose snapshot prices, valuation, and pipeline as legacy read methods."""

    __slots__ = ("_snapshot", "_pipeline")

    def __init__(
        self,
        snapshot: ResearchDatasetSnapshot,
        pipeline: ProjectedPipelineRun | None,
    ) -> None:
        self._snapshot = snapshot
        self._pipeline = pipeline

    def list_daily_prices(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[object]:
        if symbol.strip().upper() != self._snapshot.symbol.symbol:
            return []
        return [
            item
            for item in self._snapshot.price_history.observations
            if (start_date is None or item.trade_date >= start_date)
            and (end_date is None or item.trade_date <= end_date)
        ]

    def list_company_metrics(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[object]:
        if symbol.strip().upper() != self._snapshot.symbol.symbol:
            return []
        return [
            item
            for item in self._snapshot.valuation.metrics
            if (start_date is None or item.metric_date >= start_date)
            and (end_date is None or item.metric_date <= end_date)
        ]

    def get_pipeline_run(self, run_id: str) -> object | None:
        if self._pipeline is None or self._pipeline.run_id != run_id:
            return None
        return self._pipeline

    def get_pipeline_run_for_target(
        self,
        symbol: str,
        target_date: date,
    ) -> object | None:
        if (
            symbol.strip().upper() != self._snapshot.symbol.symbol
            or target_date != self._snapshot.as_of.as_of_date
        ):
            return None
        return self._pipeline


class SnapshotValidationReadAdapter:
    """Expose selected snapshot validation evidence as legacy read methods."""

    __slots__ = ("_snapshot", "_run")

    def __init__(self, snapshot: ResearchDatasetSnapshot) -> None:
        self._snapshot = snapshot
        validation = snapshot.validation
        if validation.status == "missing_source":
            self._run = None
            return
        if any(
            item is None
            for item in (
                validation.run_id,
                validation.target_date,
                validation.left_provider,
                validation.right_provider,
                validation.outcome,
                validation.created_at,
            )
        ):
            raise ResearchDatasetCompatibilityError(
                "selected validation evidence is incomplete"
            )
        self._run = ProjectedValidationRun(
            run_id=validation.run_id,
            symbol=validation.symbol,
            target_date=validation.target_date,
            requested_start_date=validation.target_date,
            left_provider=validation.left_provider,
            right_provider=validation.right_provider,
            status=PipelineRunStatus.SUCCESS,
            outcome=CrossValidationOutcome(validation.outcome),
            created_at=validation.created_at,
        )

    def get_run(self, run_id: str) -> object | None:
        if self._run is None or self._run.run_id != run_id:
            return None
        return self._run

    def list_runs(self, symbol: str) -> list[object]:
        if self._run is None or self._run.symbol != symbol.strip().upper():
            return []
        return [self._run]

    def list_discrepancies(self, run_id: str) -> list[object]:
        if self._run is None or self._run.run_id != run_id:
            return []
        return list(self._snapshot.validation.discrepancies)

    def list_observations(self, run_id: str) -> list[object]:
        if self._run is None or self._run.run_id != run_id:
            return []
        return list(self._snapshot.validation.observations)


def project_pipeline_run(
    snapshot: ResearchDatasetSnapshot,
) -> ProjectedPipelineRun | None:
    """Recover only the pipeline endpoints visible to frozen 6B provenance."""

    provenance = snapshot.provenance
    if provenance.pipeline_run_id is None:
        return None
    pipeline_artifact_endpoints = {
        item.endpoint
        for item in provenance.artifact_refs
        if item.owner_kind == "pipeline"
        and item.owner_run_id == provenance.pipeline_run_id
    }
    if pipeline_artifact_endpoints:
        endpoints = pipeline_artifact_endpoints
    else:
        validation_endpoints = {
            endpoint
            for observation in snapshot.validation.observations
            for endpoint in observation.source_endpoints
        }
        artifact_endpoints = {item.endpoint for item in provenance.artifact_refs}
        endpoints = (
            set(provenance.source_endpoints)
            - validation_endpoints
            - artifact_endpoints
        )
        if provenance.historical_run_id is not None and endpoints:
            raise ResearchDatasetCompatibilityError(
                "pipeline endpoints are ambiguous without pipeline artifact refs"
            )
    return ProjectedPipelineRun(
        run_id=provenance.pipeline_run_id,
        source_endpoints=tuple(sorted(endpoints)),
        fetched_at=provenance.fetched_at,
    )


__all__ = [
    "ProjectedPipelineRun",
    "ProjectedValidationRun",
    "ResearchDatasetCompatibilityError",
    "SnapshotResearchReadAdapter",
    "SnapshotValidationReadAdapter",
    "project_pipeline_run",
]
