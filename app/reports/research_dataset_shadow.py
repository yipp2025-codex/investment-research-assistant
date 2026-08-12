"""Shadow-only compatibility projection from ResearchDataset into Phase 6B.

This module does not replace or modify the production repository path.  It
projects one immutable dataset snapshot through the existing Phase 6B
``build_payload`` implementation so tests can compare both read paths.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Mapping

from app.as_of_policy import FrozenAsOfPolicyV1
from app.reports.daily_research import DailyResearchReportService
from app.reports.research_dataset_compat import (
    ResearchDatasetCompatibilityError,
)
from app.research_dataset import (
    DatasetArtifactRef,
    ResearchDataset,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
)


class ResearchDatasetShadowError(RuntimeError):
    """A snapshot cannot be projected into the frozen Phase 6B read shape."""


@dataclass(frozen=True, slots=True)
class ShadowProjectionResult:
    """Projected payload plus evidence intentionally kept outside canonical JSON."""

    snapshot: ResearchDatasetSnapshot
    payload: Mapping[str, object]
    canonical_json: str
    payload_sha256: str
    markdown: str
    result_id: str
    report_id: str
    artifact_refs: tuple[DatasetArtifactRef, ...]
    previous_successful_candidate_id: str | None
    previous_comparable_result_id: str | None
    previous_any_methodology_result_id: str | None


class ResearchDatasetShadowProjector:
    """Project a full dataset snapshot through the unchanged Phase 6B builder."""

    __slots__ = ("_dataset",)

    def __init__(self, dataset: ResearchDataset) -> None:
        self._dataset = dataset

    def project(
        self,
        service: DailyResearchReportService,
        request: ResearchDatasetRequest,
        *,
        requested_date: date | None = None,
        batch_run_id: str | None = None,
        symbol_run_id: str | None = None,
    ) -> ShadowProjectionResult:
        if request.history_observations is not None:
            raise ResearchDatasetShadowError(
                "Phase 6B shadow parity requires complete history"
            )
        snapshot = self._dataset.read(request)
        effective_requested_date = requested_date or request.as_of_date
        try:
            payload = service._build_payload_from_snapshot(
                snapshot,
                request.symbol,
                request.as_of_date,
                requested_date=effective_requested_date,
                batch_run_id=batch_run_id,
                symbol_run_id=symbol_run_id,
                pipeline_run_id=request.pipeline_run_id,
                validation_run_id=request.validation_run_id,
                historical_run_id=request.historical_run_id,
            )
        except ResearchDatasetCompatibilityError as error:
            raise ResearchDatasetShadowError(str(error)) from error
        canonical_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        payload_sha256 = hashlib.sha256(
            canonical_json.encode("utf-8")
        ).hexdigest()
        # Production renders ``StoredDailyResearchResult.payload``, which is
        # decoded from the sorted canonical JSON rather than the pre-storage
        # insertion order of ``build_payload``.
        markdown = service.renderer(json.loads(canonical_json))
        result_id = str(payload["result_id"])

        policy = FrozenAsOfPolicyV1()
        previous_candidate = (
            service.result_reader.get_previous_successful_result(
                request.symbol,
                request.as_of_date,
                service.methodology_version,
            )
        )
        previous_comparable = (
            previous_candidate
            if previous_candidate is not None
            and policy.previous_result_is_comparable(
                previous_candidate,
                symbol=request.symbol,
                target_date=request.as_of_date,
                methodology_version=service.methodology_version,
            )
            else None
        )
        previous_any = service.result_reader.get_previous_result_any_methodology(
            request.symbol,
            request.as_of_date,
        )
        return ShadowProjectionResult(
            snapshot=snapshot,
            payload=payload,
            canonical_json=canonical_json,
            payload_sha256=payload_sha256,
            markdown=markdown,
            result_id=result_id,
            report_id=f"report-{result_id}",
            artifact_refs=snapshot.provenance.artifact_refs,
            previous_successful_candidate_id=(
                None if previous_candidate is None else previous_candidate.result_id
            ),
            previous_comparable_result_id=(
                None if previous_comparable is None else previous_comparable.result_id
            ),
            previous_any_methodology_result_id=(
                None if previous_any is None else previous_any.result_id
            ),
        )

__all__ = [
    "ResearchDatasetShadowError",
    "ResearchDatasetShadowProjector",
    "ShadowProjectionResult",
]
