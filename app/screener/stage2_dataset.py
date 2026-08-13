"""Single-read M9 composition for pure Stage 2 candidate research."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping

from app.research_dataset import ResearchDataset, ResearchDatasetRequest
from app.screener.stage1 import Stage1ScanResult
from app.screener.stage2 import (
    STAGE2_METHODOLOGY_V1,
    Stage2AnalysisStatus,
    Stage2CandidateResult,
    Stage2ContractError,
    Stage2Methodology,
    failed_stage2_candidate,
    finalize_stage2_result,
    research_stage2_candidate,
)


class Stage2CompositionError(Stage2ContractError):
    """Stage 1 shortlist cannot be safely composed with M9 reads."""


@dataclass(frozen=True, slots=True)
class Stage2DatasetResearchEvidence:
    result: Stage2CandidateResult
    dataset_reads: int
    observations_consumed: int
    failed_candidates: int
    history_observations: int
    dataset_version_ids: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.result, Stage2CandidateResult):
            raise Stage2CompositionError("result must be Stage2CandidateResult")
        for field_name in (
            "dataset_reads",
            "observations_consumed",
            "failed_candidates",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise Stage2CompositionError(f"{field_name} must be non-negative")
        if self.dataset_reads != self.result.candidate_count:
            raise Stage2CompositionError(
                "dataset_reads must equal shortlisted candidate_count"
            )
        actual_failures = sum(
            item.analysis_status is Stage2AnalysisStatus.FAILED
            for item in self.result.candidates
        )
        if self.failed_candidates != actual_failures:
            raise Stage2CompositionError("failed_candidates is inconsistent")
        if self.history_observations != 250:
            raise Stage2CompositionError("history_observations must remain 250")


def research_stage2_from_dataset(
    *,
    stage1_result: Stage1ScanResult,
    dataset: ResearchDataset,
    market_date: date,
    dataset_version_ids: Mapping[str, str] | None = None,
    methodology: Stage2Methodology = STAGE2_METHODOLOGY_V1,
) -> Stage2DatasetResearchEvidence:
    """Research only the immutable Stage 1 shortlist, once per candidate."""

    if not isinstance(stage1_result, Stage1ScanResult):
        raise Stage2CompositionError("stage1_result is invalid")
    if methodology is not STAGE2_METHODOLOGY_V1:
        raise Stage2CompositionError("composition requires frozen Stage 2 v1")
    if stage1_result.market_date != market_date:
        raise Stage2CompositionError("Stage 1 and Stage 2 market_date differ")
    if dataset_version_ids is not None:
        if not isinstance(dataset_version_ids, Mapping):
            raise Stage2CompositionError("dataset_version_ids must be a symbol mapping")
        extra_versions = set(dataset_version_ids) - {
            item.symbol for item in stage1_result.candidates
        }
        if extra_versions:
            raise Stage2CompositionError(
                "dataset_version_ids contains symbols outside the Stage 1 shortlist: "
                + ", ".join(sorted(extra_versions))
            )
        shortlist_symbols = {item.symbol for item in stage1_result.candidates}
        missing_versions = shortlist_symbols - set(dataset_version_ids)
        if missing_versions:
            raise Stage2CompositionError(
                "dataset_version_ids must explicitly cover every Stage 1 candidate: "
                + ", ".join(sorted(missing_versions))
            )

    drafts = []
    read_symbols: set[str] = set()
    observations_consumed = 0
    for candidate in stage1_result.candidates:
        if candidate.symbol in read_symbols:
            raise Stage2CompositionError(
                f"duplicate Stage 1 candidate {candidate.symbol}"
            )
        read_symbols.add(candidate.symbol)
        try:
            snapshot = dataset.read(
                ResearchDatasetRequest(
                    symbol=candidate.symbol,
                    as_of_date=market_date,
                    history_observations=methodology.history_observations,
                    dataset_version_id=(
                        None
                        if dataset_version_ids is None
                        else dataset_version_ids.get(candidate.symbol)
                    ),
                )
            )
            observations_consumed += len(snapshot.price_history.observations)
            draft = research_stage2_candidate(
                stage1_candidate=candidate,
                dataset_snapshot=snapshot,
                market_date=market_date,
                methodology=methodology,
            )
        except Exception as error:
            draft = failed_stage2_candidate(
                stage1_candidate=candidate,
                error=error,
                market_date=market_date,
            )
        drafts.append(draft)

    result = finalize_stage2_result(
        candidates=tuple(drafts),
        market_date=market_date,
        methodology=methodology,
    )
    return Stage2DatasetResearchEvidence(
        result=result,
        dataset_reads=len(read_symbols),
        observations_consumed=observations_consumed,
        failed_candidates=sum(
            item.analysis_status is Stage2AnalysisStatus.FAILED
            for item in result.candidates
        ),
        history_observations=methodology.history_observations,
        dataset_version_ids=tuple(
            sorted(
                (symbol, value)
                for symbol, value in (dataset_version_ids or {}).items()
                if symbol in read_symbols
            )
        ),
    )
