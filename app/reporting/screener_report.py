"""S6A deterministic Daily Screener report layer.

This module is intentionally downstream of S4.  It accepts an explicit
successful ``screener_run_id``, reconstructs the frozen S4 result through the
strict read-only replay contract, and derives JSON/Markdown artifacts without
providers, research engines, ranking, migrations, or database writes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import perf_counter
from typing import TypeAlias

from app.storage.screener_replay import (
    FrozenScreenerResult,
    ScreenerFinalizationStateError,
    ScreenerReplayIntegrityError,
    SQLiteScreenerReplayRepository,
)


REPORT_CONTRACT_VERSION = "daily-screener-report.v1"
REPORT_CONTENT_VERSION = "s6a-daily-screener-v1"
REPORT_CONTRACT_VERSION_V2 = "daily-screener-report.v2"
REPORT_CONTENT_VERSION_V2 = "s6a-daily-screener-v2"
_REPORT_ID_VERSION = "daily-screener-report-id.v1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")

JsonScalar: TypeAlias = str | int | float | None

SCOPE_DISCLAIMERS = (
    "本報告為描述性研究，僅重述已保存的 Screener 研究結果。",
    "本報告不包含買賣建議、目標價格、預期報酬或交易訊號。",
    "本報告不包含財務預測、停損、停利、部位配置或未來價格預測。",
)


class ScreenerReportError(RuntimeError):
    """Base error for the derived S6A report layer."""


class ScreenerReportInputError(ScreenerReportError):
    """The requested run is absent or not a sealed successful run."""


class ScreenerReportCollisionError(ScreenerReportError):
    """An existing derived artifact has different bytes."""


class ScreenerReportContractError(ScreenerReportError):
    """A derived report object violates the S6A contract."""


@dataclass(frozen=True, slots=True)
class ScreenerReportReason:
    """One persisted reason, retaining its stage and ordinal boundary."""

    stage: str
    ordinal: int
    code: str
    metric: str
    component: str | None
    previous: JsonScalar
    current: JsonScalar
    delta: float | None
    unit: str
    operator: str
    threshold: float | str
    rule_version: str
    role: str | None
    trigger_class: str | None
    reason_kind: str | None
    reason_class: str | None
    threshold_multiple: float

    def as_dict(self) -> dict[str, object]:
        return {
            "stage": self.stage,
            "ordinal": self.ordinal,
            "code": self.code,
            "metric": self.metric,
            "component": self.component,
            "previous": self.previous,
            "current": self.current,
            "delta": self.delta,
            "unit": self.unit,
            "operator": self.operator,
            "threshold": self.threshold,
            "rule_version": self.rule_version,
            "role": self.role,
            "trigger_class": self.trigger_class,
            "reason_kind": self.reason_kind,
            "reason_class": self.reason_class,
            "threshold_multiple": self.threshold_multiple,
        }


@dataclass(frozen=True, slots=True)
class ScreenerReportMetric:
    """One frozen Stage 2 metric with explicit NULL/unavailable semantics."""

    name: str
    status: str
    value: float | None
    previous_value: float | None
    delta: float | None
    unit: str
    as_of_date: date
    previous_as_of_date: date | None
    observations: int
    previous_observations: int

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "status": self.status,
            "value": self.value,
            "previous_value": self.previous_value,
            "delta": self.delta,
            "unit": self.unit,
            "as_of_date": self.as_of_date.isoformat(),
            "previous_as_of_date": (
                None
                if self.previous_as_of_date is None
                else self.previous_as_of_date.isoformat()
            ),
            "observations": self.observations,
            "previous_observations": self.previous_observations,
        }


@dataclass(frozen=True, slots=True)
class ScreenerReportDiscrepancy:
    field: str
    left_value: JsonScalar
    right_value: JsonScalar
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "field": self.field,
            "left_value": self.left_value,
            "right_value": self.right_value,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ScreenerReportEvidence:
    owner_kind: str
    owner_run_id: str
    provider: str
    dataset: str
    source_ref: str
    contract_version: str
    payload_sha256: str
    payload_size_bytes: int
    hash_basis: str

    def as_dict(self) -> dict[str, object]:
        return {
            "owner_kind": self.owner_kind,
            "owner_run_id": self.owner_run_id,
            "provider": self.provider,
            "dataset": self.dataset,
            "source_ref": self.source_ref,
            "contract_version": self.contract_version,
            "payload_sha256": self.payload_sha256,
            "payload_size_bytes": self.payload_size_bytes,
            "hash_basis": self.hash_basis,
        }


@dataclass(frozen=True, slots=True)
class ScreenerReportProvenance:
    pipeline_run_id: str | None
    historical_run_id: str | None
    validation_run_id: str | None
    canonical_sources: tuple[str, ...]
    validation_sources: tuple[str, ...]
    evidence_refs: tuple[ScreenerReportEvidence, ...]
    discrepancies: tuple[ScreenerReportDiscrepancy, ...]
    dataset_version_id: str | None = None
    source_policy: str = "twse_baseline"
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

    def as_dict(self) -> dict[str, object]:
        result = {
            "pipeline_run_id": self.pipeline_run_id,
            "historical_run_id": self.historical_run_id,
            "validation_run_id": self.validation_run_id,
            "canonical_sources": list(self.canonical_sources),
            "validation_sources": list(self.validation_sources),
            "evidence_refs": [item.as_dict() for item in self.evidence_refs],
            "discrepancies": [item.as_dict() for item in self.discrepancies],
        }
        if self.dataset_version_id is not None or self.research_data_quality != "canonical":
            result.update(
                {
                    "dataset_version_id": self.dataset_version_id,
                    "source_policy": self.source_policy,
                    "source_status": self.source_status,
                    "authority_status": self.authority_status,
                    "reconciliation_status": self.reconciliation_status,
                    "data_quality": self.research_data_quality,
                    "canonical_authority": self.canonical_authority,
                    "supplemental_sources": list(self.supplemental_sources),
                    "twse_observation_count": self.twse_observation_count,
                    "esun_supplemental_count": self.esun_supplemental_count,
                    "missing_twse_count": self.missing_twse_count,
                    "discrepancy_count": self.discrepancy_count,
                    "provenance_map_sha256": self.provenance_map_sha256,
                    "parent_dataset_version_id": self.parent_dataset_version_id,
                }
            )
        return result


@dataclass(frozen=True, slots=True)
class ScreenerReportCandidate:
    rank: int
    symbol: str
    name: str | None
    market: str
    candidate_kind: str
    data_quality_status: str
    validation_status: str
    analysis_status: str
    stage1_reasons: tuple[ScreenerReportReason, ...]
    stage2_reasons: tuple[ScreenerReportReason, ...]
    metrics: tuple[ScreenerReportMetric, ...]
    provenance: ScreenerReportProvenance
    failure: dict[str, str] | None
    research_data_quality: str = "canonical"

    def as_dict(self) -> dict[str, object]:
        result = {
            "rank": self.rank,
            "symbol": self.symbol,
            "name": self.name,
            "market": self.market,
            "candidate_kind": self.candidate_kind,
            "data_quality_status": self.data_quality_status,
            "validation_status": self.validation_status,
            "analysis_status": self.analysis_status,
            "stage1_reasons": [item.as_dict() for item in self.stage1_reasons],
            "stage2_reasons": [item.as_dict() for item in self.stage2_reasons],
            "metrics": [item.as_dict() for item in self.metrics],
            "provenance": self.provenance.as_dict(),
            "failure": self.failure,
        }
        if self.research_data_quality != "canonical" or self.provenance.dataset_version_id is not None:
            result["data_quality"] = self.research_data_quality
        return result


@dataclass(frozen=True, slots=True)
class ScreenerReport:
    """Immutable machine-readable S6A report derived from one frozen run."""

    report_id: str
    report_contract_version: str
    content_version: str
    market_date: date
    screener_run_id: str
    universe_run_id: str
    stage1_methodology_version: str
    stage2_methodology_version: str
    source_policy: str
    universe_count: int
    screened_count: int
    triggered_count: int
    candidate_count: int
    candidate_limit: int
    truncated: bool
    screener_canonical_sha256: str
    candidates: tuple[ScreenerReportCandidate, ...]
    scope_disclaimers: tuple[str, ...]
    execution_status: str = "success"
    research_data_quality: str = "canonical"
    canonical_authority: str = "twse"
    source_status: str = "canonical_complete"
    authority_status: str = "complete"
    reconciliation_status: str = "not_applicable"
    supplemental_candidate_count: int = 0
    supplemental_sources: tuple[str, ...] = ()
    twse_observation_count: int = 0
    esun_supplemental_count: int = 0
    missing_twse_count: int = 0
    discrepancy_count: int = 0
    dataset_version_ids: tuple[str, ...] = ()
    dataset_identity_sha256: str | None = None
    provenance_map_sha256: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "report_id",
            "screener_run_id",
            "universe_run_id",
            "screener_canonical_sha256",
        ):
            _require_sha256(getattr(self, field_name), field_name)
        if self.report_contract_version not in {
            REPORT_CONTRACT_VERSION,
            REPORT_CONTRACT_VERSION_V2,
        }:
            raise ScreenerReportContractError("unsupported report contract version")
        expected_content = (
            REPORT_CONTENT_VERSION_V2
            if self.report_contract_version == REPORT_CONTRACT_VERSION_V2
            else REPORT_CONTENT_VERSION
        )
        if self.content_version != expected_content:
            raise ScreenerReportContractError("unsupported report content version")
        if not isinstance(self.market_date, date):
            raise ScreenerReportContractError("market_date must be a date")
        if self.source_policy not in {"twse_baseline", "twse_dual_source_v1"}:
            raise ScreenerReportContractError("report source policy changed")
        if self.execution_status not in {"success", "provisional_success"}:
            raise ScreenerReportContractError("unsupported report execution status")
        if self.research_data_quality not in {"canonical", "provisional", "reconciled"}:
            raise ScreenerReportContractError("unsupported report data quality")
        if self.research_data_quality == "provisional" and self.execution_status != "provisional_success":
            raise ScreenerReportContractError("provisional report requires provisional_success")
        if self.report_contract_version == REPORT_CONTRACT_VERSION_V2 and not self.dataset_version_ids:
            raise ScreenerReportContractError("v2 report requires dataset version ids")
        for field_name in (
            "universe_count",
            "screened_count",
            "triggered_count",
            "candidate_count",
            "candidate_limit",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ScreenerReportContractError(f"{field_name} is invalid")
        if not isinstance(self.truncated, bool):
            raise ScreenerReportContractError("truncated must be boolean")
        if not isinstance(self.candidates, tuple):
            raise ScreenerReportContractError("candidates must be immutable")
        if self.candidate_count != len(self.candidates):
            raise ScreenerReportContractError("candidate_count is inconsistent")
        if tuple(item.rank for item in self.candidates) != tuple(
            range(1, self.candidate_count + 1)
        ):
            raise ScreenerReportContractError("candidate rank ordering changed")
        if not isinstance(self.scope_disclaimers, tuple) or not all(
            isinstance(item, str) and item for item in self.scope_disclaimers
        ):
            raise ScreenerReportContractError("scope disclaimers are invalid")

    @classmethod
    def from_frozen(cls, frozen: FrozenScreenerResult) -> "ScreenerReport":
        if not isinstance(frozen, FrozenScreenerResult):
            raise TypeError("frozen must be a FrozenScreenerResult")
        candidates = tuple(_candidate_from_frozen(item) for item in frozen.candidates)
        is_v2 = frozen.contract_version != "screener-frozen-result-v1"
        report_contract = REPORT_CONTRACT_VERSION_V2 if is_v2 else REPORT_CONTRACT_VERSION
        content_version = REPORT_CONTENT_VERSION_V2 if is_v2 else REPORT_CONTENT_VERSION
        identity = {
            "identity_version": _REPORT_ID_VERSION,
            "screener_run_id": frozen.screener_run_id,
            "report_contract_version": report_contract,
            "content_version": content_version,
        }
        if is_v2:
            identity.update(
                {
                    "dataset_identity_sha256": frozen.dataset_identity_sha256,
                    "provenance_map_sha256": frozen.provenance_map_sha256,
                    "dataset_version_ids": list(frozen.dataset_version_ids),
                }
            )
        return cls(
            report_id=_sha256(identity),
            report_contract_version=report_contract,
            content_version=content_version,
            market_date=frozen.market_date,
            screener_run_id=frozen.screener_run_id,
            universe_run_id=frozen.universe_run_id,
            stage1_methodology_version=frozen.stage1_methodology_version,
            stage2_methodology_version=frozen.stage2_methodology_version,
            source_policy=frozen.dataset_source_policy if is_v2 else frozen.source_policy,
            universe_count=frozen.universe_count,
            screened_count=frozen.screened_count,
            triggered_count=frozen.triggered_count,
            candidate_count=frozen.candidate_count,
            candidate_limit=frozen.candidate_limit,
            truncated=frozen.truncated,
            screener_canonical_sha256=frozen.payload_sha256,
            candidates=candidates,
            scope_disclaimers=SCOPE_DISCLAIMERS,
            execution_status=frozen.execution_status,
            research_data_quality=frozen.research_data_quality,
            canonical_authority="twse",
            source_status=frozen.source_status,
            authority_status=frozen.authority_status,
            reconciliation_status=frozen.reconciliation_status,
            supplemental_candidate_count=frozen.supplemental_candidate_count,
            supplemental_sources=tuple(
                sorted(
                    {
                        source
                        for candidate in frozen.candidates
                        for source in candidate.provenance.supplemental_sources
                    }
                )
            ),
            twse_observation_count=sum(
                candidate.provenance.twse_observation_count
                for candidate in frozen.candidates
            ),
            esun_supplemental_count=sum(
                candidate.provenance.esun_supplemental_count
                for candidate in frozen.candidates
            ),
            missing_twse_count=sum(
                candidate.provenance.missing_twse_count
                for candidate in frozen.candidates
            ),
            discrepancy_count=sum(
                candidate.provenance.discrepancy_count
                for candidate in frozen.candidates
            ),
            dataset_version_ids=frozen.dataset_version_ids,
            dataset_identity_sha256=frozen.dataset_identity_sha256,
            provenance_map_sha256=frozen.provenance_map_sha256,
        )

    def as_dict(self) -> dict[str, object]:
        result = {
            "report_contract_version": self.report_contract_version,
            "content_version": self.content_version,
            "report_id": self.report_id,
            "market_date": self.market_date.isoformat(),
            "screener_run_id": self.screener_run_id,
            "universe_run_id": self.universe_run_id,
            "methodology_versions": {
                "stage1": self.stage1_methodology_version,
                "stage2": self.stage2_methodology_version,
            },
            "source_policy": self.source_policy,
            "universe_count": self.universe_count,
            "screened_count": self.screened_count,
            "triggered_count": self.triggered_count,
            "candidate_count": self.candidate_count,
            "candidate_limit": self.candidate_limit,
            "truncated": self.truncated,
            "screener_canonical_sha256": self.screener_canonical_sha256,
            "candidates": [item.as_dict() for item in self.candidates],
            "scope_disclaimers": list(self.scope_disclaimers),
        }
        if self.report_contract_version == REPORT_CONTRACT_VERSION_V2:
            result.update(
                {
                    "execution_status": self.execution_status,
                    "data_quality": self.research_data_quality,
                    "canonical_authority": self.canonical_authority,
                    "source_status": self.source_status,
                    "authority_status": self.authority_status,
                    "reconciliation_status": self.reconciliation_status,
                    "supplemental_candidate_count": self.supplemental_candidate_count,
                    "supplemental_sources": list(self.supplemental_sources),
                    "twse_coverage": {
                        "twse_observation_count": self.twse_observation_count,
                        "missing_twse_count": self.missing_twse_count,
                    },
                    "esun_supplemental_count": self.esun_supplemental_count,
                    "discrepancy_count": self.discrepancy_count,
                    "dataset_version_ids": list(self.dataset_version_ids),
                    "dataset_identity_sha256": self.dataset_identity_sha256,
                    "provenance_map_sha256": self.provenance_map_sha256,
                }
            )
        return result

    def canonical_json(self) -> str:
        return _canonical_json(self.as_dict())

    @property
    def report_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ScreenerReportGeneration:
    """Report bytes plus non-canonical performance evidence."""

    report: ScreenerReport
    canonical_json: str
    markdown: str
    report_sha256: str
    total_seconds: float
    canonical_json_seconds: float
    markdown_seconds: float

    @property
    def json_bytes(self) -> bytes:
        return self.canonical_json.encode("utf-8")

    @property
    def markdown_bytes(self) -> bytes:
        return self.markdown.encode("utf-8")


@dataclass(frozen=True, slots=True)
class ScreenerReportGenerator:
    """Generate a report only from S4 strict replay."""

    database_path: Path
    replay_repository: SQLiteScreenerReplayRepository

    def __init__(
        self,
        database_path: str | Path,
        *,
        replay_repository: SQLiteScreenerReplayRepository | None = None,
    ) -> None:
        object.__setattr__(self, "database_path", Path(database_path))
        object.__setattr__(
            self,
            "replay_repository",
            replay_repository or SQLiteScreenerReplayRepository(database_path),
        )
        if not callable(getattr(self.replay_repository, "replay_run", None)):
            raise TypeError("replay_repository must expose replay_run")

    def generate(self, screener_run_id: str) -> ScreenerReportGeneration:
        """Generate canonical JSON and Markdown for one explicit run id."""

        started = perf_counter()
        try:
            replay = self.replay_repository.replay_run(screener_run_id)
        except KeyError as error:
            raise ScreenerReportInputError(
                "requested Screener run does not exist"
            ) from error
        except ScreenerFinalizationStateError as error:
            raise ScreenerReportInputError(
                "only a sealed successful Screener run can produce a report"
            ) from error
        frozen = replay.result
        if not isinstance(frozen, FrozenScreenerResult):
            raise ScreenerReportInputError("replay did not return a frozen result")

        report = ScreenerReport.from_frozen(frozen)
        canonical_started = perf_counter()
        canonical_json = report.canonical_json()
        report_sha256 = hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()
        canonical_seconds = perf_counter() - canonical_started

        markdown_started = perf_counter()
        from app.reporting.screener_markdown import render_markdown

        markdown = render_markdown(report, report_sha256)
        markdown_seconds = perf_counter() - markdown_started
        return ScreenerReportGeneration(
            report=report,
            canonical_json=canonical_json,
            markdown=markdown,
            report_sha256=report_sha256,
            total_seconds=perf_counter() - started,
            canonical_json_seconds=canonical_seconds,
            markdown_seconds=markdown_seconds,
        )


@dataclass(frozen=True, slots=True)
class ScreenerReportArtifact:
    format: str
    path: Path
    sha256: str
    size_bytes: int
    written: bool


@dataclass(frozen=True, slots=True)
class ScreenerReportWriteResult:
    report_sha256: str
    json_artifact: ScreenerReportArtifact
    markdown_artifact: ScreenerReportArtifact

    @property
    def no_op(self) -> bool:
        return not (self.json_artifact.written or self.markdown_artifact.written)


class ScreenerReportArtifactWriter:
    """Write JSON/Markdown derived artifacts with collision fail-closed rules."""

    def write(
        self,
        generation: ScreenerReportGeneration,
        *,
        output_directory: str | Path | None = None,
        json_path: str | Path | None = None,
        markdown_path: str | Path | None = None,
    ) -> ScreenerReportWriteResult:
        if not isinstance(generation, ScreenerReportGeneration):
            raise TypeError("generation must be a ScreenerReportGeneration")
        json_target, markdown_target = _resolve_output_paths(
            generation,
            output_directory=output_directory,
            json_path=json_path,
            markdown_path=markdown_path,
        )
        payloads = (
            ("json", json_target, generation.json_bytes),
            ("markdown", markdown_target, generation.markdown_bytes),
        )

        # Preflight every target before touching any target.  A collision in
        # one format must not leave the other format newly written.
        existing: dict[Path, bool] = {}
        for unused_format, path, payload in payloads:
            if path.exists():
                if not path.is_file() or path.read_bytes() != payload:
                    raise ScreenerReportCollisionError(
                        f"derived report artifact collision: {path}"
                    )
                existing[path] = True
            else:
                existing[path] = False

        staged: list[tuple[Path, Path]] = []
        try:
            for unused_format, path, payload in payloads:
                if existing[path]:
                    continue
                path.parent.mkdir(parents=True, exist_ok=True)
                fd, temporary_name = tempfile.mkstemp(
                    prefix=f".{path.name}.",
                    suffix=".tmp",
                    dir=str(path.parent),
                )
                temporary_path = Path(temporary_name)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                staged.append((temporary_path, path))

            artifacts: list[ScreenerReportArtifact] = []
            for format_name, path, payload in payloads:
                if path.exists():
                    if not path.is_file() or path.read_bytes() != payload:
                        raise ScreenerReportCollisionError(
                            f"derived report artifact collision: {path}"
                        )
                    written = False
                else:
                    temporary_path = next(
                        temporary
                        for temporary, target in staged
                        if target == path
                    )
                    os.replace(temporary_path, path)
                    written = True
                artifacts.append(
                    ScreenerReportArtifact(
                        format=format_name,
                        path=path,
                        sha256=hashlib.sha256(payload).hexdigest(),
                        size_bytes=len(payload),
                        written=written,
                    )
                )
            return ScreenerReportWriteResult(
                report_sha256=generation.report_sha256,
                json_artifact=artifacts[0],
                markdown_artifact=artifacts[1],
            )
        finally:
            for temporary_path, unused_target in staged:
                if temporary_path.exists():
                    temporary_path.unlink()


def _resolve_output_paths(
    generation: ScreenerReportGeneration,
    *,
    output_directory: str | Path | None,
    json_path: str | Path | None,
    markdown_path: str | Path | None,
) -> tuple[Path, Path]:
    if output_directory is not None and (json_path is not None or markdown_path is not None):
        raise ValueError("output_directory cannot be mixed with explicit paths")
    if output_directory is not None:
        directory = Path(output_directory)
        if generation.report.report_contract_version == REPORT_CONTRACT_VERSION_V2:
            stem = (
                f"daily-screener-{generation.report.market_date.isoformat()}-"
                f"{generation.report.screener_run_id[:16]}-v2"
            )
        else:
            stem = f"daily-screener-{generation.report.market_date.isoformat()}"
        return directory / f"{stem}.json", directory / f"{stem}.md"
    if json_path is None or markdown_path is None:
        raise ValueError(
            "provide output_directory or both json_path and markdown_path"
        )
    json_target = Path(json_path).resolve(strict=False)
    markdown_target = Path(markdown_path).resolve(strict=False)
    if json_target == markdown_target:
        raise ValueError("json_path and markdown_path must be distinct files")
    return json_target, markdown_target


def _candidate_from_frozen(candidate: object) -> ScreenerReportCandidate:
    stage1_reasons = tuple(
        _stage1_reason(item, ordinal)
        for ordinal, item in enumerate(candidate.stage1_reasons, 1)
    )
    stage2_reasons = tuple(
        _stage2_reason(item, ordinal)
        for ordinal, item in enumerate(candidate.stage2_reasons, 1)
    )
    metrics = tuple(_metric(item) for item in candidate.metrics)
    provenance = candidate.provenance
    return ScreenerReportCandidate(
        rank=candidate.rank,
        symbol=candidate.symbol,
        name=candidate.name,
        market=candidate.market,
        candidate_kind=_enum_value(candidate.candidate_kind),
        data_quality_status=_enum_value(candidate.data_quality.status),
        validation_status=candidate.data_quality.validation_status,
        analysis_status=_enum_value(candidate.analysis_status),
        stage1_reasons=stage1_reasons,
        stage2_reasons=stage2_reasons,
        metrics=metrics,
        provenance=ScreenerReportProvenance(
            pipeline_run_id=provenance.pipeline_run_id,
            historical_run_id=provenance.historical_run_id,
            validation_run_id=provenance.validation_run_id,
            canonical_sources=provenance.canonical_sources,
            validation_sources=provenance.validation_sources,
            evidence_refs=tuple(
                ScreenerReportEvidence(
                    owner_kind=item.owner_kind,
                    owner_run_id=item.owner_run_id,
                    provider=item.provider,
                    dataset=item.dataset,
                    source_ref=item.endpoint,
                    contract_version=item.contract_version,
                    payload_sha256=item.payload_sha256,
                    payload_size_bytes=item.payload_size_bytes,
                    hash_basis=item.hash_basis,
                )
                for item in provenance.artifact_refs
            ),
            discrepancies=tuple(
                ScreenerReportDiscrepancy(
                    field=item.field,
                    left_value=item.left_value,
                    right_value=item.right_value,
                    reason=item.reason,
                )
                for item in candidate.data_quality.discrepancies
            ),
            dataset_version_id=provenance.dataset_version_id,
            source_policy=provenance.source_policy,
            source_status=provenance.source_status,
            authority_status=provenance.authority_status,
            reconciliation_status=provenance.reconciliation_status,
            research_data_quality=provenance.research_data_quality,
            canonical_authority=provenance.canonical_authority,
            supplemental_sources=provenance.supplemental_sources,
            twse_observation_count=provenance.twse_observation_count,
            esun_supplemental_count=provenance.esun_supplemental_count,
            missing_twse_count=provenance.missing_twse_count,
            discrepancy_count=provenance.discrepancy_count,
            provenance_map_sha256=provenance.provenance_map_sha256,
            parent_dataset_version_id=provenance.parent_dataset_version_id,
        ),
        failure=(
            None
            if candidate.failure is None
            else {
                "status": candidate.failure.status,
                "code": candidate.failure.code,
                "error_type": candidate.failure.error_type,
            }
        ),
        research_data_quality=provenance.research_data_quality,
    )


def _stage1_reason(reason: object, ordinal: int) -> ScreenerReportReason:
    return ScreenerReportReason(
        stage="stage1",
        ordinal=ordinal,
        code=reason.code,
        metric=reason.metric,
        component=reason.component,
        previous=reason.previous,
        current=reason.current,
        delta=reason.delta,
        unit=reason.unit,
        operator=reason.operator,
        threshold=reason.threshold,
        rule_version=reason.rule_version,
        role=reason.role,
        trigger_class=reason.trigger_class,
        reason_kind=None,
        reason_class=None,
        threshold_multiple=reason.threshold_multiple,
    )


def _stage2_reason(reason: object, ordinal: int) -> ScreenerReportReason:
    return ScreenerReportReason(
        stage="stage2",
        ordinal=ordinal,
        code=reason.code,
        metric=reason.metric,
        component=None,
        previous=reason.previous,
        current=reason.current,
        delta=reason.delta,
        unit=reason.unit,
        operator=reason.operator,
        threshold=reason.threshold,
        rule_version=reason.rule_version,
        role=None,
        trigger_class=None,
        reason_kind=reason.reason_kind,
        reason_class=reason.reason_class,
        threshold_multiple=reason.threshold_multiple,
    )


def _metric(metric: object) -> ScreenerReportMetric:
    return ScreenerReportMetric(
        name=metric.name,
        status=_enum_value(metric.status),
        value=metric.value,
        previous_value=metric.previous_value,
        delta=metric.delta,
        unit=metric.unit,
        as_of_date=metric.as_of_date,
        previous_as_of_date=metric.previous_as_of_date,
        observations=metric.observations,
        previous_observations=metric.previous_observations,
    )


def _enum_value(value: object) -> str:
    raw = getattr(value, "value", value)
    if not isinstance(raw, str):
        raise ScreenerReportContractError("enum value must be text")
    return raw


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ScreenerReportContractError(f"{field_name} must be lowercase SHA-256")
    return value


__all__ = [
    "REPORT_CONTENT_VERSION",
    "REPORT_CONTENT_VERSION_V2",
    "REPORT_CONTRACT_VERSION",
    "REPORT_CONTRACT_VERSION_V2",
    "SCOPE_DISCLAIMERS",
    "ScreenerReport",
    "ScreenerReportArtifact",
    "ScreenerReportArtifactWriter",
    "ScreenerReportCandidate",
    "ScreenerReportCollisionError",
    "ScreenerReportContractError",
    "ScreenerReportDiscrepancy",
    "ScreenerReportError",
    "ScreenerReportEvidence",
    "ScreenerReportGeneration",
    "ScreenerReportGenerator",
    "ScreenerReportInputError",
    "ScreenerReportMetric",
    "ScreenerReportProvenance",
    "ScreenerReportReason",
    "ScreenerReportWriteResult",
]
