"""Pure Stage 2 candidate research contract for Market Screener S3.

Deep price indicators are produced exclusively through the frozen
``HistoricalResearchAnalyzer``.  This module accepts immutable Stage 1
candidates and M9 snapshots and performs no acquisition, persistence,
calendar resolution, or external I/O.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, replace
from datetime import date, datetime
from enum import Enum
from typing import TypeAlias

from app.analysis import HistoricalResearchAnalysis, HistoricalResearchAnalyzer
from app.models import DailyPrice
from app.research_dataset import (
    DatasetArtifactRef,
    DatasetDiscrepancy,
    ResearchDatasetSnapshot,
    TWSE_BASELINE_SOURCE_POLICY,
)
from app.screener.stage1 import (
    STAGE1_METHODOLOGY_VERSION,
    VALUATION_UPDATE,
    RuleRole,
    Stage1Candidate,
    Stage1Metric,
    Stage1Reason,
)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


STAGE2_METHODOLOGY_VERSION = "screener-stage2-v1"

RETURN_20D = "return_20d"
RETURN_60D = "return_60d"
RETURN_120D = "return_120d"
VOLATILITY_60D = "volatility_60d"
MAX_DRAWDOWN_20D = "max_drawdown_20d"
MAX_DRAWDOWN_60D = "max_drawdown_60d"
MAX_DRAWDOWN_120D = "max_drawdown_120d"
MAX_DRAWDOWN_250D = "max_drawdown_250d"
VOLUME_RATIO_20D = "volume_ratio_20d"
MA_DISTANCE_20D = "ma_distance_20d"
MA_DISTANCE_60D = "ma_distance_60d"
MA_DISTANCE_120D = "ma_distance_120d"
MA_DISTANCE_250D = "ma_distance_250d"
VALUATION_PE = "valuation_pe_ratio"
VALUATION_PB = "valuation_pb_ratio"
VALUATION_YIELD = "valuation_dividend_yield"

_PRICE_METRIC_ORDER = (
    RETURN_20D,
    RETURN_60D,
    RETURN_120D,
    VOLATILITY_60D,
    MAX_DRAWDOWN_20D,
    MAX_DRAWDOWN_60D,
    MAX_DRAWDOWN_120D,
    MAX_DRAWDOWN_250D,
    VOLUME_RATIO_20D,
    MA_DISTANCE_20D,
    MA_DISTANCE_60D,
    MA_DISTANCE_120D,
    MA_DISTANCE_250D,
)
_VALUATION_METRIC_ORDER = (VALUATION_PE, VALUATION_PB, VALUATION_YIELD)
_ALL_METRIC_ORDER = _PRICE_METRIC_ORDER + _VALUATION_METRIC_ORDER
_METRIC_INDEX = {name: index for index, name in enumerate(_ALL_METRIC_ORDER)}
_SYMBOL = re.compile(r"^[0-9A-Z]{2,12}$")

Scalar: TypeAlias = str | int | float | None


class Stage2ContractError(ValueError):
    """Stage 2 input, methodology, or output violates the S3 contract."""


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Stage2ContractError(f"{field_name} must not be blank")
    return value.strip()


def _optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


class Stage2MetricStatus(str, Enum):
    AVAILABLE = "available"
    INSUFFICIENT_HISTORY = "insufficient_history"
    MISSING_SOURCE = "missing_source"
    MARKET_DATE_MISMATCH = "market_date_mismatch"
    VALUATION_UNAVAILABLE = "valuation_unavailable"
    ANALYSIS_FAILED = "analysis_failed"


class Stage2AnalysisStatus(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


class Stage2QualityStatus(str, Enum):
    CLEAN = "clean"
    WARNING = "warning"
    BLOCKED = "blocked"
    FAILED = "failed"


class Stage2CandidateKind(str, Enum):
    RESEARCH_CANDIDATE = "research_candidate"
    DATA_QUALITY_CANDIDATE = "data_quality_candidate"


class Stage2ReasonKind(str, Enum):
    RESEARCH_CHANGE = "research_change"
    DATA_QUALITY = "data_quality"


class Stage2Operator(str, Enum):
    ABS_DELTA_GTE = "abs_delta_gte"
    ABS_CURRENT_GTE = "abs_current_gte"
    CURRENT_GTE = "current_gte"
    STAGE1_TRANSITION_CONFIRMED = "stage1_transition_confirmed"
    STATUS_EQUALS = "status_equals"


@dataclass(frozen=True, slots=True)
class Stage2Rule:
    code: str
    metric: str
    reason_class: str
    reason_kind: Stage2ReasonKind
    operator: Stage2Operator
    threshold: float | str
    unit: str
    class_priority: int
    reason_order: int

    def __post_init__(self) -> None:
        for field_name in ("code", "metric", "reason_class", "unit"):
            object.__setattr__(
                self,
                field_name,
                _require_text(getattr(self, field_name), field_name),
            )
        if not isinstance(self.reason_kind, Stage2ReasonKind):
            raise Stage2ContractError("reason_kind is invalid")
        if not isinstance(self.operator, Stage2Operator):
            raise Stage2ContractError("operator is invalid")
        if isinstance(self.threshold, bool) or not isinstance(
            self.threshold, (int, float, str)
        ):
            raise Stage2ContractError("threshold is invalid")
        if isinstance(self.threshold, (int, float)) and (
            not math.isfinite(float(self.threshold)) or float(self.threshold) <= 0
        ):
            raise Stage2ContractError("numeric threshold must be positive")
        if isinstance(self.threshold, str) and not self.threshold.strip():
            raise Stage2ContractError("text threshold must not be blank")
        for field_name in ("class_priority", "reason_order"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise Stage2ContractError(f"{field_name} must be positive")


@dataclass(frozen=True, slots=True)
class Stage2Methodology:
    version: str
    history_observations: int
    windows: tuple[int, ...]
    volatility_window: int
    metric_decimal_places: int
    analyzer_contract: str
    rules: tuple[Stage2Rule, ...]

    def __post_init__(self) -> None:
        if self.version != STAGE2_METHODOLOGY_VERSION:
            raise Stage2ContractError("unsupported Stage 2 methodology version")
        if self.history_observations != 250:
            raise Stage2ContractError("Stage 2 history_observations must equal 250")
        if self.windows != (20, 60, 120, 250):
            raise Stage2ContractError("Stage 2 windows must remain frozen")
        if self.volatility_window != 60:
            raise Stage2ContractError("Stage 2 volatility window must remain 60")
        if (
            isinstance(self.metric_decimal_places, bool)
            or not isinstance(self.metric_decimal_places, int)
            or self.metric_decimal_places < 1
        ):
            raise Stage2ContractError("metric_decimal_places must be positive")
        object.__setattr__(
            self,
            "analyzer_contract",
            _require_text(self.analyzer_contract, "analyzer_contract"),
        )
        if not isinstance(self.rules, tuple) or not self.rules:
            raise Stage2ContractError("rules must be an immutable non-empty tuple")
        codes = tuple(rule.code for rule in self.rules)
        orders = tuple(rule.reason_order for rule in self.rules)
        if len(set(codes)) != len(codes) or len(set(orders)) != len(orders):
            raise Stage2ContractError("rule codes and orders must be unique")
        if self.rules != tuple(sorted(self.rules, key=lambda item: item.reason_order)):
            raise Stage2ContractError("rules must be ordered by reason_order")

    def rule(self, code: str) -> Stage2Rule:
        for rule in self.rules:
            if rule.code == code:
                return rule
        raise Stage2ContractError(f"unknown Stage 2 rule {code}")


STAGE2_METHODOLOGY_V1 = Stage2Methodology(
    version=STAGE2_METHODOLOGY_VERSION,
    history_observations=250,
    windows=(20, 60, 120, 250),
    volatility_window=60,
    metric_decimal_places=12,
    analyzer_contract="HistoricalResearchAnalyzer:frozen-v1",
    rules=(
        Stage2Rule(
            code="source_discrepancy",
            metric="validation_state",
            reason_class="data_quality_transition",
            reason_kind=Stage2ReasonKind.DATA_QUALITY,
            operator=Stage2Operator.STATUS_EQUALS,
            threshold="source_discrepancy",
            unit="state",
            class_priority=10,
            reason_order=10,
        ),
        Stage2Rule(
            code="market_date_mismatch",
            metric="market_date_state",
            reason_class="data_quality_transition",
            reason_kind=Stage2ReasonKind.DATA_QUALITY,
            operator=Stage2Operator.STATUS_EQUALS,
            threshold="market_date_mismatch",
            unit="state",
            class_priority=10,
            reason_order=20,
        ),
        Stage2Rule(
            code="drawdown_change",
            metric=MAX_DRAWDOWN_120D,
            reason_class="risk_change",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.ABS_DELTA_GTE,
            threshold=1.0,
            unit="percentage_point",
            class_priority=20,
            reason_order=30,
        ),
        Stage2Rule(
            code="volatility_regime_change",
            metric=VOLATILITY_60D,
            reason_class="risk_change",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.ABS_DELTA_GTE,
            threshold=1.0,
            unit="percentage_point",
            class_priority=20,
            reason_order=40,
        ),
        Stage2Rule(
            code="return_60d_change",
            metric=RETURN_60D,
            reason_class="return_change",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.ABS_DELTA_GTE,
            threshold=1.0,
            unit="percentage_point",
            class_priority=30,
            reason_order=50,
        ),
        Stage2Rule(
            code="return_120d_state",
            metric=RETURN_120D,
            reason_class="return_state",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.ABS_CURRENT_GTE,
            threshold=10.0,
            unit="percent",
            class_priority=40,
            reason_order=60,
        ),
        Stage2Rule(
            code="ma_distance_change",
            metric=MA_DISTANCE_60D,
            reason_class="trend_change",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.ABS_DELTA_GTE,
            threshold=1.0,
            unit="percentage_point",
            class_priority=50,
            reason_order=70,
        ),
        Stage2Rule(
            code="volume_anomaly",
            metric=VOLUME_RATIO_20D,
            reason_class="liquidity_change",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.CURRENT_GTE,
            threshold=2.0,
            unit="ratio",
            class_priority=60,
            reason_order=80,
        ),
        Stage2Rule(
            code="valuation_update_pe",
            metric=VALUATION_PE,
            reason_class="valuation_change",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.STAGE1_TRANSITION_CONFIRMED,
            threshold="stage1_transition",
            unit="stage1_unit",
            class_priority=70,
            reason_order=90,
        ),
        Stage2Rule(
            code="valuation_update_pb",
            metric=VALUATION_PB,
            reason_class="valuation_change",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.STAGE1_TRANSITION_CONFIRMED,
            threshold="stage1_transition",
            unit="stage1_unit",
            class_priority=70,
            reason_order=100,
        ),
        Stage2Rule(
            code="valuation_update_yield",
            metric=VALUATION_YIELD,
            reason_class="valuation_change",
            reason_kind=Stage2ReasonKind.RESEARCH_CHANGE,
            operator=Stage2Operator.STAGE1_TRANSITION_CONFIRMED,
            threshold="stage1_transition",
            unit="stage1_unit",
            class_priority=70,
            reason_order=110,
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class Stage2Metric:
    name: str
    status: Stage2MetricStatus
    value: float | None
    previous_value: float | None
    delta: float | None
    unit: str
    as_of_date: date
    previous_as_of_date: date | None
    observations: int
    previous_observations: int

    def __post_init__(self) -> None:
        if self.name not in _METRIC_INDEX:
            raise Stage2ContractError("metric name is unsupported")
        if not isinstance(self.status, Stage2MetricStatus):
            raise Stage2ContractError("metric status is invalid")
        for field_name in ("value", "previous_value", "delta"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _finite(value, field_name))
        if self.status is not Stage2MetricStatus.AVAILABLE and any(
            getattr(self, field_name) is not None
            for field_name in ("value", "previous_value", "delta")
        ):
            raise Stage2ContractError("unavailable metric values must remain null")
        object.__setattr__(self, "unit", _require_text(self.unit, "unit"))
        object.__setattr__(
            self,
            "as_of_date",
            _require_date(self.as_of_date, "as_of_date"),
        )
        if self.previous_as_of_date is not None:
            object.__setattr__(
                self,
                "previous_as_of_date",
                _require_date(self.previous_as_of_date, "previous_as_of_date"),
            )
        for field_name in ("observations", "previous_observations"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise Stage2ContractError(f"{field_name} must be non-negative")


@dataclass(frozen=True, slots=True)
class Stage2Reason:
    code: str
    metric: str
    previous: Scalar
    current: Scalar
    delta: float | None
    unit: str
    operator: str
    threshold: float | str
    rule_version: str
    reason_kind: str
    reason_class: str
    threshold_multiple: float

    def __post_init__(self) -> None:
        for field_name in (
            "code",
            "metric",
            "unit",
            "operator",
            "rule_version",
            "reason_kind",
            "reason_class",
        ):
            object.__setattr__(
                self,
                field_name,
                _require_text(getattr(self, field_name), field_name),
            )
        for field_name in ("previous", "current"):
            value = getattr(self, field_name)
            if isinstance(value, float) and not math.isfinite(value):
                raise Stage2ContractError(f"{field_name} must be finite")
        if self.delta is not None:
            object.__setattr__(self, "delta", _finite(self.delta, "delta"))
        object.__setattr__(
            self,
            "threshold_multiple",
            _finite(self.threshold_multiple, "threshold_multiple"),
        )
        if self.threshold_multiple < 0:
            raise Stage2ContractError("threshold_multiple must be non-negative")


@dataclass(frozen=True, slots=True)
class Stage2Discrepancy:
    field: str
    left_value: Scalar
    right_value: Scalar
    reason: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "field", _require_text(self.field, "field"))
        object.__setattr__(self, "reason", _require_text(self.reason, "reason"))


@dataclass(frozen=True, slots=True)
class Stage2DataQuality:
    status: Stage2QualityStatus
    validation_status: str
    discrepancies: tuple[Stage2Discrepancy, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.status, Stage2QualityStatus):
            raise Stage2ContractError("quality status is invalid")
        if self.validation_status not in {
            "available",
            "missing_source",
            "market_date_mismatch",
            "source_discrepancy",
        }:
            raise Stage2ContractError("validation_status is unsupported")
        if not isinstance(self.discrepancies, tuple):
            raise Stage2ContractError("discrepancies must be an immutable tuple")
        if any(not isinstance(item, Stage2Discrepancy) for item in self.discrepancies):
            raise Stage2ContractError("invalid discrepancy")


@dataclass(frozen=True, slots=True)
class Stage2ArtifactRef:
    owner_kind: str
    owner_run_id: str
    provider: str
    dataset: str
    endpoint: str
    contract_version: str
    payload_sha256: str
    payload_size_bytes: int
    hash_basis: str


@dataclass(frozen=True, slots=True)
class Stage2Provenance:
    pipeline_run_id: str | None
    historical_run_id: str | None
    validation_run_id: str | None
    canonical_sources: tuple[str, ...]
    validation_sources: tuple[str, ...]
    artifact_refs: tuple[Stage2ArtifactRef, ...]
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
        for field_name in (
            "pipeline_run_id",
            "historical_run_id",
            "validation_run_id",
        ):
            object.__setattr__(
                self,
                field_name,
                _optional_text(getattr(self, field_name), field_name),
            )
        for field_name in ("canonical_sources", "validation_sources"):
            value = getattr(self, field_name)
            if not isinstance(value, tuple):
                raise Stage2ContractError(f"{field_name} must be immutable")
            object.__setattr__(self, field_name, tuple(sorted(set(value))))
        if self.source_policy not in {TWSE_BASELINE_SOURCE_POLICY, "twse_dual_source_v1"}:
            raise Stage2ContractError("source_policy is unsupported")
        if self.dataset_version_id is not None and _SHA256_RE.fullmatch(self.dataset_version_id) is None:
            raise Stage2ContractError("dataset_version_id must be lowercase SHA-256")
        if self.dataset_version_id is not None and self.source_policy != "twse_dual_source_v1":
            raise Stage2ContractError("dataset_version_id requires the dual-source policy")
        if self.source_policy == "twse_dual_source_v1" and self.dataset_version_id is None:
            raise Stage2ContractError("dual-source provenance requires dataset_version_id")
        if self.source_status not in {"canonical_complete", "provisional_mixed", "reconciled"}:
            raise Stage2ContractError("source_status is unsupported")
        if self.authority_status not in {"complete", "incomplete", "reconciled"}:
            raise Stage2ContractError("authority_status is unsupported")
        if self.reconciliation_status not in {
            "not_applicable", "pending", "reconciled_equal", "reconciled_discrepant"
        }:
            raise Stage2ContractError("reconciliation_status is unsupported")
        if self.research_data_quality not in {"canonical", "provisional", "reconciled"}:
            raise Stage2ContractError("research_data_quality is unsupported")
        if self.canonical_authority != "twse":
            raise Stage2ContractError("canonical authority must remain twse")
        for field_name in (
            "twse_observation_count", "esun_supplemental_count",
            "missing_twse_count", "discrepancy_count",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise Stage2ContractError(f"{field_name} must be non-negative")
        if self.provenance_map_sha256 is not None and _SHA256_RE.fullmatch(self.provenance_map_sha256) is None:
            raise Stage2ContractError("provenance_map_sha256 must be lowercase SHA-256")
        if self.parent_dataset_version_id is not None and _SHA256_RE.fullmatch(self.parent_dataset_version_id) is None:
            raise Stage2ContractError("parent_dataset_version_id must be lowercase SHA-256")
        object.__setattr__(
            self,
            "supplemental_sources",
            tuple(sorted(set(self.supplemental_sources))),
        )
        if not isinstance(self.artifact_refs, tuple):
            raise Stage2ContractError("artifact_refs must be immutable")


@dataclass(frozen=True, slots=True)
class Stage2Failure:
    status: str
    code: str
    error_type: str

    def __post_init__(self) -> None:
        if self.status != "failed":
            raise Stage2ContractError("failure status must be failed")
        object.__setattr__(self, "code", _require_text(self.code, "failure code"))
        object.__setattr__(
            self,
            "error_type",
            _require_text(self.error_type, "error_type"),
        )


@dataclass(frozen=True, slots=True)
class Stage2Candidate:
    rank: int
    stage1_rank: int
    symbol: str
    name: str | None
    market: str
    candidate_kind: Stage2CandidateKind
    analysis_status: Stage2AnalysisStatus
    stage1_reasons: tuple[Stage1Reason, ...]
    stage2_reasons: tuple[Stage2Reason, ...]
    metrics: tuple[Stage2Metric, ...]
    data_quality: Stage2DataQuality
    provenance: Stage2Provenance
    failure: Stage2Failure | None = None

    def __post_init__(self) -> None:
        for field_name in ("rank", "stage1_rank"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise Stage2ContractError(f"{field_name} must be positive")
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _optional_text(self.name, "name"))
        if self.market != "TWSE":
            raise Stage2ContractError("candidate market must be TWSE")
        if not isinstance(self.candidate_kind, Stage2CandidateKind):
            raise Stage2ContractError("candidate_kind is invalid")
        if not isinstance(self.analysis_status, Stage2AnalysisStatus):
            raise Stage2ContractError("analysis_status is invalid")
        if not isinstance(self.stage1_reasons, tuple) or not self.stage1_reasons:
            raise Stage2ContractError("Stage 1 reasons must be preserved")
        if not isinstance(self.stage2_reasons, tuple):
            raise Stage2ContractError("Stage 2 reasons must be immutable")
        if not isinstance(self.metrics, tuple) or len(self.metrics) != len(
            _ALL_METRIC_ORDER
        ):
            raise Stage2ContractError("Stage 2 must emit every frozen metric")
        if tuple(item.name for item in self.metrics) != _ALL_METRIC_ORDER:
            raise Stage2ContractError("Stage 2 metric ordering is not canonical")
        if not isinstance(self.data_quality, Stage2DataQuality):
            raise Stage2ContractError("data_quality is invalid")
        if not isinstance(self.provenance, Stage2Provenance):
            raise Stage2ContractError("provenance is invalid")
        if self.analysis_status is Stage2AnalysisStatus.FAILED:
            if self.failure is None:
                raise Stage2ContractError("failed analysis requires failure metadata")
        elif self.failure is not None:
            raise Stage2ContractError("non-failed analysis cannot expose failure")


@dataclass(frozen=True, slots=True)
class Stage2CandidateResult:
    market_date: date
    methodology_version: str
    stage1_methodology_version: str
    source_policy: str
    candidate_count: int
    candidates: tuple[Stage2Candidate, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "market_date",
            _require_date(self.market_date, "market_date"),
        )
        if self.methodology_version != STAGE2_METHODOLOGY_VERSION:
            raise Stage2ContractError("unsupported result methodology")
        if self.stage1_methodology_version != STAGE1_METHODOLOGY_VERSION:
            raise Stage2ContractError("Stage 1 methodology version changed")
        if self.source_policy != TWSE_BASELINE_SOURCE_POLICY:
            raise Stage2ContractError("source_policy must be twse_baseline")
        if not isinstance(self.candidates, tuple):
            raise Stage2ContractError("candidates must be immutable")
        if self.candidate_count != len(self.candidates):
            raise Stage2ContractError("candidate_count is inconsistent")
        if tuple(item.rank for item in self.candidates) != tuple(
            range(1, self.candidate_count + 1)
        ):
            raise Stage2ContractError("rank must be contiguous research priority")
        symbols = tuple(item.symbol for item in self.candidates)
        if len(set(symbols)) != len(symbols):
            raise Stage2ContractError("candidate symbols must be unique")

    def as_dict(self) -> dict[str, object]:
        return {
            "market_date": self.market_date.isoformat(),
            "methodology_version": self.methodology_version,
            "stage1_methodology_version": self.stage1_methodology_version,
            "source_policy": self.source_policy,
            "candidate_count": self.candidate_count,
            "candidates": [_candidate_dict(item) for item in self.candidates],
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def research_stage2_candidate(
    *,
    stage1_candidate: Stage1Candidate,
    dataset_snapshot: ResearchDatasetSnapshot,
    market_date: date,
    methodology: Stage2Methodology = STAGE2_METHODOLOGY_V1,
) -> Stage2Candidate:
    """Research one shortlisted candidate using one immutable M9 snapshot."""

    if not isinstance(stage1_candidate, Stage1Candidate):
        raise Stage2ContractError("stage1_candidate is invalid")
    if not isinstance(dataset_snapshot, ResearchDatasetSnapshot):
        raise Stage2ContractError("dataset_snapshot is invalid")
    frozen_date = _require_date(market_date, "market_date")
    if methodology is not STAGE2_METHODOLOGY_V1:
        raise Stage2ContractError("Stage 2 requires frozen methodology v1")
    if dataset_snapshot.symbol.symbol != stage1_candidate.symbol:
        raise Stage2ContractError("Stage 1 and M9 symbols differ")
    if dataset_snapshot.as_of.as_of_date != frozen_date:
        raise Stage2ContractError("M9 as_of_date differs from frozen market_date")
    if dataset_snapshot.as_of.history_observations != methodology.history_observations:
        raise Stage2ContractError("M9 history request must equal 250")
    if len(dataset_snapshot.price_history.observations) > methodology.history_observations:
        raise Stage2ContractError("M9 snapshot exceeds 250 observations")

    metrics, analysis_status = _deep_metrics(
        stage1_candidate,
        dataset_snapshot,
        frozen_date,
        methodology,
    )
    data_quality = _data_quality(dataset_snapshot)
    reasons = _stage2_reasons(
        stage1_candidate,
        dataset_snapshot,
        metrics,
        methodology,
    )
    candidate_kind = (
        Stage2CandidateKind.DATA_QUALITY_CANDIDATE
        if data_quality.status in {
            Stage2QualityStatus.BLOCKED,
            Stage2QualityStatus.FAILED,
        }
        or any(
            item.reason_kind == Stage2ReasonKind.DATA_QUALITY.value
            for item in reasons
        )
        else Stage2CandidateKind.RESEARCH_CANDIDATE
    )
    return Stage2Candidate(
        rank=stage1_candidate.rank,
        stage1_rank=stage1_candidate.rank,
        symbol=stage1_candidate.symbol,
        name=stage1_candidate.name,
        market="TWSE",
        candidate_kind=candidate_kind,
        analysis_status=analysis_status,
        stage1_reasons=stage1_candidate.reasons,
        stage2_reasons=reasons,
        metrics=metrics,
        data_quality=data_quality,
        provenance=_provenance(dataset_snapshot),
    )


def failed_stage2_candidate(
    *,
    stage1_candidate: Stage1Candidate,
    error: Exception,
    market_date: date,
) -> Stage2Candidate:
    """Create an explicit isolated failure without leaking exception text."""

    frozen_date = _require_date(market_date, "market_date")
    metrics = tuple(
        _unavailable_metric(
            name=name,
            status=Stage2MetricStatus.ANALYSIS_FAILED,
            market_date=frozen_date,
            observations=0,
        )
        for name in _ALL_METRIC_ORDER
    )
    return Stage2Candidate(
        rank=stage1_candidate.rank,
        stage1_rank=stage1_candidate.rank,
        symbol=stage1_candidate.symbol,
        name=stage1_candidate.name,
        market="TWSE",
        candidate_kind=Stage2CandidateKind.DATA_QUALITY_CANDIDATE,
        analysis_status=Stage2AnalysisStatus.FAILED,
        stage1_reasons=stage1_candidate.reasons,
        stage2_reasons=(),
        metrics=metrics,
        data_quality=Stage2DataQuality(
            status=Stage2QualityStatus.FAILED,
            validation_status="missing_source",
            discrepancies=(),
        ),
        provenance=_empty_provenance(),
        failure=Stage2Failure(
            status="failed",
            code="candidate_research_failed",
            error_type=type(error).__name__,
        ),
    )


def finalize_stage2_result(
    *,
    candidates: tuple[Stage2Candidate, ...],
    market_date: date,
    methodology: Stage2Methodology = STAGE2_METHODOLOGY_V1,
) -> Stage2CandidateResult:
    """Refine research priority without changing candidate membership."""

    if methodology is not STAGE2_METHODOLOGY_V1:
        raise Stage2ContractError("Stage 2 requires frozen methodology v1")
    if not isinstance(candidates, tuple):
        raise Stage2ContractError("candidates must be an immutable tuple")
    if any(not isinstance(item, Stage2Candidate) for item in candidates):
        raise Stage2ContractError("invalid Stage 2 candidate")
    symbols = tuple(item.symbol for item in candidates)
    if len(set(symbols)) != len(symbols):
        raise Stage2ContractError("candidate symbols must be unique")
    ordered = tuple(
        sorted(candidates, key=lambda item: _priority_key(item, methodology))
    )
    ranked = tuple(replace(item, rank=index) for index, item in enumerate(ordered, 1))
    return Stage2CandidateResult(
        market_date=market_date,
        methodology_version=methodology.version,
        stage1_methodology_version=STAGE1_METHODOLOGY_VERSION,
        source_policy=TWSE_BASELINE_SOURCE_POLICY,
        candidate_count=len(ranked),
        candidates=ranked,
    )


def _deep_metrics(
    stage1_candidate: Stage1Candidate,
    snapshot: ResearchDatasetSnapshot,
    market_date: date,
    methodology: Stage2Methodology,
) -> tuple[tuple[Stage2Metric, ...], Stage2AnalysisStatus]:
    price_status = snapshot.price_history.status
    if price_status != "available":
        status = (
            Stage2MetricStatus.MARKET_DATE_MISMATCH
            if price_status == "market_date_mismatch"
            else Stage2MetricStatus.MISSING_SOURCE
        )
        price_metrics = tuple(
            _unavailable_metric(
                name=name,
                status=status,
                market_date=market_date,
                observations=len(snapshot.price_history.observations),
            )
            for name in _PRICE_METRIC_ORDER
        )
        analysis_status = Stage2AnalysisStatus.UNAVAILABLE
    else:
        prices = tuple(_daily_price(item) for item in snapshot.price_history.observations)
        if not prices or prices[-1].trade_date != market_date:
            raise Stage2ContractError("available M9 history lacks exact market date")
        analyzer = HistoricalResearchAnalyzer(
            windows=methodology.windows,
            volatility_window=methodology.volatility_window,
        )
        current = analyzer.analyze(prices)
        previous = analyzer.analyze(prices[:-1]) if len(prices) > 1 else None
        price_metrics = _analysis_metrics(
            current=current,
            previous=previous,
            market_date=market_date,
            previous_date=prices[-2].trade_date if len(prices) > 1 else None,
            methodology=methodology,
        )
        analysis_status = Stage2AnalysisStatus.AVAILABLE
    valuation_metrics = _valuation_metrics(
        stage1_candidate,
        snapshot,
        market_date,
        methodology,
    )
    return price_metrics + valuation_metrics, analysis_status


def _analysis_metrics(
    *,
    current: HistoricalResearchAnalysis,
    previous: HistoricalResearchAnalysis | None,
    market_date: date,
    previous_date: date | None,
    methodology: Stage2Methodology,
) -> tuple[Stage2Metric, ...]:
    metrics: list[Stage2Metric] = []
    for window, name in ((20, RETURN_20D), (60, RETURN_60D), (120, RETURN_120D)):
        current_value = current.window(window).return_pct
        previous_value = previous.window(window).return_pct if previous else None
        metrics.append(
            _numeric_metric(
                name=name,
                current_value=current_value,
                previous_value=previous_value,
                unit="percent",
                market_date=market_date,
                previous_date=previous_date,
                observations=min(current.observations, window + 1),
                previous_observations=(
                    min(previous.observations, window + 1) if previous else 0
                ),
                methodology=methodology,
            )
        )
    current_volatility = (
        current.daily_return_volatility_pct
        if current.volatility_observations >= methodology.volatility_window
        else None
    )
    previous_volatility = (
        previous.daily_return_volatility_pct
        if previous is not None
        and previous.volatility_observations >= methodology.volatility_window
        else None
    )
    metrics.append(
        _numeric_metric(
            name=VOLATILITY_60D,
            current_value=current_volatility,
            previous_value=previous_volatility,
            unit="daily_return_percent",
            market_date=market_date,
            previous_date=previous_date,
            observations=current.volatility_observations,
            previous_observations=(
                previous.volatility_observations if previous else 0
            ),
            methodology=methodology,
        )
    )
    for window, name in (
        (20, MAX_DRAWDOWN_20D),
        (60, MAX_DRAWDOWN_60D),
        (120, MAX_DRAWDOWN_120D),
        (250, MAX_DRAWDOWN_250D),
    ):
        metrics.append(
            _numeric_metric(
                name=name,
                current_value=current.window(window).max_drawdown_pct,
                previous_value=(
                    previous.window(window).max_drawdown_pct if previous else None
                ),
                unit="percent",
                market_date=market_date,
                previous_date=previous_date,
                observations=min(current.observations, window),
                previous_observations=(
                    min(previous.observations, window) if previous else 0
                ),
                methodology=methodology,
            )
        )
    metrics.append(
        _numeric_metric(
            name=VOLUME_RATIO_20D,
            current_value=current.volume_ratio_to_20d,
            previous_value=(previous.volume_ratio_to_20d if previous else None),
            unit="ratio",
            market_date=market_date,
            previous_date=previous_date,
            observations=min(current.observations, 20),
            previous_observations=(
                min(previous.observations, 20) if previous else 0
            ),
            methodology=methodology,
        )
    )
    for window, name in (
        (20, MA_DISTANCE_20D),
        (60, MA_DISTANCE_60D),
        (120, MA_DISTANCE_120D),
        (250, MA_DISTANCE_250D),
    ):
        metrics.append(
            _numeric_metric(
                name=name,
                current_value=current.window(window).distance_to_moving_average_pct,
                previous_value=(
                    previous.window(window).distance_to_moving_average_pct
                    if previous
                    else None
                ),
                unit="percent",
                market_date=market_date,
                previous_date=previous_date,
                observations=min(current.observations, window),
                previous_observations=(
                    min(previous.observations, window) if previous else 0
                ),
                methodology=methodology,
            )
        )
    return tuple(metrics)


def _valuation_metrics(
    stage1_candidate: Stage1Candidate,
    snapshot: ResearchDatasetSnapshot,
    market_date: date,
    methodology: Stage2Methodology,
) -> tuple[Stage2Metric, ...]:
    current_by_name = {item.name: item.value for item in snapshot.valuation.metrics}
    mapping = (
        (VALUATION_PE, "price_earnings_ratio", "pe_ratio", "ratio"),
        (VALUATION_PB, "price_to_book_ratio", "pb_ratio", "ratio"),
        (VALUATION_YIELD, "dividend_yield_pct", "dividend_yield", "percent"),
    )
    values: list[Stage2Metric] = []
    for metric_name, dataset_name, stage1_component, unit in mapping:
        current_value = current_by_name.get(dataset_name)
        stage1_metric = _stage1_valuation_metric(stage1_candidate, stage1_component)
        if current_value is None:
            values.append(
                _unavailable_metric(
                    name=metric_name,
                    status=Stage2MetricStatus.VALUATION_UNAVAILABLE,
                    market_date=market_date,
                    observations=0,
                )
            )
            continue
        previous_value = stage1_metric.previous if stage1_metric else None
        delta = stage1_metric.delta if stage1_metric else None
        values.append(
            Stage2Metric(
                name=metric_name,
                status=Stage2MetricStatus.AVAILABLE,
                value=_q(current_value, methodology),
                previous_value=(
                    _q(previous_value, methodology)
                    if previous_value is not None
                    else None
                ),
                delta=_q(delta, methodology) if delta is not None else None,
                unit=unit,
                as_of_date=market_date,
                previous_as_of_date=(
                    stage1_metric.previous_as_of_date if stage1_metric else None
                ),
                observations=1,
                previous_observations=(
                    1 if stage1_metric and stage1_metric.previous is not None else 0
                ),
            )
        )
    return tuple(values)


def _numeric_metric(
    *,
    name: str,
    current_value: float | None,
    previous_value: float | None,
    unit: str,
    market_date: date,
    previous_date: date | None,
    observations: int,
    previous_observations: int,
    methodology: Stage2Methodology,
) -> Stage2Metric:
    if current_value is None:
        return _unavailable_metric(
            name=name,
            status=Stage2MetricStatus.INSUFFICIENT_HISTORY,
            market_date=market_date,
            observations=observations,
            previous_date=previous_date,
            previous_observations=previous_observations,
        )
    current = _q(current_value, methodology)
    previous = (
        _q(previous_value, methodology) if previous_value is not None else None
    )
    delta = _q(current - previous, methodology) if previous is not None else None
    return Stage2Metric(
        name=name,
        status=Stage2MetricStatus.AVAILABLE,
        value=current,
        previous_value=previous,
        delta=delta,
        unit=unit,
        as_of_date=market_date,
        previous_as_of_date=previous_date,
        observations=observations,
        previous_observations=previous_observations,
    )


def _unavailable_metric(
    *,
    name: str,
    status: Stage2MetricStatus,
    market_date: date,
    observations: int,
    previous_date: date | None = None,
    previous_observations: int = 0,
) -> Stage2Metric:
    return Stage2Metric(
        name=name,
        status=status,
        value=None,
        previous_value=None,
        delta=None,
        unit=_metric_unit(name),
        as_of_date=market_date,
        previous_as_of_date=previous_date,
        observations=observations,
        previous_observations=previous_observations,
    )


def _stage2_reasons(
    stage1_candidate: Stage1Candidate,
    snapshot: ResearchDatasetSnapshot,
    metrics: tuple[Stage2Metric, ...],
    methodology: Stage2Methodology,
) -> tuple[Stage2Reason, ...]:
    by_name = {item.name: item for item in metrics}
    reasons: list[Stage2Reason] = []
    for rule in methodology.rules:
        if rule.code == "source_discrepancy":
            if snapshot.validation.status == "source_discrepancy":
                reasons.append(
                    _quality_reason(rule, "available", "source_discrepancy")
                )
            continue
        if rule.code == "market_date_mismatch":
            if "market_date_mismatch" in {
                snapshot.price_history.status,
                snapshot.validation.status,
            }:
                reasons.append(
                    _quality_reason(rule, "available", "market_date_mismatch")
                )
            continue
        if rule.operator is Stage2Operator.STAGE1_TRANSITION_CONFIRMED:
            reason = _valuation_confirmation_reason(
                rule,
                stage1_candidate,
                by_name[rule.metric],
                methodology,
            )
            if reason is not None:
                reasons.append(reason)
            continue
        metric = by_name[rule.metric]
        if metric.status is not Stage2MetricStatus.AVAILABLE:
            continue
        matched, multiple = _numeric_rule_match(rule, metric, methodology)
        if matched:
            reasons.append(
                Stage2Reason(
                    code=rule.code,
                    metric=rule.metric,
                    previous=metric.previous_value,
                    current=metric.value,
                    delta=metric.delta,
                    unit=rule.unit,
                    operator=rule.operator.value,
                    threshold=rule.threshold,
                    rule_version=methodology.version,
                    reason_kind=rule.reason_kind.value,
                    reason_class=rule.reason_class,
                    threshold_multiple=multiple,
                )
            )
    return tuple(reasons)


def _valuation_confirmation_reason(
    rule: Stage2Rule,
    candidate: Stage1Candidate,
    metric: Stage2Metric,
    methodology: Stage2Methodology,
) -> Stage2Reason | None:
    component = {
        VALUATION_PE: "pe_ratio",
        VALUATION_PB: "pb_ratio",
        VALUATION_YIELD: "dividend_yield",
    }[rule.metric]
    stage1_reasons = tuple(
        item
        for item in candidate.reasons
        if item.metric == VALUATION_UPDATE
        and item.component == component
        and item.role == RuleRole.PRIMARY.value
    )
    if not stage1_reasons or metric.status is not Stage2MetricStatus.AVAILABLE:
        return None
    source_reason = stage1_reasons[0]
    if source_reason.current is None or metric.value is None:
        return None
    if not math.isclose(float(source_reason.current), metric.value, rel_tol=0, abs_tol=1e-9):
        return None
    return Stage2Reason(
        code=rule.code,
        metric=rule.metric,
        previous=source_reason.previous,
        current=source_reason.current,
        delta=source_reason.delta,
        unit=source_reason.unit,
        operator=rule.operator.value,
        threshold=source_reason.threshold,
        rule_version=methodology.version,
        reason_kind=rule.reason_kind.value,
        reason_class=rule.reason_class,
        threshold_multiple=_q(source_reason.threshold_multiple, methodology),
    )


def _numeric_rule_match(
    rule: Stage2Rule,
    metric: Stage2Metric,
    methodology: Stage2Methodology,
) -> tuple[bool, float]:
    if not isinstance(rule.threshold, (int, float)):
        raise Stage2ContractError("numeric rule requires numeric threshold")
    threshold = float(rule.threshold)
    if rule.operator is Stage2Operator.ABS_DELTA_GTE:
        if metric.delta is None:
            return False, 0.0
        value = abs(metric.delta)
    elif rule.operator is Stage2Operator.ABS_CURRENT_GTE:
        if metric.value is None:
            return False, 0.0
        value = abs(metric.value)
    elif rule.operator is Stage2Operator.CURRENT_GTE:
        if metric.value is None:
            return False, 0.0
        value = metric.value
    else:
        raise Stage2ContractError("unsupported numeric rule operator")
    return value >= threshold, _q(max(value, 0.0) / threshold, methodology)


def _quality_reason(
    rule: Stage2Rule,
    previous: str,
    current: str,
) -> Stage2Reason:
    return Stage2Reason(
        code=rule.code,
        metric=rule.metric,
        previous=previous,
        current=current,
        delta=None,
        unit=rule.unit,
        operator=rule.operator.value,
        threshold=rule.threshold,
        rule_version=STAGE2_METHODOLOGY_VERSION,
        reason_kind=rule.reason_kind.value,
        reason_class=rule.reason_class,
        threshold_multiple=1.0,
    )


def _data_quality(snapshot: ResearchDatasetSnapshot) -> Stage2DataQuality:
    discrepancies = tuple(_discrepancy(item) for item in snapshot.validation.discrepancies)
    if snapshot.price_history.status in {"missing_source", "market_date_mismatch"}:
        status = Stage2QualityStatus.BLOCKED
    elif snapshot.validation.status in {"source_discrepancy", "market_date_mismatch"}:
        status = Stage2QualityStatus.WARNING
    elif snapshot.validation.status == "missing_source":
        status = Stage2QualityStatus.WARNING
    else:
        status = Stage2QualityStatus.CLEAN
    return Stage2DataQuality(
        status=status,
        validation_status=snapshot.validation.status,
        discrepancies=discrepancies,
    )


def _provenance(snapshot: ResearchDatasetSnapshot) -> Stage2Provenance:
    value = snapshot.provenance
    return Stage2Provenance(
        pipeline_run_id=value.pipeline_run_id,
        historical_run_id=value.historical_run_id,
        validation_run_id=value.validation_run_id,
        canonical_sources=value.canonical_sources,
        validation_sources=value.validation_sources,
        artifact_refs=tuple(_artifact(item) for item in value.artifact_refs),
        source_policy=value.source_policy,
        dataset_version_id=value.dataset_version_id,
        source_status=value.source_status,
        authority_status=value.authority_status,
        reconciliation_status=value.reconciliation_status,
        research_data_quality=value.research_data_quality,
        canonical_authority=value.canonical_authority,
        supplemental_sources=value.supplemental_sources,
        twse_observation_count=value.twse_observation_count,
        esun_supplemental_count=value.esun_supplemental_count,
        missing_twse_count=value.missing_twse_count,
        discrepancy_count=value.discrepancy_count,
        provenance_map_sha256=value.provenance_map_sha256,
        parent_dataset_version_id=value.parent_dataset_version_id,
    )


def _empty_provenance() -> Stage2Provenance:
    return Stage2Provenance(
        pipeline_run_id=None,
        historical_run_id=None,
        validation_run_id=None,
        canonical_sources=(),
        validation_sources=(),
        artifact_refs=(),
    )


def _artifact(value: DatasetArtifactRef) -> Stage2ArtifactRef:
    return Stage2ArtifactRef(
        owner_kind=value.owner_kind,
        owner_run_id=value.owner_run_id,
        provider=value.provider,
        dataset=value.dataset,
        endpoint=value.endpoint,
        contract_version=value.contract_version,
        payload_sha256=value.payload_sha256,
        payload_size_bytes=value.payload_size_bytes,
        hash_basis=value.hash_basis,
    )


def _discrepancy(value: DatasetDiscrepancy) -> Stage2Discrepancy:
    return Stage2Discrepancy(
        field=value.field,
        left_value=value.left_value,
        right_value=value.right_value,
        reason=value.reason,
    )


def _priority_key(
    candidate: Stage2Candidate,
    methodology: Stage2Methodology,
) -> tuple[int, int, int, int, float, int, str]:
    quality_priority = {
        Stage2QualityStatus.FAILED: 0,
        Stage2QualityStatus.BLOCKED: 1,
        Stage2QualityStatus.WARNING: (
            2
            if candidate.candidate_kind is Stage2CandidateKind.DATA_QUALITY_CANDIDATE
            else 4
        ),
        Stage2QualityStatus.CLEAN: 4,
    }[candidate.data_quality.status]
    research_reasons = tuple(
        item
        for item in candidate.stage2_reasons
        if item.reason_kind == Stage2ReasonKind.RESEARCH_CHANGE.value
    )
    stage1_trigger_count = sum(
        item.role == RuleRole.PRIMARY.value for item in candidate.stage1_reasons
    )
    rules = {item.code: item for item in methodology.rules}
    class_priority = min(
        (rules[item.code].class_priority for item in candidate.stage2_reasons),
        default=999,
    )
    multiple = max(
        (item.threshold_multiple for item in candidate.stage2_reasons),
        default=0.0,
    )
    return (
        quality_priority,
        -len(research_reasons),
        -stage1_trigger_count,
        class_priority,
        -multiple,
        candidate.stage1_rank,
        candidate.symbol,
    )


def _stage1_valuation_metric(
    candidate: Stage1Candidate,
    component: str,
) -> Stage1Metric | None:
    for metric in candidate.metrics:
        if metric.metric == VALUATION_UPDATE and metric.component == component:
            return metric
    return None


def _daily_price(value) -> DailyPrice:
    return DailyPrice(
        symbol=value.symbol,
        trade_date=value.trade_date,
        open=value.open,
        high=value.high,
        low=value.low,
        close=value.close,
        volume=value.volume,
        source=value.source,
    )


def _metric_unit(name: str) -> str:
    if name in {RETURN_20D, RETURN_60D, RETURN_120D}:
        return "percent"
    if name == VOLATILITY_60D:
        return "daily_return_percent"
    if name.startswith("max_drawdown_") or name.startswith("ma_distance_"):
        return "percent"
    if name == VOLUME_RATIO_20D:
        return "ratio"
    if name in {VALUATION_PE, VALUATION_PB}:
        return "ratio"
    if name == VALUATION_YIELD:
        return "percent"
    raise Stage2ContractError("unsupported metric unit")


def _candidate_dict(candidate: Stage2Candidate) -> dict[str, object]:
    return {
        "rank": candidate.rank,
        "stage1_rank": candidate.stage1_rank,
        "symbol": candidate.symbol,
        "name": candidate.name,
        "market": candidate.market,
        "candidate_kind": candidate.candidate_kind.value,
        "analysis_status": candidate.analysis_status.value,
        "stage1_reasons": [_stage1_reason_dict(item) for item in candidate.stage1_reasons],
        "stage2_reasons": [_reason_dict(item) for item in candidate.stage2_reasons],
        "metrics": [_metric_dict(item) for item in candidate.metrics],
        "data_quality": _quality_dict(candidate.data_quality),
        "provenance": _provenance_dict(candidate.provenance),
        "failure": _failure_dict(candidate.failure),
    }


def _stage1_reason_dict(reason: Stage1Reason) -> dict[str, object]:
    return {
        "code": reason.code,
        "metric": reason.metric,
        "component": reason.component,
        "previous": reason.previous,
        "current": reason.current,
        "delta": reason.delta,
        "unit": reason.unit,
        "operator": reason.operator,
        "threshold": reason.threshold,
        "rule_version": reason.rule_version,
        "role": reason.role,
        "trigger_class": reason.trigger_class,
        "threshold_multiple": reason.threshold_multiple,
    }


def _reason_dict(reason: Stage2Reason) -> dict[str, object]:
    return {
        "code": reason.code,
        "metric": reason.metric,
        "previous": reason.previous,
        "current": reason.current,
        "delta": reason.delta,
        "unit": reason.unit,
        "operator": reason.operator,
        "threshold": reason.threshold,
        "rule_version": reason.rule_version,
        "reason_kind": reason.reason_kind,
        "reason_class": reason.reason_class,
        "threshold_multiple": reason.threshold_multiple,
    }


def _metric_dict(metric: Stage2Metric) -> dict[str, object]:
    return {
        "name": metric.name,
        "status": metric.status.value,
        "value": metric.value,
        "previous_value": metric.previous_value,
        "delta": metric.delta,
        "unit": metric.unit,
        "as_of_date": metric.as_of_date.isoformat(),
        "previous_as_of_date": (
            metric.previous_as_of_date.isoformat()
            if metric.previous_as_of_date is not None
            else None
        ),
        "observations": metric.observations,
        "previous_observations": metric.previous_observations,
    }


def _quality_dict(value: Stage2DataQuality) -> dict[str, object]:
    return {
        "status": value.status.value,
        "validation_status": value.validation_status,
        "discrepancies": [
            {
                "field": item.field,
                "left_value": item.left_value,
                "right_value": item.right_value,
                "reason": item.reason,
            }
            for item in value.discrepancies
        ],
    }


def _provenance_dict(value: Stage2Provenance) -> dict[str, object]:
    result = {
        "pipeline_run_id": value.pipeline_run_id,
        "historical_run_id": value.historical_run_id,
        "validation_run_id": value.validation_run_id,
        "canonical_sources": list(value.canonical_sources),
        "validation_sources": list(value.validation_sources),
        "artifact_refs": [
            {
                "owner_kind": item.owner_kind,
                "owner_run_id": item.owner_run_id,
                "provider": item.provider,
                "dataset": item.dataset,
                "endpoint": item.endpoint,
                "contract_version": item.contract_version,
                "payload_sha256": item.payload_sha256,
                "payload_size_bytes": item.payload_size_bytes,
                "hash_basis": item.hash_basis,
            }
            for item in value.artifact_refs
        ],
    }
    if value.dataset_version_id is not None or value.research_data_quality != "canonical":
        result["dataset_version_id"] = value.dataset_version_id
        result["source_policy"] = value.source_policy
        result["source_status"] = value.source_status
        result["authority_status"] = value.authority_status
        result["reconciliation_status"] = value.reconciliation_status
        result["research_data_quality"] = value.research_data_quality
        result["canonical_authority"] = value.canonical_authority
        result["supplemental_sources"] = list(value.supplemental_sources)
        result["twse_observation_count"] = value.twse_observation_count
        result["esun_supplemental_count"] = value.esun_supplemental_count
        result["missing_twse_count"] = value.missing_twse_count
        result["discrepancy_count"] = value.discrepancy_count
        result["provenance_map_sha256"] = value.provenance_map_sha256
        result["parent_dataset_version_id"] = value.parent_dataset_version_id
    return result


def _failure_dict(value: Stage2Failure | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "status": value.status,
        "code": value.code,
        "error_type": value.error_type,
    }


def _q(value: float, methodology: Stage2Methodology) -> float:
    rounded = round(_finite(value, "metric value"), methodology.metric_decimal_places)
    return 0.0 if rounded == 0 else rounded


def _normalize_symbol(value: object) -> str:
    symbol = _require_text(value, "symbol").upper()
    if _SYMBOL.fullmatch(symbol) is None:
        raise Stage2ContractError("symbol is invalid")
    return symbol


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise Stage2ContractError(f"{field_name} must be a date")
    return value


def _finite(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Stage2ContractError(f"{field_name} must be finite")
    number = float(value)
    if not math.isfinite(number):
        raise Stage2ContractError(f"{field_name} must be finite")
    return number
