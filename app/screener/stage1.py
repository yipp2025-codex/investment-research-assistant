"""Pure deterministic Stage 1 lightweight screening contract for S2.

The engine accepts an already-frozen S1 universe and immutable screening
snapshots.  It performs no acquisition, calendar resolution, persistence, or
external I/O.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

from app.research_dataset import (
    MOCK_SYNTHETIC_SOURCE,
    TWSE_BASELINE_SOURCE_POLICY,
    TWSE_BASELINE_SOURCES,
)
from app.screener.universe import MarketUniverseSnapshot, UniverseMemberStatus


STAGE1_METHODOLOGY_VERSION = "screener-stage1-v1"

PRICE_CHANGE_1D = "price_change_1d"
VOLUME_ANOMALY = "volume_anomaly"
RETURN_20D_CHANGE = "return_20d_change"
VOLATILITY_REGIME_CHANGE = "volatility_regime_change"
MA_DISTANCE_CHANGE = "ma_distance_change"
VALUATION_UPDATE = "valuation_update"

PE_RATIO = "pe_ratio"
PB_RATIO = "pb_ratio"
DIVIDEND_YIELD = "dividend_yield"

_METRIC_ORDER = {
    PRICE_CHANGE_1D: 0,
    VOLUME_ANOMALY: 1,
    RETURN_20D_CHANGE: 2,
    VOLATILITY_REGIME_CHANGE: 3,
    MA_DISTANCE_CHANGE: 4,
    VALUATION_UPDATE: 5,
}
_COMPONENT_ORDER = {None: 0, PE_RATIO: 1, PB_RATIO: 2, DIVIDEND_YIELD: 3}
_SYMBOL = re.compile(r"^[0-9A-Z]{2,12}$")


class Stage1ContractError(ValueError):
    """Stage 1 input, methodology, or output violates the frozen S2 contract."""


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Stage1ContractError(f"{field_name} must not be blank")
    return value.strip()


def _normalize_optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


class MetricStatus(str, Enum):
    AVAILABLE = "available"
    INSUFFICIENT_HISTORY = "insufficient_history"
    MISSING_SOURCE = "missing_source"
    MARKET_DATE_MISMATCH = "market_date_mismatch"
    INVALID_DENOMINATOR = "invalid_denominator"
    VALUATION_UNAVAILABLE = "valuation_unavailable"


class PriceSnapshotStatus(str, Enum):
    AVAILABLE = "available"
    MISSING_SOURCE = "missing_source"
    MARKET_DATE_MISMATCH = "market_date_mismatch"


class ValuationSnapshotStatus(str, Enum):
    AVAILABLE = "available"
    MISSING_SOURCE = "missing_source"


class DataQualityStatus(str, Enum):
    CLEAN = "clean"
    WARNING = "warning"
    UNAVAILABLE = "unavailable"


class RuleRole(str, Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"


class RuleOperator(str, Enum):
    ABS_CURRENT_GTE = "abs_current_gte"
    CURRENT_GTE = "current_gte"
    CURRENT_LTE = "current_lte"
    ABS_DELTA_GTE = "abs_delta_gte"
    AVAILABILITY_CHANGED = "availability_changed"


@dataclass(frozen=True, slots=True)
class TriggerRule:
    code: str
    metric: str
    component: str | None
    trigger_class: str
    role: RuleRole
    operator: RuleOperator
    threshold: float | str
    unit: str
    class_priority: int
    reason_order: int

    def __post_init__(self) -> None:
        for field_name in ("code", "metric", "trigger_class", "unit"):
            object.__setattr__(
                self,
                field_name,
                _require_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "component",
            _normalize_optional_text(self.component, "component"),
        )
        if self.metric not in _METRIC_ORDER:
            raise Stage1ContractError("rule metric is unsupported")
        if self.metric == VALUATION_UPDATE:
            if self.component not in {PE_RATIO, PB_RATIO, DIVIDEND_YIELD}:
                raise Stage1ContractError("valuation rule requires a known component")
        elif self.component is not None:
            raise Stage1ContractError("non-valuation rule cannot have a component")
        if not isinstance(self.role, RuleRole):
            raise Stage1ContractError("rule role is invalid")
        if not isinstance(self.operator, RuleOperator):
            raise Stage1ContractError("rule operator is invalid")
        if isinstance(self.threshold, bool) or not isinstance(
            self.threshold, (int, float, str)
        ):
            raise Stage1ContractError("rule threshold is invalid")
        if isinstance(self.threshold, (int, float)) and (
            not math.isfinite(float(self.threshold)) or float(self.threshold) <= 0
        ):
            raise Stage1ContractError("numeric rule threshold must be positive")
        if isinstance(self.threshold, str) and not self.threshold.strip():
            raise Stage1ContractError("text rule threshold must not be blank")
        for field_name in ("class_priority", "reason_order"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise Stage1ContractError(f"{field_name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class Stage1Methodology:
    version: str
    history_observations: int
    volume_window: int
    return_window: int
    volatility_return_window: int
    ma_window: int
    volatility_annualization_days: int
    volatility_sample_ddof: int
    metric_decimal_places: int
    rules: tuple[TriggerRule, ...]

    def __post_init__(self) -> None:
        if self.version != STAGE1_METHODOLOGY_VERSION:
            raise Stage1ContractError("unsupported Stage 1 methodology version")
        for field_name in (
            "history_observations",
            "volume_window",
            "return_window",
            "volatility_return_window",
            "ma_window",
            "volatility_annualization_days",
            "metric_decimal_places",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise Stage1ContractError(f"{field_name} must be a positive integer")
        if self.volatility_sample_ddof != 1:
            raise Stage1ContractError("Stage 1 v1 volatility must use sample ddof=1")
        if not isinstance(self.rules, tuple) or not self.rules:
            raise Stage1ContractError("methodology rules must be a non-empty tuple")
        if any(not isinstance(rule, TriggerRule) for rule in self.rules):
            raise Stage1ContractError("methodology contains an invalid rule")
        codes = tuple(rule.code for rule in self.rules)
        orders = tuple(rule.reason_order for rule in self.rules)
        if len(set(codes)) != len(codes) or len(set(orders)) != len(orders):
            raise Stage1ContractError("rule codes and reason_order must be unique")
        if self.rules != tuple(sorted(self.rules, key=lambda item: item.reason_order)):
            raise Stage1ContractError("rules must be ordered by reason_order")

        required = max(
            3,
            self.volume_window + 2,
            self.return_window + 2,
            self.volatility_return_window + 2,
            self.ma_window + 1,
        )
        if self.history_observations != required:
            raise Stage1ContractError(
                "history_observations must equal the maximum v1 current/previous "
                f"lookback requirement ({required})"
            )

    def rule(self, code: str) -> TriggerRule:
        for rule in self.rules:
            if rule.code == code:
                return rule
        raise Stage1ContractError(f"unknown Stage 1 rule: {code}")


STAGE1_METHODOLOGY_V1 = Stage1Methodology(
    version=STAGE1_METHODOLOGY_VERSION,
    history_observations=62,
    volume_window=20,
    return_window=20,
    volatility_return_window=60,
    ma_window=20,
    volatility_annualization_days=252,
    volatility_sample_ddof=1,
    metric_decimal_places=12,
    rules=(
        TriggerRule(
            code="price_change_1d_threshold",
            metric=PRICE_CHANGE_1D,
            component=None,
            trigger_class="market_move",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.ABS_CURRENT_GTE,
            threshold=3.0,
            unit="percent",
            class_priority=10,
            reason_order=10,
        ),
        TriggerRule(
            code="volume_anomaly_high",
            metric=VOLUME_ANOMALY,
            component=None,
            trigger_class="liquidity_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.CURRENT_GTE,
            threshold=2.0,
            unit="ratio",
            class_priority=20,
            reason_order=20,
        ),
        TriggerRule(
            code="volume_anomaly_low_context",
            metric=VOLUME_ANOMALY,
            component=None,
            trigger_class="liquidity_context",
            role=RuleRole.SECONDARY,
            operator=RuleOperator.CURRENT_LTE,
            threshold=0.5,
            unit="ratio",
            class_priority=90,
            reason_order=30,
        ),
        TriggerRule(
            code="return_20d_delta_threshold",
            metric=RETURN_20D_CHANGE,
            component=None,
            trigger_class="trend_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.ABS_DELTA_GTE,
            threshold=1.0,
            unit="percentage_point",
            class_priority=30,
            reason_order=40,
        ),
        TriggerRule(
            code="volatility_60d_delta_threshold",
            metric=VOLATILITY_REGIME_CHANGE,
            component=None,
            trigger_class="risk_regime_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.ABS_DELTA_GTE,
            threshold=1.0,
            unit="percentage_point",
            class_priority=40,
            reason_order=50,
        ),
        TriggerRule(
            code="ma20_distance_delta_threshold",
            metric=MA_DISTANCE_CHANGE,
            component=None,
            trigger_class="trend_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.ABS_DELTA_GTE,
            threshold=1.0,
            unit="percentage_point",
            class_priority=30,
            reason_order=60,
        ),
        TriggerRule(
            code="valuation_pe_availability_transition",
            metric=VALUATION_UPDATE,
            component=PE_RATIO,
            trigger_class="valuation_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.AVAILABILITY_CHANGED,
            threshold="availability_transition",
            unit="availability",
            class_priority=50,
            reason_order=70,
        ),
        TriggerRule(
            code="valuation_pe_relative_change",
            metric=VALUATION_UPDATE,
            component=PE_RATIO,
            trigger_class="valuation_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.ABS_DELTA_GTE,
            threshold=10.0,
            unit="percent_relative_change",
            class_priority=50,
            reason_order=80,
        ),
        TriggerRule(
            code="valuation_pb_availability_transition",
            metric=VALUATION_UPDATE,
            component=PB_RATIO,
            trigger_class="valuation_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.AVAILABILITY_CHANGED,
            threshold="availability_transition",
            unit="availability",
            class_priority=50,
            reason_order=90,
        ),
        TriggerRule(
            code="valuation_pb_relative_change",
            metric=VALUATION_UPDATE,
            component=PB_RATIO,
            trigger_class="valuation_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.ABS_DELTA_GTE,
            threshold=10.0,
            unit="percent_relative_change",
            class_priority=50,
            reason_order=100,
        ),
        TriggerRule(
            code="valuation_yield_availability_transition",
            metric=VALUATION_UPDATE,
            component=DIVIDEND_YIELD,
            trigger_class="valuation_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.AVAILABILITY_CHANGED,
            threshold="availability_transition",
            unit="availability",
            class_priority=50,
            reason_order=110,
        ),
        TriggerRule(
            code="valuation_yield_delta_threshold",
            metric=VALUATION_UPDATE,
            component=DIVIDEND_YIELD,
            trigger_class="valuation_change",
            role=RuleRole.PRIMARY,
            operator=RuleOperator.ABS_DELTA_GTE,
            threshold=0.5,
            unit="percentage_point",
            class_priority=50,
            reason_order=120,
        ),
    ),
)


@dataclass(frozen=True, slots=True)
class Stage1PriceObservation:
    trade_date: date
    close: float
    volume: int
    source: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "trade_date",
            _require_date(self.trade_date, "trade_date"),
        )
        object.__setattr__(self, "close", _finite_number(self.close, "close"))
        if isinstance(self.volume, bool) or not isinstance(self.volume, int) or self.volume < 0:
            raise Stage1ContractError("volume must be a non-negative integer")
        source = _require_text(self.source, "source").casefold()
        if source not in TWSE_BASELINE_SOURCES | {MOCK_SYNTHETIC_SOURCE}:
            raise Stage1ContractError("price source is outside the canonical family")
        object.__setattr__(self, "source", source)


@dataclass(frozen=True, slots=True)
class Stage1ValuationState:
    as_of_date: date
    status: ValuationSnapshotStatus
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    dividend_yield_pct: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "as_of_date",
            _require_date(self.as_of_date, "valuation as_of_date"),
        )
        if not isinstance(self.status, ValuationSnapshotStatus):
            raise Stage1ContractError("valuation status is invalid")
        for field_name in ("pe_ratio", "pb_ratio", "dividend_yield_pct"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _finite_number(value, field_name))
        if self.status is ValuationSnapshotStatus.MISSING_SOURCE and any(
            getattr(self, field_name) is not None
            for field_name in ("pe_ratio", "pb_ratio", "dividend_yield_pct")
        ):
            raise Stage1ContractError(
                "missing valuation source cannot expose component values"
            )


@dataclass(frozen=True, slots=True)
class Stage1ResearchSnapshot:
    symbol: str
    as_of_date: date
    price_status: PriceSnapshotStatus
    prices: tuple[Stage1PriceObservation, ...]
    current_valuation: Stage1ValuationState
    previous_valuation: Stage1ValuationState | None
    canonical_sources: tuple[str, ...]
    source_policy: str = field(default=TWSE_BASELINE_SOURCE_POLICY, init=False)

    def __post_init__(self) -> None:
        symbol = _normalize_symbol(self.symbol)
        as_of_date = _require_date(self.as_of_date, "as_of_date")
        if not isinstance(self.price_status, PriceSnapshotStatus):
            raise Stage1ContractError("price_status is invalid")
        if not isinstance(self.prices, tuple):
            raise Stage1ContractError("prices must be an immutable tuple")
        if any(not isinstance(item, Stage1PriceObservation) for item in self.prices):
            raise Stage1ContractError("prices contain an invalid observation")
        dates = tuple(item.trade_date for item in self.prices)
        if dates != tuple(sorted(dates)) or len(set(dates)) != len(dates):
            raise Stage1ContractError("prices must be uniquely ordered by trade_date")
        if any(item.trade_date > as_of_date for item in self.prices):
            raise Stage1ContractError("prices must not contain future observations")
        latest = self.prices[-1] if self.prices else None
        if self.price_status is PriceSnapshotStatus.AVAILABLE:
            if latest is None or latest.trade_date != as_of_date:
                raise Stage1ContractError(
                    "available price snapshot requires exact as_of observation"
                )
        elif self.price_status is PriceSnapshotStatus.MISSING_SOURCE:
            if self.prices:
                raise Stage1ContractError("missing price source cannot expose history")
        elif latest is None or latest.trade_date >= as_of_date:
            raise Stage1ContractError(
                "market_date_mismatch requires only earlier observations"
            )
        if not isinstance(self.current_valuation, Stage1ValuationState):
            raise Stage1ContractError("current_valuation is invalid")
        if self.current_valuation.as_of_date != as_of_date:
            raise Stage1ContractError("current valuation must use snapshot as_of_date")
        prior_market_date = self.prices[-2].trade_date if len(self.prices) >= 2 else None
        if self.previous_valuation is not None:
            if not isinstance(self.previous_valuation, Stage1ValuationState):
                raise Stage1ContractError("previous_valuation is invalid")
            if prior_market_date is None or (
                self.previous_valuation.as_of_date != prior_market_date
            ):
                raise Stage1ContractError(
                    "previous valuation must use nearest prior market observation"
                )
        if not isinstance(self.canonical_sources, tuple) or not self.canonical_sources:
            raise Stage1ContractError("canonical_sources must be a non-empty tuple")
        sources = tuple(sorted({_require_text(item, "canonical source").casefold() for item in self.canonical_sources}))
        allowed = TWSE_BASELINE_SOURCES | {MOCK_SYNTHETIC_SOURCE}
        if set(sources) - allowed:
            raise Stage1ContractError("canonical source is outside TWSE baseline")
        if MOCK_SYNTHETIC_SOURCE in sources and len(sources) != 1:
            raise Stage1ContractError("synthetic and formal canonical sources cannot mix")
        if any(item.source not in sources for item in self.prices):
            raise Stage1ContractError("price observation source lacks canonical evidence")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "as_of_date", as_of_date)
        object.__setattr__(self, "canonical_sources", sources)


@dataclass(frozen=True, slots=True)
class Stage1Metric:
    metric: str
    component: str | None
    status: MetricStatus
    previous: float | None
    current: float | None
    delta: float | None
    value_unit: str
    delta_unit: str
    previous_as_of_date: date | None
    current_as_of_date: date

    def __post_init__(self) -> None:
        if self.metric not in _METRIC_ORDER:
            raise Stage1ContractError("metric name is unsupported")
        component = _normalize_optional_text(self.component, "component")
        if self.metric == VALUATION_UPDATE:
            if component not in {PE_RATIO, PB_RATIO, DIVIDEND_YIELD}:
                raise Stage1ContractError("valuation metric component is unsupported")
        elif component is not None:
            raise Stage1ContractError("non-valuation metric cannot have a component")
        if not isinstance(self.status, MetricStatus):
            raise Stage1ContractError("metric status is invalid")
        for field_name in ("previous", "current", "delta"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _finite_number(value, field_name))
        if self.status is not MetricStatus.AVAILABLE and any(
            getattr(self, field_name) is not None
            for field_name in ("previous", "current", "delta")
        ):
            raise Stage1ContractError(
                "unavailable metric values must remain null rather than sentinel numbers"
            )
        for field_name in ("value_unit", "delta_unit"):
            object.__setattr__(
                self,
                field_name,
                _require_text(getattr(self, field_name), field_name),
            )
        if self.previous_as_of_date is not None:
            object.__setattr__(
                self,
                "previous_as_of_date",
                _require_date(self.previous_as_of_date, "previous_as_of_date"),
            )
        object.__setattr__(
            self,
            "current_as_of_date",
            _require_date(self.current_as_of_date, "current_as_of_date"),
        )
        object.__setattr__(self, "component", component)


@dataclass(frozen=True, slots=True)
class Stage1Reason:
    code: str
    metric: str
    component: str | None
    previous: float | None
    current: float | None
    delta: float | None
    unit: str
    operator: str
    threshold: float | str
    rule_version: str
    role: str
    trigger_class: str
    threshold_multiple: float

    def __post_init__(self) -> None:
        for field_name in (
            "code",
            "metric",
            "unit",
            "operator",
            "rule_version",
            "role",
            "trigger_class",
        ):
            object.__setattr__(
                self,
                field_name,
                _require_text(getattr(self, field_name), field_name),
            )
        object.__setattr__(
            self,
            "component",
            _normalize_optional_text(self.component, "component"),
        )
        for field_name in ("previous", "current", "delta", "threshold_multiple"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _finite_number(value, field_name))
        if self.threshold_multiple < 0:
            raise Stage1ContractError("threshold_multiple must be non-negative")


@dataclass(frozen=True, slots=True)
class Stage1DataQualityIssue:
    metric: str
    component: str | None
    status: MetricStatus


@dataclass(frozen=True, slots=True)
class Stage1DataQuality:
    status: DataQualityStatus
    issues: tuple[Stage1DataQualityIssue, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.status, DataQualityStatus):
            raise Stage1ContractError("data quality status is invalid")
        if not isinstance(self.issues, tuple):
            raise Stage1ContractError("data quality issues must be an immutable tuple")
        if any(not isinstance(item, Stage1DataQualityIssue) for item in self.issues):
            raise Stage1ContractError("invalid data quality issue")
        if self.status is DataQualityStatus.CLEAN and self.issues:
            raise Stage1ContractError("clean data quality cannot expose issues")
        if self.status is not DataQualityStatus.CLEAN and not self.issues:
            raise Stage1ContractError("non-clean data quality requires issues")


@dataclass(frozen=True, slots=True)
class Stage1SymbolEvaluation:
    symbol: str
    metrics: tuple[Stage1Metric, ...]
    reasons: tuple[Stage1Reason, ...]
    data_quality: Stage1DataQuality

    @property
    def triggered(self) -> bool:
        return any(reason.role == RuleRole.PRIMARY.value for reason in self.reasons)


@dataclass(frozen=True, slots=True)
class Stage1Candidate:
    symbol: str
    name: str | None
    rank: int
    reasons: tuple[Stage1Reason, ...]
    metrics: tuple[Stage1Metric, ...]
    data_quality: Stage1DataQuality

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _normalize_optional_text(self.name, "name"))
        if isinstance(self.rank, bool) or not isinstance(self.rank, int) or self.rank < 1:
            raise Stage1ContractError("candidate rank must be a positive integer")
        if not isinstance(self.reasons, tuple) or not self.reasons:
            raise Stage1ContractError("candidate requires deterministic reasons")
        if not any(item.role == RuleRole.PRIMARY.value for item in self.reasons):
            raise Stage1ContractError("candidate requires at least one primary trigger")
        if not isinstance(self.metrics, tuple) or not self.metrics:
            raise Stage1ContractError("candidate requires metrics")
        if not isinstance(self.data_quality, Stage1DataQuality):
            raise Stage1ContractError("candidate data_quality is invalid")


@dataclass(frozen=True, slots=True)
class Stage1ScanResult:
    market_date: date
    methodology_version: str
    source_policy: str
    universe_count: int
    screened_count: int
    triggered_count: int
    candidate_count: int
    candidate_limit: int
    truncated: bool
    candidates: tuple[Stage1Candidate, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "market_date",
            _require_date(self.market_date, "market_date"),
        )
        if self.methodology_version != STAGE1_METHODOLOGY_VERSION:
            raise Stage1ContractError("scan methodology_version is unsupported")
        if self.source_policy != TWSE_BASELINE_SOURCE_POLICY:
            raise Stage1ContractError("scan source_policy must be twse_baseline")
        for field_name in (
            "universe_count",
            "screened_count",
            "triggered_count",
            "candidate_count",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise Stage1ContractError(f"{field_name} must be non-negative")
        if (
            isinstance(self.candidate_limit, bool)
            or not isinstance(self.candidate_limit, int)
            or self.candidate_limit < 1
        ):
            raise Stage1ContractError("candidate_limit must be positive")
        if not isinstance(self.candidates, tuple):
            raise Stage1ContractError("candidates must be an immutable tuple")
        if self.candidate_count != len(self.candidates):
            raise Stage1ContractError("candidate_count must equal candidates length")
        if self.candidate_count > self.candidate_limit:
            raise Stage1ContractError("candidate_count exceeds candidate_limit")
        if self.triggered_count < self.candidate_count:
            raise Stage1ContractError("triggered_count cannot be below candidate_count")
        if self.screened_count > self.universe_count:
            raise Stage1ContractError("screened_count cannot exceed universe_count")
        if self.truncated is not (self.triggered_count > self.candidate_count):
            raise Stage1ContractError("truncated does not match candidate counts")
        if tuple(item.rank for item in self.candidates) != tuple(
            range(1, self.candidate_count + 1)
        ):
            raise Stage1ContractError(
                "rank must be contiguous deterministic research priority"
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "market_date": self.market_date.isoformat(),
            "methodology_version": self.methodology_version,
            "source_policy": self.source_policy,
            "universe_count": self.universe_count,
            "screened_count": self.screened_count,
            "triggered_count": self.triggered_count,
            "candidate_count": self.candidate_count,
            "candidate_limit": self.candidate_limit,
            "truncated": self.truncated,
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


def evaluate_stage1_snapshot(
    snapshot: Stage1ResearchSnapshot,
    methodology: Stage1Methodology = STAGE1_METHODOLOGY_V1,
) -> Stage1SymbolEvaluation:
    """Evaluate one immutable symbol snapshot without deciding shortlist size."""

    if not isinstance(snapshot, Stage1ResearchSnapshot):
        raise Stage1ContractError("snapshot must be Stage1ResearchSnapshot")
    if methodology is not STAGE1_METHODOLOGY_V1:
        raise Stage1ContractError("Stage 1 v1 requires the frozen methodology")

    metrics = _calculate_metrics(snapshot, methodology)
    reasons = _evaluate_rules(metrics, methodology)
    issues = tuple(
        Stage1DataQualityIssue(
            metric=metric.metric,
            component=metric.component,
            status=metric.status,
        )
        for metric in metrics
        if metric.status is not MetricStatus.AVAILABLE
    )
    available_count = sum(
        metric.status is MetricStatus.AVAILABLE for metric in metrics
    )
    if not issues:
        quality_status = DataQualityStatus.CLEAN
    elif available_count:
        quality_status = DataQualityStatus.WARNING
    else:
        quality_status = DataQualityStatus.UNAVAILABLE
    return Stage1SymbolEvaluation(
        symbol=snapshot.symbol,
        metrics=metrics,
        reasons=reasons,
        data_quality=Stage1DataQuality(status=quality_status, issues=issues),
    )


def scan_stage1(
    *,
    universe: MarketUniverseSnapshot,
    research_snapshots: tuple[Stage1ResearchSnapshot, ...],
    as_of_date: date,
    candidate_limit: int,
    methodology: Stage1Methodology = STAGE1_METHODOLOGY_V1,
) -> Stage1ScanResult:
    """Evaluate exactly the S1 scan-eligible member set and rank research priority."""

    if not isinstance(universe, MarketUniverseSnapshot):
        raise Stage1ContractError("universe must be MarketUniverseSnapshot")
    frozen_date = _require_date(as_of_date, "as_of_date")
    if universe.market_date != frozen_date:
        raise Stage1ContractError("universe market_date must equal frozen as_of_date")
    if universe.source_policy != TWSE_BASELINE_SOURCE_POLICY:
        raise Stage1ContractError("universe source_policy must be twse_baseline")
    if methodology is not STAGE1_METHODOLOGY_V1:
        raise Stage1ContractError("Stage 1 v1 requires the frozen methodology")
    if (
        isinstance(candidate_limit, bool)
        or not isinstance(candidate_limit, int)
        or candidate_limit < 1
    ):
        raise Stage1ContractError("candidate_limit must be positive")
    if not isinstance(research_snapshots, tuple):
        raise Stage1ContractError("research_snapshots must be an immutable tuple")

    eligible_members = tuple(
        item
        for item in universe.members
        if item.status is UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE
    )
    eligible_by_symbol = {item.symbol: item for item in eligible_members}
    snapshots_by_symbol: dict[str, Stage1ResearchSnapshot] = {}
    for snapshot in research_snapshots:
        if not isinstance(snapshot, Stage1ResearchSnapshot):
            raise Stage1ContractError("research_snapshots contain an invalid item")
        if snapshot.symbol in snapshots_by_symbol:
            raise Stage1ContractError(
                f"duplicate screening snapshot for {snapshot.symbol}"
            )
        if snapshot.as_of_date != frozen_date:
            raise Stage1ContractError("screening snapshot as_of_date mismatch")
        snapshots_by_symbol[snapshot.symbol] = snapshot
    expected = set(eligible_by_symbol)
    actual = set(snapshots_by_symbol)
    if actual != expected:
        raise Stage1ContractError(
            "screening snapshots must exactly match the scan-eligible universe: "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )

    evaluated: list[tuple[object, Stage1SymbolEvaluation]] = []
    for symbol in sorted(expected):
        evaluation = evaluate_stage1_snapshot(
            snapshots_by_symbol[symbol],
            methodology,
        )
        if evaluation.triggered:
            evaluated.append((eligible_by_symbol[symbol], evaluation))

    evaluated.sort(key=lambda item: _research_priority_key(item[1], methodology))
    selected = evaluated[:candidate_limit]
    candidates = tuple(
        Stage1Candidate(
            symbol=evaluation.symbol,
            name=member.name,
            rank=index,
            reasons=evaluation.reasons,
            metrics=evaluation.metrics,
            data_quality=evaluation.data_quality,
        )
        for index, (member, evaluation) in enumerate(selected, start=1)
    )
    return Stage1ScanResult(
        market_date=frozen_date,
        methodology_version=methodology.version,
        source_policy=universe.source_policy,
        universe_count=universe.universe_count,
        screened_count=len(eligible_members),
        triggered_count=len(evaluated),
        candidate_count=len(candidates),
        candidate_limit=candidate_limit,
        truncated=len(evaluated) > len(candidates),
        candidates=candidates,
    )


def _calculate_metrics(
    snapshot: Stage1ResearchSnapshot,
    methodology: Stage1Methodology,
) -> tuple[Stage1Metric, ...]:
    if snapshot.price_status is PriceSnapshotStatus.MISSING_SOURCE:
        price_status = MetricStatus.MISSING_SOURCE
    elif snapshot.price_status is PriceSnapshotStatus.MARKET_DATE_MISMATCH:
        price_status = MetricStatus.MARKET_DATE_MISMATCH
    else:
        price_status = None

    if price_status is not None:
        price_metrics = tuple(
            _unavailable_metric(
                metric,
                None,
                price_status,
                snapshot.as_of_date,
                None,
                _metric_units(metric, None),
            )
            for metric in (
                PRICE_CHANGE_1D,
                VOLUME_ANOMALY,
                RETURN_20D_CHANGE,
                VOLATILITY_REGIME_CHANGE,
                MA_DISTANCE_CHANGE,
            )
        )
    else:
        price_metrics = (
            _price_change_metric(snapshot, methodology),
            _volume_metric(snapshot, methodology),
            _return_metric(snapshot, methodology),
            _volatility_metric(snapshot, methodology),
            _ma_metric(snapshot, methodology),
        )
    valuation_metrics = tuple(
        _valuation_metric(snapshot, component, methodology)
        for component in (PE_RATIO, PB_RATIO, DIVIDEND_YIELD)
    )
    return price_metrics + valuation_metrics


def _price_change_metric(
    snapshot: Stage1ResearchSnapshot,
    methodology: Stage1Methodology,
) -> Stage1Metric:
    prices = snapshot.prices
    if len(prices) < 3:
        return _insufficient_metric(snapshot, PRICE_CHANGE_1D, None)
    values = (prices[-3].close, prices[-2].close, prices[-1].close)
    if any(value <= 0 for value in values):
        return _invalid_metric(snapshot, PRICE_CHANGE_1D, None)
    previous = _q((values[1] / values[0] - 1.0) * 100.0, methodology)
    current = _q((values[2] / values[1] - 1.0) * 100.0, methodology)
    return _available_metric(
        snapshot,
        PRICE_CHANGE_1D,
        None,
        previous,
        current,
        _q(current - previous, methodology),
        ("percent", "percentage_point"),
    )


def _volume_metric(
    snapshot: Stage1ResearchSnapshot,
    methodology: Stage1Methodology,
) -> Stage1Metric:
    prices = snapshot.prices
    required = methodology.volume_window + 2
    if len(prices) < required:
        return _insufficient_metric(snapshot, VOLUME_ANOMALY, None)
    previous_baseline = statistics.fmean(
        item.volume for item in prices[-required:-2]
    )
    current_baseline = statistics.fmean(
        item.volume for item in prices[-(methodology.volume_window + 1) : -1]
    )
    if previous_baseline <= 0 or current_baseline <= 0:
        return _invalid_metric(snapshot, VOLUME_ANOMALY, None)
    previous = _q(prices[-2].volume / previous_baseline, methodology)
    current = _q(prices[-1].volume / current_baseline, methodology)
    return _available_metric(
        snapshot,
        VOLUME_ANOMALY,
        None,
        previous,
        current,
        _q(current - previous, methodology),
        ("ratio", "ratio"),
    )


def _return_metric(
    snapshot: Stage1ResearchSnapshot,
    methodology: Stage1Methodology,
) -> Stage1Metric:
    prices = snapshot.prices
    required = methodology.return_window + 2
    if len(prices) < required:
        return _insufficient_metric(snapshot, RETURN_20D_CHANGE, None)
    previous_start = prices[-required].close
    current_start = prices[-(methodology.return_window + 1)].close
    if min(previous_start, current_start, prices[-2].close, prices[-1].close) <= 0:
        return _invalid_metric(snapshot, RETURN_20D_CHANGE, None)
    previous = _q((prices[-2].close / previous_start - 1.0) * 100.0, methodology)
    current = _q((prices[-1].close / current_start - 1.0) * 100.0, methodology)
    return _available_metric(
        snapshot,
        RETURN_20D_CHANGE,
        None,
        previous,
        current,
        _q(current - previous, methodology),
        ("percent", "percentage_point"),
    )


def _volatility_metric(
    snapshot: Stage1ResearchSnapshot,
    methodology: Stage1Methodology,
) -> Stage1Metric:
    prices = snapshot.prices
    required = methodology.volatility_return_window + 2
    if len(prices) < required:
        return _insufficient_metric(snapshot, VOLATILITY_REGIME_CHANGE, None)
    relevant = prices[-required:]
    closes = tuple(item.close for item in relevant)
    if any(value <= 0 for value in closes):
        return _invalid_metric(snapshot, VOLATILITY_REGIME_CHANGE, None)
    returns = tuple(closes[index] / closes[index - 1] - 1.0 for index in range(1, len(closes)))
    previous_returns = returns[:-1]
    current_returns = returns[1:]
    annualizer = math.sqrt(methodology.volatility_annualization_days)
    previous = _q(statistics.stdev(previous_returns) * annualizer * 100.0, methodology)
    current = _q(statistics.stdev(current_returns) * annualizer * 100.0, methodology)
    return _available_metric(
        snapshot,
        VOLATILITY_REGIME_CHANGE,
        None,
        previous,
        current,
        _q(current - previous, methodology),
        ("annualized_percent", "percentage_point"),
    )


def _ma_metric(
    snapshot: Stage1ResearchSnapshot,
    methodology: Stage1Methodology,
) -> Stage1Metric:
    prices = snapshot.prices
    required = methodology.ma_window + 1
    if len(prices) < required:
        return _insufficient_metric(snapshot, MA_DISTANCE_CHANGE, None)
    relevant = prices[-required:]
    if any(item.close <= 0 for item in relevant):
        return _invalid_metric(snapshot, MA_DISTANCE_CHANGE, None)
    previous_ma = statistics.fmean(item.close for item in relevant[:-1])
    current_ma = statistics.fmean(item.close for item in relevant[1:])
    if previous_ma <= 0 or current_ma <= 0:
        return _invalid_metric(snapshot, MA_DISTANCE_CHANGE, None)
    previous = _q((relevant[-2].close / previous_ma - 1.0) * 100.0, methodology)
    current = _q((relevant[-1].close / current_ma - 1.0) * 100.0, methodology)
    return _available_metric(
        snapshot,
        MA_DISTANCE_CHANGE,
        None,
        previous,
        current,
        _q(current - previous, methodology),
        ("percent", "percentage_point"),
    )


def _valuation_metric(
    snapshot: Stage1ResearchSnapshot,
    component: str,
    methodology: Stage1Methodology,
) -> Stage1Metric:
    current_state = snapshot.current_valuation
    previous_state = snapshot.previous_valuation
    units = _metric_units(VALUATION_UPDATE, component)
    if (
        previous_state is None
        or current_state.status is ValuationSnapshotStatus.MISSING_SOURCE
        or previous_state.status is ValuationSnapshotStatus.MISSING_SOURCE
    ):
        return _unavailable_metric(
            VALUATION_UPDATE,
            component,
            MetricStatus.VALUATION_UNAVAILABLE,
            snapshot.as_of_date,
            _prior_market_date(snapshot),
            units,
        )
    field_name = {
        PE_RATIO: "pe_ratio",
        PB_RATIO: "pb_ratio",
        DIVIDEND_YIELD: "dividend_yield_pct",
    }[component]
    previous = getattr(previous_state, field_name)
    current = getattr(current_state, field_name)
    if previous is None and current is None:
        return _unavailable_metric(
            VALUATION_UPDATE,
            component,
            MetricStatus.VALUATION_UNAVAILABLE,
            snapshot.as_of_date,
            previous_state.as_of_date,
            units,
        )
    if previous is None or current is None:
        return _available_metric(
            snapshot,
            VALUATION_UPDATE,
            component,
            previous,
            current,
            None,
            units,
        )
    if component in {PE_RATIO, PB_RATIO}:
        if previous <= 0 or current <= 0:
            return _unavailable_metric(
                VALUATION_UPDATE,
                component,
                MetricStatus.INVALID_DENOMINATOR,
                snapshot.as_of_date,
                previous_state.as_of_date,
                units,
            )
        delta = _q((current / previous - 1.0) * 100.0, methodology)
    else:
        delta = _q(current - previous, methodology)
    return _available_metric(
        snapshot,
        VALUATION_UPDATE,
        component,
        _q(previous, methodology),
        _q(current, methodology),
        delta,
        units,
    )


def _evaluate_rules(
    metrics: tuple[Stage1Metric, ...],
    methodology: Stage1Methodology,
) -> tuple[Stage1Reason, ...]:
    by_key = {(item.metric, item.component): item for item in metrics}
    reasons: list[Stage1Reason] = []
    for rule in methodology.rules:
        metric = by_key[(rule.metric, rule.component)]
        if metric.status is not MetricStatus.AVAILABLE:
            continue
        matched, multiple = _rule_match(rule, metric, methodology)
        if matched:
            reasons.append(
                Stage1Reason(
                    code=rule.code,
                    metric=rule.metric,
                    component=rule.component,
                    previous=metric.previous,
                    current=metric.current,
                    delta=metric.delta,
                    unit=rule.unit,
                    operator=rule.operator.value,
                    threshold=rule.threshold,
                    rule_version=methodology.version,
                    role=rule.role.value,
                    trigger_class=rule.trigger_class,
                    threshold_multiple=multiple,
                )
            )
    return tuple(reasons)


def _rule_match(
    rule: TriggerRule,
    metric: Stage1Metric,
    methodology: Stage1Methodology,
) -> tuple[bool, float]:
    threshold = rule.threshold
    if rule.operator is RuleOperator.AVAILABILITY_CHANGED:
        matched = (metric.previous is None) != (metric.current is None)
        return matched, 1.0 if matched else 0.0
    if not isinstance(threshold, (int, float)):
        raise Stage1ContractError("numeric operator requires numeric threshold")
    threshold_value = float(threshold)
    if rule.operator is RuleOperator.ABS_CURRENT_GTE:
        if metric.current is None:
            return False, 0.0
        trigger_value = abs(metric.current)
        matched = trigger_value >= threshold_value
        multiple = trigger_value / threshold_value
    elif rule.operator is RuleOperator.CURRENT_GTE:
        if metric.current is None:
            return False, 0.0
        trigger_value = metric.current
        matched = trigger_value >= threshold_value
        multiple = max(trigger_value, 0.0) / threshold_value
    elif rule.operator is RuleOperator.CURRENT_LTE:
        if metric.current is None:
            return False, 0.0
        matched = metric.current <= threshold_value
        multiple = (
            threshold_value / metric.current if metric.current > 0 else 1.0
        )
    elif rule.operator is RuleOperator.ABS_DELTA_GTE:
        if metric.delta is None:
            return False, 0.0
        trigger_value = abs(metric.delta)
        matched = trigger_value >= threshold_value
        multiple = trigger_value / threshold_value
    else:  # pragma: no cover - Enum exhaustiveness guard.
        raise Stage1ContractError("unsupported rule operator")
    return matched, _q(multiple, methodology)


def _research_priority_key(
    evaluation: Stage1SymbolEvaluation,
    methodology: Stage1Methodology,
) -> tuple[int, int, float, str]:
    primary = tuple(
        reason for reason in evaluation.reasons if reason.role == RuleRole.PRIMARY.value
    )
    rules = {rule.code: rule for rule in methodology.rules}
    class_priority = min(rules[reason.code].class_priority for reason in primary)
    threshold_multiple = max(reason.threshold_multiple for reason in primary)
    return (-len(primary), class_priority, -threshold_multiple, evaluation.symbol)


def _available_metric(
    snapshot: Stage1ResearchSnapshot,
    metric: str,
    component: str | None,
    previous: float | None,
    current: float | None,
    delta: float | None,
    units: tuple[str, str],
) -> Stage1Metric:
    return Stage1Metric(
        metric=metric,
        component=component,
        status=MetricStatus.AVAILABLE,
        previous=previous,
        current=current,
        delta=delta,
        value_unit=units[0],
        delta_unit=units[1],
        previous_as_of_date=_prior_market_date(snapshot),
        current_as_of_date=snapshot.as_of_date,
    )


def _unavailable_metric(
    metric: str,
    component: str | None,
    status: MetricStatus,
    current_date: date,
    previous_date: date | None,
    units: tuple[str, str],
) -> Stage1Metric:
    return Stage1Metric(
        metric=metric,
        component=component,
        status=status,
        previous=None,
        current=None,
        delta=None,
        value_unit=units[0],
        delta_unit=units[1],
        previous_as_of_date=previous_date,
        current_as_of_date=current_date,
    )


def _insufficient_metric(
    snapshot: Stage1ResearchSnapshot,
    metric: str,
    component: str | None,
) -> Stage1Metric:
    return _unavailable_metric(
        metric,
        component,
        MetricStatus.INSUFFICIENT_HISTORY,
        snapshot.as_of_date,
        _prior_market_date(snapshot),
        _metric_units(metric, component),
    )


def _invalid_metric(
    snapshot: Stage1ResearchSnapshot,
    metric: str,
    component: str | None,
) -> Stage1Metric:
    return _unavailable_metric(
        metric,
        component,
        MetricStatus.INVALID_DENOMINATOR,
        snapshot.as_of_date,
        _prior_market_date(snapshot),
        _metric_units(metric, component),
    )


def _metric_units(metric: str, component: str | None) -> tuple[str, str]:
    if metric == PRICE_CHANGE_1D:
        return ("percent", "percentage_point")
    if metric == VOLUME_ANOMALY:
        return ("ratio", "ratio")
    if metric == RETURN_20D_CHANGE:
        return ("percent", "percentage_point")
    if metric == VOLATILITY_REGIME_CHANGE:
        return ("annualized_percent", "percentage_point")
    if metric == MA_DISTANCE_CHANGE:
        return ("percent", "percentage_point")
    if metric == VALUATION_UPDATE and component in {PE_RATIO, PB_RATIO}:
        return ("ratio", "percent_relative_change")
    if metric == VALUATION_UPDATE and component == DIVIDEND_YIELD:
        return ("percent", "percentage_point")
    raise Stage1ContractError("unsupported metric unit mapping")


def _prior_market_date(snapshot: Stage1ResearchSnapshot) -> date | None:
    return snapshot.prices[-2].trade_date if len(snapshot.prices) >= 2 else None


def _candidate_dict(candidate: Stage1Candidate) -> dict[str, object]:
    return {
        "symbol": candidate.symbol,
        "name": candidate.name,
        "rank": candidate.rank,
        "reasons": [_reason_dict(item) for item in candidate.reasons],
        "metrics": [_metric_dict(item) for item in candidate.metrics],
        "data_quality": _data_quality_dict(candidate.data_quality),
    }


def _reason_dict(reason: Stage1Reason) -> dict[str, object]:
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


def _metric_dict(metric: Stage1Metric) -> dict[str, object]:
    return {
        "metric": metric.metric,
        "component": metric.component,
        "status": metric.status.value,
        "previous": metric.previous,
        "current": metric.current,
        "delta": metric.delta,
        "value_unit": metric.value_unit,
        "delta_unit": metric.delta_unit,
        "previous_as_of_date": (
            metric.previous_as_of_date.isoformat()
            if metric.previous_as_of_date is not None
            else None
        ),
        "current_as_of_date": metric.current_as_of_date.isoformat(),
    }


def _data_quality_dict(value: Stage1DataQuality) -> dict[str, object]:
    return {
        "status": value.status.value,
        "issues": [
            {
                "metric": item.metric,
                "component": item.component,
                "status": item.status.value,
            }
            for item in value.issues
        ],
    }


def _q(value: float, methodology: Stage1Methodology) -> float:
    rounded = round(_finite_number(value, "metric value"), methodology.metric_decimal_places)
    return 0.0 if rounded == 0 else rounded


def _normalize_symbol(value: object) -> str:
    symbol = _require_text(value, "symbol").upper()
    if _SYMBOL.fullmatch(symbol) is None:
        raise Stage1ContractError("symbol must be 2-12 uppercase letters or digits")
    return symbol


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise Stage1ContractError(f"{field_name} must be a date")
    return value


def _finite_number(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Stage1ContractError(f"{field_name} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise Stage1ContractError(f"{field_name} must be a finite number")
    return number
