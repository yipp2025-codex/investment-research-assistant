"""Phase 6B canonical daily results and deterministic change-only reports.

This module is an output layer over frozen Phase 1-6A services.  It reads
canonical daily prices, company metrics, pipeline provenance, and optional
TWSE/E.SUN validation evidence.  It does not fetch market data, modify
providers, or implement trading logic.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Protocol, cast

from app.analysis import HistoricalResearchAnalysis, HistoricalResearchAnalyzer
from app.as_of_policy import FrozenAsOfPolicyV1
from app.models import (
    CompanyMetric,
    CrossValidationOutcome,
    PipelineRunStatus,
)
from app.reports.research_dataset_compat import (
    SnapshotResearchReadAdapter,
    SnapshotValidationReadAdapter,
    project_pipeline_run,
)
from app.reports.contracts import (
    BatchContextReader,
    DailyResultReader,
    DailyResultStore,
)
from app.research_dataset import (
    ResearchDataset,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
)
from app.storage import (
    StoredDailyResearchReport,
    StoredDailyResearchResult,
)


CANONICAL_SCHEMA_VERSION = "daily-research-result.v1"
METHODOLOGY_VERSION = "6b-daily-v1"

_AS_OF_POLICY = FrozenAsOfPolicyV1()

AVAILABLE_STATUSES = frozenset({"available"})
UNAVAILABLE_STATUSES = frozenset(
    {
        "insufficient_history",
        "missing_source",
        "market_date_mismatch",
        "source_discrepancy",
        "previous_result_missing",
        "methodology_incompatible",
        "not_applicable",
    }
)
VALUE_STATUSES = AVAILABLE_STATUSES | UNAVAILABLE_STATUSES
DATA_QUALITY_STATUSES = frozenset({"clean", "warning", "blocked", "unavailable"})
TRADE_LANGUAGE = (
    "買進",
    "賣出",
    "加碼",
    "減碼",
    "進場",
    "出場",
)

METRIC_ORDER = (
    "latest_close",
    "latest_volume",
    "return_20d",
    "return_60d",
    "return_120d",
    "volatility_60d",
    "max_drawdown_20d",
    "max_drawdown_60d",
    "max_drawdown_120d",
    "volume_ratio_20d",
    "volume_anomaly",
    "ma_distance_20d",
    "ma_distance_60d",
    "ma_distance_120d",
)
CHANGE_METRIC_ORDER = tuple(
    field for field in METRIC_ORDER if field not in {"latest_close", "latest_volume"}
)
VALUATION_ORDER = (
    "pe_ratio",
    "pb_ratio",
    "dividend_yield",
)
WINDOW_METRIC_THRESHOLDS = {
    "return_20d": 1.0,
    "return_60d": 1.0,
    "return_120d": 1.0,
    "volatility_60d": 1.0,
    "max_drawdown_20d": 1.0,
    "max_drawdown_60d": 1.0,
    "max_drawdown_120d": 1.0,
    "volume_ratio_20d": 0.25,
    "ma_distance_20d": 1.0,
    "ma_distance_60d": 1.0,
    "ma_distance_120d": 1.0,
}


class DailyResearchReportError(RuntimeError):
    """Base class for canonical report errors."""


class CanonicalSchemaError(DailyResearchReportError):
    """The canonical payload does not satisfy the Phase 6B schema."""


class _ResearchReadAdapter(Protocol):
    """Frozen legacy projection shape used only inside payload assembly."""

    def list_daily_prices(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[object]: ...

    def list_company_metrics(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[CompanyMetric]: ...

    def get_pipeline_run(self, run_id: str) -> object | None: ...

    def get_pipeline_run_for_target(
        self,
        symbol: str,
        target_date: date,
    ) -> object | None: ...


class _ValidationReadAdapter(Protocol):
    """Frozen validation projection shape used only inside payload assembly."""

    def get_run(self, run_id: str) -> object | None: ...

    def list_runs(self, symbol: str) -> list[object]: ...

    def list_discrepancies(self, run_id: str) -> list[object]: ...

    def list_observations(self, run_id: str) -> list[object]: ...


@dataclass(frozen=True, slots=True)
class MetricValue:
    """A typed value with explicit unavailable semantics."""

    status: str
    value: float | int | str | None
    unit: str
    as_of_date: date | None

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "value": self.value,
            "unit": self.unit,
            "as_of_date": (
                None if self.as_of_date is None else self.as_of_date.isoformat()
            ),
        }


@dataclass(frozen=True, slots=True)
class ChangeThresholds:
    """Versioned, neutral thresholds for descriptive change detection."""

    version: str = "6b-thresholds-v1"
    return_percentage_points: float = 1.0
    volatility_percentage_points: float = 1.0
    drawdown_percentage_points: float = 1.0
    moving_average_percentage_points: float = 1.0
    volume_ratio: float = 0.25

    def for_field(self, field: str) -> float:
        if field.startswith("return_"):
            return self.return_percentage_points
        if field == "volatility_60d":
            return self.volatility_percentage_points
        if field.startswith("max_drawdown_"):
            return self.drawdown_percentage_points
        if field.startswith("ma_distance_"):
            return self.moving_average_percentage_points
        if field == "volume_ratio_20d":
            return self.volume_ratio
        raise KeyError(field)


@dataclass(frozen=True, slots=True)
class DailyReportGenerationResult:
    canonical: StoredDailyResearchResult
    report: StoredDailyResearchReport
    idempotent_replay: bool
    render_attempted: bool


class DailyResearchReportService:
    """Build, validate, persist, and render one daily result."""

    def __init__(
        self,
        repository: object | None = None,
        *,
        report_repository: object | None = None,
        cross_validation_repository: object | None = None,
        analyzer: HistoricalResearchAnalyzer | None = None,
        thresholds: ChangeThresholds | None = None,
        methodology_version: str = METHODOLOGY_VERSION,
        clock: Callable[[], datetime] | None = None,
        renderer: Callable[[Mapping[str, object]], str] | None = None,
        dataset: ResearchDataset | None = None,
        result_reader: DailyResultReader | None = None,
        result_store: DailyResultStore | None = None,
        batch_context_reader: BatchContextReader | None = None,
    ) -> None:
        explicit_ports = (
            result_reader,
            result_store,
            batch_context_reader,
        )
        if any(item is not None for item in explicit_ports):
            if not all(item is not None for item in explicit_ports):
                raise TypeError(
                    "result_reader, result_store, and batch_context_reader "
                    "must be supplied together"
                )
            if dataset is None:
                raise TypeError("explicit report ports require ResearchDataset")
            if any(
                item is not None
                for item in (
                    repository,
                    report_repository,
                    cross_validation_repository,
                )
            ):
                raise TypeError(
                    "explicit report ports cannot be mixed with legacy repositories"
                )
            self.dataset = dataset
            self.result_reader = result_reader
            self.result_store = result_store
            self.batch_context_reader = batch_context_reader
            self._legacy_research_reader: _ResearchReadAdapter | None = None
            self._legacy_validation_reader: _ValidationReadAdapter | None = None
        else:
            # M8/M9 characterization callers retain the old constructor shape.
            # Concrete adapter selection lives in the composition module.
            from app.reports.composition import compose_compatibility_dependencies

            dependencies = compose_compatibility_dependencies(
                repository,
                report_repository=report_repository,
                cross_validation_repository=cross_validation_repository,
                dataset=dataset,
            )
            self.dataset = dependencies.dataset
            self.result_reader = dependencies.result_reader
            self.result_store = dependencies.result_store
            self.batch_context_reader = dependencies.batch_context_reader
            self._legacy_research_reader = cast(
                _ResearchReadAdapter | None,
                dependencies.legacy_research_reader,
            )
            self._legacy_validation_reader = cast(
                _ValidationReadAdapter | None,
                dependencies.legacy_validation_reader,
            )
        self.analyzer = analyzer or HistoricalResearchAnalyzer()
        self.thresholds = thresholds or ChangeThresholds()
        self.methodology_version = methodology_version
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.renderer = renderer or render_daily_research_markdown

    def generate(
        self,
        symbol: str,
        market_date: date,
        *,
        requested_date: date | None = None,
        batch_run_id: str | None = None,
        symbol_run_id: str | None = None,
        pipeline_run_id: str | None = None,
        validation_run_id: str | None = None,
        historical_run_id: str | None = None,
    ) -> DailyReportGenerationResult:
        """Generate one canonical result and its independently retryable report."""
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol must not be empty")
        requested = requested_date or market_date
        self.result_store.initialize()

        existing = self.result_reader.get_result(
            normalized_symbol, market_date, self.methodology_version
        )
        if existing is not None:
            report = self._render_if_needed(existing)
            return DailyReportGenerationResult(
                canonical=existing,
                report=report,
                idempotent_replay=True,
                render_attempted=report.report_status == "rendered",
            )

        payload = self.build_payload(
            normalized_symbol,
            market_date,
            requested_date=requested,
            batch_run_id=batch_run_id,
            symbol_run_id=symbol_run_id,
            pipeline_run_id=pipeline_run_id,
            validation_run_id=validation_run_id,
            historical_run_id=historical_run_id,
        )
        validate_canonical_payload(payload)
        payload_json = _canonical_json(payload)
        payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        provenance_json = _canonical_json(payload["provenance"])
        result_id = str(payload["result_id"])
        canonical = self.result_store.save_or_get_result(
            result_id=result_id,
            symbol=normalized_symbol,
            market_date=market_date,
            requested_date=requested,
            methodology_version=self.methodology_version,
            schema_version=CANONICAL_SCHEMA_VERSION,
            data_quality_status=str(payload["data_quality"]["status"]),
            payload_json=payload_json,
            payload_sha256=payload_sha256,
            provenance_json=provenance_json,
            created_at=self._now(),
        )
        report = self._render_if_needed(canonical)
        return DailyReportGenerationResult(
            canonical=canonical,
            report=report,
            idempotent_replay=False,
            render_attempted=True,
        )

    def generate_for_batch_symbol(
        self,
        batch_run_id: str,
        symbol: str,
        *,
        validation_run_id: str | None = None,
        historical_run_id: str | None = None,
    ) -> DailyReportGenerationResult:
        """Use an existing Phase 6A batch checkpoint as report provenance."""
        batch = self.batch_context_reader.get_batch_run(batch_run_id)
        if batch is None:
            raise DailyResearchReportError(f"unknown batch run {batch_run_id}")
        if batch.resolved_market_date is None:
            raise DailyResearchReportError(
                "batch run does not have a resolved market date"
            )
        symbol_run = self.batch_context_reader.get_symbol_run(batch_run_id, symbol)
        if symbol_run is None:
            raise DailyResearchReportError(
                f"symbol {symbol.strip().upper()} is not in batch {batch_run_id}"
            )
        if symbol_run.status != "success":
            raise DailyResearchReportError(
                f"symbol run is not successful: {symbol_run.status}"
            )
        return self.generate(
            symbol,
            batch.resolved_market_date,
            requested_date=batch.requested_date,
            batch_run_id=batch_run_id,
            symbol_run_id=symbol_run.symbol_run_id,
            pipeline_run_id=symbol_run.pipeline_run_id,
            validation_run_id=validation_run_id,
            historical_run_id=historical_run_id,
        )

    def build_payload(
        self,
        symbol: str,
        market_date: date,
        *,
        requested_date: date,
        batch_run_id: str | None = None,
        symbol_run_id: str | None = None,
        pipeline_run_id: str | None = None,
        validation_run_id: str | None = None,
        historical_run_id: str | None = None,
    ) -> dict[str, object]:
        normalized_symbol = symbol.strip().upper()
        if self.dataset is not None:
            snapshot = self.dataset.read(
                ResearchDatasetRequest(
                    symbol=normalized_symbol,
                    as_of_date=market_date,
                    history_observations=None,
                    pipeline_run_id=pipeline_run_id,
                    historical_run_id=historical_run_id,
                    validation_run_id=validation_run_id,
                )
            )
            return self._build_payload_from_snapshot(
                snapshot,
                normalized_symbol,
                market_date,
                requested_date=requested_date,
                batch_run_id=batch_run_id,
                symbol_run_id=symbol_run_id,
                pipeline_run_id=pipeline_run_id,
                validation_run_id=validation_run_id,
                historical_run_id=historical_run_id,
            )
        if (
            self._legacy_research_reader is None
            or self._legacy_validation_reader is None
        ):
            raise DailyResearchReportError("research input ports are not configured")
        return self._build_payload_from_readers(
            self._legacy_research_reader,
            self._legacy_validation_reader,
            normalized_symbol,
            market_date,
            requested_date=requested_date,
            batch_run_id=batch_run_id,
            symbol_run_id=symbol_run_id,
            pipeline_run_id=pipeline_run_id,
            validation_run_id=validation_run_id,
            historical_run_id=historical_run_id,
        )

    def _build_payload_from_snapshot(
        self,
        snapshot: ResearchDatasetSnapshot,
        symbol: str,
        market_date: date,
        *,
        requested_date: date,
        batch_run_id: str | None,
        symbol_run_id: str | None,
        pipeline_run_id: str | None,
        validation_run_id: str | None,
        historical_run_id: str | None,
    ) -> dict[str, object]:
        research_reads = SnapshotResearchReadAdapter(
            snapshot,
            project_pipeline_run(snapshot),
        )
        validation_reads = SnapshotValidationReadAdapter(snapshot)
        return self._build_payload_from_readers(
            research_reads,
            validation_reads,
            symbol,
            market_date,
            requested_date=requested_date,
            batch_run_id=batch_run_id,
            symbol_run_id=symbol_run_id,
            pipeline_run_id=pipeline_run_id,
            validation_run_id=validation_run_id,
            historical_run_id=historical_run_id,
        )

    def _build_payload_from_readers(
        self,
        research_reader: _ResearchReadAdapter,
        validation_reader: _ValidationReadAdapter,
        normalized_symbol: str,
        market_date: date,
        *,
        requested_date: date,
        batch_run_id: str | None,
        symbol_run_id: str | None,
        pipeline_run_id: str | None,
        validation_run_id: str | None,
        historical_run_id: str | None,
    ) -> dict[str, object]:
        price_cutoff = _AS_OF_POLICY.price_cutoff(market_date)
        prices = _AS_OF_POLICY.prices_as_of(
            research_reader.list_daily_prices(
                normalized_symbol,
                end_date=price_cutoff,
            ),
            market_date,
        )
        metrics = research_reader.list_company_metrics(
            normalized_symbol, end_date=market_date
        )
        previous_candidate = self.result_reader.get_previous_successful_result(
            normalized_symbol, market_date, self.methodology_version
        )
        previous = (
            previous_candidate
            if previous_candidate is not None
            and _AS_OF_POLICY.previous_result_is_comparable(
                previous_candidate,
                symbol=normalized_symbol,
                target_date=market_date,
                methodology_version=self.methodology_version,
            )
            else None
        )
        any_previous = self.result_reader.get_previous_result_any_methodology(
            normalized_symbol, market_date
        )
        validation_run, discrepancies = self._find_validation(
            validation_reader,
            normalized_symbol,
            market_date,
            validation_run_id,
        )
        quality = self._data_quality(
            market_date=market_date,
            prices=prices,
            validation_run=validation_run,
            discrepancies=discrepancies,
        )
        analysis = self.analyzer.analyze(prices) if prices else None
        value_status = self._price_status(prices, market_date)
        canonical_id = _stable_result_id(
            normalized_symbol, market_date, self.methodology_version
        )
        provenance = self._provenance(
            research_reader,
            validation_reader,
            normalized_symbol,
            market_date,
            prices=prices,
            batch_run_id=batch_run_id,
            symbol_run_id=symbol_run_id,
            pipeline_run_id=pipeline_run_id,
            validation_run_id=(
                None if validation_run is None else validation_run.run_id
            ),
            historical_run_id=historical_run_id,
            validation_run=validation_run,
        )
        payload: dict[str, object] = {
            "schema_version": CANONICAL_SCHEMA_VERSION,
            "result_id": canonical_id,
            "symbol": normalized_symbol,
            "requested_date": requested_date.isoformat(),
            "market_date": market_date.isoformat(),
            "methodology_version": self.methodology_version,
            "methodology": {
                "threshold_version": self.thresholds.version,
                "thresholds": {
                    key: value
                    for key, value in sorted(
                        {
                            "return_percentage_points": self.thresholds.return_percentage_points,
                            "volatility_percentage_points": self.thresholds.volatility_percentage_points,
                            "drawdown_percentage_points": self.thresholds.drawdown_percentage_points,
                            "moving_average_percentage_points": self.thresholds.moving_average_percentage_points,
                            "volume_ratio": self.thresholds.volume_ratio,
                        }.items()
                    )
                },
                "price_analyzer": "HistoricalResearchAnalyzer:frozen-v1",
            },
            "provenance": provenance,
            "data_quality": quality,
            "metrics": self._metrics(
                analysis=analysis,
                prices=prices,
                market_date=market_date,
                value_status=value_status,
            ),
            "valuation": self._valuation(
                metrics, market_date=market_date
            ),
            "comparison": {
                "status": (
                    "available"
                    if previous is not None
                    else (
                        "methodology_incompatible"
                        if any_previous is not None
                        else "previous_result_missing"
                    )
                ),
                "previous_result_id": (
                    None if previous is None else previous.result_id
                ),
                "previous_market_date": (
                    None if previous is None else previous.market_date.isoformat()
                ),
                "changes": (
                    []
                    if previous is None
                    else self._changes(previous.payload, quality, payload_metrics=None)
                ),
            },
            "highlights": [],
        }
        # Build comparison after the payload has its current data quality,
        # metrics, and valuation values.  This avoids a second data read.
        comparison = payload["comparison"]
        if previous is not None:
            assert isinstance(comparison, dict)
            comparison["changes"] = self._changes(
                previous.payload,
                quality,
                payload_metrics=payload["metrics"],
                payload_valuation=payload["valuation"],
            )
        highlights = comparison["changes"] if isinstance(comparison, dict) else []
        payload["highlights"] = highlights
        validate_canonical_payload(payload)
        return payload

    def _render_if_needed(
        self, canonical: StoredDailyResearchResult
    ) -> StoredDailyResearchReport:
        existing_report = self.result_reader.get_report(
            canonical.symbol,
            canonical.market_date,
            canonical.methodology_version,
        )
        if existing_report is not None and existing_report.report_status == "rendered":
            return existing_report
        try:
            markdown = self.renderer(canonical.payload)
            validate_rendered_markdown(markdown)
        except Exception as error:
            return self.result_store.mark_report_render_failed(
                result=canonical,
                error_message=self._safe_error_message(error),
                updated_at=self._now(),
            )
        return self.result_store.save_report_rendered(
            result=canonical,
            markdown=markdown,
            updated_at=self._now(),
        )

    def _find_validation(
        self,
        validation_reader: _ValidationReadAdapter,
        symbol: str,
        market_date: date,
        validation_run_id: str | None,
    ) -> tuple[object | None, tuple[object, ...]]:
        if validation_run_id is not None:
            run = validation_reader.get_run(validation_run_id)
            if run is None or not _AS_OF_POLICY.validation_target_is_consistent(
                run.target_date,
                market_date,
            ):
                return None, ()
        else:
            candidates = [
                item
                for item in validation_reader.list_runs(symbol)
                if _AS_OF_POLICY.validation_target_is_consistent(
                    item.target_date,
                    market_date,
                )
                and item.status is PipelineRunStatus.SUCCESS
            ]
            run = max(candidates, key=lambda item: item.created_at) if candidates else None
        if run is None or run.status is not PipelineRunStatus.SUCCESS:
            return None, ()
        return run, tuple(
            validation_reader.list_discrepancies(run.run_id)
        )

    @staticmethod
    def _price_status(prices: Sequence[object], market_date: date) -> str:
        if not prices:
            return "missing_source"
        if not _AS_OF_POLICY.current_price_is_exact_date(prices, market_date):
            return "market_date_mismatch"
        return "available"

    @staticmethod
    def _value(
        value: float | int | str | None,
        *,
        unit: str,
        as_of_date: date | None,
        unavailable_status: str,
        available_when: bool = True,
    ) -> dict[str, object]:
        status = "available" if value is not None and available_when else unavailable_status
        return MetricValue(
            status=status,
            value=value if status == "available" else None,
            unit=unit,
            as_of_date=as_of_date if status == "available" else None,
        ).as_dict()

    def _metrics(
        self,
        *,
        analysis: HistoricalResearchAnalysis | None,
        prices: Sequence[object],
        market_date: date,
        value_status: str,
    ) -> dict[str, dict[str, object]]:
        if analysis is None:
            return {
                key: self._value(
                    None,
                    unit=_metric_unit(key),
                    as_of_date=None,
                    unavailable_status="missing_source",
                )
                for key in METRIC_ORDER
            }
        as_of_date = analysis.period_end
        metrics: dict[str, dict[str, object]] = {}
        metrics["latest_close"] = self._value(
            analysis.latest_close,
            unit="TWD",
            as_of_date=as_of_date,
            unavailable_status=value_status,
            available_when=value_status == "available",
        )
        metrics["latest_volume"] = self._value(
            analysis.latest_volume,
            unit="shares",
            as_of_date=as_of_date,
            unavailable_status=value_status,
            available_when=value_status == "available",
        )
        for size in (20, 60, 120):
            window = analysis.window(size)
            prefix = f"{size}d"
            metrics[f"return_{prefix}"] = self._value(
                window.return_pct,
                unit="percentage_points",
                as_of_date=as_of_date,
                unavailable_status=(
                    value_status
                    if value_status != "available"
                    else "insufficient_history"
                ),
                available_when=value_status == "available",
            )
            metrics[f"max_drawdown_{prefix}"] = self._value(
                window.max_drawdown_pct,
                unit="percentage_points",
                as_of_date=as_of_date,
                unavailable_status=(
                    value_status
                    if value_status != "available"
                    else "insufficient_history"
                ),
                available_when=value_status == "available",
            )
            metrics[f"ma_distance_{prefix}"] = self._value(
                window.distance_to_moving_average_pct,
                unit="percentage_points",
                as_of_date=as_of_date,
                unavailable_status=(
                    value_status
                    if value_status != "available"
                    else "insufficient_history"
                ),
                available_when=value_status == "available",
            )
        volatility_available = (
            value_status == "available"
            and analysis.volatility_observations >= self.analyzer.volatility_window
        )
        metrics["volatility_60d"] = self._value(
            analysis.daily_return_volatility_pct,
            unit="percentage_points",
            as_of_date=as_of_date,
            unavailable_status=(
                value_status if value_status != "available" else "insufficient_history"
            ),
            available_when=volatility_available,
        )
        metrics["volume_ratio_20d"] = self._value(
            analysis.volume_ratio_to_20d,
            unit="ratio",
            as_of_date=as_of_date,
            unavailable_status=(
                value_status
                if value_status != "available"
                else "insufficient_history"
            ),
            available_when=value_status == "available",
        )
        volume_state = (
            None if analysis.volume_state == "insufficient" else analysis.volume_state
        )
        metrics["volume_anomaly"] = self._value(
            volume_state,
            unit="state",
            as_of_date=as_of_date,
            unavailable_status=(
                value_status
                if value_status != "available"
                else "insufficient_history"
            ),
            available_when=value_status == "available",
        )
        return metrics

    @staticmethod
    def _valuation(
        metrics: Sequence[CompanyMetric], *, market_date: date
    ) -> dict[str, dict[str, object]]:
        names = {
            "pe_ratio": "price_earnings_ratio",
            "pb_ratio": "price_to_book_ratio",
            "dividend_yield": "dividend_yield_pct",
        }
        result: dict[str, dict[str, object]] = {}
        for key, source_name in names.items():
            candidates = [
                item
                for item in metrics
                if item.name == source_name
            ]
            metric = _AS_OF_POLICY.select_valuation_as_of(
                candidates,
                market_date,
            )
            if metric is None:
                result[key] = MetricValue(
                    "missing_source", None, _valuation_unit(key), None
                ).as_dict()
            else:
                result[key] = MetricValue(
                    "available",
                    metric.value,
                    metric.unit or _valuation_unit(key),
                    metric.metric_date,
                ).as_dict()
        return result

    def _data_quality(
        self,
        *,
        market_date: date,
        prices: Sequence[object],
        validation_run: object | None,
        discrepancies: Sequence[object],
    ) -> dict[str, object]:
        price_status = self._price_status(prices, market_date)
        validation_status = "missing_source"
        validation_run_id = None
        validation_outcome = None
        if validation_run is not None:
            validation_run_id = validation_run.run_id
            validation_outcome = (
                None
                if validation_run.outcome is None
                else validation_run.outcome.value
            )
            if any(item.field == "market_date" for item in discrepancies):
                validation_status = "market_date_mismatch"
            elif validation_run.outcome is CrossValidationOutcome.DISCREPANCY:
                validation_status = "source_discrepancy"
            else:
                validation_status = "available"
        quality_status = (
            "clean"
            if price_status == "available" and validation_status == "available"
            else ("unavailable" if price_status == "missing_source" else "warning")
        )
        return {
            "status": quality_status,
            "price_status": price_status,
            "validation_status": validation_status,
            "validation_run_id": validation_run_id,
            "validation_outcome": validation_outcome,
            "latest_price_date": (
                None
                if not prices
                else max(item.trade_date for item in prices).isoformat()
            ),
            "discrepancy_count": len(discrepancies),
            "discrepancies": [
                {
                    "field": item.field,
                    "reason": item.reason,
                    "left_value": item.left_value,
                    "right_value": item.right_value,
                }
                for item in sorted(
                    discrepancies,
                    key=lambda item: (item.field, item.reason),
                )
            ],
        }

    def _provenance(
        self,
        research_reader: _ResearchReadAdapter,
        validation_reader: _ValidationReadAdapter,
        symbol: str,
        market_date: date,
        *,
        prices: Sequence[object],
        batch_run_id: str | None,
        symbol_run_id: str | None,
        pipeline_run_id: str | None,
        validation_run_id: str | None,
        historical_run_id: str | None,
        validation_run: object | None,
    ) -> dict[str, object]:
        pipeline = (
            research_reader.get_pipeline_run(pipeline_run_id)
            if pipeline_run_id is not None
            else research_reader.get_pipeline_run_for_target(symbol, market_date)
        )
        endpoints = set()
        fetched_at: str | None = None
        if pipeline is not None:
            endpoints.update(pipeline.source_endpoints)
            if pipeline.fetched_at is not None:
                fetched_at = pipeline.fetched_at.isoformat()
        price_sources = sorted({item.source for item in prices})
        validation_provenance: dict[str, object] = {}
        if validation_run is not None:
            validation_provenance = {
                "run_id": validation_run.run_id,
                "left_provider": validation_run.left_provider,
                "right_provider": validation_run.right_provider,
            }
            observations = validation_reader.list_observations(
                validation_run.run_id
            )
            validation_provenance["observations"] = [
                {
                    "provider": item.provider,
                    "market_date": item.market_date.isoformat(),
                    "source_endpoints": list(item.source_endpoints),
                    "fetched_at": item.fetched_at.isoformat(),
                    "source_timestamp": (
                        None
                        if item.source_timestamp is None
                        else item.source_timestamp.isoformat()
                    ),
                }
                for item in observations
            ]
            for item in observations:
                endpoints.update(item.source_endpoints)
        return {
            "target_market_date": market_date.isoformat(),
            "batch_run_id": batch_run_id,
            "symbol_run_id": symbol_run_id,
            "pipeline_run_id": (
                None if pipeline is None else pipeline.run_id
            ),
            "historical_run_id": historical_run_id,
            "validation_run_id": validation_run_id,
            "price_sources": price_sources,
            "source_endpoints": sorted(endpoints),
            "fetched_at": fetched_at,
            "validation": validation_provenance,
        }

    def _changes(
        self,
        previous_payload: Mapping[str, object],
        current_quality: Mapping[str, object],
        *,
        payload_metrics: Mapping[str, object] | None,
        payload_valuation: Mapping[str, object] | None = None,
    ) -> list[dict[str, object]]:
        if payload_metrics is None:
            return []
        changes: list[dict[str, object]] = []
        previous_quality = previous_payload.get("data_quality", {})
        if isinstance(previous_quality, Mapping):
            if previous_quality.get("status") != current_quality.get("status"):
                changes.append(
                    {
                        "kind": "data_quality_status_changed",
                        "field": "data_quality.status",
                        "previous": previous_quality.get("status"),
                        "current": current_quality.get("status"),
                    }
                )
            previous_discrepancies = _discrepancy_signatures(previous_quality)
            current_discrepancies = _discrepancy_signatures(current_quality)
            for signature in sorted(current_discrepancies - previous_discrepancies):
                changes.append(
                    {
                        "kind": "discrepancy_added",
                        "field": signature[0],
                        "reason": signature[1],
                    }
                )
            for signature in sorted(previous_discrepancies - current_discrepancies):
                changes.append(
                    {
                        "kind": "discrepancy_resolved",
                        "field": signature[0],
                        "reason": signature[1],
                    }
                )
            if previous_quality.get("validation_status") != current_quality.get(
                "validation_status"
            ):
                changes.append(
                    {
                        "kind": "data_quality_status_changed",
                        "field": "data_quality.validation_status",
                        "previous": previous_quality.get("validation_status"),
                        "current": current_quality.get("validation_status"),
                    }
                )

        previous_metrics = previous_payload.get("metrics", {})
        if isinstance(previous_metrics, Mapping):
            for field in CHANGE_METRIC_ORDER:
                previous = previous_metrics.get(field)
                current = payload_metrics.get(field)
                change = self._metric_change(field, previous, current)
                if change is not None:
                    changes.append(change)

        previous_valuation = previous_payload.get("valuation", {})
        if isinstance(previous_valuation, Mapping) and payload_valuation is not None:
            for field in VALUATION_ORDER:
                previous = previous_valuation.get(field)
                current = payload_valuation.get(field)
                change = self._valuation_change(field, previous, current)
                if change is not None:
                    changes.append(change)
        return changes

    def _metric_change(
        self,
        field: str,
        previous: object,
        current: object,
    ) -> dict[str, object] | None:
        if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
            return None
        previous_status = previous.get("status")
        current_status = current.get("status")
        if previous_status != current_status:
            return {
                "kind": "availability_changed",
                "field": field,
                "previous_status": previous_status,
                "current_status": current_status,
            }
        if field == "volume_anomaly":
            if (
                current_status == "available"
                and previous.get("value") != current.get("value")
            ):
                return {
                    "kind": "volume_anomaly_state_changed",
                    "field": field,
                    "previous": previous.get("value"),
                    "current": current.get("value"),
                }
            return None
        if current_status != "available":
            return None
        previous_value = previous.get("value")
        current_value = current.get("value")
        if not isinstance(previous_value, (int, float)) or not isinstance(
            current_value, (int, float)
        ):
            return None
        delta = float(current_value) - float(previous_value)
        if abs(delta) < self.thresholds.for_field(field):
            return None
        return {
            "kind": "metric_changed",
            "field": field,
            "previous": previous_value,
            "current": current_value,
            "delta": delta,
            "unit": current.get("unit"),
        }

    @staticmethod
    def _valuation_change(
        field: str, previous: object, current: object
    ) -> dict[str, object] | None:
        if not isinstance(previous, Mapping) or not isinstance(current, Mapping):
            return None
        if previous.get("status") != current.get("status"):
            return {
                "kind": "availability_changed",
                "field": field,
                "previous_status": previous.get("status"),
                "current_status": current.get("status"),
            }
        if current.get("status") != "available":
            return None
        if (
            previous.get("value") != current.get("value")
            or previous.get("as_of_date") != current.get("as_of_date")
        ):
            return {
                "kind": "valuation_updated",
                "field": field,
                "previous": previous.get("value"),
                "current": current.get("value"),
                "previous_as_of_date": previous.get("as_of_date"),
                "current_as_of_date": current.get("as_of_date"),
                "unit": current.get("unit"),
            }
        return None

    def _now(self) -> datetime:
        now = self.clock()
        if now.utcoffset() is None:
            raise ValueError("daily report clock must be timezone-aware")
        return now

    @staticmethod
    def _safe_error_message(error: Exception) -> str:
        detail = str(error).replace("\r", " ").replace("\n", " ").strip()
        detail = re.sub(
            r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*[^\s,;]+",
            r"\1=[REDACTED]",
            detail,
        )
        return type(error).__name__ if not detail else f"{type(error).__name__}: {detail}"[:1000]


def validate_canonical_payload(payload: Mapping[str, object]) -> None:
    """Validate the stable JSON contract without external schema dependencies."""
    required = {
        "schema_version",
        "result_id",
        "symbol",
        "requested_date",
        "market_date",
        "methodology_version",
        "methodology",
        "provenance",
        "data_quality",
        "metrics",
        "valuation",
        "comparison",
        "highlights",
    }
    missing = required - set(payload)
    if missing:
        raise CanonicalSchemaError(
            "canonical payload missing fields: " + ", ".join(sorted(missing))
        )
    if payload["schema_version"] != CANONICAL_SCHEMA_VERSION:
        raise CanonicalSchemaError("unsupported canonical schema_version")
    if not isinstance(payload["symbol"], str) or not payload["symbol"].strip():
        raise CanonicalSchemaError("canonical symbol must be non-empty")
    if not isinstance(payload["metrics"], Mapping):
        raise CanonicalSchemaError("canonical metrics must be an object")
    if not isinstance(payload["valuation"], Mapping):
        raise CanonicalSchemaError("canonical valuation must be an object")
    for field, value in list(payload["metrics"].items()) + list(
        payload["valuation"].items()
    ):
        _validate_metric_value(field, value)
    quality = payload["data_quality"]
    if not isinstance(quality, Mapping) or quality.get("status") not in DATA_QUALITY_STATUSES:
        raise CanonicalSchemaError("invalid data_quality.status")
    comparison = payload["comparison"]
    if not isinstance(comparison, Mapping):
        raise CanonicalSchemaError("canonical comparison must be an object")
    if comparison.get("status") not in (
        "available",
        "previous_result_missing",
        "methodology_incompatible",
    ):
        raise CanonicalSchemaError("invalid comparison.status")
    if not isinstance(payload["highlights"], list):
        raise CanonicalSchemaError("canonical highlights must be an array")
    try:
        json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise CanonicalSchemaError("canonical payload is not strict JSON") from error


def _validate_metric_value(field: object, value: object) -> None:
    if not isinstance(field, str) or not isinstance(value, Mapping):
        raise CanonicalSchemaError(f"metric {field!r} must be an object")
    if set(value) != {"status", "value", "unit", "as_of_date"}:
        raise CanonicalSchemaError(
            f"metric {field} must contain status/value/unit/as_of_date only"
        )
    status = value["status"]
    if status not in VALUE_STATUSES:
        raise CanonicalSchemaError(f"metric {field} has invalid status {status!r}")
    if not isinstance(value["unit"], str) or not value["unit"]:
        raise CanonicalSchemaError(f"metric {field} unit must be non-empty")
    if status == "available":
        if value["value"] is None or not isinstance(value["as_of_date"], str):
            raise CanonicalSchemaError(
                f"available metric {field} must have value and as_of_date"
            )
    elif value["value"] is not None or value["as_of_date"] is not None:
        raise CanonicalSchemaError(
            f"unavailable metric {field} must use null value/as_of_date"
        )


def render_daily_research_markdown(payload: Mapping[str, object]) -> str:
    """Render a compact, change-only human-readable report."""
    validate_canonical_payload(payload)
    symbol = payload["symbol"]
    market_date = payload["market_date"]
    quality = payload["data_quality"]
    comparison = payload["comparison"]
    assert isinstance(quality, Mapping)
    assert isinstance(comparison, Mapping)
    lines = [
        f"# {symbol} Daily Research Report",
        "",
        "## 執行與資料品質",
        "",
        f"- 市場日：{market_date}",
        f"- 方法版本：{payload['methodology_version']}",
        f"- 資料品質：{quality['status']}",
        f"- 價格資料：{quality['price_status']}",
        f"- 雙來源驗證：{quality['validation_status']}",
        "",
        "## 今日重要變化",
        "",
    ]
    changes = comparison.get("changes", [])
    if not isinstance(changes, list) or not changes:
        lines.append("- 無達到 deterministic threshold 的變化。")
    else:
        for change in changes:
            lines.append(f"- {_format_change(change)}")
    lines.extend(["", "## Discrepancy / missing-data 警告", ""])
    discrepancies = quality.get("discrepancies", [])
    if isinstance(discrepancies, list) and discrepancies:
        for item in discrepancies:
            if isinstance(item, Mapping):
                lines.append(
                    f"- {item.get('field')}：{item.get('reason')}"
                )
    else:
        missing = _missing_fields(payload)
        if missing:
            lines.append("- unavailable： " + "、".join(missing))
        else:
            lines.append("- 無 discrepancy 或 missing-data 警告。")
    lines.extend(["", "## 精簡目前狀態", ""])
    metrics = payload["metrics"]
    valuation = payload["valuation"]
    assert isinstance(metrics, Mapping)
    assert isinstance(valuation, Mapping)
    for field in (
        "return_20d",
        "return_60d",
        "return_120d",
        "volatility_60d",
        "max_drawdown_20d",
        "max_drawdown_60d",
        "max_drawdown_120d",
        "volume_anomaly",
        "ma_distance_20d",
        "ma_distance_60d",
        "ma_distance_120d",
    ):
        lines.append(f"- {field}：{_format_value(metrics[field])}")
    for field in VALUATION_ORDER:
        lines.append(f"- {field}：{_format_value(valuation[field])}")
    lines.extend(
        [
            "",
            "本報告只呈現目前資料狀態與相對上一交易日的 deterministic 變化，"
            "不輸出完整歷史序列。",
        ]
    )
    markdown = "\n".join(lines)
    validate_rendered_markdown(markdown)
    return markdown


def validate_rendered_markdown(markdown: str) -> None:
    if not markdown.strip():
        raise CanonicalSchemaError("rendered Markdown must not be empty")
    if any(term in markdown for term in TRADE_LANGUAGE):
        raise CanonicalSchemaError("rendered Markdown contains prohibited language")
    required_headings = (
        "## 執行與資料品質",
        "## 今日重要變化",
        "## Discrepancy / missing-data 警告",
        "## 精簡目前狀態",
    )
    if any(heading not in markdown for heading in required_headings):
        raise CanonicalSchemaError("rendered Markdown is missing required sections")


def _stable_result_id(symbol: str, market_date: date, methodology: str) -> str:
    key = f"{symbol.strip().upper()}|{market_date.isoformat()}|{methodology}"
    return "daily-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _metric_unit(field: str) -> str:
    if field == "latest_close":
        return "TWD"
    if field == "latest_volume":
        return "shares"
    if field == "volume_ratio_20d":
        return "ratio"
    if field == "volume_anomaly":
        return "state"
    return "percentage_points"


def _valuation_unit(field: str) -> str:
    if field == "dividend_yield":
        return "percentage_points"
    if field in {"pe_ratio", "pb_ratio"}:
        return "ratio"
    return "unknown"


def _discrepancy_signatures(quality: Mapping[str, object]) -> set[tuple[str, str]]:
    values = quality.get("discrepancies", [])
    if not isinstance(values, list):
        return set()
    result: set[tuple[str, str]] = set()
    for item in values:
        if isinstance(item, Mapping):
            result.add((str(item.get("field")), str(item.get("reason"))))
    return result


def _format_value(value: object) -> str:
    if not isinstance(value, Mapping):
        return "unavailable"
    status = value.get("status")
    if status != "available":
        return f"unavailable ({status})"
    raw = value.get("value")
    unit = value.get("unit")
    as_of = value.get("as_of_date")
    if isinstance(raw, float):
        formatted = f"{raw:.2f}"
    else:
        formatted = str(raw)
    suffix = {
        "percentage_points": "%",
        "ratio": "x",
        "TWD": " TWD",
        "shares": " shares",
        "state": "",
    }.get(str(unit), f" {unit}")
    return f"{formatted}{suffix}（as-of {as_of}）"


def _format_change(change: object) -> str:
    if not isinstance(change, Mapping):
        return str(change)
    kind = change.get("kind")
    field = change.get("field")
    if kind == "metric_changed":
        return (
            f"{field}：{change.get('previous')} → {change.get('current')} "
            f"(Δ {float(change.get('delta', 0.0)):.2f} {change.get('unit')})"
        )
    if kind == "volume_anomaly_state_changed":
        return f"volume_anomaly：{change.get('previous')} → {change.get('current')}"
    if kind == "valuation_updated":
        return (
            f"{field} 更新：{change.get('previous')} → {change.get('current')} "
            f"({change.get('previous_as_of_date')} → {change.get('current_as_of_date')})"
        )
    if kind == "availability_changed":
        return (
            f"{field} availability：{change.get('previous_status')} "
            f"→ {change.get('current_status')}"
        )
    if kind == "discrepancy_added":
        return f"新增 discrepancy：{field}（{change.get('reason')}）"
    if kind == "discrepancy_resolved":
        return f"discrepancy resolved：{field}（{change.get('reason')}）"
    if kind == "data_quality_status_changed":
        return (
            f"{field}：{change.get('previous')} → {change.get('current')}"
        )
    return f"{field}：{kind}"


def _missing_fields(payload: Mapping[str, object]) -> list[str]:
    missing: list[str] = []
    for section_name in ("metrics", "valuation"):
        section = payload.get(section_name, {})
        if not isinstance(section, Mapping):
            continue
        for field, value in section.items():
            if isinstance(value, Mapping) and value.get("status") != "available":
                missing.append(f"{field}={value.get('status')}")
    return missing
