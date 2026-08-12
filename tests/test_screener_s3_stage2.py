"""Screener S3 pure Stage 2 candidate research and boundary gates."""

from __future__ import annotations

import ast
import hashlib
import json
import socket
import sqlite3
import urllib.request
from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.screener.stage2 as stage2_module
import app.screener.stage2_dataset as stage2_dataset_module
from app.analysis import HistoricalResearchAnalyzer
from app.research_dataset import (
    DatasetArtifactRef,
    DatasetAsOf,
    DatasetDiscrepancy,
    DatasetPrice,
    DatasetProvenance,
    DatasetSymbol,
    DatasetValuationMetric,
    FakeResearchDataset,
    PriceHistoryReadModel,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
    ValidationReadModel,
    ValuationReadModel,
)
from app.screener.stage1 import (
    PRICE_CHANGE_1D,
    STAGE1_METHODOLOGY_VERSION,
    VALUATION_UPDATE,
    DataQualityStatus,
    MetricStatus,
    RuleRole,
    Stage1Candidate,
    Stage1DataQuality,
    Stage1Metric,
    Stage1Reason,
    Stage1ScanResult,
)
from app.screener.stage2 import (
    MA_DISTANCE_20D,
    MA_DISTANCE_60D,
    MA_DISTANCE_120D,
    MA_DISTANCE_250D,
    MAX_DRAWDOWN_20D,
    MAX_DRAWDOWN_60D,
    MAX_DRAWDOWN_120D,
    MAX_DRAWDOWN_250D,
    RETURN_20D,
    RETURN_60D,
    RETURN_120D,
    STAGE2_METHODOLOGY_V1,
    STAGE2_METHODOLOGY_VERSION,
    VALUATION_PE,
    VALUATION_PB,
    VALUATION_YIELD,
    VOLATILITY_60D,
    VOLUME_RATIO_20D,
    Stage2AnalysisStatus,
    Stage2CandidateKind,
    Stage2MetricStatus,
    Stage2QualityStatus,
    finalize_stage2_result,
    research_stage2_candidate,
)
from app.screener.stage2_dataset import research_stage2_from_dataset


UTC = timezone.utc
MARKET_DATE = date(2026, 8, 7)
FROZEN_STAGE2_SHA256 = (
    "193015c82d8af4753194eccb425b1ba98d8ad10e8827f3a5251cbac2be30840c"
)
FORBIDDEN_OUTPUT_TERMS = (
    "buy",
    "sell",
    "entry",
    "exit",
    "recommendation",
    "expected_return",
    "price_target",
    "outperform",
    "underperform",
)


def _stage1_price_reason(code: str = "price_change_1d_threshold") -> Stage1Reason:
    return Stage1Reason(
        code=code,
        metric=PRICE_CHANGE_1D,
        component=None,
        previous=0.0,
        current=3.0,
        delta=3.0,
        unit="percent",
        operator="abs_current_gte",
        threshold=3.0,
        rule_version=STAGE1_METHODOLOGY_VERSION,
        role=RuleRole.PRIMARY.value,
        trigger_class="market_move",
        threshold_multiple=1.0,
    )


def _stage1_price_metric() -> Stage1Metric:
    return Stage1Metric(
        metric=PRICE_CHANGE_1D,
        component=None,
        status=MetricStatus.AVAILABLE,
        previous=0.0,
        current=3.0,
        delta=3.0,
        value_unit="percent",
        delta_unit="percentage_point",
        previous_as_of_date=MARKET_DATE - timedelta(days=2),
        current_as_of_date=MARKET_DATE,
    )


def _stage1_candidate(
    symbol: str = "2330",
    *,
    rank: int = 1,
    trigger_count: int = 1,
    valuation_transition: bool = False,
) -> Stage1Candidate:
    reasons = tuple(
        _stage1_price_reason(
            "price_change_1d_threshold" if index == 0 else f"research_trigger_{index}"
        )
        for index in range(trigger_count)
    )
    metrics: tuple[Stage1Metric, ...] = (_stage1_price_metric(),)
    if valuation_transition:
        reasons += (
            Stage1Reason(
                code="valuation_pe_relative_change",
                metric=VALUATION_UPDATE,
                component="pe_ratio",
                previous=10.0,
                current=11.0,
                delta=10.0,
                unit="percent_relative_change",
                operator="abs_delta_gte",
                threshold=10.0,
                rule_version=STAGE1_METHODOLOGY_VERSION,
                role=RuleRole.PRIMARY.value,
                trigger_class="valuation_change",
                threshold_multiple=1.0,
            ),
        )
        metrics += (
            Stage1Metric(
                metric=VALUATION_UPDATE,
                component="pe_ratio",
                status=MetricStatus.AVAILABLE,
                previous=10.0,
                current=11.0,
                delta=10.0,
                value_unit="ratio",
                delta_unit="percent_relative_change",
                previous_as_of_date=MARKET_DATE - timedelta(days=2),
                current_as_of_date=MARKET_DATE,
            ),
        )
    return Stage1Candidate(
        symbol=symbol,
        name=f"公司{symbol}",
        rank=rank,
        reasons=reasons,
        metrics=metrics,
        data_quality=Stage1DataQuality(
            status=DataQualityStatus.CLEAN,
            issues=(),
        ),
    )


def _stage1_result(*candidates: Stage1Candidate) -> Stage1ScanResult:
    ordered = tuple(sorted(candidates, key=lambda item: item.rank))
    return Stage1ScanResult(
        market_date=MARKET_DATE,
        methodology_version=STAGE1_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=1_095,
        screened_count=1_082,
        triggered_count=len(ordered),
        candidate_count=len(ordered),
        candidate_limit=max(30, len(ordered)),
        truncated=False,
        candidates=ordered,
    )


def _dates(count: int, *, end: date = MARKET_DATE) -> tuple[date, ...]:
    return tuple(
        end - timedelta(days=2 * (count - index - 1)) for index in range(count)
    )


def _validation(symbol: str, status: str) -> ValidationReadModel:
    if status == "missing_source":
        return ValidationReadModel(symbol=symbol)
    discrepancy = None
    outcome = "match"
    if status == "source_discrepancy":
        outcome = "discrepancy"
        discrepancy = DatasetDiscrepancy(
            field="close",
            left_value="100",
            right_value="101",
            reason="source values differ",
        )
    elif status == "market_date_mismatch":
        outcome = "discrepancy"
        discrepancy = DatasetDiscrepancy(
            field="market_date",
            left_value="2026-08-07",
            right_value="2026-08-06",
            reason="source dates differ",
        )
    return ValidationReadModel(
        symbol=symbol,
        status=status,
        run_id=f"validation-{symbol}",
        target_date=MARKET_DATE,
        left_provider="twse",
        right_provider="esun",
        outcome=outcome,
        created_at=datetime(2026, 8, 7, 10, tzinfo=UTC),
        discrepancies=(() if discrepancy is None else (discrepancy,)),
    )


def _dataset_snapshot(
    symbol: str = "2330",
    *,
    count: int = 250,
    closes: tuple[float, ...] | None = None,
    volumes: tuple[int, ...] | None = None,
    price_status: str = "available",
    valuation_values: tuple[float | None, float | None, float | None] = (
        15.0,
        2.0,
        3.0,
    ),
    validation_status: str = "missing_source",
) -> ResearchDatasetSnapshot:
    if price_status == "missing_source":
        prices: tuple[DatasetPrice, ...] = ()
    else:
        if closes is None:
            closes = (100.0,) * count
        assert len(closes) == count
        if volumes is None:
            volumes = (1_000,) * count
        assert len(volumes) == count
        end = MARKET_DATE if price_status == "available" else MARKET_DATE - timedelta(days=2)
        prices = tuple(
            DatasetPrice(
                symbol=symbol,
                trade_date=trade_date,
                open=close,
                high=close,
                low=close,
                close=close,
                volume=volume,
                source="twse",
            )
            for trade_date, close, volume in zip(
                _dates(count, end=end),
                closes,
                volumes,
                strict=True,
            )
        )
    valuation_rows = tuple(
        DatasetValuationMetric(
            symbol=symbol,
            metric_date=MARKET_DATE,
            name=name,
            value=value,
            unit=unit,
            source="twse",
        )
        for name, value, unit in (
            ("dividend_yield_pct", valuation_values[2], "%"),
            ("price_earnings_ratio", valuation_values[0], "ratio"),
            ("price_to_book_ratio", valuation_values[1], "ratio"),
        )
        if value is not None
    )
    validation = _validation(symbol, validation_status)
    canonical_sources = ("twse",) if prices or valuation_rows else ()
    validation_sources = (
        ("esun", "twse") if validation_status != "missing_source" else ()
    )
    artifact = DatasetArtifactRef(
        owner_kind="historical",
        owner_run_id=f"history-{symbol}",
        provider="twse",
        dataset="STOCK_DAY",
        endpoint="https://www.twse.com.tw/exchangeReport/STOCK_DAY",
        contract_version="twse-historical-2026-08-05",
        content_type="application/json",
        payload_sha256=hashlib.sha256(symbol.encode()).hexdigest(),
        payload_size_bytes=count,
        hash_basis="canonical-json-v1",
    )
    return ResearchDatasetSnapshot(
        symbol=DatasetSymbol(
            symbol=symbol,
            name=f"公司{symbol}",
            market="TWSE",
            currency="TWD",
        ),
        as_of=DatasetAsOf(
            as_of_date=MARKET_DATE,
            history_observations=250,
            total_history_observations=len(prices),
            returned_history_observations=len(prices),
            history_is_truncated=False,
        ),
        price_history=PriceHistoryReadModel(
            symbol=symbol,
            as_of_date=MARKET_DATE,
            status=price_status,
            observations=prices,
            current=prices[-1] if price_status == "available" else None,
            total_observations_as_of=len(prices),
            requested_observations=250,
            is_truncated=False,
        ),
        valuation=ValuationReadModel(
            symbol=symbol,
            as_of_date=MARKET_DATE,
            status="available" if valuation_rows else "missing_source",
            metrics=valuation_rows,
        ),
        validation=validation,
        provenance=DatasetProvenance(
            symbol=symbol,
            pipeline_run_id=f"pipeline-{symbol}",
            historical_run_id=f"history-{symbol}",
            validation_run_id=(
                f"validation-{symbol}"
                if validation_status != "missing_source"
                else None
            ),
            canonical_sources=canonical_sources,
            validation_sources=validation_sources,
            artifact_refs=(artifact,),
        ),
    )


def _research(
    candidate: Stage1Candidate | None = None,
    snapshot: ResearchDatasetSnapshot | None = None,
):
    return research_stage2_candidate(
        stage1_candidate=candidate or _stage1_candidate(),
        dataset_snapshot=snapshot or _dataset_snapshot(),
        market_date=MARKET_DATE,
    )


def _metric(candidate, name: str):
    return next(item for item in candidate.metrics if item.name == name)


def _reason_codes(candidate) -> tuple[str, ...]:
    return tuple(item.code for item in candidate.stage2_reasons)


def test_stage2_methodology_is_frozen_and_reuses_analyzer_contract() -> None:
    methodology = STAGE2_METHODOLOGY_V1
    assert methodology.version == STAGE2_METHODOLOGY_VERSION
    assert methodology.history_observations == 250
    assert methodology.windows == (20, 60, 120, 250)
    assert methodology.volatility_window == 60
    assert methodology.analyzer_contract == "HistoricalResearchAnalyzer:frozen-v1"
    assert {rule.code: rule.threshold for rule in methodology.rules} == {
        "source_discrepancy": "source_discrepancy",
        "market_date_mismatch": "market_date_mismatch",
        "drawdown_change": 1.0,
        "volatility_regime_change": 1.0,
        "return_60d_change": 1.0,
        "return_120d_state": 10.0,
        "ma_distance_change": 1.0,
        "volume_anomaly": 2.0,
        "valuation_update_pe": "stage1_transition",
        "valuation_update_pb": "stage1_transition",
        "valuation_update_yield": "stage1_transition",
    }


def test_full_250_history_candidate_emits_deep_metrics_without_series() -> None:
    candidate = _research()
    assert candidate.analysis_status is Stage2AnalysisStatus.AVAILABLE
    assert len(candidate.metrics) == 16
    assert _metric(candidate, RETURN_20D).status is Stage2MetricStatus.AVAILABLE
    assert _metric(candidate, RETURN_60D).status is Stage2MetricStatus.AVAILABLE
    assert _metric(candidate, RETURN_120D).status is Stage2MetricStatus.AVAILABLE
    assert _metric(candidate, MAX_DRAWDOWN_250D).status is (
        Stage2MetricStatus.AVAILABLE
    )
    assert _metric(candidate, MA_DISTANCE_250D).status is Stage2MetricStatus.AVAILABLE
    assert _metric(candidate, MAX_DRAWDOWN_250D).previous_value is None
    assert _metric(candidate, MAX_DRAWDOWN_250D).observations == 250


@pytest.mark.parametrize(
    ("count", "available", "unavailable"),
    [
        (
            20,
            (MAX_DRAWDOWN_20D, MA_DISTANCE_20D),
            (RETURN_20D, RETURN_60D, MAX_DRAWDOWN_60D),
        ),
        (
            60,
            (RETURN_20D, MAX_DRAWDOWN_60D, MA_DISTANCE_60D),
            (RETURN_60D, VOLATILITY_60D, MAX_DRAWDOWN_120D),
        ),
        (
            120,
            (RETURN_60D, VOLATILITY_60D, MAX_DRAWDOWN_120D, MA_DISTANCE_120D),
            (RETURN_120D, MAX_DRAWDOWN_250D, MA_DISTANCE_250D),
        ),
    ],
)
def test_exact_20_60_120_observation_boundaries(
    count: int,
    available: tuple[str, ...],
    unavailable: tuple[str, ...],
) -> None:
    candidate = _research(snapshot=_dataset_snapshot(count=count))
    for name in available:
        assert _metric(candidate, name).status is Stage2MetricStatus.AVAILABLE
    for name in unavailable:
        metric = _metric(candidate, name)
        assert metric.status is Stage2MetricStatus.INSUFFICIENT_HISTORY
        assert metric.value is metric.previous_value is metric.delta is None


def test_less_than_20_and_less_than_250_remain_explicitly_unavailable() -> None:
    candidate = _research(snapshot=_dataset_snapshot(count=19))
    for name in (
        RETURN_20D,
        MAX_DRAWDOWN_20D,
        RETURN_60D,
        RETURN_120D,
        MAX_DRAWDOWN_250D,
        MA_DISTANCE_250D,
    ):
        metric = _metric(candidate, name)
        assert metric.status is Stage2MetricStatus.INSUFFICIENT_HISTORY
        assert metric.value is None


def test_multiple_stage2_reasons_include_deep_risk_trend_volume_and_return() -> None:
    closes = (100.0,) * 249 + (80.0,)
    volumes = (1_000,) * 249 + (3_000,)
    candidate = _research(
        snapshot=_dataset_snapshot(closes=closes, volumes=volumes)
    )
    assert {
        "drawdown_change",
        "volatility_regime_change",
        "return_60d_change",
        "return_120d_state",
        "ma_distance_change",
        "volume_anomaly",
    } <= set(_reason_codes(candidate))
    assert _metric(candidate, MAX_DRAWDOWN_120D).delta is not None
    assert _metric(candidate, MAX_DRAWDOWN_120D).delta <= -1.0


def test_valuation_transition_is_confirmed_from_stage1_and_current_m9_value() -> None:
    stage1 = _stage1_candidate(valuation_transition=True)
    candidate = _research(
        candidate=stage1,
        snapshot=_dataset_snapshot(valuation_values=(11.0, 2.0, 3.0)),
    )
    assert "valuation_update_pe" in _reason_codes(candidate)
    metric = _metric(candidate, VALUATION_PE)
    assert metric.previous_value == 10.0
    assert metric.value == 11.0
    assert metric.delta == 10.0


def test_missing_valuation_and_validation_are_preserved_without_trigger() -> None:
    candidate = _research(
        snapshot=_dataset_snapshot(
            valuation_values=(None, None, None),
            validation_status="missing_source",
        )
    )
    for name in (VALUATION_PE, VALUATION_PB, VALUATION_YIELD):
        metric = _metric(candidate, name)
        assert metric.status is Stage2MetricStatus.VALUATION_UNAVAILABLE
        assert metric.value is None
    assert candidate.data_quality.status is Stage2QualityStatus.WARNING
    assert candidate.data_quality.validation_status == "missing_source"
    assert candidate.candidate_kind is Stage2CandidateKind.RESEARCH_CANDIDATE
    assert candidate.stage2_reasons == ()


def test_source_discrepancy_is_data_quality_reason_not_canonical_metric() -> None:
    clean = _research(snapshot=_dataset_snapshot(validation_status="available"))
    discrepant = _research(
        snapshot=_dataset_snapshot(validation_status="source_discrepancy")
    )
    assert clean.metrics == discrepant.metrics
    assert _reason_codes(discrepant) == ("source_discrepancy",)
    assert discrepant.candidate_kind is Stage2CandidateKind.DATA_QUALITY_CANDIDATE
    assert discrepant.data_quality.validation_status == "source_discrepancy"
    assert discrepant.data_quality.discrepancies[0].field == "close"


def test_market_date_mismatch_blocks_price_metrics_without_zero_fill() -> None:
    candidate = _research(
        snapshot=_dataset_snapshot(
            count=249,
            price_status="market_date_mismatch",
            validation_status="market_date_mismatch",
        )
    )
    assert candidate.analysis_status is Stage2AnalysisStatus.UNAVAILABLE
    assert candidate.candidate_kind is Stage2CandidateKind.DATA_QUALITY_CANDIDATE
    assert "market_date_mismatch" in _reason_codes(candidate)
    for name in (
        RETURN_20D,
        RETURN_60D,
        RETURN_120D,
        VOLATILITY_60D,
        MAX_DRAWDOWN_120D,
        VOLUME_RATIO_20D,
        MA_DISTANCE_60D,
    ):
        metric = _metric(candidate, name)
        assert metric.status is Stage2MetricStatus.MARKET_DATE_MISMATCH
        assert metric.value is None


def test_no_stage2_trigger_still_preserves_stage1_candidate_and_reasons() -> None:
    stage1 = _stage1_candidate()
    candidate = _research(candidate=stage1)
    assert candidate.stage2_reasons == ()
    assert candidate.stage1_reasons == stage1.reasons
    assert candidate.symbol == stage1.symbol


def test_stage2_uses_analyzer_for_current_and_nearest_prior_states(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[int] = []
    delegate = HistoricalResearchAnalyzer()

    class SpyAnalyzer:
        def __init__(self, **unused: object) -> None:
            del unused

        def analyze(self, prices):
            calls.append(len(prices))
            return delegate.analyze(prices)

    monkeypatch.setattr(stage2_module, "HistoricalResearchAnalyzer", SpyAnalyzer)
    candidate = _research()
    assert candidate.analysis_status is Stage2AnalysisStatus.AVAILABLE
    assert calls == [250, 249]
    assert _metric(candidate, RETURN_60D).previous_as_of_date == (
        MARKET_DATE - timedelta(days=2)
    )


def test_ranking_refinement_prioritizes_data_quality_then_research_evidence() -> None:
    clean_stage1 = _stage1_candidate("1000", rank=1)
    quality_stage1 = _stage1_candidate("3000", rank=2)
    multi_stage1 = _stage1_candidate("2000", rank=3, trigger_count=2)
    clean = _research(candidate=clean_stage1, snapshot=_dataset_snapshot("1000"))
    quality = _research(
        candidate=quality_stage1,
        snapshot=_dataset_snapshot("3000", validation_status="source_discrepancy"),
    )
    multi = _research(
        candidate=multi_stage1,
        snapshot=_dataset_snapshot(
            "2000",
            closes=(100.0,) * 249 + (80.0,),
            volumes=(1_000,) * 249 + (3_000,),
        ),
    )
    result = finalize_stage2_result(
        candidates=(clean, quality, multi),
        market_date=MARKET_DATE,
    )
    assert [item.symbol for item in result.candidates] == ["3000", "2000", "1000"]
    assert [item.rank for item in result.candidates] == [1, 2, 3]


def test_symbol_is_stable_tie_break_after_equal_research_priority() -> None:
    first = _research(candidate=_stage1_candidate("2000", rank=1), snapshot=_dataset_snapshot("2000"))
    second = _research(candidate=_stage1_candidate("1000", rank=1), snapshot=_dataset_snapshot("1000"))
    result = finalize_stage2_result(candidates=(first, second), market_date=MARKET_DATE)
    assert [item.symbol for item in result.candidates] == ["1000", "2000"]


class _SelectiveDataset:
    def __init__(self, snapshots: dict[str, ResearchDatasetSnapshot], fail: str) -> None:
        self.snapshots = snapshots
        self.fail = fail
        self.requests: list[ResearchDatasetRequest] = []

    def read(self, request: ResearchDatasetRequest, /) -> ResearchDatasetSnapshot:
        self.requests.append(request)
        if request.symbol == self.fail:
            raise LookupError("synthetic isolated failure")
        return self.snapshots[request.symbol]


def test_candidate_failure_is_isolated_and_stage1_result_is_unchanged() -> None:
    candidates = tuple(
        _stage1_candidate(symbol, rank=index)
        for index, symbol in enumerate(("1000", "2000", "3000"), 1)
    )
    stage1 = _stage1_result(*candidates)
    before = stage1.canonical_json()
    dataset = _SelectiveDataset(
        {symbol: _dataset_snapshot(symbol) for symbol in ("1000", "3000")},
        fail="2000",
    )
    evidence = research_stage2_from_dataset(
        stage1_result=stage1,
        dataset=dataset,
        market_date=MARKET_DATE,
    )
    assert evidence.dataset_reads == 3
    assert evidence.failed_candidates == 1
    assert evidence.result.candidate_count == 3
    failed = next(item for item in evidence.result.candidates if item.symbol == "2000")
    assert failed.analysis_status is Stage2AnalysisStatus.FAILED
    assert failed.candidate_kind is Stage2CandidateKind.DATA_QUALITY_CANDIDATE
    assert failed.failure is not None
    assert all(
        item.status is Stage2MetricStatus.ANALYSIS_FAILED for item in failed.metrics
    )
    assert stage1.canonical_json() == before
    assert [item.symbol for item in dataset.requests] == ["1000", "2000", "3000"]
    assert all(item.history_observations == 250 for item in dataset.requests)


def test_composition_processes_only_stage1_shortlist_not_market_universe() -> None:
    stage1 = _stage1_result(
        _stage1_candidate("1000", rank=1),
        _stage1_candidate("2000", rank=2),
    )
    dataset = _SelectiveDataset(
        {symbol: _dataset_snapshot(symbol) for symbol in ("1000", "2000")},
        fail="never",
    )
    evidence = research_stage2_from_dataset(
        stage1_result=stage1,
        dataset=dataset,
        market_date=MARKET_DATE,
    )
    assert evidence.dataset_reads == stage1.candidate_count == 2
    assert {item.symbol for item in evidence.result.candidates} == {"1000", "2000"}


def test_fake_dataset_excludes_future_rows_and_returns_at_most_250() -> None:
    symbol = "2330"
    eligible_dates = _dates(251)
    future_date = MARKET_DATE + timedelta(days=2)
    prices = tuple(
        DatasetPrice(
            symbol=symbol,
            trade_date=trade_date,
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=1_000,
            source="twse",
        )
        for trade_date in (*eligible_dates, future_date)
    )
    dataset = FakeResearchDataset(
        symbols=(
            DatasetSymbol(symbol=symbol, name="台積電", market="TWSE", currency="TWD"),
        ),
        prices=prices,
        valuations=(
            DatasetValuationMetric(
                symbol=symbol,
                metric_date=MARKET_DATE,
                name="price_earnings_ratio",
                value=15.0,
                unit="ratio",
                source="twse",
            ),
        ),
        provenance=(
            DatasetProvenance(symbol=symbol, canonical_sources=("twse",)),
        ),
    )
    evidence = research_stage2_from_dataset(
        stage1_result=_stage1_result(_stage1_candidate(symbol)),
        dataset=dataset,
        market_date=MARKET_DATE,
    )
    assert evidence.dataset_reads == 1
    assert evidence.observations_consumed == 250
    assert all(
        metric.as_of_date == MARKET_DATE
        for metric in evidence.result.candidates[0].metrics
    )


def test_provenance_and_validation_roles_are_preserved_without_esun_canonical() -> None:
    candidate = _research(
        snapshot=_dataset_snapshot(validation_status="source_discrepancy")
    )
    assert candidate.provenance.pipeline_run_id == "pipeline-2330"
    assert candidate.provenance.historical_run_id == "history-2330"
    assert candidate.provenance.validation_run_id == "validation-2330"
    assert candidate.provenance.canonical_sources == ("twse",)
    assert candidate.provenance.validation_sources == ("esun", "twse")
    assert candidate.provenance.artifact_refs[0].provider == "twse"
    assert all("esun" not in metric.name for metric in candidate.metrics)


def test_deterministic_serialization_hash_and_no_full_time_series() -> None:
    stage1_candidates = (
        _stage1_candidate("3000", rank=3),
        _stage1_candidate("1000", rank=1),
        _stage1_candidate("2000", rank=2),
    )
    drafts = tuple(
        _research(candidate=item, snapshot=_dataset_snapshot(item.symbol))
        for item in stage1_candidates
    )
    first = finalize_stage2_result(candidates=drafts, market_date=MARKET_DATE)
    second = finalize_stage2_result(
        candidates=tuple(reversed(drafts)),
        market_date=MARKET_DATE,
    )
    assert first.candidates == second.candidates
    assert first.canonical_json() == second.canonical_json()
    assert first.payload_sha256 == second.payload_sha256 == FROZEN_STAGE2_SHA256
    payload = first.canonical_json()
    assert "price_history" not in payload
    assert "daily_prices" not in payload
    assert "trade_date" not in payload


def test_result_and_nested_contracts_are_immutable() -> None:
    result = finalize_stage2_result(
        candidates=(_research(),),
        market_date=MARKET_DATE,
    )
    assert isinstance(result.candidates, tuple)
    assert isinstance(result.candidates[0].metrics, tuple)
    assert isinstance(result.candidates[0].stage1_reasons, tuple)
    with pytest.raises(FrozenInstanceError):
        result.candidate_count = 0  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        result.candidates[0].rank = 2  # type: ignore[misc]


def test_pure_stage2_has_no_provider_network_sqlite_or_operations_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def blocked(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise AssertionError("S3 attempted external I/O")

    monkeypatch.setattr(urllib.request, "urlopen", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(sqlite3, "connect", blocked)
    assert _research().analysis_status is Stage2AnalysisStatus.AVAILABLE

    for module in (stage2_module, stage2_dataset_module):
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        assert "statistics" not in imported
        assert not any(
            name.startswith(
                (
                    "sqlite3",
                    "socket",
                    "urllib.request",
                    "app.providers",
                    "app.storage",
                    "app.scheduler",
                    "app.reports",
                )
            )
            for name in imported
        )
    assert "HistoricalResearchAnalyzer" in {
        node.id
        for node in ast.walk(
            ast.parse(Path(stage2_module.__file__).read_text(encoding="utf-8"))
        )
        if isinstance(node, ast.Name)
    }


def test_canonical_output_does_not_emit_forbidden_terminology() -> None:
    payload = finalize_stage2_result(
        candidates=(_research(),),
        market_date=MARKET_DATE,
    ).canonical_json().casefold()
    compact = payload.replace("_", "")
    for term in FORBIDDEN_OUTPUT_TERMS:
        assert term not in payload
        assert term.replace("_", "") not in compact
    assert json.loads(payload)["candidates"][0]["rank"] == 1
