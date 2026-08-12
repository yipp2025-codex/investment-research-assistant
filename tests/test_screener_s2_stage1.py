"""Screener S2 pure Stage 1 metrics, rules, ranking, and boundary gates."""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import socket
import sqlite3
import urllib.request
from dataclasses import FrozenInstanceError
from datetime import date, timedelta
from pathlib import Path

import pytest

import app.screener.stage1 as stage1_module
import app.screener.stage1_dataset as dataset_module
from app.research_dataset import (
    DatasetPrice,
    DatasetProvenance,
    DatasetSymbol,
    DatasetValuationMetric,
    FakeResearchDataset,
    ResearchDatasetRequest,
)
from app.screener.stage1 import (
    DIVIDEND_YIELD,
    MA_DISTANCE_CHANGE,
    PB_RATIO,
    PE_RATIO,
    PRICE_CHANGE_1D,
    RETURN_20D_CHANGE,
    STAGE1_METHODOLOGY_V1,
    STAGE1_METHODOLOGY_VERSION,
    VALUATION_UPDATE,
    VOLATILITY_REGIME_CHANGE,
    VOLUME_ANOMALY,
    DataQualityStatus,
    MetricStatus,
    PriceSnapshotStatus,
    RuleRole,
    Stage1ContractError,
    Stage1PriceObservation,
    Stage1ResearchSnapshot,
    Stage1ValuationState,
    ValuationSnapshotStatus,
    evaluate_stage1_snapshot,
    scan_stage1,
)
from app.screener.stage1_dataset import (
    PreviousValuationSnapshot,
    scan_stage1_from_dataset,
)
from app.screener.universe import (
    UNIVERSE_METHODOLOGY_VERSION,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)


MARKET_DATE = date(2026, 8, 7)
FROZEN_STAGE1_SHA256 = (
    "e301da7567a4223138879bd105bd128ccd46ae9df7c62ac121609711a1dd0952"
)
FORBIDDEN_OUTPUT_TERMS = (
    "buy",
    "sell",
    "recommendation",
    "expected_return",
    "price_target",
    "entry",
    "exit",
)


def _source_evidence() -> tuple[SourceEvidence, ...]:
    payload = b"screener-s2-test-universe"
    return (
        SourceEvidence(
            source="twse",
            dataset="FROZEN_ORDINARY_STOCK_CLASSIFICATION",
            source_ref="contract://screener-s2-tests/universe",
            contract_version="screener-s2-test-v1",
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            payload_size_bytes=len(payload),
            hash_basis="canonical-json-v1",
        ),
    )


def _universe(*symbols: str) -> MarketUniverseSnapshot:
    evidence = _source_evidence()
    members = tuple(
        MarketUniverseMember(
            symbol=symbol,
            name=f"公司{symbol}",
            market="TWSE",
            status=UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
            listing_date=date(2000, 1, 1),
            delisting_date=None,
            exclusion_reason=None,
            source_evidence=evidence,
        )
        for symbol in sorted(symbols)
    )
    return MarketUniverseSnapshot(
        market_date=MARKET_DATE,
        methodology_version=UNIVERSE_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=len(members),
        scan_eligible_count=len(members),
        scan_unavailable_count=0,
        excluded_count=0,
        inactive_count=0,
        unresolved_count=0,
        members=members,
    )


def _market_dates(count: int) -> tuple[date, ...]:
    return tuple(
        MARKET_DATE - timedelta(days=2 * (count - index - 1))
        for index in range(count)
    )


def _snapshot(
    symbol: str = "2330",
    *,
    closes: tuple[float, ...] = (100.0,) * 62,
    volumes: tuple[int, ...] | None = None,
    price_status: PriceSnapshotStatus = PriceSnapshotStatus.AVAILABLE,
    current_values: tuple[float | None, float | None, float | None] = (
        15.0,
        2.0,
        3.0,
    ),
    previous_values: tuple[float | None, float | None, float | None] | None = (
        15.0,
        2.0,
        3.0,
    ),
    valuation_status: ValuationSnapshotStatus = ValuationSnapshotStatus.AVAILABLE,
    previous_valuation_status: ValuationSnapshotStatus = (
        ValuationSnapshotStatus.AVAILABLE
    ),
) -> Stage1ResearchSnapshot:
    if price_status is PriceSnapshotStatus.MISSING_SOURCE:
        dates: tuple[date, ...] = ()
        prices: tuple[Stage1PriceObservation, ...] = ()
    else:
        dates = _market_dates(len(closes))
        if price_status is PriceSnapshotStatus.MARKET_DATE_MISMATCH:
            dates = tuple(item - timedelta(days=2) for item in dates)
        if volumes is None:
            volumes = (1_000,) * len(closes)
        assert len(volumes) == len(closes)
        prices = tuple(
            Stage1PriceObservation(
                trade_date=trade_date,
                close=close,
                volume=volume,
                source="twse",
            )
            for trade_date, close, volume in zip(dates, closes, volumes, strict=True)
        )
    current = Stage1ValuationState(
        as_of_date=MARKET_DATE,
        status=valuation_status,
        pe_ratio=current_values[0],
        pb_ratio=current_values[1],
        dividend_yield_pct=current_values[2],
    )
    previous = None
    if previous_values is not None:
        assert len(prices) >= 2
        previous = Stage1ValuationState(
            as_of_date=prices[-2].trade_date,
            status=previous_valuation_status,
            pe_ratio=previous_values[0],
            pb_ratio=previous_values[1],
            dividend_yield_pct=previous_values[2],
        )
    return Stage1ResearchSnapshot(
        symbol=symbol,
        as_of_date=MARKET_DATE,
        price_status=price_status,
        prices=prices,
        current_valuation=current,
        previous_valuation=previous,
        canonical_sources=("twse",),
    )


def _metric(evaluation, metric: str, component: str | None = None):
    return next(
        item
        for item in evaluation.metrics
        if item.metric == metric and item.component == component
    )


def _reason_codes(evaluation) -> tuple[str, ...]:
    return tuple(item.code for item in evaluation.reasons)


def _scan(
    snapshots: tuple[Stage1ResearchSnapshot, ...],
    *,
    candidate_limit: int = 30,
):
    return scan_stage1(
        universe=_universe(*(item.symbol for item in snapshots)),
        research_snapshots=snapshots,
        as_of_date=MARKET_DATE,
        candidate_limit=candidate_limit,
    )


def test_methodology_v1_centralizes_thresholds_and_explains_62_rows() -> None:
    methodology = STAGE1_METHODOLOGY_V1
    assert methodology.version == STAGE1_METHODOLOGY_VERSION
    assert methodology.history_observations == 62
    assert methodology.volume_window + 2 == 22
    assert methodology.return_window + 2 == 22
    assert methodology.volatility_return_window + 2 == 62
    assert methodology.ma_window + 1 == 21
    thresholds = {rule.code: rule.threshold for rule in methodology.rules}
    assert thresholds == {
        "price_change_1d_threshold": 3.0,
        "volume_anomaly_high": 2.0,
        "volume_anomaly_low_context": 0.5,
        "return_20d_delta_threshold": 1.0,
        "volatility_60d_delta_threshold": 1.0,
        "ma20_distance_delta_threshold": 1.0,
        "valuation_pe_availability_transition": "availability_transition",
        "valuation_pe_relative_change": 10.0,
        "valuation_pb_availability_transition": "availability_transition",
        "valuation_pb_relative_change": 10.0,
        "valuation_yield_availability_transition": "availability_transition",
        "valuation_yield_delta_threshold": 0.5,
    }


def test_no_trigger_fixture() -> None:
    evaluation = evaluate_stage1_snapshot(_snapshot())
    assert evaluation.triggered is False
    assert evaluation.reasons == ()
    assert evaluation.data_quality.status is DataQualityStatus.CLEAN


def test_single_price_change_trigger_and_exact_threshold_boundary() -> None:
    evaluation = evaluate_stage1_snapshot(
        _snapshot(closes=(100.0, 100.0, 103.0))
    )
    metric = _metric(evaluation, PRICE_CHANGE_1D)
    assert metric.current == 3.0
    assert _reason_codes(evaluation) == ("price_change_1d_threshold",)
    assert evaluation.triggered is True


def test_price_change_just_below_threshold_does_not_trigger() -> None:
    evaluation = evaluate_stage1_snapshot(
        _snapshot(closes=(100.0, 100.0, 102.999))
    )
    assert _metric(evaluation, PRICE_CHANGE_1D).current == 2.999
    assert evaluation.triggered is False


def test_volume_anomaly_trigger_uses_prior_twenty_observations() -> None:
    volumes = (1_000,) * 21 + (2_000,)
    evaluation = evaluate_stage1_snapshot(
        _snapshot(closes=(100.0,) * 22, volumes=volumes)
    )
    metric = _metric(evaluation, VOLUME_ANOMALY)
    assert metric.previous == 1.0
    assert metric.current == 2.0
    assert _reason_codes(evaluation) == ("volume_anomaly_high",)


def test_low_volume_is_secondary_context_and_cannot_create_candidate_alone() -> None:
    volumes = (1_000,) * 21 + (500,)
    snapshot = _snapshot(closes=(100.0,) * 22, volumes=volumes)
    evaluation = evaluate_stage1_snapshot(snapshot)
    assert _reason_codes(evaluation) == ("volume_anomaly_low_context",)
    assert evaluation.reasons[0].role == RuleRole.SECONDARY.value
    assert evaluation.triggered is False
    assert _scan((snapshot,)).candidate_count == 0


def test_return_20d_transition_exact_one_percentage_point_boundary() -> None:
    closes = (100.0,) * 21 + (101.0,)
    evaluation = evaluate_stage1_snapshot(_snapshot(closes=closes))
    metric = _metric(evaluation, RETURN_20D_CHANGE)
    assert metric.previous == 0.0
    assert metric.current == 1.0
    assert metric.delta == 1.0
    assert _reason_codes(evaluation) == ("return_20d_delta_threshold",)


def test_volatility_regime_transition_uses_two_60_return_windows() -> None:
    closes = (100.0,) + (110.0,) * 61
    evaluation = evaluate_stage1_snapshot(_snapshot(closes=closes))
    metric = _metric(evaluation, VOLATILITY_REGIME_CHANGE)
    assert metric.previous is not None and metric.previous > 1.0
    assert metric.current == 0.0
    assert metric.delta is not None and metric.delta < -1.0
    assert _reason_codes(evaluation) == ("volatility_60d_delta_threshold",)


def test_ma20_distance_transition_can_trigger_without_deep_windows() -> None:
    closes = (100.0,) * 20 + (101.1,)
    evaluation = evaluate_stage1_snapshot(_snapshot(closes=closes))
    metric = _metric(evaluation, MA_DISTANCE_CHANGE)
    assert metric.delta is not None and metric.delta >= 1.0
    assert _reason_codes(evaluation) == ("ma20_distance_delta_threshold",)


@pytest.mark.parametrize(
    ("component", "previous_values", "current_values", "expected_code"),
    [
        (
            PE_RATIO,
            (None, 2.0, 3.0),
            (15.0, 2.0, 3.0),
            "valuation_pe_availability_transition",
        ),
        (
            PE_RATIO,
            (10.0, 2.0, 3.0),
            (11.0, 2.0, 3.0),
            "valuation_pe_relative_change",
        ),
        (
            PB_RATIO,
            (15.0, 2.0, 3.0),
            (15.0, 2.2, 3.0),
            "valuation_pb_relative_change",
        ),
        (
            DIVIDEND_YIELD,
            (15.0, 2.0, 3.0),
            (15.0, 2.0, 3.5),
            "valuation_yield_delta_threshold",
        ),
    ],
)
def test_valuation_update_transitions_and_exact_thresholds(
    component: str,
    previous_values: tuple[float | None, float | None, float | None],
    current_values: tuple[float | None, float | None, float | None],
    expected_code: str,
) -> None:
    evaluation = evaluate_stage1_snapshot(
        _snapshot(
            closes=(100.0, 100.0, 100.0),
            previous_values=previous_values,
            current_values=current_values,
        )
    )
    assert _metric(evaluation, VALUATION_UPDATE, component).status is (
        MetricStatus.AVAILABLE
    )
    assert _reason_codes(evaluation) == (expected_code,)


def test_multiple_triggers_keep_fixed_reason_order_and_required_explanation() -> None:
    snapshot = _snapshot(
        closes=(100.0,) * 21 + (103.0,),
        volumes=(1_000,) * 21 + (2_000,),
    )
    evaluation = evaluate_stage1_snapshot(snapshot)
    assert len(
        [item for item in evaluation.reasons if item.role == RuleRole.PRIMARY.value]
    ) >= 2
    reason_orders = {
        rule.code: rule.reason_order for rule in STAGE1_METHODOLOGY_V1.rules
    }
    assert tuple(reason_orders[item.code] for item in evaluation.reasons) == tuple(
        sorted(reason_orders[item.code] for item in evaluation.reasons)
    )
    payload = _scan((snapshot,)).as_dict()["candidates"][0]["reasons"]
    assert all(
        {
            "code",
            "metric",
            "previous",
            "current",
            "delta",
            "unit",
            "operator",
            "threshold",
            "rule_version",
        }
        <= set(item)
        for item in payload
    )


def test_insufficient_history_is_explicit_and_never_zero_filled() -> None:
    evaluation = evaluate_stage1_snapshot(
        _snapshot(closes=(100.0, 100.0), previous_values=(15.0, 2.0, 3.0))
    )
    for metric_name in (
        PRICE_CHANGE_1D,
        VOLUME_ANOMALY,
        RETURN_20D_CHANGE,
        VOLATILITY_REGIME_CHANGE,
        MA_DISTANCE_CHANGE,
    ):
        metric = _metric(evaluation, metric_name)
        assert metric.status is MetricStatus.INSUFFICIENT_HISTORY
        assert metric.previous is metric.current is metric.delta is None


def test_missing_source_and_market_date_mismatch_are_explicit() -> None:
    missing = _snapshot(
        closes=(),
        price_status=PriceSnapshotStatus.MISSING_SOURCE,
        current_values=(None, None, None),
        previous_values=None,
        valuation_status=ValuationSnapshotStatus.MISSING_SOURCE,
    )
    missing_eval = evaluate_stage1_snapshot(missing)
    assert {
        item.status for item in missing_eval.metrics if item.metric != VALUATION_UPDATE
    } == {MetricStatus.MISSING_SOURCE}
    assert missing_eval.data_quality.status is DataQualityStatus.UNAVAILABLE
    assert missing_eval.triggered is False

    mismatch = _snapshot(
        closes=(100.0,) * 5,
        price_status=PriceSnapshotStatus.MARKET_DATE_MISMATCH,
    )
    mismatch_eval = evaluate_stage1_snapshot(mismatch)
    assert {
        item.status for item in mismatch_eval.metrics if item.metric != VALUATION_UPDATE
    } == {MetricStatus.MARKET_DATE_MISMATCH}
    assert mismatch_eval.triggered is False


def test_invalid_zero_or_negative_denominators_do_not_trigger() -> None:
    evaluation = evaluate_stage1_snapshot(
        _snapshot(
            closes=(0.0, 0.0, 100.0),
            previous_values=(0.0, -1.0, 3.0),
            current_values=(10.0, 2.0, 3.0),
        )
    )
    assert _metric(evaluation, PRICE_CHANGE_1D).status is (
        MetricStatus.INVALID_DENOMINATOR
    )
    assert _metric(evaluation, VALUATION_UPDATE, PE_RATIO).status is (
        MetricStatus.INVALID_DENOMINATOR
    )
    assert _metric(evaluation, VALUATION_UPDATE, PB_RATIO).status is (
        MetricStatus.INVALID_DENOMINATOR
    )
    assert evaluation.triggered is False


def test_valuation_unavailable_does_not_pass_a_rule() -> None:
    evaluation = evaluate_stage1_snapshot(
        _snapshot(
            current_values=(None, None, None),
            previous_values=None,
            valuation_status=ValuationSnapshotStatus.MISSING_SOURCE,
        )
    )
    valuation_metrics = tuple(
        item for item in evaluation.metrics if item.metric == VALUATION_UPDATE
    )
    assert {item.status for item in valuation_metrics} == {
        MetricStatus.VALUATION_UNAVAILABLE
    }
    assert evaluation.triggered is False


def test_previous_state_uses_nearest_prior_market_observation_not_calendar_minus_one() -> None:
    snapshot = _snapshot(closes=(100.0, 100.0, 103.0))
    assert snapshot.prices[-2].trade_date == MARKET_DATE - timedelta(days=2)
    evaluation = evaluate_stage1_snapshot(snapshot)
    assert _metric(evaluation, PRICE_CHANGE_1D).previous_as_of_date == (
        MARKET_DATE - timedelta(days=2)
    )


def test_ranking_is_trigger_count_then_class_priority_multiple_and_symbol() -> None:
    multi = _snapshot(
        "3000",
        closes=(100.0,) * 21 + (103.0,),
        volumes=(1_000,) * 21 + (2_000,),
    )
    price_a = _snapshot("1000", closes=(100.0, 100.0, 103.0))
    price_b = _snapshot("2000", closes=(100.0, 100.0, 103.0))
    result = _scan((price_b, multi, price_a))
    assert [item.symbol for item in result.candidates] == ["3000", "1000", "2000"]
    assert [item.rank for item in result.candidates] == [1, 2, 3]


def test_candidate_limit_preserves_total_triggered_count_and_truncation() -> None:
    snapshots = tuple(
        _snapshot(symbol, closes=(100.0, 100.0, 103.0))
        for symbol in ("3000", "1000", "2000")
    )
    result = _scan(snapshots, candidate_limit=2)
    assert result.screened_count == 3
    assert result.triggered_count == 3
    assert result.candidate_count == 2
    assert result.candidate_limit == 2
    assert result.truncated is True
    assert [item.symbol for item in result.candidates] == ["1000", "2000"]


def test_input_order_does_not_change_candidates_reasons_rank_or_hash() -> None:
    snapshots = (
        _snapshot("3000", closes=(100.0, 100.0, 104.0)),
        _snapshot("1000", closes=(100.0, 100.0, 103.0)),
        _snapshot("2000", closes=(100.0, 100.0, 103.5)),
    )
    first = _scan(snapshots, candidate_limit=2)
    second = _scan(tuple(reversed(snapshots)), candidate_limit=2)
    assert first.candidates == second.candidates
    assert first.canonical_json() == second.canonical_json()
    assert first.payload_sha256 == second.payload_sha256


def test_frozen_stage1_output_hash() -> None:
    snapshots = (
        _snapshot("3000", closes=(100.0, 100.0, 104.0)),
        _snapshot("1000", closes=(100.0,) * 21 + (101.0,)),
        _snapshot(
            "2000",
            closes=(100.0,) * 22,
            volumes=(1_000,) * 21 + (2_000,),
        ),
    )
    result = _scan(snapshots, candidate_limit=2)
    assert result.payload_sha256 == FROZEN_STAGE1_SHA256
    assert result.payload_sha256 == hashlib.sha256(
        result.canonical_json().encode("utf-8")
    ).hexdigest()


def test_outputs_are_immutable_and_rank_is_research_priority() -> None:
    result = _scan((_snapshot(closes=(100.0, 100.0, 103.0)),))
    assert isinstance(result.candidates, tuple)
    assert isinstance(result.candidates[0].reasons, tuple)
    assert isinstance(result.candidates[0].metrics, tuple)
    with pytest.raises(FrozenInstanceError):
        result.triggered_count = 0  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        result.candidates[0].rank = 2  # type: ignore[misc]


def test_scan_requires_exact_frozen_universe_member_set_and_date() -> None:
    universe = _universe("2330", "2317")
    with pytest.raises(Stage1ContractError, match="exactly match"):
        scan_stage1(
            universe=universe,
            research_snapshots=(_snapshot("2330"),),
            as_of_date=MARKET_DATE,
            candidate_limit=30,
        )
    with pytest.raises(Stage1ContractError, match="market_date"):
        scan_stage1(
            universe=_universe("2330"),
            research_snapshots=(_snapshot("2330"),),
            as_of_date=MARKET_DATE - timedelta(days=1),
            candidate_limit=30,
        )


class _CountingDataset:
    def __init__(self, inner: FakeResearchDataset) -> None:
        self.inner = inner
        self.requests: list[ResearchDatasetRequest] = []

    def read(self, request: ResearchDatasetRequest, /):
        self.requests.append(request)
        return self.inner.read(request)


def _fake_dataset(*symbols: str) -> _CountingDataset:
    dates = _market_dates(62)
    dataset = FakeResearchDataset(
        symbols=tuple(
            DatasetSymbol(
                symbol=symbol,
                name=f"公司{symbol}",
                market="TWSE",
                currency="TWD",
            )
            for symbol in symbols
        ),
        prices=tuple(
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
            for symbol in symbols
            for trade_date in dates
        ),
        valuations=tuple(
            DatasetValuationMetric(
                symbol=symbol,
                metric_date=MARKET_DATE,
                name=name,
                value=value,
                unit=unit,
                source="twse",
            )
            for symbol in symbols
            for name, value, unit in (
                ("price_earnings_ratio", 15.0, "ratio"),
                ("price_to_book_ratio", 2.0, "ratio"),
                ("dividend_yield_pct", 3.0, "%"),
            )
        ),
        provenance=tuple(
            DatasetProvenance(symbol=symbol, canonical_sources=("twse",))
            for symbol in symbols
        ),
    )
    return _CountingDataset(dataset)


def test_dataset_composition_reads_each_eligible_symbol_once_with_62_rows() -> None:
    symbols = ("2330", "2317")
    dataset = _fake_dataset(*symbols)
    prior_date = _market_dates(62)[-2]
    previous = tuple(
        PreviousValuationSnapshot(
            symbol=symbol,
            state=Stage1ValuationState(
                as_of_date=prior_date,
                status=ValuationSnapshotStatus.AVAILABLE,
                pe_ratio=15.0,
                pb_ratio=2.0,
                dividend_yield_pct=3.0,
            ),
        )
        for symbol in symbols
    )
    evidence = scan_stage1_from_dataset(
        universe=_universe(*symbols),
        dataset=dataset,
        as_of_date=MARKET_DATE,
        candidate_limit=30,
        previous_valuations=previous,
    )
    assert evidence.dataset_reads == 2
    assert evidence.price_rows_consumed == 124
    assert evidence.history_observations == 62
    assert [item.symbol for item in dataset.requests] == ["2317", "2330"]
    assert all(item.history_observations == 62 for item in dataset.requests)
    assert evidence.result.candidate_count == 0


def test_pure_engine_and_adapter_have_no_external_io_or_operations_imports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def blocked(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise AssertionError("S2 attempted external I/O")

    monkeypatch.setattr(urllib.request, "urlopen", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(sqlite3, "connect", blocked)
    result = _scan((_snapshot(),))
    assert result.candidate_count == 0

    for module in (stage1_module, dataset_module):
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
    attributes = {
        node.attr
        for node in ast.walk(ast.parse(inspect.getsource(dataset_module)))
        if isinstance(node, ast.Attribute)
    }
    assert "validation" not in attributes


def test_canonical_output_does_not_emit_forbidden_terminology() -> None:
    payload = _scan(
        (_snapshot(closes=(100.0, 100.0, 103.0)),)
    ).canonical_json().casefold()
    compact = payload.replace("_", "")
    for term in FORBIDDEN_OUTPUT_TERMS:
        assert term not in payload
        assert term.replace("_", "") not in compact
    assert json.loads(payload)["candidates"][0]["rank"] == 1
