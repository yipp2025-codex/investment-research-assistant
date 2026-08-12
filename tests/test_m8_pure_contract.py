"""Pure unit tests for the uncomposed M8.1 calendar and as-of contract."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError, dataclass
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from app.as_of_policy import FrozenAsOfPolicyV1
from app.market_calendar import (
    MarketCalendar,
    MarketDateEvidence,
    MarketDayState,
    TwseMarketCalendar,
)


TARGET = date(2026, 8, 6)


@pytest.mark.parametrize(
    ("requested", "latest", "expected"),
    [
        (date(2026, 8, 8), None, MarketDayState.WEEKEND),
        (TARGET, None, MarketDayState.LATEST_UNAVAILABLE),
        (TARGET, date(2026, 8, 5), MarketDayState.LATEST_BEFORE_REQUESTED),
        (TARGET, TARGET, MarketDayState.LATEST_ON_REQUESTED),
        (TARGET, date(2026, 8, 7), MarketDayState.LATEST_AFTER_REQUESTED),
    ],
    ids=("weekend", "latest-none", "latest-before", "latest-on", "latest-after"),
)
def test_twse_calendar_classifies_only_supplied_market_date_evidence(
    requested: date,
    latest: date | None,
    expected: MarketDayState,
) -> None:
    evidence = TwseMarketCalendar().classify(requested, latest)

    assert evidence == MarketDateEvidence(
        market="TWSE",
        timezone=ZoneInfo("Asia/Taipei"),
        requested_date=requested,
        latest_published_date=latest,
        state=expected,
    )


def test_twse_calendar_has_fixed_identity_timezone_and_protocol_shape() -> None:
    calendar = TwseMarketCalendar()

    assert isinstance(calendar, MarketCalendar)
    assert calendar.market == "TWSE"
    assert calendar.timezone == ZoneInfo("Asia/Taipei")
    assert calendar.timezone.key == "Asia/Taipei"
    assert tuple(inspect.signature(calendar.classify).parameters) == (
        "requested_date",
        "latest_published_date",
    )


def test_twse_calendar_does_not_claim_holiday_or_previous_day_knowledge() -> None:
    calendar = TwseMarketCalendar()

    assert not hasattr(calendar, "is_trading_day")
    assert not hasattr(calendar, "previous_trading_day")
    assert not hasattr(calendar, "provider")
    assert not hasattr(calendar, "repository")


def test_weekend_classification_has_precedence_but_preserves_given_evidence() -> None:
    latest = date(2026, 8, 10)

    evidence = TwseMarketCalendar().classify(date(2026, 8, 8), latest)

    assert evidence.state is MarketDayState.WEEKEND
    assert evidence.latest_published_date == latest


def test_weekday_without_latest_evidence_does_not_invent_a_holiday() -> None:
    evidence = TwseMarketCalendar().classify(TARGET, None)

    assert evidence.state is MarketDayState.LATEST_UNAVAILABLE
    assert "holiday" not in evidence.state.value
    assert "trading" not in evidence.state.value


def test_market_date_evidence_is_immutable() -> None:
    evidence = TwseMarketCalendar().classify(TARGET, TARGET)

    with pytest.raises(FrozenInstanceError):
        evidence.state = MarketDayState.LATEST_BEFORE_REQUESTED  # type: ignore[misc]


@pytest.mark.parametrize(
    ("requested", "latest"),
    [
        (datetime(2026, 8, 6, tzinfo=timezone.utc), TARGET),
        (TARGET, datetime(2026, 8, 6, tzinfo=timezone.utc)),
    ],
)
def test_twse_calendar_rejects_datetime_inputs(
    requested: date,
    latest: date,
) -> None:
    with pytest.raises(TypeError, match="must be a date, not a datetime"):
        TwseMarketCalendar().classify(requested, latest)


@dataclass(frozen=True)
class _Price:
    trade_date: date
    value: float


@dataclass(frozen=True)
class _Metric:
    metric_date: date
    value: float


@dataclass(frozen=True)
class _Result:
    result_id: str
    symbol: str
    market_date: date
    methodology_version: str
    result_status: str = "success"


def test_price_cutoff_is_inclusive_target_date() -> None:
    policy = FrozenAsOfPolicyV1()

    assert policy.price_cutoff(TARGET) == TARGET
    assert policy.is_future_data(TARGET, TARGET) is False
    assert policy.is_future_data(date(2026, 8, 7), TARGET) is True


def test_prices_as_of_excludes_future_data_without_reordering_history() -> None:
    policy = FrozenAsOfPolicyV1()
    prices = (
        _Price(date(2026, 8, 4), 100.0),
        _Price(TARGET, 105.0),
        _Price(date(2026, 8, 7), 999.0),
    )

    assert policy.prices_as_of(prices, TARGET) == prices[:2]


@pytest.mark.parametrize(
    ("dates", "expected"),
    [
        ((), False),
        ((date(2026, 8, 5),), False),
        ((date(2026, 8, 7),), False),
        ((date(2026, 8, 5), date(2026, 8, 7)), False),
        ((TARGET,), True),
        ((TARGET, date(2026, 8, 7)), True),
    ],
    ids=("empty", "older", "future-only", "older-and-future", "exact", "exact-and-future"),
)
def test_current_price_requires_exact_target_date_after_future_exclusion(
    dates: tuple[date, ...],
    expected: bool,
) -> None:
    prices = tuple(_Price(item, float(index)) for index, item in enumerate(dates))

    assert FrozenAsOfPolicyV1.current_price_is_exact_date(prices, TARGET) is expected


def test_valuation_carries_nearest_value_forward_and_excludes_future() -> None:
    older = _Metric(date(2026, 8, 4), 20.0)
    exact = _Metric(TARGET, 21.0)
    future = _Metric(date(2026, 8, 7), 99.0)

    assert FrozenAsOfPolicyV1.select_valuation_as_of(
        (older, exact, future), TARGET
    ) is exact
    assert FrozenAsOfPolicyV1.select_valuation_as_of(
        (older, future), TARGET
    ) is older
    assert FrozenAsOfPolicyV1.select_valuation_as_of((future,), TARGET) is None


@pytest.mark.parametrize(
    ("validation_target", "expected"),
    [
        (None, False),
        (date(2026, 8, 5), False),
        (TARGET, True),
        (date(2026, 8, 7), False),
    ],
    ids=("missing", "past", "exact", "future"),
)
def test_validation_requires_exact_target_date(
    validation_target: date | None,
    expected: bool,
) -> None:
    assert (
        FrozenAsOfPolicyV1.validation_target_is_consistent(
            validation_target,
            TARGET,
        )
        is expected
    )


def test_previous_successful_comparable_result_can_cross_report_gap() -> None:
    earlier = _Result("r-1", "2330", date(2026, 8, 3), "6b-daily-v1")
    nearest = _Result("r-2", "2330", date(2026, 8, 5), "6b-daily-v1")

    selected = FrozenAsOfPolicyV1.select_previous_successful_comparable(
        (earlier, nearest),
        symbol="2330",
        target_date=date(2026, 8, 7),
        methodology_version="6b-daily-v1",
    )

    assert selected is nearest


def test_methodology_mismatch_is_not_comparable() -> None:
    incompatible = _Result(
        "old",
        "2330",
        date(2026, 8, 5),
        "old-method-v0",
    )

    assert not FrozenAsOfPolicyV1.previous_result_is_comparable(
        incompatible,
        symbol="2330",
        target_date=TARGET,
        methodology_version="6b-daily-v1",
    )
    assert FrozenAsOfPolicyV1.select_previous_successful_comparable(
        (incompatible,),
        symbol="2330",
        target_date=TARGET,
        methodology_version="6b-daily-v1",
    ) is None


def test_previous_selection_excludes_wrong_symbol_non_success_and_non_previous() -> None:
    valid = _Result("valid", "2330", date(2026, 8, 2), "6b-daily-v1")
    candidates = (
        valid,
        _Result("wrong-symbol", "2317", date(2026, 8, 5), "6b-daily-v1"),
        _Result("failed", "2330", date(2026, 8, 5), "6b-daily-v1", "failed"),
        _Result("same-day", "2330", TARGET, "6b-daily-v1"),
        _Result("future", "2330", date(2026, 8, 7), "6b-daily-v1"),
    )

    selected = FrozenAsOfPolicyV1.select_previous_successful_comparable(
        candidates,
        symbol="2330",
        target_date=TARGET,
        methodology_version="6b-daily-v1",
    )

    assert selected is valid


def test_nearer_incompatible_result_does_not_hide_older_comparable_result() -> None:
    compatible = _Result(
        "compatible",
        "2330",
        date(2026, 8, 3),
        "6b-daily-v1",
    )
    newer_incompatible = _Result(
        "incompatible",
        "2330",
        date(2026, 8, 5),
        "old-method-v0",
    )

    selected = FrozenAsOfPolicyV1.select_previous_successful_comparable(
        (compatible, newer_incompatible),
        symbol="2330",
        target_date=TARGET,
        methodology_version="6b-daily-v1",
    )

    assert selected is compatible


def test_as_of_policy_is_stateless_and_rejects_datetime_cutoffs() -> None:
    policy = FrozenAsOfPolicyV1()

    assert policy.version == "frozen-as-of-v1"
    with pytest.raises(AttributeError):
        policy.provider = object()  # type: ignore[attr-defined]
    with pytest.raises(TypeError, match="must be a date, not a datetime"):
        policy.price_cutoff(datetime(2026, 8, 6, tzinfo=timezone.utc))
