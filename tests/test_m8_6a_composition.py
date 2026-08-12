"""M8.2 compatibility-adapter tests for Phase 6A calendar composition."""

from __future__ import annotations

import inspect
from datetime import date

import pytest

from app.market_calendar import MarketDateEvidence, MarketDayState, TwseMarketCalendar
from app.pipelines import trading_day
from app.pipelines.trading_day import TradingDayStatus, resolve_trading_day


TARGET = date(2026, 8, 6)


class _RecordingCalendar:
    def __init__(self) -> None:
        self.delegate = TwseMarketCalendar()
        self.calls: list[tuple[date, date | None]] = []
        self.evidence: list[MarketDateEvidence] = []

    def classify(
        self,
        requested_date: date,
        latest_published_date: date | None,
    ) -> MarketDateEvidence:
        self.calls.append((requested_date, latest_published_date))
        evidence = self.delegate.classify(requested_date, latest_published_date)
        self.evidence.append(evidence)
        return evidence


@pytest.mark.parametrize(
    (
        "requested",
        "latest",
        "expected_state",
        "expected_status",
        "expected_resolved",
        "expected_callback_calls",
    ),
    [
        (
            date(2026, 8, 8),
            date(2026, 8, 8),
            MarketDayState.WEEKEND,
            TradingDayStatus.SKIPPED_NON_TRADING_DAY,
            None,
            0,
        ),
        (
            TARGET,
            None,
            MarketDayState.LATEST_UNAVAILABLE,
            TradingDayStatus.SKIPPED_NO_NEW_MARKET_DATE,
            None,
            1,
        ),
        (
            TARGET,
            date(2026, 8, 5),
            MarketDayState.LATEST_BEFORE_REQUESTED,
            TradingDayStatus.DEFERRED_AWAITING_MARKET_DATA,
            None,
            1,
        ),
        (
            TARGET,
            TARGET,
            MarketDayState.LATEST_ON_REQUESTED,
            TradingDayStatus.RESOLVED,
            TARGET,
            1,
        ),
        (
            TARGET,
            date(2026, 8, 7),
            MarketDayState.LATEST_AFTER_REQUESTED,
            TradingDayStatus.RESOLVED,
            date(2026, 8, 7),
            1,
        ),
    ],
    ids=("weekend", "latest-none", "latest-before", "published-on", "published-after"),
)
def test_resolve_trading_day_maps_market_calendar_evidence_to_legacy_6a(
    monkeypatch: pytest.MonkeyPatch,
    requested: date,
    latest: date | None,
    expected_state: MarketDayState,
    expected_status: TradingDayStatus,
    expected_resolved: date | None,
    expected_callback_calls: int,
) -> None:
    calendar = _RecordingCalendar()
    monkeypatch.setattr(trading_day, "_TWSE_MARKET_CALENDAR", calendar)
    callback_calls = 0

    def latest_market_date_fn() -> date | None:
        nonlocal callback_calls
        callback_calls += 1
        return latest

    result = resolve_trading_day(
        requested,
        latest_market_date_fn=latest_market_date_fn,
    )

    assert result.requested_date == requested
    assert result.status is expected_status
    assert result.resolved_market_date == expected_resolved
    assert callback_calls == expected_callback_calls
    assert calendar.evidence[-1].state is expected_state
    if expected_state is MarketDayState.WEEKEND:
        assert calendar.calls == [(requested, None)]
    else:
        assert calendar.calls == [(requested, None), (requested, latest)]


def test_resolve_trading_day_public_api_and_legacy_enum_remain_frozen() -> None:
    signature = inspect.signature(resolve_trading_day)

    assert tuple(signature.parameters) == (
        "requested_date",
        "latest_market_date_fn",
    )
    assert signature.parameters["latest_market_date_fn"].kind is inspect.Parameter.KEYWORD_ONLY
    assert {item.name: item.value for item in TradingDayStatus} == {
        "RESOLVED": "resolved",
        "SKIPPED_NON_TRADING_DAY": "skipped_non_trading_day",
        "SKIPPED_NO_NEW_MARKET_DATE": "skipped_no_new_market_date",
        "DEFERRED_AWAITING_MARKET_DATA": "deferred_awaiting_market_data",
    }
