"""Trading-day resolution for Phase 6A daily batch runner.

Rules (applied in order):
1. Saturday / Sunday  -> skipped_non_trading_day  (no provider call needed)
2. Weekday: query the TWSE provider for the latest market date.
   - If latest_market_date == requested_date -> proceed (resolved).
   - If latest_market_date < requested_date  -> deferred_awaiting_market_data
     (data not yet published; caller may retry later).
   - If latest_market_date is None           -> skipped_no_new_market_date
     (holiday or provider unavailable).
3. No external network required for weekend detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Callable

from app.market_calendar import (
    MarketDateEvidence,
    MarketDayState,
    TwseMarketCalendar,
)


class TradingDayStatus(str, Enum):
    RESOLVED = "resolved"
    SKIPPED_NON_TRADING_DAY = "skipped_non_trading_day"
    SKIPPED_NO_NEW_MARKET_DATE = "skipped_no_new_market_date"
    DEFERRED_AWAITING_MARKET_DATA = "deferred_awaiting_market_data"


@dataclass(frozen=True, slots=True)
class TradingDayResolution:
    requested_date: date
    status: TradingDayStatus
    resolved_market_date: date | None  # only set when status == RESOLVED


_TWSE_MARKET_CALENDAR = TwseMarketCalendar()

_LEGACY_STATUS_BY_MARKET_STATE = {
    MarketDayState.WEEKEND: TradingDayStatus.SKIPPED_NON_TRADING_DAY,
    MarketDayState.LATEST_UNAVAILABLE: TradingDayStatus.SKIPPED_NO_NEW_MARKET_DATE,
    MarketDayState.LATEST_BEFORE_REQUESTED: (
        TradingDayStatus.DEFERRED_AWAITING_MARKET_DATA
    ),
    MarketDayState.LATEST_ON_REQUESTED: TradingDayStatus.RESOLVED,
    MarketDayState.LATEST_AFTER_REQUESTED: TradingDayStatus.RESOLVED,
}

_RESOLVED_MARKET_STATES = {
    MarketDayState.LATEST_ON_REQUESTED,
    MarketDayState.LATEST_AFTER_REQUESTED,
}


def resolve_trading_day(
    requested_date: date,
    *,
    latest_market_date_fn: Callable[[], date | None],
) -> TradingDayResolution:
    """Determine whether requested_date is a valid trading day.

    Args:
        requested_date: The calendar date the runner is being asked to process.
        latest_market_date_fn: Zero-arg callable that returns the most recent
            market date available from the primary provider, or None if the
            provider has no data (holiday / provider unavailable).  The caller
            is responsible for caching; this function calls it at most once.
    """
    # Classify without publication evidence first so weekends retain the
    # compatibility contract of never invoking the provider callback.
    initial = _TWSE_MARKET_CALENDAR.classify(requested_date, None)
    if initial.state is MarketDayState.WEEKEND:
        return _as_legacy_resolution(initial)

    # Weekdays retain the existing at-most-once callback contract.
    latest = latest_market_date_fn()
    evidence = _TWSE_MARKET_CALENDAR.classify(requested_date, latest)
    return _as_legacy_resolution(evidence)


def _as_legacy_resolution(evidence: MarketDateEvidence) -> TradingDayResolution:
    """Map the pure M8 evidence contract to the frozen Phase 6A API."""
    status = _LEGACY_STATUS_BY_MARKET_STATE[evidence.state]
    resolved_market_date = (
        evidence.latest_published_date
        if evidence.state in _RESOLVED_MARKET_STATES
        else None
    )
    if evidence.state in _RESOLVED_MARKET_STATES and resolved_market_date is None:
        raise RuntimeError("resolved market-date evidence is missing its latest date")
    return TradingDayResolution(
        requested_date=evidence.requested_date,
        status=status,
        resolved_market_date=resolved_market_date,
    )
