"""Pure TWSE market-date evidence contract for M8.

This module deliberately contains no provider, network, or persistence access.
It classifies only facts supplied by the caller: a requested calendar date and
the latest market date that an external source says has been published.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import ClassVar, Protocol, runtime_checkable
from zoneinfo import ZoneInfo


class MarketDayState(str, Enum):
    """Relationship between a requested date and publication evidence."""

    WEEKEND = "weekend"
    LATEST_UNAVAILABLE = "latest_unavailable"
    LATEST_BEFORE_REQUESTED = "latest_before_requested"
    LATEST_ON_REQUESTED = "latest_on_requested"
    LATEST_AFTER_REQUESTED = "latest_after_requested"


@dataclass(frozen=True, slots=True)
class MarketDateEvidence:
    """Caller-supplied market-date evidence after pure classification."""

    market: str
    timezone: ZoneInfo
    requested_date: date
    latest_published_date: date | None
    state: MarketDayState


@runtime_checkable
class MarketCalendar(Protocol):
    """Minimal market-date classifier with no data-acquisition responsibility."""

    market: str
    timezone: ZoneInfo

    def classify(
        self,
        requested_date: date,
        latest_published_date: date | None,
    ) -> MarketDateEvidence:
        """Classify caller-provided publication evidence for ``requested_date``."""


class TwseMarketCalendar:
    """Pure TWSE classifier; it is not a holiday or trading-session calendar."""

    __slots__ = ()

    market: ClassVar[str] = "TWSE"
    timezone: ClassVar[ZoneInfo] = ZoneInfo("Asia/Taipei")

    def classify(
        self,
        requested_date: date,
        latest_published_date: date | None,
    ) -> MarketDateEvidence:
        requested = _require_date(requested_date, "requested_date")
        latest = (
            None
            if latest_published_date is None
            else _require_date(latest_published_date, "latest_published_date")
        )

        if requested.isoweekday() in (6, 7):
            state = MarketDayState.WEEKEND
        elif latest is None:
            state = MarketDayState.LATEST_UNAVAILABLE
        elif latest < requested:
            state = MarketDayState.LATEST_BEFORE_REQUESTED
        elif latest == requested:
            state = MarketDayState.LATEST_ON_REQUESTED
        else:
            state = MarketDayState.LATEST_AFTER_REQUESTED

        return MarketDateEvidence(
            market=self.market,
            timezone=self.timezone,
            requested_date=requested,
            latest_published_date=latest,
            state=state,
        )


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"{field_name} must be a date, not a datetime")
    return value


__all__ = [
    "MarketCalendar",
    "MarketDateEvidence",
    "MarketDayState",
    "TwseMarketCalendar",
]
