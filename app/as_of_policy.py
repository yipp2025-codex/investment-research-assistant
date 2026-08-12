"""Pure, frozen as-of rules matching the Phase 6B date contract."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from typing import ClassVar, Protocol, TypeVar


class _PriceDated(Protocol):
    trade_date: date


class _MetricDated(Protocol):
    metric_date: date


class _ComparableResult(Protocol):
    symbol: str
    market_date: date
    methodology_version: str
    result_status: str


_PriceT = TypeVar("_PriceT", bound=_PriceDated)
_MetricT = TypeVar("_MetricT", bound=_MetricDated)
_ResultT = TypeVar("_ResultT", bound=_ComparableResult)


class FrozenAsOfPolicyV1:
    """Stateless date-selection rules frozen from the Phase 6B behavior.

    Callers remain responsible for acquiring records.  Every method is pure:
    it reads only its arguments and returns a value without persistence,
    provider access, clock access, or mutation.
    """

    __slots__ = ()

    version: ClassVar[str] = "frozen-as-of-v1"

    @staticmethod
    def price_cutoff(target_date: date) -> date:
        """The inclusive price-history cutoff is the report target date."""
        return _require_date(target_date, "target_date")

    @staticmethod
    def is_future_data(observation_date: date, target_date: date) -> bool:
        """Return whether an observation is later than the target date."""
        observed = _require_date(observation_date, "observation_date")
        target = _require_date(target_date, "target_date")
        return observed > target

    @classmethod
    def prices_as_of(
        cls,
        prices: Iterable[_PriceT],
        target_date: date,
    ) -> tuple[_PriceT, ...]:
        """Keep price observations on or before the inclusive cutoff."""
        cutoff = cls.price_cutoff(target_date)
        selected: list[_PriceT] = []
        for price in prices:
            trade_date = _require_date(price.trade_date, "price.trade_date")
            if trade_date <= cutoff:
                selected.append(price)
        return tuple(selected)

    @classmethod
    def current_price_is_exact_date(
        cls,
        prices: Iterable[_PriceT],
        target_date: date,
    ) -> bool:
        """Require the latest non-future price to be exactly on target date."""
        target = cls.price_cutoff(target_date)
        eligible = cls.prices_as_of(prices, target)
        if not eligible:
            return False
        return max(item.trade_date for item in eligible) == target

    @staticmethod
    def select_valuation_as_of(
        metrics: Iterable[_MetricT],
        target_date: date,
    ) -> _MetricT | None:
        """Carry forward the nearest valuation at or before target date.

        ``metrics`` must already be scoped to one symbol and metric name.
        Future-dated candidates are excluded.
        """
        target = _require_date(target_date, "target_date")
        eligible: list[_MetricT] = []
        for metric in metrics:
            metric_date = _require_date(metric.metric_date, "metric.metric_date")
            if metric_date <= target:
                eligible.append(metric)
        return (
            None
            if not eligible
            else max(eligible, key=lambda item: item.metric_date)
        )

    @staticmethod
    def validation_target_is_consistent(
        validation_target_date: date | None,
        target_date: date,
    ) -> bool:
        """Validation evidence is usable only for the exact target date."""
        target = _require_date(target_date, "target_date")
        if validation_target_date is None:
            return False
        validation_target = _require_date(
            validation_target_date,
            "validation_target_date",
        )
        return validation_target == target

    @staticmethod
    def previous_result_is_comparable(
        candidate: _ComparableResult,
        *,
        symbol: str,
        target_date: date,
        methodology_version: str,
    ) -> bool:
        """Apply the frozen same-symbol/success/earlier/same-method rules."""
        target = _require_date(target_date, "target_date")
        candidate_date = _require_date(
            candidate.market_date,
            "candidate.market_date",
        )
        return (
            candidate.symbol.strip().upper() == symbol.strip().upper()
            and candidate.result_status == "success"
            and candidate.methodology_version == methodology_version
            and candidate_date < target
        )

    @classmethod
    def select_previous_successful_comparable(
        cls,
        candidates: Iterable[_ResultT],
        *,
        symbol: str,
        target_date: date,
        methodology_version: str,
    ) -> _ResultT | None:
        """Select the nearest earlier comparable result across any date gap."""
        target = _require_date(target_date, "target_date")
        eligible = [
            candidate
            for candidate in candidates
            if cls.previous_result_is_comparable(
                candidate,
                symbol=symbol,
                target_date=target,
                methodology_version=methodology_version,
            )
        ]
        return (
            None
            if not eligible
            else max(eligible, key=lambda item: item.market_date)
        )


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"{field_name} must be a date, not a datetime")
    return value


__all__ = ["FrozenAsOfPolicyV1"]
