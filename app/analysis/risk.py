"""Transparent descriptive risk metrics without AI scoring or trade signals."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import date
from typing import Sequence

from app.models import DailyPrice


@dataclass(frozen=True, slots=True)
class RiskAnalysis:
    symbol: str
    period_start: date
    period_end: date
    observations: int
    first_close: float
    last_close: float
    minimum_close: float
    maximum_close: float
    total_return_pct: float
    average_daily_return_pct: float
    annualized_volatility_pct: float
    max_drawdown_pct: float
    average_volume: float


class DescriptiveRiskAnalyzer:
    """Calculate reproducible historical descriptors from normalized close prices."""

    trading_days_per_year = 252

    def analyze(self, prices: Sequence[DailyPrice]) -> RiskAnalysis:
        if not prices:
            raise ValueError("at least one daily price is required for analysis")
        ordered = sorted(prices, key=lambda item: item.trade_date)
        symbol = ordered[0].symbol
        if any(price.symbol != symbol for price in ordered):
            raise ValueError("analysis requires prices for exactly one symbol")

        closes = [price.close for price in ordered]
        returns = [
            current / previous - 1.0
            for previous, current in zip(closes, closes[1:], strict=False)
        ]
        average_return = statistics.fmean(returns) if returns else 0.0
        daily_volatility = statistics.pstdev(returns) if len(returns) >= 2 else 0.0

        peak = closes[0]
        max_drawdown = 0.0
        for close in closes:
            peak = max(peak, close)
            max_drawdown = min(max_drawdown, close / peak - 1.0)

        return RiskAnalysis(
            symbol=symbol,
            period_start=ordered[0].trade_date,
            period_end=ordered[-1].trade_date,
            observations=len(ordered),
            first_close=closes[0],
            last_close=closes[-1],
            minimum_close=min(closes),
            maximum_close=max(closes),
            total_return_pct=(closes[-1] / closes[0] - 1.0) * 100.0,
            average_daily_return_pct=average_return * 100.0,
            annualized_volatility_pct=(
                daily_volatility * math.sqrt(self.trading_days_per_year) * 100.0
            ),
            max_drawdown_pct=max_drawdown * 100.0,
            average_volume=statistics.fmean(price.volume for price in ordered),
        )
