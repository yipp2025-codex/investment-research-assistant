"""Deterministic multi-window historical research indicators."""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date
from typing import Sequence

from app.models import CompanyMetric, DailyPrice


@dataclass(frozen=True, slots=True)
class HistoricalWindowAnalysis:
    window: int
    return_pct: float | None
    moving_average: float | None
    distance_to_moving_average_pct: float | None
    max_drawdown_pct: float | None
    average_volume: float | None


@dataclass(frozen=True, slots=True)
class HistoricalResearchAnalysis:
    symbol: str
    period_start: date
    period_end: date
    observations: int
    latest_close: float
    latest_volume: int
    daily_return_volatility_pct: float | None
    volatility_observations: int
    volume_ratio_to_20d: float | None
    volume_state: str
    windows: tuple[HistoricalWindowAnalysis, ...]

    def window(self, size: int) -> HistoricalWindowAnalysis:
        for item in self.windows:
            if item.window == size:
                return item
        raise KeyError(size)


class HistoricalResearchAnalyzer:
    """Calculate transparent 20/60/120/250-observation indicators."""

    def __init__(
        self,
        windows: tuple[int, ...] = (20, 60, 120, 250),
        volatility_window: int = 60,
    ) -> None:
        if not windows or any(window < 2 for window in windows):
            raise ValueError("historical windows must be at least 2")
        if 20 not in windows:
            raise ValueError("historical windows must include 20 for volume ratio")
        if volatility_window < 2:
            raise ValueError("volatility_window must be at least 2")
        self.windows = windows
        self.volatility_window = volatility_window

    def analyze(self, prices: Sequence[DailyPrice]) -> HistoricalResearchAnalysis:
        if not prices:
            raise ValueError("historical analysis requires daily prices")
        ordered = sorted(prices, key=lambda item: item.trade_date)
        symbol = ordered[0].symbol
        if any(price.symbol != symbol for price in ordered):
            raise ValueError("historical analysis requires exactly one symbol")
        if len({price.trade_date for price in ordered}) != len(ordered):
            raise ValueError("historical analysis requires unique trade dates")

        closes = [price.close for price in ordered]
        daily_returns = [
            current / previous - 1.0
            for previous, current in zip(closes, closes[1:], strict=False)
        ]
        volatility_returns = daily_returns[-self.volatility_window :]
        volatility = (
            statistics.pstdev(volatility_returns) * 100.0
            if len(volatility_returns) >= 2
            else None
        )

        window_results: list[HistoricalWindowAnalysis] = []
        for window in self.windows:
            if len(ordered) < window:
                window_results.append(
                    HistoricalWindowAnalysis(window, None, None, None, None, None)
                )
                continue
            recent = ordered[-window:]
            moving_average = statistics.fmean(price.close for price in recent)
            return_pct = (
                (ordered[-1].close / ordered[-window - 1].close - 1.0) * 100.0
                if len(ordered) > window
                else None
            )
            window_results.append(
                HistoricalWindowAnalysis(
                    window=window,
                    return_pct=return_pct,
                    moving_average=moving_average,
                    distance_to_moving_average_pct=(
                        ordered[-1].close / moving_average - 1.0
                    )
                    * 100.0,
                    max_drawdown_pct=self._max_drawdown(
                        [price.close for price in recent]
                    ),
                    average_volume=statistics.fmean(
                        price.volume for price in recent
                    ),
                )
            )

        twenty = next(item for item in window_results if item.window == 20)
        volume_ratio = (
            ordered[-1].volume / twenty.average_volume
            if twenty.average_volume not in {None, 0.0}
            else None
        )
        if volume_ratio is None:
            volume_state = "insufficient"
        elif volume_ratio >= 2.0:
            volume_state = "high"
        elif volume_ratio <= 0.5:
            volume_state = "low"
        else:
            volume_state = "normal"

        return HistoricalResearchAnalysis(
            symbol=symbol,
            period_start=ordered[0].trade_date,
            period_end=ordered[-1].trade_date,
            observations=len(ordered),
            latest_close=ordered[-1].close,
            latest_volume=ordered[-1].volume,
            daily_return_volatility_pct=volatility,
            volatility_observations=len(volatility_returns),
            volume_ratio_to_20d=volume_ratio,
            volume_state=volume_state,
            windows=tuple(window_results),
        )

    @staticmethod
    def _max_drawdown(closes: Sequence[float]) -> float:
        peak = closes[0]
        drawdown = 0.0
        for close in closes:
            peak = max(peak, close)
            drawdown = min(drawdown, close / peak - 1.0)
        return drawdown * 100.0


class HistoricalResearchSummaryGenerator:
    """Render multi-window analysis and latest valuation metrics as Markdown."""

    def generate(
        self,
        analysis: HistoricalResearchAnalysis,
        metrics: Sequence[CompanyMetric],
    ) -> str:
        lines = [
            f"## {analysis.symbol} 歷史研究摘要",
            "",
            f"- 觀察期間：{analysis.period_start} 至 {analysis.period_end}",
            f"- 交易日筆數：{analysis.observations}",
            f"- 最新收盤價：{analysis.latest_close:.2f}",
            f"- 最新成交量：{analysis.latest_volume:,}",
            "- 近 20 日量比："
            + self._format_number(analysis.volume_ratio_to_20d, suffix="x"),
            f"- 量能狀態：{analysis.volume_state}",
            "- 日報酬波動度："
            + self._format_number(
                analysis.daily_return_volatility_pct, suffix="%"
            )
            + f"（{analysis.volatility_observations} 個日報酬）",
            "",
            "### 多期間價格與風險",
            "",
            "| 交易日窗口 | 報酬率 | 最大回撤 | 均線距離 | 平均成交量 |",
            "|---:|---:|---:|---:|---:|",
        ]
        for item in analysis.windows:
            lines.append(
                f"| {item.window} | "
                f"{self._format_number(item.return_pct, suffix='%')} | "
                f"{self._format_number(item.max_drawdown_pct, suffix='%')} | "
                f"{self._format_number(item.distance_to_moving_average_pct, suffix='%')} | "
                f"{self._format_volume(item.average_volume)} |"
            )

        latest_metrics: dict[str, CompanyMetric] = {}
        for metric in sorted(metrics, key=lambda item: item.metric_date):
            latest_metrics[metric.name] = metric
        lines.extend(["", "### 最新估值資料", ""])
        if latest_metrics:
            for name in (
                "price_earnings_ratio",
                "price_to_book_ratio",
                "dividend_yield_pct",
            ):
                metric = latest_metrics.get(name)
                if metric is not None:
                    unit = f" {metric.unit}" if metric.unit else ""
                    lines.append(
                        f"- {name}：{metric.value:.2f}{unit}（{metric.metric_date}）"
                    )
        else:
            lines.append("- 沒有可用估值資料。")

        trend_parts = []
        for window in (20, 60, 120):
            result = analysis.window(window)
            if result.return_pct is not None:
                trend_parts.append(f"{window} 日 {result.return_pct:.2f}%")
        lines.extend(
            [
                "",
                "### 價格趨勢與估值並列",
                "",
                "- 價格趨勢：" + ("；".join(trend_parts) or "資料不足"),
                "- 本節只並列歷史價格與最新估值，不推導合理價或買賣訊號。",
                "",
                "### 使用限制",
                "",
                "- 指標以交易日觀察筆數計算，不是日曆天。",
                "- 顯示 unavailable 代表資料不足，不以較短區間冒充指定窗口。",
                "- 本摘要不構成投資建議。",
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def _format_number(value: float | None, *, suffix: str) -> str:
        return "unavailable" if value is None else f"{value:.2f}{suffix}"

    @staticmethod
    def _format_volume(value: float | None) -> str:
        return "unavailable" if value is None else f"{value:,.0f}"
