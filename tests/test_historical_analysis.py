from datetime import date, timedelta

import pytest

from app.analysis import HistoricalResearchAnalyzer, HistoricalResearchSummaryGenerator
from app.models import CompanyMetric, DailyPrice


def _prices(count: int, *, latest_volume: int = 1_000) -> list[DailyPrice]:
    start = date(2025, 1, 1)
    prices = []
    for index in range(count):
        close = 100.0 + index
        prices.append(
            DailyPrice(
                symbol="2330",
                trade_date=start + timedelta(days=index),
                open=close - 0.5,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=latest_volume if index == count - 1 else 1_000,
                source="synthetic-history-test",
            )
        )
    return prices


def test_historical_analysis_calculates_20_60_120_250_windows() -> None:
    analysis = HistoricalResearchAnalyzer().analyze(
        _prices(251, latest_volume=3_000)
    )

    assert analysis.observations == 251
    assert analysis.window(20).return_pct == pytest.approx((350 / 330 - 1) * 100)
    assert analysis.window(60).return_pct == pytest.approx((350 / 290 - 1) * 100)
    assert analysis.window(120).return_pct == pytest.approx((350 / 230 - 1) * 100)
    assert analysis.window(250).return_pct == pytest.approx((350 / 100 - 1) * 100)
    assert analysis.window(20).moving_average == pytest.approx(340.5)
    assert analysis.window(20).max_drawdown_pct == 0.0
    assert analysis.volume_ratio_to_20d == pytest.approx(3_000 / 1_100)
    assert analysis.volume_state == "high"
    assert analysis.daily_return_volatility_pct is not None
    assert analysis.volatility_observations == 60


def test_historical_analysis_computes_recent_max_drawdown() -> None:
    prices = _prices(60)
    for index in range(40, 60):
        close = 200.0 - (index - 40) * (100.0 / 19.0)
        prices[index] = DailyPrice(
            symbol="2330",
            trade_date=prices[index].trade_date,
            open=close,
            high=close,
            low=close,
            close=close,
            volume=1_000,
            source="synthetic-history-test",
        )

    analysis = HistoricalResearchAnalyzer().analyze(prices)

    assert analysis.window(20).max_drawdown_pct == pytest.approx(-50.0)
    assert analysis.window(60).max_drawdown_pct is not None
    assert analysis.window(120).max_drawdown_pct is None


def test_historical_analysis_marks_insufficient_windows_unavailable() -> None:
    analysis = HistoricalResearchAnalyzer().analyze(_prices(19))

    assert analysis.window(20).return_pct is None
    assert analysis.window(20).moving_average is None
    assert analysis.window(60).max_drawdown_pct is None
    assert analysis.volume_ratio_to_20d is None
    assert analysis.volume_state == "insufficient"


def test_historical_summary_combines_trend_risk_volume_and_valuation() -> None:
    analysis = HistoricalResearchAnalyzer().analyze(_prices(121))
    metrics = [
        CompanyMetric(
            "2330",
            analysis.period_end,
            "price_earnings_ratio",
            31.19,
            "ratio",
            "twse",
        ),
        CompanyMetric(
            "2330",
            analysis.period_end,
            "price_to_book_ratio",
            10.21,
            "ratio",
            "twse",
        ),
        CompanyMetric(
            "2330",
            analysis.period_end,
            "dividend_yield_pct",
            0.95,
            "%",
            "twse",
        ),
    ]

    summary = HistoricalResearchSummaryGenerator().generate(analysis, metrics)

    assert "20 日" in summary
    assert "60 日" in summary
    assert "120 日" in summary
    assert "price_earnings_ratio：31.19" in summary
    assert "不推導合理價或買賣訊號" in summary
    assert "不構成投資建議" in summary
