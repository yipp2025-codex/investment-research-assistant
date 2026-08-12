from datetime import date

import pytest

from app.pipelines import MarketDataNormalizer, NormalizationError
from app.providers import MarketDataBatch, MockMarketDataProvider


def test_normalizer_creates_vendor_neutral_records() -> None:
    batch = MockMarketDataProvider().fetch_market_data(
        "mock1",
        date(2026, 8, 3),
        date(2026, 8, 5),
        timeout_seconds=1.0,
    )

    normalized = MarketDataNormalizer().normalize(batch)

    assert normalized.symbol.symbol == "MOCK1"
    assert normalized.symbol.market == "MOCK"
    assert normalized.daily_prices[0].source == "mock-synthetic"
    assert normalized.daily_prices[0].trade_date == date(2026, 8, 3)


def test_normalizer_rejects_invalid_ohlc_before_storage() -> None:
    batch = MarketDataBatch(
        source="bad-fixture",
        symbol={
            "symbol": "BAD1",
            "name": "Invalid fixture",
            "market": "MOCK",
            "currency": "TWD",
        },
        daily_prices=(
            {
                "symbol": "BAD1",
                "trade_date": "2026-08-03",
                "open": 100,
                "high": 99,
                "low": 98,
                "close": 100,
                "volume": 1,
            },
        ),
        company_metrics=(),
    )

    with pytest.raises(NormalizationError, match="high"):
        MarketDataNormalizer().normalize(batch)
