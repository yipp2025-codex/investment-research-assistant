from datetime import date

import pytest

from app.providers import (
    EsunMarketDataProvider,
    MarketDataProvider,
    MockFailureMode,
    MockMarketDataProvider,
    ProviderInvalidPayloadError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)


def test_provider_contract_is_abstract() -> None:
    class IncompleteProvider(MarketDataProvider):
        pass

    with pytest.raises(TypeError):
        IncompleteProvider()


def test_mock_provider_returns_deterministic_synthetic_batch() -> None:
    provider = MockMarketDataProvider()

    first = provider.fetch_market_data(
        "mock1",
        date(2026, 8, 3),
        date(2026, 8, 5),
        timeout_seconds=2.5,
    )
    second = provider.fetch_market_data(
        "MOCK1",
        date(2026, 8, 3),
        date(2026, 8, 5),
        timeout_seconds=2.5,
    )

    assert first == second
    assert first.source == "mock-synthetic"
    assert first.symbol["symbol"] == "MOCK1"
    assert len(first.daily_prices) == 3
    assert len(first.company_metrics) == 2
    assert first.source_endpoints == ("mock://synthetic/market-data",)
    assert len(first.source_artifacts) == 1
    assert first.source_artifacts[0].provider == "mock-synthetic"
    assert first.source_artifacts[0].dataset == "synthetic-daily-market-data"
    assert first.source_artifacts[0].hash_basis == "canonical-json-v1"
    assert provider.timeout_history == [2.5, 2.5]


@pytest.mark.parametrize(
    ("mode", "error_type"),
    [
        (MockFailureMode.TIMEOUT, ProviderTimeoutError),
        (MockFailureMode.TEMPORARY, ProviderTemporaryError),
        (MockFailureMode.PERMANENT, ProviderPermanentError),
        (MockFailureMode.INVALID_PAYLOAD, ProviderInvalidPayloadError),
    ],
)
def test_mock_provider_exposes_explicit_failure_contract(mode, error_type) -> None:
    provider = MockMarketDataProvider([mode])

    with pytest.raises(error_type):
        provider.fetch_market_data(
            "MOCK1",
            date(2026, 8, 3),
            date(2026, 8, 5),
            timeout_seconds=1.0,
        )


def test_esun_provider_requires_config_or_injected_transport() -> None:
    with pytest.raises(ValueError, match="config_path"):
        EsunMarketDataProvider()
