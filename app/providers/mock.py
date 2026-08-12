"""Deterministic synthetic provider used for local development and tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from enum import Enum
from typing import Sequence

from .base import (
    MarketDataBatch,
    MarketDataProvider,
    ProviderInvalidPayloadError,
    ProviderInvalidRequestError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)
from .artifacts import source_artifact_from_json


_SYNTHETIC_ENDPOINT = "mock://synthetic/market-data"


class MockFailureMode(str, Enum):
    NONE = "none"
    TIMEOUT = "timeout"
    TEMPORARY = "temporary"
    PERMANENT = "permanent"
    INVALID_PAYLOAD = "invalid_payload"
    MALFORMED_PAYLOAD = "malformed_payload"
    INTERRUPT = "interrupt"


class MockMarketDataProvider(MarketDataProvider):
    """Generate synthetic data without network access.

    Values returned here are fixtures, not real market observations.
    """

    def __init__(
        self,
        failure_plan: Sequence[MockFailureMode | str] = (),
    ) -> None:
        self.failure_plan = tuple(MockFailureMode(mode) for mode in failure_plan)
        self.call_count = 0
        self.timeout_history: list[float] = []

    @property
    def source(self) -> str:
        return "mock-synthetic"

    def fetch_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        timeout_seconds: float,
    ) -> MarketDataBatch:
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ProviderInvalidRequestError("symbol must not be empty")
        if start_date > end_date:
            raise ProviderInvalidRequestError("start_date must not be after end_date")
        if timeout_seconds <= 0:
            raise ProviderInvalidRequestError("timeout_seconds must be greater than zero")

        failure_index = self.call_count
        self.call_count += 1
        self.timeout_history.append(timeout_seconds)
        mode = (
            self.failure_plan[failure_index]
            if failure_index < len(self.failure_plan)
            else MockFailureMode.NONE
        )
        if mode is MockFailureMode.TIMEOUT:
            raise ProviderTimeoutError("synthetic provider timeout")
        if mode is MockFailureMode.TEMPORARY:
            raise ProviderTemporaryError("synthetic temporary provider failure")
        if mode is MockFailureMode.PERMANENT:
            raise ProviderPermanentError("synthetic permanent provider failure")
        if mode is MockFailureMode.INVALID_PAYLOAD:
            raise ProviderInvalidPayloadError("synthetic invalid provider payload")
        if mode is MockFailureMode.INTERRUPT:
            raise KeyboardInterrupt("synthetic interrupted provider call")

        prices: list[dict[str, object]] = []
        current = start_date
        observation_index = 0
        while current <= end_date:
            if current.weekday() < 5:
                open_price = 100.0 + observation_index * 0.8
                close_delta = 0.6 if observation_index % 2 == 0 else -0.25
                close_price = open_price + close_delta
                prices.append(
                    {
                        "symbol": normalized_symbol,
                        "trade_date": current.isoformat(),
                        "open": round(open_price, 2),
                        "high": round(max(open_price, close_price) + 1.0, 2),
                        "low": round(min(open_price, close_price) - 1.0, 2),
                        "close": round(close_price, 2),
                        "volume": 1_000_000 + observation_index * 25_000,
                    }
                )
                observation_index += 1
            current += timedelta(days=1)

        if not prices:
            raise ProviderDataError("requested range contains no synthetic trading day")

        batch = MarketDataBatch(
            source=self.source,
            symbol={
                "symbol": normalized_symbol,
                "name": f"Synthetic {normalized_symbol}",
                "market": "MOCK",
                "currency": "TWD",
                "is_active": True,
            },
            daily_prices=tuple(prices),
            company_metrics=(
                {
                    "symbol": normalized_symbol,
                    "metric_date": end_date.isoformat(),
                    "name": "revenue_growth_pct",
                    "value": 12.5,
                    "unit": "%",
                },
                {
                    "symbol": normalized_symbol,
                    "metric_date": end_date.isoformat(),
                    "name": "debt_to_equity",
                    "value": 0.35,
                    "unit": "ratio",
                },
            ),
            source_endpoints=(_SYNTHETIC_ENDPOINT,),
        )
        if mode is MockFailureMode.MALFORMED_PAYLOAD:
            malformed_price = dict(batch.daily_prices[0])
            malformed_price["high"] = 0
            return replace(
                batch,
                daily_prices=(malformed_price, *batch.daily_prices[1:]),
            )
        artifact = source_artifact_from_json(
            provider=self.source,
            dataset="synthetic-daily-market-data",
            endpoint=_SYNTHETIC_ENDPOINT,
            contract_version=self.manifest.contract_version,
            payload={
                "symbol": batch.symbol,
                "daily_prices": batch.daily_prices,
                "company_metrics": batch.company_metrics,
            },
        )
        return replace(batch, source_artifacts=(artifact,))
