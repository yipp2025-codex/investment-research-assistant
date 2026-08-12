"""E.SUN daily candles adapted to the existing one-month history contract."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from .base import MarketDataBatch, MarketDataProvider, ProviderInvalidRequestError
from .esun import EsunMarketDataProvider
from .esun_sdk import EsunHttpTransport


class EsunHistoricalMarketDataProvider(MarketDataProvider):
    """Expose official E.SUN candles as one calendar month per call."""

    def __init__(
        self,
        transport: EsunHttpTransport | None = None,
        *,
        config_path: str | Path | None = None,
    ) -> None:
        self.delegate = EsunMarketDataProvider(
            transport=transport,
            config_path=config_path,
        )

    @property
    def source(self) -> str:
        return "esun-historical"

    def fetch_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        timeout_seconds: float,
    ) -> MarketDataBatch:
        if (start_date.year, start_date.month) != (end_date.year, end_date.month):
            raise ProviderInvalidRequestError(
                "E.SUN historical sync accepts one calendar month per call"
            )
        batch = self.delegate.fetch_market_data(
            symbol,
            start_date,
            end_date,
            timeout_seconds=timeout_seconds,
        )
        return replace(
            batch,
            source=self.source,
            source_artifacts=tuple(
                replace(artifact, provider=self.source)
                for artifact in batch.source_artifacts
            ),
        )
