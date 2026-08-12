"""Validation and normalization of provider-shaped market data."""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Mapping

from app.models import CompanyMetric, DailyPrice, NormalizedMarketData, Symbol
from app.providers.base import MarketDataBatch


class NormalizationError(ValueError):
    """Raised when raw provider data cannot safely enter canonical storage."""


class MarketDataNormalizer:
    """Convert provider batches into validated vendor-neutral records."""

    def normalize(self, batch: MarketDataBatch) -> NormalizedMarketData:
        source = str(batch.source).strip()
        if not source:
            raise NormalizationError("provider source must not be empty")
        source_endpoints = tuple(endpoint.strip() for endpoint in batch.source_endpoints)
        if any(not endpoint for endpoint in source_endpoints):
            raise NormalizationError("source endpoints must not contain blank values")
        if len(set(source_endpoints)) != len(source_endpoints):
            raise NormalizationError("source endpoints must be unique")
        if batch.fetched_at is not None and batch.fetched_at.utcoffset() is None:
            raise NormalizationError("fetched_at must be timezone-aware")
        source_timestamp_raw = (
            None
            if batch.source_timestamp_raw is None
            else str(batch.source_timestamp_raw).strip()
        )
        if batch.source_timestamp_raw is not None and not source_timestamp_raw:
            raise NormalizationError("source_timestamp_raw must not be blank")
        if (
            batch.source_timestamp is not None
            and batch.source_timestamp.utcoffset() is None
        ):
            raise NormalizationError("source_timestamp must be timezone-aware")

        raw_symbol = batch.symbol
        symbol_code = self._text(raw_symbol, "symbol").upper()
        symbol = Symbol(
            symbol=symbol_code,
            name=self._text(raw_symbol, "name"),
            market=self._text(raw_symbol, "market").upper(),
            currency=self._text(raw_symbol, "currency").upper(),
            is_active=self._bool(raw_symbol.get("is_active", True), "is_active"),
        )

        prices: list[DailyPrice] = []
        seen_price_dates: set[date] = set()
        for raw_price in batch.daily_prices:
            row_symbol = self._text(raw_price, "symbol").upper()
            if row_symbol != symbol_code:
                raise NormalizationError(
                    f"daily price symbol {row_symbol!r} does not match {symbol_code!r}"
                )
            trade_date = self._date(raw_price, "trade_date")
            if trade_date in seen_price_dates:
                raise NormalizationError(f"duplicate daily price for {trade_date}")
            seen_price_dates.add(trade_date)

            open_price = self._number(raw_price, "open")
            high_price = self._number(raw_price, "high")
            low_price = self._number(raw_price, "low")
            close_price = self._number(raw_price, "close")
            volume = self._integer(raw_price, "volume")
            if min(open_price, high_price, low_price, close_price) <= 0:
                raise NormalizationError("OHLC values must be greater than zero")
            if high_price < max(open_price, low_price, close_price):
                raise NormalizationError("high must be the greatest OHLC value")
            if low_price > min(open_price, high_price, close_price):
                raise NormalizationError("low must be the smallest OHLC value")
            if volume < 0:
                raise NormalizationError("volume must not be negative")

            prices.append(
                DailyPrice(
                    symbol=symbol_code,
                    trade_date=trade_date,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                    source=source,
                )
            )

        if not prices:
            raise NormalizationError("at least one daily price is required")
        if (
            batch.market_date is not None
            and batch.market_date not in seen_price_dates
        ):
            raise NormalizationError(
                "market_date must match a normalized daily price date"
            )

        metrics: list[CompanyMetric] = []
        seen_metrics: set[tuple[date, str]] = set()
        for raw_metric in batch.company_metrics:
            row_symbol = self._text(raw_metric, "symbol").upper()
            if row_symbol != symbol_code:
                raise NormalizationError(
                    f"company metric symbol {row_symbol!r} does not match {symbol_code!r}"
                )
            metric_date = self._date(raw_metric, "metric_date")
            name = self._text(raw_metric, "name")
            metric_key = (metric_date, name)
            if metric_key in seen_metrics:
                raise NormalizationError(
                    f"duplicate company metric {name!r} for {metric_date}"
                )
            seen_metrics.add(metric_key)
            raw_unit = raw_metric.get("unit")
            unit = None if raw_unit is None else str(raw_unit).strip() or None
            metrics.append(
                CompanyMetric(
                    symbol=symbol_code,
                    metric_date=metric_date,
                    name=name,
                    value=self._number(raw_metric, "value"),
                    unit=unit,
                    source=source,
                )
            )

        return NormalizedMarketData(
            source=source,
            symbol=symbol,
            daily_prices=tuple(sorted(prices, key=lambda item: item.trade_date)),
            company_metrics=tuple(
                sorted(metrics, key=lambda item: (item.metric_date, item.name))
            ),
            source_endpoints=source_endpoints,
            fetched_at=batch.fetched_at,
            market_date=batch.market_date,
            source_timestamp_raw=source_timestamp_raw,
            source_timestamp=batch.source_timestamp,
        )

    @staticmethod
    def _required(mapping: Mapping[str, object], key: str) -> object:
        if key not in mapping:
            raise NormalizationError(f"missing required field {key!r}")
        return mapping[key]

    @classmethod
    def _text(cls, mapping: Mapping[str, object], key: str) -> str:
        value = str(cls._required(mapping, key)).strip()
        if not value:
            raise NormalizationError(f"field {key!r} must not be empty")
        return value

    @classmethod
    def _number(cls, mapping: Mapping[str, object], key: str) -> float:
        value = cls._required(mapping, key)
        if isinstance(value, bool):
            raise NormalizationError(f"field {key!r} must be numeric")
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise NormalizationError(f"field {key!r} must be numeric") from exc
        if not math.isfinite(number):
            raise NormalizationError(f"field {key!r} must be finite")
        return number

    @classmethod
    def _integer(cls, mapping: Mapping[str, object], key: str) -> int:
        value = cls._required(mapping, key)
        if isinstance(value, bool):
            raise NormalizationError(f"field {key!r} must be an integer")
        try:
            number = int(value)
        except (TypeError, ValueError) as exc:
            raise NormalizationError(f"field {key!r} must be an integer") from exc
        if str(value).strip() not in {str(number), f"{number}.0"}:
            raise NormalizationError(f"field {key!r} must be an integer")
        return number

    @classmethod
    def _date(cls, mapping: Mapping[str, object], key: str) -> date:
        value = cls._required(mapping, key)
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        try:
            return date.fromisoformat(str(value))
        except ValueError as exc:
            raise NormalizationError(f"field {key!r} must be an ISO date") from exc

    @staticmethod
    def _bool(value: object, key: str) -> bool:
        if isinstance(value, bool):
            return value
        if value in {0, 1}:
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no"}:
                return False
        raise NormalizationError(f"field {key!r} must be boolean")
