"""Official read-only E.SUN Securities market-data provider."""

from __future__ import annotations

import json
import math
import re
import socket
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .base import (
    MarketDataBatch,
    MarketDataProvider,
    ProviderError,
    ProviderInvalidPayloadError,
    ProviderInvalidRequestError,
    ProviderNotImplementedError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)
from .artifacts import source_artifact_from_bytes
from .esun_sdk import EsunHttpResponse, EsunHttpTransport, EsunSdkHttpTransport


INTRADAY_TICKER_PATH = "/intraday/ticker/{symbol}"
INTRADAY_QUOTE_PATH = "/intraday/quote/{symbol}"
SNAPSHOT_QUOTES_PATH = "/snapshot/quotes/{market}"
HISTORICAL_CANDLES_PATH = "/historical/candles/{symbol}"
HISTORICAL_STATS_PATH = "/historical/stats/{symbol}"

_COMMON_STOCK_CODE = re.compile(r"^[0-9]{4}$")
_ALLOWED_SECURITY_STATUSES = frozenset({"NORMAL", "SUSPENDED", "TERMINATED"})
_TAIPEI = timezone(timedelta(hours=8))


@dataclass(frozen=True, slots=True)
class EsunQuote:
    symbol: str
    name: str
    market_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume_raw: int | None
    source_timestamp_raw: str | None
    source_timestamp: datetime | None
    is_close: bool | None
    source_endpoint: str
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class EsunSnapshotQuote:
    symbol: str
    name: str
    market_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume_raw: int | None
    source_timestamp_raw: str | None
    source_timestamp: datetime | None
    source_endpoint: str
    fetched_at: datetime


@dataclass(frozen=True, slots=True)
class EsunHistoricalStats:
    symbol: str
    name: str
    market_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    turnover: float
    previous_close: float
    week52_high: float
    week52_low: float
    source_endpoint: str
    fetched_at: datetime


class EsunMarketDataProvider(MarketDataProvider):
    """HTTP validation and canonical mapping for official E.SUN daily candles."""

    def __init__(
        self,
        transport: EsunHttpTransport | None = None,
        *,
        config_path: str | Path | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if transport is None and config_path is None:
            raise ValueError("config_path is required when no E.SUN transport is injected")
        self.transport = transport or EsunSdkHttpTransport(config_path)  # type: ignore[arg-type]
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @property
    def source(self) -> str:
        return "esun"

    def fetch_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        timeout_seconds: float,
    ) -> MarketDataBatch:
        normalized_symbol = self._validate_request(
            symbol, start_date=start_date, end_date=end_date, timeout_seconds=timeout_seconds
        )
        if (end_date - start_date).days > 366:
            raise ProviderInvalidRequestError(
                "E.SUN historical candles accept at most a one-year range"
            )

        ticker, ticker_response = self._request_object(
            INTRADAY_TICKER_PATH.format(symbol=normalized_symbol),
            params=None,
            timeout_seconds=timeout_seconds,
        )
        ticker_identity = self._validate_ticker(ticker, normalized_symbol)
        candles, candles_response = self._request_object(
            HISTORICAL_CANDLES_PATH.format(symbol=normalized_symbol),
            params={
                "from": start_date.isoformat(),
                "to": end_date.isoformat(),
                "timeframe": "D",
                "fields": "open,high,low,close,volume,turnover,change",
            },
            timeout_seconds=timeout_seconds,
        )
        daily_prices = self._map_historical_candles(
            candles,
            normalized_symbol,
            start_date=start_date,
            end_date=end_date,
        )
        market_date = max(date.fromisoformat(str(row["trade_date"])) for row in daily_prices)
        fetched_at = max(ticker_response.fetched_at, candles_response.fetched_at)
        return MarketDataBatch(
            source=self.source,
            symbol={
                "symbol": normalized_symbol,
                "name": ticker_identity["name"],
                "market": "TWSE",
                "currency": "TWD",
                "is_active": ticker_identity["security_status"] == "NORMAL",
            },
            daily_prices=tuple(daily_prices),
            company_metrics=(),
            source_endpoints=(ticker_response.url, candles_response.url),
            fetched_at=fetched_at,
            market_date=market_date,
            source_artifacts=(
                source_artifact_from_bytes(
                    provider=self.source,
                    dataset="intraday-ticker",
                    endpoint=ticker_response.url,
                    contract_version=self.manifest.contract_version,
                    body=ticker_response.body,
                    headers=ticker_response.headers,
                    fetched_at=ticker_response.fetched_at,
                ),
                source_artifact_from_bytes(
                    provider=self.source,
                    dataset="historical-candles",
                    endpoint=candles_response.url,
                    contract_version=self.manifest.contract_version,
                    body=candles_response.body,
                    headers=candles_response.headers,
                    fetched_at=candles_response.fetched_at,
                ),
            ),
        )

    def fetch_quote(self, symbol: str, *, timeout_seconds: float) -> EsunQuote:
        normalized_symbol = self._validate_symbol_timeout(symbol, timeout_seconds)
        payload, response = self._request_object(
            INTRADAY_QUOTE_PATH.format(symbol=normalized_symbol),
            params=None,
            timeout_seconds=timeout_seconds,
        )
        self._validate_identity(payload, normalized_symbol, "intraday quote")
        market_date = self._parse_date(payload, "date")
        name = self._text(payload, "name")
        ohlc = self._optional_ohlc(
            payload, ("openPrice", "highPrice", "lowPrice", "closePrice")
        )
        total = payload.get("total")
        if total is not None and not isinstance(total, dict):
            raise ProviderInvalidPayloadError("E.SUN quote total must be an object")
        volume = (
            None
            if (
                total is None
                or "tradeVolume" not in total
                or total["tradeVolume"] is None
            )
            else self._integer(total, "tradeVolume")
        )
        raw_timestamp, timestamp = self._source_timestamp(
            payload.get("lastUpdated"), market_date, "lastUpdated"
        )
        is_close = payload.get("isClose")
        if is_close is not None and not isinstance(is_close, bool):
            raise ProviderInvalidPayloadError("E.SUN quote isClose must be boolean")
        return EsunQuote(
            symbol=normalized_symbol,
            name=name,
            market_date=market_date,
            open=ohlc[0],
            high=ohlc[1],
            low=ohlc[2],
            close=ohlc[3],
            volume_raw=volume,
            source_timestamp_raw=raw_timestamp,
            source_timestamp=timestamp,
            is_close=is_close,
            source_endpoint=response.url,
            fetched_at=response.fetched_at,
        )

    def fetch_snapshot_quotes(
        self,
        *,
        market: str = "TSE",
        symbols: Sequence[str] = (),
        timeout_seconds: float,
    ) -> tuple[EsunSnapshotQuote, ...]:
        normalized_market = market.strip().upper()
        if timeout_seconds <= 0:
            raise ProviderInvalidRequestError(
                "timeout_seconds must be greater than zero"
            )
        if normalized_market != "TSE":
            raise ProviderInvalidRequestError(
                "Phase 5 E.SUN snapshot scope is TSE only"
            )
        for symbol in symbols:
            self._validate_symbol_timeout(symbol, timeout_seconds)
        self._request_object(
            SNAPSHOT_QUOTES_PATH.format(market=normalized_market),
            params=None,
            timeout_seconds=timeout_seconds,
        )
        raise ProviderNotImplementedError(
            "successful E.SUN snapshot mapping remains disabled until an "
            "entitled official live payload fixture is frozen"
        )

    def fetch_historical_stats(
        self, symbol: str, *, timeout_seconds: float
    ) -> EsunHistoricalStats:
        normalized_symbol = self._validate_symbol_timeout(symbol, timeout_seconds)
        payload, response = self._request_object(
            HISTORICAL_STATS_PATH.format(symbol=normalized_symbol),
            params=None,
            timeout_seconds=timeout_seconds,
        )
        self._validate_identity(payload, normalized_symbol, "historical stats")
        ohlc = tuple(
            self._number(payload, key)
            for key in ("openPrice", "highPrice", "lowPrice", "closePrice")
        )
        self._validate_ohlc(*ohlc)
        volume = self._integer(payload, "tradeVolume")
        turnover = self._number(payload, "tradeValue")
        previous_close = self._number(payload, "previousClose")
        week52_high = self._number(payload, "week52High")
        week52_low = self._number(payload, "week52Low")
        self._number(payload, "change")
        if volume < 0 or turnover < 0:
            raise ProviderInvalidPayloadError(
                "E.SUN historical stats volume and turnover must not be negative"
            )
        if min(previous_close, week52_high, week52_low) <= 0:
            raise ProviderInvalidPayloadError(
                "E.SUN historical stats reference prices must be positive"
            )
        if week52_high < week52_low:
            raise ProviderInvalidPayloadError(
                "E.SUN historical stats 52-week range is invalid"
            )
        return EsunHistoricalStats(
            symbol=normalized_symbol,
            name=self._text(payload, "name"),
            market_date=self._parse_date(payload, "date"),
            open=ohlc[0],
            high=ohlc[1],
            low=ohlc[2],
            close=ohlc[3],
            volume=volume,
            turnover=turnover,
            previous_close=previous_close,
            week52_high=week52_high,
            week52_low=week52_low,
            source_endpoint=response.url,
            fetched_at=response.fetched_at,
        )

    def _request_object(
        self,
        path: str,
        *,
        params: Mapping[str, object] | None,
        timeout_seconds: float,
    ) -> tuple[dict[str, object], EsunHttpResponse]:
        try:
            response = self.transport.get(
                path, params=params, timeout_seconds=timeout_seconds
            )
        except ProviderError:
            raise
        except (TimeoutError, socket.timeout) as error:
            raise ProviderTimeoutError("E.SUN market-data request timed out") from error
        except OSError as error:
            raise ProviderTemporaryError(
                "E.SUN market-data connection failed"
            ) from error

        if isinstance(response.status_code, bool) or not isinstance(
            response.status_code, int
        ):
            raise ProviderInvalidPayloadError(
                "E.SUN transport status code must be an integer"
            )
        if response.fetched_at.utcoffset() is None:
            raise ProviderInvalidPayloadError(
                "E.SUN transport fetched_at must be timezone-aware"
            )
        if not response.url.strip():
            raise ProviderInvalidPayloadError(
                "E.SUN transport source URL must not be blank"
            )

        self._raise_for_status(response.status_code)
        content_type = response.headers.get("content-type", "").lower()
        if content_type and "json" not in content_type:
            raise ProviderInvalidPayloadError(
                "E.SUN market-data content type is not JSON"
            )
        try:
            payload = json.loads(response.body.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProviderInvalidPayloadError(
                "E.SUN market-data endpoint returned malformed JSON"
            ) from error
        if not isinstance(payload, dict):
            raise ProviderInvalidPayloadError(
                "E.SUN market-data payload must be an object"
            )
        embedded_status = payload.get("statusCode", payload.get("status"))
        if isinstance(embedded_status, int) and embedded_status >= 400:
            self._raise_for_status(embedded_status)
        return payload, response

    @staticmethod
    def _raise_for_status(status_code: int) -> None:
        if status_code == 429:
            raise ProviderTemporaryError(
                "E.SUN market-data endpoint returned HTTP 429"
            )
        if 500 <= status_code <= 599:
            raise ProviderTemporaryError(
                f"E.SUN market-data endpoint returned HTTP {status_code}"
            )
        if 400 <= status_code <= 499:
            raise ProviderPermanentError(
                f"E.SUN market-data endpoint returned HTTP {status_code}"
            )
        if status_code != 200:
            raise ProviderPermanentError(
                "E.SUN market-data endpoint returned unexpected HTTP "
                f"{status_code}"
            )

    def _validate_ticker(
        self, payload: Mapping[str, object], symbol: str
    ) -> dict[str, str]:
        self._validate_identity(payload, symbol, "intraday ticker")
        if self._text(payload, "securityType") != "01":
            raise ProviderInvalidRequestError(
                "E.SUN symbol is outside listed common-stock scope"
            )
        currency = self._text(payload, "tradingCurrency")
        if currency != "TWD":
            raise ProviderInvalidRequestError(
                "E.SUN symbol is outside TWD common-stock scope"
            )
        security_status = self._text(payload, "securityStatus")
        if security_status not in _ALLOWED_SECURITY_STATUSES:
            raise ProviderInvalidPayloadError(
                "E.SUN ticker returned an unknown securityStatus"
            )
        self._parse_date(payload, "date")
        self._integer(payload, "boardLot")
        return {
            "name": self._text(payload, "name"),
            "security_status": security_status,
        }

    def _map_historical_candles(
        self,
        payload: Mapping[str, object],
        symbol: str,
        *,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, object]]:
        self._validate_identity(payload, symbol, "historical candles")
        if self._text(payload, "timeframe") != "D":
            raise ProviderInvalidPayloadError(
                "E.SUN historical timeframe must be D"
            )
        sort = self._text(payload, "sort")
        if sort not in {"asc", "desc"}:
            raise ProviderInvalidPayloadError(
                "E.SUN historical sort must be asc or desc"
            )
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise ProviderInvalidPayloadError(
                "E.SUN historical data must be an array"
            )
        if not rows:
            raise ProviderInvalidRequestError(
                "E.SUN historical range contains no daily candles"
            )

        mapped: list[dict[str, object]] = []
        seen_dates: set[date] = set()
        for row in rows:
            if not isinstance(row, dict):
                raise ProviderInvalidPayloadError(
                    "E.SUN historical rows must be objects"
                )
            trade_date = self._parse_date(row, "date")
            if trade_date < start_date or trade_date > end_date:
                raise ProviderInvalidPayloadError(
                    "E.SUN historical row is outside requested range"
                )
            if trade_date in seen_dates:
                raise ProviderInvalidPayloadError(
                    "E.SUN historical row dates must be unique"
                )
            seen_dates.add(trade_date)
            ohlc = tuple(
                self._number(row, key) for key in ("open", "high", "low", "close")
            )
            self._validate_ohlc(*ohlc)
            volume = self._integer(row, "volume")
            if volume < 0:
                raise ProviderInvalidPayloadError(
                    "E.SUN historical volume must not be negative"
                )
            turnover = self._number(row, "turnover")
            if turnover < 0:
                raise ProviderInvalidPayloadError(
                    "E.SUN historical turnover must not be negative"
                )
            self._number(row, "change")
            mapped.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "open": ohlc[0],
                    "high": ohlc[1],
                    "low": ohlc[2],
                    "close": ohlc[3],
                    "volume": volume,
                }
            )
        return sorted(mapped, key=lambda row: str(row["trade_date"]))

    @staticmethod
    def _validate_identity(
        payload: Mapping[str, object], symbol: str, label: str
    ) -> None:
        expected = {
            "symbol": symbol,
            "type": "EQUITY",
            "exchange": "TWSE",
            "market": "TSE",
        }
        for key, value in expected.items():
            if str(payload.get(key, "")).strip() != value:
                raise ProviderInvalidPayloadError(
                    f"E.SUN {label} identity mismatch"
                )

    @staticmethod
    def _validate_ohlc(open_price: float, high: float, low: float, close: float) -> None:
        if min(open_price, high, low, close) <= 0:
            raise ProviderInvalidPayloadError(
                "E.SUN OHLC values must be greater than zero"
            )
        if high < max(open_price, low, close) or low > min(
            open_price, high, close
        ):
            raise ProviderInvalidPayloadError("E.SUN OHLC relationship is invalid")

    def _optional_ohlc(
        self, payload: Mapping[str, object], keys: tuple[str, str, str, str]
    ) -> tuple[float | None, float | None, float | None, float | None]:
        values = tuple(
            None
            if key not in payload or payload[key] is None
            else self._number(payload, key)
            for key in keys
        )
        if any(value is None for value in values) and not all(
            value is None for value in values
        ):
            raise ProviderInvalidPayloadError(
                "E.SUN payload must provide either all or no OHLC values"
            )
        if values[0] is not None:
            self._validate_ohlc(*values)  # type: ignore[arg-type]
        return values

    @staticmethod
    def _source_timestamp(
        value: object, market_date: date, field: str
    ) -> tuple[str | None, datetime | None]:
        if value is None:
            return None, None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ProviderInvalidPayloadError(
                f"E.SUN {field} must be a non-negative integer"
            )
        try:
            timestamp = datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as error:
            raise ProviderInvalidPayloadError(
                f"E.SUN {field} is outside the verified microsecond range"
            ) from error
        if timestamp.astimezone(_TAIPEI).date() != market_date:
            raise ProviderInvalidPayloadError(
                f"E.SUN {field} does not match the payload market date"
            )
        return str(value), timestamp

    @staticmethod
    def _validate_request(
        symbol: str,
        *,
        start_date: date,
        end_date: date,
        timeout_seconds: float,
    ) -> str:
        normalized_symbol = EsunMarketDataProvider._validate_symbol_timeout(
            symbol, timeout_seconds
        )
        if start_date > end_date:
            raise ProviderInvalidRequestError(
                "start_date must not be after end_date"
            )
        return normalized_symbol

    @staticmethod
    def _validate_symbol_timeout(symbol: str, timeout_seconds: float) -> str:
        normalized_symbol = symbol.strip()
        if not _COMMON_STOCK_CODE.fullmatch(normalized_symbol):
            raise ProviderInvalidRequestError(
                "E.SUN Phase 5 accepts four-digit listed stock codes only"
            )
        if timeout_seconds <= 0:
            raise ProviderInvalidRequestError(
                "timeout_seconds must be greater than zero"
            )
        return normalized_symbol

    @staticmethod
    def _text(payload: Mapping[str, object], key: str) -> str:
        if key not in payload or not isinstance(payload[key], str):
            raise ProviderInvalidPayloadError(
                f"E.SUN field {key} must be a string"
            )
        value = payload[key].strip()
        if not value:
            raise ProviderInvalidPayloadError(
                f"E.SUN field {key} must not be blank"
            )
        return value

    @staticmethod
    def _number(payload: Mapping[str, object], key: str) -> float:
        if key not in payload:
            raise ProviderInvalidPayloadError(f"E.SUN payload is missing {key}")
        value = payload[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ProviderInvalidPayloadError(f"E.SUN field {key} must be numeric")
        number = float(value)
        if not math.isfinite(number):
            raise ProviderInvalidPayloadError(f"E.SUN field {key} must be finite")
        return number

    @staticmethod
    def _integer(payload: Mapping[str, object], key: str) -> int:
        if key not in payload:
            raise ProviderInvalidPayloadError(f"E.SUN payload is missing {key}")
        value = payload[key]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ProviderInvalidPayloadError(f"E.SUN field {key} must be an integer")
        return value

    @staticmethod
    def _parse_date(payload: Mapping[str, object], key: str) -> date:
        value = EsunMarketDataProvider._text(payload, key)
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ProviderInvalidPayloadError(
                f"E.SUN field {key} must use yyyy-MM-dd"
            ) from error
