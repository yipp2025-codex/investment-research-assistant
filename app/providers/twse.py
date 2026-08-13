"""Read-only Taiwan Stock Exchange OpenAPI market-data provider."""

from __future__ import annotations

import json
import re
import socket
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Protocol

from .base import (
    MarketDataBatch,
    MarketDataProvider,
    ProviderError,
    ProviderInvalidPayloadError,
    ProviderInvalidRequestError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)
from .artifacts import source_artifact_from_bytes
from .http_limits import read_bounded_body, remaining_timeout


TWSE_BASE_URL = "https://openapi.twse.com.tw/v1"
STOCK_DAY_ALL_PATH = "/exchangeReport/STOCK_DAY_ALL"
BWIBBU_ALL_PATH = "/exchangeReport/BWIBBU_ALL"
STOCK_DAY_ALL_URL = TWSE_BASE_URL + STOCK_DAY_ALL_PATH
BWIBBU_ALL_URL = TWSE_BASE_URL + BWIBBU_ALL_PATH

_COMMON_STOCK_CODE = re.compile(r"^[0-9]{4}$")
_ROC_DATE = re.compile(r"^[0-9]{7}$")
_DECIMAL = re.compile(r"^[+-]?[0-9]+(?:\.[0-9]+)?$")
_UNSIGNED_INTEGER = re.compile(r"^[0-9]+$")

_STOCK_DAY_FIELDS = frozenset(
    {
        "Date",
        "Code",
        "Name",
        "TradeVolume",
        "TradeValue",
        "OpeningPrice",
        "HighestPrice",
        "LowestPrice",
        "ClosingPrice",
        "Change",
        "Transaction",
    }
)
_BWIBBU_FIELDS = frozenset(
    {"Date", "Code", "Name", "PEratio", "DividendYield", "PBratio"}
)


@dataclass(frozen=True, slots=True)
class TwseHttpResponse:
    status_code: int
    body: bytes
    headers: Mapping[str, str]
    effective_url: str | None = None


class TwseHttpTransport(Protocol):
    def get(self, url: str, *, timeout_seconds: float) -> TwseHttpResponse:
        """Perform exactly one read-only HTTP GET without retrying."""


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Expose redirects to the provider so it can enforce the frozen contract."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del req, fp, code, msg, headers, newurl
        return None


class UrllibTwseHttpTransport:
    """Single-attempt urllib transport using Python's OpenSSL-backed TLS stack."""

    def __init__(self) -> None:
        self._opener = urllib.request.build_opener(_NoRedirectHandler())

    def get(self, url: str, *, timeout_seconds: float) -> TwseHttpResponse:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "investment-research-assistant/0.3 read-only",
            },
            method="GET",
        )
        deadline = time.monotonic() + timeout_seconds
        try:
            with self._opener.open(
                request, timeout=remaining_timeout(deadline)
            ) as response:
                return TwseHttpResponse(
                    status_code=int(response.status),
                    body=read_bounded_body(response, deadline=deadline),
                    headers={key.lower(): value for key, value in response.headers.items()},
                    effective_url=response.geturl(),
                )
        except urllib.error.HTTPError as error:
            return TwseHttpResponse(
                status_code=int(error.code),
                body=read_bounded_body(error, deadline=deadline),
                headers={key.lower(): value for key, value in error.headers.items()},
                effective_url=error.geturl(),
            )
        except (TimeoutError, socket.timeout) as error:
            raise ProviderTimeoutError("TWSE OpenAPI request timed out") from error
        except urllib.error.URLError as error:
            if isinstance(error.reason, (TimeoutError, socket.timeout)):
                raise ProviderTimeoutError("TWSE OpenAPI request timed out") from error
            raise ProviderTemporaryError(
                "TWSE OpenAPI connection failed"
            ) from error
        except OSError as error:
            raise ProviderTemporaryError("TWSE OpenAPI transport failed") from error


class TwseMarketDataProvider(MarketDataProvider):
    """Map two official TWSE OpenAPI datasets into the shared provider envelope."""

    def __init__(
        self,
        transport: TwseHttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.transport = transport or UrllibTwseHttpTransport()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @property
    def source(self) -> str:
        return "twse"

    def fetch_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        timeout_seconds: float,
    ) -> MarketDataBatch:
        normalized_symbol = symbol.strip()
        if not _COMMON_STOCK_CODE.fullmatch(normalized_symbol):
            raise ProviderInvalidRequestError(
                "TWSE Phase 3 supports four-digit listed common-stock codes only"
            )
        if start_date > end_date:
            raise ProviderInvalidRequestError(
                "start_date must not be after end_date"
            )
        if timeout_seconds <= 0:
            raise ProviderInvalidRequestError(
                "timeout_seconds must be greater than zero"
            )

        stock_records, stock_response = self._request_records(
            STOCK_DAY_ALL_URL,
            required_fields=_STOCK_DAY_FIELDS,
            timeout_seconds=timeout_seconds,
        )
        metric_records, metric_response = self._request_records(
            BWIBBU_ALL_URL,
            required_fields=_BWIBBU_FIELDS,
            timeout_seconds=timeout_seconds,
        )
        stock_by_code = self._index_by_code(stock_records, STOCK_DAY_ALL_PATH)
        metric_by_code = self._index_by_code(metric_records, BWIBBU_ALL_PATH)

        stock_record = stock_by_code.get(normalized_symbol)
        metric_record = metric_by_code.get(normalized_symbol)
        if stock_record is None:
            raise ProviderInvalidRequestError(
                f"symbol {normalized_symbol} is absent from STOCK_DAY_ALL"
            )
        if metric_record is None:
            raise ProviderInvalidRequestError(
                f"symbol {normalized_symbol} is outside the Phase 3 listed "
                "common-stock intersection"
            )

        stock_name = stock_record["Name"].strip()
        metric_name = metric_record["Name"].strip()
        if not stock_name or stock_name != metric_name:
            raise ProviderInvalidPayloadError(
                "TWSE symbol name is blank or inconsistent across endpoints"
            )

        market_date = self._parse_roc_date(stock_record["Date"], "Date")
        metric_date = self._parse_roc_date(metric_record["Date"], "Date")
        if metric_date != market_date:
            raise ProviderInvalidPayloadError(
                "TWSE endpoints returned different market dates"
            )
        if market_date < start_date or market_date > end_date:
            raise ProviderInvalidRequestError(
                f"latest TWSE market date {market_date} is outside requested range "
                f"{start_date}..{end_date}"
            )

        trade_volume = self._parse_unsigned_integer(
            stock_record["TradeVolume"], "TradeVolume"
        )
        self._parse_unsigned_integer(stock_record["TradeValue"], "TradeValue")
        self._parse_unsigned_integer(stock_record["Transaction"], "Transaction")
        open_price = self._parse_decimal(stock_record["OpeningPrice"], "OpeningPrice")
        high_price = self._parse_decimal(stock_record["HighestPrice"], "HighestPrice")
        low_price = self._parse_decimal(stock_record["LowestPrice"], "LowestPrice")
        close_price = self._parse_decimal(stock_record["ClosingPrice"], "ClosingPrice")
        self._parse_decimal(stock_record["Change"], "Change")
        ohlc = (open_price, high_price, low_price, close_price)
        if all(value == 0 for value in ohlc):
            raise ProviderInvalidRequestError(
                f"symbol {normalized_symbol} has no canonical regular-lot OHLC "
                f"on {market_date}"
            )
        if any(value <= 0 for value in ohlc):
            raise ProviderInvalidPayloadError(
                "TWSE OpenAPI returned partial or non-positive OHLC"
            )

        metrics: list[Mapping[str, object]] = []
        for twse_field, canonical_name, unit in (
            ("PEratio", "price_earnings_ratio", "ratio"),
            ("DividendYield", "dividend_yield_pct", "%"),
            ("PBratio", "price_to_book_ratio", "ratio"),
        ):
            parsed_value = self._parse_optional_decimal(
                metric_record[twse_field], twse_field
            )
            if parsed_value is not None:
                metrics.append(
                    {
                        "symbol": normalized_symbol,
                        "metric_date": metric_date.isoformat(),
                        "name": canonical_name,
                        "value": parsed_value,
                        "unit": unit,
                    }
                )

        fetched_at = self.clock()
        if fetched_at.utcoffset() is None:
            raise ProviderInvalidPayloadError(
                "TWSE provider clock must return a timezone-aware timestamp"
            )
        return MarketDataBatch(
            source=self.source,
            symbol={
                "symbol": normalized_symbol,
                "name": stock_name,
                "market": "TWSE",
                "currency": "TWD",
                "is_active": True,
            },
            daily_prices=(
                {
                    "symbol": normalized_symbol,
                    "trade_date": market_date.isoformat(),
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": trade_volume,
                },
            ),
            company_metrics=tuple(metrics),
            source_endpoints=(STOCK_DAY_ALL_URL, BWIBBU_ALL_URL),
            fetched_at=fetched_at,
            market_date=market_date,
            source_artifacts=(
                source_artifact_from_bytes(
                    provider=self.source,
                    dataset="STOCK_DAY_ALL",
                    endpoint=STOCK_DAY_ALL_URL,
                    contract_version=self.manifest.contract_version,
                    body=stock_response.body,
                    headers=stock_response.headers,
                    fetched_at=fetched_at,
                ),
                source_artifact_from_bytes(
                    provider=self.source,
                    dataset="BWIBBU_ALL",
                    endpoint=BWIBBU_ALL_URL,
                    contract_version=self.manifest.contract_version,
                    body=metric_response.body,
                    headers=metric_response.headers,
                    fetched_at=fetched_at,
                ),
            ),
        )

    def _request_records(
        self,
        url: str,
        *,
        required_fields: frozenset[str],
        timeout_seconds: float,
    ) -> tuple[list[dict[str, str]], TwseHttpResponse]:
        try:
            response = self.transport.get(url, timeout_seconds=timeout_seconds)
        except ProviderError:
            raise
        except (TimeoutError, socket.timeout) as error:
            raise ProviderTimeoutError("TWSE OpenAPI request timed out") from error
        except OSError as error:
            raise ProviderTemporaryError("TWSE OpenAPI connection failed") from error

        if response.status_code == 429:
            raise ProviderTemporaryError("TWSE OpenAPI returned HTTP 429")
        if 500 <= response.status_code <= 599:
            raise ProviderTemporaryError(
                f"TWSE OpenAPI returned HTTP {response.status_code}"
            )
        if 400 <= response.status_code <= 499:
            raise ProviderPermanentError(
                f"TWSE OpenAPI returned HTTP {response.status_code}"
            )
        if response.status_code != 200:
            raise ProviderPermanentError(
                f"TWSE OpenAPI returned unexpected HTTP {response.status_code}"
            )

        content_type = response.headers.get("content-type", "").lower()
        if content_type and "json" not in content_type:
            raise ProviderInvalidPayloadError(
                "TWSE OpenAPI response content type is not JSON"
            )
        try:
            payload = json.loads(response.body.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProviderInvalidPayloadError(
                "TWSE OpenAPI returned malformed JSON"
            ) from error
        if not isinstance(payload, list) or not payload:
            raise ProviderInvalidPayloadError(
                "TWSE OpenAPI payload must be a non-empty record array"
            )

        records: list[dict[str, str]] = []
        for index, item in enumerate(payload):
            if not isinstance(item, dict):
                raise ProviderInvalidPayloadError(
                    f"TWSE payload item {index} must be an object"
                )
            missing = required_fields.difference(item)
            if missing:
                raise ProviderInvalidPayloadError(
                    f"TWSE payload item {index} is missing fields: "
                    + ", ".join(sorted(missing))
                )
            for field in required_fields:
                if not isinstance(item[field], str):
                    raise ProviderInvalidPayloadError(
                        f"TWSE payload field {field} must be a string"
                    )
            records.append({field: item[field] for field in required_fields})
        return records, response

    @staticmethod
    def _index_by_code(
        records: list[dict[str, str]], endpoint: str
    ) -> dict[str, dict[str, str]]:
        indexed: dict[str, dict[str, str]] = {}
        for record in records:
            code = record["Code"].strip()
            if not code:
                raise ProviderInvalidPayloadError(
                    f"TWSE endpoint {endpoint} returned a blank Code"
                )
            if code in indexed:
                raise ProviderInvalidPayloadError(
                    f"TWSE endpoint {endpoint} returned duplicate Code {code}"
                )
            indexed[code] = record
        return indexed

    @staticmethod
    def _parse_roc_date(value: str, field: str) -> date:
        normalized = value.strip()
        if not _ROC_DATE.fullmatch(normalized):
            raise ProviderInvalidPayloadError(
                f"TWSE field {field} must use verified ROC YYYMMDD format"
            )
        roc_year = int(normalized[:3])
        month = int(normalized[3:5])
        day = int(normalized[5:7])
        try:
            return date(roc_year + 1911, month, day)
        except ValueError as error:
            raise ProviderInvalidPayloadError(
                f"TWSE field {field} contains an invalid ROC date"
            ) from error

    @staticmethod
    def _parse_unsigned_integer(value: str, field: str) -> int:
        normalized = value.strip()
        if not _UNSIGNED_INTEGER.fullmatch(normalized):
            raise ProviderInvalidPayloadError(
                f"TWSE field {field} must be an unsigned integer string"
            )
        return int(normalized)

    @staticmethod
    def _parse_decimal(value: str, field: str) -> float:
        normalized = value.strip()
        if not _DECIMAL.fullmatch(normalized):
            raise ProviderInvalidPayloadError(
                f"TWSE field {field} must be a decimal string"
            )
        try:
            number = Decimal(normalized)
        except InvalidOperation as error:  # pragma: no cover - guarded by regex.
            raise ProviderInvalidPayloadError(
                f"TWSE field {field} must be a decimal string"
            ) from error
        if not number.is_finite():
            raise ProviderInvalidPayloadError(
                f"TWSE field {field} must be finite"
            )
        return float(number)

    @classmethod
    def _parse_optional_decimal(cls, value: str, field: str) -> float | None:
        if value.strip() == "":
            return None
        return cls._parse_decimal(value, field)
