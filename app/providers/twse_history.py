"""Official TWSE monthly historical OHLCV provider."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
import json
import re
import socket
import urllib.parse

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
from .http_limits import MAX_RETRY_AFTER_SECONDS
from .twse import TwseHttpResponse, TwseHttpTransport, UrllibTwseHttpTransport


TWSE_HISTORICAL_BASE_URL = (
    "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
)
TWSE_HISTORICAL_PAGE_URL = (
    "https://www.twse.com.tw/zh/trading/historical/stock-day.html"
)

_COMMON_STOCK_CODE = re.compile(r"^[0-9]{4}$")
_ROC_ROW_DATE = re.compile(r"^([0-9]{3})/([0-9]{2})/([0-9]{2})$")
_GROUPED_INTEGER = re.compile(r"^(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)$")
_GROUPED_DECIMAL = re.compile(
    r"^(?:[0-9]+(?:\.[0-9]+)?|[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)$"
)
_TITLE = re.compile(
    r"^[0-9]{3}年[0-9]{2}月\s+([0-9]{4})\s+(.+?)\s+各日成交資訊$"
)

_FIELDS = (
    "日期",
    "成交股數",
    "成交金額",
    "開盤價",
    "最高價",
    "最低價",
    "收盤價",
    "漲跌價差",
    "成交筆數",
    "註記",
)
_OFFICIAL_TWSE_HOSTS = frozenset(
    {"www.twse.com.tw", "wwwc.twse.com.tw", "openapi.twse.com.tw"}
)


class TwseHistoricalMarketDataProvider(MarketDataProvider):
    """Fetch exactly one official TWSE stock month per provider call."""

    def __init__(
        self,
        transport: TwseHttpTransport | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.transport = transport or UrllibTwseHttpTransport()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @property
    def source(self) -> str:
        return "twse-historical"

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
                "TWSE historical provider accepts four-digit stock codes only"
            )
        if start_date > end_date:
            raise ProviderInvalidRequestError(
                "start_date must not be after end_date"
            )
        if (start_date.year, start_date.month) != (end_date.year, end_date.month):
            raise ProviderInvalidRequestError(
                "TWSE historical provider accepts one calendar month per call"
            )
        if timeout_seconds <= 0:
            raise ProviderInvalidRequestError(
                "timeout_seconds must be greater than zero"
            )

        month_start = start_date.replace(day=1)
        query_date = month_start.strftime("%Y%m%d")
        url = TWSE_HISTORICAL_BASE_URL + "?" + urllib.parse.urlencode(
            {
                "response": "json",
                "date": query_date,
                "stockNo": normalized_symbol,
            }
        )
        payload, response, effective_url = self._request_payload(
            url, timeout_seconds=timeout_seconds
        )
        self._validate_envelope(payload, query_date=query_date)

        title_match = _TITLE.fullmatch(payload["title"].strip())
        if title_match is None:
            raise ProviderInvalidPayloadError(
                "TWSE historical title does not match the verified contract"
            )
        title_symbol, title_name = title_match.groups()
        if title_symbol != normalized_symbol or not title_name.strip():
            raise ProviderInvalidPayloadError(
                "TWSE historical title symbol/name is inconsistent"
            )

        daily_prices: list[dict[str, object]] = []
        seen_dates: set[date] = set()
        previous_date: date | None = None
        for index, row in enumerate(payload["data"]):
            if not isinstance(row, list) or len(row) != len(_FIELDS):
                raise ProviderInvalidPayloadError(
                    f"TWSE historical row {index} must contain 10 cells"
                )
            if any(not isinstance(cell, str) for cell in row):
                raise ProviderInvalidPayloadError(
                    f"TWSE historical row {index} cells must be strings"
                )
            trade_date = self._parse_roc_date(row[0])
            if (trade_date.year, trade_date.month) != (
                month_start.year,
                month_start.month,
            ):
                raise ProviderInvalidPayloadError(
                    "TWSE historical row date is outside the requested month"
                )
            if trade_date in seen_dates or (
                previous_date is not None and trade_date <= previous_date
            ):
                raise ProviderInvalidPayloadError(
                    "TWSE historical row dates must be unique and ascending"
                )
            seen_dates.add(trade_date)
            previous_date = trade_date

            volume = self._parse_grouped_integer(row[1], "成交股數")
            turnover = self._parse_grouped_integer(row[2], "成交金額")
            transaction_count = self._parse_grouped_integer(row[8], "成交筆數")
            raw_ohlc = tuple(cell.strip() for cell in row[3:7])
            if all(value == "--" for value in raw_ohlc):
                # STOCK_DAY aggregates regular, odd-lot, after-hours, and block
                # activity.  Official TWSE rules exclude odd-lot prices from
                # daily OHLC, so valid activity can exist without a canonical
                # regular-lot price.  Preserve the activity evidence in the raw
                # artifact hash, but do not synthesize a DailyPrice.
                continue
            if any(value == "--" for value in raw_ohlc):
                raise ProviderInvalidPayloadError(
                    "TWSE historical row must provide either all or no OHLC values"
                )
            open_price = self._parse_grouped_decimal(row[3], "開盤價")
            high_price = self._parse_grouped_decimal(row[4], "最高價")
            low_price = self._parse_grouped_decimal(row[5], "最低價")
            close_price = self._parse_grouped_decimal(row[6], "收盤價")

            if start_date <= trade_date <= end_date:
                daily_prices.append(
                    {
                        "symbol": normalized_symbol,
                        "trade_date": trade_date.isoformat(),
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume,
                    }
                )

        if payload["total"] != len(payload["data"]):
            raise ProviderInvalidPayloadError(
                "TWSE historical total does not match data row count"
            )
        if not daily_prices:
            raise ProviderInvalidRequestError(
                "TWSE historical month contains no trading data in requested range"
            )

        fetched_at = self.clock()
        if fetched_at.utcoffset() is None:
            raise ProviderInvalidPayloadError(
                "TWSE historical provider clock must be timezone-aware"
            )
        return MarketDataBatch(
            source=self.source,
            symbol={
                "symbol": normalized_symbol,
                "name": title_name.strip(),
                "market": "TWSE",
                "currency": "TWD",
                "is_active": True,
            },
            daily_prices=tuple(daily_prices),
            company_metrics=(),
            source_endpoints=tuple(dict.fromkeys((url, effective_url))),
            fetched_at=fetched_at,
            market_date=max(
                date.fromisoformat(str(item["trade_date"])) for item in daily_prices
            ),
            source_artifacts=(
                source_artifact_from_bytes(
                    provider=self.source,
                    dataset="STOCK_DAY",
                    endpoint=effective_url,
                    contract_version=self.manifest.contract_version,
                    body=response.body,
                    headers=response.headers,
                    fetched_at=fetched_at,
                ),
            ),
        )

    def _request_payload(
        self, url: str, *, timeout_seconds: float
    ) -> tuple[dict[str, object], TwseHttpResponse, str]:
        response = self._request_once(url, timeout_seconds=timeout_seconds)
        if response.status_code in {307, 308}:
            if not response.headers.get("location"):
                raise ProviderTemporaryError(
                    "TWSE historical endpoint returned an official pacing "
                    f"HTTP {response.status_code} response without Location",
                    retry_after_seconds=self._retry_after_seconds(response),
                )
            redirected_url = self._validated_redirect_target(url, response)
            retry_after = self._retry_after_seconds(response)
            if redirected_url == url or retry_after is not None:
                raise ProviderTemporaryError(
                    "TWSE historical endpoint returned a retryable official "
                    f"HTTP {response.status_code} redirect",
                    retry_after_seconds=retry_after,
                )
            response = self._request_once(
                redirected_url,
                timeout_seconds=timeout_seconds,
            )
            if response.status_code in {307, 308}:
                self._validated_redirect_target(redirected_url, response)
                raise ProviderTemporaryError(
                    "TWSE historical endpoint repeated an official redirect",
                    retry_after_seconds=self._retry_after_seconds(response),
                )

        effective_url = self._validated_effective_url(url, response)
        retry_after = self._retry_after_seconds(response)
        if response.status_code == 429:
            raise ProviderTemporaryError(
                "TWSE historical endpoint returned HTTP 429",
                retry_after_seconds=retry_after,
            )
        if 500 <= response.status_code <= 599:
            raise ProviderTemporaryError(
                f"TWSE historical endpoint returned HTTP {response.status_code}",
                retry_after_seconds=retry_after,
            )
        if 400 <= response.status_code <= 499:
            raise ProviderPermanentError(
                f"TWSE historical endpoint returned HTTP {response.status_code}"
            )
        if response.status_code != 200:
            raise ProviderPermanentError(
                "TWSE historical endpoint returned unexpected HTTP "
                f"{response.status_code}"
            )
        content_type = response.headers.get("content-type", "").lower()
        if content_type and "json" not in content_type:
            raise ProviderInvalidPayloadError(
                "TWSE historical content type is not JSON"
            )
        try:
            payload = json.loads(response.body.decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProviderInvalidPayloadError(
                "TWSE historical endpoint returned malformed JSON"
            ) from error
        if not isinstance(payload, dict):
            raise ProviderInvalidPayloadError(
                "TWSE historical payload must be an object"
            )
        return payload, response, effective_url

    def _request_once(
        self,
        url: str,
        *,
        timeout_seconds: float,
    ) -> TwseHttpResponse:
        try:
            return self.transport.get(url, timeout_seconds=timeout_seconds)
        except ProviderError:
            raise
        except (TimeoutError, socket.timeout) as error:
            raise ProviderTimeoutError(
                "TWSE historical request timed out"
            ) from error
        except OSError as error:
            raise ProviderTemporaryError(
                "TWSE historical connection failed"
            ) from error

    @staticmethod
    def _validated_redirect_target(
        requested_url: str,
        response: TwseHttpResponse,
    ) -> str:
        location = response.headers.get("location")
        if not location:
            raise ProviderInvalidPayloadError(
                "TWSE historical redirect is missing Location"
            )
        redirected_url = urllib.parse.urljoin(requested_url, location)
        TwseHistoricalMarketDataProvider._validate_official_url(
            requested_url,
            redirected_url,
        )
        return redirected_url

    @staticmethod
    def _validated_effective_url(
        requested_url: str,
        response: TwseHttpResponse,
    ) -> str:
        effective_url = response.effective_url or requested_url
        TwseHistoricalMarketDataProvider._validate_official_url(
            requested_url,
            effective_url,
        )
        return effective_url

    @staticmethod
    def _validate_official_url(requested_url: str, candidate_url: str) -> None:
        requested = urllib.parse.urlsplit(requested_url)
        candidate = urllib.parse.urlsplit(candidate_url)
        if (
            candidate.scheme != "https"
            or candidate.hostname not in _OFFICIAL_TWSE_HOSTS
        ):
            raise ProviderPermanentError(
                "TWSE historical redirect left the official HTTPS authority"
            )
        if requested.path != candidate.path:
            raise ProviderPermanentError(
                "TWSE historical redirect changed the endpoint path"
            )
        if urllib.parse.parse_qsl(
            requested.query,
            keep_blank_values=True,
        ) != urllib.parse.parse_qsl(candidate.query, keep_blank_values=True):
            raise ProviderPermanentError(
                "TWSE historical redirect changed the request query"
            )

    def _retry_after_seconds(
        self,
        response: TwseHttpResponse,
    ) -> float | None:
        value = response.headers.get("retry-after")
        if value is None:
            return None
        normalized = value.strip()
        if normalized.isdigit():
            return min(float(normalized), MAX_RETRY_AFTER_SECONDS)
        try:
            target = parsedate_to_datetime(normalized)
        except (TypeError, ValueError, OverflowError):
            return None
        if target.utcoffset() is None:
            target = target.replace(tzinfo=timezone.utc)
        return min(
            max(0.0, (target - self.clock()).total_seconds()),
            MAX_RETRY_AFTER_SECONDS,
        )

    @staticmethod
    def _validate_envelope(payload: dict[str, object], *, query_date: str) -> None:
        if payload.get("stat") != "OK" and payload.get("total") == 0:
            raise ProviderInvalidRequestError(
                "TWSE historical endpoint returned no usable data"
            )
        required = {"stat", "date", "title", "fields", "data", "notes", "total"}
        missing = required.difference(payload)
        if missing:
            raise ProviderInvalidPayloadError(
                "TWSE historical payload is missing fields: "
                + ", ".join(sorted(missing))
            )
        if payload["stat"] != "OK":
            raise ProviderInvalidPayloadError(
                "TWSE historical stat is not the verified success value"
            )
        if payload["date"] != query_date:
            raise ProviderInvalidPayloadError(
                "TWSE historical payload date does not match query month"
            )
        if payload["fields"] != list(_FIELDS):
            raise ProviderInvalidPayloadError(
                "TWSE historical fields do not match the verified contract"
            )
        if not isinstance(payload["title"], str):
            raise ProviderInvalidPayloadError(
                "TWSE historical title must be a string"
            )
        if not isinstance(payload["data"], list):
            raise ProviderInvalidPayloadError(
                "TWSE historical data must be an array"
            )
        if not isinstance(payload["notes"], list) or any(
            not isinstance(note, str) for note in payload["notes"]
        ):
            raise ProviderInvalidPayloadError(
                "TWSE historical notes must be a string array"
            )
        if not isinstance(payload["total"], int):
            raise ProviderInvalidPayloadError(
                "TWSE historical total must be an integer"
            )

    @staticmethod
    def _parse_roc_date(value: str) -> date:
        match = _ROC_ROW_DATE.fullmatch(value.strip())
        if match is None:
            raise ProviderInvalidPayloadError(
                "TWSE historical date must use ROC YYY/MM/DD format"
            )
        roc_year, month, day = (int(part) for part in match.groups())
        try:
            return date(roc_year + 1911, month, day)
        except ValueError as error:
            raise ProviderInvalidPayloadError(
                "TWSE historical row contains an invalid date"
            ) from error

    @staticmethod
    def _parse_grouped_integer(value: str, field: str) -> int:
        normalized = value.strip()
        if _GROUPED_INTEGER.fullmatch(normalized) is None:
            raise ProviderInvalidPayloadError(
                f"TWSE historical field {field} must be a grouped integer"
            )
        return int(normalized.replace(",", ""))

    @staticmethod
    def _parse_grouped_decimal(value: str, field: str) -> float:
        normalized = value.strip()
        if _GROUPED_DECIMAL.fullmatch(normalized) is None:
            raise ProviderInvalidPayloadError(
                f"TWSE historical field {field} must be a grouped decimal"
            )
        try:
            number = Decimal(normalized.replace(",", ""))
        except InvalidOperation as error:  # pragma: no cover - regex guarded.
            raise ProviderInvalidPayloadError(
                f"TWSE historical field {field} must be numeric"
            ) from error
        if not number.is_finite():
            raise ProviderInvalidPayloadError(
                f"TWSE historical field {field} must be finite"
            )
        return float(number)
