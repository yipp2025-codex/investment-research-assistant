import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from app.providers import (
    ProviderInvalidPayloadError,
    ProviderInvalidRequestError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
    TwseHistoricalMarketDataProvider,
)
from app.providers.twse import TwseHttpResponse


FIXTURE = Path("tests/fixtures/twse_history/stock_day_2330_202604.json")
NO_PRICE_FIXTURE = Path(
    "tests/fixtures/twse_history/stock_day_2317_202507_no_price.json"
)
FETCHED_AT = datetime(2026, 8, 5, 2, 3, 4, tzinfo=timezone.utc)


def _response(
    body: bytes | None = None,
    status: int = 200,
    *,
    headers: dict[str, str] | None = None,
    effective_url: str | None = None,
) -> TwseHttpResponse:
    return TwseHttpResponse(
        status_code=status,
        body=FIXTURE.read_bytes() if body is None else body,
        headers=headers or {"content-type": "application/json; charset=utf-8"},
        effective_url=effective_url,
    )


class StubTransport:
    def __init__(self, outcome=None) -> None:
        self.outcome = _response() if outcome is None else outcome
        self.calls: list[tuple[str, float]] = []

    def get(self, url: str, *, timeout_seconds: float) -> TwseHttpResponse:
        self.calls.append((url, timeout_seconds))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


class SequenceTransport:
    def __init__(self, *outcomes: TwseHttpResponse) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, float]] = []

    def get(self, url: str, *, timeout_seconds: float) -> TwseHttpResponse:
        self.calls.append((url, timeout_seconds))
        return self.outcomes.pop(0)


def _provider(transport=None) -> TwseHistoricalMarketDataProvider:
    return TwseHistoricalMarketDataProvider(
        transport=transport or StubTransport(), clock=lambda: FETCHED_AT
    )


def test_twse_historical_official_month_maps_to_canonical_batch() -> None:
    transport = StubTransport()
    batch = _provider(transport).fetch_market_data(
        "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=2.5
    )

    assert batch.source == "twse-historical"
    assert batch.symbol == {
        "symbol": "2330",
        "name": "台積電",
        "market": "TWSE",
        "currency": "TWD",
        "is_active": True,
    }
    assert len(batch.daily_prices) == 20
    assert batch.daily_prices[0] == {
        "symbol": "2330",
        "trade_date": "2026-04-01",
        "open": 1840.0,
        "high": 1855.0,
        "low": 1830.0,
        "close": 1855.0,
        "volume": 46_457_423,
    }
    assert batch.daily_prices[-1]["trade_date"] == "2026-04-30"
    assert batch.market_date == date(2026, 4, 30)
    assert batch.fetched_at == FETCHED_AT
    assert "date=20260401" in batch.source_endpoints[0]
    assert "stockNo=2330" in batch.source_endpoints[0]
    assert transport.calls[0][1] == 2.5
    assert len(batch.source_artifacts) == 1
    artifact = batch.source_artifacts[0]
    assert artifact.provider == "twse-historical"
    assert artifact.dataset == "STOCK_DAY"
    assert artifact.endpoint == batch.source_endpoints[0]
    assert artifact.payload_sha256 == hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    assert artifact.hash_basis == "raw-response-bytes-v1"
    assert artifact.fetched_at == FETCHED_AT


def test_twse_historical_filters_requested_subrange() -> None:
    batch = _provider().fetch_market_data(
        "2330", date(2026, 4, 20), date(2026, 4, 24), timeout_seconds=1.0
    )

    assert [row["trade_date"] for row in batch.daily_prices] == [
        "2026-04-20",
        "2026-04-21",
        "2026-04-22",
        "2026-04-23",
        "2026-04-24",
    ]


def test_twse_historical_skips_verified_no_price_rows() -> None:
    batch = _provider(
        StubTransport(_response(NO_PRICE_FIXTURE.read_bytes()))
    ).fetch_market_data(
        "2317", date(2025, 7, 1), date(2025, 7, 31), timeout_seconds=1.0
    )

    assert [row["trade_date"] for row in batch.daily_prices] == [
        "2025-07-29",
        "2025-07-31",
    ]
    assert batch.market_date == date(2025, 7, 31)


def test_twse_historical_skips_nonzero_odd_lot_activity_without_ohlc() -> None:
    payload = json.loads(NO_PRICE_FIXTURE.read_bytes())
    payload["data"][1][1] = "2"
    payload["data"][1][2] = "14"
    payload["data"][1][8] = "1"

    batch = _provider(
        StubTransport(_response(json.dumps(payload).encode("utf-8")))
    ).fetch_market_data(
        "2317", date(2025, 7, 1), date(2025, 7, 31), timeout_seconds=1.0
    )

    assert [row["trade_date"] for row in batch.daily_prices] == [
        "2025-07-29",
        "2025-07-31",
    ]


def test_twse_historical_rejects_partial_ohlc() -> None:
    payload = json.loads(NO_PRICE_FIXTURE.read_bytes())
    payload["data"][0][3] = "--"

    with pytest.raises(ProviderInvalidPayloadError):
        _provider(
            StubTransport(_response(json.dumps(payload).encode("utf-8")))
        ).fetch_market_data(
            "2317", date(2025, 7, 1), date(2025, 7, 31), timeout_seconds=1.0
        )


def test_twse_historical_rejects_multi_month_request() -> None:
    with pytest.raises(ProviderInvalidRequestError, match="one calendar month"):
        _provider().fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 5, 1), timeout_seconds=1.0
        )


@pytest.mark.parametrize("mutation", ["fields", "row_width", "number", "date"])
def test_twse_historical_rejects_schema_or_numeric_mismatch(mutation) -> None:
    payload = json.loads(FIXTURE.read_bytes())
    if mutation == "fields":
        payload["fields"][1] = "交易量"
    elif mutation == "row_width":
        payload["data"][0].pop()
    elif mutation == "number":
        payload["data"][0][3] = "1 840.00"
    else:
        payload["data"][0][0] = "2026-04-01"
    transport = StubTransport(_response(json.dumps(payload).encode("utf-8")))

    with pytest.raises(ProviderInvalidPayloadError):
        _provider(transport).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )


@pytest.mark.parametrize("body", [b"{", b"[]"])
def test_twse_historical_rejects_malformed_json_or_top_level_shape(body) -> None:
    with pytest.raises(ProviderInvalidPayloadError):
        _provider(StubTransport(_response(body))).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )


def test_twse_historical_rejects_missing_required_field() -> None:
    payload = json.loads(FIXTURE.read_bytes())
    del payload["notes"]

    with pytest.raises(ProviderInvalidPayloadError, match="missing fields"):
        _provider(
            StubTransport(_response(json.dumps(payload).encode("utf-8")))
        ).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )


def test_twse_historical_maps_timeout() -> None:
    with pytest.raises(ProviderTimeoutError):
        _provider(StubTransport(TimeoutError("timeout"))).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=0.1
        )


@pytest.mark.parametrize("status", [429, 500, 503])
def test_twse_historical_maps_retryable_http_status(status) -> None:
    with pytest.raises(ProviderTemporaryError, match=str(status)):
        _provider(StubTransport(_response(b"{}", status=status))).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )


def test_twse_historical_honors_retry_after_on_rate_limit() -> None:
    with pytest.raises(ProviderTemporaryError) as captured:
        _provider(
            StubTransport(
                _response(
                    b"{}",
                    status=429,
                    headers={
                        "content-type": "application/json",
                        "retry-after": "7",
                    },
                )
            )
        ).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )

    assert captured.value.retry_after_seconds == 7.0


def test_twse_historical_follows_one_official_semantics_preserving_redirect() -> None:
    redirected_url = (
        "https://wwwc.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
        "?response=json&date=20260401&stockNo=2330"
    )
    transport = SequenceTransport(
        _response(
            b"",
            status=307,
            headers={"location": redirected_url},
        ),
        _response(effective_url=redirected_url),
    )

    batch = _provider(transport).fetch_market_data(
        "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
    )

    assert len(transport.calls) == 2
    assert transport.calls[1][0] == redirected_url
    assert batch.source_endpoints[1] == redirected_url
    assert batch.source_artifacts[0].endpoint == redirected_url


def test_twse_historical_treats_same_url_307_as_retryable() -> None:
    transport = StubTransport(
        _response(
            b"",
            status=307,
            headers={
                "location": (
                    "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
                    "?response=json&date=20260401&stockNo=2330"
                ),
                "retry-after": "4",
            },
        )
    )

    with pytest.raises(ProviderTemporaryError) as captured:
        _provider(transport).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )

    assert captured.value.retry_after_seconds == 4.0
    assert len(transport.calls) == 1


@pytest.mark.parametrize(
    "location,error_type",
    [
        (
            "https://example.com/rwd/zh/afterTrading/STOCK_DAY"
            "?response=json&date=20260401&stockNo=2330",
            ProviderPermanentError,
        ),
        (
            "https://www.twse.com.tw/rwd/zh/afterTrading/OTHER"
            "?response=json&date=20260401&stockNo=2330",
            ProviderPermanentError,
        ),
        (
            "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
            "?response=json&date=20260401&stockNo=1213",
            ProviderPermanentError,
        ),
    ],
)
def test_twse_historical_rejects_unsafe_redirect(location, error_type) -> None:
    headers = {} if location is None else {"location": location}

    with pytest.raises(error_type):
        _provider(StubTransport(_response(b"", status=307, headers=headers))).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )


def test_twse_historical_maps_locationless_official_307_to_temporary_pacing() -> None:
    with pytest.raises(ProviderTemporaryError, match="pacing"):
        _provider(
            StubTransport(
                _response(
                    b"<html></html>",
                    status=307,
                    headers={"content-type": "text/html; charset=UTF-8"},
                )
            )
        ).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )


@pytest.mark.parametrize("status", [400, 403, 404])
def test_twse_historical_maps_permanent_http_status(status) -> None:
    with pytest.raises(ProviderPermanentError, match=str(status)):
        _provider(StubTransport(_response(b"{}", status=status))).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )


def test_twse_historical_rejects_official_no_data_response() -> None:
    body = json.dumps(
        {"stat": "很抱歉，沒有符合條件的資料!", "total": 0},
        ensure_ascii=False,
    ).encode("utf-8")

    with pytest.raises(ProviderInvalidRequestError, match="no usable data"):
        _provider(StubTransport(_response(body))).fetch_market_data(
            "2330", date(2026, 4, 1), date(2026, 4, 30), timeout_seconds=1.0
        )
