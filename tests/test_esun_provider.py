import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from app.models import PipelineRunStatus
from app.pipelines import DailyResearchPipeline, RetryPolicy
from app.providers import (
    EsunHttpResponse,
    EsunHistoricalMarketDataProvider,
    EsunMarketDataProvider,
    EsunSdkHttpTransport,
    MarketDataProvider,
    ProviderInvalidPayloadError,
    ProviderInvalidRequestError,
    ProviderNotImplementedError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)
from app.providers.esun import (
    HISTORICAL_CANDLES_PATH,
    HISTORICAL_STATS_PATH,
    INTRADAY_QUOTE_PATH,
    INTRADAY_TICKER_PATH,
    SNAPSHOT_QUOTES_PATH,
)
from app.storage import SQLiteResearchRepository


FIXTURE_ROOT = Path("tests/fixtures/esun")
START = date(2026, 8, 3)
MARKET_DATE = date(2026, 8, 5)
FETCHED_AT = datetime(2026, 8, 6, 6, 0, tzinfo=timezone.utc)


def _fixture_bytes(name: str) -> bytes:
    return (FIXTURE_ROOT / name).read_bytes()


def _response(
    path: str,
    body: bytes,
    *,
    status: int = 200,
    content_type: str = "application/json; charset=utf-8",
) -> EsunHttpResponse:
    return EsunHttpResponse(
        status_code=status,
        body=body,
        headers={"content-type": content_type},
        url="https://api.fugle.tw/marketdata/v1.0/stock" + path,
        fetched_at=FETCHED_AT,
    )


class StubTransport:
    def __init__(self, outcomes=None) -> None:
        self.outcomes = outcomes or {
            INTRADAY_TICKER_PATH.format(symbol="2330"): [
                _response(
                    INTRADAY_TICKER_PATH.format(symbol="2330"),
                    _fixture_bytes("ticker_2330_20260806.json"),
                )
            ],
            HISTORICAL_CANDLES_PATH.format(symbol="2330"): [
                _response(
                    HISTORICAL_CANDLES_PATH.format(symbol="2330"),
                    _fixture_bytes("historical_candles_2330_20260805.json"),
                )
            ],
        }
        self.calls: list[tuple[str, dict[str, object], float]] = []

    def get(self, path, *, params, timeout_seconds):
        self.calls.append((path, dict(params or {}), timeout_seconds))
        queue = self.outcomes[path]
        outcome = queue[0] if len(queue) == 1 else queue.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _provider(transport: StubTransport | None = None) -> EsunMarketDataProvider:
    return EsunMarketDataProvider(transport=transport or StubTransport())


def test_esun_official_payload_maps_to_canonical_envelope() -> None:
    provider = _provider()

    batch = provider.fetch_market_data(
        "2330", START, MARKET_DATE, timeout_seconds=2.5
    )

    assert isinstance(provider, MarketDataProvider)
    assert batch.source == "esun"
    assert batch.symbol == {
        "symbol": "2330",
        "name": "台積電",
        "market": "TWSE",
        "currency": "TWD",
        "is_active": True,
    }
    assert [row["trade_date"] for row in batch.daily_prices] == [
        "2026-08-03",
        "2026-08-04",
        "2026-08-05",
    ]
    latest = batch.daily_prices[-1]
    assert latest == {
        "symbol": "2330",
        "trade_date": "2026-08-05",
        "open": 2385.0,
        "high": 2415.0,
        "low": 2370.0,
        "close": 2405.0,
        "volume": 36_782_301,
    }
    assert batch.company_metrics == ()
    assert batch.fetched_at == FETCHED_AT
    assert batch.market_date == MARKET_DATE
    assert len(batch.source_endpoints) == 2
    assert all(endpoint.startswith("https://api.fugle.tw/") for endpoint in batch.source_endpoints)
    artifacts = {artifact.dataset: artifact for artifact in batch.source_artifacts}
    assert set(artifacts) == {"intraday-ticker", "historical-candles"}
    assert artifacts["intraday-ticker"].payload_sha256 == hashlib.sha256(
        _fixture_bytes("ticker_2330_20260806.json")
    ).hexdigest()
    assert artifacts["historical-candles"].payload_sha256 == hashlib.sha256(
        _fixture_bytes("historical_candles_2330_20260805.json")
    ).hexdigest()
    assert all(
        artifact.provider == "esun"
        and artifact.hash_basis == "raw-response-bytes-v1"
        and artifact.fetched_at == FETCHED_AT
        for artifact in artifacts.values()
    )


def test_esun_historical_adapter_reuses_mapping_with_month_contract() -> None:
    provider = EsunHistoricalMarketDataProvider(transport=StubTransport())

    batch = provider.fetch_market_data(
        "2330", START, MARKET_DATE, timeout_seconds=2.0
    )

    assert batch.source == "esun-historical"
    assert len(batch.daily_prices) == 3
    assert batch.market_date == MARKET_DATE
    assert len(batch.source_endpoints) == 2
    assert {artifact.provider for artifact in batch.source_artifacts} == {
        "esun-historical"
    }
    assert {artifact.dataset for artifact in batch.source_artifacts} == {
        "intraday-ticker",
        "historical-candles",
    }


def test_esun_historical_adapter_rejects_cross_month_request() -> None:
    transport = StubTransport()
    provider = EsunHistoricalMarketDataProvider(transport=transport)

    with pytest.raises(ProviderInvalidRequestError, match="one calendar month"):
        provider.fetch_market_data(
            "2330", date(2026, 7, 31), MARKET_DATE, timeout_seconds=1.0
        )

    assert transport.calls == []


def test_esun_request_uses_documented_daily_candle_parameters() -> None:
    transport = StubTransport()

    _provider(transport).fetch_market_data(
        "2330", START, MARKET_DATE, timeout_seconds=3.0
    )

    assert transport.calls == [
        (INTRADAY_TICKER_PATH.format(symbol="2330"), {}, 3.0),
        (
            HISTORICAL_CANDLES_PATH.format(symbol="2330"),
            {
                "from": "2026-08-03",
                "to": "2026-08-05",
                "timeframe": "D",
                "fields": "open,high,low,close,volume,turnover,change",
            },
            3.0,
        ),
    ]


@pytest.mark.parametrize("symbol", ["2330A", "abc", ""])
def test_esun_rejects_non_common_stock_code_before_http(symbol) -> None:
    transport = StubTransport()

    with pytest.raises(ProviderInvalidRequestError, match="four-digit"):
        _provider(transport).fetch_market_data(
            symbol, START, MARKET_DATE, timeout_seconds=1.0
        )

    assert transport.calls == []


def test_esun_uses_ticker_contract_to_reject_four_digit_etf() -> None:
    ticker = json.loads(_fixture_bytes("ticker_2330_20260806.json"))
    ticker.update({"symbol": "0050", "name": "synthetic ETF label", "securityType": "04"})
    path = INTRADAY_TICKER_PATH.format(symbol="0050")
    transport = StubTransport(
        {path: [_response(path, json.dumps(ticker).encode("utf-8"))]}
    )

    with pytest.raises(ProviderInvalidRequestError, match="common-stock"):
        _provider(transport).fetch_market_data(
            "0050", START, MARKET_DATE, timeout_seconds=1.0
        )


def test_esun_rejects_ticker_identity_outside_listed_common_stock_scope() -> None:
    ticker = json.loads(_fixture_bytes("ticker_2330_20260806.json"))
    ticker["securityType"] = "04"
    path = INTRADAY_TICKER_PATH.format(symbol="2330")
    transport = StubTransport(
        {path: [_response(path, json.dumps(ticker).encode("utf-8"))]}
    )

    with pytest.raises(ProviderInvalidRequestError, match="common-stock"):
        _provider(transport).fetch_market_data(
            "2330", START, MARKET_DATE, timeout_seconds=1.0
        )


def test_esun_quote_preserves_raw_volume_and_verified_source_timestamp() -> None:
    path = INTRADAY_QUOTE_PATH.format(symbol="2330")
    provider = _provider(
        StubTransport(
            {path: [_response(path, _fixture_bytes("quote_2330_20260806.json"))]}
        )
    )

    quote = provider.fetch_quote("2330", timeout_seconds=1.0)

    assert quote.market_date == date(2026, 8, 6)
    assert (quote.open, quote.high, quote.low, quote.close) == (
        2395.0,
        2395.0,
        2360.0,
        2365.0,
    )
    assert quote.volume_raw == 22_760
    assert quote.source_timestamp_raw == "1785994200000000"
    assert quote.source_timestamp == datetime(
        2026, 8, 6, 5, 30, tzinfo=timezone.utc
    )
    assert quote.is_close is True


def test_esun_quote_maps_documented_null_semantics_without_inventing_zero() -> None:
    payload = json.loads(_fixture_bytes("quote_2330_20260806.json"))
    for key in ("openPrice", "highPrice", "lowPrice", "closePrice"):
        payload[key] = None
    payload["lastUpdated"] = None
    payload["total"] = {"tradeVolume": None}
    path = INTRADAY_QUOTE_PATH.format(symbol="2330")
    provider = _provider(
        StubTransport(
            {path: [_response(path, json.dumps(payload).encode("utf-8"))]}
        )
    )

    quote = provider.fetch_quote("2330", timeout_seconds=1.0)

    assert (quote.open, quote.high, quote.low, quote.close) == (None,) * 4
    assert quote.volume_raw is None
    assert quote.source_timestamp is None


def test_esun_quote_rejects_partial_ohlc_nulls() -> None:
    payload = json.loads(_fixture_bytes("quote_2330_20260806.json"))
    payload["openPrice"] = None
    path = INTRADAY_QUOTE_PATH.format(symbol="2330")

    with pytest.raises(ProviderInvalidPayloadError, match="all or no OHLC"):
        _provider(
            StubTransport(
                {path: [_response(path, json.dumps(payload).encode("utf-8"))]}
            )
        ).fetch_quote("2330", timeout_seconds=1.0)


def test_esun_historical_stats_mapping() -> None:
    path = HISTORICAL_STATS_PATH.format(symbol="2330")
    stats = _provider(
        StubTransport(
            {
                path: [
                    _response(
                        path,
                        _fixture_bytes("historical_stats_2330_20260805.json"),
                    )
                ]
            }
        )
    ).fetch_historical_stats("2330", timeout_seconds=1.0)

    assert stats.market_date == MARKET_DATE
    assert stats.close == 2405.0
    assert stats.volume == 36_782_301
    assert stats.turnover == 88_157_683_613.0
    assert stats.previous_close == 2320.0


def test_esun_snapshot_basic_plan_403_is_permanent_not_empty() -> None:
    path = SNAPSHOT_QUOTES_PATH.format(market="TSE")
    provider = _provider(
        StubTransport(
            {
                path: [
                    _response(
                        path,
                        _fixture_bytes("snapshot_forbidden_basic_20260806.json"),
                        status=403,
                    )
                ]
            }
        )
    )

    with pytest.raises(ProviderPermanentError, match="403"):
        provider.fetch_snapshot_quotes(
            market="TSE", symbols=("2330",), timeout_seconds=1.0
        )


def test_esun_snapshot_success_mapping_is_fail_closed_until_live_verified() -> None:
    path = SNAPSHOT_QUOTES_PATH.format(market="TSE")
    provider = _provider(
        StubTransport({path: [_response(path, b'{"data":[]}')]})
    )

    with pytest.raises(ProviderNotImplementedError, match="live payload fixture"):
        provider.fetch_snapshot_quotes(market="TSE", timeout_seconds=1.0)


@pytest.mark.parametrize("body", [b"not-json", b"[]", b"null"])
def test_esun_rejects_malformed_json_or_top_level_shape(body) -> None:
    path = INTRADAY_TICKER_PATH.format(symbol="2330")
    transport = StubTransport({path: [_response(path, body)]})

    with pytest.raises(ProviderInvalidPayloadError):
        _provider(transport).fetch_market_data(
            "2330", START, MARKET_DATE, timeout_seconds=1.0
        )


@pytest.mark.parametrize("mutation", ["missing", "wrong_type", "null"])
def test_esun_rejects_required_historical_field_mismatch(mutation) -> None:
    payload = json.loads(_fixture_bytes("historical_candles_2330_20260805.json"))
    row = payload["data"][0]
    if mutation == "missing":
        row.pop("close")
    elif mutation == "wrong_type":
        row["volume"] = "36782301"
    else:
        row["open"] = None
    ticker_path = INTRADAY_TICKER_PATH.format(symbol="2330")
    candle_path = HISTORICAL_CANDLES_PATH.format(symbol="2330")
    transport = StubTransport(
        {
            ticker_path: [
                _response(ticker_path, _fixture_bytes("ticker_2330_20260806.json"))
            ],
            candle_path: [
                _response(candle_path, json.dumps(payload).encode("utf-8"))
            ],
        }
    )

    with pytest.raises(ProviderInvalidPayloadError):
        _provider(transport).fetch_market_data(
            "2330", START, MARKET_DATE, timeout_seconds=1.0
        )


def test_esun_maps_connection_timeout() -> None:
    path = INTRADAY_TICKER_PATH.format(symbol="2330")
    transport = StubTransport({path: [TimeoutError("timed out")]})

    with pytest.raises(ProviderTimeoutError):
        _provider(transport).fetch_market_data(
            "2330", START, MARKET_DATE, timeout_seconds=0.1
        )


@pytest.mark.parametrize("status", [429, 500, 503])
def test_esun_maps_429_and_5xx_to_temporary_failure(status) -> None:
    path = INTRADAY_TICKER_PATH.format(symbol="2330")
    transport = StubTransport({path: [_response(path, b"{}", status=status)]})

    with pytest.raises(ProviderTemporaryError, match=str(status)):
        _provider(transport).fetch_market_data(
            "2330", START, MARKET_DATE, timeout_seconds=1.0
        )


@pytest.mark.parametrize("status", [400, 403, 404])
def test_esun_maps_4xx_to_permanent_failure(status) -> None:
    path = INTRADAY_TICKER_PATH.format(symbol="2330")
    transport = StubTransport({path: [_response(path, b"{}", status=status)]})

    with pytest.raises(ProviderPermanentError, match=str(status)):
        _provider(transport).fetch_market_data(
            "2330", START, MARKET_DATE, timeout_seconds=1.0
        )


def test_esun_rejects_non_json_content_type() -> None:
    path = INTRADAY_TICKER_PATH.format(symbol="2330")
    transport = StubTransport(
        {path: [_response(path, b"{}", content_type="text/html")]}
    )

    with pytest.raises(ProviderInvalidPayloadError, match="content type"):
        _provider(transport).fetch_market_data(
            "2330", START, MARKET_DATE, timeout_seconds=1.0
        )


def test_esun_temporary_failure_retries_only_in_phase2_pipeline(tmp_path) -> None:
    ticker_path = INTRADAY_TICKER_PATH.format(symbol="2330")
    candle_path = HISTORICAL_CANDLES_PATH.format(symbol="2330")
    transport = StubTransport(
        {
            ticker_path: [
                _response(ticker_path, _fixture_bytes("ticker_2330_20260806.json"))
            ],
            candle_path: [
                _response(candle_path, b"{}", status=429),
                _response(
                    candle_path,
                    _fixture_bytes("historical_candles_2330_20260805.json"),
                ),
            ],
        }
    )
    delays: list[float] = []
    pipeline = DailyResearchPipeline(
        _provider(transport),
        SQLiteResearchRepository(tmp_path / "esun.db"),
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.25),
        sleep=delays.append,
    )

    result = pipeline.run("2330", START, MARKET_DATE)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert result.provider_attempts == 2
    assert delays == [0.25]
    assert [call[0] for call in transport.calls] == [
        ticker_path,
        candle_path,
        ticker_path,
        candle_path,
    ]


def test_esun_pipeline_to_sqlite_summary_is_idempotent(tmp_path) -> None:
    transport = StubTransport()
    repository = SQLiteResearchRepository(tmp_path / "esun.db")
    pipeline = DailyResearchPipeline(_provider(transport), repository)

    first = pipeline.run("2330", START, MARKET_DATE)
    second = pipeline.run("2330", START, MARKET_DATE)

    assert first.run_status is PipelineRunStatus.SUCCESS
    assert first.provider_source == "esun"
    assert first.market_date == MARKET_DATE
    assert first.analysis.observations == 3
    assert "2330" in first.summary
    assert len(repository.list_daily_prices("2330")) == 3
    assert len(repository.list_research_notes("2330")) == 1
    assert second.idempotent_replay is True
    assert second.run_id == first.run_id
    assert second.daily_prices_written == 0
    assert len(transport.calls) == 2


def test_esun_sdk_transport_missing_config_fails_without_exposing_path(
    tmp_path,
) -> None:
    missing = tmp_path / "do-not-log-this-name.ini"
    transport = EsunSdkHttpTransport(missing)

    with pytest.raises(ProviderPermanentError) as caught:
        transport.get("/intraday/ticker/2330", params=None, timeout_seconds=1.0)

    assert str(missing) not in str(caught.value)
