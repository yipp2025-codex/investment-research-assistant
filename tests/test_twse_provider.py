import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from app.models import PipelineRunStatus
from app.pipelines import DailyResearchPipeline, RetryPolicy
from app.providers import (
    MarketDataProvider,
    ProviderInvalidPayloadError,
    ProviderInvalidRequestError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
    TwseMarketDataProvider,
)
from app.providers.twse import (
    BWIBBU_ALL_URL,
    STOCK_DAY_ALL_URL,
    TwseHttpResponse,
)
from app.storage import SQLiteResearchRepository


FIXTURE_ROOT = Path("tests/fixtures/twse")
MARKET_DATE = date(2026, 8, 4)
FETCHED_AT = datetime(2026, 8, 5, 1, 2, 3, tzinfo=timezone.utc)


def _fixture_bytes(name: str) -> bytes:
    return (FIXTURE_ROOT / name).read_bytes()


def _json_response(body: bytes, status: int = 200) -> TwseHttpResponse:
    return TwseHttpResponse(
        status_code=status,
        body=body,
        headers={"content-type": "application/json; charset=utf-8"},
    )


class StubTransport:
    def __init__(self, outcomes=None) -> None:
        self.outcomes = outcomes or {
            STOCK_DAY_ALL_URL: [_json_response(_fixture_bytes("stock_day_all.json"))],
            BWIBBU_ALL_URL: [_json_response(_fixture_bytes("bwibbu_all.json"))],
        }
        self.calls: list[tuple[str, float]] = []

    def get(self, url: str, *, timeout_seconds: float) -> TwseHttpResponse:
        self.calls.append((url, timeout_seconds))
        queue = self.outcomes[url]
        outcome = queue[0] if len(queue) == 1 else queue.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _provider(transport: StubTransport | None = None) -> TwseMarketDataProvider:
    return TwseMarketDataProvider(
        transport=transport or StubTransport(),
        clock=lambda: FETCHED_AT,
    )


@pytest.mark.parametrize(
    ("symbol", "name", "close", "volume", "pe"),
    [
        ("2330", "台積電", 2320.0, 41_021_199, 31.19),
        ("2317", "鴻海", 250.0, 41_095_656, 17.76),
        ("2454", "聯發科", 3865.0, 17_318_591, 63.83),
    ],
)
def test_twse_official_payload_maps_to_canonical_envelope(
    symbol, name, close, volume, pe
) -> None:
    provider = _provider()

    batch = provider.fetch_market_data(
        symbol, MARKET_DATE, MARKET_DATE, timeout_seconds=2.5
    )

    assert isinstance(provider, MarketDataProvider)
    assert batch.source == "twse"
    assert batch.symbol == {
        "symbol": symbol,
        "name": name,
        "market": "TWSE",
        "currency": "TWD",
        "is_active": True,
    }
    assert batch.daily_prices[0]["trade_date"] == "2026-08-04"
    assert batch.daily_prices[0]["close"] == close
    assert batch.daily_prices[0]["volume"] == volume
    assert {
        metric["name"]: metric["value"] for metric in batch.company_metrics
    }["price_earnings_ratio"] == pe
    assert batch.source_endpoints == (STOCK_DAY_ALL_URL, BWIBBU_ALL_URL)
    assert batch.fetched_at == FETCHED_AT
    assert batch.market_date == MARKET_DATE


def test_twse_source_artifacts_hash_exact_official_fixture_bytes() -> None:
    batch = _provider().fetch_market_data(
        "2330", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
    )
    artifacts = {artifact.dataset: artifact for artifact in batch.source_artifacts}

    assert set(artifacts) == {"STOCK_DAY_ALL", "BWIBBU_ALL"}
    assert artifacts["STOCK_DAY_ALL"].payload_sha256 == hashlib.sha256(
        _fixture_bytes("stock_day_all.json")
    ).hexdigest()
    assert artifacts["BWIBBU_ALL"].payload_sha256 == hashlib.sha256(
        _fixture_bytes("bwibbu_all.json")
    ).hexdigest()
    assert all(
        artifact.provider == "twse"
        and artifact.hash_basis == "raw-response-bytes-v1"
        and artifact.fetched_at == FETCHED_AT
        for artifact in artifacts.values()
    )


def test_twse_number_conversion_and_missing_metric() -> None:
    batch = _provider().fetch_market_data(
        "1101", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
    )

    price = batch.daily_prices[0]
    assert price["open"] == 23.6
    assert price["high"] == 23.7
    assert price["low"] == 23.45
    assert price["close"] == 23.55
    assert isinstance(price["volume"], int)
    metrics = {metric["name"]: metric for metric in batch.company_metrics}
    assert "price_earnings_ratio" not in metrics
    assert metrics["dividend_yield_pct"]["value"] == 3.4
    assert metrics["dividend_yield_pct"]["unit"] == "%"
    assert metrics["price_to_book_ratio"]["value"] == 0.75


def test_twse_filters_out_etf_not_present_in_common_stock_intersection() -> None:
    with pytest.raises(ProviderInvalidRequestError, match="common-stock intersection"):
        _provider().fetch_market_data(
            "0050", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
        )


def test_twse_rejects_market_date_outside_requested_range() -> None:
    with pytest.raises(ProviderInvalidRequestError, match="outside requested range"):
        _provider().fetch_market_data(
            "2330", date(2026, 8, 5), date(2026, 8, 5), timeout_seconds=1.0
        )


def test_twse_classifies_all_zero_ohlc_as_no_canonical_regular_lot_price() -> None:
    payload = json.loads(_fixture_bytes("stock_day_all.json"))
    target = next(record for record in payload if record["Code"] == "1101")
    for field in ("OpeningPrice", "HighestPrice", "LowestPrice", "ClosingPrice"):
        target[field] = "0.00"
    target["TradeVolume"] = "250"
    transport = StubTransport(
        {
            STOCK_DAY_ALL_URL: [_json_response(json.dumps(payload).encode())],
            BWIBBU_ALL_URL: [_json_response(_fixture_bytes("bwibbu_all.json"))],
        }
    )

    with pytest.raises(ProviderInvalidRequestError, match="no canonical regular-lot"):
        _provider(transport).fetch_market_data(
            "1101", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
        )


def test_twse_rejects_partial_zero_ohlc_as_malformed() -> None:
    payload = json.loads(_fixture_bytes("stock_day_all.json"))
    target = next(record for record in payload if record["Code"] == "1101")
    target["OpeningPrice"] = "0.00"
    transport = StubTransport(
        {
            STOCK_DAY_ALL_URL: [_json_response(json.dumps(payload).encode())],
            BWIBBU_ALL_URL: [_json_response(_fixture_bytes("bwibbu_all.json"))],
        }
    )

    with pytest.raises(ProviderInvalidPayloadError, match="partial or non-positive"):
        _provider(transport).fetch_market_data(
            "1101", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
        )


@pytest.mark.parametrize(
    "body",
    [b"not-json", b'{"Date":"1150804"}', b"[]"],
)
def test_twse_rejects_malformed_json_or_top_level_shape(body) -> None:
    transport = StubTransport(
        {
            STOCK_DAY_ALL_URL: [_json_response(body)],
            BWIBBU_ALL_URL: [_json_response(_fixture_bytes("bwibbu_all.json"))],
        }
    )

    with pytest.raises(ProviderInvalidPayloadError):
        _provider(transport).fetch_market_data(
            "2330", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
        )


@pytest.mark.parametrize("mutation", ["missing", "wrong_type", "bad_number"])
def test_twse_rejects_required_field_schema_mismatch(mutation) -> None:
    payload = json.loads(_fixture_bytes("stock_day_all.json"))
    target = next(record for record in payload if record["Code"] == "2330")
    if mutation == "missing":
        target.pop("ClosingPrice")
    elif mutation == "wrong_type":
        target["ClosingPrice"] = 2320.0
    else:
        target["ClosingPrice"] = "2,320.00"
    transport = StubTransport(
        {
            STOCK_DAY_ALL_URL: [_json_response(json.dumps(payload).encode())],
            BWIBBU_ALL_URL: [_json_response(_fixture_bytes("bwibbu_all.json"))],
        }
    )

    with pytest.raises(ProviderInvalidPayloadError):
        _provider(transport).fetch_market_data(
            "2330", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
        )


def test_twse_maps_connection_timeout() -> None:
    transport = StubTransport({STOCK_DAY_ALL_URL: [TimeoutError("timed out")]})

    with pytest.raises(ProviderTimeoutError):
        _provider(transport).fetch_market_data(
            "2330", MARKET_DATE, MARKET_DATE, timeout_seconds=0.1
        )


@pytest.mark.parametrize("status", [429, 500, 503])
def test_twse_maps_429_and_5xx_to_temporary_failure(status) -> None:
    transport = StubTransport(
        {STOCK_DAY_ALL_URL: [_json_response(b"{}", status=status)]}
    )

    with pytest.raises(ProviderTemporaryError, match=str(status)):
        _provider(transport).fetch_market_data(
            "2330", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
        )


@pytest.mark.parametrize("status", [400, 404, 403])
def test_twse_maps_4xx_to_permanent_failure(status) -> None:
    transport = StubTransport(
        {STOCK_DAY_ALL_URL: [_json_response(b"{}", status=status)]}
    )

    with pytest.raises(ProviderPermanentError, match=str(status)):
        _provider(transport).fetch_market_data(
            "2330", MARKET_DATE, MARKET_DATE, timeout_seconds=1.0
        )


def test_twse_temporary_http_failure_uses_phase2_pipeline_retry(tmp_path) -> None:
    transport = StubTransport(
        {
            STOCK_DAY_ALL_URL: [
                _json_response(b"{}", status=429),
                _json_response(_fixture_bytes("stock_day_all.json")),
            ],
            BWIBBU_ALL_URL: [_json_response(_fixture_bytes("bwibbu_all.json"))],
        }
    )
    delays: list[float] = []
    pipeline = DailyResearchPipeline(
        _provider(transport),
        SQLiteResearchRepository(tmp_path / "twse.db"),
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.25),
        sleep=delays.append,
    )

    result = pipeline.run("2330", MARKET_DATE, MARKET_DATE)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert result.provider_attempts == 2
    assert delays == [0.25]
    assert [url for url, _ in transport.calls] == [
        STOCK_DAY_ALL_URL,
        STOCK_DAY_ALL_URL,
        BWIBBU_ALL_URL,
    ]


def test_twse_pipeline_integration_persists_provenance_and_is_idempotent(
    tmp_path,
) -> None:
    transport = StubTransport()
    repository = SQLiteResearchRepository(tmp_path / "twse.db")
    pipeline = DailyResearchPipeline(_provider(transport), repository)

    first = pipeline.run("2330", MARKET_DATE, MARKET_DATE)
    second = pipeline.run("2330", MARKET_DATE, MARKET_DATE)

    assert first.run_status is PipelineRunStatus.SUCCESS
    assert first.provider_source == "twse"
    assert first.source_endpoints == (STOCK_DAY_ALL_URL, BWIBBU_ALL_URL)
    assert first.fetched_at == FETCHED_AT
    assert first.market_date == MARKET_DATE
    assert first.analysis.observations == 1
    assert "price_earnings_ratio" in first.summary
    assert len(repository.list_daily_prices("2330")) == 1
    assert len(repository.list_company_metrics("2330")) == 3
    assert len(repository.list_research_notes("2330")) == 1

    run = repository.get_pipeline_run(first.run_id)
    assert run is not None
    assert run.source_endpoints == (STOCK_DAY_ALL_URL, BWIBBU_ALL_URL)
    assert run.fetched_at == FETCHED_AT
    assert run.market_date == MARKET_DATE

    assert second.idempotent_replay is True
    assert second.run_id == first.run_id
    assert second.research_note_id == first.research_note_id
    assert second.daily_prices_written == 0
    assert second.company_metrics_written == 0
    assert len(transport.calls) == 2
