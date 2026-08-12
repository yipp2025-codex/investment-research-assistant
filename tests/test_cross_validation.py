import json
import sqlite3
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from app.models import CrossValidationOutcome, PipelineRunStatus
from app.pipelines import MarketDataCrossValidationPipeline, RetryPolicy
from app.providers import (
    EsunHttpResponse,
    EsunMarketDataProvider,
    MarketDataBatch,
    MarketDataProvider,
    ProviderPermanentError,
    ProviderTemporaryError,
    TwseMarketDataProvider,
    get_provider_manifest,
)
from app.providers.artifacts import source_artifact_from_json
from app.providers.esun import HISTORICAL_CANDLES_PATH, INTRADAY_TICKER_PATH
from app.providers.twse import BWIBBU_ALL_URL, STOCK_DAY_ALL_URL, TwseHttpResponse
from app.storage import (
    CrossValidationConflictError,
    CrossValidationInProgressError,
    SQLiteCrossValidationRepository,
    SQLiteResearchRepository,
)


FIXTURE_DATE = date(2026, 8, 4)
START = date(2026, 8, 3)
FETCHED_AT = datetime(2026, 8, 6, 6, 0, tzinfo=timezone.utc)


def _batch(
    source: str,
    *,
    market_date: date = FIXTURE_DATE,
    close: float = 2320.0,
    volume: int = 41_021_199,
    source_timestamp: datetime | None = None,
) -> MarketDataBatch:
    endpoint = f"https://{source}.fixture.invalid/read-only"
    dataset = "STOCK_DAY_ALL" if source == "twse" else "historical-candles"
    return MarketDataBatch(
        source=source,
        symbol={
            "symbol": "2330",
            "name": "台積電",
            "market": "TWSE",
            "currency": "TWD",
            "is_active": True,
        },
        daily_prices=(
            {
                "symbol": "2330",
                "trade_date": market_date.isoformat(),
                "open": 2335.0,
                "high": 2360.0,
                "low": 2310.0,
                "close": close,
                "volume": volume,
            },
        ),
        company_metrics=(),
        source_endpoints=(endpoint,),
        fetched_at=FETCHED_AT,
        market_date=market_date,
        source_timestamp_raw=(
            None if source_timestamp is None else "synthetic-source-timestamp"
        ),
        source_timestamp=source_timestamp,
        source_artifacts=(
            source_artifact_from_json(
                provider=source,
                dataset=dataset,
                endpoint=endpoint,
                contract_version=get_provider_manifest(source).contract_version,
                payload={
                    "market_date": market_date,
                    "close": close,
                    "volume": volume,
                    "source_timestamp": source_timestamp,
                },
                fetched_at=FETCHED_AT,
            ),
        ),
    )


class StaticProvider(MarketDataProvider):
    def __init__(self, source: str, outcomes) -> None:
        self._source = source
        self.outcomes = list(outcomes)
        self.calls = 0

    @property
    def source(self) -> str:
        return self._source

    def fetch_market_data(
        self, symbol, start_date, end_date, *, timeout_seconds
    ) -> MarketDataBatch:
        self.calls += 1
        outcome = self.outcomes[0] if len(self.outcomes) == 1 else self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _pipeline(tmp_path, left=None, right=None, **kwargs):
    repository = SQLiteResearchRepository(tmp_path / "validation.db")
    return (
        MarketDataCrossValidationPipeline(
            left or StaticProvider("twse", [_batch("twse")]),
            right or StaticProvider("esun", [_batch("esun")]),
            repository,
            **kwargs,
        ),
        repository,
    )


def test_matching_sources_are_stored_independently_without_daily_price_overwrite(
    tmp_path,
) -> None:
    pipeline, repository = _pipeline(tmp_path)

    result = pipeline.run("2330", START, FIXTURE_DATE)

    assert result.status is PipelineRunStatus.SUCCESS
    assert result.outcome is CrossValidationOutcome.MATCH
    assert [item.provider for item in result.observations] == ["twse", "esun"]
    assert all(item.market_date == FIXTURE_DATE for item in result.observations)
    assert all(item.source_timestamp is None for item in result.observations)
    assert result.discrepancies == ()
    assert repository.list_daily_prices("2330") == []
    assert repository.list_research_notes("2330") == []
    artifacts = repository.list_source_artifacts(validation_run_id=result.run_id)
    assert len(artifacts) == 2
    assert [artifact.provider for artifact in artifacts] == ["twse", "esun"]
    assert all(artifact.hash_basis == "canonical-json-v1" for artifact in artifacts)


def test_ohlcv_discrepancies_preserve_both_values_and_do_not_select_a_winner(
    tmp_path,
) -> None:
    right = StaticProvider(
        "esun", [_batch("esun", close=2319.0, volume=41_000_000)]
    )
    pipeline, repository = _pipeline(tmp_path, right=right)

    result = pipeline.run("2330", START, FIXTURE_DATE)

    assert result.outcome is CrossValidationOutcome.DISCREPANCY
    differences = {item.field: item for item in result.discrepancies}
    assert differences["close"].left_value == "2320"
    assert differences["close"].right_value == "2319"
    assert differences["close"].absolute_difference == 1.0
    assert differences["close"].relative_difference_pct == pytest.approx(
        1 / 2320 * 100
    )
    assert differences["volume"].left_value == "41021199"
    assert differences["volume"].right_value == "41000000"
    assert "unresolved" in differences["volume"].reason
    observations = SQLiteCrossValidationRepository(repository).list_observations(
        result.run_id
    )
    assert {item.provider: item.close for item in observations} == {
        "twse": 2320.0,
        "esun": 2319.0,
    }
    assert repository.list_daily_prices("2330") == []


def test_market_date_mismatch_is_recorded_without_cross_date_ohlcv_comparison(
    tmp_path,
) -> None:
    right = StaticProvider(
        "esun", [_batch("esun", market_date=date(2026, 8, 3), close=2315.0)]
    )
    pipeline, _ = _pipeline(tmp_path, right=right)

    result = pipeline.run("2330", START, FIXTURE_DATE)

    assert [item.field for item in result.discrepancies] == ["market_date"]
    assert result.discrepancies[0].left_value == "2026-08-04"
    assert result.discrepancies[0].right_value == "2026-08-03"


def test_source_timestamp_missing_semantics_are_explicit(tmp_path) -> None:
    timestamp = datetime(2026, 8, 4, 5, 30, tzinfo=timezone.utc)
    left = StaticProvider(
        "twse", [_batch("twse", source_timestamp=timestamp)]
    )
    pipeline, _ = _pipeline(tmp_path, left=left)

    result = pipeline.run("2330", START, FIXTURE_DATE)

    discrepancy = result.discrepancies[0]
    assert discrepancy.field == "source_timestamp"
    assert discrepancy.left_value == timestamp.isoformat()
    assert discrepancy.right_value is None
    assert discrepancy.reason == "missing_value"


def test_successful_cross_validation_replay_is_idempotent(tmp_path) -> None:
    left = StaticProvider("twse", [_batch("twse")])
    right = StaticProvider("esun", [_batch("esun")])
    pipeline, repository = _pipeline(tmp_path, left=left, right=right)

    first = pipeline.run("2330", START, FIXTURE_DATE)
    second = pipeline.run("2330", START, FIXTURE_DATE)

    assert second.idempotent_replay is True
    assert second.run_id == first.run_id
    assert second.left_provider_attempts == 0
    assert second.right_provider_attempts == 0
    assert left.calls == right.calls == 1
    storage = SQLiteCrossValidationRepository(repository)
    assert len(storage.list_observations(first.run_id)) == 2
    assert len(storage.list_runs("2330")) == 1
    assert len(repository.list_source_artifacts(validation_run_id=first.run_id)) == 2


def test_temporary_failure_uses_bounded_phase2_retry_without_real_sleep(
    tmp_path,
) -> None:
    left = StaticProvider(
        "twse", [ProviderTemporaryError("temporary"), _batch("twse")]
    )
    delays: list[float] = []
    pipeline, _ = _pipeline(
        tmp_path,
        left=left,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.25),
        sleep=delays.append,
    )

    result = pipeline.run("2330", START, FIXTURE_DATE)

    assert result.left_provider_attempts == 2
    assert result.right_provider_attempts == 1
    assert delays == [0.25]


def test_permanent_failure_marks_run_failed_without_partial_observations(
    tmp_path,
) -> None:
    left = StaticProvider("twse", [ProviderPermanentError("permanent")])
    pipeline, repository = _pipeline(tmp_path, left=left)

    with pytest.raises(ProviderPermanentError):
        pipeline.run("2330", START, FIXTURE_DATE)

    storage = SQLiteCrossValidationRepository(repository)
    run = storage.list_runs("2330")[0]
    assert run.status is PipelineRunStatus.FAILED
    assert run.outcome is None
    assert storage.list_observations(run.run_id) == []
    assert storage.list_discrepancies(run.run_id) == []
    assert repository.list_source_artifacts(validation_run_id=run.run_id) == []


def test_sqlite_failure_rolls_back_both_observations_and_can_retry(tmp_path) -> None:
    pipeline, repository = _pipeline(tmp_path)
    repository.initialize()
    with sqlite3.connect(repository.database_path) as connection:
        connection.execute(
            "CREATE TRIGGER fail_esun_observation BEFORE INSERT "
            "ON market_data_observations WHEN NEW.provider = 'esun' "
            "BEGIN SELECT RAISE(ABORT, 'forced validation failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="forced validation failure"):
        pipeline.run("2330", START, FIXTURE_DATE)

    storage = SQLiteCrossValidationRepository(repository)
    failed = storage.list_runs("2330")[0]
    assert failed.status is PipelineRunStatus.FAILED
    assert storage.list_observations(failed.run_id) == []
    assert repository.list_source_artifacts(validation_run_id=failed.run_id) == []
    with sqlite3.connect(repository.database_path) as connection:
        connection.execute("DROP TRIGGER fail_esun_observation")

    recovered = pipeline.run("2330", START, FIXTURE_DATE)

    assert recovered.run_id == failed.run_id
    assert recovered.run_attempt_count == 2
    assert recovered.outcome is CrossValidationOutcome.MATCH
    assert len(storage.list_observations(failed.run_id)) == 2
    assert len(repository.list_source_artifacts(validation_run_id=failed.run_id)) == 2


def test_running_validation_requires_exact_run_id_to_resume(tmp_path) -> None:
    pipeline, repository = _pipeline(tmp_path)
    repository.initialize()
    storage = SQLiteCrossValidationRepository(repository)
    run = storage.get_or_create_run(
        symbol="2330",
        target_date=FIXTURE_DATE,
        requested_start_date=START,
        left_provider="twse",
        right_provider="esun",
        created_at=FETCHED_AT,
    )
    storage.start(run.run_id, started_at=FETCHED_AT)

    with pytest.raises(CrossValidationInProgressError):
        pipeline.run("2330", START, FIXTURE_DATE)
    with pytest.raises(CrossValidationConflictError):
        pipeline.run(
            "2330", START, FIXTURE_DATE, resume_run_id="different-run-id"
        )

    result = pipeline.run(
        "2330", START, FIXTURE_DATE, resume_run_id=run.run_id
    )

    assert result.run_id == run.run_id
    assert result.run_attempt_count == 2
    assert result.outcome is CrossValidationOutcome.MATCH


def test_twse_and_esun_official_fixtures_cross_validate_through_real_adapters(
    tmp_path,
) -> None:
    twse_root = Path("tests/fixtures/twse")
    esun_root = Path("tests/fixtures/esun")

    class TwseStub:
        def get(self, url, *, timeout_seconds):
            name = (
                "stock_day_all.json"
                if url == STOCK_DAY_ALL_URL
                else "bwibbu_all.json"
            )
            return TwseHttpResponse(
                status_code=200,
                body=(twse_root / name).read_bytes(),
                headers={"content-type": "application/json"},
            )

    historical = json.loads(
        (esun_root / "historical_candles_2330_20260805.json").read_bytes()
    )
    historical["data"] = [
        row for row in historical["data"] if row["date"] == "2026-08-04"
    ]
    ticker_path = INTRADAY_TICKER_PATH.format(symbol="2330")
    candle_path = HISTORICAL_CANDLES_PATH.format(symbol="2330")

    class EsunStub:
        def get(self, path, *, params, timeout_seconds):
            body = (
                (esun_root / "ticker_2330_20260806.json").read_bytes()
                if path == ticker_path
                else json.dumps(historical).encode("utf-8")
            )
            return EsunHttpResponse(
                status_code=200,
                body=body,
                headers={"content-type": "application/json"},
                url="https://api.fugle.tw/marketdata/v1.0/stock" + path,
                fetched_at=FETCHED_AT,
            )

    pipeline = MarketDataCrossValidationPipeline(
        TwseMarketDataProvider(transport=TwseStub(), clock=lambda: FETCHED_AT),
        EsunMarketDataProvider(transport=EsunStub()),
        SQLiteResearchRepository(tmp_path / "official-fixtures.db"),
    )

    result = pipeline.run("2330", FIXTURE_DATE, FIXTURE_DATE)

    assert result.outcome is CrossValidationOutcome.MATCH
    assert [(item.provider, item.close, item.volume) for item in result.observations] == [
        ("twse", 2320.0, 41_021_199),
        ("esun", 2320.0, 41_021_199),
    ]
    artifacts = pipeline.repository.research_repository.list_source_artifacts(
        validation_run_id=result.run_id
    )
    assert len(artifacts) == 4
    assert {artifact.hash_basis for artifact in artifacts} == {
        "raw-response-bytes-v1"
    }
