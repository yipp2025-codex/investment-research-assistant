import sqlite3
from datetime import date, datetime, timedelta, timezone

import pytest

from app.models import CompanyMetric, PipelineRunStatus, Symbol
from app.pipelines import HistoricalCoverageError, HistoricalSyncPipeline, RetryPolicy
from app.providers import (
    MarketDataBatch,
    MarketDataProvider,
    ProviderPermanentError,
    ProviderTemporaryError,
)
from app.providers.artifacts import source_artifact_from_json
from app.storage import (
    HistoricalSyncInProgressError,
    HistoricalSyncStateError,
    SQLiteHistoricalSyncRepository,
    SQLiteResearchRepository,
)


TARGET_DATE = date(2026, 6, 30)


class SyntheticHistoricalProvider(MarketDataProvider):
    def __init__(self, failure_plan=()) -> None:
        self.failure_plan = list(failure_plan)
        self.calls: list[date] = []

    @property
    def source(self) -> str:
        return "twse-historical"

    def fetch_market_data(
        self, symbol, start_date, end_date, *, timeout_seconds
    ) -> MarketDataBatch:
        del timeout_seconds
        self.calls.append(start_date)
        call_index = len(self.calls) - 1
        if call_index < len(self.failure_plan):
            failure = self.failure_plan[call_index]
            if failure is not None:
                raise failure

        current = start_date
        rows = []
        while current <= end_date:
            if current.weekday() < 5:
                close = 100.0 + (current.toordinal() % 200) / 10.0
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_date": current.isoformat(),
                        "open": close - 0.5,
                        "high": close + 1.0,
                        "low": close - 1.0,
                        "close": close,
                        "volume": 1_000_000 + current.day * 10_000,
                    }
                )
            current += timedelta(days=1)
        fetched_at = datetime(2026, 8, 5, tzinfo=timezone.utc)
        if self.source == "esun-historical":
            ticker_endpoint = (
                "https://api.fugle.tw/marketdata/v1.0/stock/intraday/ticker/"
                f"{symbol}"
            )
            candle_endpoint = (
                "https://api.fugle.tw/marketdata/v1.0/stock/historical/candles/"
                f"{symbol}?from={start_date.isoformat()}&to={end_date.isoformat()}"
            )
            endpoints = (ticker_endpoint, candle_endpoint)
            source_artifacts = (
                source_artifact_from_json(
                    provider=self.source,
                    dataset="intraday-ticker",
                    endpoint=ticker_endpoint,
                    contract_version=self.manifest.contract_version,
                    payload={"symbol": symbol, "name": "台積電"},
                    fetched_at=fetched_at,
                ),
                source_artifact_from_json(
                    provider=self.source,
                    dataset="historical-candles",
                    endpoint=candle_endpoint,
                    contract_version=self.manifest.contract_version,
                    payload={
                        "symbol": symbol,
                        "start_date": start_date,
                        "end_date": end_date,
                        "rows": rows,
                    },
                    fetched_at=fetched_at,
                ),
            )
        else:
            endpoint = (
                "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
                f"?date={start_date:%Y%m%d}&stockNo={symbol}"
            )
            endpoints = (endpoint,)
            source_artifacts = (
                source_artifact_from_json(
                    provider=self.source,
                    dataset="STOCK_DAY",
                    endpoint=endpoint,
                    contract_version=self.manifest.contract_version,
                    payload={
                        "symbol": symbol,
                        "start_date": start_date,
                        "end_date": end_date,
                        "rows": rows,
                    },
                    fetched_at=fetched_at,
                ),
            )
        return MarketDataBatch(
            source=self.source,
            symbol={
                "symbol": symbol,
                "name": "台積電",
                "market": "TWSE",
                "currency": "TWD",
                "is_active": True,
            },
            daily_prices=tuple(rows),
            company_metrics=(),
            source_endpoints=endpoints,
            fetched_at=fetched_at,
            market_date=date.fromisoformat(rows[-1]["trade_date"]),
            source_artifacts=source_artifacts,
        )


class SyntheticEsunHistoricalProvider(SyntheticHistoricalProvider):
    @property
    def source(self) -> str:
        return "esun-historical"


def _repository(tmp_path) -> SQLiteResearchRepository:
    repository = SQLiteResearchRepository(tmp_path / "history.db")
    repository.initialize()
    repository.upsert_symbol(Symbol("2330", "台積電", "TWSE", "TWD"))
    repository.upsert_company_metrics(
        [
            CompanyMetric(
                "2330",
                TARGET_DATE,
                "price_earnings_ratio",
                31.19,
                "ratio",
                "twse",
            ),
            CompanyMetric(
                "2330",
                TARGET_DATE,
                "price_to_book_ratio",
                10.21,
                "ratio",
                "twse",
            ),
            CompanyMetric(
                "2330",
                TARGET_DATE,
                "dividend_yield_pct",
                0.95,
                "%",
                "twse",
            ),
        ]
    )
    return repository


def test_historical_sync_accumulates_60_days_and_creates_research_note(
    tmp_path,
) -> None:
    repository = _repository(tmp_path)
    provider = SyntheticHistoricalProvider()
    pipeline = HistoricalSyncPipeline(provider, repository)

    result = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert result.observation_count == 60
    assert result.months_completed == 3
    assert result.months_fetched == 3
    assert result.analysis.observations == 60
    assert result.analysis.window(20).return_pct is not None
    assert result.analysis.window(60).moving_average is not None
    assert result.analysis.window(120).moving_average is None
    assert "price_earnings_ratio：31.19" in result.summary
    assert len(repository.list_daily_prices("2330")) >= 60
    source_observations = SQLiteHistoricalSyncRepository(
        repository
    ).list_source_observations(
        result.run_id, end_date=TARGET_DATE, limit=250
    )
    assert len(source_observations) >= 60
    assert all(item.provider == "twse-historical" for item in source_observations)
    assert all(item.source_endpoints for item in source_observations)
    notes = repository.list_research_notes("2330")
    assert len(notes) == 1
    assert notes[0].historical_run_id == result.run_id
    artifacts = repository.list_source_artifacts(historical_run_id=result.run_id)
    assert len(artifacts) == result.months_fetched == 3
    assert {artifact.provider for artifact in artifacts} == {"twse-historical"}
    assert {artifact.dataset for artifact in artifacts} == {"STOCK_DAY"}
    assert all(artifact.hash_basis == "canonical-json-v1" for artifact in artifacts)


def test_historical_sync_successful_rerun_is_idempotent(tmp_path) -> None:
    repository = _repository(tmp_path)
    provider = SyntheticHistoricalProvider()
    pipeline = HistoricalSyncPipeline(provider, repository)

    first = pipeline.run("2330", TARGET_DATE, target_observations=60)
    call_count = len(provider.calls)
    second = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert second.run_id == first.run_id
    assert second.research_note_id == first.research_note_id
    assert second.idempotent_replay is True
    assert second.months_fetched == 0
    assert second.provider_attempts == 0
    assert len(provider.calls) == call_count
    assert len(repository.list_research_notes("2330")) == 1
    assert len(repository.list_source_artifacts(historical_run_id=first.run_id)) == (
        first.months_fetched
    )


def test_esun_historical_sync_is_source_only_and_keeps_deterministic_analysis(
    tmp_path,
) -> None:
    repository = _repository(tmp_path)
    provider = SyntheticEsunHistoricalProvider()
    pipeline = HistoricalSyncPipeline(provider, repository)

    result = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert result.analysis.observations == 60
    assert result.analysis.window(60).moving_average is not None
    assert repository.list_daily_prices("2330") == []
    observations = SQLiteHistoricalSyncRepository(
        repository
    ).list_source_observations(result.run_id, end_date=TARGET_DATE, limit=250)
    assert len(observations) >= 60
    assert all(item.provider == "esun-historical" for item in observations)
    note = repository.get_research_note(result.research_note_id)
    assert note is not None
    assert note.provider_source == "esun-historical"
    artifacts = repository.list_source_artifacts(historical_run_id=result.run_id)
    assert len(artifacts) == result.months_fetched * 2
    assert {artifact.provider for artifact in artifacts} == {"esun-historical"}
    assert {artifact.dataset for artifact in artifacts} == {
        "intraday-ticker",
        "historical-candles",
    }
    ticker_hashes = {
        artifact.payload_sha256
        for artifact in artifacts
        if artifact.dataset == "intraday-ticker"
    }
    assert len(ticker_hashes) == 1


def test_esun_historical_sync_cannot_enable_canonical_overwrite(tmp_path) -> None:
    with pytest.raises(ValueError, match="source-only"):
        HistoricalSyncPipeline(
            SyntheticEsunHistoricalProvider(),
            _repository(tmp_path),
            write_canonical_prices=True,
        )


def test_historical_sync_accumulates_exact_250_analysis_observations(tmp_path) -> None:
    repository = _repository(tmp_path)
    provider = SyntheticHistoricalProvider()

    result = HistoricalSyncPipeline(provider, repository).run(
        "2330", TARGET_DATE, target_observations=250, max_months=18
    )

    assert result.observation_count == 250
    assert result.analysis.window(250).moving_average is not None
    assert result.analysis.window(120).return_pct is not None
    assert result.months_completed >= 11
    assert len(repository.list_daily_prices("2330")) >= 250


def test_historical_sync_retries_temporary_month_without_sleep(tmp_path) -> None:
    repository = _repository(tmp_path)
    provider = SyntheticHistoricalProvider(
        [ProviderTemporaryError("temporary"), None]
    )
    delays: list[float] = []
    pipeline = HistoricalSyncPipeline(
        provider,
        repository,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.25),
        sleep=delays.append,
    )

    result = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert result.provider_attempts == 4
    assert delays == [0.25]
    assert provider.calls[:2] == [date(2026, 6, 1), date(2026, 6, 1)]


def test_historical_sync_honors_provider_retry_after(tmp_path) -> None:
    repository = _repository(tmp_path)
    provider = SyntheticHistoricalProvider(
        [ProviderTemporaryError("paced", retry_after_seconds=3.5), None]
    )
    delays: list[float] = []
    pipeline = HistoricalSyncPipeline(
        provider,
        repository,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.25),
        sleep=delays.append,
    )

    result = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert delays == [3.5]


def test_historical_sync_caps_extreme_provider_retry_after(tmp_path) -> None:
    repository = _repository(tmp_path)
    provider = SyntheticHistoricalProvider(
        [ProviderTemporaryError("paced", retry_after_seconds=1_000_000_000), None]
    )
    delays: list[float] = []
    pipeline = HistoricalSyncPipeline(
        provider,
        repository,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.25),
        sleep=delays.append,
    )

    result = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert delays == [60.0]


def test_historical_month_sqlite_failure_rolls_back_and_retry_resumes_cursor(
    tmp_path,
) -> None:
    repository = _repository(tmp_path)
    database_path = repository.database_path
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TRIGGER fail_may_history BEFORE INSERT ON daily_prices "
            "WHEN NEW.trade_date < '2026-06-01' BEGIN "
            "SELECT RAISE(ABORT, 'forced May failure'); END"
        )
    provider = SyntheticHistoricalProvider()
    pipeline = HistoricalSyncPipeline(provider, repository)

    with pytest.raises(sqlite3.IntegrityError, match="forced May failure"):
        pipeline.run("2330", TARGET_DATE, target_observations=60)

    history = SQLiteHistoricalSyncRepository(repository)
    failed = history.list("2330")[0]
    assert failed.status is PipelineRunStatus.FAILED
    assert failed.months_completed == 1
    assert failed.next_month == date(2026, 5, 1)
    assert all(
        price.trade_date.month == 6
        for price in repository.list_daily_prices("2330")
    )
    assert len(repository.list_source_artifacts(historical_run_id=failed.run_id)) == 1

    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP TRIGGER fail_may_history")
    recovered = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert recovered.run_id == failed.run_id
    assert recovered.run_attempt_count == 2
    assert provider.calls.count(date(2026, 6, 1)) == 1
    assert recovered.run_status is PipelineRunStatus.SUCCESS
    assert len(
        repository.list_source_artifacts(historical_run_id=recovered.run_id)
    ) == recovered.months_completed


def test_historical_interruption_requires_explicit_resume_id(tmp_path) -> None:
    repository = _repository(tmp_path)
    provider = SyntheticHistoricalProvider([None, KeyboardInterrupt("interrupted")])
    pipeline = HistoricalSyncPipeline(provider, repository)

    with pytest.raises(KeyboardInterrupt, match="interrupted"):
        pipeline.run("2330", TARGET_DATE, target_observations=60)

    history = SQLiteHistoricalSyncRepository(repository)
    interrupted = history.list("2330")[0]
    assert interrupted.status is PipelineRunStatus.RUNNING
    assert interrupted.months_completed == 1
    assert interrupted.next_month == date(2026, 5, 1)

    with pytest.raises(HistoricalSyncInProgressError):
        pipeline.run("2330", TARGET_DATE, target_observations=60)

    recovered = pipeline.run(
        "2330",
        TARGET_DATE,
        target_observations=60,
        resume_run_id=interrupted.run_id,
    )
    assert recovered.run_id == interrupted.run_id
    assert recovered.run_attempt_count == 2
    assert recovered.run_status is PipelineRunStatus.SUCCESS


def test_historical_sync_fails_closed_when_month_bound_is_insufficient(
    tmp_path,
) -> None:
    repository = _repository(tmp_path)
    pipeline = HistoricalSyncPipeline(SyntheticHistoricalProvider(), repository)

    with pytest.raises(HistoricalCoverageError, match="max_months=1"):
        pipeline.run(
            "2330", TARGET_DATE, target_observations=250, max_months=1
        )

    run = SQLiteHistoricalSyncRepository(repository).list("2330")[0]
    assert run.status is PipelineRunStatus.FAILED
    assert run.months_completed == 1
    assert run.research_note_id is None


def test_historical_sync_requires_phase3_twse_symbol(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "history.db")
    repository.initialize()
    pipeline = HistoricalSyncPipeline(SyntheticHistoricalProvider(), repository)

    with pytest.raises(HistoricalSyncStateError, match="Phase 3"):
        pipeline.run("2330", TARGET_DATE, target_observations=60)
