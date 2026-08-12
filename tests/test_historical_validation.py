import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.models import (
    CompanyMetric,
    CrossValidationOutcome,
    HistoricalSourceObservation,
    PipelineRunStatus,
    Symbol,
)
from app.pipelines import CrossValidatedHistoricalResearchPipeline
from app.providers import MarketDataBatch, MarketDataProvider
from app.storage import (
    HistoricalValidationConflictError,
    HistoricalValidationInProgressError,
    SQLiteHistoricalSyncRepository,
    SQLiteHistoricalValidationRepository,
    SQLiteResearchRepository,
)


TARGET_DATE = date(2026, 6, 30)
FETCHED_AT = datetime(2026, 8, 6, tzinfo=timezone.utc)


class SyntheticHistoryProvider(MarketDataProvider):
    def __init__(
        self,
        source: str,
        *,
        skipped_dates=(),
        mutations=None,
    ) -> None:
        self._source = source
        self.skipped_dates = set(skipped_dates)
        self.mutations = mutations or {}
        self.calls: list[date] = []

    @property
    def source(self) -> str:
        return self._source

    def fetch_market_data(
        self, symbol, start_date, end_date, *, timeout_seconds
    ) -> MarketDataBatch:
        del timeout_seconds
        self.calls.append(start_date)
        rows = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5 and current not in self.skipped_dates:
                close = 100.0 + (current.toordinal() % 200) / 10.0
                row = {
                    "symbol": symbol,
                    "trade_date": current.isoformat(),
                    "open": close - 0.5,
                    "high": close + 1.0,
                    "low": close - 1.0,
                    "close": close,
                    "volume": 1_000_000 + current.day * 10_000,
                }
                for field, value in self.mutations.get(current, {}).items():
                    row[field] = value
                rows.append(row)
            current += timedelta(days=1)
        endpoints = (
            (
                "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
                f"?date={start_date:%Y%m%d}&stockNo={symbol}"
            ),
        )
        if self.source == "esun-historical":
            endpoints = (
                f"https://api.fugle.tw/marketdata/v1.0/stock/intraday/ticker/{symbol}",
                (
                    "https://api.fugle.tw/marketdata/v1.0/stock/historical/"
                    f"candles/{symbol}?from={start_date}&to={end_date}"
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
            fetched_at=FETCHED_AT,
            market_date=date.fromisoformat(rows[-1]["trade_date"]),
        )


def _repository(tmp_path) -> SQLiteResearchRepository:
    repository = SQLiteResearchRepository(tmp_path / "cross-history.db")
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
            )
        ]
    )
    return repository


def _pipeline(tmp_path, *, twse=None, esun=None):
    repository = _repository(tmp_path)
    twse_provider = twse or SyntheticHistoryProvider("twse-historical")
    esun_provider = esun or SyntheticHistoryProvider("esun-historical")
    pipeline = CrossValidatedHistoricalResearchPipeline(
        twse_provider,
        esun_provider,
        repository,
        clock=lambda: FETCHED_AT,
    )
    return pipeline, repository, twse_provider, esun_provider


def test_cross_validated_history_matches_60_days_without_esun_overwrite(
    tmp_path,
) -> None:
    pipeline, repository, twse, esun = _pipeline(tmp_path)

    result = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert result.outcome is CrossValidationOutcome.MATCH
    assert result.common_date_count == 60
    assert result.matched_date_count == 60
    assert result.left_only_date_count == 0
    assert result.right_only_date_count == 0
    assert result.field_discrepancy_count == 0
    assert result.discrepancies == ()
    assert result.analysis == result.left_sync.analysis
    assert result.analysis.observations == 60
    assert "指標基線：TWSE 原始歷史觀察" in result.summary
    assert "deterministic output" in result.summary

    canonical = repository.list_daily_prices("2330")
    assert len(canonical) >= 60
    assert {price.source for price in canonical} == {"twse-historical"}
    history = SQLiteHistoricalSyncRepository(repository)
    esun_rows = history.list_source_observations(
        result.right_sync.run_id, end_date=TARGET_DATE, limit=60
    )
    assert len(esun_rows) == 60
    assert all(item.provider == "esun-historical" for item in esun_rows)
    assert all(len(item.source_endpoints) == 2 for item in esun_rows)

    note = repository.get_research_note(result.research_note_id)
    assert note is not None
    assert note.historical_validation_run_id == result.run_id
    assert note.provider_source == "twse-historical+esun-historical"
    assert len(repository.list_research_notes("2330")) == 3
    assert len(twse.calls) == len(esun.calls) == 3


def test_cross_validated_history_successful_replay_fetches_neither_source(
    tmp_path,
) -> None:
    pipeline, _, twse, esun = _pipeline(tmp_path)
    first = pipeline.run("2330", TARGET_DATE, target_observations=60)
    call_counts = (len(twse.calls), len(esun.calls))

    second = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert second.idempotent_replay is True
    assert second.run_id == first.run_id
    assert second.research_note_id == first.research_note_id
    assert (len(twse.calls), len(esun.calls)) == call_counts


def test_cross_validated_history_supports_full_250_observation_window(
    tmp_path,
) -> None:
    pipeline, repository, _, _ = _pipeline(tmp_path)

    result = pipeline.run(
        "2330", TARGET_DATE, target_observations=250, max_months=18
    )

    assert result.outcome is CrossValidationOutcome.MATCH
    assert result.common_date_count == 250
    assert result.matched_date_count == 250
    assert result.analysis.observations == 250
    assert result.analysis.window(250).moving_average is not None
    assert {price.source for price in repository.list_daily_prices("2330")} == {
        "twse-historical"
    }


def test_cross_validated_history_records_same_date_ohlcv_difference(
    tmp_path,
) -> None:
    base_close = 100.0 + (TARGET_DATE.toordinal() % 200) / 10.0
    esun = SyntheticHistoryProvider(
        "esun-historical",
        mutations={TARGET_DATE: {"close": base_close + 0.2}},
    )
    pipeline, repository, _, _ = _pipeline(tmp_path, esun=esun)

    result = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert result.outcome is CrossValidationOutcome.DISCREPANCY
    assert result.common_date_count == 60
    assert result.matched_date_count == 59
    assert result.field_discrepancy_count == 1
    difference = result.discrepancies[0]
    assert difference.trade_date == TARGET_DATE
    assert difference.field == "close"
    assert difference.absolute_difference == pytest.approx(0.2)
    assert difference.reason == "source_value_or_revision_unresolved"
    canonical_latest = repository.list_daily_prices("2330")[-1]
    assert canonical_latest.close == base_close
    assert canonical_latest.source == "twse-historical"


@pytest.mark.parametrize("symbol", ["2330", "2317", "2454"])
def test_live_fixture_preserves_20260428_volume_difference_only(symbol) -> None:
    fixture = json.loads(
        Path(
            "tests/fixtures/esun/historical_cross_validation_20260428.json"
        ).read_text(encoding="utf-8")
    )
    row = next(item for item in fixture["rows"] if item["symbol"] == symbol)
    observations = []
    for provider, volume in (
        ("twse-historical", row["twse_volume"]),
        ("esun-historical", row["esun_volume"]),
    ):
        observations.append(
            HistoricalSourceObservation(
                historical_run_id=provider,
                provider=provider,
                symbol=symbol,
                trade_date=date(2026, 4, 28),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=volume,
                source_endpoints=("https://official.fixture.invalid/read-only",),
                fetched_at=FETCHED_AT,
            )
        )

    comparison = CrossValidatedHistoricalResearchPipeline._compare(
        "fixture-run", (observations[0],), (observations[1],)
    )

    assert comparison.common_date_count == 1
    assert comparison.matched_date_count == 0
    assert comparison.field_discrepancy_count == 1
    assert comparison.discrepancies[0].field == "volume"
    assert comparison.discrepancies[0].reason == fixture["classification"]


def test_cross_validated_history_marks_gaps_and_latest_market_date_difference(
    tmp_path,
) -> None:
    esun = SyntheticHistoryProvider(
        "esun-historical", skipped_dates={TARGET_DATE}
    )
    pipeline, _, _, _ = _pipeline(tmp_path, esun=esun)

    result = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert result.outcome is CrossValidationOutcome.DISCREPANCY
    assert result.left_latest_date == TARGET_DATE
    assert result.right_latest_date == date(2026, 6, 29)
    assert result.left_only_date_count == 1
    assert result.right_only_date_count == 1
    gap_reasons = {item.reason for item in result.discrepancies}
    assert gap_reasons == {"missing_in_esun", "missing_in_twse"}
    assert "E.SUN 缺口日期：2026-06-30" in result.summary
    assert "TWSE／E.SUN 最新市場日：2026-06-30／2026-06-29" in result.summary


def test_historical_validation_sqlite_failure_rolls_back_and_retries_without_refetch(
    tmp_path,
) -> None:
    base_close = 100.0 + (TARGET_DATE.toordinal() % 200) / 10.0
    esun = SyntheticHistoryProvider(
        "esun-historical",
        mutations={TARGET_DATE: {"close": base_close + 0.2}},
    )
    pipeline, repository, twse, _ = _pipeline(tmp_path, esun=esun)
    with sqlite3.connect(repository.database_path) as connection:
        connection.execute(
            "CREATE TRIGGER fail_historical_validation BEFORE INSERT "
            "ON historical_validation_discrepancies BEGIN "
            "SELECT RAISE(ABORT, 'forced historical validation failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="forced historical"):
        pipeline.run("2330", TARGET_DATE, target_observations=60)

    validation_repository = SQLiteHistoricalValidationRepository(repository)
    failed = validation_repository.list("2330")[0]
    assert failed.status is PipelineRunStatus.FAILED
    assert failed.research_note_id is None
    assert validation_repository.list_discrepancies(failed.run_id) == []
    source_call_counts = (len(twse.calls), len(esun.calls))
    with sqlite3.connect(repository.database_path) as connection:
        connection.execute("DROP TRIGGER fail_historical_validation")

    recovered = pipeline.run("2330", TARGET_DATE, target_observations=60)

    assert recovered.run_id == failed.run_id
    assert recovered.run_attempt_count == 2
    assert recovered.outcome is CrossValidationOutcome.DISCREPANCY
    assert (len(twse.calls), len(esun.calls)) == source_call_counts


def test_interrupted_historical_validation_requires_exact_run_id(tmp_path) -> None:
    pipeline, repository, twse, esun = _pipeline(tmp_path)
    left = pipeline.twse_sync.run("2330", TARGET_DATE, target_observations=60)
    right = pipeline.esun_sync.run("2330", TARGET_DATE, target_observations=60)
    history = SQLiteHistoricalSyncRepository(repository)
    left_run = history.get(left.run_id)
    right_run = history.get(right.run_id)
    assert left_run is not None and right_run is not None
    validations = SQLiteHistoricalValidationRepository(repository)
    validation = validations.get_or_create(
        left_run=left_run,
        right_run=right_run,
        created_at=FETCHED_AT,
    )
    validations.start(validation.run_id, started_at=FETCHED_AT)
    source_call_counts = (len(twse.calls), len(esun.calls))

    with pytest.raises(HistoricalValidationInProgressError):
        pipeline.run("2330", TARGET_DATE, target_observations=60)
    with pytest.raises(HistoricalValidationConflictError):
        pipeline.run(
            "2330",
            TARGET_DATE,
            target_observations=60,
            resume_validation_run_id="different-run",
        )

    recovered = pipeline.run(
        "2330",
        TARGET_DATE,
        target_observations=60,
        resume_validation_run_id=validation.run_id,
    )

    assert recovered.run_id == validation.run_id
    assert recovered.run_attempt_count == 2
    assert recovered.outcome is CrossValidationOutcome.MATCH
    assert (len(twse.calls), len(esun.calls)) == source_call_counts
