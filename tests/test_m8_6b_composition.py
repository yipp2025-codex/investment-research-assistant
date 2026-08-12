"""M8.4 compatibility tests for Phase 6B as-of policy composition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from app.as_of_policy import FrozenAsOfPolicyV1
from app.models import (
    CompanyMetric,
    CrossValidationOutcome,
    CrossValidationRun,
    DailyPrice,
    PipelineRunStatus,
    Symbol,
)
from app.reports import daily_research
from app.reports.daily_research import METHODOLOGY_VERSION, DailyResearchReportService
from app.storage import SQLiteDailyReportRepository, SQLiteResearchRepository


MARKET_DATE = date(2026, 8, 5)
UTC = timezone.utc
FIXED_NOW = datetime(2026, 8, 8, 3, 0, tzinfo=UTC)
FROZEN_SHA256 = "8730b97cb0f744d0041f1ffde7cb4baf92848ed7318722326625c2f0e356ac64"


class _RecordingAsOfPolicy:
    def __init__(self) -> None:
        self.delegate = FrozenAsOfPolicyV1()
        self.calls: list[tuple[str, object]] = []

    def price_cutoff(self, target_date: date) -> date:
        self.calls.append(("price_cutoff", target_date))
        return self.delegate.price_cutoff(target_date)

    def prices_as_of(self, prices, target_date: date):
        materialized = tuple(prices)
        self.calls.append(("prices_as_of", (materialized, target_date)))
        return self.delegate.prices_as_of(materialized, target_date)

    def current_price_is_exact_date(self, prices, target_date: date) -> bool:
        materialized = tuple(prices)
        self.calls.append(("current_price_is_exact_date", (materialized, target_date)))
        return self.delegate.current_price_is_exact_date(materialized, target_date)

    def select_valuation_as_of(self, metrics, target_date: date):
        materialized = tuple(metrics)
        self.calls.append(("select_valuation_as_of", (materialized, target_date)))
        return self.delegate.select_valuation_as_of(materialized, target_date)

    def validation_target_is_consistent(
        self,
        validation_target_date: date | None,
        target_date: date,
    ) -> bool:
        self.calls.append(
            (
                "validation_target_is_consistent",
                (validation_target_date, target_date),
            )
        )
        return self.delegate.validation_target_is_consistent(
            validation_target_date,
            target_date,
        )

    def previous_result_is_comparable(
        self,
        candidate,
        *,
        symbol: str,
        target_date: date,
        methodology_version: str,
    ) -> bool:
        self.calls.append(
            (
                "previous_result_is_comparable",
                (candidate.result_id, symbol, target_date, methodology_version),
            )
        )
        return self.delegate.previous_result_is_comparable(
            candidate,
            symbol=symbol,
            target_date=target_date,
            methodology_version=methodology_version,
        )


def _setup(
    database_path: Path,
) -> tuple[SQLiteResearchRepository, SQLiteDailyReportRepository]:
    repository = SQLiteResearchRepository(database_path)
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()
    repository.upsert_symbol(Symbol("2330", "Test 2330", "TWSE", "TWD"))
    return repository, report_repository


def _service(
    repository: SQLiteResearchRepository,
    report_repository: SQLiteDailyReportRepository,
    *,
    validation_repository=None,
    methodology_version: str = METHODOLOGY_VERSION,
) -> DailyResearchReportService:
    return DailyResearchReportService(
        repository,
        report_repository=report_repository,
        cross_validation_repository=validation_repository,
        methodology_version=methodology_version,
        clock=lambda: FIXED_NOW,
    )


def _price(trade_date: date, close: float) -> DailyPrice:
    return DailyPrice(
        symbol="2330",
        trade_date=trade_date,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1_000,
        source="twse-historical",
    )


def _metric(metric_date: date, value: float) -> CompanyMetric:
    return CompanyMetric(
        symbol="2330",
        metric_date=metric_date,
        name="price_earnings_ratio",
        value=value,
        unit="ratio",
        source="twse",
    )


def test_6b_adapter_delegates_without_changing_frozen_canonical_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, report_repository = _setup(tmp_path / "canonical.db")
    repository.upsert_daily_prices([_price(MARKET_DATE, 105.0)])
    repository.upsert_company_metrics([_metric(date(2026, 8, 4), 20.0)])
    policy = _RecordingAsOfPolicy()
    monkeypatch.setattr(daily_research, "_AS_OF_POLICY", policy)

    generated = _service(repository, report_repository).generate(
        "2330",
        MARKET_DATE,
        requested_date=MARKET_DATE,
    )

    assert generated.canonical.payload_sha256 == FROZEN_SHA256
    assert generated.canonical.result_id == (
        "daily-d7904f3bc10707ee001cdff49a2cd96b"
    )
    assert generated.report.report_id == (
        "report-daily-d7904f3bc10707ee001cdff49a2cd96b"
    )
    call_names = [name for name, _ in policy.calls]
    assert call_names.count("price_cutoff") == 1
    assert call_names.count("prices_as_of") == 1
    assert call_names.count("current_price_is_exact_date") == 2
    assert call_names.count("select_valuation_as_of") == 3
    assert report_repository.count_results("2330") == 1
    assert report_repository.count_reports("2330") == 1


def test_6b_adapter_excludes_future_price_and_metric_from_leaky_reads(
    tmp_path: Path,
) -> None:
    repository, report_repository = _setup(tmp_path / "future-defense.db")
    repository.upsert_daily_prices(
        [
            _price(date(2026, 8, 4), 100.0),
            _price(MARKET_DATE, 105.0),
            _price(date(2026, 8, 6), 999.0),
        ]
    )
    repository.upsert_company_metrics(
        [
            _metric(date(2026, 8, 4), 20.0),
            _metric(date(2026, 8, 6), 99.0),
        ]
    )
    original_prices = repository.list_daily_prices
    original_metrics = repository.list_company_metrics
    requested_cutoffs: list[tuple[str, date | None]] = []

    def leaky_prices(symbol: str, start_date=None, end_date=None):
        del start_date
        requested_cutoffs.append(("prices", end_date))
        return original_prices(symbol)

    def leaky_metrics(symbol: str, start_date=None, end_date=None):
        del start_date
        requested_cutoffs.append(("metrics", end_date))
        return original_metrics(symbol)

    repository.list_daily_prices = leaky_prices  # type: ignore[method-assign]
    repository.list_company_metrics = leaky_metrics  # type: ignore[method-assign]

    payload = _service(repository, report_repository).generate(
        "2330",
        MARKET_DATE,
    ).canonical.payload

    assert requested_cutoffs == [
        ("prices", MARKET_DATE),
        ("metrics", MARKET_DATE),
    ]
    assert payload["data_quality"]["latest_price_date"] == "2026-08-05"
    assert payload["metrics"]["latest_close"]["value"] == 105.0
    assert payload["valuation"]["pe_ratio"] == {
        "status": "available",
        "value": 20.0,
        "unit": "ratio",
        "as_of_date": "2026-08-04",
    }


@dataclass
class _ValidationRepository:
    run: CrossValidationRun

    def get_run(self, run_id: str):
        return self.run if run_id == self.run.run_id else None

    def list_runs(self, symbol: str):
        return [self.run] if symbol == self.run.symbol else []

    def list_discrepancies(self, run_id: str):
        del run_id
        raise AssertionError("future validation discrepancies must not be read")

    def list_observations(self, run_id: str):
        del run_id
        return []


def test_6b_adapter_rejects_explicit_future_validation_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, report_repository = _setup(tmp_path / "future-validation.db")
    repository.upsert_daily_prices([_price(MARKET_DATE, 105.0)])
    future_date = date(2026, 8, 6)
    run = CrossValidationRun(
        run_id="future-validation",
        symbol="2330",
        target_date=future_date,
        requested_start_date=date(2026, 8, 4),
        left_provider="twse",
        right_provider="esun",
        status=PipelineRunStatus.SUCCESS,
        outcome=CrossValidationOutcome.DISCREPANCY,
        created_at=FIXED_NOW,
        started_at=FIXED_NOW,
        finished_at=FIXED_NOW,
        error_message=None,
        attempt_count=1,
    )
    policy = _RecordingAsOfPolicy()
    monkeypatch.setattr(daily_research, "_AS_OF_POLICY", policy)
    payload = _service(
        repository,
        report_repository,
        validation_repository=_ValidationRepository(run),
    ).build_payload(
        "2330",
        MARKET_DATE,
        requested_date=MARKET_DATE,
        validation_run_id=run.run_id,
    )

    assert payload["data_quality"]["validation_status"] == "missing_source"
    assert payload["data_quality"]["validation_run_id"] is None
    assert payload["data_quality"]["discrepancy_count"] == 0
    assert (
        "validation_target_is_consistent",
        (future_date, MARKET_DATE),
    ) in policy.calls


def test_6b_adapter_validates_previous_candidate_without_changing_sql_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, report_repository = _setup(tmp_path / "previous.db")
    previous_date = date(2026, 8, 1)
    repository.upsert_daily_prices(
        [
            _price(previous_date, 100.0),
            _price(MARKET_DATE, 110.0),
        ]
    )
    service = _service(repository, report_repository)
    previous = service.generate("2330", previous_date)
    policy = _RecordingAsOfPolicy()
    monkeypatch.setattr(daily_research, "_AS_OF_POLICY", policy)

    current = service.generate("2330", MARKET_DATE)
    comparison = current.canonical.payload["comparison"]

    assert comparison["status"] == "available"
    assert comparison["previous_result_id"] == previous.canonical.result_id
    assert comparison["previous_market_date"] == previous_date.isoformat()
    assert (
        "previous_result_is_comparable",
        (
            previous.canonical.result_id,
            "2330",
            MARKET_DATE,
            METHODOLOGY_VERSION,
        ),
    ) in policy.calls


def test_6b_adapter_excludes_future_previous_candidate_even_if_query_leaks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, report_repository = _setup(tmp_path / "future-result.db")
    future_date = date(2026, 8, 6)
    repository.upsert_daily_prices(
        [
            _price(MARKET_DATE, 105.0),
            _price(future_date, 999.0),
        ]
    )
    service = _service(repository, report_repository)
    future = service.generate("2330", future_date)
    monkeypatch.setattr(
        report_repository,
        "get_previous_successful_result",
        lambda symbol, market_date, methodology_version: future.canonical,
    )

    current = service.generate("2330", MARKET_DATE)
    comparison = current.canonical.payload["comparison"]

    assert comparison == {
        "status": "previous_result_missing",
        "previous_result_id": None,
        "previous_market_date": None,
        "changes": [],
    }
