"""M9.5 architecture gates for the ResearchDataset service boundary."""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import get_type_hints

import app.reports.daily_research as daily_research_module
from app.reports.contracts import (
    BatchContextReader,
    DailyBatchContext,
    DailyBatchSymbolContext,
    DailyResultReader,
    DailyResultStore,
)
from app.reports.daily_research import DailyResearchReportService
from app.research_dataset import (
    DatasetPrice,
    DatasetProvenance,
    DatasetSymbol,
    DatasetValuationMetric,
    FakeResearchDataset,
    ResearchDataset,
    ResearchDatasetRequest,
)
from app.storage import StoredDailyResearchReport, StoredDailyResearchResult


UTC = timezone.utc
MARKET_DATE = date(2026, 8, 5)
FIXED_NOW = datetime(2026, 8, 9, 3, tzinfo=UTC)


class _CountingDataset:
    def __init__(self) -> None:
        prices = tuple(
            DatasetPrice(
                symbol="2330",
                trade_date=MARKET_DATE - timedelta(days=offset),
                open=100.0 + offset,
                high=102.0 + offset,
                low=99.0 + offset,
                close=101.0 + offset,
                volume=1_000 + offset,
                source="twse-historical",
            )
            for offset in (1, 0)
        )
        valuations = tuple(
            DatasetValuationMetric(
                symbol="2330",
                metric_date=MARKET_DATE,
                name=name,
                value=value,
                unit=unit,
                source="twse",
            )
            for name, value, unit in (
                ("price_earnings_ratio", 20.0, "ratio"),
                ("price_to_book_ratio", 4.0, "ratio"),
                ("dividend_yield_pct", 2.0, "percent"),
            )
        )
        self._delegate = FakeResearchDataset(
            symbols=(DatasetSymbol("2330", "Fake 2330", "TWSE", "TWD"),),
            prices=prices,
            valuations=valuations,
            provenance=(
                DatasetProvenance(
                    symbol="2330",
                    canonical_sources=("twse", "twse-historical"),
                ),
            ),
        )
        self.requests: list[ResearchDatasetRequest] = []

    def read(self, request: ResearchDatasetRequest):
        self.requests.append(request)
        return self._delegate.read(request)


@dataclass
class _ResultState:
    results: dict[tuple[str, date, str], StoredDailyResearchResult] = field(
        default_factory=dict
    )
    reports: dict[tuple[str, date, str], StoredDailyResearchReport] = field(
        default_factory=dict
    )


class _FakeDailyResultReader:
    def __init__(self, state: _ResultState) -> None:
        self._state = state

    def get_result(self, symbol: str, market_date: date, methodology_version: str):
        return self._state.results.get((symbol, market_date, methodology_version))

    def get_previous_successful_result(
        self,
        symbol: str,
        market_date: date,
        methodology_version: str,
    ):
        candidates = [
            item
            for (item_symbol, item_date, item_method), item in self._state.results.items()
            if item_symbol == symbol
            and item_date < market_date
            and item_method == methodology_version
            and item.result_status == "success"
        ]
        return max(candidates, key=lambda item: item.market_date, default=None)

    def get_previous_result_any_methodology(
        self,
        symbol: str,
        market_date: date,
    ):
        candidates = [
            item
            for (item_symbol, item_date, _), item in self._state.results.items()
            if item_symbol == symbol
            and item_date < market_date
            and item.result_status == "success"
        ]
        return max(candidates, key=lambda item: item.market_date, default=None)

    def get_report(self, symbol: str, market_date: date, methodology_version: str):
        return self._state.reports.get((symbol, market_date, methodology_version))


class _FakeDailyResultStore:
    def __init__(self, state: _ResultState) -> None:
        self._state = state
        self.initialize_calls = 0
        self.result_write_calls = 0
        self.report_write_calls = 0

    def initialize(self) -> None:
        self.initialize_calls += 1

    def save_or_get_result(self, **values: object) -> StoredDailyResearchResult:
        self.result_write_calls += 1
        key = (
            str(values["symbol"]),
            values["market_date"],
            str(values["methodology_version"]),
        )
        existing = self._state.results.get(key)
        if existing is not None:
            return existing
        created_at = values["created_at"]
        assert isinstance(created_at, datetime)
        result = StoredDailyResearchResult(
            result_id=str(values["result_id"]),
            symbol=str(values["symbol"]),
            market_date=values["market_date"],  # type: ignore[arg-type]
            requested_date=values["requested_date"],  # type: ignore[arg-type]
            methodology_version=str(values["methodology_version"]),
            schema_version=str(values["schema_version"]),
            result_status="success",
            data_quality_status=str(values["data_quality_status"]),
            payload_json=str(values["payload_json"]),
            payload_sha256=str(values["payload_sha256"]),
            provenance_json=str(values["provenance_json"]),
            created_at=created_at,
            updated_at=created_at,
        )
        self._state.results[key] = result
        return result

    def save_report_rendered(
        self,
        *,
        result: StoredDailyResearchResult,
        markdown: str,
        updated_at: datetime | None = None,
    ) -> StoredDailyResearchReport:
        self.report_write_calls += 1
        timestamp = updated_at or FIXED_NOW
        report = StoredDailyResearchReport(
            report_id=f"report-{result.result_id}",
            result_id=result.result_id,
            symbol=result.symbol,
            market_date=result.market_date,
            methodology_version=result.methodology_version,
            report_status="rendered",
            markdown=markdown,
            markdown_sha256=hashlib.sha256(markdown.encode("utf-8")).hexdigest(),
            render_error=None,
            created_at=timestamp,
            updated_at=timestamp,
        )
        self._state.reports[
            (result.symbol, result.market_date, result.methodology_version)
        ] = report
        return report

    def mark_report_render_failed(
        self,
        *,
        result: StoredDailyResearchResult,
        error_message: str,
        updated_at: datetime | None = None,
    ) -> StoredDailyResearchReport:
        timestamp = updated_at or FIXED_NOW
        report = StoredDailyResearchReport(
            report_id=f"report-{result.result_id}",
            result_id=result.result_id,
            symbol=result.symbol,
            market_date=result.market_date,
            methodology_version=result.methodology_version,
            report_status="failed",
            markdown=None,
            markdown_sha256=None,
            render_error=error_message,
            created_at=timestamp,
            updated_at=timestamp,
        )
        self._state.reports[
            (result.symbol, result.market_date, result.methodology_version)
        ] = report
        return report


class _FakeBatchContextReader:
    def get_batch_run(self, batch_run_id: str):
        if batch_run_id != "batch-fake":
            return None
        return DailyBatchContext(batch_run_id, MARKET_DATE, MARKET_DATE)

    def get_symbol_run(self, batch_run_id: str, symbol: str):
        if batch_run_id != "batch-fake" or symbol.strip().upper() != "2330":
            return None
        return DailyBatchSymbolContext(
            "symbol-fake",
            batch_run_id,
            "2330",
            "success",
            None,
        )


def _public_methods(contract: type[object]) -> set[str]:
    return {
        name
        for name, value in contract.__dict__.items()
        if not name.startswith("_") and callable(value)
    }


def test_report_service_runs_with_only_fake_narrow_ports_and_replays_without_data_read() -> None:
    dataset = _CountingDataset()
    state = _ResultState()
    reader = _FakeDailyResultReader(state)
    store = _FakeDailyResultStore(state)
    batch_reader = _FakeBatchContextReader()
    service = DailyResearchReportService(
        dataset=dataset,
        result_reader=reader,
        result_store=store,
        batch_context_reader=batch_reader,
        clock=lambda: FIXED_NOW,
    )

    first = service.generate_for_batch_symbol("batch-fake", "2330")
    replay = service.generate_for_batch_symbol("batch-fake", "2330")

    assert first.idempotent_replay is False
    assert replay.idempotent_replay is True
    assert first.canonical.payload_json == replay.canonical.payload_json
    assert first.canonical.payload_sha256 == replay.canonical.payload_sha256
    assert first.report.markdown == replay.report.markdown
    assert dataset.requests == [
        ResearchDatasetRequest(
            "2330",
            MARKET_DATE,
            history_observations=None,
        )
    ]
    assert store.initialize_calls == 2
    assert store.result_write_calls == 1
    assert store.report_write_calls == 1


def test_service_annotations_and_source_do_not_require_concrete_sqlite_types() -> None:
    hints = get_type_hints(DailyResearchReportService.__init__)
    source = inspect.getsource(daily_research_module)

    assert hints["dataset"] == ResearchDataset | None
    assert hints["result_reader"] == DailyResultReader | None
    assert hints["result_store"] == DailyResultStore | None
    assert hints["batch_context_reader"] == BatchContextReader | None
    assert "SQLiteResearchRepository" not in source
    assert "SQLiteDailyReportRepository" not in source
    assert "SQLiteBatchRunRepository" not in source
    assert "SQLiteResearchDataset" not in source
    assert "sqlite3" not in source
    assert "app.providers" not in source


def test_dataset_reader_store_and_batch_capabilities_remain_disjoint() -> None:
    dataset_methods = _public_methods(ResearchDataset)
    reader_methods = _public_methods(DailyResultReader)
    store_methods = _public_methods(DailyResultStore)
    batch_methods = _public_methods(BatchContextReader)

    assert dataset_methods == {"read"}
    assert reader_methods == {
        "get_result",
        "get_previous_successful_result",
        "get_previous_result_any_methodology",
        "get_report",
    }
    assert store_methods == {
        "initialize",
        "save_or_get_result",
        "save_report_rendered",
        "mark_report_render_failed",
    }
    assert batch_methods == {"get_batch_run", "get_symbol_run"}
    assert reader_methods.isdisjoint(store_methods)
    assert dataset_methods.isdisjoint(reader_methods | store_methods | batch_methods)
    assert batch_methods.isdisjoint(store_methods)
