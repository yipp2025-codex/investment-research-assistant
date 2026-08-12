"""Production and compatibility composition for the Phase 6B report service.

Concrete SQLite repositories stay in this module.  The report service itself
receives only the narrow contracts from :mod:`app.reports.contracts`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from app.analysis import HistoricalResearchAnalyzer
from app.reports.contracts import (
    BatchContextReader,
    DailyBatchContext,
    DailyBatchSymbolContext,
    DailyResultReader,
    DailyResultStore,
)
from app.reports.daily_research import (
    ChangeThresholds,
    DailyResearchReportService,
)
from app.research_dataset import (
    ResearchDataset,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
)
from app.sqlite_research_dataset import SQLiteResearchDataset
from app.storage import (
    SQLiteCrossValidationRepository,
    SQLiteDailyReportRepository,
    SQLiteResearchRepository,
    StoredDailyResearchReport,
    StoredDailyResearchResult,
)
from app.storage.batch_run import SQLiteBatchRunRepository


class SQLiteDailyResultReader(DailyResultReader):
    """Read-only capability view over the existing SQLite report repository."""

    __slots__ = ("_repository",)

    def __init__(self, repository: SQLiteDailyReportRepository) -> None:
        self._repository = repository

    def get_result(
        self,
        symbol: str,
        market_date: date,
        methodology_version: str,
    ) -> StoredDailyResearchResult | None:
        return self._repository.get_result(symbol, market_date, methodology_version)

    def get_previous_successful_result(
        self,
        symbol: str,
        market_date: date,
        methodology_version: str,
    ) -> StoredDailyResearchResult | None:
        return self._repository.get_previous_successful_result(
            symbol,
            market_date,
            methodology_version,
        )

    def get_previous_result_any_methodology(
        self,
        symbol: str,
        market_date: date,
    ) -> StoredDailyResearchResult | None:
        return self._repository.get_previous_result_any_methodology(
            symbol,
            market_date,
        )

    def get_report(
        self,
        symbol: str,
        market_date: date,
        methodology_version: str,
    ) -> StoredDailyResearchReport | None:
        return self._repository.get_report(symbol, market_date, methodology_version)


class SQLiteDailyResultStore(DailyResultStore):
    """Write-only capability view over the existing SQLite report repository."""

    __slots__ = ("_repository",)

    def __init__(self, repository: SQLiteDailyReportRepository) -> None:
        self._repository = repository

    def initialize(self) -> None:
        self._repository.initialize()

    def save_or_get_result(
        self,
        *,
        result_id: str,
        symbol: str,
        market_date: date,
        requested_date: date,
        methodology_version: str,
        schema_version: str,
        data_quality_status: str,
        payload_json: str,
        payload_sha256: str,
        provenance_json: str,
        created_at: datetime | None = None,
    ) -> StoredDailyResearchResult:
        return self._repository.save_or_get_result(
            result_id=result_id,
            symbol=symbol,
            market_date=market_date,
            requested_date=requested_date,
            methodology_version=methodology_version,
            schema_version=schema_version,
            data_quality_status=data_quality_status,
            payload_json=payload_json,
            payload_sha256=payload_sha256,
            provenance_json=provenance_json,
            created_at=created_at,
        )

    def save_report_rendered(
        self,
        *,
        result: StoredDailyResearchResult,
        markdown: str,
        updated_at: datetime | None = None,
    ) -> StoredDailyResearchReport:
        return self._repository.save_report_rendered(
            result=result,
            markdown=markdown,
            updated_at=updated_at,
        )

    def mark_report_render_failed(
        self,
        *,
        result: StoredDailyResearchResult,
        error_message: str,
        updated_at: datetime | None = None,
    ) -> StoredDailyResearchReport:
        return self._repository.mark_report_render_failed(
            result=result,
            error_message=error_message,
            updated_at=updated_at,
        )


class SQLiteBatchContextReader(BatchContextReader):
    """Narrow adapter over the existing Phase 6A batch repository."""

    __slots__ = ("_research_repository", "_repository", "_repository_factory")

    def __init__(
        self,
        research_repository: SQLiteResearchRepository,
        *,
        repository_factory: Callable[
            [SQLiteResearchRepository], SQLiteBatchRunRepository
        ]
        | None = None,
    ) -> None:
        self._research_repository = research_repository
        self._repository: SQLiteBatchRunRepository | None = None
        self._repository_factory = repository_factory

    def _reader(self) -> SQLiteBatchRunRepository:
        if self._repository is None:
            factory = self._repository_factory or SQLiteBatchRunRepository
            self._repository = factory(self._research_repository)
            self._repository.initialize()
        return self._repository

    def get_batch_run(self, batch_run_id: str) -> DailyBatchContext | None:
        batch = self._reader().get_batch_run(batch_run_id)
        if batch is None:
            return None
        return DailyBatchContext(
            batch_run_id=batch.batch_run_id,
            requested_date=batch.requested_date,
            resolved_market_date=batch.resolved_market_date,
        )

    def get_symbol_run(
        self,
        batch_run_id: str,
        symbol: str,
    ) -> DailyBatchSymbolContext | None:
        symbol_run = self._reader().get_symbol_run(batch_run_id, symbol)
        if symbol_run is None:
            return None
        return DailyBatchSymbolContext(
            symbol_run_id=symbol_run.symbol_run_id,
            batch_run_id=symbol_run.batch_run_id,
            symbol=symbol_run.symbol,
            status=symbol_run.status.value,
            pipeline_run_id=symbol_run.pipeline_run_id,
        )


class _LazySQLiteResearchDataset(ResearchDataset):
    """Delay schema validation/opening until a non-replay report needs data."""

    __slots__ = ("_database_path", "_delegate")

    def __init__(self, database_path: str | Path) -> None:
        self._database_path = database_path
        self._delegate: SQLiteResearchDataset | None = None

    def read(
        self,
        request: ResearchDatasetRequest,
        /,
    ) -> ResearchDatasetSnapshot:
        if self._delegate is None:
            self._delegate = SQLiteResearchDataset(self._database_path)
        return self._delegate.read(request)


@dataclass(frozen=True, slots=True)
class DailyResearchServiceDependencies:
    """Resolved ports, with legacy readers isolated to compatibility callers."""

    dataset: ResearchDataset | None
    result_reader: DailyResultReader
    result_store: DailyResultStore
    batch_context_reader: BatchContextReader
    legacy_research_reader: object | None = None
    legacy_validation_reader: object | None = None


def compose_compatibility_dependencies(
    repository: object,
    *,
    report_repository: object | None,
    cross_validation_repository: object | None,
    dataset: ResearchDataset | None,
) -> DailyResearchServiceDependencies:
    """Preserve the frozen pre-M9 constructor while isolating production ports."""

    if repository is None:
        raise TypeError(
            "repository is required unless result_reader/result_store/"
            "batch_context_reader are supplied"
        )
    report_adapter = report_repository or SQLiteDailyReportRepository(repository)
    result_reader = SQLiteDailyResultReader(report_adapter)
    result_store = SQLiteDailyResultStore(report_adapter)
    batch_reader = SQLiteBatchContextReader(repository)

    auto_dataset = (
        dataset is None
        and isinstance(repository, SQLiteResearchRepository)
        and (
            cross_validation_repository is None
            or isinstance(
                cross_validation_repository,
                SQLiteCrossValidationRepository,
            )
        )
        and "list_daily_prices" not in repository.__dict__
        and "list_company_metrics" not in repository.__dict__
    )
    selected_dataset = (
        _LazySQLiteResearchDataset(repository.database_path)
        if auto_dataset
        else dataset
    )
    if selected_dataset is not None:
        return DailyResearchServiceDependencies(
            dataset=selected_dataset,
            result_reader=result_reader,
            result_store=result_store,
            batch_context_reader=batch_reader,
        )

    validation_reader = (
        cross_validation_repository
        if cross_validation_repository is not None
        else SQLiteCrossValidationRepository(repository)
    )
    return DailyResearchServiceDependencies(
        dataset=None,
        result_reader=result_reader,
        result_store=result_store,
        batch_context_reader=batch_reader,
        legacy_research_reader=repository,
        legacy_validation_reader=validation_reader,
    )


def compose_sqlite_daily_research_report_service(
    repository: SQLiteResearchRepository,
    *,
    report_repository: SQLiteDailyReportRepository | None = None,
    dataset: ResearchDataset | None = None,
    analyzer: HistoricalResearchAnalyzer | None = None,
    thresholds: ChangeThresholds | None = None,
    methodology_version: str | None = None,
    clock: Callable[[], datetime] | None = None,
    renderer: Callable[[Mapping[str, object]], str] | None = None,
) -> DailyResearchReportService:
    """Production root that wires existing SQLite adapters into Phase 6B."""

    report_adapter = report_repository or SQLiteDailyReportRepository(repository)
    kwargs: dict[str, object] = {
        "dataset": (
            dataset
            if dataset is not None
            else _LazySQLiteResearchDataset(repository.database_path)
        ),
        "result_reader": SQLiteDailyResultReader(report_adapter),
        "result_store": SQLiteDailyResultStore(report_adapter),
        "batch_context_reader": SQLiteBatchContextReader(repository),
        "analyzer": analyzer,
        "thresholds": thresholds,
        "clock": clock,
        "renderer": renderer,
    }
    if methodology_version is not None:
        kwargs["methodology_version"] = methodology_version
    return DailyResearchReportService(**kwargs)


__all__ = [
    "DailyResearchServiceDependencies",
    "SQLiteBatchContextReader",
    "SQLiteDailyResultReader",
    "SQLiteDailyResultStore",
    "compose_compatibility_dependencies",
    "compose_sqlite_daily_research_report_service",
]
