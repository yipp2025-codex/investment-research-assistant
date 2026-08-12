"""Narrow read/write ports used by the Phase 6B report service.

These protocols deliberately expose only the capabilities needed by the
service.  SQLite lifecycle and SQL remain implementation details of the
composition adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol, runtime_checkable

from app.storage.daily_report import (
    StoredDailyResearchReport,
    StoredDailyResearchResult,
)


@dataclass(frozen=True, slots=True)
class DailyBatchContext:
    """Batch-level fields required to generate one frozen Phase 6B report."""

    batch_run_id: str
    requested_date: date
    resolved_market_date: date | None


@dataclass(frozen=True, slots=True)
class DailyBatchSymbolContext:
    """Per-symbol batch fields required by report provenance."""

    symbol_run_id: str
    batch_run_id: str
    symbol: str
    status: str
    pipeline_run_id: str | None


@runtime_checkable
class DailyResultReader(Protocol):
    """Read existing canonical results, reports, and comparison candidates."""

    def get_result(
        self,
        symbol: str,
        market_date: date,
        methodology_version: str,
    ) -> StoredDailyResearchResult | None: ...

    def get_previous_successful_result(
        self,
        symbol: str,
        market_date: date,
        methodology_version: str,
    ) -> StoredDailyResearchResult | None: ...

    def get_previous_result_any_methodology(
        self,
        symbol: str,
        market_date: date,
    ) -> StoredDailyResearchResult | None: ...

    def get_report(
        self,
        symbol: str,
        market_date: date,
        methodology_version: str,
    ) -> StoredDailyResearchReport | None: ...


@runtime_checkable
class DailyResultStore(Protocol):
    """Initialize and persist canonical results and rendered reports."""

    def initialize(self) -> None: ...

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
    ) -> StoredDailyResearchResult: ...

    def save_report_rendered(
        self,
        *,
        result: StoredDailyResearchResult,
        markdown: str,
        updated_at: datetime | None = None,
    ) -> StoredDailyResearchReport: ...

    def mark_report_render_failed(
        self,
        *,
        result: StoredDailyResearchResult,
        error_message: str,
        updated_at: datetime | None = None,
    ) -> StoredDailyResearchReport: ...


@runtime_checkable
class BatchContextReader(Protocol):
    """Read only the batch context needed by ``generate_for_batch_symbol``."""

    def get_batch_run(self, batch_run_id: str) -> DailyBatchContext | None: ...

    def get_symbol_run(
        self,
        batch_run_id: str,
        symbol: str,
    ) -> DailyBatchSymbolContext | None: ...


__all__ = [
    "BatchContextReader",
    "DailyBatchContext",
    "DailyBatchSymbolContext",
    "DailyResultReader",
    "DailyResultStore",
]
