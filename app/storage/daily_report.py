"""Durable storage for Phase 6B canonical daily research results and reports."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

from app.storage.batch_run import SQLiteBatchRunRepository
from app.storage.sqlite import SQLiteResearchRepository


class DailyReportError(RuntimeError):
    """Base class for canonical daily report storage errors."""


class DailyReportConflictError(DailyReportError):
    """The immutable daily-result key already has different canonical content."""


class DailyReportStateError(DailyReportError):
    """A stored daily result/report violates the Phase 6B contract."""


@dataclass(frozen=True, slots=True)
class StoredDailyResearchResult:
    result_id: str
    symbol: str
    market_date: date
    requested_date: date
    methodology_version: str
    schema_version: str
    result_status: str
    data_quality_status: str
    payload_json: str
    payload_sha256: str
    provenance_json: str
    created_at: datetime
    updated_at: datetime

    @property
    def payload(self) -> dict[str, object]:
        value = json.loads(self.payload_json)
        if not isinstance(value, dict):
            raise DailyReportStateError("stored daily result payload is not an object")
        return value

    @property
    def provenance(self) -> dict[str, object]:
        value = json.loads(self.provenance_json)
        if not isinstance(value, dict):
            raise DailyReportStateError(
                "stored daily result provenance is not an object"
            )
        return value


@dataclass(frozen=True, slots=True)
class StoredDailyResearchReport:
    report_id: str
    result_id: str
    symbol: str
    market_date: date
    methodology_version: str
    report_status: str
    markdown: str | None
    markdown_sha256: str | None
    render_error: str | None
    created_at: datetime
    updated_at: datetime


class SQLiteDailyReportRepository:
    """Stores immutable canonical results and independently retryable reports."""

    def __init__(self, research_repository: SQLiteResearchRepository) -> None:
        self.research_repository = research_repository

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self.research_repository._transaction() as connection:
            yield connection

    def initialize(self) -> None:
        """Ensure Phase 1-6A schema exists, then apply migration 0008."""
        self.research_repository.initialize()
        SQLiteBatchRunRepository(self.research_repository).initialize()
        migration_path = (
            Path(__file__).with_name("migrations") / "0008_daily_research_reports.sql"
        )
        with self._transaction() as connection:
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = 8"
            ).fetchone()
            required_tables = {
                "daily_research_results",
                "daily_research_reports",
            }
            present_tables = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if applied is not None:
                missing = required_tables - present_tables
                if missing:
                    raise DailyReportStateError(
                        "schema migration 8 is recorded but tables are missing: "
                        + ", ".join(sorted(missing))
                    )
                return
            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            connection.execute(
                "INSERT INTO schema_migrations (version, name) "
                "VALUES (8, 'phase 6b daily research reports')"
            )

    @staticmethod
    def _execute_sql_statements(
        connection: sqlite3.Connection, script: str
    ) -> None:
        pending = ""
        for line in script.splitlines(keepends=True):
            pending += line
            if sqlite3.complete_statement(pending):
                statement = pending.strip()
                if statement:
                    connection.execute(statement)
                pending = ""
        if pending.strip():
            raise sqlite3.OperationalError("incomplete migration SQL statement")

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
        """Insert a canonical result once; replay returns the stored row."""
        if created_at is None:
            created_at = datetime.now(timezone.utc)
        if created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        computed_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        if computed_hash != payload_sha256:
            raise DailyReportStateError("payload_sha256 does not match payload_json")
        normalized_symbol = symbol.strip().upper()
        timestamp = created_at.isoformat()
        with self._transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO daily_research_results ("
                "result_id, symbol, market_date, requested_date, methodology_version, "
                "schema_version, result_status, data_quality_status, payload_json, "
                "payload_sha256, provenance_json, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, 'success', ?, ?, ?, ?, ?, ?)",
                (
                    result_id,
                    normalized_symbol,
                    market_date.isoformat(),
                    requested_date.isoformat(),
                    methodology_version,
                    schema_version,
                    data_quality_status,
                    payload_json,
                    payload_sha256,
                    provenance_json,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                self._result_select()
                + " WHERE symbol = ? AND market_date = ? AND methodology_version = ?",
                (
                    normalized_symbol,
                    market_date.isoformat(),
                    methodology_version,
                ),
            ).fetchone()
        if row is None:
            raise DailyReportStateError("daily result was not persisted")
        stored = self._result_from_row(row)
        if stored.payload_sha256 != payload_sha256 or stored.payload_json != payload_json:
            raise DailyReportConflictError(
                "daily result key already contains different canonical payload"
            )
        return stored

    def get_result(
        self, symbol: str, market_date: date, methodology_version: str
    ) -> StoredDailyResearchResult | None:
        with self._transaction() as connection:
            row = connection.execute(
                self._result_select()
                + " WHERE symbol = ? AND market_date = ? AND methodology_version = ?",
                (symbol.strip().upper(), market_date.isoformat(), methodology_version),
            ).fetchone()
        return None if row is None else self._result_from_row(row)

    def get_previous_successful_result(
        self, symbol: str, market_date: date, methodology_version: str
    ) -> StoredDailyResearchResult | None:
        """Return the nearest earlier market date, never yesterday-calendar lookup."""
        with self._transaction() as connection:
            row = connection.execute(
                self._result_select()
                + " WHERE symbol = ? AND methodology_version = ? "
                "AND result_status = 'success' AND market_date < ? "
                "ORDER BY market_date DESC LIMIT 1",
                (
                    symbol.strip().upper(),
                    methodology_version,
                    market_date.isoformat(),
                ),
            ).fetchone()
        return None if row is None else self._result_from_row(row)

    def get_previous_result_any_methodology(
        self, symbol: str, market_date: date
    ) -> StoredDailyResearchResult | None:
        """Return the nearest earlier successful result, regardless of method."""
        with self._transaction() as connection:
            row = connection.execute(
                self._result_select()
                + " WHERE symbol = ? AND result_status = 'success' "
                "AND market_date < ? ORDER BY market_date DESC LIMIT 1",
                (symbol.strip().upper(), market_date.isoformat()),
            ).fetchone()
        return None if row is None else self._result_from_row(row)

    def list_results(
        self, symbol: str, methodology_version: str
    ) -> list[StoredDailyResearchResult]:
        with self._transaction() as connection:
            rows = connection.execute(
                self._result_select()
                + " WHERE symbol = ? AND methodology_version = ? "
                "ORDER BY market_date",
                (symbol.strip().upper(), methodology_version),
            ).fetchall()
        return [self._result_from_row(row) for row in rows]

    def save_report_rendered(
        self,
        *,
        result: StoredDailyResearchResult,
        markdown: str,
        updated_at: datetime | None = None,
    ) -> StoredDailyResearchReport:
        if updated_at is None:
            updated_at = datetime.now(timezone.utc)
        if updated_at.utcoffset() is None:
            raise ValueError("updated_at must be timezone-aware")
        markdown_sha256 = hashlib.sha256(markdown.encode("utf-8")).hexdigest()
        report_id = f"report-{result.result_id}"
        timestamp = updated_at.isoformat()
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO daily_research_reports ("
                "report_id, result_id, symbol, market_date, methodology_version, "
                "report_status, markdown, markdown_sha256, render_error, "
                "created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, 'rendered', ?, ?, NULL, ?, ?) "
                "ON CONFLICT(result_id) DO UPDATE SET "
                "report_status = 'rendered', markdown = excluded.markdown, "
                "markdown_sha256 = excluded.markdown_sha256, render_error = NULL, "
                "updated_at = excluded.updated_at",
                (
                    report_id,
                    result.result_id,
                    result.symbol,
                    result.market_date.isoformat(),
                    result.methodology_version,
                    markdown,
                    markdown_sha256,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                self._report_select() + " WHERE result_id = ?",
                (result.result_id,),
            ).fetchone()
        if row is None:
            raise DailyReportStateError("rendered report was not persisted")
        return self._report_from_row(row)

    def mark_report_render_failed(
        self,
        *,
        result: StoredDailyResearchResult,
        error_message: str,
        updated_at: datetime | None = None,
    ) -> StoredDailyResearchReport:
        if updated_at is None:
            updated_at = datetime.now(timezone.utc)
        if updated_at.utcoffset() is None:
            raise ValueError("updated_at must be timezone-aware")
        safe_error = error_message.replace("\r", " ").replace("\n", " ").strip()
        safe_error = safe_error[:1000] or "UnknownRenderError"
        report_id = f"report-{result.result_id}"
        timestamp = updated_at.isoformat()
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO daily_research_reports ("
                "report_id, result_id, symbol, market_date, methodology_version, "
                "report_status, markdown, markdown_sha256, render_error, "
                "created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, 'failed', NULL, NULL, ?, ?, ?) "
                "ON CONFLICT(result_id) DO UPDATE SET "
                "report_status = 'failed', markdown = NULL, markdown_sha256 = NULL, "
                "render_error = excluded.render_error, updated_at = excluded.updated_at",
                (
                    report_id,
                    result.result_id,
                    result.symbol,
                    result.market_date.isoformat(),
                    result.methodology_version,
                    safe_error,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                self._report_select() + " WHERE result_id = ?",
                (result.result_id,),
            ).fetchone()
        if row is None:
            raise DailyReportStateError("failed report was not persisted")
        return self._report_from_row(row)

    def get_report(
        self, symbol: str, market_date: date, methodology_version: str
    ) -> StoredDailyResearchReport | None:
        with self._transaction() as connection:
            row = connection.execute(
                self._report_select()
                + " WHERE symbol = ? AND market_date = ? AND methodology_version = ?",
                (symbol.strip().upper(), market_date.isoformat(), methodology_version),
            ).fetchone()
        return None if row is None else self._report_from_row(row)

    def count_results(self, symbol: str | None = None) -> int:
        with self._transaction() as connection:
            if symbol is None:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM daily_research_results"
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM daily_research_results WHERE symbol = ?",
                    (symbol.strip().upper(),),
                ).fetchone()
        return int(row["count"])

    def count_reports(self, symbol: str | None = None) -> int:
        with self._transaction() as connection:
            if symbol is None:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM daily_research_reports"
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM daily_research_reports WHERE symbol = ?",
                    (symbol.strip().upper(),),
                ).fetchone()
        return int(row["count"])

    @staticmethod
    def _result_select() -> str:
        return (
            "SELECT result_id, symbol, market_date, requested_date, "
            "methodology_version, schema_version, result_status, "
            "data_quality_status, payload_json, payload_sha256, provenance_json, "
            "created_at, updated_at FROM daily_research_results"
        )

    @staticmethod
    def _report_select() -> str:
        return (
            "SELECT report_id, result_id, symbol, market_date, methodology_version, "
            "report_status, markdown, markdown_sha256, render_error, "
            "created_at, updated_at FROM daily_research_reports"
        )

    @staticmethod
    def _result_from_row(row: sqlite3.Row) -> StoredDailyResearchResult:
        return StoredDailyResearchResult(
            result_id=row["result_id"],
            symbol=row["symbol"],
            market_date=date.fromisoformat(row["market_date"]),
            requested_date=date.fromisoformat(row["requested_date"]),
            methodology_version=row["methodology_version"],
            schema_version=row["schema_version"],
            result_status=row["result_status"],
            data_quality_status=row["data_quality_status"],
            payload_json=row["payload_json"],
            payload_sha256=row["payload_sha256"],
            provenance_json=row["provenance_json"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _report_from_row(row: sqlite3.Row) -> StoredDailyResearchReport:
        return StoredDailyResearchReport(
            report_id=row["report_id"],
            result_id=row["result_id"],
            symbol=row["symbol"],
            market_date=date.fromisoformat(row["market_date"]),
            methodology_version=row["methodology_version"],
            report_status=row["report_status"],
            markdown=row["markdown"],
            markdown_sha256=row["markdown_sha256"],
            render_error=row["render_error"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )
