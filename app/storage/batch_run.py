"""Watchlist and daily batch run persistence for Phase 6A."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Iterator, Sequence
from uuid import uuid4

from app.storage.sqlite import SQLiteResearchRepository


class BatchRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    SKIPPED_NON_TRADING_DAY = "skipped_non_trading_day"
    SKIPPED_NO_NEW_MARKET_DATE = "skipped_no_new_market_date"
    DEFERRED_AWAITING_MARKET_DATA = "deferred_awaiting_market_data"


class SymbolRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED_ALREADY_SUCCEEDED = "skipped_already_succeeded"
    SKIPPED_NON_TRADING_DAY = "skipped_non_trading_day"
    SKIPPED_NO_NEW_MARKET_DATE = "skipped_no_new_market_date"


class BatchRunError(RuntimeError):
    """Base class for daily batch run state errors."""


class BatchRunConflictError(BatchRunError):
    """An immutable batch contract conflict was detected."""


class BatchRunStateError(BatchRunError):
    """An invalid state transition was attempted."""


@dataclass(frozen=True, slots=True)
class WatchlistRevision:
    revision_id: str
    watchlist_id: str
    symbols: tuple[str, ...]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class DailyBatchRun:
    batch_run_id: str
    watchlist_revision_id: str
    requested_date: date
    resolved_market_date: date | None
    runner_policy_version: str
    status: BatchRunStatus
    total_symbols: int
    success_symbols: int
    failed_symbols: int
    skipped_symbols: int
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True, slots=True)
class DailySymbolRun:
    symbol_run_id: str
    batch_run_id: str
    symbol: str
    status: SymbolRunStatus
    attempt_count: int
    pipeline_run_id: str | None
    historical_run_id: str | None
    validation_run_id: str | None
    hist_validation_run_id: str | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class SQLiteBatchRunRepository:
    """Manages watchlists, revisions, and daily batch run checkpoints."""

    RUNNER_POLICY_VERSION = "6a-v1"

    def __init__(self, research_repository: SQLiteResearchRepository) -> None:
        self._repo = research_repository

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._repo.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        """Apply migration 0007 if not yet applied."""
        migration_path = (
            Path(self._repo.database_path).parent.parent
            / "app" / "storage" / "migrations" / "0007_daily_batch_runner.sql"
        )
        # Resolve relative to the migration directory inside the package.
        migration_path = (
            Path(__file__).with_name("migrations") / "0007_daily_batch_runner.sql"
        )
        with self._transaction() as conn:
            applied = conn.execute(
                "SELECT 1 FROM schema_migrations WHERE version = 7"
            ).fetchone()
            if applied is not None:
                return
            sql = migration_path.read_text(encoding="utf-8")
            self._execute_sql_statements(conn, sql)
            conn.execute(
                "INSERT INTO schema_migrations (version, name) "
                "VALUES (7, 'phase 6a daily batch runner')"
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

    # ------------------------------------------------------------------
    # Watchlist management
    # ------------------------------------------------------------------

    def get_or_create_watchlist(self, name: str) -> str:
        """Return watchlist_id for name, creating it if absent."""
        name = name.strip()
        if not name:
            raise ValueError("watchlist name must not be empty")
        watchlist_id = str(uuid4())
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO watchlists (watchlist_id, name) VALUES (?, ?)",
                (watchlist_id, name),
            )
            row = conn.execute(
                "SELECT watchlist_id FROM watchlists WHERE name = ?", (name,)
            ).fetchone()
        return row["watchlist_id"]

    def set_watchlist_members(
        self, watchlist_id: str, symbols: Sequence[str]
    ) -> None:
        """Replace active members of a watchlist with the given symbols."""
        normalized = [s.strip().upper() for s in symbols if s.strip()]
        with self._transaction() as conn:
            conn.execute(
                "UPDATE watchlist_members SET is_active = 0 WHERE watchlist_id = ?",
                (watchlist_id,),
            )
            for sym in normalized:
                conn.execute(
                    "INSERT INTO watchlist_members (watchlist_id, symbol, is_active) "
                    "VALUES (?, ?, 1) "
                    "ON CONFLICT(watchlist_id, symbol) DO UPDATE SET is_active = 1, "
                    "added_at = CURRENT_TIMESTAMP",
                    (watchlist_id, sym),
                )

    def get_active_watchlist_symbols(self, watchlist_id: str) -> list[str]:
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT symbol FROM watchlist_members "
                "WHERE watchlist_id = ? AND is_active = 1 ORDER BY symbol",
                (watchlist_id,),
            ).fetchall()
        return [row["symbol"] for row in rows]

    # ------------------------------------------------------------------
    # Revision (immutable snapshot)
    # ------------------------------------------------------------------

    def create_revision(self, watchlist_id: str) -> WatchlistRevision:
        """Lock the current active members into an immutable revision."""
        revision_id = str(uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._transaction() as conn:
            symbols_rows = conn.execute(
                "SELECT symbol FROM watchlist_members "
                "WHERE watchlist_id = ? AND is_active = 1 ORDER BY symbol",
                (watchlist_id,),
            ).fetchall()
            if not symbols_rows:
                raise BatchRunStateError(
                    "cannot create revision for watchlist with no active members"
                )
            symbols = tuple(row["symbol"] for row in symbols_rows)
            conn.execute(
                "INSERT INTO watchlist_revisions (revision_id, watchlist_id, created_at) "
                "VALUES (?, ?, ?)",
                (revision_id, watchlist_id, now_iso),
            )
            conn.executemany(
                "INSERT INTO watchlist_revision_members (revision_id, symbol) "
                "VALUES (?, ?)",
                [(revision_id, sym) for sym in symbols],
            )
        return WatchlistRevision(
            revision_id=revision_id,
            watchlist_id=watchlist_id,
            symbols=symbols,
            created_at=datetime.fromisoformat(now_iso),
        )

    def get_revision(self, revision_id: str) -> WatchlistRevision | None:
        with self._transaction() as conn:
            rev_row = conn.execute(
                "SELECT revision_id, watchlist_id, created_at "
                "FROM watchlist_revisions WHERE revision_id = ?",
                (revision_id,),
            ).fetchone()
            if rev_row is None:
                return None
            sym_rows = conn.execute(
                "SELECT symbol FROM watchlist_revision_members "
                "WHERE revision_id = ? ORDER BY symbol",
                (revision_id,),
            ).fetchall()
        return WatchlistRevision(
            revision_id=rev_row["revision_id"],
            watchlist_id=rev_row["watchlist_id"],
            symbols=tuple(row["symbol"] for row in sym_rows),
            created_at=datetime.fromisoformat(rev_row["created_at"]),
        )

    # ------------------------------------------------------------------
    # Daily batch runs
    # ------------------------------------------------------------------

    def get_or_create_batch_run(
        self,
        revision_id: str,
        requested_date: date,
        runner_policy_version: str = RUNNER_POLICY_VERSION,
    ) -> DailyBatchRun:
        """Idempotent: returns existing batch if same (revision, date, policy)."""
        batch_run_id = str(uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO daily_batch_runs "
                "(batch_run_id, watchlist_revision_id, requested_date, "
                "runner_policy_version, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'pending', ?, ?)",
                (
                    batch_run_id,
                    revision_id,
                    requested_date.isoformat(),
                    runner_policy_version,
                    now_iso,
                    now_iso,
                ),
            )
            row = conn.execute(
                "SELECT * FROM daily_batch_runs "
                "WHERE watchlist_revision_id = ? AND requested_date = ? "
                "AND runner_policy_version = ?",
                (revision_id, requested_date.isoformat(), runner_policy_version),
            ).fetchone()
        return self._batch_run_from_row(row)

    def start_batch_run(
        self,
        batch_run_id: str,
        *,
        started_at: datetime,
        total_symbols: int,
        resume_running: bool = False,
    ) -> DailyBatchRun:
        timestamp = started_at.isoformat()
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM daily_batch_runs WHERE batch_run_id = ?",
                (batch_run_id,),
            ).fetchone()
            if row is None:
                raise BatchRunStateError(f"unknown batch run {batch_run_id}")
            current = self._batch_run_from_row(row)
            if current.status in (
                BatchRunStatus.SUCCESS,
                BatchRunStatus.SKIPPED_NON_TRADING_DAY,
                BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE,
            ):
                raise BatchRunStateError(
                    f"batch run {batch_run_id} already completed with {current.status}"
                )
            if current.status == BatchRunStatus.RUNNING and not resume_running:
                raise BatchRunStateError(
                    f"batch run {batch_run_id} is already running; "
                    "provide batch_run_id to resume"
                )
            conn.execute(
                "UPDATE daily_batch_runs SET status = 'running', "
                "started_at = COALESCE(started_at, ?), total_symbols = ?, "
                "success_symbols = 0, failed_symbols = 0, skipped_symbols = 0, "
                "error_message = NULL, finished_at = NULL, updated_at = ? "
                "WHERE batch_run_id = ?",
                (timestamp, total_symbols, timestamp, batch_run_id),
            )
            updated = conn.execute(
                "SELECT * FROM daily_batch_runs WHERE batch_run_id = ?",
                (batch_run_id,),
            ).fetchone()
        return self._batch_run_from_row(updated)

    def finish_batch_run(
        self,
        batch_run_id: str,
        *,
        finished_at: datetime,
        resolved_market_date: date | None = None,
    ) -> DailyBatchRun:
        """Compute final status from symbol run outcomes and mark finished."""
        timestamp = finished_at.isoformat()
        market_date_iso = (
            None if resolved_market_date is None else resolved_market_date.isoformat()
        )
        with self._transaction() as conn:
            counts = conn.execute(
                "SELECT "
                "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS ok, "
                "SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS fail, "
                "SUM(CASE WHEN status LIKE 'skipped%' THEN 1 ELSE 0 END) AS skip, "
                "COUNT(*) AS total "
                "FROM daily_symbol_runs WHERE batch_run_id = ?",
                (batch_run_id,),
            ).fetchone()
            ok = counts["ok"] or 0
            fail = counts["fail"] or 0
            skip = counts["skip"] or 0
            total = counts["total"] or 0

            if fail == 0 and ok + skip == total and total > 0:
                final_status = BatchRunStatus.SUCCESS.value
            elif ok > 0 and fail > 0:
                final_status = BatchRunStatus.PARTIAL_SUCCESS.value
            elif fail > 0:
                final_status = BatchRunStatus.FAILED.value
            else:
                final_status = BatchRunStatus.SUCCESS.value

            conn.execute(
                "UPDATE daily_batch_runs SET status = ?, finished_at = ?, "
                "resolved_market_date = ?, "
                "success_symbols = ?, failed_symbols = ?, skipped_symbols = ?, "
                "updated_at = ? "
                "WHERE batch_run_id = ? AND status = 'running'",
                (
                    final_status,
                    timestamp,
                    market_date_iso,
                    ok,
                    fail,
                    skip,
                    timestamp,
                    batch_run_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM daily_batch_runs WHERE batch_run_id = ?",
                (batch_run_id,),
            ).fetchone()
        return self._batch_run_from_row(row)

    def mark_batch_skipped(
        self,
        batch_run_id: str,
        reason: BatchRunStatus,
        *,
        finished_at: datetime,
    ) -> DailyBatchRun:
        if reason not in (
            BatchRunStatus.SKIPPED_NON_TRADING_DAY,
            BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE,
            BatchRunStatus.DEFERRED_AWAITING_MARKET_DATA,
        ):
            raise BatchRunStateError(
                f"mark_batch_skipped requires a skip/defer status, got {reason}"
            )
        timestamp = finished_at.isoformat()
        with self._transaction() as conn:
            conn.execute(
                "UPDATE daily_batch_runs SET status = ?, finished_at = ?, "
                "updated_at = ? WHERE batch_run_id = ? AND status IN ('pending', 'running')",
                (reason.value, timestamp, timestamp, batch_run_id),
            )
            row = conn.execute(
                "SELECT * FROM daily_batch_runs WHERE batch_run_id = ?",
                (batch_run_id,),
            ).fetchone()
        return self._batch_run_from_row(row)

    def find_existing_batch(
        self,
        watchlist_id: str,
        requested_date: date,
        runner_policy_version: str = RUNNER_POLICY_VERSION,
    ) -> DailyBatchRun | None:
        """Find a batch run for this watchlist + date + policy, if any."""
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT dbr.* FROM daily_batch_runs dbr "
                "JOIN watchlist_revisions wr ON dbr.watchlist_revision_id = wr.revision_id "
                "WHERE wr.watchlist_id = ? AND dbr.requested_date = ? "
                "AND dbr.runner_policy_version = ? "
                "ORDER BY dbr.created_at DESC LIMIT 1",
                (watchlist_id, requested_date.isoformat(), runner_policy_version),
            ).fetchone()
        return None if row is None else self._batch_run_from_row(row)
    def get_batch_run(self, batch_run_id: str) -> DailyBatchRun | None:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM daily_batch_runs WHERE batch_run_id = ?",
                (batch_run_id,),
            ).fetchone()
        return None if row is None else self._batch_run_from_row(row)

    def list_batch_runs(
        self, requested_date: date | None = None, limit: int = 50
    ) -> list[DailyBatchRun]:
        with self._transaction() as conn:
            if requested_date is not None:
                rows = conn.execute(
                    "SELECT * FROM daily_batch_runs WHERE requested_date = ? "
                    "ORDER BY created_at DESC LIMIT ?",
                    (requested_date.isoformat(), limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM daily_batch_runs "
                    "ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [self._batch_run_from_row(row) for row in rows]

    # ------------------------------------------------------------------
    # Per-symbol runs
    # ------------------------------------------------------------------

    def get_or_create_symbol_run(
        self, batch_run_id: str, symbol: str
    ) -> DailySymbolRun:
        normalized = symbol.strip().upper()
        symbol_run_id = str(uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO daily_symbol_runs "
                "(symbol_run_id, batch_run_id, symbol, status, created_at, updated_at) "
                "VALUES (?, ?, ?, 'pending', ?, ?)",
                (symbol_run_id, batch_run_id, normalized, now_iso, now_iso),
            )
            row = conn.execute(
                "SELECT * FROM daily_symbol_runs "
                "WHERE batch_run_id = ? AND symbol = ?",
                (batch_run_id, normalized),
            ).fetchone()
        return self._symbol_run_from_row(row)

    def start_symbol_run(
        self, symbol_run_id: str, *, started_at: datetime
    ) -> DailySymbolRun:
        timestamp = started_at.isoformat()
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM daily_symbol_runs WHERE symbol_run_id = ?",
                (symbol_run_id,),
            ).fetchone()
            if row is None:
                raise BatchRunStateError(f"unknown symbol run {symbol_run_id}")
            current = self._symbol_run_from_row(row)
            if current.status == SymbolRunStatus.SUCCESS:
                raise BatchRunStateError(
                    f"symbol run {symbol_run_id} already succeeded"
                )
            conn.execute(
                "UPDATE daily_symbol_runs SET status = 'running', "
                "started_at = COALESCE(started_at, ?), "
                "attempt_count = attempt_count + 1, "
                "error_message = NULL, finished_at = NULL, updated_at = ? "
                "WHERE symbol_run_id = ?",
                (timestamp, timestamp, symbol_run_id),
            )
            updated = conn.execute(
                "SELECT * FROM daily_symbol_runs WHERE symbol_run_id = ?",
                (symbol_run_id,),
            ).fetchone()
        return self._symbol_run_from_row(updated)

    def mark_symbol_success(
        self,
        symbol_run_id: str,
        *,
        finished_at: datetime,
        pipeline_run_id: str | None = None,
        historical_run_id: str | None = None,
        validation_run_id: str | None = None,
        hist_validation_run_id: str | None = None,
    ) -> DailySymbolRun:
        timestamp = finished_at.isoformat()
        with self._transaction() as conn:
            conn.execute(
                "UPDATE daily_symbol_runs SET status = 'success', "
                "finished_at = ?, updated_at = ?, "
                "pipeline_run_id = COALESCE(?, pipeline_run_id), "
                "historical_run_id = COALESCE(?, historical_run_id), "
                "validation_run_id = COALESCE(?, validation_run_id), "
                "hist_validation_run_id = COALESCE(?, hist_validation_run_id), "
                "error_message = NULL "
                "WHERE symbol_run_id = ? AND status = 'running'",
                (
                    timestamp, timestamp,
                    pipeline_run_id, historical_run_id,
                    validation_run_id, hist_validation_run_id,
                    symbol_run_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM daily_symbol_runs WHERE symbol_run_id = ?",
                (symbol_run_id,),
            ).fetchone()
        if row is None or self._symbol_run_from_row(row).status != SymbolRunStatus.SUCCESS:
            raise BatchRunStateError(
                f"symbol run {symbol_run_id} could not transition to success"
            )
        return self._symbol_run_from_row(row)

    def mark_symbol_failed(
        self,
        symbol_run_id: str,
        *,
        finished_at: datetime,
        error_message: str,
    ) -> DailySymbolRun:
        safe_error = error_message.strip()[:1000] or "UnknownError"
        timestamp = finished_at.isoformat()
        with self._transaction() as conn:
            conn.execute(
                "UPDATE daily_symbol_runs SET status = 'failed', "
                "finished_at = ?, error_message = ?, updated_at = ? "
                "WHERE symbol_run_id = ? AND status = 'running'",
                (timestamp, safe_error, timestamp, symbol_run_id),
            )
            row = conn.execute(
                "SELECT * FROM daily_symbol_runs WHERE symbol_run_id = ?",
                (symbol_run_id,),
            ).fetchone()
        return self._symbol_run_from_row(row)

    def mark_symbol_skipped(
        self,
        symbol_run_id: str,
        reason: SymbolRunStatus,
        *,
        finished_at: datetime,
    ) -> DailySymbolRun:
        if reason not in (
            SymbolRunStatus.SKIPPED_ALREADY_SUCCEEDED,
            SymbolRunStatus.SKIPPED_NON_TRADING_DAY,
            SymbolRunStatus.SKIPPED_NO_NEW_MARKET_DATE,
        ):
            raise BatchRunStateError(f"invalid skip reason {reason}")
        timestamp = finished_at.isoformat()
        with self._transaction() as conn:
            conn.execute(
                "UPDATE daily_symbol_runs SET status = ?, finished_at = ?, updated_at = ? "
                "WHERE symbol_run_id = ? AND status IN ('pending', 'running')",
                (reason.value, timestamp, timestamp, symbol_run_id),
            )
            row = conn.execute(
                "SELECT * FROM daily_symbol_runs WHERE symbol_run_id = ?",
                (symbol_run_id,),
            ).fetchone()
        return self._symbol_run_from_row(row)

    def list_symbol_runs(self, batch_run_id: str) -> list[DailySymbolRun]:
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM daily_symbol_runs WHERE batch_run_id = ? "
                "ORDER BY symbol",
                (batch_run_id,),
            ).fetchall()
        return [self._symbol_run_from_row(row) for row in rows]

    def get_symbol_run(
        self, batch_run_id: str, symbol: str
    ) -> DailySymbolRun | None:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM daily_symbol_runs WHERE batch_run_id = ? AND symbol = ?",
                (batch_run_id, symbol.strip().upper()),
            ).fetchone()
        return None if row is None else self._symbol_run_from_row(row)

    # ------------------------------------------------------------------
    # Row converters
    # ------------------------------------------------------------------

    @staticmethod
    def _batch_run_from_row(row: sqlite3.Row) -> DailyBatchRun:
        return DailyBatchRun(
            batch_run_id=row["batch_run_id"],
            watchlist_revision_id=row["watchlist_revision_id"],
            requested_date=date.fromisoformat(row["requested_date"]),
            resolved_market_date=(
                None
                if row["resolved_market_date"] is None
                else date.fromisoformat(row["resolved_market_date"])
            ),
            runner_policy_version=row["runner_policy_version"],
            status=BatchRunStatus(row["status"]),
            total_symbols=row["total_symbols"],
            success_symbols=row["success_symbols"],
            failed_symbols=row["failed_symbols"],
            skipped_symbols=row["skipped_symbols"],
            error_message=row["error_message"],
            created_at=datetime.fromisoformat(row["created_at"]),
            started_at=(
                None
                if row["started_at"] is None
                else datetime.fromisoformat(row["started_at"])
            ),
            finished_at=(
                None
                if row["finished_at"] is None
                else datetime.fromisoformat(row["finished_at"])
            ),
        )

    @staticmethod
    def _symbol_run_from_row(row: sqlite3.Row) -> DailySymbolRun:
        return DailySymbolRun(
            symbol_run_id=row["symbol_run_id"],
            batch_run_id=row["batch_run_id"],
            symbol=row["symbol"],
            status=SymbolRunStatus(row["status"]),
            attempt_count=row["attempt_count"],
            pipeline_run_id=row["pipeline_run_id"],
            historical_run_id=row["historical_run_id"],
            validation_run_id=row["validation_run_id"],
            hist_validation_run_id=row["hist_validation_run_id"],
            error_message=row["error_message"],
            created_at=datetime.fromisoformat(row["created_at"]),
            started_at=(
                None
                if row["started_at"] is None
                else datetime.fromisoformat(row["started_at"])
            ),
            finished_at=(
                None
                if row["finished_at"] is None
                else datetime.fromisoformat(row["finished_at"])
            ),
        )
