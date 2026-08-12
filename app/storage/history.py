"""SQLite checkpoints and atomic writes for historical synchronization."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from typing import Iterator, Sequence
from uuid import uuid4

from app.models import (
    CompanyMetric,
    DailyPrice,
    HistoricalSourceObservation,
    HistoricalSyncRun,
    NormalizedMarketData,
    PipelineRunStatus,
    ResearchNote,
    SourceArtifact,
)

from .sqlite import SQLiteResearchRepository, StorageWriteResult


class HistoricalSyncError(RuntimeError):
    """Base class for historical synchronization state failures."""


class HistoricalSyncConflictError(HistoricalSyncError):
    """An existing checkpoint conflicts with the requested sync contract."""


class HistoricalSyncInProgressError(HistoricalSyncError):
    """A running sync requires its exact run id for recovery."""


class HistoricalSyncStateError(HistoricalSyncError):
    """A historical sync attempted an invalid state transition."""


class SQLiteHistoricalSyncRepository:
    """Durable range-sync state layered on the canonical research repository."""

    def __init__(self, research_repository: SQLiteResearchRepository) -> None:
        self.research_repository = research_repository

    def get_or_create(
        self,
        *,
        symbol: str,
        target_date: date,
        target_observations: int,
        provider: str,
        created_at: datetime,
    ) -> HistoricalSyncRun:
        normalized_symbol = symbol.strip().upper()
        normalized_provider = provider.strip()
        if not 60 <= target_observations <= 250:
            raise ValueError("target_observations must be between 60 and 250")
        if not normalized_provider:
            raise ValueError("provider must not be empty")
        run_id = str(uuid4())
        timestamp = created_at.isoformat()
        next_month = target_date.replace(day=1)

        with self.research_repository._transaction() as connection:
            symbol_row = connection.execute(
                "SELECT market FROM symbols WHERE symbol = ?", (normalized_symbol,)
            ).fetchone()
            if symbol_row is None or symbol_row["market"] != "TWSE":
                raise HistoricalSyncStateError(
                    "historical sync requires a Phase 3 TWSE-listed symbol record"
                )
            connection.execute(
                "INSERT OR IGNORE INTO historical_sync_runs ("
                "run_id, symbol, target_date, target_observations, provider, "
                "status, next_month, observation_count, first_trade_date, "
                "last_trade_date, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    normalized_symbol,
                    target_date.isoformat(),
                    target_observations,
                    normalized_provider,
                    PipelineRunStatus.PENDING.value,
                    next_month.isoformat(),
                    0,
                    None,
                    None,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                self._select()
                + " WHERE symbol = ? AND target_date = ? "
                "AND target_observations = ? AND provider = ?",
                (
                    normalized_symbol,
                    target_date.isoformat(),
                    target_observations,
                    normalized_provider,
                ),
            ).fetchone()
        if row is None:  # pragma: no cover - INSERT/UNIQUE contract.
            raise HistoricalSyncStateError("historical sync run could not be created")
        return self._from_row(row)

    def get(self, run_id: str) -> HistoricalSyncRun | None:
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else self._from_row(row)

    def list(self, symbol: str | None = None) -> list[HistoricalSyncRun]:
        query = self._select()
        parameters: tuple[object, ...] = ()
        if symbol is not None:
            query += " WHERE symbol = ?"
            parameters = (symbol.strip().upper(),)
        query += " ORDER BY target_date, target_observations"
        with self.research_repository._transaction() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._from_row(row) for row in rows]

    def list_recent_prices(
        self, symbol: str, *, end_date: date, limit: int
    ) -> list[DailyPrice]:
        with self.research_repository._transaction() as connection:
            unit = SQLiteHistoricalCompletionUnit(self, connection, "read-only")
            return unit.list_recent_prices(
                symbol, end_date=end_date, limit=limit
            )

    def list_run_prices(
        self,
        run_id: str,
        *,
        end_date: date,
        limit: int,
        allow_legacy_fallback: bool = True,
    ) -> list[DailyPrice]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        with self.research_repository._transaction() as connection:
            run_row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
            if run_row is None:
                raise HistoricalSyncStateError(f"unknown historical run {run_id}")
            run = self._from_row(run_row)
            prices = self._list_source_prices(
                connection, run_id, end_date=end_date, limit=limit
            )
            if prices or not allow_legacy_fallback:
                return prices
            if (
                run.status is PipelineRunStatus.SUCCESS
                and run.provider == "twse-historical"
            ):
                unit = SQLiteHistoricalCompletionUnit(self, connection, "legacy")
                return unit.list_recent_prices(
                    run.symbol, end_date=end_date, limit=limit
                )
        return []

    def list_source_observations(
        self, run_id: str, *, end_date: date, limit: int
    ) -> list[HistoricalSourceObservation]:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        with self.research_repository._transaction() as connection:
            rows = connection.execute(
                "SELECT id, historical_run_id, provider, symbol, trade_date, "
                "open_price, high_price, low_price, close_price, volume, "
                "source_endpoints_json, fetched_at, source_timestamp_raw, "
                "source_timestamp FROM historical_source_observations "
                "WHERE historical_run_id = ? AND trade_date <= ? "
                "ORDER BY trade_date DESC LIMIT ?",
                (run_id, end_date.isoformat(), limit),
            ).fetchall()
        return [self._source_observation_from_row(row) for row in reversed(rows)]

    def list_metrics(
        self, symbol: str, *, end_date: date
    ) -> list[CompanyMetric]:
        with self.research_repository._transaction() as connection:
            return self.research_repository._list_company_metrics(
                connection, symbol, end_date=end_date
            )

    def start(
        self,
        run_id: str,
        *,
        started_at: datetime,
        resume_running: bool = False,
    ) -> HistoricalSyncRun:
        timestamp = started_at.isoformat()
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise HistoricalSyncStateError(f"unknown historical run {run_id}")
            current = self._from_row(row)
            if current.status is PipelineRunStatus.SUCCESS:
                raise HistoricalSyncStateError("successful historical run cannot restart")
            if current.status is PipelineRunStatus.RUNNING and not resume_running:
                raise HistoricalSyncInProgressError(
                    f"historical run {run_id} is already running"
                )
            cursor = connection.execute(
                "UPDATE historical_sync_runs SET status = ?, "
                "started_at = COALESCE(started_at, ?), finished_at = NULL, "
                "error_message = NULL, research_note_id = NULL, "
                "attempt_count = attempt_count + 1, updated_at = ? "
                "WHERE run_id = ? AND status = ?",
                (
                    PipelineRunStatus.RUNNING.value,
                    timestamp,
                    timestamp,
                    run_id,
                    current.status.value,
                ),
            )
            if cursor.rowcount != 1:
                raise HistoricalSyncInProgressError(
                    f"historical run {run_id} changed state while being claimed"
                )
            count, first_date, last_date = self._coverage(
                connection, current.run_id, current.target_date
            )
            if count == 0 and current.months_completed > 0:
                # A completed official month may contain only all-OHLC-missing
                # rows.  Such a checkpoint has no price observations by design,
                # but it is resumable when every advanced month retains its
                # hash-only official source artifact.  Legacy cursor-only state
                # without that evidence remains rejected.
                artifact_row = connection.execute(
                    "SELECT COUNT(DISTINCT checkpoint_key) AS count "
                    "FROM source_artifacts WHERE historical_run_id = ? "
                    "AND checkpoint_key LIKE 'historical-month:%'",
                    (current.run_id,),
                ).fetchone()
                artifact_count = int(artifact_row["count"])
                if artifact_count < current.months_completed:
                    raise HistoricalSyncStateError(
                        "legacy interrupted historical run has no source observations"
                    )
            connection.execute(
                "UPDATE historical_sync_runs SET observation_count = ?, "
                "first_trade_date = ?, last_trade_date = ? WHERE run_id = ?",
                (
                    count,
                    None if first_date is None else first_date.isoformat(),
                    None if last_date is None else last_date.isoformat(),
                    run_id,
                ),
            )
            updated = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._from_row(updated)

    def checkpoint_month(
        self,
        run_id: str,
        *,
        completed_month: date,
        next_month: date,
        data: NormalizedMarketData,
        source_endpoints: Sequence[str],
        fetched_at: datetime,
        updated_at: datetime,
        write_canonical_prices: bool,
        source_artifacts: Sequence[SourceArtifact] = (),
    ) -> tuple[HistoricalSyncRun, StorageWriteResult]:
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise HistoricalSyncStateError(f"unknown historical run {run_id}")
            current = self._from_row(row)
            if current.status is not PipelineRunStatus.RUNNING:
                raise HistoricalSyncStateError(
                    "historical month checkpoint requires a running run"
                )
            if current.next_month != completed_month:
                raise HistoricalSyncConflictError(
                    "historical month does not match the durable cursor"
                )
            if data.symbol.symbol != current.symbol:
                raise HistoricalSyncConflictError(
                    "historical checkpoint symbol does not match run"
                )
            if data.source != current.provider:
                raise HistoricalSyncConflictError(
                    "historical checkpoint provider does not match run"
                )
            endpoints = tuple(endpoint.strip() for endpoint in source_endpoints)
            if not endpoints or any(not endpoint for endpoint in endpoints):
                raise HistoricalSyncStateError(
                    "historical checkpoint requires source endpoints"
                )
            if len(set(endpoints)) != len(endpoints):
                raise HistoricalSyncStateError(
                    "historical checkpoint source endpoints must be unique"
                )
            if fetched_at.utcoffset() is None or updated_at.utcoffset() is None:
                raise HistoricalSyncStateError(
                    "historical checkpoint timestamps must be timezone-aware"
                )

            self._insert_source_observations(
                connection,
                current,
                data,
                source_endpoints=endpoints,
                fetched_at=fetched_at,
            )
            self.research_repository._insert_source_artifacts(
                connection,
                source_artifacts,
                provider=current.provider,
                checkpoint_key=f"historical-month:{completed_month.isoformat()}",
                created_at=updated_at,
                historical_run_id=current.run_id,
            )
            write_result = (
                self.research_repository._save_market_data(connection, data)
                if write_canonical_prices
                else StorageWriteResult(
                    symbols=0,
                    daily_prices=0,
                    company_metrics=0,
                )
            )
            count, first_date, last_date = self._coverage(
                connection, current.run_id, current.target_date
            )
            cursor = connection.execute(
                "UPDATE historical_sync_runs SET next_month = ?, "
                "months_completed = months_completed + 1, observation_count = ?, "
                "source_endpoint = ?, fetched_at = ?, first_trade_date = ?, "
                "last_trade_date = ?, updated_at = ? "
                "WHERE run_id = ? AND status = ? AND next_month = ?",
                (
                    next_month.isoformat(),
                    count,
                    endpoints[-1],
                    fetched_at.isoformat(),
                    None if first_date is None else first_date.isoformat(),
                    None if last_date is None else last_date.isoformat(),
                    updated_at.isoformat(),
                    run_id,
                    PipelineRunStatus.RUNNING.value,
                    completed_month.isoformat(),
                ),
            )
            if cursor.rowcount != 1:
                raise HistoricalSyncStateError(
                    "historical cursor could not advance atomically"
                )
            updated = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._from_row(updated), write_result

    def mark_failed(
        self,
        run_id: str,
        *,
        finished_at: datetime,
        error_message: str,
    ) -> HistoricalSyncRun:
        timestamp = finished_at.isoformat()
        safe_error = error_message.strip()[:1000] or "UnknownError"
        with self.research_repository._transaction() as connection:
            cursor = connection.execute(
                "UPDATE historical_sync_runs SET status = ?, finished_at = ?, "
                "error_message = ?, research_note_id = NULL, updated_at = ? "
                "WHERE run_id = ? AND status = ?",
                (
                    PipelineRunStatus.FAILED.value,
                    timestamp,
                    safe_error,
                    timestamp,
                    run_id,
                    PipelineRunStatus.RUNNING.value,
                ),
            )
            if cursor.rowcount != 1:
                raise HistoricalSyncStateError(
                    "historical run was not running when failure was recorded"
                )
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._from_row(row)

    @contextmanager
    def successful_run(
        self, run_id: str
    ) -> Iterator["SQLiteHistoricalCompletionUnit"]:
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                "SELECT status FROM historical_sync_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None or row["status"] != PipelineRunStatus.RUNNING.value:
                raise HistoricalSyncStateError(
                    "historical completion requires a running run"
                )
            unit = SQLiteHistoricalCompletionUnit(self, connection, run_id)
            yield unit
            final_row = connection.execute(
                "SELECT status FROM historical_sync_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if final_row["status"] != PipelineRunStatus.SUCCESS.value:
                raise HistoricalSyncStateError(
                    "historical completion transaction ended without success"
                )

    @staticmethod
    def _insert_source_observations(
        connection: sqlite3.Connection,
        run: HistoricalSyncRun,
        data: NormalizedMarketData,
        *,
        source_endpoints: Sequence[str],
        fetched_at: datetime,
    ) -> None:
        endpoints_json = json.dumps(
            list(source_endpoints), ensure_ascii=False, separators=(",", ":")
        )
        connection.executemany(
            "INSERT INTO historical_source_observations ("
            "historical_run_id, provider, symbol, trade_date, open_price, "
            "high_price, low_price, close_price, volume, source_endpoints_json, "
            "fetched_at, source_timestamp_raw, source_timestamp"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    run.run_id,
                    run.provider,
                    price.symbol,
                    price.trade_date.isoformat(),
                    price.open,
                    price.high,
                    price.low,
                    price.close,
                    price.volume,
                    endpoints_json,
                    fetched_at.isoformat(),
                    data.source_timestamp_raw,
                    (
                        None
                        if data.source_timestamp is None
                        else data.source_timestamp.isoformat()
                    ),
                )
                for price in data.daily_prices
            ],
        )

    @staticmethod
    def _list_source_prices(
        connection: sqlite3.Connection,
        run_id: str,
        *,
        end_date: date,
        limit: int,
    ) -> list[DailyPrice]:
        rows = connection.execute(
            "SELECT symbol, trade_date, open_price, high_price, low_price, "
            "close_price, volume, provider FROM historical_source_observations "
            "WHERE historical_run_id = ? AND trade_date <= ? "
            "ORDER BY trade_date DESC LIMIT ?",
            (run_id, end_date.isoformat(), limit),
        ).fetchall()
        return list(
            reversed(
                [
                    DailyPrice(
                        symbol=row["symbol"],
                        trade_date=date.fromisoformat(row["trade_date"]),
                        open=row["open_price"],
                        high=row["high_price"],
                        low=row["low_price"],
                        close=row["close_price"],
                        volume=row["volume"],
                        source=row["provider"],
                    )
                    for row in rows
                ]
            )
        )

    @staticmethod
    def _source_observation_from_row(
        row: sqlite3.Row,
    ) -> HistoricalSourceObservation:
        try:
            endpoints = json.loads(row["source_endpoints_json"])
        except json.JSONDecodeError as error:
            raise HistoricalSyncStateError(
                "historical source endpoints are invalid JSON"
            ) from error
        if not isinstance(endpoints, list) or not endpoints or any(
            not isinstance(item, str) or not item for item in endpoints
        ):
            raise HistoricalSyncStateError(
                "historical source endpoints must be a non-empty string array"
            )
        return HistoricalSourceObservation(
            id=row["id"],
            historical_run_id=row["historical_run_id"],
            provider=row["provider"],
            symbol=row["symbol"],
            trade_date=date.fromisoformat(row["trade_date"]),
            open=row["open_price"],
            high=row["high_price"],
            low=row["low_price"],
            close=row["close_price"],
            volume=row["volume"],
            source_endpoints=tuple(endpoints),
            fetched_at=datetime.fromisoformat(row["fetched_at"]),
            source_timestamp_raw=row["source_timestamp_raw"],
            source_timestamp=(
                None
                if row["source_timestamp"] is None
                else datetime.fromisoformat(row["source_timestamp"])
            ),
        )

    @staticmethod
    def _coverage(
        connection: sqlite3.Connection, run_id: str, target_date: date
    ) -> tuple[int, date | None, date | None]:
        row = connection.execute(
            "SELECT COUNT(*) AS count, MIN(trade_date) AS first_date, "
            "MAX(trade_date) AS last_date FROM historical_source_observations "
            "WHERE historical_run_id = ? AND trade_date <= ?",
            (run_id, target_date.isoformat()),
        ).fetchone()
        return (
            int(row["count"]),
            None if row["first_date"] is None else date.fromisoformat(row["first_date"]),
            None if row["last_date"] is None else date.fromisoformat(row["last_date"]),
        )

    @staticmethod
    def _select() -> str:
        return (
            "SELECT run_id, symbol, target_date, target_observations, provider, "
            "status, next_month, months_completed, observation_count, created_at, "
            "started_at, finished_at, error_message, attempt_count, "
            "research_note_id, "
            "source_endpoint, fetched_at, first_trade_date, last_trade_date "
            "FROM historical_sync_runs"
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> HistoricalSyncRun:
        return HistoricalSyncRun(
            run_id=row["run_id"],
            symbol=row["symbol"],
            target_date=date.fromisoformat(row["target_date"]),
            target_observations=row["target_observations"],
            provider=row["provider"],
            status=PipelineRunStatus(row["status"]),
            next_month=date.fromisoformat(row["next_month"]),
            months_completed=row["months_completed"],
            observation_count=row["observation_count"],
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
            error_message=row["error_message"],
            attempt_count=row["attempt_count"],
            research_note_id=row["research_note_id"],
            source_endpoint=row["source_endpoint"],
            fetched_at=(
                None
                if row["fetched_at"] is None
                else datetime.fromisoformat(row["fetched_at"])
            ),
            first_trade_date=(
                None
                if row["first_trade_date"] is None
                else date.fromisoformat(row["first_trade_date"])
            ),
            last_trade_date=(
                None
                if row["last_trade_date"] is None
                else date.fromisoformat(row["last_trade_date"])
            ),
        )


class SQLiteHistoricalCompletionUnit:
    """Finalize a historical note and successful run in one transaction."""

    def __init__(
        self,
        repository: SQLiteHistoricalSyncRepository,
        connection: sqlite3.Connection,
        run_id: str,
    ) -> None:
        self.repository = repository
        self.connection = connection
        self.run_id = run_id

    def list_recent_prices(
        self, symbol: str, *, end_date: date, limit: int
    ) -> list[DailyPrice]:
        if self.run_id not in {"read-only", "legacy"}:
            return self.repository._list_source_prices(
                self.connection,
                self.run_id,
                end_date=end_date,
                limit=limit,
            )
        rows = self.connection.execute(
            "SELECT symbol, trade_date, open_price, high_price, low_price, "
            "close_price, volume, source FROM daily_prices "
            "WHERE symbol = ? AND trade_date <= ? "
            "ORDER BY trade_date DESC LIMIT ?",
            (symbol, end_date.isoformat(), limit),
        ).fetchall()
        prices = [
            DailyPrice(
                symbol=row["symbol"],
                trade_date=date.fromisoformat(row["trade_date"]),
                open=row["open_price"],
                high=row["high_price"],
                low=row["low_price"],
                close=row["close_price"],
                volume=row["volume"],
                source=row["source"],
            )
            for row in rows
        ]
        return list(reversed(prices))

    def list_metrics(self, symbol: str, *, end_date: date) -> list[CompanyMetric]:
        return self.repository.research_repository._list_company_metrics(
            self.connection, symbol, end_date=end_date
        )

    def create_research_note(self, note: ResearchNote) -> ResearchNote:
        if (
            note.historical_run_id != self.run_id
            or note.run_id is not None
            or note.historical_validation_run_id is not None
        ):
            raise HistoricalSyncStateError(
                "historical note must link only to its active historical run"
            )
        return self.repository.research_repository._insert_research_note(
            self.connection, note
        )

    def mark_success(
        self,
        research_note_id: int,
        *,
        observation_count: int,
        first_trade_date: date,
        last_trade_date: date,
        finished_at: datetime,
    ) -> None:
        note = self.connection.execute(
            "SELECT historical_run_id FROM research_notes WHERE id = ?",
            (research_note_id,),
        ).fetchone()
        if note is None or note["historical_run_id"] != self.run_id:
            raise HistoricalSyncStateError(
                "successful historical run must reference its own note"
            )
        timestamp = finished_at.isoformat()
        cursor = self.connection.execute(
            "UPDATE historical_sync_runs SET status = ?, finished_at = ?, "
            "error_message = NULL, research_note_id = ?, observation_count = ?, "
            "first_trade_date = ?, last_trade_date = ?, updated_at = ? "
            "WHERE run_id = ? AND status = ?",
            (
                PipelineRunStatus.SUCCESS.value,
                timestamp,
                research_note_id,
                observation_count,
                first_trade_date.isoformat(),
                last_trade_date.isoformat(),
                timestamp,
                self.run_id,
                PipelineRunStatus.RUNNING.value,
            ),
        )
        if cursor.rowcount != 1:
            raise HistoricalSyncStateError(
                "historical run could not transition to success"
            )
