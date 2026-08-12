"""SQLite repository for canonical research data and resumable pipeline runs."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, Sequence
from uuid import uuid4

from app.models import (
    CompanyMetric,
    DailyPrice,
    NormalizedMarketData,
    PipelineRun,
    PipelineRunStatus,
    ResearchNote,
    SourceArtifact,
    Symbol,
)


class PipelineRunError(RuntimeError):
    """Base class for durable pipeline-run state errors."""


class PipelineRunConflictError(PipelineRunError):
    """An existing symbol/date checkpoint has a different immutable contract."""


class PipelineRunInProgressError(PipelineRunError):
    """A running checkpoint requires its exact run id for explicit recovery."""


class PipelineRunStateError(PipelineRunError):
    """A pipeline run attempted an invalid state transition."""


@dataclass(frozen=True, slots=True)
class StorageWriteResult:
    symbols: int
    daily_prices: int
    company_metrics: int


class SQLiteResearchRepository:
    """Repository with explicit CRUD, migrations, and atomic run completion."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        if str(database_path) != ":memory:":
            self.database_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path, timeout=5.0)
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
        schema_path = Path(__file__).with_name("schema.sql")
        with self._transaction() as connection:
            connection.executescript(schema_path.read_text(encoding="utf-8"))
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version INTEGER PRIMARY KEY, name TEXT NOT NULL, "
                "applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            )
            connection.execute(
                "INSERT OR IGNORE INTO schema_migrations (version, name) "
                "VALUES (1, 'initial research schema')"
            )
        self._apply_pipeline_reliability_migration()
        self._apply_run_provenance_migration()
        self._apply_historical_sync_migration()
        self._apply_market_data_cross_validation_migration()
        self._apply_historical_cross_validation_migration()
        self._apply_source_artifacts_migration()

    def _apply_pipeline_reliability_migration(self) -> None:
        migration_path = Path(__file__).with_name("migrations") / "0002_pipeline_runs.sql"
        with self._transaction() as connection:
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = 2"
            ).fetchone()
            if applied is not None:
                columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(research_notes)"
                    ).fetchall()
                }
                if "run_id" not in columns:
                    raise PipelineRunStateError(
                        "schema migration 2 is recorded but research_notes.run_id is missing"
                    )
                return

            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(research_notes)"
                ).fetchall()
            }
            if "run_id" not in columns:
                connection.execute(
                    "ALTER TABLE research_notes ADD COLUMN run_id TEXT "
                    "REFERENCES pipeline_runs(run_id)"
                )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_research_notes_run_id "
                "ON research_notes(run_id) WHERE run_id IS NOT NULL"
            )
            connection.execute(
                "INSERT INTO schema_migrations (version, name) "
                "VALUES (2, 'pipeline runs and research-note linkage')"
            )

    def _apply_run_provenance_migration(self) -> None:
        migration_path = Path(__file__).with_name("migrations") / "0003_run_provenance.sql"
        with self._transaction() as connection:
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = 3"
            ).fetchone()
            required_columns = {
                "source_endpoints_json": "TEXT",
                "fetched_at": "TEXT",
                "market_date": "TEXT",
            }
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(pipeline_runs)")
            }
            if applied is not None:
                missing = required_columns.keys() - columns
                if missing:
                    raise PipelineRunStateError(
                        "schema migration 3 is recorded but columns are missing: "
                        + ", ".join(sorted(missing))
                    )
                return

            for column, data_type in required_columns.items():
                if column not in columns:
                    connection.execute(
                        f"ALTER TABLE pipeline_runs ADD COLUMN {column} {data_type}"
                    )
            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            connection.execute(
                "INSERT INTO schema_migrations (version, name) "
                "VALUES (3, 'pipeline run source provenance')"
            )

    def _apply_historical_sync_migration(self) -> None:
        migration_path = (
            Path(__file__).with_name("migrations") / "0004_historical_sync.sql"
        )
        with self._transaction() as connection:
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = 4"
            ).fetchone()
            note_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(research_notes)")
            }
            history_table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                "AND name = 'historical_sync_runs'"
            ).fetchone()
            history_columns = (
                {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(historical_sync_runs)"
                    )
                }
                if history_table is not None
                else set()
            )
            if applied is not None:
                if (
                    "historical_run_id" not in note_columns
                    or history_table is None
                    or "attempt_count" not in history_columns
                ):
                    raise PipelineRunStateError(
                        "schema migration 4 is recorded but historical sync schema "
                        "is incomplete"
                    )
                return

            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            if "historical_run_id" not in note_columns:
                connection.execute(
                    "ALTER TABLE research_notes ADD COLUMN historical_run_id TEXT "
                    "REFERENCES historical_sync_runs(run_id)"
                )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "idx_research_notes_historical_run_id "
                "ON research_notes(historical_run_id) "
                "WHERE historical_run_id IS NOT NULL"
            )
            connection.execute(
                "INSERT INTO schema_migrations (version, name) "
                "VALUES (4, 'historical sync checkpoints and research linkage')"
            )

    def _apply_market_data_cross_validation_migration(self) -> None:
        migration_path = (
            Path(__file__).with_name("migrations")
            / "0005_market_data_cross_validation.sql"
        )
        required_tables = {
            "market_data_validation_runs",
            "market_data_observations",
            "market_data_discrepancies",
        }
        with self._transaction() as connection:
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = 5"
            ).fetchone()
            present_tables = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if applied is not None:
                missing = required_tables - present_tables
                if missing:
                    raise PipelineRunStateError(
                        "schema migration 5 is recorded but tables are missing: "
                        + ", ".join(sorted(missing))
                    )
                return

            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            connection.execute(
                "INSERT INTO schema_migrations (version, name) "
                "VALUES (5, 'market data source cross validation')"
            )

    def _apply_historical_cross_validation_migration(self) -> None:
        migration_path = (
            Path(__file__).with_name("migrations")
            / "0006_historical_cross_validation.sql"
        )
        required_tables = {
            "historical_source_observations",
            "historical_validation_runs",
            "historical_validation_discrepancies",
        }
        with self._transaction() as connection:
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = 6"
            ).fetchone()
            present_tables = {
                row["name"]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            note_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(research_notes)")
            }
            if applied is not None:
                missing = required_tables - present_tables
                if missing or "historical_validation_run_id" not in note_columns:
                    detail = ", ".join(sorted(missing)) or "research note linkage"
                    raise PipelineRunStateError(
                        "schema migration 6 is recorded but schema is missing: "
                        + detail
                    )
                return

            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            if "historical_validation_run_id" not in note_columns:
                connection.execute(
                    "ALTER TABLE research_notes ADD COLUMN "
                    "historical_validation_run_id TEXT "
                    "REFERENCES historical_validation_runs(run_id)"
                )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "idx_research_notes_historical_validation_run_id "
                "ON research_notes(historical_validation_run_id) "
                "WHERE historical_validation_run_id IS NOT NULL"
            )
            connection.execute(
                "INSERT INTO schema_migrations (version, name) "
                "VALUES (6, 'historical source observations and cross validation')"
            )

    def _apply_source_artifacts_migration(self) -> None:
        migration_path = (
            Path(__file__).with_name("migrations") / "0010_source_artifacts.sql"
        )
        required_columns = {
            "id",
            "pipeline_run_id",
            "historical_run_id",
            "validation_run_id",
            "checkpoint_key",
            "provider",
            "dataset",
            "endpoint",
            "contract_version",
            "content_type",
            "payload_sha256",
            "payload_size_bytes",
            "hash_basis",
            "fetched_at",
            "created_at",
        }
        with self._transaction() as connection:
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = 10"
            ).fetchone()
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                "AND name = 'source_artifacts'"
            ).fetchone()
            columns = (
                {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(source_artifacts)"
                    ).fetchall()
                }
                if table is not None
                else set()
            )
            if applied is not None:
                if table is None or not required_columns <= columns:
                    raise PipelineRunStateError(
                        "schema migration 10 is recorded but source_artifacts "
                        "is missing or incomplete"
                    )
                return

            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            connection.execute(
                "INSERT INTO schema_migrations (version, name) "
                "VALUES (10, 'provider response source artifacts')"
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

    def get_schema_version(self) -> int:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
            ).fetchone()
        return int(row["version"])

    def list_source_artifacts(
        self,
        *,
        pipeline_run_id: str | None = None,
        historical_run_id: str | None = None,
        validation_run_id: str | None = None,
    ) -> list[SourceArtifact]:
        """List hash-only evidence for exactly one durable run owner."""

        owners = {
            "pipeline_run_id": pipeline_run_id,
            "historical_run_id": historical_run_id,
            "validation_run_id": validation_run_id,
        }
        selected = [(column, value) for column, value in owners.items() if value]
        if len(selected) != 1:
            raise ValueError("exactly one source-artifact run owner is required")
        column, run_id = selected[0]
        with self._transaction() as connection:
            rows = connection.execute(
                "SELECT provider, dataset, endpoint, contract_version, "
                "content_type, payload_sha256, payload_size_bytes, hash_basis, "
                "fetched_at FROM source_artifacts WHERE "
                + column
                + " = ? ORDER BY id",
                (run_id,),
            ).fetchall()
        return [
            SourceArtifact(
                provider=row["provider"],
                dataset=row["dataset"],
                endpoint=row["endpoint"],
                contract_version=row["contract_version"],
                content_type=row["content_type"],
                payload_sha256=row["payload_sha256"],
                payload_size_bytes=row["payload_size_bytes"],
                hash_basis=row["hash_basis"],
                fetched_at=(
                    None
                    if row["fetched_at"] is None
                    else datetime.fromisoformat(row["fetched_at"])
                ),
            )
            for row in rows
        ]

    def save_market_data(self, data: NormalizedMarketData) -> StorageWriteResult:
        with self._transaction() as connection:
            result = self._save_market_data(connection, data)
        return result

    def upsert_symbol(self, symbol: Symbol) -> None:
        with self._transaction() as connection:
            self._upsert_symbol(connection, symbol)

    def upsert_symbols(self, symbols: Sequence[Symbol]) -> None:
        """Atomically upsert canonical symbol master records.

        Callers must provide already validated domain ``Symbol`` records.  The
        SQL mapping remains in the existing single-symbol canonical helper so
        CLI watchlist setup cannot create a second symbol-writing contract.
        """
        with self._transaction() as connection:
            for symbol in symbols:
                self._upsert_symbol(connection, symbol)

    def get_symbol(self, symbol: str) -> Symbol | None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT symbol, name, market, currency, is_active "
                "FROM symbols WHERE symbol = ?",
                (symbol.strip().upper(),),
            ).fetchone()
        if row is None:
            return None
        return Symbol(
            symbol=row["symbol"],
            name=row["name"],
            market=row["market"],
            currency=row["currency"],
            is_active=bool(row["is_active"]),
        )

    def upsert_daily_prices(self, prices: Sequence[DailyPrice]) -> None:
        with self._transaction() as connection:
            self._upsert_daily_prices(connection, prices)

    def list_daily_prices(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[DailyPrice]:
        with self._transaction() as connection:
            return self._list_daily_prices(
                connection, symbol, start_date=start_date, end_date=end_date
            )

    def upsert_company_metrics(self, metrics: Sequence[CompanyMetric]) -> None:
        with self._transaction() as connection:
            self._upsert_company_metrics(connection, metrics)

    def list_company_metrics(
        self,
        symbol: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[CompanyMetric]:
        with self._transaction() as connection:
            return self._list_company_metrics(
                connection, symbol, start_date=start_date, end_date=end_date
            )

    def create_research_note(self, note: ResearchNote) -> ResearchNote:
        with self._transaction() as connection:
            return self._insert_research_note(connection, note)

    def get_research_note(self, note_id: int) -> ResearchNote | None:
        with self._transaction() as connection:
            row = connection.execute(
                self._research_note_select() + " WHERE id = ?", (note_id,)
            ).fetchone()
        return None if row is None else self._research_note_from_row(row)

    def get_research_note_by_run_id(self, run_id: str) -> ResearchNote | None:
        with self._transaction() as connection:
            row = connection.execute(
                self._research_note_select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else self._research_note_from_row(row)

    def get_research_note_by_historical_run_id(
        self, run_id: str
    ) -> ResearchNote | None:
        with self._transaction() as connection:
            row = connection.execute(
                self._research_note_select() + " WHERE historical_run_id = ?",
                (run_id,),
            ).fetchone()
        return None if row is None else self._research_note_from_row(row)

    def get_research_note_by_historical_validation_run_id(
        self, run_id: str
    ) -> ResearchNote | None:
        with self._transaction() as connection:
            row = connection.execute(
                self._research_note_select()
                + " WHERE historical_validation_run_id = ?",
                (run_id,),
            ).fetchone()
        return None if row is None else self._research_note_from_row(row)

    def list_research_notes(self, symbol: str) -> list[ResearchNote]:
        with self._transaction() as connection:
            rows = connection.execute(
                self._research_note_select()
                + " WHERE symbol = ? ORDER BY created_at DESC, id DESC",
                (symbol.strip().upper(),),
            ).fetchall()
        return [self._research_note_from_row(row) for row in rows]

    def delete_research_note(self, note_id: int) -> bool:
        with self._transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM research_notes WHERE id = ?", (note_id,)
            )
            deleted = cursor.rowcount
        return deleted == 1

    def get_or_create_pipeline_run(
        self,
        *,
        symbol: str,
        target_date: date,
        requested_start_date: date,
        requested_end_date: date,
        provider: str,
        created_at: datetime,
    ) -> PipelineRun:
        normalized_symbol = symbol.strip().upper()
        normalized_provider = provider.strip()
        if not normalized_symbol:
            raise ValueError("symbol must not be empty")
        if not normalized_provider:
            raise ValueError("provider must not be empty")
        if requested_start_date > requested_end_date:
            raise ValueError("requested_start_date must not be after requested_end_date")
        if target_date != requested_end_date:
            raise ValueError("target_date must equal requested_end_date")

        run_id = str(uuid4())
        timestamp = created_at.isoformat()
        with self._transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO pipeline_runs ("
                "run_id, symbol, target_date, requested_start_date, "
                "requested_end_date, status, provider, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    normalized_symbol,
                    target_date.isoformat(),
                    requested_start_date.isoformat(),
                    requested_end_date.isoformat(),
                    PipelineRunStatus.PENDING.value,
                    normalized_provider,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                self._pipeline_run_select()
                + " WHERE symbol = ? AND target_date = ?",
                (normalized_symbol, target_date.isoformat()),
            ).fetchone()
            if row is None:  # pragma: no cover - guarded by INSERT/UNIQUE contract.
                raise PipelineRunStateError("pipeline run could not be created")
            run = self._pipeline_run_from_row(row)
            self._validate_pipeline_run_contract(
                run,
                requested_start_date=requested_start_date,
                requested_end_date=requested_end_date,
                provider=normalized_provider,
            )
        return run

    def get_pipeline_run(self, run_id: str) -> PipelineRun | None:
        with self._transaction() as connection:
            row = connection.execute(
                self._pipeline_run_select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else self._pipeline_run_from_row(row)

    def get_pipeline_run_for_target(
        self, symbol: str, target_date: date
    ) -> PipelineRun | None:
        with self._transaction() as connection:
            row = connection.execute(
                self._pipeline_run_select()
                + " WHERE symbol = ? AND target_date = ?",
                (symbol.strip().upper(), target_date.isoformat()),
            ).fetchone()
        return None if row is None else self._pipeline_run_from_row(row)

    def list_pipeline_runs(self, symbol: str | None = None) -> list[PipelineRun]:
        query = self._pipeline_run_select()
        parameters: tuple[object, ...] = ()
        if symbol is not None:
            query += " WHERE symbol = ?"
            parameters = (symbol.strip().upper(),)
        query += " ORDER BY target_date, created_at"
        with self._transaction() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._pipeline_run_from_row(row) for row in rows]

    def start_pipeline_run(
        self,
        run_id: str,
        *,
        started_at: datetime,
        resume_running: bool = False,
    ) -> PipelineRun:
        timestamp = started_at.isoformat()
        with self._transaction() as connection:
            row = connection.execute(
                self._pipeline_run_select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise PipelineRunStateError(f"unknown pipeline run {run_id}")
            current = self._pipeline_run_from_row(row)
            if current.status is PipelineRunStatus.SUCCESS:
                raise PipelineRunStateError("a successful pipeline run cannot restart")
            if (
                current.status is PipelineRunStatus.RUNNING
                and not resume_running
            ):
                raise PipelineRunInProgressError(
                    f"pipeline run {run_id} is already running; provide its run id to resume"
                )

            cursor = connection.execute(
                "UPDATE pipeline_runs SET status = ?, "
                "started_at = COALESCE(started_at, ?), finished_at = NULL, "
                "error_message = NULL, research_note_id = NULL, "
                "source_endpoints_json = NULL, fetched_at = NULL, "
                "market_date = NULL, "
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
                raise PipelineRunInProgressError(
                    f"pipeline run {run_id} changed state while being claimed"
                )
            updated_row = connection.execute(
                self._pipeline_run_select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._pipeline_run_from_row(updated_row)

    def mark_pipeline_run_failed(
        self,
        run_id: str,
        *,
        finished_at: datetime,
        error_message: str,
    ) -> PipelineRun:
        safe_error = error_message.strip()[:1000] or "UnknownError"
        timestamp = finished_at.isoformat()
        with self._transaction() as connection:
            cursor = connection.execute(
                "UPDATE pipeline_runs SET status = ?, finished_at = ?, "
                "error_message = ?, research_note_id = NULL, "
                "source_endpoints_json = NULL, fetched_at = NULL, "
                "market_date = NULL, updated_at = ? "
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
                raise PipelineRunStateError(
                    f"pipeline run {run_id} was not running when failure was recorded"
                )
            row = connection.execute(
                self._pipeline_run_select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._pipeline_run_from_row(row)

    @contextmanager
    def successful_pipeline_run(
        self, run_id: str
    ) -> Iterator["SQLitePipelineUnitOfWork"]:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status FROM pipeline_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise PipelineRunStateError(f"unknown pipeline run {run_id}")
            if row["status"] != PipelineRunStatus.RUNNING.value:
                raise PipelineRunStateError(
                    f"pipeline run {run_id} must be running before completion"
                )
            unit_of_work = SQLitePipelineUnitOfWork(self, connection, run_id)
            yield unit_of_work
            final_row = connection.execute(
                "SELECT status FROM pipeline_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            if final_row["status"] != PipelineRunStatus.SUCCESS.value:
                raise PipelineRunStateError(
                    f"pipeline run {run_id} left its transaction without success"
                )

    @staticmethod
    def _save_market_data(
        connection: sqlite3.Connection, data: NormalizedMarketData
    ) -> StorageWriteResult:
        SQLiteResearchRepository._upsert_symbol(connection, data.symbol)
        SQLiteResearchRepository._upsert_daily_prices(connection, data.daily_prices)
        SQLiteResearchRepository._upsert_company_metrics(
            connection, data.company_metrics
        )
        return StorageWriteResult(
            symbols=1,
            daily_prices=len(data.daily_prices),
            company_metrics=len(data.company_metrics),
        )

    @staticmethod
    def _insert_source_artifacts(
        connection: sqlite3.Connection,
        artifacts: Sequence[SourceArtifact],
        *,
        provider: str,
        checkpoint_key: str,
        created_at: datetime,
        pipeline_run_id: str | None = None,
        historical_run_id: str | None = None,
        validation_run_id: str | None = None,
    ) -> int:
        owners = (pipeline_run_id, historical_run_id, validation_run_id)
        if sum(owner is not None for owner in owners) != 1:
            raise PipelineRunStateError(
                "source artifacts require exactly one durable run owner"
            )
        normalized_provider = provider.strip()
        if not normalized_provider:
            raise PipelineRunStateError("source artifact provider must not be blank")
        normalized_checkpoint_key = checkpoint_key.strip()
        if not normalized_checkpoint_key:
            raise PipelineRunStateError(
                "source artifact checkpoint_key must not be blank"
            )
        if created_at.utcoffset() is None:
            raise PipelineRunStateError(
                "source artifact created_at must be timezone-aware"
            )
        seen: set[tuple[str, str, str]] = set()
        rows: list[tuple[object, ...]] = []
        for artifact in artifacts:
            if artifact.provider != normalized_provider:
                raise PipelineRunStateError(
                    "source artifact provider does not match durable run"
                )
            key = (artifact.dataset, artifact.endpoint, artifact.payload_sha256)
            if key in seen:
                raise PipelineRunStateError("duplicate source artifact evidence")
            seen.add(key)
            rows.append(
                (
                    pipeline_run_id,
                    historical_run_id,
                    validation_run_id,
                    normalized_checkpoint_key,
                    artifact.provider,
                    artifact.dataset,
                    artifact.endpoint,
                    artifact.contract_version,
                    artifact.content_type,
                    artifact.payload_sha256,
                    artifact.payload_size_bytes,
                    artifact.hash_basis,
                    (
                        None
                        if artifact.fetched_at is None
                        else artifact.fetched_at.isoformat()
                    ),
                    created_at.isoformat(),
                )
            )
        if rows:
            connection.executemany(
                "INSERT INTO source_artifacts ("
                "pipeline_run_id, historical_run_id, validation_run_id, "
                "checkpoint_key, provider, dataset, endpoint, contract_version, "
                "content_type, "
                "payload_sha256, payload_size_bytes, hash_basis, fetched_at, "
                "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        return len(rows)

    @staticmethod
    def _upsert_symbol(connection: sqlite3.Connection, symbol: Symbol) -> None:
        connection.execute(
            "INSERT INTO symbols (symbol, name, market, currency, is_active) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(symbol) DO UPDATE SET "
            "name = excluded.name, market = excluded.market, "
            "currency = excluded.currency, is_active = excluded.is_active, "
            "updated_at = CURRENT_TIMESTAMP",
            (
                symbol.symbol,
                symbol.name,
                symbol.market,
                symbol.currency,
                int(symbol.is_active),
            ),
        )

    @staticmethod
    def _upsert_daily_prices(
        connection: sqlite3.Connection, prices: Sequence[DailyPrice]
    ) -> None:
        connection.executemany(
            "INSERT INTO daily_prices ("
            "symbol, trade_date, open_price, high_price, low_price, close_price, "
            "volume, source"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(symbol, trade_date) DO UPDATE SET "
            "open_price = excluded.open_price, high_price = excluded.high_price, "
            "low_price = excluded.low_price, close_price = excluded.close_price, "
            "volume = excluded.volume, source = excluded.source, "
            "updated_at = CURRENT_TIMESTAMP",
            [
                (
                    price.symbol,
                    price.trade_date.isoformat(),
                    price.open,
                    price.high,
                    price.low,
                    price.close,
                    price.volume,
                    price.source,
                )
                for price in prices
            ],
        )

    @staticmethod
    def _list_daily_prices(
        connection: sqlite3.Connection,
        symbol: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[DailyPrice]:
        clauses = ["symbol = ?"]
        parameters: list[object] = [symbol.strip().upper()]
        if start_date is not None:
            clauses.append("trade_date >= ?")
            parameters.append(start_date.isoformat())
        if end_date is not None:
            clauses.append("trade_date <= ?")
            parameters.append(end_date.isoformat())
        rows = connection.execute(
            "SELECT symbol, trade_date, open_price, high_price, low_price, "
            "close_price, volume, source FROM daily_prices WHERE "
            + " AND ".join(clauses)
            + " ORDER BY trade_date",
            parameters,
        ).fetchall()
        return [
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

    @staticmethod
    def _upsert_company_metrics(
        connection: sqlite3.Connection, metrics: Sequence[CompanyMetric]
    ) -> None:
        connection.executemany(
            "INSERT INTO company_metrics ("
            "symbol, metric_date, metric_name, metric_value, unit, source"
            ") VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(symbol, metric_date, metric_name) DO UPDATE SET "
            "metric_value = excluded.metric_value, unit = excluded.unit, "
            "source = excluded.source, updated_at = CURRENT_TIMESTAMP",
            [
                (
                    metric.symbol,
                    metric.metric_date.isoformat(),
                    metric.name,
                    metric.value,
                    metric.unit,
                    metric.source,
                )
                for metric in metrics
            ],
        )

    @staticmethod
    def _list_company_metrics(
        connection: sqlite3.Connection,
        symbol: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[CompanyMetric]:
        clauses = ["symbol = ?"]
        parameters: list[object] = [symbol.strip().upper()]
        if start_date is not None:
            clauses.append("metric_date >= ?")
            parameters.append(start_date.isoformat())
        if end_date is not None:
            clauses.append("metric_date <= ?")
            parameters.append(end_date.isoformat())
        rows = connection.execute(
            "SELECT symbol, metric_date, metric_name, metric_value, unit, source "
            "FROM company_metrics WHERE "
            + " AND ".join(clauses)
            + " ORDER BY metric_date, metric_name",
            parameters,
        ).fetchall()
        return [
            CompanyMetric(
                symbol=row["symbol"],
                metric_date=date.fromisoformat(row["metric_date"]),
                name=row["metric_name"],
                value=row["metric_value"],
                unit=row["unit"],
                source=row["source"],
            )
            for row in rows
        ]

    @staticmethod
    def _insert_research_note(
        connection: sqlite3.Connection, note: ResearchNote
    ) -> ResearchNote:
        if note.id is not None:
            raise ValueError("new research note must not already have an id")
        cursor = connection.execute(
            "INSERT INTO research_notes ("
            "symbol, created_at, analysis_type, title, summary, "
            "source_data_start, source_data_end, provider_source, run_id, "
            "historical_run_id, historical_validation_run_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                note.symbol,
                note.created_at.isoformat(),
                note.analysis_type,
                note.title,
                note.summary,
                note.source_data_start.isoformat(),
                note.source_data_end.isoformat(),
                note.provider_source,
                note.run_id,
                note.historical_run_id,
                note.historical_validation_run_id,
            ),
        )
        return replace(note, id=int(cursor.lastrowid))

    @staticmethod
    def _research_note_select() -> str:
        return (
            "SELECT id, symbol, created_at, analysis_type, title, summary, "
            "source_data_start, source_data_end, provider_source, run_id, "
            "historical_run_id, historical_validation_run_id "
            "FROM research_notes"
        )

    @staticmethod
    def _research_note_from_row(row: sqlite3.Row) -> ResearchNote:
        return ResearchNote(
            id=row["id"],
            symbol=row["symbol"],
            created_at=datetime.fromisoformat(row["created_at"]),
            analysis_type=row["analysis_type"],
            title=row["title"],
            summary=row["summary"],
            source_data_start=date.fromisoformat(row["source_data_start"]),
            source_data_end=date.fromisoformat(row["source_data_end"]),
            provider_source=row["provider_source"],
            run_id=row["run_id"],
            historical_run_id=row["historical_run_id"],
            historical_validation_run_id=row["historical_validation_run_id"],
        )

    @staticmethod
    def _pipeline_run_select() -> str:
        return (
            "SELECT run_id, symbol, target_date, requested_start_date, "
            "requested_end_date, status, provider, created_at, started_at, "
            "finished_at, error_message, attempt_count, research_note_id, "
            "source_endpoints_json, fetched_at, market_date "
            "FROM pipeline_runs"
        )

    @staticmethod
    def _pipeline_run_from_row(row: sqlite3.Row) -> PipelineRun:
        return PipelineRun(
            run_id=row["run_id"],
            symbol=row["symbol"],
            target_date=date.fromisoformat(row["target_date"]),
            requested_start_date=date.fromisoformat(row["requested_start_date"]),
            requested_end_date=date.fromisoformat(row["requested_end_date"]),
            status=PipelineRunStatus(row["status"]),
            provider=row["provider"],
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
            source_endpoints=SQLiteResearchRepository._source_endpoints_from_json(
                row["source_endpoints_json"]
            ),
            fetched_at=(
                None
                if row["fetched_at"] is None
                else datetime.fromisoformat(row["fetched_at"])
            ),
            market_date=(
                None
                if row["market_date"] is None
                else date.fromisoformat(row["market_date"])
            ),
        )

    @staticmethod
    def _source_endpoints_from_json(value: str | None) -> tuple[str, ...]:
        if value is None:
            return ()
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as error:
            raise PipelineRunStateError(
                "pipeline run source_endpoints_json is invalid"
            ) from error
        if not isinstance(decoded, list) or any(
            not isinstance(item, str) or not item for item in decoded
        ):
            raise PipelineRunStateError(
                "pipeline run source_endpoints_json must contain strings"
            )
        return tuple(decoded)

    @staticmethod
    def _validate_pipeline_run_contract(
        run: PipelineRun,
        *,
        requested_start_date: date,
        requested_end_date: date,
        provider: str,
    ) -> None:
        conflicts: list[str] = []
        if run.requested_start_date != requested_start_date:
            conflicts.append("requested_start_date")
        if run.requested_end_date != requested_end_date:
            conflicts.append("requested_end_date")
        if run.provider != provider:
            conflicts.append("provider")
        if conflicts:
            raise PipelineRunConflictError(
                "existing symbol/target_date run conflicts on " + ", ".join(conflicts)
            )


class SQLitePipelineUnitOfWork:
    """Operations committed together with a successful pipeline-run transition."""

    def __init__(
        self,
        repository: SQLiteResearchRepository,
        connection: sqlite3.Connection,
        run_id: str,
    ) -> None:
        self.repository = repository
        self.connection = connection
        self.run_id = run_id

    def save_market_data(self, data: NormalizedMarketData) -> StorageWriteResult:
        return self.repository._save_market_data(self.connection, data)

    def save_source_artifacts(
        self,
        artifacts: Sequence[SourceArtifact],
        *,
        provider: str,
        created_at: datetime,
    ) -> int:
        return self.repository._insert_source_artifacts(
            self.connection,
            artifacts,
            provider=provider,
            checkpoint_key="pipeline-success-v1",
            created_at=created_at,
            pipeline_run_id=self.run_id,
        )

    def list_daily_prices(
        self, symbol: str, *, start_date: date, end_date: date
    ) -> list[DailyPrice]:
        return self.repository._list_daily_prices(
            self.connection, symbol, start_date=start_date, end_date=end_date
        )

    def list_company_metrics(
        self, symbol: str, *, start_date: date, end_date: date
    ) -> list[CompanyMetric]:
        return self.repository._list_company_metrics(
            self.connection, symbol, start_date=start_date, end_date=end_date
        )

    def create_research_note(self, note: ResearchNote) -> ResearchNote:
        if (
            note.run_id != self.run_id
            or note.historical_run_id is not None
            or note.historical_validation_run_id is not None
        ):
            raise PipelineRunStateError(
                "research note run_id must match the active pipeline run"
            )
        return self.repository._insert_research_note(self.connection, note)

    def mark_success(
        self,
        research_note_id: int,
        *,
        finished_at: datetime,
        source_endpoints: Sequence[str] = (),
        fetched_at: datetime | None = None,
        market_date: date | None = None,
    ) -> None:
        note_row = self.connection.execute(
            "SELECT run_id FROM research_notes WHERE id = ?", (research_note_id,)
        ).fetchone()
        if note_row is None or note_row["run_id"] != self.run_id:
            raise PipelineRunStateError(
                "successful run must reference its own persisted research note"
            )
        timestamp = finished_at.isoformat()
        endpoints_json = (
            None
            if not source_endpoints
            else json.dumps(
                list(source_endpoints), ensure_ascii=False, separators=(",", ":")
            )
        )
        cursor = self.connection.execute(
            "UPDATE pipeline_runs SET status = ?, finished_at = ?, "
            "error_message = NULL, research_note_id = ?, "
            "source_endpoints_json = ?, fetched_at = ?, market_date = ?, "
            "updated_at = ? "
            "WHERE run_id = ? AND status = ?",
            (
                PipelineRunStatus.SUCCESS.value,
                timestamp,
                research_note_id,
                endpoints_json,
                None if fetched_at is None else fetched_at.isoformat(),
                None if market_date is None else market_date.isoformat(),
                timestamp,
                self.run_id,
                PipelineRunStatus.RUNNING.value,
            ),
        )
        if cursor.rowcount != 1:
            raise PipelineRunStateError(
                f"pipeline run {self.run_id} could not transition to success"
            )
