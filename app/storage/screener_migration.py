"""Explicit schema migration runner for Market Screener persistence v11.

The runner is intentionally separate from the frozen M1-M9 repository
bootstrap.  S4.1 can therefore prove the migration without activating any
Screener persistence path or changing the M9 schema-version boundary.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


class ScreenerMigrationStateError(RuntimeError):
    """The recorded Screener migration state does not match the SQLite schema."""


class SQLiteScreenerMigrationRunner:
    """Apply and verify migration 0011 as one SQLite transaction."""

    MIGRATION_VERSION = 11
    MIGRATION_NAME = "market screener persistence"

    _REQUIRED_COLUMNS = {
        "market_universe_runs": frozenset(
            {
                "universe_run_id",
                "market_date",
                "methodology_version",
                "source_policy",
                "input_evidence_sha256",
                "universe_count",
                "scan_eligible_count",
                "scan_unavailable_count",
                "excluded_count",
                "inactive_count",
                "unresolved_count",
                "status",
                "canonical_sha256",
                "attempt_count",
                "error_code",
                "created_at",
                "started_at",
                "finished_at",
                "updated_at",
            }
        ),
        "market_universe_members": frozenset(
            {
                "universe_run_id",
                "symbol",
                "name",
                "market",
                "status",
                "listing_date",
                "delisting_date",
                "exclusion_reason",
            }
        ),
        "screener_runs": frozenset(
            {
                "screener_run_id",
                "market_date",
                "universe_run_id",
                "stage1_methodology_version",
                "stage2_methodology_version",
                "source_policy",
                "candidate_limit",
                "input_manifest_sha256",
                "stage1_canonical_sha256",
                "universe_count",
                "screened_count",
                "triggered_count",
                "candidate_count",
                "truncated",
                "status",
                "canonical_sha256",
                "attempt_count",
                "error_code",
                "created_at",
                "started_at",
                "finished_at",
                "updated_at",
            }
        ),
        "screener_candidates": frozenset(
            {
                "candidate_id",
                "screener_run_id",
                "universe_run_id",
                "symbol",
                "status",
                "rank",
                "stage1_rank",
                "candidate_kind",
                "stage1_trigger_count",
                "stage1_reason_count",
                "stage2_reason_count",
                "metric_count",
                "data_quality_status",
                "validation_status",
                "analysis_status",
                "pipeline_run_id",
                "historical_run_id",
                "validation_run_id",
                "canonical_sources_json",
                "validation_sources_json",
                "discrepancies_json",
                "input_locator_sha256",
                "snapshot_sha256",
                "payload_sha256",
                "failure_code",
                "failure_type",
                "attempt_count",
                "created_at",
                "started_at",
                "finished_at",
                "updated_at",
            }
        ),
        "candidate_reasons": frozenset(
            {
                "candidate_id",
                "stage",
                "ordinal",
                "code",
                "metric",
                "component",
                "previous_json",
                "current_json",
                "delta",
                "unit",
                "operator",
                "threshold_json",
                "rule_version",
                "role",
                "trigger_class",
                "reason_kind",
                "reason_class",
                "threshold_multiple",
            }
        ),
        "candidate_metrics": frozenset(
            {
                "candidate_id",
                "ordinal",
                "name",
                "status",
                "value",
                "previous_value",
                "delta",
                "unit",
                "as_of_date",
                "previous_as_of_date",
                "observations",
                "previous_observations",
            }
        ),
        "screener_source_artifacts": frozenset(
            {
                "artifact_ref_id",
                "universe_run_id",
                "screener_run_id",
                "candidate_id",
                "ordinal",
                "source_role",
                "upstream_owner_kind",
                "upstream_owner_run_id",
                "provider",
                "dataset",
                "source_ref",
                "contract_version",
                "payload_sha256",
                "payload_size_bytes",
                "hash_basis",
                "created_at",
            }
        ),
    }
    _REQUIRED_INDEXES = frozenset(
        {
            "idx_market_universe_runs_market_date_status",
            "idx_market_universe_members_symbol_status",
            "idx_market_universe_members_run_status",
            "idx_screener_runs_market_date_status",
            "idx_screener_runs_methodology_date",
            "idx_screener_candidates_symbol_run",
            "idx_screener_candidates_run_rank_unique",
            "idx_candidate_reasons_stage_code",
            "idx_candidate_reasons_metric",
            "idx_candidate_metrics_name_status",
            "idx_screener_artifacts_universe_ordinal",
            "idx_screener_artifacts_run_ordinal",
            "idx_screener_artifacts_candidate_ordinal",
            "idx_screener_artifacts_payload_sha256",
            "idx_screener_artifacts_provider_dataset",
        }
    )
    _REQUIRED_TRIGGERS = frozenset(
        {
            "trg_market_universe_runs_success_terminal",
            "trg_screener_runs_success_terminal",
            "trg_screener_candidates_success_terminal",
        }
    )

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def migrate(self) -> None:
        """Apply v11 once, or verify the complete recorded v11 schema."""

        migration_path = (
            Path(__file__).with_name("migrations")
            / "0011_market_screener_persistence.sql"
        )
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._require_base_migration(connection)
            applied = connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?",
                (self.MIGRATION_VERSION,),
            ).fetchone()
            if applied is not None:
                self._verify_schema(connection)
                connection.commit()
                return

            self._reject_unrecorded_schema_objects(connection)
            self._execute_sql_statements(
                connection,
                migration_path.read_text(encoding="utf-8"),
            )
            self._verify_schema(connection)
            connection.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
                (self.MIGRATION_VERSION, self.MIGRATION_NAME),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @classmethod
    def _require_base_migration(cls, connection: sqlite3.Connection) -> None:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'schema_migrations'"
        ).fetchone()
        if table is None:
            raise ScreenerMigrationStateError(
                "schema_migrations is missing; initialize the frozen v10 schema first"
            )
        base = connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version = 10"
        ).fetchone()
        if base is None:
            raise ScreenerMigrationStateError(
                "migration 10 is required before Market Screener migration 11"
            )

    @classmethod
    def _reject_unrecorded_schema_objects(
        cls, connection: sqlite3.Connection
    ) -> None:
        expected = (
            set(cls._REQUIRED_COLUMNS)
            | set(cls._REQUIRED_INDEXES)
            | set(cls._REQUIRED_TRIGGERS)
        )
        present = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type IN ('table', 'index', 'trigger')"
            )
            if row["name"] in expected
        }
        if present:
            raise ScreenerMigrationStateError(
                "unrecorded Market Screener schema objects exist: "
                + ", ".join(sorted(present))
            )

    @classmethod
    def _verify_schema(cls, connection: sqlite3.Connection) -> None:
        objects = {
            (row["type"], row["name"])
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master "
                "WHERE type IN ('table', 'index', 'trigger')"
            )
        }
        missing_tables = {
            table
            for table in cls._REQUIRED_COLUMNS
            if ("table", table) not in objects
        }
        missing_indexes = {
            index
            for index in cls._REQUIRED_INDEXES
            if ("index", index) not in objects
        }
        missing_triggers = {
            trigger
            for trigger in cls._REQUIRED_TRIGGERS
            if ("trigger", trigger) not in objects
        }
        details: list[str] = []
        if missing_tables:
            details.append("tables=" + ",".join(sorted(missing_tables)))
        if missing_indexes:
            details.append("indexes=" + ",".join(sorted(missing_indexes)))
        if missing_triggers:
            details.append("triggers=" + ",".join(sorted(missing_triggers)))

        missing_columns: list[str] = []
        for table, required in cls._REQUIRED_COLUMNS.items():
            if table in missing_tables:
                continue
            columns = {
                row["name"]
                for row in connection.execute(f'PRAGMA table_info("{table}")')
            }
            missing = required - columns
            if missing:
                missing_columns.append(
                    f"{table}=" + ",".join(sorted(missing))
                )
        if missing_columns:
            details.append("columns=" + ";".join(missing_columns))
        if details:
            raise ScreenerMigrationStateError(
                "migration 11 is missing or incomplete: " + " | ".join(details)
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


__all__ = ["ScreenerMigrationStateError", "SQLiteScreenerMigrationRunner"]
