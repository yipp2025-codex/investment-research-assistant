"""Explicit DS3 dataset-version migration and persistence adapters.

This module is intentionally outside the production composition root.  The
migration runner requires an explicit isolated flag, and the repository only
reads or writes the v12 dataset-version tables.  Neither class fetches from a
provider, runs a screener stage, or changes ``daily_prices``.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import hashlib
import sqlite3
from typing import Callable, Iterator

from app.data_contracts.dataset_persistence import (
    DATASET_PERSISTENCE_CONTRACT_VERSION,
    CoverageBasis,
    DatasetArtifactRef,
    DatasetObservation,
    MixedDatasetVersion,
)
from app.data_contracts.dual_source import (
    AuthorityStatus,
    DatasetCoverage,
    DatasetProvenanceSummary,
    DatasetSourceStatus,
    DatasetVersionIdentity,
    ReconciliationStatus,
    SourceRole,
    TWSE_CANONICAL_AUTHORITY,
)


MIGRATION_VERSION = 12
MIGRATION_NAME = "dual-source dataset versions"
_PRODUCTION_DATABASE_PATH = (
    Path(__file__).resolve().parents[2] / "data" / "research.db"
).resolve()
FaultInjector = Callable[[str], None]


class DatasetMigrationStateError(RuntimeError):
    """The v12 migration state or schema is not safe to use."""


class DatasetPersistenceIntegrityError(RuntimeError):
    """A persisted dataset cannot be reconstructed exactly and safely."""


class DatasetVersionNotFoundError(KeyError):
    """The requested dataset version is not present."""


@dataclass(frozen=True, slots=True)
class DatasetMigrationResult:
    version: int
    applied: bool


@dataclass(frozen=True, slots=True)
class DatasetVersionPersistenceResult:
    dataset_version_id: str
    canonical_sha256: str
    written: bool
    created_at: str


def _inject(fault_injector: FaultInjector | None, point: str) -> None:
    if fault_injector is not None:
        fault_injector(point)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _parse_date(value: str | None, field_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DatasetPersistenceIntegrityError(
            f"persisted {field_name} is not an ISO date"
        ) from exc


class DatasetVersionMigrationRunner:
    """Apply migration 0012 only to an explicitly isolated v11 database."""

    MIGRATION_VERSION = MIGRATION_VERSION
    MIGRATION_NAME = MIGRATION_NAME

    _REQUIRED_COLUMNS = {
        "screener_runs": frozenset(
            {
                "ds5_execution_status",
                "ds5_source_policy",
                "ds5_source_status",
                "ds5_research_data_quality",
                "ds5_authority_status",
                "ds5_reconciliation_status",
                "ds5_supplemental_candidate_count",
                "ds5_canonical_authority",
                "ds5_supplemental_sources_json",
                "ds5_twse_observation_count",
                "ds5_missing_twse_count",
                "ds5_discrepancy_count",
                "ds5_dataset_identity_sha256",
                "ds5_provenance_map_sha256",
                "ds5_dataset_version_ids_json",
            }
        ),
        "screener_candidates": frozenset(
            {
                "ds5_dataset_version_id",
                "ds5_source_policy",
                "ds5_source_status",
                "ds5_authority_status",
                "ds5_reconciliation_status",
                "ds5_research_data_quality",
                "ds5_canonical_authority",
                "ds5_supplemental_sources_json",
                "ds5_twse_observation_count",
                "ds5_esun_supplemental_count",
                "ds5_missing_twse_count",
                "ds5_discrepancy_count",
                "ds5_provenance_map_sha256",
                "ds5_parent_dataset_version_id",
            }
        ),
        "research_dataset_versions": frozenset(
            {
                "dataset_version_id",
                "symbol",
                "as_of_date",
                "contract_version",
                "persistence_contract_version",
                "methodology_version",
                "source_policy",
                "source_status",
                "authority_status",
                "reconciliation_status",
                "coverage_basis",
                "required_observation_count",
                "twse_observation_count",
                "esun_supplemental_count",
                "missing_twse_count",
                "selected_observation_count",
                "discrepancy_count",
                "coverage_complete",
                "latest_reconciled_date",
                "provenance_map_sha256",
                "parent_dataset_version_id",
                "canonical_sha256",
                "created_at",
            }
        ),
        "research_dataset_observations": frozenset(
            {
                "dataset_version_id",
                "symbol",
                "trade_date",
                "provider",
                "source_role",
                "source_run_id",
                "selected",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "observation_sha256",
            }
        ),
        "research_dataset_artifacts": frozenset(
            {
                "dataset_version_id",
                "ordinal",
                "provider",
                "dataset",
                "source_ref",
                "contract_version",
                "payload_sha256",
                "payload_size_bytes",
                "hash_basis",
            }
        ),
    }
    _REQUIRED_INDEXES = frozenset(
        {
            "idx_research_dataset_versions_symbol_date",
            "idx_research_dataset_versions_parent",
            "idx_research_dataset_observations_date",
            "idx_research_dataset_observations_provider_role",
            "ux_research_dataset_observations_selected_date",
            "idx_research_dataset_artifacts_payload",
            "idx_research_dataset_artifacts_provider_dataset",
            "idx_screener_runs_ds5_quality",
            "idx_screener_candidates_ds5_dataset",
        }
    )

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def migrate(
        self,
        *,
        isolated: bool = False,
        fault_injector: FaultInjector | None = None,
    ) -> DatasetMigrationResult:
        """Migrate a temporary v11 database; refuse an implicit production write."""

        if not isolated:
            raise DatasetMigrationStateError(
                "migration 0012 requires explicit isolated=True; production is not in scope"
            )
        if self.database_path.resolve() == _PRODUCTION_DATABASE_PATH:
            raise DatasetMigrationStateError(
                "migration 0012 refuses the frozen production database path"
            )
        return self._migrate_once(fault_injector=fault_injector)

    def migrate_production_for_activation(
        self,
        *,
        expected_source_sha256: str,
        verified_backup_path: str | Path,
        verified_backup_sha256: str,
        fault_injector: FaultInjector | None = None,
    ) -> DatasetMigrationResult:
        """Apply frozen 0012 only after an explicit activation checkpoint.

        The ordinary ``migrate`` API remains isolated-only.  This separate
        method is intentionally verbose at its call boundary: production
        migration requires the caller to supply the pre-migration source SHA
        and the SHA of a separately verified v11 backup.
        """

        if self.database_path.resolve() != _PRODUCTION_DATABASE_PATH:
            raise DatasetMigrationStateError(
                "production activation migration requires the frozen production database path"
            )
        for value, label in (
            (expected_source_sha256, "expected_source_sha256"),
            (verified_backup_sha256, "verified_backup_sha256"),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdefABCDEF" for character in value)
            ):
                raise DatasetMigrationStateError(f"{label} must be a SHA-256 hex digest")
        backup = Path(verified_backup_path).resolve()
        if not backup.is_file():
            raise DatasetMigrationStateError("verified v11 backup is missing")
        source_sha256 = hashlib.sha256(self.database_path.read_bytes()).hexdigest()
        if source_sha256 != expected_source_sha256:
            raise DatasetMigrationStateError(
                "production source SHA changed after the activation checkpoint"
            )
        backup_sha256 = hashlib.sha256(backup.read_bytes()).hexdigest()
        if backup_sha256 != verified_backup_sha256:
            raise DatasetMigrationStateError(
                "verified v11 backup SHA does not match the activation checkpoint"
            )
        return self._migrate_once(fault_injector=fault_injector)

    def _migrate_once(
        self,
        *,
        fault_injector: FaultInjector | None = None,
    ) -> DatasetMigrationResult:
        migration_path = (
            Path(__file__).with_name("migrations")
            / "0012_dual_source_dataset_versions.sql"
        )
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._require_v11(connection)
            max_version = self._max_version(connection)
            if max_version > self.MIGRATION_VERSION:
                raise DatasetMigrationStateError(
                    f"database schema version {max_version} is newer than migration 0012"
                )
            applied = connection.execute(
                "SELECT name FROM schema_migrations WHERE version = ?",
                (self.MIGRATION_VERSION,),
            ).fetchone()
            if applied is not None:
                if applied["name"] != self.MIGRATION_NAME:
                    raise DatasetMigrationStateError(
                        "migration 0012 record name does not match the frozen contract"
                    )
                self._verify_schema(connection)
                self._verify_integrity(connection)
                connection.commit()
                return DatasetMigrationResult(self.MIGRATION_VERSION, False)

            self._reject_unrecorded_schema_objects(connection)
            _inject(fault_injector, "before_sql")
            self._execute_sql_statements(
                connection, migration_path.read_text(encoding="utf-8")
            )
            _inject(fault_injector, "after_tables")
            self._verify_schema(connection)
            self._verify_integrity(connection)
            connection.execute(
                "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
                (self.MIGRATION_VERSION, self.MIGRATION_NAME),
            )
            _inject(fault_injector, "before_commit")
            connection.commit()
            return DatasetMigrationResult(self.MIGRATION_VERSION, True)
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @classmethod
    def _max_version(cls, connection: sqlite3.Connection) -> int:
        row = connection.execute(
            "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
        ).fetchone()
        return int(row["version"])

    @classmethod
    def _require_v11(cls, connection: sqlite3.Connection) -> None:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'schema_migrations'"
        ).fetchone()
        if table is None:
            raise DatasetMigrationStateError(
                "schema_migrations is missing; initialize the frozen v11 schema first"
            )
        row = connection.execute(
            "SELECT name FROM schema_migrations WHERE version = 11"
        ).fetchone()
        if row is None:
            raise DatasetMigrationStateError(
                "migration 0011 is required before migration 0012"
            )
        if row["name"] != "market screener persistence":
            raise DatasetMigrationStateError(
                "migration 0011 record name does not match the frozen contract"
            )

    @classmethod
    def _require_v12(cls, connection: sqlite3.Connection) -> None:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' "
            "AND name = 'schema_migrations'"
        ).fetchone()
        if table is None:
            raise DatasetMigrationStateError(
                "schema_migrations is missing; v12 is unavailable"
            )
        row = connection.execute(
            "SELECT name FROM schema_migrations WHERE version = ?",
            (cls.MIGRATION_VERSION,),
        ).fetchone()
        if row is None or row["name"] != cls.MIGRATION_NAME:
            raise DatasetMigrationStateError(
                "schema v12 migration 0012 is not recorded with the frozen dataset-version name"
            )
        max_version = connection.execute(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
        ).fetchone()[0]
        if int(max_version) != cls.MIGRATION_VERSION:
            raise DatasetMigrationStateError(
                f"dataset repository requires schema v12, found v{max_version}"
            )

    @classmethod
    def _reject_unrecorded_schema_objects(
        cls, connection: sqlite3.Connection
    ) -> None:
        expected = {
            name
            for name in set(cls._REQUIRED_COLUMNS) | set(cls._REQUIRED_INDEXES)
            if name.startswith("research_dataset_")
            or name.startswith("idx_research_dataset_")
            or name == "ux_research_dataset_observations_selected_date"
        }
        present = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
            )
            if row["name"] in expected
        }
        if present:
            raise DatasetMigrationStateError(
                "unrecorded dataset-version schema objects exist: "
                + ", ".join(sorted(present))
            )

    @classmethod
    def _verify_schema(cls, connection: sqlite3.Connection) -> None:
        objects = {
            (row["type"], row["name"])
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master "
                "WHERE type IN ('table', 'index')"
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
        details: list[str] = []
        if missing_tables:
            details.append("tables=" + ",".join(sorted(missing_tables)))
        if missing_indexes:
            details.append("indexes=" + ",".join(sorted(missing_indexes)))
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
                missing_columns.append(f"{table}=" + ",".join(sorted(missing)))
        if missing_columns:
            details.append("columns=" + ";".join(missing_columns))
        if details:
            raise DatasetMigrationStateError(
                "migration 0012 is missing or incomplete: " + " | ".join(details)
            )

    @staticmethod
    def _verify_integrity(connection: sqlite3.Connection) -> None:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise DatasetMigrationStateError(
                f"SQLite integrity_check failed during migration 0012: {integrity}"
            )
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_keys:
            raise DatasetMigrationStateError(
                "SQLite foreign_key_check returned violations during migration 0012"
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


class DatasetVersionRepository:
    """Persist and strictly replay immutable DS3 dataset-version snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def save(
        self,
        version: MixedDatasetVersion,
        *,
        fault_injector: FaultInjector | None = None,
    ) -> DatasetVersionPersistenceResult:
        if not isinstance(version, MixedDatasetVersion):
            raise TypeError("version must be a MixedDatasetVersion")
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            connection.execute("BEGIN IMMEDIATE")
            DatasetVersionMigrationRunner._require_v12(connection)
            existing = connection.execute(
                "SELECT created_at FROM research_dataset_versions "
                "WHERE dataset_version_id = ?",
                (version.identity.dataset_version_id,),
            ).fetchone()
            if existing is not None:
                reconstructed = self._reconstruct(connection, version.identity.dataset_version_id)
                if reconstructed.canonical_json() != version.canonical_json():
                    raise DatasetPersistenceIntegrityError(
                        "existing dataset version differs; immutable rows will not be repaired"
                    )
                connection.commit()
                return DatasetVersionPersistenceResult(
                    dataset_version_id=version.identity.dataset_version_id,
                    canonical_sha256=version.canonical_sha256,
                    written=False,
                    created_at=existing["created_at"],
                )

            created_at = _utc_now()
            _inject(fault_injector, "before_version_insert")
            self._insert_version(connection, version, created_at)
            _inject(fault_injector, "after_version_insert")
            self._insert_observations(connection, version)
            _inject(fault_injector, "after_observations")
            self._insert_artifacts(connection, version)
            _inject(fault_injector, "after_artifacts")
            reconstructed = self._reconstruct(connection, version.identity.dataset_version_id)
            if reconstructed.canonical_json() != version.canonical_json():
                raise DatasetPersistenceIntegrityError(
                    "newly persisted dataset version failed canonical replay verification"
                )
            _inject(fault_injector, "before_commit")
            connection.commit()
            return DatasetVersionPersistenceResult(
                dataset_version_id=version.identity.dataset_version_id,
                canonical_sha256=version.canonical_sha256,
                written=True,
                created_at=created_at,
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def get(self, dataset_version_id: str) -> MixedDatasetVersion | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT dataset_version_id FROM research_dataset_versions "
                "WHERE dataset_version_id = ?",
                (dataset_version_id,),
            ).fetchone()
            if row is None:
                return None
            return self._reconstruct(connection, dataset_version_id)

    def list_versions(
        self,
        *,
        symbol: str,
        as_of_date: date,
        source_status: DatasetSourceStatus | str | None = None,
    ) -> tuple[MixedDatasetVersion, ...]:
        """List exact symbol/date versions for an orchestration boundary.

        This is deliberately not a ``latest`` or ``max id`` helper.  Callers
        receive every immutable match and must either identify one by an
        explicit lineage rule or fail closed.  M9 consumers do not use this
        method; they require ``dataset_version_id`` directly.
        """

        normalized_symbol = str(symbol).strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol must not be blank")
        if not isinstance(as_of_date, date):
            raise TypeError("as_of_date must be a date")
        normalized_status = None
        if source_status is not None:
            normalized_status = (
                source_status.value
                if isinstance(source_status, DatasetSourceStatus)
                else str(source_status)
            )
        with self._read_connection() as connection:
            if normalized_status is None:
                rows = connection.execute(
                    "SELECT dataset_version_id FROM research_dataset_versions "
                    "WHERE symbol = ? AND as_of_date = ?",
                    (normalized_symbol, as_of_date.isoformat()),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT dataset_version_id FROM research_dataset_versions "
                    "WHERE symbol = ? AND as_of_date = ? AND source_status = ?",
                    (
                        normalized_symbol,
                        as_of_date.isoformat(),
                        normalized_status,
                    ),
                ).fetchall()
            return tuple(
                self._reconstruct(connection, row["dataset_version_id"])
                for row in rows
            )

    def replay(self, dataset_version_id: str) -> MixedDatasetVersion:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT dataset_version_id FROM research_dataset_versions "
                "WHERE dataset_version_id = ?",
                (dataset_version_id,),
            ).fetchone()
            if row is None:
                raise DatasetVersionNotFoundError(dataset_version_id)
            return self._reconstruct(connection, dataset_version_id)

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        uri = self.database_path.resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA query_only = ON")
        try:
            DatasetVersionMigrationRunner._require_v12(connection)
            DatasetVersionMigrationRunner._verify_schema(connection)
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _insert_version(
        connection: sqlite3.Connection,
        version: MixedDatasetVersion,
        created_at: str,
    ) -> None:
        identity = version.identity
        coverage = identity.coverage
        connection.execute(
            """
            INSERT INTO research_dataset_versions (
                dataset_version_id, symbol, as_of_date, contract_version,
                persistence_contract_version, methodology_version, source_policy,
                source_status, authority_status, reconciliation_status,
                coverage_basis, required_observation_count, twse_observation_count,
                esun_supplemental_count, missing_twse_count,
                selected_observation_count, discrepancy_count, coverage_complete,
                latest_reconciled_date, provenance_map_sha256,
                parent_dataset_version_id, canonical_sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity.dataset_version_id,
                identity.symbol,
                identity.as_of_date.isoformat(),
                identity.contract_version,
                DATASET_PERSISTENCE_CONTRACT_VERSION,
                identity.methodology_version,
                identity.source_policy,
                identity.source_status.value,
                identity.authority_status.value,
                identity.reconciliation_status.value,
                version.coverage_basis.value,
                coverage.required_observation_count,
                coverage.twse_observation_count,
                coverage.esun_supplemental_count,
                coverage.missing_twse_count,
                coverage.selected_observation_count,
                coverage.discrepancy_count,
                int(bool(coverage.coverage_complete)),
                (
                    coverage.latest_reconciled_date.isoformat()
                    if coverage.latest_reconciled_date is not None
                    else None
                ),
                identity.provenance_map_sha256,
                identity.parent_dataset_version_id,
                version.canonical_sha256,
                created_at,
            ),
        )

    @staticmethod
    def _insert_observations(
        connection: sqlite3.Connection, version: MixedDatasetVersion
    ) -> None:
        rows = sorted(
            version.observations,
            key=lambda item: (
                item.trade_date,
                item.provider,
                item.source_role.value,
                item.source_run_id,
                item.observation_sha256,
            ),
        )
        connection.executemany(
            """
            INSERT INTO research_dataset_observations (
                dataset_version_id, symbol, trade_date, provider, source_role,
                source_run_id, selected, open_price, high_price, low_price,
                close_price, volume, observation_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    version.identity.dataset_version_id,
                    item.symbol,
                    item.trade_date.isoformat(),
                    item.provider,
                    item.source_role.value,
                    item.source_run_id,
                    int(item.selected),
                    item.open,
                    item.high,
                    item.low,
                    item.close,
                    item.volume,
                    item.observation_sha256,
                )
                for item in rows
            ],
        )

    @staticmethod
    def _insert_artifacts(
        connection: sqlite3.Connection, version: MixedDatasetVersion
    ) -> None:
        connection.executemany(
            """
            INSERT INTO research_dataset_artifacts (
                dataset_version_id, ordinal, provider, dataset, source_ref,
                contract_version, payload_sha256, payload_size_bytes, hash_basis
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    version.identity.dataset_version_id,
                    item.ordinal,
                    item.provider,
                    item.dataset,
                    item.source_ref,
                    item.contract_version,
                    item.payload_sha256,
                    item.payload_size_bytes,
                    item.hash_basis,
                )
                for item in version.artifacts
            ],
        )

    @staticmethod
    def _reconstruct(
        connection: sqlite3.Connection, dataset_version_id: str
    ) -> MixedDatasetVersion:
        row = connection.execute(
            "SELECT * FROM research_dataset_versions WHERE dataset_version_id = ?",
            (dataset_version_id,),
        ).fetchone()
        if row is None:
            raise DatasetVersionNotFoundError(dataset_version_id)
        try:
            if row["persistence_contract_version"] != DATASET_PERSISTENCE_CONTRACT_VERSION:
                raise DatasetPersistenceIntegrityError(
                    "persisted dataset uses an unknown persistence contract"
                )
            observation_rows = connection.execute(
                """
                SELECT symbol, trade_date, provider, source_role, source_run_id,
                       selected, open_price, high_price, low_price, close_price,
                       volume, observation_sha256
                FROM research_dataset_observations
                WHERE dataset_version_id = ?
                ORDER BY trade_date, provider, source_role, source_run_id,
                         observation_sha256
                """,
                (dataset_version_id,),
            ).fetchall()
            observations = tuple(
                DatasetObservation(
                    symbol=item["symbol"],
                    trade_date=date.fromisoformat(item["trade_date"]),
                    provider=item["provider"],
                    source_role=SourceRole(item["source_role"]),
                    source_run_id=item["source_run_id"],
                    selected=bool(item["selected"]),
                    open=item["open_price"],
                    high=item["high_price"],
                    low=item["low_price"],
                    close=item["close_price"],
                    volume=item["volume"],
                    observation_sha256=item["observation_sha256"],
                )
                for item in observation_rows
            )
            artifact_rows = connection.execute(
                """
                SELECT ordinal, provider, dataset, source_ref, contract_version,
                       payload_sha256, payload_size_bytes, hash_basis
                FROM research_dataset_artifacts
                WHERE dataset_version_id = ?
                ORDER BY ordinal
                """,
                (dataset_version_id,),
            ).fetchall()
            artifacts = tuple(
                DatasetArtifactRef(
                    ordinal=item["ordinal"],
                    provider=item["provider"],
                    dataset=item["dataset"],
                    source_ref=item["source_ref"],
                    contract_version=item["contract_version"],
                    payload_sha256=item["payload_sha256"],
                    payload_size_bytes=item["payload_size_bytes"],
                    hash_basis=item["hash_basis"],
                )
                for item in artifact_rows
            )
            coverage = DatasetCoverage(
                required_observation_count=row["required_observation_count"],
                twse_observation_count=row["twse_observation_count"],
                esun_supplemental_count=row["esun_supplemental_count"],
                missing_twse_count=row["missing_twse_count"],
                discrepancy_count=row["discrepancy_count"],
                latest_reconciled_date=_parse_date(
                    row["latest_reconciled_date"], "latest_reconciled_date"
                ),
                selected_observation_count=row["selected_observation_count"],
                coverage_complete=bool(row["coverage_complete"]),
            )
            supplemental_sources = tuple(
                sorted(
                    {
                        item.provider
                        for item in observations
                        if item.source_role is SourceRole.SUPPLEMENTAL
                    }
                )
            )
            validation_sources = tuple(
                sorted(
                    {
                        item.provider
                        for item in observations
                        if item.source_role is SourceRole.VALIDATION
                    }
                )
            )
            summary = DatasetProvenanceSummary(
                canonical_authority=TWSE_CANONICAL_AUTHORITY,
                supplemental_sources=supplemental_sources,
                validation_sources=validation_sources,
                source_status=DatasetSourceStatus(row["source_status"]),
                authority_status=AuthorityStatus(row["authority_status"]),
                reconciliation_status=ReconciliationStatus(
                    row["reconciliation_status"]
                ),
                coverage=coverage,
                provenance_map_sha256=row["provenance_map_sha256"],
            )
            identity = DatasetVersionIdentity(
                symbol=row["symbol"],
                as_of_date=date.fromisoformat(row["as_of_date"]),
                methodology_version=row["methodology_version"],
                source_policy=row["source_policy"],
                provenance_map_sha256=row["provenance_map_sha256"],
                source_status=summary.source_status,
                authority_status=summary.authority_status,
                reconciliation_status=summary.reconciliation_status,
                coverage=coverage,
                parent_dataset_version_id=row["parent_dataset_version_id"],
                contract_version=row["contract_version"],
            )
            if identity.dataset_version_id != row["dataset_version_id"]:
                raise DatasetPersistenceIntegrityError(
                    "persisted dataset_version_id does not match deterministic identity"
                )
            version = MixedDatasetVersion(
                identity=identity,
                provenance_summary=summary,
                observations=observations,
                artifacts=artifacts,
                coverage_basis=CoverageBasis(row["coverage_basis"]),
                canonical_sha256=row["canonical_sha256"],
            )
            if version.canonical_sha256 != row["canonical_sha256"]:
                raise DatasetPersistenceIntegrityError(
                    "persisted canonical_sha256 does not match reconstructed dataset"
                )
            return version
        except DatasetPersistenceIntegrityError:
            raise
        except Exception as exc:
            raise DatasetPersistenceIntegrityError(
                f"dataset version {dataset_version_id} failed closed during replay"
            ) from exc


__all__ = [
    "DatasetMigrationResult",
    "DatasetMigrationStateError",
    "DatasetPersistenceIntegrityError",
    "DatasetVersionMigrationRunner",
    "DatasetVersionNotFoundError",
    "DatasetVersionPersistenceResult",
    "DatasetVersionRepository",
    "MIGRATION_NAME",
    "MIGRATION_VERSION",
]
