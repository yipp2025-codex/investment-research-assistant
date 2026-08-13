"""Safe, isolated production deployment readiness primitives for S6C.

The functions in this module deliberately separate inspection and rehearsal
from production activation.  A production database is opened read-only for
preflight and is never migrated by these helpers.  All migration operations
require an explicit isolated destination copy.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Callable, Mapping, Sequence
from urllib.parse import quote

from app.storage import SQLiteResearchRepository
from app.storage.screener_migration import SQLiteScreenerMigrationRunner


DEPLOYMENT_CONTRACT_VERSION = "s6c-deployment-readiness-v1"
EXPECTED_SOURCE_SCHEMA_VERSION = 9
EXPECTED_SCHEMA_VERSION = 11

_EXPECTED_MIGRATION_NAMES = {
    1: "initial research schema",
    2: "pipeline runs and research-note linkage",
    3: "pipeline run source provenance",
    4: "historical sync checkpoints and research linkage",
    5: "market data source cross validation",
    6: "historical source observations and cross validation",
    7: "phase 6a daily batch runner",
    8: "phase 6b daily research reports",
    9: "phase 6c scheduler operations",
    10: "provider response source artifacts",
    11: "market screener persistence",
}


class DeploymentReadinessError(RuntimeError):
    """A readiness check failed closed."""


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def build_config_fingerprint(value: Mapping[str, object]) -> str:
    """Hash deterministic configuration only; timestamps must be excluded."""

    return hashlib.sha256(_canonical_json(dict(value)).encode("utf-8")).hexdigest()


def sha256_file(path: str | Path) -> str:
    target = _absolute_path(path, "path")
    digest = hashlib.sha256()
    with target.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class DatabasePreflight:
    path: Path
    sha256: str
    size_bytes: int
    schema_version: int
    migration_records: tuple[dict[str, object], ...]
    migration_file_hashes: dict[str, str]
    row_counts: dict[str, int]
    table_digests: dict[str, str]
    schema_objects: tuple[dict[str, object], ...]
    integrity_status: str
    foreign_key_errors: tuple[dict[str, object], ...]
    journal_mode: str
    sqlite_version: str

    @property
    def ok(self) -> bool:
        return self.integrity_status == "ok" and not self.foreign_key_errors

    def as_dict(self) -> dict[str, object]:
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "schema_version": self.schema_version,
            "migration_records": [dict(item) for item in self.migration_records],
            "migration_file_hashes": dict(self.migration_file_hashes),
            "row_counts": dict(self.row_counts),
            "table_digests": dict(self.table_digests),
            "schema_objects": [dict(item) for item in self.schema_objects],
            "integrity_status": self.integrity_status,
            "foreign_key_errors": [dict(item) for item in self.foreign_key_errors],
            "journal_mode": self.journal_mode,
            "sqlite_version": self.sqlite_version,
            "ok": self.ok,
        }


@dataclass(frozen=True, slots=True)
class BackupVerification:
    source: DatabasePreflight
    backup: DatabasePreflight
    source_sha256_before: str
    source_sha256_after: str
    source_size_bytes: int
    backup_size_bytes: int
    verified: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source.as_dict(),
            "backup": self.backup.as_dict(),
            "source_sha256_before": self.source_sha256_before,
            "source_sha256_after": self.source_sha256_after,
            "source_size_bytes": self.source_size_bytes,
            "backup_size_bytes": self.backup_size_bytes,
            "verified": self.verified,
        }


@dataclass(frozen=True, slots=True)
class MigrationRehearsal:
    source_v9: DatabasePreflight
    copied_v9: DatabasePreflight
    migrated_v11: DatabasePreflight
    second_invocation: DatabasePreflight
    preserved_table_digests: bool
    preserved_schema_objects: bool
    preserved_migration_records: bool
    strict_no_op: bool
    added_schema_objects: tuple[dict[str, object], ...]

    @property
    def verified(self) -> bool:
        return all(
            (
                self.migrated_v11.schema_version == EXPECTED_SCHEMA_VERSION,
                self.migrated_v11.ok,
                self.preserved_table_digests,
                self.preserved_schema_objects,
                self.preserved_migration_records,
                self.strict_no_op,
            )
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "source_v9": self.source_v9.as_dict(),
            "copied_v9": self.copied_v9.as_dict(),
            "migrated_v11": self.migrated_v11.as_dict(),
            "second_invocation": self.second_invocation.as_dict(),
            "preserved_table_digests": self.preserved_table_digests,
            "preserved_schema_objects": self.preserved_schema_objects,
            "preserved_migration_records": self.preserved_migration_records,
            "strict_no_op": self.strict_no_op,
            "added_schema_objects": [dict(item) for item in self.added_schema_objects],
            "verified": self.verified,
        }


@dataclass(frozen=True, slots=True)
class RuntimePreflight:
    project_root: Path
    python_executable: Path
    python_version: str | None
    python_probe_ok: bool
    required_imports: dict[str, str]
    sqlite_available: bool
    database_path: Path
    database_access: str
    report_directory: Path
    report_directory_writable: bool
    lock_directory: Path
    lock_directory_writable: bool
    environment_status: dict[str, str]
    errors: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def as_dict(self) -> dict[str, object]:
        return {
            "project_root": str(self.project_root),
            "python_executable": str(self.python_executable),
            "python_version": self.python_version,
            "python_probe_ok": self.python_probe_ok,
            "required_imports": dict(self.required_imports),
            "sqlite_available": self.sqlite_available,
            "database_path": str(self.database_path),
            "database_access": self.database_access,
            "report_directory": str(self.report_directory),
            "report_directory_writable": self.report_directory_writable,
            "lock_directory": str(self.lock_directory),
            "lock_directory_writable": self.lock_directory_writable,
            "environment_status": dict(self.environment_status),
            "errors": list(self.errors),
            "ok": self.ok,
        }


def collect_database_preflight(
    database_path: str | Path,
    *,
    migration_directory: str | Path | None = None,
) -> DatabasePreflight:
    """Read one SQLite file through ``mode=ro`` and collect integrity evidence."""

    path = _absolute_path(database_path, "database_path")
    if not path.is_file():
        raise DeploymentReadinessError(f"database does not exist: {path}")
    sha_before = sha256_file(path)
    size_before = path.stat().st_size
    connection = _open_read_only(path)
    try:
        tables = _list_tables(connection)
        if "schema_migrations" not in tables:
            raise DeploymentReadinessError("schema_migrations table is missing")
        migrations = tuple(
            {
                "version": int(row["version"]),
                "name": str(row["name"]),
                **(
                    {"applied_at": row["applied_at"]}
                    if "applied_at" in row.keys()
                    else {}
                ),
            }
            for row in connection.execute(
                "SELECT * FROM schema_migrations ORDER BY version"
            ).fetchall()
        )
        schema_version = max((int(item["version"]) for item in migrations), default=0)
        integrity_status = str(
            connection.execute("PRAGMA integrity_check").fetchone()[0]
        )
        foreign_key_errors = tuple(
            _row_to_dict(row, ("table", "rowid", "parent", "fkid"))
            for row in connection.execute("PRAGMA foreign_key_check").fetchall()
        )
        row_counts = {
            table: int(
                connection.execute(f"SELECT COUNT(*) FROM {_quote_identifier(table)}")
                .fetchone()[0]
            )
            for table in tables
        }
        table_digests = {
            table: _table_digest(connection, table) for table in tables
        }
        schema_objects = tuple(
            {
                "type": str(row["type"]),
                "name": str(row["name"]),
                "sql": row["sql"],
            }
            for row in connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' "
                "ORDER BY type, name"
            ).fetchall()
        )
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
    finally:
        connection.close()

    sha_after = sha256_file(path)
    size_after = path.stat().st_size
    if sha_before != sha_after or size_before != size_after:
        raise DeploymentReadinessError(
            "database changed during read-only preflight"
        )
    migration_root = (
        _absolute_path(migration_directory, "migration_directory")
        if migration_directory is not None
        else Path(__file__).parents[1] / "storage" / "migrations"
    )
    migration_hashes = _migration_file_hashes(migration_root)
    result = DatabasePreflight(
        path=path,
        sha256=sha_after,
        size_bytes=size_after,
        schema_version=schema_version,
        migration_records=migrations,
        migration_file_hashes=migration_hashes,
        row_counts=row_counts,
        table_digests=table_digests,
        schema_objects=schema_objects,
        integrity_status=integrity_status,
        foreign_key_errors=foreign_key_errors,
        journal_mode=journal_mode,
        sqlite_version=sqlite3.sqlite_version,
    )
    if not result.ok:
        raise DeploymentReadinessError(
            f"database integrity preflight failed: {path}"
        )
    return result


def create_verified_backup(
    source_database: str | Path,
    backup_path: str | Path,
    *,
    expected_schema_version: int = EXPECTED_SOURCE_SCHEMA_VERSION,
    protect_read_only: bool = True,
) -> BackupVerification:
    """Create a SQLite-safe immutable-by-convention backup without overwriting."""

    source = _absolute_path(source_database, "source_database")
    backup = _absolute_path(backup_path, "backup_path")
    if source == backup:
        raise DeploymentReadinessError("backup path must differ from source")
    if backup.exists():
        raise DeploymentReadinessError(f"backup path already exists: {backup}")
    source_report = collect_database_preflight(source)
    if source_report.schema_version != expected_schema_version:
        raise DeploymentReadinessError(
            f"expected source schema {expected_schema_version}, "
            f"found {source_report.schema_version}"
        )
    backup.parent.mkdir(parents=True, exist_ok=True)
    source_connection = _open_read_only(source)
    destination = sqlite3.connect(backup)
    try:
        source_connection.backup(destination)
        destination.commit()
    except BaseException:
        destination.rollback()
        raise
    finally:
        destination.close()
        source_connection.close()

    backup_report = collect_database_preflight(backup)
    source_sha_after = sha256_file(source)
    if source_sha_after != source_report.sha256:
        raise DeploymentReadinessError("source database changed during backup")
    if backup_report.schema_version != source_report.schema_version:
        raise DeploymentReadinessError("backup schema version mismatch")
    if backup_report.row_counts != source_report.row_counts:
        raise DeploymentReadinessError("backup row counts differ from source")
    if backup_report.table_digests != source_report.table_digests:
        raise DeploymentReadinessError("backup table contents differ from source")
    if backup_report.schema_objects != source_report.schema_objects:
        raise DeploymentReadinessError("backup schema objects differ from source")
    if _migration_pairs(backup_report) != _migration_pairs(source_report):
        raise DeploymentReadinessError("backup migration records differ from source")
    if protect_read_only:
        # Protect the verified pre-v11 artifact against ordinary accidental
        # writes. Future activation must explicitly copy it to a new mutable
        # destination.
        try:
            backup.chmod(stat.S_IREAD)
        except OSError as error:
            raise DeploymentReadinessError(
                "verified backup could not be made read-only"
            ) from error
    return BackupVerification(
        source=source_report,
        backup=backup_report,
        source_sha256_before=source_report.sha256,
        source_sha256_after=source_sha_after,
        source_size_bytes=source_report.size_bytes,
        backup_size_bytes=backup_report.size_bytes,
        verified=True,
    )


def rehearse_v9_to_v11(
    source_v9_database: str | Path,
    destination_v11_database: str | Path,
    *,
    before_migration_step: Callable[[int, Path], None] | None = None,
) -> MigrationRehearsal:
    """Copy a v9 database and apply only the existing v10/v11 path."""

    source = _absolute_path(source_v9_database, "source_v9_database")
    destination = _absolute_path(
        destination_v11_database,
        "destination_v11_database",
    )
    if source == destination:
        raise DeploymentReadinessError("rehearsal destination must differ from source")
    source_report = collect_database_preflight(source)
    if source_report.schema_version != EXPECTED_SOURCE_SCHEMA_VERSION:
        raise DeploymentReadinessError(
            f"rehearsal requires schema {EXPECTED_SOURCE_SCHEMA_VERSION}, "
            f"found {source_report.schema_version}"
        )
    if destination.exists():
        raise DeploymentReadinessError(
            f"rehearsal destination already exists: {destination}"
        )
    _copy_verified_bytes(source, destination)
    copied_report = collect_database_preflight(destination)
    _validate_migration_records(copied_report, max_version=9)

    if before_migration_step is not None:
        before_migration_step(10, destination)
    SQLiteResearchRepository(destination).initialize()
    after_v10 = collect_database_preflight(destination)
    if after_v10.schema_version != 10:
        raise DeploymentReadinessError("existing v10 migration path did not complete")
    if before_migration_step is not None:
        before_migration_step(11, destination)
    SQLiteScreenerMigrationRunner(destination).migrate()
    migrated = collect_database_preflight(destination)
    if migrated.schema_version != EXPECTED_SCHEMA_VERSION:
        raise DeploymentReadinessError("migration rehearsal did not reach schema v11")

    _validate_migration_records(migrated, max_version=11)
    # ``schema_migrations`` is intentionally extended by the v10/v11 records;
    # its preservation is verified separately through the ordered prefix check.
    existing_tables = set(copied_report.row_counts) - {"schema_migrations"}
    preserved_table_digests = all(
        migrated.table_digests.get(table) == copied_report.table_digests[table]
        for table in existing_tables
    )
    source_schema = _schema_object_map(copied_report)
    migrated_schema = _schema_object_map(migrated)
    preserved_schema_objects = all(
        migrated_schema.get(key) == value for key, value in source_schema.items()
    )
    preserved_migration_records = _migration_pairs(migrated)[: len(
        _migration_pairs(copied_report)
    )] == _migration_pairs(copied_report)
    if not all(
        (preserved_table_digests, preserved_schema_objects, preserved_migration_records)
    ):
        raise DeploymentReadinessError("pre-existing database state drifted in rehearsal")

    migrated_sha = migrated.sha256
    SQLiteResearchRepository(destination).initialize()
    SQLiteScreenerMigrationRunner(destination).migrate()
    second_invocation = collect_database_preflight(destination)
    strict_no_op = (
        second_invocation.sha256 == migrated_sha
        and second_invocation.row_counts == migrated.row_counts
        and second_invocation.table_digests == migrated.table_digests
        and second_invocation.schema_objects == migrated.schema_objects
    )
    if not strict_no_op:
        raise DeploymentReadinessError(
            "second migration invocation changed the rehearsal database"
        )
    added_schema_objects = tuple(
        value
        for key, value in migrated_schema.items()
        if key not in source_schema
    )
    return MigrationRehearsal(
        source_v9=source_report,
        copied_v9=copied_report,
        migrated_v11=migrated,
        second_invocation=second_invocation,
        preserved_table_digests=preserved_table_digests,
        preserved_schema_objects=preserved_schema_objects,
        preserved_migration_records=preserved_migration_records,
        strict_no_op=strict_no_op,
        added_schema_objects=added_schema_objects,
    )


def restore_verified_backup(
    backup_database: str | Path,
    restore_path: str | Path,
) -> DatabasePreflight:
    """Atomically restore a verified backup into an isolated destination."""

    backup = _absolute_path(backup_database, "backup_database")
    restore = _absolute_path(restore_path, "restore_path")
    backup_report = collect_database_preflight(backup)
    temporary = restore.with_name(
        f".{restore.name}.restore-{uuid.uuid4().hex}.tmp"
    )
    _copy_verified_bytes(backup, temporary)
    try:
        os.replace(temporary, restore)
    finally:
        if temporary.exists():
            temporary.unlink()
    restored = collect_database_preflight(restore)
    if restored.sha256 != backup_report.sha256:
        raise DeploymentReadinessError("restored database SHA differs from backup")
    if restored.size_bytes != backup_report.size_bytes:
        raise DeploymentReadinessError("restored database size differs from backup")
    if restored.schema_version != backup_report.schema_version:
        raise DeploymentReadinessError("restored database schema differs from backup")
    return restored


def verify_migration_file_hashes(
    expected_hashes: Mapping[str, str],
    *,
    migration_directory: str | Path | None = None,
) -> dict[str, str]:
    """Verify an explicit migration-file hash map without changing any file."""

    root = (
        _absolute_path(migration_directory, "migration_directory")
        if migration_directory is not None
        else Path(__file__).parents[1] / "storage" / "migrations"
    )
    actual = _migration_file_hashes(root)
    for name, expected in expected_hashes.items():
        if actual.get(name) != expected:
            raise DeploymentReadinessError(
                f"migration file hash mismatch for {name}"
            )
    return actual


def run_runtime_preflight(
    *,
    project_root: str | Path,
    python_executable: str | Path,
    database_path: str | Path,
    report_directory: str | Path,
    lock_directory: str | Path,
    required_imports: Sequence[str] = (
        "app.daily_runner",
        "app.reporting.screener_report",
        "app.windows_scheduler_adapter",
        "app.storage.screener_migration",
    ),
    required_environment_names: Sequence[str] = (),
) -> RuntimePreflight:
    """Check non-secret runtime dependencies and isolated writable paths."""

    root = _absolute_path(project_root, "project_root")
    python_path = _absolute_path(python_executable, "python_executable")
    database = _absolute_path(database_path, "database_path")
    report = _absolute_path(report_directory, "report_directory")
    lock = _absolute_path(lock_directory, "lock_directory")
    errors: list[str] = []
    if not root.is_dir():
        errors.append("project_root_missing")
    if not python_path.is_file():
        errors.append("python_executable_missing")

    import_status: dict[str, str] = {}
    for module_name in required_imports:
        try:
            importlib.import_module(module_name)
        except Exception as error:
            import_status[module_name] = type(error).__name__
            errors.append(f"import_failed:{module_name}")
        else:
            import_status[module_name] = "ok"

    sqlite_available = sqlite3.sqlite_version_info > (0,)
    if not sqlite_available:
        errors.append("sqlite_unavailable")

    python_version: str | None = None
    python_probe_ok = False
    if python_path.is_file():
        probe = subprocess.run(
            [
                str(python_path),
                "-c",
                "import json,sqlite3,sys; print(json.dumps({'python':sys.version,'sqlite':sqlite3.sqlite_version}))",
            ],
            cwd=root if root.is_dir() else None,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if probe.returncode == 0:
            try:
                payload = json.loads(probe.stdout.strip())
                python_version = str(payload["python"])
                python_probe_ok = bool(payload["sqlite"])
            except (ValueError, KeyError, TypeError):
                errors.append("python_probe_invalid")
        else:
            errors.append("python_probe_failed")

    report_writable = _probe_directory(report)
    if not report_writable:
        errors.append("report_directory_not_writable")
    lock_writable = _probe_directory(lock)
    if not lock_writable:
        errors.append("lock_directory_not_writable")

    database_access = "missing"
    if database.is_file():
        try:
            connection = sqlite3.connect(
                f"file:{quote(str(database).replace('\\', '/'), safe='/:')}?mode=rw",
                uri=True,
            )
            connection.execute("SELECT 1").fetchone()
            connection.close()
            database_access = "read_write_openable"
        except sqlite3.Error:
            database_access = "read_write_failed"
            errors.append("database_read_write_failed")
    else:
        errors.append("database_missing")

    environment_status = {
        name: "configured" if os.environ.get(name) else "missing"
        for name in required_environment_names
    }
    errors.extend(
        f"environment_missing:{name}"
        for name, status in environment_status.items()
        if status == "missing"
    )
    return RuntimePreflight(
        project_root=root,
        python_executable=python_path,
        python_version=python_version,
        python_probe_ok=python_probe_ok,
        required_imports=import_status,
        sqlite_available=sqlite_available,
        database_path=database,
        database_access=database_access,
        report_directory=report,
        report_directory_writable=report_writable,
        lock_directory=lock,
        lock_directory_writable=lock_writable,
        environment_status=environment_status,
        errors=tuple(errors),
    )


def build_readiness_manifest(
    *,
    production_preflight: DatabasePreflight,
    backup_verification: BackupVerification,
    rehearsal: MigrationRehearsal,
    runtime_preflight: RuntimePreflight,
    runtime_evidence: Mapping[str, object],
    scheduler_evidence: Mapping[str, object],
    regression_evidence: Mapping[str, object],
    report_directory: str | Path,
    lock_directory: str | Path,
    python_executable: str | Path,
    production_schedule_time: str | None,
    generated_at: str | None = None,
) -> dict[str, object]:
    """Build a non-secret readiness manifest with a stable config fingerprint."""

    configuration = {
        "deployment_contract_version": DEPLOYMENT_CONTRACT_VERSION,
        "expected_schema_version": EXPECTED_SCHEMA_VERSION,
        "production_db_path": str(production_preflight.path),
        "report_directory": str(_absolute_path(report_directory, "report_directory")),
        "lock_directory": str(_absolute_path(lock_directory, "lock_directory")),
        "python_executable": str(_absolute_path(python_executable, "python_executable")),
        "migration_file_hashes": dict(rehearsal.migrated_v11.migration_file_hashes),
        "scheduler_configuration": dict(scheduler_evidence.get("configuration", {})),
        "production_schedule_time": production_schedule_time,
        "s5_factory_locator": scheduler_evidence.get("configuration", {}).get(
            "s5_factory_locator"
        ),
        "latest_date_factory_locator": scheduler_evidence.get(
            "configuration", {}
        ).get("latest_date_factory_locator"),
        "universe_snapshot_path": scheduler_evidence.get("configuration", {}).get(
            "universe_snapshot_path"
        ),
    }
    checklist = {
        "s6a_s6b_frozen_checkpoint": True,
        "production_db_preflight": production_preflight.ok
        and production_preflight.schema_version == EXPECTED_SOURCE_SCHEMA_VERSION,
        "verified_backup_exists": backup_verification.verified,
        "v9_to_v11_rehearsal": rehearsal.verified,
        "rollback_rehearsal": bool(runtime_evidence.get("rollback_verified", False)),
        "runtime_preflight": runtime_preflight.ok,
        "production_like_daily_runner": runtime_evidence.get("first_status") == "success",
        "second_execution_replay": runtime_evidence.get("second_status") == "success_replay",
        "scheduler_validation": bool(scheduler_evidence.get("validated", False)),
        "production_composition_locators": bool(
            scheduler_evidence.get("production_composition_locators_validated", False)
        ),
        "production_schedule_time_selected": production_schedule_time is not None,
        "final_production_migration_approved": False,
        "scheduled_task_registration_approved": False,
    }
    correctness_keys = (
        "s6a_s6b_frozen_checkpoint",
        "production_db_preflight",
        "verified_backup_exists",
        "v9_to_v11_rehearsal",
        "rollback_rehearsal",
        "runtime_preflight",
        "production_like_daily_runner",
        "second_execution_replay",
        "scheduler_validation",
        "production_composition_locators",
    )
    readiness_status = (
        "READY"
        if all(bool(checklist[key]) for key in correctness_keys)
        else "NOT_READY"
    )
    return {
        "deployment_contract_version": DEPLOYMENT_CONTRACT_VERSION,
        "expected_schema_version": EXPECTED_SCHEMA_VERSION,
        "production_db_path": str(production_preflight.path),
        "production_preflight_sha256": production_preflight.sha256,
        "rehearsal_source_sha256": rehearsal.copied_v9.sha256,
        "rehearsal_migrated_db_sha256": rehearsal.migrated_v11.sha256,
        "migration_versions": [
            int(item["version"]) for item in rehearsal.migrated_v11.migration_records
        ],
        "migration_file_hashes": dict(rehearsal.migrated_v11.migration_file_hashes),
        "python_version": runtime_preflight.python_version,
        "validated_runner_command": runtime_evidence.get("runner_command"),
        "report_directory": str(_absolute_path(report_directory, "report_directory")),
        "lock_directory": str(_absolute_path(lock_directory, "lock_directory")),
        "scheduler_adapter_hash": scheduler_evidence.get("adapter_hash"),
        "scheduler_configuration_fingerprint": scheduler_evidence.get(
            "configuration_fingerprint"
        ),
        "s5_factory_locator": scheduler_evidence.get("configuration", {}).get(
            "s5_factory_locator"
        ),
        "latest_date_factory_locator": scheduler_evidence.get(
            "configuration", {}
        ).get("latest_date_factory_locator"),
        "authoritative_latest_date_source": "twse-openapi:STOCK_DAY_ALL+BWIBBU_ALL",
        "latest_date_live_smoke": runtime_evidence.get("latest_date_live_smoke"),
        "production_composition_parity": scheduler_evidence.get(
            "configuration", {}
        ).get("production_composition_parity"),
        "regression": dict(regression_evidence),
        "configuration": configuration,
        "configuration_fingerprint": build_config_fingerprint(configuration),
        "production_preflight": production_preflight.as_dict(),
        "backup_verification": backup_verification.as_dict(),
        "rehearsal": rehearsal.as_dict(),
        "runtime_preflight": runtime_preflight.as_dict(),
        "runtime_evidence": dict(runtime_evidence),
        "scheduler_evidence": dict(scheduler_evidence),
        "readiness_checklist": checklist,
        "readiness_status": readiness_status,
        "activation_status": "NOT_ACTIVATED",
        "generated_at": generated_at
        or datetime.now(timezone.utc).isoformat(),
    }


def _open_read_only(path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(path).replace('\\', '/'), safe='/:')}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
    except sqlite3.Error as error:
        raise DeploymentReadinessError(f"could not open database read-only: {path}") from error


def _absolute_path(value: str | Path, field_name: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise DeploymentReadinessError(f"{field_name} must be absolute")
    return path.resolve()


def _list_tables(connection: sqlite3.Connection) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    )


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _table_digest(connection: sqlite3.Connection, table: str) -> str:
    quoted = _quote_identifier(table)
    columns = connection.execute(f"PRAGMA table_info({quoted})").fetchall()
    column_names = [str(row["name"]) for row in columns]
    primary_keys = [str(row["name"]) for row in sorted(columns, key=lambda row: row["pk"]) if row["pk"]]
    order_columns = primary_keys or column_names
    order_sql = ""
    if order_columns:
        order_sql = " ORDER BY " + ", ".join(_quote_identifier(item) for item in order_columns)
    rows = connection.execute(f"SELECT * FROM {quoted}{order_sql}").fetchall()
    payload = {
        "table": table,
        "columns": column_names,
        "rows": [[_json_value(value) for value in row] for row in rows],
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _json_value(value: object) -> object:
    if isinstance(value, bytes):
        return {"__bytes_hex__": value.hex()}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _row_to_dict(row: sqlite3.Row, names: Sequence[str]) -> dict[str, object]:
    return {name: row[index] for index, name in enumerate(names)}


def _migration_file_hashes(root: Path) -> dict[str, str]:
    if not root.is_dir():
        raise DeploymentReadinessError(f"migration directory is missing: {root}")
    files = sorted(root.glob("*.sql"))
    return {file.name: sha256_file(file) for file in files}


def _migration_pairs(report: DatabasePreflight) -> list[tuple[int, str]]:
    return [
        (int(item["version"]), str(item["name"]))
        for item in report.migration_records
    ]


def _validate_migration_records(
    report: DatabasePreflight,
    *,
    max_version: int,
) -> None:
    pairs = _migration_pairs(report)
    expected = [
        (version, _EXPECTED_MIGRATION_NAMES[version])
        for version in range(1, max_version + 1)
    ]
    if pairs != expected:
        raise DeploymentReadinessError(
            f"migration records mismatch: expected {expected}, found {pairs}"
        )


def _schema_object_map(
    report: DatabasePreflight,
) -> dict[tuple[str, str], dict[str, object]]:
    return {
        (str(item["type"]), str(item["name"])): dict(item)
        for item in report.schema_objects
    }


def _copy_verified_bytes(source: Path, destination: Path) -> None:
    if destination.exists():
        raise DeploymentReadinessError(f"destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    source_size = source.stat().st_size
    destination_size = destination.stat().st_size
    if source_size != destination_size:
        raise DeploymentReadinessError("copied file size differs from source")
    if sha256_file(source) != sha256_file(destination):
        raise DeploymentReadinessError("copied file SHA differs from source")


def _probe_directory(directory: Path) -> bool:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / f".s6c-write-probe-{uuid.uuid4().hex}"
        probe.write_bytes(b"s6c-write-probe")
        probe.unlink()
        return True
    except OSError:
        return False


__all__ = [
    "BackupVerification",
    "DEPLOYMENT_CONTRACT_VERSION",
    "DatabasePreflight",
    "DeploymentReadinessError",
    "EXPECTED_SCHEMA_VERSION",
    "EXPECTED_SOURCE_SCHEMA_VERSION",
    "MigrationRehearsal",
    "RuntimePreflight",
    "build_config_fingerprint",
    "build_readiness_manifest",
    "collect_database_preflight",
    "create_verified_backup",
    "rehearse_v9_to_v11",
    "restore_verified_backup",
    "run_runtime_preflight",
    "sha256_file",
    "verify_migration_file_hashes",
]
