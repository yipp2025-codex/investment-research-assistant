from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from app.storage import SQLiteResearchRepository
from app.storage.screener_migration import (
    ScreenerMigrationStateError,
    SQLiteScreenerMigrationRunner,
)


UNIVERSE_RUN_ID = "a" * 64
SCREENER_RUN_ID = "b" * 64
CANDIDATE_ID = "c" * 64
INPUT_HASH = "d" * 64
STAGE1_HASH = "e" * 64
PAYLOAD_HASH = "f" * 64
SNAPSHOT_HASH = "1" * 64
ARTIFACT_HASH = "2" * 64

NEW_TABLES = {
    "market_universe_runs",
    "market_universe_members",
    "screener_runs",
    "screener_candidates",
    "candidate_reasons",
    "candidate_metrics",
    "screener_source_artifacts",
}


def _initialize_v10(database_path: Path) -> None:
    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    assert repository.get_schema_version() == 10


def _connect(database_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _insert_universe_run(
    connection: sqlite3.Connection,
    *,
    status: str = "running",
    market_date: str = "2026-08-07",
) -> None:
    canonical_hash = PAYLOAD_HASH if status == "success" else None
    finished_at = "2026-08-07T10:00:00+00:00" if status == "success" else None
    connection.execute(
        "INSERT INTO market_universe_runs ("
        "universe_run_id, market_date, methodology_version, source_policy, "
        "input_evidence_sha256, universe_count, scan_eligible_count, "
        "scan_unavailable_count, excluded_count, inactive_count, unresolved_count, "
        "status, canonical_sha256, finished_at"
        ") VALUES (?, ?, ?, 'twse_baseline', ?, 1, 1, 0, 0, 0, 0, ?, ?, ?)",
        (
            UNIVERSE_RUN_ID,
            market_date,
            "twse-universe-v1",
            INPUT_HASH,
            status,
            canonical_hash,
            finished_at,
        ),
    )


def _insert_universe_member(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO market_universe_members ("
        "universe_run_id, symbol, name, market, status, listing_date, "
        "delisting_date, exclusion_reason"
        ") VALUES (?, '2330', '台積電', 'TWSE', 'active_scan_eligible', "
        "'1994-09-05', NULL, NULL)",
        (UNIVERSE_RUN_ID,),
    )


def _insert_screener_run(
    connection: sqlite3.Connection,
    *,
    status: str = "running",
    market_date: str = "2026-08-07",
) -> None:
    canonical_hash = PAYLOAD_HASH if status == "success" else None
    finished_at = "2026-08-07T11:00:00+00:00" if status == "success" else None
    connection.execute(
        "INSERT INTO screener_runs ("
        "screener_run_id, market_date, universe_run_id, "
        "stage1_methodology_version, stage2_methodology_version, source_policy, "
        "candidate_limit, input_manifest_sha256, stage1_canonical_sha256, "
        "universe_count, screened_count, triggered_count, candidate_count, "
        "truncated, status, canonical_sha256, finished_at"
        ") VALUES (?, ?, ?, 'screener-stage1-v1', 'screener-stage2-v1', "
        "'twse_baseline', 30, ?, ?, 1, 1, 1, 1, 0, ?, ?, ?)",
        (
            SCREENER_RUN_ID,
            market_date,
            UNIVERSE_RUN_ID,
            INPUT_HASH,
            STAGE1_HASH,
            status,
            canonical_hash,
            finished_at,
        ),
    )


def _insert_candidate(
    connection: sqlite3.Connection,
    *,
    status: str = "running",
) -> None:
    completed = status in {"success", "failed"}
    failed = status == "failed"
    connection.execute(
        "INSERT INTO screener_candidates ("
        "candidate_id, screener_run_id, universe_run_id, symbol, status, rank, "
        "stage1_rank, candidate_kind, stage1_trigger_count, stage1_reason_count, "
        "stage2_reason_count, metric_count, data_quality_status, validation_status, "
        "analysis_status, canonical_sources_json, validation_sources_json, "
        "discrepancies_json, input_locator_sha256, snapshot_sha256, payload_sha256, "
        "failure_code, failure_type"
        ") VALUES (?, ?, ?, '2330', ?, 1, 1, ?, 1, 1, 0, 0, ?, ?, ?, "
        "'[\"twse\"]', '[\"esun\",\"twse\"]', '[]', ?, ?, ?, ?, ?)",
        (
            CANDIDATE_ID,
            SCREENER_RUN_ID,
            UNIVERSE_RUN_ID,
            status,
            "data_quality_candidate" if failed else (
                "research_candidate" if completed else None
            ),
            "failed" if failed else ("clean" if completed else None),
            "missing_source" if failed else (
                "available" if completed else None
            ),
            "failed" if failed else ("available" if completed else None),
            INPUT_HASH,
            None if failed else (SNAPSHOT_HASH if completed else None),
            PAYLOAD_HASH if completed else None,
            "candidate_research_failed" if failed else None,
            "RuntimeError" if failed else None,
        ),
    )


def _schema_snapshot(connection: sqlite3.Connection) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(row)
        for row in connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        )
    )


def test_fresh_database_migrates_from_frozen_v10_to_v11(tmp_path: Path) -> None:
    database_path = tmp_path / "fresh-v11.db"
    _initialize_v10(database_path)

    SQLiteScreenerMigrationRunner(database_path).migrate()

    with _connect(database_path) as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        migration = connection.execute(
            "SELECT name FROM schema_migrations WHERE version = 11"
        ).fetchone()
        version = connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0]
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert NEW_TABLES <= tables
    assert migration["name"] == "market screener persistence"
    assert version == 11
    assert integrity == "ok"
    assert foreign_keys == []


def test_existing_v10_rows_and_table_definitions_survive_v11(tmp_path: Path) -> None:
    database_path = tmp_path / "existing-v10.db"
    _initialize_v10(database_path)
    with _connect(database_path) as connection:
        connection.execute(
            "INSERT INTO symbols (symbol, name, market, currency, is_active) "
            "VALUES ('OLD1', 'Existing Company', 'MOCK', 'TWD', 1)"
        )
        connection.execute(
            "INSERT INTO research_notes ("
            "symbol, created_at, analysis_type, title, summary, source_data_start, "
            "source_data_end, provider_source"
            ") VALUES ('OLD1', '2026-08-07T00:00:00+00:00', 'frozen', "
            "'Existing', 'Must survive', '2026-08-01', '2026-08-07', "
            "'mock-synthetic')"
        )
        connection.commit()
        old_table_sql = tuple(
            tuple(row)
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        )
        old_symbol = tuple(
            connection.execute(
                "SELECT * FROM symbols WHERE symbol = 'OLD1'"
            ).fetchone()
        )
        old_note = tuple(
            connection.execute(
                "SELECT * FROM research_notes WHERE symbol = 'OLD1'"
            ).fetchone()
        )

    SQLiteScreenerMigrationRunner(database_path).migrate()

    with _connect(database_path) as connection:
        retained_table_sql = tuple(
            row
            for row in (
                tuple(item)
                for item in connection.execute(
                    "SELECT name, sql FROM sqlite_master WHERE type = 'table' "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
            )
            if row[0] not in NEW_TABLES
        )
        symbol = tuple(
            connection.execute(
                "SELECT * FROM symbols WHERE symbol = 'OLD1'"
            ).fetchone()
        )
        note = tuple(
            connection.execute(
                "SELECT * FROM research_notes WHERE symbol = 'OLD1'"
            ).fetchone()
        )

    assert retained_table_sql == old_table_sql
    assert symbol == old_symbol
    assert note == old_note


def test_migration_replay_is_a_strict_no_op(tmp_path: Path) -> None:
    database_path = tmp_path / "replay.db"
    _initialize_v10(database_path)
    runner = SQLiteScreenerMigrationRunner(database_path)
    runner.migrate()
    with _connect(database_path) as connection:
        before = _schema_snapshot(connection)
        migration_count = connection.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = 11"
        ).fetchone()[0]

    runner.migrate()

    with _connect(database_path) as connection:
        after = _schema_snapshot(connection)
        replay_count = connection.execute(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = 11"
        ).fetchone()[0]
    assert before == after
    assert migration_count == replay_count == 1


def test_migration_failure_rolls_back_all_v11_objects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "rollback.db"
    _initialize_v10(database_path)
    runner = SQLiteScreenerMigrationRunner(database_path)
    execute_all = runner._execute_sql_statements

    def execute_then_fail(connection: sqlite3.Connection, script: str) -> None:
        execute_all(connection, script)
        raise sqlite3.OperationalError("injected migration failure")

    monkeypatch.setattr(runner, "_execute_sql_statements", execute_then_fail)
    with pytest.raises(sqlite3.OperationalError, match="injected migration failure"):
        runner.migrate()

    with _connect(database_path) as connection:
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        recorded = connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version = 11"
        ).fetchone()
    assert not (NEW_TABLES & tables)
    assert recorded is None


def test_recorded_v11_with_missing_object_fails_closed(tmp_path: Path) -> None:
    database_path = tmp_path / "drift.db"
    _initialize_v10(database_path)
    runner = SQLiteScreenerMigrationRunner(database_path)
    runner.migrate()
    with _connect(database_path) as connection:
        connection.execute("DROP TABLE candidate_metrics")
        connection.commit()

    with pytest.raises(ScreenerMigrationStateError, match="migration 11"):
        runner.migrate()


def test_composite_fk_rejects_screener_universe_market_date_mismatch(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "composite-fk.db"
    _initialize_v10(database_path)
    SQLiteScreenerMigrationRunner(database_path).migrate()

    with _connect(database_path) as connection:
        _insert_universe_run(connection, market_date="2026-08-07")
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            _insert_screener_run(connection, market_date="2026-08-08")


def test_success_run_and_candidate_statuses_are_terminal(tmp_path: Path) -> None:
    database_path = tmp_path / "success-terminal.db"
    _initialize_v10(database_path)
    SQLiteScreenerMigrationRunner(database_path).migrate()

    with _connect(database_path) as connection:
        _insert_universe_run(connection, status="success")
        _insert_universe_member(connection)
        _insert_screener_run(connection, status="success")
        _insert_candidate(connection, status="success")

        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE market_universe_runs SET status = 'running' "
                "WHERE universe_run_id = ?",
                (UNIVERSE_RUN_ID,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE screener_runs SET status = 'partial_success' "
                "WHERE screener_run_id = ?",
                (SCREENER_RUN_ID,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "UPDATE screener_candidates SET status = 'failed' "
                "WHERE candidate_id = ?",
                (CANDIDATE_ID,),
            )


def test_canonical_json_null_and_unavailable_sql_null_round_trip_fidelity(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "null-fidelity.db"
    _initialize_v10(database_path)
    SQLiteScreenerMigrationRunner(database_path).migrate()

    with _connect(database_path) as connection:
        _insert_universe_run(connection)
        _insert_universe_member(connection)
        _insert_screener_run(connection)
        _insert_candidate(connection)
        connection.execute(
            "INSERT INTO candidate_reasons ("
            "candidate_id, stage, ordinal, code, metric, component, previous_json, "
            "current_json, delta, unit, operator, threshold_json, rule_version, "
            "role, trigger_class, reason_kind, reason_class, threshold_multiple"
            ") VALUES (?, 'stage1', 1, 'volume_anomaly', 'volume_ratio_20d', NULL, "
            "'null', 'null', NULL, 'ratio', 'current_gte', '2.0', "
            "'screener-stage1-v1', 'primary', 'market_activity', NULL, NULL, 1.0)",
            (CANDIDATE_ID,),
        )
        connection.execute(
            "INSERT INTO candidate_metrics ("
            "candidate_id, ordinal, name, status, value, previous_value, delta, unit, "
            "as_of_date, previous_as_of_date, observations, previous_observations"
            ") VALUES (?, 1, 'return_60d', 'insufficient_history', NULL, NULL, NULL, "
            "'percent', '2026-08-07', NULL, 20, 19)",
            (CANDIDATE_ID,),
        )

        reason = connection.execute(
            "SELECT previous_json, typeof(previous_json) AS storage_type, "
            "previous_json IS NULL AS is_sql_null FROM candidate_reasons"
        ).fetchone()
        reconstructed = json.loads(reason["previous_json"])
        canonical = json.dumps(
            reconstructed,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        connection.execute(
            "UPDATE candidate_reasons SET previous_json = ? "
            "WHERE candidate_id = ? AND stage = 'stage1' AND ordinal = 1",
            (canonical, CANDIDATE_ID),
        )
        persisted = connection.execute(
            "SELECT previous_json FROM candidate_reasons"
        ).fetchone()[0]
        metric_value = connection.execute(
            "SELECT value FROM candidate_metrics"
        ).fetchone()[0]

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO candidate_metrics ("
                "candidate_id, ordinal, name, status, value, previous_value, delta, "
                "unit, as_of_date, previous_as_of_date, observations, "
                "previous_observations"
                ") VALUES (?, 2, 'return_120d', 'insufficient_history', 0, NULL, "
                "NULL, 'percent', '2026-08-07', NULL, 20, 19)",
                (CANDIDATE_ID,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO candidate_reasons ("
                "candidate_id, stage, ordinal, code, metric, component, "
                "previous_json, current_json, delta, unit, operator, threshold_json, "
                "rule_version, role, trigger_class, reason_kind, reason_class, "
                "threshold_multiple"
                ") VALUES (?, 'stage1', 2, 'return_20d_change', 'return_20d', NULL, "
                "NULL, 'null', NULL, 'percentage_point', 'abs_delta_gte', '1.0', "
                "'screener-stage1-v1', 'primary', 'return_change', NULL, NULL, 1.0)",
                (CANDIDATE_ID,),
            )

    assert reason["storage_type"] == "text"
    assert reason["is_sql_null"] == 0
    assert reconstructed is None
    assert canonical == persisted == "null"
    assert metric_value is None
    assert json.dumps({"value": metric_value}, separators=(",", ":")) == (
        '{"value":null}'
    )
