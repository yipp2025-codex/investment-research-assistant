import sqlite3
from pathlib import Path

from app.models import PipelineRunStatus
from app.storage import SQLiteResearchRepository


def test_phase1_database_migrates_without_losing_existing_research(tmp_path) -> None:
    database_path = tmp_path / "phase1.db"
    with sqlite3.connect(database_path) as connection:
        connection.executescript(
            Path("app/storage/schema.sql").read_text(encoding="utf-8")
        )
        connection.execute(
            "INSERT INTO symbols (symbol, name, market, currency, is_active) "
            "VALUES ('OLD1', 'Existing Company', 'MOCK', 'TWD', 1)"
        )
        cursor = connection.execute(
            "INSERT INTO research_notes ("
            "symbol, created_at, analysis_type, title, summary, "
            "source_data_start, source_data_end, provider_source"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "OLD1",
                "2026-08-04T00:00:00+00:00",
                "phase1-summary",
                "Existing note",
                "Must survive migration",
                "2026-08-03",
                "2026-08-04",
                "mock-synthetic",
            ),
        )
        note_id = int(cursor.lastrowid)

    repository = SQLiteResearchRepository(database_path)
    repository.initialize()

    assert repository.get_schema_version() == 10
    note = repository.get_research_note(note_id)
    assert note is not None
    assert note.summary == "Must survive migration"
    assert note.run_id is None
    assert repository.list_pipeline_runs() == []
    with sqlite3.connect(database_path) as connection:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(research_notes)")
        }
        run_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(pipeline_runs)")
        }
        historical_run_columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(historical_sync_runs)"
            )
        }
        validation_tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name LIKE 'market_data_%'"
            )
        }
        historical_validation_tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name IN ('historical_source_observations', "
                "'historical_validation_runs', "
                "'historical_validation_discrepancies')"
            )
        }
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    assert "run_id" in columns
    assert "historical_run_id" in columns
    assert "historical_validation_run_id" in columns
    assert {"source_endpoints_json", "fetched_at", "market_date"} <= run_columns
    assert "attempt_count" in historical_run_columns
    assert validation_tables == {
        "market_data_validation_runs",
        "market_data_observations",
        "market_data_discrepancies",
    }
    assert historical_validation_tables == {
        "historical_source_observations",
        "historical_validation_runs",
        "historical_validation_discrepancies",
    }
    assert integrity == "ok"


def test_phase2_database_migrates_to_provenance_without_losing_run(tmp_path) -> None:
    database_path = tmp_path / "phase2.db"
    with sqlite3.connect(database_path) as connection:
        connection.executescript(
            Path("app/storage/schema.sql").read_text(encoding="utf-8")
        )
        connection.execute(
            "CREATE TABLE schema_migrations ("
            "version INTEGER PRIMARY KEY, name TEXT NOT NULL, "
            "applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        connection.executemany(
            "INSERT INTO schema_migrations (version, name) VALUES (?, ?)",
            [(1, "initial research schema"), (2, "pipeline runs")],
        )
        connection.executescript(
            Path("app/storage/migrations/0002_pipeline_runs.sql").read_text(
                encoding="utf-8"
            )
        )
        connection.execute(
            "ALTER TABLE research_notes ADD COLUMN run_id TEXT "
            "REFERENCES pipeline_runs(run_id)"
        )
        connection.execute(
            "CREATE UNIQUE INDEX idx_research_notes_run_id "
            "ON research_notes(run_id) WHERE run_id IS NOT NULL"
        )
        connection.execute(
            "INSERT INTO pipeline_runs ("
            "run_id, symbol, target_date, requested_start_date, "
            "requested_end_date, status, provider, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "phase2-run",
                "MOCK1",
                "2026-08-05",
                "2026-08-03",
                "2026-08-05",
                "pending",
                "mock-synthetic",
                "2026-08-05T00:00:00+00:00",
                "2026-08-05T00:00:00+00:00",
            ),
        )

    repository = SQLiteResearchRepository(database_path)
    repository.initialize()

    assert repository.get_schema_version() == 10
    run = repository.get_pipeline_run("phase2-run")
    assert run is not None
    assert run.status is PipelineRunStatus.PENDING
    assert run.source_endpoints == ()
    assert run.fetched_at is None
    assert run.market_date is None
