from datetime import date, datetime, timedelta, timezone

from app.models import (
    CompanyMetric,
    DailyPrice,
    PipelineRunStatus,
    ResearchNote,
    Symbol,
)
from app.storage import SQLiteResearchRepository


def test_sqlite_crud_round_trip(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    repository.initialize()
    assert repository.get_schema_version() == 10

    symbol = Symbol("TEST1", "Test Company", "MOCK", "TWD")
    repository.upsert_symbol(symbol)
    assert repository.get_symbol("test1") == symbol

    prices = [
        DailyPrice(
            "TEST1", date(2026, 8, 3), 100.0, 102.0, 99.0, 101.0, 1000, "test"
        ),
        DailyPrice(
            "TEST1", date(2026, 8, 4), 101.0, 103.0, 100.0, 102.0, 1100, "test"
        ),
    ]
    repository.upsert_daily_prices(prices)
    assert repository.list_daily_prices("TEST1") == prices

    updated_price = DailyPrice(
        "TEST1", date(2026, 8, 4), 101.0, 104.0, 100.0, 103.0, 1200, "test-v2"
    )
    repository.upsert_daily_prices([updated_price])
    stored_prices = repository.list_daily_prices("TEST1")
    assert len(stored_prices) == 2
    assert stored_prices[-1] == updated_price

    metric = CompanyMetric(
        "TEST1", date(2026, 8, 4), "sample_metric", 1.25, "ratio", "test"
    )
    repository.upsert_company_metrics([metric])
    assert repository.list_company_metrics("TEST1") == [metric]

    note = ResearchNote(
        symbol="TEST1",
        created_at=datetime(2026, 8, 5, 1, 2, 3, tzinfo=timezone.utc),
        analysis_type="test-summary",
        title="Test note",
        summary="Stored research note",
        source_data_start=date(2026, 8, 3),
        source_data_end=date(2026, 8, 4),
        provider_source="test",
    )
    stored_note = repository.create_research_note(note)

    assert stored_note.id is not None
    assert repository.get_research_note(stored_note.id) == stored_note
    assert repository.list_research_notes("TEST1") == [stored_note]
    assert repository.delete_research_note(stored_note.id) is True
    assert repository.get_research_note(stored_note.id) is None
    assert repository.delete_research_note(stored_note.id) is False


def test_pipeline_run_pending_running_failed_and_retry_transitions(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    repository.initialize()
    now = datetime(2026, 8, 5, 1, 0, tzinfo=timezone.utc)

    pending = repository.get_or_create_pipeline_run(
        symbol="TEST1",
        target_date=date(2026, 8, 5),
        requested_start_date=date(2026, 8, 3),
        requested_end_date=date(2026, 8, 5),
        provider="test-provider",
        created_at=now,
    )
    assert pending.status is PipelineRunStatus.PENDING
    assert pending.attempt_count == 0

    running = repository.start_pipeline_run(pending.run_id, started_at=now)
    assert running.status is PipelineRunStatus.RUNNING
    assert running.attempt_count == 1

    failed = repository.mark_pipeline_run_failed(
        running.run_id,
        finished_at=now + timedelta(seconds=1),
        error_message="SyntheticFailure",
    )
    assert failed.status is PipelineRunStatus.FAILED
    assert failed.finished_at is not None

    retried = repository.start_pipeline_run(
        failed.run_id, started_at=now + timedelta(seconds=2)
    )
    assert retried.status is PipelineRunStatus.RUNNING
    assert retried.attempt_count == 2
    assert retried.finished_at is None
    assert retried.error_message is None
