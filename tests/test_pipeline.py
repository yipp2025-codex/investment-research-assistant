import sqlite3
from dataclasses import replace
from datetime import date

import pytest

from app.models import PipelineRunStatus
from app.pipelines import DailyResearchPipeline, NormalizationError, RetryPolicy
from app.providers import (
    MockFailureMode,
    MockMarketDataProvider,
    ProviderPermanentError,
    ProviderTemporaryError,
)
from app.storage import (
    PipelineRunConflictError,
    PipelineRunInProgressError,
    SQLiteResearchRepository,
)


START = date(2026, 8, 3)
END = date(2026, 8, 5)


def test_pipeline_runs_mock_data_end_to_end(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = MockMarketDataProvider()
    pipeline = DailyResearchPipeline(provider, repository)

    result = pipeline.run("mock1", START, END)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert result.run_attempt_count == 1
    assert result.provider_attempts == 1
    assert result.idempotent_replay is False
    assert result.symbol == "MOCK1"
    assert result.provider_source == "mock-synthetic"
    assert result.daily_prices_written == 3
    assert result.company_metrics_written == 2
    assert result.research_note_id > 0
    assert result.analysis.observations == 3
    assert "不產生買賣評分或交易訊號" in result.summary

    run = repository.get_pipeline_run(result.run_id)
    assert run is not None
    assert run.status is PipelineRunStatus.SUCCESS
    assert run.target_date == END
    assert run.finished_at is not None
    assert run.error_message is None
    assert run.research_note_id == result.research_note_id
    assert repository.get_research_note(result.research_note_id).run_id == result.run_id
    assert len(repository.list_daily_prices("MOCK1")) == 3
    assert len(repository.list_company_metrics("MOCK1")) == 2
    assert len(repository.list_research_notes("MOCK1")) == 1
    artifacts = repository.list_source_artifacts(pipeline_run_id=result.run_id)
    assert len(artifacts) == 1
    assert artifacts[0].provider == "mock-synthetic"
    assert artifacts[0].dataset == "synthetic-daily-market-data"
    assert artifacts[0].hash_basis == "canonical-json-v1"


def test_provider_temporary_failure_retries_then_succeeds_without_sleep(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = MockMarketDataProvider([MockFailureMode.TEMPORARY])
    delays: list[float] = []
    pipeline = DailyResearchPipeline(
        provider,
        repository,
        retry_policy=RetryPolicy(
            max_attempts=2,
            initial_backoff_seconds=0.25,
            backoff_multiplier=3,
        ),
        provider_timeout_seconds=1.5,
        sleep=delays.append,
    )

    result = pipeline.run("MOCK1", START, END)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert result.provider_attempts == 2
    assert provider.call_count == 2
    assert provider.timeout_history == [1.5, 1.5]
    assert delays == [0.25]
    assert len(repository.list_research_notes("MOCK1")) == 1


def test_daily_pipeline_caps_extreme_provider_retry_after(tmp_path) -> None:
    class RetryAfterProvider(MockMarketDataProvider):
        def fetch_market_data(
            self, symbol, start_date, end_date, *, timeout_seconds
        ):
            if self.call_count == 0:
                self.call_count += 1
                self.timeout_history.append(timeout_seconds)
                raise ProviderTemporaryError(
                    "paced", retry_after_seconds=1_000_000_000
                )
            return super().fetch_market_data(
                symbol,
                start_date,
                end_date,
                timeout_seconds=timeout_seconds,
            )

    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = RetryAfterProvider()
    delays: list[float] = []
    pipeline = DailyResearchPipeline(
        provider,
        repository,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.25),
        sleep=delays.append,
    )

    result = pipeline.run("MOCK1", START, END)

    assert result.run_status is PipelineRunStatus.SUCCESS
    assert delays == [60.0]


def test_daily_pipeline_stops_when_total_retry_delay_budget_is_exhausted(
    tmp_path,
) -> None:
    class AlwaysPacedProvider(MockMarketDataProvider):
        def fetch_market_data(
            self, symbol, start_date, end_date, *, timeout_seconds
        ):
            del symbol, start_date, end_date
            self.call_count += 1
            self.timeout_history.append(timeout_seconds)
            raise ProviderTemporaryError(
                "paced", retry_after_seconds=1_000_000_000
            )

    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = AlwaysPacedProvider()
    delays: list[float] = []
    pipeline = DailyResearchPipeline(
        provider,
        repository,
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_backoff_seconds=0.25,
            max_delay_seconds=60.0,
            max_total_delay_seconds=60.0,
        ),
        sleep=delays.append,
    )

    with pytest.raises(ProviderTemporaryError, match="paced"):
        pipeline.run("MOCK1", START, END)

    assert provider.call_count == 2
    assert delays == [60.0]


def test_provider_permanent_failure_marks_run_failed_without_retry(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = MockMarketDataProvider([MockFailureMode.PERMANENT])
    pipeline = DailyResearchPipeline(provider, repository, sleep=lambda _: None)

    with pytest.raises(ProviderPermanentError):
        pipeline.run("MOCK1", START, END)

    run = repository.get_pipeline_run_for_target("MOCK1", END)
    assert run is not None
    assert run.status is PipelineRunStatus.FAILED
    assert run.finished_at is not None
    assert run.error_message.startswith("ProviderPermanentError:")
    assert run.attempt_count == 1
    assert provider.call_count == 1
    assert repository.list_research_notes("MOCK1") == []


def test_failed_run_can_retry_with_same_checkpoint_and_succeed(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = MockMarketDataProvider([MockFailureMode.PERMANENT])
    pipeline = DailyResearchPipeline(provider, repository, sleep=lambda _: None)

    with pytest.raises(ProviderPermanentError):
        pipeline.run("MOCK1", START, END)
    failed = repository.get_pipeline_run_for_target("MOCK1", END)

    result = pipeline.run("MOCK1", START, END)

    assert failed is not None
    assert result.run_id == failed.run_id
    assert result.run_attempt_count == 2
    assert result.run_status is PipelineRunStatus.SUCCESS
    assert provider.call_count == 2
    assert len(repository.list_research_notes("MOCK1")) == 1


def test_normalization_failure_marks_run_failed_without_partial_data(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = MockMarketDataProvider([MockFailureMode.MALFORMED_PAYLOAD])
    pipeline = DailyResearchPipeline(provider, repository)

    with pytest.raises(NormalizationError):
        pipeline.run("MOCK1", START, END)

    run = repository.get_pipeline_run_for_target("MOCK1", END)
    assert run is not None
    assert run.status is PipelineRunStatus.FAILED
    assert run.error_message.startswith("NormalizationError:")
    assert repository.get_symbol("MOCK1") is None
    assert repository.list_daily_prices("MOCK1") == []
    assert repository.list_company_metrics("MOCK1") == []
    assert repository.list_research_notes("MOCK1") == []
    assert repository.list_source_artifacts(pipeline_run_id=run.run_id) == []


def test_sqlite_failure_rolls_back_data_note_and_success_transition(tmp_path) -> None:
    database_path = tmp_path / "research.db"
    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TRIGGER force_research_note_failure "
            "BEFORE INSERT ON research_notes BEGIN "
            "SELECT RAISE(ABORT, 'forced research note failure'); END"
        )
    pipeline = DailyResearchPipeline(MockMarketDataProvider(), repository)

    with pytest.raises(sqlite3.IntegrityError, match="forced research note failure"):
        pipeline.run("MOCK1", START, END)

    run = repository.get_pipeline_run_for_target("MOCK1", END)
    assert run is not None
    assert run.status is PipelineRunStatus.FAILED
    assert run.error_message.startswith("IntegrityError:")
    assert repository.get_symbol("MOCK1") is None
    assert repository.list_daily_prices("MOCK1") == []
    assert repository.list_company_metrics("MOCK1") == []
    assert repository.list_research_notes("MOCK1") == []
    assert repository.list_source_artifacts(pipeline_run_id=run.run_id) == []


def test_successful_same_date_rerun_is_idempotent(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = MockMarketDataProvider()
    pipeline = DailyResearchPipeline(provider, repository)

    first = pipeline.run("MOCK1", START, END)
    second = pipeline.run("MOCK1", START, END)

    assert second.run_id == first.run_id
    assert second.research_note_id == first.research_note_id
    assert second.idempotent_replay is True
    assert second.provider_attempts == 0
    assert second.daily_prices_written == 0
    assert second.company_metrics_written == 0
    assert provider.call_count == 1
    assert len(repository.list_pipeline_runs("MOCK1")) == 1
    assert len(repository.list_daily_prices("MOCK1")) == 3
    assert len(repository.list_company_metrics("MOCK1")) == 2
    assert len(repository.list_research_notes("MOCK1")) == 1
    assert len(repository.list_source_artifacts(pipeline_run_id=first.run_id)) == 1


def test_interrupted_run_requires_explicit_run_id_and_can_resume(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = MockMarketDataProvider([MockFailureMode.INTERRUPT])
    pipeline = DailyResearchPipeline(provider, repository)

    with pytest.raises(KeyboardInterrupt, match="synthetic interrupted"):
        pipeline.run("MOCK1", START, END)

    interrupted = repository.get_pipeline_run_for_target("MOCK1", END)
    assert interrupted is not None
    assert interrupted.status is PipelineRunStatus.RUNNING
    assert interrupted.finished_at is None
    assert interrupted.attempt_count == 1

    with pytest.raises(PipelineRunInProgressError):
        pipeline.run("MOCK1", START, END)
    assert provider.call_count == 1

    recovered = pipeline.run(
        "MOCK1", START, END, resume_run_id=interrupted.run_id
    )

    assert recovered.run_id == interrupted.run_id
    assert recovered.run_attempt_count == 2
    assert recovered.run_status is PipelineRunStatus.SUCCESS
    assert provider.call_count == 2
    assert len(repository.list_research_notes("MOCK1")) == 1


def test_same_target_with_different_range_fails_closed(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "research.db")
    provider = MockMarketDataProvider()
    pipeline = DailyResearchPipeline(provider, repository)
    pipeline.run("MOCK1", START, END)

    with pytest.raises(PipelineRunConflictError, match="requested_start_date"):
        pipeline.run("MOCK1", date(2026, 8, 4), END)

    assert provider.call_count == 1
    assert len(repository.list_pipeline_runs("MOCK1")) == 1


def test_pipeline_rejects_provider_source_mismatch_and_records_failure(tmp_path) -> None:
    class MismatchedSourceProvider(MockMarketDataProvider):
        def fetch_market_data(
            self, symbol, start_date, end_date, *, timeout_seconds
        ):
            batch = super().fetch_market_data(
                symbol,
                start_date,
                end_date,
                timeout_seconds=timeout_seconds,
            )
            return replace(batch, source="spoofed-source")

    repository = SQLiteResearchRepository(tmp_path / "research.db")
    pipeline = DailyResearchPipeline(MismatchedSourceProvider(), repository)

    with pytest.raises(ValueError, match="source"):
        pipeline.run("MOCK1", START, END)

    run = repository.get_pipeline_run_for_target("MOCK1", END)
    assert run is not None
    assert run.status is PipelineRunStatus.FAILED


def test_pipeline_rejects_prices_outside_requested_range(tmp_path) -> None:
    class OutOfRangeProvider(MockMarketDataProvider):
        def fetch_market_data(
            self, symbol, start_date, end_date, *, timeout_seconds
        ):
            batch = super().fetch_market_data(
                symbol,
                start_date,
                end_date,
                timeout_seconds=timeout_seconds,
            )
            first = dict(batch.daily_prices[0])
            first["trade_date"] = "2026-08-02"
            return replace(batch, daily_prices=(first, *batch.daily_prices[1:]))

    repository = SQLiteResearchRepository(tmp_path / "research.db")
    pipeline = DailyResearchPipeline(OutOfRangeProvider(), repository)

    with pytest.raises(ValueError, match="outside"):
        pipeline.run("MOCK1", START, END)

    run = repository.get_pipeline_run_for_target("MOCK1", END)
    assert run is not None
    assert run.status is PipelineRunStatus.FAILED


def test_provider_error_message_is_redacted_before_checkpoint_storage(tmp_path) -> None:
    class SecretLeakingProvider(MockMarketDataProvider):
        def fetch_market_data(
            self, symbol, start_date, end_date, *, timeout_seconds
        ):
            del symbol, start_date, end_date, timeout_seconds
            leaked_value = "DO-" + "NOT-STORE"
            raise ProviderPermanentError(
                "api" + "_key=" + leaked_value + " Bearer " + leaked_value + "-EITHER"
            )

    repository = SQLiteResearchRepository(tmp_path / "research.db")
    pipeline = DailyResearchPipeline(SecretLeakingProvider(), repository)

    with pytest.raises(ProviderPermanentError):
        pipeline.run("MOCK1", START, END)

    run = repository.get_pipeline_run_for_target("MOCK1", END)
    assert run is not None
    assert run.status is PipelineRunStatus.FAILED
    assert "DO-NOT-STORE" not in run.error_message
    assert run.error_message.count("[REDACTED]") == 2
