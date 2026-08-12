"""Resumable end-to-end daily research pipeline."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone

from app.analysis import DescriptiveRiskAnalyzer, ResearchSummaryGenerator, RiskAnalysis
from app.models import PipelineRun, PipelineRunStatus, ResearchNote
from app.providers import MarketDataBatch, MarketDataProvider, ProviderTemporaryError
from app.storage import (
    PipelineRunConflictError,
    PipelineRunStateError,
    SQLiteResearchRepository,
)

from .normalization import MarketDataNormalizer
from .retry import RetryPolicy


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*[^\s,;]+"
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")


@dataclass(frozen=True, slots=True)
class PipelineResult:
    run_id: str
    target_date: date
    run_status: PipelineRunStatus
    run_attempt_count: int
    provider_attempts: int
    idempotent_replay: bool
    symbol: str
    provider_source: str
    source_endpoints: tuple[str, ...]
    fetched_at: datetime | None
    market_date: date | None
    daily_prices_written: int
    company_metrics_written: int
    research_note_id: int
    analysis: RiskAnalysis
    summary: str


class DailyResearchPipeline:
    """Fetch, normalize, atomically persist, analyze, and checkpoint a run."""

    def __init__(
        self,
        provider: MarketDataProvider,
        repository: SQLiteResearchRepository,
        normalizer: MarketDataNormalizer | None = None,
        analyzer: DescriptiveRiskAnalyzer | None = None,
        summary_generator: ResearchSummaryGenerator | None = None,
        retry_policy: RetryPolicy | None = None,
        provider_timeout_seconds: float = 10.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if provider_timeout_seconds <= 0:
            raise ValueError("provider_timeout_seconds must be greater than zero")
        self.provider = provider
        self.repository = repository
        self.normalizer = normalizer or MarketDataNormalizer()
        self.analyzer = analyzer or DescriptiveRiskAnalyzer()
        self.summary_generator = summary_generator or ResearchSummaryGenerator()
        self.retry_policy = retry_policy or RetryPolicy()
        self.provider_timeout_seconds = provider_timeout_seconds
        self.sleep = sleep
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def run(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        resume_run_id: str | None = None,
    ) -> PipelineResult:
        if start_date > end_date:
            raise ValueError("start_date must not be after end_date")
        requested_symbol = symbol.strip().upper()
        if not requested_symbol:
            raise ValueError("symbol must not be empty")

        self.repository.initialize()
        run = self.repository.get_or_create_pipeline_run(
            symbol=requested_symbol,
            target_date=end_date,
            requested_start_date=start_date,
            requested_end_date=end_date,
            provider=self.provider.source,
            created_at=self.clock(),
        )
        if resume_run_id is not None and resume_run_id != run.run_id:
            raise PipelineRunConflictError(
                "resume_run_id does not match the symbol/target_date checkpoint"
            )
        if run.status is PipelineRunStatus.SUCCESS:
            return self._load_successful_run(run)

        running = self.repository.start_pipeline_run(
            run.run_id,
            started_at=self.clock(),
            resume_running=resume_run_id == run.run_id,
        )

        try:
            raw_batch, provider_attempts = self._fetch_with_retry(
                requested_symbol, start_date, end_date
            )
            normalized = self.normalizer.normalize(raw_batch)
            self._validate_normalized_contract(
                normalized_source=normalized.source,
                normalized_symbol=normalized.symbol.symbol,
                price_dates=tuple(
                    price.trade_date for price in normalized.daily_prices
                ),
                requested_symbol=requested_symbol,
                start_date=start_date,
                end_date=end_date,
            )

            with self.repository.successful_pipeline_run(running.run_id) as unit:
                write_result = unit.save_market_data(normalized)
                unit.save_source_artifacts(
                    raw_batch.source_artifacts,
                    provider=normalized.source,
                    created_at=self.clock(),
                )
                stored_prices = unit.list_daily_prices(
                    requested_symbol, start_date=start_date, end_date=end_date
                )
                stored_metrics = unit.list_company_metrics(
                    requested_symbol, start_date=start_date, end_date=end_date
                )
                analysis = self.analyzer.analyze(stored_prices)
                summary = self.summary_generator.generate(analysis, stored_metrics)
                note = ResearchNote(
                    symbol=requested_symbol,
                    created_at=self.clock(),
                    analysis_type="descriptive-risk-summary-v1",
                    title=f"{requested_symbol} 研究摘要 {analysis.period_end}",
                    summary=summary,
                    source_data_start=analysis.period_start,
                    source_data_end=analysis.period_end,
                    provider_source=normalized.source,
                    run_id=running.run_id,
                )
                saved_note = unit.create_research_note(note)
                if saved_note.id is None:  # pragma: no cover - SQLite supplies this.
                    raise PipelineRunStateError(
                        "research note was stored without an id"
                    )
                unit.mark_success(
                    saved_note.id,
                    finished_at=self.clock(),
                    source_endpoints=normalized.source_endpoints,
                    fetched_at=normalized.fetched_at,
                    market_date=normalized.market_date,
                )
        except Exception as error:
            self.repository.mark_pipeline_run_failed(
                running.run_id,
                finished_at=self.clock(),
                error_message=self._safe_error_message(error),
            )
            raise

        return PipelineResult(
            run_id=running.run_id,
            target_date=end_date,
            run_status=PipelineRunStatus.SUCCESS,
            run_attempt_count=running.attempt_count,
            provider_attempts=provider_attempts,
            idempotent_replay=False,
            symbol=requested_symbol,
            provider_source=normalized.source,
            source_endpoints=normalized.source_endpoints,
            fetched_at=normalized.fetched_at,
            market_date=normalized.market_date,
            daily_prices_written=write_result.daily_prices,
            company_metrics_written=write_result.company_metrics,
            research_note_id=saved_note.id,
            analysis=analysis,
            summary=summary,
        )

    def _fetch_with_retry(
        self, symbol: str, start_date: date, end_date: date
    ) -> tuple[MarketDataBatch, int]:
        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                batch = self.provider.fetch_market_data(
                    symbol,
                    start_date,
                    end_date,
                    timeout_seconds=self.provider_timeout_seconds,
                )
                return batch, attempt
            except ProviderTemporaryError as error:
                if attempt >= self.retry_policy.max_attempts:
                    raise
                delay = self.retry_policy.delay_after_failure(attempt)
                if error.retry_after_seconds is not None:
                    delay = max(delay, error.retry_after_seconds)
                self.sleep(delay)
        raise AssertionError("retry loop ended without a result")  # pragma: no cover

    def _load_successful_run(self, run: PipelineRun) -> PipelineResult:
        if run.research_note_id is None:
            raise PipelineRunStateError(
                "successful pipeline run is missing research_note_id"
            )
        note = self.repository.get_research_note(run.research_note_id)
        if note is None or note.run_id != run.run_id:
            raise PipelineRunStateError(
                "successful pipeline run is not linked to its research note"
            )
        if note.provider_source != run.provider:
            raise PipelineRunStateError(
                "successful pipeline run provider does not match its research note"
            )

        stored_prices = self.repository.list_daily_prices(
            run.symbol,
            start_date=run.requested_start_date,
            end_date=run.requested_end_date,
        )
        stored_metrics = self.repository.list_company_metrics(
            run.symbol,
            start_date=run.requested_start_date,
            end_date=run.requested_end_date,
        )
        analysis = self.analyzer.analyze(stored_prices)
        return PipelineResult(
            run_id=run.run_id,
            target_date=run.target_date,
            run_status=run.status,
            run_attempt_count=run.attempt_count,
            provider_attempts=0,
            idempotent_replay=True,
            symbol=run.symbol,
            provider_source=run.provider,
            source_endpoints=run.source_endpoints,
            fetched_at=run.fetched_at,
            market_date=run.market_date,
            daily_prices_written=0,
            company_metrics_written=0,
            research_note_id=run.research_note_id,
            analysis=analysis,
            summary=note.summary,
        )

    def _validate_normalized_contract(
        self,
        *,
        normalized_source: str,
        normalized_symbol: str,
        price_dates: tuple[date, ...],
        requested_symbol: str,
        start_date: date,
        end_date: date,
    ) -> None:
        if normalized_symbol != requested_symbol:
            raise ValueError("provider returned a different symbol than requested")
        if normalized_source != self.provider.source:
            raise ValueError("provider batch source does not match provider identity")
        if any(day < start_date or day > end_date for day in price_dates):
            raise ValueError("provider returned daily prices outside the requested range")

    @staticmethod
    def _safe_error_message(error: Exception) -> str:
        detail = str(error).replace("\r", " ").replace("\n", " ").strip()
        detail = _SECRET_ASSIGNMENT.sub(r"\1=[REDACTED]", detail)
        detail = _BEARER_TOKEN.sub("Bearer [REDACTED]", detail)
        error_type = type(error).__name__
        return error_type if not detail else f"{error_type}: {detail}"[:1000]
