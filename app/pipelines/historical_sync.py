"""Resumable official historical synchronization and research summary pipeline."""

from __future__ import annotations

import calendar
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from app.analysis import (
    HistoricalResearchAnalysis,
    HistoricalResearchAnalyzer,
    HistoricalResearchSummaryGenerator,
)
from app.models import HistoricalSyncRun, PipelineRunStatus, ResearchNote
from app.providers import MarketDataBatch, MarketDataProvider, ProviderTemporaryError
from app.storage import (
    HistoricalSyncConflictError,
    HistoricalSyncStateError,
    SQLiteHistoricalSyncRepository,
    SQLiteResearchRepository,
)

from .normalization import MarketDataNormalizer
from .retry import RetryPolicy


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*[^\s,;]+"
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")


class HistoricalCoverageError(RuntimeError):
    """The bounded synchronization could not reach its observation target."""


@dataclass(frozen=True, slots=True)
class HistoricalSyncResult:
    run_id: str
    run_status: PipelineRunStatus
    run_attempt_count: int
    idempotent_replay: bool
    symbol: str
    target_date: date
    target_observations: int
    observation_count: int
    months_completed: int
    months_fetched: int
    provider_attempts: int
    research_note_id: int
    analysis: HistoricalResearchAnalysis
    summary: str


class HistoricalSyncPipeline:
    """Synchronize month checkpoints until 60-250 observations are available."""

    def __init__(
        self,
        provider: MarketDataProvider,
        repository: SQLiteResearchRepository,
        normalizer: MarketDataNormalizer | None = None,
        analyzer: HistoricalResearchAnalyzer | None = None,
        summary_generator: HistoricalResearchSummaryGenerator | None = None,
        retry_policy: RetryPolicy | None = None,
        provider_timeout_seconds: float = 10.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] | None = None,
        write_canonical_prices: bool | None = None,
    ) -> None:
        if provider_timeout_seconds <= 0:
            raise ValueError("provider_timeout_seconds must be greater than zero")
        self.provider = provider
        self.repository = repository
        self.history_repository = SQLiteHistoricalSyncRepository(repository)
        self.normalizer = normalizer or MarketDataNormalizer()
        self.analyzer = analyzer or HistoricalResearchAnalyzer()
        self.summary_generator = (
            summary_generator or HistoricalResearchSummaryGenerator()
        )
        self.retry_policy = retry_policy or RetryPolicy()
        self.provider_timeout_seconds = provider_timeout_seconds
        self.sleep = sleep
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        if provider.source == "esun-historical":
            if write_canonical_prices is True:
                raise ValueError(
                    "E.SUN historical observations must remain source-only"
                )
            self.write_canonical_prices = False
        else:
            self.write_canonical_prices = (
                True if write_canonical_prices is None else write_canonical_prices
            )

    def run(
        self,
        symbol: str,
        target_date: date,
        *,
        target_observations: int = 250,
        max_months: int = 18,
        resume_run_id: str | None = None,
    ) -> HistoricalSyncResult:
        if not 60 <= target_observations <= 250:
            raise ValueError("target_observations must be between 60 and 250")
        if max_months < 1:
            raise ValueError("max_months must be at least 1")
        requested_symbol = symbol.strip().upper()
        if not requested_symbol:
            raise ValueError("symbol must not be empty")

        self.repository.initialize()
        run = self.history_repository.get_or_create(
            symbol=requested_symbol,
            target_date=target_date,
            target_observations=target_observations,
            provider=self.provider.source,
            created_at=self.clock(),
        )
        if resume_run_id is not None and resume_run_id != run.run_id:
            raise HistoricalSyncConflictError(
                "resume_run_id does not match the historical sync contract"
            )
        if run.status is PipelineRunStatus.SUCCESS:
            return self._load_successful_run(run)

        running = self.history_repository.start(
            run.run_id,
            started_at=self.clock(),
            resume_running=resume_run_id == run.run_id,
        )
        months_fetched = 0
        provider_attempts = 0

        try:
            while running.observation_count < target_observations:
                if running.months_completed >= max_months:
                    raise HistoricalCoverageError(
                        f"historical sync reached max_months={max_months} with "
                        f"{running.observation_count} observations"
                    )
                month_start = running.next_month
                month_end = self._month_end(month_start)
                if (month_start.year, month_start.month) == (
                    target_date.year,
                    target_date.month,
                ):
                    month_end = min(month_end, target_date)

                raw_batch, attempts = self._fetch_with_retry(
                    requested_symbol, month_start, month_end
                )
                provider_attempts += attempts
                normalized = self.normalizer.normalize(raw_batch)
                self._validate_month_batch(
                    normalized_source=normalized.source,
                    normalized_symbol=normalized.symbol.symbol,
                    price_dates=tuple(
                        price.trade_date for price in normalized.daily_prices
                    ),
                    requested_symbol=requested_symbol,
                    month_start=month_start,
                    month_end=month_end,
                )
                if not normalized.source_endpoints:
                    raise HistoricalSyncStateError(
                        "historical month must record source endpoints"
                    )
                if normalized.fetched_at is None:
                    raise HistoricalSyncStateError(
                        "historical month must record fetched_at"
                    )
                running, _ = self.history_repository.checkpoint_month(
                    running.run_id,
                    completed_month=month_start,
                    next_month=self._previous_month(month_start),
                    data=normalized,
                    source_endpoints=normalized.source_endpoints,
                    fetched_at=normalized.fetched_at,
                    updated_at=self.clock(),
                    write_canonical_prices=self.write_canonical_prices,
                    source_artifacts=raw_batch.source_artifacts,
                )
                months_fetched += 1

            with self.history_repository.successful_run(running.run_id) as unit:
                prices = unit.list_recent_prices(
                    requested_symbol,
                    end_date=target_date,
                    limit=target_observations,
                )
                if len(prices) < target_observations:
                    raise HistoricalCoverageError(
                        "stored price coverage is below target at completion"
                    )
                metrics = unit.list_metrics(requested_symbol, end_date=target_date)
                analysis = self.analyzer.analyze(prices)
                summary = self.summary_generator.generate(analysis, metrics)
                note = ResearchNote(
                    symbol=requested_symbol,
                    created_at=self.clock(),
                    analysis_type="historical-research-v1",
                    title=(
                        f"{requested_symbol} 歷史研究摘要 "
                        f"{analysis.period_end} ({target_observations} 日)"
                    ),
                    summary=summary,
                    source_data_start=analysis.period_start,
                    source_data_end=analysis.period_end,
                    provider_source=self.provider.source,
                    historical_run_id=running.run_id,
                )
                saved_note = unit.create_research_note(note)
                if saved_note.id is None:  # pragma: no cover - SQLite supplies id.
                    raise HistoricalSyncStateError(
                        "historical note was stored without id"
                    )
                unit.mark_success(
                    saved_note.id,
                    observation_count=analysis.observations,
                    first_trade_date=analysis.period_start,
                    last_trade_date=analysis.period_end,
                    finished_at=self.clock(),
                )
        except Exception as error:
            self.history_repository.mark_failed(
                running.run_id,
                finished_at=self.clock(),
                error_message=self._safe_error_message(error),
            )
            raise

        return HistoricalSyncResult(
            run_id=running.run_id,
            run_status=PipelineRunStatus.SUCCESS,
            run_attempt_count=running.attempt_count,
            idempotent_replay=False,
            symbol=requested_symbol,
            target_date=target_date,
            target_observations=target_observations,
            observation_count=analysis.observations,
            months_completed=running.months_completed,
            months_fetched=months_fetched,
            provider_attempts=provider_attempts,
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
        raise AssertionError("retry loop ended without result")  # pragma: no cover

    def _load_successful_run(self, run: HistoricalSyncRun) -> HistoricalSyncResult:
        if run.research_note_id is None:
            raise HistoricalSyncStateError(
                "successful historical run is missing research_note_id"
            )
        note = self.repository.get_research_note(run.research_note_id)
        if note is None or note.historical_run_id != run.run_id:
            raise HistoricalSyncStateError(
                "successful historical run is not linked to its note"
            )
        prices = self.history_repository.list_run_prices(
            run.run_id,
            end_date=run.target_date,
            limit=run.target_observations,
        )
        metrics = self.history_repository.list_metrics(
            run.symbol, end_date=run.target_date
        )
        analysis = self.analyzer.analyze(prices)
        return HistoricalSyncResult(
            run_id=run.run_id,
            run_status=run.status,
            run_attempt_count=run.attempt_count,
            idempotent_replay=True,
            symbol=run.symbol,
            target_date=run.target_date,
            target_observations=run.target_observations,
            observation_count=analysis.observations,
            months_completed=run.months_completed,
            months_fetched=0,
            provider_attempts=0,
            research_note_id=run.research_note_id,
            analysis=analysis,
            summary=note.summary,
        )

    def _validate_month_batch(
        self,
        *,
        normalized_source: str,
        normalized_symbol: str,
        price_dates: tuple[date, ...],
        requested_symbol: str,
        month_start: date,
        month_end: date,
    ) -> None:
        if normalized_source != self.provider.source:
            raise HistoricalSyncConflictError(
                "historical batch source does not match provider"
            )
        if normalized_symbol != requested_symbol:
            raise HistoricalSyncConflictError(
                "historical batch symbol does not match request"
            )
        if any(day < month_start or day > month_end for day in price_dates):
            raise HistoricalSyncConflictError(
                "historical batch contains dates outside requested month range"
            )

    @staticmethod
    def _month_end(month_start: date) -> date:
        return date(
            month_start.year,
            month_start.month,
            calendar.monthrange(month_start.year, month_start.month)[1],
        )

    @staticmethod
    def _previous_month(month_start: date) -> date:
        return (month_start - timedelta(days=1)).replace(day=1)

    @staticmethod
    def _safe_error_message(error: Exception) -> str:
        detail = str(error).replace("\r", " ").replace("\n", " ").strip()
        detail = _SECRET_ASSIGNMENT.sub(r"\1=[REDACTED]", detail)
        detail = _BEARER_TOKEN.sub("Bearer [REDACTED]", detail)
        error_type = type(error).__name__
        return error_type if not detail else f"{error_type}: {detail}"[:1000]
