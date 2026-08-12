"""Resumable TWSE/E.SUN source-preserving market-data comparison."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone

from app.models import (
    CrossValidationOutcome,
    CrossValidationRun,
    MarketDataDiscrepancy,
    MarketDataObservation,
    NormalizedMarketData,
    PipelineRunStatus,
)
from app.providers import MarketDataBatch, MarketDataProvider, ProviderTemporaryError
from app.storage import (
    CrossValidationConflictError,
    CrossValidationStateError,
    SQLiteCrossValidationRepository,
    SQLiteResearchRepository,
)

from .normalization import MarketDataNormalizer
from .retry import RetryPolicy


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*[^\s,;]+"
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")


@dataclass(frozen=True, slots=True)
class CrossValidationResult:
    run_id: str
    symbol: str
    target_date: date
    status: PipelineRunStatus
    outcome: CrossValidationOutcome
    run_attempt_count: int
    left_provider_attempts: int
    right_provider_attempts: int
    idempotent_replay: bool
    observations: tuple[MarketDataObservation, ...]
    discrepancies: tuple[MarketDataDiscrepancy, ...]


class MarketDataCrossValidationPipeline:
    """Compare two canonical observations without selecting or overwriting a winner."""

    def __init__(
        self,
        left_provider: MarketDataProvider,
        right_provider: MarketDataProvider,
        repository: SQLiteResearchRepository,
        *,
        normalizer: MarketDataNormalizer | None = None,
        retry_policy: RetryPolicy | None = None,
        provider_timeout_seconds: float = 10.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if left_provider.source == right_provider.source:
            raise ValueError("cross-validation providers must be different")
        if provider_timeout_seconds <= 0:
            raise ValueError("provider_timeout_seconds must be greater than zero")
        self.left_provider = left_provider
        self.right_provider = right_provider
        self.research_repository = repository
        self.repository = SQLiteCrossValidationRepository(repository)
        self.normalizer = normalizer or MarketDataNormalizer()
        self.retry_policy = retry_policy or RetryPolicy()
        self.provider_timeout_seconds = provider_timeout_seconds
        self.sleep = sleep
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def run(
        self,
        symbol: str,
        start_date: date,
        target_date: date,
        *,
        resume_run_id: str | None = None,
    ) -> CrossValidationResult:
        if start_date > target_date:
            raise ValueError("start_date must not be after target_date")
        requested_symbol = symbol.strip().upper()
        if not requested_symbol:
            raise ValueError("symbol must not be empty")

        self.research_repository.initialize()
        run = self.repository.get_or_create_run(
            symbol=requested_symbol,
            target_date=target_date,
            requested_start_date=start_date,
            left_provider=self.left_provider.source,
            right_provider=self.right_provider.source,
            created_at=self._now(),
        )
        if resume_run_id is not None and resume_run_id != run.run_id:
            raise CrossValidationConflictError(
                "resume_run_id does not match the validation checkpoint"
            )
        if run.status is PipelineRunStatus.SUCCESS:
            return self._load_successful_run(run)

        running = self.repository.start(
            run.run_id,
            started_at=self._now(),
            resume_running=resume_run_id == run.run_id,
        )
        try:
            left_batch, left_attempts = self._fetch_with_retry(
                self.left_provider, requested_symbol, start_date, target_date
            )
            right_batch, right_attempts = self._fetch_with_retry(
                self.right_provider, requested_symbol, start_date, target_date
            )
            left = self._observation(
                running.run_id,
                self.left_provider,
                left_batch,
                requested_symbol,
                start_date,
                target_date,
            )
            right = self._observation(
                running.run_id,
                self.right_provider,
                right_batch,
                requested_symbol,
                start_date,
                target_date,
            )
            discrepancies = self._compare(running.run_id, left, right)
            with self.repository.successful_run(running.run_id) as unit:
                outcome = unit.complete(
                    (left, right),
                    discrepancies,
                    finished_at=self._now(),
                    source_artifacts={
                        self.left_provider.source: left_batch.source_artifacts,
                        self.right_provider.source: right_batch.source_artifacts,
                    },
                )
        except Exception as error:
            self.repository.mark_failed(
                running.run_id,
                finished_at=self._now(),
                error_message=self._safe_error_message(error),
            )
            raise

        observations = tuple(self.repository.list_observations(running.run_id))
        stored_discrepancies = tuple(
            self.repository.list_discrepancies(running.run_id)
        )
        return CrossValidationResult(
            run_id=running.run_id,
            symbol=requested_symbol,
            target_date=target_date,
            status=PipelineRunStatus.SUCCESS,
            outcome=outcome,
            run_attempt_count=running.attempt_count,
            left_provider_attempts=left_attempts,
            right_provider_attempts=right_attempts,
            idempotent_replay=False,
            observations=observations,
            discrepancies=stored_discrepancies,
        )

    def _fetch_with_retry(
        self,
        provider: MarketDataProvider,
        symbol: str,
        start_date: date,
        target_date: date,
    ) -> tuple[MarketDataBatch, int]:
        total_delay_seconds = 0.0
        for attempt in range(1, self.retry_policy.max_attempts + 1):
            try:
                return (
                    provider.fetch_market_data(
                        symbol,
                        start_date,
                        target_date,
                        timeout_seconds=self.provider_timeout_seconds,
                    ),
                    attempt,
                )
            except ProviderTemporaryError:
                if attempt >= self.retry_policy.max_attempts:
                    raise
                delay = self.retry_policy.next_delay(
                    attempt,
                    total_delay_seconds=total_delay_seconds,
                )
                if delay is None:
                    raise
                self.sleep(delay)
                total_delay_seconds += delay
        raise AssertionError("retry loop ended without a result")  # pragma: no cover

    def _observation(
        self,
        run_id: str,
        provider: MarketDataProvider,
        batch: MarketDataBatch,
        symbol: str,
        start_date: date,
        target_date: date,
    ) -> MarketDataObservation:
        normalized = self.normalizer.normalize(batch)
        self._validate_normalized(
            provider,
            normalized,
            symbol=symbol,
            start_date=start_date,
            target_date=target_date,
        )
        latest = max(normalized.daily_prices, key=lambda price: price.trade_date)
        if normalized.market_date is not None and normalized.market_date != latest.trade_date:
            raise ValueError(
                "provider market_date does not match its latest canonical observation"
            )
        return MarketDataObservation(
            run_id=run_id,
            provider=provider.source,
            symbol=symbol,
            market_date=latest.trade_date,
            open=latest.open,
            high=latest.high,
            low=latest.low,
            close=latest.close,
            volume=latest.volume,
            source_endpoints=normalized.source_endpoints,
            fetched_at=normalized.fetched_at,  # type: ignore[arg-type]
            source_timestamp_raw=normalized.source_timestamp_raw,
            source_timestamp=normalized.source_timestamp,
        )

    @staticmethod
    def _validate_normalized(
        provider: MarketDataProvider,
        normalized: NormalizedMarketData,
        *,
        symbol: str,
        start_date: date,
        target_date: date,
    ) -> None:
        if normalized.source != provider.source:
            raise ValueError("provider batch source does not match provider identity")
        if normalized.symbol.symbol != symbol:
            raise ValueError("provider returned a different symbol than requested")
        if any(
            price.trade_date < start_date or price.trade_date > target_date
            for price in normalized.daily_prices
        ):
            raise ValueError("provider returned prices outside the requested range")
        if not normalized.source_endpoints:
            raise ValueError("cross-validation requires source endpoints")
        if normalized.fetched_at is None:
            raise ValueError("cross-validation requires fetched_at provenance")

    @classmethod
    def _compare(
        cls,
        run_id: str,
        left: MarketDataObservation,
        right: MarketDataObservation,
    ) -> tuple[MarketDataDiscrepancy, ...]:
        discrepancies: list[MarketDataDiscrepancy] = []
        if left.market_date != right.market_date:
            discrepancies.append(
                MarketDataDiscrepancy(
                    run_id=run_id,
                    field="market_date",
                    left_value=left.market_date.isoformat(),
                    right_value=right.market_date.isoformat(),
                    reason="market_date_mismatch",
                )
            )
        else:
            for field in ("open", "high", "low", "close", "volume"):
                left_value = getattr(left, field)
                right_value = getattr(right, field)
                if left_value != right_value:
                    absolute_difference = abs(float(left_value) - float(right_value))
                    relative_difference_pct = (
                        None
                        if float(left_value) == 0
                        else absolute_difference / abs(float(left_value)) * 100
                    )
                    discrepancies.append(
                        MarketDataDiscrepancy(
                            run_id=run_id,
                            field=field,
                            left_value=cls._canonical_value(left_value),
                            right_value=cls._canonical_value(right_value),
                            reason=(
                                "volume_definition_or_update_timing_unresolved"
                                if field == "volume"
                                else "source_value_or_update_timing_unresolved"
                            ),
                            absolute_difference=absolute_difference,
                            relative_difference_pct=relative_difference_pct,
                        )
                    )

        left_timestamp = cls._timestamp_value(left)
        right_timestamp = cls._timestamp_value(right)
        if left_timestamp != right_timestamp:
            discrepancies.append(
                MarketDataDiscrepancy(
                    run_id=run_id,
                    field="source_timestamp",
                    left_value=left_timestamp,
                    right_value=right_timestamp,
                    reason=(
                        "missing_value"
                        if left_timestamp is None or right_timestamp is None
                        else "source_timestamp_mismatch"
                    ),
                )
            )
        return tuple(discrepancies)

    def _load_successful_run(
        self, run: CrossValidationRun
    ) -> CrossValidationResult:
        if run.outcome is None:
            raise CrossValidationStateError(
                "successful validation run is missing its outcome"
            )
        observations = tuple(self.repository.list_observations(run.run_id))
        if len(observations) != 2:
            raise CrossValidationStateError(
                "successful validation run is missing source observations"
            )
        discrepancies = tuple(self.repository.list_discrepancies(run.run_id))
        expected = (
            CrossValidationOutcome.DISCREPANCY
            if discrepancies
            else CrossValidationOutcome.MATCH
        )
        if run.outcome != expected:
            raise CrossValidationStateError(
                "validation outcome does not match stored discrepancies"
            )
        return CrossValidationResult(
            run_id=run.run_id,
            symbol=run.symbol,
            target_date=run.target_date,
            status=run.status,
            outcome=run.outcome,
            run_attempt_count=run.attempt_count,
            left_provider_attempts=0,
            right_provider_attempts=0,
            idempotent_replay=True,
            observations=observations,
            discrepancies=discrepancies,
        )

    def _now(self) -> datetime:
        now = self.clock()
        if now.utcoffset() is None:
            raise ValueError("pipeline clock must be timezone-aware")
        return now

    @staticmethod
    def _canonical_value(value: float | int) -> str:
        if isinstance(value, int):
            return str(value)
        return format(value, ".15g")

    @staticmethod
    def _timestamp_value(observation: MarketDataObservation) -> str | None:
        if observation.source_timestamp is not None:
            return observation.source_timestamp.isoformat()
        return observation.source_timestamp_raw

    @staticmethod
    def _safe_error_message(error: Exception) -> str:
        detail = str(error).replace("\r", " ").replace("\n", " ").strip()
        detail = _SECRET_ASSIGNMENT.sub(r"\1=[REDACTED]", detail)
        detail = _BEARER_TOKEN.sub("Bearer [REDACTED]", detail)
        error_type = type(error).__name__
        return error_type if not detail else f"{error_type}: {detail}"[:1000]
