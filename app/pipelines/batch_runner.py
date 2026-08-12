"""Phase 6A: Daily batch runner orchestrating per-symbol research pipelines.

Responsibilities:
- Freeze a watchlist revision at batch start.
- Resolve the trading day before doing any market work.
- Execute each symbol in isolation; one symbol failure does not block others.
- Checkpoint every step so interrupted runs can be resumed.
- Enforce idempotency: a batch/symbol already succeeded is never re-run.
- Delegate all market data work to existing Phase 1-5 pipelines (DailyResearchPipeline).
- Never touch trading, order submission, or signal generation.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone

from app.pipelines.daily_research import DailyResearchPipeline
from app.pipelines.trading_day import (
    TradingDayResolution,
    TradingDayStatus,
    resolve_trading_day,
)
from app.storage.batch_run import (
    BatchRunError,
    BatchRunStatus,
    DailyBatchRun,
    DailySymbolRun,
    SQLiteBatchRunRepository,
    SymbolRunStatus,
)


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*[^\s,;]+"
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")


@dataclass(frozen=True, slots=True)
class SymbolRunSummary:
    symbol: str
    status: SymbolRunStatus
    symbol_run_id: str
    pipeline_run_id: str | None
    error_message: str | None


@dataclass(frozen=True, slots=True)
class BatchRunSummary:
    batch_run_id: str
    watchlist_revision_id: str
    requested_date: date
    resolved_market_date: date | None
    trading_day_status: TradingDayStatus
    batch_status: BatchRunStatus
    symbol_results: tuple[SymbolRunSummary, ...]
    idempotent_replay: bool


class DailyBatchRunner:
    """Orchestrates a full daily research batch for a named watchlist.

    This runner is a composition layer over existing Phase 1-5 pipelines.
    It never re-implements market data fetching, normalization, analysis,
    or checkpointing; it calls DailyResearchPipeline per symbol.
    """

    RUNNER_POLICY_VERSION = "6a-v1"

    def __init__(
        self,
        batch_repository: SQLiteBatchRunRepository,
        research_pipeline: DailyResearchPipeline,
        watchlist_name: str = "default",
        *,
        latest_market_date_fn: Callable[[], date | None] | None = None,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.batch_repository = batch_repository
        self.research_pipeline = research_pipeline
        self.watchlist_name = watchlist_name
        self.latest_market_date_fn = latest_market_date_fn or (lambda: None)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.sleep = sleep

    def run(
        self,
        requested_date: date,
        *,
        resume_batch_run_id: str | None = None,
    ) -> BatchRunSummary:
        """Run (or resume) a daily batch for the given requested_date."""
        self.batch_repository.initialize()
        self.batch_repository._repo.initialize()

        # ------------------------------------------------------------------
        # Resolve trading day first (weekend = no provider call needed).
        # ------------------------------------------------------------------
        resolution = resolve_trading_day(
            requested_date,
            latest_market_date_fn=self.latest_market_date_fn,
        )

        # ------------------------------------------------------------------
        # If resuming, load the existing batch and its frozen revision.
        # ------------------------------------------------------------------
        if resume_batch_run_id is not None:
            return self._resume_batch(
                resume_batch_run_id, requested_date, resolution
            )

        # ------------------------------------------------------------------
        # Ensure watchlist exists and get symbols.
        # ------------------------------------------------------------------
        watchlist_id = self.batch_repository.get_or_create_watchlist(
            self.watchlist_name
        )
        symbols = self.batch_repository.get_active_watchlist_symbols(watchlist_id)
        if not symbols and resolution.status == TradingDayStatus.RESOLVED:
            raise BatchRunError(
                f"watchlist '{self.watchlist_name}' has no active symbols"
            )

        # ------------------------------------------------------------------
        # Look for an existing batch before creating a new revision.
        # ------------------------------------------------------------------
        existing_batch = self.batch_repository.find_existing_batch(
            watchlist_id=watchlist_id,
            requested_date=requested_date,
            runner_policy_version=self.RUNNER_POLICY_VERSION,
        )

        if existing_batch is not None:
            # Batch for today already exists -> idempotent replay if done,
            # or re-enter running state if partially done.
            if existing_batch.status in (
                BatchRunStatus.SUCCESS,
                BatchRunStatus.SKIPPED_NON_TRADING_DAY,
                BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE,
            ):
                symbol_runs = self.batch_repository.list_symbol_runs(
                    existing_batch.batch_run_id
                )
                return self._build_summary(
                    existing_batch, resolution, symbol_runs, idempotent_replay=True
                )
            # Otherwise fall through: re-use the existing revision + batch.
            revision_id = existing_batch.watchlist_revision_id
            existing_revision = self.batch_repository.get_revision(revision_id)
            symbols = list(existing_revision.symbols) if existing_revision else symbols
            batch_run = existing_batch
        else:
            # No batch for today yet -> create a new immutable revision.
            if not symbols:
                # Non-trading day with empty watchlist: skip gracefully.
                pass
            revision = (
                self.batch_repository.create_revision(watchlist_id)
                if symbols
                else None
            )
            revision_id = revision.revision_id if revision else ""
            batch_run = self.batch_repository.get_or_create_batch_run(
                revision_id,
                requested_date,
                runner_policy_version=self.RUNNER_POLICY_VERSION,
            )

        return self._execute_batch(
            batch_run, resolution, symbols, requested_date
        )

    def _resume_batch(
        self,
        resume_batch_run_id: str,
        requested_date: date,
        resolution: TradingDayResolution,
    ) -> BatchRunSummary:
        existing = self.batch_repository.get_batch_run(resume_batch_run_id)
        if existing is None:
            raise BatchRunError(
                f"resume_batch_run_id {resume_batch_run_id} not found"
            )
        revision_id = existing.watchlist_revision_id
        existing_revision = self.batch_repository.get_revision(revision_id)
        if existing_revision is None:
            raise BatchRunError(
                f"revision {revision_id} for batch {resume_batch_run_id} not found"
            )
        symbols = list(existing_revision.symbols)
        if existing.status in (
            BatchRunStatus.SUCCESS,
            BatchRunStatus.SKIPPED_NON_TRADING_DAY,
            BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE,
        ):
            symbol_runs = self.batch_repository.list_symbol_runs(resume_batch_run_id)
            return self._build_summary(
                existing, resolution, symbol_runs, idempotent_replay=True
            )
        return self._execute_batch(existing, resolution, symbols, requested_date)

    def _execute_batch(
        self,
        batch_run: DailyBatchRun,
        resolution: TradingDayResolution,
        symbols: list[str],
        requested_date: date,
    ) -> BatchRunSummary:
        # ------------------------------------------------------------------
        # Non-trading day: skip.
        # ------------------------------------------------------------------
        if resolution.status in (
            TradingDayStatus.SKIPPED_NON_TRADING_DAY,
            TradingDayStatus.SKIPPED_NO_NEW_MARKET_DATE,
        ):
            skip_status = (
                BatchRunStatus.SKIPPED_NON_TRADING_DAY
                if resolution.status == TradingDayStatus.SKIPPED_NON_TRADING_DAY
                else BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE
            )
            self.batch_repository.mark_batch_skipped(
                batch_run.batch_run_id,
                skip_status,
                finished_at=self.clock(),
            )
            return BatchRunSummary(
                batch_run_id=batch_run.batch_run_id,
                watchlist_revision_id=batch_run.watchlist_revision_id,
                requested_date=requested_date,
                resolved_market_date=None,
                trading_day_status=resolution.status,
                batch_status=skip_status,
                symbol_results=(),
                idempotent_replay=False,
            )

        if resolution.status == TradingDayStatus.DEFERRED_AWAITING_MARKET_DATA:
            self.batch_repository.mark_batch_skipped(
                batch_run.batch_run_id,
                BatchRunStatus.DEFERRED_AWAITING_MARKET_DATA,
                finished_at=self.clock(),
            )
            return BatchRunSummary(
                batch_run_id=batch_run.batch_run_id,
                watchlist_revision_id=batch_run.watchlist_revision_id,
                requested_date=requested_date,
                resolved_market_date=None,
                trading_day_status=resolution.status,
                batch_status=BatchRunStatus.DEFERRED_AWAITING_MARKET_DATA,
                symbol_results=(),
                idempotent_replay=False,
            )

        # ------------------------------------------------------------------
        # Start / resume the batch.
        # ------------------------------------------------------------------
        resume_running = batch_run.status == BatchRunStatus.RUNNING
        self.batch_repository.start_batch_run(
            batch_run.batch_run_id,
            started_at=self.clock(),
            total_symbols=len(symbols),
            resume_running=resume_running,
        )

        market_date = resolution.resolved_market_date
        assert market_date is not None

        # ------------------------------------------------------------------
        # Per-symbol execution (isolated: one failure never stops others).
        # ------------------------------------------------------------------
        symbol_runs: list[DailySymbolRun] = []
        for sym in symbols:
            symbol_run = self.batch_repository.get_or_create_symbol_run(
                batch_run.batch_run_id, sym
            )

            # Already succeeded -> skip.
            if symbol_run.status == SymbolRunStatus.SUCCESS:
                symbol_runs.append(symbol_run)
                continue

            symbol_run = self.batch_repository.start_symbol_run(
                symbol_run.symbol_run_id, started_at=self.clock()
            )

            try:
                result = self.research_pipeline.run(
                    sym,
                    market_date,
                    market_date,
                    resume_run_id=(
                        symbol_run.pipeline_run_id
                        if symbol_run.attempt_count > 1 and symbol_run.pipeline_run_id
                        else None
                    ),
                )
                symbol_run = self.batch_repository.mark_symbol_success(
                    symbol_run.symbol_run_id,
                    finished_at=self.clock(),
                    pipeline_run_id=result.run_id,
                )
            except Exception as exc:
                safe_msg = self._safe_error_message(exc)
                symbol_run = self.batch_repository.mark_symbol_failed(
                    symbol_run.symbol_run_id,
                    finished_at=self.clock(),
                    error_message=safe_msg,
                )

            symbol_runs.append(symbol_run)

        # ------------------------------------------------------------------
        # Finish batch.
        # ------------------------------------------------------------------
        finished_batch = self.batch_repository.finish_batch_run(
            batch_run.batch_run_id,
            finished_at=self.clock(),
            resolved_market_date=market_date,
        )

        return self._build_summary(
            finished_batch, resolution, symbol_runs, idempotent_replay=False
        )

    @staticmethod
    def _build_summary(
        batch: DailyBatchRun,
        resolution: TradingDayResolution,
        symbol_runs: list[DailySymbolRun],
        *,
        idempotent_replay: bool,
    ) -> BatchRunSummary:
        return BatchRunSummary(
            batch_run_id=batch.batch_run_id,
            watchlist_revision_id=batch.watchlist_revision_id,
            requested_date=batch.requested_date,
            resolved_market_date=batch.resolved_market_date,
            trading_day_status=resolution.status,
            batch_status=batch.status,
            symbol_results=tuple(
                SymbolRunSummary(
                    symbol=sr.symbol,
                    status=sr.status,
                    symbol_run_id=sr.symbol_run_id,
                    pipeline_run_id=sr.pipeline_run_id,
                    error_message=sr.error_message,
                )
                for sr in symbol_runs
            ),
            idempotent_replay=idempotent_replay,
        )

    @staticmethod
    def _safe_error_message(error: Exception) -> str:
        detail = str(error).replace("\r", " ").replace("\n", " ").strip()
        detail = _SECRET_ASSIGNMENT.sub(r"\1=[REDACTED]", detail)
        detail = _BEARER_TOKEN.sub("Bearer [REDACTED]", detail)
        error_type = type(error).__name__
        return error_type if not detail else f"{error_type}: {detail}"[:1000]
