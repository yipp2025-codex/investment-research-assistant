"""Phase 6C EOD scheduler orchestration.

The scheduler owns only operations concerns: a durable invocation row, one
lease per job, a bounded market-readiness wait, and a call to the existing
``scripts/run_daily_batch.py`` entrypoint.  It deliberately does not import or
reimplement the market-data pipeline.  Once the batch CLI has completed, the
existing Phase 6B report service is invoked for successful child runs.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Protocol

from app.market_calendar import (
    MarketCalendar,
    MarketDateEvidence,
    MarketDayState,
    TwseMarketCalendar,
)
from app.providers import TwseMarketDataProvider
from app.providers.base import (
    ProviderError,
    ProviderInvalidPayloadError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)
from app.reports.composition import compose_sqlite_daily_research_report_service
from app.storage import SQLiteOperationsRepository, SQLiteResearchRepository
from app.storage.batch_run import (
    BatchRunStatus,
    SQLiteBatchRunRepository,
    SymbolRunStatus,
)
from app.storage.operations import (
    HealthStatus,
    InvocationStatus,
    OperationLogLevel,
    SchedulerInvocation,
    redact_operation_message,
)


class SchedulerError(RuntimeError):
    """Base class for Phase 6C operation errors."""


class SchedulerConfigurationError(SchedulerError):
    """The scheduler configuration is invalid or attempts a forbidden mode."""


class SchedulerCliError(SchedulerError):
    """The frozen 6A CLI did not return a usable batch checkpoint."""


@dataclass(frozen=True, slots=True)
class MarketReadiness:
    """Read-only result of asking whether the requested EOD data is published."""

    status: str
    latest_market_date: date | None
    message: str = ""

    READY = "ready"
    DEFERRED = "deferred"
    TEMPORARY_FAILURE = "temporary_failure"
    PERMANENT_FAILURE = "permanent_failure"


class MarketReadinessProbe(Protocol):
    def check(self, target_date: date, *, timeout_seconds: float) -> MarketReadiness:
        """Check market readiness without writing to the research database."""


class TwseMarketReadinessProbe:
    """Probe the already-approved TWSE read-only provider for EOD readiness."""

    def __init__(
        self,
        provider: TwseMarketDataProvider | None = None,
        *,
        symbol: str = "2330",
        lookback_calendar_days: int = 14,
        market_calendar: MarketCalendar | None = None,
    ) -> None:
        self.provider = provider or TwseMarketDataProvider()
        self.symbol = symbol.strip().upper()
        self.lookback_calendar_days = lookback_calendar_days
        self.market_calendar = market_calendar or TwseMarketCalendar()
        if not self.symbol:
            raise ValueError("probe symbol must not be empty")
        if lookback_calendar_days < 1:
            raise ValueError("lookback_calendar_days must be positive")

    def check(self, target_date: date, *, timeout_seconds: float) -> MarketReadiness:
        try:
            latest = self._latest_published_date(
                target_date,
                timeout_seconds=timeout_seconds,
            )
        except (ProviderTimeoutError, ProviderTemporaryError) as exc:
            return MarketReadiness(
                status=MarketReadiness.TEMPORARY_FAILURE,
                latest_market_date=None,
                message=type(exc).__name__,
            )
        except (ProviderPermanentError, ProviderInvalidPayloadError, ProviderError) as exc:
            return MarketReadiness(
                status=MarketReadiness.PERMANENT_FAILURE,
                latest_market_date=None,
                message=type(exc).__name__,
            )

        evidence = self.market_calendar.classify(target_date, latest)
        return _readiness_from_market_evidence(evidence)

    def _latest_published_date(
        self,
        target_date: date,
        *,
        timeout_seconds: float,
    ) -> date | None:
        """Fetch and extract publication evidence without deciding readiness."""
        batch = self.provider.fetch_market_data(
            self.symbol,
            target_date - timedelta(days=self.lookback_calendar_days),
            target_date,
            timeout_seconds=timeout_seconds,
        )
        candidates: list[date] = []
        if batch.market_date is not None:
            candidates.append(batch.market_date)
        for item in batch.daily_prices:
            raw = item.get("trade_date")
            if raw is None:
                continue
            try:
                candidates.append(date.fromisoformat(str(raw)))
            except ValueError:
                continue
        return max(candidates, default=None)


class _AlwaysReadyProbe:
    def check(self, target_date: date, *, timeout_seconds: float) -> MarketReadiness:
        del timeout_seconds
        return MarketReadiness(
            status=MarketReadiness.READY,
            latest_market_date=target_date,
            message="synthetic provider readiness",
        )


_READY_MARKET_STATES = {
    MarketDayState.LATEST_ON_REQUESTED,
    MarketDayState.LATEST_AFTER_REQUESTED,
}


def _readiness_from_market_evidence(
    evidence: MarketDateEvidence,
) -> MarketReadiness:
    """Map pure market-date evidence to the frozen Phase 6C readiness API."""
    ready = evidence.state in _READY_MARKET_STATES
    if evidence.state is MarketDayState.WEEKEND:
        # Direct probe calls historically compared dates without a weekend
        # special case. SchedulerRunner still short-circuits before the probe.
        ready = (
            evidence.latest_published_date is not None
            and evidence.latest_published_date >= evidence.requested_date
        )
    if ready:
        return MarketReadiness(
            status=MarketReadiness.READY,
            latest_market_date=evidence.latest_published_date,
            message="market data is available",
        )
    return MarketReadiness(
        status=MarketReadiness.DEFERRED,
        latest_market_date=evidence.latest_published_date,
        message="market data has not reached the requested date",
    )


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    """EOD-only scheduler configuration; all operational timings are explicit."""

    job_name: str = "investment-research-eod"
    watchlist: str = "default"
    provider: str = "twse"
    mode: str = "eod"
    schedule_time: str = "18:00"
    grace_period_seconds: int = 1800
    max_deferred_attempts: int = 3
    retry_backoff_seconds: float = 60.0
    retry_backoff_multiplier: float = 2.0
    lease_ttl_seconds: int = 1800
    provider_timeout_seconds: float = 10.0
    cli_timeout_seconds: float = 900.0
    project_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parents[2]
    )
    database_path: Path | None = None
    python_path: str = sys.executable
    env_file: Path | None = None
    probe_symbol: str = "2330"

    def __post_init__(self) -> None:
        if not self.job_name.strip():
            raise SchedulerConfigurationError("job_name must not be empty")
        if not self.watchlist.strip():
            raise SchedulerConfigurationError("watchlist must not be empty")
        if self.provider not in {"twse", "mock"}:
            raise SchedulerConfigurationError(
                "Phase 6C scheduler provider must be twse or mock"
            )
        if self.mode != "eod":
            raise SchedulerConfigurationError("Phase 6C supports EOD mode only")
        _parse_schedule_time(self.schedule_time)
        if self.grace_period_seconds < 0:
            raise SchedulerConfigurationError("grace_period_seconds must be >= 0")
        if self.max_deferred_attempts < 1:
            raise SchedulerConfigurationError("max_deferred_attempts must be >= 1")
        if self.retry_backoff_seconds < 0:
            raise SchedulerConfigurationError("retry_backoff_seconds must be >= 0")
        if self.retry_backoff_multiplier < 1:
            raise SchedulerConfigurationError(
                "retry_backoff_multiplier must be >= 1"
            )
        if self.lease_ttl_seconds <= 0:
            raise SchedulerConfigurationError("lease_ttl_seconds must be positive")
        if self.provider_timeout_seconds <= 0 or self.cli_timeout_seconds <= 0:
            raise SchedulerConfigurationError("timeouts must be positive")
        if not self.python_path.strip():
            raise SchedulerConfigurationError("python_path must not be empty")
        if not self.probe_symbol.strip():
            raise SchedulerConfigurationError("probe_symbol must not be empty")

    @property
    def effective_database_path(self) -> Path:
        return self.database_path or (self.project_root / "data" / "research.db")

    @property
    def effective_env_file(self) -> Path:
        return self.env_file or (self.project_root / ".env")

    @property
    def lease_key(self) -> str:
        return f"eod:{self.job_name.strip()}"


@dataclass(frozen=True, slots=True)
class SchedulerRunResult:
    invocation_id: str
    operation_status: str
    health_status: str
    requested_date: date
    batch_run_id: str | None
    idempotent_replay: bool
    probe_attempts: int
    report_failures: tuple[str, ...]
    message: str


@dataclass(frozen=True, slots=True)
class _CliResult:
    returncode: int
    stdout: str
    stderr: str


_BATCH_ID_RE = re.compile(r"(?im)^Batch run:\s*([^\s]+)")
_IDEMPOTENT_RE = re.compile(r"(?im)^Idempotent:\s*(true|false)")


def _parse_schedule_time(value: str) -> dt_time:
    try:
        hour_text, minute_text = value.strip().split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except (AttributeError, ValueError) as exc:
        raise SchedulerConfigurationError(
            "schedule_time must use HH:MM 24-hour format"
        ) from exc
    if hour not in range(24) or minute not in range(60):
        raise SchedulerConfigurationError(
            "schedule_time must use HH:MM 24-hour format"
        )
    return dt_time(hour=hour, minute=minute)


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


def _default_executor(
    command: Sequence[str],
    cwd: Path,
    env: Mapping[str, str],
    timeout_seconds: float,
) -> _CliResult:
    try:
        completed = subprocess.run(
            list(command),
            cwd=str(cwd),
            env=dict(env),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise SchedulerCliError("Phase 6A CLI timed out") from exc
    except OSError as exc:
        raise SchedulerCliError("Phase 6A CLI could not be started") from exc
    return _CliResult(
        returncode=int(completed.returncode),
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )


class SchedulerRunner:
    """Run one bounded, lease-protected EOD operation."""

    def __init__(
        self,
        config: SchedulerConfig,
        *,
        repository: SQLiteResearchRepository | None = None,
        operations_repository: SQLiteOperationsRepository | None = None,
        readiness_probe: MarketReadinessProbe | None = None,
        market_calendar: MarketCalendar | None = None,
        execute_cli: Callable[
            [Sequence[str], Path, Mapping[str, str], float], _CliResult
        ] = _default_executor,
        clock: Callable[[], datetime] = _default_clock,
        sleep: Callable[[float], None] = time.sleep,
        process_id: int | None = None,
    ) -> None:
        self.config = config
        self.repository = repository or SQLiteResearchRepository(
            config.effective_database_path
        )
        self.operations = operations_repository or SQLiteOperationsRepository(
            self.repository
        )
        self.market_calendar = market_calendar or TwseMarketCalendar()
        self.readiness_probe = readiness_probe or (
            _AlwaysReadyProbe()
            if config.provider == "mock"
            else TwseMarketReadinessProbe(
                symbol=config.probe_symbol,
                market_calendar=self.market_calendar,
            )
        )
        self.execute_cli = execute_cli
        self.clock = clock
        self.sleep = sleep
        self.process_id = process_id if process_id is not None else os.getpid()

    def run_once(
        self,
        *,
        trigger: str = "manual",
        target_date: date | None = None,
        scheduled_for: datetime | None = None,
    ) -> SchedulerRunResult:
        """Execute a manual or Windows-scheduled EOD invocation.

        The scheduler never imports the 6A runner as a second implementation;
        it invokes the existing CLI and then reads its durable checkpoint.
        """

        if trigger not in {"manual", "scheduled"}:
            raise SchedulerConfigurationError("trigger must be manual or scheduled")
        requested = target_date or self.clock().astimezone(
            self.market_calendar.timezone
        ).date()
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise SchedulerConfigurationError("scheduler clock must be timezone-aware")
        scheduled_at = scheduled_for or self._scheduled_for(requested, trigger, now)
        self.operations.initialize()
        invocation = self.operations.create_invocation(
            job_name=self.config.job_name,
            trigger=trigger,
            mode=self.config.mode,
            requested_date=requested,
            scheduled_for=scheduled_at,
            process_id=self.process_id,
            created_at=now,
        )
        lease_result = self.operations.acquire_lease(
            lease_key=self.config.lease_key,
            invocation_id=invocation.invocation_id,
            acquired_at=now,
            expires_at=now + timedelta(seconds=self.config.lease_ttl_seconds),
        )
        if not lease_result.acquired:
            self.operations.log(
                invocation.invocation_id,
                event="overlap_skipped",
                message="another scheduler invocation owns the EOD lease",
                level=OperationLogLevel.WARNING,
                created_at=now,
            )
            finished = self.operations.finish_invocation(
                invocation.invocation_id,
                status=InvocationStatus.SKIP,
                finished_at=now,
                error_message="overlapping scheduler invocation skipped",
            )
            return self._result(
                finished,
                batch_run_id=None,
                idempotent_replay=False,
                probe_attempts=0,
                report_failures=(),
                message="overlapping scheduler invocation skipped",
            )

        lease = lease_result.lease
        assert lease is not None
        if lease_result.expired_invocation_id:
            self.operations.log(
                lease_result.expired_invocation_id,
                event="lease_expired",
                message="previous operation lease expired; its batch can be resumed",
                level=OperationLogLevel.WARNING,
                created_at=now,
            )
        self.operations.log(
            invocation.invocation_id,
            event="lease_acquired",
            message="EOD operation lease acquired",
            created_at=now,
        )
        self.operations.start_invocation(
            invocation.invocation_id, started_at=now, attempt_count=1
        )

        try:
            date_evidence = self.market_calendar.classify(requested, None)
            if date_evidence.state is MarketDayState.WEEKEND:
                return self._finish(
                    invocation.invocation_id,
                    lease.lease_id,
                    status=InvocationStatus.SKIP,
                    message="weekend is not a trading day",
                    batch_run_id=None,
                    idempotent_replay=False,
                    probe_attempts=0,
                    report_failures=(),
                )

            readiness, probe_attempts = self._await_market_data(
                requested,
                invocation_id=invocation.invocation_id,
                trigger=trigger,
                scheduled_for=scheduled_at,
            )
            if readiness.status != MarketReadiness.READY:
                if readiness.status == MarketReadiness.PERMANENT_FAILURE:
                    status = InvocationStatus.HARD_FAILURE
                elif readiness.status == MarketReadiness.DEFERRED:
                    status = InvocationStatus.DEFERRED
                else:
                    status = InvocationStatus.WARNING
                retry_at = (
                    None
                    if readiness.status == MarketReadiness.PERMANENT_FAILURE
                    else self.clock()
                    + timedelta(seconds=self.config.retry_backoff_seconds)
                )
                return self._finish(
                    invocation.invocation_id,
                    lease.lease_id,
                    status=status,
                    message=readiness.message or readiness.status,
                    batch_run_id=None,
                    idempotent_replay=False,
                    probe_attempts=probe_attempts,
                    report_failures=(),
                    next_retry_at=retry_at,
                )

            self.operations.heartbeat(
                lease_key=self.config.lease_key,
                lease_id=lease.lease_id,
                heartbeat_at=self.clock(),
                expires_at=self.clock()
                + timedelta(seconds=self.config.lease_ttl_seconds),
            )
            batch_repo = SQLiteBatchRunRepository(self.repository)
            batch_repo.initialize()
            watchlist_id = batch_repo.get_or_create_watchlist(self.config.watchlist)
            existing = batch_repo.find_existing_batch(
                watchlist_id=watchlist_id,
                requested_date=requested,
                runner_policy_version=SQLiteBatchRunRepository.RUNNER_POLICY_VERSION,
            )
            resume_batch_id = None
            if existing is not None and existing.status not in {
                BatchRunStatus.SUCCESS,
                BatchRunStatus.SKIPPED_NON_TRADING_DAY,
                BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE,
            }:
                resume_batch_id = existing.batch_run_id

            command = self.build_6a_command(
                requested,
                resume_batch_id=resume_batch_id,
            )
            cli_result = self.execute_cli(
                command,
                self.config.project_root,
                self._child_environment(),
                self.config.cli_timeout_seconds,
            )
            batch_id = _parse_batch_id(cli_result.stdout)
            if batch_id is None and existing is not None:
                batch_id = existing.batch_run_id
            if batch_id is None:
                detail = "Phase 6A CLI returned no batch checkpoint"
                if cli_result.returncode != 0:
                    detail = f"{detail} ({cli_result.returncode})"
                raise SchedulerCliError(detail)

            self.operations.set_batch_run_id(
                invocation.invocation_id, batch_id, updated_at=self.clock()
            )
            batch = batch_repo.get_batch_run(batch_id)
            if batch is None:
                raise SchedulerCliError("Phase 6A batch checkpoint was not found")

            idempotent = _parse_idempotent(cli_result.stdout) or (
                existing is not None and existing.status == BatchRunStatus.SUCCESS
            )
            report_failures = self._generate_reports(batch_id, batch_repo)
            operation_status, message = self._classify_batch(
                batch.status,
                report_failures=report_failures,
                cli_returncode=cli_result.returncode,
            )
            return self._finish(
                invocation.invocation_id,
                lease.lease_id,
                status=operation_status,
                message=message,
                batch_run_id=batch_id,
                idempotent_replay=idempotent,
                probe_attempts=probe_attempts,
                report_failures=report_failures,
            )
        except SchedulerError as exc:
            return self._finish(
                invocation.invocation_id,
                lease.lease_id,
                status=InvocationStatus.HARD_FAILURE,
                message=str(exc),
                batch_run_id=None,
                idempotent_replay=False,
                probe_attempts=0,
                report_failures=(),
            )
        except Exception as exc:  # fail closed at the operations boundary
            return self._finish(
                invocation.invocation_id,
                lease.lease_id,
                status=InvocationStatus.HARD_FAILURE,
                message=f"{type(exc).__name__}: {exc}",
                batch_run_id=None,
                idempotent_replay=False,
                probe_attempts=0,
                report_failures=(),
            )

    def build_6a_command(
        self, requested_date: date, *, resume_batch_id: str | None = None
    ) -> tuple[str, ...]:
        """Build the only child command used by scheduled and manual triggers."""

        command = [
            self.config.python_path,
            str(self.config.project_root / "scripts" / "run_daily_batch.py"),
            "--date",
            requested_date.isoformat(),
            "--watchlist",
            self.config.watchlist,
            "--provider",
            self.config.provider,
            "--provider-timeout-seconds",
            str(self.config.provider_timeout_seconds),
            "--skip-trading-day-check",
        ]
        if self.config.env_file is not None:
            command.extend(["--env-file", str(self.config.env_file)])
        if resume_batch_id is not None:
            command.extend(["--resume-batch-id", resume_batch_id])
        return tuple(command)

    def _await_market_data(
        self,
        requested: date,
        *,
        invocation_id: str,
        trigger: str,
        scheduled_for: datetime,
    ) -> tuple[MarketReadiness, int]:
        attempts = 0
        deadline = (
            scheduled_for + timedelta(seconds=self.config.grace_period_seconds)
            if trigger == "scheduled"
            else None
        )
        last = MarketReadiness(
            status=MarketReadiness.DEFERRED,
            latest_market_date=None,
            message="market data is not ready",
        )
        while attempts < self.config.max_deferred_attempts:
            attempts += 1
            last = self.readiness_probe.check(
                requested, timeout_seconds=self.config.provider_timeout_seconds
            )
            if last.status == MarketReadiness.READY:
                self.operations.log(
                    invocation_id,
                    event="market_data_ready",
                    message="EOD market data readiness confirmed",
                    created_at=self.clock(),
                )
                return last, attempts
            if last.status == MarketReadiness.PERMANENT_FAILURE:
                self.operations.log(
                    invocation_id,
                    event="market_data_probe_failed",
                    message="permanent market-readiness failure",
                    level=OperationLogLevel.ERROR,
                    created_at=self.clock(),
                )
                return last, attempts
            if attempts >= self.config.max_deferred_attempts:
                break
            now = self.clock()
            if deadline is not None and now >= deadline:
                break
            delay = self.config.retry_backoff_seconds * (
                self.config.retry_backoff_multiplier ** (attempts - 1)
            )
            if deadline is not None:
                remaining = (deadline - now).total_seconds()
                if remaining <= 0:
                    break
                delay = min(delay, remaining)
            self.operations.log(
                invocation_id,
                event="market_data_deferred_retry",
                message=f"bounded readiness retry {attempts + 1}/{self.config.max_deferred_attempts}",
                level=OperationLogLevel.WARNING,
                created_at=now,
            )
            self.sleep(delay)
        return last, attempts

    def _generate_reports(
        self, batch_run_id: str, batch_repo: SQLiteBatchRunRepository
    ) -> tuple[str, ...]:
        report_service = compose_sqlite_daily_research_report_service(self.repository)
        failures: list[str] = []
        for symbol_run in batch_repo.list_symbol_runs(batch_run_id):
            if symbol_run.status != SymbolRunStatus.SUCCESS:
                continue
            try:
                report_service.generate_for_batch_symbol(batch_run_id, symbol_run.symbol)
            except Exception as exc:
                # Only the symbol name and exception type are retained; the
                # repository's redaction is a second line of defence.
                failures.append(f"{symbol_run.symbol}:{type(exc).__name__}")
        return tuple(failures)

    @staticmethod
    def _classify_batch(
        status: BatchRunStatus,
        *,
        report_failures: Sequence[str],
        cli_returncode: int,
    ) -> tuple[str, str]:
        if status == BatchRunStatus.SUCCESS:
            if report_failures:
                return (
                    InvocationStatus.WARNING,
                    "batch succeeded; report rendering needs retry",
                )
            return InvocationStatus.SUCCESS, "EOD batch and reports completed"
        if status == BatchRunStatus.PARTIAL_SUCCESS:
            return (
                InvocationStatus.PARTIAL_FAILURE,
                "batch partial_success; only failed symbols remain resumable",
            )
        if status == BatchRunStatus.FAILED:
            return InvocationStatus.HARD_FAILURE, "EOD batch failed"
        if status in {
            BatchRunStatus.SKIPPED_NON_TRADING_DAY,
            BatchRunStatus.SKIPPED_NO_NEW_MARKET_DATE,
        }:
            return InvocationStatus.SKIP, "batch skipped"
        if status == BatchRunStatus.DEFERRED_AWAITING_MARKET_DATA:
            return InvocationStatus.DEFERRED, "batch deferred awaiting market data"
        del cli_returncode
        return InvocationStatus.WARNING, f"batch status {status.value}"

    def _finish(
        self,
        invocation_id: str,
        lease_id: str,
        *,
        status: str,
        message: str,
        batch_run_id: str | None,
        idempotent_replay: bool,
        probe_attempts: int,
        report_failures: Sequence[str],
        next_retry_at: datetime | None = None,
    ) -> SchedulerRunResult:
        now = self.clock()
        if batch_run_id is not None:
            self.operations.set_batch_run_id(invocation_id, batch_run_id, updated_at=now)
        level = (
            OperationLogLevel.INFO
            if status in {InvocationStatus.SUCCESS, InvocationStatus.SKIP}
            else (
                OperationLogLevel.ERROR
                if status == InvocationStatus.HARD_FAILURE
                else OperationLogLevel.WARNING
            )
        )
        self.operations.log(
            invocation_id,
            event="invocation_finished",
            message=message,
            level=level,
            created_at=now,
        )
        finished = self.operations.finish_invocation(
            invocation_id,
            status=status,
            finished_at=now,
            error_message=None if status == InvocationStatus.SUCCESS else message,
            next_retry_at=next_retry_at,
        )
        self.operations.release_lease(
            lease_key=self.config.lease_key,
            lease_id=lease_id,
            released_at=self.clock(),
        )
        return self._result(
            finished,
            batch_run_id=batch_run_id,
            idempotent_replay=idempotent_replay,
            probe_attempts=probe_attempts,
            report_failures=tuple(report_failures),
            message=message,
        )

    def _result(
        self,
        invocation: SchedulerInvocation,
        *,
        batch_run_id: str | None,
        idempotent_replay: bool,
        probe_attempts: int,
        report_failures: Sequence[str],
        message: str,
    ) -> SchedulerRunResult:
        return SchedulerRunResult(
            invocation_id=invocation.invocation_id,
            operation_status=invocation.status,
            health_status=_health_status(invocation.status),
            requested_date=invocation.requested_date,
            batch_run_id=batch_run_id or invocation.batch_run_id,
            idempotent_replay=idempotent_replay,
            probe_attempts=probe_attempts,
            report_failures=tuple(report_failures),
            message=redact_operation_message(message),
        )

    def _child_environment(self) -> dict[str, str]:
        environment = dict(os.environ)
        environment["IRA_DATABASE_PATH"] = str(self.config.effective_database_path)
        return environment

    def _scheduled_for(
        self, requested: date, trigger: str, now: datetime
    ) -> datetime:
        if trigger == "manual":
            return now
        schedule = _parse_schedule_time(self.config.schedule_time)
        return datetime.combine(
            requested,
            schedule,
            tzinfo=self.market_calendar.timezone,
        )


def _parse_batch_id(output: str) -> str | None:
    match = _BATCH_ID_RE.search(output or "")
    return None if match is None else match.group(1).strip()


def _parse_idempotent(output: str) -> bool:
    match = _IDEMPOTENT_RE.search(output or "")
    return bool(match and match.group(1).lower() == "true")


def _health_status(status: str) -> str:
    if status == InvocationStatus.SUCCESS:
        return HealthStatus.SUCCESS
    if status in {
        InvocationStatus.WARNING,
        InvocationStatus.RUNNING,
        InvocationStatus.PENDING,
        InvocationStatus.DEFERRED,
    }:
        return HealthStatus.WARNING
    if status == InvocationStatus.PARTIAL_FAILURE:
        return HealthStatus.PARTIAL_FAILURE
    if status == InvocationStatus.HARD_FAILURE:
        return HealthStatus.HARD_FAILURE
    if status == InvocationStatus.SKIP:
        return HealthStatus.SKIP
    return HealthStatus.WARNING


def result_as_json(result: SchedulerRunResult) -> str:
    """Safe CLI representation; command/env/config values are intentionally absent."""

    return json.dumps(
        {
            "invocation_id": result.invocation_id,
            "operation_status": result.operation_status,
            "health_status": result.health_status,
            "requested_date": result.requested_date.isoformat(),
            "batch_run_id": result.batch_run_id,
            "idempotent_replay": result.idempotent_replay,
            "probe_attempts": result.probe_attempts,
            "report_failures": list(result.report_failures),
            "message": result.message,
        },
        ensure_ascii=False,
        sort_keys=True,
    )


__all__ = [
    "MarketReadiness",
    "SchedulerCliError",
    "SchedulerConfig",
    "SchedulerConfigurationError",
    "SchedulerError",
    "SchedulerRunResult",
    "SchedulerRunner",
    "TwseMarketReadinessProbe",
    "result_as_json",
]
