"""S6B application-level Daily Runner.

The runner owns market-date gating, one logical-job lock, and sequencing of
injected frozen S5 and S6A contracts.  It does not contain Universe, Stage 1,
Stage 2, M9, ranking, persistence, or report rendering logic.
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import importlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Callable, Protocol, TypeAlias
from zoneinfo import ZoneInfo

if os.name == "nt":
    import msvcrt  # type: ignore[import-not-found]
else:
    import fcntl  # type: ignore[import-not-found]

from app.market_calendar import (
    MarketCalendar,
    MarketDayState,
    TwseMarketCalendar,
)
from app.reporting.screener_report import (
    ScreenerReportArtifactWriter,
    ScreenerReportCollisionError,
    ScreenerReportGeneration,
    ScreenerReportGenerator,
    ScreenerReportInputError,
    ScreenerReportWriteResult,
)
from app.storage.screener_replay import ScreenerReplayIntegrityError


RUNNER_CONTRACT_VERSION = "s6b-daily-runner-v1"
LOCK_CONTRACT_VERSION = "s6b-daily-lock-v1"
EXECUTION_SUCCESS = "success"
EXECUTION_SUCCESS_REPLAY = "success_replay"
EXECUTION_SKIPPED_NON_MARKET_DAY = "skipped_non_market_day"
EXECUTION_ALREADY_RUNNING = "already_running"
EXECUTION_FAILED = "failed"

MARKET_DAY = "market_day"
NON_MARKET_DAY = "non_market_day"
MARKET_DAY_UNKNOWN = "unknown"

LOCK_NOT_ACQUIRED = "not_acquired"
LOCK_ACQUIRED = "acquired"
LOCK_ALREADY_RUNNING = "already_running"
LOCK_RELEASED = "released"
LOCK_RELEASE_FAILED = "release_failed"

EXIT_SUCCESS = 0
EXIT_FAILURE = 1

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TAIPEI = ZoneInfo("Asia/Taipei")
JsonScalar: TypeAlias = str | int | float | bool | None


class DailyRunnerError(RuntimeError):
    """Base S6B runner error."""


class DailyRunnerConfigurationError(DailyRunnerError):
    """Explicit runner configuration is missing or invalid."""


class DailyRunnerLockError(DailyRunnerError):
    """The lock primitive could not be initialized or released safely."""


class MarketDatePolicy(Protocol):
    def resolve(self, target_market_date: date) -> "MarketDateDecision":
        """Resolve one explicit date without guessing."""


class ScreenerInvoker(Protocol):
    def __call__(self, target_market_date: date) -> object:
        """Invoke the already-composed frozen S5 orchestration."""


class ReportGenerator(Protocol):
    def generate(self, screener_run_id: str) -> ScreenerReportGeneration:
        """Generate S6A derived report content for one successful run."""


class ReportWriter(Protocol):
    def write(
        self,
        generation: ScreenerReportGeneration,
        *,
        output_directory: str | Path | None = None,
        json_path: str | Path | None = None,
        markdown_path: str | Path | None = None,
    ) -> ScreenerReportWriteResult:
        """Write or verify S6A derived artifacts."""


@dataclass(frozen=True, slots=True)
class MarketDateDecision:
    target_market_date: date
    status: str
    evidence_state: str
    latest_published_date: date | None
    source: str

    def __post_init__(self) -> None:
        if not isinstance(self.target_market_date, date):
            raise DailyRunnerConfigurationError("target_market_date must be a date")
        if self.status not in {MARKET_DAY, NON_MARKET_DAY, MARKET_DAY_UNKNOWN}:
            raise DailyRunnerConfigurationError("unsupported market-date status")
        if not isinstance(self.evidence_state, str) or not self.evidence_state:
            raise DailyRunnerConfigurationError("evidence_state must not be blank")
        if not isinstance(self.source, str) or not self.source:
            raise DailyRunnerConfigurationError("market-date source must not be blank")


class DeterministicMarketDatePolicy:
    """Strict adapter over the frozen TWSE evidence classifier.

    A weekday is a market day only when caller-supplied authoritative evidence
    says the requested date itself was published.  A weekend or an explicit
    non-market date is skipped.  Latest-before, latest-unavailable, and
    latest-after evidence remain unknown because this contract does not carry
    an authoritative holiday/session calendar.
    """

    def __init__(
        self,
        *,
        calendar: MarketCalendar | None = None,
        latest_published_date: date | None = None,
        latest_date_provider: Callable[[date], date | None] | None = None,
        explicit_non_market_dates: frozenset[date] = frozenset(),
        evidence_source: str = "explicit_twse_derived_evidence",
    ) -> None:
        if latest_published_date is not None and latest_date_provider is not None:
            raise DailyRunnerConfigurationError(
                "latest_published_date and latest_date_provider are mutually exclusive"
            )
        if not isinstance(explicit_non_market_dates, frozenset):
            raise TypeError("explicit_non_market_dates must be a frozenset")
        if not evidence_source.strip():
            raise DailyRunnerConfigurationError("evidence_source must not be blank")
        self.calendar = calendar or TwseMarketCalendar()
        self.latest_published_date = latest_published_date
        self.latest_date_provider = latest_date_provider
        self.explicit_non_market_dates = explicit_non_market_dates
        self.evidence_source = evidence_source.strip()

    def resolve(self, target_market_date: date) -> MarketDateDecision:
        _require_date(target_market_date, "target_market_date")
        if target_market_date in self.explicit_non_market_dates:
            return MarketDateDecision(
                target_market_date=target_market_date,
                status=NON_MARKET_DAY,
                evidence_state="explicit_non_market",
                latest_published_date=None,
                source=self.evidence_source,
            )
        latest = (
            self.latest_date_provider(target_market_date)
            if self.latest_date_provider is not None
            else self.latest_published_date
        )
        evidence = self.calendar.classify(target_market_date, latest)
        if evidence.state is MarketDayState.WEEKEND:
            status = NON_MARKET_DAY
        elif evidence.state is MarketDayState.LATEST_ON_REQUESTED:
            status = MARKET_DAY
        else:
            status = MARKET_DAY_UNKNOWN
        return MarketDateDecision(
            target_market_date=target_market_date,
            status=status,
            evidence_state=evidence.state.value,
            latest_published_date=evidence.latest_published_date,
            source=self.evidence_source,
        )


@dataclass(frozen=True, slots=True)
class DailyRunLockIdentity:
    target_market_date: date
    database_path: Path
    lock_contract_version: str
    identity_sha256: str


class DailyRunLock:
    """Per-date/per-database OS advisory lock with crash-safe release.

    The lock file is retained as a diagnostic metadata carrier.  Ownership is
    determined only by the OS lock, which is released by the operating system
    when a process crashes.  Stale metadata is never used to delete or reject
    a lock, so PID reuse cannot create a false recovery path.
    """

    def __init__(
        self,
        database_path: str | Path,
        target_market_date: date,
        *,
        lock_directory: str | Path | None = None,
        runner_contract_version: str = RUNNER_CONTRACT_VERSION,
    ) -> None:
        database = _absolute_path(database_path, "database_path")
        target = _require_date(target_market_date, "target_market_date")
        directory = (
            database.parent / ".s6b-locks"
            if lock_directory is None
            else _absolute_path(lock_directory, "lock_directory")
        )
        identity = {
            "lock_contract_version": LOCK_CONTRACT_VERSION,
            "runner_contract_version": runner_contract_version,
            "target_market_date": target.isoformat(),
            "database_path": str(database),
        }
        identity_sha256 = _canonical_sha256(identity)
        self.identity = DailyRunLockIdentity(
            target_market_date=target,
            database_path=database,
            lock_contract_version=LOCK_CONTRACT_VERSION,
            identity_sha256=identity_sha256,
        )
        self.lock_directory = directory
        self.lock_path = directory / f"daily-run-{identity_sha256}.lock"
        self._stream = None
        self.owner_token = uuid.uuid4().hex

    @property
    def identity_sha256(self) -> str:
        return self.identity.identity_sha256

    def try_acquire(self) -> bool:
        if self._stream is not None:
            raise DailyRunnerLockError("lock is already held by this object")
        self.lock_directory.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(
                self.lock_path,
                os.O_RDWR | os.O_CREAT,
                0o600,
            )
            stream = os.fdopen(descriptor, "r+b", buffering=0)
        except OSError as error:
            raise DailyRunnerLockError("lock file could not be opened") from error
        try:
            stream.seek(0, os.SEEK_END)
            if stream.tell() == 0:
                stream.write(b"\0")
                stream.flush()
            stream.seek(0)
            if not _try_os_lock(stream):
                stream.close()
                return False
            metadata = {
                "lock_contract_version": LOCK_CONTRACT_VERSION,
                "owner_token": self.owner_token,
                "pid": os.getpid(),
                "target_market_date": self.identity.target_market_date.isoformat(),
                "database_path": str(self.identity.database_path),
            }
            payload = _canonical_json(metadata).encode("utf-8")
            stream.seek(0)
            stream.write(payload)
            stream.truncate()
            stream.flush()
            os.fsync(stream.fileno())
            self._stream = stream
            return True
        except BaseException:
            try:
                _unlock_os_lock(stream)
            except OSError:
                pass
            stream.close()
            raise

    def release(self) -> None:
        stream = self._stream
        if stream is None:
            return
        self._stream = None
        try:
            _unlock_os_lock(stream)
        except OSError as error:
            stream.close()
            raise DailyRunnerLockError("OS lock could not be released") from error
        finally:
            stream.close()

    def owner_metadata(self) -> dict[str, object] | None:
        try:
            payload = self.lock_path.read_text(encoding="utf-8")
            value = json.loads(payload)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None


def _try_os_lock(stream: object) -> bool:
    if os.name == "nt":
        stream.seek(0)
        try:
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK, 13, 33}:
                return False
            raise DailyRunnerLockError("OS lock could not be acquired") from error
        return True
    stream.seek(0)
    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        if error.errno in {errno.EACCES, errno.EAGAIN}:
            return False
        raise DailyRunnerLockError("OS lock could not be acquired") from error
    return True


def _unlock_os_lock(stream: object) -> None:
    stream.seek(0)
    if os.name == "nt":
        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


@dataclass(frozen=True, slots=True)
class DailyRunnerResult:
    target_market_date: date
    execution_status: str
    market_day_status: str
    market_date_evidence_state: str
    latest_published_date: date | None
    lock_status: str
    lock_identity_sha256: str
    screener_run_id: str | None
    s5_status: str | None
    s5_replayed: bool | None
    report_sha256: str | None
    report_json_path: Path | None
    report_markdown_path: Path | None
    report_no_op: bool | None
    error_code: str | None
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    lock_acquisition_seconds: float | None = None
    s5_duration_seconds: float | None = None
    s6a_duration_seconds: float | None = None

    def __post_init__(self) -> None:
        _require_date(self.target_market_date, "target_market_date")
        if self.execution_status not in {
            EXECUTION_SUCCESS,
            EXECUTION_SUCCESS_REPLAY,
            EXECUTION_SKIPPED_NON_MARKET_DAY,
            EXECUTION_ALREADY_RUNNING,
            EXECUTION_FAILED,
        }:
            raise DailyRunnerConfigurationError("unsupported execution_status")
        if self.market_day_status not in {
            MARKET_DAY,
            NON_MARKET_DAY,
            MARKET_DAY_UNKNOWN,
        }:
            raise DailyRunnerConfigurationError("unsupported market_day_status")
        if self.lock_status not in {
            LOCK_NOT_ACQUIRED,
            LOCK_ACQUIRED,
            LOCK_ALREADY_RUNNING,
            LOCK_RELEASED,
            LOCK_RELEASE_FAILED,
        }:
            raise DailyRunnerConfigurationError("unsupported lock_status")
        _require_sha256(self.lock_identity_sha256, "lock_identity_sha256")
        if self.screener_run_id is not None:
            _require_sha256(self.screener_run_id, "screener_run_id")
        if self.report_sha256 is not None:
            _require_sha256(self.report_sha256, "report_sha256")
        if self.execution_status in {EXECUTION_SUCCESS, EXECUTION_SUCCESS_REPLAY}:
            if self.screener_run_id is None or self.report_sha256 is None:
                raise DailyRunnerConfigurationError(
                    "successful runner result requires screener and report hashes"
                )
            if self.report_json_path is None or self.report_markdown_path is None:
                raise DailyRunnerConfigurationError(
                    "successful runner result requires report paths"
                )
            if self.error_code is not None:
                raise DailyRunnerConfigurationError(
                    "successful runner result cannot expose an error"
                )
        if self.execution_status == EXECUTION_SKIPPED_NON_MARKET_DAY:
            if self.market_day_status != NON_MARKET_DAY or self.error_code is not None:
                raise DailyRunnerConfigurationError("non-market skip result is invalid")
        if self.execution_status == EXECUTION_ALREADY_RUNNING:
            if self.lock_status != LOCK_ALREADY_RUNNING:
                raise DailyRunnerConfigurationError("already-running lock status is invalid")
        if self.duration_seconds < 0:
            raise DailyRunnerConfigurationError("duration_seconds must be non-negative")
        for field_name in (
            "lock_acquisition_seconds",
            "s5_duration_seconds",
            "s6a_duration_seconds",
        ):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise DailyRunnerConfigurationError(
                    f"{field_name} must be non-negative"
                )

    @property
    def exit_code(self) -> int:
        return (
            EXIT_SUCCESS
            if self.execution_status
            in {
                EXECUTION_SUCCESS,
                EXECUTION_SUCCESS_REPLAY,
                EXECUTION_SKIPPED_NON_MARKET_DAY,
                EXECUTION_ALREADY_RUNNING,
            }
            else EXIT_FAILURE
        )

    @property
    def runner_overhead_seconds(self) -> float | None:
        """Operational remainder outside measured lock/S5/S6A segments."""

        segments = (
            self.lock_acquisition_seconds,
            self.s5_duration_seconds,
            self.s6a_duration_seconds,
        )
        if any(value is None for value in segments):
            return None
        return max(0.0, self.duration_seconds - sum(segments))

    def as_dict(self) -> dict[str, object]:
        return {
            "runner_contract_version": RUNNER_CONTRACT_VERSION,
            "target_market_date": self.target_market_date.isoformat(),
            "execution_status": self.execution_status,
            "market_day_status": self.market_day_status,
            "market_date_evidence_state": self.market_date_evidence_state,
            "latest_published_date": (
                None
                if self.latest_published_date is None
                else self.latest_published_date.isoformat()
            ),
            "lock_status": self.lock_status,
            "lock_identity_sha256": self.lock_identity_sha256,
            "screener_run_id": self.screener_run_id,
            "s5_status": self.s5_status,
            "s5_replayed": self.s5_replayed,
            "report_sha256": self.report_sha256,
            "report_json_path": (
                None if self.report_json_path is None else str(self.report_json_path)
            ),
            "report_markdown_path": (
                None
                if self.report_markdown_path is None
                else str(self.report_markdown_path)
            ),
            "report_no_op": self.report_no_op,
            "error_code": self.error_code,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "lock_acquisition_seconds": self.lock_acquisition_seconds,
            "s5_duration_seconds": self.s5_duration_seconds,
            "s6a_duration_seconds": self.s6a_duration_seconds,
            "runner_overhead_seconds": self.runner_overhead_seconds,
            "exit_code": self.exit_code,
        }

    def as_json(self) -> str:
        return json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )


class DailyRunner:
    """Application-level sequencing for one explicit target market date."""

    def __init__(
        self,
        *,
        database_path: str | Path,
        report_output_directory: str | Path,
        screener_runner: ScreenerInvoker,
        report_generator: ReportGenerator,
        report_writer: ReportWriter | None = None,
        market_date_policy: MarketDatePolicy,
        lock_directory: str | Path | None = None,
        lock_factory: Callable[..., DailyRunLock] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database_path = _absolute_path(database_path, "database_path")
        self.report_output_directory = _absolute_path(
            report_output_directory,
            "report_output_directory",
        )
        self.lock_directory = (
            self.database_path.parent / ".s6b-locks"
            if lock_directory is None
            else _absolute_path(lock_directory, "lock_directory")
        )
        if not callable(screener_runner):
            raise TypeError("screener_runner must be callable")
        if not callable(getattr(report_generator, "generate", None)):
            raise TypeError("report_generator must expose generate")
        if report_writer is not None and not callable(
            getattr(report_writer, "write", None)
        ):
            raise TypeError("report_writer must expose write")
        if not callable(getattr(market_date_policy, "resolve", None)):
            raise TypeError("market_date_policy must expose resolve")
        self.screener_runner = screener_runner
        self.report_generator = report_generator
        self.report_writer = report_writer or ScreenerReportArtifactWriter()
        self.market_date_policy = market_date_policy
        self.lock_factory = lock_factory or DailyRunLock
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def run(self, target_market_date: date) -> DailyRunnerResult:
        target = _require_date(target_market_date, "target_market_date")
        started_at = self.clock()
        started_counter = perf_counter()
        decision: MarketDateDecision | None = None
        lock: DailyRunLock | None = None
        lock_status = LOCK_NOT_ACQUIRED
        screener_run_id: str | None = None
        s5_status: str | None = None
        s5_replayed: bool | None = None
        report_sha256: str | None = None
        report_json_path: Path | None = None
        report_markdown_path: Path | None = None
        report_no_op: bool | None = None
        error_code: str | None = None
        execution_status = EXECUTION_FAILED
        lock_acquisition_seconds: float | None = None
        s5_duration_seconds: float | None = None
        s6a_duration_seconds: float | None = None

        try:
            decision = self.market_date_policy.resolve(target)
            if decision.target_market_date != target:
                raise DailyRunnerConfigurationError(
                    "market-date policy returned a different target date"
                )
            if decision.status == NON_MARKET_DAY:
                execution_status = EXECUTION_SKIPPED_NON_MARKET_DAY
            elif decision.status != MARKET_DAY:
                error_code = "market_date_unresolved"
            else:
                lock = self.lock_factory(
                    self.database_path,
                    target,
                    lock_directory=self.lock_directory,
                )
                lock_started_counter = perf_counter()
                acquired = lock.try_acquire()
                lock_acquisition_seconds = perf_counter() - lock_started_counter
                if not acquired:
                    lock_status = LOCK_ALREADY_RUNNING
                    execution_status = EXECUTION_ALREADY_RUNNING
                else:
                    lock_status = LOCK_ACQUIRED
                    phase = "s5"
                    try:
                        s5_started_counter = perf_counter()
                        try:
                            s5_result = self.screener_runner(target)
                        finally:
                            s5_duration_seconds = (
                                perf_counter() - s5_started_counter
                            )
                        s5_status = _text_attr(s5_result, "status")
                        s5_replayed = _bool_attr(s5_result, "replayed")
                        screener_run_id = _optional_sha_attr(
                            s5_result,
                            "screener_run_id",
                        )
                        if s5_status != "success":
                            error_code = (
                                "s5_partial_success"
                                if s5_status == "partial_success"
                                else "s5_failed"
                            )
                        elif screener_run_id is None:
                            error_code = "s5_invalid_result"
                        else:
                            phase = "s6a"
                            s6a_started_counter = perf_counter()
                            try:
                                generation = self.report_generator.generate(
                                    screener_run_id
                                )
                                if generation.report.screener_run_id != screener_run_id:
                                    raise DailyRunnerError(
                                        "report run identity mismatch"
                                    )
                                s5_canonical_sha = getattr(
                                    s5_result,
                                    "canonical_sha256",
                                    None,
                                )
                                if (
                                    s5_canonical_sha
                                    != generation.report.screener_canonical_sha256
                                ):
                                    raise DailyRunnerError(
                                        "report source SHA mismatch"
                                    )
                                written = self.report_writer.write(
                                    generation,
                                    output_directory=self.report_output_directory,
                                )
                                report_sha256 = generation.report_sha256
                                report_json_path = written.json_artifact.path
                                report_markdown_path = written.markdown_artifact.path
                                report_no_op = written.no_op
                                execution_status = (
                                    EXECUTION_SUCCESS_REPLAY
                                    if s5_replayed
                                    else EXECUTION_SUCCESS
                                )
                            finally:
                                s6a_duration_seconds = (
                                    perf_counter() - s6a_started_counter
                                )
                    except Exception as error:
                        error_code = _classify_runner_error(phase, error)
                    finally:
                        try:
                            lock.release()
                            lock_status = LOCK_RELEASED
                        except Exception:
                            lock_status = LOCK_RELEASE_FAILED
                            execution_status = EXECUTION_FAILED
                            error_code = "lock_release_failed"
        except Exception as error:
            if error_code is None:
                error_code = _classify_runner_error("market_date", error)
            execution_status = EXECUTION_FAILED

        finished_at = self.clock()
        if decision is None:
            market_day_status = MARKET_DAY_UNKNOWN
            evidence_state = "unresolved"
            latest_published_date = None
        else:
            market_day_status = decision.status
            evidence_state = decision.evidence_state
            latest_published_date = decision.latest_published_date
        identity = DailyRunLock(
            self.database_path,
            target,
            lock_directory=self.lock_directory,
        ).identity_sha256
        return DailyRunnerResult(
            target_market_date=target,
            execution_status=execution_status,
            market_day_status=market_day_status,
            market_date_evidence_state=evidence_state,
            latest_published_date=latest_published_date,
            lock_status=lock_status,
            lock_identity_sha256=identity,
            screener_run_id=screener_run_id,
            s5_status=s5_status,
            s5_replayed=s5_replayed,
            report_sha256=report_sha256,
            report_json_path=report_json_path,
            report_markdown_path=report_markdown_path,
            report_no_op=report_no_op,
            error_code=error_code,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=perf_counter() - started_counter,
            lock_acquisition_seconds=lock_acquisition_seconds,
            s5_duration_seconds=s5_duration_seconds,
            s6a_duration_seconds=s6a_duration_seconds,
        )


def _classify_runner_error(phase: str, error: Exception) -> str:
    if isinstance(error, ScreenerReportCollisionError):
        return "report_artifact_collision"
    if isinstance(error, ScreenerReportInputError):
        return "report_input_invalid"
    if isinstance(error, ScreenerReplayIntegrityError):
        return f"{phase}_integrity_failed"
    if isinstance(error, DailyRunnerLockError):
        return "lock_error"
    if isinstance(error, DailyRunnerConfigurationError):
        return "runner_configuration_error"
    if isinstance(error, DailyRunnerError):
        return f"{phase}_execution_error"
    return f"{phase}_unexpected_error"


def _text_attr(value: object, field_name: str) -> str:
    raw = getattr(value, field_name, None)
    if not isinstance(raw, str) or not raw:
        raise DailyRunnerError(f"S5 result {field_name} is invalid")
    return raw


def _bool_attr(value: object, field_name: str) -> bool:
    raw = getattr(value, field_name, None)
    if not isinstance(raw, bool):
        raise DailyRunnerError(f"S5 result {field_name} is invalid")
    return raw


def _optional_sha_attr(value: object, field_name: str) -> str | None:
    raw = getattr(value, field_name, None)
    if raw is None:
        return None
    if not isinstance(raw, str) or _SHA256.fullmatch(raw) is None:
        raise DailyRunnerError(f"S5 result {field_name} is invalid")
    return raw


def _absolute_path(value: str | Path, field_name: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise DailyRunnerConfigurationError(f"{field_name} must be absolute")
    return path.resolve()


def _require_date(value: object, field_name: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a date")
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DailyRunnerConfigurationError(f"{field_name} must be lowercase SHA-256")
    return value


def _taipei_today() -> date:
    return datetime.now(_TAIPEI).date()


def _load_factory(specification: str, database_path: Path) -> ScreenerInvoker:
    if ":" not in specification:
        raise DailyRunnerConfigurationError("s5_factory must use module:attribute")
    module_name, attribute_name = specification.split(":", 1)
    if not module_name or not attribute_name:
        raise DailyRunnerConfigurationError("s5_factory must use module:attribute")
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute_name, None)
    if not callable(factory):
        raise DailyRunnerConfigurationError("s5_factory is not callable")
    runner = factory(database_path)
    if not callable(runner):
        raise DailyRunnerConfigurationError("s5_factory did not return a callable")
    return runner


def _load_latest_date_provider(
    specification: str,
    database_path: Path,
) -> Callable[[date], date | None]:
    if ":" not in specification:
        raise DailyRunnerConfigurationError(
            "latest_date_factory must use module:attribute"
        )
    module_name, attribute_name = specification.split(":", 1)
    if not module_name or not attribute_name:
        raise DailyRunnerConfigurationError(
            "latest_date_factory must use module:attribute"
        )
    module = importlib.import_module(module_name)
    factory = getattr(module, attribute_name, None)
    if not callable(factory):
        raise DailyRunnerConfigurationError("latest_date_factory is not callable")
    provider = factory(database_path)
    if not callable(provider):
        raise DailyRunnerConfigurationError(
            "latest_date_factory did not return a callable"
        )
    return provider


def _build_replay_only_s5_runner(
    database_path: Path,
    expected_screener_run_id: str,
) -> ScreenerInvoker:
    from app.screener.orchestration import DailyScreenerOrchestrator

    def unavailable(*args, **kwargs):
        del args, kwargs
        raise DailyRunnerError("replay-only runner attempted research execution")

    orchestrator = DailyScreenerOrchestrator(
        database_path,
        universe_provider=unavailable,
        stage1_runner=unavailable,
        stage2_runner=unavailable,
    )

    def invoke(target_market_date: date) -> object:
        return orchestrator.run(
            target_market_date,
            expected_screener_run_id=expected_screener_run_id,
        )

    return invoke


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="S6B Daily Runner; market-date gate plus S5 and S6A."
    )
    parser.add_argument("--market-date", type=date.fromisoformat, default=None)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--lock-dir", type=Path, default=None)
    parser.add_argument("--latest-published-date", type=date.fromisoformat, default=None)
    parser.add_argument(
        "--latest-date-factory",
        default=None,
        help=(
            "Explicit module:attribute factory accepting the DB path and "
            "returning a latest-date provider."
        ),
    )
    parser.add_argument("--non-market-date", action="append", default=[])
    parser.add_argument("--screener-run-id", type=str, default=None)
    parser.add_argument(
        "--s5-factory",
        default=None,
        help="Explicit module:attribute factory returning a callable S5 runner.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    target = args.market_date or _taipei_today()
    database_path = _absolute_path(args.db, "db")
    report_dir = _absolute_path(args.report_dir, "report-dir")
    try:
        if args.latest_published_date is not None and args.latest_date_factory:
            raise DailyRunnerConfigurationError(
                "latest-published-date and latest-date-factory are mutually exclusive"
            )
        if args.s5_factory:
            screener_runner = _load_factory(args.s5_factory, database_path)
        elif args.screener_run_id:
            screener_runner = _build_replay_only_s5_runner(
                database_path,
                args.screener_run_id.strip().lower(),
            )
        else:
            raise DailyRunnerConfigurationError(
                "fresh S5 execution requires explicit --s5-factory"
            )
        latest_date_provider = (
            _load_latest_date_provider(args.latest_date_factory, database_path)
            if args.latest_date_factory
            else None
        )
        non_market_dates = frozenset(
            date.fromisoformat(item) for item in args.non_market_date
        )
        runner = DailyRunner(
            database_path=database_path,
            report_output_directory=report_dir,
            screener_runner=screener_runner,
            report_generator=ScreenerReportGenerator(database_path),
            market_date_policy=DeterministicMarketDatePolicy(
                latest_published_date=args.latest_published_date,
                latest_date_provider=latest_date_provider,
                explicit_non_market_dates=non_market_dates,
            ),
            lock_directory=args.lock_dir,
        )
        result = runner.run(target)
        print(result.as_json())
        return result.exit_code
    except Exception:
        result = _configuration_result(target, database_path, args.lock_dir)
        print(result.as_json())
        return result.exit_code


def _configuration_result(
    target: date,
    database_path: Path,
    lock_directory: Path | None,
) -> DailyRunnerResult:
    started = datetime.now(timezone.utc)
    identity = DailyRunLock(
        database_path,
        target,
        lock_directory=lock_directory,
    ).identity_sha256
    return DailyRunnerResult(
        target_market_date=target,
        execution_status=EXECUTION_FAILED,
        market_day_status=MARKET_DAY_UNKNOWN,
        market_date_evidence_state="unresolved",
        latest_published_date=None,
        lock_status=LOCK_NOT_ACQUIRED,
        lock_identity_sha256=identity,
        screener_run_id=None,
        s5_status=None,
        s5_replayed=None,
        report_sha256=None,
        report_json_path=None,
        report_markdown_path=None,
        report_no_op=None,
        error_code="runner_configuration_error",
        started_at=started,
        finished_at=started,
        duration_seconds=0.0,
    )


__all__ = [
    "DailyRunLock",
    "DailyRunLockIdentity",
    "DailyRunner",
    "DailyRunnerConfigurationError",
    "DailyRunnerError",
    "DailyRunnerLockError",
    "DailyRunnerResult",
    "DeterministicMarketDatePolicy",
    "EXIT_FAILURE",
    "EXIT_SUCCESS",
    "EXECUTION_ALREADY_RUNNING",
    "EXECUTION_FAILED",
    "EXECUTION_SKIPPED_NON_MARKET_DAY",
    "EXECUTION_SUCCESS",
    "EXECUTION_SUCCESS_REPLAY",
    "LOCK_ALREADY_RUNNING",
    "LOCK_ACQUIRED",
    "LOCK_NOT_ACQUIRED",
    "LOCK_RELEASED",
    "LOCK_RELEASE_FAILED",
    "MARKET_DAY",
    "MARKET_DAY_UNKNOWN",
    "MarketDateDecision",
    "MarketDatePolicy",
    "NON_MARKET_DAY",
    "RUNNER_CONTRACT_VERSION",
    "build_parser",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
