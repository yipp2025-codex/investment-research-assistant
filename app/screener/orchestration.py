"""S5 deterministic daily Market Screener orchestration.

This module owns sequencing and checkpoint coordination only.  Universe
construction, Stage 1 rules, Stage 2 research, final ranking, and canonical
serialization remain in the frozen S1--S4 modules and are supplied through
small execution hooks.

The orchestration boundary deliberately does not acquire data, run a SQLite
write transaction around computation, migrate a database, or expose any
watchlist, scheduler, report, UI, or trading dependency.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Protocol

from app.screener.stage1 import (
    STAGE1_METHODOLOGY_VERSION,
    Stage1Candidate,
    Stage1ScanResult,
)
from app.screener.stage2 import (
    STAGE2_METHODOLOGY_VERSION,
    Stage2AnalysisStatus,
    Stage2Candidate,
    failed_stage2_candidate,
)
from app.screener.universe import MarketUniverseSnapshot
from app.storage.candidate_persistence import (
    CandidateCheckpoint,
    CandidateInputLocator,
    SQLiteScreenerCheckpointRepository,
    ScreenerRunPersistenceResult,
)
from app.storage.screener_replay import (
    FrozenScreenerResult,
    SQLiteScreenerReplayRepository,
    ScreenerFinalizationStateError,
    ScreenerReplayIntegrityError,
)
from app.storage.universe_persistence import (
    SQLiteMarketUniverseRepository,
    UniversePersistenceResult,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TERMINAL_CANDIDATE_STATUSES = frozenset({"success", "failed"})


class DailyScreenerOrchestrationError(RuntimeError):
    """A daily orchestration input or state cannot be used safely."""


class DailyScreenerInputConflictError(DailyScreenerOrchestrationError):
    """The supplied deterministic input conflicts with persisted S4 state."""


class DailyScreenerStateError(DailyScreenerOrchestrationError):
    """A persisted run cannot be resumed through the frozen S4 contract."""


@dataclass(frozen=True, slots=True)
class Stage1Execution:
    """Frozen Stage 1 output plus the immutable S4 handoff locators."""

    result: Stage1ScanResult
    candidate_locators: tuple[CandidateInputLocator, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.result, Stage1ScanResult):
            raise TypeError("Stage1Execution.result must be a Stage1ScanResult")
        if not isinstance(self.candidate_locators, tuple):
            raise TypeError("Stage1Execution.candidate_locators must be a tuple")
        if any(
            not isinstance(item, CandidateInputLocator)
            for item in self.candidate_locators
        ):
            raise TypeError("Stage1Execution contains an invalid candidate locator")
        expected = tuple(item.symbol for item in self.result.candidates)
        actual = tuple(item.symbol for item in self.candidate_locators)
        if len(set(actual)) != len(actual) or set(actual) != set(expected):
            raise DailyScreenerInputConflictError(
                "Stage 1 candidate locators must exactly match the shortlist"
            )


@dataclass(frozen=True, slots=True)
class DailyScreenerPreparation:
    """Pure S1 preparation retained for checkpoint resume in this process."""

    universe: MarketUniverseSnapshot
    stage1: Stage1Execution

    def __post_init__(self) -> None:
        if not isinstance(self.universe, MarketUniverseSnapshot):
            raise TypeError("universe must be a MarketUniverseSnapshot")
        if not isinstance(self.stage1, Stage1Execution):
            raise TypeError("stage1 must be a Stage1Execution")
        if self.universe.market_date != self.stage1.result.market_date:
            raise DailyScreenerInputConflictError(
                "Universe and Stage 1 market_date values differ"
            )


@dataclass(frozen=True, slots=True)
class Stage2ResearchRequest:
    """One isolated Stage 2 request handed to the injected research hook."""

    market_date: date
    candidate: Stage1Candidate
    locator: CandidateInputLocator

    def __post_init__(self) -> None:
        if not isinstance(self.market_date, date):
            raise TypeError("market_date must be a date")
        if not isinstance(self.candidate, Stage1Candidate):
            raise TypeError("candidate must be a Stage1Candidate")
        if not isinstance(self.locator, CandidateInputLocator):
            raise TypeError("locator must be a CandidateInputLocator")
        if self.candidate.symbol != self.locator.symbol:
            raise DailyScreenerInputConflictError(
                "Stage 2 candidate and locator symbols differ"
            )


@dataclass(frozen=True, slots=True)
class Stage2CandidateExecution:
    """Frozen Stage 2 candidate plus the S4 hash-only input evidence."""

    candidate: Stage2Candidate
    research_locator_sha256: str
    snapshot_sha256: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.candidate, Stage2Candidate):
            raise TypeError("candidate must be a Stage2Candidate")
        _require_sha256(self.research_locator_sha256, "research_locator_sha256")
        if self.snapshot_sha256 is not None:
            _require_sha256(self.snapshot_sha256, "snapshot_sha256")


class UniverseProvider(Protocol):
    """Return one already-normalized immutable S1 Universe snapshot."""

    def __call__(self, market_date: date, /) -> MarketUniverseSnapshot:
        ...


class Stage1Runner(Protocol):
    """Execute the frozen Stage 1 contract outside S4 write transactions."""

    def __call__(
        self, universe: MarketUniverseSnapshot, market_date: date, /
    ) -> Stage1Execution:
        ...


class Stage2Runner(Protocol):
    """Execute one frozen Stage 2 candidate outside S4 write transactions."""

    def __call__(self, request: Stage2ResearchRequest, /) -> Stage2CandidateExecution:
        ...


class ReplayKeyResolver(Protocol):
    """Resolve an existing S4 run id from an immutable input manifest only."""

    def __call__(self, market_date: date, /) -> str | None:
        ...


@dataclass(frozen=True, slots=True)
class DailyScreenerResult:
    """Operational S5 result wrapping the frozen S4 result when successful.

    ``final_result`` is the only canonical result object.  This wrapper does
    not define a second serializer or hash; ``canonical_sha256`` is copied
    from the frozen S4 object.
    """

    market_date: date
    universe_run_id: str
    screener_run_id: str
    status: str
    universe_count: int
    screened_count: int
    triggered_count: int
    candidate_count: int
    truncated: bool
    candidates: tuple[Stage2Candidate, ...]
    canonical_sha256: str | None
    replayed: bool
    written: bool
    universe_created: bool
    screener_run_created: bool
    candidate_written_count: int
    candidate_replayed_count: int
    failed_candidates: tuple[str, ...]
    final_result: FrozenScreenerResult | None

    def __post_init__(self) -> None:
        if not isinstance(self.market_date, date):
            raise TypeError("market_date must be a date")
        _require_sha256(self.universe_run_id, "universe_run_id")
        _require_sha256(self.screener_run_id, "screener_run_id")
        if self.status not in {"success", "partial_success", "failed"}:
            raise DailyScreenerStateError(
                f"unsupported orchestration result status: {self.status}"
            )
        for field_name in (
            "universe_count",
            "screened_count",
            "triggered_count",
            "candidate_count",
            "candidate_written_count",
            "candidate_replayed_count",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer")
        if not isinstance(self.truncated, bool):
            raise TypeError("truncated must be a bool")
        if not isinstance(self.candidates, tuple) or any(
            not isinstance(item, Stage2Candidate) for item in self.candidates
        ):
            raise TypeError("candidates must be an immutable Stage 2 tuple")
        if not isinstance(self.failed_candidates, tuple) or any(
            not isinstance(item, str) for item in self.failed_candidates
        ):
            raise TypeError("failed_candidates must be an immutable string tuple")
        if self.status == "success":
            if not isinstance(self.final_result, FrozenScreenerResult):
                raise DailyScreenerStateError(
                    "successful orchestration requires a frozen S4 result"
                )
            if self.canonical_sha256 != self.final_result.payload_sha256:
                raise DailyScreenerStateError(
                    "S5 canonical hash differs from the frozen S4 result"
                )
            if self.candidates != self.final_result.candidates:
                raise DailyScreenerStateError(
                    "S5 candidates differ from the frozen S4 result"
                )
        elif self.final_result is not None or self.canonical_sha256 is not None:
            raise DailyScreenerStateError(
                "non-success orchestration cannot expose a canonical result"
            )

    @classmethod
    def from_frozen(
        cls,
        frozen: FrozenScreenerResult,
        *,
        replayed: bool,
        written: bool,
        universe_created: bool = False,
        screener_run_created: bool = False,
        candidate_written_count: int = 0,
        candidate_replayed_count: int = 0,
    ) -> "DailyScreenerResult":
        return cls(
            market_date=frozen.market_date,
            universe_run_id=frozen.universe_run_id,
            screener_run_id=frozen.screener_run_id,
            status="success",
            universe_count=frozen.universe_count,
            screened_count=frozen.screened_count,
            triggered_count=frozen.triggered_count,
            candidate_count=frozen.candidate_count,
            truncated=frozen.truncated,
            candidates=frozen.candidates,
            canonical_sha256=frozen.payload_sha256,
            replayed=replayed,
            written=written,
            universe_created=universe_created,
            screener_run_created=screener_run_created,
            candidate_written_count=candidate_written_count,
            candidate_replayed_count=candidate_replayed_count,
            failed_candidates=(),
            final_result=frozen,
        )


@dataclass(frozen=True, slots=True)
class _StoredCandidate:
    status: str
    checkpoint: CandidateCheckpoint | None


class DailyScreenerOrchestrator:
    """Run one deterministic daily S1--S4 Screener flow.

    The optional ``expected_screener_run_id`` is the replay key returned by a
    previous run.  Supplying it makes replay-first behavior explicit across
    process boundaries.  ``replay_key_resolver`` provides the same behavior
    from an immutable input manifest without running any market-data or
    research hook.  Within one orchestrator instance, the deterministic key is
    cached after the first execution, so a second call without the argument
    also performs strict replay before invoking any hook.
    """

    def __init__(
        self,
        database_path: str | Path,
        *,
        universe_provider: UniverseProvider,
        stage1_runner: Stage1Runner,
        stage2_runner: Stage2Runner,
        replay_key_resolver: ReplayKeyResolver | None = None,
        universe_repository: SQLiteMarketUniverseRepository | None = None,
        checkpoint_repository: SQLiteScreenerCheckpointRepository | None = None,
        replay_repository: SQLiteScreenerReplayRepository | None = None,
    ) -> None:
        if not callable(universe_provider):
            raise TypeError("universe_provider must be callable")
        if not callable(stage1_runner):
            raise TypeError("stage1_runner must be callable")
        if not callable(stage2_runner):
            raise TypeError("stage2_runner must be callable")
        if replay_key_resolver is not None and not callable(replay_key_resolver):
            raise TypeError("replay_key_resolver must be callable")
        self.database_path = Path(database_path)
        self.universe_provider = universe_provider
        self.stage1_runner = stage1_runner
        self.stage2_runner = stage2_runner
        self.replay_key_resolver = replay_key_resolver
        self.universe_repository = universe_repository or SQLiteMarketUniverseRepository(
            self.database_path
        )
        self.checkpoint_repository = (
            checkpoint_repository
            or SQLiteScreenerCheckpointRepository(self.database_path)
        )
        self.replay_repository = replay_repository or SQLiteScreenerReplayRepository(
            self.database_path
        )
        self._preparations: dict[date, DailyScreenerPreparation] = {}
        self._run_ids: dict[date, str] = {}

    def run(
        self,
        market_date: date,
        *,
        expected_screener_run_id: str | None = None,
    ) -> DailyScreenerResult:
        """Run or strictly replay one deterministic market-date Screener.

        No S5 migration is performed.  The caller must initialize the frozen
        v10 database and apply S4 migration 11 explicitly before invoking
        this method.
        """

        frozen_date = _require_date(market_date, "market_date")
        explicit_run_id = expected_screener_run_id is not None
        requested_run_id = (
            expected_screener_run_id
            or self._run_ids.get(frozen_date)
            or (
                None
                if self.replay_key_resolver is None
                else self.replay_key_resolver(frozen_date)
            )
        )
        if requested_run_id is not None:
            _require_sha256(requested_run_id, "expected_screener_run_id")
            replayed = self._try_replay(
                frozen_date,
                requested_run_id,
            )
            if replayed is not None:
                return replayed
            if explicit_run_id and self._run_status(requested_run_id) is None:
                raise DailyScreenerStateError(
                    "expected Screener run does not exist; refusing hidden duplicate"
                )

        preparation = self._preparations.get(frozen_date)
        if preparation is None:
            preparation = self._prepare(frozen_date)
            self._preparations[frozen_date] = preparation

        universe_result = self.universe_repository.persist(
            preparation.universe
        )
        run_result = self.checkpoint_repository.create_run(
            universe_run_id=universe_result.universe_run_id,
            stage1_result=preparation.stage1.result,
            candidate_locators=preparation.stage1.candidate_locators,
        )
        run_id = run_result.screener_run_id
        if requested_run_id is not None and requested_run_id != run_id:
            raise DailyScreenerInputConflictError(
                "deterministic input produced a different screener_run_id"
            )
        self._run_ids[frozen_date] = run_id

        # A run may have been created by another process after the initial
        # replay probe.  Re-check before any Stage 2 hook is called.
        replayed = self._try_replay(frozen_date, run_id)
        if replayed is not None:
            return _replace_replay_flags(
                replayed,
                universe_created=universe_result.created,
                screener_run_created=run_result.created,
            )

        stored = self._stored_candidates(run_id)
        locator_by_symbol = {
            item.symbol: item for item in preparation.stage1.candidate_locators
        }
        candidates: dict[str, Stage2Candidate] = {}
        failed_symbols: set[str] = set()
        candidate_written_count = 0
        candidate_replayed_count = 0

        for stage1_candidate in preparation.stage1.result.candidates:
            symbol = stage1_candidate.symbol
            locator = locator_by_symbol[symbol]
            previous = stored.get(symbol)
            if previous is not None and previous.status == "success":
                if previous.checkpoint is None:  # pragma: no cover - defensive.
                    raise DailyScreenerStateError(
                        f"successful candidate {symbol} has no checkpoint"
                    )
                candidates[symbol] = _candidate_from_checkpoint(previous.checkpoint)
                candidate_replayed_count += 1
                continue
            if previous is not None and previous.status == "running":
                raise DailyScreenerStateError(
                    f"candidate {symbol} is left in an unrecoverable running state"
                )

            retry_failed = previous is not None and previous.status == "failed"
            request = Stage2ResearchRequest(
                market_date=frozen_date,
                candidate=stage1_candidate,
                locator=locator,
            )
            try:
                execution = self.stage2_runner(request)
            except Exception as error:
                if retry_failed:
                    if previous is None or previous.checkpoint is None:
                        raise DailyScreenerStateError(
                            f"failed candidate {symbol} has no resumable checkpoint"
                        ) from error
                    # A retry that fails before producing a new frozen Stage 2
                    # candidate must leave the original failed checkpoint
                    # untouched.  This keeps the partial run recoverable and
                    # avoids rewriting its child timestamps or evidence.
                    candidates[symbol] = _candidate_from_checkpoint(
                        previous.checkpoint
                    )
                    failed_symbols.add(symbol)
                    continue
                # Only the isolated computation hook is converted to a frozen
                # per-candidate failure.  S4 persistence errors remain fatal.
                candidate = failed_stage2_candidate(
                    stage1_candidate=stage1_candidate,
                    error=error,
                    market_date=frozen_date,
                )
                snapshot_sha256 = None
                research_locator_sha256 = locator.research_locator_sha256
            else:
                if not isinstance(execution, Stage2CandidateExecution):
                    raise TypeError(
                        "stage2_runner must return Stage2CandidateExecution"
                    )
                if execution.candidate.symbol != symbol:
                    raise DailyScreenerInputConflictError(
                        "Stage 2 runner returned a different candidate symbol"
                    )
                candidate = execution.candidate
                snapshot_sha256 = execution.snapshot_sha256
                research_locator_sha256 = execution.research_locator_sha256

            persisted = self.checkpoint_repository.persist_candidate(
                screener_run_id=run_id,
                candidate=candidate,
                research_locator_sha256=research_locator_sha256,
                snapshot_sha256=snapshot_sha256,
                retry_failed=retry_failed,
            )
            if persisted.written:
                candidate_written_count += 1
            candidates[symbol] = candidate
            if candidate.analysis_status is Stage2AnalysisStatus.FAILED:
                failed_symbols.add(symbol)

        try:
            finalized = self.replay_repository.finalize_run(run_id)
        except ScreenerFinalizationStateError:
            stored_after = self._stored_candidates(run_id)
            terminal = {
                symbol: item
                for symbol, item in stored_after.items()
                if item.status in _TERMINAL_CANDIDATE_STATUSES
            }
            failed_symbols = {
                symbol for symbol, item in terminal.items() if item.status == "failed"
            }
            if not failed_symbols:
                raise
            return self._partial_result(
                frozen_date=frozen_date,
                universe_result=universe_result,
                run_result=run_result,
                candidates=terminal,
                stage1_result=preparation.stage1.result,
                failed_symbols=failed_symbols,
                candidate_written_count=candidate_written_count,
                candidate_replayed_count=candidate_replayed_count,
            )

        return DailyScreenerResult.from_frozen(
            finalized.result,
            replayed=False,
            written=finalized.written,
            universe_created=universe_result.created,
            screener_run_created=run_result.created,
            candidate_written_count=candidate_written_count,
            candidate_replayed_count=candidate_replayed_count,
        )

    def _prepare(self, market_date: date) -> DailyScreenerPreparation:
        # Both hooks run before the first S4 write transaction.
        universe = self.universe_provider(market_date)
        if not isinstance(universe, MarketUniverseSnapshot):
            raise TypeError("universe_provider must return MarketUniverseSnapshot")
        if universe.market_date != market_date:
            raise DailyScreenerInputConflictError(
                "Universe market_date differs from the requested market_date"
            )
        stage1 = self.stage1_runner(universe, market_date)
        if not isinstance(stage1, Stage1Execution):
            raise TypeError("stage1_runner must return Stage1Execution")
        if stage1.result.market_date != market_date:
            raise DailyScreenerInputConflictError(
                "Stage 1 market_date differs from the requested market_date"
            )
        if stage1.result.methodology_version != STAGE1_METHODOLOGY_VERSION:
            raise DailyScreenerInputConflictError("Stage 1 methodology is not frozen v1")
        if stage1.result.source_policy != universe.source_policy:
            raise DailyScreenerInputConflictError(
                "Stage 1 and Universe source policies differ"
            )
        if stage1.result.universe_count != universe.universe_count:
            raise DailyScreenerInputConflictError(
                "Stage 1 and Universe counts differ"
            )
        return DailyScreenerPreparation(universe=universe, stage1=stage1)

    def _try_replay(
        self, market_date: date, screener_run_id: str
    ) -> DailyScreenerResult | None:
        try:
            replayed = self.replay_repository.replay_run(screener_run_id)
        except KeyError:
            return None
        except ScreenerFinalizationStateError as error:
            # A sealed success that cannot be reconstructed is not a resumable
            # partial run.  Keep tamper/frozen-state failures fail-closed.
            if self._run_status(screener_run_id) == "success":
                raise ScreenerReplayIntegrityError(
                    "sealed Screener run failed strict replay"
                ) from error
            return None
        if replayed.result.market_date != market_date:
            raise DailyScreenerInputConflictError(
                "replay run market_date differs from the requested market_date"
            )
        return DailyScreenerResult.from_frozen(
            replayed.result,
            replayed=True,
            written=False,
        )

    def _stored_candidates(self, screener_run_id: str) -> dict[str, _StoredCandidate]:
        connection = self._read_connection()
        try:
            rows = connection.execute(
                "SELECT candidate_id, symbol, status "
                "FROM screener_candidates WHERE screener_run_id = ? "
                "ORDER BY stage1_rank, symbol",
                (screener_run_id,),
            ).fetchall()
        finally:
            connection.close()

        result: dict[str, _StoredCandidate] = {}
        for row in rows:
            symbol = row["symbol"]
            if symbol in result:
                raise ScreenerReplayIntegrityError(
                    f"duplicate persisted candidate {symbol}"
                )
            status = row["status"]
            checkpoint = (
                self.checkpoint_repository.load_candidate(row["candidate_id"])
                if status in _TERMINAL_CANDIDATE_STATUSES
                else None
            )
            result[symbol] = _StoredCandidate(status=status, checkpoint=checkpoint)
        return result

    def _partial_result(
        self,
        *,
        frozen_date: date,
        universe_result: UniversePersistenceResult,
        run_result: ScreenerRunPersistenceResult,
        candidates: dict[str, _StoredCandidate],
        stage1_result: Stage1ScanResult,
        failed_symbols: set[str],
        candidate_written_count: int,
        candidate_replayed_count: int,
    ) -> DailyScreenerResult:
        ordered = tuple(
            _candidate_from_checkpoint(item.checkpoint)
            for item in sorted(
                candidates.values(),
                key=lambda item: item.checkpoint.stage1_rank
                if item.checkpoint is not None
                else 10**9,
            )
            if item.checkpoint is not None
        )
        status = "partial_success" if len(ordered) > len(failed_symbols) else "failed"
        return DailyScreenerResult(
            market_date=frozen_date,
            universe_run_id=universe_result.universe_run_id,
            screener_run_id=run_result.screener_run_id,
            status=status,
            universe_count=universe_result.snapshot.universe_count,
            screened_count=stage1_result.screened_count,
            triggered_count=stage1_result.triggered_count,
            candidate_count=stage1_result.candidate_count,
            truncated=stage1_result.truncated,
            candidates=ordered,
            canonical_sha256=None,
            replayed=False,
            written=candidate_written_count > 0,
            universe_created=universe_result.created,
            screener_run_created=run_result.created,
            candidate_written_count=candidate_written_count,
            candidate_replayed_count=candidate_replayed_count,
            failed_candidates=tuple(sorted(failed_symbols)),
            final_result=None,
        )

    def _run_status(self, screener_run_id: str) -> str | None:
        connection = self._read_connection()
        try:
            row = connection.execute(
                "SELECT status FROM screener_runs WHERE screener_run_id = ?",
                (screener_run_id,),
            ).fetchone()
            return None if row is None else str(row["status"])
        finally:
            connection.close()

    def _read_connection(self) -> sqlite3.Connection:
        uri = self.database_path.resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA query_only = ON")
        return connection


def _candidate_from_checkpoint(checkpoint: CandidateCheckpoint) -> Stage2Candidate:
    """Structural S4 read-model adapter; no ranking or metric logic."""

    return Stage2Candidate(
        rank=checkpoint.stage1_rank,
        stage1_rank=checkpoint.stage1_rank,
        symbol=checkpoint.symbol,
        name=checkpoint.name,
        market=checkpoint.market,
        candidate_kind=checkpoint.candidate_kind,
        analysis_status=checkpoint.analysis_status,
        stage1_reasons=checkpoint.stage1_reasons,
        stage2_reasons=checkpoint.stage2_reasons,
        metrics=checkpoint.metrics,
        data_quality=checkpoint.data_quality,
        provenance=checkpoint.provenance,
        failure=checkpoint.failure,
    )


def _replace_replay_flags(
    result: DailyScreenerResult,
    *,
    universe_created: bool,
    screener_run_created: bool,
) -> DailyScreenerResult:
    return DailyScreenerResult(
        market_date=result.market_date,
        universe_run_id=result.universe_run_id,
        screener_run_id=result.screener_run_id,
        status=result.status,
        universe_count=result.universe_count,
        screened_count=result.screened_count,
        triggered_count=result.triggered_count,
        candidate_count=result.candidate_count,
        truncated=result.truncated,
        candidates=result.candidates,
        canonical_sha256=result.canonical_sha256,
        replayed=result.replayed,
        written=result.written,
        universe_created=universe_created,
        screener_run_created=screener_run_created,
        candidate_written_count=result.candidate_written_count,
        candidate_replayed_count=result.candidate_replayed_count,
        failed_candidates=result.failed_candidates,
        final_result=result.final_result,
    )


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be lowercase SHA-256")
    return value


def _require_date(value: object, field_name: str) -> date:
    if not isinstance(value, date):
        raise TypeError(f"{field_name} must be a date")
    return value


__all__ = [
    "DailyScreenerInputConflictError",
    "DailyScreenerOrchestrationError",
    "DailyScreenerOrchestrator",
    "DailyScreenerPreparation",
    "DailyScreenerResult",
    "DailyScreenerStateError",
    "Stage1Execution",
    "Stage1Runner",
    "Stage2CandidateExecution",
    "Stage2ResearchRequest",
    "Stage2Runner",
    "UniverseProvider",
    "ReplayKeyResolver",
]
