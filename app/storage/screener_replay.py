"""S4.4 deterministic Screener finalization and canonical replay.

This module is deliberately downstream of the frozen S1-S3 contracts.  It
reconstructs already-persisted S4.3 checkpoints, delegates research-priority
ordering to the public S3 finalizer, and never performs candidate research or
market-data acquisition.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

import app.storage.candidate_persistence as _candidate_store
from app.research_dataset import TWSE_BASELINE_SOURCE_POLICY
from app.screener.stage1 import STAGE1_METHODOLOGY_VERSION
from app.screener.stage2 import (
    STAGE2_METHODOLOGY_VERSION,
    STAGE2_METHODOLOGY_V1,
    Stage2Candidate,
    Stage2CandidateResult,
    finalize_stage2_result,
)
from app.storage.candidate_persistence import (
    CandidateCheckpoint,
    SQLiteScreenerCheckpointRepository,
    ScreenerCheckpointError,
)


FROZEN_SCREENER_RESULT_VERSION = "screener-frozen-result-v1"

FAULT_AFTER_CANDIDATE_VERIFICATION = "after_candidate_completeness_verification"
FAULT_AFTER_RANKING = "after_ranking_before_rank_write"
FAULT_AFTER_PARTIAL_RANK_WRITE = "after_partial_rank_write"
FAULT_AFTER_CANONICAL_RECONSTRUCTION = "after_canonical_reconstruction"
FAULT_AFTER_CANONICAL_HASH_VERIFICATION = "after_canonical_hash_verification"
FAULT_BEFORE_SUCCESS_TRANSITION = "before_run_success_transition"

FINALIZATION_FAULT_POINTS = (
    FAULT_AFTER_CANDIDATE_VERIFICATION,
    FAULT_AFTER_RANKING,
    FAULT_AFTER_PARTIAL_RANK_WRITE,
    FAULT_AFTER_CANONICAL_RECONSTRUCTION,
    FAULT_AFTER_CANONICAL_HASH_VERIFICATION,
    FAULT_BEFORE_SUCCESS_TRANSITION,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FaultInjector = Callable[[str], None]


class ScreenerReplayError(RuntimeError):
    """Base error for S4.4 finalization and canonical replay."""


class ScreenerFinalizationStateError(ScreenerReplayError):
    """A Screener run is not in a state that can be finalized."""


class ScreenerReplayIntegrityError(ScreenerReplayError):
    """Persisted immutable Screener state failed canonical verification."""


@dataclass(frozen=True, slots=True)
class FrozenScreenerResult:
    """Canonical persisted Screener output reconstructed without time series."""

    screener_run_id: str
    universe_run_id: str
    market_date: date
    stage1_methodology_version: str
    stage2_methodology_version: str
    source_policy: str
    universe_count: int
    screened_count: int
    triggered_count: int
    candidate_count: int
    candidate_limit: int
    truncated: bool
    candidates: tuple[Stage2Candidate, ...]
    contract_version: str = FROZEN_SCREENER_RESULT_VERSION

    def __post_init__(self) -> None:
        _require_sha256(self.screener_run_id, "screener_run_id")
        _require_sha256(self.universe_run_id, "universe_run_id")
        if not isinstance(self.market_date, date):
            raise ScreenerReplayIntegrityError("market_date must be a date")
        if self.stage1_methodology_version != STAGE1_METHODOLOGY_VERSION:
            raise ScreenerReplayIntegrityError("Stage 1 methodology is not frozen v1")
        if self.stage2_methodology_version != STAGE2_METHODOLOGY_VERSION:
            raise ScreenerReplayIntegrityError("Stage 2 methodology is not frozen v1")
        if self.source_policy != TWSE_BASELINE_SOURCE_POLICY:
            raise ScreenerReplayIntegrityError("source_policy must remain twse_baseline")
        if self.contract_version != FROZEN_SCREENER_RESULT_VERSION:
            raise ScreenerReplayIntegrityError("unsupported frozen result contract")
        for field_name in (
            "universe_count",
            "screened_count",
            "triggered_count",
            "candidate_count",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ScreenerReplayIntegrityError(
                    f"{field_name} must be a non-negative integer"
                )
        if (
            isinstance(self.candidate_limit, bool)
            or not isinstance(self.candidate_limit, int)
            or self.candidate_limit < 1
        ):
            raise ScreenerReplayIntegrityError("candidate_limit must be positive")
        if not isinstance(self.truncated, bool):
            raise ScreenerReplayIntegrityError("truncated must be boolean")
        if not isinstance(self.candidates, tuple) or any(
            not isinstance(item, Stage2Candidate) for item in self.candidates
        ):
            raise ScreenerReplayIntegrityError("candidates must be immutable")
        if self.candidate_count != len(self.candidates):
            raise ScreenerReplayIntegrityError("candidate_count is inconsistent")
        if tuple(item.rank for item in self.candidates) != tuple(
            range(1, self.candidate_count + 1)
        ):
            raise ScreenerReplayIntegrityError(
                "candidate ranks must be contiguous research priority"
            )
        if self.screened_count > self.universe_count:
            raise ScreenerReplayIntegrityError("screened_count exceeds universe_count")
        if self.triggered_count > self.screened_count:
            raise ScreenerReplayIntegrityError("triggered_count exceeds screened_count")
        if self.candidate_count > min(self.triggered_count, self.candidate_limit):
            raise ScreenerReplayIntegrityError("candidate_count exceeds frozen limits")
        expected_truncated = self.triggered_count > self.candidate_count
        if self.truncated != expected_truncated:
            raise ScreenerReplayIntegrityError("truncated metadata is inconsistent")

    @property
    def stage2_result(self) -> Stage2CandidateResult:
        return Stage2CandidateResult(
            market_date=self.market_date,
            methodology_version=self.stage2_methodology_version,
            stage1_methodology_version=self.stage1_methodology_version,
            source_policy=self.source_policy,
            candidate_count=self.candidate_count,
            candidates=self.candidates,
        )

    def as_dict(self) -> dict[str, object]:
        stage2 = self.stage2_result.as_dict()
        return {
            "contract_version": self.contract_version,
            "screener_run_id": self.screener_run_id,
            "universe_run_id": self.universe_run_id,
            "market_date": self.market_date.isoformat(),
            "stage1_methodology_version": self.stage1_methodology_version,
            "stage2_methodology_version": self.stage2_methodology_version,
            "source_policy": self.source_policy,
            "universe_count": self.universe_count,
            "screened_count": self.screened_count,
            "triggered_count": self.triggered_count,
            "candidate_count": self.candidate_count,
            "candidate_limit": self.candidate_limit,
            "truncated": self.truncated,
            "candidates": stage2["candidates"],
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class ScreenerFinalizationResult:
    screener_run_id: str
    result: FrozenScreenerResult
    written: bool


@dataclass(frozen=True, slots=True)
class _CandidateRecord:
    candidate_id: str
    stored_rank: int | None
    checkpoint: CandidateCheckpoint
    candidate: Stage2Candidate


class SQLiteScreenerReplayRepository:
    """Finalize one persisted run and strictly replay successful runs."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def finalize_run(
        self,
        screener_run_id: str,
        *,
        fault_injector: _FaultInjector | None = None,
    ) -> ScreenerFinalizationResult:
        """Assign final ranks and seal a running run in one transaction."""

        _require_sha256(screener_run_id, "screener_run_id")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault_injector must be callable")

        with self._write_transaction() as connection:
            self._require_v11(connection)
            run = self._get_run(connection, screener_run_id)
            self._verify_run_identity(run)
            if run["status"] == "success":
                result = self._reconstruct_success(connection, run)
                return ScreenerFinalizationResult(
                    screener_run_id=screener_run_id,
                    result=result,
                    written=False,
                )
            if run["status"] != "running":
                raise ScreenerFinalizationStateError(
                    "only a running Screener run can be finalized"
                )

            records = self._load_candidate_records(
                connection,
                run,
                require_final_rank=False,
            )
            self._fire(fault_injector, FAULT_AFTER_CANDIDATE_VERIFICATION)

            ranked = finalize_stage2_result(
                candidates=tuple(item.candidate for item in records),
                market_date=date.fromisoformat(run["market_date"]),
                methodology=STAGE2_METHODOLOGY_V1,
            )
            if ranked.candidate_count != run["candidate_count"]:
                raise ScreenerReplayIntegrityError(
                    "S3 ranking changed persisted candidate membership"
                )
            self._fire(fault_injector, FAULT_AFTER_RANKING)

            result = self._build_result(run, ranked.candidates)
            canonical_json = result.canonical_json()
            self._fire(fault_injector, FAULT_AFTER_CANONICAL_RECONSTRUCTION)
            canonical_sha256 = hashlib.sha256(
                canonical_json.encode("utf-8")
            ).hexdigest()
            repeated = self._build_result(run, ranked.candidates)
            if (
                canonical_sha256 != result.payload_sha256
                or repeated.canonical_json() != canonical_json
                or repeated.payload_sha256 != canonical_sha256
            ):
                raise ScreenerReplayIntegrityError(
                    "canonical Screener serialization is not deterministic"
                )
            self._fire(fault_injector, FAULT_AFTER_CANONICAL_HASH_VERIFICATION)

            candidate_ids = {item.checkpoint.symbol: item.candidate_id for item in records}
            rank_rows = tuple(
                (candidate_ids[item.symbol], item.rank) for item in ranked.candidates
            )
            if rank_rows:
                self._write_rank(connection, *rank_rows[0])
                self._fire(fault_injector, FAULT_AFTER_PARTIAL_RANK_WRITE)
                for candidate_id, rank in rank_rows[1:]:
                    self._write_rank(connection, candidate_id, rank)
            else:
                self._fire(fault_injector, FAULT_AFTER_PARTIAL_RANK_WRITE)
            self._verify_written_ranks(connection, screener_run_id, rank_rows)

            self._fire(fault_injector, FAULT_BEFORE_SUCCESS_TRANSITION)
            timestamp = _utc_now()
            cursor = connection.execute(
                "UPDATE screener_runs SET canonical_sha256 = ?, finished_at = ?, "
                "updated_at = ?, error_code = NULL, status = 'success' "
                "WHERE screener_run_id = ? AND status = 'running' "
                "AND canonical_sha256 IS NULL",
                (
                    canonical_sha256,
                    timestamp,
                    timestamp,
                    screener_run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise ScreenerFinalizationStateError(
                    "Screener run could not transition atomically to success"
                )
            sealed = self._get_run(connection, screener_run_id)
            reconstructed = self._reconstruct_success(connection, sealed)
            if reconstructed.canonical_json() != canonical_json:
                raise ScreenerReplayIntegrityError(
                    "sealed Screener output differs from pre-write canonical output"
                )

        return ScreenerFinalizationResult(
            screener_run_id=screener_run_id,
            result=reconstructed,
            written=True,
        )

    def replay_run(self, screener_run_id: str) -> ScreenerFinalizationResult:
        """Strictly reconstruct a successful run without any write path."""

        _require_sha256(screener_run_id, "screener_run_id")
        with self._read_connection() as connection:
            self._require_v11(connection)
            run = self._get_run(connection, screener_run_id)
            result = self._reconstruct_success(connection, run)
        return ScreenerFinalizationResult(
            screener_run_id=screener_run_id,
            result=result,
            written=False,
        )

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        uri = self.database_path.resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA query_only = ON")
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def _require_v11(connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT name FROM schema_migrations WHERE version = 11"
        ).fetchone()
        if row is None or row["name"] != "market screener persistence":
            raise ScreenerFinalizationStateError(
                "Market Screener migration 11 must be applied explicitly"
            )

    @staticmethod
    def _get_run(
        connection: sqlite3.Connection, screener_run_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM screener_runs WHERE screener_run_id = ?",
            (screener_run_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown Screener run {screener_run_id}")
        return row

    @classmethod
    def _verify_run_identity(cls, run: sqlite3.Row) -> None:
        identity = {
            "identity_version": _candidate_store._RUN_ID_VERSION,
            "market_date": run["market_date"],
            "universe_run_id": run["universe_run_id"],
            "stage1_methodology_version": run["stage1_methodology_version"],
            "stage2_methodology_version": run["stage2_methodology_version"],
            "source_policy": run["source_policy"],
            "candidate_limit": run["candidate_limit"],
            "input_manifest_sha256": run["input_manifest_sha256"],
        }
        if _candidate_store._canonical_sha256(identity) != run["screener_run_id"]:
            raise ScreenerReplayIntegrityError(
                "stored Screener run conflicts with its deterministic identity"
            )
        if run["stage1_methodology_version"] != STAGE1_METHODOLOGY_VERSION:
            raise ScreenerReplayIntegrityError("Stage 1 methodology changed")
        if run["stage2_methodology_version"] != STAGE2_METHODOLOGY_VERSION:
            raise ScreenerReplayIntegrityError("Stage 2 methodology changed")
        if run["source_policy"] != TWSE_BASELINE_SOURCE_POLICY:
            raise ScreenerReplayIntegrityError("Screener source policy changed")

    @classmethod
    def _load_candidate_records(
        cls,
        connection: sqlite3.Connection,
        run: sqlite3.Row,
        *,
        require_final_rank: bool,
    ) -> tuple[_CandidateRecord, ...]:
        cls._verify_universe_parent(connection, run)
        run_artifacts = connection.execute(
            "SELECT COUNT(*) FROM screener_source_artifacts "
            "WHERE screener_run_id = ?",
            (run["screener_run_id"],),
        ).fetchone()[0]
        if run_artifacts != 0:
            raise ScreenerReplayIntegrityError(
                "S4.4 does not permit hidden run-owned source artifacts"
            )

        rows = connection.execute(
            "SELECT * FROM screener_candidates WHERE screener_run_id = ? "
            "ORDER BY stage1_rank, symbol",
            (run["screener_run_id"],),
        ).fetchall()
        if len(rows) != run["candidate_count"]:
            raise ScreenerReplayIntegrityError(
                "candidate row count differs from run candidate_count"
            )
        if tuple(row["stage1_rank"] for row in rows) != tuple(
            range(1, len(rows) + 1)
        ):
            raise ScreenerReplayIntegrityError("Stage 1 ranks are not contiguous")

        records: list[_CandidateRecord] = []
        for row in rows:
            if row["status"] != "success":
                raise ScreenerFinalizationStateError(
                    "every candidate must be successful before finalization"
                )
            stored_rank = row["rank"]
            if require_final_rank:
                if (
                    isinstance(stored_rank, bool)
                    or not isinstance(stored_rank, int)
                    or stored_rank < 1
                ):
                    raise ScreenerReplayIntegrityError(
                        "successful run contains an invalid final rank"
                    )
            elif stored_rank is not None:
                raise ScreenerFinalizationStateError(
                    "running run must not contain pre-finalized candidate ranks"
                )
            if row["universe_run_id"] != run["universe_run_id"]:
                raise ScreenerReplayIntegrityError("candidate Universe parent changed")
            expected_candidate_id = SQLiteScreenerCheckpointRepository._candidate_id(
                run["screener_run_id"], row["symbol"]
            )
            if row["candidate_id"] != expected_candidate_id:
                raise ScreenerReplayIntegrityError("candidate identity changed")
            _require_sha256(row["input_locator_sha256"], "input_locator_sha256")
            _require_sha256(row["snapshot_sha256"], "snapshot_sha256")
            _require_sha256(row["payload_sha256"], "payload_sha256")

            cls._verify_child_ordinals(connection, row)
            checkpoint = cls._reconstruct_checkpoint(connection, row)
            if checkpoint.checkpoint_status != "success" or checkpoint.failure is not None:
                raise ScreenerReplayIntegrityError(
                    "successful candidate reconstructed as a failure"
                )
            candidate = cls._candidate_from_checkpoint(
                checkpoint,
                stored_rank if require_final_rank else checkpoint.stage1_rank,
            )
            records.append(
                _CandidateRecord(
                    candidate_id=row["candidate_id"],
                    stored_rank=stored_rank,
                    checkpoint=checkpoint,
                    candidate=candidate,
                )
            )

        cls._verify_input_manifest(run, rows)
        return tuple(records)

    @staticmethod
    def _verify_child_ordinals(
        connection: sqlite3.Connection, row: sqlite3.Row
    ) -> None:
        reason_rows = connection.execute(
            "SELECT stage, ordinal FROM candidate_reasons WHERE candidate_id = ? "
            "ORDER BY CASE stage WHEN 'stage1' THEN 0 ELSE 1 END, ordinal",
            (row["candidate_id"],),
        ).fetchall()
        for stage, expected_count in (
            ("stage1", row["stage1_reason_count"]),
            ("stage2", row["stage2_reason_count"]),
        ):
            ordinals = tuple(
                item["ordinal"] for item in reason_rows if item["stage"] == stage
            )
            if ordinals != tuple(range(1, expected_count + 1)):
                raise ScreenerReplayIntegrityError(
                    f"{stage} reason ordering is not canonical"
                )
        metric_ordinals = tuple(
            item["ordinal"]
            for item in connection.execute(
                "SELECT ordinal FROM candidate_metrics WHERE candidate_id = ? "
                "ORDER BY ordinal",
                (row["candidate_id"],),
            )
        )
        if metric_ordinals != tuple(range(1, row["metric_count"] + 1)):
            raise ScreenerReplayIntegrityError(
                "candidate metric ordering is not canonical"
            )

    @staticmethod
    def _verify_universe_parent(
        connection: sqlite3.Connection, run: sqlite3.Row
    ) -> None:
        parent = connection.execute(
            "SELECT market_date, universe_count, source_policy, status, "
            "canonical_sha256 FROM market_universe_runs WHERE universe_run_id = ?",
            (run["universe_run_id"],),
        ).fetchone()
        if parent is None or parent["status"] != "success":
            raise ScreenerReplayIntegrityError(
                "Screener run lost its successful Universe parent"
            )
        if (
            parent["market_date"] != run["market_date"]
            or parent["universe_count"] != run["universe_count"]
            or parent["source_policy"] != run["source_policy"]
        ):
            raise ScreenerReplayIntegrityError("Universe parent metadata changed")
        _require_sha256(parent["canonical_sha256"], "universe canonical_sha256")

    @staticmethod
    def _verify_input_manifest(run: sqlite3.Row, rows: list[sqlite3.Row]) -> None:
        manifest = {
            "manifest_version": _candidate_store._MANIFEST_VERSION,
            "stage1_canonical_sha256": run["stage1_canonical_sha256"],
            "candidate_inputs": [
                {
                    "symbol": row["symbol"],
                    "input_locator_sha256": row["input_locator_sha256"],
                }
                for row in rows
            ],
        }
        if _candidate_store._canonical_sha256(manifest) != run[
            "input_manifest_sha256"
        ]:
            raise ScreenerReplayIntegrityError(
                "candidate handoff no longer matches the frozen input manifest"
            )

    @staticmethod
    def _reconstruct_checkpoint(
        connection: sqlite3.Connection, row: sqlite3.Row
    ) -> CandidateCheckpoint:
        # Minimal S4.4 adapter: S4.3 centralizes child/hash/source checks but
        # intentionally rejects a final rank.  A rankless row view preserves all
        # those checks without copying its reconstruction semantics.
        rankless_row = dict(row)
        rankless_row["rank"] = None
        try:
            return SQLiteScreenerCheckpointRepository._reconstruct_candidate(
                connection, rankless_row
            )
        except (ScreenerCheckpointError, ValueError, TypeError, KeyError) as error:
            raise ScreenerReplayIntegrityError(
                "candidate checkpoint failed canonical reconstruction"
            ) from error

    @staticmethod
    def _candidate_from_checkpoint(
        checkpoint: CandidateCheckpoint, rank: int
    ) -> Stage2Candidate:
        return Stage2Candidate(
            rank=rank,
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

    @classmethod
    def _reconstruct_success(
        cls, connection: sqlite3.Connection, run: sqlite3.Row
    ) -> FrozenScreenerResult:
        cls._verify_run_identity(run)
        if (
            run["status"] != "success"
            or run["canonical_sha256"] is None
            or run["finished_at"] is None
        ):
            raise ScreenerFinalizationStateError(
                "only a sealed successful Screener run can be replayed"
            )
        _require_sha256(run["canonical_sha256"], "canonical_sha256")
        records = cls._load_candidate_records(
            connection,
            run,
            require_final_rank=True,
        )
        stored_order = tuple(
            sorted(records, key=lambda item: (item.stored_rank, item.checkpoint.symbol))
        )
        if tuple(item.stored_rank for item in stored_order) != tuple(
            range(1, len(stored_order) + 1)
        ):
            raise ScreenerReplayIntegrityError("final ranks are not contiguous")

        # Replay trusts no implicit SQLite order, but it also does not re-rank.
        # Final rank is immutable persisted output; canonical reconstruction and
        # the sealed run hash detect any subsequent rank mutation.
        result = cls._build_result(
            run,
            tuple(item.candidate for item in stored_order),
        )
        if result.payload_sha256 != run["canonical_sha256"]:
            raise ScreenerReplayIntegrityError(
                "stored Screener canonical hash does not match reconstructed output"
            )
        return result

    @staticmethod
    def _build_result(
        run: sqlite3.Row, candidates: tuple[Stage2Candidate, ...]
    ) -> FrozenScreenerResult:
        return FrozenScreenerResult(
            screener_run_id=run["screener_run_id"],
            universe_run_id=run["universe_run_id"],
            market_date=date.fromisoformat(run["market_date"]),
            stage1_methodology_version=run["stage1_methodology_version"],
            stage2_methodology_version=run["stage2_methodology_version"],
            source_policy=run["source_policy"],
            universe_count=run["universe_count"],
            screened_count=run["screened_count"],
            triggered_count=run["triggered_count"],
            candidate_count=run["candidate_count"],
            candidate_limit=run["candidate_limit"],
            truncated=bool(run["truncated"]),
            candidates=candidates,
        )

    @staticmethod
    def _write_rank(
        connection: sqlite3.Connection, candidate_id: str, rank: int
    ) -> None:
        cursor = connection.execute(
            "UPDATE screener_candidates SET rank = ? "
            "WHERE candidate_id = ? AND status = 'success' AND rank IS NULL",
            (rank, candidate_id),
        )
        if cursor.rowcount != 1:
            raise ScreenerFinalizationStateError(
                "candidate rank could not be written atomically"
            )

    @staticmethod
    def _verify_written_ranks(
        connection: sqlite3.Connection,
        screener_run_id: str,
        rank_rows: tuple[tuple[str, int], ...],
    ) -> None:
        rows = connection.execute(
            "SELECT candidate_id, rank FROM screener_candidates "
            "WHERE screener_run_id = ? ORDER BY rank, candidate_id",
            (screener_run_id,),
        ).fetchall()
        actual = tuple((row["candidate_id"], row["rank"]) for row in rows)
        expected = tuple(sorted(rank_rows, key=lambda item: (item[1], item[0])))
        if actual != expected:
            raise ScreenerReplayIntegrityError("candidate ranks were partially written")

    @staticmethod
    def _fire(
        fault_injector: _FaultInjector | None,
        point: str,
    ) -> None:
        if fault_injector is not None:
            fault_injector(point)


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ScreenerReplayIntegrityError(
            f"{field_name} must be lowercase SHA-256"
        )
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


__all__ = [
    "FAULT_AFTER_CANDIDATE_VERIFICATION",
    "FAULT_AFTER_CANONICAL_HASH_VERIFICATION",
    "FAULT_AFTER_CANONICAL_RECONSTRUCTION",
    "FAULT_AFTER_PARTIAL_RANK_WRITE",
    "FAULT_AFTER_RANKING",
    "FAULT_BEFORE_SUCCESS_TRANSITION",
    "FINALIZATION_FAULT_POINTS",
    "FROZEN_SCREENER_RESULT_VERSION",
    "FrozenScreenerResult",
    "SQLiteScreenerReplayRepository",
    "ScreenerFinalizationResult",
    "ScreenerFinalizationStateError",
    "ScreenerReplayError",
    "ScreenerReplayIntegrityError",
]
