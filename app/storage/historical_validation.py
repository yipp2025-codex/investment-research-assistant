"""Durable cross-validation state for two completed historical source runs."""

from __future__ import annotations

import math
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from typing import Iterator, Sequence
from uuid import uuid4

from app.models import (
    CrossValidationOutcome,
    HistoricalSyncRun,
    HistoricalValidationDiscrepancy,
    HistoricalValidationRun,
    PipelineRunStatus,
    ResearchNote,
)

from .sqlite import SQLiteResearchRepository


class HistoricalValidationError(RuntimeError):
    """Base class for historical validation state errors."""


class HistoricalValidationConflictError(HistoricalValidationError):
    """An existing validation checkpoint has a different contract."""


class HistoricalValidationInProgressError(HistoricalValidationError):
    """A running validation requires its exact run id for recovery."""


class HistoricalValidationStateError(HistoricalValidationError):
    """A historical validation attempted an invalid state transition."""


class SQLiteHistoricalValidationRepository:
    """Persist comparison results without modifying either source observation."""

    def __init__(self, research_repository: SQLiteResearchRepository) -> None:
        self.research_repository = research_repository

    def get_or_create(
        self,
        *,
        left_run: HistoricalSyncRun,
        right_run: HistoricalSyncRun,
        created_at: datetime,
    ) -> HistoricalValidationRun:
        self._validate_source_pair(left_run, right_run)
        if created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        run_id = str(uuid4())
        timestamp = created_at.isoformat()
        with self.research_repository._transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO historical_validation_runs ("
                "run_id, symbol, target_date, target_observations, "
                "left_provider, right_provider, left_historical_run_id, "
                "right_historical_run_id, status, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    left_run.symbol,
                    left_run.target_date.isoformat(),
                    left_run.target_observations,
                    left_run.provider,
                    right_run.provider,
                    left_run.run_id,
                    right_run.run_id,
                    PipelineRunStatus.PENDING.value,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                self._select()
                + " WHERE left_historical_run_id = ? "
                "AND right_historical_run_id = ?",
                (left_run.run_id, right_run.run_id),
            ).fetchone()
        if row is None:  # pragma: no cover - UNIQUE/INSERT contract.
            raise HistoricalValidationStateError(
                "historical validation run could not be created"
            )
        run = self._from_row(row)
        expected = (
            left_run.symbol,
            left_run.target_date,
            left_run.target_observations,
            left_run.provider,
            right_run.provider,
        )
        actual = (
            run.symbol,
            run.target_date,
            run.target_observations,
            run.left_provider,
            run.right_provider,
        )
        if actual != expected:
            raise HistoricalValidationConflictError(
                "existing historical validation contract does not match sources"
            )
        return run

    def get(self, run_id: str) -> HistoricalValidationRun | None:
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else self._from_row(row)

    def list(self, symbol: str | None = None) -> list[HistoricalValidationRun]:
        query = self._select()
        parameters: tuple[object, ...] = ()
        if symbol is not None:
            query += " WHERE symbol = ?"
            parameters = (symbol.strip().upper(),)
        query += " ORDER BY target_date, target_observations, run_id"
        with self.research_repository._transaction() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._from_row(row) for row in rows]

    def start(
        self,
        run_id: str,
        *,
        started_at: datetime,
        resume_running: bool = False,
    ) -> HistoricalValidationRun:
        if started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")
        timestamp = started_at.isoformat()
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise HistoricalValidationStateError(
                    f"unknown historical validation run {run_id}"
                )
            current = self._from_row(row)
            if current.status is PipelineRunStatus.SUCCESS:
                raise HistoricalValidationStateError(
                    "successful historical validation cannot restart"
                )
            if current.status is PipelineRunStatus.RUNNING and not resume_running:
                raise HistoricalValidationInProgressError(
                    f"historical validation run {run_id} is already running"
                )
            cursor = connection.execute(
                "UPDATE historical_validation_runs SET status = ?, "
                "started_at = COALESCE(started_at, ?), finished_at = NULL, "
                "error_message = NULL, outcome = NULL, research_note_id = NULL, "
                "attempt_count = attempt_count + 1, updated_at = ? "
                "WHERE run_id = ? AND status = ?",
                (
                    PipelineRunStatus.RUNNING.value,
                    timestamp,
                    timestamp,
                    run_id,
                    current.status.value,
                ),
            )
            if cursor.rowcount != 1:
                raise HistoricalValidationInProgressError(
                    "historical validation changed state while being claimed"
                )
            updated = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._from_row(updated)

    def mark_failed(
        self,
        run_id: str,
        *,
        finished_at: datetime,
        error_message: str,
    ) -> HistoricalValidationRun:
        if finished_at.utcoffset() is None:
            raise ValueError("finished_at must be timezone-aware")
        timestamp = finished_at.isoformat()
        safe_error = error_message.replace("\r", " ").replace("\n", " ").strip()
        safe_error = safe_error[:1000] or "UnknownError"
        with self.research_repository._transaction() as connection:
            cursor = connection.execute(
                "UPDATE historical_validation_runs SET status = ?, "
                "finished_at = ?, error_message = ?, outcome = NULL, "
                "research_note_id = NULL, updated_at = ? "
                "WHERE run_id = ? AND status = ?",
                (
                    PipelineRunStatus.FAILED.value,
                    timestamp,
                    safe_error,
                    timestamp,
                    run_id,
                    PipelineRunStatus.RUNNING.value,
                ),
            )
            if cursor.rowcount != 1:
                raise HistoricalValidationStateError(
                    "historical validation was not running at failure"
                )
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._from_row(row)

    @contextmanager
    def successful_run(
        self, run_id: str
    ) -> Iterator["SQLiteHistoricalValidationUnit"]:
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                "SELECT status FROM historical_validation_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None or row["status"] != PipelineRunStatus.RUNNING.value:
                raise HistoricalValidationStateError(
                    "historical validation completion requires a running run"
                )
            unit = SQLiteHistoricalValidationUnit(self, connection, run_id)
            yield unit
            final = connection.execute(
                "SELECT status FROM historical_validation_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if final["status"] != PipelineRunStatus.SUCCESS.value:
                raise HistoricalValidationStateError(
                    "historical validation transaction ended without success"
                )

    def list_discrepancies(
        self, run_id: str
    ) -> list[HistoricalValidationDiscrepancy]:
        with self.research_repository._transaction() as connection:
            rows = connection.execute(
                "SELECT id, run_id, trade_date, field, left_value, right_value, "
                "reason, absolute_difference, relative_difference_pct "
                "FROM historical_validation_discrepancies WHERE run_id = ? "
                "ORDER BY trade_date, field",
                (run_id,),
            ).fetchall()
        return [self._discrepancy_from_row(row) for row in rows]

    @staticmethod
    def _validate_source_pair(
        left: HistoricalSyncRun, right: HistoricalSyncRun
    ) -> None:
        if left.status is not PipelineRunStatus.SUCCESS or right.status is not PipelineRunStatus.SUCCESS:
            raise HistoricalValidationStateError(
                "historical source runs must both be successful"
            )
        if left.run_id == right.run_id or left.provider == right.provider:
            raise HistoricalValidationConflictError(
                "historical validation requires two independent providers"
            )
        if (
            left.symbol != right.symbol
            or left.target_date != right.target_date
            or left.target_observations != right.target_observations
        ):
            raise HistoricalValidationConflictError(
                "historical source run contracts do not align"
            )

    @staticmethod
    def _select() -> str:
        return (
            "SELECT run_id, symbol, target_date, target_observations, "
            "left_provider, right_provider, left_historical_run_id, "
            "right_historical_run_id, status, outcome, common_date_count, "
            "matched_date_count, left_only_date_count, right_only_date_count, "
            "field_discrepancy_count, left_latest_date, right_latest_date, "
            "created_at, started_at, finished_at, error_message, attempt_count, "
            "research_note_id FROM historical_validation_runs"
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> HistoricalValidationRun:
        return HistoricalValidationRun(
            run_id=row["run_id"],
            symbol=row["symbol"],
            target_date=date.fromisoformat(row["target_date"]),
            target_observations=row["target_observations"],
            left_provider=row["left_provider"],
            right_provider=row["right_provider"],
            left_historical_run_id=row["left_historical_run_id"],
            right_historical_run_id=row["right_historical_run_id"],
            status=PipelineRunStatus(row["status"]),
            outcome=(
                None
                if row["outcome"] is None
                else CrossValidationOutcome(row["outcome"])
            ),
            common_date_count=row["common_date_count"],
            matched_date_count=row["matched_date_count"],
            left_only_date_count=row["left_only_date_count"],
            right_only_date_count=row["right_only_date_count"],
            field_discrepancy_count=row["field_discrepancy_count"],
            left_latest_date=(
                None
                if row["left_latest_date"] is None
                else date.fromisoformat(row["left_latest_date"])
            ),
            right_latest_date=(
                None
                if row["right_latest_date"] is None
                else date.fromisoformat(row["right_latest_date"])
            ),
            created_at=datetime.fromisoformat(row["created_at"]),
            started_at=(
                None
                if row["started_at"] is None
                else datetime.fromisoformat(row["started_at"])
            ),
            finished_at=(
                None
                if row["finished_at"] is None
                else datetime.fromisoformat(row["finished_at"])
            ),
            error_message=row["error_message"],
            attempt_count=row["attempt_count"],
            research_note_id=row["research_note_id"],
        )

    @staticmethod
    def _discrepancy_from_row(
        row: sqlite3.Row,
    ) -> HistoricalValidationDiscrepancy:
        return HistoricalValidationDiscrepancy(
            id=row["id"],
            run_id=row["run_id"],
            trade_date=date.fromisoformat(row["trade_date"]),
            field=row["field"],
            left_value=row["left_value"],
            right_value=row["right_value"],
            reason=row["reason"],
            absolute_difference=row["absolute_difference"],
            relative_difference_pct=row["relative_difference_pct"],
        )


class SQLiteHistoricalValidationUnit:
    """Atomically save discrepancy rows, note, and successful checkpoint."""

    def __init__(
        self,
        repository: SQLiteHistoricalValidationRepository,
        connection: sqlite3.Connection,
        run_id: str,
    ) -> None:
        self.repository = repository
        self.connection = connection
        self.run_id = run_id

    def create_research_note(self, note: ResearchNote) -> ResearchNote:
        if (
            note.historical_validation_run_id != self.run_id
            or note.run_id is not None
            or note.historical_run_id is not None
        ):
            raise HistoricalValidationStateError(
                "historical validation note linkage is invalid"
            )
        return self.repository.research_repository._insert_research_note(
            self.connection, note
        )

    def complete(
        self,
        research_note_id: int,
        discrepancies: Sequence[HistoricalValidationDiscrepancy],
        *,
        common_date_count: int,
        matched_date_count: int,
        left_only_date_count: int,
        right_only_date_count: int,
        field_discrepancy_count: int,
        left_latest_date: date,
        right_latest_date: date,
        finished_at: datetime,
    ) -> CrossValidationOutcome:
        counts = (
            common_date_count,
            matched_date_count,
            left_only_date_count,
            right_only_date_count,
            field_discrepancy_count,
        )
        if any(value < 0 for value in counts) or matched_date_count > common_date_count:
            raise HistoricalValidationStateError(
                "historical validation counts are invalid"
            )
        if (
            len(discrepancies)
            != left_only_date_count
            + right_only_date_count
            + field_discrepancy_count
        ):
            raise HistoricalValidationStateError(
                "historical validation counts do not match discrepancy rows"
            )
        if finished_at.utcoffset() is None:
            raise ValueError("finished_at must be timezone-aware")
        note = self.connection.execute(
            "SELECT historical_validation_run_id FROM research_notes WHERE id = ?",
            (research_note_id,),
        ).fetchone()
        if note is None or note["historical_validation_run_id"] != self.run_id:
            raise HistoricalValidationStateError(
                "successful validation must reference its own research note"
            )

        seen: set[tuple[date, str]] = set()
        for item in discrepancies:
            key = (item.trade_date, item.field.strip())
            if item.run_id != self.run_id or not key[1] or key in seen:
                raise HistoricalValidationStateError(
                    "historical discrepancy identity is invalid or duplicated"
                )
            if not item.reason.strip() or (
                item.left_value is None and item.right_value is None
            ):
                raise HistoricalValidationStateError(
                    "historical discrepancy values and reason are required"
                )
            for value in (
                item.absolute_difference,
                item.relative_difference_pct,
            ):
                if value is not None and (not math.isfinite(value) or value < 0):
                    raise HistoricalValidationStateError(
                        "historical discrepancy differences must be non-negative"
                    )
            seen.add(key)
            self.connection.execute(
                "INSERT INTO historical_validation_discrepancies ("
                "run_id, trade_date, field, left_value, right_value, reason, "
                "absolute_difference, relative_difference_pct"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    self.run_id,
                    item.trade_date.isoformat(),
                    key[1],
                    item.left_value,
                    item.right_value,
                    item.reason.strip(),
                    item.absolute_difference,
                    item.relative_difference_pct,
                ),
            )

        outcome = (
            CrossValidationOutcome.DISCREPANCY
            if discrepancies
            else CrossValidationOutcome.MATCH
        )
        timestamp = finished_at.isoformat()
        cursor = self.connection.execute(
            "UPDATE historical_validation_runs SET status = ?, outcome = ?, "
            "common_date_count = ?, matched_date_count = ?, "
            "left_only_date_count = ?, right_only_date_count = ?, "
            "field_discrepancy_count = ?, left_latest_date = ?, "
            "right_latest_date = ?, research_note_id = ?, finished_at = ?, "
            "error_message = NULL, updated_at = ? "
            "WHERE run_id = ? AND status = ?",
            (
                PipelineRunStatus.SUCCESS.value,
                outcome.value,
                common_date_count,
                matched_date_count,
                left_only_date_count,
                right_only_date_count,
                field_discrepancy_count,
                left_latest_date.isoformat(),
                right_latest_date.isoformat(),
                research_note_id,
                timestamp,
                timestamp,
                self.run_id,
                PipelineRunStatus.RUNNING.value,
            ),
        )
        if cursor.rowcount != 1:
            raise HistoricalValidationStateError(
                "historical validation could not transition to success"
            )
        return outcome
