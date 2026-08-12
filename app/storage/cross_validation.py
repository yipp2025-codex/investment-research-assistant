"""Durable, source-preserving market-data cross-validation storage."""

from __future__ import annotations

import json
import math
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from typing import Iterator, Mapping, Sequence
from uuid import uuid4

from app.models import (
    CrossValidationOutcome,
    CrossValidationRun,
    MarketDataDiscrepancy,
    MarketDataObservation,
    PipelineRunStatus,
    SourceArtifact,
)

from .sqlite import SQLiteResearchRepository


class CrossValidationError(RuntimeError):
    """Base class for durable cross-validation state errors."""


class CrossValidationConflictError(CrossValidationError):
    """An existing checkpoint has a different immutable contract."""


class CrossValidationInProgressError(CrossValidationError):
    """A running checkpoint requires its exact run id for recovery."""


class CrossValidationStateError(CrossValidationError):
    """A cross-validation run attempted an invalid state transition."""


class SQLiteCrossValidationRepository:
    """Persist independent observations without touching canonical daily prices."""

    def __init__(self, research_repository: SQLiteResearchRepository) -> None:
        self.research_repository = research_repository

    def get_or_create_run(
        self,
        *,
        symbol: str,
        target_date: date,
        requested_start_date: date,
        left_provider: str,
        right_provider: str,
        created_at: datetime,
    ) -> CrossValidationRun:
        normalized_symbol = symbol.strip().upper()
        left = left_provider.strip()
        right = right_provider.strip()
        if not normalized_symbol:
            raise ValueError("symbol must not be empty")
        if not left or not right:
            raise ValueError("provider names must not be empty")
        if left == right:
            raise ValueError("cross-validation providers must be different")
        if requested_start_date > target_date:
            raise ValueError("requested_start_date must not be after target_date")
        if created_at.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")

        run_id = str(uuid4())
        timestamp = created_at.isoformat()
        with self.research_repository._transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO market_data_validation_runs ("
                "run_id, symbol, target_date, requested_start_date, "
                "left_provider, right_provider, status, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    normalized_symbol,
                    target_date.isoformat(),
                    requested_start_date.isoformat(),
                    left,
                    right,
                    PipelineRunStatus.PENDING.value,
                    timestamp,
                    timestamp,
                ),
            )
            row = connection.execute(
                self._select()
                + " WHERE symbol = ? AND target_date = ? "
                "AND left_provider = ? AND right_provider = ?",
                (normalized_symbol, target_date.isoformat(), left, right),
            ).fetchone()
        run = self._from_row(row)
        if run.requested_start_date != requested_start_date:
            raise CrossValidationConflictError(
                "existing validation run conflicts on requested_start_date"
            )
        return run

    def get_run(self, run_id: str) -> CrossValidationRun | None:
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return None if row is None else self._from_row(row)

    def list_runs(self, symbol: str | None = None) -> list[CrossValidationRun]:
        query = self._select()
        parameters: tuple[object, ...] = ()
        if symbol is not None:
            query += " WHERE symbol = ?"
            parameters = (symbol.strip().upper(),)
        query += " ORDER BY created_at, run_id"
        with self.research_repository._transaction() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [self._from_row(row) for row in rows]

    def start(
        self,
        run_id: str,
        *,
        started_at: datetime,
        resume_running: bool = False,
    ) -> CrossValidationRun:
        if started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")
        timestamp = started_at.isoformat()
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None:
                raise CrossValidationStateError(f"unknown validation run {run_id}")
            current = self._from_row(row)
            if current.status is PipelineRunStatus.SUCCESS:
                raise CrossValidationStateError(
                    "successful validation run cannot restart"
                )
            if current.status is PipelineRunStatus.RUNNING and not resume_running:
                raise CrossValidationInProgressError(
                    f"validation run {run_id} is already running"
                )
            cursor = connection.execute(
                "UPDATE market_data_validation_runs SET status = ?, "
                "started_at = COALESCE(started_at, ?), finished_at = NULL, "
                "error_message = NULL, outcome = NULL, "
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
                raise CrossValidationInProgressError(
                    f"validation run {run_id} changed state while being claimed"
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
    ) -> CrossValidationRun:
        if finished_at.utcoffset() is None:
            raise ValueError("finished_at must be timezone-aware")
        timestamp = finished_at.isoformat()
        safe_error = error_message.replace("\r", " ").replace("\n", " ").strip()
        safe_error = safe_error[:1000] or "UnknownError"
        with self.research_repository._transaction() as connection:
            cursor = connection.execute(
                "UPDATE market_data_validation_runs SET status = ?, "
                "finished_at = ?, error_message = ?, outcome = NULL, "
                "updated_at = ? WHERE run_id = ? AND status = ?",
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
                raise CrossValidationStateError(
                    "validation run was not running when failure was recorded"
                )
            row = connection.execute(
                self._select() + " WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._from_row(row)

    @contextmanager
    def successful_run(
        self, run_id: str
    ) -> Iterator["SQLiteCrossValidationUnit"]:
        with self.research_repository._transaction() as connection:
            row = connection.execute(
                "SELECT status FROM market_data_validation_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None or row["status"] != PipelineRunStatus.RUNNING.value:
                raise CrossValidationStateError(
                    "validation completion requires a running run"
                )
            unit = SQLiteCrossValidationUnit(self, connection, run_id)
            yield unit
            final_row = connection.execute(
                "SELECT status FROM market_data_validation_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if final_row["status"] != PipelineRunStatus.SUCCESS.value:
                raise CrossValidationStateError(
                    "validation transaction ended without success"
                )

    def list_observations(self, run_id: str) -> list[MarketDataObservation]:
        with self.research_repository._transaction() as connection:
            rows = connection.execute(
                "SELECT o.id, o.run_id, o.provider, o.symbol, o.market_date, "
                "o.open_price, o.high_price, o.low_price, o.close_price, "
                "o.volume, o.source_endpoints_json, o.fetched_at, "
                "o.source_timestamp_raw, o.source_timestamp "
                "FROM market_data_observations AS o "
                "JOIN market_data_validation_runs AS r ON r.run_id = o.run_id "
                "WHERE o.run_id = ? "
                "ORDER BY CASE o.provider WHEN r.left_provider THEN 0 ELSE 1 END",
                (run_id,),
            ).fetchall()
        return [self._observation_from_row(row) for row in rows]

    def list_discrepancies(self, run_id: str) -> list[MarketDataDiscrepancy]:
        with self.research_repository._transaction() as connection:
            rows = connection.execute(
                "SELECT id, run_id, field, left_value, right_value, reason, "
                "absolute_difference, relative_difference_pct "
                "FROM market_data_discrepancies WHERE run_id = ? ORDER BY field",
                (run_id,),
            ).fetchall()
        return [
            MarketDataDiscrepancy(
                id=row["id"],
                run_id=row["run_id"],
                field=row["field"],
                left_value=row["left_value"],
                right_value=row["right_value"],
                reason=row["reason"],
                absolute_difference=row["absolute_difference"],
                relative_difference_pct=row["relative_difference_pct"],
            )
            for row in rows
        ]

    @staticmethod
    def _select() -> str:
        return (
            "SELECT run_id, symbol, target_date, requested_start_date, "
            "left_provider, right_provider, status, outcome, created_at, "
            "started_at, finished_at, error_message, attempt_count "
            "FROM market_data_validation_runs"
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> CrossValidationRun:
        return CrossValidationRun(
            run_id=row["run_id"],
            symbol=row["symbol"],
            target_date=date.fromisoformat(row["target_date"]),
            requested_start_date=date.fromisoformat(row["requested_start_date"]),
            left_provider=row["left_provider"],
            right_provider=row["right_provider"],
            status=PipelineRunStatus(row["status"]),
            outcome=(
                None
                if row["outcome"] is None
                else CrossValidationOutcome(row["outcome"])
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
        )

    @staticmethod
    def _observation_from_row(row: sqlite3.Row) -> MarketDataObservation:
        try:
            endpoints = json.loads(row["source_endpoints_json"])
        except json.JSONDecodeError as error:
            raise CrossValidationStateError(
                "stored observation endpoints are invalid"
            ) from error
        if not isinstance(endpoints, list) or any(
            not isinstance(item, str) or not item for item in endpoints
        ):
            raise CrossValidationStateError(
                "stored observation endpoints must be a string array"
            )
        return MarketDataObservation(
            id=row["id"],
            run_id=row["run_id"],
            provider=row["provider"],
            symbol=row["symbol"],
            market_date=date.fromisoformat(row["market_date"]),
            open=row["open_price"],
            high=row["high_price"],
            low=row["low_price"],
            close=row["close_price"],
            volume=row["volume"],
            source_endpoints=tuple(endpoints),
            fetched_at=datetime.fromisoformat(row["fetched_at"]),
            source_timestamp_raw=row["source_timestamp_raw"],
            source_timestamp=(
                None
                if row["source_timestamp"] is None
                else datetime.fromisoformat(row["source_timestamp"])
            ),
        )


class SQLiteCrossValidationUnit:
    """Atomically persist both observations, discrepancies, and success."""

    def __init__(
        self,
        repository: SQLiteCrossValidationRepository,
        connection: sqlite3.Connection,
        run_id: str,
    ) -> None:
        self.repository = repository
        self.connection = connection
        self.run_id = run_id

    def complete(
        self,
        observations: Sequence[MarketDataObservation],
        discrepancies: Sequence[MarketDataDiscrepancy],
        *,
        finished_at: datetime,
        source_artifacts: Mapping[str, Sequence[SourceArtifact]] | None = None,
    ) -> CrossValidationOutcome:
        if finished_at.utcoffset() is None:
            raise ValueError("finished_at must be timezone-aware")
        row = self.connection.execute(
            self.repository._select() + " WHERE run_id = ?", (self.run_id,)
        ).fetchone()
        if row is None:
            raise CrossValidationStateError(
                f"unknown validation run {self.run_id}"
            )
        run = self.repository._from_row(row)
        if run.status is not PipelineRunStatus.RUNNING:
            raise CrossValidationStateError(
                "validation completion requires a running run"
            )
        if len(observations) != 2:
            raise CrossValidationStateError(
                "validation completion requires exactly two observations"
            )
        expected_providers = {run.left_provider, run.right_provider}
        if {observation.provider for observation in observations} != expected_providers:
            raise CrossValidationStateError(
                "validation observations do not match the provider pair"
            )
        artifacts_by_provider = source_artifacts or {}
        unknown_artifact_providers = set(artifacts_by_provider) - expected_providers
        if unknown_artifact_providers:
            raise CrossValidationStateError(
                "source artifacts contain a provider outside the validation pair"
            )
        for provider in (run.left_provider, run.right_provider):
            self.repository.research_repository._insert_source_artifacts(
                self.connection,
                artifacts_by_provider.get(provider, ()),
                provider=provider,
                checkpoint_key="validation-success-v1",
                created_at=finished_at,
                validation_run_id=self.run_id,
            )

        for observation in observations:
            self._validate_observation(observation, run)
            endpoints_json = json.dumps(
                list(observation.source_endpoints),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            self.connection.execute(
                "INSERT INTO market_data_observations ("
                "run_id, provider, symbol, market_date, open_price, high_price, "
                "low_price, close_price, volume, source_endpoints_json, "
                "fetched_at, source_timestamp_raw, source_timestamp"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    observation.run_id,
                    observation.provider,
                    observation.symbol,
                    observation.market_date.isoformat(),
                    observation.open,
                    observation.high,
                    observation.low,
                    observation.close,
                    observation.volume,
                    endpoints_json,
                    observation.fetched_at.isoformat(),
                    observation.source_timestamp_raw,
                    (
                        None
                        if observation.source_timestamp is None
                        else observation.source_timestamp.isoformat()
                    ),
                ),
            )

        seen_fields: set[str] = set()
        for discrepancy in discrepancies:
            if discrepancy.run_id != self.run_id:
                raise CrossValidationStateError(
                    "discrepancy run_id does not match the active run"
                )
            field = discrepancy.field.strip()
            reason = discrepancy.reason.strip()
            if not field or not reason or field in seen_fields:
                raise CrossValidationStateError(
                    "discrepancy fields and reasons must be unique and non-blank"
                )
            if discrepancy.left_value is None and discrepancy.right_value is None:
                raise CrossValidationStateError(
                    "discrepancy must preserve at least one source value"
                )
            for value in (
                discrepancy.absolute_difference,
                discrepancy.relative_difference_pct,
            ):
                if value is not None and (not math.isfinite(value) or value < 0):
                    raise CrossValidationStateError(
                        "discrepancy numeric differences must be finite and non-negative"
                    )
            seen_fields.add(field)
            self.connection.execute(
                "INSERT INTO market_data_discrepancies ("
                "run_id, field, left_value, right_value, reason, "
                "absolute_difference, relative_difference_pct"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    self.run_id,
                    field,
                    discrepancy.left_value,
                    discrepancy.right_value,
                    reason,
                    discrepancy.absolute_difference,
                    discrepancy.relative_difference_pct,
                ),
            )

        outcome = (
            CrossValidationOutcome.DISCREPANCY
            if discrepancies
            else CrossValidationOutcome.MATCH
        )
        timestamp = finished_at.isoformat()
        cursor = self.connection.execute(
            "UPDATE market_data_validation_runs SET status = ?, outcome = ?, "
            "finished_at = ?, error_message = NULL, updated_at = ? "
            "WHERE run_id = ? AND status = ?",
            (
                PipelineRunStatus.SUCCESS.value,
                outcome.value,
                timestamp,
                timestamp,
                self.run_id,
                PipelineRunStatus.RUNNING.value,
            ),
        )
        if cursor.rowcount != 1:
            raise CrossValidationStateError(
                "validation run could not transition to success"
            )
        return outcome

    @staticmethod
    def _validate_observation(
        observation: MarketDataObservation, run: CrossValidationRun
    ) -> None:
        if observation.id is not None:
            raise CrossValidationStateError(
                "new validation observation must not already have an id"
            )
        if observation.run_id != run.run_id or observation.symbol != run.symbol:
            raise CrossValidationStateError(
                "validation observation identity does not match the run"
            )
        if not run.requested_start_date <= observation.market_date <= run.target_date:
            raise CrossValidationStateError(
                "validation observation market date is outside the requested range"
            )
        if not observation.source_endpoints or any(
            not endpoint.strip() for endpoint in observation.source_endpoints
        ):
            raise CrossValidationStateError(
                "validation observation requires source endpoints"
            )
        if len(set(observation.source_endpoints)) != len(
            observation.source_endpoints
        ):
            raise CrossValidationStateError(
                "validation observation endpoints must be unique"
            )
        if observation.fetched_at.utcoffset() is None:
            raise CrossValidationStateError(
                "validation fetched_at must be timezone-aware"
            )
        if (
            observation.source_timestamp is not None
            and observation.source_timestamp.utcoffset() is None
        ):
            raise CrossValidationStateError(
                "validation source timestamp must be timezone-aware"
            )
        if (
            observation.source_timestamp_raw is not None
            and not observation.source_timestamp_raw.strip()
        ):
            raise CrossValidationStateError(
                "validation source timestamp raw value must not be blank"
            )
