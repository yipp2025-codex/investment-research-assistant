"""Read-only historical Screener models for S4.4.

Queries in this module consume only successful v11 persistence rows.  They do
not migrate, acquire market data, run research, or touch symbols/watchlists.
"""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterator, TypeAlias


Scalar: TypeAlias = str | int | float | None
_SYMBOL = re.compile(r"^[0-9A-Z]{2,12}$")
_STAGES = frozenset({"stage1", "stage2"})


class ScreenerHistoryError(RuntimeError):
    """Historical Screener query input or persistence state is invalid."""


@dataclass(frozen=True, slots=True)
class ReasonFrequencyRecord:
    stage: str
    code: str
    metric: str
    count: int


@dataclass(frozen=True, slots=True)
class DailyCandidateRecord:
    market_date: date
    screener_run_id: str
    candidate_id: str
    stage1_methodology_version: str
    stage2_methodology_version: str
    symbol: str
    rank: int
    candidate_kind: str
    stage1_rank: int
    stage1_trigger_count: int
    stage1_reason_count: int
    stage2_reason_count: int
    data_quality_status: str
    validation_status: str
    analysis_status: str
    research_data_quality: str = "canonical"
    execution_status: str = "success"
    dataset_version_id: str | None = None
    source_status: str = "canonical_complete"
    authority_status: str = "complete"
    reconciliation_status: str = "not_applicable"
    supplemental_count: int = 0


@dataclass(frozen=True, slots=True)
class DataQualityFrequencyRecord:
    """Aggregated historical DS5 quality/status visibility for sealed runs."""

    market_date: date
    research_data_quality: str
    execution_status: str
    run_count: int
    candidate_count: int
    supplemental_candidate_count: int
    pending_reconciliation_count: int
    discrepancy_count: int


@dataclass(frozen=True, slots=True)
class ReasonHistoryRecord:
    market_date: date
    screener_run_id: str
    symbol: str
    rank: int
    stage: str
    ordinal: int
    code: str
    metric: str
    previous: Scalar
    current: Scalar
    delta: float | None
    threshold: float | str
    unit: str
    operator: str
    rule_version: str


@dataclass(frozen=True, slots=True)
class MethodologySelector:
    stage1_methodology_version: str
    stage2_methodology_version: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "stage1_methodology_version",
            _require_text(
                self.stage1_methodology_version,
                "stage1_methodology_version",
            ),
        )
        object.__setattr__(
            self,
            "stage2_methodology_version",
            _require_text(
                self.stage2_methodology_version,
                "stage2_methodology_version",
            ),
        )


@dataclass(frozen=True, slots=True)
class MethodologySnapshot:
    selector: MethodologySelector
    market_date: date
    run_ids: tuple[str, ...]
    candidate_symbols: tuple[str, ...]
    selection_count: int
    reason_frequencies: tuple[ReasonFrequencyRecord, ...]

    @property
    def run_count(self) -> int:
        return len(self.run_ids)

    @property
    def candidate_set_count(self) -> int:
        return len(self.candidate_symbols)


@dataclass(frozen=True, slots=True)
class MethodologyComparison:
    market_date: date
    left: MethodologySnapshot
    right: MethodologySnapshot
    selection_overlap: tuple[str, ...]
    left_only: tuple[str, ...]
    right_only: tuple[str, ...]


class SQLiteScreenerHistoryReader:
    """Deterministic, query-only historical views over successful runs."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def selection_count(self, symbol: str) -> int:
        normalized = _normalize_symbol(symbol)
        with self._read_connection() as connection:
            self._require_v11(connection)
            row = connection.execute(
                "SELECT COUNT(*) AS selection_count "
                "FROM screener_candidates AS candidate "
                "JOIN screener_runs AS run "
                "ON run.screener_run_id = candidate.screener_run_id "
                "WHERE candidate.symbol = ? AND candidate.status = 'success' "
                "AND candidate.rank IS NOT NULL AND run.status = 'success'",
                (normalized,),
            ).fetchone()
        return int(row["selection_count"])

    def reason_frequency(
        self,
        *,
        stage: str | None = None,
        code: str | None = None,
        metric: str | None = None,
        stage1_methodology_version: str | None = None,
        stage2_methodology_version: str | None = None,
    ) -> tuple[ReasonFrequencyRecord, ...]:
        normalized_stage = _optional_stage(stage)
        normalized_code = _optional_text(code, "code")
        normalized_metric = _optional_text(metric, "metric")
        stage1_version = _optional_text(
            stage1_methodology_version, "stage1_methodology_version"
        )
        stage2_version = _optional_text(
            stage2_methodology_version, "stage2_methodology_version"
        )
        with self._read_connection() as connection:
            self._require_v11(connection)
            return self._reason_frequency(
                connection,
                stage=normalized_stage,
                code=normalized_code,
                metric=normalized_metric,
                stage1_methodology_version=stage1_version,
                stage2_methodology_version=stage2_version,
                market_date=None,
            )

    def daily_candidate_set(
        self,
        market_date: date,
        *,
        screener_run_id: str | None = None,
    ) -> tuple[DailyCandidateRecord, ...]:
        date_text = _date_text(market_date)
        run_id = _optional_text(screener_run_id, "screener_run_id")
        clauses = [
            "run.market_date = ?",
            "run.status = 'success'",
            "candidate.status = 'success'",
            "candidate.rank IS NOT NULL",
        ]
        parameters: list[object] = [date_text]
        if run_id is not None:
            clauses.append("run.screener_run_id = ?")
            parameters.append(run_id)
        base_select = (
            "SELECT run.market_date, run.screener_run_id, "
            "run.stage1_methodology_version, run.stage2_methodology_version, "
            "candidate.candidate_id, candidate.symbol, candidate.rank, "
            "candidate.candidate_kind, candidate.stage1_rank, "
            "candidate.stage1_trigger_count, candidate.stage1_reason_count, "
            "candidate.stage2_reason_count, candidate.data_quality_status, "
            "candidate.validation_status, candidate.analysis_status "
        )
        ds5_select = (
            ", run.ds5_research_data_quality, run.ds5_execution_status, "
            "candidate.ds5_dataset_version_id, run.ds5_source_status, "
            "run.ds5_authority_status, run.ds5_reconciliation_status, "
            "candidate.ds5_esun_supplemental_count "
        )
        sql = (
            base_select
            + (ds5_select if self._supports_ds5_columns() else "")
            + "FROM screener_runs AS run JOIN screener_candidates AS candidate "
            "ON candidate.screener_run_id = run.screener_run_id WHERE "
            + " AND ".join(clauses)
            + " ORDER BY run.stage1_methodology_version, "
            "run.stage2_methodology_version, run.screener_run_id, "
            "candidate.rank, candidate.symbol"
        )
        with self._read_connection() as connection:
            self._require_v11(connection)
            rows = connection.execute(sql, parameters).fetchall()
        return tuple(
            DailyCandidateRecord(
                market_date=date.fromisoformat(row["market_date"]),
                screener_run_id=row["screener_run_id"],
                candidate_id=row["candidate_id"],
                stage1_methodology_version=row["stage1_methodology_version"],
                stage2_methodology_version=row["stage2_methodology_version"],
                symbol=row["symbol"],
                rank=row["rank"],
                candidate_kind=row["candidate_kind"],
                stage1_rank=row["stage1_rank"],
                stage1_trigger_count=row["stage1_trigger_count"],
                stage1_reason_count=row["stage1_reason_count"],
                stage2_reason_count=row["stage2_reason_count"],
                data_quality_status=row["data_quality_status"],
                validation_status=row["validation_status"],
                analysis_status=row["analysis_status"],
                research_data_quality=(
                    row["ds5_research_data_quality"]
                    if "ds5_research_data_quality" in row.keys()
                    else "canonical"
                ),
                execution_status=(
                    row["ds5_execution_status"]
                    if "ds5_execution_status" in row.keys()
                    else "success"
                ),
                dataset_version_id=(
                    row["ds5_dataset_version_id"]
                    if "ds5_dataset_version_id" in row.keys()
                    else None
                ),
                source_status=(
                    row["ds5_source_status"]
                    if "ds5_source_status" in row.keys()
                    else "canonical_complete"
                ),
                authority_status=(
                    row["ds5_authority_status"]
                    if "ds5_authority_status" in row.keys()
                    else "complete"
                ),
                reconciliation_status=(
                    row["ds5_reconciliation_status"]
                    if "ds5_reconciliation_status" in row.keys()
                    else "not_applicable"
                ),
                supplemental_count=(
                    row["ds5_esun_supplemental_count"]
                    if "ds5_esun_supplemental_count" in row.keys()
                    else 0
                ),
            )
            for row in rows
        )

    def _supports_ds5_columns(self) -> bool:
        with self._read_connection() as connection:
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info('screener_runs')")
            }
        return "ds5_research_data_quality" in columns

    def quality_frequency(
        self,
        market_date: date | None = None,
    ) -> tuple[DataQualityFrequencyRecord, ...]:
        """Return sealed-run quality aggregates without changing legacy rows."""

        if market_date is not None and not isinstance(market_date, date):
            raise ScreenerHistoryError("market_date must be a date or None")
        clauses = ["status = 'success'"]
        parameters: list[object] = []
        if market_date is not None:
            clauses.append("market_date = ?")
            parameters.append(_date_text(market_date))
        with self._read_connection() as connection:
            self._require_v11(connection)
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info('screener_runs')")
            }
            if "ds5_research_data_quality" in columns:
                sql = (
                    "SELECT market_date, ds5_research_data_quality AS research_data_quality, "
                    "ds5_execution_status AS execution_status, COUNT(*) AS run_count, "
                    "COALESCE(SUM(candidate_count), 0) AS candidate_count, "
                    "COALESCE(SUM(ds5_supplemental_candidate_count), 0) AS supplemental_candidate_count, "
                    "COALESCE(SUM(CASE WHEN ds5_reconciliation_status = 'pending' THEN 1 ELSE 0 END), 0) AS pending_reconciliation_count, "
                    "COALESCE(SUM(ds5_discrepancy_count), 0) AS discrepancy_count "
                    "FROM screener_runs WHERE "
                    + " AND ".join(clauses)
                    + " GROUP BY market_date, ds5_research_data_quality, ds5_execution_status "
                    "ORDER BY market_date, ds5_research_data_quality, ds5_execution_status"
                )
            else:
                sql = (
                    "SELECT market_date, 'canonical' AS research_data_quality, "
                    "'success' AS execution_status, COUNT(*) AS run_count, "
                    "COALESCE(SUM(candidate_count), 0) AS candidate_count, 0 AS supplemental_candidate_count, "
                    "0 AS pending_reconciliation_count, 0 AS discrepancy_count "
                    "FROM screener_runs WHERE "
                    + " AND ".join(clauses)
                    + " GROUP BY market_date ORDER BY market_date"
                )
            rows = connection.execute(sql, parameters).fetchall()
        return tuple(
            DataQualityFrequencyRecord(
                market_date=date.fromisoformat(row["market_date"]),
                research_data_quality=row["research_data_quality"],
                execution_status=row["execution_status"],
                run_count=int(row["run_count"]),
                candidate_count=int(row["candidate_count"]),
                supplemental_candidate_count=int(row["supplemental_candidate_count"]),
                pending_reconciliation_count=int(row["pending_reconciliation_count"]),
                discrepancy_count=int(row["discrepancy_count"]),
            )
            for row in rows
        )

    data_quality_frequency = quality_frequency

    def supplemental_candidate_count(
        self,
        market_date: date,
        *,
        screener_run_id: str | None = None,
    ) -> int:
        """Return the number of sealed candidates using supplemental rows."""

        return self._quality_count(
            market_date,
            column="ds5_supplemental_candidate_count",
            screener_run_id=screener_run_id,
        )

    def pending_reconciliation_count(
        self,
        market_date: date,
        *,
        screener_run_id: str | None = None,
    ) -> int:
        """Return sealed runs whose dataset reconciliation remains pending."""

        if not isinstance(market_date, date):
            raise ScreenerHistoryError("market_date must be a date")
        clauses = ["status = 'success'", "market_date = ?"]
        parameters: list[object] = [_date_text(market_date)]
        if screener_run_id is not None:
            clauses.append("screener_run_id = ?")
            parameters.append(_optional_text(screener_run_id, "screener_run_id"))
        with self._read_connection() as connection:
            self._require_v11(connection)
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info('screener_runs')")
            }
            if "ds5_reconciliation_status" not in columns:
                return 0
            row = connection.execute(
                "SELECT COUNT(*) FROM screener_runs WHERE "
                + " AND ".join(clauses)
                + " AND ds5_reconciliation_status = 'pending'",
                parameters,
            ).fetchone()
        return int(row[0])

    def discrepancy_count(
        self,
        market_date: date,
        *,
        screener_run_id: str | None = None,
    ) -> int:
        """Return the persisted DS5 discrepancy total for sealed runs."""

        return self._quality_count(
            market_date,
            column="ds5_discrepancy_count",
            screener_run_id=screener_run_id,
        )

    def _quality_count(
        self,
        market_date: date,
        *,
        column: str,
        screener_run_id: str | None,
    ) -> int:
        if not isinstance(market_date, date):
            raise ScreenerHistoryError("market_date must be a date")
        if column not in {
            "ds5_supplemental_candidate_count",
            "ds5_discrepancy_count",
        }:
            raise ScreenerHistoryError("unsupported quality aggregate")
        clauses = ["status = 'success'", "market_date = ?"]
        parameters: list[object] = [_date_text(market_date)]
        if screener_run_id is not None:
            clauses.append("screener_run_id = ?")
            parameters.append(_optional_text(screener_run_id, "screener_run_id"))
        with self._read_connection() as connection:
            self._require_v11(connection)
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info('screener_runs')")
            }
            if column not in columns:
                return 0
            row = connection.execute(
                f"SELECT COALESCE(SUM({column}), 0) FROM screener_runs WHERE "
                + " AND ".join(clauses),
                parameters,
            ).fetchone()
        return int(row[0])

    def reason_history(
        self,
        symbol: str,
        *,
        code: str | None = None,
        metric: str | None = None,
        stage: str | None = None,
    ) -> tuple[ReasonHistoryRecord, ...]:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_code = _optional_text(code, "code")
        normalized_metric = _optional_text(metric, "metric")
        normalized_stage = _optional_stage(stage)
        if normalized_code is None and normalized_metric is None:
            raise ScreenerHistoryError("reason history requires code or metric")
        clauses = [
            "candidate.symbol = ?",
            "run.status = 'success'",
            "candidate.status = 'success'",
            "candidate.rank IS NOT NULL",
        ]
        parameters: list[object] = [normalized_symbol]
        if normalized_code is not None:
            clauses.append("reason.code = ?")
            parameters.append(normalized_code)
        if normalized_metric is not None:
            clauses.append("reason.metric = ?")
            parameters.append(normalized_metric)
        if normalized_stage is not None:
            clauses.append("reason.stage = ?")
            parameters.append(normalized_stage)
        sql = (
            "SELECT run.market_date, run.screener_run_id, candidate.symbol, "
            "candidate.rank, reason.stage, reason.ordinal, reason.code, "
            "reason.metric, reason.previous_json, reason.current_json, "
            "reason.delta, reason.threshold_json, reason.unit, reason.operator, "
            "reason.rule_version FROM candidate_reasons AS reason "
            "JOIN screener_candidates AS candidate "
            "ON candidate.candidate_id = reason.candidate_id "
            "JOIN screener_runs AS run "
            "ON run.screener_run_id = candidate.screener_run_id WHERE "
            + " AND ".join(clauses)
            + " ORDER BY run.market_date, run.stage1_methodology_version, "
            "run.stage2_methodology_version, run.screener_run_id, "
            "CASE reason.stage WHEN 'stage1' THEN 0 ELSE 1 END, "
            "reason.ordinal, candidate.symbol"
        )
        with self._read_connection() as connection:
            self._require_v11(connection)
            rows = connection.execute(sql, parameters).fetchall()
        return tuple(
            ReasonHistoryRecord(
                market_date=date.fromisoformat(row["market_date"]),
                screener_run_id=row["screener_run_id"],
                symbol=row["symbol"],
                rank=row["rank"],
                stage=row["stage"],
                ordinal=row["ordinal"],
                code=row["code"],
                metric=row["metric"],
                previous=_json_scalar(row["previous_json"]),
                current=_json_scalar(row["current_json"]),
                delta=row["delta"],
                threshold=_json_threshold(row["threshold_json"]),
                unit=row["unit"],
                operator=row["operator"],
                rule_version=row["rule_version"],
            )
            for row in rows
        )

    def compare_methodologies(
        self,
        market_date: date,
        *,
        left: MethodologySelector,
        right: MethodologySelector,
    ) -> MethodologyComparison:
        if not isinstance(left, MethodologySelector) or not isinstance(
            right, MethodologySelector
        ):
            raise TypeError("left and right must be MethodologySelector values")
        date_text = _date_text(market_date)
        with self._read_connection() as connection:
            self._require_v11(connection)
            left_snapshot = self._methodology_snapshot(connection, date_text, left)
            right_snapshot = self._methodology_snapshot(connection, date_text, right)
        left_symbols = set(left_snapshot.candidate_symbols)
        right_symbols = set(right_snapshot.candidate_symbols)
        return MethodologyComparison(
            market_date=market_date,
            left=left_snapshot,
            right=right_snapshot,
            selection_overlap=tuple(sorted(left_symbols & right_symbols)),
            left_only=tuple(sorted(left_symbols - right_symbols)),
            right_only=tuple(sorted(right_symbols - left_symbols)),
        )

    @classmethod
    def _methodology_snapshot(
        cls,
        connection: sqlite3.Connection,
        market_date: str,
        selector: MethodologySelector,
    ) -> MethodologySnapshot:
        parameters = (
            market_date,
            selector.stage1_methodology_version,
            selector.stage2_methodology_version,
        )
        run_rows = connection.execute(
            "SELECT screener_run_id FROM screener_runs WHERE market_date = ? "
            "AND stage1_methodology_version = ? "
            "AND stage2_methodology_version = ? AND status = 'success' "
            "ORDER BY screener_run_id",
            parameters,
        ).fetchall()
        selection_rows = connection.execute(
            "SELECT candidate.symbol FROM screener_candidates AS candidate "
            "JOIN screener_runs AS run "
            "ON run.screener_run_id = candidate.screener_run_id "
            "WHERE run.market_date = ? AND run.stage1_methodology_version = ? "
            "AND run.stage2_methodology_version = ? AND run.status = 'success' "
            "AND candidate.status = 'success' AND candidate.rank IS NOT NULL "
            "ORDER BY candidate.symbol, run.screener_run_id, candidate.rank",
            parameters,
        ).fetchall()
        frequencies = cls._reason_frequency(
            connection,
            stage=None,
            code=None,
            metric=None,
            stage1_methodology_version=selector.stage1_methodology_version,
            stage2_methodology_version=selector.stage2_methodology_version,
            market_date=market_date,
        )
        symbols = tuple(row["symbol"] for row in selection_rows)
        return MethodologySnapshot(
            selector=selector,
            market_date=date.fromisoformat(market_date),
            run_ids=tuple(row["screener_run_id"] for row in run_rows),
            candidate_symbols=tuple(sorted(set(symbols))),
            selection_count=len(symbols),
            reason_frequencies=frequencies,
        )

    @staticmethod
    def _reason_frequency(
        connection: sqlite3.Connection,
        *,
        stage: str | None,
        code: str | None,
        metric: str | None,
        stage1_methodology_version: str | None,
        stage2_methodology_version: str | None,
        market_date: str | None,
    ) -> tuple[ReasonFrequencyRecord, ...]:
        clauses = [
            "run.status = 'success'",
            "candidate.status = 'success'",
            "candidate.rank IS NOT NULL",
        ]
        parameters: list[object] = []
        for column, value in (
            ("reason.stage", stage),
            ("reason.code", code),
            ("reason.metric", metric),
            ("run.stage1_methodology_version", stage1_methodology_version),
            ("run.stage2_methodology_version", stage2_methodology_version),
            ("run.market_date", market_date),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                parameters.append(value)
        rows = connection.execute(
            "SELECT reason.stage, reason.code, reason.metric, COUNT(*) AS frequency "
            "FROM candidate_reasons AS reason "
            "JOIN screener_candidates AS candidate "
            "ON candidate.candidate_id = reason.candidate_id "
            "JOIN screener_runs AS run "
            "ON run.screener_run_id = candidate.screener_run_id WHERE "
            + " AND ".join(clauses)
            + " GROUP BY reason.stage, reason.code, reason.metric "
            "ORDER BY CASE reason.stage WHEN 'stage1' THEN 0 ELSE 1 END, "
            "reason.code, reason.metric",
            parameters,
        ).fetchall()
        return tuple(
            ReasonFrequencyRecord(
                stage=row["stage"],
                code=row["code"],
                metric=row["metric"],
                count=int(row["frequency"]),
            )
            for row in rows
        )

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
            raise ScreenerHistoryError(
                "Market Screener migration 11 must be applied explicitly"
            )


def _normalize_symbol(value: object) -> str:
    if not isinstance(value, str):
        raise ScreenerHistoryError("symbol must be text")
    symbol = value.strip().upper()
    if _SYMBOL.fullmatch(symbol) is None:
        raise ScreenerHistoryError("symbol must be 2-12 uppercase letters or digits")
    return symbol


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ScreenerHistoryError(f"{field_name} must not be blank")
    return value.strip()


def _optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _optional_stage(value: object) -> str | None:
    if value is None:
        return None
    stage = _require_text(value, "stage")
    if stage not in _STAGES:
        raise ScreenerHistoryError("stage must be stage1 or stage2")
    return stage


def _date_text(value: object) -> str:
    if not isinstance(value, date):
        raise ScreenerHistoryError("market_date must be a date")
    return value.isoformat()


def _json_scalar(value: str) -> Scalar:
    parsed = json.loads(value)
    if parsed is not None and (
        isinstance(parsed, bool) or not isinstance(parsed, (str, int, float))
    ):
        raise ScreenerHistoryError("persisted reason scalar is invalid")
    return parsed


def _json_threshold(value: str) -> float | str:
    parsed = json.loads(value)
    if isinstance(parsed, bool) or not isinstance(parsed, (str, int, float)):
        raise ScreenerHistoryError("persisted reason threshold is invalid")
    return parsed


__all__ = [
    "DailyCandidateRecord",
    "MethodologyComparison",
    "MethodologySelector",
    "MethodologySnapshot",
    "ReasonFrequencyRecord",
    "ReasonHistoryRecord",
    "SQLiteScreenerHistoryReader",
    "ScreenerHistoryError",
]
