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
        sql = (
            "SELECT run.market_date, run.screener_run_id, "
            "run.stage1_methodology_version, run.stage2_methodology_version, "
            "candidate.candidate_id, candidate.symbol, candidate.rank, "
            "candidate.candidate_kind, candidate.stage1_rank, "
            "candidate.stage1_trigger_count, candidate.stage1_reason_count, "
            "candidate.stage2_reason_count, candidate.data_quality_status, "
            "candidate.validation_status, candidate.analysis_status "
            "FROM screener_runs AS run JOIN screener_candidates AS candidate "
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
            )
            for row in rows
        )

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
