"""Read-only SQLite adapter for the pure M9 ResearchDataset contract.

Every connection uses SQLite URI ``mode=ro`` plus ``PRAGMA query_only=ON``.
The adapter validates the frozen v10 schema, executes SELECT-only mappings,
and closes the connection before returning an immutable snapshot.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from app.as_of_policy import FrozenAsOfPolicyV1
from app.research_dataset import (
    DatasetArtifactRef,
    DatasetAsOf,
    DatasetDiscrepancy,
    DatasetPrice,
    DatasetProvenance,
    DatasetSourcePolicyError,
    DatasetSymbol,
    DatasetValidationObservation,
    DatasetValuationMetric,
    PriceHistoryReadModel,
    ResearchDataset,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
    TwseBaselineSourcePolicy,
    ValidationReadModel,
    ValuationReadModel,
)


class SQLiteResearchDatasetError(RuntimeError):
    """Base error for the read-only SQLite dataset adapter."""


class SQLiteDatasetSchemaError(SQLiteResearchDatasetError):
    """The database is not the frozen schema required by this adapter."""


class SQLiteDatasetReadError(SQLiteResearchDatasetError):
    """Stored read evidence cannot be mapped into the pure contract."""


_AS_OF_POLICY = FrozenAsOfPolicyV1()
_SCHEMA_VERSION = 10
_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "schema_migrations": frozenset({"version", "name", "applied_at"}),
    "symbols": frozenset(
        {"symbol", "name", "market", "currency", "is_active"}
    ),
    "daily_prices": frozenset(
        {
            "symbol",
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "source",
        }
    ),
    "company_metrics": frozenset(
        {
            "symbol",
            "metric_date",
            "metric_name",
            "metric_value",
            "unit",
            "source",
        }
    ),
    "pipeline_runs": frozenset(
        {
            "run_id",
            "symbol",
            "target_date",
            "status",
            "provider",
            "source_endpoints_json",
            "fetched_at",
            "market_date",
        }
    ),
    "historical_sync_runs": frozenset(
        {
            "run_id",
            "symbol",
            "target_date",
            "status",
            "provider",
            "source_endpoint",
            "fetched_at",
        }
    ),
    "market_data_validation_runs": frozenset(
        {
            "run_id",
            "symbol",
            "target_date",
            "requested_start_date",
            "left_provider",
            "right_provider",
            "status",
            "outcome",
            "created_at",
        }
    ),
    "market_data_observations": frozenset(
        {
            "id",
            "run_id",
            "provider",
            "symbol",
            "market_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "source_endpoints_json",
            "fetched_at",
            "source_timestamp",
        }
    ),
    "market_data_discrepancies": frozenset(
        {
            "id",
            "run_id",
            "field",
            "left_value",
            "right_value",
            "reason",
            "absolute_difference",
            "relative_difference_pct",
        }
    ),
    "source_artifacts": frozenset(
        {
            "id",
            "pipeline_run_id",
            "historical_run_id",
            "validation_run_id",
            "provider",
            "dataset",
            "endpoint",
            "contract_version",
            "content_type",
            "payload_sha256",
            "payload_size_bytes",
            "hash_basis",
            "fetched_at",
        }
    ),
}


@dataclass(frozen=True, slots=True)
class _RunEvidence:
    run_id: str
    symbol: str
    provider: str
    source_endpoints: tuple[str, ...]
    fetched_at: datetime | None


class SQLiteResearchDataset(ResearchDataset):
    """Map the existing v10 SQLite system-of-record into immutable snapshots."""

    __slots__ = ("_database_path", "_database_uri")

    def __init__(self, database_path: str | Path) -> None:
        if str(database_path) == ":memory:":
            raise SQLiteDatasetSchemaError("ResearchDataset requires a file database")
        path = Path(database_path).expanduser().resolve()
        if not path.is_file():
            raise SQLiteDatasetSchemaError(f"database file does not exist: {path}")
        self._database_path = path
        self._database_uri = path.as_uri() + "?mode=ro"
        self._validate_schema()

    def read(
        self,
        request: ResearchDatasetRequest,
        /,
    ) -> ResearchDatasetSnapshot:
        if not isinstance(request, ResearchDatasetRequest):
            raise TypeError("request must be a ResearchDatasetRequest")
        try:
            with self._read_connection() as connection:
                symbol = self._read_symbol(connection, request.symbol)
                all_prices = self._read_prices(connection, request)
                all_valuations = self._read_valuations(connection, request)
                validation = self._read_validation(connection, request)
                pipeline = self._read_pipeline(connection, request)
                historical = self._read_historical(connection, request)
                artifacts = self._read_artifacts(
                    connection,
                    pipeline_run_id=None if pipeline is None else pipeline.run_id,
                    historical_run_id=(
                        None if historical is None else historical.run_id
                    ),
                    validation_run_id=(
                        None
                        if validation.status == "missing_source"
                        else validation.run_id
                    ),
                )
                snapshot = self._build_snapshot(
                    request=request,
                    symbol=symbol,
                    all_prices=all_prices,
                    all_valuations=all_valuations,
                    validation=validation,
                    pipeline=pipeline,
                    historical=historical,
                    artifacts=artifacts,
                )
            return snapshot
        except DatasetSourcePolicyError:
            raise
        except KeyError:
            raise
        except SQLiteResearchDatasetError:
            raise
        except (json.JSONDecodeError, sqlite3.Error, TypeError, ValueError) as error:
            raise SQLiteDatasetReadError(
                f"stored dataset evidence is incompatible: {type(error).__name__}"
            ) from error

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        try:
            connection = sqlite3.connect(
                self._database_uri,
                uri=True,
                timeout=5.0,
            )
        except sqlite3.Error as error:
            raise SQLiteDatasetSchemaError(
                "database could not be opened in read-only mode"
            ) from error
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            query_only = connection.execute("PRAGMA query_only").fetchone()[0]
            if int(query_only) != 1:
                raise SQLiteDatasetSchemaError("SQLite query_only could not be enabled")
            yield connection
        finally:
            connection.close()

    def _validate_schema(self) -> None:
        try:
            with self._read_connection() as connection:
                tables = {
                    row["name"]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                missing_tables = set(_REQUIRED_COLUMNS) - tables
                if missing_tables:
                    raise SQLiteDatasetSchemaError(
                        "required dataset tables are missing: "
                        + ", ".join(sorted(missing_tables))
                    )
                version_row = connection.execute(
                    "SELECT COALESCE(MAX(version), 0) AS version "
                    "FROM schema_migrations"
                ).fetchone()
                version = int(version_row["version"])
                if version != _SCHEMA_VERSION:
                    raise SQLiteDatasetSchemaError(
                        f"dataset requires schema version 10, found {version}"
                    )
                for table, required in _REQUIRED_COLUMNS.items():
                    columns = {
                        row["name"]
                        for row in connection.execute(
                            f'PRAGMA table_info("{table}")'
                        )
                    }
                    missing_columns = required - columns
                    if missing_columns:
                        raise SQLiteDatasetSchemaError(
                            f"table {table} is missing required columns: "
                            + ", ".join(sorted(missing_columns))
                        )
        except SQLiteResearchDatasetError:
            raise
        except sqlite3.Error as error:
            raise SQLiteDatasetSchemaError(
                "database schema could not be validated read-only"
            ) from error

    @staticmethod
    def _read_symbol(
        connection: sqlite3.Connection,
        symbol: str,
    ) -> DatasetSymbol:
        row = connection.execute(
            "SELECT symbol, name, market, currency, is_active "
            "FROM symbols WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown dataset symbol {symbol}")
        return DatasetSymbol(
            symbol=row["symbol"],
            name=row["name"],
            market=row["market"],
            currency=row["currency"],
            is_active=bool(row["is_active"]),
        )

    @staticmethod
    def _read_prices(
        connection: sqlite3.Connection,
        request: ResearchDatasetRequest,
    ) -> tuple[DatasetPrice, ...]:
        cutoff = _AS_OF_POLICY.price_cutoff(request.as_of_date)
        rows = connection.execute(
            "SELECT symbol, trade_date, open_price, high_price, low_price, "
            "close_price, volume, source FROM daily_prices "
            "WHERE symbol = ? AND trade_date <= ? ORDER BY trade_date",
            (request.symbol, cutoff.isoformat()),
        ).fetchall()
        prices = tuple(
            DatasetPrice(
                symbol=row["symbol"],
                trade_date=date.fromisoformat(row["trade_date"]),
                open=row["open_price"],
                high=row["high_price"],
                low=row["low_price"],
                close=row["close_price"],
                volume=row["volume"],
                source=row["source"],
            )
            for row in rows
        )
        return _AS_OF_POLICY.prices_as_of(prices, request.as_of_date)

    @staticmethod
    def _read_valuations(
        connection: sqlite3.Connection,
        request: ResearchDatasetRequest,
    ) -> tuple[DatasetValuationMetric, ...]:
        rows = connection.execute(
            "SELECT symbol, metric_date, metric_name, metric_value, unit, source "
            "FROM company_metrics WHERE symbol = ? AND metric_date <= ? "
            "ORDER BY metric_date, metric_name",
            (request.symbol, request.as_of_date.isoformat()),
        ).fetchall()
        return tuple(
            DatasetValuationMetric(
                symbol=row["symbol"],
                metric_date=date.fromisoformat(row["metric_date"]),
                name=row["metric_name"],
                value=row["metric_value"],
                unit=row["unit"],
                source=row["source"],
            )
            for row in rows
        )

    def _read_validation(
        self,
        connection: sqlite3.Connection,
        request: ResearchDatasetRequest,
    ) -> ValidationReadModel:
        select = (
            "SELECT run_id, symbol, target_date, requested_start_date, "
            "left_provider, right_provider, status, outcome, created_at "
            "FROM market_data_validation_runs"
        )
        if request.validation_run_id is not None:
            row = connection.execute(
                select + " WHERE run_id = ?",
                (request.validation_run_id,),
            ).fetchone()
            candidates = () if row is None else (row,)
        else:
            candidates = tuple(
                connection.execute(
                    select
                    + " WHERE symbol = ? ORDER BY created_at, run_id",
                    (request.symbol,),
                ).fetchall()
            )
        eligible = [
            row
            for row in candidates
            if row["symbol"] == request.symbol
            and row["status"] == "success"
            and _AS_OF_POLICY.validation_target_is_consistent(
                date.fromisoformat(row["target_date"]),
                request.as_of_date,
            )
        ]
        if not eligible:
            return ValidationReadModel(symbol=request.symbol)
        selected = max(
            eligible,
            key=lambda row: datetime.fromisoformat(row["created_at"]),
        )
        run_id = selected["run_id"]
        observation_rows = connection.execute(
            "SELECT o.id, o.provider, o.symbol, o.market_date, o.open_price, "
            "o.high_price, o.low_price, o.close_price, o.volume, "
            "o.source_endpoints_json, o.fetched_at, o.source_timestamp "
            "FROM market_data_observations AS o "
            "JOIN market_data_validation_runs AS r ON r.run_id = o.run_id "
            "WHERE o.run_id = ? "
            "ORDER BY CASE o.provider WHEN r.left_provider THEN 0 ELSE 1 END",
            (run_id,),
        ).fetchall()
        observations = tuple(
            DatasetValidationObservation(
                symbol=row["symbol"],
                provider=row["provider"],
                market_date=date.fromisoformat(row["market_date"]),
                open=row["open_price"],
                high=row["high_price"],
                low=row["low_price"],
                close=row["close_price"],
                volume=row["volume"],
                source_endpoints=_decode_string_array(
                    row["source_endpoints_json"],
                    "market_data_observations.source_endpoints_json",
                ),
                fetched_at=datetime.fromisoformat(row["fetched_at"]),
                source_timestamp=(
                    None
                    if row["source_timestamp"] is None
                    else datetime.fromisoformat(row["source_timestamp"])
                ),
            )
            for row in observation_rows
        )
        discrepancy_rows = connection.execute(
            "SELECT id, field, left_value, right_value, reason "
            "FROM market_data_discrepancies WHERE run_id = ? ORDER BY field",
            (run_id,),
        ).fetchall()
        discrepancies = tuple(
            DatasetDiscrepancy(
                field=row["field"],
                left_value=row["left_value"],
                right_value=row["right_value"],
                reason=row["reason"],
            )
            for row in discrepancy_rows
        )
        if any(item.field == "market_date" for item in discrepancies):
            status = "market_date_mismatch"
        elif selected["outcome"] == "discrepancy":
            status = "source_discrepancy"
        else:
            status = "available"
        return ValidationReadModel(
            symbol=selected["symbol"],
            status=status,
            run_id=run_id,
            target_date=date.fromisoformat(selected["target_date"]),
            left_provider=selected["left_provider"],
            right_provider=selected["right_provider"],
            outcome=selected["outcome"],
            created_at=datetime.fromisoformat(selected["created_at"]),
            observations=observations,
            discrepancies=discrepancies,
        )

    @staticmethod
    def _read_pipeline(
        connection: sqlite3.Connection,
        request: ResearchDatasetRequest,
    ) -> _RunEvidence | None:
        select = (
            "SELECT run_id, symbol, provider, source_endpoints_json, fetched_at "
            "FROM pipeline_runs"
        )
        if request.pipeline_run_id is not None:
            row = connection.execute(
                select + " WHERE run_id = ?",
                (request.pipeline_run_id,),
            ).fetchone()
        else:
            row = connection.execute(
                select + " WHERE symbol = ? AND target_date = ?",
                (request.symbol, request.as_of_date.isoformat()),
            ).fetchone()
        if row is None:
            return None
        if row["symbol"] != request.symbol:
            raise SQLiteDatasetReadError("pipeline provenance symbol mismatch")
        return _RunEvidence(
            run_id=row["run_id"],
            symbol=row["symbol"],
            provider=row["provider"],
            source_endpoints=_decode_string_array(
                row["source_endpoints_json"],
                "pipeline_runs.source_endpoints_json",
            ),
            fetched_at=(
                None
                if row["fetched_at"] is None
                else datetime.fromisoformat(row["fetched_at"])
            ),
        )

    @staticmethod
    def _read_historical(
        connection: sqlite3.Connection,
        request: ResearchDatasetRequest,
    ) -> _RunEvidence | None:
        if request.historical_run_id is None:
            return None
        row = connection.execute(
            "SELECT run_id, symbol, provider, source_endpoint, fetched_at "
            "FROM historical_sync_runs WHERE run_id = ?",
            (request.historical_run_id,),
        ).fetchone()
        if row is None:
            return None
        if row["symbol"] != request.symbol:
            raise SQLiteDatasetReadError("historical provenance symbol mismatch")
        return _RunEvidence(
            run_id=row["run_id"],
            symbol=row["symbol"],
            provider=row["provider"],
            source_endpoints=(
                () if row["source_endpoint"] is None else (row["source_endpoint"],)
            ),
            fetched_at=(
                None
                if row["fetched_at"] is None
                else datetime.fromisoformat(row["fetched_at"])
            ),
        )

    @staticmethod
    def _read_artifacts(
        connection: sqlite3.Connection,
        *,
        pipeline_run_id: str | None,
        historical_run_id: str | None,
        validation_run_id: str | None,
    ) -> tuple[DatasetArtifactRef, ...]:
        refs: list[DatasetArtifactRef] = []
        owners = (
            ("pipeline", "pipeline_run_id", pipeline_run_id),
            ("historical", "historical_run_id", historical_run_id),
            ("validation", "validation_run_id", validation_run_id),
        )
        for owner_kind, column, run_id in owners:
            if run_id is None:
                continue
            rows = connection.execute(
                "SELECT id, provider, dataset, endpoint, contract_version, "
                "content_type, payload_sha256, payload_size_bytes, hash_basis, "
                f"fetched_at FROM source_artifacts WHERE {column} = ? ORDER BY id",
                (run_id,),
            ).fetchall()
            refs.extend(
                DatasetArtifactRef(
                    owner_kind=owner_kind,
                    owner_run_id=run_id,
                    provider=row["provider"],
                    dataset=row["dataset"],
                    endpoint=row["endpoint"],
                    contract_version=row["contract_version"],
                    content_type=row["content_type"],
                    payload_sha256=row["payload_sha256"],
                    payload_size_bytes=row["payload_size_bytes"],
                    hash_basis=row["hash_basis"],
                    fetched_at=(
                        None
                        if row["fetched_at"] is None
                        else datetime.fromisoformat(row["fetched_at"])
                    ),
                )
                for row in rows
            )
        return tuple(refs)

    @staticmethod
    def _build_snapshot(
        *,
        request: ResearchDatasetRequest,
        symbol: DatasetSymbol,
        all_prices: tuple[DatasetPrice, ...],
        all_valuations: tuple[DatasetValuationMetric, ...],
        validation: ValidationReadModel,
        pipeline: _RunEvidence | None,
        historical: _RunEvidence | None,
        artifacts: tuple[DatasetArtifactRef, ...],
    ) -> ResearchDatasetSnapshot:
        canonical_sources = {
            *(item.source for item in all_prices),
            *(item.source for item in all_valuations),
        }
        validation_sources = {
            *(item.provider for item in validation.observations),
            *(
                item
                for item in (
                    validation.left_provider,
                    validation.right_provider,
                )
                if item is not None
            ),
        }
        provenance_sources = {
            *(item.provider for item in artifacts),
            *(
                item.provider
                for item in (pipeline, historical)
                if item is not None
            ),
        }
        TwseBaselineSourcePolicy.validate_sources(
            canonical_sources=canonical_sources,
            validation_sources=validation_sources,
            artifact_sources=provenance_sources,
            has_canonical_history=bool(all_prices),
        )

        requested_count = request.history_observations
        returned_prices = (
            all_prices if requested_count is None else all_prices[-requested_count:]
        )
        if not all_prices:
            price_status = "missing_source"
            current = None
        elif _AS_OF_POLICY.current_price_is_exact_date(
            all_prices,
            request.as_of_date,
        ):
            price_status = "available"
            current = all_prices[-1]
        else:
            price_status = "market_date_mismatch"
            current = None
        history = PriceHistoryReadModel(
            symbol=request.symbol,
            as_of_date=request.as_of_date,
            status=price_status,
            observations=returned_prices,
            current=current,
            total_observations_as_of=len(all_prices),
            requested_observations=requested_count,
            is_truncated=len(returned_prices) < len(all_prices),
        )

        metrics_by_name: dict[str, list[DatasetValuationMetric]] = {}
        for metric in all_valuations:
            metrics_by_name.setdefault(metric.name, []).append(metric)
        selected_metrics = tuple(
            selected
            for name in sorted(metrics_by_name)
            if (
                selected := _AS_OF_POLICY.select_valuation_as_of(
                    metrics_by_name[name],
                    request.as_of_date,
                )
            )
            is not None
        )
        valuation = ValuationReadModel(
            symbol=request.symbol,
            as_of_date=request.as_of_date,
            status="available" if selected_metrics else "missing_source",
            metrics=selected_metrics,
        )

        # Keep endpoint aggregation separate from source-role validation.
        source_endpoints = {
            *(item.endpoint for item in artifacts),
            *(
                endpoint
                for run in (pipeline, historical)
                if run is not None
                for endpoint in run.source_endpoints
            ),
            *(
                endpoint
                for observation in validation.observations
                for endpoint in observation.source_endpoints
            ),
        }
        fetched_at = (
            pipeline.fetched_at
            if pipeline is not None
            else (None if historical is None else historical.fetched_at)
        )
        provenance = DatasetProvenance(
            symbol=request.symbol,
            pipeline_run_id=None if pipeline is None else pipeline.run_id,
            historical_run_id=None if historical is None else historical.run_id,
            validation_run_id=(
                None if validation.status == "missing_source" else validation.run_id
            ),
            canonical_sources=tuple(canonical_sources),
            validation_sources=tuple(validation_sources),
            source_endpoints=tuple(source_endpoints),
            artifact_refs=artifacts,
            fetched_at=fetched_at,
        )
        as_of = DatasetAsOf(
            as_of_date=request.as_of_date,
            history_observations=requested_count,
            total_history_observations=len(all_prices),
            returned_history_observations=len(returned_prices),
            history_is_truncated=len(returned_prices) < len(all_prices),
        )
        return ResearchDatasetSnapshot(
            symbol=symbol,
            as_of=as_of,
            price_history=history,
            valuation=valuation,
            validation=validation,
            provenance=provenance,
        )


def _decode_string_array(value: object, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, str):
        raise SQLiteDatasetReadError(f"{field_name} must be JSON text")
    decoded = json.loads(value)
    if not isinstance(decoded, list) or any(
        not isinstance(item, str) or not item.strip() for item in decoded
    ):
        raise SQLiteDatasetReadError(f"{field_name} must contain strings")
    return tuple(decoded)


__all__ = [
    "SQLiteDatasetReadError",
    "SQLiteDatasetSchemaError",
    "SQLiteResearchDataset",
    "SQLiteResearchDatasetError",
]
