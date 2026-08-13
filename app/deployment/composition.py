"""S6C.1 production-only S5 composition root.

This module wires the already-frozen S1--S5 contracts for the application
runner.  It does not define Universe classification, Stage 1 rules, Stage 2
metrics, ranking, persistence identity, or replay behavior.

The production Universe input is an explicit, canonical
``MarketUniverseSnapshot`` JSON artifact.  The artifact is supplied through
an environment variable so that deployment can point at an authoritative
daily source snapshot without putting a path, watchlist, or credential into
the locator itself.  Loading the artifact is deferred until the S5 call; the
factory only resolves configuration and builds the frozen adapters.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass, replace
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Mapping

from app.research_dataset import (
    DatasetPrice,
    ResearchDataset,
    ResearchDatasetRequest,
    TWSE_BASELINE_SOURCE_POLICY,
    TWSE_BASELINE_SOURCES,
)
from app.screener.orchestration import (
    DailyScreenerOrchestrator,
    Stage1Execution,
    Stage2CandidateExecution,
)
from app.screener.stage1 import STAGE1_METHODOLOGY_VERSION
from app.screener.stage1_dataset import scan_stage1_from_dataset
from app.screener.stage2 import (
    STAGE2_METHODOLOGY_VERSION,
    research_stage2_candidate,
)
from app.screener.universe import (
    UNIVERSE_METHODOLOGY_VERSION,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)
from app.sqlite_research_dataset import (
    SQLiteResearchDataset,
    SQLiteResearchDatasetError,
    _REQUIRED_COLUMNS,
)
from app.storage.candidate_persistence import CandidateInputLocator


PRODUCTION_S5_FACTORY_LOCATOR = "app.deployment.composition:create_s5"
PRODUCTION_COMPOSITION_CONTRACT_VERSION = "s6c1-production-composition-v1"
UNIVERSE_SNAPSHOT_PATH_ENV = "IRA_SCREENER_UNIVERSE_SNAPSHOT_PATH"
CANDIDATE_LIMIT_ENV = "IRA_SCREENER_CANDIDATE_LIMIT"
DEFAULT_CANDIDATE_LIMIT = 30
HISTORY_OBSERVATIONS = 250

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ProductionCompositionError(RuntimeError):
    """Explicit production composition configuration or input failure."""


@dataclass(frozen=True, slots=True)
class ProductionS5Configuration:
    """Non-secret inputs needed to resolve the production S5 composition."""

    database_path: Path
    universe_snapshot_path: Path
    candidate_limit: int = DEFAULT_CANDIDATE_LIMIT

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.database_path, "database_path"),
            (self.universe_snapshot_path, "universe_snapshot_path"),
        ):
            if not isinstance(value, Path) or not value.is_absolute():
                raise ProductionCompositionError(
                    f"{field_name} must be an absolute Path"
                )
        if (
            isinstance(self.candidate_limit, bool)
            or not isinstance(self.candidate_limit, int)
            or self.candidate_limit <= 0
        ):
            raise ProductionCompositionError(
                "candidate_limit must be a positive integer"
            )

    @classmethod
    def from_environment(cls, database_path: str | Path) -> "ProductionS5Configuration":
        database = _absolute_path(database_path, "database_path")
        raw_snapshot = os.environ.get(UNIVERSE_SNAPSHOT_PATH_ENV, "").strip()
        if not raw_snapshot:
            raise ProductionCompositionError(
                f"{UNIVERSE_SNAPSHOT_PATH_ENV} is required"
            )
        snapshot = _absolute_path(raw_snapshot, UNIVERSE_SNAPSHOT_PATH_ENV)
        raw_limit = os.environ.get(CANDIDATE_LIMIT_ENV, str(DEFAULT_CANDIDATE_LIMIT))
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError) as error:
            raise ProductionCompositionError(
                f"{CANDIDATE_LIMIT_ENV} must be a positive integer"
            ) from error
        return cls(
            database_path=database,
            universe_snapshot_path=snapshot,
            candidate_limit=limit,
        )


class SQLiteScreenerResearchDataset(SQLiteResearchDataset):
    """Read-only M9 adapter for unchanged legacy tables inside v11/v12.

    S4 migration 0011 adds screener tables and leaves the existing M9 tables
    unchanged.  The frozen ``SQLiteResearchDataset`` deliberately guards its
    original v10 boundary, so this deployment-only adapter reuses its read
        implementation while accepting the additive v11/v12 schema after validating
    the same required legacy tables and columns.  It never exposes writes or
    migration behavior.
    """

    __slots__ = ()

    @staticmethod
    def _read_prices(connection, request):
        """Project the selected TWSE owner without mutating legacy evidence.

        The frozen ``daily_prices`` primary key cannot store a second source
        for the same symbol/date.  Production may therefore retain historical
        E.SUN rows while new TWSE-historical checkpoints cover the remaining
        dates.  When a durable historical owner is selected, its immutable
        source observations are the canonical view.  The daily-price table is
        only the legal-source fallback for older fixtures and pipeline owners.
        This enforces the already frozen ``twse_baseline`` policy instead of
        deleting, relabelling, or overwriting legacy rows.
        """

        historical_prices: tuple[DatasetPrice, ...] = ()
        if request.historical_run_id is not None:
            rows = connection.execute(
                "SELECT symbol, trade_date, open_price, high_price, low_price, "
                "close_price, volume, provider FROM historical_source_observations "
                "WHERE historical_run_id = ? AND symbol = ? AND trade_date <= ? "
                "ORDER BY trade_date",
                (
                    request.historical_run_id,
                    request.symbol,
                    request.as_of_date.isoformat(),
                ),
            ).fetchall()
            if rows:
                historical_prices = tuple(
                    DatasetPrice(
                        symbol=row["symbol"],
                        trade_date=date.fromisoformat(row["trade_date"]),
                        open=row["open_price"],
                        high=row["high_price"],
                        low=row["low_price"],
                        close=row["close_price"],
                        volume=row["volume"],
                        source=row["provider"],
                    )
                    for row in rows
                )
        daily_prices = tuple(
            item
            for item in SQLiteResearchDataset._read_prices(connection, request)
            if item.source in TWSE_BASELINE_SOURCES
        )
        if not historical_prices:
            return daily_prices

        # Daily preparation appends the newest authoritative TWSE observation
        # through the existing daily pipeline while retaining the most recent
        # successful TWSE-historical owner for the older window.  Merge those
        # two immutable owners by trade date.  An overlapping date must agree
        # exactly; otherwise the deployment view fails closed instead of
        # silently choosing one official dataset over the other.
        merged = {item.trade_date: item for item in historical_prices}
        for item in daily_prices:
            existing = merged.get(item.trade_date)
            if existing is None:
                merged[item.trade_date] = item
                continue
            if (
                existing.open,
                existing.high,
                existing.low,
                existing.close,
                existing.volume,
            ) != (
                item.open,
                item.high,
                item.low,
                item.close,
                item.volume,
            ):
                raise ProductionCompositionError(
                    "official TWSE daily/historical price overlap disagrees"
                )
        return tuple(merged[trade_date] for trade_date in sorted(merged))

    @staticmethod
    def _read_valuations(connection, request):
        """Exclude non-TWSE valuation rows from the production S5 view."""

        return tuple(
            item
            for item in SQLiteResearchDataset._read_valuations(connection, request)
            if item.source in TWSE_BASELINE_SOURCES
        )

    def __init__(self, database_path: str | Path) -> None:
        path = _absolute_path(database_path, "database_path")
        if not path.is_file():
            raise ProductionCompositionError(
                f"production research database does not exist: {path}"
            )
        self._database_path = path
        self._database_uri = path.as_uri() + "?mode=ro"
        self._validate_additive_v11_schema()

    def _validate_additive_v11_schema(self) -> None:
        try:
            with self._read_connection() as connection:
                version = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
                    ).fetchone()[0]
                )
                if version not in {10, 11, 12}:
                    raise ProductionCompositionError(
                        "production S5 dataset requires schema v10, v11, or v12, "
                        f"found {version}"
                    )
                tables = {
                    str(row["name"])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                missing_tables = set(_REQUIRED_COLUMNS) - tables
                if missing_tables:
                    raise ProductionCompositionError(
                        "production S5 dataset is missing legacy tables: "
                        + ", ".join(sorted(missing_tables))
                    )
                for table, required in _REQUIRED_COLUMNS.items():
                    columns = {
                        str(row["name"])
                        for row in connection.execute(
                            f'PRAGMA table_info("{table}")'
                        )
                    }
                    missing_columns = required - columns
                    if missing_columns:
                        raise ProductionCompositionError(
                            f"production S5 dataset table {table} is missing columns: "
                            + ", ".join(sorted(missing_columns))
                        )
        except ProductionCompositionError:
            raise
        except (sqlite3.Error, SQLiteResearchDatasetError) as error:
            raise ProductionCompositionError(
                "production S5 dataset schema could not be validated read-only"
            ) from error

    def read(self, request: ResearchDatasetRequest, /):
        """Read through the newest valid TWSE-owned upstream checkpoint.

        S6D.1 remediation may rebuild canonical prices through the existing
        ``twse-historical`` checkpoint contract while legacy E.SUN daily
        checkpoints remain immutable.  The frozen M9 adapter supports an
        explicit owner id; production composition resolves that owner before
        delegating, preferring a successful TWSE historical run and falling
        back to a successful TWSE daily pipeline run.  E.SUN is never selected
        as a canonical owner by this deployment adapter.
        """

        if not isinstance(request, ResearchDatasetRequest):
            raise TypeError("request must be ResearchDatasetRequest")
        if request.pipeline_run_id is not None or request.historical_run_id is not None:
            return super().read(request)
        resolved = self._resolve_canonical_owner(request)
        return super().read(resolved)

    def _resolve_canonical_owner(
        self,
        request: ResearchDatasetRequest,
    ) -> ResearchDatasetRequest:
        with self._read_connection() as connection:
            historical = connection.execute(
                "SELECT run_id FROM historical_sync_runs "
                "WHERE symbol = ? AND target_date <= ? "
                "AND provider = 'twse-historical' AND status = 'success' "
                "ORDER BY target_date DESC, target_observations DESC, "
                "finished_at DESC, run_id DESC "
                "LIMIT 1",
                (request.symbol, request.as_of_date.isoformat()),
            ).fetchone()
            pipeline = connection.execute(
                "SELECT run_id FROM pipeline_runs "
                "WHERE symbol = ? AND target_date = ? "
                "AND provider = 'twse' AND status = 'success' "
                "ORDER BY finished_at DESC, run_id DESC LIMIT 1",
                (request.symbol, request.as_of_date.isoformat()),
            ).fetchone()
        return replace(
            request,
            historical_run_id=(
                None if historical is None else str(historical["run_id"])
            ),
            pipeline_run_id=None if pipeline is None else str(pipeline["run_id"]),
        )


class JsonUniverseSnapshotProvider:
    """Load one explicit canonical S1 snapshot per requested market date."""

    def __init__(self, snapshot_path: str | Path) -> None:
        self.snapshot_path = _absolute_path(snapshot_path, "snapshot_path")

    def __call__(self, market_date: date) -> MarketUniverseSnapshot:
        target = _require_date(market_date, "market_date")
        if not self.snapshot_path.is_file():
            raise ProductionCompositionError(
                f"Universe snapshot artifact does not exist: {self.snapshot_path}"
            )
        try:
            payload = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProductionCompositionError(
                "Universe snapshot artifact could not be read as JSON"
            ) from error
        snapshot = _snapshot_from_dict(payload)
        if snapshot.market_date != target:
            raise ProductionCompositionError(
                "Universe snapshot market_date differs from requested market_date"
            )
        return snapshot


@dataclass(frozen=True, slots=True)
class ProductionS5Composition:
    """Callable production S5 runner plus non-canonical parity metadata."""

    orchestrator: DailyScreenerOrchestrator
    configuration: ProductionS5Configuration

    def __call__(self, market_date: date):
        return self.orchestrator.run(_require_date(market_date, "market_date"))

    @property
    def parity(self) -> dict[str, object]:
        return {
            "composition_contract_version": PRODUCTION_COMPOSITION_CONTRACT_VERSION,
            "universe_provider": "app.deployment.composition:JsonUniverseSnapshotProvider",
            "stage1_adapter": "app.screener.stage1_dataset:scan_stage1_from_dataset",
            "stage1_methodology_version": STAGE1_METHODOLOGY_VERSION,
            "stage2_adapter": "app.screener.stage2:research_stage2_candidate",
            "stage2_methodology_version": STAGE2_METHODOLOGY_VERSION,
            "s4_universe_repository": "app.storage.universe_persistence:SQLiteMarketUniverseRepository",
            "s4_checkpoint_repository": "app.storage.candidate_persistence:SQLiteScreenerCheckpointRepository",
            "s4_replay_repository": "app.storage.screener_replay:SQLiteScreenerReplayRepository",
            "s5_orchestrator": "app.screener.orchestration:DailyScreenerOrchestrator",
            "source_policy": TWSE_BASELINE_SOURCE_POLICY,
            "dataset_owner_resolution": (
                "successful twse-historical owner preferred; "
                "successful twse pipeline fallback; esun never selected"
            ),
            "canonical_row_projection": (
                "selected twse-historical source observations with legal-row fallback; "
                "legacy rows preserved"
            ),
            "candidate_limit": self.configuration.candidate_limit,
            "history_observations": HISTORY_OBSERVATIONS,
            "universe_snapshot_path": str(self.configuration.universe_snapshot_path),
        }


def _compose_s5(
    configuration: ProductionS5Configuration,
    dataset: ResearchDataset,
) -> ProductionS5Composition:
    """Wire frozen S1--S5 behavior around an explicit read-only dataset."""

    universe_provider = JsonUniverseSnapshotProvider(
        configuration.universe_snapshot_path
    )

    def stage1_runner(universe: MarketUniverseSnapshot, market_date: date) -> Stage1Execution:
        evidence = scan_stage1_from_dataset(
            universe=universe,
            dataset=dataset,
            as_of_date=_require_date(market_date, "market_date"),
            candidate_limit=configuration.candidate_limit,
        )
        return Stage1Execution(
            result=evidence.result,
            candidate_locators=tuple(
                CandidateInputLocator(
                    candidate.symbol,
                    _candidate_locator_sha256(candidate.symbol, market_date),
                )
                for candidate in evidence.result.candidates
            ),
        )

    def stage2_runner(request) -> Stage2CandidateExecution:
        snapshot = dataset.read(
            ResearchDatasetRequest(
                symbol=request.candidate.symbol,
                as_of_date=request.market_date,
                history_observations=HISTORY_OBSERVATIONS,
            )
        )
        candidate = research_stage2_candidate(
            stage1_candidate=request.candidate,
            dataset_snapshot=snapshot,
            market_date=request.market_date,
        )
        return Stage2CandidateExecution(
            candidate=candidate,
            research_locator_sha256=request.locator.research_locator_sha256,
            snapshot_sha256=_snapshot_evidence_sha256(snapshot),
        )

    orchestrator = DailyScreenerOrchestrator(
        configuration.database_path,
        universe_provider=universe_provider,
        stage1_runner=stage1_runner,
        stage2_runner=stage2_runner,
        candidate_validation_hook=(
            getattr(dataset, "ensure_candidate_validation", None)
            if callable(getattr(dataset, "ensure_candidate_validation", None))
            else None
        ),
    )
    return ProductionS5Composition(
        orchestrator=orchestrator,
        configuration=configuration,
    )


def create_s5(database_path: str | Path) -> ProductionS5Composition:
    """Resolve the formal production S5 composition for one explicit DB.

    Factory construction performs no Universe loading, network access,
    research execution, persistence, migration, report generation, or
    scheduler setup.  Those actions begin only when the returned callable is
    invoked by the frozen Daily Runner.
    """

    configuration = ProductionS5Configuration.from_environment(database_path)
    dataset = SQLiteScreenerResearchDataset(configuration.database_path)
    return _compose_s5(configuration, dataset)


def validate_production_s5_configuration(
    database_path: str | Path,
    *,
    environment: Mapping[str, str] | None = None,
) -> ProductionS5Configuration:
    """Validate explicit non-secret S5 configuration without running it."""

    if environment is None:
        return ProductionS5Configuration.from_environment(database_path)
    current = os.environ.get(UNIVERSE_SNAPSHOT_PATH_ENV)
    current_limit = os.environ.get(CANDIDATE_LIMIT_ENV)
    try:
        if UNIVERSE_SNAPSHOT_PATH_ENV in environment:
            os.environ[UNIVERSE_SNAPSHOT_PATH_ENV] = environment[
                UNIVERSE_SNAPSHOT_PATH_ENV
            ]
        if CANDIDATE_LIMIT_ENV in environment:
            os.environ[CANDIDATE_LIMIT_ENV] = environment[CANDIDATE_LIMIT_ENV]
        return ProductionS5Configuration.from_environment(database_path)
    finally:
        _restore_environment(UNIVERSE_SNAPSHOT_PATH_ENV, current)
        _restore_environment(CANDIDATE_LIMIT_ENV, current_limit)


def _snapshot_from_dict(payload: object) -> MarketUniverseSnapshot:
    if not isinstance(payload, dict):
        raise ProductionCompositionError("Universe snapshot must be a JSON object")
    try:
        market_date = date.fromisoformat(str(payload["market_date"]))
        members_payload = payload["members"]
        if not isinstance(members_payload, list):
            raise TypeError("members must be an array")
        members = tuple(
            _member_from_dict(item) for item in members_payload
        )
        return MarketUniverseSnapshot(
            market_date=market_date,
            methodology_version=str(payload["methodology_version"]),
            source_policy=str(payload["source_policy"]),
            universe_count=int(payload["universe_count"]),
            scan_eligible_count=int(payload["scan_eligible_count"]),
            scan_unavailable_count=int(payload["scan_unavailable_count"]),
            excluded_count=int(payload["excluded_count"]),
            inactive_count=int(payload["inactive_count"]),
            unresolved_count=int(payload["unresolved_count"]),
            members=members,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ProductionCompositionError(
            "Universe snapshot does not match the frozen S1 canonical model"
        ) from error


def _member_from_dict(payload: object) -> MarketUniverseMember:
    if not isinstance(payload, dict):
        raise TypeError("Universe member must be an object")
    evidence_payload = payload["source_evidence"]
    if not isinstance(evidence_payload, list):
        raise TypeError("source_evidence must be an array")
    evidence = tuple(_evidence_from_dict(item) for item in evidence_payload)
    listing_date = payload.get("listing_date")
    delisting_date = payload.get("delisting_date")
    return MarketUniverseMember(
        symbol=str(payload["symbol"]),
        name=None if payload.get("name") is None else str(payload["name"]),
        market=str(payload["market"]),
        status=UniverseMemberStatus(str(payload["status"])),
        listing_date=(None if listing_date is None else date.fromisoformat(str(listing_date))),
        delisting_date=(
            None if delisting_date is None else date.fromisoformat(str(delisting_date))
        ),
        exclusion_reason=(
            None
            if payload.get("exclusion_reason") is None
            else str(payload["exclusion_reason"])
        ),
        source_evidence=evidence,
    )


def _evidence_from_dict(payload: object) -> SourceEvidence:
    if not isinstance(payload, dict):
        raise TypeError("source evidence must be an object")
    return SourceEvidence(
        source=str(payload["source"]),
        dataset=str(payload["dataset"]),
        source_ref=str(payload["source_ref"]),
        contract_version=str(payload["contract_version"]),
        payload_sha256=str(payload["payload_sha256"]),
        payload_size_bytes=int(payload["payload_size_bytes"]),
        hash_basis=str(payload["hash_basis"]),
    )


def _candidate_locator_sha256(symbol: str, market_date: date) -> str:
    return _sha256(
        {
            "symbol": symbol,
            "market_date": _require_date(market_date, "market_date").isoformat(),
            "history_observations": HISTORY_OBSERVATIONS,
            "source_policy": TWSE_BASELINE_SOURCE_POLICY,
        }
    )


def _snapshot_evidence_sha256(snapshot: object) -> str:
    return _sha256(
        {
            "symbol": snapshot.symbol.symbol,
            "as_of_date": snapshot.as_of.as_of_date.isoformat(),
            "history_observations": snapshot.as_of.history_observations,
            "returned_history_observations": snapshot.as_of.returned_history_observations,
            "total_history_observations": snapshot.as_of.total_history_observations,
            "price_rows": [
                {
                    "trade_date": item.trade_date.isoformat(),
                    "open": item.open,
                    "high": item.high,
                    "low": item.low,
                    "close": item.close,
                    "volume": item.volume,
                    "source": item.source,
                }
                for item in snapshot.price_history.observations
            ],
            "valuation": [
                {
                    "metric_date": item.metric_date.isoformat(),
                    "name": item.name,
                    "value": item.value,
                    "unit": item.unit,
                    "source": item.source,
                }
                for item in snapshot.valuation.metrics
            ],
            "canonical_sources": snapshot.provenance.canonical_sources,
            "validation_sources": snapshot.provenance.validation_sources,
            "artifacts": [item.payload_sha256 for item in snapshot.provenance.artifact_refs],
        }
    )


def _sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _absolute_path(value: str | Path, field_name: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ProductionCompositionError(f"{field_name} must be absolute")
    return path.resolve()


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ProductionCompositionError(f"{field_name} must be a date")
    return value


def _restore_environment(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


__all__ = [
    "CANDIDATE_LIMIT_ENV",
    "DEFAULT_CANDIDATE_LIMIT",
    "HISTORY_OBSERVATIONS",
    "PRODUCTION_COMPOSITION_CONTRACT_VERSION",
    "PRODUCTION_S5_FACTORY_LOCATOR",
    "ProductionCompositionError",
    "ProductionS5Composition",
    "ProductionS5Configuration",
    "SQLiteScreenerResearchDataset",
    "UNIVERSE_SNAPSHOT_PATH_ENV",
    "JsonUniverseSnapshotProvider",
    "create_s5",
    "validate_production_s5_configuration",
]
