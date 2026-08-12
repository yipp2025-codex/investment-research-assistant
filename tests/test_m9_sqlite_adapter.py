"""M9.2 tests for the read-only v10 SQLite ResearchDataset adapter."""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

import app.sqlite_research_dataset as sqlite_dataset_module
from app.as_of_policy import FrozenAsOfPolicyV1
from app.models import CompanyMetric, DailyPrice, Symbol
from app.research_dataset import (
    DatasetSourcePolicyError,
    ResearchDataset,
    ResearchDatasetRequest,
)
from app.sqlite_research_dataset import (
    SQLiteDatasetSchemaError,
    SQLiteResearchDataset,
)
from app.storage import (
    SQLiteCrossValidationRepository,
    SQLiteDailyReportRepository,
    SQLiteResearchRepository,
)


UTC = timezone.utc
MARKET_DATE = date(2026, 8, 5)
FIXED_TIME = datetime(2026, 8, 5, 10, tzinfo=UTC)


def _price(
    symbol: str,
    trade_date: date,
    close: float,
    *,
    source: str = "twse-historical",
) -> DailyPrice:
    return DailyPrice(
        symbol=symbol,
        trade_date=trade_date,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1_000,
        source=source,
    )


def _metric(
    symbol: str,
    metric_date: date,
    value: float,
    *,
    source: str = "twse",
    name: str = "price_earnings_ratio",
) -> CompanyMetric:
    return CompanyMetric(
        symbol=symbol,
        metric_date=metric_date,
        name=name,
        value=value,
        unit="ratio",
        source=source,
    )


def _seed_pipeline(
    repository: SQLiteResearchRepository,
    *,
    run_id: str,
    symbol: str,
    target_date: date,
    provider: str,
    endpoint: str,
) -> None:
    timestamp = FIXED_TIME.isoformat()
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO pipeline_runs ("
            "run_id, symbol, target_date, requested_start_date, "
            "requested_end_date, status, provider, attempt_count, "
            "source_endpoints_json, fetched_at, market_date, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, 'pending', ?, 0, ?, ?, ?, ?, ?)",
            (
                run_id,
                symbol,
                target_date.isoformat(),
                (target_date - timedelta(days=7)).isoformat(),
                target_date.isoformat(),
                provider,
                json.dumps([endpoint], separators=(",", ":")),
                timestamp,
                target_date.isoformat(),
                timestamp,
                timestamp,
            ),
        )


def _seed_historical(repository: SQLiteResearchRepository) -> None:
    timestamp = FIXED_TIME.isoformat()
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO historical_sync_runs ("
            "run_id, symbol, target_date, target_observations, provider, status, "
            "next_month, months_completed, observation_count, created_at, "
            "attempt_count, source_endpoint, fetched_at, updated_at) "
            "VALUES ('historical-main', '2330', ?, 250, 'twse-historical', "
            "'pending', ?, 0, 0, ?, 0, ?, ?, ?)",
            (
                MARKET_DATE.isoformat(),
                date(2026, 8, 1).isoformat(),
                timestamp,
                "https://evidence.invalid/historical-main",
                timestamp,
                timestamp,
            ),
        )


def _seed_validation(
    repository: SQLiteResearchRepository,
    *,
    run_id: str,
    target_date: date,
    right_provider: str,
    created_at: datetime,
    discrepancies: tuple[tuple[str, str, str, str], ...] = (),
) -> None:
    outcome = "discrepancy" if discrepancies else "match"
    finished_at = created_at + timedelta(minutes=2)
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO market_data_validation_runs ("
            "run_id, symbol, target_date, requested_start_date, left_provider, "
            "right_provider, status, outcome, created_at, started_at, finished_at, "
            "attempt_count, updated_at) "
            "VALUES (?, '2330', ?, ?, 'twse', ?, 'success', ?, ?, ?, ?, 1, ?)",
            (
                run_id,
                target_date.isoformat(),
                (target_date - timedelta(days=2)).isoformat(),
                right_provider,
                outcome,
                created_at.isoformat(),
                (created_at + timedelta(minutes=1)).isoformat(),
                finished_at.isoformat(),
                finished_at.isoformat(),
            ),
        )
        # Reverse provider order so selected AUTOINCREMENT ids prove SQL ordering.
        for provider, close in ((right_provider, 105.5), ("twse", 105.0)):
            connection.execute(
                "INSERT INTO market_data_observations ("
                "run_id, provider, symbol, market_date, open_price, high_price, "
                "low_price, close_price, volume, source_endpoints_json, fetched_at"
                ") VALUES (?, ?, '2330', ?, 105, 106, 104, ?, 1000, ?, ?)",
                (
                    run_id,
                    provider,
                    target_date.isoformat(),
                    close,
                    json.dumps(
                        [f"https://evidence.invalid/{provider}"],
                        separators=(",", ":"),
                    ),
                    finished_at.isoformat(),
                ),
            )
        for field, left_value, right_value, reason in discrepancies:
            connection.execute(
                "INSERT INTO market_data_discrepancies ("
                "run_id, field, left_value, right_value, reason) "
                "VALUES (?, ?, ?, ?, ?)",
                (run_id, field, left_value, right_value, reason),
            )


def _seed_artifact(
    repository: SQLiteResearchRepository,
    *,
    owner_column: str,
    run_id: str,
    provider: str,
    marker: str,
) -> None:
    if owner_column not in {
        "pipeline_run_id",
        "historical_run_id",
        "validation_run_id",
    }:
        raise AssertionError("test owner column must be fixed")
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO source_artifacts ("
            f"{owner_column}, checkpoint_key, provider, dataset, endpoint, "
            "contract_version, content_type, payload_sha256, payload_size_bytes, "
            "hash_basis, fetched_at, created_at) "
            "VALUES (?, 'm9-adapter-parity', ?, ?, ?, 'm7-v1', "
            "'application/json', ?, 123, 'raw-response-bytes-v1', ?, ?)",
            (
                run_id,
                provider,
                f"dataset-{marker}",
                f"https://evidence.invalid/artifacts/{marker}",
                marker * 64,
                FIXED_TIME.isoformat(),
                FIXED_TIME.isoformat(),
            ),
        )


@dataclass(frozen=True)
class _AdapterFixture:
    database_path: Path
    repository: SQLiteResearchRepository
    cross_repository: SQLiteCrossValidationRepository


@pytest.fixture
def adapter_fixture(tmp_path: Path) -> _AdapterFixture:
    database_path = tmp_path / "m9-adapter.db"
    repository = SQLiteResearchRepository(database_path)
    SQLiteDailyReportRepository(repository).initialize()
    cross_repository = SQLiteCrossValidationRepository(repository)

    for symbol, market in (
        ("2330", "TWSE"),
        ("2882", "TWSE"),
        ("9999", "TWSE"),
        ("MOCK1", "MOCK"),
        ("MIX1", "MOCK"),
    ):
        repository.upsert_symbol(Symbol(symbol, f"Adapter {symbol}", market, "TWD"))

    repository.upsert_daily_prices(
        [
            _price("2330", date(2026, 8, 6), 999.0, source="esun"),
            _price("2330", MARKET_DATE, 105.0),
            _price("2330", date(2026, 7, 31), 98.0, source="twse"),
            _price("2330", date(2026, 8, 4), 103.0, source="twse"),
            _price("2330", date(2026, 8, 1), 100.0),
            _price("2882", MARKET_DATE, 60.0, source="esun"),
            _price("9999", MARKET_DATE, 50.0, source="vendor-x"),
            _price("MOCK1", MARKET_DATE, 70.0, source="mock-synthetic"),
            _price("MIX1", MARKET_DATE, 80.0, source="mock-synthetic"),
        ]
    )
    repository.upsert_company_metrics(
        [
            _metric("2330", date(2026, 8, 6), 99.0, source="esun"),
            _metric("2330", date(2026, 7, 31), 18.0),
            _metric("2330", date(2026, 8, 4), 20.0),
            _metric("MOCK1", MARKET_DATE, 12.5, source="mock-synthetic"),
            _metric("MIX1", MARKET_DATE, 15.0, source="twse"),
        ]
    )

    _seed_pipeline(
        repository,
        run_id="pipeline-main",
        symbol="2330",
        target_date=MARKET_DATE,
        provider="twse",
        endpoint="https://evidence.invalid/pipeline-main",
    )
    _seed_pipeline(
        repository,
        run_id="pipeline-future",
        symbol="2330",
        target_date=date(2026, 8, 6),
        provider="twse",
        endpoint="https://evidence.invalid/pipeline-future",
    )
    _seed_historical(repository)
    _seed_validation(
        repository,
        run_id="validation-old",
        target_date=MARKET_DATE,
        right_provider="esun-historical",
        created_at=datetime(2026, 8, 5, 9, tzinfo=UTC),
    )
    _seed_validation(
        repository,
        run_id="validation-new",
        target_date=MARKET_DATE,
        right_provider="esun",
        created_at=datetime(2026, 8, 5, 10, tzinfo=UTC),
        discrepancies=(
            ("volume", "1000", "1100", "volume differs"),
            ("close", "105", "105.5", "close differs"),
        ),
    )
    _seed_validation(
        repository,
        run_id="validation-future",
        target_date=date(2026, 8, 6),
        right_provider="esun",
        created_at=datetime(2026, 8, 6, 11, tzinfo=UTC),
    )
    _seed_artifact(
        repository,
        owner_column="pipeline_run_id",
        run_id="pipeline-main",
        provider="twse",
        marker="a",
    )
    _seed_artifact(
        repository,
        owner_column="historical_run_id",
        run_id="historical-main",
        provider="twse-historical",
        marker="b",
    )
    _seed_artifact(
        repository,
        owner_column="validation_run_id",
        run_id="validation-new",
        provider="esun",
        marker="c",
    )
    return _AdapterFixture(database_path, repository, cross_repository)


@dataclass(frozen=True)
class _DatabaseSnapshot:
    file_sha256: str
    file_size: int
    application_schema_version: int
    sqlite_schema_version: int
    sqlite_master_sha256: str
    row_counts: tuple[tuple[str, int], ...]
    integrity_check: tuple[str, ...]
    foreign_key_violations: tuple[tuple[object, ...], ...]


def _database_snapshot(database_path: Path) -> _DatabaseSnapshot:
    uri = database_path.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        tables = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        )
        row_counts = tuple(
            (
                table,
                int(
                    connection.execute(
                        f'SELECT COUNT(*) FROM "{table}"'
                    ).fetchone()[0]
                ),
            )
            for table in tables
        )
        master = connection.execute(
            "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_master "
            "ORDER BY type, name"
        ).fetchall()
        master_json = json.dumps(master, separators=(",", ":"))
        application_version = int(
            connection.execute(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
            ).fetchone()[0]
        )
        sqlite_schema_version = int(
            connection.execute("PRAGMA schema_version").fetchone()[0]
        )
        integrity = tuple(
            row[0] for row in connection.execute("PRAGMA integrity_check")
        )
        foreign_keys = tuple(
            tuple(row) for row in connection.execute("PRAGMA foreign_key_check")
        )
    raw = database_path.read_bytes()
    return _DatabaseSnapshot(
        file_sha256=hashlib.sha256(raw).hexdigest(),
        file_size=len(raw),
        application_schema_version=application_version,
        sqlite_schema_version=sqlite_schema_version,
        sqlite_master_sha256=hashlib.sha256(master_json.encode("utf-8")).hexdigest(),
        row_counts=row_counts,
        integrity_check=integrity,
        foreign_key_violations=foreign_keys,
    )


def _sidecars(database_path: Path) -> tuple[str, ...]:
    candidates = (
        Path(str(database_path) + "-journal"),
        Path(str(database_path) + "-wal"),
        Path(str(database_path) + "-shm"),
    )
    return tuple(item.name for item in candidates if item.exists())


def _domain_price(item: object) -> tuple[object, ...]:
    return (
        item.symbol,
        item.trade_date,
        item.open,
        item.high,
        item.low,
        item.close,
        item.volume,
        item.source,
    )


def _domain_metric(item: object) -> tuple[object, ...]:
    return (
        item.symbol,
        item.metric_date,
        item.name,
        item.value,
        item.unit,
        item.source,
    )


def test_sqlite_adapter_matches_frozen_repository_rows_and_is_byte_read_only(
    adapter_fixture: _AdapterFixture,
) -> None:
    fixture = adapter_fixture
    repository_prices = fixture.repository.list_daily_prices(
        "2330", end_date=MARKET_DATE
    )
    repository_metrics = fixture.repository.list_company_metrics(
        "2330", end_date=MARKET_DATE
    )
    runs = fixture.cross_repository.list_runs("2330")
    selected_run = max(
        (
            item
            for item in runs
            if item.target_date == MARKET_DATE and item.status.value == "success"
        ),
        key=lambda item: item.created_at,
    )
    repository_observations = fixture.cross_repository.list_observations(
        selected_run.run_id
    )
    repository_discrepancies = fixture.cross_repository.list_discrepancies(
        selected_run.run_id
    )
    pipeline = fixture.repository.get_pipeline_run_for_target("2330", MARKET_DATE)
    assert pipeline is not None
    artifact_baseline = {
        "pipeline": fixture.repository.list_source_artifacts(
            pipeline_run_id="pipeline-main"
        ),
        "historical": fixture.repository.list_source_artifacts(
            historical_run_id="historical-main"
        ),
        "validation": fixture.repository.list_source_artifacts(
            validation_run_id="validation-new"
        ),
    }
    before = _database_snapshot(fixture.database_path)
    sidecars_before = _sidecars(fixture.database_path)

    adapter = SQLiteResearchDataset(fixture.database_path)
    snapshot = adapter.read(
        ResearchDatasetRequest(
            "2330",
            MARKET_DATE,
            history_observations=None,
            historical_run_id="historical-main",
        )
    )

    after = _database_snapshot(fixture.database_path)
    assert before == after
    assert sidecars_before == _sidecars(fixture.database_path) == ()
    assert before.application_schema_version == 10
    assert before.integrity_check == ("ok",)
    assert before.foreign_key_violations == ()

    assert [_domain_price(item) for item in snapshot.price_history.observations] == [
        _domain_price(item) for item in repository_prices
    ]
    assert [item.trade_date for item in snapshot.price_history.observations] == [
        date(2026, 7, 31),
        date(2026, 8, 1),
        date(2026, 8, 4),
        MARKET_DATE,
    ]
    assert snapshot.price_history.current.close == 105.0

    grouped: dict[str, list[CompanyMetric]] = {}
    for metric in repository_metrics:
        grouped.setdefault(metric.name, []).append(metric)
    selected_metrics = tuple(
        FrozenAsOfPolicyV1.select_valuation_as_of(values, MARKET_DATE)
        for _, values in sorted(grouped.items())
    )
    assert [_domain_metric(item) for item in snapshot.valuation.metrics] == [
        _domain_metric(item) for item in selected_metrics
    ]
    assert snapshot.valuation.metrics[0].metric_date == date(2026, 8, 4)
    assert snapshot.valuation.metrics[0].value == 20.0

    assert [item.run_id for item in runs] == [
        "validation-old",
        "validation-new",
        "validation-future",
    ]
    assert selected_run.run_id == "validation-new"
    assert [(item.id, item.provider) for item in repository_observations] == [
        (4, "twse"),
        (3, "esun"),
    ]
    assert [item.provider for item in snapshot.validation.observations] == [
        item.provider for item in repository_observations
    ]
    assert [
        (item.market_date, item.close, item.volume)
        for item in snapshot.validation.observations
    ] == [
        (item.market_date, item.close, item.volume)
        for item in repository_observations
    ]
    assert [(item.id, item.field) for item in repository_discrepancies] == [
        (2, "close"),
        (1, "volume"),
    ]
    assert [item.field for item in snapshot.validation.discrepancies] == [
        item.field for item in repository_discrepancies
    ]
    assert snapshot.validation.status == "source_discrepancy"

    assert pipeline.run_id == "pipeline-main"
    assert snapshot.provenance.pipeline_run_id == pipeline.run_id
    assert snapshot.provenance.historical_run_id == "historical-main"
    assert snapshot.provenance.validation_run_id == selected_run.run_id
    assert snapshot.provenance.canonical_sources == (
        "twse",
        "twse-historical",
    )
    assert snapshot.provenance.validation_sources == ("esun", "twse")
    assert [
        (item.owner_kind, item.owner_run_id, item.provider, item.payload_sha256)
        for item in snapshot.provenance.artifact_refs
    ] == [
        ("historical", "historical-main", "twse-historical", "b" * 64),
        ("pipeline", "pipeline-main", "twse", "a" * 64),
        ("validation", "validation-new", "esun", "c" * 64),
    ]
    assert {
        item.provider
        for values in artifact_baseline.values()
        for item in values
    } == {"twse", "twse-historical", "esun"}
    assert {item.endpoint for item in snapshot.provenance.artifact_refs} == {
        item.endpoint
        for values in artifact_baseline.values()
        for item in values
    }


def test_history_limit_uses_latest_n_then_returns_ascending_without_future_rows(
    adapter_fixture: _AdapterFixture,
) -> None:
    before = _database_snapshot(adapter_fixture.database_path)
    adapter = SQLiteResearchDataset(adapter_fixture.database_path)

    complete = adapter.read(
        ResearchDatasetRequest("2330", MARKET_DATE, history_observations=None)
    )
    limited = adapter.read(
        ResearchDatasetRequest("2330", MARKET_DATE, history_observations=2)
    )

    assert [item.trade_date for item in complete.price_history.observations] == [
        date(2026, 7, 31),
        date(2026, 8, 1),
        date(2026, 8, 4),
        MARKET_DATE,
    ]
    assert [item.trade_date for item in limited.price_history.observations] == [
        date(2026, 8, 4),
        MARKET_DATE,
    ]
    assert limited.as_of.total_history_observations == 4
    assert limited.as_of.returned_history_observations == 2
    assert limited.as_of.history_is_truncated is True
    assert all(
        item.trade_date <= MARKET_DATE
        for item in complete.price_history.observations
    )
    assert before == _database_snapshot(adapter_fixture.database_path)


def test_adapter_source_policy_preserves_esun_validation_and_fails_contamination(
    adapter_fixture: _AdapterFixture,
) -> None:
    before = _database_snapshot(adapter_fixture.database_path)
    adapter = SQLiteResearchDataset(adapter_fixture.database_path)

    formal = adapter.read(ResearchDatasetRequest("2330", MARKET_DATE))
    synthetic = adapter.read(ResearchDatasetRequest("MOCK1", MARKET_DATE))
    assert [item.provider for item in formal.validation.observations] == [
        "twse",
        "esun",
    ]
    assert "esun" not in formal.provenance.canonical_sources
    assert synthetic.price_history.current.source == "mock-synthetic"
    assert synthetic.valuation.metrics[0].source == "mock-synthetic"
    assert synthetic.provenance.canonical_sources == ("mock-synthetic",)

    for symbol, expected in (
        ("2882", "esun"),
        ("9999", "vendor-x"),
        ("MIX1", "must not mix"),
    ):
        with pytest.raises(DatasetSourcePolicyError, match=expected):
            adapter.read(ResearchDatasetRequest(symbol, MARKET_DATE))
    assert before == _database_snapshot(adapter_fixture.database_path)


def test_validation_exact_selection_and_status_mapping_use_frozen_as_of_policy(
    adapter_fixture: _AdapterFixture,
) -> None:
    with adapter_fixture.repository._transaction() as connection:
        connection.execute(
            "INSERT INTO market_data_discrepancies ("
            "run_id, field, left_value, right_value, reason) "
            "VALUES ('validation-new', 'market_date', '2026-08-05', "
            "'2026-08-04', 'provider market dates differ')"
        )
    before = _database_snapshot(adapter_fixture.database_path)
    adapter = SQLiteResearchDataset(adapter_fixture.database_path)

    implicit = adapter.read(ResearchDatasetRequest("2330", MARKET_DATE))
    explicit_match = adapter.read(
        ResearchDatasetRequest(
            "2330",
            MARKET_DATE,
            validation_run_id="validation-old",
        )
    )
    inconsistent_future = adapter.read(
        ResearchDatasetRequest(
            "2330",
            MARKET_DATE,
            validation_run_id="validation-future",
        )
    )

    assert implicit.validation.run_id == "validation-new"
    assert implicit.validation.status == "market_date_mismatch"
    assert [item.field for item in implicit.validation.discrepancies] == [
        "close",
        "market_date",
        "volume",
    ]
    assert explicit_match.validation.run_id == "validation-old"
    assert explicit_match.validation.status == "available"
    assert explicit_match.validation.discrepancies == ()
    assert inconsistent_future.validation.status == "missing_source"
    assert inconsistent_future.provenance.validation_run_id is None
    assert before == _database_snapshot(adapter_fixture.database_path)


def test_adapter_connections_are_mode_ro_query_only_and_never_authorize_writes(
    adapter_fixture: _AdapterFixture,
) -> None:
    before = _database_snapshot(adapter_fixture.database_path)
    sidecars_before = _sidecars(adapter_fixture.database_path)
    real_connect = sqlite3.connect
    opened: list[tuple[tuple[object, ...], dict[str, object]]] = []
    actions: list[tuple[int, str | None, str | None]] = []
    write_actions: list[tuple[int, str | None, str | None]] = []
    forbidden = {
        getattr(sqlite3, name)
        for name in (
            "SQLITE_INSERT",
            "SQLITE_UPDATE",
            "SQLITE_DELETE",
            "SQLITE_CREATE_INDEX",
            "SQLITE_CREATE_TABLE",
            "SQLITE_CREATE_TEMP_INDEX",
            "SQLITE_CREATE_TEMP_TABLE",
            "SQLITE_CREATE_TEMP_TRIGGER",
            "SQLITE_CREATE_TEMP_VIEW",
            "SQLITE_CREATE_TRIGGER",
            "SQLITE_CREATE_VIEW",
            "SQLITE_DROP_INDEX",
            "SQLITE_DROP_TABLE",
            "SQLITE_DROP_TEMP_INDEX",
            "SQLITE_DROP_TEMP_TABLE",
            "SQLITE_DROP_TEMP_TRIGGER",
            "SQLITE_DROP_TEMP_VIEW",
            "SQLITE_DROP_TRIGGER",
            "SQLITE_DROP_VIEW",
            "SQLITE_ALTER_TABLE",
            "SQLITE_REINDEX",
            "SQLITE_ANALYZE",
            "SQLITE_CREATE_VTABLE",
            "SQLITE_DROP_VTABLE",
            "SQLITE_ATTACH",
            "SQLITE_DETACH",
        )
        if hasattr(sqlite3, name)
    }

    def monitored_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        opened.append((args, dict(kwargs)))
        connection = real_connect(*args, **kwargs)

        def authorize(
            action: int,
            first: str | None,
            second: str | None,
            database: str | None,
            trigger: str | None,
        ) -> int:
            del database, trigger
            actions.append((action, first, second))
            if action in forbidden:
                write_actions.append((action, first, second))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorize)
        return connection

    with patch.object(
        SQLiteResearchRepository,
        "initialize",
        side_effect=AssertionError("adapter must not initialize repositories"),
    ), patch(
        "app.sqlite_research_dataset.sqlite3.connect",
        side_effect=monitored_connect,
    ):
        adapter = SQLiteResearchDataset(adapter_fixture.database_path)
        snapshot = adapter.read(ResearchDatasetRequest("2330", MARKET_DATE))

    assert isinstance(adapter, ResearchDataset)
    assert snapshot.symbol.symbol == "2330"
    assert opened
    assert all("mode=ro" in str(args[0]) for args, _ in opened)
    assert all(kwargs.get("uri") is True for _, kwargs in opened)
    assert any(
        action == sqlite3.SQLITE_PRAGMA
        and first == "query_only"
        and second == "ON"
        for action, first, second in actions
    )
    assert write_actions == []
    for method in ("execute", "insert", "update", "delete", "create", "read_many"):
        assert not hasattr(adapter, method)
    assert before == _database_snapshot(adapter_fixture.database_path)
    assert sidecars_before == _sidecars(adapter_fixture.database_path) == ()


def test_adapter_module_has_no_repository_provider_or_write_surface() -> None:
    tree = ast.parse(inspect.getsource(sqlite_dataset_module))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    public_methods = [
        name
        for name, value in inspect.getmembers(
            SQLiteResearchDataset,
            predicate=inspect.isfunction,
        )
        if not name.startswith("_")
    ]

    assert public_methods == ["read"]
    assert not any(name.startswith("app.storage") for name in imports)
    assert not any(name.startswith("app.providers") for name in imports)
    assert not any(name.startswith("app.reports") for name in imports)


@pytest.mark.parametrize(
    "drift",
    ("missing-table", "missing-column", "wrong-version"),
)
def test_schema_drift_fails_closed_without_adapter_side_effects(
    tmp_path: Path,
    drift: str,
) -> None:
    database_path = tmp_path / f"schema-{drift}.db"
    repository = SQLiteResearchRepository(database_path)
    SQLiteDailyReportRepository(repository).initialize()
    with sqlite3.connect(database_path) as connection:
        if drift == "missing-table":
            connection.execute("DROP TABLE source_artifacts")
            expected = "tables are missing"
        elif drift == "missing-column":
            connection.execute(
                "ALTER TABLE daily_prices RENAME COLUMN source TO source_drifted"
            )
            expected = "missing required columns"
        else:
            connection.execute(
                "UPDATE schema_migrations SET version = 11 WHERE version = 10"
            )
            expected = "requires schema version 10"
    before = _database_snapshot(database_path)
    sidecars_before = _sidecars(database_path)

    with pytest.raises(SQLiteDatasetSchemaError, match=expected):
        SQLiteResearchDataset(database_path)

    assert before == _database_snapshot(database_path)
    assert before.integrity_check == ("ok",)
    assert before.foreign_key_violations == ()
    assert sidecars_before == _sidecars(database_path) == ()


def test_mode_ro_does_not_create_a_missing_database(tmp_path: Path) -> None:
    database_path = tmp_path / "does-not-exist.db"

    with pytest.raises(SQLiteDatasetSchemaError, match="does not exist"):
        SQLiteResearchDataset(database_path)

    assert not database_path.exists()
    assert _sidecars(database_path) == ()
