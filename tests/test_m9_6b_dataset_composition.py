"""M9.4 production composition tests for DailyResearchReportService."""

from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from app.models import CompanyMetric, DailyPrice, Symbol
from app.reports.composition import compose_sqlite_daily_research_report_service
from app.reports.daily_research import DailyResearchReportService
from app.research_dataset import DatasetSourcePolicyError, ResearchDatasetRequest
from app.sqlite_research_dataset import SQLiteResearchDataset
from app.storage import (
    SQLiteCrossValidationRepository,
    SQLiteDailyReportRepository,
    SQLiteResearchRepository,
)


UTC = timezone.utc
MARKET_DATE = date(2026, 8, 5)
FIXED_NOW = datetime(2026, 8, 8, 3, tzinfo=UTC)
FROZEN_SHA256 = (
    "8730b97cb0f744d0041f1ffde7cb4baf92848ed7318722326625c2f0e356ac64"
)
FROZEN_RESULT_ID = "daily-d7904f3bc10707ee001cdff49a2cd96b"
FROZEN_REPORT_ID = "report-daily-d7904f3bc10707ee001cdff49a2cd96b"


def _price(
    close: float = 105.0,
    *,
    symbol: str = "2330",
    trade_date: date = MARKET_DATE,
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
    value: float = 20.0,
    *,
    symbol: str = "2330",
    metric_date: date = date(2026, 8, 4),
    source: str = "twse",
) -> CompanyMetric:
    return CompanyMetric(
        symbol=symbol,
        metric_date=metric_date,
        name="price_earnings_ratio",
        value=value,
        unit="ratio",
        source=source,
    )


def _setup_fixed(
    database_path: Path,
    *,
    symbol: str = "2330",
) -> tuple[
    SQLiteResearchRepository,
    SQLiteDailyReportRepository,
]:
    repository = SQLiteResearchRepository(database_path)
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()
    repository.upsert_symbol(Symbol(symbol, f"Composition {symbol}", "TWSE", "TWD"))
    repository.upsert_daily_prices([_price(symbol=symbol)])
    repository.upsert_company_metrics([_metric(symbol=symbol)])
    return repository, report_repository


@dataclass(frozen=True)
class _ReadOnlyState:
    file_sha256: str
    schema_version: int
    sqlite_master_sha256: str
    row_counts: tuple[tuple[str, int], ...]
    integrity: tuple[str, ...]
    foreign_keys: tuple[tuple[object, ...], ...]


def _read_only_state(database_path: Path) -> _ReadOnlyState:
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
        schema_version = int(
            connection.execute(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
            ).fetchone()[0]
        )
        integrity = tuple(
            row[0] for row in connection.execute("PRAGMA integrity_check")
        )
        foreign_keys = tuple(
            tuple(row) for row in connection.execute("PRAGMA foreign_key_check")
        )
    return _ReadOnlyState(
        file_sha256=hashlib.sha256(database_path.read_bytes()).hexdigest(),
        schema_version=schema_version,
        sqlite_master_sha256=hashlib.sha256(master_json.encode("utf-8")).hexdigest(),
        row_counts=row_counts,
        integrity=integrity,
        foreign_keys=foreign_keys,
    )


class _AuditedDataset:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.delegate = SQLiteResearchDataset(database_path)
        self.requests: list[ResearchDatasetRequest] = []
        self.read_states: list[tuple[_ReadOnlyState, _ReadOnlyState]] = []

    def read(self, request: ResearchDatasetRequest):
        self.requests.append(request)
        before = _read_only_state(self.database_path)
        snapshot = self.delegate.read(request)
        after = _read_only_state(self.database_path)
        self.read_states.append((before, after))
        return snapshot


class _ExplodingDataset:
    def __init__(self) -> None:
        self.calls = 0

    def read(self, request: ResearchDatasetRequest):
        del request
        self.calls += 1
        raise AssertionError("replay must not read ResearchDataset")


@dataclass(frozen=True)
class _Call:
    name: str
    args: tuple[object, ...]
    kwargs: dict[str, object]


class _RecordingReportStore:
    def __init__(self, target: SQLiteDailyReportRepository, calls: list[_Call]) -> None:
        self._target = target
        self._calls = calls

    def __getattr__(self, name: str):
        value = getattr(self._target, name)
        if not callable(value) or name.startswith("_"):
            return value

        def recorded(*args: object, **kwargs: object):
            self._calls.append(_Call(name, tuple(args), dict(kwargs)))
            return value(*args, **kwargs)

        return recorded


def _unexpected_market_read(*args: object, **kwargs: object) -> object:
    del args, kwargs
    raise AssertionError("production 6B market reads must use ResearchDataset")


def test_production_dataset_payload_hash_ids_markdown_and_store_match_legacy(
    tmp_path: Path,
) -> None:
    legacy_path = tmp_path / "legacy.db"
    production_path = tmp_path / "production.db"
    legacy_repo, legacy_store = _setup_fixed(legacy_path)
    production_repo, production_store = _setup_fixed(production_path)
    legacy_prices = legacy_repo.list_daily_prices
    legacy_metrics = legacy_repo.list_company_metrics
    legacy_repo.list_daily_prices = (  # type: ignore[method-assign]
        lambda symbol, start_date=None, end_date=None: legacy_prices(
            symbol,
            start_date=start_date,
            end_date=end_date,
        )
    )
    legacy_repo.list_company_metrics = (  # type: ignore[method-assign]
        lambda symbol, start_date=None, end_date=None: legacy_metrics(
            symbol,
            start_date=start_date,
            end_date=end_date,
        )
    )
    legacy_service = DailyResearchReportService(
        legacy_repo,
        report_repository=legacy_store,
        cross_validation_repository=SQLiteCrossValidationRepository(legacy_repo),
        clock=lambda: FIXED_NOW,
    )
    legacy = legacy_service.generate("2330", MARKET_DATE)

    audited_dataset = _AuditedDataset(production_path)
    calls: list[_Call] = []
    recording_store = _RecordingReportStore(production_store, calls)
    production_service = compose_sqlite_daily_research_report_service(
        production_repo,
        report_repository=recording_store,  # type: ignore[arg-type]
        dataset=audited_dataset,
        clock=lambda: FIXED_NOW,
    )
    production_repo.list_daily_prices = _unexpected_market_read  # type: ignore[method-assign]
    production_repo.list_company_metrics = _unexpected_market_read  # type: ignore[method-assign]
    production_repo.get_pipeline_run = _unexpected_market_read  # type: ignore[method-assign]
    production_repo.get_pipeline_run_for_target = _unexpected_market_read  # type: ignore[method-assign]

    generated = production_service.generate("2330", MARKET_DATE)

    assert len(audited_dataset.requests) == 1
    request = audited_dataset.requests[0]
    assert request == ResearchDatasetRequest(
        "2330",
        MARKET_DATE,
        history_observations=None,
    )
    assert audited_dataset.read_states[0][0] == audited_dataset.read_states[0][1]
    assert generated.canonical.payload_json == legacy.canonical.payload_json
    assert generated.canonical.payload == legacy.canonical.payload
    assert generated.canonical.payload_sha256 == legacy.canonical.payload_sha256
    assert generated.canonical.payload_sha256 == FROZEN_SHA256
    assert generated.canonical.result_id == legacy.canonical.result_id == FROZEN_RESULT_ID
    assert generated.report.report_id == legacy.report.report_id == FROZEN_REPORT_ID
    assert generated.report.markdown == legacy.report.markdown
    assert generated.canonical.methodology_version == legacy.canonical.methodology_version
    assert generated.canonical.data_quality_status == legacy.canonical.data_quality_status
    assert [call.name for call in calls].count("save_or_get_result") == 1
    assert [call.name for call in calls].count("save_report_rendered") == 1
    assert production_store.count_results("2330") == legacy_store.count_results("2330") == 1
    assert production_store.count_reports("2330") == legacy_store.count_reports("2330") == 1


def test_default_sqlite_composition_is_lazy_and_reads_dataset_exactly_once(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "auto-composition.db"
    repository, report_store = _setup_fixed(database_path)
    events: list[str] = []
    calls: list[_Call] = []
    recording_store = _RecordingReportStore(report_store, calls)
    real_dataset_type = SQLiteResearchDataset

    class CountingSQLiteDataset:
        def __init__(self, database_path: Path) -> None:
            events.append("dataset-constructed")
            self.delegate = real_dataset_type(database_path)

        def read(self, request: ResearchDatasetRequest):
            events.append("dataset-read")
            return self.delegate.read(request)

    service = compose_sqlite_daily_research_report_service(
        repository,
        report_repository=recording_store,  # type: ignore[arg-type]
        clock=lambda: FIXED_NOW,
    )
    assert events == []

    with patch(
        "app.reports.composition.SQLiteResearchDataset",
        CountingSQLiteDataset,
    ):
        generated = service.generate("2330", MARKET_DATE)

    assert generated.canonical.payload_sha256 == FROZEN_SHA256
    assert events == ["dataset-constructed", "dataset-read"]
    assert [call.name for call in calls][:2] == ["initialize", "get_result"]
    assert [call.name for call in calls].count("save_or_get_result") == 1
    assert [call.name for call in calls].count("save_report_rendered") == 1


def test_existing_result_replay_never_constructs_or_reads_dataset_or_market_repo(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "replay.db"
    repository, report_store = _setup_fixed(database_path)
    first = compose_sqlite_daily_research_report_service(
        repository,
        report_repository=report_store,
        clock=lambda: FIXED_NOW,
    ).generate("2330", MARKET_DATE)
    replay_service = compose_sqlite_daily_research_report_service(
        repository,
        report_repository=report_store,
        clock=lambda: FIXED_NOW,
    )
    repository.list_daily_prices = _unexpected_market_read  # type: ignore[method-assign]
    repository.list_company_metrics = _unexpected_market_read  # type: ignore[method-assign]

    with patch(
        "app.reports.composition.SQLiteResearchDataset",
        side_effect=AssertionError("replay must not construct Dataset"),
    ):
        replay = replay_service.generate("2330", MARKET_DATE)

    assert replay.idempotent_replay is True
    assert replay.canonical.result_id == first.canonical.result_id
    assert replay.canonical.payload_sha256 == first.canonical.payload_sha256
    assert replay.report.report_id == first.report.report_id
    assert report_store.count_results("2330") == 1
    assert report_store.count_reports("2330") == 1


def test_explicit_dataset_replay_never_calls_read(tmp_path: Path) -> None:
    database_path = tmp_path / "explicit.db"
    repository, report_store = _setup_fixed(database_path)
    compose_sqlite_daily_research_report_service(
        repository,
        report_repository=report_store,
        clock=lambda: FIXED_NOW,
    ).generate("2330", MARKET_DATE)
    exploding = _ExplodingDataset()
    replay = compose_sqlite_daily_research_report_service(
        repository,
        report_repository=report_store,
        dataset=exploding,
        clock=lambda: FIXED_NOW,
    ).generate("2330", MARKET_DATE)

    assert replay.idempotent_replay is True
    assert exploding.calls == 0


def test_artifact_hashes_and_dataset_policy_never_enter_canonical_payload(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "artifact-boundary.db"
    repository, report_store = _setup_fixed(database_path)
    endpoint = "https://evidence.invalid/pipeline-artifact"
    timestamp = FIXED_NOW.isoformat()
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO pipeline_runs ("
            "run_id, symbol, target_date, requested_start_date, "
            "requested_end_date, status, provider, attempt_count, "
            "source_endpoints_json, fetched_at, market_date, created_at, updated_at"
            ") VALUES ('pipeline-artifact', '2330', ?, ?, ?, 'pending', "
            "'twse', 0, ?, ?, ?, ?, ?)",
            (
                MARKET_DATE.isoformat(),
                (MARKET_DATE - timedelta(days=7)).isoformat(),
                MARKET_DATE.isoformat(),
                json.dumps([endpoint], separators=(",", ":")),
                timestamp,
                MARKET_DATE.isoformat(),
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            "INSERT INTO source_artifacts ("
            "pipeline_run_id, checkpoint_key, provider, dataset, endpoint, "
            "contract_version, content_type, payload_sha256, payload_size_bytes, "
            "hash_basis, fetched_at, created_at) "
            "VALUES ('pipeline-artifact', 'm9.4', 'twse', 'daily', ?, 'm7-v1', "
            "'application/json', ?, 123, 'raw-response-bytes-v1', ?, ?)",
            (endpoint, "e" * 64, timestamp, timestamp),
        )
    audited = _AuditedDataset(database_path)
    generated = compose_sqlite_daily_research_report_service(
        repository,
        report_repository=report_store,
        dataset=audited,
        clock=lambda: FIXED_NOW,
    ).generate(
        "2330",
        MARKET_DATE,
        pipeline_run_id="pipeline-artifact",
    )
    payload_json = generated.canonical.payload_json

    assert "e" * 64 not in payload_json
    assert "twse_baseline" not in payload_json
    assert "dataset_version" not in payload_json
    assert "artifact_refs" not in payload_json
    assert generated.canonical.payload["provenance"]["pipeline_run_id"] == (
        "pipeline-artifact"
    )
    assert len(audited.requests) == 1
    assert audited.read_states[0][0] == audited.read_states[0][1]


def test_production_esun_canonical_contamination_fails_before_result_store_write(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "esun-fail-closed.db"
    repository = SQLiteResearchRepository(database_path)
    report_store = SQLiteDailyReportRepository(repository)
    report_store.initialize()
    repository.upsert_symbol(Symbol("2882", "Composition 2882", "TWSE", "TWD"))
    repository.upsert_daily_prices(
        [_price(symbol="2882", close=60.0, source="esun")]
    )
    repository.upsert_company_metrics(
        [_metric(symbol="2882", value=15.0, metric_date=MARKET_DATE, source="esun")]
    )
    before = _read_only_state(database_path)
    service = compose_sqlite_daily_research_report_service(
        repository,
        report_repository=report_store,
        clock=lambda: FIXED_NOW,
    )

    with pytest.raises(DatasetSourcePolicyError, match="esun"):
        service.generate("2882", MARKET_DATE)

    assert report_store.count_results("2882") == 0
    assert report_store.count_reports("2882") == 0
    assert before == _read_only_state(database_path)


def test_constructor_keeps_existing_shape_and_adds_kw_only_dataset() -> None:
    parameters = inspect.signature(DailyResearchReportService).parameters

    assert tuple(parameters)[:2] == ("repository", "report_repository")
    assert parameters["dataset"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["dataset"].default is None
