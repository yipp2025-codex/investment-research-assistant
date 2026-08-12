"""M9.3 shadow parity between repository reads and ResearchDataset snapshots."""

from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.reports.composition as report_composition_module
import app.reports.daily_research as daily_research_module
from app.models import CompanyMetric, DailyPrice, Symbol
from app.pipelines import HistoricalSyncPipeline, RetryPolicy
from app.providers import MockMarketDataProvider
from app.reports.daily_research import (
    METHODOLOGY_VERSION,
    DailyResearchReportService,
)
from app.reports.research_dataset_shadow import (
    ResearchDatasetShadowProjector,
    ShadowProjectionResult,
)
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
    trade_date: date,
    close: float,
    *,
    symbol: str = "2330",
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
    metric_date: date,
    value: float,
    *,
    symbol: str = "2330",
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


def _setup(
    database_path: Path,
    *,
    symbol: str = "2330",
    market: str = "TWSE",
) -> tuple[
    SQLiteResearchRepository,
    SQLiteDailyReportRepository,
    DailyResearchReportService,
]:
    repository = SQLiteResearchRepository(database_path)
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()
    repository.upsert_symbol(Symbol(symbol, f"Shadow {symbol}", market, "TWD"))
    service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        clock=lambda: FIXED_NOW,
    )
    return repository, report_repository, service


@dataclass(frozen=True)
class _DatabaseSnapshot:
    file_sha256: str
    file_size: int
    schema_version: int
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
        schema_version = int(
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
        schema_version=schema_version,
        sqlite_schema_version=sqlite_schema_version,
        sqlite_master_sha256=hashlib.sha256(master_json.encode("utf-8")).hexdigest(),
        row_counts=row_counts,
        integrity_check=integrity,
        foreign_key_violations=foreign_keys,
    )


def _assert_shadow_is_identical(
    generated: object,
    shadow: ShadowProjectionResult,
) -> None:
    assert shadow.payload == generated.canonical.payload
    assert shadow.canonical_json == generated.canonical.payload_json
    assert shadow.payload_sha256 == generated.canonical.payload_sha256
    assert shadow.result_id == generated.canonical.result_id
    assert shadow.report_id == generated.report.report_id
    assert shadow.markdown == generated.report.markdown


def _seed_pipeline(
    repository: SQLiteResearchRepository,
    *,
    run_id: str = "pipeline-rich",
    endpoint: str = "https://evidence.invalid/pipeline-rich",
) -> None:
    timestamp = FIXED_NOW.isoformat()
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO pipeline_runs ("
            "run_id, symbol, target_date, requested_start_date, "
            "requested_end_date, status, provider, attempt_count, "
            "source_endpoints_json, fetched_at, market_date, created_at, updated_at"
            ") VALUES (?, '2330', ?, ?, ?, 'pending', 'twse', 0, ?, ?, ?, ?, ?)",
            (
                run_id,
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


def _seed_historical(repository: SQLiteResearchRepository) -> None:
    timestamp = FIXED_NOW.isoformat()
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO historical_sync_runs ("
            "run_id, symbol, target_date, target_observations, provider, status, "
            "next_month, months_completed, observation_count, created_at, "
            "attempt_count, source_endpoint, fetched_at, updated_at) "
            "VALUES ('historical-rich', '2330', ?, 250, 'twse-historical', "
            "'pending', ?, 0, 0, ?, 0, ?, ?, ?)",
            (
                MARKET_DATE.isoformat(),
                date(2026, 8, 1).isoformat(),
                timestamp,
                "https://evidence.invalid/historical-rich",
                timestamp,
                timestamp,
            ),
        )


def _seed_validation(
    repository: SQLiteResearchRepository,
    *,
    run_id: str,
    discrepancy_field: str,
) -> None:
    created_at = datetime(2026, 8, 5, 10, tzinfo=UTC)
    finished_at = created_at + timedelta(minutes=2)
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO market_data_validation_runs ("
            "run_id, symbol, target_date, requested_start_date, left_provider, "
            "right_provider, status, outcome, created_at, started_at, finished_at, "
            "attempt_count, updated_at) "
            "VALUES (?, '2330', ?, ?, 'twse', 'esun', 'success', "
            "'discrepancy', ?, ?, ?, 1, ?)",
            (
                run_id,
                MARKET_DATE.isoformat(),
                (MARKET_DATE - timedelta(days=2)).isoformat(),
                created_at.isoformat(),
                (created_at + timedelta(minutes=1)).isoformat(),
                finished_at.isoformat(),
                finished_at.isoformat(),
            ),
        )
        for provider, close in (("esun", 105.5), ("twse", 105.0)):
            connection.execute(
                "INSERT INTO market_data_observations ("
                "run_id, provider, symbol, market_date, open_price, high_price, "
                "low_price, close_price, volume, source_endpoints_json, fetched_at"
                ") VALUES (?, ?, '2330', ?, 105, 106, 104, ?, 1000, ?, ?)",
                (
                    run_id,
                    provider,
                    MARKET_DATE.isoformat(),
                    close,
                    json.dumps(
                        [f"https://evidence.invalid/{provider}"],
                        separators=(",", ":"),
                    ),
                    finished_at.isoformat(),
                ),
            )
        if discrepancy_field == "market_date":
            left_value = MARKET_DATE.isoformat()
            right_value = date(2026, 8, 4).isoformat()
            reason = "provider market dates differ"
        else:
            left_value = "1000"
            right_value = "1100"
            reason = "volume differs"
        connection.execute(
            "INSERT INTO market_data_discrepancies ("
            "run_id, field, left_value, right_value, reason) "
            "VALUES (?, ?, ?, ?, ?)",
            (run_id, discrepancy_field, left_value, right_value, reason),
        )


def _seed_artifact(
    repository: SQLiteResearchRepository,
    *,
    owner_column: str,
    run_id: str,
    provider: str,
    endpoint: str,
    marker: str,
) -> None:
    if owner_column not in {
        "pipeline_run_id",
        "historical_run_id",
        "validation_run_id",
    }:
        raise AssertionError("fixed test owner required")
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO source_artifacts ("
            f"{owner_column}, checkpoint_key, provider, dataset, endpoint, "
            "contract_version, content_type, payload_sha256, payload_size_bytes, "
            "hash_basis, fetched_at, created_at) "
            "VALUES (?, 'm9-shadow', ?, ?, ?, 'm7-v1', 'application/json', "
            "?, 123, 'raw-response-bytes-v1', ?, ?)",
            (
                run_id,
                provider,
                f"shadow-{marker}",
                endpoint,
                marker * 64,
                FIXED_NOW.isoformat(),
                FIXED_NOW.isoformat(),
            ),
        )


def test_frozen_payload_hash_ids_and_markdown_are_bit_for_bit_identical(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "shadow-frozen.db"
    repository, report_repository, service = _setup(database_path)
    repository.upsert_daily_prices([_price(MARKET_DATE, 105.0)])
    repository.upsert_company_metrics(
        [_metric(date(2026, 8, 4), 20.0)]
    )
    generated = service.generate("2330", MARKET_DATE, requested_date=MARKET_DATE)
    before = _database_snapshot(database_path)

    shadow = ResearchDatasetShadowProjector(
        SQLiteResearchDataset(database_path)
    ).project(
        service,
        ResearchDatasetRequest("2330", MARKET_DATE),
        requested_date=MARKET_DATE,
    )

    _assert_shadow_is_identical(generated, shadow)
    assert shadow.payload_sha256 == FROZEN_SHA256
    assert shadow.result_id == FROZEN_RESULT_ID
    assert shadow.report_id == FROZEN_REPORT_ID
    assert shadow.artifact_refs == ()
    assert shadow.previous_successful_candidate_id is None
    assert shadow.previous_comparable_result_id is None
    assert shadow.previous_any_methodology_result_id is None
    assert report_repository.count_results("2330") == 1
    assert report_repository.count_reports("2330") == 1
    assert before == _database_snapshot(database_path)


def test_rich_shadow_projection_matches_inputs_payload_markdown_and_artifacts(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "shadow-rich.db"
    repository, report_repository, service = _setup(database_path)
    repository.upsert_daily_prices(
        [
            _price(date(2026, 8, 6), 999.0, source="esun"),
            _price(MARKET_DATE, 105.0),
            _price(date(2026, 8, 4), 103.0, source="twse"),
            _price(date(2026, 8, 1), 100.0),
        ]
    )
    repository.upsert_company_metrics(
        [
            _metric(date(2026, 8, 6), 99.0, source="esun"),
            _metric(date(2026, 8, 1), 18.0),
            _metric(date(2026, 8, 4), 20.0),
        ]
    )
    _seed_pipeline(repository)
    _seed_historical(repository)
    _seed_validation(
        repository,
        run_id="validation-rich",
        discrepancy_field="volume",
    )
    _seed_artifact(
        repository,
        owner_column="pipeline_run_id",
        run_id="pipeline-rich",
        provider="twse",
        endpoint="https://evidence.invalid/pipeline-rich",
        marker="a",
    )
    _seed_artifact(
        repository,
        owner_column="historical_run_id",
        run_id="historical-rich",
        provider="twse-historical",
        endpoint="https://evidence.invalid/historical-rich",
        marker="b",
    )
    _seed_artifact(
        repository,
        owner_column="validation_run_id",
        run_id="validation-rich",
        provider="esun",
        endpoint="https://evidence.invalid/esun",
        marker="c",
    )

    previous = service.generate("2330", date(2026, 8, 1))
    old_method = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        methodology_version="old-method-v0",
        clock=lambda: FIXED_NOW,
    ).generate("2330", date(2026, 8, 4))
    generated = service.generate(
        "2330",
        MARKET_DATE,
        pipeline_run_id="pipeline-rich",
        historical_run_id="historical-rich",
        validation_run_id="validation-rich",
    )
    before = _database_snapshot(database_path)
    request = ResearchDatasetRequest(
        "2330",
        MARKET_DATE,
        pipeline_run_id="pipeline-rich",
        historical_run_id="historical-rich",
        validation_run_id="validation-rich",
    )

    shadow = ResearchDatasetShadowProjector(
        SQLiteResearchDataset(database_path)
    ).project(service, request)

    _assert_shadow_is_identical(generated, shadow)
    assert [item.trade_date for item in shadow.snapshot.price_history.observations] == [
        date(2026, 8, 1),
        date(2026, 8, 4),
        MARKET_DATE,
    ]
    assert shadow.snapshot.price_history.current.close == 105.0
    assert shadow.snapshot.valuation.metrics[0].metric_date == date(2026, 8, 4)
    assert shadow.snapshot.valuation.metrics[0].value == 20.0
    assert shadow.snapshot.validation.status == "source_discrepancy"
    assert [item.provider for item in shadow.snapshot.validation.observations] == [
        "twse",
        "esun",
    ]
    assert [item.field for item in shadow.snapshot.validation.discrepancies] == [
        "volume"
    ]
    assert shadow.snapshot.provenance.pipeline_run_id == "pipeline-rich"
    assert shadow.snapshot.provenance.historical_run_id == "historical-rich"
    assert shadow.snapshot.provenance.validation_run_id == "validation-rich"
    assert [
        (item.owner_kind, item.provider, item.payload_sha256)
        for item in shadow.artifact_refs
    ] == [
        ("historical", "twse-historical", "b" * 64),
        ("pipeline", "twse", "a" * 64),
        ("validation", "esun", "c" * 64),
    ]
    repository_artifacts = (
        repository.list_source_artifacts(pipeline_run_id="pipeline-rich")
        + repository.list_source_artifacts(historical_run_id="historical-rich")
        + repository.list_source_artifacts(validation_run_id="validation-rich")
    )
    assert {item.payload_sha256 for item in shadow.artifact_refs} == {
        item.payload_sha256 for item in repository_artifacts
    }
    assert shadow.previous_successful_candidate_id == previous.canonical.result_id
    assert shadow.previous_comparable_result_id == previous.canonical.result_id
    assert shadow.previous_any_methodology_result_id == old_method.canonical.result_id
    assert shadow.payload["comparison"]["status"] == "available"
    assert shadow.payload["comparison"]["previous_market_date"] == "2026-08-01"
    assert 999.0 not in {
        value.get("value")
        for section in (shadow.payload["metrics"], shadow.payload["valuation"])
        for value in section.values()
    }
    assert before == _database_snapshot(database_path)


@pytest.mark.parametrize(
    ("case", "expected_field", "expected_status"),
    [
        ("missing", "price_status", "missing_source"),
        ("price-mismatch", "price_status", "market_date_mismatch"),
        ("validation-mismatch", "validation_status", "market_date_mismatch"),
    ],
)
def test_unavailable_and_market_date_statuses_have_payload_and_markdown_parity(
    tmp_path: Path,
    case: str,
    expected_field: str,
    expected_status: str,
) -> None:
    database_path = tmp_path / f"shadow-{case}.db"
    repository, _, service = _setup(database_path)
    validation_run_id = None
    if case == "price-mismatch":
        repository.upsert_daily_prices([_price(date(2026, 8, 4), 103.0)])
    elif case == "validation-mismatch":
        repository.upsert_daily_prices([_price(MARKET_DATE, 105.0)])
        validation_run_id = "validation-date-mismatch"
        _seed_validation(
            repository,
            run_id=validation_run_id,
            discrepancy_field="market_date",
        )
    generated = service.generate(
        "2330",
        MARKET_DATE,
        validation_run_id=validation_run_id,
    )
    before = _database_snapshot(database_path)
    request = ResearchDatasetRequest(
        "2330",
        MARKET_DATE,
        validation_run_id=validation_run_id,
    )

    shadow = ResearchDatasetShadowProjector(
        SQLiteResearchDataset(database_path)
    ).project(service, request)

    _assert_shadow_is_identical(generated, shadow)
    assert shadow.payload["data_quality"][expected_field] == expected_status
    assert before == _database_snapshot(database_path)


def test_methodology_mismatch_previous_input_and_projection_are_identical(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "shadow-methodology.db"
    repository, report_repository, _ = _setup(database_path)
    repository.upsert_daily_prices(
        [
            _price(date(2026, 8, 3), 100.0),
            _price(MARKET_DATE, 105.0),
        ]
    )
    old = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        methodology_version="old-method-v0",
        clock=lambda: FIXED_NOW,
    ).generate("2330", date(2026, 8, 3))
    service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        methodology_version=METHODOLOGY_VERSION,
        clock=lambda: FIXED_NOW,
    )
    generated = service.generate("2330", MARKET_DATE)
    before = _database_snapshot(database_path)

    shadow = ResearchDatasetShadowProjector(
        SQLiteResearchDataset(database_path)
    ).project(service, ResearchDatasetRequest("2330", MARKET_DATE))

    _assert_shadow_is_identical(generated, shadow)
    assert shadow.payload["comparison"] == {
        "status": "methodology_incompatible",
        "previous_result_id": None,
        "previous_market_date": None,
        "changes": [],
    }
    assert shadow.previous_successful_candidate_id is None
    assert shadow.previous_comparable_result_id is None
    assert shadow.previous_any_methodology_result_id == old.canonical.result_id
    assert before == _database_snapshot(database_path)


def test_esun_canonical_contamination_is_visible_legacy_but_shadow_fails_closed(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "shadow-esun-contamination.db"
    repository, report_repository, _ = _setup(database_path, symbol="2882")
    legacy_prices = repository.list_daily_prices
    legacy_metrics = repository.list_company_metrics
    repository.list_daily_prices = (  # type: ignore[method-assign]
        lambda symbol, start_date=None, end_date=None: legacy_prices(
            symbol,
            start_date=start_date,
            end_date=end_date,
        )
    )
    repository.list_company_metrics = (  # type: ignore[method-assign]
        lambda symbol, start_date=None, end_date=None: legacy_metrics(
            symbol,
            start_date=start_date,
            end_date=end_date,
        )
    )
    legacy_service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        cross_validation_repository=SQLiteCrossValidationRepository(repository),
        clock=lambda: FIXED_NOW,
    )
    repository.upsert_daily_prices(
        [_price(MARKET_DATE, 60.0, symbol="2882", source="esun")]
    )
    repository.upsert_company_metrics(
        [_metric(MARKET_DATE, 15.0, symbol="2882", source="esun")]
    )
    legacy = legacy_service.generate("2882", MARKET_DATE)
    before = _database_snapshot(database_path)

    with pytest.raises(DatasetSourcePolicyError, match="esun"):
        ResearchDatasetShadowProjector(
            SQLiteResearchDataset(database_path)
        ).project(legacy_service, ResearchDatasetRequest("2882", MARKET_DATE))

    assert legacy.canonical.payload["data_quality"]["price_status"] == "available"
    assert legacy.canonical.payload["provenance"]["price_sources"] == ["esun"]
    assert before == _database_snapshot(database_path)


class _ProvenanceMockProvider(MockMarketDataProvider):
    def fetch_market_data(
        self,
        symbol,
        start_date,
        end_date,
        *,
        timeout_seconds,
    ):
        batch = super().fetch_market_data(
            symbol,
            start_date,
            end_date,
            timeout_seconds=timeout_seconds,
        )
        latest = max(
            date.fromisoformat(str(item["trade_date"]))
            for item in batch.daily_prices
        )
        endpoint = "mock://historical-fixture"
        fetched_at = datetime(2026, 8, 6, 12, tzinfo=UTC)
        return replace(
            batch,
            source_endpoints=(endpoint,),
            fetched_at=fetched_at,
            market_date=latest,
            source_artifacts=tuple(
                replace(artifact, endpoint=endpoint, fetched_at=fetched_at)
                for artifact in batch.source_artifacts
            ),
        )


def test_mock_synthetic_ten_day_shadow_parity_keeps_ten_results_and_reports(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "shadow-mock-ten-day.db"
    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    repository.upsert_symbol(Symbol("2330", "Synthetic 2330", "TWSE", "TWD"))
    historical = HistoricalSyncPipeline(
        _ProvenanceMockProvider(),
        repository,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.0),
        sleep=lambda _: None,
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=UTC),
    )
    historical_result = historical.run(
        "2330",
        date(2026, 7, 31),
        target_observations=250,
    )
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()
    service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=UTC),
    )
    dates = sorted(
        {item.trade_date for item in repository.list_daily_prices("2330")}
    )[-10:]
    first_pass = [
        service.generate(
            "2330",
            market_date,
            historical_run_id=historical_result.run_id,
        )
        for market_date in dates
    ]
    replay = [
        service.generate(
            "2330",
            market_date,
            historical_run_id=historical_result.run_id,
        )
        for market_date in dates
    ]
    before = _database_snapshot(database_path)
    projector = ResearchDatasetShadowProjector(
        SQLiteResearchDataset(database_path)
    )

    shadows = [
        projector.project(
            service,
            ResearchDatasetRequest(
                "2330",
                market_date,
                historical_run_id=historical_result.run_id,
            ),
        )
        for market_date in dates
    ]

    for generated, shadow in zip(first_pass, shadows, strict=True):
        _assert_shadow_is_identical(generated, shadow)
        assert shadow.snapshot.provenance.canonical_sources == ("mock-synthetic",)
        assert shadow.snapshot.price_history.current.source == "mock-synthetic"
        assert len(shadow.artifact_refs) == 12
        assert {item.provider for item in shadow.artifact_refs} == {
            "mock-synthetic"
        }
    assert len(first_pass) == len(shadows) == 10
    assert all(item.idempotent_replay for item in replay)
    assert report_repository.count_results("2330") == 10
    assert report_repository.count_reports("2330") == 10
    assert before == _database_snapshot(database_path)


def test_production_6b_consumer_depends_on_dataset_but_not_shadow() -> None:
    service_source = inspect.getsource(daily_research_module)
    composition_source = inspect.getsource(report_composition_module)

    assert "ResearchDataset" in service_source
    assert "SQLiteResearchDataset" not in service_source
    assert "SQLiteResearchDataset" in composition_source
    assert "research_dataset_shadow" not in service_source
