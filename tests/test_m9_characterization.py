"""M9.0 characterization freeze for Phase 6B read behavior.

These tests intentionally exercise the concrete SQLite repositories used by
``DailyResearchReportService`` before a read-only ``ResearchDataset`` exists.
They freeze both desirable behavior and the known E.SUN-canonical pollution
case; M9.0 does not correct either behavior.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from app.analysis import HistoricalResearchAnalyzer
from app.as_of_policy import FrozenAsOfPolicyV1
from app.models import CompanyMetric, DailyPrice, Symbol
from app.pipelines import HistoricalSyncPipeline, RetryPolicy
from app.providers import MockMarketDataProvider
from app.reports.daily_research import (
    CANONICAL_SCHEMA_VERSION,
    METHODOLOGY_VERSION,
    DailyResearchReportService,
)
from app.storage import (
    SQLiteCrossValidationRepository,
    SQLiteDailyReportRepository,
    SQLiteResearchRepository,
)
from app.storage.batch_run import SQLiteBatchRunRepository


UTC = timezone.utc
MARKET_DATE = date(2026, 8, 5)
REQUESTED_DATE = date(2026, 8, 6)
FIXED_NOW = datetime(2026, 8, 8, 3, 0, tzinfo=UTC)

FROZEN_PAYLOAD_SHA256 = (
    "8730b97cb0f744d0041f1ffde7cb4baf92848ed7318722326625c2f0e356ac64"
)
FROZEN_RESULT_ID = "daily-d7904f3bc10707ee001cdff49a2cd96b"
FROZEN_REPORT_ID = "report-daily-d7904f3bc10707ee001cdff49a2cd96b"
FROZEN_CANONICAL_JSON = (
    '{"comparison":{"changes":[],"previous_market_date":null,'
    '"previous_result_id":null,"status":"previous_result_missing"},'
    '"data_quality":{"discrepancies":[],"discrepancy_count":0,'
    '"latest_price_date":"2026-08-05","price_status":"available",'
    '"status":"warning","validation_outcome":null,'
    '"validation_run_id":null,"validation_status":"missing_source"},'
    '"highlights":[],"market_date":"2026-08-05","methodology":{'
    '"price_analyzer":"HistoricalResearchAnalyzer:frozen-v1",'
    '"threshold_version":"6b-thresholds-v1","thresholds":{'
    '"drawdown_percentage_points":1.0,"moving_average_percentage_points":1.0,'
    '"return_percentage_points":1.0,"volatility_percentage_points":1.0,'
    '"volume_ratio":0.25}},"methodology_version":"6b-daily-v1",'
    '"metrics":{"latest_close":{"as_of_date":"2026-08-05",'
    '"status":"available","unit":"TWD","value":105.0},'
    '"latest_volume":{"as_of_date":"2026-08-05","status":"available",'
    '"unit":"shares","value":1000},"ma_distance_120d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"ma_distance_20d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"ma_distance_60d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"max_drawdown_120d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"max_drawdown_20d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"max_drawdown_60d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"return_120d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"return_20d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"return_60d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"volatility_60d":{'
    '"as_of_date":null,"status":"insufficient_history",'
    '"unit":"percentage_points","value":null},"volume_anomaly":{'
    '"as_of_date":null,"status":"insufficient_history","unit":"state",'
    '"value":null},"volume_ratio_20d":{"as_of_date":null,'
    '"status":"insufficient_history","unit":"ratio","value":null}},'
    '"provenance":{"batch_run_id":null,"fetched_at":null,'
    '"historical_run_id":null,"pipeline_run_id":null,'
    '"price_sources":["twse-historical"],"source_endpoints":[],'
    '"symbol_run_id":null,"target_market_date":"2026-08-05",'
    '"validation":{},"validation_run_id":null},'
    '"requested_date":"2026-08-05","result_id":'
    '"daily-d7904f3bc10707ee001cdff49a2cd96b",'
    '"schema_version":"daily-research-result.v1","symbol":"2330",'
    '"valuation":{"dividend_yield":{"as_of_date":null,'
    '"status":"missing_source","unit":"percentage_points","value":null},'
    '"pb_ratio":{"as_of_date":null,"status":"missing_source",'
    '"unit":"ratio","value":null},"pe_ratio":{'
    '"as_of_date":"2026-08-04","status":"available","unit":"ratio",'
    '"value":20.0}}}'
)

FROZEN_TEN_DAY_RESULT_IDS = (
    "daily-0e0a8e02bebf5f51d4a363b1892811ad",
    "daily-5269070261f63bbdde83d64dfec014a9",
    "daily-f5f97bd81630025f897e6721376bb260",
    "daily-7424978d04758da3e3db4c9b7383e09b",
    "daily-ee2dbf3c33c468364b311ce8515ec9f5",
    "daily-d5246f7bd4faec0c87740037221c77f1",
    "daily-4add6bc23a006e995baf47481a539cb3",
    "daily-99a4660d244018555520f587eebc7738",
    "daily-da9d7171142db57441de16dafb4dc83d",
    "daily-e195c2c613784d837e5501797350ba07",
)


def _price(
    symbol: str,
    trade_date: date,
    close: float,
    *,
    source: str = "twse-historical",
    volume: int = 1_000,
) -> DailyPrice:
    return DailyPrice(
        symbol=symbol,
        trade_date=trade_date,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=volume,
        source=source,
    )


def _metric(
    symbol: str,
    metric_date: date,
    value: float,
    *,
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


def _seed_result(
    repository: SQLiteDailyReportRepository,
    *,
    result_id: str,
    market_date: date,
    methodology_version: str = METHODOLOGY_VERSION,
    payload: dict[str, object] | None = None,
) -> object:
    payload_json = json.dumps(
        payload or {},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return repository.save_or_get_result(
        result_id=result_id,
        symbol="2330",
        market_date=market_date,
        requested_date=market_date,
        methodology_version=methodology_version,
        schema_version=CANONICAL_SCHEMA_VERSION,
        data_quality_status="clean",
        payload_json=payload_json,
        payload_sha256=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        provenance_json="{}",
        created_at=datetime(
            market_date.year,
            market_date.month,
            market_date.day,
            12,
            tzinfo=UTC,
        ),
    )


def _seed_pipeline_run(
    repository: SQLiteResearchRepository,
    *,
    run_id: str,
    symbol: str,
    target_date: date,
    endpoint: str,
    created_at: datetime,
) -> None:
    timestamp = created_at.isoformat()
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO pipeline_runs ("
            "run_id, symbol, target_date, requested_start_date, "
            "requested_end_date, status, provider, attempt_count, "
            "source_endpoints_json, fetched_at, market_date, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, 'pending', 'twse', 0, ?, ?, ?, ?, ?)",
            (
                run_id,
                symbol,
                target_date.isoformat(),
                (target_date - timedelta(days=7)).isoformat(),
                target_date.isoformat(),
                json.dumps([endpoint], separators=(",", ":")),
                created_at.isoformat(),
                target_date.isoformat(),
                timestamp,
                timestamp,
            ),
        )


def _seed_validation_run(
    repository: SQLiteResearchRepository,
    *,
    run_id: str,
    target_date: date,
    right_provider: str,
    created_at: datetime,
    discrepancies: tuple[tuple[str, str, str, str], ...] = (),
) -> None:
    outcome = "discrepancy" if discrepancies else "match"
    started_at = created_at + timedelta(minutes=1)
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
                started_at.isoformat(),
                finished_at.isoformat(),
                finished_at.isoformat(),
            ),
        )
        # Deliberately insert right before left. Repository reads must return
        # left-provider first, independent of AUTOINCREMENT order.
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


@dataclass(frozen=True)
class _ParityFixture:
    database_path: Path
    repository: SQLiteResearchRepository
    report_repository: SQLiteDailyReportRepository
    cross_repository: SQLiteCrossValidationRepository
    service: DailyResearchReportService


@pytest.fixture
def m9_parity_fixture(tmp_path: Path) -> _ParityFixture:
    database_path = tmp_path / "m9-parity.db"
    repository = SQLiteResearchRepository(database_path)
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()
    cross_repository = SQLiteCrossValidationRepository(repository)

    for symbol in ("2330", "2317", "2454", "6505", "2882"):
        repository.upsert_symbol(
            Symbol(symbol, f"Parity {symbol}", "TWSE", "TWD")
        )

    # Input order is intentionally not chronological. The future rows are
    # present in SQLite but must not reach the analyzer or canonical output.
    repository.upsert_daily_prices(
        [
            _price("2330", date(2026, 8, 6), 999.0, source="esun"),
            _price("2330", MARKET_DATE, 105.0),
            _price("2330", date(2026, 7, 31), 98.0, source="twse"),
            _price("2330", date(2026, 8, 4), 103.0, source="twse"),
            _price("2330", date(2026, 8, 1), 100.0),
            _price("2317", date(2026, 8, 6), 999.0, source="twse"),
            _price("2317", date(2026, 8, 4), 50.0, source="twse"),
            _price("6505", MARKET_DATE, 70.0, source="twse"),
            # Known pre-M9 pollution case: E.SUN occupies a canonical row.
            _price("2882", MARKET_DATE, 60.0, source="esun"),
        ]
    )
    repository.upsert_company_metrics(
        [
            _metric("2330", date(2026, 8, 6), 99.0, source="esun"),
            _metric("2330", date(2026, 7, 31), 18.0),
            _metric("2330", date(2026, 8, 4), 20.0),
            _metric("2882", MARKET_DATE, 15.0, source="esun"),
        ]
    )

    _seed_result(
        report_repository,
        result_id="result-same-method-older",
        market_date=date(2026, 7, 25),
    )
    _seed_result(
        report_repository,
        result_id="result-same-method-gap",
        market_date=date(2026, 8, 1),
        payload={
            "data_quality": {
                "status": "warning",
                "validation_status": "source_discrepancy",
                "discrepancies": [
                    {"field": "close", "reason": "close differs"},
                    {"field": "volume", "reason": "volume differs"},
                ],
            },
            "metrics": {},
            "valuation": {},
        },
    )
    _seed_result(
        report_repository,
        result_id="result-newer-other-method",
        market_date=date(2026, 8, 4),
        methodology_version="old-method-v0",
    )
    _seed_result(
        report_repository,
        result_id="result-future",
        market_date=date(2026, 8, 6),
    )

    _seed_pipeline_run(
        repository,
        run_id="pipeline-main",
        symbol="2330",
        target_date=MARKET_DATE,
        endpoint="https://evidence.invalid/pipeline-main",
        created_at=datetime(2026, 8, 5, 8, tzinfo=UTC),
    )
    _seed_pipeline_run(
        repository,
        run_id="pipeline-future",
        symbol="2330",
        target_date=date(2026, 8, 6),
        endpoint="https://evidence.invalid/pipeline-future",
        created_at=datetime(2026, 8, 6, 8, tzinfo=UTC),
    )

    _seed_validation_run(
        repository,
        run_id="validation-old",
        target_date=MARKET_DATE,
        right_provider="esun-historical",
        created_at=datetime(2026, 8, 5, 9, tzinfo=UTC),
    )
    _seed_validation_run(
        repository,
        run_id="validation-new",
        target_date=MARKET_DATE,
        right_provider="esun",
        created_at=datetime(2026, 8, 5, 10, tzinfo=UTC),
        # Deliberately reverse lexical order; repository reads sort by field.
        discrepancies=(
            ("volume", "1000", "1100", "volume differs"),
            ("close", "105", "105.5", "close differs"),
        ),
    )
    _seed_validation_run(
        repository,
        run_id="validation-future",
        target_date=date(2026, 8, 6),
        right_provider="esun",
        created_at=datetime(2026, 8, 6, 11, tzinfo=UTC),
    )

    service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        cross_validation_repository=cross_repository,
        clock=lambda: FIXED_NOW,
    )
    return _ParityFixture(
        database_path,
        repository,
        report_repository,
        cross_repository,
        service,
    )


@dataclass(frozen=True)
class _DatabaseSnapshot:
    file_sha256: str
    file_size: int
    application_schema_version: int
    sqlite_schema_version: int
    user_version: int
    schema_sha256: str
    row_counts: tuple[tuple[str, int], ...]
    integrity_check: tuple[str, ...]
    foreign_key_violations: tuple[tuple[object, ...], ...]


def _database_snapshot(database_path: Path) -> _DatabaseSnapshot:
    uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
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
        schema_rows = connection.execute(
            "SELECT type, name, tbl_name, COALESCE(sql, '') FROM sqlite_master "
            "ORDER BY type, name"
        ).fetchall()
        schema_json = json.dumps(schema_rows, separators=(",", ":"))
        application_schema_version = int(
            connection.execute(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
            ).fetchone()[0]
        )
        sqlite_schema_version = int(
            connection.execute("PRAGMA schema_version").fetchone()[0]
        )
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        integrity_check = tuple(
            row[0] for row in connection.execute("PRAGMA integrity_check")
        )
        foreign_key_violations = tuple(
            tuple(row) for row in connection.execute("PRAGMA foreign_key_check")
        )
    raw = database_path.read_bytes()
    return _DatabaseSnapshot(
        file_sha256=hashlib.sha256(raw).hexdigest(),
        file_size=len(raw),
        application_schema_version=application_schema_version,
        sqlite_schema_version=sqlite_schema_version,
        user_version=user_version,
        schema_sha256=hashlib.sha256(schema_json.encode("utf-8")).hexdigest(),
        row_counts=row_counts,
        integrity_check=integrity_check,
        foreign_key_violations=foreign_key_violations,
    )


class _RecordingAnalyzer:
    def __init__(self) -> None:
        self.delegate = HistoricalResearchAnalyzer()
        self.price_batches: list[tuple[tuple[date, float, str], ...]] = []

    def analyze(self, prices):
        self.price_batches.append(
            tuple((item.trade_date, item.close, item.source) for item in prices)
        )
        return self.delegate.analyze(prices)

    def __getattr__(self, name: str) -> object:
        return getattr(self.delegate, name)


def _build_payload(
    service: DailyResearchReportService,
    symbol: str,
    *,
    pipeline_run_id: str | None = None,
    validation_run_id: str | None = None,
) -> dict[str, object]:
    return service.build_payload(
        symbol,
        MARKET_DATE,
        requested_date=MARKET_DATE,
        pipeline_run_id=pipeline_run_id,
        validation_run_id=validation_run_id,
    )


def test_fixed_parity_fixture_freezes_as_of_reads_and_is_read_only(
    m9_parity_fixture: _ParityFixture,
) -> None:
    fixture = m9_parity_fixture
    before = _database_snapshot(fixture.database_path)
    analyzer = _RecordingAnalyzer()
    service = DailyResearchReportService(
        fixture.repository,
        report_repository=fixture.report_repository,
        cross_validation_repository=fixture.cross_repository,
        analyzer=analyzer,  # type: ignore[arg-type]
        clock=lambda: FIXED_NOW,
    )

    available = _build_payload(service, "2330")
    mismatch = _build_payload(service, "2317")
    missing = _build_payload(service, "2454")
    insufficient = _build_payload(service, "6505")
    after = _database_snapshot(fixture.database_path)

    assert before == after
    assert before.application_schema_version == 10
    assert before.integrity_check == ("ok",)
    assert before.foreign_key_violations == ()
    counts = dict(before.row_counts)
    assert {
        name: counts[name]
        for name in (
            "symbols",
            "daily_prices",
            "company_metrics",
            "pipeline_runs",
            "market_data_validation_runs",
            "market_data_observations",
            "market_data_discrepancies",
            "daily_research_results",
            "daily_research_reports",
        )
    } == {
        "symbols": 5,
        "daily_prices": 9,
        "company_metrics": 4,
        "pipeline_runs": 2,
        "market_data_validation_runs": 3,
        "market_data_observations": 6,
        "market_data_discrepancies": 2,
        "daily_research_results": 4,
        "daily_research_reports": 0,
    }

    assert analyzer.price_batches == [
        (
            (date(2026, 7, 31), 98.0, "twse"),
            (date(2026, 8, 1), 100.0, "twse-historical"),
            (date(2026, 8, 4), 103.0, "twse"),
            (MARKET_DATE, 105.0, "twse-historical"),
        ),
        ((date(2026, 8, 4), 50.0, "twse"),),
        ((MARKET_DATE, 70.0, "twse"),),
    ]
    assert [
        item.trade_date
        for item in fixture.repository.list_daily_prices(
            "2330", end_date=MARKET_DATE
        )
    ] == [date(2026, 7, 31), date(2026, 8, 1), date(2026, 8, 4), MARKET_DATE]

    assert available["data_quality"] == {
        "status": "warning",
        "price_status": "available",
        "validation_status": "source_discrepancy",
        "validation_run_id": "validation-new",
        "validation_outcome": "discrepancy",
        "latest_price_date": "2026-08-05",
        "discrepancy_count": 2,
        "discrepancies": [
            {
                "field": "close",
                "reason": "close differs",
                "left_value": "105",
                "right_value": "105.5",
            },
            {
                "field": "volume",
                "reason": "volume differs",
                "left_value": "1000",
                "right_value": "1100",
            },
        ],
    }
    assert available["metrics"]["latest_close"] == {
        "status": "available",
        "value": 105.0,
        "unit": "TWD",
        "as_of_date": "2026-08-05",
    }
    assert available["metrics"]["return_20d"]["status"] == "insufficient_history"
    assert available["valuation"]["pe_ratio"] == {
        "status": "available",
        "value": 20.0,
        "unit": "ratio",
        "as_of_date": "2026-08-04",
    }
    assert available["comparison"] == {
        "status": "available",
        "previous_result_id": "result-same-method-gap",
        "previous_market_date": "2026-08-01",
        "changes": [],
    }
    assert available["provenance"]["price_sources"] == [
        "twse",
        "twse-historical",
    ]
    assert available["provenance"]["pipeline_run_id"] == "pipeline-main"
    assert available["provenance"]["validation_run_id"] == "validation-new"
    assert [
        item["provider"]
        for item in available["provenance"]["validation"]["observations"]
    ] == ["twse", "esun"]
    assert "pipeline-future" not in json.dumps(available)
    assert "validation-future" not in json.dumps(available)
    assert "result-future" not in json.dumps(available)
    assert 999.0 not in {
        item.get("value")
        for section in (available["metrics"], available["valuation"])
        for item in section.values()
    }

    assert mismatch["data_quality"]["price_status"] == "market_date_mismatch"
    assert mismatch["data_quality"]["latest_price_date"] == "2026-08-04"
    assert mismatch["metrics"]["latest_close"]["value"] is None
    assert missing["data_quality"]["price_status"] == "missing_source"
    assert missing["data_quality"]["status"] == "unavailable"
    assert insufficient["data_quality"]["price_status"] == "available"
    assert insufficient["metrics"]["latest_close"]["status"] == "available"
    assert insufficient["metrics"]["return_20d"]["status"] == (
        "insufficient_history"
    )


@dataclass(frozen=True)
class _RecordedCall:
    owner: str
    method: str
    args: tuple[object, ...]
    kwargs: dict[str, object]

    @property
    def name(self) -> str:
        return f"{self.owner}.{self.method}"


class _RecordingProxy:
    def __init__(
        self,
        owner: str,
        target: object,
        calls: list[_RecordedCall],
    ) -> None:
        self._owner = owner
        self._target = target
        self._calls = calls

    def __getattr__(self, name: str) -> object:
        value = getattr(self._target, name)
        if not callable(value) or name.startswith("_"):
            return value

        def recorded(*args: object, **kwargs: object) -> object:
            self._calls.append(
                _RecordedCall(self._owner, name, tuple(args), dict(kwargs))
            )
            return value(*args, **kwargs)

        return recorded


def test_daily_service_repository_call_order_and_required_parameters(
    m9_parity_fixture: _ParityFixture,
) -> None:
    fixture = m9_parity_fixture
    calls: list[_RecordedCall] = []
    research = _RecordingProxy("research", fixture.repository, calls)
    reports = _RecordingProxy("report", fixture.report_repository, calls)
    validation = _RecordingProxy("validation", fixture.cross_repository, calls)
    analyzer = _RecordingAnalyzer()
    service = DailyResearchReportService(
        research,  # type: ignore[arg-type]
        report_repository=reports,  # type: ignore[arg-type]
        cross_validation_repository=validation,  # type: ignore[arg-type]
        analyzer=analyzer,  # type: ignore[arg-type]
        clock=lambda: FIXED_NOW,
    )

    generated = service.generate("2330", MARKET_DATE, requested_date=MARKET_DATE)

    assert [call.name for call in calls] == [
        "report.initialize",
        "report.get_result",
        "research.list_daily_prices",
        "research.list_company_metrics",
        "report.get_previous_successful_result",
        "report.get_previous_result_any_methodology",
        "validation.list_runs",
        "validation.list_discrepancies",
        "research.get_pipeline_run_for_target",
        "validation.list_observations",
        "report.save_or_get_result",
        "report.get_report",
        "report.save_report_rendered",
    ]
    by_name = {call.name: call for call in calls}
    assert by_name["report.get_result"].args == (
        "2330",
        MARKET_DATE,
        METHODOLOGY_VERSION,
    )
    assert by_name["research.list_daily_prices"].args == ("2330",)
    assert by_name["research.list_daily_prices"].kwargs == {
        "end_date": MARKET_DATE
    }
    assert by_name["research.list_company_metrics"].args == ("2330",)
    assert by_name["research.list_company_metrics"].kwargs == {
        "end_date": MARKET_DATE
    }
    assert by_name["report.get_previous_successful_result"].args == (
        "2330",
        MARKET_DATE,
        METHODOLOGY_VERSION,
    )
    assert by_name["report.get_previous_result_any_methodology"].args == (
        "2330",
        MARKET_DATE,
    )
    assert by_name["validation.list_runs"].args == ("2330",)
    assert by_name["validation.list_discrepancies"].args == ("validation-new",)
    assert by_name["research.get_pipeline_run_for_target"].args == (
        "2330",
        MARKET_DATE,
    )
    assert by_name["validation.list_observations"].args == ("validation-new",)
    persisted = by_name["report.save_or_get_result"].kwargs
    assert {
        key: persisted[key]
        for key in (
            "result_id",
            "symbol",
            "market_date",
            "requested_date",
            "methodology_version",
            "schema_version",
            "data_quality_status",
        )
    } == {
        "result_id": FROZEN_RESULT_ID,
        "symbol": "2330",
        "market_date": MARKET_DATE,
        "requested_date": MARKET_DATE,
        "methodology_version": METHODOLOGY_VERSION,
        "schema_version": CANONICAL_SCHEMA_VERSION,
        "data_quality_status": "warning",
    }
    assert by_name["report.get_report"].args == (
        "2330",
        MARKET_DATE,
        METHODOLOGY_VERSION,
    )
    assert (
        by_name["report.save_report_rendered"].kwargs["result"].result_id
        == FROZEN_RESULT_ID
    )
    assert analyzer.price_batches[0][-1] == (
        MARKET_DATE,
        105.0,
        "twse-historical",
    )
    assert generated.canonical.result_id == FROZEN_RESULT_ID
    assert "research.list_source_artifacts" not in by_name


def test_validation_selection_and_sql_row_order_are_frozen(
    m9_parity_fixture: _ParityFixture,
) -> None:
    cross = m9_parity_fixture.cross_repository

    assert [item.run_id for item in cross.list_runs("2330")] == [
        "validation-old",
        "validation-new",
        "validation-future",
    ]
    observations = cross.list_observations("validation-new")
    assert [(item.id, item.provider) for item in observations] == [
        (4, "twse"),
        (3, "esun"),
    ]
    discrepancies = cross.list_discrepancies("validation-new")
    assert [(item.id, item.field) for item in discrepancies] == [
        (2, "close"),
        (1, "volume"),
    ]

    implicit = _build_payload(m9_parity_fixture.service, "2330")
    inconsistent_explicit = _build_payload(
        m9_parity_fixture.service,
        "2330",
        validation_run_id="validation-future",
    )
    assert implicit["data_quality"]["validation_run_id"] == "validation-new"
    assert [
        item["field"] for item in implicit["data_quality"]["discrepancies"]
    ] == ["close", "volume"]
    assert inconsistent_explicit["data_quality"]["validation_status"] == (
        "missing_source"
    )
    assert inconsistent_explicit["provenance"]["validation"] == {}


def test_previous_result_sql_set_order_and_m8_policy_are_frozen(
    m9_parity_fixture: _ParityFixture,
) -> None:
    fixture = m9_parity_fixture
    selected = fixture.report_repository.get_previous_successful_result(
        "2330", MARKET_DATE, METHODOLOGY_VERSION
    )
    selected_any = fixture.report_repository.get_previous_result_any_methodology(
        "2330", MARKET_DATE
    )
    assert selected is not None
    assert selected.result_id == "result-same-method-gap"
    assert selected_any is not None
    assert selected_any.result_id == "result-newer-other-method"

    uri = f"file:{fixture.database_path.resolve().as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        same_method_set = [
            row[0]
            for row in connection.execute(
                "SELECT result_id FROM daily_research_results "
                "WHERE symbol = '2330' AND methodology_version = ? "
                "AND result_status = 'success' AND market_date < ? "
                "ORDER BY market_date DESC",
                (METHODOLOGY_VERSION, MARKET_DATE.isoformat()),
            )
        ]
        any_method_set = [
            row[0]
            for row in connection.execute(
                "SELECT result_id FROM daily_research_results "
                "WHERE symbol = '2330' AND result_status = 'success' "
                "AND market_date < ? ORDER BY market_date DESC",
                (MARKET_DATE.isoformat(),),
            )
        ]
    assert same_method_set == [
        "result-same-method-gap",
        "result-same-method-older",
    ]
    assert any_method_set == [
        "result-newer-other-method",
        "result-same-method-gap",
        "result-same-method-older",
    ]

    policy = FrozenAsOfPolicyV1()
    old_method = fixture.report_repository.get_result(
        "2330", date(2026, 8, 4), "old-method-v0"
    )
    future = fixture.report_repository.get_result(
        "2330", date(2026, 8, 6), METHODOLOGY_VERSION
    )
    assert old_method is not None
    assert future is not None
    assert policy.previous_result_is_comparable(
        selected,
        symbol="2330",
        target_date=MARKET_DATE,
        methodology_version=METHODOLOGY_VERSION,
    )
    assert not policy.previous_result_is_comparable(
        old_method,
        symbol="2330",
        target_date=MARKET_DATE,
        methodology_version=METHODOLOGY_VERSION,
    )
    assert not policy.previous_result_is_comparable(
        future,
        symbol="2330",
        target_date=MARKET_DATE,
        methodology_version=METHODOLOGY_VERSION,
    )
    assert policy.select_previous_successful_comparable(
        fixture.report_repository.list_results("2330", METHODOLOGY_VERSION),
        symbol="2330",
        target_date=MARKET_DATE,
        methodology_version=METHODOLOGY_VERSION,
    ).result_id == "result-same-method-gap"
    assert _build_payload(fixture.service, "2330")["comparison"] == {
        "status": "available",
        "previous_result_id": "result-same-method-gap",
        "previous_market_date": "2026-08-01",
        "changes": [],
    }


def test_pipeline_provenance_implicit_and_explicit_selection_are_frozen(
    m9_parity_fixture: _ParityFixture,
) -> None:
    fixture = m9_parity_fixture
    calls: list[_RecordedCall] = []
    repository = _RecordingProxy("research", fixture.repository, calls)
    service = DailyResearchReportService(
        repository,  # type: ignore[arg-type]
        report_repository=fixture.report_repository,
        cross_validation_repository=fixture.cross_repository,
        clock=lambda: FIXED_NOW,
    )

    implicit = _build_payload(service, "2330")
    explicit = _build_payload(service, "2330", pipeline_run_id="pipeline-future")

    assert implicit["provenance"]["pipeline_run_id"] == "pipeline-main"
    assert implicit["provenance"]["source_endpoints"] == [
        "https://evidence.invalid/esun",
        "https://evidence.invalid/pipeline-main",
        "https://evidence.invalid/twse",
    ]
    # Current 6B trusts an explicit run id without a target-date consistency
    # check. This is characterization evidence, not a desired M9 policy.
    assert explicit["provenance"]["pipeline_run_id"] == "pipeline-future"
    assert "https://evidence.invalid/pipeline-future" in explicit["provenance"][
        "source_endpoints"
    ]
    pipeline_calls = [
        call for call in calls if call.method.startswith("get_pipeline_run")
    ]
    assert [(call.method, call.args) for call in pipeline_calls] == [
        ("get_pipeline_run_for_target", ("2330", MARKET_DATE)),
        ("get_pipeline_run", ("pipeline-future",)),
    ]


def test_esun_canonical_pollution_currently_remains_visible(
    m9_parity_fixture: _ParityFixture,
) -> None:
    legacy_repository = _RecordingProxy(
        "legacy-research",
        m9_parity_fixture.repository,
        [],
    )
    legacy_service = DailyResearchReportService(
        legacy_repository,  # type: ignore[arg-type]
        report_repository=m9_parity_fixture.report_repository,
        cross_validation_repository=m9_parity_fixture.cross_repository,
        clock=lambda: FIXED_NOW,
    )
    payload = _build_payload(legacy_service, "2882")

    # This freezes the known pre-M9 behavior: 6B reads canonical tables and
    # does not independently reject a row merely because its source is E.SUN.
    assert payload["data_quality"]["price_status"] == "available"
    assert payload["metrics"]["latest_close"] == {
        "status": "available",
        "value": 60.0,
        "unit": "TWD",
        "as_of_date": "2026-08-05",
    }
    assert payload["valuation"]["pe_ratio"] == {
        "status": "available",
        "value": 15.0,
        "unit": "ratio",
        "as_of_date": "2026-08-05",
    }
    assert payload["provenance"]["price_sources"] == ["esun"]
    assert m9_parity_fixture.repository.get_symbol("2882").market == "TWSE"


def _fixed_baseline_service(
    database_path: Path,
) -> tuple[
    SQLiteResearchRepository,
    SQLiteDailyReportRepository,
    SQLiteCrossValidationRepository,
    DailyResearchReportService,
]:
    repository = SQLiteResearchRepository(database_path)
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()
    repository.upsert_symbol(Symbol("2330", "Test 2330", "TWSE", "TWD"))
    repository.upsert_daily_prices([_price("2330", MARKET_DATE, 105.0)])
    repository.upsert_company_metrics(
        [_metric("2330", date(2026, 8, 4), 20.0)]
    )
    cross_repository = SQLiteCrossValidationRepository(repository)
    service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        cross_validation_repository=cross_repository,
        clock=lambda: FIXED_NOW,
    )
    return repository, report_repository, cross_repository, service


def test_frozen_canonical_payload_hash_ids_and_replay_read_short_circuit(
    tmp_path: Path,
) -> None:
    repository, report_repository, cross_repository, service = (
        _fixed_baseline_service(tmp_path / "m9-canonical.db")
    )
    first = service.generate("2330", MARKET_DATE, requested_date=MARKET_DATE)

    assert first.canonical.payload == json.loads(FROZEN_CANONICAL_JSON)
    assert first.canonical.payload_json == FROZEN_CANONICAL_JSON
    assert first.canonical.payload_sha256 == FROZEN_PAYLOAD_SHA256
    assert hashlib.sha256(FROZEN_CANONICAL_JSON.encode("utf-8")).hexdigest() == (
        FROZEN_PAYLOAD_SHA256
    )
    assert first.canonical.result_id == FROZEN_RESULT_ID
    assert first.report.report_id == FROZEN_REPORT_ID
    assert report_repository.count_results("2330") == 1
    assert report_repository.count_reports("2330") == 1

    calls: list[_RecordedCall] = []
    research = _RecordingProxy("research", repository, calls)
    reports = _RecordingProxy("report", report_repository, calls)
    validation = _RecordingProxy("validation", cross_repository, calls)
    replay_service = DailyResearchReportService(
        research,  # type: ignore[arg-type]
        report_repository=reports,  # type: ignore[arg-type]
        cross_validation_repository=validation,  # type: ignore[arg-type]
        clock=lambda: FIXED_NOW,
    )
    replay = replay_service.generate(
        "2330", MARKET_DATE, requested_date=MARKET_DATE
    )

    assert [call.name for call in calls] == [
        "report.initialize",
        "report.get_result",
        "report.get_report",
    ]
    assert replay.idempotent_replay is True
    assert replay.canonical.result_id == first.canonical.result_id
    assert replay.report.report_id == first.report.report_id
    assert replay.canonical.payload_sha256 == first.canonical.payload_sha256
    assert report_repository.count_results("2330") == 1
    assert report_repository.count_reports("2330") == 1


def _seed_fixed_batch_context(repository: SQLiteResearchRepository) -> None:
    timestamp = datetime(2026, 8, 6, 12, tzinfo=UTC).isoformat()
    with repository._transaction() as connection:
        connection.execute(
            "INSERT INTO watchlists (watchlist_id, name) "
            "VALUES ('watchlist-m9', 'm9 batch parity')"
        )
        connection.execute(
            "INSERT INTO watchlist_revisions (revision_id, watchlist_id, created_at) "
            "VALUES ('revision-m9', 'watchlist-m9', ?)",
            (timestamp,),
        )
        connection.execute(
            "INSERT INTO watchlist_revision_members (revision_id, symbol) "
            "VALUES ('revision-m9', '2330')"
        )
        connection.execute(
            "INSERT INTO daily_batch_runs ("
            "batch_run_id, watchlist_revision_id, requested_date, "
            "resolved_market_date, runner_policy_version, status, total_symbols, "
            "success_symbols, failed_symbols, skipped_symbols, created_at, "
            "started_at, finished_at, updated_at) "
            "VALUES ('batch-m9', 'revision-m9', ?, ?, '6a-v1', 'success', "
            "1, 1, 0, 0, ?, ?, ?, ?)",
            (
                REQUESTED_DATE.isoformat(),
                MARKET_DATE.isoformat(),
                timestamp,
                timestamp,
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            "INSERT INTO daily_symbol_runs ("
            "symbol_run_id, batch_run_id, symbol, status, attempt_count, "
            "pipeline_run_id, created_at, started_at, finished_at, updated_at) "
            "VALUES ('symbol-run-m9', 'batch-m9', '2330', 'success', 1, "
            "'pipeline-batch', ?, ?, ?, ?)",
            (timestamp, timestamp, timestamp, timestamp),
        )


def test_batch_context_selection_and_repository_read_order(tmp_path: Path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "m9-batch.db")
    report_repository = SQLiteDailyReportRepository(repository)
    report_repository.initialize()
    repository.upsert_symbol(Symbol("2330", "Batch 2330", "TWSE", "TWD"))
    repository.upsert_daily_prices([_price("2330", MARKET_DATE, 105.0)])
    _seed_pipeline_run(
        repository,
        run_id="pipeline-batch",
        symbol="2330",
        target_date=MARKET_DATE,
        endpoint="https://evidence.invalid/pipeline-batch",
        created_at=datetime(2026, 8, 5, 8, tzinfo=UTC),
    )
    _seed_fixed_batch_context(repository)
    real_batch_repository = SQLiteBatchRunRepository(repository)
    calls: list[_RecordedCall] = []
    batch_repository = _RecordingProxy(
        "batch", real_batch_repository, calls
    )
    service = DailyResearchReportService(
        repository,
        report_repository=report_repository,
        clock=lambda: FIXED_NOW,
    )

    with patch(
        "app.reports.composition.SQLiteBatchRunRepository",
        return_value=batch_repository,
    ):
        generated = service.generate_for_batch_symbol("batch-m9", "2330")

    assert [(call.name, call.args) for call in calls] == [
        ("batch.initialize", ()),
        ("batch.get_batch_run", ("batch-m9",)),
        ("batch.get_symbol_run", ("batch-m9", "2330")),
    ]
    payload = generated.canonical.payload
    assert payload["requested_date"] == "2026-08-06"
    assert payload["market_date"] == "2026-08-05"
    assert payload["provenance"]["batch_run_id"] == "batch-m9"
    assert payload["provenance"]["symbol_run_id"] == "symbol-run-m9"
    assert payload["provenance"]["pipeline_run_id"] == "pipeline-batch"
    assert generated.canonical.result_id == FROZEN_RESULT_ID
    assert generated.report.report_id == FROZEN_REPORT_ID
    assert report_repository.count_results("2330") == 1
    assert report_repository.count_reports("2330") == 1


class _ProvenanceMockProvider(MockMarketDataProvider):
    """Existing deterministic mock values with complete historical evidence."""

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


def test_mock_synthetic_ten_day_e2e_results_reports_ids_and_row_counts(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "m9-ten-day.db"
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
        "2330", date(2026, 7, 31), target_observations=250
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
    second_pass = [
        service.generate(
            "2330",
            market_date,
            historical_run_id=historical_result.run_id,
        )
        for market_date in dates
    ]

    assert [item.canonical.result_id for item in first_pass] == list(
        FROZEN_TEN_DAY_RESULT_IDS
    )
    assert [item.report.report_id for item in first_pass] == [
        f"report-{result_id}" for result_id in FROZEN_TEN_DAY_RESULT_IDS
    ]
    assert all(item.report.report_status == "rendered" for item in first_pass)
    assert all(item.idempotent_replay for item in second_pass)
    assert [item.canonical.result_id for item in second_pass] == [
        item.canonical.result_id for item in first_pass
    ]
    assert [item.canonical.payload_sha256 for item in second_pass] == [
        item.canonical.payload_sha256 for item in first_pass
    ]
    assert first_pass[0].canonical.payload["comparison"]["status"] == (
        "previous_result_missing"
    )
    assert all(
        item.canonical.payload["comparison"]["status"] == "available"
        for item in first_pass[1:]
    )
    assert all(
        item.canonical.payload["provenance"]["price_sources"]
        == ["mock-synthetic"]
        for item in first_pass
    )
    assert all(
        item.canonical.payload["provenance"]["historical_run_id"]
        == historical_result.run_id
        for item in first_pass
    )
    assert {item.source for item in repository.list_daily_prices("2330")} == {
        "mock-synthetic"
    }

    with sqlite3.connect(database_path) as connection:
        expected_counts = {
            "symbols": 1,
            "daily_prices": 261,
            "company_metrics": 24,
            "historical_sync_runs": 1,
            "research_notes": 1,
            "source_artifacts": 12,
            "daily_research_results": 10,
            "daily_research_reports": 10,
        }
        assert {
            table: connection.execute(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            for table in expected_counts
        } == expected_counts
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
