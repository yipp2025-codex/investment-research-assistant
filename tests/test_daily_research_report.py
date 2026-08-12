"""Phase 6B canonical result, comparison, and change-only report tests."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.analysis import HistoricalResearchAnalyzer
from app.models import (
    CompanyMetric,
    CrossValidationOutcome,
    DailyPrice,
    MarketDataDiscrepancy,
    PipelineRunStatus,
    Symbol,
)
from app.reports.daily_research import (
    CANONICAL_SCHEMA_VERSION,
    METHODOLOGY_VERSION,
    CanonicalSchemaError,
    DailyResearchReportService,
    render_daily_research_markdown,
    validate_canonical_payload,
)
from app.storage import SQLiteDailyReportRepository, SQLiteResearchRepository
from app.storage.daily_report import StoredDailyResearchReport


MARKET_DATE = date(2026, 8, 5)


def _weekday_dates(end: date, count: int) -> list[date]:
    values: list[date] = []
    current = end
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current -= timedelta(days=1)
    return list(reversed(values))


def _setup(tmp_path: Path) -> tuple[SQLiteResearchRepository, SQLiteDailyReportRepository]:
    repo = SQLiteResearchRepository(tmp_path / "reports.db")
    report_repo = SQLiteDailyReportRepository(repo)
    report_repo.initialize()
    repo.upsert_symbol(
        Symbol(symbol="2330", name="Test 2330", market="TWSE", currency="TWD")
    )
    return repo, report_repo


def _seed_prices(
    repo: SQLiteResearchRepository,
    *,
    end_date: date = MARKET_DATE,
    count: int = 250,
    close: float = 100.0,
    volume: int = 1_000,
    source: str = "twse-historical",
) -> list[date]:
    dates = _weekday_dates(end_date, count)
    repo.upsert_daily_prices(
        [
            DailyPrice(
                symbol="2330",
                trade_date=trade_date,
                open=close,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=volume,
                source=source,
            )
            for trade_date in dates
        ]
    )
    return dates


def _seed_valuation(repo: SQLiteResearchRepository) -> None:
    repo.upsert_company_metrics(
        [
            CompanyMetric(
                symbol="2330",
                metric_date=date(2026, 7, 31),
                name="price_earnings_ratio",
                value=18.0,
                unit="ratio",
                source="twse",
            ),
            CompanyMetric(
                symbol="2330",
                metric_date=date(2026, 8, 4),
                name="price_earnings_ratio",
                value=20.0,
                unit="ratio",
                source="twse",
            ),
            CompanyMetric(
                symbol="2330",
                metric_date=date(2026, 8, 6),
                name="price_earnings_ratio",
                value=99.0,
                unit="ratio",
                source="twse",
            ),
        ]
    )


class _DiscrepancyValidationRepository:
    def __init__(self, target_date: date, *, discrepancy: bool) -> None:
        from app.models import CrossValidationRun

        self.run = CrossValidationRun(
            run_id="validation-1",
            symbol="2330",
            target_date=target_date,
            requested_start_date=target_date - timedelta(days=2),
            left_provider="twse",
            right_provider="esun",
            status=PipelineRunStatus.SUCCESS,
            outcome=(
                CrossValidationOutcome.DISCREPANCY
                if discrepancy
                else CrossValidationOutcome.MATCH
            ),
            created_at=datetime(2026, 8, 5, 10, tzinfo=timezone.utc),
            started_at=datetime(2026, 8, 5, 10, tzinfo=timezone.utc),
            finished_at=datetime(2026, 8, 5, 10, 1, tzinfo=timezone.utc),
            error_message=None,
            attempt_count=1,
        )
        self.discrepancy = discrepancy

    def get_run(self, run_id: str):
        return self.run if run_id == self.run.run_id else None

    def list_runs(self, symbol: str):
        return [self.run] if symbol == self.run.symbol else []

    def list_discrepancies(self, run_id: str):
        if not self.discrepancy:
            return []
        return [
            MarketDataDiscrepancy(
                run_id=run_id,
                field="volume",
                left_value="1000",
                right_value="1100",
                reason="volume_definition_or_update_timing_unresolved",
                absolute_difference=100.0,
                relative_difference_pct=10.0,
            )
        ]

    def list_observations(self, run_id: str):
        return []


def _service(
    repo: SQLiteResearchRepository,
    report_repo: SQLiteDailyReportRepository,
    *,
    validation_repo=None,
    renderer=None,
    methodology_version: str = METHODOLOGY_VERSION,
) -> DailyResearchReportService:
    return DailyResearchReportService(
        repo,
        report_repository=report_repo,
        cross_validation_repository=validation_repo,
        renderer=renderer,
        methodology_version=methodology_version,
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=timezone.utc),
    )


class TestCanonicalDailyResult:
    def test_schema_v10_and_canonical_hash(self, tmp_path: Path) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo)
        _seed_valuation(repo)
        service = _service(repo, report_repo)

        generated = service.generate(
            "2330",
            MARKET_DATE,
            requested_date=MARKET_DATE,
            batch_run_id="batch-1",
            symbol_run_id="symbol-1",
        )
        payload = generated.canonical.payload
        validate_canonical_payload(payload)

        assert repo.get_schema_version() == 10
        assert payload["schema_version"] == CANONICAL_SCHEMA_VERSION
        assert payload["methodology_version"] == METHODOLOGY_VERSION
        assert generated.canonical.payload_sha256 == hashlib.sha256(
            generated.canonical.payload_json.encode("utf-8")
        ).hexdigest()
        assert generated.canonical.result_id.startswith("daily-")
        assert "prices" not in payload
        assert "historical_series" not in payload
        assert generated.report.report_status == "rendered"
        assert generated.canonical.provenance["batch_run_id"] == "batch-1"

    def test_metrics_reuse_frozen_historical_analyzer(self, tmp_path: Path) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo, count=250, close=100.0)
        service = _service(repo, report_repo)
        generated = service.generate("2330", MARKET_DATE)
        prices = repo.list_daily_prices("2330", end_date=MARKET_DATE)
        analysis = HistoricalResearchAnalyzer().analyze(prices)
        metrics = generated.canonical.payload["metrics"]

        for size in (20, 60, 120):
            window = analysis.window(size)
            assert metrics[f"return_{size}d"]["value"] == window.return_pct
            assert (
                metrics[f"max_drawdown_{size}d"]["value"]
                == window.max_drawdown_pct
            )
            assert (
                metrics[f"ma_distance_{size}d"]["value"]
                == window.distance_to_moving_average_pct
            )
        assert metrics["volatility_60d"]["value"] == analysis.daily_return_volatility_pct
        assert metrics["volume_ratio_20d"]["value"] == analysis.volume_ratio_to_20d

    def test_unavailable_fields_are_explicit(self, tmp_path: Path) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo, count=5)
        generated = _service(repo, report_repo).generate("2330", MARKET_DATE)
        metrics = generated.canonical.payload["metrics"]
        valuation = generated.canonical.payload["valuation"]

        for field in ("return_20d", "return_60d", "return_120d", "volatility_60d"):
            assert metrics[field]["status"] == "insufficient_history"
            assert metrics[field]["value"] is None
            assert metrics[field]["as_of_date"] is None
        assert valuation["pe_ratio"]["status"] == "missing_source"
        assert valuation["pe_ratio"]["value"] is None
        assert generated.canonical.payload["data_quality"]["status"] == "warning"

    def test_valuation_uses_latest_metric_on_or_before_market_date(
        self, tmp_path: Path
    ) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo)
        _seed_valuation(repo)
        payload = _service(repo, report_repo).generate(
            "2330", MARKET_DATE
        ).canonical.payload
        pe = payload["valuation"]["pe_ratio"]
        assert pe["value"] == 20.0
        assert pe["as_of_date"] == "2026-08-04"


class TestPreviousTradingDayAndMethodology:
    def test_previous_result_uses_previous_market_date_not_calendar_yesterday(
        self, tmp_path: Path
    ) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo, end_date=date(2026, 8, 5), count=250, close=100.0)
        service = _service(repo, report_repo)
        first = service.generate("2330", date(2026, 8, 3))
        second = service.generate("2330", date(2026, 8, 5))
        comparison = second.canonical.payload["comparison"]

        assert comparison["status"] == "available"
        assert comparison["previous_result_id"] == first.canonical.result_id
        assert comparison["previous_market_date"] == "2026-08-03"
        assert comparison["previous_market_date"] != "2026-08-04"

    def test_different_methodology_is_not_compared(self, tmp_path: Path) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo)
        old = _service(
            repo, report_repo, methodology_version="old-method-v0"
        ).generate("2330", date(2026, 8, 4))
        current = _service(repo, report_repo).generate("2330", MARKET_DATE)
        comparison = current.canonical.payload["comparison"]
        assert old.canonical.methodology_version == "old-method-v0"
        assert comparison["status"] == "methodology_incompatible"
        assert comparison["previous_result_id"] is None
        assert comparison["changes"] == []


class TestChangeOnlyReport:
    def test_discrepancy_is_warning_and_change_is_stable(self, tmp_path: Path) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo)
        validation = _DiscrepancyValidationRepository(
            MARKET_DATE, discrepancy=True
        )
        generated = _service(
            repo, report_repo, validation_repo=validation
        ).generate("2330", MARKET_DATE)
        payload = generated.canonical.payload
        assert payload["data_quality"]["status"] == "warning"
        assert payload["data_quality"]["validation_status"] == "source_discrepancy"
        assert "volume" in generated.report.markdown
        assert "買進" not in generated.report.markdown
        assert "賣出" not in generated.report.markdown

    def test_no_change_report_is_compact(self, tmp_path: Path) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo, count=250, close=100.0, volume=1_000)
        service = _service(repo, report_repo)
        first = service.generate("2330", date(2026, 8, 4))
        second = service.generate("2330", MARKET_DATE)
        assert "無達到 deterministic threshold 的變化。" in second.report.markdown
        assert len(second.report.markdown) < 2_000
        assert first.canonical.payload["comparison"]["changes"] == []
        assert second.canonical.payload["comparison"]["changes"] == []

    def test_threshold_change_is_reported_once_and_replayed(self, tmp_path: Path) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo, count=250, close=100.0, volume=1_000)
        service = _service(repo, report_repo)
        service.generate("2330", date(2026, 8, 4))
        # Make the newest observation an observable volume-state change.
        repo.upsert_daily_prices(
            [
                DailyPrice(
                    symbol="2330",
                    trade_date=MARKET_DATE,
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.0,
                    volume=3_000,
                    source="twse-historical",
                )
            ]
        )
        changed = service.generate("2330", MARKET_DATE)
        replay = service.generate("2330", MARKET_DATE)
        changes = changed.canonical.payload["comparison"]["changes"]
        assert any(item["kind"] == "volume_anomaly_state_changed" for item in changes)
        assert replay.idempotent_replay is True
        assert replay.canonical.payload_sha256 == changed.canonical.payload_sha256
        assert replay.canonical.payload["comparison"]["changes"] == changes
        assert report_repo.count_results("2330") == 2
        assert report_repo.count_reports("2330") == 2


class TestRenderRetry:
    def test_render_failure_preserves_canonical_and_retries_only_render(
        self, tmp_path: Path
    ) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo)
        attempts = {"count": 0}

        def failing_renderer(payload):
            attempts["count"] += 1
            raise RuntimeError("renderer unavailable")

        failed = _service(
            repo, report_repo, renderer=failing_renderer
        ).generate("2330", MARKET_DATE)
        assert failed.canonical.result_status == "success"
        assert failed.report.report_status == "failed"
        assert report_repo.count_results("2330") == 1
        assert report_repo.count_reports("2330") == 1

        recovered = _service(repo, report_repo).generate("2330", MARKET_DATE)
        assert recovered.idempotent_replay is True
        assert recovered.canonical.payload_sha256 == failed.canonical.payload_sha256
        assert recovered.report.report_status == "rendered"
        assert attempts["count"] == 1
        assert report_repo.count_results("2330") == 1
        assert report_repo.count_reports("2330") == 1


class TestCanonicalValidator:
    def test_unavailable_value_cannot_use_zero_as_sentinel(self) -> None:
        with pytest.raises(CanonicalSchemaError):
            from app.reports.daily_research import _validate_metric_value

            _validate_metric_value(
                "return_20d",
                {
                    "status": "insufficient_history",
                    "value": 0,
                    "unit": "percentage_points",
                    "as_of_date": None,
                },
            )

    def test_report_has_required_sections(self, tmp_path: Path) -> None:
        repo, report_repo = _setup(tmp_path)
        _seed_prices(repo)
        generated = _service(repo, report_repo).generate("2330", MARKET_DATE)
        for heading in (
            "## 執行與資料品質",
            "## 今日重要變化",
            "## Discrepancy / missing-data 警告",
            "## 精簡目前狀態",
        ):
            assert heading in generated.report.markdown
