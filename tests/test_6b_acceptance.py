"""Phase 6B acceptance: ten existing historical market dates and replay."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

from app.pipelines import HistoricalSyncPipeline, RetryPolicy
from app.providers import MockMarketDataProvider
from app.reports.daily_research import DailyResearchReportService
from app.storage import SQLiteDailyReportRepository, SQLiteResearchRepository
from app.models import Symbol


class _ProvenanceMockProvider(MockMarketDataProvider):
    """Reuse the existing deterministic Mock provider with valid provenance."""

    def fetch_market_data(self, symbol, start_date, end_date, *, timeout_seconds):
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
        fetched_at = datetime(2026, 8, 6, 12, tzinfo=timezone.utc)
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


def test_ten_existing_historical_dates_generate_and_replay(
    tmp_path: Path,
) -> None:
    repository = SQLiteResearchRepository(tmp_path / "ten-day.db")
    repository.initialize()
    repository.upsert_symbol(
        Symbol(symbol="2330", name="Synthetic 2330", market="TWSE", currency="TWD")
    )

    # Existing Phase 4 historical engine creates the canonical daily_prices
    # used by Phase 6B.  No Phase 6B code fabricates a second price series.
    historical = HistoricalSyncPipeline(
        _ProvenanceMockProvider(),
        repository,
        retry_policy=RetryPolicy(max_attempts=2, initial_backoff_seconds=0.0),
        sleep=lambda _: None,
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=timezone.utc),
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
        clock=lambda: datetime(2026, 8, 6, 12, tzinfo=timezone.utc),
    )
    dates = sorted(
        {
            item.trade_date
            for item in repository.list_daily_prices("2330")
        }
    )[-10:]
    assert len(dates) == 10
    assert all(left.weekday() < 5 for left in dates)

    first_pass = [
        service.generate(
            "2330",
            market_date,
            historical_run_id=historical_result.run_id,
        )
        for market_date in dates
    ]
    assert all(item.report.report_status == "rendered" for item in first_pass)
    assert report_repository.count_results("2330") == 10
    assert report_repository.count_reports("2330") == 10

    second_pass = [
        service.generate(
            "2330",
            market_date,
            historical_run_id=historical_result.run_id,
        )
        for market_date in dates
    ]
    assert all(item.idempotent_replay for item in second_pass)
    assert [
        item.canonical.result_id for item in second_pass
    ] == [item.canonical.result_id for item in first_pass]
    assert [
        item.canonical.payload_sha256 for item in second_pass
    ] == [item.canonical.payload_sha256 for item in first_pass]
    assert report_repository.count_results("2330") == 10
    assert report_repository.count_reports("2330") == 10

    # The report is change-only: it contains current state and changes, not
    # the complete historical price series.
    for item in first_pass:
        markdown = item.report.markdown or ""
        assert "## 今日重要變化" in markdown
        assert "## 精簡目前狀態" in markdown
        assert "historical_series" not in markdown
        assert "買進" not in markdown
        assert "賣出" not in markdown

    # At least the second report has a previous market-date comparison.
    assert first_pass[0].canonical.payload["comparison"]["status"] in {
        "previous_result_missing",
        "methodology_incompatible",
    }
    assert all(
        item.canonical.payload["comparison"]["status"] == "available"
        for item in first_pass[1:]
    )
