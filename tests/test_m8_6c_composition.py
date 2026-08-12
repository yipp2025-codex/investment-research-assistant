"""M8.3 compatibility tests for Phase 6C calendar composition."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from app.market_calendar import MarketDateEvidence, MarketDayState, TwseMarketCalendar
from app.operations.scheduler import (
    MarketReadiness,
    SchedulerConfig,
    SchedulerRunner,
    TwseMarketReadinessProbe,
)
from app.providers import MarketDataBatch
from app.storage import SQLiteResearchRepository


TARGET = date(2026, 8, 6)
UTC = timezone.utc
TAIPEI = ZoneInfo("Asia/Taipei")


class _RecordingCalendar:
    market = "TWSE"
    timezone = TAIPEI

    def __init__(self) -> None:
        self.delegate = TwseMarketCalendar()
        self.calls: list[tuple[date, date | None]] = []
        self.evidence: list[MarketDateEvidence] = []

    def classify(
        self,
        requested_date: date,
        latest_published_date: date | None,
    ) -> MarketDateEvidence:
        self.calls.append((requested_date, latest_published_date))
        evidence = self.delegate.classify(requested_date, latest_published_date)
        self.evidence.append(evidence)
        return evidence


@dataclass
class _LatestDateProvider:
    latest_market_date: date | None

    def __post_init__(self) -> None:
        self.calls: list[tuple[str, date, date, float]] = []

    def fetch_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        timeout_seconds: float,
    ) -> MarketDataBatch:
        self.calls.append((symbol, start_date, end_date, timeout_seconds))
        prices = (
            ()
            if self.latest_market_date is None
            else ({"trade_date": self.latest_market_date.isoformat()},)
        )
        return MarketDataBatch(
            source="twse",
            symbol={"symbol": symbol},
            daily_prices=prices,
            company_metrics=(),
            market_date=self.latest_market_date,
        )


@pytest.mark.parametrize(
    ("latest", "expected_state", "expected_status"),
    [
        (None, MarketDayState.LATEST_UNAVAILABLE, MarketReadiness.DEFERRED),
        (
            date(2026, 8, 5),
            MarketDayState.LATEST_BEFORE_REQUESTED,
            MarketReadiness.DEFERRED,
        ),
        (TARGET, MarketDayState.LATEST_ON_REQUESTED, MarketReadiness.READY),
        (
            date(2026, 8, 7),
            MarketDayState.LATEST_AFTER_REQUESTED,
            MarketReadiness.READY,
        ),
    ],
    ids=("latest-none", "latest-before", "published-on", "published-after"),
)
def test_twse_readiness_maps_calendar_evidence_to_frozen_6c_status(
    latest: date | None,
    expected_state: MarketDayState,
    expected_status: str,
) -> None:
    provider = _LatestDateProvider(latest)
    calendar = _RecordingCalendar()
    probe = TwseMarketReadinessProbe(
        provider=provider,  # type: ignore[arg-type]
        market_calendar=calendar,
    )

    result = probe.check(TARGET, timeout_seconds=7.5)

    assert result.status == expected_status
    assert result.latest_market_date == latest
    assert calendar.calls == [(TARGET, latest)]
    assert calendar.evidence[-1].state is expected_state
    assert provider.calls == [
        ("2330", TARGET - timedelta(days=14), TARGET, 7.5)
    ]


@dataclass
class _CountingProbe:
    result: MarketReadiness

    def __post_init__(self) -> None:
        self.calls = 0

    def check(self, target_date: date, *, timeout_seconds: float) -> MarketReadiness:
        del target_date, timeout_seconds
        self.calls += 1
        return self.result


def _unexpected_child(*args, **kwargs):
    del args, kwargs
    raise AssertionError("Phase 6A child CLI must not run")


def test_scheduler_weekend_uses_calendar_and_never_calls_readiness_provider(
    tmp_path: Path,
) -> None:
    repository = SQLiteResearchRepository(tmp_path / "weekend.db")
    calendar = _RecordingCalendar()
    probe = _CountingProbe(
        MarketReadiness(MarketReadiness.READY, date(2026, 8, 8), "unexpected")
    )
    runner = SchedulerRunner(
        SchedulerConfig(
            provider="mock",
            project_root=Path.cwd(),
            database_path=repository.database_path,
        ),
        repository=repository,
        readiness_probe=probe,
        market_calendar=calendar,
        execute_cli=_unexpected_child,
        clock=lambda: datetime(2026, 8, 8, 10, 0, tzinfo=TAIPEI),
        sleep=lambda _: None,
    )

    result = runner.run_once(target_date=date(2026, 8, 8))

    assert result.operation_status == "skip"
    assert result.probe_attempts == 0
    assert probe.calls == 0
    assert calendar.calls == [(date(2026, 8, 8), None)]
    assert calendar.evidence[-1].state is MarketDayState.WEEKEND


@pytest.mark.parametrize(
    ("instant", "expected_date"),
    [
        (datetime(2026, 8, 5, 15, 59, 59, tzinfo=UTC), date(2026, 8, 5)),
        (datetime(2026, 8, 5, 16, 0, 0, tzinfo=UTC), date(2026, 8, 6)),
    ],
    ids=("before-taipei-midnight", "at-taipei-midnight"),
)
def test_scheduler_requested_date_uses_taipei_boundary_from_utc(
    tmp_path: Path,
    instant: datetime,
    expected_date: date,
) -> None:
    repository = SQLiteResearchRepository(
        tmp_path / f"utc-boundary-{expected_date.isoformat()}.db"
    )
    probe = _CountingProbe(
        MarketReadiness(
            MarketReadiness.PERMANENT_FAILURE,
            None,
            "stop after requested-date resolution",
        )
    )
    runner = SchedulerRunner(
        SchedulerConfig(
            provider="mock",
            project_root=Path.cwd(),
            database_path=repository.database_path,
        ),
        repository=repository,
        readiness_probe=probe,
        execute_cli=_unexpected_child,
        clock=lambda: instant,
        sleep=lambda _: None,
    )

    result = runner.run_once(trigger="manual")

    assert result.requested_date == expected_date
    assert result.operation_status == "hard_failure"
    assert probe.calls == 1


class _AdvancingUtcClock:
    def __init__(self, value: datetime) -> None:
        self.value = value
        self.sleeps: list[float] = []

    def __call__(self) -> datetime:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += timedelta(seconds=seconds)


def test_scheduled_deadline_is_taipei_wall_clock_plus_grace_from_utc(
    tmp_path: Path,
) -> None:
    repository = SQLiteResearchRepository(tmp_path / "scheduled-taipei.db")
    clock = _AdvancingUtcClock(datetime(2026, 8, 6, 10, 29, 0, tzinfo=UTC))
    probe = _CountingProbe(
        MarketReadiness(
            MarketReadiness.DEFERRED,
            date(2026, 8, 5),
            "not published",
        )
    )
    runner = SchedulerRunner(
        SchedulerConfig(
            provider="mock",
            schedule_time="18:00",
            grace_period_seconds=30 * 60,
            max_deferred_attempts=3,
            retry_backoff_seconds=120,
            project_root=Path.cwd(),
            database_path=repository.database_path,
        ),
        repository=repository,
        readiness_probe=probe,
        execute_cli=_unexpected_child,
        clock=clock,
        sleep=clock.sleep,
    )

    result = runner.run_once(trigger="scheduled", target_date=TARGET)
    invocation = runner.operations.get_invocation(result.invocation_id)

    assert result.operation_status == "deferred"
    assert result.probe_attempts == 2
    assert probe.calls == 2
    assert clock.sleeps == [60.0]
    assert invocation is not None
    assert invocation.scheduled_for == datetime(2026, 8, 6, 18, 0, tzinfo=TAIPEI)
    assert invocation.scheduled_for.astimezone(UTC) == datetime(
        2026, 8, 6, 10, 0, tzinfo=UTC
    )
