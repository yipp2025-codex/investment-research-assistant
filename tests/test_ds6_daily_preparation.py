from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import hashlib
from pathlib import Path
import sqlite3
from typing import Callable

import pytest

from app.data_contracts.dataset_persistence import DatasetArtifactRef
from app.data_contracts.dual_source import FailureClassification
from app.data_contracts.supplemental_eligibility import (
    InstrumentIdentity,
    OHLCVObservation,
    SecurityBoundaryEvidence,
    TwseFailureEvidence,
)
from app.deployment.ds6_daily_preparation import (
    DS6Configuration,
    DS6DailyPreparationCoordinator,
    DS6PreparationError,
    DS6SourcePreparation,
    ExplicitDatasetVersionConsumer,
)
from app.providers.base import ProviderInvalidPayloadError
from app.reporting.screener_report import REPORT_CONTRACT_VERSION_V2, ScreenerReportGenerator
from app.screener.universe import (
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UNIVERSE_METHODOLOGY_VERSION,
    UniverseMemberStatus,
)
from app.storage import SQLiteResearchRepository
from app.storage.candidate_persistence import CandidateInputLocator, SQLiteScreenerCheckpointRepository
from app.storage.dataset_versions import (
    DatasetMigrationStateError,
    DatasetVersionMigrationRunner,
    DatasetVersionRepository,
)
from app.storage.screener_replay import SQLiteScreenerReplayRepository
from app.storage.screener_migration import SQLiteScreenerMigrationRunner
from app.storage.universe_persistence import SQLiteMarketUniverseRepository


BASE_DATE = date(2026, 1, 1)
TARGET_DATE = BASE_DATE + timedelta(days=249)
UTC = timezone.utc


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _database(tmp_path: Path) -> Path:
    database = tmp_path / "ds6-v12.db"
    SQLiteResearchRepository(database).initialize()
    with sqlite3.connect(database) as connection:
        connection.executemany(
            "INSERT INTO symbols (symbol, name, market, currency, is_active) "
            "VALUES (?, ?, 'TSE', 'TWD', 1)",
            [
                (symbol, f"Company {symbol}")
                for symbol in ("3044", "6108", "4590", "6589", "7740")
            ],
        )
    SQLiteScreenerMigrationRunner(database).migrate()
    DatasetVersionMigrationRunner(database).migrate(isolated=True)
    return database


def _identity(symbol: str) -> InstrumentIdentity:
    return InstrumentIdentity(
        symbol=symbol,
        market="TSE",
        exchange="TWSE",
        currency="TWD",
        security_type="EQUITY",
    )


def _artifact(provider: str, symbol: str, label: str) -> DatasetArtifactRef:
    return DatasetArtifactRef(
        ordinal=1,
        provider=provider,
        dataset=f"ds6-{label}",
        source_ref=f"https://example.invalid/ds6/{provider}/{symbol}/{label}",
        contract_version="ds6-fixture-v1",
        payload_sha256=_hash(f"artifact|{provider}|{symbol}|{label}"),
        payload_size_bytes=128,
        hash_basis="canonical-json-v1",
    )


def _raw(symbol: str, offset: int, *, provider: str = "twse", volume_delta: int = 0) -> OHLCVObservation:
    close = 100.0
    if offset == 248:
        close = 100.0
    elif offset == 249:
        close = 104.0
    return OHLCVObservation(
        trade_date=BASE_DATE + timedelta(days=offset),
        open=100.0,
        high=max(105.0, close),
        low=95.0,
        close=close,
        volume=1000 + offset + volume_delta,
        symbol=symbol,
    )


def _universe(symbols: tuple[str, ...]) -> MarketUniverseSnapshot:
    evidence = (
        SourceEvidence(
            source="twse",
            dataset="ds6-universe",
            source_ref="contract://ds6/universe",
            contract_version="ds6-universe-v1",
            payload_sha256=_hash("ds6-universe"),
            payload_size_bytes=12,
            hash_basis="canonical-json-v1",
        ),
    )
    members = tuple(
        MarketUniverseMember(
            symbol=symbol,
            name=f"Company {symbol}",
            market="TWSE",
            status=UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
            listing_date=date(2000, 1, 1),
            delisting_date=None,
            exclusion_reason=None,
            source_evidence=evidence,
        )
        for symbol in sorted(symbols)
    )
    return MarketUniverseSnapshot(
        market_date=TARGET_DATE,
        methodology_version=UNIVERSE_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=len(members),
        scan_eligible_count=len(members),
        scan_unavailable_count=0,
        excluded_count=0,
        inactive_count=0,
        unresolved_count=0,
        members=members,
    )


class _Source:
    def __init__(self, plans: dict[str, list[DS6SourcePreparation]]) -> None:
        self.plans = {symbol: list(values) for symbol, values in plans.items()}
        self.calls: list[tuple[str, tuple[date, ...] | None, object | None]] = []

    def prepare(
        self,
        symbol: str,
        target_date: date,
        *,
        target_observation_count: int,
        requested_dates: tuple[date, ...] | None,
        resume_state: object | None,
    ) -> DS6SourcePreparation:
        del target_observation_count
        self.calls.append((symbol, requested_dates, resume_state))
        values = self.plans.get(symbol)
        if not values:
            raise ProviderInvalidPayloadError(f"no fixture for {symbol}")
        result = values.pop(0)
        if result.target_date != target_date:
            raise AssertionError("fixture target date mismatch")
        return result


def _twse_complete(symbol: str, *, provider: str = "twse-historical") -> DS6SourcePreparation:
    dates = tuple(BASE_DATE + timedelta(days=offset) for offset in range(250))
    return DS6SourcePreparation(
        provider=provider,
        symbol=symbol,
        target_date=TARGET_DATE,
        requested_dates=dates,
        observations=tuple(_raw(symbol, offset) for offset in range(250)),
        source_run_id=f"twse-run-{symbol}",
        artifact=_artifact(provider, symbol, "twse"),
        status="complete",
        failure_evidence=TwseFailureEvidence(
            endpoint_identity_verified=True,
            classification_basis="complete-fixture",
        ),
    )


def _twse_incomplete(symbol: str, *, available: int = 228) -> DS6SourcePreparation:
    dates = tuple(BASE_DATE + timedelta(days=offset) for offset in range(250))
    return DS6SourcePreparation(
        provider="twse-historical",
        symbol=symbol,
        target_date=TARGET_DATE,
        requested_dates=dates,
        observations=tuple(_raw(symbol, offset) for offset in range(available)),
        source_run_id=f"twse-run-{symbol}",
        artifact=_artifact("twse-historical", symbol, "twse-incomplete"),
        status="incomplete",
        failure_class=FailureClassification.PACING_BLOCKED,
        failure_evidence=TwseFailureEvidence(
            endpoint_identity_verified=True,
            classification_basis="official-pacing",
            bounded_pacing_exhausted=True,
        ),
    )


def _esun_supplemental(symbol: str, *, count: int = 22, volume_delta: int = 0) -> DS6SourcePreparation:
    dates = tuple(BASE_DATE + timedelta(days=offset) for offset in range(228, 250))
    return DS6SourcePreparation(
        provider="esun-historical",
        symbol=symbol,
        target_date=TARGET_DATE,
        requested_dates=dates,
        observations=tuple(
            _raw(
                symbol,
                offset,
                volume_delta=volume_delta if offset == 249 else 0,
            )
            for offset in range(228, 228 + count)
        ),
        source_run_id=f"esun-run-{symbol}",
        artifact=_artifact("esun-historical", symbol, "esun-supplemental"),
        status="complete" if count == 22 else "incomplete",
        requested_identity=_identity(symbol),
        returned_identity=_identity(symbol),
        security_boundary=SecurityBoundaryEvidence.passed(),
    )


def _esun_validation(
    symbol: str,
    *,
    failure: FailureClassification | None = None,
    returned_symbol: str | None = None,
    volume_delta: int = 0,
) -> DS6SourcePreparation:
    dates = tuple(BASE_DATE + timedelta(days=offset) for offset in range(250))
    return DS6SourcePreparation(
        provider="esun-historical",
        symbol=symbol,
        target_date=TARGET_DATE,
        requested_dates=dates,
        observations=(
            ()
            if failure is not None
            else tuple(
                _raw(
                    symbol,
                    offset,
                    volume_delta=volume_delta if offset == 249 else 0,
                )
                for offset in range(250)
            )
        ),
        source_run_id=f"esun-validation-{symbol}",
        artifact=_artifact("esun-historical", symbol, "esun-validation"),
        status="failed" if failure is not None else "complete",
        failure_class=failure,
        requested_identity=None if failure is not None else _identity(symbol),
        returned_identity=(
            None
            if failure is not None
            else _identity(symbol if returned_symbol is None else returned_symbol)
        ),
        security_boundary=SecurityBoundaryEvidence.passed(),
    )


def _coordinator(
    database: Path,
    symbols: tuple[str, ...],
    twse: _Source,
    esun: _Source | None = None,
    *,
    clock: CallableClock | None = None,
) -> DS6DailyPreparationCoordinator:
    return DS6DailyPreparationCoordinator(
        DS6Configuration(database_path=database, candidate_limit=30),
        latest_date_provider=lambda anchor: TARGET_DATE,
        universe_provider=lambda target: _universe(symbols),
        twse_source=twse,
        esun_source=esun,
        clock=clock or (lambda: datetime(2026, 8, 13, 20, 1, tzinfo=UTC)),
        sleep=lambda _seconds: None,
    )


CallableClock = Callable[[], datetime]


def test_canonical_daily_entrypoint_uses_explicit_id_and_replays_without_source_fetch(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    twse = _Source({"3044": [_twse_complete("3044")]})
    coordinator = _coordinator(database, ("3044",), twse)

    first = coordinator.prepare(TARGET_DATE)
    item = first.preparation_results[0]
    assert first.status == "success"
    assert item.source_status == "canonical_complete"
    assert item.research_data_quality == "canonical"
    assert item.dataset_version_id is not None
    assert first.stage1 is not None
    assert first.stage1.dataset_reads == 1
    assert first.stage2 is not None
    assert first.stage2.dataset_reads == first.stage1.result.candidate_count
    assert len(twse.calls) == 1

    snapshot = ExplicitDatasetVersionConsumer(database).read(
        symbol="3044",
        target_date=TARGET_DATE,
        dataset_version_id=item.dataset_version_id,
        history_observations=250,
    )
    assert snapshot.provenance.dataset_version_id == item.dataset_version_id
    assert snapshot.price_history.current is not None

    second = coordinator.prepare(TARGET_DATE)
    assert second.replayed is True
    assert second.preparation_results[0].preparation_outcome == "replay"
    assert second.provider_request_count == 0
    assert len(twse.calls) == 1


def test_canonical_ds6_result_reaches_s4_and_report(tmp_path: Path) -> None:
    database = _database(tmp_path)
    prepared = _coordinator(
        database,
        ("3044",),
        _Source({"3044": [_twse_complete("3044")]}),
    ).prepare(TARGET_DATE)
    assert prepared.stage1 is not None
    assert prepared.stage2 is not None
    assert prepared.stage2.result.candidate_count == 1

    universe_run = SQLiteMarketUniverseRepository(database).persist(
        _universe(("3044",))
    )
    locators = tuple(
        CandidateInputLocator.from_stage2_provenance(
            symbol=candidate.symbol,
            provenance=candidate.provenance,
        )
        for candidate in prepared.stage2.result.candidates
    )
    checkpoint_repository = SQLiteScreenerCheckpointRepository(database)
    run = checkpoint_repository.create_run(
        universe_run_id=universe_run.universe_run_id,
        stage1_result=prepared.stage1.result,
        candidate_locators=locators,
    )
    for candidate, locator in zip(prepared.stage2.result.candidates, locators):
        checkpoint_repository.persist_candidate(
            screener_run_id=run.screener_run_id,
            candidate=candidate,
            research_locator_sha256=locator.research_locator_sha256 or "",
            snapshot_sha256=_hash(f"ds6-s4-snapshot|{candidate.symbol}"),
        )

    sealed = SQLiteScreenerReplayRepository(database).finalize_run(run.screener_run_id)
    assert sealed.result.execution_status == "success"
    assert sealed.result.research_data_quality == "canonical"
    generated = ScreenerReportGenerator(database).generate(run.screener_run_id)
    assert generated.report.report_contract_version == REPORT_CONTRACT_VERSION_V2
    assert generated.report.as_dict()["data_quality"] == "canonical"


def test_explicit_m9_consumer_rejects_implicit_or_wrong_version(tmp_path: Path) -> None:
    database = _database(tmp_path)
    twse = _Source({"3044": [_twse_complete("3044")]})
    result = _coordinator(database, ("3044",), twse).prepare(TARGET_DATE)
    version_id = result.preparation_results[0].dataset_version_id
    assert version_id is not None
    consumer = ExplicitDatasetVersionConsumer(database)
    with pytest.raises(DS6PreparationError):
        consumer.read(symbol="3044", target_date=TARGET_DATE, dataset_version_id="")
    with pytest.raises(Exception, match="does not match"):
        consumer.read(symbol="2330", target_date=TARGET_DATE, dataset_version_id=version_id)


def test_ds6_configuration_requires_an_absolute_isolated_database_path() -> None:
    with pytest.raises(DS6PreparationError, match="absolute"):
        DS6Configuration(database_path="relative-ds6.db")


def test_latest_date_unavailable_stops_before_universe_or_source(tmp_path: Path) -> None:
    database = _database(tmp_path)
    source = _Source({"3044": [_twse_complete("3044")]})
    coordinator = DS6DailyPreparationCoordinator(
        DS6Configuration(database_path=database),
        latest_date_provider=lambda _anchor: None,
        universe_provider=lambda _target: (_ for _ in ()).throw(
            AssertionError("Universe must not run")
        ),
        twse_source=source,
    )
    result = coordinator.prepare(TARGET_DATE)
    assert result.status == "latest_date_unavailable"
    assert result.target_date is None
    assert source.calls == []


def test_supplemental_security_boundary_failure_is_fail_closed(tmp_path: Path) -> None:
    database = _database(tmp_path)
    twse = _Source({"4590": [_twse_incomplete("4590")]})
    esun = _Source(
        {
            "4590": [
                replace(
                    _esun_supplemental("4590"),
                    security_boundary=SecurityBoundaryEvidence(),
                )
            ]
        }
    )
    result = _coordinator(database, ("4590",), twse, esun).prepare(TARGET_DATE)
    item = result.preparation_results[0]
    assert item.dataset_version_id is None
    assert item.preparation_outcome == "supplemental_rejected"
    assert item.supplemental_source_eligible is False


def test_ds6_does_not_migrate_or_write_a_v11_database(tmp_path: Path) -> None:
    database = tmp_path / "v11-only.db"
    SQLiteResearchRepository(database).initialize()
    SQLiteScreenerMigrationRunner(database).migrate()
    before = database.read_bytes()
    coordinator = _coordinator(
        database,
        ("3044",),
        _Source({"3044": [_twse_complete("3044")]})
    )
    with pytest.raises(DatasetMigrationStateError):
        coordinator.prepare(TARGET_DATE)
    assert database.read_bytes() == before
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 11
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'research_dataset_versions'"
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    ("esun_plan", "expected_validation", "expected_discrepancy"),
    [
        (None, "not_requested", 0),
        ("equal", "available", 0),
        ("discrepant", "available", 1),
        ("unavailable", "unavailable", 0),
        ("identity_mismatch", "identity_mismatch", 0),
    ],
)
def test_complete_twse_always_remains_canonical_with_optional_validation(
    tmp_path: Path,
    esun_plan: str | None,
    expected_validation: str,
    expected_discrepancy: int,
) -> None:
    database = _database(tmp_path)
    twse = _Source({"3044": [_twse_complete("3044")]})
    esun = None
    if esun_plan is not None:
        if esun_plan == "equal":
            value = _esun_validation("3044")
        elif esun_plan == "discrepant":
            value = _esun_validation("3044", volume_delta=77)
        elif esun_plan == "unavailable":
            value = _esun_validation("3044", failure=FailureClassification.UNAVAILABLE)
        else:
            value = _esun_validation("3044", returned_symbol="6589")
        esun = _Source({"3044": [value]})

    result = _coordinator(database, ("3044",), twse, esun).prepare(TARGET_DATE)
    item = result.preparation_results[0]
    assert item.source_status == "canonical_complete"
    assert item.research_data_quality == "canonical"
    assert item.esun_count == 0
    assert item.discrepancy_count == expected_discrepancy
    assert item.validation_status == expected_validation


def test_complete_twse_keeps_validation_rows_out_when_security_boundary_fails(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    twse = _Source({"3044": [_twse_complete("3044")]})
    esun = _Source(
        {
            "3044": [
                replace(
                    _esun_validation("3044"),
                    security_boundary=SecurityBoundaryEvidence(),
                )
            ]
        }
    )

    result = _coordinator(database, ("3044",), twse, esun).prepare(TARGET_DATE)
    item = result.preparation_results[0]
    assert item.source_status == "canonical_complete"
    assert item.validation_status == "security_boundary_failed"
    stored = DatasetVersionRepository(database).replay(item.dataset_version_id or "")
    assert stored.provenance_summary.validation_sources == ()
    assert stored.provenance_summary.supplemental_sources == ()


def test_complete_twse_treats_non_symbol_identity_difference_as_validation_failure(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    twse = _Source({"3044": [_twse_complete("3044")]})
    mismatched_identity = replace(_identity("3044"), market="OTC")
    esun = _Source(
        {
            "3044": [
                replace(
                    _esun_validation("3044"),
                    returned_identity=mismatched_identity,
                )
            ]
        }
    )

    result = _coordinator(database, ("3044",), twse, esun).prepare(TARGET_DATE)
    item = result.preparation_results[0]
    assert item.source_status == "canonical_complete"
    assert item.validation_status == "identity_mismatch"
    assert item.discrepancy_count == 0


def test_full_6108_supplemental_uses_ds2_then_ds3_constructor(tmp_path: Path) -> None:
    database = _database(tmp_path)
    twse = _Source({"6108": [_twse_incomplete("6108")]})
    esun = _Source({"6108": [_esun_supplemental("6108")]})
    result = _coordinator(database, ("6108",), twse, esun).prepare(TARGET_DATE)
    item = result.preparation_results[0]
    assert result.status == "success"
    assert item.dataset_version_id is not None
    assert item.source_status == "provisional_mixed"
    assert item.research_data_quality == "provisional"
    assert item.twse_coverage == 228
    assert item.esun_count == 22
    assert item.supplemental_source_eligible is True
    assert item.reconciliation_status == "pending"
    assert result.stage1 is not None
    assert result.stage1.history_observations == 62
    assert result.stage1.dataset_version_ids == (("6108", item.dataset_version_id),)
    assert result.stage2 is not None
    assert result.stage2.history_observations == 250
    assert result.stage2.dataset_version_ids == (("6108", item.dataset_version_id),)
    stored = DatasetVersionRepository(database).replay(item.dataset_version_id)
    assert stored.identity.symbol == "6108"
    assert len(twse.calls) >= 1
    assert len(esun.calls) == 1


def test_6108_ds6_result_reaches_s4_and_v2_report(tmp_path: Path) -> None:
    database = _database(tmp_path)
    twse = _Source({"6108": [_twse_incomplete("6108")]})
    esun = _Source({"6108": [_esun_supplemental("6108")]})
    prepared = _coordinator(database, ("6108",), twse, esun).prepare(TARGET_DATE)
    assert prepared.stage1 is not None
    assert prepared.stage2 is not None
    assert prepared.stage2.result.candidate_count == 1

    universe_run = SQLiteMarketUniverseRepository(database).persist(
        _universe(("6108",))
    )
    locators = tuple(
        CandidateInputLocator.from_stage2_provenance(
            symbol=candidate.symbol,
            provenance=candidate.provenance,
        )
        for candidate in prepared.stage2.result.candidates
    )
    checkpoint_repository = SQLiteScreenerCheckpointRepository(database)
    run = checkpoint_repository.create_run(
        universe_run_id=universe_run.universe_run_id,
        stage1_result=prepared.stage1.result,
        candidate_locators=locators,
    )
    for candidate, locator in zip(prepared.stage2.result.candidates, locators):
        checkpoint_repository.persist_candidate(
            screener_run_id=run.screener_run_id,
            candidate=candidate,
            research_locator_sha256=locator.research_locator_sha256 or "",
            snapshot_sha256=_hash(f"ds6-s4-snapshot|{candidate.symbol}"),
        )

    sealed = SQLiteScreenerReplayRepository(database).finalize_run(run.screener_run_id)
    assert sealed.result.execution_status == "provisional_success"
    assert sealed.result.research_data_quality == "provisional"
    generated = ScreenerReportGenerator(database).generate(run.screener_run_id)
    assert generated.report.report_contract_version == REPORT_CONTRACT_VERSION_V2
    assert generated.report.as_dict()["data_quality"] == "provisional"
    assert "PROVISIONAL" in generated.markdown


def test_partial_supplemental_is_incomplete_and_keeps_resume_state(tmp_path: Path) -> None:
    database = _database(tmp_path)
    twse = _Source({"6108": [_twse_incomplete("6108")]})
    esun = _Source({"6108": [_esun_supplemental("6108", count=18)]})
    result = _coordinator(database, ("6108",), twse, esun).prepare(TARGET_DATE)
    item = result.preparation_results[0]
    assert result.status == "preparation_incomplete"
    assert item.dataset_version_id is None
    assert item.preparation_outcome == "supplemental_incomplete"
    assert len(item.remaining_dates) == 4
    assert result.stage1 is None
    assert result.stage2 is None


@pytest.mark.parametrize("failure", [
    FailureClassification.MALFORMED,
    FailureClassification.PERMANENT,
    FailureClassification.IDENTITY_MISMATCH,
    FailureClassification.UNAVAILABLE,
    None,
])
def test_noneligible_twse_failures_never_fall_through_to_esun(
    tmp_path: Path,
    failure: FailureClassification | None,
) -> None:
    database = _database(tmp_path)
    dates = tuple(BASE_DATE + timedelta(days=offset) for offset in range(250))
    twse_value = replace(
        _twse_incomplete("4590"),
        failure_class=failure,
        failure_evidence=TwseFailureEvidence(
            endpoint_identity_verified=True,
            classification_basis="explicit-fixture-failure",
        ),
    )
    twse = _Source({"4590": [twse_value]})
    esun = _Source({"4590": [_esun_supplemental("4590")]})
    result = _coordinator(database, ("4590",), twse, esun).prepare(TARGET_DATE)
    item = result.preparation_results[0]
    assert item.dataset_version_id is None
    assert item.preparation_outcome == "supplemental_rejected"
    assert len(esun.calls) == 0
    assert dates[-1] in item.remaining_dates


def test_esun_identity_mismatch_is_rejected_by_ds2_and_twse_incomplete_fails_closed(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    twse = _Source({"6589": [_twse_incomplete("6589")]})
    esun = _Source({"6589": [_esun_supplemental("6589")]})
    # Keep the source output structurally valid while changing only the
    # independently verified returned identity.
    esun.plans["6589"][0] = replace(
        _esun_supplemental("6589"),
        returned_identity=_identity("4590"),
    )
    result = _coordinator(database, ("6589",), twse, esun).prepare(TARGET_DATE)
    item = result.preparation_results[0]
    assert item.dataset_version_id is None
    assert item.preparation_outcome == "supplemental_rejected"
    assert item.supplemental_source_eligible is False


def test_timing_and_retry_are_bounded_and_not_part_of_dataset_identity(tmp_path: Path) -> None:
    database = _database(tmp_path)
    twse = _Source({
        "7740": [_twse_incomplete("7740"), _twse_incomplete("7740"), _twse_complete("7740")]
    })
    clock_values = [
        datetime(2026, 8, 13, 19, 30, tzinfo=UTC),
        datetime(2026, 8, 13, 19, 31, tzinfo=UTC),
        datetime(2026, 8, 13, 19, 32, tzinfo=UTC),
    ]
    clock_index = 0

    def clock() -> datetime:
        nonlocal clock_index
        value = clock_values[min(clock_index, len(clock_values) - 1)]
        clock_index += 1
        return value
    sleeps: list[float] = []
    coordinator = DS6DailyPreparationCoordinator(
        DS6Configuration(database_path=database, max_twse_attempts=3),
        latest_date_provider=lambda anchor: TARGET_DATE,
        universe_provider=lambda target: _universe(("7740",)),
        twse_source=twse,
        clock=clock,
        sleep=sleeps.append,
    )
    result = coordinator.prepare(TARGET_DATE)
    assert result.preparation_results[0].source_status == "canonical_complete"
    assert len(twse.calls) == 3
    assert len(sleeps) == 2
    assert all(item <= 1.0 for item in sleeps)


def test_latest_date_is_the_only_target_date_source(tmp_path: Path) -> None:
    database = _database(tmp_path)
    requested: list[date] = []
    twse = _Source({"3044": [_twse_complete("3044")]})

    def latest(anchor: date) -> date:
        requested.append(anchor)
        return TARGET_DATE

    coordinator = DS6DailyPreparationCoordinator(
        DS6Configuration(database_path=database),
        latest_date_provider=latest,
        universe_provider=lambda target: _universe(("3044",)),
        twse_source=twse,
    )
    result = coordinator.prepare(date(1999, 1, 1))
    assert result.target_date == TARGET_DATE
    assert requested == [date(1999, 1, 1)]
    assert all(item.target_date == TARGET_DATE for item in result.preparation_results)


def test_reconciliation_creates_child_and_keeps_parent_immutable(tmp_path: Path) -> None:
    database = _database(tmp_path)
    parent_twse = _Source({"6108": [_twse_incomplete("6108")]})
    parent_esun = _Source({"6108": [_esun_supplemental("6108")]})
    parent_result = _coordinator(database, ("6108",), parent_twse, parent_esun).prepare(TARGET_DATE)
    parent_id = parent_result.preparation_results[0].dataset_version_id
    assert parent_id is not None
    parent_before = DatasetVersionRepository(database).replay(parent_id)

    current_twse = _Source({"6108": [_twse_complete("6108")]})
    child_result = _coordinator(database, ("6108",), current_twse).prepare(TARGET_DATE)
    child = child_result.preparation_results[0]
    assert child.dataset_version_id is not None
    assert child.dataset_version_id != parent_id
    assert child.source_status == "reconciled"
    assert child.preparation_outcome == "reconciled_equal"
    assert DatasetVersionRepository(database).replay(parent_id).canonical_json() == parent_before.canonical_json()


def test_reconciled_ds6_result_reaches_s4_and_report(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _coordinator(
        database,
        ("6108",),
        _Source({"6108": [_twse_incomplete("6108")]}),
        _Source({"6108": [_esun_supplemental("6108")]}),
    ).prepare(TARGET_DATE)
    prepared = _coordinator(
        database,
        ("6108",),
        _Source({"6108": [_twse_complete("6108")]}),
    ).prepare(TARGET_DATE)
    item = prepared.preparation_results[0]
    assert item.source_status == "reconciled"
    assert prepared.stage1 is not None
    assert prepared.stage2 is not None
    assert prepared.stage2.result.candidate_count == 1

    universe_run = SQLiteMarketUniverseRepository(database).persist(
        _universe(("6108",))
    )
    locators = tuple(
        CandidateInputLocator.from_stage2_provenance(
            symbol=candidate.symbol,
            provenance=candidate.provenance,
        )
        for candidate in prepared.stage2.result.candidates
    )
    checkpoint_repository = SQLiteScreenerCheckpointRepository(database)
    run = checkpoint_repository.create_run(
        universe_run_id=universe_run.universe_run_id,
        stage1_result=prepared.stage1.result,
        candidate_locators=locators,
    )
    for candidate, locator in zip(prepared.stage2.result.candidates, locators):
        checkpoint_repository.persist_candidate(
            screener_run_id=run.screener_run_id,
            candidate=candidate,
            research_locator_sha256=locator.research_locator_sha256 or "",
            snapshot_sha256=_hash(f"ds6-s4-snapshot|{candidate.symbol}"),
        )

    sealed = SQLiteScreenerReplayRepository(database).finalize_run(run.screener_run_id)
    assert sealed.result.execution_status == "success"
    assert sealed.result.research_data_quality == "reconciled"
    generated = ScreenerReportGenerator(database).generate(run.screener_run_id)
    assert generated.report.report_contract_version == REPORT_CONTRACT_VERSION_V2
    assert generated.report.as_dict()["data_quality"] == "reconciled"
    assert "RECONCILED" in generated.markdown


def test_reconciliation_resume_requests_only_remaining_dates(tmp_path: Path) -> None:
    database = _database(tmp_path)
    parent = _coordinator(
        database,
        ("6108",),
        _Source({"6108": [_twse_incomplete("6108")]}),
        _Source({"6108": [_esun_supplemental("6108")]}),
    ).prepare(TARGET_DATE)
    parent_id = parent.preparation_results[0].dataset_version_id
    assert parent_id is not None

    partial_dates = tuple(BASE_DATE + timedelta(days=offset) for offset in range(232, 250))
    partial = DS6SourcePreparation(
        provider="twse-historical",
        symbol="6108",
        target_date=TARGET_DATE,
        requested_dates=partial_dates,
        observations=tuple(_raw("6108", offset) for offset in range(232, 250)),
        source_run_id="twse-resume-partial-6108",
        artifact=_artifact("twse-historical", "6108", "twse-resume-partial"),
        status="complete",
    )
    partial_source = _Source({"6108": [partial]})
    partial_result = _coordinator(database, ("6108",), partial_source).prepare(TARGET_DATE)
    child_id = partial_result.preparation_results[0].dataset_version_id
    assert child_id is not None and child_id != parent_id
    assert partial_source.calls[0][1] == tuple(
        BASE_DATE + timedelta(days=offset) for offset in range(228, 250)
    )

    remaining_dates = tuple(BASE_DATE + timedelta(days=offset) for offset in range(228, 232))
    resume = DS6SourcePreparation(
        provider="twse-historical",
        symbol="6108",
        target_date=TARGET_DATE,
        requested_dates=remaining_dates,
        observations=tuple(_raw("6108", offset) for offset in range(228, 232)),
        source_run_id="twse-resume-final-6108",
        artifact=_artifact("twse-historical", "6108", "twse-resume-final"),
        status="complete",
        checkpoint_reused=True,
    )
    resume_source = _Source({"6108": [resume]})
    final = _coordinator(database, ("6108",), resume_source).prepare(TARGET_DATE)
    assert final.preparation_results[0].source_status == "reconciled"
    assert resume_source.calls[0][1] == remaining_dates
    assert resume_source.calls[0][2] is not None
    assert resume_source.calls[0][2].remaining_dates == remaining_dates


def test_partial_reconciliation_creates_provisional_child(tmp_path: Path) -> None:
    database = _database(tmp_path)
    parent = _coordinator(
        database,
        ("6108",),
        _Source({"6108": [_twse_incomplete("6108")]}),
        _Source({"6108": [_esun_supplemental("6108")]})
    ).prepare(TARGET_DATE)
    parent_id = parent.preparation_results[0].dataset_version_id
    assert parent_id is not None

    subset = tuple(BASE_DATE + timedelta(days=offset) for offset in range(232, 250))
    partial_rows = tuple(_raw("6108", offset) for offset in range(232, 250))
    partial_twse = DS6SourcePreparation(
        provider="twse-historical",
        symbol="6108",
        target_date=TARGET_DATE,
        requested_dates=subset,
        observations=partial_rows,
        source_run_id="twse-reconcile-partial-6108",
        artifact=_artifact("twse-historical", "6108", "twse-reconcile-partial"),
        status="complete",
    )
    child_result = _coordinator(
        database,
        ("6108",),
        _Source({"6108": [partial_twse]}),
    ).prepare(TARGET_DATE)
    child = child_result.preparation_results[0]
    assert child.dataset_version_id is not None
    assert child.dataset_version_id != parent_id
    assert child.source_status == "provisional_mixed"
    assert child.preparation_outcome == "reconciliation_partial"
    assert len(child.remaining_dates) == 4


def test_new_market_date_reaches_same_daily_entrypoint(tmp_path: Path) -> None:
    database = _database(tmp_path)
    next_date = TARGET_DATE + timedelta(days=1)
    twse = _Source({"3044": [_twse_complete("3044")]})
    calls: list[date] = []
    coordinator = DS6DailyPreparationCoordinator(
        DS6Configuration(database_path=database),
        latest_date_provider=lambda anchor: (calls.append(anchor) or next_date),
        universe_provider=lambda target: replace(_universe(("3044",)), market_date=target),
        twse_source=twse,
    )
    # The fixture source is deliberately shifted to the new date so the
    # entrypoint, not a manual script, owns the date transition.
    shifted_dates = tuple(next_date - timedelta(days=249 - i) for i in range(250))
    shifted = replace(
        twse.plans["3044"][0],
        target_date=next_date,
        requested_dates=shifted_dates,
        observations=tuple(
            replace(_raw("3044", offset), trade_date=shifted_dates[offset])
            for offset in range(250)
        ),
    )
    twse.plans["3044"] = [shifted]
    result = coordinator.prepare(TARGET_DATE)
    assert result.target_date == next_date
    assert result.status == "success"
    assert calls == [TARGET_DATE]


def test_already_prepared_date_replays_across_a_new_coordinator(tmp_path: Path) -> None:
    database = _database(tmp_path)
    first_source = _Source({"3044": [_twse_complete("3044")]})
    first = _coordinator(database, ("3044",), first_source).prepare(TARGET_DATE)
    version_id = first.preparation_results[0].dataset_version_id
    assert version_id is not None

    second_source = _Source({"3044": []})
    second = _coordinator(database, ("3044",), second_source).prepare(TARGET_DATE)
    assert second.status == "success"
    assert second.preparation_results[0].dataset_version_id == version_id
    assert second.preparation_results[0].preparation_outcome == "replay"
    assert second.provider_request_count == 0
    assert second_source.calls == []
