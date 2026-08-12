from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date
from pathlib import Path

import pytest

import app.storage.universe_persistence as universe_persistence_module
from app.screener.universe import (
    CLASSIFICATION_DATASET,
    DELISTING_DATASET,
    IDENTITY_DATASET,
    STOCK_DAY_ALL_DATASET,
    UNIVERSE_METHODOLOGY_VERSION,
    VALUATION_DATASET,
    DailyTradingRecord,
    InputDatasetSnapshot,
    InstrumentClassification,
    InstrumentClassificationRecord,
    ListedIdentityRecord,
    MarketUniverseBuildInput,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
    ValuationCoverageRecord,
    build_market_universe,
)
from app.storage import SQLiteResearchRepository
from app.storage.screener_migration import SQLiteScreenerMigrationRunner
from app.storage.universe_persistence import (
    MarketUniverseConflictError,
    MarketUniverseStateError,
    SQLiteMarketUniverseRepository,
)


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "screener"
    / "twse_universe_20260807.json"
)
FROZEN_S1_SHA256 = (
    "ce43e0e9bdb525ec807170cd9b49bf79e55d2fd6f46a42f6deebbe8f740b9324"
)
SOURCE_EVIDENCE_FIELDS = (
    "source",
    "dataset",
    "source_ref",
    "contract_version",
    "payload_sha256",
    "payload_size_bytes",
    "hash_basis",
)


def _v11_database(tmp_path: Path, name: str) -> Path:
    database_path = tmp_path / name
    research = SQLiteResearchRepository(database_path)
    research.initialize()
    assert research.get_schema_version() == 10
    SQLiteScreenerMigrationRunner(database_path).migrate()
    return database_path


def _fixture_evidence(value: dict[str, object]) -> SourceEvidence:
    return SourceEvidence(**{name: value[name] for name in SOURCE_EVIDENCE_FIELDS})


def _frozen_s1_snapshot() -> MarketUniverseSnapshot:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    sources = payload["source_snapshots"]
    eligible = payload["eligible"]
    unavailable = payload["active_unavailable"]
    excluded = payload["excluded"]
    assert isinstance(sources, dict)
    assert isinstance(eligible, list)
    assert isinstance(unavailable, list)
    assert isinstance(excluded, list)

    identity_rows = [
        ListedIdentityRecord(symbol, name, date.fromisoformat(listing_date))
        for symbol, name, listing_date in [*eligible, *unavailable]
    ]
    identity_rows.extend(
        ListedIdentityRecord(symbol, name, date.fromisoformat(listing_date))
        for symbol, name, unused_classification, listing_date in excluded
        if listing_date is not None
    )
    trading_rows = [
        DailyTradingRecord(symbol, name, True)
        for symbol, name, unused_listing_date in eligible
    ]
    trading_rows.extend(
        DailyTradingRecord(symbol, name, True)
        for symbol, name, unused_classification, unused_listing_date in excluded
    )
    valuation_rows = [
        ValuationCoverageRecord(symbol, name)
        for symbol, name, unused_listing_date in eligible
    ]
    classification_rows = [
        InstrumentClassificationRecord(
            symbol,
            InstrumentClassification.COMMON_EQUITY,
        )
        for symbol, unused_name, unused_listing_date in [*eligible, *unavailable]
    ]
    classification_rows.extend(
        InstrumentClassificationRecord(
            symbol,
            InstrumentClassification(classification),
        )
        for symbol, unused_name, classification, unused_listing_date in excluded
    )
    inputs = MarketUniverseBuildInput(
        market_date=date.fromisoformat(str(payload["market_date"])),
        methodology_version=str(payload["methodology_version"]),
        source_policy=str(payload["source_policy"]),
        listed_identity=InputDatasetSnapshot(
            _fixture_evidence(sources["identity"]),
            tuple(identity_rows),
        ),
        daily_trading=InputDatasetSnapshot(
            _fixture_evidence(sources["daily_trading"]),
            tuple(trading_rows),
        ),
        valuation_coverage=InputDatasetSnapshot(
            _fixture_evidence(sources["valuation_coverage"]),
            tuple(valuation_rows),
        ),
        classifications=InputDatasetSnapshot(
            _fixture_evidence(sources["classifications"]),
            tuple(classification_rows),
        ),
        delistings=InputDatasetSnapshot(
            _fixture_evidence(sources["delistings"]),
            (),
        ),
    )
    return build_market_universe(inputs)


def _evidence_set() -> tuple[SourceEvidence, ...]:
    evidence = []
    for dataset in (
        IDENTITY_DATASET,
        STOCK_DAY_ALL_DATASET,
        VALUATION_DATASET,
        CLASSIFICATION_DATASET,
        DELISTING_DATASET,
    ):
        raw = dataset.encode("utf-8")
        evidence.append(
            SourceEvidence(
                source="twse",
                dataset=dataset,
                source_ref=f"contract://screener-s4-tests/{dataset}",
                contract_version="screener-s4-test-v1",
                payload_sha256=hashlib.sha256(raw).hexdigest(),
                payload_size_bytes=len(raw),
                hash_basis="canonical-json-v1",
            )
        )
    return tuple(
        sorted(
            evidence,
            key=lambda item: (
                item.source,
                item.dataset,
                item.source_ref,
                item.contract_version,
                item.payload_sha256,
                str(item.payload_size_bytes),
                item.hash_basis,
            ),
        )
    )


def _five_status_snapshot() -> MarketUniverseSnapshot:
    evidence = _evidence_set()
    members = (
        MarketUniverseMember(
            symbol="1001",
            name="Eligible",
            market="TWSE",
            status=UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
            listing_date=date(2000, 1, 1),
            delisting_date=None,
            exclusion_reason=None,
            source_evidence=evidence,
        ),
        MarketUniverseMember(
            symbol="1002",
            name="Unavailable",
            market="TWSE",
            status=UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE,
            listing_date=date(2001, 1, 1),
            delisting_date=None,
            exclusion_reason="stock_day_all_missing",
            source_evidence=evidence,
        ),
        MarketUniverseMember(
            symbol="1003",
            name="Excluded",
            market="TWSE",
            status=UniverseMemberStatus.EXCLUDED_NON_COMMON_EQUITY,
            listing_date=date(2002, 1, 1),
            delisting_date=None,
            exclusion_reason="non_common_equity_etf",
            source_evidence=evidence,
        ),
        MarketUniverseMember(
            symbol="1004",
            name="Inactive",
            market="TWSE",
            status=UniverseMemberStatus.INACTIVE,
            listing_date=date(2003, 1, 1),
            delisting_date=date(2025, 1, 1),
            exclusion_reason="explicit_delisting_evidence",
            source_evidence=evidence,
        ),
        MarketUniverseMember(
            symbol="1005",
            name="Unresolved",
            market="TWSE",
            status=UniverseMemberStatus.CLASSIFICATION_UNRESOLVED,
            listing_date=date(2004, 1, 1),
            delisting_date=None,
            exclusion_reason="classification_evidence_unresolved",
            source_evidence=evidence,
        ),
    )
    return MarketUniverseSnapshot(
        market_date=date(2026, 8, 7),
        methodology_version=UNIVERSE_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=5,
        scan_eligible_count=1,
        scan_unavailable_count=1,
        excluded_count=1,
        inactive_count=1,
        unresolved_count=1,
        members=members,
    )


def _universe_rows(database_path: Path) -> tuple[tuple[tuple[object, ...], ...], ...]:
    with sqlite3.connect(database_path) as connection:
        runs = tuple(
            tuple(row)
            for row in connection.execute(
                "SELECT * FROM market_universe_runs ORDER BY universe_run_id"
            )
        )
        members = tuple(
            tuple(row)
            for row in connection.execute(
                "SELECT * FROM market_universe_members ORDER BY universe_run_id, symbol"
            )
        )
        evidence = tuple(
            tuple(row)
            for row in connection.execute(
                "SELECT * FROM screener_source_artifacts "
                "WHERE universe_run_id IS NOT NULL "
                "ORDER BY universe_run_id, ordinal"
            )
        )
    return runs, members, evidence


def test_frozen_s1_round_trip_reconstructs_identical_canonical_sha(
    tmp_path: Path,
) -> None:
    database_path = _v11_database(tmp_path, "frozen-round-trip.db")
    snapshot = _frozen_s1_snapshot()
    assert snapshot.payload_sha256 == FROZEN_S1_SHA256
    repository = SQLiteMarketUniverseRepository(database_path)

    stored = repository.persist(snapshot)
    reconstructed = repository.load(stored.universe_run_id)

    assert stored.created is True
    assert reconstructed == snapshot
    assert reconstructed.canonical_json() == snapshot.canonical_json()
    assert reconstructed.payload_sha256 == FROZEN_S1_SHA256
    with sqlite3.connect(database_path) as connection:
        run = connection.execute(
            "SELECT status, canonical_sha256, universe_count, scan_eligible_count "
            "FROM market_universe_runs"
        ).fetchone()
        member_count = connection.execute(
            "SELECT COUNT(*) FROM market_universe_members"
        ).fetchone()[0]
        evidence_count = connection.execute(
            "SELECT COUNT(*) FROM screener_source_artifacts "
            "WHERE universe_run_id IS NOT NULL"
        ).fetchone()[0]
        unrelated_counts = tuple(
            connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "screener_runs",
                "screener_candidates",
                "candidate_reasons",
                "candidate_metrics",
            )
        )
    assert run == ("success", FROZEN_S1_SHA256, 1_095, 1_082)
    assert member_count == 1_095
    assert evidence_count == 5
    assert unrelated_counts == (0, 0, 0, 0)


def test_identical_replay_is_zero_write_and_preserves_timestamps_and_evidence(
    tmp_path: Path,
) -> None:
    database_path = _v11_database(tmp_path, "zero-write-replay.db")
    snapshot = _five_status_snapshot()
    repository = SQLiteMarketUniverseRepository(database_path)
    first = repository.persist(snapshot)
    before = _universe_rows(database_path)

    second = repository.persist(snapshot)
    after = _universe_rows(database_path)

    assert first.created is True
    assert second.created is False
    assert second.universe_run_id == first.universe_run_id
    assert second.snapshot == snapshot
    assert after == before


@pytest.mark.parametrize(
    "method_name",
    ("_insert_run", "_insert_members", "_insert_evidence", "_mark_success"),
)
def test_universe_commit_fault_at_any_stage_rolls_back_everything(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    database_path = _v11_database(tmp_path, f"rollback-{method_name}.db")
    repository = SQLiteMarketUniverseRepository(database_path)
    original = getattr(repository, method_name)

    def execute_then_fail(*args: object, **kwargs: object) -> None:
        original(*args, **kwargs)
        raise RuntimeError(f"fault after {method_name}")

    monkeypatch.setattr(repository, method_name, execute_then_fail)
    with pytest.raises(RuntimeError, match=method_name):
        repository.persist(_five_status_snapshot())

    assert _universe_rows(database_path) == ((), (), ())


def test_all_five_classification_counts_round_trip_exactly(tmp_path: Path) -> None:
    database_path = _v11_database(tmp_path, "classification-counts.db")
    snapshot = _five_status_snapshot()
    repository = SQLiteMarketUniverseRepository(database_path)
    result = repository.persist(snapshot)

    reconstructed = repository.load(result.universe_run_id)
    with sqlite3.connect(database_path) as connection:
        run_counts = connection.execute(
            "SELECT universe_count, scan_eligible_count, scan_unavailable_count, "
            "excluded_count, inactive_count, unresolved_count "
            "FROM market_universe_runs"
        ).fetchone()
        member_counts = dict(
            connection.execute(
                "SELECT status, COUNT(*) FROM market_universe_members GROUP BY status"
            )
        )

    assert reconstructed == snapshot
    assert run_counts == (5, 1, 1, 1, 1, 1)
    assert member_counts == {
        "active_scan_eligible": 1,
        "active_scan_unavailable": 1,
        "excluded_non_common_equity": 1,
        "inactive": 1,
        "classification_unresolved": 1,
    }


def test_persistence_never_reads_or_writes_symbols_or_watchlists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = _v11_database(tmp_path, "independent-universe.db")
    original_connect = sqlite3.connect
    forbidden_tables = {
        "symbols",
        "watchlists",
        "watchlist_members",
        "watchlist_revisions",
        "watchlist_revision_members",
    }

    def guarded_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = original_connect(*args, **kwargs)

        def authorize(
            action: int,
            arg1: str | None,
            arg2: str | None,
            database: str | None,
            source: str | None,
        ) -> int:
            del arg2, database, source
            if action in {
                sqlite3.SQLITE_READ,
                sqlite3.SQLITE_INSERT,
                sqlite3.SQLITE_UPDATE,
                sqlite3.SQLITE_DELETE,
            } and arg1 in forbidden_tables:
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorize)
        return connection

    monkeypatch.setattr(
        universe_persistence_module.sqlite3,
        "connect",
        guarded_connect,
    )
    result = SQLiteMarketUniverseRepository(database_path).persist(
        _five_status_snapshot()
    )

    assert result.created is True
    with original_connect(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM symbols").fetchone()[0] == 0
        watchlist_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'watchlists'"
        ).fetchone()
    assert watchlist_table is None


def test_tampered_member_replay_fails_closed_without_repair_or_duplicate(
    tmp_path: Path,
) -> None:
    database_path = _v11_database(tmp_path, "tampered-replay.db")
    snapshot = _five_status_snapshot()
    repository = SQLiteMarketUniverseRepository(database_path)
    first = repository.persist(snapshot)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE market_universe_members SET status = 'inactive', "
            "exclusion_reason = 'external_tamper' "
            "WHERE universe_run_id = ? AND symbol = '1001'",
            (first.universe_run_id,),
        )
        connection.commit()
    tampered = _universe_rows(database_path)

    with pytest.raises(MarketUniverseConflictError):
        repository.persist(snapshot)

    after = _universe_rows(database_path)
    assert after == tampered
    assert len(after[0]) == 1
    assert len(after[1]) == 5
    eligible_row = next(row for row in after[1] if row[1] == "1001")
    assert eligible_row[4] == "inactive"
    assert eligible_row[7] == "external_tamper"


def test_repository_requires_explicit_v11_migration(tmp_path: Path) -> None:
    database_path = tmp_path / "still-v10.db"
    research = SQLiteResearchRepository(database_path)
    research.initialize()

    with pytest.raises(MarketUniverseStateError, match="migration 11"):
        SQLiteMarketUniverseRepository(database_path).persist(
            _five_status_snapshot()
        )
