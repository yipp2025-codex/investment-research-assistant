from __future__ import annotations

from datetime import date, timedelta
import hashlib
from pathlib import Path
import sqlite3

import pytest

import app.storage.dataset_versions as dataset_versions_module
from app.data_contracts.dataset_persistence import (
    CoverageBasis,
    DatasetArtifactRef,
    DatasetObservation,
    DatasetPersistenceContractError,
    IncompleteDatasetCoverageError,
    build_canonical_dataset_version,
    build_provisional_dataset_version,
)
from app.data_contracts.dual_source import (
    FailureClassification,
    SourceRole,
)
from app.data_contracts.supplemental_eligibility import (
    InstrumentIdentity,
    OHLCVObservation,
    SecurityBoundaryEvidence,
    SupplementalCandidateInput,
    evaluate_supplemental,
)
from app.storage import SQLiteResearchRepository
from app.storage.dataset_versions import (
    DatasetMigrationStateError,
    DatasetPersistenceIntegrityError,
    DatasetVersionMigrationRunner,
    DatasetVersionRepository,
)
from app.storage.screener_migration import SQLiteScreenerMigrationRunner


BASE_DATE = date(2026, 1, 1)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _identity(symbol: str) -> InstrumentIdentity:
    return InstrumentIdentity(
        symbol=symbol,
        market="TSE",
        exchange="TWSE",
        currency="TWD",
        security_type="EQUITY",
    )


def _eligibility_row(offset: int, symbol: str) -> OHLCVObservation:
    return OHLCVObservation(
        trade_date=BASE_DATE + timedelta(days=offset),
        open=100.0 + offset,
        high=105.0 + offset,
        low=95.0 + offset,
        close=102.0 + offset,
        volume=1000 + offset,
        symbol=symbol,
    )


def _candidate(
    *,
    symbol: str = "6108",
    esun_count: int = 22,
    returned_symbol: str | None = None,
) -> SupplementalCandidateInput:
    missing = tuple(BASE_DATE + timedelta(days=offset) for offset in range(228, 250))
    return SupplementalCandidateInput(
        requested_symbol=symbol,
        target_date=BASE_DATE + timedelta(days=249),
        requested_start_date=BASE_DATE,
        missing_twse_dates=missing,
        twse_failure_class=FailureClassification.PACING_BLOCKED,
        source_run_id=f"esun-run-{symbol}",
        artifact_sha256=_hash(f"esun-artifact-{symbol}"),
        twse_observations=tuple(
            _eligibility_row(offset, symbol) for offset in range(228)
        ),
        esun_requested_identity=_identity(symbol),
        esun_returned_identity=_identity(
            symbol if returned_symbol is None else returned_symbol
        ),
        esun_observations=tuple(
            _eligibility_row(offset, symbol)
            for offset in range(228, 228 + esun_count)
        ),
        security_boundary=SecurityBoundaryEvidence.passed(),
    )


def _observation(
    offset: int,
    *,
    symbol: str,
    provider: str,
    role: SourceRole,
    run_id: str,
    selected: bool,
    price_delta: float = 0.0,
    volume_delta: int = 0,
) -> DatasetObservation:
    trade_date = BASE_DATE + timedelta(days=offset)
    open_price = 100.0 + price_delta
    high = 105.0 + price_delta
    low = 95.0 + price_delta
    close = 102.0 + price_delta
    volume = 1000 + offset + volume_delta
    return DatasetObservation(
        symbol=symbol,
        trade_date=trade_date,
        provider=provider,
        source_role=role,
        source_run_id=run_id,
        selected=selected,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        observation_sha256=_hash(
            f"{symbol}|{trade_date.isoformat()}|{provider}|{role.value}|"
            f"{run_id}|{selected}|{price_delta}|{volume}"
        ),
    )


def _artifact(ordinal: int, provider: str, symbol: str) -> DatasetArtifactRef:
    return DatasetArtifactRef(
        ordinal=ordinal,
        provider=provider,
        dataset=f"{provider}-historical",
        source_ref=f"https://example.invalid/{provider}/{symbol}/{ordinal}",
        contract_version=f"{provider}-contract.v1",
        payload_sha256=_hash(f"artifact|{provider}|{symbol}|{ordinal}"),
        payload_size_bytes=128 + ordinal,
        hash_basis="raw-response-bytes-v1",
    )


def _provisional_6108(*, esun_count: int = 22):
    candidate = _candidate(esun_count=esun_count)
    eligibility = evaluate_supplemental(candidate)
    twse = tuple(
        _observation(
            offset,
            symbol="6108",
            provider="twse",
            role=SourceRole.CANONICAL,
            run_id="twse-run-6108",
            selected=True,
        )
        for offset in range(228)
    )
    esun = tuple(
        _observation(
            offset,
            symbol="6108",
            provider="esun",
            role=SourceRole.SUPPLEMENTAL,
            run_id="esun-run-6108",
            selected=True,
        )
        for offset in range(228, 250)
    )
    version = build_provisional_dataset_version(
        symbol="6108",
        as_of_date=BASE_DATE + timedelta(days=249),
        required_observation_count=250,
        twse_observations=twse,
        eligibility_result=eligibility,
        esun_observations=esun,
        artifacts=(_artifact(1, "twse", "6108"), _artifact(2, "esun", "6108")),
        methodology_version="dataset-v1.1",
    )
    return version, eligibility


def _canonical_3044():
    twse = tuple(
        _observation(
            offset,
            symbol="3044",
            provider="twse",
            role=SourceRole.CANONICAL,
            run_id="twse-run-3044",
            selected=True,
        )
        for offset in range(250)
    )
    validation = (
        _observation(
            249,
            symbol="3044",
            provider="esun",
            role=SourceRole.VALIDATION,
            run_id="esun-validation-3044",
            selected=False,
            volume_delta=77,
        ),
    )
    return build_canonical_dataset_version(
        symbol="3044",
        as_of_date=BASE_DATE + timedelta(days=249),
        required_observation_count=250,
        twse_observations=twse,
        validation_observations=validation,
        artifacts=(_artifact(1, "twse", "3044"), _artifact(2, "esun", "3044")),
        methodology_version="dataset-v1.1",
        discrepancy_count=1,
    )


def _canonical_legal_short():
    twse = tuple(
        _observation(
            offset,
            symbol="6589",
            provider="twse",
            role=SourceRole.CANONICAL,
            run_id="twse-run-6589",
            selected=True,
        )
        for offset in range(2)
    )
    return build_canonical_dataset_version(
        symbol="6589",
        as_of_date=BASE_DATE + timedelta(days=1),
        required_observation_count=2,
        twse_observations=twse,
        validation_observations=(),
        artifacts=(_artifact(1, "twse", "6589"),),
        methodology_version="dataset-v1.1",
        coverage_basis=CoverageBasis.LEGAL_SHORT_LISTING_HISTORY,
    )


def _v11_database(tmp_path: Path, *, symbols: tuple[str, ...] = ("6108",)) -> Path:
    database_path = tmp_path / "isolated-v11.db"
    SQLiteResearchRepository(database_path).initialize()
    with sqlite3.connect(database_path) as connection:
        connection.executemany(
            "INSERT INTO symbols (symbol, name, market, currency, is_active) "
            "VALUES (?, ?, 'TSE', 'TWD', 1)",
            [(symbol, f"Company {symbol}") for symbol in symbols],
        )
    SQLiteScreenerMigrationRunner(database_path).migrate()
    return database_path


def _v12_database(tmp_path: Path, *, symbols: tuple[str, ...] = ("6108",)) -> Path:
    database_path = _v11_database(tmp_path, symbols=symbols)
    DatasetVersionMigrationRunner(database_path).migrate(isolated=True)
    return database_path


def _row_counts(database_path: Path) -> tuple[int, int, int]:
    with sqlite3.connect(database_path) as connection:
        return tuple(
            int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
            )
            for table in (
                "research_dataset_versions",
                "research_dataset_observations",
                "research_dataset_artifacts",
            )
        )


def test_6108_provisional_constructor_is_deterministic_and_complete() -> None:
    version, eligibility = _provisional_6108()

    assert eligibility.eligible is True
    assert eligibility.coverage_complete is True
    assert version.identity.source_status.value == "provisional_mixed"
    assert version.identity.authority_status.value == "incomplete"
    assert version.identity.reconciliation_status.value == "pending"
    assert version.identity.coverage.twse_observation_count == 228
    assert version.identity.coverage.esun_supplemental_count == 22
    assert version.identity.coverage.missing_twse_count == 22
    assert version.identity.coverage.selected_observation_count == 250
    assert len({item.trade_date for item in version.observations if item.selected}) == 250
    assert {
        item.trade_date
        for item in version.observations
        if item.source_role is SourceRole.SUPPLEMENTAL
    } == set(eligibility.eligible_observation_dates)
    assert len(version.identity.dataset_version_id) == 64
    assert len(version.canonical_sha256) == 64

    reversed_version = build_provisional_dataset_version(
        symbol="6108",
        as_of_date=BASE_DATE + timedelta(days=249),
        required_observation_count=250,
        twse_observations=tuple(
            reversed(
                [
                    _observation(
                        offset,
                        symbol="6108",
                        provider="twse",
                        role=SourceRole.CANONICAL,
                        run_id="twse-run-6108",
                        selected=True,
                    )
                    for offset in range(228)
                ]
            )
        ),
        eligibility_result=eligibility,
        esun_observations=tuple(reversed(version.observations[-22:])),
        artifacts=tuple(reversed(version.artifacts)),
        methodology_version="dataset-v1.1",
    )
    assert reversed_version.identity.dataset_version_id == version.identity.dataset_version_id
    assert reversed_version.canonical_sha256 == version.canonical_sha256


def test_partial_ds2_18_of_22_cannot_become_formal_dataset() -> None:
    version_candidate = _candidate(esun_count=18)
    eligibility = evaluate_supplemental(version_candidate)
    assert eligibility.eligible is True
    assert eligibility.coverage_complete is False
    twse = tuple(
        _observation(
            offset,
            symbol="6108",
            provider="twse",
            role=SourceRole.CANONICAL,
            run_id="twse-run-6108",
            selected=True,
        )
        for offset in range(228)
    )
    esun = tuple(
        _observation(
            offset,
            symbol="6108",
            provider="esun",
            role=SourceRole.SUPPLEMENTAL,
            run_id="esun-run-6108",
            selected=True,
        )
        for offset in range(228, 246)
    )
    with pytest.raises(IncompleteDatasetCoverageError):
        build_provisional_dataset_version(
            symbol="6108",
            as_of_date=BASE_DATE + timedelta(days=249),
            required_observation_count=250,
            twse_observations=twse,
            eligibility_result=eligibility,
            esun_observations=esun,
            artifacts=(_artifact(1, "twse", "6108"), _artifact(2, "esun", "6108")),
            methodology_version="dataset-v1.1",
        )


def test_3044_validation_discrepancy_is_not_supplemental() -> None:
    version = _canonical_3044()

    assert version.identity.source_status.value == "canonical_complete"
    assert version.identity.coverage.twse_observation_count == 250
    assert version.identity.coverage.esun_supplemental_count == 0
    assert version.identity.coverage.discrepancy_count == 1
    assert [
        item.provider
        for item in version.observations
        if item.source_role is SourceRole.VALIDATION
    ] == ["esun"]
    assert all(
        not item.selected
        for item in version.observations
        if item.source_role is SourceRole.VALIDATION
    )


def test_legal_short_coverage_basis_is_explicit() -> None:
    version = _canonical_legal_short()
    assert version.coverage_basis is CoverageBasis.LEGAL_SHORT_LISTING_HISTORY
    assert version.identity.coverage.required_observation_count == 2
    assert version.identity.coverage.coverage_complete is True


@pytest.mark.parametrize("symbol", ["4590", "6589", "7740"])
def test_identity_mismatch_fixtures_are_rejected_before_ds3(
    symbol: str,
) -> None:
    result = evaluate_supplemental(
        _candidate(symbol=symbol, esun_count=1, returned_symbol="2330")
    )
    assert result.eligible is False
    with pytest.raises(DatasetPersistenceContractError):
        build_provisional_dataset_version(
            symbol=symbol,
            as_of_date=BASE_DATE,
            required_observation_count=1,
            twse_observations=(),
            eligibility_result=result,
            esun_observations=(),
            artifacts=(_artifact(1, "esun", symbol),),
            methodology_version="dataset-v1.1",
        )


def test_migration_fresh_v11_to_v12_preserves_old_rows_and_integrity(
    tmp_path: Path,
) -> None:
    database_path = _v11_database(tmp_path, symbols=("6108",))
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO daily_prices (symbol, trade_date, open_price, high_price, "
            "low_price, close_price, volume, source) VALUES "
            "('6108', '2026-01-01', 100, 105, 95, 102, 1000, 'twse')"
        )
        connection.execute(
            "INSERT INTO research_notes (symbol, created_at, analysis_type, title, "
            "summary, source_data_start, source_data_end, provider_source) VALUES "
            "('6108', '2026-01-01T00:00:00+00:00', 'test', 'old', 'preserve', "
            "'2026-01-01', '2026-01-01', 'twse')"
        )

    result = DatasetVersionMigrationRunner(database_path).migrate(isolated=True)
    assert result.applied is True
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0] == 12
        assert connection.execute(
            "SELECT name FROM schema_migrations WHERE version = 12"
        ).fetchone()[0] == "dual-source dataset versions"
        assert connection.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM research_notes").fetchone()[0] == 1
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_migration_replay_is_strict_noop_and_requires_isolation(tmp_path: Path) -> None:
    database_path = _v11_database(tmp_path)
    runner = DatasetVersionMigrationRunner(database_path)
    with pytest.raises(DatasetMigrationStateError, match="isolated=True"):
        runner.migrate()
    runner.migrate(isolated=True)
    first_bytes = database_path.read_bytes()
    second = runner.migrate(isolated=True)
    assert second.applied is False
    assert database_path.read_bytes() == first_bytes


def test_migration_rejects_frozen_production_database_path() -> None:
    production = Path(__file__).parents[1] / "data" / "research.db"
    with pytest.raises(DatasetMigrationStateError, match="production"):
        DatasetVersionMigrationRunner(production).migrate(isolated=True)


def test_explicit_production_activation_migration_requires_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    production = _v11_database(tmp_path)
    backup = tmp_path / "verified-v11-backup.db"
    backup.write_bytes(production.read_bytes())
    monkeypatch.setattr(
        dataset_versions_module,
        "_PRODUCTION_DATABASE_PATH",
        production.resolve(),
    )
    source_sha256 = hashlib.sha256(production.read_bytes()).hexdigest()
    backup_sha256 = hashlib.sha256(backup.read_bytes()).hexdigest()
    runner = DatasetVersionMigrationRunner(production)

    first = runner.migrate_production_for_activation(
        expected_source_sha256=source_sha256,
        verified_backup_path=backup,
        verified_backup_sha256=backup_sha256,
    )
    assert first.applied is True

    post_migration_sha256 = hashlib.sha256(production.read_bytes()).hexdigest()
    second = runner.migrate_production_for_activation(
        expected_source_sha256=post_migration_sha256,
        verified_backup_path=backup,
        verified_backup_sha256=backup_sha256,
    )
    assert second.applied is False


def test_migration_fault_rolls_back_all_v12_objects(tmp_path: Path) -> None:
    database_path = _v11_database(tmp_path)

    def inject(point: str) -> None:
        if point == "after_tables":
            raise RuntimeError("injected DS3 migration failure")

    with pytest.raises(RuntimeError, match="injected DS3"):
        DatasetVersionMigrationRunner(database_path).migrate(
            isolated=True, fault_injector=inject
        )
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0] == 11
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'research_dataset_%'"
        ).fetchone()[0] == 0


def test_corrupted_recorded_migration_fails_closed(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE schema_migrations SET name='tampered' WHERE version=12"
        )
    with pytest.raises(DatasetMigrationStateError, match="0012"):
        DatasetVersionMigrationRunner(database_path).migrate(isolated=True)


def test_6108_persist_reconstruct_and_replay_zero_write(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path)
    version, _ = _provisional_6108()
    repository = DatasetVersionRepository(database_path)

    first = repository.save(version)
    before_replay = database_path.read_bytes()
    second = repository.save(version)
    replayed = repository.replay(first.dataset_version_id)

    assert first.written is True
    assert second.written is False
    assert second.created_at == first.created_at
    assert second.dataset_version_id == version.identity.dataset_version_id
    assert second.canonical_sha256 == version.canonical_sha256
    assert replayed.as_dict() == version.as_dict()
    assert database_path.read_bytes() == before_replay
    assert _row_counts(database_path) == (1, 250, 2)


def test_canonical_and_legal_short_versions_persist(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path, symbols=("3044", "6589"))
    repository = DatasetVersionRepository(database_path)

    canonical = _canonical_3044()
    legal_short = _canonical_legal_short()
    canonical_result = repository.save(canonical)
    legal_result = repository.save(legal_short)

    assert repository.replay(canonical_result.dataset_version_id).as_dict() == canonical.as_dict()
    assert repository.replay(legal_result.dataset_version_id).coverage_basis is CoverageBasis.LEGAL_SHORT_LISTING_HISTORY
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM research_dataset_observations "
            "WHERE provider='esun' AND source_role='validation' AND selected=1"
        ).fetchone()[0] == 0
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_repository_rejects_incomplete_schema_and_old_v11_remains_untouched(
    tmp_path: Path,
) -> None:
    database_path = _v11_database(tmp_path)
    version, _ = _provisional_6108()
    with pytest.raises(DatasetMigrationStateError, match="v12"):
        DatasetVersionRepository(database_path).save(version)
    assert _row_counts.__name__ == "_row_counts"  # keep this test read-only on v11
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()[0] == 11


@pytest.mark.parametrize(
    "fault_point",
    ["before_version_insert", "after_version_insert", "after_observations", "after_artifacts", "before_commit"],
)
def test_repository_atomic_fault_rolls_back_every_child(
    tmp_path: Path, fault_point: str
) -> None:
    database_path = _v12_database(tmp_path)
    version, _ = _provisional_6108()

    def inject(point: str) -> None:
        if point == fault_point:
            raise RuntimeError(f"injected {point}")

    with pytest.raises(RuntimeError, match=fault_point):
        DatasetVersionRepository(database_path).save(
            version, fault_injector=inject
        )
    assert _row_counts(database_path) == (0, 0, 0)


@pytest.mark.parametrize(
    "tamper",
    ["canonical_sha", "methodology", "observation_hash", "ohlc", "trade_date", "run_id", "coverage"],
)
def test_tampered_version_or_observation_fails_closed(
    tmp_path: Path, tamper: str
) -> None:
    database_path = _v12_database(tmp_path)
    version, _ = _provisional_6108()
    repository = DatasetVersionRepository(database_path)
    repository.save(version)
    dataset_id = version.identity.dataset_version_id
    with sqlite3.connect(database_path) as connection:
        if tamper == "canonical_sha":
            connection.execute(
                "UPDATE research_dataset_versions SET canonical_sha256=? "
                "WHERE dataset_version_id=?",
                ("f" * 64, dataset_id),
            )
        elif tamper == "methodology":
            connection.execute(
                "UPDATE research_dataset_versions SET methodology_version='tampered' "
                "WHERE dataset_version_id=?",
                (dataset_id,),
            )
        elif tamper == "observation_hash":
            connection.execute(
                "UPDATE research_dataset_observations SET observation_sha256=? "
                "WHERE dataset_version_id=? AND trade_date='2026-01-01'",
                ("e" * 64, dataset_id),
            )
        elif tamper == "ohlc":
            connection.execute(
                "UPDATE research_dataset_observations SET open_price=101 "
                "WHERE dataset_version_id=? AND trade_date='2026-01-01'",
                (dataset_id,),
            )
        elif tamper == "trade_date":
            connection.execute(
                "UPDATE research_dataset_observations SET trade_date='2030-01-01' "
                "WHERE dataset_version_id=? AND trade_date='2026-01-01'",
                (dataset_id,),
            )
        elif tamper == "run_id":
            connection.execute(
                "UPDATE research_dataset_observations SET source_run_id='tampered-run' "
                "WHERE dataset_version_id=? AND trade_date='2026-01-01'",
                (dataset_id,),
            )
        elif tamper == "coverage":
            connection.execute(
                "UPDATE research_dataset_versions SET discrepancy_count=1 "
                "WHERE dataset_version_id=?",
                (dataset_id,),
            )

    with pytest.raises(DatasetPersistenceIntegrityError):
        repository.replay(dataset_id)


@pytest.mark.parametrize(
    ("provider", "role", "selected"),
    [
        ("esun", "canonical", 1),
        ("twse", "supplemental", 1),
        ("esun", "validation", 1),
    ],
)
def test_database_rejects_invalid_source_role_selection(
    tmp_path: Path, provider: str, role: str, selected: int
) -> None:
    database_path = _v12_database(tmp_path)
    version, _ = _provisional_6108()
    DatasetVersionRepository(database_path).save(version)
    dataset_id = version.identity.dataset_version_id
    with sqlite3.connect(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO research_dataset_observations (
                    dataset_version_id, symbol, trade_date, provider, source_role,
                    source_run_id, selected, open_price, high_price, low_price,
                    close_price, volume, observation_sha256
                ) VALUES (?, '6108', '2027-01-01', ?, ?, 'invalid-role-run', ?,
                          100, 105, 95, 102, 1, ?)
                """,
                (dataset_id, provider, role, selected, _hash(f"invalid-{provider}-{role}")),
            )


def test_database_rejects_duplicate_selected_trade_date(tmp_path: Path) -> None:
    database_path = _v12_database(tmp_path)
    version, _ = _provisional_6108()
    DatasetVersionRepository(database_path).save(version)
    dataset_id = version.identity.dataset_version_id
    with sqlite3.connect(database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO research_dataset_observations (
                    dataset_version_id, symbol, trade_date, provider, source_role,
                    source_run_id, selected, open_price, high_price, low_price,
                    close_price, volume, observation_sha256
                ) VALUES (?, '6108', '2026-01-01', 'esun', 'supplemental',
                          'duplicate-date-run', 1, 100, 105, 95, 102, 1, ?)
                """,
                (dataset_id, _hash("duplicate-date")),
            )


def test_source_run_and_artifact_credential_material_is_rejected() -> None:
    with pytest.raises(DatasetPersistenceContractError, match="source_run_id"):
        _observation(
            0,
            symbol="6108",
            provider="twse",
            role=SourceRole.CANONICAL,
            run_id="Bearer secret-value",
            selected=True,
        )
    with pytest.raises(DatasetPersistenceContractError, match="source_ref"):
        DatasetArtifactRef(
            ordinal=1,
            provider="twse",
            dataset="twse",
            source_ref="https://example.invalid/data?token=secret",
            contract_version="twse-v1",
            payload_sha256=_hash("artifact"),
            payload_size_bytes=1,
            hash_basis="raw-response-bytes-v1",
        )
