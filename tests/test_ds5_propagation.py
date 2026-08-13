"""Focused DS5 M9 -> S4 -> S6A propagation checks."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
import hashlib
import json
import sqlite3

import pytest

from app.research_dataset import ResearchDatasetRequest
from app.data_contracts.dataset_persistence import (
    build_canonical_dataset_version,
    build_provisional_dataset_version,
)
from app.data_contracts.dual_source import SourceRole
from app.data_contracts.reconciliation import reconcile_dataset_version
from app.reporting.screener_report import (
    REPORT_CONTRACT_VERSION_V2,
    ScreenerReportArtifactWriter,
    ScreenerReportGenerator,
)
from app.sqlite_dataset_version import (
    DatasetVersionReadError,
    SQLiteDatasetVersionResearchDataset,
)
from app.screener.stage2 import Stage2Provenance
from app.screener.stage1_dataset import Stage1CompositionError, scan_stage1_from_dataset
from app.screener.stage2_dataset import Stage2CompositionError, research_stage2_from_dataset
from app.storage.candidate_persistence import (
    CandidateInputLocator,
    SQLiteScreenerCheckpointRepository,
)
from app.storage.dataset_versions import DatasetVersionRepository, DatasetVersionMigrationRunner
from app.storage.screener_replay import SQLiteScreenerReplayRepository
from app.storage.screener_history import SQLiteScreenerHistoryReader
from app.storage.screener_migration import SQLiteScreenerMigrationRunner

from test_dataset_persistence import _canonical_3044, _provisional_6108, _v12_database
from test_reconciliation import _parent_6108, _reconciliation_input
from test_screener_s4_candidate_persistence import (
    _stage1_result,
    _setup_run,
    _success_candidate,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _version_for_symbol(symbol: str):
    version, eligibility = _provisional_6108()
    return build_provisional_dataset_version(
        symbol=symbol,
        as_of_date=version.identity.as_of_date,
        required_observation_count=version.identity.coverage.required_observation_count,
        twse_observations=tuple(
            replace(item, symbol=symbol)
            for item in version.observations
            if item.source_role is SourceRole.CANONICAL
        ),
        eligibility_result=eligibility,
        esun_observations=tuple(
            replace(item, symbol=symbol)
            for item in version.observations
            if item.source_role is SourceRole.SUPPLEMENTAL
        ),
        artifacts=version.artifacts,
        methodology_version=version.identity.methodology_version,
    )


def _canonical_version_for_symbol(symbol: str):
    version = _canonical_3044()
    return build_canonical_dataset_version(
        symbol=symbol,
        as_of_date=version.identity.as_of_date,
        required_observation_count=version.identity.coverage.required_observation_count,
        twse_observations=tuple(
            replace(item, symbol=symbol)
            for item in version.observations
            if item.source_role is SourceRole.CANONICAL
        ),
        validation_observations=tuple(
            replace(item, symbol=symbol)
            for item in version.observations
            if item.source_role is SourceRole.VALIDATION
        ),
        artifacts=version.artifacts,
        methodology_version=version.identity.methodology_version,
        discrepancy_count=version.identity.coverage.discrepancy_count,
    )


def test_m9_requires_explicit_dataset_version_and_preserves_roles(tmp_path) -> None:
    database = _v12_database(tmp_path, symbols=("6108",))
    version, unused_eligibility = _provisional_6108()
    DatasetVersionRepository(database).save(version)
    dataset = SQLiteDatasetVersionResearchDataset(database)

    try:
        dataset.read(ResearchDatasetRequest("6108", date(2026, 9, 7), history_observations=250))
    except DatasetVersionReadError as error:
        assert "explicit dataset_version_id" in str(error)
    else:  # pragma: no cover - fail closed assertion
        raise AssertionError("implicit v12 lookup unexpectedly succeeded")

    snapshot = dataset.read(
        ResearchDatasetRequest(
            "6108",
            date(2026, 9, 7),
            history_observations=250,
            dataset_version_id=version.identity.dataset_version_id,
        )
    )
    assert snapshot.provenance.source_policy == "twse_dual_source_v1"
    assert snapshot.provenance.research_data_quality == "provisional"
    assert len(snapshot.price_history.observations) == 250
    assert sum(item.source_role == "canonical" for item in snapshot.price_history.observations) == 228
    assert sum(item.source_role == "supplemental" for item in snapshot.price_history.observations) == 22
    assert all(item != "esun" for item in snapshot.provenance.canonical_sources)
    assert snapshot.provenance.supplemental_sources == ("esun",)


def test_dataset_compositions_reject_partial_explicit_version_maps() -> None:
    # The legacy path remains available only when no DS5 map is supplied.  A
    # supplied map is an explicit handoff and must cover the complete consumer set.
    from test_screener_s2_stage1 import _universe

    universe = _universe("2330", "2317")
    with pytest.raises(Stage1CompositionError, match="explicitly cover"):
        scan_stage1_from_dataset(
            universe=universe,
            dataset=object(),
            as_of_date=universe.market_date,
            candidate_limit=2,
            dataset_version_ids={"2330": "a" * 64},
        )

    from test_screener_s3_stage2 import _stage1_candidate, _stage1_result

    stage1_result = _stage1_result(
        _stage1_candidate("2330", rank=1),
        _stage1_candidate("2317", rank=2),
    )
    with pytest.raises(Stage2CompositionError, match="explicitly cover"):
        research_stage2_from_dataset(
            stage1_result=stage1_result,
            dataset=object(),
            market_date=stage1_result.market_date,
            dataset_version_ids={stage1_result.candidates[0].symbol: "a" * 64},
        )


def test_provisional_s4_and_v2_report_are_replay_stable(tmp_path) -> None:
    database, unused_repository, unused_old_run, stage1_candidates, unused_locators = _setup_run(
        tmp_path,
        count=1,
        name="ds5-provisional.db",
    )
    DatasetVersionMigrationRunner(database).migrate(isolated=True)
    version = _version_for_symbol(stage1_candidates[0].symbol)
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT OR IGNORE INTO symbols (symbol, name, market, currency, is_active) "
            "VALUES (?, ?, 'TSE', 'TWD', 1)",
            (stage1_candidates[0].symbol, f"Company {stage1_candidates[0].symbol}"),
        )
    DatasetVersionRepository(database).save(version)

    with sqlite3.connect(database) as connection:
        universe_run_id = connection.execute(
            "SELECT universe_run_id FROM screener_runs ORDER BY created_at LIMIT 1"
        ).fetchone()[0]

    candidate = _success_candidate(stage1_candidates[0])
    candidate = replace(
        candidate,
        provenance=replace(
            candidate.provenance,
            source_policy="twse_dual_source_v1",
            dataset_version_id=version.identity.dataset_version_id,
            source_status="provisional_mixed",
            authority_status="incomplete",
            reconciliation_status="pending",
            research_data_quality="provisional",
            supplemental_sources=("esun",),
            twse_observation_count=228,
            esun_supplemental_count=22,
            missing_twse_count=22,
            discrepancy_count=0,
            provenance_map_sha256=version.identity.provenance_map_sha256,
        ),
    )
    locator = CandidateInputLocator.from_stage2_provenance(
        symbol=candidate.symbol,
        provenance=candidate.provenance,
    )
    repository = SQLiteScreenerCheckpointRepository(database)
    run = repository.create_run(
        universe_run_id=universe_run_id,
        stage1_result=_stage1_result(stage1_candidates),
        candidate_locators=(locator,),
    )
    persisted = repository.persist_candidate(
        screener_run_id=run.screener_run_id,
        candidate=candidate,
        research_locator_sha256=locator.research_locator_sha256,
        snapshot_sha256=_sha("snapshot-provisional"),
    )
    assert persisted.checkpoint.provenance.research_data_quality == "provisional"

    replay = SQLiteScreenerReplayRepository(database)
    sealed = replay.finalize_run(run.screener_run_id)
    assert sealed.result.execution_status == "provisional_success"
    assert sealed.result.contract_version == "screener-frozen-result-v2"
    first_sha = sealed.result.payload_sha256

    before = database.read_bytes()
    replayed = replay.replay_run(run.screener_run_id)
    assert replayed.written is False
    assert replayed.result.payload_sha256 == first_sha
    assert database.read_bytes() == before

    generated = ScreenerReportGenerator(database).generate(run.screener_run_id)
    assert generated.report.report_contract_version == REPORT_CONTRACT_VERSION_V2
    assert generated.report.as_dict()["data_quality"] == "provisional"
    assert "PROVISIONAL" in generated.markdown
    assert "E.SUN" in generated.markdown
    written = ScreenerReportArtifactWriter().write(generated, output_directory=tmp_path / "reports")
    assert written.json_artifact.path.name.endswith("-v2.json")


def test_mixed_canonical_and_provisional_run_is_provisional_success(tmp_path) -> None:
    database, repository, unused_old_run, stage1_candidates, unused_locators = _setup_run(
        tmp_path,
        count=30,
        name="ds5-mixed-28-2.db",
    )
    DatasetVersionMigrationRunner(database).migrate(isolated=True)
    with sqlite3.connect(database) as connection:
        connection.executemany(
            "INSERT OR IGNORE INTO symbols (symbol, name, market, currency, is_active) "
            "VALUES (?, ?, 'TSE', 'TWD', 1)",
            [(candidate.symbol, candidate.name) for candidate in stage1_candidates],
        )

    versions = {}
    for index, candidate in enumerate(stage1_candidates):
        version = (
            _canonical_version_for_symbol(candidate.symbol)
            if index < 28
            else _version_for_symbol(candidate.symbol)
        )
        DatasetVersionRepository(database).save(version)
        versions[candidate.symbol] = version

    with sqlite3.connect(database) as connection:
        universe_run_id = connection.execute(
            "SELECT universe_run_id FROM screener_runs ORDER BY created_at LIMIT 1"
        ).fetchone()[0]

    candidates = []
    locators = []
    for stage1 in stage1_candidates:
        version = versions[stage1.symbol]
        summary = version.provenance_summary
        candidate = _success_candidate(stage1)
        candidate = replace(
            candidate,
            provenance=replace(
                candidate.provenance,
                canonical_sources=(summary.canonical_authority,),
                validation_sources=tuple(
                    sorted(
                        set(summary.validation_sources)
                        | set(candidate.provenance.validation_sources)
                    )
                ),
                source_policy=version.identity.source_policy,
                dataset_version_id=version.identity.dataset_version_id,
                source_status=version.identity.source_status.value,
                authority_status=version.identity.authority_status.value,
                reconciliation_status=version.identity.reconciliation_status.value,
                research_data_quality=(
                    "provisional"
                    if version.identity.source_status.value == "provisional_mixed"
                    else "canonical"
                ),
                canonical_authority=summary.canonical_authority,
                supplemental_sources=summary.supplemental_sources,
                twse_observation_count=version.identity.coverage.twse_observation_count,
                esun_supplemental_count=version.identity.coverage.esun_supplemental_count,
                missing_twse_count=version.identity.coverage.missing_twse_count,
                discrepancy_count=version.identity.coverage.discrepancy_count,
                provenance_map_sha256=version.identity.provenance_map_sha256,
            ),
        )
        candidates.append(candidate)
        locators.append(
            CandidateInputLocator.from_stage2_provenance(
                symbol=candidate.symbol,
                provenance=candidate.provenance,
            )
        )

    run = repository.create_run(
        universe_run_id=universe_run_id,
        stage1_result=_stage1_result(stage1_candidates),
        candidate_locators=tuple(locators),
    )
    for candidate, locator in zip(candidates, locators):
        repository.persist_candidate(
            screener_run_id=run.screener_run_id,
            candidate=candidate,
            research_locator_sha256=locator.research_locator_sha256,
            snapshot_sha256=_sha(f"snapshot-mixed-{candidate.symbol}"),
        )

    replay = SQLiteScreenerReplayRepository(database)
    sealed = replay.finalize_run(run.screener_run_id)
    assert sealed.result.execution_status == "provisional_success"
    assert sealed.result.research_data_quality == "provisional"
    assert sealed.result.supplemental_candidate_count == 2
    assert len(sealed.result.dataset_version_ids) == 30
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT status, ds5_execution_status, ds5_research_data_quality "
            "FROM screener_runs WHERE screener_run_id = ?",
            (run.screener_run_id,),
        ).fetchone()
    # v11's CHECK-constrained status stays the compatibility value; DS5's
    # explicit terminal status is persisted in the additive DS5 column.
    assert row == ("success", "provisional_success", "provisional")

    generated = ScreenerReportGenerator(database).generate(run.screener_run_id)
    assert generated.report.report_contract_version == REPORT_CONTRACT_VERSION_V2
    assert generated.report.as_dict()["data_quality"] == "provisional"
    assert generated.report.as_dict()["supplemental_candidate_count"] == 2
    assert generated.report.as_dict()["esun_supplemental_count"] == 44
    assert "PROVISIONAL" in generated.markdown
    history = SQLiteScreenerHistoryReader(database)
    daily = history.daily_candidate_set(date(2026, 8, 7), screener_run_id=run.screener_run_id)
    assert len(daily) == 30
    assert sum(item.supplemental_count > 0 for item in daily) == 2
    assert history.supplemental_candidate_count(
        date(2026, 8, 7), screener_run_id=run.screener_run_id
    ) == 2
    assert history.pending_reconciliation_count(
        date(2026, 8, 7), screener_run_id=run.screener_run_id
    ) == 1
    frequency = history.quality_frequency(date(2026, 8, 7))
    assert frequency[-1].research_data_quality == "provisional"
    assert frequency[-1].supplemental_candidate_count == 2


def test_canonical_validation_discrepancy_remains_canonical_not_provisional(tmp_path) -> None:
    database = _v12_database(tmp_path, symbols=("3044",))
    version = _canonical_3044()
    DatasetVersionRepository(database).save(version)
    snapshot = SQLiteDatasetVersionResearchDataset(database).read(
        ResearchDatasetRequest(
            "3044",
            version.identity.as_of_date,
            history_observations=250,
            dataset_version_id=version.identity.dataset_version_id,
        )
    )
    assert snapshot.provenance.research_data_quality == "canonical"
    assert snapshot.provenance.source_status == "canonical_complete"
    assert snapshot.provenance.discrepancy_count == 1
    assert snapshot.validation.status == "source_discrepancy"
    assert snapshot.provenance.canonical_sources == ("twse",)
    assert snapshot.provenance.supplemental_sources == ()


def test_reconciled_child_is_a_new_m9_identity_and_keeps_esun_as_validation(tmp_path) -> None:
    database = _v12_database(tmp_path, symbols=("6108",))
    parent = _parent_6108()
    child = reconcile_dataset_version(
        _reconciliation_input(parent, tuple(range(228, 250)))
    ).new_dataset_version
    repository = DatasetVersionRepository(database)
    repository.save(parent)
    repository.save(child)
    assert child.identity.dataset_version_id != parent.identity.dataset_version_id

    snapshot = SQLiteDatasetVersionResearchDataset(database).read(
        ResearchDatasetRequest(
            "6108",
            child.identity.as_of_date,
            history_observations=250,
            dataset_version_id=child.identity.dataset_version_id,
        )
    )
    assert snapshot.provenance.research_data_quality == "reconciled"
    assert snapshot.provenance.source_status == "reconciled"
    assert snapshot.provenance.supplemental_sources == ()
    assert snapshot.provenance.validation_sources == ("esun",)
    assert snapshot.provenance.parent_dataset_version_id == parent.identity.dataset_version_id


def test_v12_migration_is_still_isolated_and_production_v11_unchanged(tmp_path) -> None:
    database = _v12_database(tmp_path, symbols=("6108",))
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 12
        assert connection.execute(
            "SELECT ds5_execution_status, ds5_source_policy FROM screener_runs"
        ).fetchone() is None

    legacy = tmp_path / "legacy.db"
    from app.storage import SQLiteResearchRepository

    SQLiteResearchRepository(legacy).initialize()
    SQLiteScreenerMigrationRunner(legacy).migrate()
    with sqlite3.connect(legacy) as connection:
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 11
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'research_dataset_%'"
        ).fetchone()[0] == 0


def test_v12_keeps_legacy_locator_defaults_without_reinterpreting_v1(tmp_path) -> None:
    database, repository, unused_old_run, stage1_candidates, legacy_hashes = _setup_run(
        tmp_path,
        count=1,
        name="ds5-v12-legacy-compat.db",
    )
    DatasetVersionMigrationRunner(database).migrate(isolated=True)
    with sqlite3.connect(database) as connection:
        universe_run_id = connection.execute(
            "SELECT universe_run_id FROM screener_runs ORDER BY created_at LIMIT 1"
        ).fetchone()[0]
    run = repository.create_run(
        universe_run_id=universe_run_id,
        stage1_result=_stage1_result(stage1_candidates),
        candidate_locators=(
            CandidateInputLocator(
                stage1_candidates[0].symbol,
                legacy_hashes[stage1_candidates[0].symbol],
            ),
        ),
    )
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT ds5_execution_status, ds5_source_policy, ds5_source_status, "
            "ds5_research_data_quality FROM screener_runs WHERE screener_run_id = ?",
            (run.screener_run_id,),
        ).fetchone()
    assert row == ("success", "twse_baseline", "canonical_complete", "canonical")


def test_v12_legacy_candidate_completion_refreshes_ds5_metadata_without_identity(
    tmp_path,
) -> None:
    database, repository, unused_old_run, stage1_candidates, legacy_hashes = _setup_run(
        tmp_path,
        count=1,
        name="ds5-v12-legacy-completion.db",
    )
    DatasetVersionMigrationRunner(database).migrate(isolated=True)
    with sqlite3.connect(database) as connection:
        universe_run_id = connection.execute(
            "SELECT universe_run_id FROM screener_runs ORDER BY created_at LIMIT 1"
        ).fetchone()[0]
    run = repository.create_run(
        universe_run_id=universe_run_id,
        stage1_result=_stage1_result(stage1_candidates),
        candidate_locators=(
            CandidateInputLocator(
                stage1_candidates[0].symbol,
                legacy_hashes[stage1_candidates[0].symbol],
            ),
        ),
    )

    persisted = repository.persist_candidate(
        screener_run_id=run.screener_run_id,
        candidate=_success_candidate(stage1_candidates[0]),
        research_locator_sha256=legacy_hashes[stage1_candidates[0].symbol],
        snapshot_sha256=_sha("snapshot-legacy-completion"),
    )

    assert persisted.checkpoint.checkpoint_status == "success"
    SQLiteScreenerReplayRepository(database).finalize_run(run.screener_run_id)
    with sqlite3.connect(database) as connection:
        row = connection.execute(
            "SELECT status, ds5_execution_status, ds5_source_policy, "
            "ds5_research_data_quality FROM screener_runs WHERE screener_run_id = ?",
            (run.screener_run_id,),
        ).fetchone()
    assert row == ("success", "success", "twse_baseline", "canonical")
