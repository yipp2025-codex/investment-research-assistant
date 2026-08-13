from __future__ import annotations

import ast
import hashlib
import json
import sqlite3
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.reporting.screener_markdown import _cell
from app.reporting.screener_report import (
    ScreenerReportArtifactWriter,
    ScreenerReportCollisionError,
    ScreenerReportGenerator,
    ScreenerReportInputError,
)
from app.screener.orchestration import Stage1Execution, Stage2CandidateExecution
from app.screener.stage1 import (
    PRICE_CHANGE_1D,
    STAGE1_METHODOLOGY_VERSION,
    DataQualityStatus,
    MetricStatus,
    Stage1Candidate,
    Stage1DataQuality,
    Stage1Metric,
    Stage1Reason,
    Stage1ScanResult,
)
from app.screener.stage2 import (
    MA_DISTANCE_120D,
    MA_DISTANCE_20D,
    MA_DISTANCE_250D,
    MA_DISTANCE_60D,
    MAX_DRAWDOWN_120D,
    MAX_DRAWDOWN_20D,
    MAX_DRAWDOWN_250D,
    MAX_DRAWDOWN_60D,
    RETURN_120D,
    RETURN_20D,
    RETURN_60D,
    STAGE2_METHODOLOGY_VERSION,
    VALUATION_PB,
    VALUATION_PE,
    VALUATION_YIELD,
    VOLATILITY_60D,
    VOLUME_RATIO_20D,
    Stage2AnalysisStatus,
    Stage2ArtifactRef,
    Stage2Candidate,
    Stage2CandidateKind,
    Stage2DataQuality,
    Stage2Metric,
    Stage2MetricStatus,
    Stage2Provenance,
    Stage2QualityStatus,
    Stage2Reason,
)
from app.screener.universe import (
    UNIVERSE_METHODOLOGY_VERSION,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)
from app.screener.orchestration import DailyScreenerOrchestrator
from app.storage import SQLiteResearchRepository
from app.storage.candidate_persistence import CandidateInputLocator
from app.storage.screener_migration import SQLiteScreenerMigrationRunner
from app.storage.screener_replay import ScreenerReplayIntegrityError


MARKET_DATE = date(2026, 8, 7)
SYMBOLS = ("1001", "1002", "1003")
METRIC_ORDER = (
    RETURN_20D,
    RETURN_60D,
    RETURN_120D,
    VOLATILITY_60D,
    MAX_DRAWDOWN_20D,
    MAX_DRAWDOWN_60D,
    MAX_DRAWDOWN_120D,
    MAX_DRAWDOWN_250D,
    VOLUME_RATIO_20D,
    MA_DISTANCE_20D,
    MA_DISTANCE_60D,
    MA_DISTANCE_120D,
    MA_DISTANCE_250D,
    VALUATION_PE,
    VALUATION_PB,
    VALUATION_YIELD,
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _initialize(database_path: Path) -> None:
    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    assert repository.get_schema_version() == 10
    SQLiteScreenerMigrationRunner(database_path).migrate()


def _universe() -> MarketUniverseSnapshot:
    evidence = (
        SourceEvidence(
            source="twse",
            dataset="s6a-test-universe",
            source_ref="contract://s6a-tests/universe",
            contract_version="s6a-test-v1",
            payload_sha256=_hash("s6a-universe"),
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
        for symbol in SYMBOLS
    )
    return MarketUniverseSnapshot(
        market_date=MARKET_DATE,
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


def _stage1_candidate(symbol: str, rank: int) -> Stage1Candidate:
    reason = Stage1Reason(
        code="price_change_1d_threshold",
        metric=PRICE_CHANGE_1D,
        component=None,
        previous=1.0,
        current=4.0,
        delta=3.0,
        unit="percent",
        operator="abs_current_gte",
        threshold=3.0,
        rule_version=STAGE1_METHODOLOGY_VERSION,
        role="primary",
        trigger_class="price_change",
        threshold_multiple=4.0 / 3.0,
    )
    metric = Stage1Metric(
        metric=PRICE_CHANGE_1D,
        component=None,
        status=MetricStatus.AVAILABLE,
        previous=1.0,
        current=4.0,
        delta=3.0,
        value_unit="percent",
        delta_unit="percentage_point",
        previous_as_of_date=MARKET_DATE - timedelta(days=1),
        current_as_of_date=MARKET_DATE,
    )
    return Stage1Candidate(
        symbol=symbol,
        name=f"Company {symbol}",
        rank=rank,
        reasons=(reason,),
        metrics=(metric,),
        data_quality=Stage1DataQuality(
            status=DataQualityStatus.CLEAN,
            issues=(),
        ),
    )


def _stage1_execution(universe: MarketUniverseSnapshot) -> Stage1Execution:
    candidates = tuple(
        _stage1_candidate(symbol, rank)
        for rank, symbol in enumerate(SYMBOLS, 1)
    )
    result = Stage1ScanResult(
        market_date=MARKET_DATE,
        methodology_version=STAGE1_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=universe.universe_count,
        screened_count=universe.scan_eligible_count,
        triggered_count=len(candidates),
        candidate_count=len(candidates),
        candidate_limit=len(candidates),
        truncated=False,
        candidates=candidates,
    )
    return Stage1Execution(
        result=result,
        candidate_locators=tuple(
            CandidateInputLocator(symbol, _hash(f"locator-{symbol}"))
            for symbol in SYMBOLS
        ),
    )


def _stage2_candidate(
    stage1: Stage1Candidate,
    *,
    unavailable_first_metric: bool = False,
) -> Stage2Candidate:
    metrics = tuple(
        Stage2Metric(
            name=name,
            status=(
                Stage2MetricStatus.INSUFFICIENT_HISTORY
                if unavailable_first_metric and index == 0
                else Stage2MetricStatus.AVAILABLE
            ),
            value=None if unavailable_first_metric and index == 0 else float(index + 1),
            previous_value=None if unavailable_first_metric and index == 0 else float(index),
            delta=None if unavailable_first_metric and index == 0 else 1.0,
            unit="ratio" if name in {VOLUME_RATIO_20D, VALUATION_PE, VALUATION_PB} else "percent",
            as_of_date=MARKET_DATE,
            previous_as_of_date=MARKET_DATE - timedelta(days=1),
            observations=250,
            previous_observations=249,
        )
        for index, name in enumerate(METRIC_ORDER)
    )
    reason = Stage2Reason(
        code="return_60d_change",
        metric=RETURN_60D,
        previous=2.0,
        current=4.0,
        delta=2.0,
        unit="percentage_point",
        operator="abs_delta_gte",
        threshold=1.0,
        rule_version=STAGE2_METHODOLOGY_VERSION,
        reason_kind="research_change",
        reason_class="return_change",
        threshold_multiple=2.0,
    )
    artifact = Stage2ArtifactRef(
        owner_kind="historical",
        owner_run_id=_hash(f"historical-{stage1.symbol}"),
        provider="twse",
        dataset="s6a-test-history",
        endpoint="https://evidence.invalid/s6a/history",
        contract_version="s6a-test-v1",
        payload_sha256=_hash(f"artifact-{stage1.symbol}"),
        payload_size_bytes=128,
        hash_basis="canonical-json-v1",
    )
    return Stage2Candidate(
        rank=stage1.rank,
        stage1_rank=stage1.rank,
        symbol=stage1.symbol,
        name=stage1.name,
        market="TWSE",
        candidate_kind=Stage2CandidateKind.RESEARCH_CANDIDATE,
        analysis_status=Stage2AnalysisStatus.AVAILABLE,
        stage1_reasons=stage1.reasons,
        stage2_reasons=(reason,),
        metrics=metrics,
        data_quality=Stage2DataQuality(
            status=Stage2QualityStatus.CLEAN,
            validation_status="available",
            discrepancies=(),
        ),
        provenance=Stage2Provenance(
            pipeline_run_id=None,
            historical_run_id=None,
            validation_run_id=None,
            canonical_sources=("twse",),
            validation_sources=(),
            artifact_refs=(artifact,),
        ),
    )


def _orchestrator(
    tmp_path: Path,
    *,
    failure_budget: dict[str, int] | None = None,
    unavailable_first_metric: bool = False,
) -> tuple[DailyScreenerOrchestrator, Counter[str]]:
    database_path = tmp_path / "s6a.db"
    _initialize(database_path)
    calls: Counter[str] = Counter()
    remaining_failures = dict(failure_budget or {})

    def universe_provider(market_date: date) -> MarketUniverseSnapshot:
        calls["universe"] += 1
        assert market_date == MARKET_DATE
        return _universe()

    def stage1_runner(
        universe: MarketUniverseSnapshot, market_date: date
    ) -> Stage1Execution:
        calls["stage1"] += 1
        assert market_date == MARKET_DATE
        return _stage1_execution(universe)

    def stage2_runner(request) -> Stage2CandidateExecution:
        symbol = request.candidate.symbol
        calls[f"stage2:{symbol}"] += 1
        if remaining_failures.get(symbol, 0) > 0:
            remaining_failures[symbol] -= 1
            raise RuntimeError("synthetic candidate failure")
        return Stage2CandidateExecution(
            candidate=_stage2_candidate(
                request.candidate,
                unavailable_first_metric=unavailable_first_metric,
            ),
            research_locator_sha256=request.locator.research_locator_sha256,
            snapshot_sha256=_hash(f"snapshot-{symbol}"),
        )

    return (
        DailyScreenerOrchestrator(
            database_path,
            universe_provider=universe_provider,
            stage1_runner=stage1_runner,
            stage2_runner=stage2_runner,
        ),
        calls,
    )


def _seed_success(
    tmp_path: Path,
    *,
    unavailable_first_metric: bool = False,
) -> tuple[Path, str, object]:
    orchestrator, calls = _orchestrator(
        tmp_path,
        unavailable_first_metric=unavailable_first_metric,
    )
    result = orchestrator.run(MARKET_DATE)
    assert result.status == "success"
    assert calls["universe"] == 1
    return orchestrator.database_path, result.screener_run_id, result


def _seed_running(tmp_path: Path) -> tuple[Path, str]:
    orchestrator, unused_calls = _orchestrator(tmp_path)
    preparation = orchestrator._prepare(MARKET_DATE)
    universe_result = orchestrator.universe_repository.persist(preparation.universe)
    run_result = orchestrator.checkpoint_repository.create_run(
        universe_run_id=universe_result.universe_run_id,
        stage1_result=preparation.stage1.result,
        candidate_locators=preparation.stage1.candidate_locators,
    )
    connection = sqlite3.connect(orchestrator.database_path)
    try:
        connection.execute(
            "UPDATE screener_runs SET status = 'running' WHERE screener_run_id = ?",
            (run_result.screener_run_id,),
        )
        connection.commit()
    finally:
        connection.close()
    return orchestrator.database_path, run_result.screener_run_id


def _database_fingerprint(database_path: Path) -> tuple[str, dict[str, int]]:
    tables = (
        "market_universe_runs",
        "market_universe_members",
        "screener_runs",
        "screener_candidates",
        "candidate_reasons",
        "candidate_metrics",
        "screener_source_artifacts",
        "schema_migrations",
    )
    connection = sqlite3.connect(database_path)
    try:
        counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables
        }
    finally:
        connection.close()
    return hashlib.sha256(database_path.read_bytes()).hexdigest(), counts


def test_s6a_gate1_deterministic_generation_and_contract(tmp_path: Path) -> None:
    database_path, run_id, frozen_result = _seed_success(tmp_path)
    generator = ScreenerReportGenerator(database_path)
    before = _database_fingerprint(database_path)

    first = generator.generate(run_id)
    second = generator.generate(run_id)

    assert first.canonical_json == second.canonical_json
    assert first.markdown == second.markdown
    assert first.report_sha256 == second.report_sha256
    assert first.report.report_id == second.report.report_id
    assert [item.rank for item in first.report.candidates] == [1, 2, 3]
    assert [item.symbol for item in first.report.candidates] == [
        item.symbol for item in frozen_result.candidates
    ]
    payload = json.loads(first.canonical_json)
    assert payload["market_date"] == "2026-08-07"
    assert payload["screener_run_id"] == run_id
    assert payload["screener_canonical_sha256"] == frozen_result.canonical_sha256
    assert payload["methodology_versions"] == {
        "stage1": STAGE1_METHODOLOGY_VERSION,
        "stage2": STAGE2_METHODOLOGY_VERSION,
    }
    assert first.report_sha256 == hashlib.sha256(first.json_bytes).hexdigest()
    assert before == _database_fingerprint(database_path)


@pytest.mark.parametrize("state", ("running", "partial_success", "failed"))
def test_s6a_gate2_rejects_non_success_runs(tmp_path: Path, state: str) -> None:
    if state == "running":
        database_path, run_id = _seed_running(tmp_path)
    elif state == "partial_success":
        orchestrator, unused_calls = _orchestrator(
            tmp_path,
            failure_budget={"1003": 1},
        )
        partial = orchestrator.run(MARKET_DATE)
        assert partial.status == "partial_success"
        database_path, run_id = orchestrator.database_path, partial.screener_run_id
    else:
        orchestrator, unused_calls = _orchestrator(
            tmp_path,
            failure_budget={symbol: 1 for symbol in SYMBOLS},
        )
        failed = orchestrator.run(MARKET_DATE)
        assert failed.status == "failed"
        database_path, run_id = orchestrator.database_path, failed.screener_run_id

    output_directory = tmp_path / "formal-report"
    with pytest.raises(ScreenerReportInputError):
        ScreenerReportGenerator(database_path).generate(run_id)
    assert not output_directory.exists()


@pytest.mark.parametrize("tamper", ("run", "candidate", "reason", "metric", "artifact"))
def test_s6a_gate3_tamper_fails_closed_without_report(
    tmp_path: Path,
    tamper: str,
) -> None:
    database_path, run_id, unused_result = _seed_success(tmp_path)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        candidate = connection.execute(
            "SELECT candidate_id FROM screener_candidates "
            "WHERE screener_run_id = ? ORDER BY rank LIMIT 1",
            (run_id,),
        ).fetchone()
        assert candidate is not None
        candidate_id = candidate["candidate_id"]
        if tamper == "run":
            connection.execute(
                "UPDATE screener_runs SET canonical_sha256 = ? "
                "WHERE screener_run_id = ?",
                ("0" * 64, run_id),
            )
        elif tamper == "candidate":
            connection.execute(
                "UPDATE screener_candidates SET payload_sha256 = ? "
                "WHERE candidate_id = ?",
                ("0" * 64, candidate_id),
            )
        elif tamper == "reason":
            connection.execute(
                "UPDATE candidate_reasons SET code = 'tampered_reason' "
                "WHERE candidate_id = ?",
                (candidate_id,),
            )
        elif tamper == "metric":
            connection.execute(
                "UPDATE candidate_metrics SET value = 999.0 "
                "WHERE candidate_id = ? AND ordinal = 1",
                (candidate_id,),
            )
        else:
            connection.execute(
                "UPDATE screener_source_artifacts SET payload_sha256 = ? "
                "WHERE candidate_id = ?",
                ("0" * 64, candidate_id),
            )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(ScreenerReplayIntegrityError):
        ScreenerReportGenerator(database_path).generate(run_id)
    assert not (tmp_path / "formal-report").exists()


def test_s6a_gate4_report_only_calls_s4_replay_and_has_no_research_imports(
    tmp_path: Path,
) -> None:
    database_path, run_id, unused_result = _seed_success(tmp_path)
    calls: list[str] = []
    replay = SQLiteReplaySpy(
        database_path,
        calls,
    )
    generator = ScreenerReportGenerator(database_path, replay_repository=replay)
    generator.generate(run_id)
    assert calls == [run_id]

    source_path = Path(__file__).parents[1] / "app" / "reporting" / "screener_report.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert not any(
        module.startswith(
            (
                "app.providers",
                "app.pipelines",
                "app.operations",
                "app.reports",
                "app.analysis",
                "app.research_dataset",
                "app.screener.stage1",
                "app.screener.stage2",
                "app.screener.orchestration",
            )
        )
        for module in imported
    )


class SQLiteReplaySpy:
    def __init__(self, database_path: Path, calls: list[str]) -> None:
        from app.storage.screener_replay import SQLiteScreenerReplayRepository

        self.repository = SQLiteScreenerReplayRepository(database_path)
        self.calls = calls

    def replay_run(self, screener_run_id: str):
        self.calls.append(screener_run_id)
        return self.repository.replay_run(screener_run_id)


def test_s6a_gate5_null_fidelity_and_explicit_markdown_unavailable(
    tmp_path: Path,
) -> None:
    database_path, run_id, unused_result = _seed_success(
        tmp_path,
        unavailable_first_metric=True,
    )
    generation = ScreenerReportGenerator(database_path).generate(run_id)
    payload = json.loads(generation.canonical_json)
    metric = payload["candidates"][0]["metrics"][0]
    assert metric["status"] == "insufficient_history"
    assert metric["value"] is None
    assert metric["previous_value"] is None
    assert metric["delta"] is None
    assert "N/A (unavailable)" in generation.markdown
    assert "| 0 |" not in generation.markdown


def test_s6a_gate6_preserves_frozen_ordering_and_provenance(tmp_path: Path) -> None:
    database_path, run_id, frozen_result = _seed_success(tmp_path)
    report = ScreenerReportGenerator(database_path).generate(run_id).report

    assert [item.rank for item in report.candidates] == [
        item.rank for item in frozen_result.candidates
    ]
    for report_candidate, frozen_candidate in zip(
        report.candidates,
        frozen_result.candidates,
    ):
        assert [item.code for item in report_candidate.stage1_reasons] == [
            item.code for item in frozen_candidate.stage1_reasons
        ]
        assert [item.code for item in report_candidate.stage2_reasons] == [
            item.code for item in frozen_candidate.stage2_reasons
        ]
        assert [item.name for item in report_candidate.metrics] == [
            item.name for item in frozen_candidate.metrics
        ]
        assert [item.payload_sha256 for item in report_candidate.provenance.evidence_refs] == [
            item.payload_sha256 for item in frozen_candidate.provenance.artifact_refs
        ]


def test_s6a_gate7_same_file_is_noop_and_different_bytes_fail_closed(
    tmp_path: Path,
) -> None:
    database_path, run_id, unused_result = _seed_success(tmp_path)
    generation = ScreenerReportGenerator(database_path).generate(run_id)
    output_directory = tmp_path / "formal-report"
    writer = ScreenerReportArtifactWriter()

    first = writer.write(generation, output_directory=output_directory)
    second = writer.write(generation, output_directory=output_directory)
    assert first.json_artifact.written is True
    assert first.markdown_artifact.written is True
    assert second.no_op is True
    assert second.json_artifact.written is False
    assert second.markdown_artifact.written is False

    json_path = output_directory / "daily-screener-2026-08-07.json"
    markdown_path = output_directory / "daily-screener-2026-08-07.md"
    json_path.write_bytes(b"tampered derived content")
    markdown_before = markdown_path.read_bytes()
    with pytest.raises(ScreenerReportCollisionError):
        writer.write(generation, output_directory=output_directory)
    assert markdown_path.read_bytes() == markdown_before


def test_s6a_report_rejects_aliased_json_and_markdown_targets(tmp_path: Path) -> None:
    database_path, run_id, unused_result = _seed_success(tmp_path)
    generation = ScreenerReportGenerator(database_path).generate(run_id)
    target = tmp_path / "same-report-target"

    with pytest.raises(ValueError, match="distinct"):
        ScreenerReportArtifactWriter().write(
            generation,
            json_path=target,
            markdown_path=target / ".",
        )


def test_s6a_gate8_generation_is_database_read_only(tmp_path: Path) -> None:
    database_path, run_id, unused_result = _seed_success(tmp_path)
    before = _database_fingerprint(database_path)
    generator = ScreenerReportGenerator(database_path)
    generator.generate(run_id)
    generator.generate(run_id)
    after = _database_fingerprint(database_path)
    assert before == after


def test_s6a_markdown_cells_escape_markup_and_code_context() -> None:
    rendered = _cell("<script>alert(`x`)</script> | [link](https://evil.invalid)")

    assert "<script>" not in rendered
    assert "[link]" not in rendered
    assert "&#96;" in rendered
    assert "\\|" in rendered


def test_s6a_public_contract_requires_explicit_run_and_no_scheduler_boundary() -> None:
    source_path = Path(__file__).parents[1] / "app" / "reporting" / "screener_report.py"
    source = source_path.read_text(encoding="utf-8")
    assert "def generate(self, screener_run_id: str)" in source
    assert "latest run" not in source.lower()
    assert "migrate(" not in source.lower()
    assert "scheduler" not in source.lower()
