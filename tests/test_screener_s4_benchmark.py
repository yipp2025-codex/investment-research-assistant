from __future__ import annotations

import ast
import re
from pathlib import Path

from benchmarks.benchmark_screener_persistence_s4 import run_benchmark


def test_s4_capacity_benchmark_records_deterministic_storage_evidence(
    tmp_path: Path,
) -> None:
    first = run_benchmark(tmp_path / "s4-capacity-first.db")
    second = run_benchmark(tmp_path / "s4-capacity-second.db")

    assert first["schema_version"] == second["schema_version"] == 11
    assert first["universe_count"] == second["universe_count"] == 1_095
    assert first["candidate_count"] == second["candidate_count"] == 30
    assert first["row_counts"] == second["row_counts"] == {
        "market_universe_members": 1_095,
        "screener_candidates": 30,
        "candidate_reasons": 30,
        "candidate_metrics": 480,
        "screener_source_artifacts": 35,
    }
    assert first["canonical_sha256"] == second["canonical_sha256"]
    assert re.fullmatch(r"[0-9a-f]{64}", str(first["canonical_sha256"]))
    assert first["database_size_bytes"]["delta"] > 0
    assert first["persistence_elapsed_seconds"]["total"] >= 0
    assert len(first["replay_elapsed_seconds"]["measurements"]) == 3
    assert set(first["historical_query_elapsed_seconds"]) == {
        "selection_count",
        "reason_frequency",
        "daily_candidate_set",
        "reason_history",
        "methodology_comparison",
    }
    assert all(
        len(value["measurements"]) == 3
        for value in first["historical_query_elapsed_seconds"].values()
    )
    assert first["dataset_reads"] == 0
    assert first["full_time_series_rows_persisted"] == 0
    assert first["raw_provider_responses_persisted"] is False
    assert first["market_scan_observations_table_present"] is False
    assert first["performance_budget"] is None
    assert first["benchmark_version"] == second["benchmark_version"]
    assert second["performance_budget"] is None


def test_s4_benchmark_has_no_provider_or_analytics_optimization_dependency() -> None:
    import benchmarks.benchmark_screener_persistence_s4 as benchmark_module

    tree = ast.parse(Path(benchmark_module.__file__).read_text(encoding="utf-8"))
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported.update(
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    )
    assert not any(
        value.startswith(
            (
                "app.providers",
                "pandas",
                "numpy",
                "duckdb",
            )
        )
        for value in imported
    )
