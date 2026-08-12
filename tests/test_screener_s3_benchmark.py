"""Frozen evidence gates for the S3 shortlist benchmark."""

from __future__ import annotations

import ast
import statistics
from functools import lru_cache
from pathlib import Path

from benchmarks.benchmark_screener_stage2_s3 import run_benchmark


ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_SOURCE = ROOT / "benchmarks" / "benchmark_screener_stage2_s3.py"


@lru_cache(maxsize=1)
def _payload() -> dict[str, object]:
    return run_benchmark()


def test_s3_benchmark_records_10_30_50_shortlists_and_exact_accounting() -> None:
    payload = _payload()
    assert payload["benchmark_version"] == "screener-stage2-s3-benchmark-v1"
    assert payload["methodology_version"] == "screener-stage2-v1"
    assert payload["history_observations"] == 250
    assert payload["shortlist_sizes"] == [10, 30, 50]
    assert payload["repeats_per_size"] == 2
    assert len(payload["measurements"]) == 6
    for size in (10, 30, 50):
        rows = [
            item for item in payload["measurements"] if item["shortlist_size"] == size
        ]
        assert len(rows) == 2
        assert {item["dataset_reads"] for item in rows} == {size}
        assert {item["observations_consumed"] for item in rows} == {size * 250}
        assert {item["candidate_count"] for item in rows} == {size}
        assert {item["failed_candidates"] for item in rows} == {0}
        assert len({item["canonical_sha256"] for item in rows}) == 1


def test_s3_benchmark_elapsed_summaries_and_assessment_are_evidence_bound() -> None:
    payload = _payload()
    for size in (10, 30, 50):
        elapsed = [
            item["elapsed_seconds"]
            for item in payload["measurements"]
            if item["shortlist_size"] == size
        ]
        assert all(value > 0 for value in elapsed)
        assert payload["elapsed_summary_seconds"][str(size)] == {
            "minimum": min(elapsed),
            "median": statistics.median(elapsed),
            "maximum": max(elapsed),
        }
    assessment = payload["optimization_assessment"]
    assert assessment["needed"] is False
    assert assessment["decision"] == "目前無最佳化必要"
    assert "production" in assessment["note"].casefold()


def test_s3_benchmark_is_local_read_only_without_deferred_engines() -> None:
    tree = ast.parse(BENCHMARK_SOURCE.read_text(encoding="utf-8"))
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not any(
        name.startswith(
            (
                "app.providers",
                "app.storage",
                "sqlite3",
                "pandas",
                "numpy",
                "duckdb",
                "urllib",
                "socket",
            )
        )
        for name in imported
    )
    calls = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert not {"write_text", "write_bytes", "open"} & calls
