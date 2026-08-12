"""Frozen evidence gates for the S2 1,082-symbol local benchmark."""

from __future__ import annotations

import ast
import statistics
from functools import lru_cache
from pathlib import Path

from benchmarks.benchmark_screener_stage1_s2 import run_benchmark


ROOT = Path(__file__).resolve().parents[1]
BENCHMARK_SOURCE = ROOT / "benchmarks" / "benchmark_screener_stage1_s2.py"


@lru_cache(maxsize=1)
def _payload() -> dict[str, object]:
    return run_benchmark()


def test_s2_benchmark_evidence_has_three_reproducible_1082_symbol_rounds() -> None:
    payload = _payload()
    assert payload["benchmark_version"] == "screener-stage1-s2-benchmark-v1"
    assert payload["methodology_version"] == "screener-stage1-v1"
    assert payload["universe_size"] == 1_082
    assert payload["history_observations"] == 62
    assert payload["candidate_limit"] == 30
    assert len(payload["rounds"]) == 3
    assert {item["dataset_reads"] for item in payload["rounds"]} == {1_082}
    assert {item["price_rows_consumed"] for item in payload["rounds"]} == {
        1_082 * 62
    }
    assert {item["result_sha256"] for item in payload["rounds"]} == {
        payload["canonical_result_sha256"]
    }
    elapsed = [item["elapsed_seconds"] for item in payload["rounds"]]
    assert all(value > 0 for value in elapsed)
    assert payload["elapsed_summary_seconds"] == {
        "minimum": min(elapsed),
        "median": statistics.median(elapsed),
        "maximum": max(elapsed),
    }


def test_s2_benchmark_records_no_current_optimization_need() -> None:
    payload = _payload()
    assessment = payload["optimization_assessment"]
    assert assessment["needed"] is False
    assert assessment["decision"] == "目前無最佳化必要"
    assert assessment["deferred"] == [
        "read_many",
        "additional database indexes",
        "Pandas",
        "NumPy",
        "DuckDB",
    ]


def test_s2_benchmark_is_local_read_only_and_uses_no_deferred_engine() -> None:
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
