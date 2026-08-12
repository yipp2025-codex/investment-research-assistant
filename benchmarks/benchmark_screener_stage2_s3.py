"""Local 10/30/50-candidate benchmark for Screener S3 Stage 2.

Immutable M9 snapshots are constructed before timing.  Timed runs include one
indexed dataset read per shortlisted candidate, current/previous analyzer
passes, reason construction, and deterministic ranking refinement.
"""

from __future__ import annotations

import gc
import json
import statistics
from datetime import date, timedelta
from pathlib import Path
from time import perf_counter

from app.research_dataset import (
    DatasetAsOf,
    DatasetPrice,
    DatasetProvenance,
    DatasetSymbol,
    DatasetValuationMetric,
    PriceHistoryReadModel,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
    ValidationReadModel,
    ValuationReadModel,
)
from app.screener.stage1 import (
    PRICE_CHANGE_1D,
    STAGE1_METHODOLOGY_VERSION,
    DataQualityStatus,
    MetricStatus,
    RuleRole,
    Stage1Candidate,
    Stage1DataQuality,
    Stage1Metric,
    Stage1Reason,
    Stage1ScanResult,
)
from app.screener.stage2_dataset import research_stage2_from_dataset


MARKET_DATE = date(2026, 8, 7)
HISTORY_OBSERVATIONS = 250
SHORTLIST_SIZES = (10, 30, 50)
REPEATS = 2
FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "screener"
    / "twse_universe_20260807.json"
)


class IndexedDataset:
    def __init__(self, snapshots: tuple[ResearchDatasetSnapshot, ...]) -> None:
        self.snapshots = {item.symbol.symbol: item for item in snapshots}
        self.read_count = 0

    def reset(self) -> None:
        self.read_count = 0

    def read(self, request: ResearchDatasetRequest, /) -> ResearchDatasetSnapshot:
        if request.as_of_date != MARKET_DATE:
            raise ValueError("benchmark as_of_date changed")
        if request.history_observations != HISTORY_OBSERVATIONS:
            raise ValueError("benchmark history_observations changed")
        self.read_count += 1
        return self.snapshots[request.symbol]


def run_benchmark() -> dict[str, object]:
    symbols = _load_symbols(max(SHORTLIST_SIZES))
    snapshots = _snapshots(symbols)
    dataset = IndexedDataset(snapshots)
    measurements: list[dict[str, object]] = []

    for size in SHORTLIST_SIZES:
        stage1_result = _stage1_result(symbols[:size])
        size_hashes: set[str] = set()
        for repeat in range(1, REPEATS + 1):
            dataset.reset()
            gc.collect()
            started = perf_counter()
            evidence = research_stage2_from_dataset(
                stage1_result=stage1_result,
                dataset=dataset,
                market_date=MARKET_DATE,
            )
            elapsed = perf_counter() - started
            if dataset.read_count != size or evidence.dataset_reads != size:
                raise AssertionError("one-read-per-candidate contract changed")
            if evidence.observations_consumed != size * HISTORY_OBSERVATIONS:
                raise AssertionError("observation accounting changed")
            size_hashes.add(evidence.result.payload_sha256)
            measurements.append(
                {
                    "shortlist_size": size,
                    "repeat": repeat,
                    "elapsed_seconds": round(elapsed, 6),
                    "dataset_reads": evidence.dataset_reads,
                    "observations_consumed": evidence.observations_consumed,
                    "failed_candidates": evidence.failed_candidates,
                    "candidate_count": evidence.result.candidate_count,
                    "canonical_sha256": evidence.result.payload_sha256,
                }
            )
        if len(size_hashes) != 1:
            raise AssertionError("repeated shortlist hash changed")

    summaries = {}
    for size in SHORTLIST_SIZES:
        elapsed = [
            float(item["elapsed_seconds"])
            for item in measurements
            if item["shortlist_size"] == size
        ]
        summaries[str(size)] = {
            "minimum": min(elapsed),
            "median": statistics.median(elapsed),
            "maximum": max(elapsed),
        }
    return {
        "benchmark_version": "screener-stage2-s3-benchmark-v1",
        "measurement_mode": (
            "two repeated local indexed M9-style rounds per shortlist size; "
            "fixture construction excluded"
        ),
        "market_date": MARKET_DATE.isoformat(),
        "methodology_version": "screener-stage2-v1",
        "history_observations": HISTORY_OBSERVATIONS,
        "shortlist_sizes": list(SHORTLIST_SIZES),
        "repeats_per_size": REPEATS,
        "measurements": measurements,
        "elapsed_summary_seconds": summaries,
        "optimization_assessment": {
            "needed": False,
            "decision": "目前無最佳化必要",
            "scope": (
                "S3 local indexed M9-style shortlist composition and pure "
                "Stage 2 research only"
            ),
            "note": (
                "No production acquisition or SQLite latency is claimed by "
                "this benchmark."
            ),
        },
    }


def _load_symbols(count: int) -> tuple[str, ...]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    symbols = tuple(item[0] for item in payload["eligible"][:count])
    if len(symbols) != count:
        raise ValueError("S1 fixture lacks benchmark symbols")
    return symbols


def _stage1_result(symbols: tuple[str, ...]) -> Stage1ScanResult:
    candidates = tuple(
        _stage1_candidate(symbol, rank)
        for rank, symbol in enumerate(symbols, start=1)
    )
    return Stage1ScanResult(
        market_date=MARKET_DATE,
        methodology_version=STAGE1_METHODOLOGY_VERSION,
        source_policy="twse_baseline",
        universe_count=1_095,
        screened_count=1_082,
        triggered_count=len(candidates),
        candidate_count=len(candidates),
        candidate_limit=max(SHORTLIST_SIZES),
        truncated=False,
        candidates=candidates,
    )


def _stage1_candidate(symbol: str, rank: int) -> Stage1Candidate:
    reason = Stage1Reason(
        code="price_change_1d_threshold",
        metric=PRICE_CHANGE_1D,
        component=None,
        previous=0.0,
        current=3.0,
        delta=3.0,
        unit="percent",
        operator="abs_current_gte",
        threshold=3.0,
        rule_version=STAGE1_METHODOLOGY_VERSION,
        role=RuleRole.PRIMARY.value,
        trigger_class="market_move",
        threshold_multiple=1.0,
    )
    metric = Stage1Metric(
        metric=PRICE_CHANGE_1D,
        component=None,
        status=MetricStatus.AVAILABLE,
        previous=0.0,
        current=3.0,
        delta=3.0,
        value_unit="percent",
        delta_unit="percentage_point",
        previous_as_of_date=MARKET_DATE - timedelta(days=2),
        current_as_of_date=MARKET_DATE,
    )
    return Stage1Candidate(
        symbol=symbol,
        name=f"公司{symbol}",
        rank=rank,
        reasons=(reason,),
        metrics=(metric,),
        data_quality=Stage1DataQuality(
            status=DataQualityStatus.CLEAN,
            issues=(),
        ),
    )


def _snapshots(symbols: tuple[str, ...]) -> tuple[ResearchDatasetSnapshot, ...]:
    dates = tuple(
        MARKET_DATE - timedelta(days=2 * (HISTORY_OBSERVATIONS - index - 1))
        for index in range(HISTORY_OBSERVATIONS)
    )
    return tuple(_snapshot(symbol, dates) for symbol in symbols)


def _snapshot(
    symbol: str,
    dates: tuple[date, ...],
) -> ResearchDatasetSnapshot:
    prices = tuple(
        DatasetPrice(
            symbol=symbol,
            trade_date=trade_date,
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=1_000,
            source="mock-synthetic",
        )
        for trade_date in dates
    )
    valuations = tuple(
        DatasetValuationMetric(
            symbol=symbol,
            metric_date=MARKET_DATE,
            name=name,
            value=value,
            unit=unit,
            source="mock-synthetic",
        )
        for name, value, unit in (
            ("dividend_yield_pct", 3.0, "%"),
            ("price_earnings_ratio", 15.0, "ratio"),
            ("price_to_book_ratio", 2.0, "ratio"),
        )
    )
    return ResearchDatasetSnapshot(
        symbol=DatasetSymbol(
            symbol=symbol,
            name=f"公司{symbol}",
            market="TWSE",
            currency="TWD",
        ),
        as_of=DatasetAsOf(
            as_of_date=MARKET_DATE,
            history_observations=HISTORY_OBSERVATIONS,
            total_history_observations=HISTORY_OBSERVATIONS,
            returned_history_observations=HISTORY_OBSERVATIONS,
            history_is_truncated=False,
        ),
        price_history=PriceHistoryReadModel(
            symbol=symbol,
            as_of_date=MARKET_DATE,
            status="available",
            observations=prices,
            current=prices[-1],
            total_observations_as_of=HISTORY_OBSERVATIONS,
            requested_observations=HISTORY_OBSERVATIONS,
            is_truncated=False,
        ),
        valuation=ValuationReadModel(
            symbol=symbol,
            as_of_date=MARKET_DATE,
            status="available",
            metrics=valuations,
        ),
        validation=ValidationReadModel(symbol=symbol),
        provenance=DatasetProvenance(
            symbol=symbol,
            canonical_sources=("mock-synthetic",),
        ),
    )


if __name__ == "__main__":
    print(json.dumps(run_benchmark(), ensure_ascii=False, indent=2))
