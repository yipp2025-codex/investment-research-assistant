"""Reproducible local 1,082-symbol benchmark for Screener S2.

The benchmark constructs immutable M9 snapshots before timing.  Timed rounds
cover one indexed ``ResearchDataset.read`` per symbol, translation, all Stage 1
metrics/rules, and shortlist construction.  It performs no network or storage
I/O and writes no result file.
"""

from __future__ import annotations

import gc
import hashlib
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
    STAGE1_METHODOLOGY_V1,
    Stage1ValuationState,
    ValuationSnapshotStatus,
)
from app.screener.stage1_dataset import (
    PreviousValuationSnapshot,
    scan_stage1_from_dataset,
)
from app.screener.universe import (
    UNIVERSE_METHODOLOGY_VERSION,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)


MARKET_DATE = date(2026, 8, 7)
UNIVERSE_SIZE = 1_082
CANDIDATE_LIMIT = 30
ROUNDS = 3
FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "screener"
    / "twse_universe_20260807.json"
)


class IndexedSyntheticResearchDataset:
    """Constant-time local implementation of the frozen M9 read protocol."""

    def __init__(self, snapshots: tuple[ResearchDatasetSnapshot, ...]) -> None:
        self._snapshots = {item.symbol.symbol: item for item in snapshots}
        if len(self._snapshots) != len(snapshots):
            raise ValueError("synthetic benchmark symbols must be unique")
        self.read_count = 0

    def reset_count(self) -> None:
        self.read_count = 0

    def read(self, request: ResearchDatasetRequest, /) -> ResearchDatasetSnapshot:
        if request.as_of_date != MARKET_DATE:
            raise ValueError("benchmark request as_of_date changed")
        if request.history_observations != (
            STAGE1_METHODOLOGY_V1.history_observations
        ):
            raise ValueError("benchmark request history_observations changed")
        self.read_count += 1
        return self._snapshots[request.symbol]


def run_benchmark() -> dict[str, object]:
    symbols_and_names = _load_symbols()
    universe = _build_universe(symbols_and_names)
    snapshots, previous = _build_research_snapshots(symbols_and_names)
    dataset = IndexedSyntheticResearchDataset(snapshots)

    measurements: list[dict[str, object]] = []
    hashes: set[str] = set()
    for index, label in enumerate(
        ("cold_equivalent", "warm_repeat_1", "warm_repeat_2"),
        start=1,
    ):
        dataset.reset_count()
        gc.collect()
        started = perf_counter()
        evidence = scan_stage1_from_dataset(
            universe=universe,
            dataset=dataset,
            as_of_date=MARKET_DATE,
            candidate_limit=CANDIDATE_LIMIT,
            previous_valuations=previous,
        )
        elapsed = perf_counter() - started
        if dataset.read_count != evidence.dataset_reads:
            raise AssertionError("adapter and dataset read counters diverged")
        hashes.add(evidence.result.payload_sha256)
        measurements.append(
            {
                "round": index,
                "label": label,
                "elapsed_seconds": round(elapsed, 6),
                "dataset_reads": evidence.dataset_reads,
                "price_rows_consumed": evidence.price_rows_consumed,
                "triggered_count": evidence.result.triggered_count,
                "candidate_count": evidence.result.candidate_count,
                "result_sha256": evidence.result.payload_sha256,
            }
        )

    if len(hashes) != 1:
        raise AssertionError("benchmark rounds produced different canonical results")
    elapsed_values = [float(item["elapsed_seconds"]) for item in measurements]
    return {
        "benchmark_version": "screener-stage1-s2-benchmark-v1",
        "measurement_mode": (
            "three repeated local in-memory rounds; immutable fixture construction "
            "excluded from elapsed time"
        ),
        "market_date": MARKET_DATE.isoformat(),
        "methodology_version": STAGE1_METHODOLOGY_V1.version,
        "universe_size": UNIVERSE_SIZE,
        "candidate_limit": CANDIDATE_LIMIT,
        "history_observations": STAGE1_METHODOLOGY_V1.history_observations,
        "expected_dataset_reads_per_round": UNIVERSE_SIZE,
        "expected_price_rows_per_round": (
            UNIVERSE_SIZE * STAGE1_METHODOLOGY_V1.history_observations
        ),
        "rounds": measurements,
        "elapsed_summary_seconds": {
            "minimum": min(elapsed_values),
            "median": statistics.median(elapsed_values),
            "maximum": max(elapsed_values),
        },
        "canonical_result_sha256": next(iter(hashes)),
        "optimization_assessment": {
            "needed": False,
            "decision": "目前無最佳化必要",
            "scope": (
                "S2 local indexed ResearchDataset composition and pure "
                "Stage 1 engine only"
            ),
            "deferred": [
                "read_many",
                "additional database indexes",
                "Pandas",
                "NumPy",
                "DuckDB",
            ],
        },
    }


def _load_symbols() -> tuple[tuple[str, str], ...]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    values = tuple((symbol, name) for symbol, name, unused_date in payload["eligible"])
    if len(values) != UNIVERSE_SIZE:
        raise ValueError("S1 fixture no longer contains 1,082 eligible symbols")
    return values


def _build_universe(
    symbols_and_names: tuple[tuple[str, str], ...],
) -> MarketUniverseSnapshot:
    evidence_payload = json.dumps(
        symbols_and_names,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    evidence = (
        SourceEvidence(
            source="twse",
            dataset="FROZEN_ORDINARY_STOCK_CLASSIFICATION",
            source_ref="contract://screener-stage1-s2-benchmark/universe",
            contract_version="screener-stage1-s2-benchmark-v1",
            payload_sha256=hashlib.sha256(evidence_payload).hexdigest(),
            payload_size_bytes=len(evidence_payload),
            hash_basis="canonical-json-v1",
        ),
    )
    members = tuple(
        MarketUniverseMember(
            symbol=symbol,
            name=name,
            market="TWSE",
            status=UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
            listing_date=date(2000, 1, 1),
            delisting_date=None,
            exclusion_reason=None,
            source_evidence=evidence,
        )
        for symbol, name in symbols_and_names
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


def _build_research_snapshots(
    symbols_and_names: tuple[tuple[str, str], ...],
) -> tuple[
    tuple[ResearchDatasetSnapshot, ...],
    tuple[PreviousValuationSnapshot, ...],
]:
    history_count = STAGE1_METHODOLOGY_V1.history_observations
    dates = tuple(
        MARKET_DATE - timedelta(days=2 * (history_count - index - 1))
        for index in range(history_count)
    )
    snapshots: list[ResearchDatasetSnapshot] = []
    previous: list[PreviousValuationSnapshot] = []
    for symbol, name in symbols_and_names:
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
                name=metric_name,
                value=value,
                unit=unit,
                source="mock-synthetic",
            )
            for metric_name, value, unit in (
                ("dividend_yield_pct", 3.0, "%"),
                ("price_earnings_ratio", 15.0, "ratio"),
                ("price_to_book_ratio", 2.0, "ratio"),
            )
        )
        snapshots.append(
            ResearchDatasetSnapshot(
                symbol=DatasetSymbol(
                    symbol=symbol,
                    name=name,
                    market="TWSE",
                    currency="TWD",
                ),
                as_of=DatasetAsOf(
                    as_of_date=MARKET_DATE,
                    history_observations=history_count,
                    total_history_observations=history_count,
                    returned_history_observations=history_count,
                    history_is_truncated=False,
                ),
                price_history=PriceHistoryReadModel(
                    symbol=symbol,
                    as_of_date=MARKET_DATE,
                    status="available",
                    observations=prices,
                    current=prices[-1],
                    total_observations_as_of=history_count,
                    requested_observations=history_count,
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
        )
        previous.append(
            PreviousValuationSnapshot(
                symbol=symbol,
                state=Stage1ValuationState(
                    as_of_date=dates[-2],
                    status=ValuationSnapshotStatus.AVAILABLE,
                    pe_ratio=15.0,
                    pb_ratio=2.0,
                    dividend_yield_pct=3.0,
                ),
            )
        )
    return tuple(snapshots), tuple(previous)


if __name__ == "__main__":
    print(json.dumps(run_benchmark(), ensure_ascii=False, indent=2))
