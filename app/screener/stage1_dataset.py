"""M9 single-read composition for the pure S2 Stage 1 engine.

This adapter only translates immutable ``ResearchDataset`` results.  It owns
no acquisition, storage, calendar, scheduling, reporting, or watchlist logic.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import date
from typing import Mapping

from app.research_dataset import (
    ResearchDataset,
    ResearchDatasetRequest,
    ResearchDatasetSnapshot,
)
from app.screener.stage1 import (
    STAGE1_METHODOLOGY_V1,
    PriceSnapshotStatus,
    Stage1ContractError,
    Stage1Methodology,
    Stage1PriceObservation,
    Stage1ResearchSnapshot,
    Stage1ScanResult,
    Stage1ValuationState,
    ValuationSnapshotStatus,
    scan_stage1,
)
from app.screener.universe import MarketUniverseSnapshot, UniverseMemberStatus


_SYMBOL = re.compile(r"^[0-9A-Z]{2,12}$")
_VALUATION_FIELDS = {
    "price_earnings_ratio": ("pe_ratio", "ratio"),
    "price_to_book_ratio": ("pb_ratio", "ratio"),
    "dividend_yield_pct": ("dividend_yield_pct", "%"),
}


class Stage1CompositionError(Stage1ContractError):
    """M9 read output cannot be safely translated into the S2 contract."""


@dataclass(frozen=True, slots=True)
class PreviousValuationSnapshot:
    symbol: str
    state: Stage1ValuationState

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or _SYMBOL.fullmatch(
            self.symbol.strip().upper()
        ) is None:
            raise Stage1CompositionError("previous valuation symbol is invalid")
        if not isinstance(self.state, Stage1ValuationState):
            raise Stage1CompositionError("previous valuation state is invalid")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())


@dataclass(frozen=True, slots=True)
class Stage1DatasetScanEvidence:
    result: Stage1ScanResult
    dataset_reads: int
    price_rows_consumed: int
    history_observations: int
    dataset_version_ids: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.result, Stage1ScanResult):
            raise Stage1CompositionError("result must be Stage1ScanResult")
        for field_name in ("dataset_reads", "price_rows_consumed"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise Stage1CompositionError(f"{field_name} must be non-negative")
        if self.dataset_reads != self.result.screened_count:
            raise Stage1CompositionError(
                "dataset_reads must equal the exact scan-eligible member count"
            )
        if self.history_observations != STAGE1_METHODOLOGY_V1.history_observations:
            raise Stage1CompositionError("history_observations must remain frozen")


def scan_stage1_from_dataset(
    *,
    universe: MarketUniverseSnapshot,
    dataset: ResearchDataset,
    as_of_date: date,
    candidate_limit: int,
    previous_valuations: tuple[PreviousValuationSnapshot, ...] = (),
    dataset_version_ids: Mapping[str, str] | None = None,
    methodology: Stage1Methodology = STAGE1_METHODOLOGY_V1,
) -> Stage1DatasetScanEvidence:
    """Read every eligible symbol once, translate, then call the pure engine."""

    if methodology is not STAGE1_METHODOLOGY_V1:
        raise Stage1CompositionError("composition requires frozen Stage 1 v1")
    if not isinstance(universe, MarketUniverseSnapshot):
        raise Stage1CompositionError("universe must be MarketUniverseSnapshot")
    if not isinstance(previous_valuations, tuple):
        raise Stage1CompositionError(
            "previous_valuations must be an immutable tuple"
        )
    previous_by_symbol: dict[str, Stage1ValuationState] = {}
    for item in previous_valuations:
        if not isinstance(item, PreviousValuationSnapshot):
            raise Stage1CompositionError("invalid previous valuation item")
        prior = previous_by_symbol.get(item.symbol)
        if prior is None:
            previous_by_symbol[item.symbol] = item.state
        elif prior != item.state:
            raise Stage1CompositionError(
                f"conflicting previous valuation snapshots for {item.symbol}"
            )

    eligible = tuple(
        item
        for item in universe.members
        if item.status is UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE
    )
    eligible_symbols = {item.symbol for item in eligible}
    if dataset_version_ids is not None:
        if not isinstance(dataset_version_ids, Mapping):
            raise Stage1CompositionError("dataset_version_ids must be a symbol mapping")
        extra_versions = set(dataset_version_ids) - eligible_symbols
        if extra_versions:
            raise Stage1CompositionError(
                "dataset_version_ids contains symbols outside the scan universe: "
                + ", ".join(sorted(extra_versions))
            )
        missing_versions = eligible_symbols - set(dataset_version_ids)
        if missing_versions:
            raise Stage1CompositionError(
                "dataset_version_ids must explicitly cover every scan-eligible symbol: "
                + ", ".join(sorted(missing_versions))
            )
    extra_previous = set(previous_by_symbol) - eligible_symbols
    if extra_previous:
        raise Stage1CompositionError(
            "previous valuation input is outside scan-eligible universe: "
            + ", ".join(sorted(extra_previous))
        )

    screening_snapshots: list[Stage1ResearchSnapshot] = []
    read_symbols: set[str] = set()
    price_rows_consumed = 0
    for member in sorted(eligible, key=lambda item: item.symbol):
        if member.symbol in read_symbols:  # pragma: no cover - S1 uniqueness guard.
            raise Stage1CompositionError(
                f"dataset read would repeat symbol {member.symbol}"
            )
        request = ResearchDatasetRequest(
            symbol=member.symbol,
            as_of_date=as_of_date,
            history_observations=methodology.history_observations,
            dataset_version_id=(
                None
                if dataset_version_ids is None
                else dataset_version_ids.get(member.symbol)
            ),
        )
        dataset_snapshot = dataset.read(request)
        read_symbols.add(member.symbol)
        translated = screening_snapshot_from_dataset(
            dataset_snapshot,
            previous_valuation=previous_by_symbol.get(member.symbol),
            methodology=methodology,
        )
        screening_snapshots.append(translated)
        price_rows_consumed += len(translated.prices)

    result = scan_stage1(
        universe=universe,
        research_snapshots=tuple(screening_snapshots),
        as_of_date=as_of_date,
        candidate_limit=candidate_limit,
        methodology=methodology,
    )
    return Stage1DatasetScanEvidence(
        result=result,
        dataset_reads=len(read_symbols),
        price_rows_consumed=price_rows_consumed,
        history_observations=methodology.history_observations,
        dataset_version_ids=tuple(
            sorted(
                (symbol, value)
                for symbol, value in (dataset_version_ids or {}).items()
                if symbol in read_symbols
            )
        ),
    )


def screening_snapshot_from_dataset(
    snapshot: ResearchDatasetSnapshot,
    *,
    previous_valuation: Stage1ValuationState | None = None,
    methodology: Stage1Methodology = STAGE1_METHODOLOGY_V1,
) -> Stage1ResearchSnapshot:
    """Translate one M9 snapshot without consulting validation observations."""

    if not isinstance(snapshot, ResearchDatasetSnapshot):
        raise Stage1CompositionError("snapshot must be ResearchDatasetSnapshot")
    if methodology is not STAGE1_METHODOLOGY_V1:
        raise Stage1CompositionError("translation requires frozen Stage 1 v1")
    if snapshot.as_of.history_observations != methodology.history_observations:
        raise Stage1CompositionError(
            "M9 snapshot history_observations does not match Stage 1 v1"
        )
    if len(snapshot.price_history.observations) > methodology.history_observations:
        raise Stage1CompositionError("M9 snapshot returned too many price rows")

    try:
        price_status = PriceSnapshotStatus(snapshot.price_history.status)
    except ValueError as error:
        raise Stage1CompositionError("unsupported M9 price status") from error
    prices = tuple(
        Stage1PriceObservation(
            trade_date=item.trade_date,
            close=item.close,
            volume=item.volume,
            # Stage 1's frozen source field describes the canonical
            # authority family, while DS5 keeps the original source role and
            # provider on the M9 row.  A selected provisional E.SUN row is
            # therefore translated to the TWSE family for the pure Stage 1
            # contract; provenance remains intact in the M9 snapshot.
            source=(
                item.source
                if item.source in {"twse", "twse-historical"}
                else "twse-historical"
            ),
        )
        for item in snapshot.price_history.observations
    )
    current_valuation = _current_valuation_state(snapshot)
    source_values = {
        *snapshot.provenance.canonical_sources,
        *(item.source for item in snapshot.price_history.observations),
        *(item.source for item in snapshot.valuation.metrics),
    }
    canonical_sources = tuple(
        sorted(
            source if source in {"twse", "twse-historical"} else "twse-historical"
            for source in source_values
        )
    )
    if not canonical_sources:
        raise Stage1CompositionError("M9 snapshot lacks canonical source evidence")
    return Stage1ResearchSnapshot(
        symbol=snapshot.symbol.symbol,
        as_of_date=snapshot.as_of.as_of_date,
        price_status=price_status,
        prices=prices,
        current_valuation=current_valuation,
        previous_valuation=previous_valuation,
        canonical_sources=canonical_sources,
    )


def _current_valuation_state(
    snapshot: ResearchDatasetSnapshot,
) -> Stage1ValuationState:
    if snapshot.valuation.status == "missing_source":
        return Stage1ValuationState(
            as_of_date=snapshot.as_of.as_of_date,
            status=ValuationSnapshotStatus.MISSING_SOURCE,
        )
    if snapshot.valuation.status != "available":
        raise Stage1CompositionError("unsupported M9 valuation status")
    values: dict[str, float | None] = {
        "pe_ratio": None,
        "pb_ratio": None,
        "dividend_yield_pct": None,
    }
    for metric in snapshot.valuation.metrics:
        mapping = _VALUATION_FIELDS.get(metric.name)
        if mapping is None:
            continue
        field_name, expected_unit = mapping
        if metric.unit != expected_unit:
            raise Stage1CompositionError(
                f"valuation metric {metric.name} has unexpected unit"
            )
        if not math.isfinite(metric.value):
            raise Stage1CompositionError(
                f"valuation metric {metric.name} must be finite"
            )
        values[field_name] = float(metric.value)
    return Stage1ValuationState(
        as_of_date=snapshot.as_of.as_of_date,
        status=ValuationSnapshotStatus.AVAILABLE,
        **values,
    )
