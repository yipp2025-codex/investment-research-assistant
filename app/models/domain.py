"""Vendor-neutral domain records."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum


_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class PipelineRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Symbol:
    symbol: str
    name: str
    market: str
    currency: str
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class DailyPrice:
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str


@dataclass(frozen=True, slots=True)
class CompanyMetric:
    symbol: str
    metric_date: date
    name: str
    value: float
    unit: str | None
    source: str


@dataclass(frozen=True, slots=True)
class ResearchNote:
    symbol: str
    created_at: datetime
    analysis_type: str
    title: str
    summary: str
    source_data_start: date
    source_data_end: date
    provider_source: str
    run_id: str | None = None
    historical_run_id: str | None = None
    historical_validation_run_id: str | None = None
    id: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedMarketData:
    source: str
    symbol: Symbol
    daily_prices: tuple[DailyPrice, ...]
    company_metrics: tuple[CompanyMetric, ...]
    source_endpoints: tuple[str, ...] = ()
    fetched_at: datetime | None = None
    market_date: date | None = None
    source_timestamp_raw: str | None = None
    source_timestamp: datetime | None = None


@dataclass(frozen=True, slots=True)
class SourceArtifact:
    """Hash-only evidence for one provider response; raw bytes are never stored."""

    provider: str
    dataset: str
    endpoint: str
    contract_version: str
    content_type: str
    payload_sha256: str
    payload_size_bytes: int
    hash_basis: str
    fetched_at: datetime | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "provider",
            "dataset",
            "endpoint",
            "contract_version",
            "content_type",
            "hash_basis",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"source artifact {field_name} must not be blank")
        if (
            not isinstance(self.payload_sha256, str)
            or _SHA256.fullmatch(self.payload_sha256) is None
        ):
            raise ValueError("source artifact payload_sha256 must be lowercase SHA-256")
        if (
            isinstance(self.payload_size_bytes, bool)
            or not isinstance(self.payload_size_bytes, int)
            or self.payload_size_bytes < 0
        ):
            raise ValueError("source artifact payload_size_bytes must be non-negative")
        if self.hash_basis not in {"raw-response-bytes-v1", "canonical-json-v1"}:
            raise ValueError("source artifact hash_basis is unsupported")
        if self.fetched_at is not None and self.fetched_at.utcoffset() is None:
            raise ValueError("source artifact fetched_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class PipelineRun:
    run_id: str
    symbol: str
    target_date: date
    requested_start_date: date
    requested_end_date: date
    status: PipelineRunStatus
    provider: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    attempt_count: int
    research_note_id: int | None
    source_endpoints: tuple[str, ...]
    fetched_at: datetime | None
    market_date: date | None


@dataclass(frozen=True, slots=True)
class HistoricalSyncRun:
    run_id: str
    symbol: str
    target_date: date
    target_observations: int
    provider: str
    status: PipelineRunStatus
    next_month: date
    months_completed: int
    observation_count: int
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    attempt_count: int
    research_note_id: int | None
    source_endpoint: str | None
    fetched_at: datetime | None
    first_trade_date: date | None
    last_trade_date: date | None


@dataclass(frozen=True, slots=True)
class HistoricalSourceObservation:
    historical_run_id: str
    provider: str
    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source_endpoints: tuple[str, ...]
    fetched_at: datetime
    source_timestamp_raw: str | None = None
    source_timestamp: datetime | None = None
    id: int | None = None


class CrossValidationOutcome(str, Enum):
    MATCH = "match"
    DISCREPANCY = "discrepancy"


@dataclass(frozen=True, slots=True)
class CrossValidationRun:
    run_id: str
    symbol: str
    target_date: date
    requested_start_date: date
    left_provider: str
    right_provider: str
    status: PipelineRunStatus
    outcome: CrossValidationOutcome | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    attempt_count: int


@dataclass(frozen=True, slots=True)
class HistoricalValidationRun:
    run_id: str
    symbol: str
    target_date: date
    target_observations: int
    left_provider: str
    right_provider: str
    left_historical_run_id: str
    right_historical_run_id: str
    status: PipelineRunStatus
    outcome: CrossValidationOutcome | None
    common_date_count: int
    matched_date_count: int
    left_only_date_count: int
    right_only_date_count: int
    field_discrepancy_count: int
    left_latest_date: date | None
    right_latest_date: date | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    attempt_count: int
    research_note_id: int | None


@dataclass(frozen=True, slots=True)
class MarketDataObservation:
    run_id: str
    provider: str
    symbol: str
    market_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source_endpoints: tuple[str, ...]
    fetched_at: datetime
    source_timestamp_raw: str | None = None
    source_timestamp: datetime | None = None
    id: int | None = None


@dataclass(frozen=True, slots=True)
class MarketDataDiscrepancy:
    run_id: str
    field: str
    left_value: str | None
    right_value: str | None
    reason: str
    absolute_difference: float | None = None
    relative_difference_pct: float | None = None
    id: int | None = None


@dataclass(frozen=True, slots=True)
class HistoricalValidationDiscrepancy:
    run_id: str
    trade_date: date
    field: str
    left_value: str | None
    right_value: str | None
    reason: str
    absolute_difference: float | None = None
    relative_difference_pct: float | None = None
    id: int | None = None
