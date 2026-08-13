"""Production daily data preparation before the frozen S5 composition.

The preparation layer acquires authoritative TWSE upstream data and
candidate-only E.SUN source observations.  E.SUN remains source-only: the
TWSE historical observations are still the canonical research baseline, while
the two completed histories are compared before the frozen S5 composition is
allowed to finish.  It does not score Stage 1, make Stage 2 research
decisions, rank candidates, persist S4 output, or render reports.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Callable, Mapping

from app.config import Settings
from app.deployment.composition import (
    CANDIDATE_LIMIT_ENV,
    DEFAULT_CANDIDATE_LIMIT,
    PRODUCTION_S5_FACTORY_LOCATOR,
    ProductionS5Configuration,
    SQLiteScreenerResearchDataset,
    _compose_s5,
    _snapshot_from_dict,
)
from app.models.domain import PipelineRunStatus
from app.pipelines.daily_research import DailyResearchPipeline
from app.pipelines.historical_sync import HistoricalSyncPipeline
from app.pipelines.historical_validation import (
    CrossValidatedHistoricalResearchPipeline,
)
from app.pipelines.retry import RetryPolicy
from app.providers.base import (
    MarketDataProvider,
    ProviderInvalidRequestError,
    ProviderTemporaryError,
)
from app.providers.twse import (
    BWIBBU_ALL_URL,
    STOCK_DAY_ALL_URL,
    TwseHttpResponse,
    TwseHttpTransport,
    TwseMarketDataProvider,
    UrllibTwseHttpTransport,
    _BWIBBU_FIELDS,
    _STOCK_DAY_FIELDS,
)
from app.providers.esun_history import EsunHistoricalMarketDataProvider
from app.providers.twse_history import TwseHistoricalMarketDataProvider
from app.research_dataset import (
    ResearchDataset,
    ResearchDatasetRequest,
    TWSE_BASELINE_SOURCES,
)
from app.screener.stage1 import STAGE1_METHODOLOGY_V1
from app.screener.universe import (
    CLASSIFICATION_DATASET,
    DELISTING_DATASET,
    IDENTITY_DATASET,
    STOCK_DAY_ALL_DATASET,
    UNIVERSE_METHODOLOGY_VERSION,
    VALUATION_DATASET,
    DailyTradingRecord,
    DelistingRecord,
    InputDatasetSnapshot,
    InstrumentClassification,
    InstrumentClassificationRecord,
    ListedIdentityRecord,
    MarketUniverseBuildInput,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
    ValuationCoverageRecord,
    build_market_universe,
)
from app.storage.sqlite import SQLiteResearchRepository
from app.storage.universe_persistence import SQLiteMarketUniverseRepository


DAILY_PREPARATION_CONTRACT_VERSION = "s6e1-production-daily-preparation-v2"
PREPARED_S5_FACTORY_LOCATOR = (
    "app.deployment.daily_preparation:create_prepared_s5"
)
BASE_UNIVERSE_PATH_ENV = "IRA_DAILY_PREPARATION_BASE_UNIVERSE_SNAPSHOT_PATH"
ARTIFACT_DIRECTORY_ENV = "IRA_DAILY_PREPARATION_ARTIFACT_DIRECTORY"
EVIDENCE_PATH_ENV = "IRA_DAILY_PREPARATION_EVIDENCE_PATH"
FROZEN_S5_FACTORY_ENV = "IRA_DAILY_PREPARATION_FROZEN_S5_FACTORY"
HISTORICAL_CADENCE_ENV = "IRA_DAILY_PREPARATION_HISTORICAL_CADENCE_SECONDS"
ESUN_CONFIG_PATH_ENV = "ESUN_MARKETDATA_CONFIG_PATH"
IDENTITY_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
DELISTING_URL = (
    "https://openapi.twse.com.tw/v1/company/suspendListingCsvAndHtml"
)

_COMMON_STOCK_CODE = re.compile(r"^[0-9]{4}$")


class DailyPreparationError(RuntimeError):
    """A production preparation invariant failed closed."""


@dataclass(frozen=True, slots=True)
class DailyPreparationConfiguration:
    database_path: Path
    base_universe_snapshot_path: Path
    artifact_directory: Path
    evidence_path: Path
    esun_config_path: Path | None = None
    candidate_limit: int = DEFAULT_CANDIDATE_LIMIT
    timeout_seconds: float = 20.0
    historical_cadence_seconds: float = 1.0

    def __post_init__(self) -> None:
        for value, name in (
            (self.database_path, "database_path"),
            (self.base_universe_snapshot_path, "base_universe_snapshot_path"),
            (self.artifact_directory, "artifact_directory"),
            (self.evidence_path, "evidence_path"),
        ):
            if not isinstance(value, Path) or not value.is_absolute():
                raise DailyPreparationError(f"{name} must be an absolute Path")
        if self.esun_config_path is not None and not self.esun_config_path.is_absolute():
            raise DailyPreparationError("esun_config_path must be an absolute Path")
        if self.candidate_limit != DEFAULT_CANDIDATE_LIMIT:
            raise DailyPreparationError("candidate_limit must remain frozen at 30")
        if self.timeout_seconds <= 0:
            raise DailyPreparationError("timeout_seconds must be positive")
        if self.historical_cadence_seconds < 0:
            raise DailyPreparationError(
                "historical_cadence_seconds must not be negative"
            )

    @classmethod
    def from_environment(
        cls,
        database_path: str | Path,
    ) -> "DailyPreparationConfiguration":
        database = _absolute(database_path, "database_path")
        raw_base = os.environ.get(BASE_UNIVERSE_PATH_ENV, "").strip()
        raw_directory = os.environ.get(ARTIFACT_DIRECTORY_ENV, "").strip()
        raw_evidence = os.environ.get(EVIDENCE_PATH_ENV, "").strip()
        if not raw_base:
            raise DailyPreparationError(f"{BASE_UNIVERSE_PATH_ENV} is required")
        if not raw_directory:
            raise DailyPreparationError(f"{ARTIFACT_DIRECTORY_ENV} is required")
        if not raw_evidence:
            raise DailyPreparationError(f"{EVIDENCE_PATH_ENV} is required")
        try:
            settings = Settings.from_env()
        except (OSError, UnicodeError, ValueError) as error:
            raise DailyPreparationError(
                "E.SUN environment configuration is invalid"
            ) from error
        if settings.esun_marketdata_config_path is None:
            raise DailyPreparationError(
                f"{ESUN_CONFIG_PATH_ENV} is required for scheduled preparation"
            )
        esun_config = _absolute(
            settings.esun_marketdata_config_path,
            ESUN_CONFIG_PATH_ENV,
        )
        if not esun_config.is_file():
            raise DailyPreparationError(
                f"{ESUN_CONFIG_PATH_ENV} does not point to a readable config file"
            )
        try:
            candidate_limit = int(
                os.environ.get(CANDIDATE_LIMIT_ENV, str(DEFAULT_CANDIDATE_LIMIT))
            )
            cadence = float(os.environ.get(HISTORICAL_CADENCE_ENV, "1.0"))
        except ValueError as error:
            raise DailyPreparationError(
                "daily preparation numeric environment is invalid"
            ) from error
        return cls(
            database_path=database,
            base_universe_snapshot_path=_absolute(raw_base, BASE_UNIVERSE_PATH_ENV),
            artifact_directory=_absolute(raw_directory, ARTIFACT_DIRECTORY_ENV),
            evidence_path=_absolute(raw_evidence, EVIDENCE_PATH_ENV),
            esun_config_path=esun_config,
            candidate_limit=candidate_limit,
            historical_cadence_seconds=cadence,
        )


@dataclass(frozen=True, slots=True)
class DailyPreparationResult:
    target_date: date
    status: str
    snapshot_path: Path
    snapshot_file_sha256: str
    universe_canonical_sha256: str
    universe_run_id: str
    universe_created: bool
    counts: Mapping[str, int]
    network_requests: int
    provider_symbol_calls: int
    replayed_pipeline_runs: int
    resumed_pipeline_runs: int
    new_pipeline_runs: int
    stage1_ready: int
    stage1_legal_short_history: int
    illegal_canonical_sources: int

    def as_dict(self) -> dict[str, object]:
        return {
            "target_date": self.target_date.isoformat(),
            "status": self.status,
            "snapshot_path": str(self.snapshot_path),
            "snapshot_file_sha256": self.snapshot_file_sha256,
            "universe_canonical_sha256": self.universe_canonical_sha256,
            "universe_run_id": self.universe_run_id,
            "universe_created": self.universe_created,
            "counts": dict(self.counts),
            "network_requests": self.network_requests,
            "provider_symbol_calls": self.provider_symbol_calls,
            "replayed_pipeline_runs": self.replayed_pipeline_runs,
            "resumed_pipeline_runs": self.resumed_pipeline_runs,
            "new_pipeline_runs": self.new_pipeline_runs,
            "stage1_ready": self.stage1_ready,
            "stage1_legal_short_history": self.stage1_legal_short_history,
            "illegal_canonical_sources": self.illegal_canonical_sources,
        }


class CachingTwseTransport(TwseHttpTransport):
    """Cache successful official endpoint responses for one invocation."""

    def __init__(self, delegate: TwseHttpTransport | None = None) -> None:
        self.delegate = delegate or UrllibTwseHttpTransport()
        self.responses: dict[str, TwseHttpResponse] = {}
        self.network_requests = 0

    def get(self, url: str, *, timeout_seconds: float) -> TwseHttpResponse:
        cached = self.responses.get(url)
        if cached is not None:
            return cached
        self.network_requests += 1
        response = self.delegate.get(url, timeout_seconds=timeout_seconds)
        if response.status_code == 200:
            self.responses[url] = response
        return response


class PacedHistoricalProvider(MarketDataProvider):
    """Serialize candidate-only historical calls with a bounded cadence."""

    def __init__(
        self,
        delegate: MarketDataProvider,
        *,
        cadence_seconds: float,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if cadence_seconds < 0:
            raise ValueError("cadence_seconds must not be negative")
        self.delegate = delegate
        self.cadence_seconds = cadence_seconds
        self.sleep = sleep
        self.clock = clock
        self._last_call: float | None = None

    @property
    def source(self) -> str:
        return self.delegate.source

    def fetch_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        timeout_seconds: float,
    ):
        if self._last_call is not None:
            remaining = self.cadence_seconds - (self.clock() - self._last_call)
            if remaining > 0:
                self.sleep(remaining)
        try:
            return self.delegate.fetch_market_data(
                symbol,
                start_date,
                end_date,
                timeout_seconds=timeout_seconds,
            )
        finally:
            self._last_call = self.clock()


class CandidatePreparingDataset(ResearchDataset):
    """Prepare candidate history and complete the TWSE/E.SUN validation pair."""

    def __init__(
        self,
        database_path: Path,
        base: SQLiteScreenerResearchDataset,
        *,
        cadence_seconds: float,
        timeout_seconds: float,
        listing_dates: Mapping[str, date] | None = None,
        esun_config_path: Path | None = None,
    ) -> None:
        self.database_path = database_path
        self.base = base
        self.repository = SQLiteResearchRepository(database_path)
        retry_policy = RetryPolicy(max_attempts=3)
        twse_provider = PacedHistoricalProvider(
            TwseHistoricalMarketDataProvider(),
            cadence_seconds=cadence_seconds,
        )
        self.historical_provider = twse_provider
        self.esun_config_path = esun_config_path
        self.historical_validation_pipeline: CrossValidatedHistoricalResearchPipeline | None
        self.esun_pipeline: HistoricalSyncPipeline | None
        if esun_config_path is None:
            self.pipeline = HistoricalSyncPipeline(
                twse_provider,
                self.repository,
                provider_timeout_seconds=timeout_seconds,
                retry_policy=retry_policy,
            )
            self.historical_validation_pipeline = None
            self.esun_pipeline = None
        else:
            esun_provider = PacedHistoricalProvider(
                EsunHistoricalMarketDataProvider(config_path=esun_config_path),
                cadence_seconds=cadence_seconds,
            )
            self.historical_validation_pipeline = (
                CrossValidatedHistoricalResearchPipeline(
                    twse_provider,
                    esun_provider,
                    self.repository,
                    provider_timeout_seconds=timeout_seconds,
                    retry_policy=retry_policy,
                )
            )
            # Reuse the exact source pipelines owned by the pair coordinator.
            # This keeps the source checkpoints used for validation identical to
            # the checkpoints prepared for the candidate dataset.
            self.pipeline = self.historical_validation_pipeline.twse_sync
            self.esun_pipeline = self.historical_validation_pipeline.esun_sync
        self.listing_dates = dict(listing_dates or {})
        self.requested_candidates: list[str] = []
        self.reused_candidates: list[str] = []
        self.updated_candidates: list[str] = []
        self.resumed_candidates: list[str] = []
        self.legal_short_candidates: list[str] = []
        self.legal_short_observations: dict[str, int] = {}
        self.esun_records: dict[str, dict[str, object]] = {}
        self.validation_records: dict[str, dict[str, object]] = {}
        self._esun_results: dict[tuple[str, date], object] = {}
        self._validation_attempted: set[tuple[str, date]] = set()
        self._validated_pairs: set[tuple[str, date]] = set()

    def read(self, request: ResearchDatasetRequest, /):
        if request.history_observations != 250:
            return self.base.read(request)
        if request.symbol not in self.requested_candidates:
            self.requested_candidates.append(request.symbol)
        snapshot = self.base.read(request)
        existing = self._historical_run(request.symbol, request.as_of_date)
        if existing is not None and self._is_legal_short_checkpoint(
            request,
            existing,
        ):
            # A legal-short symbol cannot satisfy the two-sided 250-observation
            # contract.  Preserve the verified TWSE boundary and report that the
            # E.SUN pair was not applicable; never manufacture a shorter match.
            if self.historical_validation_pipeline is not None:
                self.esun_records[request.symbol] = {
                    "provider": "esun-historical",
                    "status": "skipped",
                    "reason": "twse_legal_short_history",
                }
            legal_short = self._read_legal_short(request, existing)
            if request.symbol not in self.reused_candidates:
                self.reused_candidates.append(request.symbol)
            self._record_legal_short(request.symbol, legal_short)
            return legal_short
        if self.historical_validation_pipeline is not None:
            return self._read_with_cross_validation(request, snapshot, existing)
        return self._read_twse_only(request, snapshot, existing)

    def ensure_candidate_validation(self, symbol: str, market_date: date) -> None:
        """Ensure one shortlist candidate has both source checkpoints.

        This hook is called even when frozen S5 replays an already-successful
        candidate.  It therefore closes the gap where strict S5 replay would
        otherwise skip the newly added E.SUN validation side effect.
        """

        if self.historical_validation_pipeline is None:
            return
        request = ResearchDatasetRequest(
            symbol=symbol,
            as_of_date=market_date,
            history_observations=250,
        )
        if request.symbol not in self.requested_candidates:
            self.requested_candidates.append(request.symbol)
        key = (request.symbol, request.as_of_date)
        if key in self._validated_pairs or key in self._validation_attempted:
            return
        existing = self._historical_run(request.symbol, request.as_of_date)
        if existing is not None and self._is_legal_short_checkpoint(
            request,
            existing,
        ):
            self.esun_records[request.symbol] = {
                "provider": "esun-historical",
                "status": "skipped",
                "reason": "twse_legal_short_history",
            }
            self._validated_pairs.add(key)
            return
        snapshot = self.base.read(request)
        self._read_with_cross_validation(request, snapshot, existing)

    def _read_twse_only(
        self,
        request: ResearchDatasetRequest,
        snapshot: object,
        existing: dict[str, object] | None,
    ):
        if (
            snapshot.price_history.status == "available"
            and len(snapshot.price_history.observations) >= 250
        ):
            if request.symbol not in self.reused_candidates:
                self.reused_candidates.append(request.symbol)
            return snapshot

        resume_run_id = None
        if (
            existing is not None
            and existing["status"] == PipelineRunStatus.RUNNING.value
        ):
            resume_run_id = str(existing["run_id"])
            self.resumed_candidates.append(request.symbol)
        try:
            result = self.pipeline.run(
                request.symbol,
                request.as_of_date,
                target_observations=250,
                max_months=18,
                resume_run_id=resume_run_id,
            )
        except ProviderInvalidRequestError:
            failed = self._historical_run(request.symbol, request.as_of_date)
            if failed is None or not self._is_legal_short_checkpoint(
                request,
                failed,
            ):
                raise
            legal_short = self._read_legal_short(request, failed)
            if request.symbol not in self.updated_candidates:
                self.updated_candidates.append(request.symbol)
            self._record_legal_short(request.symbol, legal_short)
            return legal_short
        if result.run_status is not PipelineRunStatus.SUCCESS:
            raise DailyPreparationError(
                "candidate-only historical preparation did not reach success"
            )
        self.updated_candidates.append(request.symbol)
        refreshed = self.base.read(request)
        if (
            refreshed.price_history.status != "available"
            or len(refreshed.price_history.observations) < 250
        ):
            raise DailyPreparationError(
                "candidate-only Stage 2 readiness remains incomplete"
            )
        return refreshed

    def _read_with_cross_validation(
        self,
        request: ResearchDatasetRequest,
        snapshot: object,
        existing: dict[str, object] | None,
    ):
        esun_result = self._sync_esun(request)
        if esun_result is None:
            record = self.esun_records.get(request.symbol, {})
            self.validation_records[request.symbol] = {
                "role": "validation",
                "status": "failed",
                "left_provider": "twse-historical",
                "right_provider": "esun-historical",
                "blocking": True,
                "error_type": record.get("error_type", "E.SUNPreparationError"),
                "error_message": record.get(
                    "error_message",
                    "E.SUN historical checkpoint did not reach success",
                ),
            }
        resume_run_id = None
        if (
            existing is not None
            and existing["status"] == PipelineRunStatus.RUNNING.value
        ):
            resume_run_id = str(existing["run_id"])
            if request.symbol not in self.resumed_candidates:
                self.resumed_candidates.append(request.symbol)
        try:
            twse_result = self.pipeline.run(
                request.symbol,
                request.as_of_date,
                target_observations=250,
                max_months=18,
                resume_run_id=resume_run_id,
            )
        except ProviderInvalidRequestError:
            failed = self._historical_run(request.symbol, request.as_of_date)
            if failed is None or not self._is_legal_short_checkpoint(
                request,
                failed,
            ):
                raise
            if request.symbol not in self.reused_candidates:
                self.reused_candidates.append(request.symbol)
            legal_short = self._read_legal_short(request, failed)
            self._record_legal_short(request.symbol, legal_short)
            self.esun_records[request.symbol] = {
                "provider": "esun-historical",
                "status": "skipped",
                "reason": "twse_legal_short_history",
            }
            return legal_short
        if twse_result.run_status is not PipelineRunStatus.SUCCESS:
            raise DailyPreparationError(
                "candidate-only TWSE historical preparation did not reach success"
            )
        if getattr(twse_result, "idempotent_replay", False):
            if request.symbol not in self.reused_candidates:
                self.reused_candidates.append(request.symbol)
        elif request.symbol not in self.updated_candidates:
            self.updated_candidates.append(request.symbol)
        refreshed = self.base.read(request)
        if (
            refreshed.price_history.status != "available"
            or len(refreshed.price_history.observations) < 250
        ):
            raise DailyPreparationError(
                "candidate-only Stage 2 readiness remains incomplete"
            )
        if esun_result is None:
            # E.SUN is validation-only on the canonical TWSE path.  Its strict
            # identity rejection and failure evidence remain recorded above,
            # but an unavailable/malformed validation payload must not turn a
            # complete TWSE history into a failed candidate.  Conversely, this
            # branch is reached only after canonical TWSE readiness is proven;
            # an incomplete TWSE path remains fail-closed above.
            self.validation_records[request.symbol]["blocking"] = False
            self._validation_attempted.add((request.symbol, request.as_of_date))
            return refreshed
        self._validate_historical_pair(request, twse_result, esun_result)
        self._validated_pairs.add((request.symbol, request.as_of_date))
        return refreshed

    def _sync_esun(self, request: ResearchDatasetRequest):
        if self.esun_pipeline is None:  # pragma: no cover - guarded by caller.
            return None
        key = (request.symbol, request.as_of_date)
        cached = self._esun_results.get(key)
        if cached is not None:
            return cached
        try:
            result = self.esun_pipeline.run(
                request.symbol,
                request.as_of_date,
                target_observations=250,
                max_months=18,
            )
        except Exception as error:
            self.esun_records[request.symbol] = {
                "provider": "esun-historical",
                "status": "failed",
                "error_type": type(error).__name__,
                "error_message": _safe_error_message(error),
            }
            return None
        if result.run_status is not PipelineRunStatus.SUCCESS:
            self.esun_records[request.symbol] = {
                "provider": "esun-historical",
                "status": "failed",
                "error_type": "HistoricalSyncStateError",
                "error_message": "E.SUN historical checkpoint did not reach success",
            }
            return None
        self._esun_results[key] = result
        self.esun_records[request.symbol] = {
            "provider": "esun-historical",
            "status": "success",
            **_historical_sync_evidence(result),
        }
        return result

    def _validate_historical_pair(
        self,
        request: ResearchDatasetRequest,
        twse_result: object,
        esun_result: object,
    ) -> None:
        coordinator = self.historical_validation_pipeline
        if coordinator is None:  # pragma: no cover - guarded by caller.
            return
        twse_run_id = str(getattr(twse_result, "run_id"))
        esun_run_id = str(getattr(esun_result, "run_id"))
        resume_validation_run_id = self._running_validation_run_id(
            twse_run_id,
            esun_run_id,
        )
        try:
            result = coordinator.run(
                request.symbol,
                request.as_of_date,
                target_observations=250,
                max_months=18,
                resume_twse_run_id=twse_run_id,
                resume_esun_run_id=esun_run_id,
                resume_validation_run_id=resume_validation_run_id,
            )
        except Exception as error:
            self.validation_records[request.symbol] = {
                "status": "failed",
                "left_provider": "twse-historical",
                "right_provider": "esun-historical",
                "error_type": type(error).__name__,
                "error_message": _safe_error_message(error),
            }
            raise DailyPreparationError(
                "candidate-only historical cross-validation did not reach success"
            ) from error
        self.validation_records[request.symbol] = {
            "status": "success",
            **_historical_validation_evidence(result),
        }

    def _running_validation_run_id(
        self,
        twse_run_id: str,
        esun_run_id: str,
    ) -> str | None:
        connection = sqlite3.connect(
            self.database_path.as_uri() + "?mode=ro",
            uri=True,
        )
        try:
            row = connection.execute(
                "SELECT run_id FROM historical_validation_runs "
                "WHERE left_historical_run_id = ? AND right_historical_run_id = ? "
                "AND status = ? LIMIT 1",
                (twse_run_id, esun_run_id, PipelineRunStatus.RUNNING.value),
            ).fetchone()
            return None if row is None else str(row[0])
        finally:
            connection.close()

    def _historical_run(
        self,
        symbol: str,
        target_date: date,
    ) -> dict[str, object] | None:
        connection = sqlite3.connect(
            self.database_path.as_uri() + "?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                "SELECT h.run_id, h.target_date, h.status, h.next_month, "
                "h.months_completed, "
                "h.observation_count, h.first_trade_date, h.last_trade_date, "
                "h.error_message, "
                "(SELECT COUNT(*) FROM historical_source_observations o "
                " WHERE o.historical_run_id = h.run_id) AS source_row_count, "
                "(SELECT COUNT(DISTINCT a.checkpoint_key) FROM source_artifacts a "
                " WHERE a.historical_run_id = h.run_id "
                " AND a.checkpoint_key LIKE 'historical-month:%') "
                " AS artifact_month_count, "
                "(SELECT p.run_id FROM pipeline_runs p "
                " WHERE p.symbol = h.symbol AND p.target_date = ? "
                " AND p.provider = 'twse' AND p.status = 'success' "
                " ORDER BY p.finished_at DESC, p.run_id DESC LIMIT 1) "
                " AS pipeline_run_id "
                "FROM historical_sync_runs h "
                "WHERE h.symbol = ? AND h.target_date <= ? "
                "AND h.target_observations = 250 "
                "AND h.provider = 'twse-historical' "
                "ORDER BY h.target_date DESC, h.updated_at DESC, h.run_id DESC "
                "LIMIT 1",
                (
                    target_date.isoformat(),
                    symbol,
                    target_date.isoformat(),
                ),
            ).fetchone()
            return None if row is None else dict(row)
        finally:
            connection.close()

    def _is_legal_short_checkpoint(
        self,
        request: ResearchDatasetRequest,
        checkpoint: Mapping[str, object],
    ) -> bool:
        listing_date = self.listing_dates.get(request.symbol)
        first = checkpoint.get("first_trade_date")
        last = checkpoint.get("last_trade_date")
        next_month = checkpoint.get("next_month")
        checkpoint_target = checkpoint.get("target_date")
        error_message = str(checkpoint.get("error_message") or "")
        if (
            listing_date is None
            or checkpoint.get("status") != PipelineRunStatus.FAILED.value
            or first is None
            or last is None
            or next_month is None
            or checkpoint_target is None
            or checkpoint.get("pipeline_run_id") is None
            or not error_message.startswith("ProviderInvalidRequestError:")
            or "no usable data" not in error_message
        ):
            return False
        observation_count = int(checkpoint.get("observation_count") or 0)
        months_completed = int(checkpoint.get("months_completed") or 0)
        source_row_count = int(checkpoint.get("source_row_count") or 0)
        artifact_month_count = int(checkpoint.get("artifact_month_count") or 0)
        return (
            0 < observation_count < 250
            and source_row_count == observation_count
            and months_completed > 0
            and artifact_month_count >= months_completed
            and date.fromisoformat(str(first)) == listing_date
            and date.fromisoformat(str(last))
            == date.fromisoformat(str(checkpoint_target))
            and date.fromisoformat(str(checkpoint_target)) <= request.as_of_date
            and date.fromisoformat(str(next_month)) < listing_date.replace(day=1)
        )

    def _read_legal_short(
        self,
        request: ResearchDatasetRequest,
        checkpoint: Mapping[str, object],
    ):
        resolved = replace(
            request,
            historical_run_id=str(checkpoint["run_id"]),
            pipeline_run_id=str(checkpoint["pipeline_run_id"]),
        )
        snapshot = self.base.read(resolved)
        observations = snapshot.price_history.observations
        listing_date = self.listing_dates[request.symbol]
        canonical = set(snapshot.provenance.canonical_sources)
        if (
            snapshot.price_history.status != "available"
            or not observations
            or len(observations) < int(checkpoint["observation_count"])
            or len(observations) >= 250
            or observations[0].trade_date != listing_date
            or observations[-1].trade_date != request.as_of_date
            or canonical - TWSE_BASELINE_SOURCES
        ):
            raise DailyPreparationError(
                "candidate legal-short historical checkpoint is not authoritative"
            )
        return snapshot

    def _record_legal_short(self, symbol: str, snapshot: object) -> None:
        if symbol not in self.legal_short_candidates:
            self.legal_short_candidates.append(symbol)
        self.legal_short_observations[symbol] = len(
            snapshot.price_history.observations  # type: ignore[attr-defined]
        )

    def evidence(self) -> dict[str, object]:
        return {
            "candidate_count": len(self.requested_candidates),
            "requested_candidates": self.requested_candidates,
            "reused_candidates": self.reused_candidates,
            "updated_candidates": self.updated_candidates,
            "resumed_candidates": self.resumed_candidates,
            "legal_short_candidates": self.legal_short_candidates,
            "legal_short_observations": self.legal_short_observations,
            "candidate_only": True,
            "esun_historical": {
                "enabled": self.esun_pipeline is not None,
                "provider": (
                    "esun-historical" if self.esun_pipeline is not None else None
                ),
                "canonical_write": False,
                "records": self.esun_records,
            },
            "historical_validation": {
                "enabled": self.historical_validation_pipeline is not None,
                "left_provider": (
                    "twse-historical"
                    if self.historical_validation_pipeline is not None
                    else None
                ),
                "right_provider": (
                    "esun-historical"
                    if self.historical_validation_pipeline is not None
                    else None
                ),
                "records": self.validation_records,
            },
        }


class ProductionDailyDataPreparer:
    """Incrementally prepare one authoritative target date for frozen S5."""

    def __init__(
        self,
        configuration: DailyPreparationConfiguration,
        *,
        transport_factory: Callable[[], CachingTwseTransport] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.configuration = configuration
        self.transport_factory = transport_factory or CachingTwseTransport
        self.sleep = sleep

    def prepare(self, target_date: date) -> DailyPreparationResult:
        target = _require_date(target_date, "target_date")
        self._require_v11()
        base = _load_snapshot(self.configuration.base_universe_snapshot_path)
        target_path = self._target_snapshot_path(target)
        existing = _load_snapshot(target_path) if target_path.is_file() else None
        if existing is not None:
            self._validate_target_snapshot(existing, target)
            readiness = self._readiness(existing, target)
            if readiness["complete"] is True:
                persistence = SQLiteMarketUniverseRepository(
                    self.configuration.database_path
                ).persist(existing)
                return self._result(
                    target=target,
                    snapshot=existing,
                    snapshot_path=target_path,
                    universe_run_id=persistence.universe_run_id,
                    universe_created=persistence.created,
                    status="prepared_replay",
                    network_requests=0,
                    pipeline_stats={
                        "provider_symbol_calls": 0,
                        "replayed_pipeline_runs": existing.scan_eligible_count,
                        "resumed_pipeline_runs": 0,
                        "new_pipeline_runs": 0,
                    },
                    readiness=readiness,
                )

        transport = self.transport_factory()
        provider = TwseMarketDataProvider(transport=transport)
        snapshot = self._acquire_universe(provider, base, target)
        if existing is not None and existing.canonical_json() != snapshot.canonical_json():
            raise DailyPreparationError(
                "partial preparation authoritative Universe evidence changed"
            )
        self._persist_snapshot_file(target_path, snapshot)
        persistence = SQLiteMarketUniverseRepository(
            self.configuration.database_path
        ).persist(snapshot)
        pipeline_stats = self._prepare_stage1_inputs(
            snapshot,
            target,
            provider,
        )
        readiness = self._readiness(snapshot, target)
        if readiness["complete"] is not True:
            raise DailyPreparationError(
                "full-Universe Stage 1 preparation is incomplete"
            )
        return self._result(
            target=target,
            snapshot=snapshot,
            snapshot_path=target_path,
            universe_run_id=persistence.universe_run_id,
            universe_created=persistence.created,
            status="prepared" if existing is None else "prepared_resume",
            network_requests=transport.network_requests,
            pipeline_stats=pipeline_stats,
            readiness=readiness,
        )

    def _target_snapshot_path(self, target: date) -> Path:
        return self.configuration.artifact_directory / (
            f"twse_universe_{target:%Y%m%d}.s6e1-daily.canonical.json"
        )

    def _require_v11(self) -> None:
        database = self.configuration.database_path
        if not database.is_file():
            raise DailyPreparationError("production database does not exist")
        connection = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
        try:
            version = int(
                connection.execute(
                    "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
                ).fetchone()[0]
            )
            if version not in {11, 12}:
                raise DailyPreparationError(
                    "daily preparation requires schema v11 or v12, "
                    f"found {version}"
                )
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise DailyPreparationError("production database integrity failed")
            if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise DailyPreparationError("production database foreign keys failed")
        finally:
            connection.close()

    def _acquire_universe(
        self,
        provider: TwseMarketDataProvider,
        base: MarketUniverseSnapshot,
        target: date,
    ) -> MarketUniverseSnapshot:
        stock_records = metric_records = None
        stock_response = metric_response = None
        retry = RetryPolicy(max_attempts=3)
        for attempt in range(1, retry.max_attempts + 1):
            try:
                stock_records, stock_response = provider._request_records(
                    STOCK_DAY_ALL_URL,
                    required_fields=_STOCK_DAY_FIELDS,
                    timeout_seconds=self.configuration.timeout_seconds,
                )
                metric_records, metric_response = provider._request_records(
                    BWIBBU_ALL_URL,
                    required_fields=_BWIBBU_FIELDS,
                    timeout_seconds=self.configuration.timeout_seconds,
                )
                break
            except ProviderTemporaryError:
                if attempt >= retry.max_attempts:
                    raise
                self.sleep(retry.delay_after_failure(attempt))
        if (
            stock_records is None
            or metric_records is None
            or stock_response is None
            or metric_response is None
        ):
            raise DailyPreparationError("official daily snapshots were not acquired")

        stock_by_code = provider._index_by_code(stock_records, STOCK_DAY_ALL_URL)
        metric_by_code = provider._index_by_code(metric_records, BWIBBU_ALL_URL)
        identity_records, identity_response = self._request_official_array(
            provider.transport,
            IDENTITY_URL,
        )
        delisting_records, delisting_response = self._request_official_array(
            provider.transport,
            DELISTING_URL,
        )
        trading: list[DailyTradingRecord] = []
        valuations: list[ValuationCoverageRecord] = []
        identities: list[ListedIdentityRecord] = []
        delistings: list[DelistingRecord] = []
        stock_dates: set[date] = set()
        metric_dates: set[date] = set()
        for symbol in sorted(stock_by_code):
            if _COMMON_STOCK_CODE.fullmatch(symbol) is None:
                continue
            item = stock_by_code[symbol]
            name = item["Name"].strip()
            if not name:
                raise DailyPreparationError("STOCK_DAY_ALL name is blank")
            stock_dates.add(provider._parse_roc_date(item["Date"], "Date"))
            ohlc = tuple(
                provider._parse_decimal(item[field], field)
                for field in (
                    "OpeningPrice",
                    "HighestPrice",
                    "LowestPrice",
                    "ClosingPrice",
                )
            )
            if all(value == 0 for value in ohlc):
                available = False
            elif any(value <= 0 for value in ohlc):
                raise DailyPreparationError(
                    "STOCK_DAY_ALL contains partial/non-positive OHLC"
                )
            else:
                available = True
            trading.append(DailyTradingRecord(symbol, name, available))

        for symbol in sorted(metric_by_code):
            if _COMMON_STOCK_CODE.fullmatch(symbol) is None:
                continue
            item = metric_by_code[symbol]
            name = item["Name"].strip()
            if not name:
                raise DailyPreparationError("BWIBBU_ALL name is blank")
            metric_dates.add(provider._parse_roc_date(item["Date"], "Date"))
            valuations.append(ValuationCoverageRecord(symbol, name))
        if stock_dates != {target} or metric_dates != {target}:
            raise DailyPreparationError(
                "official daily source date differs from formal latest date"
            )

        stable = _stable_inputs_from_snapshot(base)
        stable_classifications = {
            item.symbol: item.classification
            for item in stable.classifications.records
        }
        for item in identity_records:
            if not isinstance(item, dict):
                raise DailyPreparationError("identity endpoint row is not an object")
            symbol = _clean_text(item.get("公司代號"))
            if _COMMON_STOCK_CODE.fullmatch(symbol) is None:
                continue
            name = _clean_text(item.get("公司簡稱"))
            listing_date = _official_date(item.get("上市日期"))
            if not name or listing_date is None:
                raise DailyPreparationError(
                    "identity endpoint row lacks name/listing date"
                )
            identities.append(ListedIdentityRecord(symbol, name, listing_date))
        identity_symbols = {item.symbol for item in identities}
        identity_by_symbol = {item.symbol: item for item in identities}

        current_symbols = {
            *(
                symbol
                for symbol in stock_by_code
                if _COMMON_STOCK_CODE.fullmatch(symbol) is not None
            ),
            *(
                symbol
                for symbol in metric_by_code
                if _COMMON_STOCK_CODE.fullmatch(symbol) is not None
            ),
            *identity_symbols,
            *stable_classifications,
        }
        classifications = tuple(
            InstrumentClassificationRecord(
                symbol,
                (
                    stable_classifications[symbol]
                    if stable_classifications.get(symbol)
                    in {InstrumentClassification.ETF, InstrumentClassification.TDR}
                    else (
                        InstrumentClassification.COMMON_EQUITY
                        if symbol in identity_symbols
                        else stable_classifications.get(
                            symbol, InstrumentClassification.UNKNOWN
                        )
                    )
                ),
            )
            for symbol in sorted(current_symbols)
        )
        for item in delisting_records:
            if not isinstance(item, dict):
                raise DailyPreparationError("delisting endpoint row is not an object")
            symbol = _clean_text(item.get("Code"))
            if _COMMON_STOCK_CODE.fullmatch(symbol) is None:
                continue
            delisting_date = _official_date(item.get("DelistingDate"))
            if delisting_date is None:
                continue
            current_identity = identity_by_symbol.get(symbol)
            if current_identity is not None:
                # This endpoint is historical.  A code present in the current
                # official listed-company identity remains active even when a
                # predecessor/merger entry exists in delisting history.
                continue
            delistings.append(
                DelistingRecord(
                    symbol,
                    _clean_text(item.get("Company")) or None,
                    delisting_date,
                )
            )

        classification_payload = {
            "contract_version": "screener-s1-ordinary-stock-v1",
            "identity_payload_sha256": hashlib.sha256(
                identity_response.body
            ).hexdigest(),
            "preserved_non_common_equity": sorted(
                symbol
                for symbol, classification in stable_classifications.items()
                if classification
                in {InstrumentClassification.ETF, InstrumentClassification.TDR}
            ),
            "records": [
                {
                    "symbol": item.symbol,
                    "classification": item.classification.value,
                }
                for item in classifications
            ],
        }
        classification_bytes = json.dumps(
            classification_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        build_input = MarketUniverseBuildInput(
            market_date=target,
            listed_identity=InputDatasetSnapshot(
                _raw_source_evidence(
                    provider,
                    dataset=IDENTITY_DATASET,
                    endpoint=IDENTITY_URL,
                    response=identity_response,
                ),
                tuple(identities),
            ),
            daily_trading=InputDatasetSnapshot(
                _raw_source_evidence(
                    provider,
                    dataset=STOCK_DAY_ALL_DATASET,
                    endpoint=STOCK_DAY_ALL_URL,
                    response=stock_response,
                ),
                tuple(trading),
            ),
            valuation_coverage=InputDatasetSnapshot(
                _raw_source_evidence(
                    provider,
                    dataset=VALUATION_DATASET,
                    endpoint=BWIBBU_ALL_URL,
                    response=metric_response,
                ),
                tuple(valuations),
            ),
            classifications=InputDatasetSnapshot(
                SourceEvidence(
                    source="twse",
                    dataset=CLASSIFICATION_DATASET,
                    source_ref=(
                        "contract://market-screener/s1/"
                        "ordinary-stock-classification-v1"
                    ),
                    contract_version="screener-s1-ordinary-stock-v1",
                    payload_sha256=hashlib.sha256(
                        classification_bytes
                    ).hexdigest(),
                    payload_size_bytes=len(classification_bytes),
                    hash_basis="canonical-json-v1",
                ),
                classifications,
            ),
            delistings=InputDatasetSnapshot(
                _raw_source_evidence(
                    provider,
                    dataset=DELISTING_DATASET,
                    endpoint=DELISTING_URL,
                    response=delisting_response,
                ),
                tuple(delistings),
            ),
            methodology_version=base.methodology_version,
            source_policy=base.source_policy,
        )
        snapshot = build_market_universe(build_input)
        self._validate_target_snapshot(snapshot, target)
        if snapshot.unresolved_count:
            unresolved = [
                f"{member.symbol}:{member.exclusion_reason}"
                for member in snapshot.members
                if member.status is UniverseMemberStatus.CLASSIFICATION_UNRESOLVED
            ]
            raise DailyPreparationError(
                "target-date Universe contains unresolved classifications: "
                + ", ".join(unresolved[:20])
            )
        return snapshot

    def _request_official_array(
        self,
        transport: TwseHttpTransport,
        url: str,
    ) -> tuple[list[object], TwseHttpResponse]:
        retry = RetryPolicy(max_attempts=3)
        for attempt in range(1, retry.max_attempts + 1):
            try:
                response = transport.get(
                    url,
                    timeout_seconds=self.configuration.timeout_seconds,
                )
            except ProviderTemporaryError:
                if attempt >= retry.max_attempts:
                    raise
                self.sleep(retry.delay_after_failure(attempt))
                continue
            if response.status_code == 200:
                try:
                    payload = json.loads(response.body.decode("utf-8-sig"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise DailyPreparationError(
                        "official identity/delisting response is malformed"
                    ) from error
                if not isinstance(payload, list) or not payload:
                    raise DailyPreparationError(
                        "official identity/delisting response must be an array"
                    )
                return payload, response
            if response.status_code not in {307, 429, 500, 502, 503, 504}:
                raise DailyPreparationError(
                    f"official endpoint returned HTTP {response.status_code}"
                )
            if attempt >= retry.max_attempts:
                raise DailyPreparationError(
                    "official endpoint retry budget was exhausted"
                )
            self.sleep(retry.delay_after_failure(attempt))
        raise AssertionError("official endpoint retry loop exhausted")

    def _persist_snapshot_file(
        self,
        path: Path,
        snapshot: MarketUniverseSnapshot,
    ) -> None:
        rendered = (
            json.dumps(
                snapshot.as_dict(),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        if path.is_file():
            current = _load_snapshot(path)
            if current.canonical_json() != snapshot.canonical_json():
                raise DailyPreparationError(
                    "target-date Universe artifact collision"
                )
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)

    def _prepare_stage1_inputs(
        self,
        snapshot: MarketUniverseSnapshot,
        target: date,
        provider: TwseMarketDataProvider,
    ) -> dict[str, int]:
        repository = SQLiteResearchRepository(self.configuration.database_path)
        pipeline = DailyResearchPipeline(
            provider,
            repository,
            retry_policy=RetryPolicy(max_attempts=3),
            provider_timeout_seconds=self.configuration.timeout_seconds,
        )
        replayed = resumed = created = calls = 0
        eligible = sorted(
            member.symbol
            for member in snapshot.members
            if member.status is UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE
        )
        for symbol in eligible:
            existing = repository.get_pipeline_run_for_target(symbol, target)
            if (
                existing is not None
                and existing.status is PipelineRunStatus.SUCCESS
                and existing.provider == "twse"
            ):
                result = pipeline.run(symbol, target, target)
                if not result.idempotent_replay:
                    raise DailyPreparationError(
                        "successful daily checkpoint did not replay"
                    )
                replayed += 1
                continue
            if existing is not None and existing.provider != "twse":
                raise DailyPreparationError(
                    "target-date pipeline owner is not authoritative TWSE"
                )
            resume_run_id = None
            if existing is None:
                created += 1
            elif existing.status is PipelineRunStatus.RUNNING:
                resume_run_id = existing.run_id
                resumed += 1
            else:
                resumed += 1
            result = pipeline.run(
                symbol,
                target,
                target,
                resume_run_id=resume_run_id,
            )
            calls += 1
            if (
                result.run_status is not PipelineRunStatus.SUCCESS
                or result.market_date != target
                or result.provider_source != "twse"
            ):
                raise DailyPreparationError(
                    "daily Stage 1 input checkpoint did not reach TWSE success"
                )
        return {
            "provider_symbol_calls": calls,
            "replayed_pipeline_runs": replayed,
            "resumed_pipeline_runs": resumed,
            "new_pipeline_runs": created,
        }

    def _readiness(
        self,
        snapshot: MarketUniverseSnapshot,
        target: date,
    ) -> dict[str, object]:
        dataset = SQLiteScreenerResearchDataset(self.configuration.database_path)
        ready = legal_short = illegal = 0
        missing: list[str] = []
        eligible = tuple(
            member
            for member in snapshot.members
            if member.status is UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE
        )
        for member in eligible:
            try:
                value = dataset.read(
                    ResearchDatasetRequest(
                        symbol=member.symbol,
                        as_of_date=target,
                        history_observations=(
                            STAGE1_METHODOLOGY_V1.history_observations
                        ),
                    )
                )
            except Exception:
                missing.append(member.symbol)
                continue
            canonical = set(value.provenance.canonical_sources)
            unexpected = canonical - TWSE_BASELINE_SOURCES
            if unexpected:
                illegal += 1
                missing.append(member.symbol)
                continue
            if (
                value.price_history.status != "available"
                or not value.price_history.observations
                or value.price_history.observations[-1].trade_date != target
            ):
                missing.append(member.symbol)
                continue
            ready += 1
            if (
                len(value.price_history.observations)
                < STAGE1_METHODOLOGY_V1.history_observations
            ):
                legal_short += 1
        return {
            "complete": ready == len(eligible) and illegal == 0,
            "eligible": len(eligible),
            "stage1_ready": ready,
            "stage1_legal_short_history": legal_short,
            "illegal_canonical_sources": illegal,
            "missing_symbols": missing,
        }

    def _result(
        self,
        *,
        target: date,
        snapshot: MarketUniverseSnapshot,
        snapshot_path: Path,
        universe_run_id: str,
        universe_created: bool,
        status: str,
        network_requests: int,
        pipeline_stats: Mapping[str, int],
        readiness: Mapping[str, object],
    ) -> DailyPreparationResult:
        return DailyPreparationResult(
            target_date=target,
            status=status,
            snapshot_path=snapshot_path,
            snapshot_file_sha256=_sha256_file(snapshot_path),
            universe_canonical_sha256=snapshot.payload_sha256,
            universe_run_id=universe_run_id,
            universe_created=universe_created,
            counts={
                "universe_total": snapshot.universe_count,
                "eligible": snapshot.scan_eligible_count,
                "unavailable": snapshot.scan_unavailable_count,
                "excluded": snapshot.excluded_count,
                "inactive": snapshot.inactive_count,
                "unresolved": snapshot.unresolved_count,
            },
            network_requests=network_requests,
            provider_symbol_calls=int(pipeline_stats["provider_symbol_calls"]),
            replayed_pipeline_runs=int(
                pipeline_stats["replayed_pipeline_runs"]
            ),
            resumed_pipeline_runs=int(
                pipeline_stats["resumed_pipeline_runs"]
            ),
            new_pipeline_runs=int(pipeline_stats["new_pipeline_runs"]),
            stage1_ready=int(readiness["stage1_ready"]),
            stage1_legal_short_history=int(
                readiness["stage1_legal_short_history"]
            ),
            illegal_canonical_sources=int(
                readiness["illegal_canonical_sources"]
            ),
        )

    @staticmethod
    def _validate_target_snapshot(
        snapshot: MarketUniverseSnapshot,
        target: date,
    ) -> None:
        if snapshot.market_date != target:
            raise DailyPreparationError(
                "Universe snapshot date differs from preparation target"
            )
        if snapshot.methodology_version != UNIVERSE_METHODOLOGY_VERSION:
            raise DailyPreparationError("Universe methodology drifted")
        if snapshot.source_policy != "twse_baseline":
            raise DailyPreparationError("Universe source policy drifted")


class PreparedProductionS5:
    """Application/deployment wrapper: preparation, then frozen S5."""

    def __init__(self, configuration: DailyPreparationConfiguration) -> None:
        self.configuration = configuration
        self.preparer = ProductionDailyDataPreparer(configuration)

    def __call__(self, market_date: date):
        target = _require_date(market_date, "market_date")
        started = datetime.now(timezone.utc)
        preparation: DailyPreparationResult | None = None
        dataset: CandidatePreparingDataset | None = None
        payload: dict[str, object] = {
            "contract_version": DAILY_PREPARATION_CONTRACT_VERSION,
            "target_date": target.isoformat(),
            "started_at": started.isoformat(),
            "frozen_s5_factory": PRODUCTION_S5_FACTORY_LOCATOR,
        }
        try:
            preparation = self.preparer.prepare(target)
            universe = _load_snapshot(preparation.snapshot_path)
            base_dataset = SQLiteScreenerResearchDataset(
                self.configuration.database_path
            )
            dataset = CandidatePreparingDataset(
                self.configuration.database_path,
                base_dataset,
                cadence_seconds=self.configuration.historical_cadence_seconds,
                timeout_seconds=self.configuration.timeout_seconds,
                esun_config_path=self.configuration.esun_config_path,
                listing_dates={
                    member.symbol: member.listing_date
                    for member in universe.members
                    if member.listing_date is not None
                },
            )
            s5_configuration = ProductionS5Configuration(
                database_path=self.configuration.database_path,
                universe_snapshot_path=preparation.snapshot_path,
                candidate_limit=self.configuration.candidate_limit,
            )
            result = _compose_s5(s5_configuration, dataset)(target)
            payload.update(
                {
                    "status": (
                        "success" if result.status == "success" else "failed"
                    ),
                    "failure_boundary": (
                        None
                        if result.status == "success"
                        else "frozen_s5_non_success"
                    ),
                    "preparation": preparation.as_dict(),
                    "candidate_stage2_preparation": dataset.evidence(),
                    "s5_result": {
                        "status": result.status,
                        "replayed": result.replayed,
                        "screener_run_id": result.screener_run_id,
                        "canonical_sha256": result.canonical_sha256,
                    },
                }
            )
        except Exception as error:
            payload.update(
                {
                    "status": "failed",
                    "error_type": type(error).__name__,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            if preparation is not None:
                payload["preparation"] = preparation.as_dict()
            if dataset is not None:
                payload["candidate_stage2_preparation"] = dataset.evidence()
            _write_json(self.configuration.evidence_path, payload)
            raise
        payload["finished_at"] = datetime.now(timezone.utc).isoformat()
        _write_json(self.configuration.evidence_path, payload)
        return result

    @property
    def parity(self) -> dict[str, object]:
        return {
            "contract_version": DAILY_PREPARATION_CONTRACT_VERSION,
            "responsibility": "authoritative inputs before frozen S5",
            "frozen_s5_factory": PRODUCTION_S5_FACTORY_LOCATOR,
            "candidate_limit": self.configuration.candidate_limit,
            "esun_historical_validation": self.configuration.esun_config_path
            is not None,
        }


def create_prepared_s5(database_path: str | Path) -> PreparedProductionS5:
    frozen = os.environ.get(FROZEN_S5_FACTORY_ENV, "").strip()
    if frozen != PRODUCTION_S5_FACTORY_LOCATOR:
        raise DailyPreparationError(
            "prepared production entrypoint requires the frozen formal S5 locator"
        )
    return PreparedProductionS5(
        DailyPreparationConfiguration.from_environment(database_path)
    )


def _stable_inputs_from_snapshot(
    snapshot: MarketUniverseSnapshot,
) -> MarketUniverseBuildInput:
    evidence = _common_evidence_by_dataset(snapshot)
    identities: list[ListedIdentityRecord] = []
    classifications: list[InstrumentClassificationRecord] = []
    delistings: list[DelistingRecord] = []
    for member in snapshot.members:
        if member.name is None:
            raise DailyPreparationError("base Universe member name is missing")
        if member.listing_date is not None:
            identities.append(
                ListedIdentityRecord(
                    member.symbol,
                    member.name,
                    member.listing_date,
                )
            )
        if member.status is UniverseMemberStatus.CLASSIFICATION_UNRESOLVED:
            raise DailyPreparationError(
                "base Universe has unresolved classification"
            )
        if member.status is UniverseMemberStatus.EXCLUDED_NON_COMMON_EQUITY:
            if member.exclusion_reason == "non_common_equity_etf":
                classification = InstrumentClassification.ETF
            elif member.exclusion_reason == "non_common_equity_tdr":
                classification = InstrumentClassification.TDR
            else:
                raise DailyPreparationError(
                    "base non-common-equity classification is unsupported"
                )
        else:
            classification = InstrumentClassification.COMMON_EQUITY
        classifications.append(
            InstrumentClassificationRecord(member.symbol, classification)
        )
        if member.delisting_date is not None:
            delistings.append(
                DelistingRecord(
                    member.symbol,
                    member.name,
                    member.delisting_date,
                )
            )
    return MarketUniverseBuildInput(
        market_date=snapshot.market_date,
        listed_identity=InputDatasetSnapshot(
            evidence[IDENTITY_DATASET], tuple(identities)
        ),
        daily_trading=InputDatasetSnapshot(
            evidence[STOCK_DAY_ALL_DATASET], ()
        ),
        valuation_coverage=InputDatasetSnapshot(
            evidence[VALUATION_DATASET], ()
        ),
        classifications=InputDatasetSnapshot(
            evidence[CLASSIFICATION_DATASET], tuple(classifications)
        ),
        delistings=InputDatasetSnapshot(
            evidence[DELISTING_DATASET], tuple(delistings)
        ),
        methodology_version=snapshot.methodology_version,
        source_policy=snapshot.source_policy,
    )


def _common_evidence_by_dataset(
    snapshot: MarketUniverseSnapshot,
) -> dict[str, SourceEvidence]:
    if not snapshot.members:
        raise DailyPreparationError("base Universe has no members")
    common = snapshot.members[0].source_evidence
    if any(member.source_evidence != common for member in snapshot.members):
        raise DailyPreparationError("base Universe source evidence is not common")
    by_dataset = {item.dataset: item for item in common}
    expected = {
        IDENTITY_DATASET,
        STOCK_DAY_ALL_DATASET,
        VALUATION_DATASET,
        CLASSIFICATION_DATASET,
        DELISTING_DATASET,
    }
    if set(by_dataset) != expected:
        raise DailyPreparationError("base Universe source evidence roles drifted")
    return by_dataset


def _raw_source_evidence(
    provider: TwseMarketDataProvider,
    *,
    dataset: str,
    endpoint: str,
    response: TwseHttpResponse,
) -> SourceEvidence:
    return SourceEvidence(
        source="twse",
        dataset=dataset,
        source_ref=endpoint,
        contract_version=provider.manifest.contract_version,
        payload_sha256=hashlib.sha256(response.body).hexdigest(),
        payload_size_bytes=len(response.body),
        hash_basis="raw-response-bytes-v1",
    )


def _load_snapshot(path: Path) -> MarketUniverseSnapshot:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DailyPreparationError("Universe artifact could not be loaded") from error
    try:
        return _snapshot_from_dict(payload)
    except Exception as error:
        raise DailyPreparationError("Universe artifact contract is invalid") from error


def _write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _historical_sync_evidence(result: object) -> dict[str, object]:
    analysis = getattr(result, "analysis", None)
    return {
        "run_id": str(getattr(result, "run_id")),
        "idempotent_replay": bool(getattr(result, "idempotent_replay", False)),
        "observation_count": int(getattr(result, "observation_count", 0)),
        "months_completed": int(getattr(result, "months_completed", 0)),
        "provider_attempts": int(getattr(result, "provider_attempts", 0)),
        "first_trade_date": _date_text(getattr(analysis, "period_start", None)),
        "last_trade_date": _date_text(getattr(analysis, "period_end", None)),
    }


def _historical_validation_evidence(result: object) -> dict[str, object]:
    outcome = getattr(result, "outcome", None)
    left_sync = getattr(result, "left_sync", None)
    right_sync = getattr(result, "right_sync", None)
    return {
        "run_id": str(getattr(result, "run_id")),
        "idempotent_replay": bool(getattr(result, "idempotent_replay", False)),
        "outcome": getattr(outcome, "value", outcome),
        "common_date_count": int(getattr(result, "common_date_count", 0)),
        "matched_date_count": int(getattr(result, "matched_date_count", 0)),
        "left_only_date_count": int(getattr(result, "left_only_date_count", 0)),
        "right_only_date_count": int(getattr(result, "right_only_date_count", 0)),
        "field_discrepancy_count": int(
            getattr(result, "field_discrepancy_count", 0)
        ),
        "left_latest_date": _date_text(getattr(result, "left_latest_date", None)),
        "right_latest_date": _date_text(getattr(result, "right_latest_date", None)),
        "twse_run_id": None if left_sync is None else str(getattr(left_sync, "run_id")),
        "esun_run_id": None if right_sync is None else str(getattr(right_sync, "run_id")),
    }


def _date_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|secret|token|password)\b\s*[:=]\s*[^\s,;]+"
)
_BEARER_TOKEN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")


def _safe_error_message(error: BaseException) -> str:
    rendered = str(error).replace("\r", " ").replace("\n", " ").strip()
    rendered = _SECRET_ASSIGNMENT.sub(r"\1=[REDACTED]", rendered)
    rendered = _BEARER_TOKEN.sub("Bearer [REDACTED]", rendered)
    return rendered[:1000] or type(error).__name__


def _clean_text(value: object) -> str:
    rendered = html.unescape("" if value is None else str(value)).strip()
    return re.sub(r"<[^>]+>", "", rendered).strip()


def _official_date(value: object) -> date | None:
    raw = re.sub(r"\D", "", _clean_text(value))
    if not raw:
        return None
    if len(raw) == 8:
        year = int(raw[:4])
        month = int(raw[4:6])
        day = int(raw[6:8])
    elif len(raw) == 7:
        year = int(raw[:3]) + 1911
        month = int(raw[3:5])
        day = int(raw[5:7])
    else:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _absolute(value: str | Path, field_name: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise DailyPreparationError(f"{field_name} must be absolute")
    return path.resolve()


def _require_date(value: object, field_name: str) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a date")
    return value


__all__ = [
    "ARTIFACT_DIRECTORY_ENV",
    "BASE_UNIVERSE_PATH_ENV",
    "CachingTwseTransport",
    "CandidatePreparingDataset",
    "DAILY_PREPARATION_CONTRACT_VERSION",
    "DailyPreparationConfiguration",
    "DailyPreparationError",
    "DailyPreparationResult",
    "ESUN_CONFIG_PATH_ENV",
    "EVIDENCE_PATH_ENV",
    "FROZEN_S5_FACTORY_ENV",
    "HISTORICAL_CADENCE_ENV",
    "PREPARED_S5_FACTORY_LOCATOR",
    "PacedHistoricalProvider",
    "PreparedProductionS5",
    "ProductionDailyDataPreparer",
    "create_prepared_s5",
]
