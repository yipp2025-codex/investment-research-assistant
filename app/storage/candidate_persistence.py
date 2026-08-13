"""SQLite candidate checkpoint boundary for Market Screener S4.3.

The repository persists deterministic Stage 1 shortlist shells and atomic
Stage 2 candidate bundles.  It never performs research, assigns final rank,
or transitions a Screener run to final success; those remain outside S4.3.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator
from urllib.parse import parse_qsl, urlsplit

from app.research_dataset import (
    ESUN_VALIDATION_SOURCES,
    MOCK_SYNTHETIC_SOURCE,
    TWSE_BASELINE_SOURCE_POLICY,
    TWSE_BASELINE_SOURCES,
)
from app.screener.stage1 import (
    STAGE1_METHODOLOGY_VERSION,
    Stage1Candidate,
    Stage1Reason,
    Stage1ScanResult,
)
from app.screener.stage2 import (
    STAGE2_METHODOLOGY_VERSION,
    Stage2AnalysisStatus,
    Stage2ArtifactRef,
    Stage2Candidate,
    Stage2CandidateKind,
    Stage2DataQuality,
    Stage2Discrepancy,
    Stage2Failure,
    Stage2Metric,
    Stage2MetricStatus,
    Stage2Provenance,
    Stage2QualityStatus,
    Stage2Reason,
)
from app.storage.dataset_versions import (
    DatasetMigrationStateError,
    DatasetPersistenceIntegrityError,
    DatasetVersionMigrationRunner,
    DatasetVersionNotFoundError,
    DatasetVersionRepository,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID_VERSION = "screener-run-id-v1"
_MANIFEST_VERSION = "screener-input-manifest-v1"
_CANDIDATE_ID_VERSION = "screener-candidate-id-v1"
_CANDIDATE_INPUT_VERSION = "screener-candidate-input-v1"
_CHECKPOINT_VERSION = "screener-candidate-checkpoint-v1"
_SENSITIVE_QUERY_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "access_token",
        "api_token",
        "api-token",
        "x-api-key",
        "x_api_key",
        "authorization",
        "auth",
        "bearer",
        "credential",
        "credentials",
        "key",
        "password",
        "secret",
        "signature",
        "token",
        "client_secret",
        "private_key",
        "x-amz-credential",
        "x-amz-signature",
    }
)


class ScreenerCheckpointError(RuntimeError):
    """Base error for S4.3 run and candidate checkpoints."""


class ScreenerCheckpointConflictError(ScreenerCheckpointError):
    """Stored immutable checkpoint state conflicts with deterministic input."""


class ScreenerCheckpointStateError(ScreenerCheckpointError):
    """A run or candidate is not in a valid checkpoint transition state."""


class CandidateSourcePolicyError(ScreenerCheckpointError):
    """Candidate provenance violates the TWSE canonical source boundary."""


@dataclass(frozen=True, slots=True)
class CandidateInputLocator:
    symbol: str
    research_locator_sha256: str | None = None
    dataset_version_id: str | None = None
    source_policy: str = TWSE_BASELINE_SOURCE_POLICY
    source_status: str = "canonical_complete"
    authority_status: str = "complete"
    reconciliation_status: str = "not_applicable"
    research_data_quality: str = "canonical"
    supplemental_count: int = 0
    twse_observation_count: int = 0
    missing_twse_count: int = 0
    discrepancy_count: int = 0
    supplemental_sources: tuple[str, ...] = ()
    provenance_map_sha256: str | None = None

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper() if isinstance(self.symbol, str) else ""
        if not symbol or len(symbol) > 12 or not symbol.isalnum():
            raise ValueError("locator symbol must be 2-12 uppercase letters or digits")
        if len(symbol) < 2:
            raise ValueError("locator symbol must be 2-12 uppercase letters or digits")
        object.__setattr__(self, "symbol", symbol)
        if self.source_policy not in {TWSE_BASELINE_SOURCE_POLICY, "twse_dual_source_v1"}:
            raise ValueError("source_policy is unsupported")
        if self.dataset_version_id is None:
            _require_sha256(self.research_locator_sha256, "research_locator_sha256")
            if self.source_policy != TWSE_BASELINE_SOURCE_POLICY:
                raise ValueError("dual-source locator requires dataset_version_id")
        else:
            _require_sha256(self.dataset_version_id, "dataset_version_id")
            if self.source_policy != "twse_dual_source_v1":
                raise ValueError("dataset_version_id requires the dual-source policy")
            _require_sha256(self.provenance_map_sha256, "provenance_map_sha256")
            if self.source_status not in {"canonical_complete", "provisional_mixed", "reconciled"}:
                raise ValueError("source_status is unsupported")
            if self.authority_status not in {"complete", "incomplete", "reconciled"}:
                raise ValueError("authority_status is unsupported")
            if self.reconciliation_status not in {
                "not_applicable", "pending", "reconciled_equal", "reconciled_discrepant"
            }:
                raise ValueError("reconciliation_status is unsupported")
            if self.research_data_quality not in {"canonical", "provisional", "reconciled"}:
                raise ValueError("research_data_quality is unsupported")
            if (
                isinstance(self.supplemental_count, bool)
                or not isinstance(self.supplemental_count, int)
                or self.supplemental_count < 0
            ):
                raise ValueError("supplemental_count must be non-negative")
            for field_name in (
                "twse_observation_count", "missing_twse_count", "discrepancy_count"
            ):
                value = getattr(self, field_name)
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError(f"{field_name} must be non-negative")
            object.__setattr__(
                self,
                "supplemental_sources",
                tuple(sorted(set(self.supplemental_sources))),
            )
            if any(item not in {"esun", "esun-historical"} for item in self.supplemental_sources):
                raise ValueError("supplemental_sources must contain only E.SUN")
            computed = _canonical_sha256(self.identity_payload())
            if self.research_locator_sha256 is None:
                object.__setattr__(self, "research_locator_sha256", computed)
            else:
                _require_sha256(self.research_locator_sha256, "research_locator_sha256")
                if self.research_locator_sha256 != computed:
                    raise ScreenerCheckpointConflictError(
                        "research locator hash does not match the explicit dataset identity"
                    )

    def identity_payload(self) -> dict[str, object]:
        return {
            "identity_version": "screener-dataset-input-v1",
            "dataset_version_id": self.dataset_version_id,
            "source_policy": self.source_policy,
            "source_status": self.source_status,
            "authority_status": self.authority_status,
            "reconciliation_status": self.reconciliation_status,
            "research_data_quality": self.research_data_quality,
            "supplemental_count": self.supplemental_count,
            "twse_observation_count": self.twse_observation_count,
            "missing_twse_count": self.missing_twse_count,
            "discrepancy_count": self.discrepancy_count,
            "supplemental_sources": list(self.supplemental_sources),
            "provenance_map_sha256": self.provenance_map_sha256,
        }

    @classmethod
    def from_stage2_provenance(cls, *, symbol: str, provenance: Stage2Provenance) -> "CandidateInputLocator":
        if not isinstance(provenance, Stage2Provenance) or provenance.dataset_version_id is None:
            raise ValueError("dual-source Stage2 provenance is required")
        return cls(
            symbol=symbol,
            dataset_version_id=provenance.dataset_version_id,
            source_policy=provenance.source_policy,
            source_status=provenance.source_status,
            authority_status=provenance.authority_status,
            reconciliation_status=provenance.reconciliation_status,
            research_data_quality=provenance.research_data_quality,
            supplemental_count=provenance.esun_supplemental_count,
            twse_observation_count=provenance.twse_observation_count,
            missing_twse_count=provenance.missing_twse_count,
            discrepancy_count=provenance.discrepancy_count,
            supplemental_sources=provenance.supplemental_sources,
            provenance_map_sha256=provenance.provenance_map_sha256,
        )


@dataclass(frozen=True, slots=True)
class ScreenerRunPersistenceResult:
    screener_run_id: str
    created: bool


@dataclass(frozen=True, slots=True)
class CandidateCheckpoint:
    candidate_id: str
    screener_run_id: str
    checkpoint_status: str
    input_locator_sha256: str
    snapshot_sha256: str | None
    stage1_rank: int
    symbol: str
    name: str | None
    market: str
    candidate_kind: Stage2CandidateKind
    analysis_status: Stage2AnalysisStatus
    stage1_reasons: tuple[Stage1Reason, ...]
    stage2_reasons: tuple[Stage2Reason, ...]
    metrics: tuple[Stage2Metric, ...]
    data_quality: Stage2DataQuality
    provenance: Stage2Provenance
    failure: Stage2Failure | None

    def as_dict(self) -> dict[str, object]:
        return {
            "checkpoint_version": _CHECKPOINT_VERSION,
            "candidate_id": self.candidate_id,
            "screener_run_id": self.screener_run_id,
            "checkpoint_status": self.checkpoint_status,
            "input_locator_sha256": self.input_locator_sha256,
            "snapshot_sha256": self.snapshot_sha256,
            "stage1_rank": self.stage1_rank,
            "symbol": self.symbol,
            "name": self.name,
            "market": self.market,
            "candidate_kind": self.candidate_kind.value,
            "analysis_status": self.analysis_status.value,
            "stage1_reasons": [
                _stage1_reason_dict(item) for item in self.stage1_reasons
            ],
            "stage2_reasons": [
                _stage2_reason_dict(item) for item in self.stage2_reasons
            ],
            "metrics": [_metric_dict(item) for item in self.metrics],
            "data_quality": _quality_dict(self.data_quality),
            "provenance": _provenance_dict(self.provenance),
            "failure": _failure_dict(self.failure),
        }

    def canonical_json(self) -> str:
        return _canonical_json(self.as_dict())

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class CandidatePersistenceResult:
    candidate_id: str
    checkpoint: CandidateCheckpoint
    written: bool


class SQLiteScreenerCheckpointRepository:
    """Persist S4.3 run shells and per-candidate bundles atomically."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def create_run(
        self,
        *,
        universe_run_id: str,
        stage1_result: Stage1ScanResult,
        candidate_locators: tuple[CandidateInputLocator, ...],
        stage2_methodology_version: str = STAGE2_METHODOLOGY_VERSION,
    ) -> ScreenerRunPersistenceResult:
        _require_sha256(universe_run_id, "universe_run_id")
        if not isinstance(stage1_result, Stage1ScanResult):
            raise TypeError("stage1_result must be a Stage1ScanResult")
        if stage1_result.methodology_version != STAGE1_METHODOLOGY_VERSION:
            raise ScreenerCheckpointConflictError("Stage 1 methodology changed")
        if stage2_methodology_version != STAGE2_METHODOLOGY_VERSION:
            raise ScreenerCheckpointConflictError("Stage 2 methodology changed")
        locators = self._locator_map(candidate_locators)
        expected_symbols = tuple(item.symbol for item in stage1_result.candidates)
        if set(locators) != set(expected_symbols) or len(locators) != len(
            expected_symbols
        ):
            raise ScreenerCheckpointConflictError(
                "candidate locators must exactly match the Stage 1 shortlist"
            )
        candidate_inputs = tuple(
            (
                candidate,
                locators[candidate.symbol],
                self._candidate_input_sha256(
                    candidate,
                    locators[candidate.symbol].research_locator_sha256,
                ),
            )
            for candidate in stage1_result.candidates
        )
        input_manifest_sha256 = _canonical_sha256(
            {
                "manifest_version": _MANIFEST_VERSION,
                "stage1_canonical_sha256": stage1_result.payload_sha256,
                "candidate_inputs": [
                    {
                        "symbol": candidate.symbol,
                        "input_locator_sha256": input_sha256,
                    }
                    for candidate, unused_locator, input_sha256 in candidate_inputs
                ],
            }
        )
        identity = {
            "identity_version": _RUN_ID_VERSION,
            "market_date": stage1_result.market_date.isoformat(),
            "universe_run_id": universe_run_id,
            "stage1_methodology_version": stage1_result.methodology_version,
            "stage2_methodology_version": stage2_methodology_version,
            "source_policy": stage1_result.source_policy,
            "candidate_limit": stage1_result.candidate_limit,
            "input_manifest_sha256": input_manifest_sha256,
        }
        screener_run_id = _canonical_sha256(identity)
        timestamp = _utc_now()
        self._validate_dataset_version_locators(
            candidate_inputs,
            market_date=stage1_result.market_date,
        )

        with self._write_transaction() as connection:
            self._require_v11(connection)
            self._validate_universe(
                connection,
                universe_run_id=universe_run_id,
                stage1_result=stage1_result,
            )
            existing = self._find_run(
                connection,
                screener_run_id=screener_run_id,
                identity=identity,
            )
            if existing is not None:
                self._verify_run_metadata(existing, stage1_result)
                self._verify_candidate_shells(
                    connection,
                    screener_run_id=screener_run_id,
                    universe_run_id=universe_run_id,
                    candidate_inputs=candidate_inputs,
                )
                created = False
            else:
                self._insert_run(
                    connection,
                    screener_run_id=screener_run_id,
                    universe_run_id=universe_run_id,
                    stage1_result=stage1_result,
                    stage2_methodology_version=stage2_methodology_version,
                    input_manifest_sha256=input_manifest_sha256,
                    candidate_inputs=candidate_inputs,
                    timestamp=timestamp,
                )
                self._insert_candidate_shells(
                    connection,
                    screener_run_id=screener_run_id,
                    universe_run_id=universe_run_id,
                    candidate_inputs=candidate_inputs,
                    timestamp=timestamp,
                )
                created = True
        return ScreenerRunPersistenceResult(
            screener_run_id=screener_run_id,
            created=created,
        )

    def persist_candidate(
        self,
        *,
        screener_run_id: str,
        candidate: Stage2Candidate,
        research_locator_sha256: str,
        snapshot_sha256: str | None,
        retry_failed: bool = False,
    ) -> CandidatePersistenceResult:
        _require_sha256(screener_run_id, "screener_run_id")
        _require_sha256(research_locator_sha256, "research_locator_sha256")
        if not isinstance(candidate, Stage2Candidate):
            raise TypeError("candidate must be a Stage2Candidate")
        if snapshot_sha256 is not None:
            _require_sha256(snapshot_sha256, "snapshot_sha256")
        checkpoint_status = (
            "failed"
            if candidate.analysis_status is Stage2AnalysisStatus.FAILED
            else "success"
        )
        if checkpoint_status == "success" and snapshot_sha256 is None:
            raise ScreenerCheckpointConflictError(
                "successful candidate checkpoint requires snapshot_sha256"
            )
        if checkpoint_status == "success" and not candidate.provenance.canonical_sources:
            raise CandidateSourcePolicyError(
                "successful candidate requires explicit canonical source provenance"
            )
        if candidate.provenance.dataset_version_id is not None:
            expected_locator = CandidateInputLocator.from_stage2_provenance(
                symbol=candidate.symbol,
                provenance=candidate.provenance,
            )
            if expected_locator.research_locator_sha256 != research_locator_sha256:
                raise ScreenerCheckpointConflictError(
                    "candidate provenance does not match the explicit input locator"
                )
        self._validate_source_policy(candidate.provenance)
        input_locator_sha256 = self._candidate_input_sha256(
            candidate,
            research_locator_sha256,
        )
        candidate_id = self._candidate_id(screener_run_id, candidate.symbol)
        expected = self._checkpoint_from_candidate(
            candidate_id=candidate_id,
            screener_run_id=screener_run_id,
            checkpoint_status=checkpoint_status,
            input_locator_sha256=input_locator_sha256,
            snapshot_sha256=snapshot_sha256,
            candidate=candidate,
        )
        timestamp = _utc_now()

        with self._write_transaction() as connection:
            self._require_v11(connection)
            run = self._get_run(connection, screener_run_id)
            if run["status"] == "success":
                raise ScreenerCheckpointStateError(
                    "successful Screener run cannot accept candidate checkpoints"
                )
            if candidate.provenance.dataset_version_id is not None:
                self._validate_dataset_version_connection(
                    connection,
                    locator=CandidateInputLocator.from_stage2_provenance(
                        symbol=candidate.symbol,
                        provenance=candidate.provenance,
                    ),
                    market_date=date.fromisoformat(run["market_date"]),
                )
            row = self._get_candidate_row(connection, candidate_id)
            self._validate_candidate_identity(
                connection,
                run=run,
                row=row,
                expected=expected,
            )
            if row["status"] == "success":
                if retry_failed:
                    raise ScreenerCheckpointStateError(
                        "successful candidate cannot use failed-candidate retry"
                    )
                reconstructed = self._reconstruct_candidate(connection, row)
                if reconstructed.canonical_json() != expected.canonical_json():
                    raise ScreenerCheckpointConflictError(
                        "successful candidate is immutable and conflicts with replay"
                    )
                result = CandidatePersistenceResult(
                    candidate_id=candidate_id,
                    checkpoint=reconstructed,
                    written=False,
                )
            elif row["status"] == "failed" and not retry_failed:
                reconstructed = self._reconstruct_candidate(connection, row)
                if reconstructed.canonical_json() != expected.canonical_json():
                    raise ScreenerCheckpointStateError(
                        "failed candidate replacement requires retry_failed=True"
                    )
                result = CandidatePersistenceResult(
                    candidate_id=candidate_id,
                    checkpoint=reconstructed,
                    written=False,
                )
            else:
                if row["status"] == "running":
                    raise ScreenerCheckpointStateError(
                        "running candidate checkpoint requires explicit recovery"
                    )
                if row["status"] == "pending" and retry_failed:
                    raise ScreenerCheckpointStateError(
                        "pending candidate cannot use failed-candidate retry"
                    )
                if row["status"] not in {"pending", "failed"}:
                    raise ScreenerCheckpointStateError(
                        f"unsupported candidate checkpoint status {row['status']}"
                    )
                was_failed = row["status"] == "failed"
                self._prepare_candidate(
                    connection,
                    candidate_id=candidate_id,
                    was_failed=was_failed,
                    timestamp=timestamp,
                )
                if was_failed:
                    self._reopen_run_for_retry(
                        connection,
                        screener_run_id=screener_run_id,
                        timestamp=timestamp,
                    )
                self._insert_reasons(connection, candidate_id, candidate)
                self._insert_metrics(connection, candidate_id, candidate.metrics)
                self._insert_artifacts(
                    connection,
                    candidate_id,
                    candidate.provenance,
                )
                self._complete_candidate(
                    connection,
                    candidate_id=candidate_id,
                    candidate=candidate,
                    checkpoint_status=checkpoint_status,
                    snapshot_sha256=snapshot_sha256,
                    payload_sha256=expected.payload_sha256,
                    timestamp=timestamp,
                )
                completed = self._get_candidate_row(connection, candidate_id)
                reconstructed = self._reconstruct_candidate(connection, completed)
                if reconstructed.canonical_json() != expected.canonical_json():
                    raise ScreenerCheckpointConflictError(
                        "candidate bundle does not reconstruct its canonical input"
                    )
                self._refresh_run_status(
                    connection,
                    screener_run_id=screener_run_id,
                    timestamp=timestamp,
                )
                result = CandidatePersistenceResult(
                    candidate_id=candidate_id,
                    checkpoint=reconstructed,
                    written=True,
                )
        return result

    def load_candidate(self, candidate_id: str) -> CandidateCheckpoint:
        _require_sha256(candidate_id, "candidate_id")
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA query_only = ON")
        try:
            self._require_v11(connection)
            row = self._get_candidate_row(connection, candidate_id)
            return self._reconstruct_candidate(connection, row)
        finally:
            connection.close()

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _require_v11(connection: sqlite3.Connection) -> None:
        row = connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version = 11"
        ).fetchone()
        if row is None:
            raise ScreenerCheckpointStateError(
                "Market Screener migration 11 must be applied explicitly"
            )

    @staticmethod
    def _locator_map(
        locators: tuple[CandidateInputLocator, ...]
    ) -> dict[str, CandidateInputLocator]:
        if not isinstance(locators, tuple) or any(
            not isinstance(item, CandidateInputLocator) for item in locators
        ):
            raise TypeError("candidate_locators must be an immutable locator tuple")
        by_symbol = {item.symbol: item for item in locators}
        if len(by_symbol) != len(locators):
            raise ScreenerCheckpointConflictError("candidate locators must be unique")
        return by_symbol

    @staticmethod
    def _validate_universe(
        connection: sqlite3.Connection,
        *,
        universe_run_id: str,
        stage1_result: Stage1ScanResult,
    ) -> None:
        run = connection.execute(
            "SELECT market_date, universe_count, source_policy, status "
            "FROM market_universe_runs WHERE universe_run_id = ?",
            (universe_run_id,),
        ).fetchone()
        if run is None or run["status"] != "success":
            raise ScreenerCheckpointStateError(
                "Screener run requires one successful immutable Universe run"
            )
        expected = (
            stage1_result.market_date.isoformat(),
            stage1_result.universe_count,
            stage1_result.source_policy,
        )
        actual = (run["market_date"], run["universe_count"], run["source_policy"])
        if actual != expected:
            raise ScreenerCheckpointConflictError(
                "Stage 1 result conflicts with the selected Universe run"
            )
        for candidate in stage1_result.candidates:
            member = connection.execute(
                "SELECT name, market, status FROM market_universe_members "
                "WHERE universe_run_id = ? AND symbol = ?",
                (universe_run_id, candidate.symbol),
            ).fetchone()
            if member is None or member["status"] != "active_scan_eligible":
                raise ScreenerCheckpointConflictError(
                    f"Stage 1 candidate {candidate.symbol} is not scan eligible"
                )
            if member["name"] != candidate.name or member["market"] != "TWSE":
                raise ScreenerCheckpointConflictError(
                    f"Stage 1 candidate {candidate.symbol} identity changed"
                )

    @classmethod
    def _find_run(
        cls,
        connection: sqlite3.Connection,
        *,
        screener_run_id: str,
        identity: dict[str, object],
    ) -> sqlite3.Row | None:
        row = connection.execute(
            "SELECT * FROM screener_runs WHERE screener_run_id = ?",
            (screener_run_id,),
        ).fetchone()
        by_identity = connection.execute(
            "SELECT * FROM screener_runs WHERE market_date = ? "
            "AND universe_run_id = ? AND stage1_methodology_version = ? "
            "AND stage2_methodology_version = ? AND source_policy = ? "
            "AND candidate_limit = ? AND input_manifest_sha256 = ?",
            (
                identity["market_date"],
                identity["universe_run_id"],
                identity["stage1_methodology_version"],
                identity["stage2_methodology_version"],
                identity["source_policy"],
                identity["candidate_limit"],
                identity["input_manifest_sha256"],
            ),
        ).fetchone()
        if row is not None and by_identity is not None:
            if row["screener_run_id"] != by_identity["screener_run_id"]:
                raise ScreenerCheckpointConflictError(
                    "Screener run ID and deterministic identity resolve differently"
                )
        existing = row if row is not None else by_identity
        if existing is None:
            return None
        actual = {
            "identity_version": _RUN_ID_VERSION,
            "market_date": existing["market_date"],
            "universe_run_id": existing["universe_run_id"],
            "stage1_methodology_version": existing["stage1_methodology_version"],
            "stage2_methodology_version": existing["stage2_methodology_version"],
            "source_policy": existing["source_policy"],
            "candidate_limit": existing["candidate_limit"],
            "input_manifest_sha256": existing["input_manifest_sha256"],
        }
        if actual != identity or _canonical_sha256(actual) != existing[
            "screener_run_id"
        ]:
            raise ScreenerCheckpointConflictError(
                "stored Screener run conflicts with its immutable identity"
            )
        return existing

    @staticmethod
    def _verify_run_metadata(
        run: sqlite3.Row, stage1_result: Stage1ScanResult
    ) -> None:
        actual = (
            run["stage1_canonical_sha256"],
            run["universe_count"],
            run["screened_count"],
            run["triggered_count"],
            run["candidate_count"],
            bool(run["truncated"]),
        )
        expected = (
            stage1_result.payload_sha256,
            stage1_result.universe_count,
            stage1_result.screened_count,
            stage1_result.triggered_count,
            stage1_result.candidate_count,
            stage1_result.truncated,
        )
        if actual != expected:
            raise ScreenerCheckpointConflictError(
                "stored Screener run metadata conflicts with Stage 1 output"
            )

    @staticmethod
    def _insert_run(
        connection: sqlite3.Connection,
        *,
        screener_run_id: str,
        universe_run_id: str,
        stage1_result: Stage1ScanResult,
        stage2_methodology_version: str,
        input_manifest_sha256: str,
        candidate_inputs: tuple[
            tuple[Stage1Candidate, CandidateInputLocator, str], ...
        ],
        timestamp: str,
    ) -> None:
        values = (
                screener_run_id,
                stage1_result.market_date.isoformat(),
                universe_run_id,
                stage1_result.methodology_version,
                stage2_methodology_version,
                stage1_result.source_policy,
                stage1_result.candidate_limit,
                input_manifest_sha256,
                stage1_result.payload_sha256,
                stage1_result.universe_count,
                stage1_result.screened_count,
                stage1_result.triggered_count,
                stage1_result.candidate_count,
                int(stage1_result.truncated),
                timestamp,
                timestamp,
                timestamp,
        )
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info('screener_runs')")
        }
        if "ds5_execution_status" not in columns:
            connection.execute(
                "INSERT INTO screener_runs ("
                "screener_run_id, market_date, universe_run_id, "
                "stage1_methodology_version, stage2_methodology_version, source_policy, "
                "candidate_limit, input_manifest_sha256, stage1_canonical_sha256, "
                "universe_count, screened_count, triggered_count, candidate_count, "
                "truncated, status, attempt_count, created_at, started_at, updated_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', 1, ?, ?, ?)",
                values,
            )
            return
        metadata = _dataset_metadata_from_locators(
            tuple(locator for unused_candidate, locator, unused_hash in candidate_inputs)
        )
        insert_columns = (
            "screener_run_id, market_date, universe_run_id, "
            "stage1_methodology_version, stage2_methodology_version, source_policy, "
            "candidate_limit, input_manifest_sha256, stage1_canonical_sha256, "
            "universe_count, screened_count, triggered_count, candidate_count, "
            "truncated, status, attempt_count, created_at, started_at, updated_at, "
            "ds5_execution_status, ds5_source_policy, ds5_research_data_quality, "
            "ds5_source_status, ds5_authority_status, ds5_reconciliation_status, "
            "ds5_supplemental_candidate_count, ds5_canonical_authority, "
            "ds5_supplemental_sources_json, ds5_twse_observation_count, "
            "ds5_missing_twse_count, ds5_discrepancy_count, ds5_dataset_identity_sha256, "
            "ds5_provenance_map_sha256, ds5_dataset_version_ids_json"
        )
        insert_values = values[:14] + ("running", 1) + values[14:] + (
                metadata["execution_status"],
                metadata["source_policy"],
                metadata["research_data_quality"],
                metadata["source_status"],
                metadata["authority_status"],
                metadata["reconciliation_status"],
                metadata["supplemental_candidate_count"],
                "twse",
                metadata["supplemental_sources"],
                metadata["twse_observation_count"],
                metadata["missing_twse_count"],
                metadata["discrepancy_count"],
                metadata["dataset_identity_sha256"],
                metadata["provenance_map_sha256"],
                metadata["dataset_version_ids_json"],
        )
        connection.execute(
            f"INSERT INTO screener_runs ({insert_columns}) VALUES ("
            + ", ".join("?" for unused in insert_values)
            + ")",
            insert_values,
        )

    @classmethod
    def _insert_candidate_shells(
        cls,
        connection: sqlite3.Connection,
        *,
        screener_run_id: str,
        universe_run_id: str,
        candidate_inputs: tuple[
            tuple[Stage1Candidate, CandidateInputLocator, str], ...
        ],
        timestamp: str,
    ) -> None:
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info('screener_candidates')")
        }
        if "ds5_dataset_version_id" in columns:
            connection.executemany(
                "INSERT INTO screener_candidates ("
                "candidate_id, screener_run_id, universe_run_id, symbol, status, rank, "
                "stage1_rank, stage1_trigger_count, stage1_reason_count, input_locator_sha256, "
                "ds5_dataset_version_id, ds5_source_policy, ds5_source_status, ds5_authority_status, "
                "ds5_reconciliation_status, ds5_research_data_quality, ds5_esun_supplemental_count, "
                "ds5_provenance_map_sha256, attempt_count, created_at, updated_at"
                ") VALUES (?, ?, ?, ?, 'pending', NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
                (
                    (
                        cls._candidate_id(screener_run_id, candidate.symbol),
                        screener_run_id,
                        universe_run_id,
                        candidate.symbol,
                        candidate.rank,
                        sum(reason.role == "primary" for reason in candidate.reasons),
                        len(candidate.reasons),
                        input_sha256,
                        locator.dataset_version_id,
                        locator.source_policy,
                        locator.source_status,
                        locator.authority_status,
                        locator.reconciliation_status,
                        locator.research_data_quality,
                        locator.supplemental_count,
                        locator.provenance_map_sha256,
                        timestamp,
                        timestamp,
                    )
                    for candidate, locator, input_sha256 in candidate_inputs
                ),
            )
            return
        connection.executemany(
            "INSERT INTO screener_candidates ("
            "candidate_id, screener_run_id, universe_run_id, symbol, status, rank, "
            "stage1_rank, stage1_trigger_count, stage1_reason_count, "
            "input_locator_sha256, attempt_count, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, 'pending', NULL, ?, ?, ?, ?, 0, ?, ?)",
            (
                (
                    cls._candidate_id(screener_run_id, candidate.symbol),
                    screener_run_id,
                    universe_run_id,
                    candidate.symbol,
                    candidate.rank,
                    sum(reason.role == "primary" for reason in candidate.reasons),
                    len(candidate.reasons),
                    input_sha256,
                    timestamp,
                    timestamp,
                )
                for candidate, unused_locator, input_sha256 in candidate_inputs
            ),
        )

    @classmethod
    def _verify_candidate_shells(
        cls,
        connection: sqlite3.Connection,
        *,
        screener_run_id: str,
        universe_run_id: str,
        candidate_inputs: tuple[
            tuple[Stage1Candidate, CandidateInputLocator, str], ...
        ],
    ) -> None:
        rows = connection.execute(
            "SELECT candidate_id, universe_run_id, symbol, stage1_rank, "
            "stage1_trigger_count, stage1_reason_count, input_locator_sha256 "
            "FROM screener_candidates WHERE screener_run_id = ? ORDER BY stage1_rank",
            (screener_run_id,),
        ).fetchall()
        expected = tuple(
            (
                cls._candidate_id(screener_run_id, candidate.symbol),
                universe_run_id,
                candidate.symbol,
                candidate.rank,
                sum(reason.role == "primary" for reason in candidate.reasons),
                len(candidate.reasons),
                input_sha256,
            )
            for candidate, unused_locator, input_sha256 in candidate_inputs
        )
        actual = tuple(tuple(row) for row in rows)
        if actual != expected:
            raise ScreenerCheckpointConflictError(
                "stored candidate shells conflict with the Stage 1 handoff"
            )

    @staticmethod
    def _get_run(
        connection: sqlite3.Connection, screener_run_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM screener_runs WHERE screener_run_id = ?",
            (screener_run_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown Screener run {screener_run_id}")
        return row

    @staticmethod
    def _get_candidate_row(
        connection: sqlite3.Connection, candidate_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM screener_candidates WHERE candidate_id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown candidate checkpoint {candidate_id}")
        return row

    @staticmethod
    def _validate_candidate_identity(
        connection: sqlite3.Connection,
        *,
        run: sqlite3.Row,
        row: sqlite3.Row,
        expected: CandidateCheckpoint,
    ) -> None:
        if row["screener_run_id"] != run["screener_run_id"]:
            raise ScreenerCheckpointConflictError("candidate parent run changed")
        member = connection.execute(
            "SELECT name, market FROM market_universe_members "
            "WHERE universe_run_id = ? AND symbol = ?",
            (row["universe_run_id"], row["symbol"]),
        ).fetchone()
        actual = (
            row["candidate_id"],
            row["symbol"],
            row["stage1_rank"],
            row["input_locator_sha256"],
            None if member is None else member["name"],
            None if member is None else member["market"],
        )
        wanted = (
            expected.candidate_id,
            expected.symbol,
            expected.stage1_rank,
            expected.input_locator_sha256,
            expected.name,
            expected.market,
        )
        if actual != wanted:
            raise ScreenerCheckpointConflictError(
                "candidate input locator or Stage 1 handoff changed"
            )
        if row["rank"] is not None:
            raise ScreenerCheckpointStateError(
                "S4.3 candidate checkpoint must not have final rank"
            )

    @staticmethod
    def _prepare_candidate(
        connection: sqlite3.Connection,
        *,
        candidate_id: str,
        was_failed: bool,
        timestamp: str,
    ) -> None:
        if was_failed:
            connection.execute(
                "DELETE FROM candidate_reasons WHERE candidate_id = ?",
                (candidate_id,),
            )
            connection.execute(
                "DELETE FROM candidate_metrics WHERE candidate_id = ?",
                (candidate_id,),
            )
            connection.execute(
                "DELETE FROM screener_source_artifacts WHERE candidate_id = ?",
                (candidate_id,),
            )
        allowed = "failed" if was_failed else "pending"
        cursor = connection.execute(
            "UPDATE screener_candidates SET status = 'running', rank = NULL, "
            "candidate_kind = NULL, stage2_reason_count = 0, metric_count = 0, "
            "data_quality_status = NULL, validation_status = NULL, "
            "analysis_status = NULL, pipeline_run_id = NULL, historical_run_id = NULL, "
            "validation_run_id = NULL, canonical_sources_json = '[]', "
            "validation_sources_json = '[]', discrepancies_json = '[]', "
            "snapshot_sha256 = NULL, payload_sha256 = NULL, failure_code = NULL, "
            "failure_type = NULL, attempt_count = attempt_count + 1, "
            "started_at = COALESCE(started_at, ?), finished_at = NULL, updated_at = ? "
            "WHERE candidate_id = ? AND status = ?",
            (timestamp, timestamp, candidate_id, allowed),
        )
        if cursor.rowcount != 1:
            raise ScreenerCheckpointStateError(
                "candidate could not transition atomically to running"
            )

    @staticmethod
    def _reopen_run_for_retry(
        connection: sqlite3.Connection,
        *,
        screener_run_id: str,
        timestamp: str,
    ) -> None:
        cursor = connection.execute(
            "UPDATE screener_runs SET status = 'running', canonical_sha256 = NULL, "
            "attempt_count = attempt_count + 1, error_code = NULL, finished_at = NULL, "
            "updated_at = ? WHERE screener_run_id = ? "
            "AND status IN ('partial_success', 'failed', 'running')",
            (timestamp, screener_run_id),
        )
        if cursor.rowcount != 1:
            raise ScreenerCheckpointStateError(
                "Screener run could not reopen for failed-candidate retry"
            )

    @staticmethod
    def _insert_reasons(
        connection: sqlite3.Connection,
        candidate_id: str,
        candidate: Stage2Candidate,
    ) -> None:
        rows: list[tuple[object, ...]] = []
        for ordinal, reason in enumerate(candidate.stage1_reasons, 1):
            rows.append(
                (
                    candidate_id,
                    "stage1",
                    ordinal,
                    reason.code,
                    reason.metric,
                    reason.component,
                    _scalar_json(reason.previous),
                    _scalar_json(reason.current),
                    reason.delta,
                    reason.unit,
                    reason.operator,
                    _scalar_json(reason.threshold),
                    reason.rule_version,
                    reason.role,
                    reason.trigger_class,
                    None,
                    None,
                    reason.threshold_multiple,
                )
            )
        for ordinal, reason in enumerate(candidate.stage2_reasons, 1):
            rows.append(
                (
                    candidate_id,
                    "stage2",
                    ordinal,
                    reason.code,
                    reason.metric,
                    None,
                    _scalar_json(reason.previous),
                    _scalar_json(reason.current),
                    reason.delta,
                    reason.unit,
                    reason.operator,
                    _scalar_json(reason.threshold),
                    reason.rule_version,
                    None,
                    None,
                    reason.reason_kind,
                    reason.reason_class,
                    reason.threshold_multiple,
                )
            )
        connection.executemany(
            "INSERT INTO candidate_reasons ("
            "candidate_id, stage, ordinal, code, metric, component, previous_json, "
            "current_json, delta, unit, operator, threshold_json, rule_version, role, "
            "trigger_class, reason_kind, reason_class, threshold_multiple"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )

    @staticmethod
    def _insert_metrics(
        connection: sqlite3.Connection,
        candidate_id: str,
        metrics: tuple[Stage2Metric, ...],
    ) -> None:
        connection.executemany(
            "INSERT INTO candidate_metrics ("
            "candidate_id, ordinal, name, status, value, previous_value, delta, unit, "
            "as_of_date, previous_as_of_date, observations, previous_observations"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    candidate_id,
                    ordinal,
                    metric.name,
                    metric.status.value,
                    metric.value,
                    metric.previous_value,
                    metric.delta,
                    metric.unit,
                    metric.as_of_date.isoformat(),
                    _date_text(metric.previous_as_of_date),
                    metric.observations,
                    metric.previous_observations,
                )
                for ordinal, metric in enumerate(metrics, 1)
            ),
        )

    @classmethod
    def _insert_artifacts(
        cls,
        connection: sqlite3.Connection,
        candidate_id: str,
        provenance: Stage2Provenance,
    ) -> None:
        rows = []
        for ordinal, artifact in enumerate(provenance.artifact_refs, 1):
            role = cls._artifact_role(artifact, provenance)
            rows.append(
                (
                    cls._artifact_ref_id(candidate_id, ordinal, role, artifact),
                    candidate_id,
                    ordinal,
                    role,
                    artifact.owner_kind,
                    artifact.owner_run_id,
                    artifact.provider,
                    artifact.dataset,
                    artifact.endpoint,
                    artifact.contract_version,
                    artifact.payload_sha256,
                    artifact.payload_size_bytes,
                    artifact.hash_basis,
                )
            )
        connection.executemany(
            "INSERT INTO screener_source_artifacts ("
            "artifact_ref_id, universe_run_id, screener_run_id, candidate_id, "
            "ordinal, source_role, upstream_owner_kind, upstream_owner_run_id, "
            "provider, dataset, source_ref, contract_version, payload_sha256, "
            "payload_size_bytes, hash_basis"
            ") VALUES (?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )

    @staticmethod
    def _complete_candidate(
        connection: sqlite3.Connection,
        *,
        candidate_id: str,
        candidate: Stage2Candidate,
        checkpoint_status: str,
        snapshot_sha256: str | None,
        payload_sha256: str,
        timestamp: str,
    ) -> None:
        failure_code = None if candidate.failure is None else candidate.failure.code
        failure_type = (
            None if candidate.failure is None else candidate.failure.error_type
        )
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info('screener_candidates')")
        }
        base_sql = (
            "UPDATE screener_candidates SET status = ?, rank = NULL, "
            "candidate_kind = ?, stage2_reason_count = ?, metric_count = ?, "
            "data_quality_status = ?, validation_status = ?, analysis_status = ?, "
            "pipeline_run_id = ?, historical_run_id = ?, validation_run_id = ?, "
            "canonical_sources_json = ?, validation_sources_json = ?, "
            "discrepancies_json = ?, snapshot_sha256 = ?, payload_sha256 = ?, "
            "failure_code = ?, failure_type = ?, finished_at = ?, updated_at = ?"
        )
        values = [
                checkpoint_status,
                candidate.candidate_kind.value,
                len(candidate.stage2_reasons),
                len(candidate.metrics),
                candidate.data_quality.status.value,
                candidate.data_quality.validation_status,
                candidate.analysis_status.value,
                candidate.provenance.pipeline_run_id,
                candidate.provenance.historical_run_id,
                candidate.provenance.validation_run_id,
                _canonical_json(list(candidate.provenance.canonical_sources)),
                _canonical_json(list(candidate.provenance.validation_sources)),
                _canonical_json(_discrepancy_dicts(candidate.data_quality)),
                snapshot_sha256,
                payload_sha256,
                failure_code,
                failure_type,
                timestamp,
                timestamp,
        ]
        if "ds5_dataset_version_id" in columns:
            base_sql += (
                ", ds5_dataset_version_id = ?, ds5_source_policy = ?, "
                "ds5_source_status = ?, ds5_authority_status = ?, "
                "ds5_reconciliation_status = ?, ds5_research_data_quality = ?, "
                "ds5_canonical_authority = ?, ds5_supplemental_sources_json = ?, "
                "ds5_twse_observation_count = ?, ds5_esun_supplemental_count = ?, "
                "ds5_missing_twse_count = ?, ds5_discrepancy_count = ?, "
                "ds5_provenance_map_sha256 = ?, ds5_parent_dataset_version_id = ?"
            )
            values.extend(
                [
                    candidate.provenance.dataset_version_id,
                    candidate.provenance.source_policy,
                    candidate.provenance.source_status,
                    candidate.provenance.authority_status,
                    candidate.provenance.reconciliation_status,
                    candidate.provenance.research_data_quality,
                    candidate.provenance.canonical_authority,
                    _canonical_json(list(candidate.provenance.supplemental_sources)),
                    candidate.provenance.twse_observation_count,
                    candidate.provenance.esun_supplemental_count,
                    candidate.provenance.missing_twse_count,
                    candidate.provenance.discrepancy_count,
                    candidate.provenance.provenance_map_sha256,
                    candidate.provenance.parent_dataset_version_id,
                ]
            )
        base_sql += " WHERE candidate_id = ? AND status = 'running'"
        values.append(candidate_id)
        cursor = connection.execute(base_sql, values)
        if cursor.rowcount != 1:
            raise ScreenerCheckpointStateError(
                "candidate bundle could not transition atomically to terminal checkpoint"
            )

    @staticmethod
    def _refresh_run_status(
        connection: sqlite3.Connection,
        *,
        screener_run_id: str,
        timestamp: str,
    ) -> None:
        counts = connection.execute(
            "SELECT "
            "SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS succeeded, "
            "SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed, "
            "SUM(CASE WHEN status IN ('pending', 'running') THEN 1 ELSE 0 END) AS open, "
            "COUNT(*) AS total FROM screener_candidates WHERE screener_run_id = ?",
            (screener_run_id,),
        ).fetchone()
        succeeded = int(counts["succeeded"] or 0)
        failed = int(counts["failed"] or 0)
        open_count = int(counts["open"] or 0)
        total = int(counts["total"] or 0)
        if total == 0:
            raise ScreenerCheckpointStateError("Screener run has no candidates")
        if open_count > 0 or failed == 0:
            status = "running"
            finished_at = None
            error_code = None
        elif succeeded > 0:
            status = "partial_success"
            finished_at = timestamp
            error_code = "candidate_failure"
        else:
            status = "failed"
            finished_at = timestamp
            error_code = "all_candidates_failed"
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info('screener_runs')")
        }
        if "ds5_execution_status" not in columns:
            connection.execute(
                "UPDATE screener_runs SET status = ?, canonical_sha256 = NULL, "
                "finished_at = ?, error_code = ?, updated_at = ? "
                "WHERE screener_run_id = ? AND status <> 'success'",
                (status, finished_at, error_code, timestamp, screener_run_id),
            )
            return
        metadata = {
            "execution_status": "success",
            "source_policy": TWSE_BASELINE_SOURCE_POLICY,
            "source_status": "canonical_complete",
            "research_data_quality": "canonical",
            "authority_status": "complete",
            "reconciliation_status": "not_applicable",
            "supplemental_count": 0,
            "supplemental_candidate_count": 0,
            "supplemental_sources": "[]",
            "twse_observation_count": 0,
            "missing_twse_count": 0,
            "discrepancy_count": 0,
            "dataset_identity_sha256": None,
            "provenance_map_sha256": None,
            "dataset_version_ids_json": "[]",
        }
        if failed == 0 and open_count == 0:
            candidate_rows = connection.execute(
                "SELECT symbol, input_locator_sha256, ds5_dataset_version_id, ds5_source_policy, "
                "ds5_source_status, ds5_authority_status, ds5_reconciliation_status, "
                "ds5_research_data_quality, ds5_esun_supplemental_count, "
                "ds5_supplemental_sources_json, ds5_twse_observation_count, "
                "ds5_missing_twse_count, ds5_discrepancy_count, ds5_provenance_map_sha256 FROM screener_candidates "
                "WHERE screener_run_id = ? ORDER BY symbol",
                (screener_run_id,),
            ).fetchall()
            locators = tuple(
                CandidateInputLocator(
                    symbol=row["symbol"],
                    # Legacy TWSE rows predate DS5 dataset-version identity.
                    # Their immutable S4 input locator is only a valid
                    # placeholder for the legacy metadata aggregation path;
                    # it is never treated as a DS5 dataset identity.
                    research_locator_sha256=(
                        row["input_locator_sha256"]
                        if row["ds5_dataset_version_id"] is None
                        else None
                    ),
                    dataset_version_id=row["ds5_dataset_version_id"],
                    source_policy=row["ds5_source_policy"],
                    source_status=row["ds5_source_status"],
                    authority_status=row["ds5_authority_status"],
                    reconciliation_status=row["ds5_reconciliation_status"],
                    research_data_quality=row["ds5_research_data_quality"],
                    supplemental_count=row["ds5_esun_supplemental_count"],
                    supplemental_sources=tuple(json.loads(row["ds5_supplemental_sources_json"])),
                    twse_observation_count=row["ds5_twse_observation_count"],
                    missing_twse_count=row["ds5_missing_twse_count"],
                    discrepancy_count=row["ds5_discrepancy_count"],
                    provenance_map_sha256=row["ds5_provenance_map_sha256"],
                )
                for row in candidate_rows
            )
            metadata = _dataset_metadata_from_locators(locators)
        connection.execute(
            "UPDATE screener_runs SET status = ?, canonical_sha256 = NULL, "
            "finished_at = ?, error_code = ?, updated_at = ?, "
            "ds5_execution_status = ?, ds5_source_policy = ?, "
            "ds5_source_status = ?, ds5_research_data_quality = ?, ds5_authority_status = ?, "
            "ds5_reconciliation_status = ?, ds5_supplemental_candidate_count = ?, "
            "ds5_canonical_authority = ?, ds5_supplemental_sources_json = ?, "
            "ds5_twse_observation_count = ?, ds5_missing_twse_count = ?, ds5_discrepancy_count = ?, "
            "ds5_dataset_identity_sha256 = ?, ds5_provenance_map_sha256 = ?, "
            "ds5_dataset_version_ids_json = ? "
            "WHERE screener_run_id = ? AND status <> 'success'",
            (
                status,
                finished_at,
                error_code,
                timestamp,
                metadata["execution_status"],
                metadata["source_policy"],
                metadata["source_status"],
                metadata["research_data_quality"],
                metadata["authority_status"],
                metadata["reconciliation_status"],
                metadata["supplemental_candidate_count"],
                "twse",
                metadata["supplemental_sources"],
                metadata["twse_observation_count"],
                metadata["missing_twse_count"],
                metadata["discrepancy_count"],
                metadata["dataset_identity_sha256"],
                metadata["provenance_map_sha256"],
                metadata["dataset_version_ids_json"],
                screener_run_id,
            ),
        )

    @classmethod
    def _reconstruct_candidate(
        cls, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> CandidateCheckpoint:
        if row["status"] not in {"success", "failed"}:
            raise ScreenerCheckpointStateError(
                "only a terminal candidate bundle can be reconstructed"
            )
        if row["rank"] is not None:
            raise ScreenerCheckpointStateError(
                "S4.3 candidate checkpoint unexpectedly has final rank"
            )
        member = connection.execute(
            "SELECT name, market FROM market_universe_members "
            "WHERE universe_run_id = ? AND symbol = ?",
            (row["universe_run_id"], row["symbol"]),
        ).fetchone()
        if member is None:
            raise ScreenerCheckpointConflictError("candidate Universe member is missing")
        reason_rows = connection.execute(
            "SELECT * FROM candidate_reasons WHERE candidate_id = ? "
            "ORDER BY CASE stage WHEN 'stage1' THEN 0 ELSE 1 END, ordinal",
            (row["candidate_id"],),
        ).fetchall()
        stage1_reasons = tuple(
            Stage1Reason(
                code=item["code"],
                metric=item["metric"],
                component=item["component"],
                previous=json.loads(item["previous_json"]),
                current=json.loads(item["current_json"]),
                delta=item["delta"],
                unit=item["unit"],
                operator=item["operator"],
                threshold=json.loads(item["threshold_json"]),
                rule_version=item["rule_version"],
                role=item["role"],
                trigger_class=item["trigger_class"],
                threshold_multiple=item["threshold_multiple"],
            )
            for item in reason_rows
            if item["stage"] == "stage1"
        )
        stage2_reasons = tuple(
            Stage2Reason(
                code=item["code"],
                metric=item["metric"],
                previous=json.loads(item["previous_json"]),
                current=json.loads(item["current_json"]),
                delta=item["delta"],
                unit=item["unit"],
                operator=item["operator"],
                threshold=json.loads(item["threshold_json"]),
                rule_version=item["rule_version"],
                reason_kind=item["reason_kind"],
                reason_class=item["reason_class"],
                threshold_multiple=item["threshold_multiple"],
            )
            for item in reason_rows
            if item["stage"] == "stage2"
        )
        metric_rows = connection.execute(
            "SELECT * FROM candidate_metrics WHERE candidate_id = ? ORDER BY ordinal",
            (row["candidate_id"],),
        ).fetchall()
        metrics = tuple(
            Stage2Metric(
                name=item["name"],
                status=Stage2MetricStatus(item["status"]),
                value=item["value"],
                previous_value=item["previous_value"],
                delta=item["delta"],
                unit=item["unit"],
                as_of_date=date.fromisoformat(item["as_of_date"]),
                previous_as_of_date=_optional_date(item["previous_as_of_date"]),
                observations=item["observations"],
                previous_observations=item["previous_observations"],
            )
            for item in metric_rows
        )
        discrepancy_values = json.loads(row["discrepancies_json"])
        discrepancies = tuple(
            Stage2Discrepancy(
                field=item["field"],
                left_value=item["left_value"],
                right_value=item["right_value"],
                reason=item["reason"],
            )
            for item in discrepancy_values
        )
        quality = Stage2DataQuality(
            status=Stage2QualityStatus(row["data_quality_status"]),
            validation_status=row["validation_status"],
            discrepancies=discrepancies,
        )
        row_keys = set(row.keys())
        ds5_present = "ds5_source_policy" in row_keys
        supplemental_sources = (
            tuple(json.loads(row["ds5_supplemental_sources_json"]))
            if ds5_present and "ds5_supplemental_sources_json" in row_keys
            else ()
        )
        provenance = Stage2Provenance(
            pipeline_run_id=row["pipeline_run_id"],
            historical_run_id=row["historical_run_id"],
            validation_run_id=row["validation_run_id"],
            canonical_sources=tuple(json.loads(row["canonical_sources_json"])),
            validation_sources=tuple(json.loads(row["validation_sources_json"])),
            artifact_refs=cls._reconstruct_artifacts(
                connection,
                row["candidate_id"],
                canonical_sources=tuple(
                    json.loads(row["canonical_sources_json"])
                ),
                validation_sources=tuple(
                    json.loads(row["validation_sources_json"])
                ),
                supplemental_sources=supplemental_sources,
            ),
            source_policy=(
                row["ds5_source_policy"] if ds5_present else TWSE_BASELINE_SOURCE_POLICY
            ),
            dataset_version_id=(row["ds5_dataset_version_id"] if ds5_present else None),
            source_status=(row["ds5_source_status"] if ds5_present else "canonical_complete"),
            authority_status=(row["ds5_authority_status"] if ds5_present else "complete"),
            reconciliation_status=(
                row["ds5_reconciliation_status"] if ds5_present else "not_applicable"
            ),
            research_data_quality=(
                row["ds5_research_data_quality"] if ds5_present else "canonical"
            ),
            canonical_authority=(
                row["ds5_canonical_authority"] if ds5_present else "twse"
            ),
            supplemental_sources=supplemental_sources,
            twse_observation_count=(
                row["ds5_twse_observation_count"] if ds5_present else 0
            ),
            esun_supplemental_count=(
                row["ds5_esun_supplemental_count"] if ds5_present else 0
            ),
            missing_twse_count=(
                row["ds5_missing_twse_count"] if ds5_present else 0
            ),
            discrepancy_count=(
                row["ds5_discrepancy_count"] if ds5_present else 0
            ),
            provenance_map_sha256=(
                row["ds5_provenance_map_sha256"] if ds5_present else None
            ),
            parent_dataset_version_id=(
                row["ds5_parent_dataset_version_id"] if ds5_present else None
            ),
        )
        failure = (
            Stage2Failure(
                status="failed",
                code=row["failure_code"],
                error_type=row["failure_type"],
            )
            if row["status"] == "failed"
            else None
        )
        candidate = Stage2Candidate(
            rank=row["stage1_rank"],
            stage1_rank=row["stage1_rank"],
            symbol=row["symbol"],
            name=member["name"],
            market=member["market"],
            candidate_kind=Stage2CandidateKind(row["candidate_kind"]),
            analysis_status=Stage2AnalysisStatus(row["analysis_status"]),
            stage1_reasons=stage1_reasons,
            stage2_reasons=stage2_reasons,
            metrics=metrics,
            data_quality=quality,
            provenance=provenance,
            failure=failure,
        )
        checkpoint = cls._checkpoint_from_candidate(
            candidate_id=row["candidate_id"],
            screener_run_id=row["screener_run_id"],
            checkpoint_status=row["status"],
            input_locator_sha256=row["input_locator_sha256"],
            snapshot_sha256=row["snapshot_sha256"],
            candidate=candidate,
        )
        if len(stage1_reasons) != row["stage1_reason_count"]:
            raise ScreenerCheckpointConflictError("Stage 1 reason count changed")
        if sum(item.role == "primary" for item in stage1_reasons) != row[
            "stage1_trigger_count"
        ]:
            raise ScreenerCheckpointConflictError("Stage 1 trigger count changed")
        if len(stage2_reasons) != row["stage2_reason_count"]:
            raise ScreenerCheckpointConflictError("Stage 2 reason count changed")
        if len(metrics) != row["metric_count"] or len(metrics) != 16:
            raise ScreenerCheckpointConflictError(
                "candidate checkpoint must preserve all 16 Stage 2 metrics"
            )
        if checkpoint.payload_sha256 != row["payload_sha256"]:
            raise ScreenerCheckpointConflictError(
                "candidate bundle hash does not match reconstructed children"
            )
        cls._validate_source_policy(provenance)
        return checkpoint

    @classmethod
    def _reconstruct_artifacts(
        cls,
        connection: sqlite3.Connection,
        candidate_id: str,
        *,
        canonical_sources: tuple[str, ...],
        validation_sources: tuple[str, ...],
        supplemental_sources: tuple[str, ...] = (),
    ) -> tuple[Stage2ArtifactRef, ...]:
        rows = connection.execute(
            "SELECT * FROM screener_source_artifacts "
            "WHERE candidate_id = ? ORDER BY ordinal",
            (candidate_id,),
        ).fetchall()
        provenance_shell = Stage2Provenance(
            pipeline_run_id=None,
            historical_run_id=None,
            validation_run_id=None,
            canonical_sources=canonical_sources,
            validation_sources=validation_sources,
            artifact_refs=(),
            supplemental_sources=supplemental_sources,
        )
        artifacts = []
        for ordinal, row in enumerate(rows, 1):
            artifact = Stage2ArtifactRef(
                owner_kind=row["upstream_owner_kind"],
                owner_run_id=row["upstream_owner_run_id"],
                provider=row["provider"],
                dataset=row["dataset"],
                endpoint=row["source_ref"],
                contract_version=row["contract_version"],
                payload_sha256=row["payload_sha256"],
                payload_size_bytes=row["payload_size_bytes"],
                hash_basis=row["hash_basis"],
            )
            role = cls._artifact_role(artifact, provenance_shell)
            expected_id = cls._artifact_ref_id(
                candidate_id,
                ordinal,
                role,
                artifact,
            )
            if (
                row["ordinal"] != ordinal
                or row["source_role"] != role
                or row["artifact_ref_id"] != expected_id
            ):
                raise ScreenerCheckpointConflictError(
                    "candidate artifact role, identity, or ordering changed"
                )
            artifacts.append(artifact)
        return tuple(artifacts)

    @classmethod
    def _checkpoint_from_candidate(
        cls,
        *,
        candidate_id: str,
        screener_run_id: str,
        checkpoint_status: str,
        input_locator_sha256: str,
        snapshot_sha256: str | None,
        candidate: Stage2Candidate,
    ) -> CandidateCheckpoint:
        return CandidateCheckpoint(
            candidate_id=candidate_id,
            screener_run_id=screener_run_id,
            checkpoint_status=checkpoint_status,
            input_locator_sha256=input_locator_sha256,
            snapshot_sha256=snapshot_sha256,
            stage1_rank=candidate.stage1_rank,
            symbol=candidate.symbol,
            name=candidate.name,
            market=candidate.market,
            candidate_kind=candidate.candidate_kind,
            analysis_status=candidate.analysis_status,
            stage1_reasons=candidate.stage1_reasons,
            stage2_reasons=candidate.stage2_reasons,
            metrics=candidate.metrics,
            data_quality=candidate.data_quality,
            provenance=candidate.provenance,
            failure=candidate.failure,
        )

    @classmethod
    def _candidate_input_sha256(
        cls,
        candidate: Stage1Candidate | Stage2Candidate,
        research_locator_sha256: str,
    ) -> str:
        if isinstance(candidate, Stage1Candidate):
            stage1_rank = candidate.rank
            reasons = candidate.reasons
        else:
            stage1_rank = candidate.stage1_rank
            reasons = candidate.stage1_reasons
        return _canonical_sha256(
            {
                "input_version": _CANDIDATE_INPUT_VERSION,
                "research_locator_sha256": research_locator_sha256,
                "stage1_handoff": {
                    "symbol": candidate.symbol,
                    "name": candidate.name,
                    "stage1_rank": stage1_rank,
                    "reasons": [_stage1_reason_dict(item) for item in reasons],
                },
            }
        )

    @staticmethod
    def _candidate_id(screener_run_id: str, symbol: str) -> str:
        return _canonical_sha256(
            {
                "identity_version": _CANDIDATE_ID_VERSION,
                "screener_run_id": screener_run_id,
                "symbol": symbol,
            }
        )

    @classmethod
    def _validate_source_policy(cls, provenance: Stage2Provenance) -> None:
        if provenance.source_policy == "twse_dual_source_v1":
            if provenance.dataset_version_id is None or provenance.provenance_map_sha256 is None:
                raise CandidateSourcePolicyError(
                    "dual-source candidate requires dataset identity and provenance hash"
                )
            expected_quality = {
                "canonical_complete": "canonical",
                "provisional_mixed": "provisional",
                "reconciled": "reconciled",
            }[provenance.source_status]
            if provenance.research_data_quality != expected_quality:
                raise CandidateSourcePolicyError(
                    "candidate source status and research data quality disagree"
                )
            if provenance.source_status == "provisional_mixed":
                if provenance.authority_status != "incomplete" or provenance.reconciliation_status != "pending":
                    raise CandidateSourcePolicyError(
                        "provisional candidate must remain incomplete and pending"
                    )
                if provenance.esun_supplemental_count <= 0:
                    raise CandidateSourcePolicyError(
                        "provisional candidate requires E.SUN supplemental coverage"
                    )
            if provenance.source_status == "reconciled":
                if provenance.authority_status != "reconciled" or provenance.reconciliation_status not in {
                    "reconciled_equal", "reconciled_discrepant"
                }:
                    raise CandidateSourcePolicyError(
                        "reconciled candidate status is incomplete"
                    )
                if provenance.esun_supplemental_count != 0:
                    raise CandidateSourcePolicyError(
                        "reconciled candidate cannot select E.SUN supplemental rows"
                    )
        canonical = {item.casefold() for item in provenance.canonical_sources}
        validation = {item.casefold() for item in provenance.validation_sources}
        supplemental = {item.casefold() for item in provenance.supplemental_sources}
        for artifact in provenance.artifact_refs:
            cls._validate_artifact(artifact)
        artifact_sources = {
            artifact.provider.casefold() for artifact in provenance.artifact_refs
        }
        all_sources = canonical | validation | artifact_sources
        if MOCK_SYNTHETIC_SOURCE in all_sources:
            if canonical != {MOCK_SYNTHETIC_SOURCE} or all_sources != {
                MOCK_SYNTHETIC_SOURCE
            }:
                raise CandidateSourcePolicyError(
                    "mock-synthetic cannot mix with formal candidate sources"
                )
            return
        forbidden = canonical & ESUN_VALIDATION_SOURCES
        if forbidden:
            raise CandidateSourcePolicyError(
                "E.SUN cannot appear in canonical candidate sources: "
                + ", ".join(sorted(forbidden))
            )
        allowed_canonical = TWSE_BASELINE_SOURCES | {MOCK_SYNTHETIC_SOURCE}
        if canonical - allowed_canonical:
            raise CandidateSourcePolicyError(
                "candidate canonical source is outside the TWSE baseline"
            )
        allowed_validation = (
            TWSE_BASELINE_SOURCES | ESUN_VALIDATION_SOURCES | {MOCK_SYNTHETIC_SOURCE}
        )
        if validation - allowed_validation:
            raise CandidateSourcePolicyError(
                "candidate validation source is outside the frozen source families"
            )
        if supplemental - ESUN_VALIDATION_SOURCES:
            raise CandidateSourcePolicyError(
                "candidate supplemental source is outside the E.SUN family"
            )
        if provenance.esun_supplemental_count and not supplemental:
            raise CandidateSourcePolicyError(
                "supplemental observation count requires explicit E.SUN provenance"
            )
        for artifact in provenance.artifact_refs:
            cls._artifact_role(artifact, provenance)

    def _validate_dataset_version_locators(
        self,
        candidate_inputs: tuple[
            tuple[Stage1Candidate, CandidateInputLocator, str], ...
        ],
        *,
        market_date: date,
    ) -> None:
        for candidate, locator, unused_hash in candidate_inputs:
            if locator.dataset_version_id is None:
                continue
            try:
                version = DatasetVersionRepository(self.database_path).replay(
                    locator.dataset_version_id
                )
            except (
                DatasetMigrationStateError,
                DatasetVersionNotFoundError,
                DatasetPersistenceIntegrityError,
            ) as error:
                raise ScreenerCheckpointConflictError(
                    f"dataset version for {candidate.symbol} is not a verified persisted v12 version"
                ) from error
            self._assert_dataset_version_locator(
                version,
                locator=locator,
                market_date=market_date,
            )

    @staticmethod
    def _validate_dataset_version_connection(
        connection: sqlite3.Connection,
        *,
        locator: CandidateInputLocator,
        market_date: date,
    ) -> None:
        try:
            DatasetVersionMigrationRunner._require_v12(connection)
            version = DatasetVersionRepository._reconstruct(
                connection,
                locator.dataset_version_id,
            )
        except (
            DatasetMigrationStateError,
            DatasetVersionNotFoundError,
            DatasetPersistenceIntegrityError,
        ) as error:
            raise ScreenerCheckpointConflictError(
                "candidate dataset_version_id is not a verified persisted v12 version"
            ) from error
        SQLiteScreenerCheckpointRepository._assert_dataset_version_locator(
            version,
            locator=locator,
            market_date=market_date,
        )

    @staticmethod
    def _assert_dataset_version_locator(
        version: object,
        *,
        locator: CandidateInputLocator,
        market_date: date,
    ) -> None:
        identity = version.identity
        coverage = identity.coverage
        summary = version.provenance_summary
        quality = {
            "canonical_complete": "canonical",
            "provisional_mixed": "provisional",
            "reconciled": "reconciled",
        }[identity.source_status.value]
        expected = (
            identity.symbol == locator.symbol
            and identity.source_policy == locator.source_policy
            and identity.source_status.value == locator.source_status
            and identity.authority_status.value == locator.authority_status
            and identity.reconciliation_status.value == locator.reconciliation_status
            and quality == locator.research_data_quality
            and coverage.twse_observation_count == locator.twse_observation_count
            and coverage.esun_supplemental_count == locator.supplemental_count
            and coverage.missing_twse_count == locator.missing_twse_count
            and coverage.discrepancy_count == locator.discrepancy_count
            and summary.supplemental_sources == locator.supplemental_sources
            and identity.provenance_map_sha256 == locator.provenance_map_sha256
        )
        if not expected:
            raise ScreenerCheckpointConflictError(
                "candidate locator does not match the persisted dataset version"
            )

    @classmethod
    def _artifact_role(
        cls,
        artifact: Stage2ArtifactRef,
        provenance: Stage2Provenance,
    ) -> str:
        provider = artifact.provider.casefold()
        canonical = {item.casefold() for item in provenance.canonical_sources}
        validation = {item.casefold() for item in provenance.validation_sources}
        supplemental = {item.casefold() for item in provenance.supplemental_sources}
        if provider in ESUN_VALIDATION_SOURCES:
            if provider in supplemental:
                # v11's artifact table has no supplemental role; retain the
                # source as a validation-shaped evidence row while the DS5
                # metadata preserves its actual supplemental role.
                return "validation"
            if provider not in validation:
                raise CandidateSourcePolicyError(
                    "E.SUN artifact requires explicit validation provenance"
                )
            return "validation"
        if artifact.owner_kind == "validation":
            if provider not in validation:
                raise CandidateSourcePolicyError(
                    "validation-owned artifact lacks validation source provenance"
                )
            return "validation"
        if provider in canonical:
            return "canonical"
        if provider in validation:
            return "validation"
        raise CandidateSourcePolicyError(
            "candidate artifact provider has no canonical or validation role"
        )

    @staticmethod
    def _validate_artifact(artifact: Stage2ArtifactRef) -> None:
        if not isinstance(artifact, Stage2ArtifactRef):
            raise CandidateSourcePolicyError("invalid candidate artifact reference")
        if artifact.owner_kind not in {
            "pipeline", "historical", "validation", "dataset_version"
        }:
            raise CandidateSourcePolicyError("invalid artifact owner kind")
        for value, field_name in (
            (artifact.owner_run_id, "owner_run_id"),
            (artifact.provider, "provider"),
            (artifact.dataset, "dataset"),
            (artifact.endpoint, "endpoint"),
            (artifact.contract_version, "contract_version"),
        ):
            if not isinstance(value, str) or not value.strip():
                raise CandidateSourcePolicyError(f"artifact {field_name} is blank")
        _validate_source_ref(artifact.endpoint)
        _require_sha256(artifact.payload_sha256, "artifact payload_sha256")
        if (
            isinstance(artifact.payload_size_bytes, bool)
            or not isinstance(artifact.payload_size_bytes, int)
            or artifact.payload_size_bytes < 0
        ):
            raise CandidateSourcePolicyError("artifact payload size is invalid")
        if artifact.hash_basis not in {
            "raw-response-bytes-v1",
            "canonical-json-v1",
        }:
            raise CandidateSourcePolicyError("artifact hash basis is invalid")

    @staticmethod
    def _artifact_ref_id(
        candidate_id: str,
        ordinal: int,
        role: str,
        artifact: Stage2ArtifactRef,
    ) -> str:
        return _canonical_sha256(
            {
                "candidate_id": candidate_id,
                "ordinal": ordinal,
                "source_role": role,
                "artifact": _artifact_dict(artifact),
            }
        )


def _dataset_metadata_from_locators(
    locators: tuple[CandidateInputLocator, ...],
) -> dict[str, object]:
    dual = tuple(item for item in locators if item.dataset_version_id is not None)
    if not dual:
        return {
            "execution_status": "success",
            "source_policy": TWSE_BASELINE_SOURCE_POLICY,
            "source_status": "canonical_complete",
            "research_data_quality": "canonical",
            "authority_status": "complete",
        "reconciliation_status": "not_applicable",
        "supplemental_count": 0,
        "supplemental_candidate_count": 0,
        "supplemental_sources": "[]",
            "twse_observation_count": 0,
            "missing_twse_count": 0,
            "discrepancy_count": 0,
            "dataset_identity_sha256": None,
            "provenance_map_sha256": None,
            "dataset_version_ids_json": "[]",
        }
    policies = {item.source_policy for item in dual}
    if policies != {"twse_dual_source_v1"} or len(dual) != len(locators):
        raise ScreenerCheckpointConflictError(
            "a Screener run cannot mix legacy and dual-source dataset locators"
        )
    qualities = {item.research_data_quality for item in dual}
    quality = "provisional" if "provisional" in qualities else (
        "reconciled" if "reconciled" in qualities else "canonical"
    )
    authorities = {item.authority_status for item in dual}
    authority = "incomplete" if "incomplete" in authorities else (
        "reconciled" if "reconciled" in authorities else "complete"
    )
    reconciliation_values = {item.reconciliation_status for item in dual}
    if "pending" in reconciliation_values:
        reconciliation = "pending"
    elif "reconciled_discrepant" in reconciliation_values:
        reconciliation = "reconciled_discrepant"
    elif "reconciled_equal" in reconciliation_values:
        reconciliation = "reconciled_equal"
    else:
        reconciliation = "not_applicable"
    ordered = tuple(sorted(dual, key=lambda item: (item.symbol, item.dataset_version_id or "")))
    return {
        "execution_status": "provisional_success" if quality == "provisional" else "success",
        "source_policy": "twse_dual_source_v1",
        "source_status": (
            "provisional_mixed" if quality == "provisional"
            else ("reconciled" if quality == "reconciled" else "canonical_complete")
        ),
        "research_data_quality": quality,
        "authority_status": authority,
        "reconciliation_status": reconciliation,
        "supplemental_count": sum(item.supplemental_count for item in ordered),
        "supplemental_candidate_count": sum(
            item.supplemental_count > 0 for item in ordered
        ),
        "supplemental_sources": _canonical_json(
            sorted({source for item in ordered for source in item.supplemental_sources})
        ),
        "twse_observation_count": sum(item.twse_observation_count for item in ordered),
        "missing_twse_count": sum(item.missing_twse_count for item in ordered),
        "discrepancy_count": sum(item.discrepancy_count for item in ordered),
        "dataset_identity_sha256": _canonical_sha256(
            [item.identity_payload() for item in ordered]
        ),
        "provenance_map_sha256": _canonical_sha256(
            [item.provenance_map_sha256 for item in ordered]
        ),
        "dataset_version_ids_json": _canonical_json(
            [item.dataset_version_id for item in ordered]
        ),
    }


def _stage1_reason_dict(reason: Stage1Reason) -> dict[str, object]:
    return {
        "code": reason.code,
        "metric": reason.metric,
        "component": reason.component,
        "previous": reason.previous,
        "current": reason.current,
        "delta": reason.delta,
        "unit": reason.unit,
        "operator": reason.operator,
        "threshold": reason.threshold,
        "rule_version": reason.rule_version,
        "role": reason.role,
        "trigger_class": reason.trigger_class,
        "threshold_multiple": reason.threshold_multiple,
    }


def _stage2_reason_dict(reason: Stage2Reason) -> dict[str, object]:
    return {
        "code": reason.code,
        "metric": reason.metric,
        "previous": reason.previous,
        "current": reason.current,
        "delta": reason.delta,
        "unit": reason.unit,
        "operator": reason.operator,
        "threshold": reason.threshold,
        "rule_version": reason.rule_version,
        "reason_kind": reason.reason_kind,
        "reason_class": reason.reason_class,
        "threshold_multiple": reason.threshold_multiple,
    }


def _metric_dict(metric: Stage2Metric) -> dict[str, object]:
    return {
        "name": metric.name,
        "status": metric.status.value,
        "value": metric.value,
        "previous_value": metric.previous_value,
        "delta": metric.delta,
        "unit": metric.unit,
        "as_of_date": metric.as_of_date.isoformat(),
        "previous_as_of_date": _date_text(metric.previous_as_of_date),
        "observations": metric.observations,
        "previous_observations": metric.previous_observations,
    }


def _quality_dict(value: Stage2DataQuality) -> dict[str, object]:
    return {
        "status": value.status.value,
        "validation_status": value.validation_status,
        "discrepancies": _discrepancy_dicts(value),
    }


def _discrepancy_dicts(value: Stage2DataQuality) -> list[dict[str, object]]:
    return [
        {
            "field": item.field,
            "left_value": item.left_value,
            "right_value": item.right_value,
            "reason": item.reason,
        }
        for item in value.discrepancies
    ]


def _provenance_dict(value: Stage2Provenance) -> dict[str, object]:
    return {
        "pipeline_run_id": value.pipeline_run_id,
        "historical_run_id": value.historical_run_id,
        "validation_run_id": value.validation_run_id,
        "canonical_sources": list(value.canonical_sources),
        "validation_sources": list(value.validation_sources),
        "artifact_refs": [_artifact_dict(item) for item in value.artifact_refs],
    }


def _artifact_dict(value: Stage2ArtifactRef) -> dict[str, object]:
    return {
        "owner_kind": value.owner_kind,
        "owner_run_id": value.owner_run_id,
        "provider": value.provider,
        "dataset": value.dataset,
        "endpoint": value.endpoint,
        "contract_version": value.contract_version,
        "payload_sha256": value.payload_sha256,
        "payload_size_bytes": value.payload_size_bytes,
        "hash_basis": value.hash_basis,
    }


def _failure_dict(value: Stage2Failure | None) -> dict[str, object] | None:
    if value is None:
        return None
    return {
        "status": value.status,
        "code": value.code,
        "error_type": value.error_type,
    }


def _scalar_json(value: object) -> str:
    return _canonical_json(value)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _date_text(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_source_ref(value: str) -> None:
    parsed = urlsplit(value)
    if not parsed.scheme:
        raise CandidateSourcePolicyError("artifact source reference must be absolute")
    if parsed.username is not None or parsed.password is not None:
        raise CandidateSourcePolicyError(
            "artifact source reference must not contain credentials"
        )
    sensitive = {
        key.casefold()
        for key, unused_value in parse_qsl(parsed.query, keep_blank_values=True)
    } & _SENSITIVE_QUERY_KEYS
    if sensitive:
        raise CandidateSourcePolicyError(
            "artifact source reference must not contain credential query parameters"
        )


__all__ = [
    "CandidateCheckpoint",
    "CandidateInputLocator",
    "CandidatePersistenceResult",
    "CandidateSourcePolicyError",
    "SQLiteScreenerCheckpointRepository",
    "ScreenerCheckpointConflictError",
    "ScreenerCheckpointError",
    "ScreenerCheckpointStateError",
    "ScreenerRunPersistenceResult",
]
