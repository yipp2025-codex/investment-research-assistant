"""SQLite persistence boundary for frozen S1 Market Universe snapshots.

This module owns only ``market_universe_runs``, ``market_universe_members``,
and Universe-owned rows in ``screener_source_artifacts``.  It does not read or
write symbols, watchlists, Screener runs, candidates, reasons, or metrics.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

from app.screener.universe import (
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseContractError,
    UniverseMemberStatus,
)


_IDENTITY_VERSION = "market-universe-run-id-v1"


class MarketUniversePersistenceError(RuntimeError):
    """Base error for the S4.2 Universe persistence boundary."""


class MarketUniverseConflictError(MarketUniversePersistenceError):
    """Stored immutable Universe state conflicts with its identity or hash."""


class MarketUniverseStateError(MarketUniversePersistenceError):
    """The v11 schema or Universe run state cannot be used safely."""


@dataclass(frozen=True, slots=True)
class UniversePersistenceResult:
    universe_run_id: str
    snapshot: MarketUniverseSnapshot
    created: bool


class SQLiteMarketUniverseRepository:
    """Atomically persist and reconstruct immutable S1 Universe snapshots."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def persist(
        self, snapshot: MarketUniverseSnapshot
    ) -> UniversePersistenceResult:
        if not isinstance(snapshot, MarketUniverseSnapshot):
            raise TypeError("snapshot must be a MarketUniverseSnapshot")
        evidence = self._common_evidence(snapshot)
        input_evidence_sha256 = self._evidence_sha256(evidence)
        universe_run_id = self._run_id(snapshot, input_evidence_sha256)
        expected_identity = (
            snapshot.market_date.isoformat(),
            snapshot.methodology_version,
            snapshot.source_policy,
            input_evidence_sha256,
        )
        timestamp = datetime.now(timezone.utc).isoformat()

        with self._write_transaction() as connection:
            self._require_v11(connection)
            existing = self._find_existing_run(
                connection,
                universe_run_id=universe_run_id,
                identity=expected_identity,
            )
            if existing is not None:
                reconstructed = self._reconstruct_success(connection, existing)
                if reconstructed.canonical_json() != snapshot.canonical_json():
                    raise MarketUniverseConflictError(
                        "Universe replay input conflicts with the stored canonical output"
                    )
                result = UniversePersistenceResult(
                    universe_run_id=universe_run_id,
                    snapshot=reconstructed,
                    created=False,
                )
            else:
                self._insert_run(
                    connection,
                    universe_run_id=universe_run_id,
                    snapshot=snapshot,
                    input_evidence_sha256=input_evidence_sha256,
                    timestamp=timestamp,
                )
                self._insert_members(connection, universe_run_id, snapshot)
                self._insert_evidence(connection, universe_run_id, evidence)
                running = self._get_run(connection, universe_run_id)
                reconstructed = self._reconstruct_snapshot(connection, running)
                if reconstructed.canonical_json() != snapshot.canonical_json():
                    raise MarketUniverseConflictError(
                        "persisted Universe rows do not reconstruct the input snapshot"
                    )
                self._mark_success(
                    connection,
                    universe_run_id=universe_run_id,
                    canonical_sha256=snapshot.payload_sha256,
                    timestamp=timestamp,
                )
                completed = self._get_run(connection, universe_run_id)
                verified = self._reconstruct_success(connection, completed)
                result = UniversePersistenceResult(
                    universe_run_id=universe_run_id,
                    snapshot=verified,
                    created=True,
                )
        return result

    def load(self, universe_run_id: str) -> MarketUniverseSnapshot:
        self._require_sha256(universe_run_id, "universe_run_id")
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA query_only = ON")
        try:
            self._require_v11(connection)
            row = connection.execute(
                "SELECT * FROM market_universe_runs WHERE universe_run_id = ?",
                (universe_run_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown Universe run {universe_run_id}")
            return self._reconstruct_success(connection, row)
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
        migration = connection.execute(
            "SELECT name FROM schema_migrations WHERE version = 11"
        ).fetchone()
        if migration is None:
            raise MarketUniverseStateError(
                "Market Screener migration 11 must be applied explicitly"
            )

    @classmethod
    def _find_existing_run(
        cls,
        connection: sqlite3.Connection,
        *,
        universe_run_id: str,
        identity: tuple[str, str, str, str],
    ) -> sqlite3.Row | None:
        by_id = connection.execute(
            "SELECT * FROM market_universe_runs WHERE universe_run_id = ?",
            (universe_run_id,),
        ).fetchone()
        by_identity = connection.execute(
            "SELECT * FROM market_universe_runs WHERE market_date = ? "
            "AND methodology_version = ? AND source_policy = ? "
            "AND input_evidence_sha256 = ?",
            identity,
        ).fetchone()
        if by_id is not None and by_identity is not None:
            if by_id["universe_run_id"] != by_identity["universe_run_id"]:
                raise MarketUniverseConflictError(
                    "Universe run ID and deterministic identity resolve differently"
                )
        row = by_id if by_id is not None else by_identity
        if row is None:
            return None
        actual_identity = (
            row["market_date"],
            row["methodology_version"],
            row["source_policy"],
            row["input_evidence_sha256"],
        )
        if row["universe_run_id"] != universe_run_id or actual_identity != identity:
            raise MarketUniverseConflictError(
                "stored Universe identity conflicts with the deterministic run ID"
            )
        if row["status"] != "success":
            raise MarketUniverseStateError(
                "an atomic Universe run may only be replayed from success"
            )
        return row

    @staticmethod
    def _insert_run(
        connection: sqlite3.Connection,
        *,
        universe_run_id: str,
        snapshot: MarketUniverseSnapshot,
        input_evidence_sha256: str,
        timestamp: str,
    ) -> None:
        connection.execute(
            "INSERT INTO market_universe_runs ("
            "universe_run_id, market_date, methodology_version, source_policy, "
            "input_evidence_sha256, universe_count, scan_eligible_count, "
            "scan_unavailable_count, excluded_count, inactive_count, "
            "unresolved_count, status, attempt_count, created_at, started_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', 1, ?, ?, ?)",
            (
                universe_run_id,
                snapshot.market_date.isoformat(),
                snapshot.methodology_version,
                snapshot.source_policy,
                input_evidence_sha256,
                snapshot.universe_count,
                snapshot.scan_eligible_count,
                snapshot.scan_unavailable_count,
                snapshot.excluded_count,
                snapshot.inactive_count,
                snapshot.unresolved_count,
                timestamp,
                timestamp,
                timestamp,
            ),
        )

    @staticmethod
    def _insert_members(
        connection: sqlite3.Connection,
        universe_run_id: str,
        snapshot: MarketUniverseSnapshot,
    ) -> None:
        connection.executemany(
            "INSERT INTO market_universe_members ("
            "universe_run_id, symbol, name, market, status, listing_date, "
            "delisting_date, exclusion_reason"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    universe_run_id,
                    member.symbol,
                    member.name,
                    member.market,
                    member.status.value,
                    _date_text(member.listing_date),
                    _date_text(member.delisting_date),
                    member.exclusion_reason,
                )
                for member in snapshot.members
            ),
        )

    @classmethod
    def _insert_evidence(
        cls,
        connection: sqlite3.Connection,
        universe_run_id: str,
        evidence: tuple[SourceEvidence, ...],
    ) -> None:
        connection.executemany(
            "INSERT INTO screener_source_artifacts ("
            "artifact_ref_id, universe_run_id, screener_run_id, candidate_id, "
            "ordinal, source_role, upstream_owner_kind, upstream_owner_run_id, "
            "provider, dataset, source_ref, contract_version, payload_sha256, "
            "payload_size_bytes, hash_basis"
            ") VALUES (?, ?, NULL, NULL, ?, 'authority_input', NULL, NULL, "
            "?, ?, ?, ?, ?, ?, ?)",
            (
                (
                    cls._artifact_ref_id(universe_run_id, ordinal, item),
                    universe_run_id,
                    ordinal,
                    item.source,
                    item.dataset,
                    item.source_ref,
                    item.contract_version,
                    item.payload_sha256,
                    item.payload_size_bytes,
                    item.hash_basis,
                )
                for ordinal, item in enumerate(evidence, 1)
            ),
        )

    @staticmethod
    def _mark_success(
        connection: sqlite3.Connection,
        *,
        universe_run_id: str,
        canonical_sha256: str,
        timestamp: str,
    ) -> None:
        cursor = connection.execute(
            "UPDATE market_universe_runs SET status = 'success', "
            "canonical_sha256 = ?, finished_at = ?, updated_at = ? "
            "WHERE universe_run_id = ? AND status = 'running'",
            (canonical_sha256, timestamp, timestamp, universe_run_id),
        )
        if cursor.rowcount != 1:
            raise MarketUniverseStateError(
                "Universe run could not transition atomically to success"
            )

    @staticmethod
    def _get_run(
        connection: sqlite3.Connection, universe_run_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM market_universe_runs WHERE universe_run_id = ?",
            (universe_run_id,),
        ).fetchone()
        if row is None:
            raise MarketUniverseStateError("Universe run disappeared during persistence")
        return row

    @classmethod
    def _reconstruct_success(
        cls, connection: sqlite3.Connection, run: sqlite3.Row
    ) -> MarketUniverseSnapshot:
        if run["status"] != "success" or run["canonical_sha256"] is None:
            raise MarketUniverseStateError(
                "only a completed Universe run can be reconstructed for replay"
            )
        snapshot = cls._reconstruct_snapshot(connection, run)
        if snapshot.payload_sha256 != run["canonical_sha256"]:
            raise MarketUniverseConflictError(
                "stored Universe canonical hash does not match reconstructed rows"
            )
        return snapshot

    @classmethod
    def _reconstruct_snapshot(
        cls, connection: sqlite3.Connection, run: sqlite3.Row
    ) -> MarketUniverseSnapshot:
        evidence_rows = connection.execute(
            "SELECT * FROM screener_source_artifacts "
            "WHERE universe_run_id = ? ORDER BY ordinal",
            (run["universe_run_id"],),
        ).fetchall()
        if not evidence_rows:
            raise MarketUniverseConflictError(
                "Universe run is missing its authority evidence"
            )
        try:
            evidence = tuple(
                SourceEvidence(
                    source=row["provider"],
                    dataset=row["dataset"],
                    source_ref=row["source_ref"],
                    contract_version=row["contract_version"],
                    payload_sha256=row["payload_sha256"],
                    payload_size_bytes=row["payload_size_bytes"],
                    hash_basis=row["hash_basis"],
                )
                for row in evidence_rows
            )
            for ordinal, (row, item) in enumerate(zip(evidence_rows, evidence), 1):
                expected_ref = cls._artifact_ref_id(
                    run["universe_run_id"], ordinal, item
                )
                if row["ordinal"] != ordinal or row["artifact_ref_id"] != expected_ref:
                    raise MarketUniverseConflictError(
                        "Universe evidence identity or ordering was modified"
                    )
            if cls._evidence_sha256(evidence) != run["input_evidence_sha256"]:
                raise MarketUniverseConflictError(
                    "Universe evidence hash does not match the run identity"
                )

            member_rows = connection.execute(
                "SELECT * FROM market_universe_members "
                "WHERE universe_run_id = ? ORDER BY symbol",
                (run["universe_run_id"],),
            ).fetchall()
            members = tuple(
                MarketUniverseMember(
                    symbol=row["symbol"],
                    name=row["name"],
                    market=row["market"],
                    status=UniverseMemberStatus(row["status"]),
                    listing_date=_optional_date(row["listing_date"]),
                    delisting_date=_optional_date(row["delisting_date"]),
                    exclusion_reason=row["exclusion_reason"],
                    source_evidence=evidence,
                )
                for row in member_rows
            )
            snapshot = MarketUniverseSnapshot(
                market_date=date.fromisoformat(run["market_date"]),
                methodology_version=run["methodology_version"],
                source_policy=run["source_policy"],
                universe_count=run["universe_count"],
                scan_eligible_count=run["scan_eligible_count"],
                scan_unavailable_count=run["scan_unavailable_count"],
                excluded_count=run["excluded_count"],
                inactive_count=run["inactive_count"],
                unresolved_count=run["unresolved_count"],
                members=members,
            )
        except MarketUniversePersistenceError:
            raise
        except (TypeError, ValueError, UniverseContractError) as error:
            raise MarketUniverseConflictError(
                "stored Universe rows violate the frozen S1 contract"
            ) from error

        expected_run_id = cls._run_id(snapshot, run["input_evidence_sha256"])
        if expected_run_id != run["universe_run_id"]:
            raise MarketUniverseConflictError(
                "stored Universe run ID no longer matches its immutable identity"
            )
        return snapshot

    @classmethod
    def _common_evidence(
        cls, snapshot: MarketUniverseSnapshot
    ) -> tuple[SourceEvidence, ...]:
        if not snapshot.members:
            raise MarketUniversePersistenceError(
                "Universe persistence requires at least one member with evidence"
            )
        evidence = snapshot.members[0].source_evidence
        if not evidence:
            raise MarketUniversePersistenceError(
                "Universe persistence requires authority evidence"
            )
        if any(member.source_evidence != evidence for member in snapshot.members[1:]):
            raise MarketUniversePersistenceError(
                "all S1 Universe members must share one frozen evidence set"
            )
        expected = tuple(sorted(set(evidence), key=cls._evidence_key))
        if evidence != expected:
            raise MarketUniversePersistenceError(
                "Universe authority evidence must be unique and canonically ordered"
            )
        return evidence

    @classmethod
    def _run_id(
        cls, snapshot: MarketUniverseSnapshot, input_evidence_sha256: str
    ) -> str:
        value = {
            "identity_version": _IDENTITY_VERSION,
            "input_evidence_sha256": input_evidence_sha256,
            "market_date": snapshot.market_date.isoformat(),
            "methodology_version": snapshot.methodology_version,
            "source_policy": snapshot.source_policy,
        }
        return cls._canonical_sha256(value)

    @classmethod
    def _evidence_sha256(cls, evidence: tuple[SourceEvidence, ...]) -> str:
        return cls._canonical_sha256(
            [cls._evidence_dict(item) for item in evidence]
        )

    @classmethod
    def _artifact_ref_id(
        cls,
        universe_run_id: str,
        ordinal: int,
        evidence: SourceEvidence,
    ) -> str:
        return cls._canonical_sha256(
            {
                "evidence": cls._evidence_dict(evidence),
                "ordinal": ordinal,
                "owner_kind": "universe_run",
                "universe_run_id": universe_run_id,
            }
        )

    @staticmethod
    def _evidence_dict(evidence: SourceEvidence) -> dict[str, object]:
        return {
            "source": evidence.source,
            "dataset": evidence.dataset,
            "source_ref": evidence.source_ref,
            "contract_version": evidence.contract_version,
            "payload_sha256": evidence.payload_sha256,
            "payload_size_bytes": evidence.payload_size_bytes,
            "hash_basis": evidence.hash_basis,
        }

    @staticmethod
    def _evidence_key(evidence: SourceEvidence) -> tuple[str, ...]:
        return (
            evidence.source,
            evidence.dataset,
            evidence.source_ref,
            evidence.contract_version,
            evidence.payload_sha256,
            str(evidence.payload_size_bytes),
            evidence.hash_basis,
        )

    @staticmethod
    def _canonical_sha256(value: object) -> str:
        payload = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _require_sha256(value: object, field_name: str) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
        return value


def _date_text(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


__all__ = [
    "MarketUniverseConflictError",
    "MarketUniversePersistenceError",
    "MarketUniverseStateError",
    "SQLiteMarketUniverseRepository",
    "UniversePersistenceResult",
]
