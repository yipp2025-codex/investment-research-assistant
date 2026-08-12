"""Immutable TWSE listed-common-equity universe contract for Screener S1.

This module consumes normalized, immutable snapshots only.  Acquisition,
providers, network access, SQLite, watchlists, screening, and persistence are
deliberately outside this boundary.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Generic, TypeVar
from urllib.parse import parse_qsl, urlsplit

from app.research_dataset import TWSE_BASELINE_SOURCE_POLICY


UNIVERSE_METHODOLOGY_VERSION = "twse-listed-common-equity-universe-v1"
IDENTITY_DATASET = "t187ap03_L"
STOCK_DAY_ALL_DATASET = "STOCK_DAY_ALL"
VALUATION_DATASET = "BWIBBU_ALL"
CLASSIFICATION_DATASET = "FROZEN_ORDINARY_STOCK_CLASSIFICATION"
DELISTING_DATASET = "suspendListingCsvAndHtml"

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^[0-9A-Z]{2,12}$")
_SENSITIVE_QUERY_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "credential",
        "credentials",
        "key",
        "password",
        "secret",
        "signature",
        "token",
    }
)


class UniverseContractError(ValueError):
    """A universe input or output violates the frozen S1 contract."""


class UniverseMemberStatus(str, Enum):
    """Mutually exclusive daily universe states."""

    ACTIVE_SCAN_ELIGIBLE = "active_scan_eligible"
    ACTIVE_SCAN_UNAVAILABLE = "active_scan_unavailable"
    EXCLUDED_NON_COMMON_EQUITY = "excluded_non_common_equity"
    INACTIVE = "inactive"
    CLASSIFICATION_UNRESOLVED = "classification_unresolved"


class InstrumentClassification(str, Enum):
    """Frozen official-evidence classification; symbol shape is not a class."""

    COMMON_EQUITY = "common_equity"
    ETF = "etf"
    TDR = "tdr"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class SourceEvidence:
    """Hash-only metadata for one immutable input snapshot."""

    source: str
    dataset: str
    source_ref: str
    contract_version: str
    payload_sha256: str
    payload_size_bytes: int
    hash_basis: str

    def __post_init__(self) -> None:
        for field_name in (
            "source",
            "dataset",
            "source_ref",
            "contract_version",
            "hash_basis",
        ):
            object.__setattr__(
                self,
                field_name,
                _require_text(getattr(self, field_name), field_name),
            )
        if self.source != "twse":
            raise UniverseContractError("universe evidence must use TWSE authority")
        if _SHA256.fullmatch(self.payload_sha256) is None:
            raise UniverseContractError(
                "payload_sha256 must be a lowercase SHA-256 digest"
            )
        if (
            isinstance(self.payload_size_bytes, bool)
            or not isinstance(self.payload_size_bytes, int)
            or self.payload_size_bytes < 0
        ):
            raise UniverseContractError(
                "payload_size_bytes must be a non-negative integer"
            )
        if self.hash_basis not in {
            "raw-response-bytes-v1",
            "canonical-json-v1",
        }:
            raise UniverseContractError("unsupported source evidence hash_basis")
        _validate_source_ref(self.source_ref)


@dataclass(frozen=True, slots=True)
class ListedIdentityRecord:
    symbol: str
    name: str
    listing_date: date

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _require_text(self.name, "name"))
        object.__setattr__(
            self,
            "listing_date",
            _require_date(self.listing_date, "listing_date"),
        )


@dataclass(frozen=True, slots=True)
class DailyTradingRecord:
    symbol: str
    name: str
    price_available: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _require_text(self.name, "name"))
        if not isinstance(self.price_available, bool):
            raise UniverseContractError("price_available must be a bool")


@dataclass(frozen=True, slots=True)
class ValuationCoverageRecord:
    symbol: str
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _require_text(self.name, "name"))


@dataclass(frozen=True, slots=True)
class InstrumentClassificationRecord:
    symbol: str
    classification: InstrumentClassification

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        if not isinstance(self.classification, InstrumentClassification):
            raise UniverseContractError(
                "classification must be an InstrumentClassification"
            )


@dataclass(frozen=True, slots=True)
class DelistingRecord:
    symbol: str
    name: str | None
    delisting_date: date

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _normalize_optional_text(self.name, "name"))
        object.__setattr__(
            self,
            "delisting_date",
            _require_date(self.delisting_date, "delisting_date"),
        )


RecordT = TypeVar("RecordT")


@dataclass(frozen=True, slots=True)
class InputDatasetSnapshot(Generic[RecordT]):
    """One already-acquired immutable source snapshot and normalized rows."""

    evidence: SourceEvidence
    records: tuple[RecordT, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, SourceEvidence):
            raise UniverseContractError("snapshot evidence must be SourceEvidence")
        if not isinstance(self.records, tuple):
            raise UniverseContractError("snapshot records must be an immutable tuple")


@dataclass(frozen=True, slots=True)
class MarketUniverseBuildInput:
    """Complete normalized inputs required by the S1 pure builder."""

    market_date: date
    listed_identity: InputDatasetSnapshot[ListedIdentityRecord]
    daily_trading: InputDatasetSnapshot[DailyTradingRecord]
    valuation_coverage: InputDatasetSnapshot[ValuationCoverageRecord]
    classifications: InputDatasetSnapshot[InstrumentClassificationRecord]
    delistings: InputDatasetSnapshot[DelistingRecord]
    methodology_version: str = UNIVERSE_METHODOLOGY_VERSION
    source_policy: str = TWSE_BASELINE_SOURCE_POLICY

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "market_date",
            _require_date(self.market_date, "market_date"),
        )
        object.__setattr__(
            self,
            "methodology_version",
            _require_text(self.methodology_version, "methodology_version"),
        )
        if self.methodology_version != UNIVERSE_METHODOLOGY_VERSION:
            raise UniverseContractError("unsupported universe methodology_version")
        if self.source_policy != TWSE_BASELINE_SOURCE_POLICY:
            raise UniverseContractError("universe source_policy must be twse_baseline")

        expected = (
            ("listed_identity", self.listed_identity, IDENTITY_DATASET),
            ("daily_trading", self.daily_trading, STOCK_DAY_ALL_DATASET),
            ("valuation_coverage", self.valuation_coverage, VALUATION_DATASET),
            ("classifications", self.classifications, CLASSIFICATION_DATASET),
            ("delistings", self.delistings, DELISTING_DATASET),
        )
        for field_name, snapshot, dataset in expected:
            if not isinstance(snapshot, InputDatasetSnapshot):
                raise UniverseContractError(
                    f"{field_name} must be an InputDatasetSnapshot"
                )
            if snapshot.evidence.dataset != dataset:
                raise UniverseContractError(
                    f"{field_name} evidence must identify {dataset}"
                )


@dataclass(frozen=True, slots=True)
class MarketUniverseMember:
    symbol: str
    name: str | None
    market: str
    status: UniverseMemberStatus
    listing_date: date | None
    delisting_date: date | None
    exclusion_reason: str | None
    source_evidence: tuple[SourceEvidence, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_symbol(self.symbol))
        object.__setattr__(self, "name", _normalize_optional_text(self.name, "name"))
        if self.market != "TWSE":
            raise UniverseContractError("universe member market must be TWSE")
        if not isinstance(self.status, UniverseMemberStatus):
            raise UniverseContractError("status must be a UniverseMemberStatus")
        for field_name in ("listing_date", "delisting_date"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _require_date(value, field_name))
        object.__setattr__(
            self,
            "exclusion_reason",
            _normalize_optional_text(self.exclusion_reason, "exclusion_reason"),
        )
        if self.status is UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE:
            if self.exclusion_reason is not None:
                raise UniverseContractError(
                    "active_scan_eligible cannot have an exclusion_reason"
                )
        elif self.exclusion_reason is None:
            raise UniverseContractError(
                "non-eligible universe members require an exclusion_reason"
            )
        if not isinstance(self.source_evidence, tuple) or not self.source_evidence:
            raise UniverseContractError(
                "source_evidence must be a non-empty immutable tuple"
            )
        if any(not isinstance(item, SourceEvidence) for item in self.source_evidence):
            raise UniverseContractError("invalid source_evidence item")
        expected_evidence = tuple(sorted(set(self.source_evidence), key=_evidence_key))
        if self.source_evidence != expected_evidence:
            raise UniverseContractError(
                "source_evidence must be unique and deterministically ordered"
            )


@dataclass(frozen=True, slots=True)
class MarketUniverseSnapshot:
    """Canonical deterministic output of the S1 universe builder."""

    market_date: date
    methodology_version: str
    source_policy: str
    universe_count: int
    scan_eligible_count: int
    scan_unavailable_count: int
    excluded_count: int
    inactive_count: int
    unresolved_count: int
    members: tuple[MarketUniverseMember, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "market_date",
            _require_date(self.market_date, "market_date"),
        )
        if self.methodology_version != UNIVERSE_METHODOLOGY_VERSION:
            raise UniverseContractError("snapshot methodology_version is unsupported")
        if self.source_policy != TWSE_BASELINE_SOURCE_POLICY:
            raise UniverseContractError("snapshot source_policy must be twse_baseline")
        if not isinstance(self.members, tuple):
            raise UniverseContractError("members must be an immutable tuple")
        if any(not isinstance(item, MarketUniverseMember) for item in self.members):
            raise UniverseContractError("invalid universe member")
        if self.members != tuple(sorted(self.members, key=lambda item: item.symbol)):
            raise UniverseContractError("members must be ordered by symbol")
        symbols = [item.symbol for item in self.members]
        if len(symbols) != len(set(symbols)):
            raise UniverseContractError("members must have unique symbols")

        expected_counts = {
            "universe_count": len(self.members),
            "scan_eligible_count": _status_count(
                self.members, UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE
            ),
            "scan_unavailable_count": _status_count(
                self.members, UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE
            ),
            "excluded_count": _status_count(
                self.members, UniverseMemberStatus.EXCLUDED_NON_COMMON_EQUITY
            ),
            "inactive_count": _status_count(
                self.members, UniverseMemberStatus.INACTIVE
            ),
            "unresolved_count": _status_count(
                self.members, UniverseMemberStatus.CLASSIFICATION_UNRESOLVED
            ),
        }
        for field_name, expected in expected_counts.items():
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value != expected:
                raise UniverseContractError(
                    f"{field_name} must equal derived count {expected}"
                )

    def as_dict(self) -> dict[str, object]:
        return {
            "market_date": self.market_date.isoformat(),
            "methodology_version": self.methodology_version,
            "source_policy": self.source_policy,
            "universe_count": self.universe_count,
            "scan_eligible_count": self.scan_eligible_count,
            "scan_unavailable_count": self.scan_unavailable_count,
            "excluded_count": self.excluded_count,
            "inactive_count": self.inactive_count,
            "unresolved_count": self.unresolved_count,
            "members": [_member_dict(item) for item in self.members],
        }

    def canonical_json(self) -> str:
        return json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


def build_market_universe(inputs: MarketUniverseBuildInput) -> MarketUniverseSnapshot:
    """Build one deterministic universe without side effects or I/O."""

    if not isinstance(inputs, MarketUniverseBuildInput):
        raise UniverseContractError("inputs must be MarketUniverseBuildInput")

    identity = _index_records(
        inputs.listed_identity.records,
        ListedIdentityRecord,
        IDENTITY_DATASET,
    )
    trading = _index_records(
        inputs.daily_trading.records,
        DailyTradingRecord,
        STOCK_DAY_ALL_DATASET,
    )
    valuation = _index_records(
        inputs.valuation_coverage.records,
        ValuationCoverageRecord,
        VALUATION_DATASET,
    )
    classifications = _index_records(
        inputs.classifications.records,
        InstrumentClassificationRecord,
        CLASSIFICATION_DATASET,
    )
    delistings = _index_records(
        inputs.delistings.records,
        DelistingRecord,
        DELISTING_DATASET,
    )

    # Delisting history is evidence, never an enumeration source by itself.
    symbols = sorted(set(identity) | set(trading) | set(valuation) | set(classifications))
    evidence = tuple(
        sorted(
            {
                inputs.listed_identity.evidence,
                inputs.daily_trading.evidence,
                inputs.valuation_coverage.evidence,
                inputs.classifications.evidence,
                inputs.delistings.evidence,
            },
            key=_evidence_key,
        )
    )
    members = tuple(
        _build_member(
            symbol=symbol,
            market_date=inputs.market_date,
            identity=identity.get(symbol),
            trading=trading.get(symbol),
            valuation=valuation.get(symbol),
            classification=classifications.get(symbol),
            delisting=delistings.get(symbol),
            source_evidence=evidence,
        )
        for symbol in symbols
    )

    return MarketUniverseSnapshot(
        market_date=inputs.market_date,
        methodology_version=inputs.methodology_version,
        source_policy=inputs.source_policy,
        universe_count=len(members),
        scan_eligible_count=_status_count(
            members, UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE
        ),
        scan_unavailable_count=_status_count(
            members, UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE
        ),
        excluded_count=_status_count(
            members, UniverseMemberStatus.EXCLUDED_NON_COMMON_EQUITY
        ),
        inactive_count=_status_count(members, UniverseMemberStatus.INACTIVE),
        unresolved_count=_status_count(
            members, UniverseMemberStatus.CLASSIFICATION_UNRESOLVED
        ),
        members=members,
    )


def _build_member(
    *,
    symbol: str,
    market_date: date,
    identity: ListedIdentityRecord | None,
    trading: DailyTradingRecord | None,
    valuation: ValuationCoverageRecord | None,
    classification: InstrumentClassificationRecord | None,
    delisting: DelistingRecord | None,
    source_evidence: tuple[SourceEvidence, ...],
) -> MarketUniverseMember:
    names = tuple(
        item.name
        for item in (identity, trading, valuation, delisting)
        if item is not None and item.name is not None
    )
    name = _preferred_name(identity, trading, valuation, delisting)
    listing_date = identity.listing_date if identity is not None else None
    delisting_date = delisting.delisting_date if delisting is not None else None

    if len(set(names)) > 1:
        return _member(
            symbol,
            name,
            UniverseMemberStatus.CLASSIFICATION_UNRESOLVED,
            listing_date,
            delisting_date,
            "official_identity_conflict",
            source_evidence,
        )
    if listing_date is not None and listing_date > market_date:
        return _member(
            symbol,
            name,
            UniverseMemberStatus.CLASSIFICATION_UNRESOLVED,
            listing_date,
            delisting_date,
            "listing_date_after_market_date",
            source_evidence,
        )
    if delisting_date is not None and delisting_date <= market_date:
        return _member(
            symbol,
            name,
            UniverseMemberStatus.INACTIVE,
            listing_date,
            delisting_date,
            "explicit_delisting_evidence",
            source_evidence,
        )
    if classification is None:
        return _member(
            symbol,
            name,
            UniverseMemberStatus.CLASSIFICATION_UNRESOLVED,
            listing_date,
            delisting_date,
            "classification_evidence_missing",
            source_evidence,
        )
    if classification.classification is InstrumentClassification.UNKNOWN:
        return _member(
            symbol,
            name,
            UniverseMemberStatus.CLASSIFICATION_UNRESOLVED,
            listing_date,
            delisting_date,
            "classification_evidence_unresolved",
            source_evidence,
        )
    if classification.classification in {
        InstrumentClassification.ETF,
        InstrumentClassification.TDR,
    }:
        return _member(
            symbol,
            name,
            UniverseMemberStatus.EXCLUDED_NON_COMMON_EQUITY,
            listing_date,
            delisting_date,
            "non_common_equity_" + classification.classification.value,
            source_evidence,
        )
    if identity is None:
        return _member(
            symbol,
            name,
            UniverseMemberStatus.CLASSIFICATION_UNRESOLVED,
            listing_date,
            delisting_date,
            "listed_identity_missing",
            source_evidence,
        )
    if trading is None and valuation is None:
        reason = "stock_day_all_and_bwibbu_all_missing"
    elif trading is None:
        reason = "stock_day_all_missing"
    elif not trading.price_available:
        reason = "stock_day_all_price_unavailable"
    elif valuation is None:
        reason = "bwibbu_all_missing"
    else:
        return _member(
            symbol,
            name,
            UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
            listing_date,
            delisting_date,
            None,
            source_evidence,
        )
    return _member(
        symbol,
        name,
        UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE,
        listing_date,
        delisting_date,
        reason,
        source_evidence,
    )


def _member(
    symbol: str,
    name: str | None,
    status: UniverseMemberStatus,
    listing_date: date | None,
    delisting_date: date | None,
    exclusion_reason: str | None,
    source_evidence: tuple[SourceEvidence, ...],
) -> MarketUniverseMember:
    return MarketUniverseMember(
        symbol=symbol,
        name=name,
        market="TWSE",
        status=status,
        listing_date=listing_date,
        delisting_date=delisting_date,
        exclusion_reason=exclusion_reason,
        source_evidence=source_evidence,
    )


def _preferred_name(
    identity: ListedIdentityRecord | None,
    trading: DailyTradingRecord | None,
    valuation: ValuationCoverageRecord | None,
    delisting: DelistingRecord | None,
) -> str | None:
    for item in (identity, trading, valuation, delisting):
        if item is not None and item.name is not None:
            return item.name
    return None


def _index_records(
    records: tuple[RecordT, ...],
    record_type: type[RecordT],
    dataset: str,
) -> dict[str, RecordT]:
    indexed: dict[str, RecordT] = {}
    for record in records:
        if not isinstance(record, record_type):
            raise UniverseContractError(f"{dataset} contains an invalid record type")
        prior = indexed.get(record.symbol)
        if prior is None:
            indexed[record.symbol] = record
        elif prior != record:
            raise UniverseContractError(
                f"{dataset} has conflicting duplicate rows for {record.symbol}"
            )
        # Exact normalized duplicates are intentionally and deterministically deduped.
    return indexed


def _status_count(
    members: tuple[MarketUniverseMember, ...],
    status: UniverseMemberStatus,
) -> int:
    return sum(item.status is status for item in members)


def _member_dict(member: MarketUniverseMember) -> dict[str, object]:
    return {
        "symbol": member.symbol,
        "name": member.name,
        "market": member.market,
        "status": member.status.value,
        "listing_date": (
            member.listing_date.isoformat() if member.listing_date is not None else None
        ),
        "delisting_date": (
            member.delisting_date.isoformat()
            if member.delisting_date is not None
            else None
        ),
        "exclusion_reason": member.exclusion_reason,
        "source_evidence": [_evidence_dict(item) for item in member.source_evidence],
    }


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


def _normalize_symbol(value: object) -> str:
    symbol = _require_text(value, "symbol").upper()
    if _SYMBOL.fullmatch(symbol) is None:
        raise UniverseContractError("symbol must be 2-12 uppercase letters or digits")
    return symbol


def _require_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise UniverseContractError(f"{field_name} must not be blank")
    return value.strip()


def _normalize_optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name)


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise UniverseContractError(f"{field_name} must be a date")
    return value


def _validate_source_ref(value: str) -> None:
    parsed = urlsplit(value)
    if not parsed.scheme:
        raise UniverseContractError("source_ref must be an absolute source reference")
    if parsed.username is not None or parsed.password is not None:
        raise UniverseContractError("source_ref must not contain credentials")
    for key, unused_value in parse_qsl(parsed.query, keep_blank_values=True):
        del unused_value
        if key.casefold() in _SENSITIVE_QUERY_KEYS:
            raise UniverseContractError(
                "source_ref must not contain credential-bearing query keys"
            )
