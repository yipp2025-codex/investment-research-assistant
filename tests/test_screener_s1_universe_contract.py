"""Screener S1 universe contract and deterministic-builder gates."""

from __future__ import annotations

import ast
import hashlib
import json
import socket
import sqlite3
import urllib.request
from dataclasses import FrozenInstanceError, fields, replace
from datetime import date
from pathlib import Path

import pytest

import app.screener.universe as universe_module
from app.providers import manifest as provider_manifest_module
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
    UniverseContractError,
    UniverseMemberStatus,
    ValuationCoverageRecord,
    build_market_universe,
)


MARKET_DATE = date(2026, 8, 7)
FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "screener"
    / "twse_universe_20260807.json"
)
FROZEN_SNAPSHOT_SHA256 = (
    "ce43e0e9bdb525ec807170cd9b49bf79e55d2fd6f46a42f6deebbe8f740b9324"
)

SOURCE_EVIDENCE_FIELDS = (
    "source",
    "dataset",
    "source_ref",
    "contract_version",
    "payload_sha256",
    "payload_size_bytes",
    "hash_basis",
)


def _fixture_payload() -> dict[str, object]:
    value = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _fixture_evidence(value: dict[str, object]) -> SourceEvidence:
    return SourceEvidence(**{name: value[name] for name in SOURCE_EVIDENCE_FIELDS})


def _frozen_inputs() -> MarketUniverseBuildInput:
    payload = _fixture_payload()
    sources = payload["source_snapshots"]
    assert isinstance(sources, dict)
    eligible = payload["eligible"]
    unavailable = payload["active_unavailable"]
    excluded = payload["excluded"]
    assert isinstance(eligible, list)
    assert isinstance(unavailable, list)
    assert isinstance(excluded, list)

    identity_rows = [
        ListedIdentityRecord(symbol, name, date.fromisoformat(listing_date))
        for symbol, name, listing_date in [*eligible, *unavailable]
    ]
    identity_rows.extend(
        ListedIdentityRecord(symbol, name, date.fromisoformat(listing_date))
        for symbol, name, unused_classification, listing_date in excluded
        if listing_date is not None
    )
    trading_rows = [
        DailyTradingRecord(symbol, name, True)
        for symbol, name, unused_listing_date in eligible
    ]
    trading_rows.extend(
        DailyTradingRecord(symbol, name, True)
        for symbol, name, unused_classification, unused_listing_date in excluded
    )
    valuation_rows = [
        ValuationCoverageRecord(symbol, name)
        for symbol, name, unused_listing_date in eligible
    ]
    classification_rows = [
        InstrumentClassificationRecord(
            symbol,
            InstrumentClassification.COMMON_EQUITY,
        )
        for symbol, unused_name, unused_listing_date in [*eligible, *unavailable]
    ]
    classification_rows.extend(
        InstrumentClassificationRecord(
            symbol,
            InstrumentClassification(classification),
        )
        for symbol, unused_name, classification, unused_listing_date in excluded
    )

    return MarketUniverseBuildInput(
        market_date=date.fromisoformat(str(payload["market_date"])),
        methodology_version=str(payload["methodology_version"]),
        source_policy=str(payload["source_policy"]),
        listed_identity=InputDatasetSnapshot(
            _fixture_evidence(sources["identity"]),
            tuple(identity_rows),
        ),
        daily_trading=InputDatasetSnapshot(
            _fixture_evidence(sources["daily_trading"]),
            tuple(trading_rows),
        ),
        valuation_coverage=InputDatasetSnapshot(
            _fixture_evidence(sources["valuation_coverage"]),
            tuple(valuation_rows),
        ),
        classifications=InputDatasetSnapshot(
            _fixture_evidence(sources["classifications"]),
            tuple(classification_rows),
        ),
        delistings=InputDatasetSnapshot(
            _fixture_evidence(sources["delistings"]),
            (),
        ),
    )


def _evidence(dataset: str) -> SourceEvidence:
    raw = dataset.encode("utf-8")
    return SourceEvidence(
        source="twse",
        dataset=dataset,
        source_ref=f"contract://screener-s1-tests/{dataset}",
        contract_version="screener-s1-test-v1",
        payload_sha256=hashlib.sha256(raw).hexdigest(),
        payload_size_bytes=len(raw),
        hash_basis="canonical-json-v1",
    )


def _minimal_inputs(
    *,
    identity: tuple[ListedIdentityRecord, ...] | None = None,
    trading: tuple[DailyTradingRecord, ...] | None = None,
    valuation: tuple[ValuationCoverageRecord, ...] | None = None,
    classifications: tuple[InstrumentClassificationRecord, ...] | None = None,
    delistings: tuple[DelistingRecord, ...] = (),
) -> MarketUniverseBuildInput:
    return MarketUniverseBuildInput(
        market_date=MARKET_DATE,
        listed_identity=InputDatasetSnapshot(
            _evidence(IDENTITY_DATASET),
            identity
            if identity is not None
            else (ListedIdentityRecord("2330", "台積電", date(1994, 9, 5)),),
        ),
        daily_trading=InputDatasetSnapshot(
            _evidence(STOCK_DAY_ALL_DATASET),
            trading
            if trading is not None
            else (DailyTradingRecord("2330", "台積電", True),),
        ),
        valuation_coverage=InputDatasetSnapshot(
            _evidence(VALUATION_DATASET),
            valuation
            if valuation is not None
            else (ValuationCoverageRecord("2330", "台積電"),),
        ),
        classifications=InputDatasetSnapshot(
            _evidence(CLASSIFICATION_DATASET),
            classifications
            if classifications is not None
            else (
                InstrumentClassificationRecord(
                    "2330",
                    InstrumentClassification.COMMON_EQUITY,
                ),
            ),
        ),
        delistings=InputDatasetSnapshot(
            _evidence(DELISTING_DATASET),
            delistings,
        ),
    )


def _member(snapshot: MarketUniverseSnapshot, symbol: str):
    return next(item for item in snapshot.members if item.symbol == symbol)


def test_frozen_fixture_reproduces_20260807_authority_counts_and_gate() -> None:
    payload = _fixture_payload()
    sources = payload["source_snapshots"]
    expected = payload["expected"]
    assert isinstance(sources, dict)
    assert isinstance(expected, dict)
    assert sources["daily_trading"]["raw_row_count"] == 1_377
    assert sources["daily_trading"]["normalized_record_count"] == 1_094
    assert sources["valuation_coverage"]["raw_row_count"] == 1_082
    assert expected["stock_only_four_digit_rows"] == 12

    snapshot = build_market_universe(_frozen_inputs())

    assert len(payload["eligible"]) == 1_082
    assert snapshot.universe_count == expected["universe_count"] == 1_095
    assert snapshot.scan_eligible_count == expected["scan_eligible_count"] == 1_082
    assert snapshot.scan_unavailable_count == 1
    assert snapshot.excluded_count == 12
    assert snapshot.inactive_count == 0
    assert snapshot.unresolved_count == 0
    unavailable = _member(snapshot, "1589")
    assert unavailable.name == "永冠-KY"
    assert unavailable.status is UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE
    assert unavailable.exclusion_reason == "stock_day_all_and_bwibbu_all_missing"
    assert [item.symbol for item in snapshot.members] == sorted(
        item.symbol for item in snapshot.members
    )


def test_frozen_fixture_keeps_twelve_exclusions_explainable() -> None:
    snapshot = build_market_universe(_frozen_inputs())
    expected = {
        "0050": "non_common_equity_etf",
        "0051": "non_common_equity_etf",
        "0052": "non_common_equity_etf",
        "0053": "non_common_equity_etf",
        "0055": "non_common_equity_etf",
        "0056": "non_common_equity_etf",
        "0057": "non_common_equity_etf",
        "0061": "non_common_equity_etf",
        "9103": "non_common_equity_tdr",
        "9105": "non_common_equity_tdr",
        "9110": "non_common_equity_tdr",
        "9136": "non_common_equity_tdr",
    }
    actual = {
        item.symbol: item.exclusion_reason
        for item in snapshot.members
        if item.status is UniverseMemberStatus.EXCLUDED_NON_COMMON_EQUITY
    }
    assert actual == expected


def test_four_digit_symbol_shape_never_decides_ordinary_stock() -> None:
    inputs = _minimal_inputs(
        identity=(ListedIdentityRecord("9999", "待分類", date(2020, 1, 1)),),
        trading=(DailyTradingRecord("9999", "待分類", True),),
        valuation=(ValuationCoverageRecord("9999", "待分類"),),
        classifications=(
            InstrumentClassificationRecord(
                "9999",
                InstrumentClassification.UNKNOWN,
            ),
        ),
    )
    result = build_market_universe(inputs)
    assert _member(result, "9999").status is (
        UniverseMemberStatus.CLASSIFICATION_UNRESOLVED
    )
    assert _member(result, "9999").exclusion_reason == (
        "classification_evidence_unresolved"
    )


def test_suspended_or_no_price_identity_remains_active_but_scan_unavailable() -> None:
    result = build_market_universe(
        _minimal_inputs(
            trading=(DailyTradingRecord("2330", "台積電", False),),
        )
    )
    member = _member(result, "2330")
    assert member.status is UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE
    assert member.exclusion_reason == "stock_day_all_price_unavailable"
    assert member.delisting_date is None


def test_stock_day_absence_does_not_mean_inactive() -> None:
    result = build_market_universe(_minimal_inputs(trading=()))
    member = _member(result, "2330")
    assert member.status is UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE
    assert member.exclusion_reason == "stock_day_all_missing"


def test_explicit_delisting_evidence_marks_member_inactive() -> None:
    result = build_market_universe(
        _minimal_inputs(
            delistings=(DelistingRecord("2330", "台積電", date(2026, 8, 1)),),
        )
    )
    member = _member(result, "2330")
    assert member.status is UniverseMemberStatus.INACTIVE
    assert member.delisting_date == date(2026, 8, 1)
    assert member.exclusion_reason == "explicit_delisting_evidence"


def test_newly_listed_incomplete_identity_is_active_scan_unavailable() -> None:
    result = build_market_universe(
        _minimal_inputs(
            identity=(
                ListedIdentityRecord("7777", "新上市", MARKET_DATE),
            ),
            trading=(),
            valuation=(),
            classifications=(
                InstrumentClassificationRecord(
                    "7777",
                    InstrumentClassification.COMMON_EQUITY,
                ),
            ),
        )
    )
    member = _member(result, "7777")
    assert member.status is UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE
    assert member.exclusion_reason == "stock_day_all_and_bwibbu_all_missing"


@pytest.mark.parametrize(
    ("classifications", "reason"),
    [
        ((), "classification_evidence_missing"),
        (
            (
                InstrumentClassificationRecord(
                    "2330",
                    InstrumentClassification.UNKNOWN,
                ),
            ),
            "classification_evidence_unresolved",
        ),
    ],
)
def test_missing_or_unknown_classification_fails_closed(
    classifications: tuple[InstrumentClassificationRecord, ...],
    reason: str,
) -> None:
    member = _member(
        build_market_universe(_minimal_inputs(classifications=classifications)),
        "2330",
    )
    assert member.status is UniverseMemberStatus.CLASSIFICATION_UNRESOLVED
    assert member.exclusion_reason == reason


def test_cross_source_identity_conflict_is_unresolved_not_arbitrarily_chosen() -> None:
    result = build_market_universe(
        _minimal_inputs(
            trading=(DailyTradingRecord("2330", "名稱衝突", True),),
        )
    )
    member = _member(result, "2330")
    assert member.status is UniverseMemberStatus.CLASSIFICATION_UNRESOLVED
    assert member.exclusion_reason == "official_identity_conflict"


def test_missing_listed_identity_for_common_equity_fails_closed() -> None:
    result = build_market_universe(_minimal_inputs(identity=()))
    member = _member(result, "2330")
    assert member.status is UniverseMemberStatus.CLASSIFICATION_UNRESOLVED
    assert member.exclusion_reason == "listed_identity_missing"


def test_exact_duplicate_official_rows_are_deterministically_deduped() -> None:
    base = _minimal_inputs()
    duplicate = replace(
        base,
        daily_trading=replace(
            base.daily_trading,
            records=base.daily_trading.records + base.daily_trading.records,
        ),
    )
    assert build_market_universe(duplicate).canonical_json() == (
        build_market_universe(base).canonical_json()
    )


def test_conflicting_duplicate_official_rows_fail_closed() -> None:
    base = _minimal_inputs()
    conflicting = replace(
        base,
        daily_trading=replace(
            base.daily_trading,
            records=base.daily_trading.records
            + (DailyTradingRecord("2330", "不同名稱", True),),
        ),
    )
    with pytest.raises(
        UniverseContractError,
        match="STOCK_DAY_ALL has conflicting duplicate rows for 2330",
    ):
        build_market_universe(conflicting)


def test_raw_source_order_changes_do_not_change_output_or_hash() -> None:
    first_inputs = _frozen_inputs()
    reversed_inputs = replace(
        first_inputs,
        listed_identity=replace(
            first_inputs.listed_identity,
            records=tuple(reversed(first_inputs.listed_identity.records)),
        ),
        daily_trading=replace(
            first_inputs.daily_trading,
            records=tuple(reversed(first_inputs.daily_trading.records)),
        ),
        valuation_coverage=replace(
            first_inputs.valuation_coverage,
            records=tuple(reversed(first_inputs.valuation_coverage.records)),
        ),
        classifications=replace(
            first_inputs.classifications,
            records=tuple(reversed(first_inputs.classifications.records)),
        ),
    )
    first = build_market_universe(first_inputs)
    second = build_market_universe(reversed_inputs)
    assert first.members == second.members
    assert first.canonical_json() == second.canonical_json()
    assert first.payload_sha256 == second.payload_sha256


def test_frozen_fixture_canonical_sha256_is_stable() -> None:
    snapshot = build_market_universe(_frozen_inputs())
    assert snapshot.payload_sha256 == FROZEN_SNAPSHOT_SHA256
    assert snapshot.payload_sha256 == hashlib.sha256(
        snapshot.canonical_json().encode("utf-8")
    ).hexdigest()


def test_snapshot_and_nested_contracts_are_immutable() -> None:
    inputs = _minimal_inputs()
    snapshot = build_market_universe(inputs)
    assert isinstance(snapshot.members, tuple)
    assert isinstance(snapshot.members[0].source_evidence, tuple)
    assert isinstance(inputs.listed_identity.records, tuple)
    with pytest.raises(FrozenInstanceError):
        snapshot.universe_count = 0  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        snapshot.members[0].status = (  # type: ignore[misc]
            UniverseMemberStatus.INACTIVE
        )
    with pytest.raises(UniverseContractError, match="immutable tuple"):
        InputDatasetSnapshot(  # type: ignore[arg-type]
            _evidence(IDENTITY_DATASET),
            [ListedIdentityRecord("2330", "台積電", date(1994, 9, 5))],
        )


def test_delisting_history_is_evidence_not_a_symbol_enumerator() -> None:
    result = build_market_universe(
        _minimal_inputs(
            delistings=(DelistingRecord("1234", "歷史公司", date(2020, 1, 1)),),
        )
    )
    assert [item.symbol for item in result.members] == ["2330"]


def test_output_contract_fields_and_hash_only_evidence_are_exact() -> None:
    snapshot = build_market_universe(_minimal_inputs())
    payload = snapshot.as_dict()
    assert set(payload) == {
        "market_date",
        "methodology_version",
        "source_policy",
        "universe_count",
        "scan_eligible_count",
        "scan_unavailable_count",
        "excluded_count",
        "inactive_count",
        "unresolved_count",
        "members",
    }
    assert set(payload["members"][0]) == {
        "symbol",
        "name",
        "market",
        "status",
        "listing_date",
        "delisting_date",
        "exclusion_reason",
        "source_evidence",
    }
    assert {item["dataset"] for item in payload["members"][0]["source_evidence"]} == {
        IDENTITY_DATASET,
        STOCK_DAY_ALL_DATASET,
        VALUATION_DATASET,
        CLASSIFICATION_DATASET,
        DELISTING_DATASET,
    }
    assert all(
        set(item) == set(SOURCE_EVIDENCE_FIELDS)
        for item in payload["members"][0]["source_evidence"]
    )
    assert "raw" not in snapshot.canonical_json().casefold()


def test_source_evidence_rejects_credential_bearing_refs() -> None:
    with pytest.raises(UniverseContractError, match="credential-bearing"):
        SourceEvidence(
            source="twse",
            dataset=IDENTITY_DATASET,
            source_ref="https://openapi.twse.com.tw/data?api_key=secret",
            contract_version="v1",
            payload_sha256="a" * 64,
            payload_size_bytes=1,
            hash_basis="raw-response-bytes-v1",
        )
    with pytest.raises(UniverseContractError, match="TWSE authority"):
        SourceEvidence(
            source="esun",
            dataset=IDENTITY_DATASET,
            source_ref="contract://screener-s1-tests/identity",
            contract_version="v1",
            payload_sha256="a" * 64,
            payload_size_bytes=1,
            hash_basis="canonical-json-v1",
        )


def test_universe_builder_has_no_watchlist_provider_network_or_sqlite_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def blocked(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise AssertionError("pure universe builder attempted external I/O")

    monkeypatch.setattr(urllib.request, "urlopen", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr(sqlite3, "connect", blocked)
    monkeypatch.setattr(provider_manifest_module, "get_provider_manifest", blocked)
    monkeypatch.setattr(provider_manifest_module, "list_provider_manifests", blocked)

    snapshot = build_market_universe(_frozen_inputs())
    assert snapshot.scan_eligible_count == 1_082
    assert "watchlist" not in {item.name for item in fields(MarketUniverseBuildInput)}

    tree = ast.parse(Path(universe_module.__file__).read_text(encoding="utf-8"))
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
                "socket",
                "urllib.request",
            )
        )
        for name in imported
    )


def test_source_policy_and_dataset_roles_are_frozen() -> None:
    inputs = _minimal_inputs()
    assert inputs.methodology_version == UNIVERSE_METHODOLOGY_VERSION
    assert inputs.source_policy == "twse_baseline"
    assert inputs.listed_identity.evidence.dataset == IDENTITY_DATASET
    assert inputs.daily_trading.evidence.dataset == STOCK_DAY_ALL_DATASET
    assert inputs.valuation_coverage.evidence.dataset == VALUATION_DATASET
    with pytest.raises(UniverseContractError, match="twse_baseline"):
        MarketUniverseBuildInput(
            market_date=MARKET_DATE,
            listed_identity=inputs.listed_identity,
            daily_trading=inputs.daily_trading,
            valuation_coverage=inputs.valuation_coverage,
            classifications=inputs.classifications,
            delistings=inputs.delistings,
            source_policy="esun_baseline",
        )
