"""Screener S0 characterization gates over the frozen M1-M9 system.

This module deliberately defines only a test-local candidate output contract.
There is no production screener, acquisition path, persistence adapter, or
migration in S0.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import inspect
import json
import re
import socket
import sqlite3
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.research_dataset as research_dataset_module
import app.sqlite_research_dataset as sqlite_dataset_module
from app.as_of_policy import FrozenAsOfPolicyV1
from app.market_calendar import MarketDayState, TwseMarketCalendar
from app.models import Symbol
from app.pipelines.batch_runner import DailyBatchRunner
from app.pipelines.daily_research import DailyResearchPipeline
from app.pipelines.retry import RetryPolicy
from app.providers.artifacts import source_artifact_from_bytes
from app.providers.manifest import (
    SourceAuthority,
    get_provider_manifest,
    list_provider_manifests,
)
from app.providers.mock import MockMarketDataProvider
from app.reports.composition import compose_sqlite_daily_research_report_service
from app.research_dataset import (
    DatasetDiscrepancy,
    DatasetPrice,
    DatasetProvenance,
    DatasetSourcePolicyError,
    DatasetSymbol,
    DatasetValidationObservation,
    ESUN_VALIDATION_SOURCES,
    FakeResearchDataset,
    ResearchDataset,
    ResearchDatasetRequest,
    TWSE_BASELINE_SOURCE_POLICY,
    TWSE_BASELINE_SOURCES,
    ValidationReadModel,
)
from app.storage import SQLiteResearchRepository
from app.storage.batch_run import BatchRunStatus, SQLiteBatchRunRepository


UTC = timezone.utc
MARKET_DATE = date(2026, 8, 7)
FIXED_TIME = datetime(2026, 8, 7, 10, tzinfo=UTC)
FROZEN_6B_CANONICAL_SHA256 = (
    "8730b97cb0f744d0041f1ffde7cb4baf92848ed7318722326625c2f0e356ac64"
)

SCREENER_FLOW = (
    "universe_snapshot",
    "stage_1_scan",
    "candidate_shortlist",
    "stage_2_research",
    "candidate_report",
)
CANDIDATE_MEANING = "今日出現值得進一步研究的變化"

TOP_LEVEL_FIELDS = frozenset(
    {
        "market_date",
        "methodology_version",
        "source_policy",
        "universe_size",
        "screened_count",
        "triggered_count",
        "candidate_count",
        "candidate_limit",
        "truncated",
        "candidates",
    }
)
CANDIDATE_FIELDS = frozenset(
    {
        "rank",
        "symbol",
        "name",
        "market",
        "also_in_watchlist",
        "data_quality",
        "reasons",
        "metrics",
        "provenance",
    }
)
DATA_QUALITY_FIELDS = frozenset(
    {"status", "price_status", "validation_status", "discrepancies"}
)
METRIC_FIELDS = frozenset(
    {"name", "status", "value", "unit", "as_of_date", "source"}
)
PROVENANCE_FIELDS = frozenset(
    {"source_policy", "canonical_sources", "validation_sources", "artifact_refs"}
)
ARTIFACT_FIELDS = frozenset(
    {"provider", "dataset", "payload_sha256", "hash_basis"}
)

METRIC_STATUSES = frozenset(
    {
        "available",
        "insufficient_history",
        "missing_source",
        "market_date_mismatch",
        "source_discrepancy",
        "not_applicable",
    }
)
PRICE_STATUSES = frozenset(
    {"available", "missing_source", "market_date_mismatch"}
)
VALIDATION_STATUSES = frozenset(
    {
        "available",
        "missing_source",
        "market_date_mismatch",
        "source_discrepancy",
    }
)
DATA_QUALITY_STATUSES = frozenset(
    {"clean", "warning", "blocked", "unavailable"}
)

PROHIBITED_TERMS = (
    "buy",
    "sell",
    "買進",
    "賣出",
    "加碼",
    "減碼",
    "entry",
    "exit",
    "進場",
    "出場",
    "price target",
    "expected return",
    "buy_score",
    "sell_score",
    "score",
    "recommendation",
)


class CandidateContractError(ValueError):
    """The test-only S0 candidate skeleton violates a frozen safety rule."""


@pytest.fixture(autouse=True)
def _forbid_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """S0 tests fail immediately if any code attempts a network call."""

    def blocked(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise AssertionError("S0 characterization must not call the network")

    monkeypatch.setattr(urllib.request, "urlopen", blocked)
    monkeypatch.setattr(socket.socket, "connect", blocked)


def _require_exact_fields(
    value: object,
    expected: frozenset[str],
    path: str,
) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise CandidateContractError(f"{path} must be an object")
    actual = set(value)
    if actual != expected:
        raise CandidateContractError(
            f"{path} fields differ: missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )
    return value


def _normalized_language(value: str) -> str:
    return re.sub(r"[\s_-]+", " ", value.casefold()).strip()


def _assert_no_prohibited_language(value: object, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            _assert_no_prohibited_language(str(key), f"{path}.<key>")
            _assert_no_prohibited_language(item, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_no_prohibited_language(item, f"{path}[{index}]")
        return
    if not isinstance(value, str):
        return

    normalized = _normalized_language(value)
    compact = normalized.replace(" ", "")
    for term in PROHIBITED_TERMS:
        normalized_term = _normalized_language(term)
        if any("\u4e00" <= char <= "\u9fff" for char in normalized_term):
            matched = normalized_term in normalized
        elif " " in normalized_term:
            matched = normalized_term in normalized
        else:
            matched = re.search(
                rf"(?<![a-z]){re.escape(normalized_term)}(?![a-z])",
                normalized,
            ) is not None
        if matched or normalized_term.replace(" ", "") in compact and "_" in term:
            raise CandidateContractError(
                f"{path} contains prohibited candidate language: {term}"
            )


def _validate_metric(metric: object, path: str) -> None:
    item = _require_exact_fields(metric, METRIC_FIELDS, path)
    name = item["name"]
    status = item["status"]
    unit = item["unit"]
    source = item["source"]
    if not isinstance(name, str) or not name.strip():
        raise CandidateContractError(f"{path}.name must not be blank")
    if status not in METRIC_STATUSES:
        raise CandidateContractError(f"{path}.status is unsupported")
    if not isinstance(unit, str) or not unit.strip():
        raise CandidateContractError(f"{path}.unit must not be blank")
    if source not in TWSE_BASELINE_SOURCES:
        raise CandidateContractError(
            f"{path}.source must remain TWSE canonical"
        )
    if status == "available":
        if item["value"] is None or not isinstance(item["as_of_date"], str):
            raise CandidateContractError(
                f"{path} available metric requires value and as_of_date"
            )
        date.fromisoformat(item["as_of_date"])
    elif item["value"] is not None or item["as_of_date"] is not None:
        raise CandidateContractError(
            f"{path} unavailable metric requires null value and as_of_date"
        )


def _validate_provenance(provenance: object, path: str) -> None:
    item = _require_exact_fields(provenance, PROVENANCE_FIELDS, path)
    if item["source_policy"] != TWSE_BASELINE_SOURCE_POLICY:
        raise CandidateContractError(f"{path}.source_policy must be twse_baseline")
    canonical_sources = set(item["canonical_sources"])
    validation_sources = set(item["validation_sources"])
    if not canonical_sources <= TWSE_BASELINE_SOURCES:
        raise CandidateContractError(
            f"{path}.canonical_sources contain a non-TWSE source"
        )
    allowed_validation = TWSE_BASELINE_SOURCES | ESUN_VALIDATION_SOURCES
    if not validation_sources <= allowed_validation:
        raise CandidateContractError(
            f"{path}.validation_sources contain an unknown source"
        )
    artifacts = item["artifact_refs"]
    if not isinstance(artifacts, list):
        raise CandidateContractError(f"{path}.artifact_refs must be an array")
    for index, artifact in enumerate(artifacts):
        artifact_path = f"{path}.artifact_refs[{index}]"
        ref = _require_exact_fields(artifact, ARTIFACT_FIELDS, artifact_path)
        digest = ref["payload_sha256"]
        if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise CandidateContractError(
                f"{artifact_path}.payload_sha256 must be lowercase SHA-256"
            )
        if ref["hash_basis"] not in {
            "raw-response-bytes-v1",
            "canonical-json-v1",
        }:
            raise CandidateContractError(f"{artifact_path}.hash_basis is unsupported")


def _validate_candidate_output(payload: object) -> str:
    """Validate and return deterministic canonical JSON for the S0 skeleton."""

    _assert_no_prohibited_language(payload)
    root = _require_exact_fields(payload, TOP_LEVEL_FIELDS, "$")
    try:
        date.fromisoformat(str(root["market_date"]))
    except ValueError as error:
        raise CandidateContractError("market_date must be ISO date") from error
    if not isinstance(root["methodology_version"], str) or not root[
        "methodology_version"
    ].strip():
        raise CandidateContractError("methodology_version must not be blank")
    if root["source_policy"] != TWSE_BASELINE_SOURCE_POLICY:
        raise CandidateContractError("source_policy must be twse_baseline")

    count_names = (
        "universe_size",
        "screened_count",
        "triggered_count",
        "candidate_count",
        "candidate_limit",
    )
    counts: dict[str, int] = {}
    for name in count_names:
        value = root[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CandidateContractError(f"{name} must be a non-negative integer")
        counts[name] = value
    if counts["candidate_limit"] < 1:
        raise CandidateContractError("candidate_limit must be positive")
    if not (
        counts["candidate_count"]
        <= counts["triggered_count"]
        <= counts["screened_count"]
        <= counts["universe_size"]
    ):
        raise CandidateContractError("candidate counts are inconsistent")
    if counts["candidate_count"] > counts["candidate_limit"]:
        raise CandidateContractError("candidate_count exceeds candidate_limit")
    if not isinstance(root["truncated"], bool):
        raise CandidateContractError("truncated must be boolean")
    if root["truncated"] is (counts["triggered_count"] == counts["candidate_count"]):
        raise CandidateContractError("truncated does not match triggered_count")

    candidates = root["candidates"]
    if not isinstance(candidates, list):
        raise CandidateContractError("candidates must be an array")
    if len(candidates) != counts["candidate_count"]:
        raise CandidateContractError("candidate_count must equal candidates length")
    ranks: list[int] = []
    for index, candidate in enumerate(candidates):
        path = f"$.candidates[{index}]"
        item = _require_exact_fields(candidate, CANDIDATE_FIELDS, path)
        rank = item["rank"]
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 1:
            raise CandidateContractError(f"{path}.rank must be a positive integer")
        ranks.append(rank)
        for name in ("symbol", "name", "market"):
            if not isinstance(item[name], str) or not item[name].strip():
                raise CandidateContractError(f"{path}.{name} must not be blank")
        if not isinstance(item["also_in_watchlist"], bool):
            raise CandidateContractError(
                f"{path}.also_in_watchlist must be a read-only boolean"
            )

        quality = _require_exact_fields(
            item["data_quality"], DATA_QUALITY_FIELDS, f"{path}.data_quality"
        )
        if quality["status"] not in DATA_QUALITY_STATUSES:
            raise CandidateContractError(f"{path}.data_quality.status is unsupported")
        if quality["price_status"] not in PRICE_STATUSES:
            raise CandidateContractError(
                f"{path}.data_quality.price_status is unsupported"
            )
        if quality["validation_status"] not in VALIDATION_STATUSES:
            raise CandidateContractError(
                f"{path}.data_quality.validation_status is unsupported"
            )
        if not isinstance(quality["discrepancies"], list):
            raise CandidateContractError(
                f"{path}.data_quality.discrepancies must be an array"
            )

        reasons = item["reasons"]
        if not isinstance(reasons, list) or not reasons:
            raise CandidateContractError(f"{path}.reasons must not be empty")
        for reason_index, reason in enumerate(reasons):
            if not isinstance(reason, Mapping) or not isinstance(reason.get("code"), str):
                raise CandidateContractError(
                    f"{path}.reasons[{reason_index}] requires a reason code"
                )

        metrics = item["metrics"]
        if not isinstance(metrics, list):
            raise CandidateContractError(f"{path}.metrics must be an array")
        for metric_index, metric in enumerate(metrics):
            _validate_metric(metric, f"{path}.metrics[{metric_index}]")
        _validate_provenance(item["provenance"], f"{path}.provenance")

    if ranks != list(range(1, len(candidates) + 1)):
        raise CandidateContractError(
            "rank must be deterministic, unique, contiguous research priority"
        )
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _valid_candidate_output() -> dict[str, object]:
    return {
        "market_date": MARKET_DATE.isoformat(),
        "methodology_version": "screener-s0-contract-v1",
        "source_policy": "twse_baseline",
        "universe_size": 1_082,
        "screened_count": 1_082,
        "triggered_count": 1,
        "candidate_count": 1,
        "candidate_limit": 30,
        "truncated": False,
        "candidates": [
            {
                "rank": 1,
                "symbol": "2330",
                "name": "台積電",
                "market": "TWSE",
                "also_in_watchlist": True,
                "data_quality": {
                    "status": "warning",
                    "price_status": "available",
                    "validation_status": "source_discrepancy",
                    "discrepancies": [
                        {"field": "volume", "reason": "source_discrepancy"}
                    ],
                },
                "reasons": [
                    {
                        "code": "volume_anomaly",
                        "trigger": "threshold_crossed",
                        "priority": 1,
                        "metric": "volume_ratio_20d",
                    }
                ],
                "metrics": [
                    {
                        "name": "volume_ratio_20d",
                        "status": "available",
                        "value": 2.1,
                        "unit": "ratio",
                        "as_of_date": MARKET_DATE.isoformat(),
                        "source": "twse-historical",
                    },
                    {
                        "name": "volatility_60d",
                        "status": "insufficient_history",
                        "value": None,
                        "unit": "percentage_points",
                        "as_of_date": None,
                        "source": "twse-historical",
                    },
                ],
                "provenance": {
                    "source_policy": "twse_baseline",
                    "canonical_sources": ["twse", "twse-historical"],
                    "validation_sources": ["twse", "esun"],
                    "artifact_refs": [
                        {
                            "provider": "twse",
                            "dataset": "STOCK_DAY_ALL",
                            "payload_sha256": "a" * 64,
                            "hash_basis": "raw-response-bytes-v1",
                        }
                    ],
                },
            }
        ],
    }


def _validation(status: str) -> ValidationReadModel:
    if status == "market_date_mismatch":
        discrepancy = DatasetDiscrepancy(
            "market_date",
            MARKET_DATE.isoformat(),
            (MARKET_DATE - timedelta(days=1)).isoformat(),
            "provider market dates differ",
        )
    else:
        discrepancy = DatasetDiscrepancy(
            "volume",
            1_000,
            1_100,
            "volume differs",
        )
    observations = tuple(
        DatasetValidationObservation(
            symbol="2330",
            provider=provider,
            market_date=MARKET_DATE,
            open=100.0,
            high=102.0,
            low=99.0,
            close=101.0,
            volume=1_000 if provider == "twse" else 1_100,
            source_endpoints=(f"https://evidence.invalid/{provider}",),
            fetched_at=FIXED_TIME,
        )
        for provider in ("twse", "esun")
    )
    return ValidationReadModel(
        symbol="2330",
        status=status,
        run_id=f"validation-{status}",
        target_date=MARKET_DATE,
        left_provider="twse",
        right_provider="esun",
        outcome="discrepancy",
        created_at=FIXED_TIME,
        observations=observations,
        discrepancies=(discrepancy,),
    )


@dataclass(frozen=True)
class _DatedPrice:
    trade_date: date


@dataclass(frozen=True)
class _DatedMetric:
    metric_date: date


@dataclass(frozen=True)
class _Phase6State:
    file_sha256: str
    row_counts: tuple[tuple[str, int], ...]
    schema_sql: tuple[tuple[str, str], ...]


def _phase6_state(database_path: Path) -> _Phase6State:
    tables = (
        "watchlists",
        "watchlist_members",
        "watchlist_revisions",
        "watchlist_revision_members",
        "daily_batch_runs",
        "daily_symbol_runs",
        "pipeline_runs",
        "research_notes",
        "source_artifacts",
        "daily_research_results",
        "daily_research_reports",
    )
    uri = database_path.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        row_counts = tuple(
            (table, int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]))
            for table in tables
        )
        schema_sql = tuple(
            (row[0], row[1] or "")
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index') "
                "ORDER BY name"
            )
        )
    return _Phase6State(
        file_sha256=hashlib.sha256(database_path.read_bytes()).hexdigest(),
        row_counts=row_counts,
        schema_sql=schema_sql,
    )


def test_s0_workflow_candidate_meaning_and_canonical_baseline_are_frozen() -> None:
    assert SCREENER_FLOW == (
        "universe_snapshot",
        "stage_1_scan",
        "candidate_shortlist",
        "stage_2_research",
        "candidate_report",
    )
    assert CANDIDATE_MEANING == "今日出現值得進一步研究的變化"
    assert FROZEN_6B_CANONICAL_SHA256 == (
        "8730b97cb0f744d0041f1ffde7cb4baf92848ed7318722326625c2f0e356ac64"
    )


def test_s0_m7_manifest_and_hash_only_artifact_provenance_are_frozen() -> None:
    manifests = {manifest.source: manifest for manifest in list_provider_manifests()}
    assert manifests["twse"].authority is SourceAuthority.OFFICIAL_EXCHANGE
    assert manifests["twse-historical"].authority is SourceAuthority.OFFICIAL_EXCHANGE
    assert manifests["esun"].read_only is True
    assert manifests["esun-historical"].read_only is True
    assert {dataset.name for dataset in get_provider_manifest("twse").datasets} == {
        "STOCK_DAY_ALL",
        "BWIBBU_ALL",
    }

    body = b'{"Date":"1150807","Code":"2330"}'
    artifact = source_artifact_from_bytes(
        provider="twse",
        dataset="STOCK_DAY_ALL",
        endpoint="https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
        contract_version=manifests["twse"].contract_version,
        body=body,
        headers={"content-type": "application/json"},
        fetched_at=FIXED_TIME,
    )

    assert artifact.payload_sha256 == hashlib.sha256(body).hexdigest()
    assert artifact.payload_size_bytes == len(body)
    assert artifact.hash_basis == "raw-response-bytes-v1"
    assert not hasattr(artifact, "body")
    assert not hasattr(artifact, "headers")


def test_s0_m8_calendar_and_as_of_policy_are_reused_without_new_rules() -> None:
    calendar = TwseMarketCalendar()
    policy = FrozenAsOfPolicyV1()
    saturday = date(2026, 8, 8)

    assert calendar.classify(saturday, MARKET_DATE).state is MarketDayState.WEEKEND
    assert (
        calendar.classify(MARKET_DATE, MARKET_DATE).state
        is MarketDayState.LATEST_ON_REQUESTED
    )
    assert calendar.timezone.key == "Asia/Taipei"
    assert policy.version == "frozen-as-of-v1"

    earlier = _DatedPrice(MARKET_DATE - timedelta(days=1))
    current = _DatedPrice(MARKET_DATE)
    future = _DatedPrice(MARKET_DATE + timedelta(days=1))
    assert policy.prices_as_of((earlier, current, future), MARKET_DATE) == (
        earlier,
        current,
    )
    assert policy.current_price_is_exact_date((earlier, current, future), MARKET_DATE)
    old_metric = _DatedMetric(MARKET_DATE - timedelta(days=2))
    future_metric = _DatedMetric(MARKET_DATE + timedelta(days=1))
    assert policy.select_valuation_as_of(
        (old_metric, future_metric), MARKET_DATE
    ) is old_metric


def test_s0_m9_twse_baseline_preserves_missing_and_discrepancy_semantics() -> None:
    symbol = DatasetSymbol("2330", "台積電", "TWSE", "TWD")
    source_discrepancy = FakeResearchDataset(
        symbols=(symbol,),
        validations=(_validation("source_discrepancy"),),
        provenance=(
            DatasetProvenance(
                symbol="2330",
                validation_sources=("twse", "esun"),
            ),
        ),
    ).read(ResearchDatasetRequest("2330", MARKET_DATE))

    assert source_discrepancy.price_history.status == "missing_source"
    assert source_discrepancy.price_history.current is None
    assert source_discrepancy.price_history.observations == ()
    assert source_discrepancy.validation.status == "source_discrepancy"
    assert source_discrepancy.provenance.canonical_sources == ()
    assert source_discrepancy.provenance.validation_sources == ("esun", "twse")

    mismatch = FakeResearchDataset(
        symbols=(symbol,),
        prices=(
            DatasetPrice(
                symbol="2330",
                trade_date=MARKET_DATE - timedelta(days=1),
                open=100.0,
                high=102.0,
                low=99.0,
                close=101.0,
                volume=1_000,
                source="twse-historical",
            ),
        ),
        validations=(_validation("market_date_mismatch"),),
    ).read(ResearchDatasetRequest("2330", MARKET_DATE))

    assert mismatch.price_history.status == "market_date_mismatch"
    assert mismatch.price_history.current is None
    assert mismatch.validation.status == "market_date_mismatch"
    assert mismatch.validation.discrepancies[0].field == "market_date"


def test_s0_m9_esun_can_validate_but_never_create_canonical_metrics() -> None:
    dataset = FakeResearchDataset(
        symbols=(DatasetSymbol("2330", "台積電", "TWSE", "TWD"),),
        prices=(
            DatasetPrice(
                symbol="2330",
                trade_date=MARKET_DATE,
                open=100.0,
                high=102.0,
                low=99.0,
                close=101.0,
                volume=1_100,
                source="esun",
            ),
        ),
    )

    with pytest.raises(DatasetSourcePolicyError, match="esun"):
        dataset.read(ResearchDatasetRequest("2330", MARKET_DATE))

    payload = _valid_candidate_output()
    _validate_candidate_output(payload)
    candidate = payload["candidates"][0]
    assert isinstance(candidate, dict)
    candidate["metrics"][0]["source"] = "esun"
    with pytest.raises(CandidateContractError, match="TWSE canonical"):
        _validate_candidate_output(payload)


def test_s0_dataset_contract_has_no_provider_network_or_sqlite_write_surface() -> None:
    pure_tree = ast.parse(inspect.getsource(research_dataset_module))
    pure_imports = {
        alias.name
        for node in ast.walk(pure_tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(pure_tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert "sqlite3" not in pure_imports
    assert not any(name.startswith("app.storage") for name in pure_imports)
    assert not any(name.startswith("app.providers") for name in pure_imports)
    assert {
        name
        for name, value in ResearchDataset.__dict__.items()
        if callable(value) and not name.startswith("_")
    } == {"read"}

    sqlite_source = inspect.getsource(sqlite_dataset_module)
    sqlite_tree = ast.parse(sqlite_source)
    sqlite_imports = {
        alias.name
        for node in ast.walk(sqlite_tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(sqlite_tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert not any(name.startswith("app.providers") for name in sqlite_imports)
    assert "?mode=ro" in sqlite_source
    assert "PRAGMA query_only = ON" in sqlite_source
    assert sqlite_dataset_module._SCHEMA_VERSION == 10

    prohibited_sql = ("INSERT ", "UPDATE ", "DELETE ", "REPLACE ", "CREATE ", "ALTER ", "DROP ")
    for node in ast.walk(sqlite_tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert not node.value.lstrip().upper().startswith(prohibited_sql)


def test_s0_candidate_output_skeleton_is_deterministic_and_source_bound() -> None:
    payload = _valid_candidate_output()
    first = _validate_candidate_output(payload)
    reordered = dict(reversed(list(copy.deepcopy(payload).items())))
    second = _validate_candidate_output(reordered)

    assert first == second
    assert hashlib.sha256(first.encode("utf-8")).hexdigest() == hashlib.sha256(
        second.encode("utf-8")
    ).hexdigest()
    assert set(payload) == TOP_LEVEL_FIELDS
    candidate = payload["candidates"][0]
    assert set(candidate) == CANDIDATE_FIELDS
    assert "watchlist_id" not in candidate
    assert "watchlist_revision_id" not in candidate
    assert candidate["rank"] == 1
    assert candidate["also_in_watchlist"] is True


@pytest.mark.parametrize("term", PROHIBITED_TERMS)
def test_s0_candidate_contract_rejects_recommendation_or_trade_language(
    term: str,
) -> None:
    payload = _valid_candidate_output()
    payload["candidates"][0]["reasons"][0]["code"] = term

    with pytest.raises(CandidateContractError, match="prohibited candidate language"):
        _validate_candidate_output(payload)


@pytest.mark.parametrize(
    "mutation",
    ("missing_value", "zero_value", "missing_unit", "missing_as_of_date"),
)
def test_s0_unavailable_metric_requires_status_null_value_unit_and_as_of_date(
    mutation: str,
) -> None:
    payload = _valid_candidate_output()
    metric = payload["candidates"][0]["metrics"][1]
    if mutation == "missing_value":
        del metric["value"]
    elif mutation == "zero_value":
        metric["value"] = 0
    elif mutation == "missing_unit":
        del metric["unit"]
    else:
        del metric["as_of_date"]

    with pytest.raises(CandidateContractError):
        _validate_candidate_output(payload)


def test_s0_candidate_failure_cannot_mutate_phase6_watchlist_loop(tmp_path: Path) -> None:
    database_path = tmp_path / "s0-phase6-isolation.db"
    repository = SQLiteResearchRepository(database_path)
    repository.initialize()
    repository.upsert_symbol(
        Symbol("MOCK1", "Synthetic MOCK1", "MOCK", "TWD")
    )
    batch_repository = SQLiteBatchRunRepository(repository)
    batch_repository.initialize()
    watchlist_id = batch_repository.get_or_create_watchlist("fixed-research")
    batch_repository.set_watchlist_members(watchlist_id, ("MOCK1",))

    pipeline = DailyResearchPipeline(
        MockMarketDataProvider(),
        repository,
        retry_policy=RetryPolicy(max_attempts=1, initial_backoff_seconds=0),
        sleep=lambda _: None,
        clock=lambda: FIXED_TIME,
    )
    batch = DailyBatchRunner(
        batch_repository,
        pipeline,
        "fixed-research",
        latest_market_date_fn=lambda: MARKET_DATE,
        clock=lambda: FIXED_TIME,
        sleep=lambda _: None,
    ).run(MARKET_DATE)
    assert batch.batch_status is BatchRunStatus.SUCCESS
    report = compose_sqlite_daily_research_report_service(
        repository,
        clock=lambda: FIXED_TIME,
    ).generate_for_batch_symbol(batch.batch_run_id, "MOCK1")
    assert report.canonical.result_status == "success"
    assert report.report.report_status == "rendered"

    before = _phase6_state(database_path)
    assert dict(before.row_counts)["watchlist_members"] == 1
    assert dict(before.row_counts)["daily_batch_runs"] == 1
    assert dict(before.row_counts)["daily_research_results"] == 1
    assert dict(before.row_counts)["daily_research_reports"] == 1

    invalid_candidate = _valid_candidate_output()
    invalid_candidate["candidates"][0]["metrics"][1]["value"] = 0
    with pytest.raises(CandidateContractError):
        _validate_candidate_output(invalid_candidate)

    after = _phase6_state(database_path)
    assert after == before
