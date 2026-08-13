from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.deployment import daily_preparation as preparation
from app.deployment.composition import (
    ProductionCompositionError,
    SQLiteScreenerResearchDataset,
)
from app.models import DailyPrice, PipelineRunStatus, Symbol
from app.providers.base import ProviderInvalidPayloadError, ProviderTemporaryError
from app.providers.twse import (
    BWIBBU_ALL_URL,
    STOCK_DAY_ALL_URL,
    TwseHttpResponse,
)
from app.research_dataset import ResearchDatasetRequest
from app.screener.universe import (
    CLASSIFICATION_DATASET,
    DELISTING_DATASET,
    IDENTITY_DATASET,
    STOCK_DAY_ALL_DATASET,
    UNIVERSE_METHODOLOGY_VERSION,
    VALUATION_DATASET,
    MarketUniverseMember,
    MarketUniverseSnapshot,
    SourceEvidence,
    UniverseMemberStatus,
)
from app.storage import SQLiteResearchRepository
from app.storage.dataset_versions import DatasetVersionMigrationRunner
from app.storage.screener_migration import SQLiteScreenerMigrationRunner


BASE_DATE = date(2026, 8, 7)
TARGET_DATE = date(2026, 8, 10)


def _sha(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _evidence(dataset: str, source_ref: str) -> SourceEvidence:
    return SourceEvidence(
        source="twse",
        dataset=dataset,
        source_ref=source_ref,
        contract_version="s6e1-test-v1",
        payload_sha256=_sha(dataset),
        payload_size_bytes=len(dataset),
        hash_basis="canonical-json-v1",
    )


def _base_snapshot(*, symbols: tuple[str, ...] = ("2330",)) -> MarketUniverseSnapshot:
    evidence = tuple(
        sorted(
            (
                _evidence(IDENTITY_DATASET, preparation.IDENTITY_URL),
                _evidence(STOCK_DAY_ALL_DATASET, STOCK_DAY_ALL_URL),
                _evidence(VALUATION_DATASET, BWIBBU_ALL_URL),
                _evidence(
                    CLASSIFICATION_DATASET,
                    "contract://market-screener/s1/ordinary-stock-classification-v1",
                ),
                _evidence(DELISTING_DATASET, preparation.DELISTING_URL),
            ),
            key=lambda item: item.dataset,
        )
    )
    members = tuple(
        MarketUniverseMember(
            symbol=symbol,
            name={"2330": "台積電", "2317": "鴻海"}.get(symbol, symbol),
            market="TWSE",
            status=UniverseMemberStatus.ACTIVE_SCAN_ELIGIBLE,
            listing_date=date(1994, 9, 5),
            delisting_date=None,
            exclusion_reason=None,
            source_evidence=evidence,
        )
        for symbol in sorted(symbols)
    )
    return MarketUniverseSnapshot(
        market_date=BASE_DATE,
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


def _stock_row(
    symbol: str = "2330",
    name: str = "台積電",
    *,
    target: date = TARGET_DATE,
    available: bool = True,
) -> dict[str, str]:
    roc = f"{target.year - 1911:03d}{target:%m%d}"
    values = ("200.00", "205.00", "195.00", "202.00") if available else ("0",) * 4
    return {
        "Date": roc,
        "Code": symbol,
        "Name": name,
        "TradeVolume": "1000000",
        "TradeValue": "200000000",
        "OpeningPrice": values[0],
        "HighestPrice": values[1],
        "LowestPrice": values[2],
        "ClosingPrice": values[3],
        "Change": "100.00" if available else "0",
        "Transaction": "1000",
    }


def _metric_row(
    symbol: str = "2330",
    name: str = "台積電",
    *,
    target: date = TARGET_DATE,
) -> dict[str, str]:
    roc = f"{target.year - 1911:03d}{target:%m%d}"
    return {
        "Date": roc,
        "Code": symbol,
        "Name": name,
        "PEratio": "20.0",
        "DividendYield": "2.0",
        "PBratio": "5.0",
    }


def _identity_row(
    symbol: str = "2330",
    name: str = "台積電",
) -> dict[str, str]:
    return {
        "出表日期": "1150810",
        "公司代號": symbol,
        "公司名稱": name + "股份有限公司",
        "公司簡稱": name,
        "上市日期": "19940905",
    }


class _Transport:
    def __init__(
        self,
        responses: dict[str, TwseHttpResponse],
    ) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, *, timeout_seconds: float) -> TwseHttpResponse:
        assert timeout_seconds > 0
        self.calls.append(url)
        return self.responses[url]


def _response(payload: object, *, status: int = 200) -> TwseHttpResponse:
    return TwseHttpResponse(
        status_code=status,
        body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"content-type": "application/json; charset=utf-8"},
    )


def _transport(
    *,
    target: date = TARGET_DATE,
    include_unavailable: bool = False,
    stock_status: int = 200,
) -> _Transport:
    stocks = [_stock_row(target=target)]
    metrics = [_metric_row(target=target)]
    identities = [_identity_row()]
    if include_unavailable:
        stocks.append(_stock_row("2317", "鴻海", target=target, available=False))
        metrics.append(_metric_row("2317", "鴻海", target=target))
        identities.append(_identity_row("2317", "鴻海"))
    return _Transport(
        {
            STOCK_DAY_ALL_URL: _response(stocks, status=stock_status),
            BWIBBU_ALL_URL: _response(metrics),
            preparation.IDENTITY_URL: _response(identities),
            preparation.DELISTING_URL: _response(
                [{"Code": "9999", "Company": "歷史公司", "DelistingDate": "090/01/01"}]
            ),
        }
    )


def _database(tmp_path: Path, *, history_rows: int = 250) -> Path:
    database = (tmp_path / "research.db").resolve()
    repository = SQLiteResearchRepository(database)
    repository.initialize()
    SQLiteScreenerMigrationRunner(database).migrate()
    repository.upsert_symbol(Symbol("2330", "台積電", "TWSE", "TWD"))
    start = TARGET_DATE - timedelta(days=history_rows)
    repository.upsert_daily_prices(
        tuple(
            DailyPrice(
                symbol="2330",
                trade_date=start + timedelta(days=index),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.0,
                volume=100_000,
                source="twse",
            )
            for index in range(history_rows)
        )
    )
    return database


def _config(tmp_path: Path, database: Path | None = None) -> preparation.DailyPreparationConfiguration:
    base = (tmp_path / "base.json").resolve()
    base.write_text(
        json.dumps(_base_snapshot().as_dict(), ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    return preparation.DailyPreparationConfiguration(
        database_path=database or (tmp_path / "research.db").resolve(),
        base_universe_snapshot_path=base,
        artifact_directory=(tmp_path / "universe").resolve(),
        evidence_path=(tmp_path / "runtime" / "evidence.json").resolve(),
        historical_cadence_seconds=0,
    )


def test_01_configuration_requires_absolute_paths(tmp_path: Path) -> None:
    with pytest.raises(preparation.DailyPreparationError, match="absolute"):
        preparation.DailyPreparationConfiguration(
            database_path=Path("research.db"),
            base_universe_snapshot_path=(tmp_path / "base.json").resolve(),
            artifact_directory=(tmp_path / "artifacts").resolve(),
            evidence_path=(tmp_path / "evidence.json").resolve(),
        )


def test_02_candidate_limit_remains_frozen(tmp_path: Path) -> None:
    with pytest.raises(preparation.DailyPreparationError, match="frozen"):
        preparation.DailyPreparationConfiguration(
            database_path=(tmp_path / "research.db").resolve(),
            base_universe_snapshot_path=(tmp_path / "base.json").resolve(),
            artifact_directory=(tmp_path / "artifacts").resolve(),
            evidence_path=(tmp_path / "evidence.json").resolve(),
            candidate_limit=29,
        )


def test_03_environment_requires_explicit_base_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(preparation.BASE_UNIVERSE_PATH_ENV, raising=False)
    with pytest.raises(preparation.DailyPreparationError, match="BASE_UNIVERSE"):
        preparation.DailyPreparationConfiguration.from_environment(
            (tmp_path / "research.db").resolve()
        )


def test_04_cache_reuses_successful_response() -> None:
    delegate = _transport()
    cache = preparation.CachingTwseTransport(delegate)
    first = cache.get(STOCK_DAY_ALL_URL, timeout_seconds=1)
    second = cache.get(STOCK_DAY_ALL_URL, timeout_seconds=1)
    assert first is second
    assert delegate.calls == [STOCK_DAY_ALL_URL]
    assert cache.network_requests == 1


def test_05_cache_does_not_reuse_failed_response() -> None:
    delegate = _transport(stock_status=503)
    cache = preparation.CachingTwseTransport(delegate)
    cache.get(STOCK_DAY_ALL_URL, timeout_seconds=1)
    cache.get(STOCK_DAY_ALL_URL, timeout_seconds=1)
    assert delegate.calls == [STOCK_DAY_ALL_URL, STOCK_DAY_ALL_URL]


def test_05b_daily_preparation_accepts_additive_v12_schema(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path)
    DatasetVersionMigrationRunner(database).migrate(isolated=True)
    preparation.ProductionDailyDataPreparer(_config(tmp_path, database))._require_v11()


def test_06_paced_history_provider_enforces_cadence() -> None:
    clock_values = iter((1.0, 1.2, 2.0))
    sleeps: list[float] = []

    class Delegate:
        source = "twse-historical"

        def fetch_market_data(self, *_args: object, **_kwargs: object) -> str:
            return "ok"

    provider = preparation.PacedHistoricalProvider(
        Delegate(),  # type: ignore[arg-type]
        cadence_seconds=1.0,
        sleep=sleeps.append,
        clock=lambda: next(clock_values),
    )
    assert provider.fetch_market_data("2330", BASE_DATE, BASE_DATE, timeout_seconds=1) == "ok"
    assert provider.fetch_market_data("2330", BASE_DATE, BASE_DATE, timeout_seconds=1) == "ok"
    assert sleeps == [pytest.approx(0.8)]


def test_07_stable_input_reconstruction_preserves_frozen_methodology() -> None:
    inputs = preparation._stable_inputs_from_snapshot(_base_snapshot())
    assert inputs.methodology_version == UNIVERSE_METHODOLOGY_VERSION
    assert inputs.source_policy == "twse_baseline"
    assert inputs.classifications.records[0].classification.value == "common_equity"


def test_08_target_universe_is_rebuilt_with_new_identity(tmp_path: Path) -> None:
    config = _config(tmp_path)
    preparer = preparation.ProductionDailyDataPreparer(config)
    transport = preparation.CachingTwseTransport(_transport())
    snapshot = preparer._acquire_universe(
        preparation.TwseMarketDataProvider(transport=transport),
        _base_snapshot(),
        TARGET_DATE,
    )
    assert snapshot.market_date == TARGET_DATE
    assert snapshot.payload_sha256 != _base_snapshot().payload_sha256
    assert snapshot.scan_eligible_count == 1


def test_09_unavailable_ohlc_uses_frozen_unavailable_semantics(tmp_path: Path) -> None:
    config = _config(tmp_path)
    preparer = preparation.ProductionDailyDataPreparer(config)
    snapshot = preparer._acquire_universe(
        preparation.TwseMarketDataProvider(
            transport=preparation.CachingTwseTransport(
                _transport(include_unavailable=True)
            )
        ),
        _base_snapshot(symbols=("2330", "2317")),
        TARGET_DATE,
    )
    member = next(item for item in snapshot.members if item.symbol == "2317")
    assert member.status is UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE
    assert member.exclusion_reason == "stock_day_all_price_unavailable"


def test_10_official_date_mismatch_never_falls_back(tmp_path: Path) -> None:
    config = _config(tmp_path)
    preparer = preparation.ProductionDailyDataPreparer(config)
    with pytest.raises(preparation.DailyPreparationError, match="formal latest"):
        preparer._acquire_universe(
            preparation.TwseMarketDataProvider(
                transport=preparation.CachingTwseTransport(
                    _transport(target=BASE_DATE)
                )
            ),
            _base_snapshot(),
            TARGET_DATE,
        )


def test_11_provider_failure_fails_closed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    preparer = preparation.ProductionDailyDataPreparer(config, sleep=lambda _seconds: None)
    with pytest.raises(ProviderTemporaryError):
        preparer._acquire_universe(
            preparation.TwseMarketDataProvider(
                transport=preparation.CachingTwseTransport(
                    _transport(stock_status=503)
                )
            ),
            _base_snapshot(),
            TARGET_DATE,
        )


def test_12_preparation_requires_schema_v11(tmp_path: Path) -> None:
    database = (tmp_path / "research.db").resolve()
    SQLiteResearchRepository(database).initialize()
    config = _config(tmp_path, database)
    with pytest.raises(preparation.DailyPreparationError, match="v11"):
        preparation.ProductionDailyDataPreparer(config).prepare(TARGET_DATE)


def test_13_newer_date_automatically_prepares_stage1_inputs(tmp_path: Path) -> None:
    database = _database(tmp_path)
    config = _config(tmp_path, database)
    transports: list[preparation.CachingTwseTransport] = []

    def factory() -> preparation.CachingTwseTransport:
        item = preparation.CachingTwseTransport(_transport())
        transports.append(item)
        return item

    result = preparation.ProductionDailyDataPreparer(
        config,
        transport_factory=factory,
    ).prepare(TARGET_DATE)
    assert result.status == "prepared"
    assert result.stage1_ready == 1
    assert result.new_pipeline_runs == 1
    assert result.network_requests == 4
    assert len(transports) == 1


def test_14_prepared_replay_avoids_all_provider_refetch(tmp_path: Path) -> None:
    database = _database(tmp_path)
    config = _config(tmp_path, database)
    calls = 0

    def factory() -> preparation.CachingTwseTransport:
        nonlocal calls
        calls += 1
        return preparation.CachingTwseTransport(_transport())

    preparer = preparation.ProductionDailyDataPreparer(
        config,
        transport_factory=factory,
    )
    first = preparer.prepare(TARGET_DATE)
    second = preparer.prepare(TARGET_DATE)
    assert first.status == "prepared"
    assert second.status == "prepared_replay"
    assert second.network_requests == 0
    assert second.provider_symbol_calls == 0
    assert calls == 1


def test_15_preparation_keeps_migration_highest_at_11(tmp_path: Path) -> None:
    database = _database(tmp_path)
    config = _config(tmp_path, database)
    preparation.ProductionDailyDataPreparer(
        config,
        transport_factory=lambda: preparation.CachingTwseTransport(_transport()),
    ).prepare(TARGET_DATE)
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("select max(version) from schema_migrations").fetchone()[0] == 11
        assert connection.execute("select count(1) from schema_migrations where version=12").fetchone()[0] == 0
    finally:
        connection.close()


def test_16_dataset_merges_prior_history_with_new_daily_owner() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        "create table historical_source_observations (historical_run_id text, provider text, "
        "symbol text, trade_date text, open_price real, high_price real, low_price real, "
        "close_price real, volume integer);"
        "create table daily_prices (symbol text, trade_date text, open_price real, "
        "high_price real, low_price real, close_price real, volume integer, source text);"
    )
    connection.execute(
        "insert into historical_source_observations values (?,?,?,?,?,?,?,?,?)",
        ("h", "twse-historical", "2330", "2026-08-07", 100, 101, 99, 100, 10),
    )
    connection.execute(
        "insert into daily_prices values (?,?,?,?,?,?,?,?)",
        ("2330", "2026-08-10", 200, 205, 195, 202, 20, "twse"),
    )
    request = ResearchDatasetRequest(
        "2330", TARGET_DATE, historical_run_id="h"
    )
    prices = SQLiteScreenerResearchDataset._read_prices(connection, request)
    assert [item.trade_date for item in prices] == [BASE_DATE, TARGET_DATE]


def test_17_dataset_overlap_disagreement_fails_closed() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        "create table historical_source_observations (historical_run_id text, provider text, "
        "symbol text, trade_date text, open_price real, high_price real, low_price real, "
        "close_price real, volume integer);"
        "create table daily_prices (symbol text, trade_date text, open_price real, "
        "high_price real, low_price real, close_price real, volume integer, source text);"
    )
    connection.execute(
        "insert into historical_source_observations values (?,?,?,?,?,?,?,?,?)",
        ("h", "twse-historical", "2330", "2026-08-10", 100, 101, 99, 100, 10),
    )
    connection.execute(
        "insert into daily_prices values (?,?,?,?,?,?,?,?)",
        ("2330", "2026-08-10", 200, 205, 195, 202, 20, "twse"),
    )
    with pytest.raises(ProductionCompositionError, match="disagrees"):
        SQLiteScreenerResearchDataset._read_prices(
            connection,
            ResearchDatasetRequest("2330", TARGET_DATE, historical_run_id="h"),
        )


def test_18_candidate_stage2_reuses_complete_250_day_window(tmp_path: Path) -> None:
    database = _database(tmp_path)
    snapshot = SimpleNamespace(
        price_history=SimpleNamespace(
            status="available",
            observations=tuple(range(250)),
        )
    )
    base = SimpleNamespace(read=lambda _request: snapshot)
    dataset = preparation.CandidatePreparingDataset(
        database,
        base,  # type: ignore[arg-type]
        cadence_seconds=0,
        timeout_seconds=1,
    )
    assert dataset.read(ResearchDatasetRequest("2330", TARGET_DATE, 250)) is snapshot
    assert dataset.evidence()["reused_candidates"] == ["2330"]
    assert dataset.evidence()["updated_candidates"] == []


def test_19_candidate_stage2_prepares_only_requested_candidate(tmp_path: Path) -> None:
    database = _database(tmp_path, history_rows=62)
    short = SimpleNamespace(
        price_history=SimpleNamespace(status="available", observations=tuple(range(62)))
    )
    full = SimpleNamespace(
        price_history=SimpleNamespace(status="available", observations=tuple(range(250)))
    )
    reads = 0

    def read(_request: object) -> object:
        nonlocal reads
        reads += 1
        return short if reads == 1 else full

    base = SimpleNamespace(read=read)
    dataset = preparation.CandidatePreparingDataset(
        database,
        base,  # type: ignore[arg-type]
        cadence_seconds=0,
        timeout_seconds=1,
    )
    dataset.pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: SimpleNamespace(
            run_status=PipelineRunStatus.SUCCESS
        )
    )
    assert dataset.read(ResearchDatasetRequest("2330", TARGET_DATE, 250)) is full
    assert dataset.evidence()["requested_candidates"] == ["2330"]
    assert dataset.evidence()["updated_candidates"] == ["2330"]


def test_20_future_date_production_entrypoint_prepares_then_runs_s5(tmp_path: Path) -> None:
    database = _database(tmp_path)
    config = _config(tmp_path, database)
    composed = preparation.PreparedProductionS5(config)
    transports = 0

    def factory() -> preparation.CachingTwseTransport:
        nonlocal transports
        transports += 1
        return preparation.CachingTwseTransport(_transport())

    composed.preparer = preparation.ProductionDailyDataPreparer(
        config,
        transport_factory=factory,
    )
    first = composed(TARGET_DATE)
    second = composed(TARGET_DATE)
    evidence = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    assert first.status == "success"
    assert first.replayed is False
    assert second.status == "success"
    assert second.replayed is True
    assert evidence["preparation"]["status"] == "prepared_replay"
    assert transports == 1


def test_21_factory_requires_frozen_formal_s5_locator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    monkeypatch.setenv(preparation.BASE_UNIVERSE_PATH_ENV, str(config.base_universe_snapshot_path))
    monkeypatch.setenv(preparation.ARTIFACT_DIRECTORY_ENV, str(config.artifact_directory))
    monkeypatch.setenv(preparation.EVIDENCE_PATH_ENV, str(config.evidence_path))
    monkeypatch.setenv(preparation.FROZEN_S5_FACTORY_ENV, "wrong:factory")
    with pytest.raises(preparation.DailyPreparationError, match="frozen"):
        preparation.create_prepared_s5(config.database_path)


def test_22_nonsecret_evidence_contains_no_credentials(tmp_path: Path) -> None:
    database = _database(tmp_path)
    config = _config(tmp_path, database)
    composed = preparation.PreparedProductionS5(config)
    composed.preparer = preparation.ProductionDailyDataPreparer(
        config,
        transport_factory=lambda: preparation.CachingTwseTransport(_transport()),
    )
    composed(TARGET_DATE)
    rendered = config.evidence_path.read_text(encoding="utf-8").casefold()
    assert "api_key" not in rendered
    assert "authorization" not in rendered
    assert "password" not in rendered


def test_23_official_metadata_temporary_failure_uses_bounded_retry(
    tmp_path: Path,
) -> None:
    attempts = 0
    sleeps: list[float] = []

    class TemporaryTransport:
        def get(self, url: str, *, timeout_seconds: float) -> TwseHttpResponse:
            nonlocal attempts
            assert url == preparation.IDENTITY_URL
            assert timeout_seconds > 0
            attempts += 1
            if attempts < 3:
                raise ProviderTemporaryError("temporary")
            return _response([_identity_row()])

    preparer = preparation.ProductionDailyDataPreparer(
        _config(tmp_path),
        sleep=sleeps.append,
    )
    payload, _response_value = preparer._request_official_array(
        TemporaryTransport(),  # type: ignore[arg-type]
        preparation.IDENTITY_URL,
    )
    assert payload == [_identity_row()]
    assert attempts == 3
    assert len(sleeps) == 2


def test_24_failed_preparation_evidence_is_closed_and_finished(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    composed = preparation.PreparedProductionS5(config)

    class FailedPreparer:
        def prepare(self, _target: date) -> None:
            raise preparation.DailyPreparationError("closed")

    composed.preparer = FailedPreparer()  # type: ignore[assignment]
    with pytest.raises(preparation.DailyPreparationError, match="closed"):
        composed(TARGET_DATE)
    payload = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["error_type"] == "DailyPreparationError"
    assert payload["finished_at"]


def test_25_candidate_legal_short_history_reuses_verified_listing_boundary(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path, history_rows=2)
    listing_date = date(2025, 11, 19)
    observations = (
        SimpleNamespace(trade_date=listing_date),
        SimpleNamespace(trade_date=TARGET_DATE),
    )
    snapshot = SimpleNamespace(
        price_history=SimpleNamespace(
            status="available",
            observations=observations,
        ),
        provenance=SimpleNamespace(
            canonical_sources=("twse", "twse-historical"),
        ),
    )
    requests: list[ResearchDatasetRequest] = []

    def read(request: ResearchDatasetRequest) -> object:
        requests.append(request)
        return snapshot

    dataset = preparation.CandidatePreparingDataset(
        database,
        SimpleNamespace(read=read),  # type: ignore[arg-type]
        cadence_seconds=0,
        timeout_seconds=1,
        listing_dates={"2330": listing_date},
    )
    dataset._historical_run = lambda *_args: {  # type: ignore[method-assign]
        "run_id": "historical-short",
        "target_date": TARGET_DATE.isoformat(),
        "status": PipelineRunStatus.FAILED.value,
        "next_month": "2025-10-01",
        "months_completed": 10,
        "observation_count": 2,
        "first_trade_date": listing_date.isoformat(),
        "last_trade_date": TARGET_DATE.isoformat(),
        "error_message": (
            "ProviderInvalidRequestError: TWSE historical endpoint returned "
            "no usable data"
        ),
        "source_row_count": 2,
        "artifact_month_count": 10,
        "pipeline_run_id": "daily-target",
    }
    dataset.pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: pytest.fail("legal short must not refetch")
    )
    assert dataset.read(ResearchDatasetRequest("2330", TARGET_DATE, 250)) is snapshot
    assert requests[-1].historical_run_id == "historical-short"
    assert requests[-1].pipeline_run_id == "daily-target"
    assert dataset.evidence()["legal_short_candidates"] == ["2330"]
    assert dataset.evidence()["legal_short_observations"] == {"2330": 2}


def test_26_future_date_reuses_prior_legal_short_without_history_refetch(
    tmp_path: Path,
) -> None:
    database = _database(tmp_path, history_rows=3)
    listing_date = date(2025, 11, 19)
    future_date = TARGET_DATE + timedelta(days=1)
    observations = (
        SimpleNamespace(trade_date=listing_date),
        SimpleNamespace(trade_date=TARGET_DATE),
        SimpleNamespace(trade_date=future_date),
    )
    snapshot = SimpleNamespace(
        price_history=SimpleNamespace(
            status="available",
            observations=observations,
        ),
        provenance=SimpleNamespace(
            canonical_sources=("twse", "twse-historical"),
        ),
    )
    dataset = preparation.CandidatePreparingDataset(
        database,
        SimpleNamespace(read=lambda _request: snapshot),  # type: ignore[arg-type]
        cadence_seconds=0,
        timeout_seconds=1,
        listing_dates={"2330": listing_date},
    )
    dataset._historical_run = lambda *_args: {  # type: ignore[method-assign]
        "run_id": "prior-legal-short",
        "target_date": TARGET_DATE.isoformat(),
        "status": PipelineRunStatus.FAILED.value,
        "next_month": "2025-10-01",
        "months_completed": 10,
        "observation_count": 2,
        "first_trade_date": listing_date.isoformat(),
        "last_trade_date": TARGET_DATE.isoformat(),
        "error_message": (
            "ProviderInvalidRequestError: TWSE historical endpoint returned "
            "no usable data"
        ),
        "source_row_count": 2,
        "artifact_month_count": 10,
        "pipeline_run_id": "future-daily-target",
    }
    dataset.pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: pytest.fail("future legal short must replay")
    )
    result = dataset.read(ResearchDatasetRequest("2330", future_date, 250))
    assert result is snapshot
    assert dataset.evidence()["legal_short_observations"] == {"2330": 3}


def test_27_new_official_identity_without_daily_row_is_unavailable_not_unresolved(
    tmp_path: Path,
) -> None:
    transport = _transport()
    transport.responses[preparation.IDENTITY_URL] = _response(
        [_identity_row(), _identity_row("2317", "鴻海")]
    )
    snapshot = preparation.ProductionDailyDataPreparer(
        _config(tmp_path)
    )._acquire_universe(
        preparation.TwseMarketDataProvider(
            transport=preparation.CachingTwseTransport(transport)
        ),
        _base_snapshot(),
        TARGET_DATE,
    )
    member = next(item for item in snapshot.members if item.symbol == "2317")
    assert member.status is UniverseMemberStatus.ACTIVE_SCAN_UNAVAILABLE
    assert snapshot.unresolved_count == 0


def test_28_partial_s5_result_is_not_recorded_as_preparation_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _database(tmp_path)
    config = _config(tmp_path, database)
    composed = preparation.PreparedProductionS5(config)
    composed.preparer = preparation.ProductionDailyDataPreparer(
        config,
        transport_factory=lambda: preparation.CachingTwseTransport(_transport()),
    )
    partial = SimpleNamespace(
        status="partial_success",
        replayed=False,
        screener_run_id="a" * 64,
        canonical_sha256=None,
    )
    monkeypatch.setattr(
        preparation,
        "_compose_s5",
        lambda *_args: lambda _target: partial,
    )
    assert composed(TARGET_DATE) is partial
    payload = json.loads(config.evidence_path.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["failure_boundary"] == "frozen_s5_non_success"


def test_29_enabled_candidate_preparation_updates_both_sources_then_validates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _database(tmp_path)
    esun_config = (tmp_path / "esun.ini").resolve()
    esun_config.write_text("fixture", encoding="utf-8")

    class FakeEsunProvider:
        source = "esun-historical"

    monkeypatch.setattr(
        preparation,
        "EsunHistoricalMarketDataProvider",
        lambda **_kwargs: FakeEsunProvider(),
    )
    snapshot = SimpleNamespace(
        price_history=SimpleNamespace(
            status="available",
            observations=tuple(range(250)),
        )
    )
    dataset = preparation.CandidatePreparingDataset(
        database,
        SimpleNamespace(read=lambda _request: snapshot),  # type: ignore[arg-type]
        cadence_seconds=0,
        timeout_seconds=1,
        esun_config_path=esun_config,
    )
    twse_result = SimpleNamespace(
        run_status=PipelineRunStatus.SUCCESS,
        run_id="twse-run",
        idempotent_replay=False,
    )
    esun_result = SimpleNamespace(
        run_status=PipelineRunStatus.SUCCESS,
        run_id="esun-run",
        idempotent_replay=False,
    )
    validation_result = SimpleNamespace(
        run_id="validation-run",
        idempotent_replay=False,
        outcome=SimpleNamespace(value="match"),
        common_date_count=250,
        matched_date_count=250,
        left_only_date_count=0,
        right_only_date_count=0,
        field_discrepancy_count=0,
        left_latest_date=TARGET_DATE,
        right_latest_date=TARGET_DATE,
        left_sync=twse_result,
        right_sync=esun_result,
    )
    calls: list[str] = []
    dataset.esun_pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: calls.append("esun") or esun_result
    )
    dataset.pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: calls.append("twse") or twse_result
    )
    dataset.historical_validation_pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: calls.append("validation")
        or validation_result
    )  # type: ignore[assignment]

    assert dataset.read(ResearchDatasetRequest("2330", TARGET_DATE, 250)) is snapshot
    assert calls == ["esun", "twse", "validation"]
    evidence = dataset.evidence()
    assert evidence["esun_historical"]["canonical_write"] is False  # type: ignore[index]
    assert evidence["esun_historical"]["records"]["2330"]["run_id"] == "esun-run"  # type: ignore[index]
    assert evidence["historical_validation"]["records"]["2330"]["outcome"] == "match"  # type: ignore[index]


def test_30_complete_twse_candidate_continues_when_esun_validation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _database(tmp_path)
    esun_config = (tmp_path / "esun.ini").resolve()
    esun_config.write_text("fixture", encoding="utf-8")

    class FakeEsunProvider:
        source = "esun-historical"

    monkeypatch.setattr(
        preparation,
        "EsunHistoricalMarketDataProvider",
        lambda **_kwargs: FakeEsunProvider(),
    )
    snapshot = SimpleNamespace(
        price_history=SimpleNamespace(
            status="available",
            observations=tuple(range(250)),
        )
    )
    dataset = preparation.CandidatePreparingDataset(
        database,
        SimpleNamespace(read=lambda _request: snapshot),  # type: ignore[arg-type]
        cadence_seconds=0,
        timeout_seconds=1,
        esun_config_path=esun_config,
    )
    dataset.esun_pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ProviderTemporaryError("temporary E.SUN outage")
        )
    )
    dataset.pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: SimpleNamespace(
            run_status=PipelineRunStatus.SUCCESS,
            run_id="twse-run",
            idempotent_replay=False,
        )
    )
    assert dataset.read(ResearchDatasetRequest("2330", TARGET_DATE, 250)) is snapshot
    evidence = dataset.evidence()
    record = evidence["esun_historical"]["records"]["2330"]  # type: ignore[index]
    assert record["status"] == "failed"
    assert record["error_type"] == "ProviderTemporaryError"
    validation = evidence["historical_validation"]["records"]["2330"]  # type: ignore[index]
    assert validation["role"] == "validation"
    assert validation["status"] == "failed"
    assert validation["blocking"] is False


@pytest.mark.parametrize(
    ("symbol", "message"),
    (
        ("7740", "E.SUN intraday ticker identity mismatch"),
        ("6771", "E.SUN intraday ticker identity mismatch"),
        ("7795", "E.SUN historical candles identity mismatch"),
    ),
)
def test_31_actual_scheduler_failure_symbols_keep_complete_twse_canonical(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    symbol: str,
    message: str,
) -> None:
    database = _database(tmp_path)
    esun_config = (tmp_path / "esun.ini").resolve()
    esun_config.write_text("fixture", encoding="utf-8")

    class FakeEsunProvider:
        source = "esun-historical"

    monkeypatch.setattr(
        preparation,
        "EsunHistoricalMarketDataProvider",
        lambda **_kwargs: FakeEsunProvider(),
    )
    snapshot = SimpleNamespace(
        price_history=SimpleNamespace(
            status="available",
            observations=tuple(range(250)),
        )
    )
    dataset = preparation.CandidatePreparingDataset(
        database,
        SimpleNamespace(read=lambda _request: snapshot),  # type: ignore[arg-type]
        cadence_seconds=0,
        timeout_seconds=1,
        esun_config_path=esun_config,
    )
    dataset.esun_pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ProviderInvalidPayloadError(message)
        )
    )
    twse_result = SimpleNamespace(
        run_status=PipelineRunStatus.SUCCESS,
        run_id=f"twse-{symbol}",
        idempotent_replay=True,
    )
    dataset.pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: twse_result
    )

    assert dataset.read(ResearchDatasetRequest(symbol, TARGET_DATE, 250)) is snapshot
    dataset.ensure_candidate_validation(symbol, TARGET_DATE)
    evidence = dataset.evidence()
    validation = evidence["historical_validation"]["records"][symbol]  # type: ignore[index]
    assert validation["status"] == "failed"
    assert validation["blocking"] is False
    assert evidence["esun_historical"]["records"][symbol]["error_message"] == message  # type: ignore[index]


def test_32_incomplete_twse_with_esun_identity_mismatch_stays_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = _database(tmp_path)
    esun_config = (tmp_path / "esun.ini").resolve()
    esun_config.write_text("fixture", encoding="utf-8")

    class FakeEsunProvider:
        source = "esun-historical"

    monkeypatch.setattr(
        preparation,
        "EsunHistoricalMarketDataProvider",
        lambda **_kwargs: FakeEsunProvider(),
    )
    snapshot = SimpleNamespace(
        price_history=SimpleNamespace(
            status="available",
            observations=tuple(range(12)),
        )
    )
    dataset = preparation.CandidatePreparingDataset(
        database,
        SimpleNamespace(read=lambda _request: snapshot),  # type: ignore[arg-type]
        cadence_seconds=0,
        timeout_seconds=1,
        esun_config_path=esun_config,
    )
    dataset.esun_pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ProviderInvalidPayloadError("E.SUN intraday ticker identity mismatch")
        )
    )
    dataset.pipeline = SimpleNamespace(
        run=lambda *_args, **_kwargs: SimpleNamespace(
            run_status=PipelineRunStatus.SUCCESS,
            run_id="twse-incomplete",
            idempotent_replay=False,
        )
    )

    with pytest.raises(
        preparation.DailyPreparationError,
        match="Stage 2 readiness remains incomplete",
    ):
        dataset.read(ResearchDatasetRequest("6589", TARGET_DATE, 250))
    validation = dataset.evidence()["historical_validation"]["records"]["6589"]  # type: ignore[index]
    assert validation["status"] == "failed"
    assert validation["blocking"] is True
