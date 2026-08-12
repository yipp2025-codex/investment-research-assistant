"""Pure unit tests for the uncomposed M9.1 ResearchDataset contract."""

from __future__ import annotations

import ast
import inspect
from dataclasses import FrozenInstanceError
from datetime import date, datetime, timedelta, timezone

import pytest

import app.research_dataset as research_dataset_module
from app.research_dataset import (
    DatasetArtifactRef,
    DatasetDiscrepancy,
    DatasetPrice,
    DatasetProvenance,
    DatasetSourcePolicyError,
    DatasetSymbol,
    DatasetValidationObservation,
    DatasetValuationMetric,
    FakeResearchDataset,
    MOCK_SYNTHETIC_SOURCE,
    ResearchDataset,
    ResearchDatasetRequest,
    TWSE_BASELINE_SOURCE_POLICY,
    TWSE_BASELINE_SOURCES,
    TwseBaselineSourcePolicy,
    ValidationReadModel,
)


UTC = timezone.utc
AS_OF = date(2026, 8, 5)
FIXED_TIME = datetime(2026, 8, 5, 10, tzinfo=UTC)


def _symbol(
    symbol: str = "2330",
    *,
    market: str = "TWSE",
    name: str | None = "Test 2330",
) -> DatasetSymbol:
    return DatasetSymbol(symbol, name, market, "TWD")


def _price(
    trade_date: date,
    close: float,
    *,
    symbol: str = "2330",
    source: str = "twse-historical",
) -> DatasetPrice:
    return DatasetPrice(
        symbol=symbol,
        trade_date=trade_date,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=1_000,
        source=source,
    )


def _valuation(
    metric_date: date,
    value: float,
    *,
    symbol: str = "2330",
    source: str = "twse",
    name: str = "price_earnings_ratio",
) -> DatasetValuationMetric:
    return DatasetValuationMetric(
        symbol=symbol,
        metric_date=metric_date,
        name=name,
        value=value,
        unit="ratio",
        source=source,
    )


def _observation(
    provider: str,
    *,
    market_date: date = AS_OF,
    close: float = 105.0,
) -> DatasetValidationObservation:
    return DatasetValidationObservation(
        symbol="2330",
        provider=provider,
        market_date=market_date,
        open=105.0,
        high=106.0,
        low=104.0,
        close=close,
        volume=1_000,
        source_endpoints=(f"https://evidence.invalid/{provider}",),
        fetched_at=FIXED_TIME,
    )


def _validation(
    *,
    run_id: str = "validation-1",
    status: str = "source_discrepancy",
    target_date: date = AS_OF,
    created_at: datetime = FIXED_TIME,
) -> ValidationReadModel:
    if status == "available":
        outcome = "match"
        discrepancies: tuple[DatasetDiscrepancy, ...] = ()
    elif status == "market_date_mismatch":
        outcome = "discrepancy"
        discrepancies = (
            DatasetDiscrepancy(
                "market_date",
                AS_OF.isoformat(),
                (AS_OF - timedelta(days=1)).isoformat(),
                "provider market dates differ",
            ),
        )
    else:
        outcome = "discrepancy"
        discrepancies = (
            DatasetDiscrepancy(
                "volume",
                1_000,
                1_100,
                "volume differs",
            ),
        )
    return ValidationReadModel(
        symbol="2330",
        status=status,
        run_id=run_id,
        target_date=target_date,
        left_provider="twse",
        right_provider="esun",
        outcome=outcome,
        created_at=created_at,
        # Deliberately reversed: the read model must expose left then right.
        observations=(
            _observation("esun", close=105.5),
            _observation("twse"),
        ),
        discrepancies=discrepancies,
    )


def _artifact(
    provider: str = "twse",
    *,
    owner_kind: str = "pipeline",
    owner_run_id: str = "pipeline-1",
) -> DatasetArtifactRef:
    return DatasetArtifactRef(
        owner_kind=owner_kind,
        owner_run_id=owner_run_id,
        provider=provider,
        dataset="daily-market-data",
        endpoint=f"https://evidence.invalid/artifacts/{provider}",
        contract_version="m7-v1",
        content_type="application/json",
        payload_sha256="a" * 64,
        payload_size_bytes=123,
        hash_basis="raw-response-bytes-v1",
        fetched_at=FIXED_TIME,
    )


def test_protocol_has_one_read_surface_and_module_has_no_storage_or_provider_imports(
) -> None:
    dataset = FakeResearchDataset(symbols=(_symbol(),))
    request = ResearchDatasetRequest("2330", AS_OF)

    assert isinstance(dataset, ResearchDataset)
    assert dataset.read(request).as_of.source_policy == TWSE_BASELINE_SOURCE_POLICY
    assert [
        name
        for name, value in inspect.getmembers(
            FakeResearchDataset,
            predicate=inspect.isfunction,
        )
        if not name.startswith("_")
    ] == ["read"]
    assert not hasattr(dataset, "connection")
    assert not hasattr(dataset, "cursor")
    assert not hasattr(dataset, "repository")
    assert not hasattr(dataset, "provider")

    tree = ast.parse(inspect.getsource(research_dataset_module))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module or ""
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert "sqlite3" not in imports
    assert not any(name.startswith("app.storage") for name in imports)
    assert not any(name.startswith("app.providers") for name in imports)


def test_request_is_normalized_fixed_policy_and_preserves_all_selectors() -> None:
    request = ResearchDatasetRequest(
        " 2330 ",
        AS_OF,
        history_observations=120,
        pipeline_run_id="pipeline-1",
        historical_run_id="historical-1",
        validation_run_id="validation-1",
    )

    assert request.symbol == "2330"
    assert request.source_policy == "twse_baseline"
    assert request.history_observations == 120
    assert request.pipeline_run_id == "pipeline-1"
    assert request.historical_run_id == "historical-1"
    assert request.validation_run_id == "validation-1"
    with pytest.raises(TypeError, match="source_policy"):
        ResearchDatasetRequest(  # type: ignore[call-arg]
            "2330",
            AS_OF,
            source_policy="anything-else",
        )
    with pytest.raises(ValueError, match="positive integer or None"):
        ResearchDatasetRequest("2330", AS_OF, history_observations=0)
    with pytest.raises(TypeError, match="date, not a datetime"):
        ResearchDatasetRequest("2330", FIXED_TIME)  # type: ignore[arg-type]


def test_twse_baseline_success_sorts_history_excludes_future_and_carries_valuation(
) -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(),),
        prices=(
            _price(date(2026, 8, 6), 999.0, source="twse"),
            _price(AS_OF, 105.0),
            _price(date(2026, 8, 1), 100.0, source="twse"),
            _price(date(2026, 8, 4), 103.0),
        ),
        valuations=(
            _valuation(date(2026, 8, 6), 99.0),
            _valuation(date(2026, 8, 1), 18.0),
            _valuation(date(2026, 8, 4), 20.0, source="twse-historical"),
        ),
        provenance=(
            DatasetProvenance(
                symbol="2330",
                pipeline_run_id="pipeline-1",
                historical_run_id="historical-1",
                canonical_sources=("twse-historical", "twse"),
                source_endpoints=("https://evidence.invalid/twse",),
                artifact_refs=(_artifact(),),
                fetched_at=FIXED_TIME,
            ),
        ),
    )

    snapshot = dataset.read(
        ResearchDatasetRequest(
            "2330",
            AS_OF,
            pipeline_run_id="pipeline-1",
            historical_run_id="historical-1",
        )
    )

    assert snapshot.symbol.symbol == "2330"
    assert snapshot.price_history.status == "available"
    assert [item.trade_date for item in snapshot.price_history.observations] == [
        date(2026, 8, 1),
        date(2026, 8, 4),
        AS_OF,
    ]
    assert snapshot.price_history.current is snapshot.price_history.observations[-1]
    assert snapshot.price_history.current.close == 105.0
    assert snapshot.valuation.status == "available"
    assert [(item.name, item.metric_date, item.value) for item in snapshot.valuation.metrics] == [
        ("price_earnings_ratio", date(2026, 8, 4), 20.0)
    ]
    assert snapshot.provenance.canonical_sources == (
        "twse",
        "twse-historical",
    )
    assert snapshot.provenance.pipeline_run_id == "pipeline-1"
    assert snapshot.provenance.historical_run_id == "historical-1"
    assert snapshot.provenance.artifact_refs == (_artifact(),)
    assert snapshot.validation.status == "missing_source"


def test_history_observation_none_is_complete_and_limit_returns_latest_rows() -> None:
    prices = tuple(
        _price(date(2026, 8, day), 100.0 + day)
        for day in (1, 2, 3, 4, 5)
    )
    dataset = FakeResearchDataset(symbols=(_symbol(),), prices=prices)

    complete = dataset.read(
        ResearchDatasetRequest("2330", AS_OF, history_observations=None)
    )
    limited = dataset.read(
        ResearchDatasetRequest("2330", AS_OF, history_observations=2)
    )

    assert len(complete.price_history.observations) == 5
    assert complete.as_of.total_history_observations == 5
    assert complete.as_of.returned_history_observations == 5
    assert complete.as_of.history_is_truncated is False
    assert complete.price_history.is_truncated is False
    assert [item.trade_date for item in limited.price_history.observations] == [
        date(2026, 8, 4),
        AS_OF,
    ]
    assert limited.as_of.total_history_observations == 5
    assert limited.as_of.returned_history_observations == 2
    assert limited.as_of.history_is_truncated is True
    assert limited.price_history.current.trade_date == AS_OF


def test_older_latest_price_preserves_market_date_mismatch_without_sentinel() -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(),),
        prices=(_price(date(2026, 8, 4), 103.0),),
    )

    snapshot = dataset.read(ResearchDatasetRequest("2330", AS_OF))

    assert snapshot.price_history.status == "market_date_mismatch"
    assert snapshot.price_history.current is None
    assert snapshot.price_history.observations[-1].trade_date == date(2026, 8, 4)


def test_esun_validation_is_visible_and_never_fills_missing_twse_history() -> None:
    validation = _validation()
    dataset = FakeResearchDataset(
        symbols=(_symbol(),),
        validations=(validation,),
        provenance=(
            DatasetProvenance(
                symbol="2330",
                validation_run_id="validation-1",
                validation_sources=("esun", "twse"),
                artifact_refs=(
                    _artifact(
                        "esun",
                        owner_kind="validation",
                        owner_run_id="validation-1",
                    ),
                ),
            ),
        ),
    )

    snapshot = dataset.read(
        ResearchDatasetRequest(
            "2330",
            AS_OF,
            validation_run_id="validation-1",
        )
    )

    assert snapshot.price_history.status == "missing_source"
    assert snapshot.price_history.current is None
    assert snapshot.price_history.observations == ()
    assert snapshot.validation.status == "source_discrepancy"
    assert snapshot.validation.run_id == "validation-1"
    assert [item.provider for item in snapshot.validation.observations] == [
        "twse",
        "esun",
    ]
    assert snapshot.validation.discrepancies[0].field == "volume"
    assert snapshot.provenance.validation_sources == ("esun", "twse")
    assert snapshot.provenance.canonical_sources == ()


def test_validation_preserves_market_date_mismatch_status() -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(),),
        prices=(_price(AS_OF, 105.0),),
        validations=(_validation(status="market_date_mismatch"),),
    )

    snapshot = dataset.read(ResearchDatasetRequest("2330", AS_OF))

    assert snapshot.validation.status == "market_date_mismatch"
    assert snapshot.validation.discrepancies[0].field == "market_date"
    assert snapshot.price_history.status == "available"


@pytest.mark.parametrize(
    ("prices", "valuations", "expected_source"),
    [
        ((_price(AS_OF, 105.0, source="esun"),), (), "esun"),
        (
            (_price(AS_OF, 105.0, source="twse"),),
            (_valuation(AS_OF, 20.0, source="esun"),),
            "esun",
        ),
        ((_price(AS_OF, 105.0, source="vendor-x"),), (), "vendor-x"),
    ],
    ids=("esun-price", "esun-valuation", "unknown-canonical"),
)
def test_esun_or_unknown_canonical_rows_fail_closed(
    prices: tuple[DatasetPrice, ...],
    valuations: tuple[DatasetValuationMetric, ...],
    expected_source: str,
) -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(),),
        prices=prices,
        valuations=valuations,
    )

    with pytest.raises(DatasetSourcePolicyError, match=expected_source):
        dataset.read(ResearchDatasetRequest("2330", AS_OF))


def test_source_policy_checks_all_as_of_rows_before_history_limit() -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(),),
        prices=(
            _price(date(2026, 8, 1), 100.0, source="esun"),
            _price(AS_OF, 105.0, source="twse"),
        ),
    )

    with pytest.raises(DatasetSourcePolicyError, match="esun"):
        dataset.read(
            ResearchDatasetRequest("2330", AS_OF, history_observations=1)
        )


def test_complete_mock_synthetic_snapshot_succeeds_without_formal_sources() -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(market="MOCK"),),
        prices=(_price(AS_OF, 105.0, source=MOCK_SYNTHETIC_SOURCE),),
        valuations=(
            _valuation(
                AS_OF,
                12.5,
                source=MOCK_SYNTHETIC_SOURCE,
                name="synthetic_metric",
            ),
        ),
        provenance=(
            DatasetProvenance(
                symbol="2330",
                canonical_sources=(MOCK_SYNTHETIC_SOURCE,),
                source_endpoints=("mock://synthetic/market-data",),
                artifact_refs=(
                    _artifact(MOCK_SYNTHETIC_SOURCE),
                ),
            ),
        ),
    )

    snapshot = dataset.read(ResearchDatasetRequest("2330", AS_OF))

    assert snapshot.price_history.status == "available"
    assert snapshot.price_history.current.source == "mock-synthetic"
    assert snapshot.valuation.metrics[0].source == "mock-synthetic"
    assert snapshot.provenance.canonical_sources == ("mock-synthetic",)
    assert snapshot.provenance.validation_sources == ()


@pytest.mark.parametrize(
    ("prices", "valuations", "validations"),
    [
        (
            (_price(AS_OF, 105.0, source="mock-synthetic"),),
            (_valuation(AS_OF, 20.0, source="twse"),),
            (),
        ),
        (
            (_price(AS_OF, 105.0, source="mock-synthetic"),),
            (),
            (_validation(),),
        ),
    ],
    ids=("mock-with-twse-canonical", "mock-with-formal-validation"),
)
def test_mock_and_formal_sources_cannot_mix(
    prices: tuple[DatasetPrice, ...],
    valuations: tuple[DatasetValuationMetric, ...],
    validations: tuple[ValidationReadModel, ...],
) -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(market="MOCK"),),
        prices=prices,
        valuations=valuations,
        validations=validations,
    )

    with pytest.raises(DatasetSourcePolicyError, match="must not mix"):
        dataset.read(ResearchDatasetRequest("2330", AS_OF))


def test_mock_without_canonical_history_is_not_a_complete_synthetic_snapshot() -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(market="MOCK"),),
        provenance=(
            DatasetProvenance(
                symbol="2330",
                canonical_sources=("mock-synthetic",),
            ),
        ),
    )

    with pytest.raises(DatasetSourcePolicyError, match="complete synthetic"):
        dataset.read(ResearchDatasetRequest("2330", AS_OF))


def test_missing_values_use_none_or_empty_tuples_never_numeric_or_text_sentinels(
) -> None:
    dataset = FakeResearchDataset(
        symbols=(_symbol(name=None),),
        provenance=(
            DatasetProvenance(symbol="2330", canonical_sources=("twse",)),
        ),
    )

    snapshot = dataset.read(ResearchDatasetRequest("2330", AS_OF))

    assert snapshot.symbol.name is None
    assert snapshot.price_history.status == "missing_source"
    assert snapshot.price_history.current is None
    assert snapshot.price_history.observations == ()
    assert snapshot.valuation.status == "missing_source"
    assert snapshot.valuation.metrics == ()
    assert snapshot.validation.status == "missing_source"
    assert snapshot.validation.run_id is None
    with pytest.raises(ValueError, match="None rather than an empty string"):
        DatasetDiscrepancy("close", "", "105", "different")


def test_snapshot_and_nested_models_are_deeply_immutable() -> None:
    input_prices = [_price(AS_OF, 105.0)]
    dataset = FakeResearchDataset(
        symbols=[_symbol()],
        prices=input_prices,
    )
    input_prices.append(_price(date(2026, 8, 6), 999.0))
    snapshot = dataset.read(ResearchDatasetRequest("2330", AS_OF))

    assert len(snapshot.price_history.observations) == 1
    with pytest.raises(FrozenInstanceError):
        snapshot.symbol = _symbol("2317")  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        snapshot.price_history.status = "missing_source"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        snapshot.price_history.observations.append(  # type: ignore[attr-defined]
            _price(AS_OF, 106.0)
        )


def test_provenance_artifacts_are_hash_only_and_reject_credentials() -> None:
    artifact = _artifact()

    assert artifact.payload_sha256 == "a" * 64
    assert not hasattr(artifact, "payload")
    assert not hasattr(artifact, "body")
    assert not hasattr(artifact, "headers")
    assert not hasattr(artifact, "credentials")
    assert TwseBaselineSourcePolicy.name == "twse_baseline"
    assert TwseBaselineSourcePolicy.canonical_family == TWSE_BASELINE_SOURCES
    with pytest.raises(ValueError, match="credential query parameters"):
        DatasetArtifactRef(
            owner_kind="pipeline",
            owner_run_id="pipeline-1",
            provider="twse",
            dataset="daily-market-data",
            endpoint="https://example.invalid/data?api_key=secret",
            contract_version="m7-v1",
            content_type="application/json",
            payload_sha256="a" * 64,
            payload_size_bytes=1,
            hash_basis="raw-response-bytes-v1",
        )
    with pytest.raises(ValueError, match="must not contain credentials"):
        DatasetProvenance(
            symbol="2330",
            source_endpoints=("https://user:secret@example.invalid/data",),
        )
