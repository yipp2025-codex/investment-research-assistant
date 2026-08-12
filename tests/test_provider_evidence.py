import hashlib
import sqlite3
from dataclasses import fields
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.models import SourceArtifact
from app.providers import (
    CredentialMode,
    DatasetCapability,
    MockMarketDataProvider,
    ProviderManifest,
    SourceAuthority,
    get_provider_manifest,
    list_provider_manifests,
)
from app.providers.artifacts import (
    source_artifact_from_bytes,
    source_artifact_from_json,
)
from app.storage import PipelineRunStateError, SQLiteResearchRepository


FETCHED_AT = datetime(2026, 8, 8, 1, 2, 3, tzinfo=timezone.utc)


def test_builtin_provider_manifests_are_discoverable_read_only_contracts() -> None:
    manifests = list_provider_manifests()

    assert [manifest.source for manifest in manifests] == [
        "esun",
        "esun-historical",
        "mock-synthetic",
        "twse",
        "twse-historical",
    ]
    assert all(manifest.read_only is True for manifest in manifests)
    assert all(Path(manifest.contract_document).is_file() for manifest in manifests)
    assert get_provider_manifest("twse").authority is SourceAuthority.OFFICIAL_EXCHANGE
    assert get_provider_manifest("esun").credential_mode is (
        CredentialMode.WINDOWS_KEYRING_VIA_OFFICIAL_SDK
    )
    assert {
        dataset.name: dataset.canonical_write
        for dataset in get_provider_manifest("esun").datasets
    } == {"intraday-ticker": False, "historical-candles": True}
    assert all(
        dataset.canonical_write is False
        for dataset in get_provider_manifest("esun-historical").datasets
    )
    assert MockMarketDataProvider().manifest == get_provider_manifest(
        "mock-synthetic"
    )


def test_provider_manifest_rejects_unknown_or_write_capable_contracts() -> None:
    with pytest.raises(KeyError, match="unknown built-in provider"):
        get_provider_manifest("third-party")

    with pytest.raises(ValueError, match="read-only"):
        ProviderManifest(
            source="unsafe",
            display_name="Unsafe provider",
            authority=SourceAuthority.SYNTHETIC,
            credential_mode=CredentialMode.NONE,
            contract_version="unsafe-v1",
            contract_document="README.md",
            read_only=False,
            datasets=(
                DatasetCapability(
                    name="unsafe",
                    market="MOCK",
                    instrument="synthetic-equity",
                    granularity="daily",
                    canonical_write=False,
                ),
            ),
        )


def test_source_artifact_hashes_exact_bytes_without_retaining_payload() -> None:
    body = b'{"example":"hash-only evidence"}\r\n'

    artifact = source_artifact_from_bytes(
        provider="twse",
        dataset="STOCK_DAY_ALL",
        endpoint="https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
        contract_version="twse-openapi-2026-08-05",
        body=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        fetched_at=FETCHED_AT,
    )

    assert artifact.payload_sha256 == hashlib.sha256(body).hexdigest()
    assert artifact.payload_size_bytes == len(body)
    assert artifact.hash_basis == "raw-response-bytes-v1"
    assert artifact.content_type == "application/json; charset=utf-8"
    assert "body" not in {field.name for field in fields(SourceArtifact)}


def test_synthetic_canonical_json_hash_is_order_independent() -> None:
    common = {
        "provider": "mock-synthetic",
        "dataset": "synthetic-daily-market-data",
        "endpoint": "mock://synthetic/market-data",
        "contract_version": "mock-synthetic-v1",
        "fetched_at": FETCHED_AT,
    }

    first = source_artifact_from_json(payload={"b": 2, "a": 1}, **common)
    second = source_artifact_from_json(payload={"a": 1, "b": 2}, **common)

    assert first == second
    assert first.hash_basis == "canonical-json-v1"


@pytest.mark.parametrize(
    ("field_name", "value", "message"),
    [
        ("payload_sha256", "A" * 64, "lowercase SHA-256"),
        ("payload_size_bytes", -1, "non-negative"),
        ("hash_basis", "normalized-object-v1", "unsupported"),
        ("fetched_at", datetime(2026, 8, 8), "timezone-aware"),
    ],
)
def test_source_artifact_domain_validation_fails_closed(
    field_name: str, value: object, message: str
) -> None:
    values = {
        "provider": "twse",
        "dataset": "STOCK_DAY_ALL",
        "endpoint": "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
        "contract_version": "twse-openapi-2026-08-05",
        "content_type": "application/json",
        "payload_sha256": "0" * 64,
        "payload_size_bytes": 10,
        "hash_basis": "raw-response-bytes-v1",
        "fetched_at": FETCHED_AT,
    }
    values[field_name] = value

    with pytest.raises(ValueError, match=message):
        SourceArtifact(**values)


def test_schema_v10_contains_only_hash_metadata_and_detects_drift(tmp_path) -> None:
    repository = SQLiteResearchRepository(tmp_path / "evidence.db")
    repository.initialize()

    with sqlite3.connect(repository.database_path) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(source_artifacts)")
        }
        migration = connection.execute(
            "SELECT name FROM schema_migrations WHERE version = 10"
        ).fetchone()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        foreign_key_violations = connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()

    assert repository.get_schema_version() == 10
    assert columns == {
        "id",
        "pipeline_run_id",
        "historical_run_id",
        "validation_run_id",
        "checkpoint_key",
        "provider",
        "dataset",
        "endpoint",
        "contract_version",
        "content_type",
        "payload_sha256",
        "payload_size_bytes",
        "hash_basis",
        "fetched_at",
        "created_at",
    }
    assert not {"body", "payload", "raw_payload", "response_body"} & columns
    assert migration == ("provider response source artifacts",)
    assert integrity == "ok"
    assert foreign_key_violations == []

    with sqlite3.connect(repository.database_path) as connection:
        connection.execute("DROP TABLE source_artifacts")

    with pytest.raises(PipelineRunStateError, match="migration 10"):
        repository.initialize()
