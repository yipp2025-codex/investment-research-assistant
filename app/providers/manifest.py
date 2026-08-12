"""Discoverable metadata for built-in market-data providers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SourceAuthority(str, Enum):
    """Authority class for a provider's market-data source."""

    SYNTHETIC = "synthetic"
    OFFICIAL_EXCHANGE = "official-exchange"
    OFFICIAL_BROKER = "official-broker"


class CredentialMode(str, Enum):
    """Credential boundary used by a provider."""

    NONE = "none"
    WINDOWS_KEYRING_VIA_OFFICIAL_SDK = "windows-keyring-via-official-sdk"


@dataclass(frozen=True, slots=True)
class DatasetCapability:
    """One stable ingestion dataset participating in a batch/artifact contract."""

    name: str
    market: str
    instrument: str
    granularity: str
    canonical_write: bool

    def __post_init__(self) -> None:
        for field_name in ("name", "market", "instrument", "granularity"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"dataset capability {field_name} must not be blank")
        if not isinstance(self.canonical_write, bool):
            raise ValueError("dataset capability canonical_write must be boolean")


@dataclass(frozen=True, slots=True)
class ProviderManifest:
    """Stable discovery record for one provider's read-only ingestion contract."""

    source: str
    display_name: str
    authority: SourceAuthority
    credential_mode: CredentialMode
    contract_version: str
    contract_document: str
    read_only: bool
    datasets: tuple[DatasetCapability, ...]

    def __post_init__(self) -> None:
        for field_name in (
            "source",
            "display_name",
            "contract_version",
            "contract_document",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"provider manifest {field_name} must not be blank")
        if not isinstance(self.authority, SourceAuthority):
            raise ValueError("provider manifest authority must be a SourceAuthority")
        if not isinstance(self.credential_mode, CredentialMode):
            raise ValueError(
                "provider manifest credential_mode must be a CredentialMode"
            )
        if self.read_only is not True:
            raise ValueError("market-data provider manifests must be read-only")
        if not self.datasets:
            raise ValueError("provider manifest must declare at least one dataset")
        names = [dataset.name for dataset in self.datasets]
        if len(set(names)) != len(names):
            raise ValueError("provider manifest dataset names must be unique")


_MANIFESTS = (
    ProviderManifest(
        source="mock-synthetic",
        display_name="Deterministic synthetic test provider",
        authority=SourceAuthority.SYNTHETIC,
        credential_mode=CredentialMode.NONE,
        contract_version="mock-synthetic-v1",
        contract_document="README.md",
        read_only=True,
        datasets=(
            DatasetCapability(
                name="synthetic-daily-market-data",
                market="MOCK",
                instrument="synthetic-equity",
                granularity="daily",
                canonical_write=True,
            ),
        ),
    ),
    ProviderManifest(
        source="twse",
        display_name="Taiwan Stock Exchange OpenAPI",
        authority=SourceAuthority.OFFICIAL_EXCHANGE,
        credential_mode=CredentialMode.NONE,
        contract_version="twse-openapi-2026-08-05",
        contract_document="docs/twse-openapi-contract.md",
        read_only=True,
        datasets=(
            DatasetCapability(
                name="STOCK_DAY_ALL",
                market="TWSE",
                instrument="listed-common-stock",
                granularity="latest-daily",
                canonical_write=True,
            ),
            DatasetCapability(
                name="BWIBBU_ALL",
                market="TWSE",
                instrument="listed-common-stock",
                granularity="latest-daily",
                canonical_write=True,
            ),
        ),
    ),
    ProviderManifest(
        source="twse-historical",
        display_name="Taiwan Stock Exchange historical monthly data",
        authority=SourceAuthority.OFFICIAL_EXCHANGE,
        credential_mode=CredentialMode.NONE,
        contract_version="twse-historical-2026-08-10",
        contract_document="docs/twse-historical-contract.md",
        read_only=True,
        datasets=(
            DatasetCapability(
                name="STOCK_DAY",
                market="TWSE",
                instrument="listed-common-stock",
                granularity="daily-by-calendar-month",
                canonical_write=True,
            ),
        ),
    ),
    ProviderManifest(
        source="esun",
        display_name="E.SUN Securities official market-data adapter",
        authority=SourceAuthority.OFFICIAL_BROKER,
        credential_mode=CredentialMode.WINDOWS_KEYRING_VIA_OFFICIAL_SDK,
        contract_version="esun-marketdata-sdk-2.2.0-2026-08-06",
        contract_document="docs/esun-marketdata-contract.md",
        read_only=True,
        datasets=(
            DatasetCapability(
                name="intraday-ticker",
                market="TWSE",
                instrument="listed-common-stock",
                granularity="latest",
                canonical_write=False,
            ),
            DatasetCapability(
                name="historical-candles",
                market="TWSE",
                instrument="listed-common-stock",
                granularity="daily",
                canonical_write=True,
            ),
        ),
    ),
    ProviderManifest(
        source="esun-historical",
        display_name="E.SUN Securities historical source-only adapter",
        authority=SourceAuthority.OFFICIAL_BROKER,
        credential_mode=CredentialMode.WINDOWS_KEYRING_VIA_OFFICIAL_SDK,
        contract_version="esun-marketdata-sdk-2.2.0-2026-08-06",
        contract_document="docs/esun-marketdata-contract.md",
        read_only=True,
        datasets=(
            DatasetCapability(
                name="intraday-ticker",
                market="TWSE",
                instrument="listed-common-stock",
                granularity="latest",
                canonical_write=False,
            ),
            DatasetCapability(
                name="historical-candles",
                market="TWSE",
                instrument="listed-common-stock",
                granularity="daily-by-calendar-month",
                canonical_write=False,
            ),
        ),
    ),
)

_MANIFEST_BY_SOURCE = {manifest.source: manifest for manifest in _MANIFESTS}

if len(_MANIFEST_BY_SOURCE) != len(_MANIFESTS):  # pragma: no cover - import guard.
    raise RuntimeError("built-in provider manifest sources must be unique")


def list_provider_manifests() -> tuple[ProviderManifest, ...]:
    """Return every built-in provider manifest in stable source order."""

    return tuple(sorted(_MANIFESTS, key=lambda manifest: manifest.source))


def get_provider_manifest(source: str) -> ProviderManifest:
    """Resolve an exact built-in provider source without guessing aliases."""

    normalized = source.strip()
    if not normalized:
        raise ValueError("provider source must not be blank")
    try:
        return _MANIFEST_BY_SOURCE[normalized]
    except KeyError as error:
        raise KeyError(f"unknown built-in provider source: {normalized}") from error
