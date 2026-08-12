"""Market-data provider adapters."""

from .base import (
    MarketDataBatch,
    MarketDataProvider,
    ProviderError,
    ProviderInvalidPayloadError,
    ProviderInvalidRequestError,
    ProviderNotImplementedError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)
from .manifest import (
    CredentialMode,
    DatasetCapability,
    ProviderManifest,
    SourceAuthority,
    get_provider_manifest,
    list_provider_manifests,
)
from .esun import (
    EsunHistoricalStats,
    EsunMarketDataProvider,
    EsunQuote,
    EsunSnapshotQuote,
)
from .esun_sdk import (
    EsunHttpResponse,
    EsunHttpTransport,
    EsunSdkHttpTransport,
)
from .esun_history import EsunHistoricalMarketDataProvider
from .mock import MockFailureMode, MockMarketDataProvider
from .twse import TwseMarketDataProvider
from .twse_history import TwseHistoricalMarketDataProvider

__all__ = [
    "EsunMarketDataProvider",
    "EsunHistoricalStats",
    "EsunHistoricalMarketDataProvider",
    "EsunHttpResponse",
    "EsunHttpTransport",
    "EsunQuote",
    "EsunSdkHttpTransport",
    "EsunSnapshotQuote",
    "MarketDataBatch",
    "MarketDataProvider",
    "CredentialMode",
    "DatasetCapability",
    "MockFailureMode",
    "MockMarketDataProvider",
    "ProviderError",
    "ProviderInvalidPayloadError",
    "ProviderInvalidRequestError",
    "ProviderNotImplementedError",
    "ProviderManifest",
    "ProviderPermanentError",
    "ProviderTemporaryError",
    "ProviderTimeoutError",
    "SourceAuthority",
    "TwseMarketDataProvider",
    "TwseHistoricalMarketDataProvider",
    "get_provider_manifest",
    "list_provider_manifests",
]
