"""Broker-neutral provider contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
import math
from typing import Mapping

from app.models import SourceArtifact

from .manifest import ProviderManifest, get_provider_manifest


class ProviderError(RuntimeError):
    """Base class for provider failures."""


class ProviderTemporaryError(ProviderError):
    """A transient provider failure that may succeed when retried."""

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        if retry_after_seconds is not None:
            if (
                isinstance(retry_after_seconds, bool)
                or not isinstance(retry_after_seconds, (int, float))
                or not math.isfinite(retry_after_seconds)
                or retry_after_seconds < 0
            ):
                raise ValueError("retry_after_seconds must be finite and non-negative")
            retry_after_seconds = float(retry_after_seconds)
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class ProviderTimeoutError(ProviderTemporaryError):
    """A provider request exceeded its configured timeout."""


class ProviderPermanentError(ProviderError):
    """A non-retryable provider failure."""


class ProviderInvalidRequestError(ProviderPermanentError):
    """The caller supplied a request that the provider cannot accept."""


class ProviderInvalidPayloadError(ProviderPermanentError):
    """The provider detected a malformed or contract-breaking payload."""


class ProviderNotImplementedError(ProviderPermanentError):
    """Raised by an adapter whose official integration contract is unavailable."""


@dataclass(frozen=True, slots=True)
class MarketDataBatch:
    """Raw provider payload with a stable outer envelope.

    Inner mappings remain provider-shaped and must pass through normalization before
    they can enter SQLite.
    """

    source: str
    symbol: Mapping[str, object]
    daily_prices: tuple[Mapping[str, object], ...]
    company_metrics: tuple[Mapping[str, object], ...]
    source_endpoints: tuple[str, ...] = ()
    fetched_at: datetime | None = None
    market_date: date | None = None
    source_timestamp_raw: str | None = None
    source_timestamp: datetime | None = None
    source_artifacts: tuple[SourceArtifact, ...] = ()

    def __post_init__(self) -> None:
        source = self.source.strip()
        if not source:
            raise ValueError("market-data batch source must not be blank")
        endpoints = tuple(endpoint.strip() for endpoint in self.source_endpoints)
        if any(not endpoint for endpoint in endpoints):
            raise ValueError("market-data batch endpoints must not be blank")
        if len(set(endpoints)) != len(endpoints):
            raise ValueError("market-data batch endpoints must be unique")
        artifact_keys: set[tuple[str, str, str]] = set()
        for artifact in self.source_artifacts:
            if artifact.provider != source:
                raise ValueError("source artifact provider must match batch source")
            if endpoints and artifact.endpoint not in endpoints:
                raise ValueError("source artifact endpoint must belong to the batch")
            key = (artifact.dataset, artifact.endpoint, artifact.payload_sha256)
            if key in artifact_keys:
                raise ValueError("source artifacts must not contain duplicates")
            artifact_keys.add(key)


class MarketDataProvider(ABC):
    """Abstract interface implemented by every market-data source."""

    @property
    @abstractmethod
    def source(self) -> str:
        """Return the stable source identifier stored with normalized records."""

    @property
    def manifest(self) -> ProviderManifest:
        """Return discoverable metadata for a built-in provider."""

        return get_provider_manifest(self.source)

    @abstractmethod
    def fetch_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        timeout_seconds: float,
    ) -> MarketDataBatch:
        """Fetch a raw batch for one symbol and inclusive date range."""
