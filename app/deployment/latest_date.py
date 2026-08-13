"""S6C.1 authoritative latest-date provider.

The provider is an application gating dependency only.  It performs one
explicit read of the already-frozen official TWSE OpenAPI provider when its
callable is invoked.  Importing this module or creating the provider performs
no network request and no database write.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from app.providers.base import ProviderError, ProviderInvalidPayloadError
from app.providers.twse import TwseMarketDataProvider


PRODUCTION_LATEST_DATE_FACTORY_LOCATOR = (
    "app.deployment.latest_date:create_latest_date_provider"
)
LATEST_DATE_PROVIDER_CONTRACT_VERSION = "s6c1-twse-latest-date-v1"
LATEST_DATE_PROBE_SYMBOL_ENV = "IRA_TWSE_LATEST_DATE_PROBE_SYMBOL"
LATEST_DATE_TIMEOUT_ENV = "IRA_TWSE_LATEST_DATE_TIMEOUT_SECONDS"
DEFAULT_LATEST_DATE_PROBE_SYMBOL = "2330"
DEFAULT_LATEST_DATE_TIMEOUT_SECONDS = 10.0
TWSE_LATEST_DATE_DATASET = "STOCK_DAY_ALL+BWIBBU_ALL"
TWSE_LATEST_DATE_SOURCE = "twse-openapi"


class LatestDateProviderError(RuntimeError):
    """Configuration error for the latest-date provider."""


@dataclass(frozen=True, slots=True)
class LatestDateResolution:
    """Operational evidence for one latest-date attempt.

    This metadata is not part of S5 identity or S6A report content.
    """

    status: str
    latest_date: date | None
    source: str
    dataset: str
    error_code: str | None = None
    error_type: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"resolved", "unavailable", "malformed"}:
            raise LatestDateProviderError("unsupported latest-date status")
        if not self.source.strip() or not self.dataset.strip():
            raise LatestDateProviderError("latest-date source metadata is blank")
        if self.status == "resolved" and self.latest_date is None:
            raise LatestDateProviderError(
                "resolved latest-date evidence requires a date"
            )
        if self.status != "resolved" and self.latest_date is not None:
            raise LatestDateProviderError(
                "unresolved latest-date evidence cannot expose a date"
            )


class OfficialTwseLatestDateProvider:
    """Resolve the latest date published by the official TWSE OpenAPI."""

    def __init__(
        self,
        provider: TwseMarketDataProvider | None = None,
        *,
        probe_symbol: str = DEFAULT_LATEST_DATE_PROBE_SYMBOL,
        timeout_seconds: float = DEFAULT_LATEST_DATE_TIMEOUT_SECONDS,
    ) -> None:
        normalized_symbol = probe_symbol.strip().upper()
        if len(normalized_symbol) != 4 or not normalized_symbol.isdigit():
            raise LatestDateProviderError(
                "probe_symbol must be an explicit four-digit TWSE code"
            )
        if timeout_seconds <= 0:
            raise LatestDateProviderError("timeout_seconds must be positive")
        self.provider = provider or TwseMarketDataProvider()
        self.probe_symbol = normalized_symbol
        self.timeout_seconds = float(timeout_seconds)
        self.last_resolution: LatestDateResolution | None = None

    def __call__(self, target_market_date: date) -> date | None:
        target = _require_date(target_market_date, "target_market_date")
        try:
            # The frozen TWSE provider returns the latest row in the official
            # payload and validates it against this caller-supplied range.
            # A broad fixed range lets the policy distinguish latest-before
            # and latest-after without using the system clock as evidence.
            batch = self.provider.fetch_market_data(
                self.probe_symbol,
                date(1990, 1, 1),
                date.max,
                timeout_seconds=self.timeout_seconds,
            )
            candidates = [
                batch.market_date,
                *(
                    date.fromisoformat(str(item["trade_date"]))
                    for item in batch.daily_prices
                    if item.get("trade_date") is not None
                ),
            ]
            resolved = max((item for item in candidates if item is not None), default=None)
            if resolved is None:
                raise ProviderInvalidPayloadError(
                    "TWSE response did not contain a market date"
                )
        except ProviderInvalidPayloadError as error:
            self.last_resolution = LatestDateResolution(
                status="malformed",
                latest_date=None,
                source=TWSE_LATEST_DATE_SOURCE,
                dataset=TWSE_LATEST_DATE_DATASET,
                error_code="official_source_malformed",
                error_type=type(error).__name__,
            )
            return None
        except ProviderError as error:
            self.last_resolution = LatestDateResolution(
                status="unavailable",
                latest_date=None,
                source=TWSE_LATEST_DATE_SOURCE,
                dataset=TWSE_LATEST_DATE_DATASET,
                error_code="official_source_unavailable",
                error_type=type(error).__name__,
            )
            return None
        except (KeyError, TypeError, ValueError) as error:
            self.last_resolution = LatestDateResolution(
                status="malformed",
                latest_date=None,
                source=TWSE_LATEST_DATE_SOURCE,
                dataset=TWSE_LATEST_DATE_DATASET,
                error_code="official_source_malformed",
                error_type=type(error).__name__,
            )
            return None

        self.last_resolution = LatestDateResolution(
            status="resolved",
            latest_date=resolved,
            source=TWSE_LATEST_DATE_SOURCE,
            dataset=TWSE_LATEST_DATE_DATASET,
        )
        return resolved


def create_latest_date_provider(
    database_path: str | Path,
) -> OfficialTwseLatestDateProvider:
    """Create the formal provider without opening the database or network."""

    _require_absolute_database_path(database_path)
    probe_symbol = os.environ.get(
        LATEST_DATE_PROBE_SYMBOL_ENV,
        DEFAULT_LATEST_DATE_PROBE_SYMBOL,
    )
    raw_timeout = os.environ.get(
        LATEST_DATE_TIMEOUT_ENV,
        str(DEFAULT_LATEST_DATE_TIMEOUT_SECONDS),
    )
    try:
        timeout = float(raw_timeout)
    except (TypeError, ValueError) as error:
        raise LatestDateProviderError(
            f"{LATEST_DATE_TIMEOUT_ENV} must be a positive number"
        ) from error
    return OfficialTwseLatestDateProvider(
        probe_symbol=probe_symbol,
        timeout_seconds=timeout,
    )


def _require_absolute_database_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise LatestDateProviderError("database_path must be absolute")
    return path.resolve()


def _require_date(value: object, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise LatestDateProviderError(f"{field_name} must be a date")
    return value


__all__ = [
    "DEFAULT_LATEST_DATE_PROBE_SYMBOL",
    "DEFAULT_LATEST_DATE_TIMEOUT_SECONDS",
    "LATEST_DATE_PROVIDER_CONTRACT_VERSION",
    "LATEST_DATE_PROBE_SYMBOL_ENV",
    "LATEST_DATE_TIMEOUT_ENV",
    "LatestDateProviderError",
    "LatestDateResolution",
    "OfficialTwseLatestDateProvider",
    "PRODUCTION_LATEST_DATE_FACTORY_LOCATOR",
    "TWSE_LATEST_DATE_DATASET",
    "TWSE_LATEST_DATE_SOURCE",
    "create_latest_date_provider",
]
