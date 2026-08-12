"""Credential-safe authenticated HTTP transport for official E.SUN market data."""

from __future__ import annotations

import importlib.metadata
import socket
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from configparser import ConfigParser
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Protocol

from .base import (
    ProviderError,
    ProviderPermanentError,
    ProviderTemporaryError,
    ProviderTimeoutError,
)


VERIFIED_ESUN_SDK_VERSION = "2.2.0"
_ALLOWED_REST_HOSTS = frozenset({"api.fugle.tw"})
_EXPECTED_REST_PATH_PREFIX = "/marketdata/v1.0/stock"


@dataclass(frozen=True, slots=True)
class EsunHttpResponse:
    status_code: int
    body: bytes
    headers: Mapping[str, str]
    url: str
    fetched_at: datetime


class EsunHttpTransport(Protocol):
    def get(
        self,
        path: str,
        *,
        params: Mapping[str, object] | None,
        timeout_seconds: float,
    ) -> EsunHttpResponse:
        """Perform exactly one authenticated, read-only HTTP GET."""


class EsunSdkHttpTransport:
    """Use SDK 2.2.0 only for login/token exchange, then bounded HTTP GETs."""

    def __init__(
        self,
        config_path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
        opener: Callable[..., object] | None = None,
    ) -> None:
        self.config_path = Path(config_path).expanduser()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._opener = opener or urllib.request.urlopen
        self._base_url: str | None = None
        self._sdk_token: str | None = None
        self._authentication_lock = Lock()

    def get(
        self,
        path: str,
        *,
        params: Mapping[str, object] | None,
        timeout_seconds: float,
    ) -> EsunHttpResponse:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        if (
            not path.startswith("/")
            or ".." in path
            or "?" in path
            or "#" in path
        ):
            raise ProviderPermanentError("E.SUN endpoint path is not allowed")
        self._ensure_authenticated()
        assert self._base_url is not None and self._sdk_token is not None

        query = ""
        if params:
            query = "?" + urllib.parse.urlencode(
                {key: str(value) for key, value in params.items()}
            )
        url = self._base_url + path + query
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "investment-research-assistant/0.5 read-only",
                "X-SDK-TOKEN": self._sdk_token,
            },
            method="GET",
        )
        try:
            with self._opener(request, timeout=timeout_seconds) as response:
                return EsunHttpResponse(
                    status_code=int(response.status),
                    body=response.read(),
                    headers={
                        key.lower(): value for key, value in response.headers.items()
                    },
                    url=url,
                    fetched_at=self._aware_now(),
                )
        except urllib.error.HTTPError as error:
            return EsunHttpResponse(
                status_code=int(error.code),
                body=error.read(),
                headers={key.lower(): value for key, value in error.headers.items()},
                url=url,
                fetched_at=self._aware_now(),
            )
        except (TimeoutError, socket.timeout) as error:
            raise ProviderTimeoutError("E.SUN market-data request timed out") from error
        except urllib.error.URLError as error:
            if isinstance(error.reason, (TimeoutError, socket.timeout)):
                raise ProviderTimeoutError(
                    "E.SUN market-data request timed out"
                ) from error
            raise ProviderTemporaryError(
                "E.SUN market-data connection failed"
            ) from error
        except OSError as error:
            raise ProviderTemporaryError(
                "E.SUN market-data transport failed"
            ) from error

    def _ensure_authenticated(self) -> None:
        if self._base_url is not None and self._sdk_token is not None:
            return

        # A transport represents one SDK login session.  Daily batch symbols
        # share the same provider/transport instance, so authenticate once and
        # reuse the bounded market-data token for the rest of that process.
        # The second check prevents duplicate logins if callers become
        # concurrent in the future.
        with self._authentication_lock:
            if self._base_url is not None and self._sdk_token is not None:
                return
            self._authenticate()

    def _authenticate(self) -> None:
        config_path = self.config_path.resolve()
        if not config_path.is_file():
            raise ProviderPermanentError(
                "E.SUN market-data config path does not reference a file"
            )

        config = ConfigParser(interpolation=None)
        try:
            loaded = config.read(config_path, encoding="utf-8-sig")
        except (OSError, UnicodeError):
            raise ProviderPermanentError(
                "E.SUN market-data config could not be read"
            ) from None
        if not loaded:
            raise ProviderPermanentError(
                "E.SUN market-data config could not be read"
            )
        required_sections = ("Core", "Cert", "Api", "User")
        if not all(config.has_section(section) for section in required_sections):
            raise ProviderPermanentError(
                "E.SUN market-data config is missing required sections"
            )
        required_options = (
            ("Core", "Entry"),
            ("Core", "Environment"),
            ("Api", "Key"),
            ("Api", "Secret"),
            ("User", "Account"),
        )
        if any(
            not config.get(section, option, fallback="").strip()
            for section, option in required_options
        ):
            raise ProviderPermanentError(
                "E.SUN market-data config is missing required values"
            )

        cert_value = config.get("Cert", "Path", fallback="").strip()
        cert_path = Path(cert_value).expanduser()
        if not cert_path.is_absolute():
            cert_path = config_path.parent / cert_path
        cert_path = cert_path.resolve()
        if cert_path.suffix.lower() != ".p12" or not cert_path.is_file():
            raise ProviderPermanentError(
                "E.SUN certificate path is not a readable .p12 file"
            )
        config.set("Cert", "Path", str(cert_path))

        try:
            installed_version = importlib.metadata.version("esun_marketdata")
        except importlib.metadata.PackageNotFoundError:
            raise ProviderPermanentError(
                "official esun_marketdata SDK 2.2.0 is not installed"
            ) from None
        if installed_version != VERIFIED_ESUN_SDK_VERSION:
            raise ProviderPermanentError(
                "installed esun_marketdata version is outside the verified contract"
            )

        account = config.get("User", "Account", fallback="").strip()
        try:
            import keyring

            credentials_ready = bool(
                account
                and keyring.get_password("esun_trade_sdk:account", account)
                and keyring.get_password("esun_trade_sdk:cert", account)
            )
        except Exception:
            credentials_ready = False
        if not credentials_ready:
            raise ProviderPermanentError(
                "E.SUN SDK passwords are not initialized; run the bootstrap helper"
            )

        try:
            from esun_marketdata import EsunMarketdata

            sdk = EsunMarketdata(config)
            sdk.login()
            stock_config = sdk.rest_client.stock.config
            base_url = str(stock_config.get("base_url", "")).rstrip("/")
            sdk_token = str(stock_config.get("sdk_token", ""))
        except ProviderError:
            raise
        except (TimeoutError, socket.timeout) as error:
            raise ProviderTimeoutError(
                "E.SUN SDK authentication timed out"
            ) from error
        except OSError as error:
            raise ProviderTemporaryError(
                "E.SUN SDK authentication connection failed"
            ) from error
        except Exception as error:
            error_name = type(error).__name__.lower()
            if "timeout" in error_name:
                raise ProviderTimeoutError(
                    "E.SUN SDK authentication timed out"
                ) from error
            if "connection" in error_name:
                raise ProviderTemporaryError(
                    "E.SUN SDK authentication connection failed"
                ) from error
            raise ProviderPermanentError(
                "E.SUN SDK authentication failed"
            ) from None

        parsed = urllib.parse.urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in _ALLOWED_REST_HOSTS
            or parsed.path.rstrip("/") != _EXPECTED_REST_PATH_PREFIX
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ProviderPermanentError(
                "E.SUN SDK returned an unverified market-data REST URL"
            )
        if not sdk_token:
            raise ProviderPermanentError(
                "E.SUN SDK did not return a market-data token"
            )
        self._base_url = base_url
        self._sdk_token = sdk_token

    def _aware_now(self) -> datetime:
        now = self.clock()
        if now.utcoffset() is None:
            raise ProviderPermanentError(
                "E.SUN transport clock must be timezone-aware"
            )
        return now
