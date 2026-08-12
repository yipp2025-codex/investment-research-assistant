import configparser
import importlib.metadata
import sys
import types
from pathlib import Path
from threading import Event, Thread

import pytest

from app.providers import (
    EsunSdkHttpTransport,
    ProviderInvalidPayloadError,
    ProviderPermanentError,
    ProviderTimeoutError,
)
from app.providers.esun_sdk import _NoRedirectHandler


def _config(tmp_path: Path) -> Path:
    certificate = tmp_path / "synthetic-test-certificate.p12"
    certificate.write_bytes(b"not-a-real-certificate")
    parser = configparser.ConfigParser(interpolation=None)
    parser.read_dict(
        {
            "Core": {"Entry": "simulation", "Environment": "simulation"},
            "Cert": {"Path": str(certificate)},
            "Api": {
                "Key": "synthetic-test-key",
                "Secret": "synthetic-test-secret",
            },
            "User": {"Account": "synthetic-test-account"},
        }
    )
    config_path = tmp_path / "synthetic-test-config.ini"
    with config_path.open("w", encoding="utf-8") as stream:
        parser.write(stream)
    return config_path


def _install_fake_modules(
    monkeypatch,
    *,
    base_url: str = "https://api.fugle.tw/marketdata/v1.0/stock",
    login_error: Exception | None = None,
    credentials_ready: bool = True,
    keyring_reads: list[tuple[str, str]] | None = None,
    login_calls: list[str] | None = None,
) -> None:
    keyring = types.ModuleType("keyring")

    def get_password(service, account):
        if keyring_reads is not None:
            keyring_reads.append((service, account))
        return "synthetic-cached-password" if credentials_ready else None

    keyring.get_password = get_password
    monkeypatch.setitem(sys.modules, "keyring", keyring)

    module = types.ModuleType("esun_marketdata")

    class FakeEsunMarketdata:
        def __init__(self, config) -> None:
            stock = types.SimpleNamespace(
                config={
                    "base_url": base_url,
                    "sdk_token": "synthetic-runtime-token",
                }
            )
            self.rest_client = types.SimpleNamespace(stock=stock)

        def login(self) -> None:
            if login_calls is not None:
                login_calls.append("login")
            if login_error is not None:
                raise login_error

    module.EsunMarketdata = FakeEsunMarketdata
    monkeypatch.setitem(sys.modules, "esun_marketdata", module)


class _FakeResponse:
    status = 200
    headers = {"Content-Type": "application/json"}

    def __init__(
        self,
        url: str,
        *,
        body: bytes = b'{"ok":true}',
        headers: dict[str, str] | None = None,
    ) -> None:
        self._url = url
        self._body = body
        self._offset = 0
        if headers is not None:
            self.headers = headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._body) - self._offset
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def geturl(self) -> str:
        return self._url


class _RedirectedFakeResponse(_FakeResponse):
    def __init__(self, effective_url: str) -> None:
        super().__init__(effective_url)


class _OversizedFakeResponse(_FakeResponse):
    def __init__(self, effective_url: str) -> None:
        super().__init__(
            effective_url,
            headers={
                "Content-Type": "application/json",
                "Content-Length": "999999999",
            },
        )
        self.read_called = False

    def read(self, size: int = -1) -> bytes:
        self.read_called = True
        return super().read(size)


def test_esun_sdk_transport_authenticates_then_sends_one_bounded_get(
    tmp_path, monkeypatch
) -> None:
    config_path = _config(tmp_path)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "2.2.0")
    _install_fake_modules(monkeypatch)
    captured: list[tuple[object, float]] = []

    def opener(request, *, timeout):
        captured.append((request, timeout))
        return _FakeResponse(request.full_url)

    transport = EsunSdkHttpTransport(config_path)
    transport._opener = opener

    response = transport.get(
        "/historical/candles/2330",
        params={"from": "2026-08-05", "to": "2026-08-05"},
        timeout_seconds=2.5,
    )

    assert response.status_code == 200
    assert len(captured) == 1
    request, timeout = captured[0]
    assert timeout == 2.5
    assert request.get_method() == "GET"
    assert request.full_url.startswith(
        "https://api.fugle.tw/marketdata/v1.0/stock/historical/candles/2330?"
    )
    assert "synthetic-runtime-token" not in request.full_url
    headers = {key.lower(): value for key, value in request.header_items()}
    assert headers["x-sdk-token"] == "synthetic-runtime-token"


def test_esun_sdk_transport_reuses_one_authenticated_session(
    tmp_path, monkeypatch
) -> None:
    config_path = _config(tmp_path)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "2.2.0")
    keyring_reads: list[tuple[str, str]] = []
    login_calls: list[str] = []
    _install_fake_modules(
        monkeypatch,
        keyring_reads=keyring_reads,
        login_calls=login_calls,
    )
    captured: list[object] = []

    def opener(request, *, timeout):
        captured.append(request)
        return _FakeResponse(request.full_url)

    transport = EsunSdkHttpTransport(config_path)
    transport._opener = opener

    transport.get(
        "/intraday/ticker/2330", params=None, timeout_seconds=1.0
    )
    transport.get(
        "/historical/candles/2317",
        params={"from": "2026-08-07", "to": "2026-08-07"},
        timeout_seconds=1.0,
    )

    assert login_calls == ["login"]
    assert keyring_reads == [
        ("esun_trade_sdk:account", "synthetic-test-account"),
        ("esun_trade_sdk:cert", "synthetic-test-account"),
    ]
    assert len(captured) == 2


@pytest.mark.parametrize(
    "effective_url",
    [
        "https://redirect-sink.invalid/capture",
        "http://api.fugle.tw/marketdata/v1.0/stock/intraday/ticker/2330",
        "https://api.fugle.tw:8443/marketdata/v1.0/stock/intraday/ticker/2330",
    ],
)
def test_esun_sdk_transport_rejects_unverified_effective_url(
    tmp_path, monkeypatch, effective_url
) -> None:
    config_path = _config(tmp_path)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "2.2.0")
    _install_fake_modules(monkeypatch)
    captured: list[object] = []

    def opener(request, *, timeout):
        del timeout
        captured.append(request)
        return _RedirectedFakeResponse(effective_url)

    transport = EsunSdkHttpTransport(config_path)
    transport._opener = opener

    with pytest.raises(ProviderPermanentError, match="redirect"):
        transport.get(
            "/intraday/ticker/2330", params=None, timeout_seconds=1.0
        )

    assert len(captured) == 1


def test_esun_transport_rejects_oversized_content_length_before_read(
    tmp_path, monkeypatch
) -> None:
    config_path = _config(tmp_path)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "2.2.0")
    _install_fake_modules(monkeypatch)
    captured_response: list[_OversizedFakeResponse] = []

    def opener(request, *, timeout):
        del timeout
        response = _OversizedFakeResponse(request.full_url)
        captured_response.append(response)
        return response

    transport = EsunSdkHttpTransport(config_path)
    transport._opener = opener

    with pytest.raises(ProviderInvalidPayloadError, match="exceeds"):
        transport.get(
            "/intraday/ticker/2330", params=None, timeout_seconds=1.0
        )

    assert len(captured_response) == 1
    assert captured_response[0].read_called is False


def test_esun_default_redirect_handler_never_follows_location() -> None:
    handler = _NoRedirectHandler()

    assert (
        handler.redirect_request(
            object(),
            object(),
            302,
            "Found",
            {"Location": "https://redirect-sink.invalid/capture"},
            "https://redirect-sink.invalid/capture",
        )
        is None
    )


def test_esun_sdk_transport_serializes_concurrent_authentication(tmp_path) -> None:
    transport = EsunSdkHttpTransport(tmp_path / "unused-synthetic-config.ini")
    authentication_started = Event()
    release_authentication = Event()
    authentication_calls: list[str] = []
    errors: list[Exception] = []

    def authenticate() -> None:
        authentication_calls.append("authenticate")
        authentication_started.set()
        release_authentication.wait(timeout=2.0)
        transport._base_url = "https://api.fugle.tw/marketdata/v1.0/stock"
        transport._sdk_token = "synthetic-runtime-token"

    def ensure_authenticated() -> None:
        try:
            transport._ensure_authenticated()
        except Exception as error:  # pragma: no cover - asserted below.
            errors.append(error)

    transport._authenticate = authenticate  # type: ignore[method-assign]
    first = Thread(target=ensure_authenticated)
    second = Thread(target=ensure_authenticated)
    first.start()
    assert authentication_started.wait(timeout=1.0)
    second.start()
    release_authentication.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert authentication_calls == ["authenticate"]


def test_esun_sdk_transport_rejects_unverified_sdk_version(
    tmp_path, monkeypatch
) -> None:
    config_path = _config(tmp_path)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "9.9.9")

    with pytest.raises(ProviderPermanentError, match="version"):
        EsunSdkHttpTransport(config_path).get(
            "/intraday/ticker/2330", params=None, timeout_seconds=1.0
        )


def test_esun_sdk_transport_requires_initialized_keyring(tmp_path, monkeypatch) -> None:
    config_path = _config(tmp_path)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "2.2.0")
    _install_fake_modules(monkeypatch, credentials_ready=False)

    with pytest.raises(ProviderPermanentError, match="bootstrap"):
        EsunSdkHttpTransport(config_path).get(
            "/intraday/ticker/2330", params=None, timeout_seconds=1.0
        )


@pytest.mark.parametrize(
    "base_url",
    [
        "https://unverified.fixture.invalid/marketdata/v1.0/stock",
        "https://api.fugle.tw:8443/marketdata/v1.0/stock",
    ],
)
def test_esun_sdk_transport_rejects_unverified_market_data_origin(
    tmp_path, monkeypatch, base_url
) -> None:
    config_path = _config(tmp_path)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "2.2.0")
    _install_fake_modules(
        monkeypatch,
        base_url=base_url,
    )

    with pytest.raises(ProviderPermanentError, match="unverified") as caught:
        EsunSdkHttpTransport(config_path).get(
            "/intraday/ticker/2330", params=None, timeout_seconds=1.0
        )

    assert base_url not in str(caught.value)
    assert "synthetic-runtime-token" not in str(caught.value)


def test_esun_sdk_authentication_timeout_maps_to_provider_timeout(
    tmp_path, monkeypatch
) -> None:
    config_path = _config(tmp_path)
    monkeypatch.setattr(importlib.metadata, "version", lambda name: "2.2.0")
    _install_fake_modules(monkeypatch, login_error=TimeoutError("synthetic timeout"))

    with pytest.raises(ProviderTimeoutError, match="authentication timed out"):
        EsunSdkHttpTransport(config_path).get(
            "/intraday/ticker/2330", params=None, timeout_seconds=1.0
        )
