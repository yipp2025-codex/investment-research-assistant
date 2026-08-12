import pytest

from app.providers import ProviderInvalidPayloadError, ProviderTimeoutError
from app.providers._http_response import read_bounded_response_body


class _ChunkedResponse:
    def __init__(
        self,
        body: bytes,
        *,
        headers: dict[str, str] | None = None,
        after_read=None,
    ) -> None:
        self.body = body
        self.headers = headers or {}
        self.offset = 0
        self.after_read = after_read

    def read(self, size: int) -> bytes:
        chunk = self.body[self.offset : self.offset + size]
        self.offset += len(chunk)
        if self.after_read is not None:
            self.after_read()
        return chunk


def _read(response, *, max_bytes=8, timeout_seconds=1.0, clock=None) -> bytes:
    kwargs = {
        "max_bytes": max_bytes,
        "timeout_seconds": timeout_seconds,
        "source_name": "Synthetic provider",
    }
    if clock is not None:
        kwargs["clock"] = clock
    return read_bounded_response_body(response, **kwargs)


def test_bounded_reader_preserves_legitimate_body() -> None:
    assert _read(_ChunkedResponse(b"payload")) == b"payload"


def test_bounded_reader_rejects_chunked_body_over_limit() -> None:
    with pytest.raises(ProviderInvalidPayloadError, match="exceeds"):
        _read(_ChunkedResponse(b"123456789"))


@pytest.mark.parametrize("content_length", ["-1", "1, 1", "invalid"])
def test_bounded_reader_rejects_malformed_content_length(content_length) -> None:
    response = _ChunkedResponse(
        b"x",
        headers={"Content-Length": content_length},
    )

    with pytest.raises(ProviderInvalidPayloadError, match="Content-Length"):
        _read(response)


def test_bounded_reader_rejects_pathologically_long_content_length() -> None:
    response = _ChunkedResponse(
        b"x",
        headers={"Content-Length": "9" * 10_000},
    )

    with pytest.raises(ProviderInvalidPayloadError, match="exceeds"):
        _read(response)


def test_bounded_reader_enforces_elapsed_time_against_slow_drip() -> None:
    now = [0.0]

    def advance() -> None:
        now[0] += 0.6

    response = _ChunkedResponse(b"ab", after_read=advance)

    with pytest.raises(ProviderTimeoutError, match="read timeout"):
        _read(response, timeout_seconds=1.0, clock=lambda: now[0])
