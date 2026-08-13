from __future__ import annotations

import pytest

from app.providers import ProviderTimeoutError
from app.providers.http_limits import read_bounded_body


class _ChunkedResponse:
    headers: dict[str, str] = {}

    def __init__(self, *chunks: bytes) -> None:
        self.chunks = list(chunks)

    def read(self, size: int = -1) -> bytes:
        del size
        return self.chunks.pop(0) if self.chunks else b""


def test_bounded_body_enforces_total_deadline_between_chunks(monkeypatch) -> None:
    import app.providers.http_limits as http_limits

    clock = iter((0.0, 2.0))
    monkeypatch.setattr(http_limits.time, "monotonic", lambda: next(clock))

    with pytest.raises(ProviderTimeoutError, match="deadline"):
        read_bounded_body(
            _ChunkedResponse(b"first", b"second"),
            deadline=1.0,
        )
