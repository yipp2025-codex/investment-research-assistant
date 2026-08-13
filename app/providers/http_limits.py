"""Shared bounded-read guards for official provider HTTP transports."""

from __future__ import annotations

import time
from collections.abc import Mapping

from .base import ProviderInvalidPayloadError, ProviderTimeoutError


# Deliberately generous for TWSE all-symbol responses while still bounding an
# untrusted response before it reaches JSON parsing or hashing.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_RETRY_AFTER_SECONDS = 300.0
_READ_CHUNK_BYTES = 64 * 1024


def remaining_timeout(deadline: float) -> float:
    """Return the remaining socket timeout or fail closed at the deadline."""

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ProviderTimeoutError("provider response deadline exceeded")
    return max(0.001, remaining)


def read_bounded_body(response: object, *, deadline: float) -> bytes:
    """Read a provider response without exceeding size or total-time limits."""

    headers = getattr(response, "headers", {})
    content_length = _header_value(headers, "content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length.strip())
        except (TypeError, ValueError) as error:
            raise ProviderInvalidPayloadError(
                "provider response Content-Length is invalid"
            ) from error
        if declared_length < 0:
            raise ProviderInvalidPayloadError(
                "provider response Content-Length is negative"
            )
        if declared_length > MAX_RESPONSE_BYTES:
            raise ProviderInvalidPayloadError(
                "provider response exceeds the bounded byte limit"
            )

    body = bytearray()
    while True:
        remaining_timeout(deadline)
        read_size = min(_READ_CHUNK_BYTES, MAX_RESPONSE_BYTES + 1 - len(body))
        chunk = response.read(read_size)  # type: ignore[attr-defined]
        if not isinstance(chunk, (bytes, bytearray)):
            raise ProviderInvalidPayloadError("provider response body must be bytes")
        if not chunk:
            break
        body.extend(chunk)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ProviderInvalidPayloadError(
                "provider response exceeds the bounded byte limit"
            )

    remaining_timeout(deadline)
    return bytes(body)


def _header_value(headers: Mapping[object, object], name: str) -> str | None:
    for key, value in headers.items():
        if str(key).casefold() == name.casefold():
            return str(value)
    return None


__all__ = [
    "MAX_RESPONSE_BYTES",
    "MAX_RETRY_AFTER_SECONDS",
    "read_bounded_body",
    "remaining_timeout",
]
