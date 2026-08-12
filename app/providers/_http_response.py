"""Shared fail-closed limits for urllib response bodies."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping

from .base import ProviderInvalidPayloadError, ProviderTimeoutError


_READ_CHUNK_BYTES = 64 * 1024


def read_bounded_response_body(
    response: object,
    *,
    max_bytes: int,
    timeout_seconds: float,
    source_name: str,
    clock: Callable[[], float] = time.monotonic,
) -> bytes:
    """Read one HTTP body within an explicit byte cap and elapsed-time budget."""

    if max_bytes < 1:
        raise ValueError("max_bytes must be at least 1")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero")

    headers = getattr(response, "headers", {})
    declared_length = _content_length(headers, source_name=source_name)
    if declared_length is not None and declared_length > max_bytes:
        raise ProviderInvalidPayloadError(
            f"{source_name} response exceeds the configured size limit"
        )

    read = getattr(response, "read", None)
    if not callable(read):
        raise ProviderInvalidPayloadError(
            f"{source_name} response body is not readable"
        )

    started_at = clock()
    chunks: list[bytes] = []
    total_bytes = 0
    while True:
        _check_deadline(
            clock,
            started_at=started_at,
            timeout_seconds=timeout_seconds,
            source_name=source_name,
        )
        remaining_probe_bytes = max_bytes + 1 - total_bytes
        chunk = read(min(_READ_CHUNK_BYTES, remaining_probe_bytes))
        _check_deadline(
            clock,
            started_at=started_at,
            timeout_seconds=timeout_seconds,
            source_name=source_name,
        )
        if not isinstance(chunk, (bytes, bytearray)):
            raise ProviderInvalidPayloadError(
                f"{source_name} response body is not binary data"
            )
        if not chunk:
            break
        chunk_bytes = bytes(chunk)
        total_bytes += len(chunk_bytes)
        if total_bytes > max_bytes:
            raise ProviderInvalidPayloadError(
                f"{source_name} response exceeds the configured size limit"
            )
        chunks.append(chunk_bytes)

    return b"".join(chunks)


def _content_length(
    headers: object,
    *,
    source_name: str,
) -> int | None:
    if not isinstance(headers, Mapping) and not hasattr(headers, "get"):
        raise ProviderInvalidPayloadError(
            f"{source_name} response headers are invalid"
        )
    raw_length = headers.get("Content-Length")
    if raw_length is None:
        raw_length = headers.get("content-length")
    if raw_length is None:
        return None
    normalized = str(raw_length).strip()
    if not normalized or not normalized.isascii() or not normalized.isdigit():
        raise ProviderInvalidPayloadError(
            f"{source_name} response Content-Length is invalid"
        )
    if len(normalized) > 20:
        raise ProviderInvalidPayloadError(
            f"{source_name} response exceeds the configured size limit"
        )
    return int(normalized)


def _check_deadline(
    clock: Callable[[], float],
    *,
    started_at: float,
    timeout_seconds: float,
    source_name: str,
) -> None:
    if clock() - started_at > timeout_seconds:
        raise ProviderTimeoutError(
            f"{source_name} response body exceeded its read timeout"
        )
