"""Hash-only source evidence helpers used at provider response boundaries."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any

from app.models import SourceArtifact


def source_artifact_from_bytes(
    *,
    provider: str,
    dataset: str,
    endpoint: str,
    contract_version: str,
    body: bytes,
    headers: Mapping[str, str] | None = None,
    fetched_at: datetime | None = None,
    hash_basis: str = "raw-response-bytes-v1",
) -> SourceArtifact:
    """Describe exact response bytes without retaining or logging those bytes."""

    content_type = "application/octet-stream"
    if headers:
        header_value = next(
            (
                value
                for key, value in headers.items()
                if key.lower() == "content-type" and value.strip()
            ),
            None,
        )
        if header_value is not None:
            content_type = header_value.strip()
    return SourceArtifact(
        provider=provider,
        dataset=dataset,
        endpoint=endpoint,
        contract_version=contract_version,
        content_type=content_type,
        payload_sha256=hashlib.sha256(body).hexdigest(),
        payload_size_bytes=len(body),
        hash_basis=hash_basis,
        fetched_at=fetched_at,
    )


def source_artifact_from_json(
    *,
    provider: str,
    dataset: str,
    endpoint: str,
    contract_version: str,
    payload: object,
    fetched_at: datetime | None = None,
) -> SourceArtifact:
    """Hash a deterministic synthetic JSON envelope when no HTTP bytes exist."""

    body = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    ).encode("utf-8")
    return source_artifact_from_bytes(
        provider=provider,
        dataset=dataset,
        endpoint=endpoint,
        contract_version=contract_version,
        body=body,
        headers={"content-type": "application/json"},
        fetched_at=fetched_at,
        hash_basis="canonical-json-v1",
    )


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")
