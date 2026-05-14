from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _get(mapping: dict[Any, Any], key: str, default: Any = None) -> Any:
    if key in mapping:
        return mapping[key]
    raw = key.encode()
    if raw in mapping:
        return mapping[raw]
    return default


def _str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def _int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    return int(value)


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 3
    backoff: str = "fixed"
    base_ms: int = 100
    max_ms: int = 1_000
    jitter_pct: int = 0
    exhausted_to: str = "failed"


@dataclass(frozen=True)
class ChildSpec:
    id: str
    type: str
    payload: bytes = b""
    partition_key: str | None = None


@dataclass(frozen=True)
class CreateItem:
    id: str
    payload: Any = None
    partition_key: str | None = None


@dataclass(frozen=True)
class ClaimedItem:
    id: str
    lease_token: bytes
    fencing_token: int
    partition_key: str | None = None


@dataclass(frozen=True)
class FencedItem:
    id: str
    fencing_token: int
    lease_token: bytes | None = None
    partition_key: str | None = None


@dataclass(frozen=True)
class FlowRecord:
    id: str
    type: str
    state: str
    partition_key: str
    payload: Any = None
    lease_token: bytes = b""
    fencing_token: int = 0
    version: int = 0
    parent_flow_id: str | None = None
    root_flow_id: str | None = None
    correlation_id: str | None = None
    raw: dict[Any, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any], payload: Any = None) -> FlowRecord:
        return cls(
            id=_str(_get(value, "id")),
            type=_str(_get(value, "type")),
            state=_str(_get(value, "state")),
            partition_key=_str(_get(value, "partition_key")),
            payload=payload,
            lease_token=_bytes(_get(value, "lease_token")),
            fencing_token=_int(_get(value, "fencing_token")),
            version=_int(_get(value, "version")),
            parent_flow_id=_optional_str(_get(value, "parent_flow_id")),
            root_flow_id=_optional_str(_get(value, "root_flow_id")),
            correlation_id=_optional_str(_get(value, "correlation_id")),
            raw=value,
        )


def _optional_str(value: Any) -> str | None:
    if value is None or value == b"" or value == "":
        return None
    return _str(value)
