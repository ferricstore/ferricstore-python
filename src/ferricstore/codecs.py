from __future__ import annotations

import json
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Codec(Protocol):
    def encode(self, value: Any) -> bytes:
        """Encode user payload/result into bytes."""

    def decode(self, value: bytes | None) -> Any:
        """Decode user payload/result bytes."""


class RawCodec:
    """Raw bytes codec. Fast default; no JSON unless user asks."""

    def encode(self, value: Any) -> bytes:
        if value is None:
            return b""
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        if isinstance(value, str):
            return value.encode()
        raise TypeError("RawCodec accepts bytes, bytearray, str, or None")

    def decode(self, value: bytes | None) -> bytes | None:
        return value


class JsonCodec:
    """JSON codec for users who want language-neutral structured payloads."""

    def encode(self, value: Any) -> bytes:
        return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()

    def decode(self, value: bytes | None) -> Any:
        if value is None:
            return None
        return json.loads(value.decode())
