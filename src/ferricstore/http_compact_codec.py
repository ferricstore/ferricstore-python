from __future__ import annotations

import importlib
from typing import Any, cast

from ferricstore.errors import HttpError


class MessagePackCodec:
    """Lazily loaded codec behind the optional compact transport extra."""

    def __init__(self) -> None:
        try:
            self._msgpack = importlib.import_module("msgpack")
        except ImportError as exc:
            raise ImportError(
                "compact HTTP envelopes require the 'ferricstore[compact]' optional dependency"
            ) from exc

    def pack(self, value: Any) -> bytes:
        try:
            return cast(bytes, self._msgpack.packb(value, use_bin_type=True))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(
                "HTTP request body must contain MessagePack-compatible values"
            ) from exc

    def unpack_object(self, raw: bytes, *, status_code: int) -> dict[str, Any]:
        try:
            value = self._msgpack.unpackb(raw, raw=False, strict_map_key=False)
        except Exception as exc:
            raise HttpError(
                "FerricStore HTTP endpoint returned invalid MessagePack",
                status_code=status_code,
                error_code="invalid_response",
                raw=raw,
                retryable=False,
                safe_to_retry=False,
            ) from exc
        if not isinstance(value, dict):
            raise HttpError(
                "FerricStore HTTP endpoint returned a non-object MessagePack response",
                status_code=status_code,
                error_code="invalid_response",
                raw=value,
                retryable=False,
                safe_to_retry=False,
            )
        return cast(dict[str, Any], value)

    def unpack_error(self, raw: bytes, *, status_code: int) -> dict[str, Any]:
        try:
            return self.unpack_object(raw, status_code=status_code)
        except HttpError:
            return {
                "error": {
                    "code": "http_error",
                    "message": f"FerricStore HTTP endpoint returned status {status_code}",
                },
                "raw_body": raw,
            }


__all__ = ["MessagePackCodec"]
