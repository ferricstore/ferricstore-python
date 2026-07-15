from __future__ import annotations

from collections.abc import Iterator
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


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value is None or value == b"" or value == "":
        return None
    return _str(value)


def _optional_str_or_int(value: Any) -> str | int | None:
    if value is None or value == b"" or value == "":
        return None
    if type(value) is int:
        return value
    return _str(value)


def _normalize_ref_meta(value: Any) -> Any:
    if isinstance(value, dict):
        return {_str(key): _normalize_ref_meta(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_ref_meta(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_ref_meta(item) for item in value)
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return value
    return value


def _str_key_map(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {_str(key): _normalize_ref_meta(item) for key, item in value.items()}


def _raw_map(value: dict[Any, Any]) -> dict[str, Any]:
    return {_str(key): _normalize_ref_meta(item) for key, item in value.items()}


class _MappingResult:
    raw: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw or {})

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def items(self) -> Iterator[tuple[str, Any]]:
        return iter(self.to_dict().items())


__all__ = [
    "_MappingResult",
    "_bytes",
    "_get",
    "_int",
    "_normalize_ref_meta",
    "_optional_int",
    "_optional_str",
    "_optional_str_or_int",
    "_raw_map",
    "_str",
    "_str_key_map",
]
