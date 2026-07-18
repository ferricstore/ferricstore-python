from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ferricstore.command_helpers import _validate_mset_slots
from ferricstore.errors import InvalidCommandError
from ferricstore.protocol_constants import (
    _COMPACT_PIPELINE_HEADER,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_U32,
)

_INVALID_COMPACT_MSET = "MSET payload must be a valid compact key/value payload"


def _validate_mset_keys(keys: Sequence[Any]) -> None:
    _validate_mset_slots(list(keys))


def _parse_compact_mset_payload(payload: object) -> tuple[bytes, ...]:
    """Parse one structurally valid compact MSET payload and return its keys."""
    if not isinstance(payload, bytes) or len(payload) < _COMPACT_PIPELINE_HEADER.size:
        raise InvalidCommandError(_INVALID_COMPACT_MSET)

    marker, mode, item_count = _COMPACT_PIPELINE_HEADER.unpack_from(payload)
    if marker != _COMPACT_PIPELINE_REQUEST or mode != 1 or item_count == 0:
        raise InvalidCommandError(_INVALID_COMPACT_MSET)

    offset = _COMPACT_PIPELINE_HEADER.size
    payload_size = len(payload)
    keys: list[bytes] = []
    for _ in range(item_count):
        if offset + _COMPACT_U32.size > payload_size:
            raise InvalidCommandError(_INVALID_COMPACT_MSET)
        key_size = _COMPACT_U32.unpack_from(payload, offset)[0]
        offset += _COMPACT_U32.size
        key_end = offset + key_size
        if key_end > payload_size:
            raise InvalidCommandError(_INVALID_COMPACT_MSET)
        keys.append(payload[offset:key_end])
        offset = key_end

        if offset + _COMPACT_U32.size > payload_size:
            raise InvalidCommandError(_INVALID_COMPACT_MSET)
        value_size = _COMPACT_U32.unpack_from(payload, offset)[0]
        offset += _COMPACT_U32.size
        value_end = offset + value_size
        if value_end > payload_size:
            raise InvalidCommandError(_INVALID_COMPACT_MSET)
        offset = value_end

    if offset != payload_size:
        raise InvalidCommandError(_INVALID_COMPACT_MSET)
    return tuple(keys)


def _validate_compact_mset_payload(payload: object) -> tuple[bytes, ...]:
    """Validate one compact MSET payload, including its single-slot contract."""
    keys = _parse_compact_mset_payload(payload)
    _validate_mset_keys(keys)
    return keys


__all__ = [
    "_parse_compact_mset_payload",
    "_validate_compact_mset_payload",
    "_validate_mset_keys",
]
