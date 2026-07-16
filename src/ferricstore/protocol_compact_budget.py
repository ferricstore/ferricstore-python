from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from ferricstore.protocol_constants import _HEADER
from ferricstore.protocol_lifecycle import PendingRequestCapacityError


def _pending_request_capacity_error(max_bytes: int | None) -> PendingRequestCapacityError:
    return PendingRequestCapacityError(
        f"protocol pending request bytes exceed max_pending_request_bytes={max_bytes}"
    )


def _compact_payload_budget(
    max_pending_request_bytes: int | None,
    compression: str,
) -> tuple[bool, int | None]:
    """Return whether eager compact encoding is safe and its raw body budget."""

    if max_pending_request_bytes is None:
        return True, None
    if compression == "zlib":
        # Generic encoding streams directly into the compressor. Compact encoding
        # would first materialize the entire uncompressed request body.
        return False, None
    body_limit = max_pending_request_bytes - _HEADER.size
    if body_limit < 0:
        raise _pending_request_capacity_error(max_pending_request_bytes)
    return True, body_limit


@dataclass(frozen=True, slots=True)
class CompactEncodingPolicy:
    """Per-command bounds for eager compact payload construction."""

    enabled: bool = True
    max_payload_bytes: int | None = None
    pending_limit: int | None = None


_UNBOUNDED_POLICY = CompactEncodingPolicy()
_ACTIVE_POLICY: ContextVar[CompactEncodingPolicy] = ContextVar(
    "ferricstore_compact_encoding_policy",
    default=_UNBOUNDED_POLICY,
)


@contextmanager
def compact_encoding_policy(
    *,
    enabled: bool,
    max_payload_bytes: int | None,
    pending_limit: int | None,
) -> Iterator[None]:
    """Apply a transport admission policy to one synchronous command build."""

    token = _ACTIVE_POLICY.set(
        CompactEncodingPolicy(
            enabled=enabled,
            max_payload_bytes=max_payload_bytes,
            pending_limit=pending_limit,
        )
    )
    try:
        yield
    finally:
        _ACTIVE_POLICY.reset(token)


@contextmanager
def transport_compact_encoding_policy(
    max_pending_request_bytes: int | None,
    compression: str,
) -> Iterator[None]:
    enabled, max_payload_bytes = _compact_payload_budget(
        max_pending_request_bytes,
        compression,
    )
    with compact_encoding_policy(
        enabled=enabled,
        max_payload_bytes=max_payload_bytes,
        pending_limit=max_pending_request_bytes,
    ):
        yield


def current_compact_encoding_policy() -> CompactEncodingPolicy:
    return _ACTIVE_POLICY.get()


def _binary_wire_size(value: Any) -> int | None:
    """Return compact length-prefix plus UTF-8 bytes without allocating the encoding."""

    if isinstance(value, bytes):
        return 4 + bytes.__len__(value)
    if not isinstance(value, str):
        return None
    if str.isascii(value):
        return 4 + str.__len__(value)
    size = 4
    for index in range(str.__len__(value)):
        codepoint = ord(str.__getitem__(value, index))
        if codepoint <= 0x7F:
            size += 1
        elif codepoint <= 0x7FF:
            size += 2
        elif 0xD800 <= codepoint <= 0xDFFF:
            return None
        elif codepoint <= 0xFFFF:
            size += 3
        else:
            size += 4
    return size


class _CompactPayloadBudget:
    """Track exact compact wire bytes before each corresponding allocation."""

    __slots__ = ("_max_payload_bytes", "_pending_limit", "size")

    def __init__(
        self,
        max_payload_bytes: int | None = None,
        pending_limit: int | None = None,
        *,
        initial_size: int = 0,
        policy: CompactEncodingPolicy | None = None,
    ) -> None:
        active = current_compact_encoding_policy() if policy is None else policy
        self._max_payload_bytes = (
            active.max_payload_bytes if max_payload_bytes is None else max_payload_bytes
        )
        self._pending_limit = active.pending_limit if pending_limit is None else pending_limit
        self.size = 0
        self.reserve(initial_size)

    def reserve(self, size: int) -> None:
        if size < 0:
            raise ValueError("compact payload reservation must be non-negative")
        if self._max_payload_bytes is not None and size > self._max_payload_bytes - self.size:
            raise _pending_request_capacity_error(self._pending_limit)
        self.size += size

    def can_reserve(self, size: int) -> bool:
        return self._max_payload_bytes is None or size <= self._max_payload_bytes - self.size


def _bounded_maybe_bytes(
    value: Any,
    max_payload_bytes: int | None = None,
    pending_limit: int | None = None,
    *,
    budget: _CompactPayloadBudget | None = None,
) -> bytes | None:
    if isinstance(value, bytes):
        encoded = value
        wire_size = 4 + bytes.__len__(value)
    elif isinstance(value, str):
        encoded = None
        maximum_wire_size = 4 + 4 * str.__len__(value)
        if budget is not None:
            encode_first = budget.can_reserve(maximum_wire_size)
        elif max_payload_bytes is not None:
            encode_first = maximum_wire_size <= max_payload_bytes
        else:
            active = current_compact_encoding_policy()
            if not active.enabled:
                return None
            encode_first = (
                active.max_payload_bytes is None or maximum_wire_size <= active.max_payload_bytes
            )
        if encode_first:
            encoded = str.encode(value)
            wire_size = 4 + bytes.__len__(encoded)
        else:
            measured_wire_size = _binary_wire_size(value)
            if measured_wire_size is None:
                # The only string case without a wire size is invalid UTF-8.
                str.encode(value)
                return None
            wire_size = measured_wire_size
    else:
        return None
    if budget is not None:
        budget.reserve(wire_size)
    elif max_payload_bytes is not None and wire_size > max_payload_bytes:
        raise _pending_request_capacity_error(pending_limit)
    elif max_payload_bytes is None:
        active = current_compact_encoding_policy()
        if not active.enabled:
            return None
        if active.max_payload_bytes is not None and wire_size > active.max_payload_bytes:
            raise _pending_request_capacity_error(active.pending_limit)
    return encoded if encoded is not None else str.encode(value)


def _bounded_optional_bytes(
    value: Any,
    max_payload_bytes: int | None = None,
    pending_limit: int | None = None,
    *,
    budget: _CompactPayloadBudget | None = None,
) -> bytes | None | bool:
    if value is None:
        return None
    encoded = _bounded_maybe_bytes(
        value,
        max_payload_bytes,
        pending_limit,
        budget=budget,
    )
    return encoded if encoded is not None else False


__all__ = [
    "CompactEncodingPolicy",
    "_CompactPayloadBudget",
    "_binary_wire_size",
    "_bounded_maybe_bytes",
    "_bounded_optional_bytes",
    "_compact_payload_budget",
    "_pending_request_capacity_error",
    "compact_encoding_policy",
    "current_compact_encoding_policy",
    "transport_compact_encoding_policy",
]
