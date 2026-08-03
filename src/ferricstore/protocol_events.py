from __future__ import annotations

from collections import deque
from typing import Any

from ferricstore.protocol_common import _map_get


def expand_pubsub_batch_event(value: Any) -> tuple[Any, ...]:
    """Expand a negotiated Pub/Sub batch into legacy logical message events."""
    if not isinstance(value, dict) or _text(_map_get(value, "event")) != "PUBSUB_MESSAGE":
        return (value,)
    payload = _map_get(value, "payload")
    if not isinstance(payload, dict) or _text(_map_get(payload, "kind")) != "message_batch":
        return (value,)
    channel = _map_get(payload, "channel")
    messages = _map_get(payload, "messages")
    if not _binary_scalar(channel) or not isinstance(messages, (list, tuple)) or not messages:
        return (value,)
    if any(not _binary_scalar(message) for message in messages):
        return (value,)

    at_ms = _map_get(value, "at_ms")
    return tuple(
        {
            "event": "PUBSUB_MESSAGE",
            "at_ms": at_ms,
            "payload": {
                "kind": "message",
                "channel": channel,
                "message": message,
            },
        }
        for message in messages
    )


def append_expanded_pubsub_events(events: deque[Any], value: Any, limit: int | None) -> bool:
    """Append logical Pub/Sub messages, returning false before exceeding the limit."""
    expanded = expand_pubsub_batch_event(value)
    if limit is not None and len(expanded) > limit - len(events):
        return False
    events.extend(expanded)
    return True


def _binary_scalar(value: Any) -> bool:
    return isinstance(value, (str, bytes, bytearray, memoryview))


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return ""
    return value if isinstance(value, str) else ""


__all__ = ["append_expanded_pubsub_events", "expand_pubsub_batch_event"]
