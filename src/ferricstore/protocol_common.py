from __future__ import annotations

import contextlib
import inspect
import io
import socket
import ssl
import time
import zlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from functools import partial
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.command_core import normalize_command_name
from ferricstore.config_validation import (
    validate_bool,
    validate_optional_flow_priority,
    validate_string_sequence,
)
from ferricstore.config_validation import (
    validate_optional_nonnegative_int as validated_optional_nonnegative_int,
)
from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
)
from ferricstore.lifecycle_core import (
    close_resources_async,
    close_resources_sync,
)
from ferricstore.protocol_codec import (
    encode_value_into,
)
from ferricstore.protocol_compact_budget import (
    _compact_payload_budget,
    _pending_request_capacity_error,
)
from ferricstore.protocol_constants import (
    _ASYNC_ADAPTER_FANOUT_LIMIT,
    _CONTROL_OPCODES,
    _HEADER,
    _MAX_LANE_ID,
    _ROUTE_SLOT_COUNT,
    _ROUTE_SLOT_MASK,
    _SAFE_CONTROL_RETRY_COMMANDS,
    _SUPPORTED_SCHEMES,
    _TLS_SCHEMES,
)
from ferricstore.protocol_framing import (
    ResponseIdentity,
    frame_parts,
    validate_response_identity,
)
from ferricstore.protocol_framing import (
    send_frames as _send_frames,
)
from ferricstore.protocol_lifecycle import (
    PendingRequestBudget,
)


def _request_body_byte_limit(
    budget: PendingRequestBudget,
    request_id: int,
) -> int | None:
    try:
        remaining = budget.remaining_bytes(request_id)
    except KeyError:
        # Private send helpers are also used by custom adapters that provide
        # their own final size accounting.  The authoritative set_size call
        # still rejects a real request whose reservation disappeared.
        return None
    if remaining is None:
        return None
    body_limit = remaining - _HEADER.size
    if body_limit < 0:
        raise _pending_request_capacity_error(budget.max_bytes)
    return body_limit


class _RequestBodyBuffer:
    __slots__ = ("_max_bytes", "_pending_limit", "_size", "_stream")

    def __init__(self, max_bytes: int | None, pending_limit: int | None) -> None:
        self._max_bytes = max_bytes
        self._pending_limit = pending_limit
        self._size = 0
        self._stream = io.BytesIO()

    def write(self, chunk: bytes | bytearray) -> None:
        size = memoryview(chunk).nbytes
        if self._max_bytes is not None and size > self._max_bytes - self._size:
            raise _pending_request_capacity_error(self._pending_limit)
        self._stream.write(chunk)
        self._size += size

    def finish(self) -> bytes:
        return self._stream.getvalue()


def _encode_request_body(
    payload: dict[str, Any] | bytes,
    *,
    compression: str,
    max_body_bytes: int | None,
    pending_limit: int | None,
) -> tuple[bytes, bool]:
    if isinstance(payload, bytes) and type(payload) is not bytes:
        # Framing and admission accounting must not dispatch to a subclass's
        # overridden __len__ or __bytes__.  Ordinary bytes retain the zero-copy
        # fast path; uncommon subclasses are normalized through their buffer.
        payload = memoryview(payload).tobytes()
    if isinstance(payload, bytes) and (not payload or compression != "zlib"):
        if max_body_bytes is not None and len(payload) > max_body_bytes:
            raise _pending_request_capacity_error(pending_limit)
        return payload, False

    output = _RequestBodyBuffer(max_body_bytes, pending_limit)
    if compression != "zlib":
        if isinstance(payload, bytes):
            output.write(payload)
        else:
            encode_value_into(payload, output.write)
        return output.finish(), False

    compressor = zlib.compressobj()

    def write_encoded(chunk: bytes | bytearray) -> None:
        view = memoryview(chunk)
        for offset in range(0, len(view), 64 * 1024):
            output.write(compressor.compress(view[offset : offset + 64 * 1024]))

    if isinstance(payload, bytes):
        write_encoded(payload)
    else:
        encode_value_into(payload, write_encoded)
    output.write(compressor.flush())
    return output.finish(), True


def _sync_adapter_deadline(adapter: Any) -> float | None:
    context = getattr(adapter, "_deadline_context", None)
    return cast(float | None, getattr(context, "deadline", None))


def _timeout_with_deadline(timeout: float | None, deadline: float | None) -> float | None:
    if deadline is None:
        return timeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise FutureTimeoutError
    return remaining if timeout is None else min(timeout, remaining)


def _protocol_connection_count(value: Any) -> int:
    if value is None:
        return 1
    from ferricstore.config_validation import validate_positive_int

    return validate_positive_int(value, name="max_connections")


def _protocol_lane_count(value: Any) -> int:
    from ferricstore.config_validation import validate_positive_int

    return validate_positive_int(value, name="lanes")


def _protocol_collection_limit(value: Any) -> int | None:
    return validated_optional_nonnegative_int(
        value,
        name="max_decoded_collection_items",
    )


def _response_item_count_map(adapter: Any) -> dict[int, int]:
    counts = getattr(adapter, "_pending_response_item_counts", None)
    if counts is None:
        counts = {}
        adapter._pending_response_item_counts = counts
    return cast(dict[int, int], counts)


def _response_identity_map(adapter: Any) -> dict[int, ResponseIdentity]:
    identities = getattr(adapter, "_pending_response_identities", None)
    if identities is None:
        identities = {}
        adapter._pending_response_identities = identities
    return cast(dict[int, ResponseIdentity], identities)


def _validate_pending_response_identity(
    adapter: Any,
    *,
    lane_id: int,
    opcode: int,
    request_id: int,
) -> None:
    pending_lock = getattr(adapter, "_pending_lock", None)
    if pending_lock is None:
        expected = _response_identity_map(adapter).get(request_id)
    else:
        with pending_lock:
            expected = _response_identity_map(adapter).get(request_id)
    if expected is not None:
        validate_response_identity(
            expected,
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
        )


def _pop_response_item_count(adapter: Any, request_id: int) -> int | None:
    pending_lock = getattr(adapter, "_pending_lock", None)
    if pending_lock is None:
        return _response_item_count_map(adapter).pop(request_id, None)
    with pending_lock:
        return _response_item_count_map(adapter).pop(request_id, None)


def _validated_route_lane(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_LANE_ID:
        raise ValueError("route lane_id must be an unsigned 32-bit integer")
    return value


def _notify_event_listeners(listeners: Sequence[Callable[[], None]]) -> None:
    for listener in listeners:
        with contextlib.suppress(Exception):
            listener()


def _unique_adapters(adapters: Iterable[Any]) -> list[Any]:
    unique: list[Any] = []
    identities: set[int] = set()
    for adapter in adapters:
        identity = id(adapter)
        if identity not in identities:
            identities.add(identity)
            unique.append(adapter)
    return unique


def _close_adapter_sync(
    adapter: Any,
    event_listener: Callable[[], None],
) -> None:
    first_error: BaseException | None = None
    remove_listener = getattr(adapter, "remove_event_listener", None)
    if callable(remove_listener):
        try:
            remove_listener(event_listener)
        except BaseException as exc:
            first_error = exc
    close = getattr(adapter, "close", None)
    if callable(close):
        try:
            close()
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


def _close_adapters_sync(
    adapters: Sequence[Any],
    event_listener: Callable[[], None],
) -> None:
    close_resources_sync(
        [partial(_close_adapter_sync, adapter, event_listener) for adapter in adapters]
    )


async def _close_adapters_async(
    adapters: Sequence[Any],
    event_listener: Callable[[], None],
) -> None:
    await close_resources_async(
        [partial(_close_adapter_async, adapter, event_listener) for adapter in adapters],
        max_concurrency=_async_adapter_outer_fanout_limit(adapters),
    )


async def _close_adapter_async(
    adapter: Any,
    event_listener: Callable[[], None],
) -> None:
    first_error: BaseException | None = None
    remove_listener = getattr(adapter, "remove_event_listener", None)
    if callable(remove_listener):
        try:
            remove_listener(event_listener)
        except BaseException as exc:
            first_error = exc
    close = getattr(adapter, "close", None)
    if callable(close):
        try:
            result = close()
            if inspect.isawaitable(result):
                await result
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


def _async_adapter_outer_fanout_limit(adapters: Sequence[Any]) -> int:
    nested_width = (
        max(
            (
                min(len(children), _ASYNC_ADAPTER_FANOUT_LIMIT)
                if isinstance((children := getattr(adapter, "adapters", None)), Sequence)
                and children
                else 1
            )
            for adapter in adapters
        )
        if adapters
        else 1
    )
    return max(1, _ASYNC_ADAPTER_FANOUT_LIMIT // nested_width)


def _endpoint_adapter_is_idle(adapter: Any) -> bool:
    """Conservatively identify endpoint adapters with no leased or pending work."""
    active = getattr(adapter, "_active", None)
    if active is not None and any(int(count) > 0 for count in active):
        return False
    if getattr(adapter, "_leased", None):
        return False
    if bool(getattr(adapter, "_broadcasting", False)):
        return False
    if getattr(adapter, "_pending", None):
        return False
    connect_lock = getattr(adapter, "_connect_lock", None)
    if (
        connect_lock is not None
        and callable(getattr(connect_lock, "locked", None))
        and connect_lock.locked()
    ):
        return False
    return not bool(getattr(adapter, "_connecting", False))


def _flow_wake_payload(
    type: str,
    *,
    state: str | None = None,
    states: list[str] | None = None,
    partition_key: str | None = None,
    partition_keys: list[str] | None = None,
    priority: int | None = 0,
    limit: int | None = None,
) -> dict[str, Any]:
    if state is not None and states is not None:
        raise ValueError("state and states are mutually exclusive")
    if partition_key is not None and partition_keys is not None:
        raise ValueError("partition_key and partition_keys are mutually exclusive")

    flow_wake: dict[str, Any] = {"type": type}
    if states is not None:
        flow_wake["states"] = list(
            validate_string_sequence(states, name="states", allow_empty=False)
        )
    elif state is not None:
        flow_wake["state"] = state
    if partition_keys is not None:
        flow_wake["partition_keys"] = list(
            validate_string_sequence(
                partition_keys,
                name="partition_keys",
                allow_empty=False,
            )
        )
    elif partition_key is not None:
        flow_wake["partition_key"] = partition_key
    priority = validate_optional_flow_priority(priority)
    if priority is not None:
        flow_wake["priority"] = priority
    if limit is not None:
        flow_wake["limit"] = limit
    return {"events": ["FLOW_WAKE"], "flow_wake": flow_wake}


def _set_wire_future_sources(
    future: Future[Any],
    sources: Sequence[Future[Any]],
) -> None:
    cast(Any, future)._ferricstore_sources = tuple(sources)


@dataclass(frozen=True, slots=True)
class RoutingTopology:
    route_epoch: int
    shard_count: int
    slots: tuple[dict[str, Any] | None, ...]
    endpoints: dict[tuple[str, int, int], dict[str, Any]]
    route_destinations: tuple[dict[str, Any], ...]

    @classmethod
    def empty(cls) -> RoutingTopology:
        return cls(
            route_epoch=0,
            shard_count=0,
            slots=(None,) * _ROUTE_SLOT_COUNT,
            endpoints={},
            route_destinations=(),
        )

    @classmethod
    def build(cls, payload: Mapping[Any, Any]) -> RoutingTopology:
        ranges = _map_get(payload, "ranges")
        route_epoch = _int_or_none(_map_get(payload, "route_epoch"))
        shard_count = _int_or_none(_map_get(payload, "shard_count"))
        if (
            not isinstance(ranges, list)
            or route_epoch is None
            or route_epoch < 0
            or shard_count is None
            or shard_count <= 0
            or shard_count > _ROUTE_SLOT_COUNT
        ):
            raise FerricStoreError("invalid SHARDS topology payload", raw=payload)

        slots: list[dict[str, Any] | None] = [None] * _ROUTE_SLOT_COUNT
        endpoints: dict[tuple[str, int, int], dict[str, Any]] = {}
        routed_shards: set[int] = set()

        for item in ranges:
            if not isinstance(item, Mapping):
                raise FerricStoreError("invalid SHARDS range", raw=item)
            if _text_or_none(_map_get(item, "hint")) == "leader_unknown":
                raise FerricStoreError("SHARDS range has no leader", raw=item)

            first = _int_or_none(_map_get(item, "first_slot"))
            last = _int_or_none(_map_get(item, "last_slot"))
            shard = _int_or_none(_map_get(item, "shard"))
            lane_id = _int_or_none(_map_get(item, "lane_id"))
            endpoint = cls._endpoint_from_range(item)

            if (
                first is None
                or last is None
                or shard is None
                or lane_id is None
                or first < 0
                or last < first
                or last >= _ROUTE_SLOT_COUNT
                or shard < 0
                or shard >= shard_count
                or lane_id < 0
                or lane_id > _MAX_LANE_ID
            ):
                raise FerricStoreError("invalid SHARDS range", raw=item)

            endpoint_key = cls.endpoint_key(endpoint)
            endpoint_identity = cls.endpoint_identity(endpoint)
            route = {
                "shard": shard,
                "lane_id": lane_id,
                "endpoint_key": endpoint_key,
                "endpoint_identity": endpoint_identity,
                "endpoint": endpoint,
                "leader_node": endpoint["node"],
            }
            for slot in range(first, last + 1):
                if slots[slot] is not None:
                    raise FerricStoreError(
                        f"SHARDS slot table overlaps at slot {slot}",
                        raw=item,
                    )
                slots[slot] = route
            endpoints[endpoint_identity] = endpoint
            routed_shards.add(shard)

        if any(route is None for route in slots):
            raise FerricStoreError("SHARDS slot table is incomplete", raw=payload)
        if len(routed_shards) != shard_count:
            raise FerricStoreError("SHARDS shard_count does not match slot table", raw=payload)

        complete_slots = cast(list[dict[str, Any]], slots)
        destinations: dict[tuple[tuple[str, int, int], int], dict[str, Any]] = {}
        for slot_route in complete_slots:
            destinations.setdefault(
                (slot_route["endpoint_identity"], slot_route["lane_id"]),
                slot_route,
            )

        return cls(
            route_epoch=route_epoch,
            shard_count=shard_count,
            slots=tuple(complete_slots),
            endpoints=endpoints,
            route_destinations=tuple(destinations.values()),
        )

    @staticmethod
    def endpoint_key(endpoint: Mapping[str, Any]) -> tuple[str, int]:
        host = _text_or_none(_map_get(endpoint, "host", "native_host"))
        port = _int_or_none(_map_get(endpoint, "native_port"))
        if not host or not _valid_port(port):
            raise FerricStoreError("invalid FerricStore endpoint", raw=endpoint)
        return (host.lower(), cast(int, port))

    @staticmethod
    def endpoint_identity(endpoint: Mapping[str, Any]) -> tuple[str, int, int]:
        """Identify both plaintext and TLS destinations without transport context."""
        host, native_port = RoutingTopology.endpoint_key(endpoint)
        tls_port = _int_or_none(_map_get(endpoint, "native_tls_port"))
        if tls_port is not None and not _valid_port(tls_port):
            raise FerricStoreError("invalid FerricStore endpoint", raw=endpoint)
        return (host, native_port, native_port if tls_port is None else tls_port)

    @staticmethod
    def slot_for_key(key: str | bytes) -> int:
        if isinstance(key, bytes):
            if key.startswith(b"f:{"):
                hash_input = _flow_hash_tag(key[3:], key)
            elif key.startswith(b"X:f:{"):
                hash_input = _flow_hash_tag(key[5:], key)
            else:
                hash_input = _hash_tag_or_key(key)
        else:
            text = str(key)
            if text.startswith("f:{"):
                hash_input = _flow_hash_tag(text[3:], text)
            elif text.startswith("X:f:{"):
                hash_input = _flow_hash_tag(text[5:], text)
            else:
                hash_input = _hash_tag_or_key(text)
        encoded = hash_input if isinstance(hash_input, bytes) else hash_input.encode()
        return zlib.crc32(encoded) & _ROUTE_SLOT_MASK

    def route_key(self, key: str | bytes) -> dict[str, Any]:
        slot = self.slot_for_key(key)
        route = self.slots[slot]
        if route is None:
            raise FerricStoreError(f"no route for slot {slot}")
        result = dict(route)
        result["slot"] = slot
        return result

    @classmethod
    def _endpoint_from_range(cls, item: Mapping[Any, Any]) -> dict[str, Any]:
        endpoint = _map_get(item, "endpoint")
        raw = endpoint if isinstance(endpoint, Mapping) else item
        host = _text_or_none(_map_get(raw, "host", "native_host"))
        port = _int_or_none(_map_get(raw, "native_port"))
        tls_port = _int_or_none(_map_get(raw, "native_tls_port"))
        if (
            not host
            or not _valid_port(port)
            or (tls_port is not None and not _valid_port(tls_port))
        ):
            raise FerricStoreError("invalid SHARDS endpoint", raw=item)
        result = {
            "node": _text_or_none(_map_get(raw, "node", "leader_node", "owner_node")) or host,
            "host": host,
            "native_port": cast(int, port),
        }
        if tls_port is not None:
            result["native_tls_port"] = tls_port
        return result


def _send_frame(sock: socket.socket | ssl.SSLSocket, header: bytes, body: bytes) -> None:
    _send_frames(sock, frame_parts(header, body), timeout=None)


def _lane_for_opcode(opcode: int) -> int:
    return 0 if opcode in _CONTROL_OPCODES else 1


def _command_name(value: Any) -> str:
    return normalize_command_name(value)


def _command_token(value: Any) -> str:
    return normalize_command_name(value)


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, str):
        return value
    return None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _valid_port(value: int | None) -> bool:
    return value is not None and 1 <= value <= 65535


def _protocol_url_port(parsed: Any, *, tls: bool) -> int:
    port = parsed.port
    if port is None:
        return 6389 if tls else 6388
    if not _valid_port(port):
        raise ValueError("port must be between 1 and 65535")
    return cast(int, port)


def _hash_tag_or_key(key: str | bytes) -> str | bytes:
    if isinstance(key, bytes):
        start = key.find(b"{")
        if start < 0:
            return key
        end = key.find(b"}", start + 1)
        if end > start + 1:
            return key[start + 1 : end]
        return key

    start = key.find("{")
    if start < 0:
        return key
    end = key.find("}", start + 1)
    if end > start + 1:
        return key[start + 1 : end]
    return key


def _flow_hash_tag(rest: str | bytes, fallback_key: str | bytes) -> str | bytes:
    end = rest.find(b"}") if isinstance(rest, bytes) else rest.find("}")
    if end > 0:
        return rest[:end]
    return _hash_tag_or_key(fallback_key)


def _endpoint_from_url(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    tls = scheme in _TLS_SCHEMES
    if scheme not in _SUPPORTED_SCHEMES:
        raise ValueError(f"unsupported FerricStore URL scheme: {parsed.scheme}")
    host = parsed.hostname or "127.0.0.1"
    port = _protocol_url_port(parsed, tls=tls)
    endpoint: dict[str, Any] = {
        "node": host,
        "host": host,
        "native_port": port,
        "tls": tls,
    }
    if tls:
        endpoint["native_tls_port"] = port
    return endpoint


def _url_from_endpoint(endpoint: Mapping[str, Any], *, tls: bool) -> str:
    host = _text_or_none(_map_get(endpoint, "host", "native_host"))
    native_port = _int_or_none(_map_get(endpoint, "native_port"))
    tls_port = _int_or_none(_map_get(endpoint, "native_tls_port"))
    if host is None or native_port is None:
        raise FerricStoreError("invalid FerricStore endpoint", raw=endpoint)
    port = tls_port if tls and tls_port is not None else native_port
    scheme = "ferrics" if tls else "ferric"
    return f"{scheme}://{_url_host(host)}:{port}"


def _topology_candidate_urls(
    selector: Any,
    seed_urls: Sequence[str],
    topology: RoutingTopology,
    *,
    tls: bool,
) -> list[str]:
    discovered = [_url_from_endpoint(endpoint, tls=tls) for endpoint in topology.endpoints.values()]
    live = {*seed_urls, *discovered}
    return [url for url in selector.candidates(discovered) if url in live]


def _connection_endpoint_key(
    endpoint: Mapping[str, Any],
    *,
    tls: bool,
) -> tuple[str, int]:
    """Identify the socket endpoint the pool will actually connect to."""
    host = _text_or_none(_map_get(endpoint, "host", "native_host"))
    native_port = _int_or_none(_map_get(endpoint, "native_port"))
    tls_port = _int_or_none(_map_get(endpoint, "native_tls_port"))
    if host is None or native_port is None:
        raise FerricStoreError("invalid FerricStore endpoint", raw=endpoint)
    return (host.lower(), tls_port if tls and tls_port is not None else native_port)


def _topology_update_required(
    current: RoutingTopology,
    candidate: RoutingTopology,
) -> bool:
    """Report whether a trusted SHARDS observation changes routing state.

    ``route_epoch`` is an opaque hash of the slot map, not an ordered topology
    revision.  Leader changes therefore legitimately keep the same value, and
    a changed slot map may produce a numerically smaller value.
    """
    return candidate != current


def _url_host(host: str) -> str:
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host


def _normalized_host_set(hosts: Sequence[str | None]) -> set[str]:
    return {host.lower() for host in hosts if host}


def _is_retryable_route_error(exc: BaseException) -> bool:
    if isinstance(exc, (OSError, TimeoutError, FutureTimeoutError)):
        return True
    if not isinstance(exc, FerricStoreError):
        return False
    message = str(exc).lower()
    if any(token in message for token in ("connection", "closed", "reroute", "leader")):
        return True
    raw = getattr(exc, "raw", None)
    reason = _optional_text(raw, "reason")
    return reason is not None and reason.lower() in {"reroute", "leader_changed", "not_leader"}


def _is_safe_control_retry(args: tuple[Any, ...]) -> bool:
    return bool(args) and _command_name(args[0]) in _SAFE_CONTROL_RETRY_COMMANDS


def _require_arg(args: tuple[Any, ...], idx: int, command: str) -> Any:
    if idx >= len(args):
        raise InvalidCommandError(f"{command} is missing argument {idx + 1}")
    return args[idx]


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, bytes):
        try:
            value = value.decode()
        except UnicodeDecodeError as exc:
            raise InvalidCommandError("protocol boolean must be valid UTF-8") from exc
    if isinstance(value, str):
        normalized = value.lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    elif type(value) is int:
        if value == 1:
            return True
        if value == 0:
            return False
    raise InvalidCommandError("protocol boolean must be true/false, yes/no, on/off, or 1/0")


def _map_get(mapping: Any, *keys: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping:
            return mapping[key]
        encoded = key.encode()
        if encoded in mapping:
            return mapping[encoded]
    return None


def _optional_text(mapping: Any, key: str) -> str | None:
    value = _map_get(mapping, key)
    if value is None:
        return None
    return _text(value)


def _optional_int(mapping: Any, key: str) -> int | None:
    value = _map_get(mapping, key)
    if value is None:
        return None
    return int(value)


def _error_message(value: Any) -> str:
    message = _optional_text(value, "message")
    if message is not None:
        return message
    return _text(value)


def _normalize_protocol_url_kwargs(
    kwargs: dict[str, Any],
    *,
    tls_from_scheme: bool | None = None,
) -> None:
    if tls_from_scheme is not None:
        explicit_tls = kwargs.get("tls", tls_from_scheme)
        if not isinstance(explicit_tls, bool) or explicit_tls is not tls_from_scheme:
            raise ValueError("protocol TLS setting must match the URL scheme")
        kwargs["tls"] = tls_from_scheme
    if "socket_timeout" in kwargs and "timeout" not in kwargs:
        kwargs["timeout"] = kwargs.pop("socket_timeout")
    if "health_check_interval" in kwargs and "heartbeat_interval" not in kwargs:
        kwargs["heartbeat_interval"] = kwargs.pop("health_check_interval")
    for compatibility_only in (
        "decode_responses",
        "max_connections",
        "protocol",
        "retry_on_timeout",
    ):
        kwargs.pop(compatibility_only, None)


def _pool_topology_options(kwargs: dict[str, Any]) -> tuple[list[str] | None, bool]:
    seeds = kwargs.pop("seeds", None)
    resolved_seeds = (
        list(validate_string_sequence(seeds, name="seeds")) if seeds is not None else None
    )
    ha_routing = validate_bool(kwargs.pop("ha_routing", False), name="ha_routing")
    return resolved_seeds, ha_routing


__all__ = [
    "RoutingTopology",
    "_RequestBodyBuffer",
    "_async_adapter_outer_fanout_limit",
    "_close_adapter_async",
    "_close_adapter_sync",
    "_close_adapters_async",
    "_close_adapters_sync",
    "_coerce_bool",
    "_command_name",
    "_command_token",
    "_compact_payload_budget",
    "_connection_endpoint_key",
    "_encode_request_body",
    "_endpoint_adapter_is_idle",
    "_endpoint_from_url",
    "_error_message",
    "_flow_hash_tag",
    "_flow_wake_payload",
    "_hash_tag_or_key",
    "_int_or_none",
    "_is_retryable_route_error",
    "_is_safe_control_retry",
    "_lane_for_opcode",
    "_map_get",
    "_normalize_protocol_url_kwargs",
    "_normalized_host_set",
    "_notify_event_listeners",
    "_optional_int",
    "_optional_text",
    "_pending_request_capacity_error",
    "_pop_response_item_count",
    "_protocol_collection_limit",
    "_protocol_connection_count",
    "_protocol_lane_count",
    "_request_body_byte_limit",
    "_require_arg",
    "_response_identity_map",
    "_response_item_count_map",
    "_send_frame",
    "_set_wire_future_sources",
    "_sync_adapter_deadline",
    "_text",
    "_text_or_none",
    "_timeout_with_deadline",
    "_unique_adapters",
    "_url_from_endpoint",
    "_url_host",
    "_valid_port",
    "_validate_pending_response_identity",
    "_validated_route_lane",
]
