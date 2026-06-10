from __future__ import annotations

import asyncio
import contextlib
import socket
import ssl
import struct
import threading
import time
import zlib
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import unquote, urlparse

from ferricstore.errors import FerricStoreError, InvalidCommandError, OverloadedError

_MAGIC = b"FSNP"
_REQUEST_VERSION = 0x01
_RESPONSE_VERSION = 0x81
_HEADER = struct.Struct(">4sBBIHQI")
_STATUS = struct.Struct(">H")

_FLAG_COMPRESSED = 0x08
_FLAG_CUSTOM_PAYLOAD = 0x02
_FLAG_MORE_CHUNKS = 0x20

_OP_BATCH = 0x000E
_OP_STARTUP = 0x000C
_OP_AUTH = 0x0002
_OP_SUBSCRIBE_EVENTS = 0x0011
_OP_UNSUBSCRIBE_EVENTS = 0x0012
_OP_FLOW_CLAIM_DUE = 0x0203
_OP_FLOW_CREATE_MANY = 0x020F
_OP_FLOW_COMPLETE_MANY = 0x0210
_OP_FLOW_RETRY_MANY = 0x0212
_OP_FLOW_FAIL_MANY = 0x0213
_OP_FLOW_CANCEL_MANY = 0x0214

_STATUS_OK = 0
_STATUS_BUSY = 4

_COMPACT_FLOW_CLAIM_JOBS = 0x80
_COMPACT_OK_LIST = 0x81
_COMPACT_FLOW_CREATE_MANY_REQUEST = 0x90
_COMPACT_FLOW_CLAIM_DUE_REQUEST = 0x91
_COMPACT_FLOW_COMPLETE_MANY_REQUEST = 0x92
_NULL_U32 = 0xFFFFFFFF
_I64_MIN = -(1 << 63)

_PLAIN_SCHEMES = {"ferric", "native", "ferricstore-native"}
_TLS_SCHEMES = {"ferrics", "ferric+tls", "native+tls", "ferricstore-native+tls"}
_SUPPORTED_SCHEMES = _PLAIN_SCHEMES | _TLS_SCHEMES

_OPCODES = {
    "PING": 0x0003,
    "CLIENT.SETNAME": 0x0004,
    "CLIENT.INFO": 0x0005,
    "ROUTE": 0x0006,
    "SHARDS": 0x0007,
    "BACKPRESSURE": 0x0008,
    "QUIT": 0x0009,
    "OPTIONS": 0x000B,
    "SUBSCRIBE_EVENTS": _OP_SUBSCRIBE_EVENTS,
    "UNSUBSCRIBE_EVENTS": _OP_UNSUBSCRIBE_EVENTS,
    "GET": 0x0101,
    "SET": 0x0102,
    "DEL": 0x0103,
    "MGET": 0x0104,
    "MSET": 0x0105,
    "CAS": 0x0106,
    "LOCK": 0x0107,
    "UNLOCK": 0x0108,
    "EXTEND": 0x0109,
    "RATELIMIT.ADD": 0x010A,
    "FETCH_OR_COMPUTE": 0x010B,
    "FETCH_OR_COMPUTE_RESULT": 0x010C,
    "FETCH_OR_COMPUTE_ERROR": 0x010D,
    "CLUSTER.HEALTH": 0x0301,
    "CLUSTER.STATS": 0x0302,
    "CLUSTER.KEYSLOT": 0x0303,
    "CLUSTER.SLOTS": 0x0304,
    "CLUSTER.STATUS": 0x0305,
    "CLUSTER.JOIN": 0x0306,
    "CLUSTER.LEAVE": 0x0307,
    "CLUSTER.FAILOVER": 0x0308,
    "CLUSTER.PROMOTE": 0x0309,
    "CLUSTER.DEMOTE": 0x030A,
    "CLUSTER.ROLE": 0x030B,
    "FERRICSTORE.KEY_INFO": 0x030C,
    "FERRICSTORE.CONFIG": 0x030D,
    "FERRICSTORE.HOTNESS": 0x030E,
    "FERRICSTORE.METRICS": 0x030F,
    "FERRICSTORE.BLOBGC": 0x0310,
    "FLOW.CREATE": 0x0201,
    "FLOW.GET": 0x0202,
    "FLOW.CLAIM_DUE": 0x0203,
    "FLOW.COMPLETE": 0x0204,
    "FLOW.TRANSITION": 0x0205,
    "FLOW.RETRY": 0x0206,
    "FLOW.FAIL": 0x0207,
    "FLOW.CANCEL": 0x0208,
    "FLOW.EXTEND_LEASE": 0x0209,
    "FLOW.HISTORY": 0x020A,
    "FLOW.VALUE.PUT": 0x020B,
    "FLOW.VALUE.MGET": 0x020C,
    "FLOW.SIGNAL": 0x020D,
    "FLOW.LIST": 0x020E,
    "FLOW.CREATE_MANY": 0x020F,
    "FLOW.COMPLETE_MANY": 0x0210,
    "FLOW.TRANSITION_MANY": 0x0211,
    "FLOW.RETRY_MANY": 0x0212,
    "FLOW.FAIL_MANY": 0x0213,
    "FLOW.CANCEL_MANY": 0x0214,
    "FLOW.RECLAIM": 0x0215,
    "FLOW.REWIND": 0x0216,
    "FLOW.TERMINALS": 0x0217,
    "FLOW.FAILURES": 0x0218,
    "FLOW.BY_PARENT": 0x0219,
    "FLOW.BY_ROOT": 0x021A,
    "FLOW.BY_CORRELATION": 0x021B,
    "FLOW.INFO": 0x021C,
    "FLOW.STUCK": 0x021D,
    "FLOW.POLICY.SET": 0x021E,
    "FLOW.POLICY.GET": 0x021F,
    "FLOW.SPAWN_CHILDREN": 0x0220,
    "FLOW.RETENTION_CLEANUP": 0x0221,
}

_CONTROL_OPCODES = set(range(0x0001, 0x0013))

_FIELD_NAMES = {
    "AFTER_MS": "after_ms",
    "BACKOFF": "backoff",
    "BASE_MS": "base_ms",
    "BLOCK": "block_ms",
    "CONSISTENT_PROJECTION": "consistent_projection",
    "CORRELATION_ID": "correlation_id",
    "COUNT": "count",
    "DELAY_MS": "delay_ms",
    "ERROR": "error",
    "ERROR_REF": "error_ref",
    "EVENT": "event",
    "EXHAUSTED_TO": "exhausted_to",
    "EXPECT_STATE": "expect_state",
    "FENCING": "fencing_token",
    "FENCING_TOKEN": "fencing_token",
    "FAILURE": "failure",
    "FROM_EVENT": "from_event",
    "FROM_MS": "from_ms",
    "FROM_STATE": "from_state",
    "FROM_VERSION": "from_version",
    "FULL": "full",
    "GROUP": "group_id",
    "GROUP_ID": "group_id",
    "HISTORY_HOT_MAX_EVENTS": "history_hot_max_events",
    "HISTORY_MAX_EVENTS": "history_max_events",
    "IDEMPOTENT": "idempotent",
    "INCLUDE_COLD": "include_cold",
    "INDEPENDENT": "independent",
    "JITTER_PCT": "jitter_pct",
    "LEASE_MS": "lease_ms",
    "LEASE_TOKEN": "lease_token",
    "LIMIT": "limit",
    "LOCAL_CACHE": "local_cache",
    "MAX_ATTEMPTS": "max_attempts",
    "MAX_BYTES": "max_bytes",
    "MAX_MS": "max_ms",
    "MAX_RETRIES": "max_retries",
    "NAME": "name",
    "NOW": "now_ms",
    "OLDER_THAN": "older_than_ms",
    "ON_CHILD_FAILED": "on_child_failed",
    "ON_PARENT_CLOSED": "on_parent_closed",
    "OVERRIDE": "override",
    "OWNER_FLOW_ID": "owner_flow_id",
    "PARENT_FLOW_ID": "parent_id",
    "PARENT_ID": "parent_id",
    "PARTITION": "partition_key",
    "PAYLOAD": "payload",
    "PAYLOAD_MAX_BYTES": "payload_max_bytes",
    "PRIORITY": "priority",
    "REASON": "reason",
    "REASON_REF": "reason_ref",
    "RECLAIM_EXPIRED": "reclaim_expired",
    "RECLAIM_RATIO": "reclaim_ratio",
    "RESULT": "result",
    "RESULT_REF": "result_ref",
    "RETENTION_TTL_MS": "retention_ttl_ms",
    "REV": "rev",
    "ROOT_FLOW_ID": "root_id",
    "ROOT_ID": "root_id",
    "RUN_AT": "run_at_ms",
    "RUN_AT_MS": "run_at_ms",
    "STATE": "state",
    "SUCCESS": "success",
    "TERMINAL_ONLY": "terminal_only",
    "TO_EVENT": "to_event",
    "TO_MS": "to_ms",
    "TO_STATE": "to_state",
    "TO_VERSION": "to_version",
    "TTL": "ttl_ms",
    "TTL_MS": "ttl_ms",
    "TYPE": "type",
    "VALUE_MAX_BYTES": "value_max_bytes",
    "VALUES": "values",
    "WAIT": "wait",
    "WAIT_STATE": "wait_state",
    "WORKER": "worker",
}

_BOOL_FIELDS = {
    "consistent_projection",
    "full",
    "idempotent",
    "include_cold",
    "independent",
    "local_cache",
    "override",
    "reclaim_expired",
    "rev",
    "terminal_only",
    "values",
}


@dataclass(frozen=True, slots=True)
class NativeCommand:
    opcode: int
    payload: dict[str, Any] | bytes
    lane_id: int = 1
    flags: int = 0


@dataclass(frozen=True, slots=True)
class NativeResponse:
    lane_id: int
    opcode: int
    request_id: int
    flags: int
    status: int
    value: Any


class NativeAdapter:
    """FerricStore native TCP adapter for the sync SDK.

    The adapter accepts the same `execute_command(*args)` shape as the Redis
    adapter. It translates supported Redis/FerricFlow commands into native
    typed frames so high-level Queue/Workflow code can switch transport by URL.
    """

    client: NativeAdapter

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6388,
        *,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        timeout: float | None = 30.0,
        client_name: str | None = "ferricstore-python",
        compression: str = "none",
        lanes: int = 16,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username or "default"
        self.password = password
        self.tls = tls
        self.timeout = timeout
        self.client_name = client_name
        self.compression = compression
        self.lanes = max(1, lanes)
        self.ssl_context = ssl_context
        self.client = self
        self._lock = threading.Lock()
        self._request_id = 0
        self._lane_cursor = 0
        self._sock: socket.socket | ssl.SSLSocket | None = None
        self._reader_thread: threading.Thread | None = None
        self._pending: dict[int, Future[NativeResponse]] = {}
        self._events: list[Any] = []
        self._events_cv = threading.Condition()
        self._connect()

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> NativeAdapter:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        tls = scheme in _TLS_SCHEMES
        if scheme not in _SUPPORTED_SCHEMES:
            raise ValueError(f"unsupported FerricStore URL scheme: {parsed.scheme}")

        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (6389 if tls else 6388)
        username = unquote(parsed.username) if parsed.username else None
        password = unquote(parsed.password) if parsed.password else None
        _normalize_native_url_kwargs(kwargs)
        kwargs.setdefault("username", username)
        kwargs.setdefault("password", password)
        kwargs.setdefault("tls", tls)
        return cls(host, port, **kwargs)

    @property
    def events(self) -> list[Any]:
        with self._events_cv:
            return list(self._events)

    def wait_event(self, timeout: float | None = None) -> Any | None:
        with self._events_cv:
            if not self._events:
                self._events_cv.wait(timeout)
            if not self._events:
                return None
            return self._events.pop(0)

    def close(self) -> None:
        sock = self._sock
        self._sock = None
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        self._fail_pending(FerricStoreError("native connection is closed"))

    def pipeline(self, transaction: bool = False) -> NativePipeline:
        if transaction:
            raise InvalidCommandError("native pipeline does not support Redis transactions")
        return NativePipeline(self)

    def execute_command(self, *args: Any) -> Any:
        command = translate_command(*args)
        response = self._request(command.opcode, command.lane_id, command.payload, command.flags)
        return self._response_value(response)

    def submit_command(self, *args: Any) -> Future[Any]:
        command = translate_command(*args)
        _request_id, response_future = self._submit_request(
            command.opcode, command.lane_id, command.payload, command.flags
        )
        value_future: Future[Any] = Future()

        def complete(source: Future[NativeResponse]) -> None:
            if value_future.cancelled():
                return
            try:
                value_future.set_result(self._response_value(source.result()))
            except Exception as exc:
                if not value_future.cancelled():
                    value_future.set_exception(exc)

        response_future.add_done_callback(complete)
        return value_future

    def subscribe_flow_wake(
        self,
        type: str,
        *,
        state: str | None = None,
        states: list[str] | None = None,
        partition_key: str | None = None,
        partition_keys: list[str] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> Any:
        if state is not None and states is not None:
            raise ValueError("state and states are mutually exclusive")
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")

        flow_wake: dict[str, Any] = {"type": type}
        if states is not None:
            if not states:
                raise ValueError("states must be non-empty")
            flow_wake["states"] = list(states)
        elif state is not None:
            flow_wake["state"] = state
        if partition_keys is not None:
            if not partition_keys:
                raise ValueError("partition_keys must be non-empty")
            flow_wake["partition_keys"] = list(partition_keys)
        elif partition_key is not None:
            flow_wake["partition_key"] = partition_key
        if priority is not None:
            flow_wake["priority"] = priority
        if limit is not None:
            flow_wake["limit"] = limit

        response = self._request(
            _OP_SUBSCRIBE_EVENTS,
            0,
            {"events": ["FLOW_WAKE"], "flow_wake": flow_wake},
        )
        return self._response_value(response)

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        if not commands:
            return []

        native_commands = [translate_command(*command) for command in commands]
        if any(command.opcode in _CONTROL_OPCODES or command.flags for command in native_commands):
            return [self.execute_command(*command) for command in commands]

        batch_commands = [
            {
                "opcode": command.opcode,
                "lane_id": command.lane_id,
                "request_id": idx + 1,
                "body": command.payload,
            }
            for idx, command in enumerate(native_commands)
        ]
        response = self._request(
            _OP_BATCH,
            0,
            {"atomicity": "none", "commands": batch_commands},
        )

        values = self._response_value(response)
        if not isinstance(values, list):
            raise FerricStoreError("native BATCH returned non-list response", raw=values)

        return [self._batch_item_value(item) for item in values]

    def _connect(self) -> None:
        raw_sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        raw_sock.settimeout(None)
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if self.tls:
            context = self.ssl_context or ssl.create_default_context()
            self._sock = context.wrap_socket(raw_sock, server_hostname=self.host)
            self._sock.settimeout(None)
        else:
            self._sock = raw_sock
        self._reader_thread = threading.Thread(target=self._reader_loop, daemon=True)
        self._reader_thread.start()

        startup: dict[str, Any] = {
            "compression": self.compression,
            "compact_flow_responses": True,
        }
        if self.client_name is not None:
            startup["client_name"] = self.client_name
            startup["driver_name"] = self.client_name
        self._response_value(self._request(_OP_STARTUP, 0, startup))

        if self.password is not None:
            self._response_value(
                self._request(
                    _OP_AUTH,
                    0,
                    {"username": self.username, "password": self.password},
                )
            )

    def _next_request_id(self) -> int:
        self._request_id = (self._request_id + 1) & 0xFFFFFFFFFFFFFFFF
        if self._request_id == 0:
            self._request_id = 1
        return self._request_id

    def _next_lane_id(self, lane_id: int) -> int:
        if lane_id == 0 or self.lanes == 1:
            return lane_id
        self._lane_cursor = (self._lane_cursor % self.lanes) + 1
        return self._lane_cursor

    def _send(
        self,
        opcode: int,
        lane_id: int,
        request_id: int,
        payload: dict[str, Any] | bytes,
        extra_flags: int = 0,
    ) -> None:
        sock = self._require_socket()
        body = payload if isinstance(payload, bytes) else encode_value(payload)
        flags = extra_flags
        if self.compression == "zlib" and body:
            body = zlib.compress(body)
            flags |= _FLAG_COMPRESSED
        header = _HEADER.pack(
            _MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)
        )
        _send_frame(sock, header, body)

    def _request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
    ) -> NativeResponse:
        request_id, future = self._submit_request(opcode, lane_id, payload, flags)

        try:
            if self.timeout is None:
                return future.result()
            return future.result(timeout=self.timeout)
        except FutureTimeoutError as exc:
            self._pending.pop(request_id, None)
            raise FerricStoreError("native request timed out") from exc

    def _submit_request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
    ) -> tuple[int, Future[NativeResponse]]:
        with self._lock:
            request_id = self._next_request_id()
            lane_id = self._next_lane_id(lane_id)
            future: Future[NativeResponse] = Future()
            self._pending[request_id] = future
            try:
                self._send(opcode, lane_id, request_id, payload, flags)
            except Exception:
                self._pending.pop(request_id, None)
                raise
            return request_id, future

    def _reader_loop(self) -> None:
        try:
            while self._sock is not None:
                response = self._recv_response()
                if response.request_id == 0:
                    with self._events_cv:
                        self._events.append(response.value)
                        self._events_cv.notify_all()
                    continue
                future = self._pending.pop(response.request_id, None)
                if future is not None:
                    future.set_result(response)
        except Exception as exc:
            if self._sock is not None:
                self._fail_pending(exc)

    def _fail_pending(self, exc: BaseException) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        for future in pending:
            if not future.done():
                future.set_exception(exc)

    def _recv_matching(self, request_id: int) -> NativeResponse:
        while True:
            response = self._recv_response()
            if response.request_id == request_id:
                return response
            if response.request_id == 0:
                with self._events_cv:
                    self._events.append(response.value)
                    self._events_cv.notify_all()
                continue
            raise FerricStoreError(
                "native response request_id mismatch: "
                f"expected {request_id}, got {response.request_id}",
                raw=response,
            )

    def _recv_response(self) -> NativeResponse:
        header = self._recv_exact(_HEADER.size)
        magic, version, flags, lane_id, opcode, request_id, body_len = _HEADER.unpack(header)
        if magic != _MAGIC or version != _RESPONSE_VERSION:
            raise FerricStoreError("invalid native response frame header")

        body = self._recv_exact(body_len)
        chunks = [body]
        final_flags = flags
        while final_flags & _FLAG_MORE_CHUNKS:
            next_header = self._recv_exact(_HEADER.size)
            (
                next_magic,
                next_version,
                next_flags,
                next_lane_id,
                next_opcode,
                next_request_id,
                next_body_len,
            ) = _HEADER.unpack(next_header)
            if (
                next_magic != _MAGIC
                or next_version != _RESPONSE_VERSION
                or next_lane_id != lane_id
                or next_opcode != opcode
                or next_request_id != request_id
            ):
                raise FerricStoreError("invalid native chunk continuation")
            chunks.append(self._recv_exact(next_body_len))
            final_flags = next_flags

        body = b"".join(chunks)
        if final_flags & _FLAG_COMPRESSED:
            body = zlib.decompress(body)

        if len(body) < _STATUS.size:
            raise FerricStoreError("native response body is too short")

        status = _STATUS.unpack_from(body, 0)[0]
        value = (
            _try_fast_response_value_at(opcode, body, _STATUS.size)
            if status == _STATUS_OK
            else None
        )
        if value is None:
            value_body = body[_STATUS.size :]
            value, rest = decode_value(value_body)
        else:
            rest = b""
        if rest:
            raise FerricStoreError("native response value has trailing bytes")

        return NativeResponse(
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
            flags=final_flags,
            status=status,
            value=value,
        )

    def _recv_exact(self, size: int) -> bytes:
        sock = self._require_socket()
        if size == 0:
            return b""
        buffer = bytearray(size)
        view = memoryview(buffer)
        received = 0
        while received < size:
            count = sock.recv_into(view[received:], size - received)
            if count == 0:
                raise FerricStoreError("native connection closed")
            received += count
        return bytes(buffer)

    def _require_socket(self) -> socket.socket | ssl.SSLSocket:
        if self._sock is None:
            raise FerricStoreError("native connection is closed")
        return self._sock

    def _response_value(self, response: NativeResponse) -> Any:
        return _response_value(response)

    def _batch_item_value(self, item: Any) -> Any:
        return _batch_item_value(item)


class NativeAdapterPool:
    """Small native socket pool; each socket still multiplexes request lanes."""

    client: NativeAdapterPool

    def __init__(self, adapters: list[NativeAdapter]) -> None:
        if not adapters:
            raise ValueError("NativeAdapterPool requires at least one adapter")
        self.adapters = adapters
        self.client = self
        self._lock = threading.Lock()
        self._cursor = 0

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> NativeAdapterPool | NativeAdapter:
        max_connections = int(kwargs.pop("max_connections", 1) or 1)
        if max_connections <= 1:
            return NativeAdapter.from_url(url, **kwargs)
        return cls([NativeAdapter.from_url(url, **kwargs) for _ in range(max_connections)])

    @property
    def events(self) -> list[Any]:
        events: list[Any] = []
        for adapter in self.adapters:
            events.extend(adapter.events)
        return events

    def wait_event(self, timeout: float | None = None) -> Any | None:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            for adapter in self.adapters:
                event = adapter.wait_event(timeout=0.0)
                if event is not None:
                    return event
            if timeout == 0.0:
                return None
            wait_for = 0.05
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                wait_for = min(wait_for, remaining)
            time.sleep(wait_for)

    def close(self) -> None:
        for adapter in self.adapters:
            adapter.close()

    def pipeline(self, transaction: bool = False) -> NativePipeline:
        if transaction:
            raise InvalidCommandError("native pipeline does not support Redis transactions")
        return NativePipeline(self)

    def execute_command(self, *args: Any) -> Any:
        return self._next_adapter().execute_command(*args)

    def submit_command(self, *args: Any) -> Future[Any]:
        return self._next_adapter().submit_command(*args)

    def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> Any:
        replies = [adapter.subscribe_flow_wake(*args, **kwargs) for adapter in self.adapters]
        return replies[0] if len(replies) == 1 else replies

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return self._next_adapter().execute_batch(commands)

    def _next_adapter(self) -> NativeAdapter:
        with self._lock:
            adapter = self.adapters[self._cursor % len(self.adapters)]
            self._cursor += 1
            return adapter


class NativePipeline:
    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        self.commands: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> NativePipeline:
        self.commands.append(args)
        return self

    def execute(self) -> list[Any]:
        return cast(list[Any], self.adapter.execute_batch(self.commands))


class AsyncNativeAdapter:
    """FerricStore native TCP adapter for the async SDK."""

    client: AsyncNativeAdapter

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6388,
        *,
        username: str | None = None,
        password: str | None = None,
        tls: bool = False,
        timeout: float | None = 30.0,
        client_name: str | None = "ferricstore-python-async",
        compression: str = "none",
        lanes: int = 16,
        write_drain_bytes: int = 1_048_576,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username or "default"
        self.password = password
        self.tls = tls
        self.timeout = timeout
        self.client_name = client_name
        self.compression = compression
        self.lanes = max(1, lanes)
        self.write_drain_bytes = max(0, write_drain_bytes)
        self.ssl_context = ssl_context
        self.client = self
        self._connect_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._request_id = 0
        self._lane_cursor = 0
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[NativeResponse]] = {}
        self._events: list[Any] = []
        self._queued_write_bytes = 0

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AsyncNativeAdapter:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        tls = scheme in _TLS_SCHEMES
        if scheme not in _SUPPORTED_SCHEMES:
            raise ValueError(f"unsupported FerricStore URL scheme: {parsed.scheme}")

        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (6389 if tls else 6388)
        username = unquote(parsed.username) if parsed.username else None
        password = unquote(parsed.password) if parsed.password else None
        _normalize_native_url_kwargs(kwargs)
        kwargs.setdefault("username", username)
        kwargs.setdefault("password", password)
        kwargs.setdefault("tls", tls)
        return cls(host, port, **kwargs)

    @property
    def events(self) -> list[Any]:
        return list(self._events)

    async def close(self) -> None:
        writer = self._writer
        reader_task = self._reader_task
        self._reader = None
        self._writer = None
        self._reader_task = None
        if reader_task is not None:
            reader_task.cancel()
        if writer is not None:
            writer.close()
            await writer.wait_closed()
        if reader_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await reader_task
        self._fail_pending(FerricStoreError("native connection is closed"))

    def pipeline(self, transaction: bool = False) -> AsyncNativePipeline:
        if transaction:
            raise InvalidCommandError("native pipeline does not support Redis transactions")
        return AsyncNativePipeline(self)

    async def execute_command(self, *args: Any) -> Any:
        command = translate_command(*args)
        await self._ensure_connected()
        response = await self._request(
            command.opcode, command.lane_id, command.payload, command.flags
        )
        return self._response_value(response)

    async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        if not commands:
            return []

        native_commands = [translate_command(*command) for command in commands]
        if any(command.opcode in _CONTROL_OPCODES or command.flags for command in native_commands):
            return [await self.execute_command(*command) for command in commands]

        batch_commands = [
            {
                "opcode": command.opcode,
                "lane_id": command.lane_id,
                "request_id": idx + 1,
                "body": command.payload,
            }
            for idx, command in enumerate(native_commands)
        ]
        await self._ensure_connected()
        response = await self._request(
            _OP_BATCH,
            0,
            {"atomicity": "none", "commands": batch_commands},
        )

        values = self._response_value(response)
        if not isinstance(values, list):
            raise FerricStoreError("native BATCH returned non-list response", raw=values)

        return [self._batch_item_value(item) for item in values]

    async def _ensure_connected(self) -> None:
        if self._writer is not None:
            return

        async with self._connect_lock:
            if self._writer is not None:
                return

            context = (self.ssl_context or ssl.create_default_context()) if self.tls else None
            connect = asyncio.open_connection(
                self.host,
                self.port,
                ssl=context,
                server_hostname=self.host if self.tls else None,
            )
            if self.timeout is None:
                self._reader, self._writer = await connect
            else:
                self._reader, self._writer = await asyncio.wait_for(connect, self.timeout)
            self._reader_task = asyncio.create_task(self._reader_loop())

            startup: dict[str, Any] = {
                "compression": self.compression,
                "compact_flow_responses": True,
            }
            if self.client_name is not None:
                startup["client_name"] = self.client_name
                startup["driver_name"] = self.client_name

            self._response_value(await self._request(_OP_STARTUP, 0, startup))

            if self.password is not None:
                self._response_value(
                    await self._request(
                        _OP_AUTH,
                        0,
                        {"username": self.username, "password": self.password},
                    )
                )

    def _next_request_id(self) -> int:
        self._request_id = (self._request_id + 1) & 0xFFFFFFFFFFFFFFFF
        if self._request_id == 0:
            self._request_id = 1
        return self._request_id

    def _next_lane_id(self, lane_id: int) -> int:
        if lane_id == 0 or self.lanes == 1:
            return lane_id
        self._lane_cursor = (self._lane_cursor % self.lanes) + 1
        return self._lane_cursor

    async def _send(
        self,
        opcode: int,
        lane_id: int,
        request_id: int,
        payload: dict[str, Any] | bytes,
        extra_flags: int = 0,
    ) -> None:
        writer = self._require_writer()
        body = payload if isinstance(payload, bytes) else encode_value(payload)
        flags = extra_flags
        if self.compression == "zlib" and body:
            body = zlib.compress(body)
            flags |= _FLAG_COMPRESSED
        header = _HEADER.pack(
            _MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)
        )
        writer.writelines((header, body))
        self._queued_write_bytes += len(header) + len(body)
        if self.write_drain_bytes == 0 or self._queued_write_bytes >= self.write_drain_bytes:
            self._queued_write_bytes = 0
            await writer.drain()

    async def _request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
    ) -> NativeResponse:
        loop = asyncio.get_running_loop()
        async with self._write_lock:
            request_id = self._next_request_id()
            lane_id = self._next_lane_id(lane_id)
            future: asyncio.Future[NativeResponse] = loop.create_future()
            self._pending[request_id] = future
            try:
                await self._send(opcode, lane_id, request_id, payload, flags)
            except Exception:
                self._pending.pop(request_id, None)
                raise
        return await future

    async def _reader_loop(self) -> None:
        try:
            while self._reader is not None:
                response = await self._recv_response()
                if response.request_id == 0:
                    self._events.append(response.value)
                    continue
                future = self._pending.pop(response.request_id, None)
                if future is not None and not future.done():
                    future.set_result(response)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._reader = None
            self._writer = None
            self._fail_pending(exc)

    def _fail_pending(self, exc: BaseException) -> None:
        pending = list(self._pending.values())
        self._pending.clear()
        for future in pending:
            if not future.done():
                future.set_exception(exc)

    async def _recv_matching(self, request_id: int) -> NativeResponse:
        while True:
            response = await self._recv_response()
            if response.request_id == request_id:
                return response
            if response.request_id == 0:
                self._events.append(response.value)
                continue
            raise FerricStoreError(
                "native response request_id mismatch: "
                f"expected {request_id}, got {response.request_id}",
                raw=response,
            )

    async def _recv_response(self) -> NativeResponse:
        header = await self._recv_exact(_HEADER.size)
        magic, version, flags, lane_id, opcode, request_id, body_len = _HEADER.unpack(header)
        if magic != _MAGIC or version != _RESPONSE_VERSION:
            raise FerricStoreError("invalid native response frame header")

        body = await self._recv_exact(body_len)
        chunks = [body]
        final_flags = flags
        while final_flags & _FLAG_MORE_CHUNKS:
            next_header = await self._recv_exact(_HEADER.size)
            (
                next_magic,
                next_version,
                next_flags,
                next_lane_id,
                next_opcode,
                next_request_id,
                next_body_len,
            ) = _HEADER.unpack(next_header)
            if (
                next_magic != _MAGIC
                or next_version != _RESPONSE_VERSION
                or next_lane_id != lane_id
                or next_opcode != opcode
                or next_request_id != request_id
            ):
                raise FerricStoreError("invalid native chunk continuation")
            chunks.append(await self._recv_exact(next_body_len))
            final_flags = next_flags

        body = b"".join(chunks)
        if final_flags & _FLAG_COMPRESSED:
            body = zlib.decompress(body)

        if len(body) < _STATUS.size:
            raise FerricStoreError("native response body is too short")

        status = _STATUS.unpack_from(body, 0)[0]
        value = (
            _try_fast_response_value_at(opcode, body, _STATUS.size)
            if status == _STATUS_OK
            else None
        )
        if value is None:
            value_body = body[_STATUS.size :]
            value, rest = decode_value(value_body)
        else:
            rest = b""
        if rest:
            raise FerricStoreError("native response value has trailing bytes")

        return NativeResponse(
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
            flags=final_flags,
            status=status,
            value=value,
        )

    async def _recv_exact(self, size: int) -> bytes:
        reader = self._require_reader()
        try:
            return await reader.readexactly(size)
        except asyncio.IncompleteReadError as exc:
            raise FerricStoreError("native connection closed") from exc

    def _require_reader(self) -> asyncio.StreamReader:
        if self._reader is None:
            raise FerricStoreError("native connection is closed")
        return self._reader

    def _require_writer(self) -> asyncio.StreamWriter:
        if self._writer is None:
            raise FerricStoreError("native connection is closed")
        return self._writer

    def _response_value(self, response: NativeResponse) -> Any:
        return _response_value(response)

    def _batch_item_value(self, item: Any) -> Any:
        return _batch_item_value(item)


class AsyncNativeAdapterPool:
    """Small async native socket pool; each socket still multiplexes request lanes."""

    client: AsyncNativeAdapterPool

    def __init__(self, adapters: list[AsyncNativeAdapter]) -> None:
        if not adapters:
            raise ValueError("AsyncNativeAdapterPool requires at least one adapter")
        self.adapters = adapters
        self.client = self
        self._lock = asyncio.Lock()
        self._cursor = 0

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AsyncNativeAdapterPool | AsyncNativeAdapter:
        max_connections = int(kwargs.pop("max_connections", 1) or 1)
        if max_connections <= 1:
            return AsyncNativeAdapter.from_url(url, **kwargs)
        return cls([AsyncNativeAdapter.from_url(url, **kwargs) for _ in range(max_connections)])

    @property
    def events(self) -> list[Any]:
        events: list[Any] = []
        for adapter in self.adapters:
            events.extend(adapter.events)
        return events

    async def close(self) -> None:
        await asyncio.gather(*(adapter.close() for adapter in self.adapters))

    def pipeline(self, transaction: bool = False) -> AsyncNativePipeline:
        if transaction:
            raise InvalidCommandError("native pipeline does not support Redis transactions")
        return AsyncNativePipeline(self)

    async def execute_command(self, *args: Any) -> Any:
        adapter = await self._next_adapter()
        return await adapter.execute_command(*args)

    async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        adapter = await self._next_adapter()
        return await adapter.execute_batch(commands)

    async def _next_adapter(self) -> AsyncNativeAdapter:
        async with self._lock:
            adapter = self.adapters[self._cursor % len(self.adapters)]
            self._cursor += 1
            return adapter


class AsyncNativePipeline:
    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        self.commands: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> AsyncNativePipeline:
        self.commands.append(args)
        return self

    async def execute(self) -> list[Any]:
        return cast(list[Any], await self.adapter.execute_batch(self.commands))


def translate_command(*args: Any) -> NativeCommand:
    if not args:
        raise InvalidCommandError("native command requires command name")

    name = _command_name(args[0])
    if name not in _OPCODES:
        raise InvalidCommandError(f"native transport does not support command {name}")

    if name in {
        "GET",
        "SET",
        "DEL",
        "MGET",
        "MSET",
        "PING",
        "CLIENT.SETNAME",
        "CLIENT.INFO",
        "CAS",
        "LOCK",
        "UNLOCK",
        "EXTEND",
        "RATELIMIT.ADD",
        "FETCH_OR_COMPUTE",
        "FETCH_OR_COMPUTE_RESULT",
        "FETCH_OR_COMPUTE_ERROR",
        "CLUSTER.HEALTH",
        "CLUSTER.STATS",
        "CLUSTER.KEYSLOT",
        "CLUSTER.SLOTS",
        "CLUSTER.STATUS",
        "CLUSTER.JOIN",
        "CLUSTER.LEAVE",
        "CLUSTER.FAILOVER",
        "CLUSTER.PROMOTE",
        "CLUSTER.DEMOTE",
        "CLUSTER.ROLE",
        "FERRICSTORE.KEY_INFO",
        "FERRICSTORE.CONFIG",
        "FERRICSTORE.HOTNESS",
        "FERRICSTORE.METRICS",
        "FERRICSTORE.BLOBGC",
    }:
        return _translate_basic(name, args[1:])

    if name.startswith("FLOW."):
        return _translate_flow(name, args[1:])

    return NativeCommand(_OPCODES[name], _option_map(args[1:]), _lane_for_opcode(_OPCODES[name]))


def encode_frame(opcode: int, lane_id: int, request_id: int, value: Any, flags: int = 0) -> bytes:
    body = encode_value(value)
    return (
        _HEADER.pack(_MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)) + body
    )


def _compact_flow_create_many_payload(payload: dict[str, Any]) -> bytes | None:
    allowed = {"type", "state", "now_ms", "run_at_ms", "independent", "items", "return"}
    if not set(payload).issubset(allowed):
        return None
    if "partition_key" in payload:
        return None
    items = payload.get("items")
    if not isinstance(items, list):
        return None
    type_value = _maybe_bytes(payload.get("type"))
    state = _maybe_bytes(payload.get("state"))
    now_ms = payload.get("now_ms")
    run_at_ms = payload.get("run_at_ms")
    if (
        type_value is None
        or state is None
        or not isinstance(now_ms, int)
        or not isinstance(run_at_ms, int)
    ):
        return None
    return_mode = _compact_create_many_return_mode(payload.get("return"))
    if return_mode is None:
        return None

    parts = [
        bytes([_COMPACT_FLOW_CREATE_MANY_REQUEST]),
        _compact_binary(type_value),
        _compact_binary(state),
        struct.pack(
            ">qqBBI",
            now_ms,
            run_at_ms,
            _compact_bool_marker(payload.get("independent")),
            return_mode,
            len(items),
        ),
    ]
    for item in items:
        if not isinstance(item, list) or len(item) != 2:
            return None
        item_id = _maybe_bytes(item[0])
        item_payload = _maybe_bytes(item[1])
        if item_id is None or item_payload is None:
            return None
        parts.append(_compact_binary(item_id))
        parts.append(_compact_binary(item_payload))
    return b"".join(parts)


def _compact_flow_claim_due_payload(payload: dict[str, Any]) -> bytes | None:
    allowed = {
        "type",
        "state",
        "worker",
        "lease_ms",
        "limit",
        "partition_key",
        "partition_keys",
        "return",
        "block_ms",
        "reclaim_expired",
        "reclaim_ratio",
        "priority",
    }
    if not set(payload).issubset(allowed):
        return None
    if "partition_key" in payload and "partition_keys" in payload:
        return None
    type_value = _maybe_bytes(payload.get("type"))
    state = _optional_bytes(payload.get("state"))
    worker = _maybe_bytes(payload.get("worker"))
    lease_ms = payload.get("lease_ms")
    limit = payload.get("limit")
    block_ms = payload.get("block_ms", -1)
    reclaim_ratio = payload.get("reclaim_ratio", 0)
    priority = payload.get("priority", _I64_MIN)
    if (
        type_value is None
        or state is False
        or worker is None
        or not isinstance(lease_ms, int)
        or not isinstance(limit, int)
        or not isinstance(block_ms, int)
        or not isinstance(reclaim_ratio, int)
        or not isinstance(priority, int)
    ):
        return None
    return_mode = _compact_return_mode(payload.get("return"))
    if return_mode is None:
        return None

    partition_mode, partition_body = _compact_partition_request(payload)
    if partition_mode is None:
        return None

    return b"".join(
        [
            bytes([_COMPACT_FLOW_CLAIM_DUE_REQUEST]),
            _compact_binary(type_value),
            _compact_optional_binary(cast(bytes | None, state)),
            _compact_binary(worker),
            struct.pack(
                ">qqqBqqBB",
                lease_ms,
                limit,
                block_ms,
                1 if payload.get("reclaim_expired") else 0,
                reclaim_ratio,
                priority,
                return_mode,
                partition_mode,
            ),
            partition_body,
        ]
    )


def _compact_flow_complete_many_payload(payload: dict[str, Any]) -> bytes | None:
    allowed = {"partition_key", "now_ms", "independent", "items"}
    if not set(payload).issubset(allowed):
        return None
    now_ms = payload.get("now_ms")
    items = payload.get("items")
    if not isinstance(now_ms, int) or not isinstance(items, list):
        return None
    partition_key = _optional_bytes(payload.get("partition_key"))
    if partition_key is False:
        return None

    parts = [
        bytes([_COMPACT_FLOW_COMPLETE_MANY_REQUEST]),
        _compact_optional_binary(cast(bytes | None, partition_key)),
        struct.pack(">qBI", now_ms, _compact_bool_marker(payload.get("independent")), len(items)),
    ]
    for item in items:
        if not isinstance(item, list) or len(item) not in {3, 4}:
            return None
        item_id = _maybe_bytes(item[0])
        item_partition = None
        if len(item) == 4:
            item_partition = _maybe_bytes(item[1])
            if item_partition is None:
                return None
            lease_token = _maybe_bytes(item[2])
            fencing_token = item[3]
        else:
            lease_token = _maybe_bytes(item[1])
            fencing_token = item[2]
        if (
            item_id is None
            or lease_token is None
            or not isinstance(fencing_token, int)
        ):
            return None
        parts.append(_compact_binary(item_id))
        parts.append(_compact_optional_binary(cast(bytes | None, item_partition)))
        parts.append(_compact_binary(lease_token))
        parts.append(struct.pack(">q", fencing_token))
    return b"".join(parts)


def _compact_partition_request(payload: dict[str, Any]) -> tuple[int | None, bytes]:
    if "partition_key" in payload:
        value = _maybe_bytes(payload.get("partition_key"))
        if value is None:
            return None, b""
        return 1, _compact_binary(value)
    if "partition_keys" in payload:
        values = payload.get("partition_keys")
        if not isinstance(values, list):
            return None, b""
        parts = [struct.pack(">I", len(values))]
        for value in values:
            encoded = _maybe_bytes(value)
            if encoded is None:
                return None, b""
            parts.append(_compact_binary(encoded))
        return 2, b"".join(parts)
    return 0, b""


def _maybe_bytes(value: Any) -> bytes | None:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    return None


def _optional_bytes(value: Any) -> bytes | None | bool:
    if value is None:
        return None
    encoded = _maybe_bytes(value)
    return encoded if encoded is not None else False


def _compact_binary(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def _compact_optional_binary(value: bytes | None) -> bytes:
    if value is None:
        return struct.pack(">I", _NULL_U32)
    return _compact_binary(value)


def _compact_bool_marker(value: Any) -> int:
    if value is None:
        return 0
    return 2 if bool(value) else 1


def _compact_return_mode(value: Any) -> int | None:
    if value is None:
        return 0
    if value in {"jobs_compact", "JOBS_COMPACT"}:
        return 1
    if value in {"jobs_compact_state", "JOBS_COMPACT_STATE"}:
        return 2
    return None


def _compact_create_many_return_mode(value: Any) -> int | None:
    if value is None:
        return 0
    normalized = _maybe_bytes(value)
    if normalized is None:
        return None
    if normalized.upper() == b"OK_ON_SUCCESS":
        return 1
    return None


def _send_frame(sock: socket.socket | ssl.SSLSocket, header: bytes, body: bytes) -> None:
    sock.sendall(header if not body else header + body)


def encode_value(value: Any) -> bytes:
    if value is None:
        return b"\x00"
    if value is True:
        return b"\x01"
    if value is False:
        return b"\x02"
    if isinstance(value, int):
        return b"\x03" + struct.pack(">q", value)
    if isinstance(value, str):
        return _encode_binary(value.encode())
    if isinstance(value, bytes):
        return _encode_binary(value)
    if isinstance(value, bytearray):
        return _encode_binary(bytes(value))
    if isinstance(value, (list, tuple)):
        body = b"".join(encode_value(item) for item in value)
        return b"\x05" + struct.pack(">I", len(value)) + body
    if isinstance(value, dict):
        entries = []
        for key, item in value.items():
            encoded_key = _key_bytes(key)
            entries.append(struct.pack(">I", len(encoded_key)) + encoded_key + encode_value(item))
        return b"\x06" + struct.pack(">I", len(value)) + b"".join(entries)
    if isinstance(value, float):
        return b"\x07" + struct.pack(">d", value)
    return _encode_binary(str(value).encode())


def decode_value(data: bytes) -> tuple[Any, bytes]:
    if not data:
        raise FerricStoreError("native value is empty")
    tag = data[0]
    rest = data[1:]
    if tag == 0:
        return None, rest
    if tag == 1:
        return True, rest
    if tag == 2:
        return False, rest
    if tag == 3:
        _require_len(rest, 8)
        return struct.unpack(">q", rest[:8])[0], rest[8:]
    if tag == 4:
        return _decode_binary(rest)
    if tag == 5:
        _require_len(rest, 4)
        count = struct.unpack(">I", rest[:4])[0]
        items = []
        next_data = rest[4:]
        for _ in range(count):
            item, next_data = decode_value(next_data)
            items.append(item)
        return items, next_data
    if tag == 6:
        _require_len(rest, 4)
        count = struct.unpack(">I", rest[:4])[0]
        result: dict[bytes, Any] = {}
        next_data = rest[4:]
        for _ in range(count):
            key, after_key = _decode_binary(next_data)
            value, next_data = decode_value(after_key)
            result[key] = value
        return result, next_data
    if tag == 7:
        _require_len(rest, 8)
        return struct.unpack(">d", rest[:8])[0], rest[8:]
    raise FerricStoreError("native value has unknown tag")


def _try_fast_response_value(opcode: int, data: bytes) -> Any | None:
    return _try_fast_response_value_at(opcode, data, 0)


def _try_fast_response_value_at(opcode: int, data: bytes, offset: int) -> Any | None:
    if opcode == _OP_FLOW_CLAIM_DUE:
        if len(data) > offset and data[offset] == _COMPACT_FLOW_CLAIM_JOBS:
            return _try_decode_custom_claim_jobs(data, offset)
        return _try_decode_claim_jobs_compact(data, offset)
    if opcode in {
        _OP_FLOW_CREATE_MANY,
        _OP_FLOW_COMPLETE_MANY,
        _OP_FLOW_RETRY_MANY,
        _OP_FLOW_FAIL_MANY,
        _OP_FLOW_CANCEL_MANY,
    }:
        if len(data) > offset and data[offset] == _COMPACT_OK_LIST:
            return _try_decode_custom_ok_list(data, offset)
        return _try_decode_binary_list(data, offset)
    return None


def _try_decode_custom_claim_jobs(data: bytes, offset: int = 0) -> list[list[Any]] | None:
    try:
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        items: list[list[Any]] = []
        for _ in range(count):
            id_value, offset = _read_compact_binary(data, offset)
            partition, offset = _read_compact_optional_binary(data, offset)
            lease, offset = _read_compact_binary(data, offset)
            _require_available(data, offset, 8)
            fencing = struct.unpack_from(">q", data, offset)[0]
            offset += 8
            items.append([id_value, partition, lease, fencing])
        if offset != len(data):
            return None
        return items
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_custom_ok_list(data: bytes, offset: int = 0) -> list[bytes] | None:
    try:
        if len(data) - offset != 5:
            return None
        return [b"OK"] * _read_u32(data, offset + 1)
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_claim_jobs_compact(data: bytes, offset: int = 0) -> list[list[Any]] | None:
    try:
        if data[offset] != 5:
            return None
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        items: list[list[Any]] = []
        for _ in range(count):
            if data[offset] != 5:
                return None
            offset += 1
            width = _read_u32(data, offset)
            offset += 4
            if width != 4:
                return None
            id_value, offset = _read_tagged_binary(data, offset)
            partition, offset = _read_tagged_binary(data, offset)
            lease, offset = _read_tagged_binary(data, offset)
            fencing, offset = _read_tagged_i64(data, offset)
            items.append([id_value, partition, lease, fencing])
        if offset != len(data):
            return None
        return items
    except (IndexError, struct.error, FerricStoreError):
        return None


def _try_decode_binary_list(data: bytes, offset: int = 0) -> list[bytes] | None:
    try:
        if data[offset] != 5:
            return None
        offset += 1
        count = _read_u32(data, offset)
        offset += 4
        items: list[bytes] = []
        for _ in range(count):
            item, offset = _read_tagged_binary(data, offset)
            items.append(item)
        if offset != len(data):
            return None
        return items
    except (IndexError, struct.error, FerricStoreError):
        return None


def _read_u32(data: bytes, offset: int) -> int:
    _require_available(data, offset, 4)
    value: int = struct.unpack_from(">I", data, offset)[0]
    return value


def _read_tagged_binary(data: bytes, offset: int) -> tuple[bytes, int]:
    if data[offset] != 4:
        raise FerricStoreError("native fast path expected binary")
    offset += 1
    size = _read_u32(data, offset)
    offset += 4
    _require_available(data, offset, size)
    return data[offset : offset + size], offset + size


def _read_tagged_i64(data: bytes, offset: int) -> tuple[int, int]:
    if data[offset] != 3:
        raise FerricStoreError("native fast path expected integer")
    offset += 1
    _require_available(data, offset, 8)
    return struct.unpack_from(">q", data, offset)[0], offset + 8


def _read_compact_binary(data: bytes, offset: int) -> tuple[bytes, int]:
    size = _read_u32(data, offset)
    offset += 4
    if size == _NULL_U32:
        raise FerricStoreError("native compact value expected binary")
    _require_available(data, offset, size)
    return data[offset : offset + size], offset + size


def _read_compact_optional_binary(data: bytes, offset: int) -> tuple[bytes | None, int]:
    size = _read_u32(data, offset)
    offset += 4
    if size == _NULL_U32:
        return None, offset
    _require_available(data, offset, size)
    return data[offset : offset + size], offset + size


def _require_available(data: bytes, offset: int, size: int) -> None:
    if len(data) - offset < size:
        raise FerricStoreError("native value is truncated")


def _translate_basic(name: str, args: tuple[Any, ...]) -> NativeCommand:
    opcode = _OPCODES[name]
    if name == "PING":
        payload = {"message": args[0]} if args else {}
        return NativeCommand(opcode, payload, 0)
    if name == "CLIENT.SETNAME":
        return NativeCommand(opcode, {"name": _require_arg(args, 0, name)}, 0)
    if name == "CLIENT.INFO":
        return NativeCommand(opcode, {}, 0)
    if name == "GET":
        return NativeCommand(opcode, {"key": _require_arg(args, 0, name)})
    if name == "SET":
        payload = {"key": _require_arg(args, 0, name), "value": _require_arg(args, 1, name)}
        payload.update(_kv_set_options(args[2:]))
        return NativeCommand(opcode, payload)
    if name == "DEL":
        return NativeCommand(opcode, {"keys": list(args)})
    if name == "MGET":
        return NativeCommand(opcode, {"keys": list(args)})
    if name == "MSET":
        if len(args) % 2 != 0:
            raise InvalidCommandError("MSET requires key/value pairs")
        pairs = [{"key": args[idx], "value": args[idx + 1]} for idx in range(0, len(args), 2)]
        return NativeCommand(opcode, {"pairs": pairs})
    if name == "CAS":
        payload = {
            "key": _require_arg(args, 0, name),
            "expected": _require_arg(args, 1, name),
            "value": _require_arg(args, 2, name),
        }
        idx = 3
        while idx < len(args):
            token = _command_token(args[idx])
            if token == "EX":
                payload["ttl"] = int(_require_arg(args, idx + 1, "EX")) * 1000
                idx += 2
            elif token == "PX":
                payload["ttl"] = int(_require_arg(args, idx + 1, "PX"))
                idx += 2
            else:
                raise InvalidCommandError(f"native CAS does not support option {token}")
        return NativeCommand(opcode, payload)
    if name in {"LOCK", "EXTEND"}:
        return NativeCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "owner": _require_arg(args, 1, name),
                "ttl_ms": _require_arg(args, 2, name),
            },
        )
    if name == "UNLOCK":
        return NativeCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "owner": _require_arg(args, 1, name)},
        )
    if name == "RATELIMIT.ADD":
        payload = {
            "key": _require_arg(args, 0, name),
            "window_ms": _require_arg(args, 1, name),
            "max": _require_arg(args, 2, name),
        }
        if len(args) > 3:
            payload["count"] = _require_arg(args, 3, name)
        return NativeCommand(opcode, payload)
    if name == "FETCH_OR_COMPUTE":
        payload = {"key": _require_arg(args, 0, name), "ttl_ms": _require_arg(args, 1, name)}
        if len(args) > 2:
            payload["hint"] = _require_arg(args, 2, name)
        return NativeCommand(opcode, payload)
    if name == "FETCH_OR_COMPUTE_RESULT":
        return NativeCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "value": _require_arg(args, 1, name),
                "ttl_ms": _require_arg(args, 2, name),
            },
        )
    if name == "FETCH_OR_COMPUTE_ERROR":
        return NativeCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "message": _require_arg(args, 1, name)},
        )
    if name in {"CLUSTER.KEYSLOT", "FERRICSTORE.KEY_INFO"}:
        key = _require_arg(args, 0, name)
        return NativeCommand(opcode, {"key": key, "args": [key]})
    if name in {
        "CLUSTER.HEALTH",
        "CLUSTER.STATS",
        "CLUSTER.SLOTS",
        "CLUSTER.STATUS",
        "CLUSTER.ROLE",
        "CLUSTER.LEAVE",
        "FERRICSTORE.HOTNESS",
        "FERRICSTORE.METRICS",
        "FERRICSTORE.BLOBGC",
    }:
        return NativeCommand(opcode, {"args": list(args)})
    if name in {
        "CLUSTER.JOIN",
        "CLUSTER.FAILOVER",
        "CLUSTER.PROMOTE",
        "CLUSTER.DEMOTE",
        "FERRICSTORE.CONFIG",
    }:
        return NativeCommand(opcode, {"args": list(args)})
    raise InvalidCommandError(f"native transport does not support command {name}")


def _translate_flow(name: str, args: tuple[Any, ...]) -> NativeCommand:
    opcode = _OPCODES[name]
    if name == "FLOW.CREATE":
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return NativeCommand(opcode, payload)
    if name == "FLOW.CREATE_MANY":
        payload = _flow_create_many_payload(args)
        compact = _compact_flow_create_many_payload(payload)
        if compact is not None:
            return NativeCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return NativeCommand(opcode, payload)
    if name == "FLOW.CLAIM_DUE":
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        _collapse_states(payload)
        compact = _compact_flow_claim_due_payload(payload)
        if compact is not None:
            return NativeCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return NativeCommand(opcode, payload)
    if name in {"FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL", "FLOW.EXTEND_LEASE"}:
        payload = {"id": _require_arg(args, 0, name), "lease_token": _require_arg(args, 1, name)}
        payload.update(_option_map(args[2:]))
        return NativeCommand(opcode, payload)
    if name == "FLOW.TRANSITION":
        payload = {
            "id": _require_arg(args, 0, name),
            "from_state": _require_arg(args, 1, name),
            "to_state": _require_arg(args, 2, name),
        }
        payload.update(_option_map(args[3:]))
        return NativeCommand(opcode, payload)
    if name == "FLOW.CANCEL":
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return NativeCommand(opcode, payload)
    if name in {"FLOW.COMPLETE_MANY", "FLOW.RETRY_MANY", "FLOW.FAIL_MANY"}:
        payload = _flow_claimed_many_payload(name, args)
        if name == "FLOW.COMPLETE_MANY":
            compact = _compact_flow_complete_many_payload(payload)
            if compact is not None:
                return NativeCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return NativeCommand(opcode, payload)
    if name == "FLOW.TRANSITION_MANY":
        payload = {
            "from_state": _require_arg(args, 1, name),
            "to_state": _require_arg(args, 2, name),
        }
        payload.update(_flow_fenced_many_payload(name, args[0:1] + args[3:], include_lease=True))
        return NativeCommand(opcode, payload)
    if name == "FLOW.CANCEL_MANY":
        return NativeCommand(opcode, _flow_fenced_many_payload(name, args, include_lease=False))
    if name in {
        "FLOW.GET",
        "FLOW.HISTORY",
        "FLOW.REWIND",
        "FLOW.BY_PARENT",
        "FLOW.BY_ROOT",
        "FLOW.BY_CORRELATION",
        "FLOW.SIGNAL",
    }:
        key = {
            "FLOW.BY_PARENT": "parent_id",
            "FLOW.BY_ROOT": "root_id",
            "FLOW.BY_CORRELATION": "correlation_id",
        }.get(name, "id")
        payload = {key: _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return NativeCommand(opcode, payload)
    if name in {"FLOW.LIST", "FLOW.TERMINALS", "FLOW.FAILURES", "FLOW.INFO", "FLOW.STUCK"}:
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return NativeCommand(opcode, payload)
    if name == "FLOW.VALUE.PUT":
        payload = {"value": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return NativeCommand(opcode, payload)
    if name == "FLOW.VALUE.MGET":
        refs, options = _split_refs_and_options(args)
        payload = {"refs": refs}
        payload.update(options)
        return NativeCommand(opcode, payload)
    if name in {"FLOW.POLICY.SET", "FLOW.POLICY.GET", "FLOW.RECLAIM"}:
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return NativeCommand(opcode, payload)
    if name == "FLOW.SPAWN_CHILDREN":
        return NativeCommand(opcode, _flow_spawn_children_payload(args))
    if name == "FLOW.RETENTION_CLEANUP":
        return NativeCommand(opcode, _option_map(args))
    raise InvalidCommandError(f"native transport does not support command {name}")


def _flow_create_many_payload(args: tuple[Any, ...]) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, "FLOW.CREATE_MANY"))
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if wire_partition not in {"AUTO", "MIXED", "None", "none"}:
        payload["partition_key"] = args[0]

    token = _command_token(args[item_token])
    if token == "ITEMS_EXT":
        payload["items"] = _parse_create_items_ext(
            args[item_token + 2 :], wire_partition == "MIXED"
        )
    else:
        payload["items"] = _parse_create_items(args[item_token + 1 :], wire_partition == "MIXED")
    return payload


def _flow_spawn_children_payload(args: tuple[Any, ...]) -> dict[str, Any]:
    parent_id = _require_arg(args, 0, "FLOW.SPAWN_CHILDREN")
    item_token = _find_item_token(args, 1)
    payload = {"id": parent_id}
    payload.update(_option_map(args[1:item_token]))

    token = _command_token(args[item_token])
    if token == "ITEMS_EXT":
        payload["children"] = _parse_spawn_children_ext(args[item_token + 2 :])
    else:
        mixed = item_token + 1 < len(args) and _command_token(args[item_token + 1]) == "MIXED"
        start = item_token + 2 if mixed else item_token + 1
        payload["children"] = _parse_spawn_children(args[start:], mixed)
    return payload


def _parse_spawn_children(values: tuple[Any, ...], mixed: bool) -> list[dict[str, Any]]:
    width = 4 if mixed else 3
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.SPAWN_CHILDREN ITEMS has wrong child width")
    children = []
    for idx in range(0, len(values), width):
        if mixed:
            children.append(
                {
                    "id": values[idx],
                    "partition_key": values[idx + 1],
                    "type": values[idx + 2],
                    "payload": values[idx + 3],
                }
            )
        else:
            children.append(
                {
                    "id": values[idx],
                    "type": values[idx + 1],
                    "payload": values[idx + 2],
                }
            )
    return children


def _parse_spawn_children_ext(values: tuple[Any, ...]) -> list[dict[str, Any]]:
    children = []
    idx = 0
    while idx < len(values):
        child = {
            "id": values[idx],
            "type": values[idx + 2],
            "payload": values[idx + 3],
        }
        partition = values[idx + 1]
        if partition != "-":
            child["partition_key"] = partition
        idx += 4

        value_count = int(values[idx])
        idx += 1
        child_values = {}
        for _ in range(value_count):
            child_values[_text(values[idx])] = values[idx + 1]
            idx += 2
        if child_values:
            child["values"] = child_values

        ref_count = int(values[idx])
        idx += 1
        child_refs = {}
        for _ in range(ref_count):
            child_refs[_text(values[idx])] = values[idx + 1]
            idx += 2
        if child_refs:
            child["value_refs"] = child_refs

        children.append(child)
    return children


def _flow_claimed_many_payload(name: str, args: tuple[Any, ...]) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, name))
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if wire_partition != "MIXED":
        payload["partition_key"] = args[0]
    payload["items"] = _parse_claimed_items(args[item_token + 1 :], wire_partition == "MIXED")
    return payload


def _flow_fenced_many_payload(
    name: str, args: tuple[Any, ...], *, include_lease: bool
) -> dict[str, Any]:
    wire_partition = _text(_require_arg(args, 0, name))
    item_token = _find_item_token(args, 1)
    payload = _option_map(args[1:item_token])
    if wire_partition != "MIXED":
        payload["partition_key"] = args[0]
    payload["items"] = _parse_fenced_items(
        args[item_token + 1 :],
        wire_partition == "MIXED",
        include_lease=include_lease,
    )
    return payload


def _option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "NOPAYLOAD":
            payload["payload"] = False
            idx += 1
            continue
        if token == "PAYLOAD":
            payload["payload"] = True
            idx += 1
            continue
        if token == "PARTITIONS":
            count = int(_require_arg(args, idx + 1, "PARTITIONS"))
            payload["partition_keys"] = list(args[idx + 2 : idx + 2 + count])
            idx += 2 + count
            continue
        if token == "STATE":
            value = _require_arg(args, idx + 1, "STATE")
            if "states" in payload:
                payload["states"].append(value)
            elif "state" in payload:
                payload["states"] = [payload.pop("state"), value]
            else:
                payload["state"] = value
            idx += 2
            continue
        if token == "RETURN":
            return_value = _text(_require_arg(args, idx + 1, "RETURN"))
            if return_value in {"JOBS_COMPACT", "JOBS_COMPACT_STATE"}:
                payload["return"] = return_value.lower()
            else:
                payload["return"] = return_value.lower()
            idx += 2
            continue
        if token == "VALUE":
            name = _text(_require_arg(args, idx + 1, "VALUE"))
            value = _require_arg(args, idx + 2, "VALUE")
            payload.setdefault("values", {})[name] = value
            idx += 3
            continue
        if token == "VALUE_REF":
            name = _text(_require_arg(args, idx + 1, "VALUE_REF"))
            ref = _require_arg(args, idx + 2, "VALUE_REF")
            payload.setdefault("value_refs", {})[name] = ref
            idx += 3
            continue
        if token in {"DROP_VALUE", "OVERRIDE_VALUE"}:
            list_field = "drop_values" if token == "DROP_VALUE" else "override_values"
            payload.setdefault(list_field, []).append(_text(_require_arg(args, idx + 1, token)))
            idx += 2
            continue

        mapped_field = _FIELD_NAMES.get(token)
        if mapped_field is None:
            raise InvalidCommandError(f"native transport does not support option {token}")
        value = _require_arg(args, idx + 1, token)
        payload[mapped_field] = _coerce_bool(value) if mapped_field in _BOOL_FIELDS else value
        idx += 2
    return payload


def _kv_set_options(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "EX":
            payload["ttl"] = int(_require_arg(args, idx + 1, "EX")) * 1000
            idx += 2
        elif token == "PX":
            payload["ttl"] = int(_require_arg(args, idx + 1, "PX"))
            idx += 2
        elif token in {"NX", "XX", "GET", "KEEPTTL"}:
            payload[token.lower()] = True
            idx += 1
        else:
            raise InvalidCommandError(f"native SET does not support option {token}")
    return payload


def _parse_create_items(values: tuple[Any, ...], mixed: bool) -> list[list[Any]]:
    width = 3 if mixed else 2
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.CREATE_MANY ITEMS has wrong item width")
    items = []
    for idx in range(0, len(values), width):
        if mixed:
            items.append([values[idx], values[idx + 1], values[idx + 2]])
        else:
            items.append([values[idx], values[idx + 1]])
    return items


def _parse_create_items_ext(values: tuple[Any, ...], mixed: bool) -> list[dict[str, Any]]:
    items = []
    idx = 0
    while idx < len(values):
        item = {"id": values[idx], "payload": values[idx + 2]}
        partition = values[idx + 1]
        if mixed or partition != "-":
            item["partition_key"] = partition
        idx += 3
        value_count = int(values[idx])
        idx += 1
        item_values = {}
        for _ in range(value_count):
            item_values[_text(values[idx])] = values[idx + 1]
            idx += 2
        if item_values:
            item["values"] = item_values
        ref_count = int(values[idx])
        idx += 1
        item_refs = {}
        for _ in range(ref_count):
            item_refs[_text(values[idx])] = values[idx + 1]
            idx += 2
        if item_refs:
            item["value_refs"] = item_refs
        items.append(item)
    return items


def _parse_claimed_items(values: tuple[Any, ...], mixed: bool) -> list[list[Any]]:
    width = 4 if mixed else 3
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.*_MANY ITEMS has wrong claimed item width")
    items = []
    for idx in range(0, len(values), width):
        if mixed:
            items.append([values[idx], values[idx + 1], values[idx + 2], values[idx + 3]])
        else:
            items.append([values[idx], values[idx + 1], values[idx + 2]])
    return items


def _parse_fenced_items(
    values: tuple[Any, ...], mixed: bool, *, include_lease: bool
) -> list[dict[str, Any]]:
    width = (4 if mixed else 3) if include_lease else (3 if mixed else 2)
    if len(values) % width != 0:
        raise InvalidCommandError("FLOW.*_MANY ITEMS has wrong fenced item width")
    items = []
    for idx in range(0, len(values), width):
        item = {"id": values[idx], "fencing_token": values[idx + (2 if mixed else 1)]}
        if mixed:
            item["partition_key"] = values[idx + 1]
        if include_lease:
            lease = values[idx + (3 if mixed else 2)]
            if lease != "-":
                item["lease_token"] = lease
        items.append(item)
    return items


def _split_refs_and_options(args: tuple[Any, ...]) -> tuple[list[Any], dict[str, Any]]:
    refs: list[Any] = []
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token in {"MAX_BYTES", "MAXBYTES"}:
            return refs, {"max_bytes": _require_arg(args, idx + 1, token)}
        refs.append(args[idx])
        idx += 1
    return refs, {}


def _find_item_token(args: tuple[Any, ...], start: int) -> int:
    for idx in range(start, len(args)):
        if _command_token(args[idx]) in {"ITEMS", "ITEMS_EXT"}:
            return idx
    raise InvalidCommandError("FLOW many command requires ITEMS or ITEMS_EXT")


def _collapse_states(payload: dict[str, Any]) -> None:
    states = payload.get("states")
    if isinstance(states, list) and len(states) == 1:
        payload["state"] = states[0]
        del payload["states"]


def _lane_for_opcode(opcode: int) -> int:
    return 0 if opcode in _CONTROL_OPCODES else 1


def _command_name(value: Any) -> str:
    return _text(value).upper()


def _command_token(value: Any) -> str:
    return _text(value).upper()


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _require_arg(args: tuple[Any, ...], idx: int, command: str) -> Any:
    if idx >= len(args):
        raise InvalidCommandError(f"{command} is missing argument {idx + 1}")
    return args[idx]


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, bytes):
        value = value.decode()
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes"}
    return bool(value)


def _encode_binary(value: bytes) -> bytes:
    return b"\x04" + struct.pack(">I", len(value)) + value


def _decode_binary(data: bytes) -> tuple[bytes, bytes]:
    _require_len(data, 4)
    size = struct.unpack(">I", data[:4])[0]
    rest = data[4:]
    _require_len(rest, size)
    return rest[:size], rest[size:]


def _require_len(data: bytes, size: int) -> None:
    if len(data) < size:
        raise FerricStoreError("native value is truncated")


def _key_bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    return str(value).encode()


def _map_get(mapping: Any, key: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    return mapping.get(key) if key in mapping else mapping.get(key.encode())


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


def _response_value(response: NativeResponse) -> Any:
    if response.status == _STATUS_OK:
        return response.value

    message = _error_message(response.value)
    if response.status == _STATUS_BUSY:
        raise OverloadedError(
            message,
            raw=response.value,
            retry_after_ms=_optional_int(response.value, "retry_after_ms"),
            reason=_optional_text(response.value, "reason"),
        )
    raise FerricStoreError(message, raw=response.value)


def _batch_item_value(item: Any) -> Any:
    if not isinstance(item, dict):
        raise FerricStoreError("native BATCH item is not a map", raw=item)

    status = _optional_text(item, "status") or "error"
    value = _map_get(item, "value")
    if status == "ok":
        return value
    message = _error_message(value)
    if status == "busy":
        raise OverloadedError(message, raw=item)
    raise FerricStoreError(message, raw=item)


def _normalize_native_url_kwargs(kwargs: dict[str, Any]) -> None:
    if "socket_timeout" in kwargs and "timeout" not in kwargs:
        kwargs["timeout"] = kwargs.pop("socket_timeout")
    for redis_only in (
        "decode_responses",
        "health_check_interval",
        "max_connections",
        "protocol",
        "retry_on_timeout",
    ):
        kwargs.pop(redis_only, None)
