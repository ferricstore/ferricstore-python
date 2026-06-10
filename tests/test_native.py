from __future__ import annotations

import asyncio
import socket
import struct
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, AsyncNativeAdapter, FlowClient, NativeAdapter
from ferricstore.errors import InvalidCommandError
from ferricstore.native import (
    _FLAG_CUSTOM_PAYLOAD,
    AsyncNativeAdapterPool,
    NativeAdapterPool,
    NativeCommand,
    _try_fast_response_value,
    _try_fast_response_value_at,
    decode_value,
    encode_value,
    translate_command,
)
from ferricstore.types import ChildSpec, ClaimedItem, CreateItem


def test_native_value_codec_round_trips_binary_safe_nested_values():
    value = {
        "key": "k1",
        "payload": b"\x00raw",
        "items": [1, True, None, {"nested": b"value"}],
    }

    decoded, rest = decode_value(encode_value(value))

    assert rest == b""
    assert decoded == {
        b"key": b"k1",
        b"payload": b"\x00raw",
        b"items": [1, True, None, {b"nested": b"value"}],
    }


def test_flow_client_from_native_url_uses_native_adapter_pool(monkeypatch):
    created = {}

    class FakeNativeAdapterPool:
        @classmethod
        def from_url(cls, url, **kwargs):
            created["url"] = url
            created["kwargs"] = kwargs
            return cls()

        def execute_command(self, *args):
            return b"OK"

    monkeypatch.setattr("ferricstore.native.NativeAdapterPool", FakeNativeAdapterPool)

    client = FlowClient.from_url("ferric://localhost:6388", timeout=1.0)

    assert client.command("PING") == b"OK"
    assert created == {"url": "ferric://localhost:6388", "kwargs": {"timeout": 1.0}}


def test_native_from_url_ignores_redis_only_kwargs(monkeypatch):
    captured = {}

    def fake_init(self, host, port, **kwargs):
        captured["host"] = host
        captured["port"] = port
        captured["kwargs"] = kwargs

    monkeypatch.setattr(NativeAdapter, "__init__", fake_init)

    NativeAdapter.from_url(
        "ferric://localhost:6388",
        max_connections=8,
        protocol=3,
        decode_responses=False,
        socket_timeout=2.5,
    )

    assert captured == {
        "host": "localhost",
        "port": 6388,
        "kwargs": {
            "timeout": 2.5,
            "username": None,
            "password": None,
            "tls": False,
        },
    }


def test_native_connect_uses_timeout_only_for_connect_not_socket_reads(monkeypatch):
    captured = {}

    class FakeSocket:
        def __init__(self) -> None:
            self.timeouts: list[float | None] = []

        def settimeout(self, value):
            self.timeouts.append(value)

        def setsockopt(self, *args):
            pass

        def shutdown(self, *args):
            pass

        def close(self):
            pass

    fake_socket = FakeSocket()

    def fake_create_connection(address, timeout=None):
        captured["address"] = address
        captured["timeout"] = timeout
        return fake_socket

    monkeypatch.setattr(socket, "create_connection", fake_create_connection)
    monkeypatch.setattr(NativeAdapter, "_reader_loop", lambda self: None)
    monkeypatch.setattr(NativeAdapter, "_request", lambda self, *args, **kwargs: object())
    monkeypatch.setattr(NativeAdapter, "_response_value", lambda self, response: b"OK")

    adapter = NativeAdapter(timeout=7.5)

    assert captured == {"address": ("127.0.0.1", 6388), "timeout": 7.5}
    assert fake_socket.timeouts == [None]

    adapter.close()


def test_native_adapter_pool_uses_max_connections_and_round_robins(monkeypatch):
    created = []

    class FakeNativeAdapter:
        def __init__(self, index: int):
            self.index = index
            self.calls = []

        def execute_command(self, *args):
            self.calls.append(args)
            return self.index

        def execute_batch(self, commands):
            self.calls.append(tuple(commands))
            return [self.index]

        @property
        def events(self):
            return []

        def close(self):
            self.closed = True

    def from_url(url, **kwargs):
        adapter = FakeNativeAdapter(len(created))
        created.append((url, kwargs, adapter))
        return adapter

    monkeypatch.setattr(NativeAdapter, "from_url", staticmethod(from_url))

    pool = NativeAdapterPool.from_url("ferric://localhost:6388", max_connections=3, timeout=1.0)

    assert isinstance(pool, NativeAdapterPool)
    assert [pool.execute_command("PING") for _ in range(4)] == [0, 1, 2, 0]
    assert [call[:2] for call in created] == [
        ("ferric://localhost:6388", {"timeout": 1.0}),
        ("ferric://localhost:6388", {"timeout": 1.0}),
        ("ferric://localhost:6388", {"timeout": 1.0}),
    ]


def test_native_adapter_wait_event_returns_unsolicited_events(monkeypatch):
    monkeypatch.setattr(NativeAdapter, "_connect", lambda self: None)

    adapter = NativeAdapter("127.0.0.1", 6388)

    with adapter._events_cv:
        adapter._events.append({"event": "FLOW_WAKE"})
        adapter._events_cv.notify_all()

    assert adapter.wait_event(timeout=0.01) == {"event": "FLOW_WAKE"}
    assert adapter.wait_event(timeout=0.0) is None


def test_native_subscribe_flow_wake_sends_event_filter(monkeypatch):
    monkeypatch.setattr(NativeAdapter, "_connect", lambda self: None)
    captured = {}

    def fake_request(self, opcode, lane_id, payload, flags=0):
        captured["opcode"] = opcode
        captured["lane_id"] = lane_id
        captured["payload"] = payload
        captured["flags"] = flags
        return type("Response", (), {"status": 0, "value": {"subscribed": ["FLOW_WAKE"]}})()

    monkeypatch.setattr(NativeAdapter, "_request", fake_request)

    adapter = NativeAdapter("127.0.0.1", 6388)
    reply = adapter.subscribe_flow_wake(
        "email",
        state="queued",
        partition_keys=["bucket-0", "bucket-1"],
        priority=0,
        limit=500,
    )

    assert reply == {"subscribed": ["FLOW_WAKE"]}
    assert captured == {
        "opcode": 0x0011,
        "lane_id": 0,
        "payload": {
            "events": ["FLOW_WAKE"],
            "flow_wake": {
                "type": "email",
                "state": "queued",
                "partition_keys": ["bucket-0", "bucket-1"],
                "priority": 0,
                "limit": 500,
            },
        },
        "flags": 0,
    }


def test_ferrics_url_enables_native_tls(monkeypatch):
    captured = {}

    def fake_init(self, host, port, **kwargs):
        captured["host"] = host
        captured["port"] = port
        captured["kwargs"] = kwargs

    monkeypatch.setattr(NativeAdapter, "__init__", fake_init)

    NativeAdapter.from_url("ferrics://default:secret@store.example.com")

    assert captured == {
        "host": "store.example.com",
        "port": 6389,
        "kwargs": {
            "username": "default",
            "password": "secret",
            "tls": True,
        },
    }


def test_native_adapter_uses_real_tcp_frames():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    received: list[tuple[int, int, Any]] = []

    def recv_exact(conn: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = conn.recv(size - len(chunks))
            if not chunk:
                raise RuntimeError("connection closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def recv_frame(conn: socket.socket) -> tuple[int, int, int, Any]:
        raw_header = recv_exact(conn, header.size)
        magic, version, _flags, lane_id, opcode, request_id, body_len = header.unpack(raw_header)
        assert magic == b"FSNP"
        assert version == 0x01
        value, rest = decode_value(recv_exact(conn, body_len))
        assert rest == b""
        return opcode, lane_id, request_id, value

    def response(opcode: int, lane_id: int, request_id: int, value: Any) -> bytes:
        body = status.pack(0) + encode_value(value)
        return header.pack(b"FSNP", 0x81, 0, lane_id, opcode, request_id, len(body)) + body

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def serve() -> None:
        with listener:
            conn, _addr = listener.accept()
            with conn:
                startup_opcode, startup_lane, startup_id, startup_payload = recv_frame(conn)
                received.append((startup_opcode, startup_lane, startup_payload))
                conn.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))

                get_opcode, get_lane, get_id, get_payload = recv_frame(conn)
                received.append((get_opcode, get_lane, get_payload))
                conn.sendall(response(get_opcode, get_lane, get_id, b"value"))

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    adapter = NativeAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
    try:
        assert adapter.execute_command("GET", "k") == b"value"
    finally:
        adapter.close()
        thread.join(timeout=1.0)

    assert received[0][0] == 0x000C
    assert received[0][1] == 0
    assert received[0][2][b"compact_flow_responses"] is True
    assert received[1][0] == 0x0101
    assert received[1][1] == 1


def test_native_fast_decodes_custom_flow_claim_jobs():
    def bin_field(value: bytes | None) -> bytes:
        if value is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(value)) + value

    body = (
        b"\x80"
        + struct.pack(">I", 2)
        + bin_field(b"f1")
        + bin_field(b"p1")
        + bin_field(b"lease1")
        + struct.pack(">q", 7)
        + bin_field(b"f2")
        + bin_field(None)
        + bin_field(b"lease2")
        + struct.pack(">q", 8)
    )

    assert _try_fast_response_value(0x0203, body) == [
        [b"f1", b"p1", b"lease1", 7],
        [b"f2", None, b"lease2", 8],
    ]


def test_native_fast_decodes_custom_ok_list():
    assert _try_fast_response_value(0x0210, b"\x81" + struct.pack(">I", 3)) == [
        b"OK",
        b"OK",
        b"OK",
    ]


def test_native_fast_decodes_custom_ok_list_from_frame_body_offset():
    body = struct.pack(">H", 0) + b"\x81" + struct.pack(">I", 3)

    assert _try_fast_response_value_at(0x0210, body, 2) == [
        b"OK",
        b"OK",
        b"OK",
    ]


def test_native_fast_decodes_custom_claim_jobs_from_frame_body_offset():
    def bin_field(value: bytes | None) -> bytes:
        if value is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(value)) + value

    body = (
        struct.pack(">H", 0)
        + b"\x80"
        + struct.pack(">I", 1)
        + bin_field(b"f1")
        + bin_field(None)
        + bin_field(b"lease1")
        + struct.pack(">q", 7)
    )

    assert _try_fast_response_value_at(0x0203, body, 2) == [[b"f1", None, b"lease1", 7]]


def test_async_flow_client_from_native_url_uses_async_native_adapter_pool(monkeypatch):
    created = {}

    class FakeAsyncNativeAdapterPool:
        @classmethod
        def from_url(cls, url, **kwargs):
            created["url"] = url
            created["kwargs"] = kwargs
            return cls()

        async def execute_command(self, *args):
            return b"OK"

    monkeypatch.setattr("ferricstore.native.AsyncNativeAdapterPool", FakeAsyncNativeAdapterPool)

    async def run():
        client = AsyncFlowClient.from_url("ferric://localhost:6388", max_connections=4)
        assert await client.command("PING") == b"OK"

    asyncio.run(run())

    assert created == {
        "url": "ferric://localhost:6388",
        "kwargs": {"max_connections": 4},
    }


def test_async_native_pipeline_uses_batch():
    class FakeAsyncNativeAdapter(AsyncNativeAdapter):
        def __init__(self):
            pass

        async def execute_batch(self, commands):
            return [command[0] for command in commands]

    async def run():
        pipe = FakeAsyncNativeAdapter().pipeline()
        pipe.execute_command("SET", "k", "v")
        pipe.execute_command("GET", "k")
        assert await pipe.execute() == ["SET", "GET"]

    asyncio.run(run())


def test_async_native_adapter_pool_uses_max_connections_and_round_robins(monkeypatch):
    created = []

    class FakeAsyncNativeAdapter:
        def __init__(self, index: int):
            self.index = index
            self.calls = []

        async def execute_command(self, *args):
            self.calls.append(args)
            return self.index

        async def execute_batch(self, commands):
            self.calls.append(tuple(commands))
            return [self.index]

        @property
        def events(self):
            return []

        async def close(self):
            self.closed = True

    def from_url(url, **kwargs):
        adapter = FakeAsyncNativeAdapter(len(created))
        created.append((url, kwargs, adapter))
        return adapter

    monkeypatch.setattr(AsyncNativeAdapter, "from_url", staticmethod(from_url))

    async def run():
        pool = AsyncNativeAdapterPool.from_url(
            "ferric://localhost:6388", max_connections=2, timeout=1.0
        )

        assert isinstance(pool, AsyncNativeAdapterPool)
        assert [await pool.execute_command("PING") for _ in range(3)] == [0, 1, 0]

    asyncio.run(run())

    assert [call[:2] for call in created] == [
        ("ferric://localhost:6388", {"timeout": 1.0}),
        ("ferric://localhost:6388", {"timeout": 1.0}),
    ]


def test_native_adapter_allows_multiple_inflight_requests():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    received: list[tuple[int, int]] = []

    def recv_exact(conn: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = conn.recv(size - len(chunks))
            if not chunk:
                raise RuntimeError("connection closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def recv_frame(conn: socket.socket) -> tuple[int, int, int, Any]:
        raw_header = recv_exact(conn, header.size)
        magic, version, _flags, lane_id, opcode, request_id, body_len = header.unpack(raw_header)
        assert magic == b"FSNP"
        assert version == 0x01
        value, rest = decode_value(recv_exact(conn, body_len))
        assert rest == b""
        return opcode, lane_id, request_id, value

    def response(opcode: int, lane_id: int, request_id: int, value: Any) -> bytes:
        body = status.pack(0) + encode_value(value)
        return header.pack(b"FSNP", 0x81, 0, lane_id, opcode, request_id, len(body)) + body

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def serve() -> None:
        with listener:
            conn, _addr = listener.accept()
            with conn:
                startup_opcode, startup_lane, startup_id, _startup_payload = recv_frame(conn)
                conn.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))

                first_opcode, first_lane, first_id, _first_payload = recv_frame(conn)
                second_opcode, second_lane, second_id, _second_payload = recv_frame(conn)
                received.extend([(first_opcode, first_lane), (second_opcode, second_lane)])

                conn.sendall(response(first_opcode, first_lane, first_id, b"one"))
                conn.sendall(response(second_opcode, second_lane, second_id, b"two"))

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    adapter = NativeAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(adapter.execute_command, "GET", "a")
            second = pool.submit(adapter.execute_command, "GET", "b")
            assert [first.result(timeout=1.0), second.result(timeout=1.0)] == [b"one", b"two"]
    finally:
        adapter.close()
        thread.join(timeout=1.0)

    assert received == [(0x0101, 1), (0x0101, 2)]


def test_native_adapter_submit_command_returns_future_without_waiting():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    request_received = threading.Event()
    release_response = threading.Event()

    def recv_exact(conn: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = conn.recv(size - len(chunks))
            if not chunk:
                raise RuntimeError("connection closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def recv_frame(conn: socket.socket) -> tuple[int, int, int, Any]:
        raw_header = recv_exact(conn, header.size)
        magic, version, _flags, lane_id, opcode, request_id, body_len = header.unpack(raw_header)
        assert magic == b"FSNP"
        assert version == 0x01
        value, rest = decode_value(recv_exact(conn, body_len))
        assert rest == b""
        return opcode, lane_id, request_id, value

    def response(opcode: int, lane_id: int, request_id: int, value: Any) -> bytes:
        body = status.pack(0) + encode_value(value)
        return header.pack(b"FSNP", 0x81, 0, lane_id, opcode, request_id, len(body)) + body

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def serve() -> None:
        with listener:
            conn, _addr = listener.accept()
            with conn:
                startup_opcode, startup_lane, startup_id, _startup_payload = recv_frame(conn)
                conn.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))

                opcode, lane, request_id, _payload = recv_frame(conn)
                request_received.set()
                assert release_response.wait(timeout=1.0)
                conn.sendall(response(opcode, lane, request_id, b"value"))

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    adapter = NativeAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
    try:
        future = adapter.submit_command("GET", "a")
        assert request_received.wait(timeout=1.0)
        assert future.done() is False
        release_response.set()
        assert future.result(timeout=1.0) == b"value"
    finally:
        adapter.close()
        thread.join(timeout=1.0)


def test_async_native_adapter_allows_multiple_inflight_requests():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    received: list[tuple[int, int]] = []

    async def recv_frame(
        reader: asyncio.StreamReader,
    ) -> tuple[int, int, int, Any]:
        raw_header = await reader.readexactly(header.size)
        magic, version, _flags, lane_id, opcode, request_id, body_len = header.unpack(raw_header)
        assert magic == b"FSNP"
        assert version == 0x01
        value, rest = decode_value(await reader.readexactly(body_len))
        assert rest == b""
        return opcode, lane_id, request_id, value

    def response(opcode: int, lane_id: int, request_id: int, value: Any) -> bytes:
        body = status.pack(0) + encode_value(value)
        return header.pack(b"FSNP", 0x81, 0, lane_id, opcode, request_id, len(body)) + body

    async def run() -> None:
        async def handle(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            startup_opcode, startup_lane, startup_id, _startup_payload = await recv_frame(reader)
            writer.write(response(startup_opcode, startup_lane, startup_id, {"ok": True}))
            await writer.drain()

            first_opcode, first_lane, first_id, _first_payload = await recv_frame(reader)
            second_opcode, second_lane, second_id, _second_payload = await recv_frame(reader)
            received.extend([(first_opcode, first_lane), (second_opcode, second_lane)])

            writer.write(response(first_opcode, first_lane, first_id, b"one"))
            writer.write(response(second_opcode, second_lane, second_id, b"two"))
            await writer.drain()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        adapter = AsyncNativeAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
        try:
            result = await asyncio.wait_for(
                asyncio.gather(
                    adapter.execute_command("GET", "a"),
                    adapter.execute_command("GET", "b"),
                ),
                timeout=1.0,
            )
            assert result == [b"one", b"two"]
        finally:
            await adapter.close()
            server.close()
            await server.wait_closed()

    asyncio.run(run())

    assert received == [(0x0101, 1), (0x0101, 2)]


def test_async_native_send_drains_only_after_threshold():
    class FakeWriter:
        def __init__(self) -> None:
            self.parts: list[bytes] = []
            self.drains = 0

        def writelines(self, parts) -> None:
            self.parts.extend(parts)

        async def drain(self) -> None:
            self.drains += 1

    async def run() -> None:
        writer = FakeWriter()
        adapter = object.__new__(AsyncNativeAdapter)
        adapter._writer = writer
        adapter.compression = "none"
        adapter._queued_write_bytes = 0
        adapter.write_drain_bytes = 10_000

        await adapter._send(0x0101, 1, 1, {"key": "a"})
        assert writer.drains == 0

        adapter.write_drain_bytes = 1
        await adapter._send(0x0101, 1, 2, {"key": "b"})
        assert writer.drains == 1
        assert adapter._queued_write_bytes == 0
        assert len(writer.parts) == 4

    asyncio.run(run())


def test_native_translates_simple_flow_create_many_to_compact_request():
    command = translate_command(
        "FLOW.CREATE_MANY",
        "AUTO",
        "TYPE",
        "email",
        "STATE",
        "queued",
        "NOW",
        123,
        "RUN_AT",
        123,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
        "flow-1",
        b"payload",
    )

    assert command.opcode == 0x020F
    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)
    assert command.payload[0] == 0x90


def test_native_translates_simple_claim_due_to_compact_request():
    command = translate_command(
        "FLOW.CLAIM_DUE",
        "email",
        "STATE",
        "queued",
        "WORKER",
        "worker-1",
        "LEASE_MS",
        30_000,
        "LIMIT",
        500,
        "RETURN",
        "JOBS_COMPACT",
        "PARTITIONS",
        2,
        "p1",
        "p2",
        "BLOCK",
        -1,
        "RECLAIM_EXPIRED",
        "false",
        "RECLAIM_RATIO",
        25,
    )

    assert command.opcode == 0x0203
    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)
    assert command.payload[0] == 0x91


def test_native_translates_simple_complete_many_to_compact_request():
    command = translate_command(
        "FLOW.COMPLETE_MANY",
        "MIXED",
        "NOW",
        123,
        "INDEPENDENT",
        "true",
        "ITEMS",
        "flow-1",
        "p1",
        "lease-1",
        7,
    )

    assert command.opcode == 0x0210
    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)
    assert command.payload[0] == 0x92


def test_native_keeps_complex_complete_many_on_generic_request_codec():
    command = translate_command(
        "FLOW.COMPLETE_MANY",
        "MIXED",
        "NOW",
        123,
        "RESULT",
        b"result",
        "ITEMS",
        "flow-1",
        "p1",
        "lease-1",
        7,
    )

    assert command.opcode == 0x0210
    assert command.flags == 0
    assert isinstance(command.payload, dict)


def test_translates_set_get_and_rejects_unknown_commands():
    set_cmd = translate_command("SET", "k", b"v", "PX", 10, "NX")
    get_cmd = translate_command("GET", "k")

    assert set_cmd == NativeCommand(
        opcode=0x0102,
        payload={"key": "k", "value": b"v", "ttl": 10, "nx": True},
        lane_id=1,
    )
    assert get_cmd == NativeCommand(opcode=0x0101, payload={"key": "k"}, lane_id=1)

    with pytest.raises(InvalidCommandError):
        translate_command("HSET", "h", "f", "v")


def test_translates_native_custom_commands():
    assert translate_command("CAS", "k", b"old", b"new", "PX", 100) == NativeCommand(
        opcode=0x0106,
        payload={"key": "k", "expected": b"old", "value": b"new", "ttl": 100},
        lane_id=1,
    )
    assert translate_command("LOCK", "lock:k", "owner", 500) == NativeCommand(
        opcode=0x0107,
        payload={"key": "lock:k", "owner": "owner", "ttl_ms": 500},
        lane_id=1,
    )
    assert translate_command("UNLOCK", "lock:k", "owner") == NativeCommand(
        opcode=0x0108,
        payload={"key": "lock:k", "owner": "owner"},
        lane_id=1,
    )
    assert translate_command("EXTEND", "lock:k", "owner", 750) == NativeCommand(
        opcode=0x0109,
        payload={"key": "lock:k", "owner": "owner", "ttl_ms": 750},
        lane_id=1,
    )
    assert translate_command("RATELIMIT.ADD", "rl:k", 1000, 10, 2) == NativeCommand(
        opcode=0x010A,
        payload={"key": "rl:k", "window_ms": 1000, "max": 10, "count": 2},
        lane_id=1,
    )
    assert translate_command("FETCH_OR_COMPUTE", "cache:k", 1000, "h") == NativeCommand(
        opcode=0x010B,
        payload={"key": "cache:k", "ttl_ms": 1000, "hint": "h"},
        lane_id=1,
    )
    assert translate_command("FETCH_OR_COMPUTE_RESULT", "cache:k", b"v", 1000) == NativeCommand(
        opcode=0x010C,
        payload={"key": "cache:k", "value": b"v", "ttl_ms": 1000},
        lane_id=1,
    )
    assert translate_command("FETCH_OR_COMPUTE_ERROR", "cache:k", "failed") == NativeCommand(
        opcode=0x010D,
        payload={"key": "cache:k", "message": "failed"},
        lane_id=1,
    )


def test_translates_native_admin_commands():
    assert translate_command("CLUSTER.KEYSLOT", "k") == NativeCommand(
        opcode=0x0303,
        payload={"key": "k", "args": ["k"]},
        lane_id=1,
    )
    assert translate_command("CLUSTER.JOIN", "node@host", "REPLACE") == NativeCommand(
        opcode=0x0306,
        payload={"args": ["node@host", "REPLACE"]},
        lane_id=1,
    )
    assert translate_command("CLUSTER.FAILOVER", 1, "node@host") == NativeCommand(
        opcode=0x0308,
        payload={"args": [1, "node@host"]},
        lane_id=1,
    )
    assert translate_command("FERRICSTORE.KEY_INFO", "k") == NativeCommand(
        opcode=0x030C,
        payload={"key": "k", "args": ["k"]},
        lane_id=1,
    )
    assert translate_command("FERRICSTORE.CONFIG", "GET", "prefix") == NativeCommand(
        opcode=0x030D,
        payload={"args": ["GET", "prefix"]},
        lane_id=1,
    )
    assert translate_command("FERRICSTORE.METRICS") == NativeCommand(
        opcode=0x030F,
        payload={"args": []},
        lane_id=1,
    )


def test_translates_flow_value_and_retention_options():
    create = translate_command(
        "FLOW.CREATE",
        "f1",
        "TYPE",
        "email",
        "RETENTION_TTL_MS",
        5000,
    )
    assert create.payload["retention_ttl_ms"] == 5000
    assert "terminal_ttl_ms" not in create.payload

    value_put = translate_command(
        "FLOW.VALUE.PUT",
        b"value",
        "OWNER_FLOW_ID",
        "f1",
        "NAME",
        "reservation",
        "OVERRIDE",
        "true",
    )
    assert value_put == NativeCommand(
        opcode=0x020B,
        payload={
            "value": b"value",
            "owner_flow_id": "f1",
            "name": "reservation",
            "override": True,
        },
        lane_id=1,
    )

    value_mget = translate_command("FLOW.VALUE.MGET", "ref-a", "ref-b", "MAX_BYTES", 10)
    assert value_mget == NativeCommand(
        opcode=0x020C,
        payload={"refs": ["ref-a", "ref-b"], "max_bytes": 10},
        lane_id=1,
    )


def test_translates_flow_create_many_auto_and_complete_many_mixed():
    create = translate_command(
        "FLOW.CREATE_MANY",
        "AUTO",
        "TYPE",
        "email",
        "STATE",
        "queued",
        "NOW",
        100,
        "RUN_AT",
        100,
        "INDEPENDENT",
        "true",
        "ITEMS",
        "f1",
        b"p1",
        "f2",
        b"p2",
    )

    assert create.opcode == 0x020F
    assert create.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(create.payload, bytes)
    assert create.payload[0] == 0x90

    complete = translate_command(
        "FLOW.COMPLETE_MANY",
        "MIXED",
        "NOW",
        200,
        "INDEPENDENT",
        "true",
        "ITEMS",
        "f1",
        "p1",
        b"lease1",
        7,
        "f2",
        "p2",
        b"lease2",
        8,
    )

    assert complete.opcode == 0x0210
    assert complete.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(complete.payload, bytes)
    assert complete.payload[0] == 0x92


def test_flow_client_create_many_can_request_ok_on_success_return():
    class CaptureExecutor:
        def __init__(self):
            self.calls = []

        def execute_command(self, *args):
            self.calls.append(args)
            return b"OK"

    executor = CaptureExecutor()
    FlowClient(executor).create_many(
        None,
        [CreateItem("f1", b"p1")],
        type="email",
        independent=True,
        return_ok_on_success=True,
    )

    assert "RETURN" in executor.calls[0]
    assert "OK_ON_SUCCESS" in executor.calls[0]


def test_translates_flow_claim_jobs_and_high_level_many_calls():
    claim = translate_command(
        "FLOW.CLAIM_DUE",
        "email",
        "STATE",
        "queued",
        "WORKER",
        "w1",
        "LEASE_MS",
        30_000,
        "LIMIT",
        100,
        "RETURN",
        "JOBS_COMPACT",
    )

    assert claim.opcode == 0x0203
    assert claim.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(claim.payload, bytes)
    assert claim.payload[0] == 0x91

    class CaptureExecutor:
        def __init__(self):
            self.calls = []

        def execute_command(self, *args):
            self.calls.append(args)
            return [b"OK", b"OK"]

    executor = CaptureExecutor()
    client = FlowClient(executor)
    client.create_many(
        None,
        [CreateItem("f1", b"p1"), CreateItem("f2", b"p2")],
        type="email",
        now_ms=100,
        independent=True,
    )
    client.complete_many(
        None,
        [
            ClaimedItem("f1", b"lease1", 1, partition_key="p1"),
            ClaimedItem("f2", b"lease2", 2, partition_key="p2"),
        ],
        now_ms=200,
        independent=True,
    )

    assert translate_command(*executor.calls[0]).opcode == 0x020F
    assert translate_command(*executor.calls[1]).opcode == 0x0210


def test_translates_spawn_children_and_retention_cleanup():
    spawn = translate_command(
        "FLOW.SPAWN_CHILDREN",
        "parent-1",
        "GROUP",
        "fanout",
        "PARTITION",
        "tenant-a",
        "FENCING",
        7,
        "WAIT",
        "all",
        "SUCCESS",
        "done",
        "FAILURE",
        "failed",
        "ITEMS",
        "child-1",
        "email",
        b"payload-1",
        "child-2",
        "email",
        b"payload-2",
    )

    assert spawn == NativeCommand(
        opcode=0x0220,
        payload={
            "id": "parent-1",
            "group_id": "fanout",
            "partition_key": "tenant-a",
            "fencing_token": 7,
            "wait": "all",
            "success": "done",
            "failure": "failed",
            "children": [
                {"id": "child-1", "type": "email", "payload": b"payload-1"},
                {"id": "child-2", "type": "email", "payload": b"payload-2"},
            ],
        },
        lane_id=1,
    )

    cleanup = translate_command("FLOW.RETENTION_CLEANUP", "LIMIT", 100, "NOW", 1234)
    assert cleanup == NativeCommand(
        opcode=0x0221,
        payload={"limit": 100, "now_ms": 1234},
        lane_id=1,
    )


def test_high_level_spawn_children_uses_native_translation():
    class CaptureExecutor:
        def __init__(self):
            self.calls = []

        def execute_command(self, *args):
            self.calls.append(args)
            return b"OK"

    executor = CaptureExecutor()
    client = FlowClient(executor)
    client.spawn_children(
        "parent-1",
        [ChildSpec("child-1", "email", b"payload")],
        partition_key="tenant-a",
        fencing_token=7,
        success="done",
        failure="failed",
        now_ms=100,
    )

    command = translate_command(*executor.calls[0])
    assert command.opcode == 0x0220
    assert command.payload["children"] == [
        {"id": "child-1", "type": "email", "payload": b"payload"}
    ]


def test_native_pipeline_uses_batch_and_returns_item_values():
    class FakeNativeAdapter(NativeAdapter):
        def __init__(self):
            pass

        def execute_batch(self, commands):
            return [command[0] for command in commands]

    pipe = FakeNativeAdapter().pipeline()
    pipe.execute_command("SET", "k", "v")
    pipe.execute_command("GET", "k")

    assert pipe.execute() == ["SET", "GET"]
