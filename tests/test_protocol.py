from __future__ import annotations

import asyncio
import socket
import struct
import threading
import zlib
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

import pytest

import ferricstore.protocol as protocol_module
from ferricstore import AsyncFlowClient, AsyncProtocolAdapter, FlowClient, ProtocolAdapter
from ferricstore.errors import FerricStoreError, InvalidCommandError
from ferricstore.protocol import (
    _COMPACT_BINARY_LIST_LIST,
    _COMPACT_FLOW_LIST_REQUEST,
    _COMPACT_FLOW_RECORD,
    _COMPACT_FLOW_RECORD_LIST,
    _COMPACT_INTEGER_LIST,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_PIPELINE_RESPONSE,
    _FLAG_CUSTOM_PAYLOAD,
    AsyncProtocolAdapterPool,
    ProtocolAdapterPool,
    ProtocolCommand,
    ProtocolResponse,
    _compact_flow_many_payloads_from_raw,
    _compact_pipeline_payload_from_raw,
    _try_fast_response_value,
    _try_fast_response_value_at,
    build_protocol_command,
    decode_value,
    encode_value,
)
from ferricstore.types import ChildSpec, ClaimedFlow, CreateItem


def test_protocol_value_codec_round_trips_binary_safe_nested_values():
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


def test_flow_client_from_protocol_url_uses_protocol_adapter_pool(monkeypatch):
    created = {}

    class FakeProtocolAdapterPool:
        @classmethod
        def from_url(cls, url, **kwargs):
            created["url"] = url
            created["kwargs"] = kwargs
            return cls()

        def execute_command(self, *args):
            return b"OK"

    monkeypatch.setattr("ferricstore.protocol.ProtocolAdapterPool", FakeProtocolAdapterPool)

    client = FlowClient.from_url("ferric://localhost:6388", timeout=1.0)

    assert client.command("PING") == b"OK"
    assert created == {"url": "ferric://localhost:6388", "kwargs": {"timeout": 1.0}}


def test_protocol_from_url_ignores_redis_only_kwargs(monkeypatch):
    captured = {}

    def fake_init(self, host, port, **kwargs):
        captured["host"] = host
        captured["port"] = port
        captured["kwargs"] = kwargs

    monkeypatch.setattr(ProtocolAdapter, "__init__", fake_init)

    ProtocolAdapter.from_url(
        "ferric://localhost:6388",
        max_connections=8,
        protocol=3,
        decode_responses=False,
        socket_timeout=2.5,
        health_check_interval=12.0,
    )

    assert captured == {
        "host": "localhost",
        "port": 6388,
        "kwargs": {
            "timeout": 2.5,
            "heartbeat_interval": 12.0,
            "username": None,
            "password": None,
            "tls": False,
        },
    }


def test_protocol_connect_uses_timeout_only_for_connect_not_socket_reads(monkeypatch):
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
    monkeypatch.setattr(ProtocolAdapter, "_reader_loop", lambda self: None)
    monkeypatch.setattr(ProtocolAdapter, "_request", lambda self, *args, **kwargs: object())
    monkeypatch.setattr(ProtocolAdapter, "_response_value", lambda self, response: b"OK")

    adapter = ProtocolAdapter(timeout=7.5)

    assert captured == {"address": ("127.0.0.1", 6388), "timeout": 7.5}
    assert fake_socket.timeouts == [None]

    adapter.close()


def test_protocol_adapter_defaults_to_latency_first_lanes(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    adapter = ProtocolAdapter("127.0.0.1", 6388, heartbeat_interval=None)

    assert adapter.lanes == 8

    adapter.close()


def test_async_protocol_adapter_defaults_to_latency_first_lanes():
    adapter = AsyncProtocolAdapter("127.0.0.1", 6388, heartbeat_interval=None)

    assert adapter.lanes == 8


def test_protocol_recv_response_rejects_body_before_large_allocation(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    header = struct.Struct(">4sBBIHQI")

    class FakeSocket:
        def __init__(self, data: bytes):
            self.data = bytearray(data)

        def recv(self, size: int) -> bytes:
            chunk = bytes(self.data[:size])
            del self.data[:size]
            return chunk

        def shutdown(self, *args):
            pass

        def close(self):
            pass

    frame = header.pack(b"FSNP", 0x81, 0, 1, 0x0101, 1, 11)
    adapter = ProtocolAdapter(max_response_bytes=10, heartbeat_interval=None)
    adapter._sock = FakeSocket(frame)

    with pytest.raises(FerricStoreError, match="max_response_bytes"):
        adapter._recv_response()

    adapter.close()


def test_protocol_recv_response_rejects_decompressed_body_over_limit(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")

    class FakeSocket:
        def __init__(self, data: bytes):
            self.data = bytearray(data)

        def recv(self, size: int) -> bytes:
            chunk = bytes(self.data[:size])
            del self.data[:size]
            return chunk

        def shutdown(self, *args):
            pass

        def close(self):
            pass

    body = zlib.compress(status.pack(0) + encode_value(b"x" * 32))
    frame = header.pack(b"FSNP", 0x81, 0x08, 1, 0x0101, 1, len(body)) + body
    adapter = ProtocolAdapter(
        max_response_bytes=1024,
        max_decompressed_response_bytes=16,
        heartbeat_interval=None,
    )
    adapter._sock = FakeSocket(frame)

    with pytest.raises(FerricStoreError, match="max_decompressed_response_bytes"):
        adapter._recv_response()

    adapter.close()


def test_protocol_adapter_retries_transient_startup_reset():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    received: list[tuple[int, int, Any]] = []
    attempts: list[str] = []

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
    listener.listen(2)
    port = listener.getsockname()[1]

    def serve() -> None:
        with listener:
            conn, _addr = listener.accept()
            attempts.append("drop")
            conn.close()

            conn, _addr = listener.accept()
            attempts.append("startup")
            with conn:
                startup_opcode, startup_lane, startup_id, startup_payload = recv_frame(conn)
                received.append((startup_opcode, startup_lane, startup_payload))
                conn.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    adapter = ProtocolAdapter(
        "127.0.0.1",
        port,
        client_name="pytest",
        timeout=1.0,
        heartbeat_interval=None,
    )
    try:
        thread.join(timeout=1.0)
    finally:
        adapter.close()

    assert attempts == ["drop", "startup"]
    assert received[0][0] == 0x000C
    assert received[0][1] == 0


def test_protocol_adapter_pool_uses_max_connections_and_round_robins(monkeypatch):
    created = []

    class FakeProtocolAdapter:
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
        adapter = FakeProtocolAdapter(len(created))
        created.append((url, kwargs, adapter))
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", staticmethod(from_url))

    pool = ProtocolAdapterPool.from_url("ferric://localhost:6388", max_connections=3, timeout=1.0)

    assert isinstance(pool, ProtocolAdapterPool)
    assert [pool.execute_command("PING") for _ in range(4)] == [0, 1, 2, 0]
    assert [call[:2] for call in created] == [
        ("ferric://localhost:6388", {"timeout": 1.0}),
        ("ferric://localhost:6388", {"timeout": 1.0}),
        ("ferric://localhost:6388", {"timeout": 1.0}),
    ]


def test_protocol_submit_commands_writes_pipeline_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388)
    adapter._sock = FakeSocket()

    futures = adapter.submit_commands([("SET", "k", "v"), ("GET", "k")])

    assert len(futures) == 2
    assert len(adapter._sock.sent) == 1
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x000E]
    assert len(adapter._pending) == 1


def test_protocol_submit_commands_coalesces_homogeneous_set_pipeline(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    futures = adapter.submit_commands([("SET", "k1", "v1"), ("SET", "k2", "v2")])

    assert len(futures) == 2
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x000E]


def test_protocol_submit_commands_coalesces_homogeneous_get_pipeline(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    futures = adapter.submit_commands([("GET", "k1"), ("GET", "k2")])

    assert len(futures) == 2
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x000E]


def test_protocol_submit_mget_sends_direct_compact_bulk_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    future = adapter.submit_mget(["k1", b"k2"])

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x0104]
    assert adapter._sock.sent[0][24] == _COMPACT_PIPELINE_REQUEST
    assert adapter._sock.sent[0][25] == 2


def test_protocol_submit_mset_same_value_sends_direct_compact_bulk_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    future = adapter.submit_mset_same_value(["k1", b"k2"], b"value")

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x0105]
    assert adapter._sock.sent[0][24] == _COMPACT_PIPELINE_REQUEST
    assert adapter._sock.sent[0][25] == 1


def test_protocol_submit_mget_payload_sends_preencoded_direct_compact_bulk_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()
    payload = (
        bytes([_COMPACT_PIPELINE_REQUEST, 2]) + struct.pack(">I", 1) + struct.pack(">I", 1) + b"k"
    )

    future = adapter.submit_mget_payload(payload)

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x0104]
    assert adapter._sock.sent[0][24:] == payload


def test_protocol_submit_mset_payload_sends_preencoded_direct_compact_bulk_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()
    payload = (
        bytes([_COMPACT_PIPELINE_REQUEST, 1])
        + struct.pack(">I", 1)
        + struct.pack(">I", 1)
        + b"k"
        + struct.pack(">I", 5)
        + b"value"
    )

    future = adapter.submit_mset_payload(payload)

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x0105]
    assert adapter._sock.sent[0][24:] == payload


def test_protocol_submit_pipeline_payload_sends_preencoded_compact_pipeline_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()
    payload = bytes([_COMPACT_PIPELINE_REQUEST, 0x80 | 22]) + struct.pack(">I", 1)

    future = adapter.submit_pipeline_payload(payload, 1)

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x000E]
    assert adapter._sock.sent[0][24:] == payload


def test_protocol_submit_flow_many_payload_sends_preencoded_custom_flow_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()
    payload = b"\x41" + b"compact-flow-create-many"

    future = adapter.submit_flow_many_payload("FLOW.CREATE_MANY", payload, 3)

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x020F]
    assert adapter._sock.sent[0][24:] == payload


def test_protocol_submit_flow_many_payload_expands_scalar_ok_response(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def sendall(self, _data):
            pass

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    future = adapter.submit_flow_many_payload("FLOW.CREATE_MANY", b"\x90payload", 3)
    [response_future] = list(adapter._pending.values())

    response_future.set_result(
        ProtocolResponse(
            lane_id=1,
            opcode=0x020F,
            request_id=1,
            flags=0,
            status=0,
            value="OK",
        )
    )

    assert future.result(timeout=1) == ["OK", "OK", "OK"]


def test_protocol_submit_flow_transition_many_payload_sends_preencoded_custom_flow_frame(
    monkeypatch,
):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()
    payload = b"\x9c" + b"compact-flow-transition-many"

    future = adapter.submit_flow_many_payload("FLOW.TRANSITION_MANY", payload, 3)

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x0211]
    assert adapter._sock.sent[0][24:] == payload


def test_protocol_submit_flow_value_mget_payload_sends_preencoded_custom_flow_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()
    payload = b"\x9d" + struct.pack(">qI", -(1 << 63), 1) + struct.pack(">I", 5) + b"ref-1"

    future = adapter.submit_flow_value_mget_payload(payload)

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x020C]
    assert adapter._sock.sent[0][24:] == payload


def test_compact_kv_payloads_keep_exact_wire_shape():
    def binary(value: bytes) -> bytes:
        return struct.pack(">I", len(value)) + value

    mget = build_protocol_command("MGET", b"k1", "k2")
    assert mget.flags == _FLAG_CUSTOM_PAYLOAD
    assert mget.payload == (
        bytes([_COMPACT_PIPELINE_REQUEST, 2]) + struct.pack(">I", 2) + binary(b"k1") + binary(b"k2")
    )

    mset = build_protocol_command("MSET", b"k1", b"v1", "k2", "v2")
    assert mset.flags == _FLAG_CUSTOM_PAYLOAD
    assert mset.payload == (
        bytes([_COMPACT_PIPELINE_REQUEST, 1])
        + struct.pack(">I", 2)
        + binary(b"k1")
        + binary(b"v1")
        + binary(b"k2")
        + binary(b"v2")
    )

    assert _compact_pipeline_payload_from_raw(
        [("GET", b"k1"), ("GET", "k2")], values_only=True
    ) == (
        bytes([_COMPACT_PIPELINE_REQUEST, 0x82])
        + struct.pack(">I", 2)
        + binary(b"k1")
        + binary(b"k2")
    )

    assert _compact_pipeline_payload_from_raw(
        [("SET", b"k1", b"v1"), ("SET", "k2", "v2")], values_only=True
    ) == (
        bytes([_COMPACT_PIPELINE_REQUEST, 0x81])
        + struct.pack(">I", 2)
        + binary(b"k1")
        + binary(b"v1")
        + binary(b"k2")
        + binary(b"v2")
    )


def test_compact_data_structure_write_pipeline_modes_keep_exact_wire_shape():
    def binary(value: bytes) -> bytes:
        return struct.pack(">I", len(value)) + value

    assert _compact_pipeline_payload_from_raw(
        [("HSET", b"h1", b"f", b"v1"), ("HSET", "h2", "f", "v2")],
        values_only=True,
    ) == (
        bytes([_COMPACT_PIPELINE_REQUEST, 0x96])
        + struct.pack(">I", 2)
        + binary(b"h1")
        + binary(b"f")
        + binary(b"v1")
        + binary(b"h2")
        + binary(b"f")
        + binary(b"v2")
    )

    assert _compact_pipeline_payload_from_raw(
        [("LPUSH", b"l1", b"v1"), ("LPUSH", "l2", "v2")],
        values_only=True,
    ) == (
        bytes([_COMPACT_PIPELINE_REQUEST, 0x97])
        + struct.pack(">I", 2)
        + binary(b"l1")
        + binary(b"v1")
        + binary(b"l2")
        + binary(b"v2")
    )

    assert _compact_pipeline_payload_from_raw(
        [("RPUSH", b"l1", b"v1"), ("RPUSH", "l2", "v2")],
        values_only=True,
    ) == (
        bytes([_COMPACT_PIPELINE_REQUEST, 0x98])
        + struct.pack(">I", 2)
        + binary(b"l1")
        + binary(b"v1")
        + binary(b"l2")
        + binary(b"v2")
    )

    assert _compact_pipeline_payload_from_raw(
        [("SADD", b"s1", b"m1"), ("SADD", "s2", "m2")],
        values_only=True,
    ) == (
        bytes([_COMPACT_PIPELINE_REQUEST, 0x99])
        + struct.pack(">I", 2)
        + binary(b"s1")
        + binary(b"m1")
        + binary(b"s2")
        + binary(b"m2")
    )

    assert _compact_pipeline_payload_from_raw(
        [("ZADD", b"z1", 1.5, b"m1"), ("ZADD", "z2", "2.25", "m2")],
        values_only=True,
    ) == (
        bytes([_COMPACT_PIPELINE_REQUEST, 0x9A])
        + struct.pack(">I", 2)
        + binary(b"z1")
        + struct.pack(">d", 1.5)
        + binary(b"m1")
        + binary(b"z2")
        + struct.pack(">d", 2.25)
        + binary(b"m2")
    )


def test_protocol_submit_batch_writes_one_pipeline_frame(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    future = adapter.submit_batch([("GET", "k1"), ("GET", "k2")])

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x000E]
    assert len(adapter._pending) == 1
    assert adapter._sock.sent[0][24 + 1] == 0x82


def test_protocol_submit_batch_kv_pipeline_does_not_try_flow_many(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    def fail_flow_many(_commands):
        raise AssertionError("KV submit_batch should not scan Flow-many compaction")

    monkeypatch.setattr(protocol_module, "_compact_flow_many_payloads_from_raw", fail_flow_many)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    future = adapter.submit_batch([("GET", "k1"), ("GET", "k2")])

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_opcodes(adapter._sock.sent[0]) == [0x000E]
    assert adapter._sock.sent[0][24 + 1] == 0x82


def test_protocol_submit_batch_coalesces_safe_mixed_kv_pipeline(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    future = adapter.submit_batch([("GET", "k1"), ("SET", "k2", "v2")])

    assert not future.done()
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x000E]
    assert adapter._sock.sent[0][24 + 1] == 0x05


def test_protocol_execute_batch_coalesces_flow_create_to_compact_many(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    calls = []

    def request(opcode, lane_id, payload, flags=0):
        calls.append((opcode, lane_id, payload, flags))
        return ProtocolResponse(lane_id, opcode, 1, flags, 0, b"OK")

    adapter._request = request

    result = adapter.execute_batch(
        [
            (
                "FLOW.CREATE",
                "f1",
                "TYPE",
                "email",
                "STATE",
                "queued",
                "NOW",
                123,
                "RUN_AT",
                123,
                "PAYLOAD",
                b"",
            ),
            (
                "FLOW.CREATE",
                "f2",
                "TYPE",
                "email",
                "STATE",
                "queued",
                "NOW",
                123,
                "RUN_AT",
                123,
                "PAYLOAD",
                b"",
            ),
        ]
    )

    assert result == [b"OK", b"OK"]
    assert len(calls) == 1
    opcode, lane_id, payload, flags = calls[0]
    assert opcode == 0x020F
    assert lane_id == 1
    assert flags == _FLAG_CUSTOM_PAYLOAD
    assert payload[0] == 0x90


def test_protocol_encodes_partitioned_flow_create_many_compact_payload():
    command = build_protocol_command(
        "FLOW.CREATE_MANY",
        "__flow_auto__:7",
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
        "f1",
        b"",
        "f2",
        b"",
    )

    assert command.opcode == 0x020F
    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)
    assert command.payload[0] == 0x96


def test_protocol_encodes_mixed_flow_create_many_compact_payload():
    command = build_protocol_command(
        "FLOW.CREATE_MANY",
        "MIXED",
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
        "f1",
        "tenant-a",
        b"",
        "f2",
        "tenant-b",
        b"",
    )

    assert command.opcode == 0x020F
    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)
    assert command.payload[0] == 0x9E


def test_protocol_execute_batch_coalesces_flow_complete_to_compact_many(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    calls = []

    def request(opcode, lane_id, payload, flags=0):
        calls.append((opcode, lane_id, payload, flags))
        return ProtocolResponse(lane_id, opcode, 1, flags, 0, b"OK")

    adapter._request = request

    result = adapter.execute_batch(
        [
            ("FLOW.COMPLETE", "f1", "lease-1", "FENCING", 1, "NOW", 123, "PARTITION", "p1"),
            ("FLOW.COMPLETE", "f2", "lease-2", "FENCING", 2, "NOW", 123, "PARTITION", "p2"),
        ]
    )

    assert result == [b"OK", b"OK"]
    assert len(calls) == 1
    opcode, lane_id, payload, flags = calls[0]
    assert opcode == 0x0210
    assert lane_id == 1
    assert flags == _FLAG_CUSTOM_PAYLOAD
    assert payload[0] == 0x93


def test_protocol_submit_batch_coalesces_flow_start_and_claim_to_compact_pipeline(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    captured = {}
    records = [{b"id": b"f1"}, {b"id": b"f2"}]

    def submit_request(opcode, lane_id, payload, flags=0):
        captured["opcode"] = opcode
        captured["lane_id"] = lane_id
        captured["payload"] = payload
        captured["flags"] = flags
        future = Future()
        future.set_result(ProtocolResponse(lane_id, opcode, 1, flags, 0, records))
        return 1, future

    adapter._submit_request = submit_request

    future = adapter.submit_batch(
        [
            (
                "FLOW.START_AND_CLAIM",
                "f1",
                "TYPE",
                "order",
                "INITIAL_STATE",
                "queued",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                30_000,
                "NOW",
                123,
                "PARTITION",
                "p1",
                "PAYLOAD",
                b"payload-1",
            ),
            (
                "FLOW.START_AND_CLAIM",
                "f2",
                "TYPE",
                "order",
                "INITIAL_STATE",
                "queued",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                30_000,
                "NOW",
                124,
                "PARTITION",
                "p2",
                "PAYLOAD",
                b"payload-2",
            ),
        ]
    )

    assert future.result() == records
    assert captured["opcode"] == 0x000E
    assert captured["lane_id"] == 1
    assert captured["flags"] == _FLAG_CUSTOM_PAYLOAD
    assert captured["payload"][:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 12, 2)


def test_protocol_submit_batch_unwraps_flow_many_pipeline_pairs(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    record1 = {b"id": b"f1"}
    record2 = {b"id": b"f2"}

    def submit_request(opcode, lane_id, payload, flags=0):
        future = Future()
        future.set_result(
            ProtocolResponse(lane_id, opcode, 1, flags, 0, [["ok", record1], ["ok", record2]])
        )
        return 1, future

    adapter._submit_request = submit_request

    future = adapter.submit_batch(
        [
            (
                "FLOW.START_AND_CLAIM",
                "f1",
                "TYPE",
                "order",
                "INITIAL_STATE",
                "queued",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                30_000,
                "NOW",
                123,
            ),
            (
                "FLOW.START_AND_CLAIM",
                "f2",
                "TYPE",
                "order",
                "INITIAL_STATE",
                "queued",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                30_000,
                "NOW",
                124,
            ),
        ]
    )

    assert future.result() == [record1, record2]


def test_protocol_execute_batch_requests_compact_pipeline_responses(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    captured = {}

    def request(opcode, lane_id, payload, flags=0):
        captured["opcode"] = opcode
        captured["lane_id"] = lane_id
        captured["payload"] = payload
        captured["flags"] = flags
        return type(
            "Response",
            (),
            {"status": 0, "value": [[b"ok", b"value"]], "trace": None},
        )()

    adapter._request = request

    assert adapter.execute_batch([("FLOW.GET", "flow-1")]) == [b"value"]
    assert captured["opcode"] == 0x000E
    assert captured["lane_id"] == 1
    assert captured["flags"] == _FLAG_CUSTOM_PAYLOAD
    assert captured["payload"][:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 9, 1)


def test_protocol_execute_batch_compacts_partitioned_flow_get(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    captured = {}

    def request(opcode, lane_id, payload, flags=0):
        captured["opcode"] = opcode
        captured["lane_id"] = lane_id
        captured["payload"] = payload
        captured["flags"] = flags
        return type("Response", (), {"status": 0, "value": [[b"ok", b"value"]], "trace": None})()

    adapter._request = request

    assert adapter.execute_batch([("FLOW.GET", "flow-1", "PARTITION", "tenant-a")]) == [b"value"]
    assert captured["opcode"] == 0x000E
    assert captured["lane_id"] == 1
    assert captured["flags"] == _FLAG_CUSTOM_PAYLOAD
    assert captured["payload"][:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 16, 1)
    assert b"tenant-a" in captured["payload"]


def test_protocol_execute_batch_compacts_flow_get_meta(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    captured = {}

    def request(opcode, lane_id, payload, flags=0):
        captured["opcode"] = opcode
        captured["lane_id"] = lane_id
        captured["payload"] = payload
        captured["flags"] = flags
        return type("Response", (), {"status": 0, "value": [[b"ok", b"value"]], "trace": None})()

    adapter._request = request

    command = ("FLOW.GET", "flow-1", "PARTITION", "tenant-a", "RETURN", "META")

    assert adapter.execute_batch([command]) == [b"value"]
    assert captured["opcode"] == 0x000E
    assert captured["lane_id"] == 1
    assert captured["flags"] == _FLAG_CUSTOM_PAYLOAD
    assert captured["payload"][:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 17, 1)
    assert b"tenant-a" in captured["payload"]


def test_protocol_execute_batch_compacts_flow_get_meta_without_partition(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    captured = {}

    def request(opcode, lane_id, payload, flags=0):
        captured["opcode"] = opcode
        captured["lane_id"] = lane_id
        captured["payload"] = payload
        captured["flags"] = flags
        return type("Response", (), {"status": 0, "value": [[b"ok", b"value"]], "trace": None})()

    adapter._request = request

    command = ("FLOW.GET", "flow-1", "RETURN", "META")

    assert adapter.execute_batch([command]) == [b"value"]
    assert captured["opcode"] == 0x000E
    assert captured["lane_id"] == 1
    assert captured["flags"] == _FLAG_CUSTOM_PAYLOAD
    assert captured["payload"] == (
        struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 17, 1)
        + struct.pack(">I", 6)
        + b"flow-1"
        + struct.pack(">I", 0xFFFF_FFFF)
    )


def test_protocol_builds_flow_list_return_meta_payload():
    command = build_protocol_command(
        "FLOW.LIST", "email", "STATE", "queued", "COUNT", 10, "RETURN", "META"
    )

    assert command.opcode == 0x020E
    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert command.payload == (
        bytes([_COMPACT_FLOW_LIST_REQUEST])
        + struct.pack(">I", 5)
        + b"email"
        + struct.pack(">I", 6)
        + b"queued"
        + struct.pack(">qB", 10, 1)
    )


def test_protocol_keeps_generic_flow_list_payload_for_unsupported_filters():
    command = build_protocol_command(
        "FLOW.LIST", "email", "STATE", "queued", "COUNT", 10, "REV", True
    )

    assert command.opcode == 0x020E
    assert command.flags == 0
    assert command.payload == {
        "type": "email",
        "state": "queued",
        "count": 10,
        "rev": True,
    }


def test_protocol_builds_flow_stats_with_attributes():
    command = build_protocol_command(
        "FLOW.STATS",
        "email",
        "STATE",
        "queued",
        "ATTRIBUTE",
        "tenant",
        "acme",
    )

    assert command.opcode == 0x022D
    assert command.flags == 0
    assert command.payload == {
        "type": "email",
        "state": "queued",
        "attributes": {"tenant": "acme"},
    }


def test_protocol_encodes_mset_and_mget_compact_wire_requests():
    mset = build_protocol_command("MSET", "k1", "v1", "k2", "v2")
    assert mset.opcode == 0x0105
    assert mset.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(mset.payload, bytes)
    assert mset.payload[0] == 0x94
    assert mset.payload[1] == 1

    mget = build_protocol_command("MGET", "k1", "k2")
    assert mget.opcode == 0x0104
    assert mget.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(mget.payload, bytes)
    assert mget.payload[0] == 0x94
    assert mget.payload[1] == 2


def test_protocol_encodes_data_structure_commands_as_compact_pipeline():
    hget_payload = _compact_pipeline_payload_from_raw(
        [("HGET", "h1", "field"), ("HGET", b"h2", b"field")],
        values_only=True,
    )
    assert hget_payload is not None
    assert hget_payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 18, 2)

    hmget_payload = _compact_pipeline_payload_from_raw(
        [("HMGET", "h1", "f1", "f2"), ("HMGET", b"h2", b"f1")],
        values_only=True,
    )
    assert hmget_payload is not None
    assert hmget_payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 28, 2)

    hgetall_payload = _compact_pipeline_payload_from_raw(
        [("HGETALL", "h1"), ("HGETALL", b"h2")],
        values_only=True,
    )
    assert hgetall_payload is not None
    assert hgetall_payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 30, 2)

    sismember_payload = _compact_pipeline_payload_from_raw(
        [("SISMEMBER", "s1", "member"), ("SISMEMBER", b"s2", b"member")],
        values_only=True,
    )
    assert sismember_payload is not None
    assert sismember_payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 19, 2)

    smembers_payload = _compact_pipeline_payload_from_raw(
        [("SMEMBERS", "s1"), ("SMEMBERS", b"s2")],
        values_only=True,
    )
    assert smembers_payload is not None
    assert smembers_payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 27, 2)

    lrange_payload = _compact_pipeline_payload_from_raw(
        [("LRANGE", "l1", 0, -1), ("LRANGE", b"l2", 0, -1)],
        values_only=True,
    )
    assert lrange_payload is not None
    assert lrange_payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 20, 2)

    zrange_payload = _compact_pipeline_payload_from_raw(
        [("ZRANGE", "z1", 0, -1), ("ZRANGE", b"z2", 0, -1)],
        values_only=True,
    )
    assert zrange_payload is not None
    assert zrange_payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 21, 2)

    zscore_payload = _compact_pipeline_payload_from_raw(
        [("ZSCORE", "z1", "member"), ("ZSCORE", b"z2", b"member")],
        values_only=True,
    )
    assert zscore_payload is not None
    assert zscore_payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 29, 2)


def test_protocol_decodes_compact_flow_record_response():
    record = compact_flow_record(
        [
            (1, b"flow-1"),
            (2, b"email"),
            (3, b"queued"),
            (4, 7),
            (0, b"custom", b"value"),
        ]
    )

    assert _try_fast_response_value_at(0x0202, record, 0) == {
        b"id": b"flow-1",
        b"type": b"email",
        b"state": b"queued",
        b"version": 7,
        b"custom": b"value",
    }


def test_protocol_decodes_compact_flow_records_inside_pipeline_response():
    record = compact_flow_record([(1, b"flow-1"), (2, b"email"), (3, b"queued")])
    record_list = (
        bytes([_COMPACT_FLOW_RECORD_LIST])
        + struct.pack(">I", 1)
        + compact_flow_record([(1, b"flow-2"), (2, b"email"), (3, b"queued")])
    )
    payload = (
        bytes([_COMPACT_PIPELINE_RESPONSE])
        + struct.pack(">I", 2)
        + b"\x00\x02"
        + record
        + b"\x00\x03"
        + record_list
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) == [
        ["ok", {b"id": b"flow-1", b"type": b"email", b"state": b"queued"}],
        ["ok", [{b"id": b"flow-2", b"type": b"email", b"state": b"queued"}]],
    ]


def test_protocol_decodes_compact_binary_lists_inside_pipeline_response():
    payload = (
        bytes([_COMPACT_PIPELINE_RESPONSE])
        + struct.pack(">I", 2)
        + b"\x00\x06"
        + struct.pack(">I", 2)
        + struct.pack(">I", 1)
        + b"a"
        + struct.pack(">I", 2)
        + b"bb"
        + b"\x00\x06"
        + struct.pack(">I", 0)
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) == [
        ["ok", [b"a", b"bb"]],
        ["ok", []],
    ]


def test_protocol_decodes_compact_binary_list_list_as_pipeline_values():
    payload = (
        bytes([_COMPACT_BINARY_LIST_LIST])
        + struct.pack(">I", 2)
        + struct.pack(">I", 2)
        + struct.pack(">I", 1)
        + b"a"
        + struct.pack(">I", 2)
        + b"bb"
        + struct.pack(">I", 0)
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) == [[b"a", b"bb"], []]


def test_protocol_decodes_compact_binary_list_list_singleton_fast_path():
    payload = (
        bytes([_COMPACT_BINARY_LIST_LIST])
        + struct.pack(">I", 3)
        + struct.pack(">I", 1)
        + struct.pack(">I", 3)
        + b"one"
        + struct.pack(">I", 0)
        + struct.pack(">I", 1)
        + struct.pack(">I", 3)
        + b"two"
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) == [[b"one"], [], [b"two"]]


def test_protocol_rejects_truncated_compact_binary_list_list():
    payload = (
        bytes([_COMPACT_BINARY_LIST_LIST])
        + struct.pack(">I", 1)
        + struct.pack(">I", 1)
        + struct.pack(">I", 10)
        + b"short"
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) is None


def test_protocol_decodes_compact_binary_map_list_as_pipeline_values():
    payload = (
        b"\x87"
        + struct.pack(">I", 2)
        + struct.pack(">I", 1)
        + struct.pack(">I", 1)
        + b"f"
        + struct.pack(">I", 1)
        + b"v"
        + struct.pack(">I", 0)
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) == [{b"f": b"v"}, {}]


def test_protocol_rejects_truncated_compact_binary_map_list():
    payload = (
        b"\x87"
        + struct.pack(">I", 1)
        + struct.pack(">I", 1)
        + struct.pack(">I", 1)
        + b"f"
        + struct.pack(">I", 10)
        + b"short"
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) is None


def test_protocol_decodes_compact_integer_list_as_pipeline_values():
    payload = (
        bytes([_COMPACT_INTEGER_LIST])
        + struct.pack(">I", 3)
        + struct.pack(">q", 1)
        + struct.pack(">q", 0)
        + struct.pack(">q", -2)
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) == [1, 0, -2]


def test_protocol_rejects_truncated_compact_integer_list():
    payload = bytes([_COMPACT_INTEGER_LIST]) + struct.pack(">I", 2) + struct.pack(">q", 1)

    assert _try_fast_response_value_at(0x000E, payload, 0) is None


def test_protocol_decodes_compact_claim_jobs_as_pipeline_values():
    payload = (
        b"\x80"
        + struct.pack(">I", 1)
        + struct.pack(">I", 6)
        + b"flow-1"
        + struct.pack(">I", 4)
        + b"part"
        + struct.pack(">I", 7)
        + b"lease-1"
        + struct.pack(">q", 42)
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) == [[b"flow-1", b"part", b"lease-1", 42]]


def test_protocol_decodes_typed_compact_claim_jobs_with_attributes():
    def tagged_binary(value: bytes) -> bytes:
        return b"\x04" + struct.pack(">I", len(value)) + value

    attrs = encode_value({b"tenant": b"acme"})
    payload = (
        b"\x05"
        + struct.pack(">I", 1)
        + b"\x05"
        + struct.pack(">I", 5)
        + tagged_binary(b"flow-1")
        + tagged_binary(b"part")
        + tagged_binary(b"lease-1")
        + b"\x03"
        + struct.pack(">q", 42)
        + attrs
    )

    assert protocol_module._try_decode_claim_jobs_compact(payload, 0) == [
        [b"flow-1", b"part", b"lease-1", 42, {b"tenant": b"acme"}]
    ]


def test_protocol_decodes_typed_compact_claim_jobs_with_state_and_attributes():
    def tagged_binary(value: bytes) -> bytes:
        return b"\x04" + struct.pack(">I", len(value)) + value

    def compact_optional_binary(value: bytes | None) -> bytes:
        if value is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(value)) + value

    attrs = encode_value({b"tenant": b"acme"})
    payload = (
        b"\x05"
        + struct.pack(">I", 1)
        + b"\x05"
        + struct.pack(">I", 6)
        + tagged_binary(b"flow-1")
        + tagged_binary(b"part")
        + tagged_binary(b"lease-1")
        + b"\x03"
        + struct.pack(">q", 42)
        + compact_optional_binary(b"ready")
        + attrs
    )

    assert protocol_module._try_decode_claim_jobs_compact(payload, 0) == [
        [b"flow-1", b"part", b"lease-1", 42, b"ready", {b"tenant": b"acme"}]
    ]


def test_protocol_decodes_compact_flow_record_list_as_pipeline_values():
    payload = (
        bytes([_COMPACT_FLOW_RECORD_LIST])
        + struct.pack(">I", 2)
        + compact_flow_record([(1, b"flow-1"), (2, b"email"), (3, b"queued")])
        + compact_flow_record([(1, b"flow-2"), (2, b"email"), (3, b"done")])
    )

    assert _try_fast_response_value_at(0x000E, payload, 0) == [
        {b"id": b"flow-1", b"type": b"email", b"state": b"queued"},
        {b"id": b"flow-2", b"type": b"email", b"state": b"done"},
    ]


def test_protocol_submit_commands_coalesces_mixed_pipeline(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.sent = []

        def sendall(self, data):
            self.sent.append(data)

    adapter = ProtocolAdapter("127.0.0.1", 6388, lanes=16)
    adapter._sock = FakeSocket()

    futures = adapter.submit_commands([("SET", "k", "v"), ("GET", "k")])

    assert len(futures) == 2
    assert adapter._sock.sent[0].count(b"FSNP") == 1
    assert frame_lanes(adapter._sock.sent[0]) == [1]
    assert frame_opcodes(adapter._sock.sent[0]) == [0x000E]


def frame_lanes(data: bytes) -> list[int]:
    lanes = []
    offset = 0

    while offset < len(data):
        magic, _version, _flags, lane_id, _opcode, _request_id, body_len = struct.unpack(
            ">4sBBIHQI", data[offset : offset + 24]
        )
        assert magic == b"FSNP"
        lanes.append(lane_id)
        offset += 24 + body_len

    return lanes


def frame_opcodes(data: bytes) -> list[int]:
    opcodes = []
    offset = 0

    while offset < len(data):
        magic, _version, _flags, _lane_id, opcode, _request_id, body_len = struct.unpack(
            ">4sBBIHQI", data[offset : offset + 24]
        )
        assert magic == b"FSNP"
        opcodes.append(opcode)
        offset += 24 + body_len

    return opcodes


def compact_flow_record(fields: list[tuple[int, bytes, Any] | tuple[int, Any]]) -> bytes:
    entries = []
    for field in fields:
        key_id = field[0]
        if key_id == 0:
            _key_id, key, value = field
            entries.append(b"\x00" + struct.pack(">I", len(key)) + key + encode_value(value))
        else:
            _key_id, value = field
            entries.append(bytes([key_id]) + encode_value(value))
    return bytes([_COMPACT_FLOW_RECORD]) + struct.pack(">I", len(entries)) + b"".join(entries)


def test_protocol_adapter_wait_event_returns_unsolicited_events(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    adapter = ProtocolAdapter("127.0.0.1", 6388)

    with adapter._events_cv:
        adapter._events.append({"event": "FLOW_WAKE"})
        adapter._events_cv.notify_all()

    assert adapter.wait_event(timeout=0.01) == {"event": "FLOW_WAKE"}
    assert adapter.wait_event(timeout=0.0) is None


def test_protocol_subscribe_flow_wake_sends_event_filter(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    captured = {}

    def fake_request(self, opcode, lane_id, payload, flags=0):
        captured["opcode"] = opcode
        captured["lane_id"] = lane_id
        captured["payload"] = payload
        captured["flags"] = flags
        return type("Response", (), {"status": 0, "value": {"subscribed": ["FLOW_WAKE"]}})()

    monkeypatch.setattr(ProtocolAdapter, "_request", fake_request)

    adapter = ProtocolAdapter("127.0.0.1", 6388)
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


def test_ferrics_url_enables_protocol_tls(monkeypatch):
    captured = {}

    def fake_init(self, host, port, **kwargs):
        captured["host"] = host
        captured["port"] = port
        captured["kwargs"] = kwargs

    monkeypatch.setattr(ProtocolAdapter, "__init__", fake_init)

    ProtocolAdapter.from_url("ferrics://default:secret@store.example.com")

    assert captured == {
        "host": "store.example.com",
        "port": 6389,
        "kwargs": {
            "username": "default",
            "password": "secret",
            "tls": True,
        },
    }


def test_protocol_adapter_uses_real_tcp_frames():
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

    adapter = ProtocolAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
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


def test_protocol_adapter_sends_idle_heartbeat_ping():
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
            conn.settimeout(1.0)
            with conn:
                startup_opcode, startup_lane, startup_id, startup_payload = recv_frame(conn)
                received.append((startup_opcode, startup_lane, startup_payload))
                conn.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))

                ping_opcode, ping_lane, ping_id, ping_payload = recv_frame(conn)
                received.append((ping_opcode, ping_lane, ping_payload))
                conn.sendall(response(ping_opcode, ping_lane, ping_id, "PONG"))

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    adapter = ProtocolAdapter(
        "127.0.0.1",
        port,
        client_name="pytest",
        timeout=1.0,
        heartbeat_interval=0.02,
        heartbeat_timeout=1.0,
    )
    try:
        thread.join(timeout=1.0)
    finally:
        adapter.close()

    assert received[0][0] == 0x000C
    assert received[1] == (0x0003, 0, {})


def test_protocol_adapter_execute_command_with_trace_unwraps_value_and_timings():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    seen_flags: list[int] = []

    def recv_exact(conn: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = conn.recv(size - len(chunks))
            if not chunk:
                raise RuntimeError("connection closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def recv_frame(conn: socket.socket) -> tuple[int, int, int, int, Any]:
        raw_header = recv_exact(conn, header.size)
        magic, version, flags, lane_id, opcode, request_id, body_len = header.unpack(raw_header)
        assert magic == b"FSNP"
        assert version == 0x01
        value, rest = decode_value(recv_exact(conn, body_len))
        assert rest == b""
        return opcode, lane_id, request_id, flags, value

    def response(opcode: int, lane_id: int, request_id: int, value: Any, flags: int = 0) -> bytes:
        body = status.pack(0) + encode_value(value)
        return header.pack(b"FSNP", 0x81, flags, lane_id, opcode, request_id, len(body)) + body

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def serve() -> None:
        with listener:
            conn, _addr = listener.accept()
            with conn:
                startup_opcode, startup_lane, startup_id, _startup_flags, _startup_payload = (
                    recv_frame(conn)
                )
                conn.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))

                opcode, lane, request_id, flags, _payload = recv_frame(conn)
                seen_flags.append(flags)
                conn.sendall(
                    response(
                        opcode,
                        lane,
                        request_id,
                        {
                            "value": b"value",
                            "trace": {
                                "server_decode_us": 1,
                                "server_command_execute_us": 2,
                            },
                        },
                        flags=0x01,
                    )
                )

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    adapter = ProtocolAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
    try:
        result = adapter.execute_command_with_trace("GET", "k")
    finally:
        adapter.close()
        thread.join(timeout=1.0)

    assert seen_flags == [0x01]
    assert result["value"] == b"value"
    assert result["trace"]["server"]["server_decode_us"] == 1
    assert result["trace"]["server"]["server_command_execute_us"] == 2
    assert result["trace"]["client"]["encode_us"] >= 0
    assert result["trace"]["client"]["socket_write_us"] >= 0
    assert result["trace"]["client"]["response_read_us"] >= 0
    assert result["trace"]["client"]["decode_us"] >= 0
    assert result["trace"]["client"]["request_lock_wait_us"] >= 0
    assert result["trace"]["client"]["submit_locked_us"] >= 0
    assert result["trace"]["client"]["submit_total_us"] >= 0
    assert result["trace"]["client"]["future_wait_us"] >= 0
    assert result["trace"]["client"]["request_total_us"] >= 0


def test_protocol_fast_decodes_custom_flow_claim_jobs():
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


def test_protocol_fast_decodes_custom_ok_list():
    assert _try_fast_response_value(0x0210, b"\x81" + struct.pack(">I", 3)) == [
        b"OK",
        b"OK",
        b"OK",
    ]


def test_protocol_fast_decodes_custom_ok_list_from_frame_body_offset():
    body = struct.pack(">H", 0) + b"\x81" + struct.pack(">I", 3)

    assert _try_fast_response_value_at(0x0210, body, 2) == [
        b"OK",
        b"OK",
        b"OK",
    ]


def test_protocol_fast_decodes_custom_kv_values_from_frame_body_offset():
    get_value = b"\x82\x01" + struct.pack(">I", 3) + b"abc"
    get_nil = b"\x82\x00"
    mget = (
        b"\x83"
        + struct.pack(">I", 3)
        + b"\x01"
        + struct.pack(">I", 1)
        + b"a"
        + b"\x00"
        + b"\x01"
        + struct.pack(">I", 2)
        + b"bc"
    )

    assert _try_fast_response_value_at(0x0101, b"\x00\x00" + get_value, 2) == b"abc"
    assert _try_fast_response_value_at(0x0101, b"\x00\x00" + get_nil, 2) is None
    assert _try_fast_response_value_at(0x0104, b"\x00\x00" + mget, 2) == [b"a", None, b"bc"]

    fixed_mget = b"\x89" + struct.pack(">II", 3, 2) + b"aabbcc"
    assert _try_fast_response_value_at(0x0104, b"\x00\x00" + fixed_mget, 2) == [
        b"aa",
        b"bb",
        b"cc",
    ]


def test_protocol_fast_decodes_direct_kv_write_ok_payloads_from_frame_body_offset():
    ok = struct.pack(">H", 0) + b"\x81" + struct.pack(">I", 1)

    assert _try_fast_response_value_at(0x0102, ok, 2) == b"OK"
    assert _try_fast_response_value_at(0x0105, ok, 2) == b"OK"


def test_protocol_fast_decodes_custom_pipeline_response_from_frame_body_offset():
    body = (
        struct.pack(">H", 0)
        + b"\x95"
        + struct.pack(">I", 4)
        + b"\x00\x01"
        + struct.pack(">I", 2)
        + b"OK"
        + b"\x00\x00"
        + b"\x00\x05"
        + struct.pack(">I", 5)
        + b"ref-1"
        + struct.pack(">I", 2)
        + b"p1"
        + struct.pack(">I", 0xFFFFFFFF)
        + b"\x02"
        + struct.pack(">I", 3)
        + b"ERR"
    )

    assert _try_fast_response_value_at(0x000E, body, 2) == [
        ["ok", b"OK"],
        ["ok", None],
        ["ok", {b"ref": b"ref-1", b"partition_key": b"p1"}],
        ["error", b"ERR"],
    ]


def test_protocol_fast_decodes_pipeline_values_payloads_from_frame_body_offset():
    ok_list = struct.pack(">H", 0) + b"\x81" + struct.pack(">I", 3)
    mget = (
        struct.pack(">H", 0)
        + b"\x83"
        + struct.pack(">I", 2)
        + b"\x01"
        + struct.pack(">I", 2)
        + b"v1"
        + b"\x00"
    )

    assert _try_fast_response_value_at(0x000E, ok_list, 2) == [b"OK", b"OK", b"OK"]
    assert _try_fast_response_value_at(0x000E, mget, 2) == [b"v1", None]

    fixed_mget = struct.pack(">H", 0) + b"\x89" + struct.pack(">II", 2, 2) + b"v1v2"
    assert _try_fast_response_value_at(0x000E, fixed_mget, 2) == [b"v1", b"v2"]


def test_protocol_fast_decodes_custom_claim_jobs_from_frame_body_offset():
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


def test_protocol_fast_decodes_custom_claim_jobs_with_attributes():
    def bin_field(value: bytes | None) -> bytes:
        if value is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(value)) + value

    attrs = b"\x06" + struct.pack(">I", 1) + bin_field(b"tenant") + b"\x04" + bin_field(b"acme")
    body = (
        struct.pack(">H", 0)
        + b"\x80"
        + struct.pack(">I", 1)
        + bin_field(b"f1")
        + bin_field(b"p1")
        + bin_field(b"lease1")
        + struct.pack(">q", 7)
        + attrs
    )

    assert _try_fast_response_value_at(0x0203, body, 2) == [
        [b"f1", b"p1", b"lease1", 7, {b"tenant": b"acme"}]
    ]


def test_protocol_fast_decodes_custom_claim_jobs_with_state_and_attributes():
    def bin_field(value: bytes | None) -> bytes:
        if value is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(value)) + value

    attrs = b"\x06" + struct.pack(">I", 1) + bin_field(b"tenant") + b"\x04" + bin_field(b"acme")
    body = (
        struct.pack(">H", 0)
        + b"\x80"
        + struct.pack(">I", 1)
        + bin_field(b"f1")
        + bin_field(b"p1")
        + bin_field(b"lease1")
        + struct.pack(">q", 7)
        + bin_field(b"ready")
        + attrs
    )

    assert _try_fast_response_value_at(0x0203, body, 2) == [
        [b"f1", b"p1", b"lease1", 7, b"ready", {b"tenant": b"acme"}]
    ]


def test_async_flow_client_from_protocol_url_uses_async_protocol_adapter_pool(monkeypatch):
    created = {}

    class FakeAsyncProtocolAdapterPool:
        @classmethod
        def from_url(cls, url, **kwargs):
            created["url"] = url
            created["kwargs"] = kwargs
            return cls()

        async def execute_command(self, *args):
            return b"OK"

    monkeypatch.setattr(
        "ferricstore.protocol.AsyncProtocolAdapterPool", FakeAsyncProtocolAdapterPool
    )

    async def run():
        client = AsyncFlowClient.from_url("ferric://localhost:6388", max_connections=4)
        assert await client.command("PING") == b"OK"

    asyncio.run(run())

    assert created == {
        "url": "ferric://localhost:6388",
        "kwargs": {"max_connections": 4},
    }


def test_async_protocol_pipeline_uses_batch():
    class FakeAsyncProtocolAdapter(AsyncProtocolAdapter):
        def __init__(self):
            pass

        async def execute_batch(self, commands):
            return [command[0] for command in commands]

    async def run():
        pipe = FakeAsyncProtocolAdapter().pipeline()
        pipe.execute_command("SET", "k", "v")
        pipe.execute_command("GET", "k")
        assert await pipe.execute() == ["SET", "GET"]

    asyncio.run(run())


def test_async_protocol_adapter_pool_uses_max_connections_and_round_robins(monkeypatch):
    created = []

    class FakeAsyncProtocolAdapter:
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
        adapter = FakeAsyncProtocolAdapter(len(created))
        created.append((url, kwargs, adapter))
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", staticmethod(from_url))

    async def run():
        pool = AsyncProtocolAdapterPool.from_url(
            "ferric://localhost:6388", max_connections=2, timeout=1.0
        )

        assert isinstance(pool, AsyncProtocolAdapterPool)
        assert [await pool.execute_command("PING") for _ in range(3)] == [0, 1, 0]

    asyncio.run(run())

    assert [call[:2] for call in created] == [
        ("ferric://localhost:6388", {"timeout": 1.0}),
        ("ferric://localhost:6388", {"timeout": 1.0}),
    ]


def test_protocol_adapter_trace_command_returns_client_and_server_timings():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    trace_flag = 0x01

    def recv_exact(conn: socket.socket, size: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < size:
            chunk = conn.recv(size - len(chunks))
            if not chunk:
                raise RuntimeError("connection closed")
            chunks.extend(chunk)
        return bytes(chunks)

    def recv_frame(conn: socket.socket) -> tuple[int, int, int, int, Any]:
        raw_header = recv_exact(conn, header.size)
        magic, version, flags, lane_id, opcode, request_id, body_len = header.unpack(raw_header)
        assert magic == b"FSNP"
        assert version == 0x01
        value, rest = decode_value(recv_exact(conn, body_len))
        assert rest == b""
        return opcode, lane_id, request_id, flags, value

    def response(opcode: int, lane_id: int, request_id: int, value: Any, flags: int = 0) -> bytes:
        body = status.pack(0) + encode_value(value)
        return header.pack(b"FSNP", 0x81, flags, lane_id, opcode, request_id, len(body)) + body

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def serve() -> None:
        with listener:
            conn, _addr = listener.accept()
            with conn:
                startup_opcode, startup_lane, startup_id, startup_flags, _startup_payload = (
                    recv_frame(conn)
                )
                assert startup_flags == 0
                conn.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))

                opcode, lane, request_id, flags, _payload = recv_frame(conn)
                assert flags & trace_flag == trace_flag
                conn.sendall(
                    response(
                        opcode,
                        lane,
                        request_id,
                        {
                            "value": b"value",
                            "trace": {
                                "server_decode_us": 1,
                                "server_route_us": 2,
                                "server_command_execute_us": 3,
                            },
                        },
                        flags=trace_flag,
                    )
                )

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    adapter = ProtocolAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
    try:
        result = adapter.execute_command_with_trace("GET", "a")
    finally:
        adapter.close()
        thread.join(timeout=1.0)

    assert result["value"] == b"value"
    assert result["trace"]["server"] == {
        "server_decode_us": 1,
        "server_route_us": 2,
        "server_command_execute_us": 3,
    }
    for key in (
        "request_lock_wait_us",
        "encode_us",
        "socket_write_us",
        "submit_locked_us",
        "submit_total_us",
        "response_read_us",
        "decode_us",
        "future_wait_us",
        "request_total_us",
    ):
        assert isinstance(result["trace"]["client"][key], int)
        assert result["trace"]["client"][key] >= 0


def test_protocol_adapter_allows_multiple_inflight_requests():
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
    adapter = ProtocolAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(adapter.execute_command, "GET", "a")
            second = pool.submit(adapter.execute_command, "GET", "b")
            assert [first.result(timeout=1.0), second.result(timeout=1.0)] == [b"one", b"two"]
    finally:
        adapter.close()
        thread.join(timeout=1.0)

    assert received == [(0x0101, 1), (0x0101, 2)]


def test_protocol_adapter_submit_command_returns_future_without_waiting():
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
    adapter = ProtocolAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
    try:
        future = adapter.submit_command("GET", "a")
        assert request_received.wait(timeout=1.0)
        assert future.done() is False
        release_response.set()
        assert future.result(timeout=1.0) == b"value"
    finally:
        adapter.close()
        thread.join(timeout=1.0)


def test_async_protocol_adapter_allows_multiple_inflight_requests():
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
            try:
                startup_opcode, startup_lane, startup_id, _startup_payload = await recv_frame(
                    reader
                )
                writer.write(response(startup_opcode, startup_lane, startup_id, {"ok": True}))
                await writer.drain()

                first_opcode, first_lane, first_id, _first_payload = await recv_frame(reader)
                second_opcode, second_lane, second_id, _second_payload = await recv_frame(reader)
                received.extend([(first_opcode, first_lane), (second_opcode, second_lane)])

                writer.write(response(first_opcode, first_lane, first_id, b"one"))
                writer.write(response(second_opcode, second_lane, second_id, b"two"))
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        adapter = AsyncProtocolAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
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


def test_async_protocol_adapter_trace_command_returns_client_and_server_timings():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    trace_flag = 0x01

    async def recv_frame(
        reader: asyncio.StreamReader,
    ) -> tuple[int, int, int, int, Any]:
        raw_header = await reader.readexactly(header.size)
        magic, version, flags, lane_id, opcode, request_id, body_len = header.unpack(raw_header)
        assert magic == b"FSNP"
        assert version == 0x01
        value, rest = decode_value(await reader.readexactly(body_len))
        assert rest == b""
        return opcode, lane_id, request_id, flags, value

    def response(opcode: int, lane_id: int, request_id: int, value: Any, flags: int = 0) -> bytes:
        body = status.pack(0) + encode_value(value)
        return header.pack(b"FSNP", 0x81, flags, lane_id, opcode, request_id, len(body)) + body

    async def run() -> dict[str, Any]:
        async def handle(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            try:
                (
                    startup_opcode,
                    startup_lane,
                    startup_id,
                    startup_flags,
                    _startup_payload,
                ) = await recv_frame(reader)
                assert startup_flags == 0
                writer.write(response(startup_opcode, startup_lane, startup_id, {"ok": True}))
                await writer.drain()

                opcode, lane, request_id, flags, _payload = await recv_frame(reader)
                assert flags & trace_flag == trace_flag
                writer.write(
                    response(
                        opcode,
                        lane,
                        request_id,
                        {
                            "value": b"async-value",
                            "trace": {
                                "server_decode_us": 4,
                                "server_route_us": 5,
                                "server_command_execute_us": 6,
                            },
                        },
                        flags=trace_flag,
                    )
                )
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        adapter = AsyncProtocolAdapter("127.0.0.1", port, client_name="pytest", timeout=1.0)
        try:
            return await asyncio.wait_for(
                adapter.execute_command_with_trace("GET", "a"), timeout=1.0
            )
        finally:
            await adapter.close()
            server.close()
            await server.wait_closed()

    result = asyncio.run(run())

    assert result["value"] == b"async-value"
    assert result["trace"]["server"] == {
        "server_decode_us": 4,
        "server_route_us": 5,
        "server_command_execute_us": 6,
    }
    for key in ("encode_us", "socket_write_us", "response_read_us", "decode_us"):
        assert isinstance(result["trace"]["client"][key], int)


def test_async_protocol_request_timeout_clears_pending():
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        adapter.timeout = 0.01
        adapter._write_lock = asyncio.Lock()
        adapter._request_id = 0
        adapter.lanes = 1
        adapter._lane_cursor = 0
        adapter._pending = {}
        adapter._pending_traces = {}

        async def fake_send(opcode, lane_id, request_id, payload, flags=0):
            return None

        adapter._send = fake_send

        with pytest.raises(FerricStoreError, match="timed out"):
            await adapter._request(0x0101, 1, {"key": "a"})

        assert adapter._pending == {}
        assert adapter._pending_traces == {}

    asyncio.run(run())


def test_async_protocol_adapter_sends_idle_heartbeat_ping():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    received: list[tuple[int, int, Any]] = []

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
            try:
                startup_opcode, startup_lane, startup_id, startup_payload = await recv_frame(reader)
                received.append((startup_opcode, startup_lane, startup_payload))
                writer.write(response(startup_opcode, startup_lane, startup_id, {"ok": True}))
                await writer.drain()

                ping_opcode, ping_lane, ping_id, ping_payload = await asyncio.wait_for(
                    recv_frame(reader),
                    timeout=1.0,
                )
                received.append((ping_opcode, ping_lane, ping_payload))
                writer.write(response(ping_opcode, ping_lane, ping_id, "PONG"))
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        adapter = AsyncProtocolAdapter(
            "127.0.0.1",
            port,
            client_name="pytest",
            timeout=1.0,
            heartbeat_interval=0.02,
            heartbeat_timeout=1.0,
        )
        try:
            await adapter._ensure_connected()
            await asyncio.sleep(0.08)
        finally:
            await adapter.close()
            server.close()
            await server.wait_closed()

    asyncio.run(run())

    assert received[0][0] == 0x000C
    assert received[1] == (0x0003, 0, {})


def test_async_protocol_send_drains_only_after_threshold():
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
        adapter = object.__new__(AsyncProtocolAdapter)
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


def test_protocol_encodes_simple_flow_create_many_to_compact_request():
    command = build_protocol_command(
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


def test_protocol_encodes_simple_claim_due_to_compact_request():
    command = build_protocol_command(
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


def test_protocol_encodes_simple_complete_many_to_compact_request():
    command = build_protocol_command(
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


def test_protocol_encodes_ok_on_success_complete_many_to_compact_request():
    command = build_protocol_command(
        "FLOW.COMPLETE_MANY",
        "MIXED",
        "NOW",
        123,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
        "flow-1",
        "p1",
        "lease-1",
        7,
    )

    assert command.opcode == 0x0210
    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)
    assert command.payload[0] == 0x93


def test_protocol_encodes_retry_fail_cancel_transition_many_to_compact_requests():
    retry = build_protocol_command(
        "FLOW.RETRY_MANY",
        "MIXED",
        "NOW",
        123,
        "RUN_AT",
        456,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
        "flow-1",
        "p1",
        "lease-1",
        7,
    )
    fail = build_protocol_command(
        "FLOW.FAIL_MANY",
        "MIXED",
        "NOW",
        123,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
        "flow-1",
        "p1",
        "lease-1",
        7,
    )
    cancel = build_protocol_command(
        "FLOW.CANCEL_MANY",
        "MIXED",
        "NOW",
        123,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
        "flow-1",
        "p1",
        7,
    )
    transition = build_protocol_command(
        "FLOW.TRANSITION_MANY",
        "MIXED",
        "queued",
        "next",
        "NOW",
        123,
        "RUN_AT",
        456,
        "INDEPENDENT",
        "true",
        "RETURN",
        "OK_ON_SUCCESS",
        "ITEMS",
        "flow-1",
        "p1",
        7,
        None,
    )

    assert retry.opcode == 0x0212
    assert retry.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(retry.payload, bytes)
    assert retry.payload[0] == 0x98

    assert fail.opcode == 0x0213
    assert fail.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(fail.payload, bytes)
    assert fail.payload[0] == 0x93

    assert cancel.opcode == 0x0214
    assert cancel.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(cancel.payload, bytes)
    assert cancel.payload[0] == 0x9A

    assert transition.opcode == 0x0211
    assert transition.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(transition.payload, bytes)
    assert transition.payload[0] == 0x9C


def test_protocol_keeps_complex_complete_many_on_generic_request_codec():
    command = build_protocol_command(
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


def test_encodes_protocol_set_get_and_rejects_unknown_commands():
    set_cmd = build_protocol_command("SET", "k", b"v", "PX", 10, "NX")
    get_cmd = build_protocol_command("GET", "k")

    assert set_cmd == ProtocolCommand(
        opcode=0x0102,
        payload={"key": "k", "value": b"v", "ttl": 10, "nx": True},
        lane_id=1,
    )
    assert get_cmd == ProtocolCommand(opcode=0x0101, payload={"key": "k"}, lane_id=1)
    mget = build_protocol_command("MGET", "a", "b")
    assert mget.opcode == 0x0104
    assert mget.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(mget.payload, bytes)
    assert mget.payload[0] == 0x94
    assert mget.payload[1] == 2

    with pytest.raises(InvalidCommandError):
        build_protocol_command("GET.COMPACT", "k")

    with pytest.raises(InvalidCommandError):
        build_protocol_command("MGET.COMPACT", "a", "b")


def test_encodes_protocol_data_structure_commands():
    assert build_protocol_command("HSET", "h", "f", "v", "g", b"w") == ProtocolCommand(
        opcode=0x0110,
        payload={"key": "h", "fields": {"f": "v", "g": b"w"}},
        lane_id=1,
    )
    assert build_protocol_command("HGET", "h", "f") == ProtocolCommand(
        opcode=0x0111,
        payload={"key": "h", "field": "f"},
        lane_id=1,
    )
    assert build_protocol_command("HMGET", "h", "f", "g") == ProtocolCommand(
        opcode=0x0112,
        payload={"key": "h", "fields": ["f", "g"]},
        lane_id=1,
    )
    assert build_protocol_command("HGETALL", "h") == ProtocolCommand(
        opcode=0x0113,
        payload={"key": "h"},
        lane_id=1,
    )
    assert build_protocol_command("LPUSH", "l", "a", "b") == ProtocolCommand(
        opcode=0x0120,
        payload={"key": "l", "values": ["a", "b"]},
        lane_id=1,
    )
    assert build_protocol_command("LPOP", "l", 2) == ProtocolCommand(
        opcode=0x0122,
        payload={"key": "l", "count": 2},
        lane_id=1,
    )
    assert build_protocol_command("LRANGE", "l", 0, -1) == ProtocolCommand(
        opcode=0x0124,
        payload={"key": "l", "start": 0, "stop": -1},
        lane_id=1,
    )
    assert build_protocol_command("SADD", "s", "a", "b") == ProtocolCommand(
        opcode=0x0130,
        payload={"key": "s", "members": ["a", "b"]},
        lane_id=1,
    )
    assert build_protocol_command("SISMEMBER", "s", "a") == ProtocolCommand(
        opcode=0x0133,
        payload={"key": "s", "member": "a"},
        lane_id=1,
    )
    assert build_protocol_command("ZADD", "z", "1.5", "a", 2, "b") == ProtocolCommand(
        opcode=0x0140,
        payload={"key": "z", "items": [[1.5, "a"], [2.0, "b"]]},
        lane_id=1,
    )
    assert build_protocol_command("ZRANGE", "z", 0, -1, "WITHSCORES") == ProtocolCommand(
        opcode=0x0142,
        payload={"key": "z", "start": 0, "stop": -1, "withscores": True},
        lane_id=1,
    )
    assert build_protocol_command("ZSCORE", "z", "a") == ProtocolCommand(
        opcode=0x0143,
        payload={"key": "z", "member": "a"},
        lane_id=1,
    )

    with pytest.raises(InvalidCommandError):
        build_protocol_command("HSET", "h", "field-without-value")

    with pytest.raises(InvalidCommandError):
        build_protocol_command("HMGET", "h")

    with pytest.raises(InvalidCommandError):
        build_protocol_command("ZADD", "z", "not-a-score", "a")


def test_encodes_protocol_protocol_custom_commands():
    assert build_protocol_command("CAS", "k", b"old", b"new", "PX", 100) == ProtocolCommand(
        opcode=0x0106,
        payload={"key": "k", "expected": b"old", "value": b"new", "ttl": 100},
        lane_id=1,
    )
    assert build_protocol_command("LOCK", "lock:k", "owner", 500) == ProtocolCommand(
        opcode=0x0107,
        payload={"key": "lock:k", "owner": "owner", "ttl_ms": 500},
        lane_id=1,
    )
    assert build_protocol_command("UNLOCK", "lock:k", "owner") == ProtocolCommand(
        opcode=0x0108,
        payload={"key": "lock:k", "owner": "owner"},
        lane_id=1,
    )
    assert build_protocol_command("EXTEND", "lock:k", "owner", 750) == ProtocolCommand(
        opcode=0x0109,
        payload={"key": "lock:k", "owner": "owner", "ttl_ms": 750},
        lane_id=1,
    )
    assert build_protocol_command("RATELIMIT.ADD", "rl:k", 1000, 10, 2) == ProtocolCommand(
        opcode=0x010A,
        payload={"key": "rl:k", "window_ms": 1000, "max": 10, "count": 2},
        lane_id=1,
    )
    assert build_protocol_command("FETCH_OR_COMPUTE", "cache:k", 1000, "h") == ProtocolCommand(
        opcode=0x010B,
        payload={"key": "cache:k", "ttl_ms": 1000, "hint": "h"},
        lane_id=1,
    )
    assert build_protocol_command(
        "FETCH_OR_COMPUTE_RESULT", "cache:k", b"v", 1000
    ) == ProtocolCommand(
        opcode=0x010C,
        payload={"key": "cache:k", "value": b"v", "ttl_ms": 1000},
        lane_id=1,
    )
    assert build_protocol_command("FETCH_OR_COMPUTE_ERROR", "cache:k", "failed") == ProtocolCommand(
        opcode=0x010D,
        payload={"key": "cache:k", "message": "failed"},
        lane_id=1,
    )


def test_encodes_protocol_protocol_admin_commands():
    assert build_protocol_command("CLUSTER.KEYSLOT", "k") == ProtocolCommand(
        opcode=0x0303,
        payload={"key": "k", "args": ["k"]},
        lane_id=1,
    )
    assert build_protocol_command("CLUSTER.JOIN", "node@host", "REPLACE") == ProtocolCommand(
        opcode=0x0306,
        payload={"args": ["node@host", "REPLACE"]},
        lane_id=1,
    )
    assert build_protocol_command("CLUSTER.FAILOVER", 1, "node@host") == ProtocolCommand(
        opcode=0x0308,
        payload={"args": [1, "node@host"]},
        lane_id=1,
    )
    assert build_protocol_command("FERRICSTORE.KEY_INFO", "k") == ProtocolCommand(
        opcode=0x030C,
        payload={"key": "k", "args": ["k"]},
        lane_id=1,
    )
    assert build_protocol_command("FERRICSTORE.CONFIG", "GET", "prefix") == ProtocolCommand(
        opcode=0x030D,
        payload={"args": ["GET", "prefix"]},
        lane_id=1,
    )
    assert build_protocol_command("FERRICSTORE.METRICS") == ProtocolCommand(
        opcode=0x030F,
        payload={"args": []},
        lane_id=1,
    )


def test_encodes_protocol_flow_value_and_retention_options():
    create = build_protocol_command(
        "FLOW.CREATE",
        "f1",
        "TYPE",
        "email",
        "RETENTION_TTL_MS",
        5000,
    )
    assert create.payload["retention_ttl_ms"] == 5000
    assert "terminal_ttl_ms" not in create.payload

    value_put = build_protocol_command(
        "FLOW.VALUE.PUT",
        b"value",
        "OWNER_FLOW_ID",
        "f1",
        "NAME",
        "reservation",
        "OVERRIDE",
        "true",
    )
    assert value_put == ProtocolCommand(
        opcode=0x020B,
        payload={
            "value": b"value",
            "owner_flow_id": "f1",
            "name": "reservation",
            "override": True,
        },
        lane_id=1,
    )

    value_mget = build_protocol_command("FLOW.VALUE.MGET", "ref-a", "ref-b", "MAX_BYTES", 10)
    assert value_mget.opcode == 0x020C
    assert value_mget.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(value_mget.payload, bytes)
    assert value_mget.payload[0] == 0x9D


def test_encodes_protocol_flow_schedule_governance_and_attribute_commands():
    attrs = build_protocol_command(
        "FLOW.ATTRIBUTES",
        "order",
        "STATE",
        "queued",
        "PARTITION",
        "tenant-a",
        "COUNT",
        10,
    )
    assert attrs == ProtocolCommand(
        opcode=0x022E,
        payload={
            "type": "order",
            "state": "queued",
            "partition_key": "tenant-a",
            "count": 10,
        },
        lane_id=1,
    )

    values = build_protocol_command(
        "FLOW.ATTRIBUTE_VALUES",
        "order",
        "tenant",
        "STATE",
        "queued",
        "REV",
        "true",
    )
    assert values == ProtocolCommand(
        opcode=0x022F,
        payload={"type": "order", "attribute": "tenant", "state": "queued", "rev": True},
        lane_id=1,
    )

    schedule = build_protocol_command(
        "FLOW.SCHEDULE.CREATE",
        "daily-report",
        "TARGET",
        {"id": "flow-1", "type": "report", "state": "queued"},
        "TIMEZONE",
        "Asia/Jerusalem",
        "OVERWRITE",
        "true",
    )
    assert schedule.opcode == 0x0225
    assert schedule.payload == {
        "id": "daily-report",
        "target": {"id": "flow-1", "type": "report", "state": "queued"},
        "timezone": "Asia/Jerusalem",
        "overwrite": True,
    }

    fire_due = build_protocol_command("FLOW.SCHEDULE.FIRE_DUE", "BLOCK", 1000, "LIMIT", 50)
    assert fire_due == ProtocolCommand(
        opcode=0x0228,
        payload={"block_ms": 1000, "limit": 50},
        lane_id=1,
    )

    approval = build_protocol_command(
        "FLOW.APPROVAL.REQUEST",
        "approval-1",
        "FLOW_ID",
        "flow-1",
        "SCOPE",
        "tenant-a",
        "ASSIGNEES",
        ["ops"],
        "POLICY_HASH",
        "hash",
        "POLICY_VERSION",
        2,
        "TIMEOUT_MS",
        30_000,
    )
    assert approval.opcode == 0x0246
    assert approval.payload == {
        "id": "approval-1",
        "flow_id": "flow-1",
        "scope": "tenant-a",
        "assignees": ["ops"],
        "policy_hash": "hash",
        "policy_version": 2,
        "timeout_ms": 30_000,
    }

    assert build_protocol_command(
        "FLOW.EFFECT.RESERVE",
        "flow-1",
        "EFFECT_KEY",
        "send-email",
        "EFFECT_TYPE",
        "email.send",
        "OPERATION_DIGEST",
        "digest",
    ) == ProtocolCommand(
        opcode=0x0240,
        payload={
            "id": "flow-1",
            "effect_key": "send-email",
            "effect_type": "email.send",
            "operation_digest": "digest",
        },
        lane_id=1,
    )

    assert build_protocol_command(
        "FLOW.EFFECT.CONFIRM",
        "flow-1",
        "EFFECT_KEY",
        "send-email",
        "LATENCY_MS",
        42,
    ) == ProtocolCommand(
        opcode=0x0241,
        payload={"id": "flow-1", "effect_key": "send-email", "latency_ms": 42},
        lane_id=1,
    )

    assert build_protocol_command(
        "FLOW.BUDGET.RESERVE",
        "tenant-a",
        "AMOUNT",
        10,
        "WINDOW_MS",
        60_000,
        "RESERVATION_ID",
        "budget-res-1",
    ) == ProtocolCommand(
        opcode=0x024D,
        payload={
            "scope": "tenant-a",
            "amount": 10,
            "window_ms": 60_000,
            "reservation_id": "budget-res-1",
        },
        lane_id=1,
    )

    assert build_protocol_command(
        "FLOW.BUDGET.COMMIT",
        "tenant-a",
        "RESERVATION_ID",
        "budget-res-1",
        "ACTUAL_AMOUNT",
        7,
        "USAGE",
        {"tokens": 7},
    ) == ProtocolCommand(
        opcode=0x0257,
        payload={
            "scope": "tenant-a",
            "reservation_id": "budget-res-1",
            "actual_amount": 7,
            "usage": {"tokens": 7},
        },
        lane_id=1,
    )

    assert build_protocol_command(
        "FLOW.BUDGET.RELEASE",
        "tenant-a",
        "RESERVATION_ID",
        "budget-res-unused",
    ) == ProtocolCommand(
        opcode=0x0258,
        payload={"scope": "tenant-a", "reservation_id": "budget-res-unused"},
        lane_id=1,
    )

    assert build_protocol_command(
        "FLOW.LIMIT.LEASE",
        "tenant-a",
        "SHARD_ID",
        1,
        "AMOUNT",
        5,
        "TTL_MS",
        1000,
    ) == ProtocolCommand(
        opcode=0x024F,
        payload={"scope": "tenant-a", "shard_id": 1, "amount": 5, "ttl_ms": 1000},
        lane_id=1,
    )


def test_encodes_protocol_flow_signal_guards_and_transition():
    command = build_protocol_command(
        "FLOW.SIGNAL",
        "f1",
        "SIGNAL",
        "payment_received",
        "PARTITION",
        "tenant-a",
        "IDEMPOTENCY",
        "evt-1",
        "IF_STATE",
        "manual_review",
        "IF_STATE",
        "waiting_payment",
        "TRANSITION_TO",
        "verify_payment",
        "RUN_AT",
        123,
        "NOW",
        100,
        "PRIORITY",
        7,
    )

    assert command == ProtocolCommand(
        opcode=0x020D,
        payload={
            "id": "f1",
            "signal": "payment_received",
            "partition_key": "tenant-a",
            "idempotency_key": "evt-1",
            "if_state": ["manual_review", "waiting_payment"],
            "transition_to": "verify_payment",
            "run_at_ms": 123,
            "now_ms": 100,
            "priority": 7,
        },
        lane_id=1,
    )


def test_encodes_protocol_flow_start_and_step_continue():
    start = build_protocol_command(
        "FLOW.START_AND_CLAIM",
        "f1",
        "TYPE",
        "order",
        "INITIAL_STATE",
        "reserve_inventory",
        "WORKER",
        "worker-1",
        "LEASE_MS",
        30_000,
        "PAYLOAD",
        b"payload",
        "PARTITION",
        "tenant-a",
    )

    assert start == ProtocolCommand(
        opcode=0x0223,
        payload={
            "id": "f1",
            "type": "order",
            "initial_state": "reserve_inventory",
            "worker": "worker-1",
            "lease_ms": 30_000,
            "payload": b"payload",
            "partition_key": "tenant-a",
        },
        lane_id=1,
    )

    step = build_protocol_command(
        "FLOW.STEP_CONTINUE",
        "f1",
        b"lease-1",
        "reserve_inventory",
        "charge_card",
        "FENCING",
        1,
        "LEASE_MS",
        45_000,
        "PAYLOAD",
        b"next",
    )

    assert step == ProtocolCommand(
        opcode=0x0222,
        payload={
            "id": "f1",
            "lease_token": b"lease-1",
            "from_state": "reserve_inventory",
            "to_state": "charge_card",
            "fencing_token": 1,
            "lease_ms": 45_000,
            "payload": b"next",
        },
        lane_id=1,
    )


def test_encodes_protocol_flow_run_steps_many():
    command = build_protocol_command(
        "FLOW.RUN_STEPS_MANY",
        "TYPE",
        "order",
        "STATES",
        ["reserve", "charge", "email"],
        "WORKER",
        "worker-1",
        "LEASE_MS",
        30_000,
        "NOW",
        1234,
        "RESULT",
        b"ok",
        "ITEMS",
        [{"id": "f1", "partition_key": "p1"}, {"id": "f2"}],
    )

    assert command == ProtocolCommand(
        opcode=0x0224,
        payload={
            "type": "order",
            "states": ["reserve", "charge", "email"],
            "worker": "worker-1",
            "lease_ms": 30_000,
            "now_ms": 1234,
            "result": b"ok",
            "items": [{"id": "f1", "partition_key": "p1"}, {"id": "f2"}],
        },
        lane_id=1,
    )


def test_protocol_compacts_batched_step_continue_as_pipeline_payload():
    payloads = _compact_flow_many_payloads_from_raw(
        [
            (
                "FLOW.STEP_CONTINUE",
                "f1",
                b"lease-1",
                "queued",
                "next",
                "FENCING",
                1,
                "LEASE_MS",
                30_000,
                "NOW",
                123,
                "PARTITION",
                "p1",
            ),
            (
                "FLOW.STEP_CONTINUE",
                "f2",
                b"lease-2",
                "queued",
                "next",
                "FENCING",
                2,
                "LEASE_MS",
                30_000,
                "NOW",
                124,
                "PARTITION",
                "p2",
            ),
        ]
    )

    assert payloads is not None
    assert len(payloads) == 1
    opcode, payload, count = payloads[0]

    assert opcode == 0x000E
    assert count == 2
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 6, 2)


def test_protocol_compacts_batched_step_continue_without_generic_builder(monkeypatch):
    def fail_build_protocol_command(*_args, **_kwargs):
        raise AssertionError("generic builder should not be used for compact STEP_CONTINUE")

    monkeypatch.setattr(protocol_module, "build_protocol_command", fail_build_protocol_command)

    payloads = _compact_flow_many_payloads_from_raw(
        [
            (
                "FLOW.STEP_CONTINUE",
                "f1",
                b"lease-1",
                "queued",
                "next",
                "FENCING",
                1,
                "LEASE_MS",
                30_000,
                "NOW",
                123,
                "PARTITION",
                "p1",
            )
        ]
    )

    assert payloads is not None
    assert payloads[0][0] == 0x000E
    assert payloads[0][2] == 1


def test_protocol_compacts_batched_step_continue_with_job_return_mode():
    payloads = _compact_flow_many_payloads_from_raw(
        [
            (
                "FLOW.STEP_CONTINUE",
                "f1",
                b"lease-1",
                "queued",
                "next",
                "FENCING",
                1,
                "LEASE_MS",
                30_000,
                "NOW",
                123,
                "PARTITION",
                "p1",
                "RETURN",
                "JOBS_COMPACT",
            )
        ]
    )

    assert payloads is not None
    opcode, payload, count = payloads[0]
    assert opcode == 0x000E
    assert count == 1
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 33, 1)


def test_protocol_compacts_batched_value_put_as_pipeline_payloads():
    shared_payloads = _compact_flow_many_payloads_from_raw(
        [
            ("FLOW.VALUE.PUT", b"value-1", "NOW", 123),
            ("FLOW.VALUE.PUT", b"value-2", "NOW", 124),
        ]
    )

    assert shared_payloads is not None
    assert len(shared_payloads) == 1
    opcode, payload, count = shared_payloads[0]
    assert opcode == 0x000E
    assert count == 2
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 7, 2)

    owned_payloads = _compact_flow_many_payloads_from_raw(
        [
            (
                "FLOW.VALUE.PUT",
                b"value-1",
                "OWNER_FLOW_ID",
                "flow-1",
                "NAME",
                "reservation",
                "PARTITION",
                "p1",
                "NOW",
                125,
            )
        ]
    )

    assert owned_payloads is not None
    assert len(owned_payloads) == 1
    opcode, payload, count = owned_payloads[0]
    assert opcode == 0x000E
    assert count == 1
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 8, 1)

    owned_ok_payloads = _compact_flow_many_payloads_from_raw(
        [
            (
                "FLOW.VALUE.PUT",
                b"value-1",
                "OWNER_FLOW_ID",
                "flow-1",
                "NAME",
                "reservation",
                "PARTITION",
                "p1",
                "NOW",
                125,
                "RETURN",
                "OK_ON_SUCCESS",
            )
        ]
    )

    assert owned_ok_payloads is not None
    assert len(owned_ok_payloads) == 1
    opcode, payload, count = owned_ok_payloads[0]
    assert opcode == 0x000E
    assert count == 1
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 14, 1)

    shared_ok_payloads = _compact_flow_many_payloads_from_raw(
        [("FLOW.VALUE.PUT", b"value-1", "NOW", 126, "RETURN", "OK_ON_SUCCESS")]
    )

    assert shared_ok_payloads is not None
    assert len(shared_ok_payloads) == 1
    opcode, payload, count = shared_ok_payloads[0]
    assert opcode == 0x000E
    assert count == 1
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 15, 1)


def test_protocol_compacts_batched_value_put_without_generic_builder(monkeypatch):
    def fail_build_protocol_command(*_args, **_kwargs):
        raise AssertionError("generic builder should not be used for compact VALUE.PUT")

    monkeypatch.setattr(protocol_module, "build_protocol_command", fail_build_protocol_command)

    payloads = _compact_flow_many_payloads_from_raw(
        [
            ("FLOW.VALUE.PUT", b"value-1", "NOW", 123),
            ("FLOW.VALUE.PUT", b"value-shared-ok", "NOW", 124, "RETURN", "OK_ON_SUCCESS"),
            (
                "FLOW.VALUE.PUT",
                b"value-2",
                "OWNER_FLOW_ID",
                "flow-1",
                "NAME",
                "reservation",
                "PARTITION",
                "p1",
                "NOW",
                124,
                "RETURN",
                "OK_ON_SUCCESS",
            ),
        ]
    )

    assert payloads is not None
    assert len(payloads) == 3
    assert payloads[0][1][:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 7, 1)
    assert payloads[1][1][:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 15, 1)
    assert payloads[2][1][:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 14, 1)


def test_protocol_compacts_batched_flow_get_as_pipeline_payload():
    payload = _compact_pipeline_payload_from_raw(
        [("FLOW.GET", "f1"), ("FLOW.GET", "f2")],
        values_only=True,
    )

    assert payload is not None
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 9, 2)


def test_protocol_compacts_partitioned_batched_flow_get_as_pipeline_payload():
    payload = _compact_pipeline_payload_from_raw(
        [
            ("FLOW.GET", "f1", "PARTITION", "p1"),
            ("FLOW.GET", "f2", "PARTITION", "p2"),
        ],
        values_only=True,
    )

    assert payload is not None
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 16, 2)
    assert b"p1" in payload
    assert b"p2" in payload


def test_protocol_compacts_batched_flow_get_meta_as_pipeline_payload():
    payload = _compact_pipeline_payload_from_raw(
        [
            ("FLOW.GET", "f1", "PARTITION", "p1", "RETURN", "META"),
            ("FLOW.GET", "f2", "PARTITION", "p2", "RETURN", "META"),
        ],
        values_only=True,
    )

    assert payload is not None
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 17, 2)
    assert b"p1" in payload
    assert b"p2" in payload


def test_protocol_compacts_batched_flow_history_as_pipeline_payload():
    payload = _compact_pipeline_payload_from_raw(
        [
            (
                "FLOW.HISTORY",
                "f1",
                "COUNT",
                10,
                "PARTITION",
                "p1",
                "INCLUDE_COLD",
                False,
                "CONSISTENT_PROJECTION",
                False,
            ),
            (
                "FLOW.HISTORY",
                "f2",
                "COUNT",
                10,
                "PARTITION",
                "p2",
                "INCLUDE_COLD",
                False,
                "CONSISTENT_PROJECTION",
                False,
            ),
        ],
        values_only=True,
    )

    assert payload is not None
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 10, 2)


def test_protocol_compacts_batched_flow_signal_as_pipeline_payload():
    payload = _compact_pipeline_payload_from_raw(
        [
            (
                "FLOW.SIGNAL",
                "f1",
                "SIGNAL",
                "bench_signal",
                "PARTITION",
                "p1",
                "IF_STATE",
                "queued",
                "TRANSITION_TO",
                "next",
                "NOW",
                123,
            ),
            (
                "FLOW.SIGNAL",
                "f2",
                "SIGNAL",
                "bench_signal",
                "PARTITION",
                "p2",
                "IF_STATE",
                "queued",
                "TRANSITION_TO",
                "next",
                "NOW",
                124,
            ),
        ],
        values_only=True,
    )

    assert payload is not None
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 11, 2)


def test_protocol_compacts_batched_flow_start_and_claim_as_pipeline_payload():
    payloads = _compact_flow_many_payloads_from_raw(
        [
            (
                "FLOW.START_AND_CLAIM",
                "f1",
                "TYPE",
                "order",
                "INITIAL_STATE",
                "reserve_inventory",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                30_000,
                "NOW",
                123,
                "PARTITION",
                "tenant-a",
                "PAYLOAD",
                b"payload-1",
            ),
            (
                "FLOW.START_AND_CLAIM",
                "f2",
                "TYPE",
                "order",
                "INITIAL_STATE",
                "reserve_inventory",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                30_000,
                "NOW",
                124,
                "PARTITION",
                "tenant-b",
                "PAYLOAD",
                b"payload-2",
            ),
        ]
    )

    assert payloads is not None
    assert len(payloads) == 1
    opcode, payload, count = payloads[0]
    assert opcode == 0x000E
    assert count == 2
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 12, 2)


def test_protocol_compacts_batched_flow_start_and_claim_job_only_as_pipeline_payload():
    payloads = _compact_flow_many_payloads_from_raw(
        [
            (
                "FLOW.START_AND_CLAIM",
                "f1",
                "TYPE",
                "order",
                "INITIAL_STATE",
                "reserve_inventory",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                30_000,
                "NOW",
                123,
                "PARTITION",
                "tenant-a",
                "PAYLOAD",
                b"payload-1",
                "RETURN",
                "JOBS_COMPACT",
            ),
            (
                "FLOW.START_AND_CLAIM",
                "f2",
                "TYPE",
                "order",
                "INITIAL_STATE",
                "reserve_inventory",
                "WORKER",
                "worker-1",
                "LEASE_MS",
                30_000,
                "NOW",
                124,
                "PARTITION",
                "tenant-b",
                "PAYLOAD",
                b"payload-2",
                "RETURN",
                "JOBS_COMPACT",
            ),
        ]
    )

    assert payloads is not None
    assert len(payloads) == 1
    opcode, payload, count = payloads[0]
    assert opcode == 0x000E
    assert count == 2
    assert payload[:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 13, 2)


def test_protocol_decodes_compact_pipeline_job_items():
    body = (
        bytes([_COMPACT_PIPELINE_RESPONSE])
        + struct.pack(">I", 1)
        + b"\x00\x04"
        + struct.pack(">I", 2)
        + b"f1"
        + struct.pack(">I", 6)
        + b"tenant"
        + struct.pack(">I", 5)
        + b"lease"
        + struct.pack(">q", 7)
    )

    assert _try_fast_response_value(0x000E, body) == [["ok", [b"f1", b"tenant", b"lease", 7]]]


def test_flow_client_start_and_step_continue_use_protocol_command_shape():
    class CaptureExecutor:
        def __init__(self):
            self.calls = []

        def execute_command(self, *args):
            self.calls.append(args)
            return {
                b"id": b"f1",
                b"type": b"order",
                b"state": b"running",
                b"run_state": b"reserve_inventory",
                b"lease_token": b"lease",
                b"fencing_token": 1,
                b"partition_key": b"tenant-a",
            }

    executor = CaptureExecutor()
    client = FlowClient(executor)

    started = client.start_and_claim(
        "f1",
        type="order",
        initial_state="reserve_inventory",
        worker="worker-1",
        partition_key="tenant-a",
        lease_ms=30_000,
        payload=b"payload",
        now_ms=100,
    )
    continued = client.step_continue(
        "f1",
        lease_token=started.lease_token,
        from_state="reserve_inventory",
        to_state="charge_card",
        fencing_token=started.fencing_token,
        lease_ms=45_000,
        now_ms=101,
    )

    assert started.state == "running"
    assert continued.state == "running"
    assert build_protocol_command(*executor.calls[0]).opcode == 0x0223
    assert build_protocol_command(*executor.calls[1]).opcode == 0x0222


def test_flow_client_step_continue_can_return_compact_job():
    class CaptureExecutor:
        def __init__(self):
            self.calls = []

        def execute_command(self, *args):
            self.calls.append(args)
            return [b"f1", b"tenant-a", b"lease-2", 2]

    executor = CaptureExecutor()
    client = FlowClient(executor)

    job = client.step_continue(
        "f1",
        lease_token=b"lease-1",
        from_state="reserve_inventory",
        to_state="charge_card",
        fencing_token=1,
        partition_key="tenant-a",
        return_job=True,
        now_ms=101,
    )

    assert job.id == "f1"
    assert job.partition_key == "tenant-a"
    assert job.lease_token == b"lease-2"
    assert job.fencing_token == 2
    assert "RETURN" in executor.calls[0]
    assert "JOBS_COMPACT" in executor.calls[0]


def test_encodes_protocol_flow_create_many_auto_and_complete_many_mixed():
    create = build_protocol_command(
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

    complete = build_protocol_command(
        "FLOW.COMPLETE_MANY",
        "MIXED",
        "NOW",
        200,
        "INDEPENDENT",
        "true",
        "TERMINAL_LOCAL_ONLY",
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
    assert complete.payload[13] == 3


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


def test_encodes_protocol_flow_claim_jobs_and_high_level_many_calls():
    claim = build_protocol_command(
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
            ClaimedFlow("f1", b"lease1", 1, partition_key="p1"),
            ClaimedFlow("f2", b"lease2", 2, partition_key="p2"),
        ],
        now_ms=200,
        independent=True,
    )

    assert build_protocol_command(*executor.calls[0]).opcode == 0x020F
    assert build_protocol_command(*executor.calls[1]).opcode == 0x0210


def test_protocol_complete_jobs_marks_terminal_local_only_for_partitioned_jobs():
    client = FlowClient(object())
    args = client._complete_jobs_args(
        [
            ClaimedFlow("f1", b"lease1", 1, partition_key="p1"),
            ClaimedFlow("f2", b"lease2", 2, partition_key="p2"),
        ],
        now_ms=200,
        independent=False,
    )

    command = build_protocol_command(*args)

    assert command.opcode == 0x0210
    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)
    assert command.payload[0] == 0x92
    assert command.payload[13] == 4


def test_encodes_protocol_spawn_children_and_retention_cleanup():
    spawn = build_protocol_command(
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

    assert spawn == ProtocolCommand(
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

    cleanup = build_protocol_command("FLOW.RETENTION_CLEANUP", "LIMIT", 100, "NOW", 1234)
    assert cleanup == ProtocolCommand(
        opcode=0x0221,
        payload={"limit": 100, "now_ms": 1234},
        lane_id=1,
    )


def test_high_level_spawn_children_uses_protocol_command():
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

    command = build_protocol_command(*executor.calls[0])
    assert command.opcode == 0x0220
    assert command.payload["children"] == [
        {"id": "child-1", "type": "email", "payload": b"payload"}
    ]


def test_protocol_pipeline_uses_batch_and_returns_item_values():
    class FakeProtocolAdapter(ProtocolAdapter):
        def __init__(self):
            pass

        def execute_batch(self, commands):
            return [command[0] for command in commands]

    pipe = FakeProtocolAdapter().pipeline()
    pipe.execute_command("SET", "k", "v")
    pipe.execute_command("GET", "k")

    assert pipe.execute() == ["SET", "GET"]
