from __future__ import annotations

import asyncio
import gc
import random
import struct
import threading
import time
import tracemalloc
import zlib
from concurrent.futures import Future
from typing import Any

import pytest

import ferricstore.protocol as protocol_module
from ferricstore.errors import FerricStoreError
from ferricstore.protocol import (
    AsyncProtocolAdapter,
    AsyncProtocolAdapterPool,
    ProtocolAdapter,
    ProtocolAdapterPool,
    ProtocolCommand,
    ProtocolResponse,
    TopologyProtocolAdapterPool,
)
from ferricstore.protocol_codec import encode_value
from ferricstore.protocol_framing import decompress_response
from ferricstore.topology_lifecycle import SyncSingleFlight


def _single_shard_topology(host: str = "leader.local", port: int = 6391) -> dict[str, Any]:
    return {
        "route_epoch": 1,
        "shard_count": 1,
        "ranges": [
            {
                "first_slot": 0,
                "last_slot": 1023,
                "shard": 0,
                "lane_id": 1,
                "endpoint": {
                    "node": f"{host}@cluster",
                    "host": host,
                    "native_port": port,
                },
            }
        ],
    }


@pytest.mark.parametrize("limit", [None, 1024])
def test_response_decompression_normalizes_corrupt_zlib_errors(limit: int | None) -> None:
    with pytest.raises(FerricStoreError, match="invalid compressed data") as exc_info:
        decompress_response(b"not-a-zlib-stream", limit)

    assert isinstance(exc_info.value.raw, zlib.error)


def test_generic_map_encoding_uses_near_linear_peak_memory() -> None:
    value = {f"key-{index}": b"x" * (128 * 1024) for index in range(24)}
    gc.collect()
    tracemalloc.start()
    try:
        encoded = encode_value(value)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < len(encoded) * 1.7


def test_pending_request_budget_stops_generic_encoding_before_late_values(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class Socket:
        def sendall(self, _frame: bytes) -> None:
            raise AssertionError("an over-budget request must not be written")

        def shutdown(self, *_args: Any) -> None:
            return None

        def close(self) -> None:
            return None

    class MustNotEncode:
        def __str__(self) -> str:
            raise AssertionError("encoding must stop as soon as the byte budget is exceeded")

    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_pending_request_bytes=64,
    )
    adapter._sock = Socket()  # type: ignore[assignment]
    adapter._ensure_connected = lambda: None  # type: ignore[method-assign]
    try:
        with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
            adapter._submit_request_on_lane(
                protocol_module._OP_SET,
                1,
                {"large": b"x" * 128, "late": MustNotEncode()},
            )
    finally:
        adapter.close()


def test_streamed_zlib_request_body_round_trips_generic_codec_values() -> None:
    payload = {
        "name": "order",
        "items": [b"one", b"two", {"count": 3, "ready": True}],
    }

    body, compressed = protocol_module._encode_request_body(
        payload,
        compression="zlib",
        max_body_bytes=None,
        pending_limit=None,
    )

    assert compressed is True
    assert zlib.decompress(body) == encode_value(payload)


def test_compressed_request_budget_stops_before_late_payload_values(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class Socket:
        def sendall(self, _frame: bytes) -> None:
            raise AssertionError("an over-budget request must not be written")

        def shutdown(self, *_args: Any) -> None:
            return None

        def close(self) -> None:
            return None

    class MustNotEncode:
        def __str__(self) -> str:
            raise AssertionError("compressed encoding must stop at its wire-byte budget")

    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        compression="zlib",
        max_pending_request_bytes=128,
    )
    adapter._sock = Socket()  # type: ignore[assignment]
    adapter._ensure_connected = lambda: None  # type: ignore[method-assign]
    try:
        with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
            adapter._submit_request_on_lane(
                protocol_module._OP_SET,
                1,
                {
                    "large": random.Random(0).randbytes(256 * 1024),
                    "late": MustNotEncode(),
                },
            )
    finally:
        adapter.close()


def test_sync_response_identity_is_validated_before_body_decode(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    adapter._ensure_connected = lambda: None  # type: ignore[method-assign]
    adapter._send = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    request_id, future = adapter._submit_request_on_lane(
        protocol_module._OP_GET,
        3,
        {"key": "one"},
    )
    header = protocol_module._HEADER.pack(
        protocol_module._MAGIC,
        protocol_module._RESPONSE_VERSION,
        0,
        4,
        protocol_module._OP_GET,
        request_id,
        1_000_000,
    )
    reads = iter([header])
    adapter._recv_exact = lambda _size, _sock=None: next(reads)  # type: ignore[method-assign]

    try:
        with pytest.raises(FerricStoreError, match="response identity mismatch"):
            adapter._recv_response()
        assert future.done() is False
    finally:
        adapter._discard_pending_request(request_id, expected_future=future)
        adapter.close()


def test_async_response_identity_is_validated_before_body_decode() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(timeout=None, heartbeat_interval=None)
        sent = asyncio.Event()

        async def connected() -> None:
            return None

        async def send(*_args: Any, **_kwargs: Any) -> None:
            sent.set()

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        adapter._send = send  # type: ignore[method-assign]
        request = asyncio.create_task(
            adapter._request_without_timeout(
                protocol_module._OP_GET,
                3,
                {"key": "one"},
                exact_lane=True,
            )
        )
        await sent.wait()
        request_id = next(iter(adapter._pending))
        header = protocol_module._HEADER.pack(
            protocol_module._MAGIC,
            protocol_module._RESPONSE_VERSION,
            0,
            3,
            protocol_module._OP_SET,
            request_id,
            1_000_000,
        )
        reads = iter([header])

        async def recv_exact(_size: int, _reader: Any = None) -> bytes:
            return next(reads)

        adapter._recv_exact = recv_exact  # type: ignore[method-assign]
        try:
            with pytest.raises(FerricStoreError, match="response identity mismatch"):
                await adapter._recv_response()
            assert request.done() is False
        finally:
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
            await adapter.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    ("commands", "mode"),
    [
        (
            [
                ("FLOW.HISTORY", "f1", "COUNT", 10, "PARTITION", "p1"),
                ("FLOW.HISTORY", "f2", "COUNT", 10, "PARTITION", "p2"),
            ],
            10,
        ),
        (
            [
                (
                    "FLOW.SIGNAL",
                    "f1",
                    "SIGNAL",
                    "ready",
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
                    "ready",
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
            11,
        ),
    ],
)
def test_submit_commands_uses_authoritative_raw_compact_pipeline_registry(
    commands: list[tuple[Any, ...]],
    mode: int,
) -> None:
    adapter = object.__new__(ProtocolAdapter)
    captured: dict[str, Any] = {}

    def submit(
        opcode: int,
        lane_id: int,
        payload: Any,
        flags: int = 0,
        **_kwargs: Any,
    ) -> tuple[int, Future[ProtocolResponse]]:
        captured.update(opcode=opcode, lane_id=lane_id, payload=payload, flags=flags)
        source: Future[ProtocolResponse] = Future()
        source.set_result(
            ProtocolResponse(
                lane_id,
                opcode,
                1,
                flags,
                0,
                [[b"ok", b"one"], [b"ok", b"two"]],
            )
        )
        return 1, source

    adapter._submit_request = submit  # type: ignore[method-assign]
    futures = adapter.submit_commands(commands)

    assert captured["opcode"] == protocol_module._OP_PIPELINE
    assert captured["flags"] == protocol_module._FLAG_CUSTOM_PAYLOAD
    assert captured["payload"][:6] == struct.pack(
        ">BBI",
        protocol_module._COMPACT_PIPELINE_REQUEST,
        mode,
        2,
    )
    assert [future.result() for future in futures] == [b"one", b"two"]


def test_submit_commands_compacts_history_before_structured_command_allocation(
    monkeypatch: Any,
) -> None:
    adapter = object.__new__(ProtocolAdapter)
    captured: dict[str, Any] = {}

    def submit(
        opcode: int,
        lane_id: int,
        payload: Any,
        flags: int = 0,
        **_kwargs: Any,
    ) -> tuple[int, Future[ProtocolResponse]]:
        captured.update(payload=payload, flags=flags)
        source: Future[ProtocolResponse] = Future()
        source.set_result(ProtocolResponse(lane_id, opcode, 1, flags, 0, [[b"ok", b"history"]]))
        return 1, source

    adapter._submit_request = submit  # type: ignore[method-assign]
    monkeypatch.setattr(
        protocol_module,
        "build_protocol_command",
        lambda *_args, **_kwargs: pytest.fail(
            "the raw compact path must not allocate structured commands"
        ),
    )

    futures = adapter.submit_commands([("FLOW.HISTORY", "f1", "COUNT", 10, "PARTITION", "p1")])

    assert captured["flags"] == protocol_module._FLAG_CUSTOM_PAYLOAD
    assert futures[0].result() == b"history"


def test_sync_protocol_reader_thread_start_failure_closes_published_socket(
    monkeypatch: Any,
) -> None:
    class Socket:
        def __init__(self) -> None:
            self.closed = False

        def setsockopt(self, *_args: Any) -> None:
            return None

        def settimeout(self, _timeout: float | None) -> None:
            return None

        def shutdown(self, *_args: Any) -> None:
            return None

        def close(self) -> None:
            self.closed = True

    sock = Socket()
    original_start = threading.Thread.start

    def fail_reader_start(thread: threading.Thread) -> None:
        target = getattr(thread, "_target", None)
        if getattr(target, "__name__", None) == "_reader_loop":
            raise RuntimeError("reader thread start failed")
        original_start(thread)

    monkeypatch.setattr(protocol_module.socket, "create_connection", lambda *_args, **_kwargs: sock)
    monkeypatch.setattr(threading.Thread, "start", fail_reader_start)

    with pytest.raises(RuntimeError, match="reader thread start failed"):
        ProtocolAdapter(timeout=None, heartbeat_interval=None)

    assert sock.closed is True


def test_sync_protocol_heartbeat_thread_start_failure_rolls_back_state(
    monkeypatch: Any,
) -> None:
    adapter = object.__new__(ProtocolAdapter)
    adapter.heartbeat_interval = 1.0
    adapter._heartbeat_stop = None
    adapter._heartbeat_thread = None
    adapter._sock = object()  # type: ignore[assignment]
    original_start = threading.Thread.start

    def fail_heartbeat_start(thread: threading.Thread) -> None:
        if thread.name == "ferricstore-protocol-heartbeat":
            raise RuntimeError("heartbeat thread start failed")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_heartbeat_start)

    with pytest.raises(RuntimeError, match="heartbeat thread start failed"):
        adapter._start_heartbeat()

    assert adapter._heartbeat_thread is None
    assert adapter._heartbeat_stop is None


def test_sync_singleflight_never_overlaps_refresh_operations() -> None:
    singleflight = SyncSingleFlight[int]()
    callers = 32
    start = threading.Barrier(callers + 1)
    state_lock = threading.Lock()
    active = 0
    maximum_active = 0

    def refresh() -> int:
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        time.sleep(0)
        with state_lock:
            active -= 1
        return 1

    def call_repeatedly() -> None:
        start.wait()
        for _ in range(500):
            assert singleflight.run(refresh) == 1

    threads = [threading.Thread(target=call_repeatedly) for _ in range(callers)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(5)

    assert not any(thread.is_alive() for thread in threads)
    assert maximum_active == 1


def test_sync_protocol_request_timeout_bounds_write_lock_wait(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=0.02, heartbeat_interval=None)
    monkeypatch.setattr(adapter, "_ensure_connected", lambda: None)
    monkeypatch.setattr(adapter, "_send", lambda *_args, **_kwargs: None)

    finished = threading.Event()
    outcome: dict[str, Any] = {}
    adapter._lock.acquire()
    started = time.monotonic()

    def request() -> None:
        try:
            adapter._request(0x0101, 1, {"key": "a"})
        except BaseException as exc:
            outcome["error"] = exc
        finally:
            outcome["elapsed"] = time.monotonic() - started
            finished.set()

    thread = threading.Thread(target=request)
    thread.start()
    completed_in_budget = finished.wait(0.5)
    adapter._lock.release()
    thread.join(1)
    adapter.close()

    assert completed_in_budget is True
    assert isinstance(outcome["error"], FerricStoreError)
    assert "timed out" in str(outcome["error"])
    assert outcome["elapsed"] < 0.25


def test_async_protocol_request_timeout_bounds_connect_lock_wait() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(timeout=0.02, heartbeat_interval=None)
        await adapter._connect_lock.acquire()
        started = time.monotonic()
        task = asyncio.create_task(adapter.execute_command("PING"))
        error: BaseException | None = None
        try:
            await asyncio.wait_for(task, timeout=0.5)
        except BaseException as exc:
            error = exc
        finally:
            adapter._connect_lock.release()

        assert isinstance(error, FerricStoreError)
        assert "timed out" in str(error)
        assert time.monotonic() - started < 0.25

    asyncio.run(run())


def test_sync_protocol_event_waiters_loop_until_each_has_an_event(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(heartbeat_interval=None)
    results: list[str | None] = []
    results_lock = threading.Lock()

    def wait() -> None:
        value = adapter.wait_event(timeout=1)
        with results_lock:
            results.append(value)

    threads = [threading.Thread(target=wait) for _ in range(2)]
    for thread in threads:
        thread.start()
    time.sleep(0.02)

    adapter._enqueue_event("first")
    time.sleep(0.02)
    with results_lock:
        first_count = len(results)
    adapter._enqueue_event("second")
    for thread in threads:
        thread.join(1)
    adapter.close()

    assert first_count == 1
    assert sorted(results) == ["first", "second"]


def test_async_protocol_event_waiters_loop_until_each_has_an_event() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        waiters = [asyncio.create_task(adapter.wait_event(timeout=1)) for _ in range(2)]
        await asyncio.sleep(0)

        await adapter._enqueue_event("first")
        done, _pending = await asyncio.wait(
            waiters,
            timeout=1,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert len(done) == 1
        assert sum(waiter.done() for waiter in waiters) == 1

        await adapter._enqueue_event("second")
        assert sorted(await asyncio.gather(*waiters)) == ["first", "second"]
        await adapter.close()

    asyncio.run(run())


def test_async_connect_failure_is_published_to_event_waiters(monkeypatch: Any) -> None:
    async def fail_connect(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("connection refused")

    async def run() -> None:
        monkeypatch.setattr(protocol_module.asyncio, "open_connection", fail_connect)
        adapter = AsyncProtocolAdapter(timeout=0.02, heartbeat_interval=None)
        adapter._event_error = FerricStoreError("previous connection failed")

        with pytest.raises(OSError, match="connection refused"):
            await adapter._ensure_connected()
        with pytest.raises(OSError, match="connection refused"):
            await adapter.wait_event(timeout=0.01)

    asyncio.run(run())


def test_sync_protocol_pool_close_retries_only_failed_adapters() -> None:
    class Adapter:
        def __init__(self, *, fail_once: bool = False) -> None:
            self.fail_once = fail_once
            self.close_calls = 0

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def close(self) -> None:
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("transient close failure")

    failed = Adapter(fail_once=True)
    succeeded = Adapter()
    pool = ProtocolAdapterPool([failed, succeeded])

    with pytest.raises(RuntimeError, match="transient close failure"):
        pool.close()
    pool.close()

    assert [failed.close_calls, succeeded.close_calls] == [2, 1]


def test_async_protocol_pool_close_retries_only_failed_adapters() -> None:
    class Adapter:
        def __init__(self, *, fail_once: bool = False) -> None:
            self.fail_once = fail_once
            self.close_calls = 0

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        async def close(self) -> None:
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("transient close failure")

    async def run() -> None:
        failed = Adapter(fail_once=True)
        succeeded = Adapter()
        pool = AsyncProtocolAdapterPool([failed, succeeded])

        with pytest.raises(RuntimeError, match="transient close failure"):
            await pool.close()
        await pool.close()

        assert [failed.close_calls, succeeded.close_calls] == [2, 1]

    asyncio.run(run())


def test_sync_write_failure_retires_transport_and_fails_all_pending(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FailingSocket:
        def __init__(self) -> None:
            self.closed = False

        def sendall(self, _data: bytes) -> None:
            raise OSError("partial write")

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    sock = FailingSocket()
    adapter._sock = sock  # type: ignore[assignment]
    unrelated: Future[Any] = Future()
    adapter._pending[99] = unrelated

    with pytest.raises(OSError, match="partial write"):
        adapter._submit_request(0x0101, 1, {"key": "a"})

    assert adapter._sock is None
    assert sock.closed is True
    assert unrelated.done() is True
    assert isinstance(unrelated.exception(), FerricStoreError)


def test_sync_old_transport_close_does_not_fail_replacement_requests(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self) -> None:
            self.closed = False

        def setsockopt(self, *_args: Any) -> None:
            pass

        def settimeout(self, _timeout: float | None) -> None:
            pass

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    old_socket = FakeSocket()
    new_socket = FakeSocket()
    adapter._sock = old_socket  # type: ignore[assignment]
    close_paused = threading.Event()
    allow_close = threading.Event()
    close_errors: list[BaseException] = []

    def pause_old_close() -> None:
        close_paused.set()
        assert allow_close.wait(timeout=1)

    adapter.add_event_listener(pause_old_close)

    def close_old_transport() -> None:
        try:
            adapter._close_transport(
                FerricStoreError("old transport failed"),
                expected_sock=old_socket,  # type: ignore[arg-type]
            )
        except BaseException as exc:
            close_errors.append(exc)

    close_thread = threading.Thread(target=close_old_transport)
    close_thread.start()
    assert close_paused.wait(timeout=1)

    monkeypatch.setattr(protocol_module.socket, "create_connection", lambda *_a, **_k: new_socket)
    monkeypatch.setattr(adapter, "_reader_loop", lambda *_args: None)
    monkeypatch.setattr(adapter, "_request", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(adapter, "_response_value", lambda _response: None)
    adapter._connect_once()
    monkeypatch.setattr(adapter, "_send", lambda *_args, **_kwargs: None)
    _request_id, replacement_future = adapter._submit_request(0x0101, 1, {"key": "new"})

    allow_close.set()
    close_thread.join(timeout=1)
    replacement_was_failed = replacement_future.done()
    adapter.remove_event_listener(pause_old_close)
    adapter.close()

    assert close_errors == []
    assert close_thread.is_alive() is False
    assert replacement_was_failed is False
    assert old_socket.closed is True


def test_sync_pending_completion_race_cannot_abort_socket_cleanup(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self) -> None:
            self.closed = False

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    completion_started = threading.Event()
    completion_finished = threading.Event()

    class RacingFuture(Future[Any]):
        def set_exception(self, exception: BaseException) -> None:
            completion_started.set()
            assert completion_finished.wait(timeout=1)
            super().set_exception(exception)

    adapter = ProtocolAdapter(heartbeat_interval=None)
    sock = FakeSocket()
    adapter._sock = sock  # type: ignore[assignment]
    pending = RacingFuture()
    adapter._pending[1] = pending

    def complete_response() -> None:
        assert completion_started.wait(timeout=1)
        pending.set_result("response")
        completion_finished.set()

    completion_thread = threading.Thread(target=complete_response)
    completion_thread.start()
    adapter._close_transport(FerricStoreError("reader failed"))
    completion_thread.join(timeout=1)

    assert completion_thread.is_alive() is False
    assert pending.result() == "response"
    assert sock.closed is True


def test_sync_multiframe_write_failure_retires_transport(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    monkeypatch.setattr(protocol_module, "_pipeline_frame_supported", lambda _commands: False)

    class FailingSocket:
        def __init__(self) -> None:
            self.closed = False

        def sendall(self, _data: bytes) -> None:
            raise OSError("batch write failed")

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    sock = FailingSocket()
    adapter._sock = sock  # type: ignore[assignment]

    with pytest.raises(OSError, match="batch write failed"):
        adapter.submit_commands([("GET", "a"), ("GET", "b")])

    assert adapter._sock is None
    assert sock.closed is True
    assert adapter._pending == {}


def test_sync_write_preserves_primary_error_when_timeout_restore_fails(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FailingSocket:
        def __init__(self) -> None:
            self.settimeout_calls = 0
            self.closed = False

        def gettimeout(self) -> None:
            return None

        def settimeout(self, _value: float | None) -> None:
            self.settimeout_calls += 1
            if self.settimeout_calls == 2:
                raise RuntimeError("timeout restore failed")

        def sendall(self, _data: bytes) -> None:
            raise OSError("partial write")

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = ProtocolAdapter(timeout=0.1, heartbeat_interval=None)
    sock = FailingSocket()
    adapter._sock = sock  # type: ignore[assignment]

    with pytest.raises(OSError, match="partial write") as raised:
        adapter._submit_request(0x0101, 1, {"key": "a"})

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "timeout restore failed" in str(raised.value.__cause__)
    assert adapter._sock is None
    assert sock.closed is True


def test_async_drain_failure_retires_transport_and_fails_all_pending() -> None:
    class FailingWriter:
        def __init__(self) -> None:
            self.closed = False

        def is_closing(self) -> bool:
            return self.closed

        def write(self, _part: bytes) -> None:
            pass

        async def drain(self) -> None:
            raise OSError("drain failed")

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            pass

    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=None,
            heartbeat_interval=None,
            write_drain_bytes=0,
        )
        writer = FailingWriter()
        adapter._writer = writer  # type: ignore[assignment]
        unrelated = asyncio.get_running_loop().create_future()
        adapter._pending[99] = unrelated

        with pytest.raises(OSError, match="drain failed"):
            await adapter._request_without_timeout(0x0101, 1, {"key": "a"})

        assert adapter._writer is None
        assert writer.closed is True
        assert unrelated.done() is True
        with pytest.raises(FerricStoreError):
            await unrelated

    asyncio.run(run())


def test_sync_close_failure_still_publishes_state_and_retains_socket_for_retry(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FailOnceSocket:
        def __init__(self) -> None:
            self.close_calls = 0

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("transient socket close failure")

    adapter = ProtocolAdapter(heartbeat_interval=None)
    sock = FailOnceSocket()
    adapter._sock = sock  # type: ignore[assignment]
    pending: Future[Any] = Future()
    adapter._pending[1] = pending

    with pytest.raises(RuntimeError, match="transient socket close failure"):
        adapter._close_transport(FerricStoreError("reader failed"))

    assert adapter._sock is None
    assert pending.done() is True
    with pytest.raises(FerricStoreError, match="reader failed"):
        adapter.wait_event(timeout=0)

    adapter.close()
    assert sock.close_calls == 2


def test_async_close_failure_still_publishes_state_and_retains_writer_for_retry() -> None:
    class FailOnceWriter:
        def __init__(self) -> None:
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("transient writer close failure")

        async def wait_closed(self) -> None:
            pass

    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        writer = FailOnceWriter()
        adapter._writer = writer  # type: ignore[assignment]
        pending = asyncio.get_running_loop().create_future()
        adapter._pending[1] = pending

        with pytest.raises(RuntimeError, match="transient writer close failure"):
            await adapter.close()

        assert adapter._writer is None
        with pytest.raises(FerricStoreError, match="closed"):
            await pending
        with pytest.raises(FerricStoreError, match="closed"):
            await adapter.wait_event(timeout=0)

        await adapter.close()
        assert writer.close_calls == 2

    asyncio.run(run())


def test_async_close_notifies_event_waiters_before_writer_cleanup_finishes() -> None:
    class BlockingWriter:
        def __init__(self) -> None:
            self.close_called = asyncio.Event()
            self.release = asyncio.Event()

        def close(self) -> None:
            self.close_called.set()

        async def wait_closed(self) -> None:
            await self.release.wait()

    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        writer = BlockingWriter()
        adapter._writer = writer  # type: ignore[assignment]
        event_waiter = asyncio.create_task(adapter.wait_event())
        await asyncio.sleep(0)
        close_task = asyncio.create_task(adapter.close())
        await asyncio.wait_for(writer.close_called.wait(), timeout=1)

        try:
            with pytest.raises(FerricStoreError, match="closed"):
                await asyncio.wait_for(event_waiter, timeout=0.1)
            with pytest.raises(FerricStoreError, match="closed"):
                await adapter.wait_event(timeout=0)
            assert close_task.done() is False
        finally:
            writer.release.set()
            await close_task

    asyncio.run(run())


def test_async_invalidate_cancellation_cannot_interrupt_notification_then_cleanup() -> None:
    class Writer:
        def __init__(self) -> None:
            self.closed = False
            self.finished = asyncio.Event()

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            self.finished.set()

    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        writer = Writer()
        adapter._writer = writer  # type: ignore[assignment]
        notification_started = asyncio.Event()
        publish_event_error = adapter._publish_event_error

        async def publish(error: BaseException) -> None:
            notification_started.set()
            await publish_event_error(error)

        adapter._publish_event_error = publish  # type: ignore[method-assign]
        await adapter._events_cv.acquire()
        invalidate_task = asyncio.create_task(adapter.invalidate())
        try:
            await asyncio.wait_for(notification_started.wait(), timeout=1)
            assert adapter._writer is None
            invalidate_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await invalidate_task
        finally:
            adapter._events_cv.release()

        await asyncio.wait_for(writer.finished.wait(), timeout=1)
        assert writer.closed is True

    asyncio.run(run())


def test_sync_pool_event_poll_reserves_adapter_against_session_lease() -> None:
    poll_entered = threading.Event()
    release_poll = threading.Event()

    class Adapter:
        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def wait_event(self, *, timeout: float | None = None) -> str:
            assert timeout == 0.0
            poll_entered.set()
            assert release_poll.wait(timeout=1)
            return "event"

        def close(self) -> None:
            pass

    pool = ProtocolAdapterPool([Adapter()])  # type: ignore[list-item]
    event_result: list[Any] = []
    session_result: list[Any] = []
    session_acquired = threading.Event()

    poll_thread = threading.Thread(target=lambda: event_result.append(pool.wait_event(timeout=1)))

    def acquire_session() -> None:
        session_result.append(pool.acquire_session())
        session_acquired.set()

    session_thread = threading.Thread(target=acquire_session)
    poll_thread.start()
    assert poll_entered.wait(timeout=1)
    session_thread.start()
    leased_during_poll = session_acquired.wait(timeout=0.05)
    release_poll.set()
    poll_thread.join(timeout=1)
    session_thread.join(timeout=1)
    session_result[0].close()
    pool.close()

    assert leased_during_poll is False
    assert event_result == ["event"]


def test_async_pool_event_poll_reserves_adapter_against_session_lease() -> None:
    async def run() -> None:
        poll_entered = asyncio.Event()
        release_poll = asyncio.Event()

        class Adapter:
            def add_event_listener(self, _listener: Any) -> None:
                pass

            def remove_event_listener(self, _listener: Any) -> None:
                pass

            async def wait_event(self, *, timeout: float | None = None) -> str:
                assert timeout == 0.0
                poll_entered.set()
                await release_poll.wait()
                return "event"

            async def close(self) -> None:
                pass

        pool = AsyncProtocolAdapterPool([Adapter()])  # type: ignore[list-item]
        poll_task = asyncio.create_task(pool.wait_event(timeout=1))
        await asyncio.wait_for(poll_entered.wait(), timeout=1)
        session_task = asyncio.create_task(pool.acquire_session())
        await asyncio.sleep(0)
        leased_during_poll = session_task.done()
        release_poll.set()
        assert await poll_task == "event"
        session = await asyncio.wait_for(session_task, timeout=1)
        await session.close()
        await pool.close()

        assert leased_during_poll is False

    asyncio.run(run())


@pytest.mark.parametrize("limit", [0, -1])
def test_protocol_rejects_nonpositive_response_chunk_limit(limit: int, monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    with pytest.raises(ValueError, match="max_response_chunks"):
        ProtocolAdapter(max_response_chunks=limit)


def test_sync_chunked_response_has_bounded_frame_count(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    header = struct.Struct(">4sBBIHQI")
    frames = [
        header.pack(b"FSNP", 0x81, 0x20, 1, 0x0101, 1, 0),
        header.pack(b"FSNP", 0x81, 0x20, 1, 0x0101, 1, 0),
        header.pack(b"FSNP", 0x81, 0, 1, 0x0101, 1, 0),
    ]
    adapter = ProtocolAdapter(max_response_chunks=2, heartbeat_interval=None)
    reads = iter(frames)

    def recv_exact(size: int, _sock: Any = None) -> bytes:
        return b"" if size == 0 else next(reads)

    adapter._recv_exact = recv_exact  # type: ignore[method-assign]

    with pytest.raises(FerricStoreError, match="max_response_chunks"):
        adapter._recv_response()

    adapter.close()


def test_async_chunked_response_has_bounded_frame_count() -> None:
    async def run() -> None:
        header = struct.Struct(">4sBBIHQI")
        frames = iter(
            [
                header.pack(b"FSNP", 0x81, 0x20, 1, 0x0101, 1, 0),
                header.pack(b"FSNP", 0x81, 0x20, 1, 0x0101, 1, 0),
                header.pack(b"FSNP", 0x81, 0, 1, 0x0101, 1, 0),
            ]
        )
        adapter = AsyncProtocolAdapter(max_response_chunks=2, heartbeat_interval=None)

        async def recv_exact(size: int, _reader: Any = None) -> bytes:
            return b"" if size == 0 else next(frames)

        adapter._recv_exact = recv_exact  # type: ignore[method-assign]
        with pytest.raises(FerricStoreError, match="max_response_chunks"):
            await adapter._recv_response()
        await adapter.close()

    asyncio.run(run())


def test_sync_topology_refresh_does_not_hold_routing_lock_and_is_singleflight(
    monkeypatch: Any,
) -> None:
    block = False
    entered = threading.Event()
    release = threading.Event()
    calls = 0

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            nonlocal calls
            if args[0] == "SHARDS":
                calls += 1
                if block:
                    entered.set()
                    release.wait(1)
                return _single_shard_topology()
            return b"OK"

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    initial_calls = calls
    block = True
    errors: list[BaseException] = []

    def refresh() -> None:
        try:
            pool.refresh_topology()
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=refresh) for _ in range(2)]
    for thread in threads:
        thread.start()
    assert entered.wait(1)
    lock_available = pool._lock.acquire(timeout=0.05)
    if lock_available:
        pool._lock.release()
    release.set()
    for thread in threads:
        thread.join(1)

    assert lock_available is True
    assert errors == []
    assert calls - initial_calls == 1
    pool.close()


def test_sync_topology_event_snapshots_hold_the_registry_lock() -> None:
    pool = object.__new__(TopologyProtocolAdapterPool)
    lock = threading.Lock()

    class LockCheckedAdapters:
        def values(self) -> list[Any]:
            if lock.acquire(blocking=False):
                lock.release()
                raise AssertionError("adapter registry read without its lock")
            return []

    pool._lock = lock  # type: ignore[assignment]
    pool._adapters = LockCheckedAdapters()  # type: ignore[assignment]

    assert pool.events == []
    assert pool._take_event() is None


def test_sync_topology_endpoint_connect_does_not_hold_lock_and_is_singleflight(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391}
    entered = threading.Event()
    release = threading.Event()
    created: list[str] = []

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(current["host"], current["port"])
            return b"OK"

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        created.append(url)
        if "leader-2.local" in url:
            entered.set()
            release.wait(1)
        return FakeAdapter(url)

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    current.update(host="leader-2.local", port=6392)
    pool.refresh_topology()
    endpoint = next(iter(pool.topology.endpoints.values()))
    results: list[Any] = []

    threads = [
        threading.Thread(target=lambda: results.append(pool._adapter_for_endpoint(endpoint)))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    assert entered.wait(1)
    lock_available = pool._lock.acquire(timeout=0.05)
    if lock_available:
        pool._lock.release()
    release.set()
    for thread in threads:
        thread.join(1)

    assert lock_available is True
    assert len(results) == 2
    assert results[0] is results[1]
    assert created.count("ferric://leader-2.local:6392") == 1
    pool.close()


def test_async_topology_refresh_does_not_hold_routing_lock_and_is_singleflight(
    monkeypatch: Any,
) -> None:
    async def run() -> None:
        block = False
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        class FakeAdapter:
            def __init__(self, url: str) -> None:
                self.url = url

            async def execute_command(self, *args: Any) -> Any:
                nonlocal calls
                if args[0] == "SHARDS":
                    calls += 1
                    if block:
                        entered.set()
                        await release.wait()
                    return _single_shard_topology()
                return b"OK"

            async def close(self) -> None:
                pass

        monkeypatch.setattr(
            AsyncProtocolAdapter,
            "from_url",
            lambda url, **_kwargs: FakeAdapter(url),
        )
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        initial_calls = calls
        block = True
        refreshes = [asyncio.create_task(pool.refresh_topology()) for _ in range(2)]
        await asyncio.wait_for(entered.wait(), 1)
        lock_available = True
        try:
            await asyncio.wait_for(pool._lock.acquire(), 0.05)
        except asyncio.TimeoutError:
            lock_available = False
        else:
            pool._lock.release()
        release.set()
        await asyncio.gather(*refreshes)

        assert lock_available is True
        assert calls - initial_calls == 1
        await pool.close()

    asyncio.run(run())


def test_sync_retired_endpoint_closes_when_it_becomes_idle_without_refresh(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False
            self._active = [0]
            self._idle_listeners: list[Any] = []

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(current["host"], current["port"])
            return b"OK"

        def add_idle_listener(self, listener: Any) -> None:
            self._idle_listeners.append(listener)

        def remove_idle_listener(self, listener: Any) -> None:
            if listener in self._idle_listeners:
                self._idle_listeners.remove(listener)

        def become_idle(self) -> None:
            self._active[0] = 0
            for listener in tuple(self._idle_listeners):
                listener()

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    retired = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))
    retired._active[0] = 1

    current.update(host="leader-2.local", port=6392)
    pool.refresh_topology()
    assert retired.closed is False

    retired.become_idle()
    assert retired.closed is True
    pool.close()


def test_sync_failed_retired_cleanup_retries_on_next_topology_refresh(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.close_calls = 0
            self.fail_once = "leader-1.local" in url

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(current["host"], current["port"])
            return b"OK"

        def close(self) -> None:
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("retired cleanup failed")

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    retired = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

    current.update(host="leader-2.local", port=6392)
    pool.refresh_topology()
    current.update(host="leader-3.local", port=6393)
    pool.refresh_topology()
    calls_before_pool_close = retired.close_calls
    pool.close()

    assert calls_before_pool_close == 2
    assert retired.close_calls == 2


def test_async_retired_endpoint_closes_when_it_becomes_idle_without_refresh(
    monkeypatch: Any,
) -> None:
    async def run() -> None:
        current = {"host": "leader-1.local", "port": 6391}

        class FakeAdapter:
            def __init__(self, url: str) -> None:
                self.url = url
                self.closed = False
                self._active = [0]
                self._idle_listeners: list[Any] = []

            async def execute_command(self, *args: Any) -> Any:
                if args[0] == "SHARDS":
                    return _single_shard_topology(current["host"], current["port"])
                return b"OK"

            def add_idle_listener(self, listener: Any) -> None:
                self._idle_listeners.append(listener)

            def remove_idle_listener(self, listener: Any) -> None:
                if listener in self._idle_listeners:
                    self._idle_listeners.remove(listener)

            def become_idle(self) -> None:
                self._active[0] = 0
                for listener in tuple(self._idle_listeners):
                    listener()

            async def close(self) -> None:
                self.closed = True

        monkeypatch.setattr(
            AsyncProtocolAdapter,
            "from_url",
            lambda url, **_kwargs: FakeAdapter(url),
        )
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        retired = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))
        retired._active[0] = 1

        current.update(host="leader-2.local", port=6392)
        await pool.refresh_topology()
        assert retired.closed is False

        retired.become_idle()
        await asyncio.sleep(0)
        assert retired.closed is True
        await pool.close()

    asyncio.run(run())


def test_async_failed_retired_cleanup_retries_on_next_topology_refresh(
    monkeypatch: Any,
) -> None:
    async def run() -> None:
        current = {"host": "leader-1.local", "port": 6391}

        class FakeAdapter:
            def __init__(self, url: str) -> None:
                self.url = url
                self.close_calls = 0
                self.fail_once = "leader-1.local" in url

            async def execute_command(self, *args: Any) -> Any:
                if args[0] == "SHARDS":
                    return _single_shard_topology(current["host"], current["port"])
                return b"OK"

            async def close(self) -> None:
                self.close_calls += 1
                if self.fail_once and self.close_calls == 1:
                    raise RuntimeError("retired cleanup failed")

        monkeypatch.setattr(
            AsyncProtocolAdapter,
            "from_url",
            lambda url, **_kwargs: FakeAdapter(url),
        )
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        retired = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

        current.update(host="leader-2.local", port=6392)
        await pool.refresh_topology()
        current.update(host="leader-3.local", port=6393)
        await pool.refresh_topology()
        while pool._cleanup_tasks:
            await asyncio.gather(*tuple(pool._cleanup_tasks))
        calls_before_pool_close = retired.close_calls
        await pool.close()

        assert calls_before_pool_close == 2
        assert retired.close_calls == 2

    asyncio.run(run())


def test_sync_topology_subscription_updates_cannot_finish_stale() -> None:
    first_entered = threading.Event()
    release_first = threading.Event()
    second_applied = threading.Event()
    applied: list[str] = []

    class Adapter:
        def subscribe_flow_wake(self, name: str) -> str:
            if name == "A":
                first_entered.set()
                assert release_first.wait(timeout=1)
            else:
                second_applied.set()
            applied.append(name)
            return name

    pool = object.__new__(TopologyProtocolAdapterPool)
    pool._lock = threading.RLock()
    pool._subscription_update_lock = threading.Lock()
    pool._closed = False
    pool._subscription_registry = protocol_module.FlowWakeSubscriptionRegistry()
    pool._subscription_generation = 0
    pool._adapters = {"node": Adapter()}
    errors: list[BaseException] = []

    def subscribe(name: str) -> None:
        try:
            pool.subscribe_flow_wake(name)
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=subscribe, args=("A",))
    second = threading.Thread(target=subscribe, args=("B",))
    first.start()
    assert first_entered.wait(timeout=1)
    second.start()
    second_applied.wait(timeout=0.05)
    release_first.set()
    first.join(timeout=1)
    second.join(timeout=1)

    desired = pool._subscription_registry.snapshot()[0].args[0]
    assert errors == []
    assert first.is_alive() is False
    assert second.is_alive() is False
    assert desired == "B"
    assert applied[-1] == "B"


def test_async_topology_subscription_updates_cannot_finish_stale() -> None:
    async def run() -> None:
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        applied: list[str] = []

        class Adapter:
            async def subscribe_flow_wake(self, name: str) -> str:
                if name == "A":
                    first_entered.set()
                    await release_first.wait()
                applied.append(name)
                return name

        pool = object.__new__(protocol_module.AsyncTopologyProtocolAdapterPool)
        pool._closed = False
        pool._subscription_update_lock = asyncio.Lock()
        pool._subscription_registry = protocol_module.FlowWakeSubscriptionRegistry()
        pool._subscription_generation = 0
        pool._adapters = {"node": Adapter()}

        first = asyncio.create_task(pool.subscribe_flow_wake("A"))
        await asyncio.wait_for(first_entered.wait(), timeout=1)
        second = asyncio.create_task(pool.subscribe_flow_wake("B"))
        await asyncio.sleep(0)
        release_first.set()
        await asyncio.gather(first, second)

        desired = pool._subscription_registry.snapshot()[0].args[0]
        assert desired == "B"
        assert applied[-1] == "B"

    asyncio.run(run())


def test_pipeline_target_cancellation_does_not_fail_sibling_future() -> None:
    adapter = object.__new__(ProtocolAdapter)
    source: Future[ProtocolResponse] = Future()
    adapter._submit_pipeline_request = lambda *args, **kwargs: source
    decode_entered = threading.Event()
    release_decode = threading.Event()

    def decode(item: Any) -> Any:
        if item == b"first":
            decode_entered.set()
            assert release_decode.wait(timeout=1)
        return item

    adapter._batch_item_value = decode
    targets = adapter._submit_pipeline([ProtocolCommand(1, {}), ProtocolCommand(2, {})])

    completion = threading.Thread(
        target=source.set_result,
        args=(ProtocolResponse(1, 1, 1, 0, 0, [b"first", b"second"]),),
    )
    completion.start()
    assert decode_entered.wait(timeout=1)
    assert targets[0].cancel()
    release_decode.set()
    completion.join(timeout=1)

    assert completion.is_alive() is False
    assert targets[0].cancelled()
    assert targets[1].result(timeout=1) == b"second"


def test_pipeline_completion_publishes_all_siblings_before_callbacks() -> None:
    adapter = object.__new__(ProtocolAdapter)
    source: Future[ProtocolResponse] = Future()
    adapter._submit_pipeline_request = lambda *args, **kwargs: source
    targets = adapter._submit_pipeline([ProtocolCommand(1, {}), ProtocolCommand(2, {})])
    observations: list[tuple[bool, Any]] = []

    def inspect_sibling(_future: Future[Any]) -> None:
        observations.append(
            (
                targets[1].done(),
                targets[1].result(timeout=0.05) if targets[1].done() else None,
            )
        )

    targets[0].add_done_callback(inspect_sibling)
    source.set_result(
        ProtocolResponse(
            1,
            1,
            1,
            0,
            0,
            [[b"ok", b"first"], [b"ok", b"second"]],
        )
    )

    assert observations == [(True, b"second")]


class _SyncEventAdapter:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self._listeners: list[Any] = []

    def add_event_listener(self, listener: Any) -> None:
        self._listeners.append(listener)

    def remove_event_listener(self, listener: Any) -> None:
        self._listeners.remove(listener)

    def emit(self, event: Any) -> None:
        self.events.append(event)
        for listener in tuple(self._listeners):
            listener()

    def wait_event(self, timeout: float | None = None) -> Any | None:
        del timeout
        return self.events.pop(0) if self.events else None

    def close(self) -> None:
        return None


def test_sync_pool_rechecks_buffered_events_after_session_waiter_is_served() -> None:
    adapters = [_SyncEventAdapter(), _SyncEventAdapter()]
    pool = ProtocolAdapterPool(adapters)  # type: ignore[arg-type]
    first_index, _ = pool._acquire_adapter()
    second_index, _ = pool._acquire_adapter()
    acquired_session: list[Any] = []
    session_ready = threading.Event()

    def acquire_session() -> None:
        acquired_session.append(pool.acquire_session())
        session_ready.set()

    session_thread = threading.Thread(target=acquire_session, daemon=True)
    session_thread.start()
    deadline = time.monotonic() + 1
    while pool._session_waiters != 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert pool._session_waiters == 1

    adapters[second_index].emit("ready")
    event_result: list[Any] = []
    event_ready = threading.Event()

    def wait_event() -> None:
        event_result.append(pool.wait_event(timeout=1))
        event_ready.set()

    event_thread = threading.Thread(target=wait_event, daemon=True)
    event_thread.start()
    deadline = time.monotonic() + 1
    while pool._event_ready.is_set() and time.monotonic() < deadline:
        time.sleep(0.001)
    pool._release_adapter(first_index)

    try:
        assert session_ready.wait(timeout=1)
        assert event_ready.wait(timeout=0.2)
        assert event_result == ["ready"]
    finally:
        if acquired_session:
            acquired_session[0].close()
        pool._release_adapter(second_index)
        pool.close()
        session_thread.join(timeout=1)
        event_thread.join(timeout=1)


class _AsyncEventAdapter:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self._listeners: list[Any] = []

    def add_event_listener(self, listener: Any) -> None:
        self._listeners.append(listener)

    def remove_event_listener(self, listener: Any) -> None:
        self._listeners.remove(listener)

    def emit(self, event: Any) -> None:
        self.events.append(event)
        for listener in tuple(self._listeners):
            listener()

    async def wait_event(self, timeout: float | None = None) -> Any | None:
        del timeout
        return self.events.pop(0) if self.events else None

    async def close(self) -> None:
        return None


def test_async_pool_rechecks_buffered_events_after_session_waiter_is_served() -> None:
    async def run() -> None:
        adapters = [_AsyncEventAdapter(), _AsyncEventAdapter()]
        pool = AsyncProtocolAdapterPool(adapters)  # type: ignore[arg-type]
        first_index, _ = await pool._acquire_adapter()
        second_index, _ = await pool._acquire_adapter()
        session_task = asyncio.create_task(pool.acquire_session())
        await asyncio.sleep(0)
        assert pool._session_waiters == 1

        adapters[second_index].emit("ready")
        event_task = asyncio.create_task(pool.wait_event(timeout=1))
        await asyncio.sleep(0)
        assert pool._event_ready.is_set() is False
        await pool._release_adapter(first_index)

        session = await asyncio.wait_for(session_task, timeout=1)
        try:
            assert await asyncio.wait_for(event_task, timeout=0.2) == "ready"
        finally:
            await session.close()
            await pool._release_adapter(second_index)
            await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize("limit_name", ["max_inflight_requests", "max_pending_request_bytes"])
@pytest.mark.parametrize("invalid", [0, -1, True])
def test_protocol_pending_limits_require_positive_integers(
    monkeypatch: Any,
    limit_name: str,
    invalid: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    with pytest.raises(ValueError, match=limit_name):
        ProtocolAdapter(heartbeat_interval=None, **{limit_name: invalid})
    with pytest.raises(ValueError, match=limit_name):
        AsyncProtocolAdapter(heartbeat_interval=None, **{limit_name: invalid})


def test_sync_protocol_bounds_inflight_request_count(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_inflight_requests=2,
    )
    monkeypatch.setattr(adapter, "_send", lambda *args, **kwargs: None)

    first_id, first = adapter._submit_request(protocol_module._OP_GET, 1, b"a")
    second_id, second = adapter._submit_request(protocol_module._OP_GET, 1, b"b")
    assert adapter.pending_request_count == 2
    with pytest.raises(FerricStoreError, match="max_inflight_requests"):
        adapter._submit_request(protocol_module._OP_GET, 1, b"c")

    adapter._discard_pending_request(first_id, expected_future=first)
    third_id, third = adapter._submit_request(protocol_module._OP_GET, 1, b"c")
    assert adapter.pending_request_count == 2

    adapter._discard_pending_request(second_id, expected_future=second)
    adapter._discard_pending_request(third_id, expected_future=third)
    adapter.close()


def test_sync_protocol_bounds_total_pending_request_bytes(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_pending_request_bytes=64,
    )
    adapter._sock = object()  # type: ignore[assignment]
    sent: list[Any] = []
    monkeypatch.setattr(protocol_module, "_send_frames", lambda *args, **kwargs: sent.append(args))

    with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
        adapter._submit_request(protocol_module._OP_SET, 1, b"x" * 65)

    assert sent == []
    assert adapter.pending_request_count == 0
    assert adapter.pending_request_bytes == 0
    assert adapter._sock is not None
    adapter._sock = None
    adapter.close()


def test_sync_submitted_request_expires_at_adapter_timeout(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=0.02, heartbeat_interval=None)
    monkeypatch.setattr(adapter, "_send", lambda *args, **kwargs: None)

    _request_id, response = adapter._submit_request(protocol_module._OP_GET, 1, b"key")
    with pytest.raises(FerricStoreError, match="protocol request timed out"):
        response.result(timeout=1)

    assert adapter.pending_request_count == 0
    assert adapter.pending_request_bytes == 0
    adapter.close()


def test_sync_explicit_unbounded_blocking_request_does_not_gain_submit_deadline(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=0.02, heartbeat_interval=None)
    monkeypatch.setattr(adapter, "_send", lambda *args, **kwargs: None)
    errors: list[BaseException] = []

    def request() -> None:
        try:
            adapter._request(protocol_module._OP_GET, 1, b"key", timeout=None)
        except BaseException as exc:
            errors.append(exc)

    caller = threading.Thread(target=request)
    caller.start()
    deadline = time.monotonic() + 1
    while adapter.pending_request_count != 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert adapter.pending_request_count == 1

    time.sleep(0.05)
    try:
        assert caller.is_alive()
        assert errors == []
    finally:
        adapter._fail_pending(FerricStoreError("test cleanup"))
        caller.join(timeout=1)
        adapter.close()


def test_sync_deadline_scheduler_compacts_completed_request_churn(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=60, heartbeat_interval=None)
    monkeypatch.setattr(adapter, "_send", lambda *args, **kwargs: None)

    for _ in range(5_000):
        request_id, response = adapter._submit_request(protocol_module._OP_GET, 1, b"key")
        adapter._discard_pending_request(request_id, expected_future=response)

    assert adapter.pending_request_count == 0
    assert adapter.pending_request_bytes == 0
    assert len(adapter._deadline_scheduler._heap) < 64
    adapter.close()


def test_async_protocol_bounds_inflight_request_count() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=None,
            heartbeat_interval=None,
            max_inflight_requests=2,
        )

        async def connected() -> None:
            return None

        async def sent(*_args: Any, **_kwargs: Any) -> None:
            return None

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        adapter._send = sent  # type: ignore[method-assign]
        first = asyncio.create_task(
            adapter._request_without_timeout(protocol_module._OP_GET, 1, b"a")
        )
        second = asyncio.create_task(
            adapter._request_without_timeout(protocol_module._OP_GET, 1, b"b")
        )
        await asyncio.sleep(0)
        assert adapter.pending_request_count == 2

        with pytest.raises(FerricStoreError, match="max_inflight_requests"):
            await adapter._request_without_timeout(protocol_module._OP_GET, 1, b"c")

        adapter._fail_pending(FerricStoreError("test cleanup"))
        await asyncio.gather(first, second, return_exceptions=True)
        assert adapter.pending_request_count == 0
        assert adapter.pending_request_bytes == 0
        await adapter.close()

    asyncio.run(run())


def test_async_protocol_bounds_total_pending_request_bytes() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=None,
            heartbeat_interval=None,
            max_pending_request_bytes=64,
        )

        class Writer:
            def __init__(self) -> None:
                self.writes: list[Any] = []

            def write(self, frame: bytes) -> None:
                self.writes.append(frame)

            async def drain(self) -> None:
                return None

        writer = Writer()
        adapter._writer = writer  # type: ignore[assignment]

        async def connected() -> None:
            return None

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
            await adapter._request_without_timeout(protocol_module._OP_SET, 1, b"x" * 65)

        assert writer.writes == []
        assert adapter.pending_request_count == 0
        assert adapter.pending_request_bytes == 0
        assert adapter._writer is writer
        adapter._writer = None
        await adapter.close()

    asyncio.run(run())
