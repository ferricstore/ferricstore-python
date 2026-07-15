from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import logging
import socket
import struct
import threading
import time
import tracemalloc
import zlib
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, cast

import pytest

import ferricstore.protocol as protocol_module
import ferricstore.protocol_async as protocol_async_module
import ferricstore.protocol_async_topology as protocol_async_topology_module
import ferricstore.protocol_commands as protocol_commands_module
import ferricstore.protocol_responses as protocol_response_module
import ferricstore.protocol_sync as protocol_sync_module
import ferricstore.protocol_sync_batch as protocol_sync_batch_module
import ferricstore.protocol_sync_topology as protocol_sync_topology_module
from ferricstore import AsyncFlowClient, AsyncProtocolAdapter, FlowClient, ProtocolAdapter
from ferricstore.command_core import command_route_keys
from ferricstore.errors import (
    FerricStoreError,
    FlowAlreadyExistsError,
    InvalidCommandError,
)
from ferricstore.protocol import (
    _COMPACT_BINARY_LIST_LIST,
    _COMPACT_FLOW_LIST_REQUEST,
    _COMPACT_FLOW_RECORD,
    _COMPACT_FLOW_RECORD_LIST,
    _COMPACT_INTEGER_LIST,
    _COMPACT_PIPELINE_REQUEST,
    _COMPACT_PIPELINE_RESPONSE,
    _FLAG_CUSTOM_PAYLOAD,
    _OP_COMMAND_EXEC,
    AsyncProtocolAdapterPool,
    ProtocolAdapterPool,
    ProtocolCommand,
    ProtocolResponse,
    RoutingTopology,
    _compact_flow_many_payloads_from_raw,
    _compact_pipeline_payload_from_raw,
    _try_fast_response_value,
    _try_fast_response_value_at,
    build_protocol_command,
    decode_value,
    encode_value,
)
from ferricstore.protocol_codec import MAX_VALUE_NESTING
from ferricstore.protocol_pipeline_codec import (
    _blocks_forever,
    _expected_command_collection_items,
    _expected_payload_collection_items,
)
from ferricstore.types import ChildSpec, ClaimedFlow, CreateItem


def _flow_partition_route_key(value: str | bytes) -> str:
    encoded = value if isinstance(value, bytes) else value.encode()
    digest = base64.urlsafe_b64encode(hashlib.sha256(encoded).digest()).rstrip(b"=").decode()
    return f"f:{{f:{digest}}}:route"


def _flow_id_route_key(value: str | bytes) -> str:
    encoded = value if isinstance(value, bytes) else value.encode()
    return f"f:{{fa:{zlib.crc32(encoded) % 256}}}:route"


def _single_shard_topology(
    host: str = "leader.local",
    port: int = 6391,
    *,
    route_epoch: int = 1,
) -> dict[str, Any]:
    return {
        "route_epoch": route_epoch,
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


def _two_shard_topology() -> dict[str, Any]:
    return {
        "route_epoch": 1,
        "shard_count": 2,
        "ranges": [
            {
                "first_slot": 0,
                "last_slot": 511,
                "shard": 0,
                "lane_id": 1,
                "endpoint": {
                    "node": "leader-a@cluster",
                    "host": "leader-a.local",
                    "native_port": 6391,
                },
            },
            {
                "first_slot": 512,
                "last_slot": 1023,
                "shard": 1,
                "lane_id": 1,
                "endpoint": {
                    "node": "leader-b@cluster",
                    "host": "leader-b.local",
                    "native_port": 6392,
                },
            },
        ],
    }


def test_protocol_adapter_nonretryable_startup_failure_closes_transport():
    class FakeSocket:
        def __init__(self) -> None:
            self.closed = False

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = object.__new__(ProtocolAdapter)
    sock = FakeSocket()
    adapter.password = None
    adapter.heartbeat_interval = None
    adapter._sock = None
    adapter._connecting = False
    adapter._pending = {}
    adapter._pending_traces = {}
    adapter._closed = False
    adapter._events_cv = threading.Condition()
    adapter._event_error = None
    adapter._event_listeners = []

    def fail_startup() -> None:
        adapter._sock = sock
        raise FerricStoreError("bad startup metadata")

    adapter._connect_once = fail_startup

    with pytest.raises(FerricStoreError, match="bad startup metadata"):
        adapter._connect()

    assert sock.closed
    assert adapter._sock is None


def test_protocol_pool_session_waits_for_submitted_future_completion():
    class FakeProtocolAdapter:
        def __init__(self) -> None:
            self.future: Future[Any] = Future()

        def submit_command(self, *_args: Any) -> Future[Any]:
            return self.future

        def close(self) -> None:
            pass

    adapter = FakeProtocolAdapter()
    pool = ProtocolAdapterPool([adapter])
    future = pool.submit_command("SET", "key", "value")
    acquired = threading.Event()
    session_holder: list[Any] = []

    def acquire() -> None:
        session_holder.append(pool.acquire_session())
        acquired.set()

    thread = threading.Thread(target=acquire, daemon=True)
    thread.start()
    assert not acquired.wait(timeout=0.05)

    future.set_result(b"OK")
    assert acquired.wait(timeout=0.5)
    session_holder[0].close()
    pool.close()
    thread.join(timeout=0.5)


def test_protocol_pool_cancelled_wrapper_waits_for_wire_request_completion():
    class FakeProtocolAdapter:
        def __init__(self) -> None:
            self.source: Future[Any] = Future()
            self.wrapper: Future[Any] = Future()
            cast(Any, self.wrapper)._ferricstore_sources = (self.source,)

        def submit_command(self, *_args: Any) -> Future[Any]:
            return self.wrapper

        def close(self) -> None:
            pass

    adapter = FakeProtocolAdapter()
    pool = ProtocolAdapterPool([adapter])
    wrapper = pool.submit_command("SET", "key", "value")
    wrapper.cancel()
    acquired = threading.Event()
    sessions: list[Any] = []

    def acquire() -> None:
        sessions.append(pool.acquire_session())
        acquired.set()

    thread = threading.Thread(target=acquire, daemon=True)
    thread.start()
    assert not acquired.wait(timeout=0.05)
    adapter.source.set_result(b"OK")
    assert acquired.wait(timeout=0.5)
    sessions[0].close()
    pool.close()
    thread.join(timeout=0.5)


def test_protocol_pool_close_wakes_blocked_acquisitions():
    class FakeProtocolAdapter:
        def close(self) -> None:
            pass

        def execute_command(self, *_args: Any) -> Any:
            return b"OK"

    pool = ProtocolAdapterPool([FakeProtocolAdapter()])
    session = pool.acquire_session()
    done = threading.Event()
    errors: list[BaseException] = []

    def execute() -> None:
        try:
            pool.execute_command("PING")
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=execute, daemon=True)
    thread.start()
    try:
        pool.close()
        assert done.wait(timeout=0.5)
        assert len(errors) == 1
        assert isinstance(errors[0], FerricStoreError)
        assert "closed" in str(errors[0])
    finally:
        session.close()
        thread.join(timeout=0.5)


def test_protocol_pool_close_attempts_every_adapter_and_raises_first_error():
    class Adapter:
        def __init__(self, error: BaseException | None = None) -> None:
            self.error = error
            self.closed = False

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True
            if self.error is not None:
                raise self.error

    adapters = [Adapter(RuntimeError("first close failed")), Adapter()]
    pool = ProtocolAdapterPool(adapters)

    with pytest.raises(RuntimeError, match="first close failed"):
        pool.close()

    assert all(adapter.closed for adapter in adapters)


def test_protocol_pool_close_retries_after_adapter_failure():
    class Adapter:
        def __init__(self) -> None:
            self.close_calls = 0

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("transient close failure")

    adapter = Adapter()
    pool = ProtocolAdapterPool([adapter])

    with pytest.raises(RuntimeError, match="transient close failure"):
        pool.close()
    pool.close()

    assert adapter.close_calls == 2


def test_topology_pool_close_retries_only_failed_adapters():
    class Adapter:
        def __init__(self, *, fail_once: bool = False) -> None:
            self.fail_once = fail_once
            self.close_calls = 0

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def close(self) -> None:
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("transient topology close failure")

    failed = Adapter(fail_once=True)
    succeeded = Adapter()
    pool = object.__new__(protocol_module.TopologyProtocolAdapterPool)
    pool._lock = threading.Lock()
    pool._closed = False
    pool._adapters = {("failed", 6388): failed, ("succeeded", 6388): succeeded}
    pool._retired_adapters = {}
    pool._event_ready = threading.Event()
    pool._event_listener = lambda: None

    with pytest.raises(RuntimeError, match="transient topology close failure"):
        pool.close()
    pool.close()

    assert failed.close_calls == 2
    assert succeeded.close_calls == 1


def test_async_protocol_pool_close_wakes_blocked_acquisitions():
    class FakeAsyncProtocolAdapter:
        async def close(self) -> None:
            pass

        async def execute_command(self, *_args: Any) -> Any:
            return b"OK"

    async def run() -> None:
        pool = AsyncProtocolAdapterPool([FakeAsyncProtocolAdapter()])
        session = await pool.acquire_session()
        task = asyncio.create_task(pool.execute_command("PING"))
        await asyncio.sleep(0)
        try:
            await pool.close()
            with pytest.raises(FerricStoreError, match="closed"):
                await asyncio.wait_for(task, timeout=0.5)
        finally:
            await session.close()
            if not task.done():
                task.cancel()

    asyncio.run(run())


def test_async_topology_close_retries_only_failed_adapters():
    class Adapter:
        def __init__(self, *, fail_once: bool = False) -> None:
            self.fail_once = fail_once
            self.close_calls = 0

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        async def close(self) -> None:
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("first close failed")

    async def run() -> None:
        adapters = [Adapter(fail_once=True), Adapter()]
        pool = object.__new__(protocol_module.AsyncTopologyProtocolAdapterPool)
        pool._lock = asyncio.Lock()
        pool._closed = False
        pool._adapters = {("first", 1): adapters[0], ("second", 2): adapters[1]}
        pool._retired_adapters = {}
        pool._cleanup_tasks = set()
        pool._event_ready = asyncio.Event()
        pool._event_listener = lambda: None

        with pytest.raises(RuntimeError, match="first close failed"):
            await pool.close()
        await pool.close()

        assert [adapter.close_calls for adapter in adapters] == [2, 1]

    asyncio.run(run())


def test_async_protocol_pool_close_cancellation_still_closes_every_adapter():
    class Adapter:
        def __init__(self) -> None:
            self.entered = asyncio.Event()
            self.release = asyncio.Event()
            self.finished = asyncio.Event()

        async def close(self) -> None:
            self.entered.set()
            await self.release.wait()
            self.finished.set()

    async def run() -> None:
        adapters = [Adapter(), Adapter()]
        pool = AsyncProtocolAdapterPool(adapters)
        close_task = asyncio.create_task(pool.close())
        await asyncio.gather(*(adapter.entered.wait() for adapter in adapters))

        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task

        for adapter in adapters:
            adapter.release.set()
        await asyncio.wait_for(
            asyncio.gather(*(adapter.finished.wait() for adapter in adapters)),
            timeout=1.0,
        )

    asyncio.run(run())


def test_async_protocol_pool_cancelled_close_can_be_rejoined():
    class Adapter:
        def __init__(self) -> None:
            self.entered = asyncio.Event()
            self.release = asyncio.Event()
            self.finished = False

        async def close(self) -> None:
            self.entered.set()
            await self.release.wait()
            self.finished = True

    async def run() -> None:
        adapter = Adapter()
        pool = AsyncProtocolAdapterPool([adapter])
        first = asyncio.create_task(pool.close())
        await adapter.entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        second = asyncio.create_task(pool.close())
        try:
            await asyncio.sleep(0)
            assert second.done() is False
        finally:
            adapter.release.set()
            await second

        assert adapter.finished is True

    asyncio.run(run())


def test_async_protocol_pool_wake_broadcast_uses_bounded_concurrency():
    async def run() -> None:
        active = 0
        peak = 0

        class Adapter:
            def add_event_listener(self, _listener: Any) -> None:
                pass

            async def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> bytes:
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0)
                active -= 1
                return b"OK"

        pool = AsyncProtocolAdapterPool([Adapter() for _ in range(40)])
        replies = await pool.subscribe_flow_wake("email")

        assert replies == [b"OK"] * 40
        assert 1 < peak <= 16

    asyncio.run(run())


def test_async_topology_wake_broadcast_uses_bounded_concurrency():
    async def run() -> None:
        active = 0
        peak = 0

        class Adapter:
            async def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> bytes:
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0)
                active -= 1
                return b"OK"

        pool = object.__new__(protocol_module.AsyncTopologyProtocolAdapterPool)
        pool._closed = False
        pool._adapters = {(f"endpoint-{index}", 6388): Adapter() for index in range(40)}
        pool._subscription_registry = protocol_module.FlowWakeSubscriptionRegistry()

        replies = await pool.subscribe_flow_wake("email")

        assert replies == [b"OK"] * 40
        assert 1 < peak <= 16

    asyncio.run(run())


def test_async_topology_wake_broadcast_has_one_nested_concurrency_budget():
    async def run() -> None:
        active = 0
        peak = 0

        class Connection:
            async def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> bytes:
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0)
                active -= 1
                return b"OK"

        class EndpointPool:
            def __init__(self) -> None:
                self.adapters = [Connection() for _ in range(8)]

            async def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> list[bytes]:
                return await asyncio.gather(
                    *(adapter.subscribe_flow_wake(*args, **kwargs) for adapter in self.adapters)
                )

        pool = object.__new__(protocol_module.AsyncTopologyProtocolAdapterPool)
        pool._closed = False
        pool._adapters = {(f"endpoint-{index}", 6388): EndpointPool() for index in range(6)}
        pool._subscription_registry = protocol_module.FlowWakeSubscriptionRegistry()

        replies = await pool.subscribe_flow_wake("email")

        assert len(replies) == 6
        assert 8 < peak <= 16

    asyncio.run(run())


def test_protocol_pool_wake_broadcast_blocks_new_affine_session():
    class FakeProtocolAdapter:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> bytes:
            self.started.set()
            assert self.release.wait(timeout=0.5)
            return b"OK"

        def close(self) -> None:
            pass

    adapter = FakeProtocolAdapter()
    pool = ProtocolAdapterPool([adapter])
    subscribed = threading.Thread(target=pool.subscribe_flow_wake, args=("jobs",), daemon=True)
    subscribed.start()
    assert adapter.started.wait(timeout=0.5)

    acquired = threading.Event()
    sessions = []

    def acquire() -> None:
        sessions.append(pool.acquire_session())
        acquired.set()

    acquiring = threading.Thread(target=acquire, daemon=True)
    acquiring.start()
    try:
        assert not acquired.wait(timeout=0.05)
        adapter.release.set()
        assert acquired.wait(timeout=0.5)
    finally:
        adapter.release.set()
        subscribed.join(timeout=0.5)
        acquiring.join(timeout=0.5)
        for session in sessions:
            session.close()
        pool.close()


def test_async_protocol_pool_wake_broadcast_blocks_new_affine_session():
    class FakeAsyncProtocolAdapter:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        async def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> bytes:
            self.started.set()
            await self.release.wait()
            return b"OK"

        async def close(self) -> None:
            pass

    async def run() -> None:
        adapter = FakeAsyncProtocolAdapter()
        pool = AsyncProtocolAdapterPool([adapter])
        subscribed = asyncio.create_task(pool.subscribe_flow_wake("jobs"))
        await asyncio.wait_for(adapter.started.wait(), timeout=0.5)
        acquiring = asyncio.create_task(pool.acquire_session())
        await asyncio.sleep(0)
        try:
            assert not acquiring.done()
            adapter.release.set()
            await asyncio.wait_for(subscribed, timeout=0.5)
            session = await asyncio.wait_for(acquiring, timeout=0.5)
            await session.close()
        finally:
            adapter.release.set()
            if not subscribed.done():
                subscribed.cancel()
            if not acquiring.done():
                acquiring.cancel()
            await pool.close()

    asyncio.run(run())


def test_protocol_event_queue_is_bounded_deque(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(heartbeat_interval=None, max_event_queue_size=2)
    ready = threading.Event()
    adapter.add_event_listener(ready.set)

    adapter._enqueue_event("one")
    adapter._enqueue_event("two")
    ready.clear()

    assert isinstance(adapter._events, deque)
    with pytest.raises(FerricStoreError, match="event queue"):
        adapter._enqueue_event("three")
    assert ready.is_set()


def test_async_protocol_event_queue_is_bounded_deque():
    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None, max_event_queue_size=2)
        ready = asyncio.Event()
        adapter.add_event_listener(ready.set)

        await adapter._enqueue_event("one")
        await adapter._enqueue_event("two")
        ready.clear()

        assert isinstance(adapter._events, deque)
        with pytest.raises(FerricStoreError, match="event queue"):
            await adapter._enqueue_event("three")
        assert ready.is_set()

    asyncio.run(run())


def test_protocol_event_listener_is_notified_on_transport_error(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(heartbeat_interval=None)
    ready = threading.Event()
    adapter.add_event_listener(ready.set)

    adapter._close_transport(FerricStoreError("reader failed"))

    assert ready.is_set()
    with pytest.raises(FerricStoreError, match="reader failed"):
        adapter.wait_event(timeout=0)


def test_async_protocol_event_listener_is_notified_on_transport_error():
    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        ready = asyncio.Event()
        adapter.add_event_listener(ready.set)

        await adapter._close_transport(FerricStoreError("reader failed"))

        assert ready.is_set()
        with pytest.raises(FerricStoreError, match="reader failed"):
            await adapter.wait_event(timeout=0)

    asyncio.run(run())


def test_protocol_pool_wait_event_uses_notification_instead_of_polling(monkeypatch):
    class FakeProtocolAdapter:
        def __init__(self) -> None:
            self.events: deque[Any] = deque()
            self.listeners: list[Any] = []

        def add_event_listener(self, listener: Any) -> None:
            self.listeners.append(listener)

        def remove_event_listener(self, listener: Any) -> None:
            self.listeners.remove(listener)

        def wait_event(self, timeout: float | None = None) -> Any | None:
            return self.events.popleft() if self.events else None

        def emit(self, value: Any) -> None:
            self.events.append(value)
            for listener in list(self.listeners):
                listener()

        def close(self) -> None:
            pass

    adapter = FakeProtocolAdapter()
    pool = ProtocolAdapterPool([adapter])
    timer = threading.Timer(0.01, adapter.emit, args=("wake",))
    timer.start()
    monkeypatch.setattr(
        protocol_module.time,
        "sleep",
        lambda _delay: (_ for _ in ()).throw(AssertionError("wait_event polled")),
    )
    try:
        assert pool.wait_event(timeout=0.5) == "wake"
    finally:
        timer.cancel()
        pool.close()


def test_protocol_pool_session_release_wakes_queued_event_waiter():
    class FakeProtocolAdapter:
        def __init__(self) -> None:
            self.events: deque[Any] = deque()
            self.listeners: list[Any] = []

        def add_event_listener(self, listener: Any) -> None:
            self.listeners.append(listener)

        def remove_event_listener(self, listener: Any) -> None:
            self.listeners.remove(listener)

        def wait_event(self, timeout: float | None = None) -> Any | None:
            return self.events.popleft() if self.events else None

        def emit(self, value: Any) -> None:
            self.events.append(value)
            for listener in list(self.listeners):
                listener()

        def close(self) -> None:
            pass

    adapter = FakeProtocolAdapter()
    pool = ProtocolAdapterPool([adapter])
    session = pool.acquire_session()
    results = []
    waiting = threading.Thread(
        target=lambda: results.append(pool.wait_event(timeout=1)), daemon=True
    )
    waiting.start()
    adapter.emit("wake-after-release")
    deadline = time.monotonic() + 0.5
    while pool._event_ready.is_set() and time.monotonic() < deadline:
        time.sleep(0.001)
    assert not pool._event_ready.is_set()
    assert waiting.is_alive()

    session.close()
    waiting.join(timeout=0.2)
    try:
        assert results == ["wake-after-release"]
    finally:
        pool.close()


def test_async_protocol_pool_session_release_wakes_queued_event_waiter():
    class FakeAsyncProtocolAdapter:
        def __init__(self) -> None:
            self.events: deque[Any] = deque()
            self.listeners: list[Any] = []

        def add_event_listener(self, listener: Any) -> None:
            self.listeners.append(listener)

        def remove_event_listener(self, listener: Any) -> None:
            self.listeners.remove(listener)

        async def wait_event(self, timeout: float | None = None) -> Any | None:
            return self.events.popleft() if self.events else None

        def emit(self, value: Any) -> None:
            self.events.append(value)
            for listener in list(self.listeners):
                listener()

        async def close(self) -> None:
            pass

    async def run() -> None:
        adapter = FakeAsyncProtocolAdapter()
        pool = AsyncProtocolAdapterPool([adapter])
        session = await pool.acquire_session()
        waiting = asyncio.create_task(pool.wait_event(timeout=1))
        adapter.emit("wake-after-release")
        for _ in range(10):
            await asyncio.sleep(0)
            if not pool._event_ready.is_set():
                break
        assert not pool._event_ready.is_set()
        assert not waiting.done()

        await session.close()
        try:
            assert await asyncio.wait_for(waiting, timeout=0.2) == "wake-after-release"
        finally:
            if not waiting.done():
                waiting.cancel()
            await pool.close()

    asyncio.run(run())


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


def test_protocol_value_decoder_does_not_slice_remaining_buffer_per_item():
    class TrackingBytes(bytes):
        sliced_bytes = 0

        def __getitem__(self, index):
            value = super().__getitem__(index)
            if isinstance(index, slice):
                type(self).sliced_bytes += len(value)
                return type(self)(value)
            return value

    payload = TrackingBytes(encode_value([0] * 2_000) + b"tail")

    decoded, rest = decode_value(payload)

    assert decoded == [0] * 2_000
    assert rest == b"tail"
    assert TrackingBytes.sliced_bytes <= len(payload) * 2


def test_protocol_value_decoder_rejects_excessive_nesting_with_protocol_error():
    payload = (b"\x05" + struct.pack(">I", 1)) * 1_100 + b"\x00"

    with pytest.raises(FerricStoreError, match="nesting exceeds maximum depth"):
        decode_value(payload)


def test_protocol_value_codec_accepts_documented_maximum_nesting():
    value: Any = None
    for _ in range(MAX_VALUE_NESTING):
        value = [value]

    decoded, rest = decode_value(encode_value(value))

    assert rest == b""
    assert decoded == value
    with pytest.raises(FerricStoreError, match="nesting exceeds maximum depth"):
        encode_value([value])


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


def test_flow_client_from_protocol_urls_uses_topology_adapter_pool(monkeypatch):
    created = {}

    class FakeProtocolAdapterPool:
        @classmethod
        def from_urls(cls, urls, **kwargs):
            created["urls"] = urls
            created["kwargs"] = kwargs
            return cls()

        def execute_command(self, *args):
            return b"OK"

    monkeypatch.setattr("ferricstore.protocol.ProtocolAdapterPool", FakeProtocolAdapterPool)

    client = FlowClient.from_urls(
        ["ferric://seed-a:6388", "ferric://seed-b:6388"],
        timeout=1.0,
        endpoint_policy="any",
    )

    assert client.command("PING") == b"OK"
    assert created == {
        "urls": ["ferric://seed-a:6388", "ferric://seed-b:6388"],
        "kwargs": {"timeout": 1.0, "endpoint_policy": "any"},
    }


def test_flow_client_topology_helpers_delegate_to_executor():
    class FakeTopologyExecutor:
        def execute_command(self, *args):
            return b"OK"

        def refresh_topology(self):
            return "refreshed"

        def route(self, key):
            return {"key": key}

    client = FlowClient(FakeTopologyExecutor())

    assert client.refresh_topology() == "refreshed"
    assert client.route("k1") == {"key": "k1"}


def test_async_flow_client_from_protocol_urls_uses_topology_adapter_pool(monkeypatch):
    created = {}

    class FakeAsyncProtocolAdapterPool:
        @classmethod
        def from_urls(cls, urls, **kwargs):
            created["urls"] = urls
            created["kwargs"] = kwargs
            return cls()

        async def execute_command(self, *args):
            return b"OK"

    monkeypatch.setattr(
        "ferricstore.protocol.AsyncProtocolAdapterPool",
        FakeAsyncProtocolAdapterPool,
    )

    async def run():
        client = AsyncFlowClient.from_urls(
            ["ferric://seed-a:6388", "ferric://seed-b:6388"],
            endpoint_policy="any",
        )
        assert await client.command("PING") == b"OK"

    asyncio.run(run())

    assert created == {
        "urls": ["ferric://seed-a:6388", "ferric://seed-b:6388"],
        "kwargs": {"endpoint_policy": "any"},
    }


def test_protocol_from_url_ignores_compatibility_only_kwargs(monkeypatch):
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


def test_routing_topology_builds_slot_table_from_shards_payload():
    topology = RoutingTopology.build(
        {
            "route_epoch": 7,
            "shard_count": 2,
            "ranges": [
                {
                    "first_slot": 0,
                    "last_slot": 511,
                    "shard": 0,
                    "lane_id": 1,
                    "endpoint": {
                        "node": "a@cluster",
                        "host": "node-a.local",
                        "native_port": 6388,
                    },
                },
                {
                    "first_slot": 512,
                    "last_slot": 1023,
                    "shard": 1,
                    "lane_id": 2,
                    "endpoint": {
                        "node": "b@cluster",
                        "host": "node-b.local",
                        "native_port": 6389,
                    },
                },
            ],
        }
    )

    key_for_shard_0 = next(
        f"slot-a-{idx}" for idx in range(10_000) if topology.slot_for_key(f"slot-a-{idx}") < 512
    )
    key_for_shard_1 = next(
        f"slot-b-{idx}" for idx in range(10_000) if topology.slot_for_key(f"slot-b-{idx}") >= 512
    )

    route0 = topology.route_key(key_for_shard_0)
    route1 = topology.route_key(key_for_shard_1)

    assert topology.route_epoch == 7
    assert topology.slot_for_key("{tenant}:one") == topology.slot_for_key("{tenant}:two")
    assert route0["shard"] == 0
    assert route0["endpoint"]["host"] == "node-a.local"
    assert route1["shard"] == 1
    assert route1["lane_id"] == 2
    assert route1["endpoint_key"] == ("node-b.local", 6389)


def test_routing_topology_hashes_binary_keys_without_utf8_decoding():
    key = b"\xff{\x00tag\xfe}:suffix"

    assert RoutingTopology.slot_for_key(key) == zlib.crc32(b"\x00tag\xfe") & 1023
    assert RoutingTopology.slot_for_key(b"{\x00tag\xfe}:other") == RoutingTopology.slot_for_key(key)


@pytest.mark.parametrize(
    "ranges",
    [
        [(0, 700), (700, 1023)],
        [(0, 500), (502, 1023)],
    ],
)
def test_routing_topology_rejects_ambiguous_or_incomplete_slot_tables(ranges):
    payload_ranges = [
        {
            "first_slot": first,
            "last_slot": last,
            "shard": index,
            "lane_id": 1,
            "endpoint": {
                "node": f"leader-{index}@cluster",
                "host": f"leader-{index}.local",
                "native_port": 6388 + index,
            },
        }
        for index, (first, last) in enumerate(ranges)
    ]

    with pytest.raises(FerricStoreError, match="slot table"):
        RoutingTopology.build(
            {
                "route_epoch": 1,
                "shard_count": 2,
                "ranges": payload_ranges,
            }
        )


@pytest.mark.parametrize(
    "case",
    [
        "negative_epoch",
        "zero_shards",
        "too_many_shards",
        "shard_out_of_range",
        "negative_lane",
        "oversized_lane",
        "empty_host",
        "invalid_port",
        "invalid_tls_port",
        "shard_count_mismatch",
    ],
)
def test_routing_topology_rejects_invalid_metadata(case: str):
    payload = _single_shard_topology("leader.local", 6388)
    if case == "negative_epoch":
        payload["route_epoch"] = -1
    elif case == "zero_shards":
        payload["shard_count"] = 0
    elif case == "too_many_shards":
        payload["shard_count"] = 1025
    elif case == "shard_out_of_range":
        payload["ranges"][0]["shard"] = 1
    elif case == "negative_lane":
        payload["ranges"][0]["lane_id"] = -1
    elif case == "oversized_lane":
        payload["ranges"][0]["lane_id"] = 1 << 32
    elif case == "empty_host":
        payload["ranges"][0]["endpoint"]["host"] = ""
    elif case == "invalid_port":
        payload["ranges"][0]["endpoint"]["native_port"] = 65536
    elif case == "invalid_tls_port":
        payload["ranges"][0]["endpoint"]["native_tls_port"] = 0
    elif case == "shard_count_mismatch":
        payload["shard_count"] = 2

    with pytest.raises(FerricStoreError):
        RoutingTopology.build(payload)


def test_protocol_adapter_pool_from_urls_routes_keyed_commands_to_shard_leader(monkeypatch):
    created: dict[str, FakeProtocolAdapter] = {}

    class FakeProtocolAdapter:
        def __init__(self, url, **kwargs):
            self.url = url
            self.kwargs = kwargs
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args):
            self.calls.append(args)
            if args[0] == "SHARDS":
                return {
                    "route_epoch": 1,
                    "shard_count": 1,
                    "ranges": [
                        {
                            "first_slot": 0,
                            "last_slot": 1023,
                            "shard": 0,
                            "lane_id": 3,
                            "endpoint": {
                                "node": "leader@cluster",
                                "host": "leader.local",
                                "native_port": 6391,
                            },
                        }
                    ],
                }
            return {"url": self.url, "args": args}

        def close(self):
            self.calls.append(("CLOSE",))

    def fake_from_url(url, **kwargs):
        adapter = FakeProtocolAdapter(url, **kwargs)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", fake_from_url)

    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        timeout=1.0,
        endpoint_policy="any",
    )

    result = pool.execute_command("SET", "tenant-key", b"value")

    assert result["url"] == "ferric://leader.local:6391"
    assert created["ferric://seed.local:6388"].calls == [("SHARDS",)]
    assert created["ferric://leader.local:6391"].calls == [("SET", "tenant-key", b"value")]
    assert created["ferric://leader.local:6391"].kwargs["timeout"] == 1.0


def test_topology_routes_generic_command_exec_using_command_registry(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args: Any) -> Any:
            self.calls.append(args)
            if args[0] == "SHARDS":
                return _single_shard_topology()
            return {"url": self.url, "args": args}

        def close(self) -> None:
            pass

    def factory(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", factory)
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )

    result = pool.execute_command("INCR", "tenant-key")

    assert result["url"] == "ferric://leader.local:6391"
    assert created["ferric://seed.local:6388"].calls == [("SHARDS",)]


def test_topology_routes_flow_commands_by_effective_partition(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args: Any) -> Any:
            self.calls.append(args)
            if args[0] == "SHARDS":
                return _two_shard_topology()
            return self.url

        def close(self) -> None:
            pass

    def factory(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", factory)
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )
    flow_id = "flow-1"
    expected_auto_url = (
        "ferric://leader-a.local:6391"
        if RoutingTopology.slot_for_key(_flow_id_route_key(flow_id)) < 512
        else "ferric://leader-b.local:6392"
    )
    explicit_partition = "tenant-a"
    expected_explicit_url = (
        "ferric://leader-a.local:6391"
        if RoutingTopology.slot_for_key(_flow_partition_route_key(explicit_partition)) < 512
        else "ferric://leader-b.local:6392"
    )

    assert (
        pool.execute_command(
            "FLOW.CREATE",
            flow_id,
            "TYPE",
            "order",
            "STATE",
            "queued",
            "NOW",
            1,
            "RUN_AT",
            1,
        )
        == expected_auto_url
    )
    assert (
        pool.execute_command(
            "FLOW.CREATE_MANY",
            explicit_partition,
            "TYPE",
            "order",
            "STATE",
            "queued",
            "NOW",
            1,
            "RUN_AT",
            1,
            "ITEMS",
            "flow-many",
            b"payload",
        )
        == expected_explicit_url
    )
    assert (
        pool.execute_command(
            "FLOW.GET",
            flow_id,
            "PAYLOAD",
            "PARTITION",
            explicit_partition,
        )
        == expected_explicit_url
    )
    approval_id = "approval-1"
    expected_approval_url = (
        "ferric://leader-a.local:6391"
        if RoutingTopology.slot_for_key(_flow_partition_route_key(approval_id)) < 512
        else "ferric://leader-b.local:6392"
    )
    assert pool.execute_command("FLOW.APPROVAL.GET", approval_id) == expected_approval_url
    assert pool.execute_command("FLOW.SCHEDULE.GET", "daily-report") == ("ferric://seed.local:6388")
    assert created["ferric://seed.local:6388"].calls == [
        ("SHARDS",),
        ("FLOW.SCHEDULE.GET", "daily-report"),
    ]


def test_topology_rejects_cross_slot_generic_multi_key_command(monkeypatch):
    calls: list[tuple[Any, ...]] = []

    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            calls.append(args)
            return _two_shard_topology() if args[0] == "SHARDS" else b"OK"

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )
    first = next(
        f"left-{index}"
        for index in range(10_000)
        if RoutingTopology.slot_for_key(f"left-{index}") < 512
    )
    second = next(
        f"right-{index}"
        for index in range(10_000)
        if RoutingTopology.slot_for_key(f"right-{index}") >= 512
    )

    with pytest.raises(InvalidCommandError, match="same slot"):
        pool.execute_command("RENAME", first, second)

    assert calls == [("SHARDS",)]


def test_async_topology_connects_cold_learned_adapter_before_direct_request(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAsyncAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.connected = False
            self.ensure_connected_calls = 0

        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology()
            return b"fallback"

        async def _ensure_connected(self) -> None:
            self.ensure_connected_calls += 1
            self.connected = True

        async def _request(self, opcode, lane_id, payload, flags=0):
            if not self.connected:
                raise RuntimeError("request before connect")
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, b"OK")

        def _response_value(self, response: ProtocolResponse) -> Any:
            return response.value

        async def close(self) -> None:
            pass

    def factory(url: str, **_kwargs: Any) -> FakeAsyncAdapter:
        adapter = FakeAsyncAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", factory)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
        )
        assert await pool.execute_command("SET", "tenant-key", b"value") == b"OK"

    asyncio.run(run())

    leader = created["ferric://leader.local:6391"]
    assert leader.ensure_connected_calls == 1


def test_topology_direct_request_preserves_server_route_lane(monkeypatch):
    routed_lanes: list[int] = []

    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology()
                topology["ranges"][0]["lane_id"] = 7
                return topology
            return b"fallback"

        def _request(self, *_args: Any, **_kwargs: Any) -> ProtocolResponse:
            raise AssertionError("topology must use the exact-lane request path")

        def _request_on_lane(
            self,
            opcode: int,
            lane_id: int,
            payload: Any,
            flags: int = 0,
        ) -> ProtocolResponse:
            routed_lanes.append(lane_id)
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, payload)

        @staticmethod
        def _response_value(response: ProtocolResponse) -> Any:
            return response.value

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )

    pool.execute_command("SET", "tenant-key", b"value")

    assert routed_lanes == [7]


def test_async_topology_direct_request_preserves_server_route_lane(monkeypatch):
    routed_lanes: list[int] = []

    class FakeAsyncAdapter:
        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology()
                topology["ranges"][0]["lane_id"] = 7
                return topology
            return b"fallback"

        async def _ensure_connected(self) -> None:
            pass

        async def _request(self, *_args: Any, **_kwargs: Any) -> ProtocolResponse:
            raise AssertionError("topology must use the exact-lane request path")

        async def _request_on_lane(
            self,
            opcode: int,
            lane_id: int,
            payload: Any,
            flags: int = 0,
        ) -> ProtocolResponse:
            routed_lanes.append(lane_id)
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, payload)

        @staticmethod
        def _response_value(response: ProtocolResponse) -> Any:
            return response.value

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAsyncAdapter(),
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
        )
        await pool.refresh_topology()
        await pool.execute_command("SET", "tenant-key", b"value")

    asyncio.run(run())
    assert routed_lanes == [7]


def test_protocol_adapter_pool_from_urls_rejects_untrusted_learned_endpoint(monkeypatch):
    class FakeProtocolAdapter:
        def execute_command(self, *args):
            if args[0] == "SHARDS":
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
                                "node": "other@cluster",
                                "host": "other.local",
                                "native_port": 6388,
                            },
                        }
                    ],
                }
            return b"OK"

        def close(self):
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeProtocolAdapter(),
    )

    pool = ProtocolAdapterPool.from_urls(["ferric://seed.local:6388"])

    with pytest.raises(FerricStoreError, match="unsafe learned endpoint"):
        pool.execute_command("GET", "tenant-key")


def test_protocol_adapter_pool_from_urls_rejects_untrusted_learned_seed_host_port(
    monkeypatch,
):
    created: dict[str, Any] = {}

    class FakeProtocolAdapter:
        def __init__(self, url, **kwargs):
            self.url = url
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args):
            self.calls.append(args)
            if args[0] == "SHARDS":
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
                                "node": "other-port@cluster",
                                "host": "seed.local",
                                "native_port": 6391,
                            },
                        }
                    ],
                }
            return {"url": self.url, "args": args}

        def close(self):
            pass

    def fake_from_url(url, **kwargs):
        adapter = FakeProtocolAdapter(url, **kwargs)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", fake_from_url)

    strict_pool = ProtocolAdapterPool.from_urls(["ferric://seed.local:6388"])
    with pytest.raises(FerricStoreError, match="unsafe learned endpoint"):
        strict_pool.execute_command("GET", "tenant-key")

    trusted_pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        trusted_hosts=["seed.local"],
    )
    result = trusted_pool.execute_command("GET", "tenant-key")
    assert result["url"] == "ferric://seed.local:6391"


def test_tls_topology_reuses_seed_by_its_tls_connection_endpoint(monkeypatch):
    created: dict[str, Any] = {}

    class FakeProtocolAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology("SEED.LOCAL", 6388)
                topology["ranges"][0]["endpoint"]["native_tls_port"] = 6389
                return topology
            return {"url": self.url, "args": args}

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeProtocolAdapter:
        adapter = FakeProtocolAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)

    pool = ProtocolAdapterPool.from_urls(["ferrics://seed.local:6389"])
    result = pool.execute_command("GET", "tenant-key")

    assert result["url"] == "ferrics://seed.local:6389"
    assert list(created) == ["ferrics://seed.local:6389"]


def test_topology_honors_max_connections_per_endpoint(monkeypatch):
    created: dict[str, list[Any]] = {}

    class FakeProtocolAdapter:
        def __init__(self, url: str, index: int) -> None:
            self.url = url
            self.index = index
            self.events: list[Any] = []
            self.ensure_connected_calls = 0

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology("leader.local", 6391)
                topology["ranges"][0]["lane_id"] = 7
                return topology
            return (self.url, self.index, None)

        def execute_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
            return (self.url, self.index, lane_id, args)

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeProtocolAdapter:
        adapters = created.setdefault(url, [])
        adapter = FakeProtocolAdapter(url, len(adapters))
        adapters.append(adapter)
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
        max_connections=2,
    )

    results = [pool.execute_command("SET", "tenant-key", b"value") for _ in range(2)]

    assert len(created["ferric://seed.local:6388"]) == 2
    assert len(created["ferric://leader.local:6391"]) == 2
    assert [(result[1], result[2]) for result in results] == [(0, 7), (1, 7)]
    assert pool._event_poll_fallback is False


def test_async_tls_topology_reuses_seed_by_its_tls_connection_endpoint(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAsyncProtocolAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology("SEED.LOCAL", 6388)
                topology["ranges"][0]["endpoint"]["native_tls_port"] = 6389
                return topology
            return {"url": self.url, "args": args}

        async def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAsyncProtocolAdapter:
        adapter = FakeAsyncProtocolAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(["ferrics://seed.local:6389"])
        await pool.refresh_topology()
        result = await pool.execute_command("GET", "tenant-key")
        assert result["url"] == "ferrics://seed.local:6389"

    asyncio.run(run())
    assert list(created) == ["ferrics://seed.local:6389"]


def test_async_topology_honors_max_connections_per_endpoint(monkeypatch):
    created: dict[str, list[Any]] = {}

    class FakeAsyncProtocolAdapter:
        def __init__(self, url: str, index: int) -> None:
            self.url = url
            self.index = index
            self.events: list[Any] = []
            self.ensure_connected_calls = 0

        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology("leader.local", 6391)
                topology["ranges"][0]["lane_id"] = 7
                return topology
            return (self.url, self.index, None)

        async def execute_command_on_lane(
            self,
            args: tuple[Any, ...],
            lane_id: int,
        ) -> Any:
            return (self.url, self.index, lane_id, args)

        async def _ensure_connected(self) -> None:
            self.ensure_connected_calls += 1

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        async def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAsyncProtocolAdapter:
        adapters = created.setdefault(url, [])
        adapter = FakeAsyncProtocolAdapter(url, len(adapters))
        adapters.append(adapter)
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
            max_connections=2,
            warm_connections=True,
        )
        await pool.refresh_topology()
        results = [await pool.execute_command("SET", "tenant-key", b"value") for _ in range(2)]
        assert [(result[1], result[2]) for result in results] == [(0, 7), (1, 7)]
        assert pool._event_poll_fallback is False

    asyncio.run(run())
    assert len(created["ferric://seed.local:6388"]) == 2
    assert len(created["ferric://leader.local:6391"]) == 2
    assert [
        adapter.ensure_connected_calls for adapter in created["ferric://leader.local:6391"]
    ] == [1, 1]


def test_async_topology_pool_warm_connections_opens_learned_adapters(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAsyncProtocolAdapter:
        def __init__(self, url, **kwargs):
            self.url = url
            self.ensure_connected_calls = 0

        async def execute_command(self, *args):
            if args[0] == "SHARDS":
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
                                "node": "leader@cluster",
                                "host": "leader.local",
                                "native_port": 6391,
                            },
                        }
                    ],
                }
            return b"OK"

        async def _ensure_connected(self):
            self.ensure_connected_calls += 1

        async def close(self):
            pass

    def fake_from_url(url, **kwargs):
        adapter = FakeAsyncProtocolAdapter(url, **kwargs)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", fake_from_url)

    async def run():
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
            warm_connections=True,
        )
        await pool.refresh_topology()

    asyncio.run(run())

    assert created["ferric://leader.local:6391"].ensure_connected_calls == 1


def test_async_topology_warming_has_a_global_concurrency_bound(monkeypatch):
    endpoint_count = 64
    width = 1024 // endpoint_count
    topology = {
        "route_epoch": 1,
        "shard_count": endpoint_count,
        "ranges": [
            {
                "first_slot": index * width,
                "last_slot": (index + 1) * width - 1,
                "shard": index,
                "lane_id": 1,
                "endpoint": {
                    "node": f"leader-{index}@cluster",
                    "host": f"leader-{index}.local",
                    "native_port": 6400 + index,
                },
            }
            for index in range(endpoint_count)
        ],
    }

    class ControlAdapter:
        async def execute_command(self, *_args):
            return topology

    async def run() -> None:
        active = 0
        peak = 0
        warmed = 0
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
            warm_connections=True,
        )
        control = ControlAdapter()
        monkeypatch.setattr(pool, "_adapter_for_url", lambda _url: control)

        async def warm(_endpoint):
            nonlocal active, peak, warmed
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            warmed += 1

        monkeypatch.setattr(pool, "_safe_warm_endpoint", warm)
        await pool.refresh_topology()

        assert warmed == endpoint_count
        assert peak <= 16

    asyncio.run(run())


def test_async_adapter_shutdown_has_a_global_concurrency_bound():
    async def run() -> None:
        active = 0
        peak = 0
        closed = 0

        class Adapter:
            def remove_event_listener(self, _listener):
                pass

            async def close(self):
                nonlocal active, peak, closed
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0)
                active -= 1
                closed += 1

        adapters = [Adapter() for _ in range(64)]
        await protocol_module._close_adapters_async(adapters, lambda: None)

        assert closed == len(adapters)
        assert peak <= 16

    asyncio.run(run())


def test_async_topology_warming_bounds_connections_across_endpoint_pools(monkeypatch):
    endpoint_count = 32
    connections_per_endpoint = 4
    width = 1024 // endpoint_count
    topology = {
        "route_epoch": 1,
        "shard_count": endpoint_count,
        "ranges": [
            {
                "first_slot": index * width,
                "last_slot": (index + 1) * width - 1,
                "shard": index,
                "lane_id": 1,
                "endpoint": {
                    "node": f"leader-{index}@cluster",
                    "host": f"leader-{index}.local",
                    "native_port": 6500 + index,
                },
            }
            for index in range(endpoint_count)
        ],
    }

    class ControlAdapter:
        async def execute_command(self, *_args):
            return topology

    async def run() -> None:
        active = 0
        peak = 0
        connected = 0

        class Connection:
            async def _ensure_connected(self):
                nonlocal active, peak, connected
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0)
                active -= 1
                connected += 1

        class EndpointPool:
            def __init__(self):
                self.adapters = [Connection() for _ in range(connections_per_endpoint)]

            async def _ensure_connected(self):
                await asyncio.gather(*(adapter._ensure_connected() for adapter in self.adapters))

        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
            warm_connections=True,
        )
        control = ControlAdapter()
        endpoint_pools: dict[str, EndpointPool] = {}
        monkeypatch.setattr(pool, "_adapter_for_url", lambda _url: control)
        monkeypatch.setattr(
            pool,
            "_adapter_for_endpoint",
            lambda endpoint: endpoint_pools.setdefault(endpoint["host"], EndpointPool()),
        )

        await pool.refresh_topology()

        assert connected == endpoint_count * connections_per_endpoint
        assert peak <= 16

    asyncio.run(run())


def test_async_adapter_shutdown_bounds_connections_across_endpoint_pools():
    async def run() -> None:
        active = 0
        peak = 0
        closed = 0

        class Connection:
            async def close(self):
                nonlocal active, peak, closed
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0)
                active -= 1
                closed += 1

        class EndpointPool:
            def __init__(self):
                self.adapters = [Connection() for _ in range(4)]

            def remove_event_listener(self, _listener):
                pass

            async def close(self):
                await asyncio.gather(*(adapter.close() for adapter in self.adapters))

        adapters = [EndpointPool() for _ in range(32)]
        await protocol_module._close_adapters_async(adapters, lambda: None)

        assert closed == 128
        assert peak <= 16

    asyncio.run(run())


def test_topology_protocol_adapter_pool_delegates_helper_methods(monkeypatch):
    created: dict[str, Any] = {}

    class FakeProtocolAdapter:
        def __init__(self, url, **kwargs):
            self.url = url
            self.events = [f"event:{url}"]
            self.closed = False

        def execute_command(self, *args):
            if args[0] == "SHARDS":
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
                                "node": "leader@cluster",
                                "host": "leader.local",
                                "native_port": 6391,
                            },
                        }
                    ],
                }
            return {"url": self.url, "args": args}

        def execute_command_with_trace(self, *args):
            return {"value": self.execute_command(*args), "trace": {"url": self.url}}

        def wait_event(self, timeout=None):
            return f"wait:{self.url}"

        def submit_command(self, *args):
            return _future(("submit_command", self.url, args))

        def submit_batch(self, commands):
            return _future([("submit_batch", self.url, commands)])

        def submit_mget(self, keys):
            return _future(("submit_mget", self.url, list(keys)))

        def submit_mset_same_value(self, keys, value):
            return _future(("submit_mset_same_value", self.url, list(keys), value))

        def submit_mset_payload(self, payload):
            return _future(("submit_mset_payload", self.url, payload))

        def submit_pipeline_payload(self, payload, count):
            return _future(("submit_pipeline_payload", self.url, payload, count))

        def submit_flow_many_payload(self, command, payload, count):
            return _future(("submit_flow_many_payload", self.url, command, payload, count))

        def submit_flow_value_mget_payload(self, payload):
            return _future(("submit_flow_value_mget_payload", self.url, payload))

        def subscribe_flow_wake(self, *args, **kwargs):
            return ("subscribe_flow_wake", self.url, args, kwargs)

        def execute_batch(self, commands):
            return [("execute_batch", self.url, commands)]

        def close(self):
            self.closed = True

    def fake_from_url(url, **kwargs):
        adapter = FakeProtocolAdapter(url, **kwargs)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", fake_from_url)

    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )

    assert pool.wait_event(timeout=0) == "wait:ferric://seed.local:6388"
    with pytest.raises(InvalidCommandError):
        pool.pipeline(transaction=True)
    assert pool.pipeline().commands == []

    routed_trace = pool.execute_command_with_trace("SET", "tenant-key", b"value")
    assert routed_trace["trace"] == {"url": "ferric://leader.local:6391"}
    control_trace = pool.execute_command_with_trace("PING")
    assert control_trace["trace"] == {"url": "ferric://seed.local:6388"}
    assert (
        pool.submit_command("SET", "tenant-key", b"value").result()[1]
        == "ferric://leader.local:6391"
    )
    assert [
        item.result()[0] for item in pool.submit_commands([("SET", "tenant-key", b"value")])
    ] == ["submit_command"]
    assert pool.submit_batch([("PING",)]).result()[0][0] == "submit_batch"
    assert pool.submit_mget(["{tenant}:a", "{tenant}:b"]).result()[0] == "submit_mget"
    assert pool.submit_mset_same_value(["{tenant}:a", "{tenant}:b"], b"value").result()[0] == (
        "submit_mset_same_value"
    )
    assert pool.submit_mset_payload(b"payload").result()[:2] == (
        "submit_mset_payload",
        "ferric://leader.local:6391",
    )
    assert pool.submit_pipeline_payload(b"payload", 2).result()[:2] == (
        "submit_pipeline_payload",
        "ferric://leader.local:6391",
    )
    assert pool.submit_flow_many_payload("FLOW.CREATE_MANY", b"payload", 2).result()[:2] == (
        "submit_flow_many_payload",
        "ferric://leader.local:6391",
    )
    assert pool.submit_flow_value_mget_payload(b"payload").result()[:2] == (
        "submit_flow_value_mget_payload",
        "ferric://leader.local:6391",
    )
    assert pool.execute_batch([("PING",)])[0][0] == "execute_batch"
    subscriptions = pool.subscribe_flow_wake("type", worker="worker-1")
    assert {item[1] for item in subscriptions} == {
        "ferric://seed.local:6388",
        "ferric://leader.local:6391",
    }
    assert set(pool.events) == {
        "event:ferric://seed.local:6388",
        "event:ferric://leader.local:6391",
    }

    pool.close()
    assert all(adapter.closed for adapter in created.values())


def test_topology_retries_new_adapter_subscription_after_activation_failure(monkeypatch):
    leader_adapters: list[Any] = []

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False
            self.subscribe_calls = 0
            if "leader.local" in url:
                leader_adapters.append(self)

        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> Any:
            self.subscribe_calls += 1
            if "leader.local" in self.url and len(leader_adapters) == 1:
                raise RuntimeError("transient subscription failure")
            return b"OK"

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )
    pool.subscribe_flow_wake("jobs")

    with pytest.raises(RuntimeError, match="transient subscription failure"):
        pool.execute_command("SET", "tenant-key", b"first")

    assert pool.execute_command("SET", "tenant-key", b"second") == b"OK"
    assert len(leader_adapters) == 2
    assert leader_adapters[0].closed
    assert leader_adapters[1].subscribe_calls == 1


def test_async_topology_retries_adapter_registration_after_activation_failure(monkeypatch):
    leader_adapters: list[Any] = []

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False
            self.registration_calls = 0
            if "leader.local" in url:
                leader_adapters.append(self)

        async def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        async def subscribe_flow_wake(self, *_args: Any, **_kwargs: Any) -> Any:
            return b"OK"

        def register_flow_wake_subscription(self, *_args: Any, **_kwargs: Any) -> None:
            self.registration_calls += 1
            if "leader.local" in self.url and len(leader_adapters) == 1:
                raise RuntimeError("transient registration failure")

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
        )
        await pool.subscribe_flow_wake("jobs")

        with pytest.raises(RuntimeError, match="transient registration failure"):
            await pool.execute_command("SET", "tenant-key", b"first")

        assert await pool.execute_command("SET", "tenant-key", b"second") == b"OK"

        assert len(leader_adapters) == 2
        assert leader_adapters[1].registration_calls == 1
        await pool.close()
        assert leader_adapters[0].closed

    asyncio.run(run())


def test_topology_rejects_opaque_payload_submit_with_multiple_leaders(monkeypatch):
    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            return _two_shard_topology() if args[0] == "SHARDS" else b"OK"

        def submit_pipeline_payload(self, _payload: bytes, _count: int) -> Future[list[Any]]:
            return _future([b"unexpected"])

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )

    with pytest.raises(InvalidCommandError, match="opaque payload"):
        pool.submit_pipeline_payload(b"payload", 1)


def test_topology_rejects_opaque_payload_across_multiple_route_lanes(monkeypatch):
    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _two_shard_topology()
                topology["ranges"][1]["endpoint"] = dict(topology["ranges"][0]["endpoint"])
                topology["ranges"][1]["lane_id"] = 2
                return topology
            return b"OK"

        def submit_pipeline_payload(
            self,
            _payload: bytes,
            _count: int,
        ) -> Future[list[Any]]:
            return _future([b"unexpected"])

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )

    with pytest.raises(InvalidCommandError, match="opaque payload"):
        pool.submit_pipeline_payload(b"payload", 1)


def test_topology_opaque_payload_preserves_its_single_exact_route_lane(monkeypatch):
    lanes: list[int] = []

    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology()
                topology["ranges"][0]["lane_id"] = 7
                return topology
            return b"OK"

        def submit_pipeline_payload(self, _payload: bytes, _count: int) -> Future[list[Any]]:
            raise AssertionError("opaque payload must preserve the topology lane")

        def submit_pipeline_payload_on_lane(
            self,
            _payload: bytes,
            _count: int,
            lane_id: int,
        ) -> Future[list[Any]]:
            lanes.append(lane_id)
            return _future([b"OK"])

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )

    assert pool.submit_pipeline_payload(b"payload", 1).result() == [b"OK"]
    assert lanes == [7]


def test_async_topology_protocol_adapter_pool_delegates_helper_methods(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAsyncProtocolAdapter:
        def __init__(self, url, **kwargs):
            self.url = url
            self.events = [f"event:{url}"]
            self.closed = False

        async def execute_command(self, *args):
            if args[0] == "SHARDS":
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
                                "node": "leader@cluster",
                                "host": "leader.local",
                                "native_port": 6391,
                            },
                        }
                    ],
                }
            return {"url": self.url, "args": args}

        async def execute_command_with_trace(self, *args):
            return {"value": await self.execute_command(*args), "trace": {"url": self.url}}

        async def wait_event(self, timeout=None):
            return f"wait:{self.url}"

        async def execute_batch(self, commands):
            return [("execute_batch", self.url, commands)]

        async def close(self):
            self.closed = True

    def fake_from_url(url, **kwargs):
        adapter = FakeAsyncProtocolAdapter(url, **kwargs)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", fake_from_url)

    async def run():
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
        )
        assert (await pool.route("tenant-key"))["endpoint"]["host"] == "leader.local"
        with pytest.raises(InvalidCommandError):
            pool.pipeline(transaction=True)
        assert pool.pipeline().commands == []

        routed_trace = await pool.execute_command_with_trace("SET", "tenant-key", b"value")
        assert routed_trace["trace"] == {"url": "ferric://leader.local:6391"}
        control_trace = await pool.execute_command_with_trace("PING")
        assert control_trace["trace"] == {"url": "ferric://seed.local:6388"}
        assert (await pool.execute_batch([("PING",)]))[0][0] == "execute_batch"
        assert await pool.wait_event(timeout=0) == "wait:ferric://seed.local:6388"
        assert set(pool.events) == {
            "event:ferric://seed.local:6388",
            "event:ferric://leader.local:6391",
        }
        await pool.close()

    asyncio.run(run())
    assert all(adapter.closed for adapter in created.values())


def _future(value: Any) -> Future[Any]:
    future: Future[Any] = Future()
    future.set_result(value)
    return future


def test_protocol_adapter_pool_refreshes_topology_after_route_failure_without_replay(
    monkeypatch,
):
    created: dict[str, FakeProtocolAdapter] = {}
    shard_epoch = {"value": 0}

    class FakeProtocolAdapter:
        def __init__(self, url, **kwargs):
            self.url = url
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args):
            self.calls.append(args)
            if args[0] == "SHARDS":
                shard_epoch["value"] += 1
                leader = "old.local" if shard_epoch["value"] == 1 else "new.local"
                return {
                    "route_epoch": shard_epoch["value"],
                    "shard_count": 1,
                    "ranges": [
                        {
                            "first_slot": 0,
                            "last_slot": 1023,
                            "shard": 0,
                            "lane_id": 1,
                            "endpoint": {
                                "node": f"{leader}@cluster",
                                "host": leader,
                                "native_port": 6388,
                            },
                        }
                    ],
                }
            if self.url == "ferric://old.local:6388":
                raise FerricStoreError("protocol connection closed")
            return {"url": self.url, "args": args}

        def close(self):
            self.calls.append(("CLOSE",))

    def fake_from_url(url, **kwargs):
        adapter = FakeProtocolAdapter(url, **kwargs)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", fake_from_url)

    pool = ProtocolAdapterPool.from_urls(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )

    with pytest.raises(FerricStoreError, match="connection closed"):
        pool.execute_command("SET", "tenant-key", b"first")

    assert created["ferric://old.local:6388"].calls == [
        ("SET", "tenant-key", b"first"),
        ("CLOSE",),
    ]

    result = pool.execute_command("SET", "tenant-key", b"second")

    assert result["url"] == "ferric://new.local:6388"
    assert created["ferric://new.local:6388"].calls == [("SET", "tenant-key", b"second")]


def test_protocol_connect_keeps_socket_reads_blocking(monkeypatch):
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
    monkeypatch.setattr(ProtocolAdapter, "_reader_loop", lambda self, *_args: None)
    monkeypatch.setattr(ProtocolAdapter, "_request", lambda self, *args, **kwargs: object())
    monkeypatch.setattr(ProtocolAdapter, "_response_value", lambda self, response: b"OK")

    adapter = ProtocolAdapter(timeout=7.5)

    assert captured == {"address": ("127.0.0.1", 6388), "timeout": 7.5}
    assert fake_socket.timeouts == [None]

    adapter.close()


def test_protocol_request_timeout_includes_send_time(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=0.01, heartbeat_interval=None)
    response = ProtocolResponse(1, 0x0101, 1, 0, 0, b"value")

    def slow_submit(*_args, **_kwargs):
        time.sleep(0.02)
        future = Future()
        future.set_result(response)
        return 1, future

    adapter._submit_request = slow_submit

    with pytest.raises(FerricStoreError, match="timed out"):
        adapter._request(0x0101, 1, {"key": "a"})

    adapter.close()


def test_protocol_send_applies_and_restores_send_timeout(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self):
            self.previous_timeout = b"previous"
            self.timeout_options = []

        def getsockopt(self, level, option, size):
            assert (level, option) == (socket.SOL_SOCKET, socket.SO_SNDTIMEO)
            assert size > 0
            return self.previous_timeout

        def setsockopt(self, level, option, value):
            assert (level, option) == (socket.SOL_SOCKET, socket.SO_SNDTIMEO)
            self.timeout_options.append(value)

        def settimeout(self, _value):
            pytest.fail("a write deadline must not change the shared read timeout")

        def sendall(self, _data):
            return None

    adapter = ProtocolAdapter(timeout=0.25, heartbeat_interval=None)
    sock = FakeSocket()
    adapter._sock = sock
    response: Future[ProtocolResponse] = Future()
    adapter._register_pending_request(1, response, binding=(0, sock))

    adapter._send(0x0101, 1, 1, {"key": "a"})
    adapter._discard_pending_request(1, expected_future=response)

    assert len(sock.timeout_options) == 2
    assert sock.timeout_options[0] != sock.previous_timeout
    assert sock.timeout_options[1] == sock.previous_timeout


def test_protocol_send_does_not_copy_large_frame_body(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self) -> None:
            self.writes: list[bytes] = []

        def sendall(self, data: bytes) -> None:
            self.writes.append(data)

        def shutdown(self, _how: int) -> None:
            return None

        def close(self) -> None:
            return None

    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    socket = FakeSocket()
    adapter._sock = socket
    response: Future[ProtocolResponse] = Future()
    adapter._register_pending_request(1, response, binding=(0, socket))
    body = b"x" * (256 * 1024)

    adapter._send(0x0101, 1, 1, body)
    adapter._discard_pending_request(1, expected_future=response)

    assert len(socket.writes) == 2
    assert socket.writes[1] is body
    adapter.close()


def test_protocol_fallback_decoder_uses_original_response_buffer(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    wire_body = protocol_module._STATUS.pack(protocol_module._STATUS_OK) + encode_value(
        {b"key": b"value"}
    )
    wire_header = protocol_module._HEADER.pack(
        protocol_module._MAGIC,
        protocol_module._RESPONSE_VERSION,
        0,
        1,
        0xFFFF,
        1,
        len(wire_body),
    )
    reads = iter((wire_header, wire_body))
    seen: list[tuple[bytes, int]] = []
    original_decode = protocol_module._decode_value_at

    def decode_at(data: bytes, offset: int, **kwargs: Any) -> tuple[Any, int]:
        seen.append((data, offset))
        return original_decode(data, offset, **kwargs)

    monkeypatch.setattr(protocol_response_module, "_decode_value_at", decode_at)
    adapter = ProtocolAdapter(heartbeat_interval=None)
    adapter._recv_exact = lambda _size, _sock=None: next(reads)  # type: ignore[method-assign]

    response = adapter._recv_response()

    assert response.value == {b"key": b"value"}
    assert seen == [(wire_body, protocol_module._STATUS.size)]
    adapter.close()


def test_chunk_accumulator_keeps_large_response_assembly_near_linear_memory():
    from ferricstore.protocol_framing import ResponseBodyAccumulator

    chunk_size = 128 * 1024
    chunk_count = 32
    tracemalloc.start()
    try:
        accumulator = ResponseBodyAccumulator(bytes(chunk_size))
        for index in range(1, chunk_count):
            accumulator.append(bytes([index % 251]) * chunk_size)
        result = accumulator.finish()
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert len(result) == chunk_size * chunk_count
    assert peak < len(result) * 1.6


def test_protocol_adapter_defaults_to_latency_first_lanes(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    adapter = ProtocolAdapter("127.0.0.1", 6388, heartbeat_interval=None)

    assert adapter.lanes == 8

    adapter.close()


def test_async_protocol_adapter_defaults_to_latency_first_lanes():
    adapter = AsyncProtocolAdapter("127.0.0.1", 6388, heartbeat_interval=None)

    assert adapter.lanes == 8


@pytest.mark.parametrize(
    ("option", "invalid"),
    [
        ("max_response_bytes", -1),
        ("max_response_bytes", True),
        ("max_response_bytes", 1.5),
        ("max_response_bytes", "1024"),
        ("max_decompressed_response_bytes", -1),
        ("max_decompressed_response_bytes", False),
        ("max_event_queue_size", -1),
        ("max_event_queue_size", True),
        ("max_decoded_collection_items", -1),
        ("max_decoded_collection_items", 1.5),
        ("max_response_chunks", 0),
        ("max_response_chunks", True),
        ("max_response_chunks", 1.5),
    ],
)
def test_protocol_resource_limits_require_strict_nonnegative_integers(
    option: str,
    invalid: Any,
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    kwargs = {option: invalid, "heartbeat_interval": None}

    with pytest.raises(ValueError, match=option):
        ProtocolAdapter(**kwargs)
    with pytest.raises(ValueError, match=option):
        AsyncProtocolAdapter(**kwargs)


@pytest.mark.parametrize("invalid", [-1, True, 1.5, "1024"])
def test_async_write_drain_limit_requires_a_strict_nonnegative_integer(invalid: Any) -> None:
    with pytest.raises(ValueError, match="write_drain_bytes"):
        AsyncProtocolAdapter(write_drain_bytes=invalid, heartbeat_interval=None)


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

    def reject_unbounded_decompression(*_args, **_kwargs):
        raise AssertionError("the unbounded zlib.decompress API must not be used")

    monkeypatch.setattr(protocol_module.zlib, "decompress", reject_unbounded_decompression)

    with pytest.raises(FerricStoreError, match="max_decompressed_response_bytes"):
        adapter._recv_response()

    adapter.close()


def test_native_protocol_errors_are_classified_into_domain_exceptions():
    response = ProtocolResponse(
        lane_id=1,
        opcode=0x0201,
        request_id=1,
        flags=0,
        status=1,
        value={b"message": b"ERR flow already exists"},
    )

    with pytest.raises(FlowAlreadyExistsError) as exc_info:
        protocol_module._response_value(response)

    assert exc_info.value.raw == response.value


def test_native_pipeline_errors_are_classified_into_domain_exceptions():
    item = [b"error", {b"message": b"ERR flow already exists"}]

    with pytest.raises(FlowAlreadyExistsError) as exc_info:
        protocol_module._batch_item_value(item)

    assert exc_info.value.raw == item


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


def test_protocol_adapter_exact_lane_bypasses_local_round_robin():
    adapter = object.__new__(ProtocolAdapter)
    adapter._lock = threading.Lock()
    adapter._request_id = 0
    adapter._lane_cursor = 0
    adapter.lanes = 8
    adapter._pending = {}
    adapter._pending_traces = {}
    adapter._ensure_connected = lambda: None
    sent_lanes: list[int] = []

    def send(_opcode, lane_id, _request_id, _payload, _flags=0):
        sent_lanes.append(lane_id)
        return None

    adapter._send = send

    adapter._submit_request(0x0101, 7, {"key": "key"}, exact_lane=True)

    assert sent_lanes == [7]


def test_protocol_dedicated_session_can_pin_server_route_lane(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda _self: None)
    adapter = ProtocolAdapter(heartbeat_interval=None)

    session = adapter.acquire_session_on_lane(7)

    assert session._next_lane_id(1) == 7
    assert session._next_lane_id(0) == 0


def test_async_protocol_adapter_exact_lane_bypasses_local_round_robin():
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        adapter._write_lock = asyncio.Lock()
        adapter._request_id = 0
        adapter._lane_cursor = 0
        adapter.lanes = 8
        adapter._pending = {}
        adapter._pending_traces = {}
        sent_lanes: list[int] = []

        async def send(_opcode, lane_id, request_id, _payload, _flags=0):
            sent_lanes.append(lane_id)
            asyncio.get_running_loop().call_soon(
                adapter._pending[request_id].set_result,
                ProtocolResponse(lane_id, 0x0101, request_id, 0, 0, b"value"),
            )
            return None

        adapter._send = send
        response = await adapter._request_without_timeout(
            0x0101,
            7,
            {"key": "key"},
            exact_lane=True,
        )

        assert response.value == b"value"
        assert sent_lanes == [7]

    asyncio.run(run())


def test_async_protocol_dedicated_session_can_pin_server_route_lane():
    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        session = await adapter.acquire_session_on_lane(7)

        assert session._next_lane_id(1) == 7
        assert session._next_lane_id(0) == 0

    asyncio.run(run())


@pytest.mark.parametrize("max_connections", [0, -1])
def test_protocol_pools_reject_non_positive_max_connections(monkeypatch, max_connections: int):
    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: pytest.fail("invalid size must fail before adapter creation"),
    )
    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: pytest.fail("invalid size must fail before adapter creation"),
    )

    with pytest.raises(ValueError, match="max_connections must be positive"):
        ProtocolAdapterPool.from_url(
            "ferric://localhost:6388",
            max_connections=max_connections,
        )
    with pytest.raises(ValueError, match="max_connections must be positive"):
        AsyncProtocolAdapterPool.from_url(
            "ferric://localhost:6388",
            max_connections=max_connections,
        )


@pytest.mark.parametrize("max_connections", [True, 1.5, "3"])
def test_protocol_pools_reject_non_integer_max_connections(
    monkeypatch,
    max_connections: Any,
):
    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: pytest.fail("invalid size must fail before adapter creation"),
    )
    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: pytest.fail("invalid size must fail before adapter creation"),
    )

    with pytest.raises(ValueError, match="max_connections must be a positive integer"):
        ProtocolAdapterPool.from_url(
            "ferric://localhost:6388",
            max_connections=max_connections,
        )
    with pytest.raises(ValueError, match="max_connections must be a positive integer"):
        AsyncProtocolAdapterPool.from_url(
            "ferric://localhost:6388",
            max_connections=max_connections,
        )


@pytest.mark.parametrize("lanes", [0, -1])
def test_protocol_adapters_reject_non_positive_lane_counts(monkeypatch, lanes: int):
    monkeypatch.setattr(
        ProtocolAdapter,
        "_connect",
        lambda _self: pytest.fail("invalid lanes must fail before connecting"),
    )

    with pytest.raises(ValueError, match="lanes must be positive"):
        ProtocolAdapter(lanes=lanes)
    with pytest.raises(ValueError, match="lanes must be positive"):
        AsyncProtocolAdapter(lanes=lanes)


@pytest.mark.parametrize("lanes", [True, 1.5, "3"])
def test_protocol_adapters_reject_non_integer_lane_counts(monkeypatch, lanes: Any):
    monkeypatch.setattr(
        ProtocolAdapter,
        "_connect",
        lambda _self: pytest.fail("invalid lanes must fail before connecting"),
    )

    with pytest.raises(ValueError, match="lanes must be a positive integer"):
        ProtocolAdapter(lanes=lanes)
    with pytest.raises(ValueError, match="lanes must be a positive integer"):
        AsyncProtocolAdapter(lanes=lanes)


@pytest.mark.parametrize("port", [0, 65_536, True, 1.5, "6388"])
def test_direct_protocol_adapters_reject_invalid_ports(monkeypatch, port: Any):
    monkeypatch.setattr(
        ProtocolAdapter,
        "_ensure_connected",
        lambda _self: pytest.fail("invalid port must fail before connecting"),
    )

    for adapter_type in (ProtocolAdapter, AsyncProtocolAdapter):
        with pytest.raises(ValueError, match="port must be between 1 and 65535"):
            adapter_type(port=port)


@pytest.mark.parametrize("host", [None, "", "   ", True, 123])
def test_direct_protocol_adapters_reject_invalid_hosts(monkeypatch, host: Any):
    monkeypatch.setattr(
        ProtocolAdapter,
        "_ensure_connected",
        lambda _self: pytest.fail("invalid host must fail before connecting"),
    )

    for adapter_type in (ProtocolAdapter, AsyncProtocolAdapter):
        with pytest.raises(ValueError, match="host must be a non-empty string"):
            adapter_type(host=host)


def test_all_sync_protocol_wait_event_boundaries_reject_nan() -> None:
    for adapter_type in (
        ProtocolAdapter,
        ProtocolAdapterPool,
        protocol_module.TopologyProtocolAdapterPool,
    ):
        adapter = object.__new__(adapter_type)
        with pytest.raises(ValueError, match="wait_event timeout must be non-negative and finite"):
            adapter.wait_event(timeout=float("nan"))


def test_all_async_protocol_wait_event_boundaries_reject_nan() -> None:
    async def run() -> None:
        for adapter_type in (
            AsyncProtocolAdapter,
            AsyncProtocolAdapterPool,
            protocol_module.AsyncTopologyProtocolAdapterPool,
        ):
            adapter = object.__new__(adapter_type)
            with pytest.raises(
                ValueError,
                match="wait_event timeout must be non-negative and finite",
            ):
                await adapter.wait_event(timeout=float("nan"))

    asyncio.run(run())


def test_protocol_adapter_pool_session_is_affine_and_excluded_from_rotation():
    class FakeProtocolAdapter:
        def __init__(self, name: str):
            self.name = name
            self.calls = []

        def execute_command(self, *args):
            self.calls.append(args)
            return self.name

        @property
        def events(self):
            return []

        def wait_event(self, timeout=None):
            return None

    first = FakeProtocolAdapter("first")
    second = FakeProtocolAdapter("second")
    pool = ProtocolAdapterPool([first, second])

    session = pool.acquire_session()
    assert session.execute_command("MULTI") == "first"
    assert session.execute_command("COMMAND_EXEC", "SET", "k", "v") == "first"
    assert pool.execute_command("PING") == "second"
    assert session.execute_command("EXEC") == "first"
    session.close()

    assert first.calls == [
        ("MULTI",),
        ("COMMAND_EXEC", "SET", "k", "v"),
        ("EXEC",),
    ]
    assert second.calls == [("PING",)]


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


def test_protocol_unknown_command_uses_native_command_exec_fallback():
    command = build_protocol_command("XADD", "stream-1", "*", "field", "value")

    assert command.opcode == 0x0100
    assert command.lane_id == 1
    assert command.payload == {
        "command": "XADD",
        "args": ["stream-1", "*", "field", "value"],
    }


def test_protocol_blocking_zero_timeout_disables_adapter_request_deadline(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=0.01, heartbeat_interval=None)
    captured = {}

    def request(opcode, lane_id, payload, flags=0, **kwargs):
        captured.update(kwargs)
        return ProtocolResponse(lane_id, opcode, 1, flags, 0, None)

    adapter._request = request

    assert adapter.execute_command("BLPOP", "jobs", 0) is None
    assert captured["timeout"] is None


def test_async_protocol_blocking_zero_timeout_disables_adapter_request_deadline():
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        captured = {}

        async def ensure_connected():
            return None

        async def request(opcode, lane_id, payload, flags=0, **kwargs):
            captured.update(kwargs)
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, None)

        adapter._ensure_connected = ensure_connected
        adapter._request = request

        assert await adapter.execute_command("BLPOP", "jobs", 0) is None
        assert captured["timeout"] is None

    asyncio.run(run())


def test_protocol_explicit_command_exec_forces_raw_command_envelope():
    command = build_protocol_command("COMMAND_EXEC", "SET", "k", "v")

    assert command.opcode == 0x0100
    assert command.payload == {"command": "SET", "args": ["k", "v"]}


def test_protocol_module_command_uses_native_command_exec_fallback():
    command = build_protocol_command("BF.ADD", "bf-1", "member-1")

    assert command.opcode == 0x0100
    assert command.payload == {"command": "BF.ADD", "args": ["bf-1", "member-1"]}


def test_protocol_command_exec_fallback_can_carry_request_context():
    command = build_protocol_command(
        "INVOCATION.CREATE",
        "send-email",
        "{}",
        "REQUEST_CONTEXT",
        {
            "subject": "proxy",
            "tenant": "acme",
            "scopes": "invocation:create:* tenant:acme",
        },
    )

    assert command.opcode == 0x0100
    assert command.payload == {
        "command": "INVOCATION.CREATE",
        "args": ["send-email", "{}"],
        "request_context": {
            "subject": "proxy",
            "tenant": "acme",
            "scopes": ["invocation:create:*", "tenant:acme"],
        },
    }


def test_protocol_explicit_command_exec_can_carry_request_context():
    command = build_protocol_command(
        "COMMAND_EXEC",
        "INVOCATION.CREATE",
        "send-email",
        "{}",
        "REQUEST_CONTEXT",
        {"subject": "proxy", "scopes": ["invocation:create:*", "invocation:create:*"]},
    )

    assert command.opcode == 0x0100
    assert command.payload == {
        "command": "INVOCATION.CREATE",
        "args": ["send-email", "{}"],
        "request_context": {
            "subject": "proxy",
            "scopes": ["invocation:create:*"],
        },
    }


def test_protocol_stateful_command_exec_is_not_wrapped_in_pipeline_frame():
    commands = [
        build_protocol_command("MULTI"),
        build_protocol_command("SET", "k", "v"),
        build_protocol_command("EXEC"),
    ]

    assert protocol_module._pipeline_frame_supported(commands) is False

    commands = [
        build_protocol_command("SUBSCRIBE", "jobs"),
        build_protocol_command("UNSUBSCRIBE", "jobs"),
    ]

    assert protocol_module._pipeline_frame_supported(commands) is False

    commands = [build_protocol_command("BLPOP", "jobs", 1)]

    assert protocol_module._pipeline_frame_supported(commands) is False


def test_protocol_control_metadata_builders():
    assert build_protocol_command("OPTIONS").opcode == 0x000B
    assert build_protocol_command("SHARDS").opcode == 0x0007
    assert build_protocol_command("BACKPRESSURE").opcode == 0x0008

    route = build_protocol_command("ROUTE", "key-1")
    assert route.opcode == 0x0006
    assert route.payload == {"key": "key-1"}


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

    monkeypatch.setattr(
        protocol_sync_batch_module,
        "_compact_flow_many_payloads_from_raw",
        fail_flow_many,
    )

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


def test_protocol_pipeline_decodes_flow_many_status_pairs(monkeypatch):
    adapter = object.__new__(ProtocolAdapter)
    record = {b"id": b"f1"}
    responses = iter(
        [
            [["ok", record]],
            [["error", {"message": "create rejected"}]],
        ]
    )

    def request(opcode, lane_id, payload, flags=0):
        return ProtocolResponse(lane_id, opcode, 1, flags, 0, next(responses))

    adapter._request = request
    monkeypatch.setattr(
        protocol_module,
        "_compact_flow_many_payloads_from_raw",
        lambda _commands: [(0x020F, b"compact", 1)],
    )
    client = FlowClient(adapter)

    assert client.pipeline().command("ignored").execute() == [record]
    with pytest.raises(FerricStoreError, match="create rejected"):
        client.pipeline().command("ignored").execute()


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
            {"status": 0, "value": [["ok", b"value"]], "trace": None},
        )()

    adapter._request = request

    assert adapter.execute_batch([("FLOW.GET", "flow-1")]) == [b"value"]
    assert captured["opcode"] == 0x000E
    assert captured["lane_id"] == 1
    assert captured["flags"] == _FLAG_CUSTOM_PAYLOAD
    assert captured["payload"][:6] == struct.pack(">BBI", _COMPACT_PIPELINE_REQUEST, 0x80 | 9, 1)


def test_protocol_values_only_pipeline_preserves_two_item_list_value(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(heartbeat_interval=None)
    monkeypatch.setattr(
        adapter,
        "_request",
        lambda *_args, **_kwargs: ProtocolResponse(
            lane_id=1,
            opcode=protocol_module._OP_PIPELINE,
            request_id=1,
            flags=0,
            status=0,
            value=[[b"ok", b"second"]],
        ),
    )

    assert adapter.execute_batch([("LRANGE", "items", 0, -1)]) == [[b"ok", b"second"]]


def test_protocol_general_pipeline_rejects_short_response(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(heartbeat_interval=None)
    monkeypatch.setattr(
        adapter,
        "_request",
        lambda *_args, **_kwargs: ProtocolResponse(1, 1, 1, 0, 0, []),
    )

    with pytest.raises(FerricStoreError, match="expected 1"):
        adapter.execute_batch([("INCR", "counter")])


def test_async_protocol_values_only_pipeline_preserves_two_item_list_and_cardinality():
    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)

        async def ensure_connected() -> None:
            pass

        async def list_request(*_args: Any, **_kwargs: Any) -> ProtocolResponse:
            return ProtocolResponse(
                lane_id=1,
                opcode=protocol_module._OP_PIPELINE,
                request_id=1,
                flags=0,
                status=0,
                value=[[b"ok", b"second"]],
            )

        adapter._ensure_connected = ensure_connected
        adapter._request = list_request
        assert await adapter.execute_batch([("LRANGE", "items", 0, -1)]) == [[b"ok", b"second"]]

        async def short_request(*_args: Any, **_kwargs: Any) -> ProtocolResponse:
            return ProtocolResponse(1, 1, 1, 0, 0, [])

        adapter._request = short_request
        with pytest.raises(FerricStoreError, match="expected 1"):
            await adapter.execute_batch([("INCR", "counter")])

    asyncio.run(run())


def test_protocol_execute_batch_does_not_pipeline_connection_stateful_commands(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    calls = []

    def execute_command(*args):
        calls.append(args)
        return args[0]

    adapter.execute_command = execute_command
    adapter._request = lambda *_args, **_kwargs: pytest.fail("stateful commands were pipelined")

    assert adapter.execute_batch([("BLPOP", "jobs", 1), ("GET", "k")]) == ["BLPOP", "GET"]
    assert calls == [("BLPOP", "jobs", 1), ("GET", "k")]


def test_protocol_flow_payload_flag_uses_shared_option_boundaries() -> None:
    command = build_protocol_command(
        "FLOW.GET",
        "flow-1",
        "PAYLOAD",
        "PARTITION",
        "tenant-a",
    )
    binary_payload = build_protocol_command(
        "FLOW.GET",
        "flow-2",
        "PAYLOAD",
        b"PARTITION",
        "PARTITION",
        "tenant-b",
    )
    binary_options = build_protocol_command(
        b"FLOW.GET",
        b"flow-3",
        b"PAYLOAD",
        b"PARTITION",
        b"tenant-c",
    )

    assert command.payload == {
        "id": "flow-1",
        "payload": True,
        "partition_key": "tenant-a",
    }
    assert binary_payload.payload == {
        "id": "flow-2",
        "payload": b"PARTITION",
        "partition_key": "tenant-b",
    }
    assert binary_options.payload == {
        "id": b"flow-3",
        "payload": True,
        "partition_key": b"tenant-c",
    }


@pytest.mark.parametrize("reserved_payload", [b"ITEMS", b"ITEMS_EXT"])
def test_protocol_flow_many_item_marker_ignores_opaque_payload_values(
    reserved_payload: bytes,
) -> None:
    command = build_protocol_command(
        "FLOW.COMPLETE_MANY",
        "tenant",
        "PAYLOAD",
        reserved_payload,
        "NOW",
        123,
        "ITEMS",
        "job-1",
        b"lease",
        7,
    )

    assert command.payload == {
        "payload": reserved_payload,
        "now_ms": 123,
        "partition_key": "tenant",
        "items": [["job-1", b"lease", 7]],
    }


@pytest.mark.parametrize("state", ["ITEMS", "ITEMS_EXT"])
def test_protocol_flow_many_item_marker_ignores_option_values(state: str) -> None:
    command = build_protocol_command(
        "FLOW.CREATE_MANY",
        "AUTO",
        "TYPE",
        "order",
        "STATE",
        state,
        "NOW",
        123,
        "ITEMS",
        "job-1",
        b"payload",
    )

    assert command.payload == {
        "type": "order",
        "state": state,
        "now_ms": 123,
        "items": [["job-1", b"payload"]],
    }


def test_protocol_flow_value_mget_only_treats_typed_trailing_limit_as_option() -> None:
    keyword_ref = ("FLOW.VALUE.MGET", "MAX_BYTES")
    keyword_and_ref = ("FLOW.VALUE.MGET", "MAX_BYTES", "ref-2")
    limited = ("FLOW.VALUE.MGET", "MAX_BYTES", "ref-2", "MAX_BYTES", 10)

    for args, expected_refs in (
        (keyword_ref, 1),
        (keyword_and_ref, 2),
        (limited, 2),
    ):
        command = build_protocol_command(*args)
        assert _expected_command_collection_items(args) == expected_refs
        assert _expected_payload_collection_items(command.opcode, command.payload) == expected_refs

    assert command_route_keys(keyword_ref[0], keyword_ref[1:]) == ("MAX_BYTES",)
    assert command_route_keys(keyword_and_ref[0], keyword_and_ref[1:]) == (
        "MAX_BYTES",
        "ref-2",
    )
    assert command_route_keys(limited[0], limited[1:]) == ("MAX_BYTES", "ref-2")


def test_protocol_create_many_cardinality_uses_grammar_item_marker() -> None:
    args = (
        "FLOW.CREATE_MANY",
        "MIXED",
        "TYPE",
        "order",
        "STATE",
        "ITEMS",
        "NOW",
        123,
        "ITEMS",
        "job-1",
        "tenant",
        b"payload",
    )

    command = build_protocol_command(*args)

    assert _expected_command_collection_items(args) == 1
    assert _expected_payload_collection_items(command.opcode, command.payload) == 1


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("XREADGROUP", "GROUP", "BLOCK", 0, "STREAMS", "orders", ">"), False),
        (
            (
                "XREADGROUP",
                "GROUP",
                "BLOCK",
                "worker",
                "BLOCK",
                0,
                "STREAMS",
                "orders",
                ">",
            ),
            True,
        ),
        (("XREAD", "STREAMS", "BLOCK", 0), False),
        (("XREAD", "BLOCK", 0, "STREAMS", "orders", "$"), True),
    ],
)
def test_protocol_blocking_timeout_uses_stream_command_grammar(
    args: tuple[Any, ...], expected: bool
) -> None:
    assert _blocks_forever(args) is expected


def test_protocol_trace_and_submit_preserve_infinite_blocking_timeout() -> None:
    trace_adapter = object.__new__(ProtocolAdapter)
    trace_call: dict[str, Any] = {}

    def request(*_args: Any, **kwargs: Any) -> ProtocolResponse:
        trace_call.update(kwargs)
        return ProtocolResponse(1, 0, 1, 0, 0, b"value", trace={})

    trace_adapter._request = request
    trace_adapter._response_value = lambda response: response.value

    assert trace_adapter.execute_command_with_trace("BLPOP", "jobs", 0)["value"] == b"value"
    assert trace_call["timeout"] is None
    trace_call.clear()
    trace_adapter._request_on_lane = request
    assert (
        trace_adapter.execute_command_with_trace_on_lane(("BLPOP", "jobs", 0), 7)["value"]
        == b"value"
    )
    assert trace_call["timeout"] is None

    submit_adapter = object.__new__(ProtocolAdapter)
    submit_call: dict[str, Any] = {}
    response_future: Future[ProtocolResponse] = Future()

    def submit(*_args: Any, **kwargs: Any) -> tuple[int, Future[ProtocolResponse]]:
        submit_call.update(kwargs)
        return 1, response_future

    submit_adapter._submit_request = submit
    submit_adapter._value_future = lambda future: future

    assert submit_adapter.submit_command("BLPOP", "jobs", 0) is response_future
    assert submit_call["_expire_at_adapter_timeout"] is False
    submit_call.clear()
    submit_adapter._submit_request_on_lane = submit
    assert submit_adapter.submit_command_on_lane(("BLPOP", "jobs", 0), 7) is response_future
    assert submit_call["_expire_at_adapter_timeout"] is False


def test_async_protocol_trace_preserves_infinite_blocking_timeout() -> None:
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        call: dict[str, Any] = {}

        async def request(*_args: Any, **kwargs: Any) -> ProtocolResponse:
            call.update(kwargs)
            return ProtocolResponse(1, 0, 1, 0, 0, b"value", trace={})

        adapter._request = request
        adapter._response_value = lambda response: response.value

        result = await adapter.execute_command_with_trace("BLPOP", "jobs", 0)

        assert result["value"] == b"value"
        assert call["timeout"] is None
        call.clear()
        adapter._request_on_lane = request
        result = await adapter.execute_command_with_trace_on_lane(("BLPOP", "jobs", 0), 7)
        assert result["value"] == b"value"
        assert call["timeout"] is None

    asyncio.run(run())


def test_protocol_flow_option_parser_handles_deep_opaque_keyword_payloads() -> None:
    args: list[Any] = [
        "FLOW.CREATE",
        "flow-1",
        "TYPE",
        "order",
        "STATE",
        "queued",
        "PAYLOAD",
        b"NOW",
    ]
    for index in range(1_100):
        args.extend(("ATTRIBUTE", f"key-{index}", f"value-{index}"))

    command = build_protocol_command(*args)

    assert command.payload["payload"] == b"NOW"
    assert len(command.payload["attributes"]) == 1_100


def test_protocol_command_only_tokens_are_semantic_options_not_opaque_values() -> None:
    get = build_protocol_command("FLOW.GET", "STATE_META")
    create = build_protocol_command(
        "FLOW.CREATE",
        "flow-1",
        "TYPE",
        "order",
        "STATE",
        "queued",
        "PAYLOAD",
        b"STATE_META",
    )
    command_only = build_protocol_command(
        "FLOW.CREATE",
        "flow-1",
        "TYPE",
        "order",
        "STATE",
        "queued",
        "STATE_META",
        "version",
        1,
    )

    assert get.opcode != _OP_COMMAND_EXEC
    assert get.payload == {"id": "STATE_META"}
    assert create.opcode != _OP_COMMAND_EXEC
    assert create.payload["payload"] == b"STATE_META"
    assert command_only.opcode == _OP_COMMAND_EXEC


def test_topology_routed_command_builds_protocol_payload_once(monkeypatch: Any) -> None:
    class InMemoryAdapter(ProtocolAdapter):
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False

        def _request(
            self,
            opcode: int,
            lane_id: int,
            _payload: Any,
            flags: int = 0,
            **_kwargs: Any,
        ) -> ProtocolResponse:
            if opcode == 0x0007:
                value: Any = _single_shard_topology()
            elif opcode == 0x020F:
                value = [b"OK", b"OK"]
            else:
                value = b"OK"
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, value)

        def _request_on_lane(self, *args: Any, **kwargs: Any) -> ProtocolResponse:
            return self._request(*args, **kwargs)

        def _response_value(self, response: ProtocolResponse) -> Any:
            return response.value

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def register_flow_wake_subscription(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: InMemoryAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    calls = 0
    original = build_protocol_command

    def counted(*args: Any) -> ProtocolCommand:
        nonlocal calls
        calls += 1
        return original(*args)

    monkeypatch.setattr(protocol_sync_topology_module, "build_protocol_command", counted)
    monkeypatch.setattr(protocol_sync_module, "build_protocol_command", counted)
    monkeypatch.setattr(protocol_commands_module, "build_protocol_command", counted)
    try:
        assert pool.execute_command("FLOW.GET", "flow-1") == b"OK"
        assert calls == 1
        calls = 0
        assert pool.execute_batch(
            [
                (
                    "FLOW.CREATE",
                    f"flow-{index}",
                    "TYPE",
                    "order",
                    "STATE",
                    "queued",
                    "NOW",
                    123,
                    "RUN_AT",
                    123,
                    "PAYLOAD",
                    b"payload",
                )
                for index in range(2)
            ]
        ) == [b"OK", b"OK"]
    finally:
        pool.close()

    assert calls == 2


def test_async_topology_routed_command_builds_protocol_payload_once(monkeypatch: Any) -> None:
    class InMemoryAdapter(AsyncProtocolAdapter):
        def __init__(self, url: str) -> None:
            self.url = url

        async def _request(
            self,
            opcode: int,
            lane_id: int,
            _payload: Any,
            flags: int = 0,
            **_kwargs: Any,
        ) -> ProtocolResponse:
            if opcode == 0x0007:
                value: Any = _single_shard_topology()
            elif opcode == 0x020F:
                value = [b"OK", b"OK"]
            else:
                value = b"OK"
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, value)

        async def _request_on_lane(self, *args: Any, **kwargs: Any) -> ProtocolResponse:
            return await self._request(*args, **kwargs)

        def _response_value(self, response: ProtocolResponse) -> Any:
            return response.value

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def register_flow_wake_subscription(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: InMemoryAdapter(url),
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        calls = 0
        original = build_protocol_command

        def counted(*args: Any) -> ProtocolCommand:
            nonlocal calls
            calls += 1
            return original(*args)

        monkeypatch.setattr(protocol_async_topology_module, "build_protocol_command", counted)
        monkeypatch.setattr(protocol_commands_module, "build_protocol_command", counted)
        try:
            assert await pool.execute_command("FLOW.GET", "flow-1") == b"OK"
            assert calls == 1
            calls = 0
            assert await pool.execute_batch(
                [
                    (
                        "FLOW.CREATE",
                        f"flow-{index}",
                        "TYPE",
                        "order",
                        "STATE",
                        "queued",
                        "NOW",
                        123,
                        "RUN_AT",
                        123,
                        "PAYLOAD",
                        b"payload",
                    )
                    for index in range(2)
                ]
            ) == [b"OK", b"OK"]
        finally:
            await pool.close()

        assert calls == 2

    asyncio.run(run())


def test_protocol_execute_batch_compacts_partitioned_flow_get(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter("127.0.0.1", 6388)
    captured = {}

    def request(opcode, lane_id, payload, flags=0):
        captured["opcode"] = opcode
        captured["lane_id"] = lane_id
        captured["payload"] = payload
        captured["flags"] = flags
        return type("Response", (), {"status": 0, "value": [["ok", b"value"]], "trace": None})()

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
        return type("Response", (), {"status": 0, "value": [["ok", b"value"]], "trace": None})()

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
        return type("Response", (), {"status": 0, "value": [["ok", b"value"]], "trace": None})()

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


def test_protocol_builds_control_opcode_payloads():
    assert build_protocol_command("HELLO", "ROLE", "sdk") == ProtocolCommand(
        0x0001,
        {"role": "sdk"},
        0,
    )
    assert build_protocol_command("AUTH", "platform", "secret") == ProtocolCommand(
        0x0002,
        {"username": "platform", "password": "secret"},
        0,
    )
    assert build_protocol_command("STARTUP", "CLIENT", "python") == ProtocolCommand(
        0x000C,
        {"client": "python"},
        0,
    )
    assert build_protocol_command("WINDOW_UPDATE", "LANE", 1, "CREDIT", 64) == ProtocolCommand(
        0x000D,
        {"lane": 1, "credit": 64},
        0,
    )
    assert build_protocol_command("ROUTE_BATCH", "key-1", "key-2") == ProtocolCommand(
        0x000F,
        {"keys": ["key-1", "key-2"]},
        0,
    )


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


def test_protocol_builds_flow_search_with_attributes_and_state_meta():
    command = build_protocol_command(
        "FLOW.SEARCH",
        "email",
        "STATE",
        "queued",
        "ATTRIBUTE",
        "tenant",
        "acme",
        "STATE_META",
        "queued",
        {"version": 3},
        "TERMINAL_ONLY",
        "true",
    )

    assert command.opcode == 0x0230
    assert command.flags == 0
    assert command.payload == {
        "type": "email",
        "state": "queued",
        "attributes": {"tenant": "acme"},
        "state_meta": {"queued": {"version": 3}},
        "terminal_only": True,
    }

    flat_meta = build_protocol_command(
        "FLOW.SEARCH",
        "email",
        "STATE",
        "queued",
        "STATE_META",
        "version",
        3,
    )
    assert flat_meta.payload == {
        "type": "email",
        "state": "queued",
        "state_meta": {"queued": {"version": 3}},
    }


def test_protocol_builds_flow_state_meta_and_indexed_policy_payloads():
    create = build_protocol_command(
        "FLOW.CREATE",
        "f1",
        "TYPE",
        "order",
        "STATE",
        "accept",
        "STATE_META",
        "version",
        1,
    )
    assert create == ProtocolCommand(
        _OP_COMMAND_EXEC,
        {
            "command": "FLOW.CREATE",
            "args": [
                "f1",
                "TYPE",
                "order",
                "STATE",
                "accept",
                "STATE_META",
                "version",
                1,
            ],
        },
        1,
    )

    complete = build_protocol_command(
        "FLOW.COMPLETE",
        "f1",
        b"lease",
        "FENCING",
        7,
        "STATE_META",
        "version",
        3,
    )
    assert complete == ProtocolCommand(
        _OP_COMMAND_EXEC,
        {
            "command": "FLOW.COMPLETE",
            "args": ["f1", b"lease", "FENCING", 7, "STATE_META", "version", 3],
        },
        1,
    )

    policy = build_protocol_command("FLOW.POLICY.SET", "order", "INDEXED_STATE_META", "version")
    assert policy == ProtocolCommand(
        _OP_COMMAND_EXEC,
        {"command": "FLOW.POLICY.SET", "args": ["order", "INDEXED_STATE_META", "version"]},
        1,
    )


def test_protocol_builds_native_flow_policy_state_modes_payload():
    policy = build_protocol_command(
        "FLOW.POLICY.SET",
        "order",
        "STATE",
        "queued",
        "MODE",
        "FIFO",
        "MAX_RETRIES",
        5,
        "STATE",
        "ready",
        "MODE",
        "PARALLEL",
    )

    assert policy == ProtocolCommand(
        protocol_module._OPCODES["FLOW.POLICY.SET"],
        {
            "type": "order",
            "states": {
                "queued": {"mode": "FIFO", "max_retries": 5},
                "ready": {"mode": "PARALLEL"},
            },
        },
    )


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


def test_protocol_rejects_duplicate_generic_map_keys_while_decoding():
    payload = (
        b"\x06"
        + struct.pack(">I", 2)
        + struct.pack(">I", 4)
        + b"same"
        + encode_value(b"first")
        + struct.pack(">I", 4)
        + b"same"
        + encode_value(b"second")
    )

    with pytest.raises(FerricStoreError, match="duplicate protocol map key"):
        decode_value(payload)


def test_protocol_rejects_duplicate_compact_flow_record_keys():
    record = compact_flow_record([(1, b"flow-1"), (1, b"flow-2")])

    with pytest.raises(FerricStoreError, match="duplicate protocol map key"):
        _try_fast_response_value_at(0x0202, record, 0)


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


def test_protocol_rejects_duplicate_compact_binary_map_list_keys():
    payload = (
        b"\x87"
        + struct.pack(">I", 1)
        + struct.pack(">I", 2)
        + struct.pack(">I", 1)
        + b"f"
        + struct.pack(">I", 3)
        + b"one"
        + struct.pack(">I", 1)
        + b"f"
        + struct.pack(">I", 3)
        + b"two"
    )

    with pytest.raises(FerricStoreError, match="duplicate protocol map key"):
        _try_fast_response_value_at(0x000E, payload, 0)


def test_protocol_rejects_duplicate_compact_pipeline_map_keys():
    payload = (
        bytes([_COMPACT_PIPELINE_RESPONSE])
        + struct.pack(">I", 1)
        + b"\x00\x07"
        + struct.pack(">I", 2)
        + struct.pack(">I", 1)
        + b"f"
        + struct.pack(">I", 3)
        + b"one"
        + struct.pack(">I", 1)
        + b"f"
        + struct.pack(">I", 3)
        + b"two"
    )

    with pytest.raises(FerricStoreError, match="duplicate protocol map key"):
        _try_fast_response_value_at(0x000E, payload, 0)


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


def test_protocol_flow_wake_reconnect_registration_is_last_write_wins():
    adapter = object.__new__(ProtocolAdapter)
    adapter._subscription_lock = threading.Lock()
    adapter._flow_wake_subscriptions = []
    async_adapter = object.__new__(AsyncProtocolAdapter)
    async_adapter._flow_wake_subscriptions = []

    for target in (adapter, async_adapter):
        target.register_flow_wake_subscription("jobs-a", state="queued")
        target.register_flow_wake_subscription("jobs-b", state="queued")
        target.register_flow_wake_subscription("jobs-a", state="queued")

        assert len(target._flow_wake_subscriptions) == 1
        assert target._flow_wake_subscriptions[0]["flow_wake"]["type"] == "jobs-a"


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


def test_protocol_adapter_reconnects_after_server_closes_idle_socket():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    received: list[tuple[int, int, Any]] = []
    first_connection_closed = threading.Event()

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
            first, _addr = listener.accept()
            with first:
                startup_opcode, startup_lane, startup_id, startup_payload = recv_frame(first)
                received.append((startup_opcode, startup_lane, startup_payload))
                first.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))
            first_connection_closed.set()

            second, _addr = listener.accept()
            with second:
                startup_opcode, startup_lane, startup_id, startup_payload = recv_frame(second)
                received.append((startup_opcode, startup_lane, startup_payload))
                second.sendall(response(startup_opcode, startup_lane, startup_id, {"ok": True}))

                get_opcode, get_lane, get_id, get_payload = recv_frame(second)
                received.append((get_opcode, get_lane, get_payload))
                second.sendall(response(get_opcode, get_lane, get_id, b"value-after-reconnect"))

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
        assert first_connection_closed.wait(timeout=1.0)
        deadline = time.monotonic() + 1.0
        while adapter._sock is not None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert adapter._sock is None

        assert adapter.execute_command("GET", "k") == b"value-after-reconnect"
    finally:
        adapter.close()
        thread.join(timeout=1.0)

    assert received[0][0] == 0x000C
    assert received[1][0] == 0x000C
    assert received[2] == (0x0101, 1, {b"key": b"k"})


def test_protocol_adapter_sync_reconnect_is_single_flight():
    adapter = object.__new__(ProtocolAdapter)
    adapter._sock = None
    adapter._closed = False
    adapter._connecting = False
    adapter._connect_lock = threading.Lock()

    calls = 0
    calls_lock = threading.Lock()
    start = threading.Barrier(9)

    def fake_connect() -> None:
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.03)
        adapter._sock = object()

    adapter._connect = fake_connect

    def worker() -> None:
        start.wait(timeout=1.0)
        adapter._ensure_connected()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    start.wait(timeout=1.0)
    for thread in threads:
        thread.join(timeout=1.0)

    assert all(not thread.is_alive() for thread in threads)
    assert calls == 1


def test_protocol_adapter_concurrent_caller_waits_for_socket_publication():
    adapter = object.__new__(ProtocolAdapter)
    adapter._sock = None
    adapter._closed = False
    adapter._connecting = False
    adapter._connect_lock = threading.Lock()
    connect_entered = threading.Event()
    release_connect = threading.Event()
    errors: list[BaseException] = []

    def fake_connect() -> None:
        adapter._connecting = True
        connect_entered.set()
        assert release_connect.wait(timeout=1.0)
        adapter._sock = cast(Any, object())
        adapter._connecting = False

    adapter._connect = fake_connect

    def ensure_connected() -> None:
        try:
            adapter._ensure_connected()
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=ensure_connected)
    second = threading.Thread(target=ensure_connected)
    first.start()
    assert connect_entered.wait(timeout=1.0)
    second.start()
    time.sleep(0.02)

    assert second.is_alive()
    release_connect.set()
    first.join(timeout=1.0)
    second.join(timeout=1.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []


def test_protocol_adapter_concurrent_caller_waits_for_handshake_readiness():
    adapter = object.__new__(ProtocolAdapter)
    adapter._sock = None
    adapter._closed = False
    adapter._connecting = False
    adapter._connect_lock = threading.Lock()
    socket_published = threading.Event()
    release_handshake = threading.Event()
    second_returned = threading.Event()
    errors: list[BaseException] = []

    def fake_connect() -> None:
        adapter._connecting = True
        adapter._sock = cast(Any, object())
        socket_published.set()
        assert release_handshake.wait(timeout=1.0)
        adapter._connecting = False

    adapter._connect = fake_connect

    def first_caller() -> None:
        try:
            adapter._ensure_connected()
        except BaseException as exc:
            errors.append(exc)

    def second_caller() -> None:
        try:
            adapter._ensure_connected()
        except BaseException as exc:
            errors.append(exc)
        finally:
            second_returned.set()

    first = threading.Thread(target=first_caller)
    second = threading.Thread(target=second_caller)
    first.start()
    assert socket_published.wait(timeout=1.0)
    second.start()

    assert not second_returned.wait(timeout=0.05)
    release_handshake.set()
    first.join(timeout=1.0)
    second.join(timeout=1.0)

    assert second_returned.is_set()
    assert errors == []


def test_protocol_adapter_stale_reader_does_not_close_newer_socket():
    class FakeSocket:
        def __init__(self) -> None:
            self.closed = False

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = object.__new__(ProtocolAdapter)
    old_socket = FakeSocket()
    new_socket = FakeSocket()
    adapter._sock = old_socket
    adapter._pending = {}
    adapter._pending_traces = {}
    adapter._closed = False
    entered = threading.Event()
    proceed = threading.Event()

    def fake_recv_response(*_args: Any, **_kwargs: Any) -> ProtocolResponse:
        entered.set()
        assert proceed.wait(timeout=1.0)
        raise FerricStoreError("old reader failed")

    adapter._recv_response = fake_recv_response

    thread = threading.Thread(target=adapter._reader_loop)
    thread.start()
    assert entered.wait(timeout=1.0)
    adapter._sock = new_socket
    proceed.set()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert adapter._sock is new_socket
    assert not new_socket.closed


def test_protocol_adapter_stale_heartbeat_does_not_close_newer_socket():
    class FakeSocket:
        def __init__(self) -> None:
            self.closed = False

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = object.__new__(ProtocolAdapter)
    old_socket = FakeSocket()
    new_socket = FakeSocket()
    adapter._sock = old_socket
    adapter.heartbeat_interval = 0.001
    adapter.heartbeat_timeout = 0.001
    adapter._last_activity = 0.0
    adapter._pending = {}
    adapter._pending_traces = {}
    adapter._closed = False

    def fake_submit_request(*_args: Any, **_kwargs: Any) -> tuple[int, Future[ProtocolResponse]]:
        adapter._sock = new_socket
        raise FerricStoreError("old heartbeat failed")

    adapter._submit_request = fake_submit_request

    adapter._heartbeat_loop()

    assert adapter._sock is new_socket
    assert not new_socket.closed


def test_protocol_adapter_restarting_heartbeat_wakes_previous_thread():
    adapter = object.__new__(ProtocolAdapter)
    adapter._sock = object()
    adapter.heartbeat_interval = 60.0
    adapter.heartbeat_timeout = 1.0
    adapter._heartbeat_thread = None
    adapter._heartbeat_stop = None

    adapter._start_heartbeat()
    previous = adapter._heartbeat_thread
    assert previous is not None

    adapter._start_heartbeat()
    previous.join(timeout=0.1)
    adapter._sock = None
    if adapter._heartbeat_stop is not None:
        adapter._heartbeat_stop.set()

    assert not previous.is_alive()


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


def test_async_protocol_adapter_pool_session_is_affine_and_excluded_from_rotation():
    class FakeAsyncProtocolAdapter:
        def __init__(self, name: str):
            self.name = name
            self.calls = []

        async def execute_command(self, *args):
            self.calls.append(args)
            return self.name

        @property
        def events(self):
            return []

        async def wait_event(self, timeout=None):
            return None

    async def run():
        first = FakeAsyncProtocolAdapter("first")
        second = FakeAsyncProtocolAdapter("second")
        pool = AsyncProtocolAdapterPool([first, second])

        session = await pool.acquire_session()
        assert await session.execute_command("MULTI") == "first"
        assert await session.execute_command("COMMAND_EXEC", "SET", "k", "v") == "first"
        assert await pool.execute_command("PING") == "second"
        assert await session.execute_command("EXEC") == "first"
        await session.close()

        assert first.calls == [
            ("MULTI",),
            ("COMMAND_EXEC", "SET", "k", "v"),
            ("EXEC",),
        ]
        assert second.calls == [("PING",)]

    asyncio.run(run())


def test_async_protocol_pool_cancelled_session_close_still_releases_lease():
    class FakeAsyncProtocolAdapter:
        def __init__(self) -> None:
            self.listeners = []

        @property
        def events(self):
            return []

        def add_event_listener(self, listener):
            self.listeners.append(listener)

        def remove_event_listener(self, listener):
            self.listeners.remove(listener)

        async def close(self):
            pass

    async def run():
        pool = AsyncProtocolAdapterPool([FakeAsyncProtocolAdapter()])
        session = await pool.acquire_session()

        await pool._condition.acquire()
        closing = asyncio.create_task(session.close())
        await asyncio.sleep(0)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        pool._condition.release()

        await session.close()
        assert pool._leased == set()
        replacement = await asyncio.wait_for(pool.acquire_session(), timeout=0.2)
        await replacement.close()
        await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "marker",
    [
        protocol_module._COMPACT_OK_LIST,
        protocol_module._COMPACT_KV_MGET_FIXED,
        protocol_module._COMPACT_BINARY_LIST_LIST,
        protocol_module._COMPACT_BINARY_MAP_LIST,
        protocol_module._COMPACT_FLOW_CLAIM_JOBS,
        protocol_module._COMPACT_FLOW_RECORD_LIST,
    ],
)
def test_compact_response_rejects_oversized_collection_before_allocation(marker):
    payload = bytes([marker]) + struct.pack(">I", 101)
    if marker == protocol_module._COMPACT_KV_MGET_FIXED:
        payload += struct.pack(">I", 0)

    with pytest.raises(FerricStoreError, match="max_decoded_collection_items"):
        _try_fast_response_value_at(
            protocol_module._OP_PIPELINE,
            payload,
            0,
            max_collection_items=100,
        )


def test_compact_response_rejects_count_that_exceeds_request_cardinality():
    payload = bytes([protocol_module._COMPACT_OK_LIST]) + struct.pack(">I", 3)

    with pytest.raises(FerricStoreError, match="expected 2"):
        _try_fast_response_value_at(
            protocol_module._OP_PIPELINE,
            payload,
            0,
            expected_collection_items=2,
        )


@pytest.mark.parametrize(
    "opcode",
    [
        protocol_module._OP_MGET,
        protocol_module._OP_PIPELINE,
        protocol_module._OP_FLOW_VALUE_MGET,
    ],
)
def test_generic_collection_response_enforces_request_cardinality(opcode: int) -> None:
    class Adapter:
        max_decoded_collection_items = 100

        def __init__(self) -> None:
            self._pending_response_item_counts = {7: 2}

    body = protocol_module._STATUS.pack(protocol_module._STATUS_OK) + encode_value([b"only"])

    with pytest.raises(FerricStoreError, match="returned 1 items; expected 2"):
        protocol_response_module._decode_protocol_response(
            Adapter(),
            lane_id=1,
            opcode=opcode,
            request_id=7,
            flags=0,
            body=body,
            read_started_ns=0,
            read_done_ns=0,
        )


def test_generic_collection_response_validates_after_trace_unwrap() -> None:
    class Adapter:
        max_decoded_collection_items = 100

        def __init__(self) -> None:
            self._pending_response_item_counts = {7: 2}

    body = protocol_module._STATUS.pack(protocol_module._STATUS_OK) + encode_value(
        {b"value": [b"only"], b"trace": {b"worker_us": 1}}
    )

    with pytest.raises(FerricStoreError, match="returned 1 items; expected 2"):
        protocol_response_module._decode_protocol_response(
            Adapter(),
            lane_id=1,
            opcode=protocol_module._OP_MGET,
            request_id=7,
            flags=protocol_module._FLAG_TRACE,
            body=body,
            read_started_ns=0,
            read_done_ns=0,
        )


def test_generic_collection_response_rejects_scalar_for_exact_contract() -> None:
    class Adapter:
        max_decoded_collection_items = 100

        def __init__(self) -> None:
            self._pending_response_item_counts = {7: 2}

    body = protocol_module._STATUS.pack(protocol_module._STATUS_OK) + encode_value(b"OK")

    with pytest.raises(FerricStoreError, match="scalar; expected 2 items"):
        protocol_response_module._decode_protocol_response(
            Adapter(),
            lane_id=1,
            opcode=protocol_module._OP_MGET,
            request_id=7,
            flags=0,
            body=body,
            read_started_ns=0,
            read_done_ns=0,
        )


def test_generic_collection_response_accepts_exact_request_cardinality() -> None:
    class Adapter:
        max_decoded_collection_items = 100

        def __init__(self) -> None:
            self._pending_response_item_counts = {7: 2}

    body = protocol_module._STATUS.pack(protocol_module._STATUS_OK) + encode_value(
        [b"first", b"second"]
    )
    response = protocol_response_module._decode_protocol_response(
        Adapter(),
        lane_id=1,
        opcode=protocol_module._OP_MGET,
        request_id=7,
        flags=0,
        body=body,
        read_started_ns=0,
        read_done_ns=0,
    )

    assert response.value == [b"first", b"second"]


@pytest.mark.parametrize(
    ("opcode", "expected", "ok_value"),
    [
        (protocol_module._OP_SET, 1, "OK"),
        (protocol_module._OP_FLOW_CREATE_MANY, 2, b"OK"),
    ],
)
def test_generic_collection_contract_preserves_legacy_ok_scalar(
    opcode: int,
    expected: int,
    ok_value: str | bytes,
) -> None:
    class Adapter:
        max_decoded_collection_items = 100

        def __init__(self) -> None:
            self._pending_response_item_counts = {7: expected}

    body = protocol_module._STATUS.pack(protocol_module._STATUS_OK) + encode_value(ok_value)

    response = protocol_response_module._decode_protocol_response(
        Adapter(),
        lane_id=1,
        opcode=opcode,
        request_id=7,
        flags=0,
        body=body,
        read_started_ns=0,
        read_done_ns=0,
    )

    assert protocol_response_module._ok_scalar(response.value)


@pytest.mark.parametrize("invalid", [True, 1.5, "2"])
def test_protocol_decode_collection_limit_rejects_non_integer_values(invalid: Any) -> None:
    encoded = encode_value(b"value")

    with pytest.raises(ValueError, match="max_collection_items"):
        decode_value(encoded, max_collection_items=invalid)


def test_protocol_value_decoder_enforces_one_cumulative_nested_collection_budget():
    payload = encode_value([[b"one"], [b"two"]])

    with pytest.raises(FerricStoreError, match="max_decoded_collection_items"):
        decode_value(payload, max_collection_items=3)

    assert decode_value(payload, max_collection_items=4) == (
        [[b"one"], [b"two"]],
        b"",
    )


@pytest.mark.parametrize(
    "nested_value",
    [
        b"\x03" + bytes([protocol_module._COMPACT_FLOW_RECORD_LIST]) + struct.pack(">I", 101),
        b"\x02" + bytes([protocol_module._COMPACT_FLOW_RECORD]) + struct.pack(">I", 101),
        b"\x06" + struct.pack(">I", 101),
        b"\x07" + struct.pack(">I", 101),
    ],
)
def test_compact_pipeline_rejects_oversized_nested_collection_before_allocation(
    nested_value,
):
    payload = (
        bytes([protocol_module._COMPACT_PIPELINE_RESPONSE])
        + struct.pack(">I", 1)
        + b"\x00"
        + nested_value
    )

    with pytest.raises(FerricStoreError, match="max_decoded_collection_items"):
        _try_fast_response_value_at(
            protocol_module._OP_PIPELINE,
            payload,
            0,
            max_collection_items=100,
        )


def test_compact_pipeline_uses_one_budget_across_sibling_nested_collections():
    payload = (
        bytes([protocol_module._COMPACT_PIPELINE_RESPONSE])
        + struct.pack(">I", 2)
        + b"\x00\x06"
        + struct.pack(">I", 2)
        + struct.pack(">I", 1)
        + b"a"
        + struct.pack(">I", 1)
        + b"b"
        + b"\x00\x06"
        + struct.pack(">I", 2)
        + struct.pack(">I", 1)
        + b"c"
        + struct.pack(">I", 1)
        + b"d"
    )

    with pytest.raises(FerricStoreError, match="max_decoded_collection_items"):
        _try_fast_response_value_at(
            protocol_module._OP_PIPELINE,
            payload,
            0,
            max_collection_items=9,
        )

    assert _try_fast_response_value_at(
        protocol_module._OP_PIPELINE,
        payload,
        0,
        max_collection_items=10,
    ) == [["ok", [b"a", b"b"]], ["ok", [b"c", b"d"]]]


def test_custom_claim_jobs_charges_each_fixed_width_row_to_decode_budget():
    def binary(value: bytes | None) -> bytes:
        if value is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(value)) + value

    payload = (
        bytes([protocol_module._COMPACT_FLOW_CLAIM_JOBS])
        + struct.pack(">I", 1)
        + binary(b"flow-1")
        + binary(None)
        + binary(b"lease-1")
        + struct.pack(">q", 7)
    )

    with pytest.raises(FerricStoreError, match="max_decoded_collection_items"):
        _try_fast_response_value_at(
            protocol_module._OP_FLOW_CLAIM_DUE,
            payload,
            0,
            max_collection_items=4,
        )

    assert _try_fast_response_value_at(
        protocol_module._OP_FLOW_CLAIM_DUE,
        payload,
        0,
        max_collection_items=5,
    ) == [[b"flow-1", None, b"lease-1", 7]]


def test_custom_pipeline_budget_matches_generic_nested_collection_accounting():
    def binary(value: bytes | None) -> bytes:
        if value is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(value)) + value

    custom = (
        bytes([protocol_module._COMPACT_PIPELINE_RESPONSE])
        + struct.pack(">I", 1)
        + b"\x00\x04"
        + binary(b"flow-1")
        + binary(None)
        + binary(b"lease-1")
        + struct.pack(">q", 7)
    )
    generic = encode_value([["ok", [b"flow-1", None, b"lease-1", 7]]])

    for payload, decode, expected in (
        (
            custom,
            lambda limit: _try_fast_response_value_at(
                protocol_module._OP_PIPELINE,
                custom,
                0,
                max_collection_items=limit,
            ),
            [["ok", [b"flow-1", None, b"lease-1", 7]]],
        ),
        (
            generic,
            lambda limit: decode_value(generic, max_collection_items=limit)[0],
            [[b"ok", [b"flow-1", None, b"lease-1", 7]]],
        ),
    ):
        assert payload
        with pytest.raises(FerricStoreError, match="max_decoded_collection_items"):
            decode(6)
        assert decode(7) == expected


def test_custom_pipeline_charges_flow_value_ref_map_and_status_pair():
    def binary(value: bytes | None) -> bytes:
        if value is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(value)) + value

    payload = (
        bytes([protocol_module._COMPACT_PIPELINE_RESPONSE])
        + struct.pack(">I", 1)
        + b"\x00\x05"
        + binary(b"ref-1")
        + binary(b"tenant-1")
        + binary(None)
    )

    with pytest.raises(FerricStoreError, match="max_decoded_collection_items"):
        _try_fast_response_value_at(
            protocol_module._OP_PIPELINE,
            payload,
            0,
            max_collection_items=4,
        )
    assert _try_fast_response_value_at(
        protocol_module._OP_PIPELINE,
        payload,
        0,
        max_collection_items=5,
    ) == [["ok", {b"ref": b"ref-1", b"partition_key": b"tenant-1"}]]


def test_compact_flow_many_request_exposes_exact_response_cardinality():
    payloads = _compact_flow_many_payloads_from_raw(
        [
            (
                "FLOW.CREATE",
                "flow-1",
                "TYPE",
                "order",
                "STATE",
                "queued",
                "NOW",
                100,
                "RUN_AT",
                100,
                "PAYLOAD",
                b"one",
            ),
            (
                "FLOW.CREATE",
                "flow-2",
                "TYPE",
                "order",
                "STATE",
                "queued",
                "NOW",
                100,
                "RUN_AT",
                100,
                "PAYLOAD",
                b"two",
            ),
        ]
    )

    assert payloads is not None
    [(opcode, payload, count)] = payloads
    assert count == 2
    assert protocol_module._expected_payload_collection_items(opcode, payload) == 2


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


def test_async_protocol_adapter_waits_for_authenticated_handshake_before_commands():
    header = struct.Struct(">4sBBIHQI")
    status = struct.Struct(">H")
    observed_opcodes: list[int] = []

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
        startup_received = asyncio.Event()
        handler_finished = asyncio.Event()
        handler_errors: list[BaseException] = []

        async def handle(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            get_count = 0
            try:
                startup_opcode, startup_lane, startup_id, _startup_payload = await recv_frame(
                    reader
                )
                observed_opcodes.append(startup_opcode)
                startup_received.set()

                try:
                    early = await asyncio.wait_for(recv_frame(reader), timeout=0.1)
                except asyncio.TimeoutError:
                    early = None
                if early is not None:
                    opcode, lane, request_id, _payload = early
                    observed_opcodes.append(opcode)
                    if opcode == 0x0101:
                        get_count += 1
                    writer.write(response(opcode, lane, request_id, b"value"))
                    await writer.drain()

                writer.write(response(startup_opcode, startup_lane, startup_id, {"ok": True}))
                await writer.drain()

                auth_opcode, auth_lane, auth_id, _auth_payload = await recv_frame(reader)
                observed_opcodes.append(auth_opcode)
                writer.write(response(auth_opcode, auth_lane, auth_id, {"ok": True}))
                await writer.drain()

                while get_count < 2:
                    opcode, lane, request_id, _payload = await recv_frame(reader)
                    observed_opcodes.append(opcode)
                    if opcode == 0x0101:
                        get_count += 1
                    writer.write(response(opcode, lane, request_id, b"value"))
                    await writer.drain()
            except BaseException as exc:
                handler_errors.append(exc)
            finally:
                writer.close()
                await writer.wait_closed()
                handler_finished.set()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        adapter = AsyncProtocolAdapter(
            "127.0.0.1",
            port,
            client_name="pytest",
            password="secret",
            timeout=1.0,
            heartbeat_interval=None,
        )
        first: asyncio.Task[Any] | None = None
        second: asyncio.Task[Any] | None = None
        try:
            first = asyncio.create_task(adapter.execute_command("GET", "first"))
            await asyncio.wait_for(startup_received.wait(), timeout=1.0)
            second = asyncio.create_task(adapter.execute_command("GET", "second"))
            assert await asyncio.wait_for(asyncio.gather(first, second), timeout=1.0) == [
                b"value",
                b"value",
            ]
            await asyncio.wait_for(handler_finished.wait(), timeout=1.0)
            assert handler_errors == []
        finally:
            for task in (first, second):
                if task is not None and not task.done():
                    task.cancel()
            await adapter.close()
            server.close()
            await server.wait_closed()

    asyncio.run(run())

    assert observed_opcodes == [0x000C, 0x0002, 0x0101, 0x0101]


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


def test_async_protocol_request_timeout_includes_slow_send():
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        adapter.timeout = 0.01
        adapter._write_lock = asyncio.Lock()
        adapter._request_id = 0
        adapter.lanes = 1
        adapter._lane_cursor = 0
        adapter._pending = {}
        adapter._pending_traces = {}

        async def blocked_send(*_args, **_kwargs):
            await asyncio.Event().wait()

        adapter._send = blocked_send

        with pytest.raises(FerricStoreError, match="timed out"):
            await asyncio.wait_for(
                adapter._request(0x0101, 1, {"key": "a"}),
                timeout=0.2,
            )

        assert adapter._pending == {}
        assert adapter._pending_traces == {}

    asyncio.run(run())


def test_async_protocol_request_cancellation_clears_pending_during_send():
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        adapter.timeout = None
        adapter._write_lock = asyncio.Lock()
        adapter._request_id = 0
        adapter.lanes = 1
        adapter._lane_cursor = 0
        adapter._pending = {}
        adapter._pending_traces = {}
        entered = asyncio.Event()

        async def blocked_send(*_args, **_kwargs):
            entered.set()
            await asyncio.Event().wait()

        adapter._send = blocked_send
        task = asyncio.create_task(adapter._request(0x0101, 1, {"key": "a"}))
        await asyncio.wait_for(entered.wait(), timeout=1.0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert adapter._pending == {}
        assert adapter._pending_traces == {}

    asyncio.run(run())


def test_async_protocol_close_cancellation_still_fails_pending_requests():
    class BlockingWriter:
        def __init__(self) -> None:
            self.entered = asyncio.Event()
            self.release = asyncio.Event()
            self.finished = asyncio.Event()
            self.closed = False

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            self.entered.set()
            await self.release.wait()
            self.finished.set()

        def is_closing(self) -> bool:
            return self.closed

    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        writer = BlockingWriter()
        adapter._writer = cast(Any, writer)
        pending = asyncio.get_running_loop().create_future()
        adapter._pending[7] = pending

        close_task = asyncio.create_task(adapter.close())
        await writer.entered.wait()
        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task

        joined_close = asyncio.create_task(adapter.close())
        try:
            await asyncio.sleep(0.01)
            assert joined_close.done() is False
            assert writer.finished.is_set() is False
        finally:
            writer.release.set()
            await joined_close
        await asyncio.wait_for(writer.finished.wait(), timeout=1.0)

        assert adapter._writer is None
        assert pending.done()
        with pytest.raises(FerricStoreError, match="connection is closed"):
            pending.result()

    asyncio.run(run())


def test_protocol_close_racing_reconnect_cannot_resurrect_socket(monkeypatch):
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(heartbeat_interval=None)
    reconnect_started = threading.Event()
    allow_reconnect = threading.Event()
    errors: list[BaseException] = []

    class FakeSocket:
        def __init__(self) -> None:
            self.closed = False

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    sock = FakeSocket()

    def reconnect() -> None:
        reconnect_started.set()
        allow_reconnect.wait(timeout=1.0)
        adapter._sock = cast(Any, sock)

    adapter._connect = reconnect

    def ensure_connected() -> None:
        try:
            adapter._ensure_connected()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=ensure_connected)
    thread.start()
    assert reconnect_started.wait(timeout=1.0)
    adapter.close()
    allow_reconnect.set()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    assert adapter._sock is None
    assert sock.closed
    assert len(errors) == 1
    assert isinstance(errors[0], FerricStoreError)
    assert "closed" in str(errors[0])


def test_async_protocol_close_while_waiting_to_reconnect_stays_closed(monkeypatch):
    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        await adapter._connect_lock.acquire()
        open_calls = 0

        async def forbidden_open(*_args: Any, **_kwargs: Any) -> Any:
            nonlocal open_calls
            open_calls += 1
            raise AssertionError("closed adapter attempted to reconnect")

        monkeypatch.setattr(asyncio, "open_connection", forbidden_open)
        reconnect = asyncio.create_task(adapter._ensure_connected())
        await asyncio.sleep(0)
        await adapter.close()
        adapter._connect_lock.release()

        with pytest.raises(FerricStoreError, match="connection is closed"):
            await reconnect
        assert open_calls == 0
        assert adapter._writer is None

    asyncio.run(run())


def test_async_protocol_startup_failure_closes_partial_transport(monkeypatch):
    class FakeWriter:
        def __init__(self) -> None:
            self.closed = False

        def is_closing(self) -> bool:
            return self.closed

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            return None

    async def run() -> None:
        reader = asyncio.StreamReader()
        writer = FakeWriter()

        async def fake_open_connection(*_args, **_kwargs):
            return reader, writer

        async def idle_reader_loop(*_args, **_kwargs):
            await asyncio.Event().wait()

        async def failed_startup(*_args, **_kwargs):
            return ProtocolResponse(0, 0x000C, 1, 0, 1, {b"message": b"ERR bad startup"})

        monkeypatch.setattr(asyncio, "open_connection", fake_open_connection)
        adapter = AsyncProtocolAdapter(timeout=None, heartbeat_interval=None)
        adapter._reader_loop = idle_reader_loop
        adapter._request = failed_startup

        with pytest.raises(FerricStoreError, match="bad startup"):
            await adapter._ensure_connected()

        assert writer.closed
        assert adapter._reader is None
        assert adapter._writer is None
        assert adapter._reader_task is None

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


def test_async_protocol_adapter_reconnects_after_server_closes_idle_socket():
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
        connected = 0
        first_connection_closed = asyncio.Event()

        async def handle(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            nonlocal connected
            connected += 1
            try:
                startup_opcode, startup_lane, startup_id, startup_payload = await recv_frame(reader)
                received.append((startup_opcode, startup_lane, startup_payload))
                writer.write(response(startup_opcode, startup_lane, startup_id, {"ok": True}))
                await writer.drain()

                if connected == 1:
                    return

                get_opcode, get_lane, get_id, get_payload = await recv_frame(reader)
                received.append((get_opcode, get_lane, get_payload))
                writer.write(response(get_opcode, get_lane, get_id, b"async-value-after-reconnect"))
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                if connected == 1:
                    first_connection_closed.set()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        adapter = AsyncProtocolAdapter(
            "127.0.0.1",
            port,
            client_name="pytest",
            timeout=1.0,
            heartbeat_interval=None,
        )
        transport_dropped = asyncio.Event()
        original_close_transport = adapter._close_transport

        async def close_transport_and_notify(*args: Any, **kwargs: Any) -> None:
            await original_close_transport(*args, **kwargs)
            if not kwargs.get("mark_closed", False):
                transport_dropped.set()

        adapter._close_transport = close_transport_and_notify
        try:
            await adapter._ensure_connected()
            await asyncio.wait_for(first_connection_closed.wait(), timeout=1.0)
            await asyncio.wait_for(transport_dropped.wait(), timeout=1.0)
            assert await adapter.execute_command("GET", "k") == b"async-value-after-reconnect"
        finally:
            await adapter.close()
            server.close()
            await server.wait_closed()

    asyncio.run(run())

    assert received[0][0] == 0x000C
    assert received[1][0] == 0x000C
    assert received[2] == (0x0101, 1, {b"key": b"k"})


def test_async_protocol_adapter_stale_reader_does_not_close_newer_writer():
    class FakeWriter:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            return None

        def is_closing(self) -> bool:
            return self.closed

    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        old_reader = object()
        new_reader = object()
        old_writer = FakeWriter()
        new_writer = FakeWriter()
        adapter._reader = old_reader
        adapter._writer = old_writer
        adapter._reader_task = None
        adapter._heartbeat_task = None
        adapter._pending = {}
        adapter._pending_traces = {}
        entered = asyncio.Event()
        proceed = asyncio.Event()

        async def fake_recv_response(*_args: Any, **_kwargs: Any) -> ProtocolResponse:
            entered.set()
            await proceed.wait()
            raise FerricStoreError("old reader failed")

        adapter._recv_response = fake_recv_response

        task = asyncio.create_task(adapter._reader_loop())
        adapter._reader_task = task
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        adapter._reader = new_reader
        adapter._writer = new_writer
        proceed.set()
        await asyncio.wait_for(task, timeout=1.0)

        assert adapter._reader is new_reader
        assert adapter._writer is new_writer
        assert not new_writer.closed

    asyncio.run(run())


def test_async_protocol_adapter_stale_heartbeat_does_not_close_newer_writer():
    class FakeWriter:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            return None

        def is_closing(self) -> bool:
            return self.closed

    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        old_reader = object()
        new_reader = object()
        old_writer = FakeWriter()
        new_writer = FakeWriter()
        adapter._reader = old_reader
        adapter._writer = old_writer
        adapter._reader_task = None
        adapter._heartbeat_task = None
        adapter._pending = {}
        adapter._pending_traces = {}
        adapter.heartbeat_interval = 0.001
        adapter.heartbeat_timeout = 0.001
        adapter._last_activity = 0.0

        async def fake_request(*_args: Any, **_kwargs: Any) -> ProtocolResponse:
            adapter._reader = new_reader
            adapter._writer = new_writer
            raise FerricStoreError("old heartbeat failed")

        adapter._request = fake_request

        task = asyncio.create_task(adapter._heartbeat_loop())
        adapter._heartbeat_task = task
        await asyncio.wait_for(task, timeout=1.0)

        assert adapter._reader is new_reader
        assert adapter._writer is new_writer
        assert not new_writer.closed

    asyncio.run(run())


def test_async_protocol_send_writes_large_body_without_writelines_join():
    class FakeWriter:
        def __init__(self) -> None:
            self.parts: list[bytes] = []

        def write(self, part: bytes) -> None:
            self.parts.append(part)

        def writelines(self, _parts: Any) -> None:
            pytest.fail("writelines may join the full frame on supported Python versions")

        async def drain(self) -> None:
            return None

    async def run() -> None:
        writer = FakeWriter()
        adapter = object.__new__(AsyncProtocolAdapter)
        adapter._writer = writer
        adapter.compression = "none"
        adapter._queued_write_bytes = 0
        adapter.write_drain_bytes = 10_000_000
        adapter._set_pending_request_size = lambda _request_id, _size: None
        body = b"x" * (256 * 1024)

        await adapter._send(0x0101, 1, 1, body)

        assert len(writer.parts) == 2
        assert writer.parts[1] is body

    asyncio.run(run())


def test_async_protocol_send_drains_only_after_threshold():
    class FakeWriter:
        def __init__(self) -> None:
            self.parts: list[bytes] = []
            self.drains = 0

        def write(self, part: bytes) -> None:
            self.parts.append(part)

        async def drain(self) -> None:
            self.drains += 1

    async def run() -> None:
        writer = FakeWriter()
        adapter = object.__new__(AsyncProtocolAdapter)
        adapter._writer = writer
        adapter.compression = "none"
        adapter._queued_write_bytes = 0
        adapter.write_drain_bytes = 10_000

        adapter._reserve_pending_request(1)
        await adapter._send(0x0101, 1, 1, {"key": "a"})
        adapter._release_pending_request(1)
        assert writer.drains == 0

        adapter.write_drain_bytes = 1
        adapter._reserve_pending_request(2)
        await adapter._send(0x0101, 1, 2, {"key": "b"})
        adapter._release_pending_request(2)
        assert writer.drains == 1
        assert adapter._queued_write_bytes == 0
        assert len(writer.parts) == 4

    asyncio.run(run())


def test_async_protocol_execute_batch_coalesces_flow_writes_to_compact_many():
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        calls = []

        async def ensure_connected():
            return None

        async def request(opcode, lane_id, payload, flags=0):
            calls.append((opcode, lane_id, payload, flags))
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, b"OK")

        adapter._ensure_connected = ensure_connected
        adapter._request = request

        result = await adapter.execute_batch(
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
        assert calls[0][0] == 0x020F
        assert calls[0][3] == _FLAG_CUSTOM_PAYLOAD
        assert calls[0][2][0] == 0x90

    asyncio.run(run())


def test_async_protocol_pipeline_decodes_flow_many_status_pairs(monkeypatch):
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        record = {b"id": b"f1"}
        responses = iter(
            [
                [["ok", record]],
                [["error", {"message": "create rejected"}]],
            ]
        )

        async def ensure_connected():
            return None

        async def request(opcode, lane_id, payload, flags=0):
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, next(responses))

        adapter._ensure_connected = ensure_connected
        adapter._request = request
        monkeypatch.setattr(
            protocol_module,
            "_compact_flow_many_payloads_from_raw",
            lambda _commands: [(0x020F, b"compact", 1)],
        )
        client = AsyncFlowClient(adapter)

        assert await client.pipeline().command("ignored").execute() == [record]
        with pytest.raises(FerricStoreError, match="create rejected"):
            await client.pipeline().command("ignored").execute()

    asyncio.run(run())


def test_async_protocol_compact_flow_many_uses_bounded_concurrency(monkeypatch):
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        active = 0
        peak = 0
        request_order: list[int] = []

        async def ensure_connected():
            return None

        async def request(opcode, lane_id, payload, flags=0):
            nonlocal active, peak
            index = int(payload.decode())
            request_order.append(index)
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0)
            active -= 1
            return ProtocolResponse(lane_id, opcode, index + 1, flags, 0, [index])

        adapter._ensure_connected = ensure_connected
        adapter._request = request
        payloads = [(0x020F, str(index).encode(), 1) for index in range(40)]
        monkeypatch.setattr(
            protocol_async_module,
            "_compact_flow_many_payloads_from_raw",
            lambda _commands: payloads,
        )

        result = await adapter.execute_batch([("ignored",)] * len(payloads))

        assert result == list(range(40))
        assert 1 < peak <= 16
        assert request_order == list(range(40))

    asyncio.run(run())


def test_async_client_pipeline_preserves_compact_group_execution_order():
    async def run() -> None:
        adapter = AsyncProtocolAdapter(timeout=None, heartbeat_interval=None, lanes=8)
        timeline: list[str] = []
        send_index = 0

        async def ensure_connected() -> None:
            return None

        async def send(opcode, lane_id, request_id, payload, flags=0):
            nonlocal send_index
            index = send_index
            send_index += 1
            timeline.append(f"send-{index}")
            future = adapter._pending[request_id]
            delay = 0.02 if index == 0 else 0.001

            def finish() -> None:
                timeline.append(f"finish-{index}")
                future.set_result(ProtocolResponse(lane_id, opcode, request_id, flags, 0, b"OK"))

            asyncio.get_running_loop().call_later(delay, finish)
            return None

        adapter._ensure_connected = ensure_connected
        adapter._send = send
        client = AsyncFlowClient(adapter)
        commands = [
            (
                "FLOW.CREATE",
                "same-flow",
                "TYPE",
                "email",
                "STATE",
                "queued",
                "NOW",
                1,
                "RUN_AT",
                1,
                "PAYLOAD",
                b"",
            ),
            (
                "FLOW.CREATE",
                "same-flow",
                "TYPE",
                "email",
                "STATE",
                "queued",
                "NOW",
                2,
                "RUN_AT",
                1,
                "PAYLOAD",
                b"",
            ),
        ]
        pipeline = client.pipeline()
        for command in commands:
            pipeline.command(*command)

        assert await pipeline.execute() == [b"OK", b"OK"]
        assert timeline == ["send-0", "finish-0", "send-1", "finish-1"]

    asyncio.run(run())


def test_async_protocol_execute_batch_uses_compact_pipeline_payload():
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        captured = {}

        async def ensure_connected():
            return None

        async def request(opcode, lane_id, payload, flags=0):
            captured.update(opcode=opcode, lane_id=lane_id, payload=payload, flags=flags)
            return ProtocolResponse(lane_id, opcode, 1, flags, 0, [b"one", b"two"])

        adapter._ensure_connected = ensure_connected
        adapter._request = request

        result = await adapter.execute_batch([("GET", "k1"), ("GET", "k2")])

        assert result == [b"one", b"two"]
        assert captured["opcode"] == 0x000E
        assert captured["flags"] == _FLAG_CUSTOM_PAYLOAD
        assert captured["payload"] == _compact_pipeline_payload_from_raw(
            [("GET", "k1"), ("GET", "k2")], values_only=True
        )

    asyncio.run(run())


def test_async_protocol_execute_batch_does_not_pipeline_connection_stateful_commands():
    async def run() -> None:
        adapter = object.__new__(AsyncProtocolAdapter)
        calls = []

        async def execute_command(*args):
            calls.append(args)
            return args[0]

        async def request(*_args, **_kwargs):
            pytest.fail("stateful commands were pipelined")

        adapter.execute_command = execute_command
        adapter._request = request

        result = await adapter.execute_batch([("BLPOP", "jobs", 1), ("GET", "k")])

        assert result == ["BLPOP", "GET"]
        assert calls == [("BLPOP", "jobs", 1), ("GET", "k")]

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


def test_encodes_protocol_set_get_and_falls_back_for_unknown_commands():
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

    fallback = build_protocol_command("GET.COMPACT", "k")
    assert fallback.opcode == 0x0100
    assert fallback.payload == {"command": "GET.COMPACT", "args": ["k"]}

    fallback = build_protocol_command("MGET.COMPACT", "a", "b")
    assert fallback.opcode == 0x0100
    assert fallback.payload == {"command": "MGET.COMPACT", "args": ["a", "b"]}


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
        "KIND",
        "one_shot",
        "TIMEZONE",
        "Asia/Jerusalem",
        "OVERWRITE",
        "true",
    )
    assert schedule.opcode == 0x0225
    assert schedule.payload == {
        "id": "daily-report",
        "target": {"id": "flow-1", "type": "report", "state": "queued"},
        "kind": "one_shot",
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

    monkeypatch.setattr(
        protocol_commands_module,
        "build_protocol_command",
        fail_build_protocol_command,
    )

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

    monkeypatch.setattr(
        protocol_commands_module,
        "build_protocol_command",
        fail_build_protocol_command,
    )

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


def test_topology_refresh_never_connects_to_untrusted_learned_endpoint(monkeypatch):
    created: list[str] = []

    class FakeAdapter:
        seed_fails = False

        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            if "seed.local" in self.url and self.seed_fails:
                raise OSError("seed unavailable")
            if args[0] == "SHARDS":
                return _single_shard_topology("untrusted.local", 6399)
            return b"OK"

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        created.append(url)
        return FakeAdapter(url)

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(["ferric://seed.local:6388"])
    FakeAdapter.seed_fails = True

    with pytest.raises(FerricStoreError, match="no FerricStore topology endpoint reachable"):
        pool.refresh_topology()

    assert "ferric://untrusted.local:6399" not in created


def test_async_topology_refresh_never_connects_to_untrusted_learned_endpoint(monkeypatch):
    created: list[str] = []

    class FakeAdapter:
        seed_fails = False

        def __init__(self, url: str) -> None:
            self.url = url

        async def execute_command(self, *args: Any) -> Any:
            if "seed.local" in self.url and self.seed_fails:
                raise OSError("seed unavailable")
            if args[0] == "SHARDS":
                return _single_shard_topology("untrusted.local", 6399)
            return b"OK"

        async def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        created.append(url)
        return FakeAdapter(url)

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(["ferric://seed.local:6388"])
        await pool.refresh_topology()
        FakeAdapter.seed_fails = True
        with pytest.raises(FerricStoreError, match="no FerricStore topology endpoint reachable"):
            await pool.refresh_topology()

    asyncio.run(run())
    assert "ferric://untrusted.local:6399" not in created


def test_topology_refresh_closes_idle_stale_endpoint_adapters(monkeypatch):
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}
    created: dict[str, list[Any]] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            return b"OK"

        def close(self) -> None:
            self.closed = True

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created.setdefault(url, []).append(adapter)
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    leader_1 = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

    current.update(host="leader-2.local", port=6392, epoch=2)
    pool.refresh_topology()
    leader_2 = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))
    assert leader_1.closed is True

    current.update(host="leader-3.local", port=6393, epoch=3)
    pool.refresh_topology()
    leader_3 = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

    assert leader_1.closed is True
    assert leader_2.closed is True
    assert leader_3.closed is False
    assert created["ferric://seed.local:6388"][0].closed is False
    assert len(pool._adapters) + len(pool._retired_adapters) <= 3
    pool.close()


def test_topology_refresh_recreates_idle_endpoint_that_reappears(monkeypatch):
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}
    created: dict[str, list[Any]] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            return b"OK"

        def close(self) -> None:
            self.closed = True

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created.setdefault(url, []).append(adapter)
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    original = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

    current.update(host="leader-2.local", port=6392, epoch=2)
    pool.refresh_topology()
    current.update(host="leader-1.local", port=6391, epoch=3)
    pool.refresh_topology()
    reused = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

    assert reused is not original
    assert original.closed is True
    assert len(created["ferric://leader-1.local:6391"]) == 2
    pool.close()


def test_topology_refresh_keeps_busy_retired_adapter_until_it_is_idle(monkeypatch):
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False
            self._active = [0]
            self._leased = set()
            self._broadcasting = False

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            return b"OK"

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
    leader_1 = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))
    leader_1._active[0] = 1

    current.update(host="leader-2.local", port=6392, epoch=2)
    pool.refresh_topology()
    current.update(host="leader-3.local", port=6393, epoch=3)
    pool.refresh_topology()
    assert leader_1.closed is False

    leader_1._active[0] = 0
    current.update(host="leader-4.local", port=6394, epoch=4)
    pool.refresh_topology()
    assert leader_1.closed is True
    pool.close()


def test_async_topology_refresh_closes_idle_stale_endpoint_adapters(monkeypatch):
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}
    created: dict[str, list[Any]] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False

        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            return b"OK"

        async def close(self) -> None:
            self.closed = True

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created.setdefault(url, []).append(adapter)
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        leader_1 = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

        current.update(host="leader-2.local", port=6392, epoch=2)
        await pool.refresh_topology()
        leader_2 = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))
        if pool._cleanup_tasks:
            await asyncio.gather(*pool._cleanup_tasks)
        assert leader_1.closed is True

        current.update(host="leader-3.local", port=6393, epoch=3)
        await pool.refresh_topology()
        leader_3 = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))
        if pool._cleanup_tasks:
            await asyncio.gather(*pool._cleanup_tasks)

        assert leader_1.closed is True
        assert leader_2.closed is True
        assert leader_3.closed is False
        assert created["ferric://seed.local:6388"][0].closed is False
        assert len(pool._adapters) + len(pool._retired_adapters) <= 3
        await pool.close()

    asyncio.run(run())


def test_closed_topology_pool_rejects_unwarmed_routed_command(monkeypatch):
    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

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
    pool.close()

    with pytest.raises(FerricStoreError, match="closed"):
        pool.execute_command("SET", "key", b"value")


def test_closed_async_topology_pool_rejects_unwarmed_routed_command(monkeypatch):
    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        async def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        await pool.close()
        with pytest.raises(FerricStoreError, match="closed"):
            await pool.execute_command("SET", "key", b"value")

    asyncio.run(run())


def test_topology_adapter_creation_is_single_flight(monkeypatch):
    created_leaders: list[Any] = []
    start = threading.Barrier(2)

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        if "leader.local" in url:
            created_leaders.append(adapter)
            time.sleep(0.03)
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )

    def execute() -> Any:
        start.wait()
        return pool.execute_command("SET", "key", b"value")

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert list(executor.map(lambda _index: execute(), range(2))) == [b"OK", b"OK"]

    assert len(created_leaders) == 1


def test_topology_replays_flow_wake_subscription_to_later_adapter(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.active_subscriptions: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
            self.registered_subscriptions: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> bytes:
            self.active_subscriptions.append((args, kwargs))
            self.register_flow_wake_subscription(*args, **kwargs)
            return b"OK"

        def register_flow_wake_subscription(self, *args: Any, **kwargs: Any) -> None:
            self.registered_subscriptions.append((args, kwargs))

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    pool.subscribe_flow_wake("jobs-a", state="queued", limit=100)
    pool.subscribe_flow_wake("jobs-b", state="queued", limit=100)
    pool.subscribe_flow_wake("jobs-a", state="queued", limit=100)
    pool.execute_command("SET", "key", b"value")

    assert created["ferric://leader.local:6391"].active_subscriptions == [
        (("jobs-a",), {"state": "queued", "limit": 100})
    ]


def test_async_topology_replays_subscription_to_each_later_pooled_connection(monkeypatch):
    created: dict[str, list[Any]] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.events: list[Any] = []
            self.registered_subscriptions: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        async def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        async def execute_command_on_lane(
            self,
            args: tuple[Any, ...],
            _lane_id: int,
        ) -> Any:
            return await self.execute_command(*args)

        async def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> bytes:
            self.register_flow_wake_subscription(*args, **kwargs)
            return b"OK"

        def register_flow_wake_subscription(self, *args: Any, **kwargs: Any) -> None:
            self.registered_subscriptions[:] = [(args, kwargs)]

        async def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created.setdefault(url, []).append(adapter)
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
            max_connections=2,
        )
        await pool.subscribe_flow_wake("jobs-a", state="queued", limit=100)
        await pool.subscribe_flow_wake("jobs-b", state="running", limit=50)
        assert await pool.execute_command("SET", "key", b"value") == b"OK"

        expected = [(("jobs-b",), {"state": "running", "limit": 50})]
        leader_adapters = created["ferric://leader.local:6391"]
        assert len(leader_adapters) == 2
        assert all(adapter.registered_subscriptions == expected for adapter in leader_adapters)
        await pool.close()

    asyncio.run(run())


def test_topology_rejects_cross_shard_multi_key_command(monkeypatch):
    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"unexpected"

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

    assert RoutingTopology.slot_for_key("a") != RoutingTopology.slot_for_key("b")
    with pytest.raises(InvalidCommandError, match="same slot"):
        pool.execute_command("MGET", "a", "b")


def test_topology_batch_failure_refreshes_routes_without_replaying_writes(monkeypatch):
    shards_calls = 0
    leader_batch_calls = 0

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            nonlocal shards_calls
            if args[0] == "SHARDS":
                shards_calls += 1
                return _single_shard_topology()
            return b"OK"

        def execute_batch(self, _commands: list[tuple[Any, ...]]) -> list[Any]:
            nonlocal leader_batch_calls
            leader_batch_calls += 1
            raise OSError("leader moved")

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

    with pytest.raises(OSError, match="leader moved"):
        pool.execute_batch([("SET", "key", b"value")])

    assert leader_batch_calls == 1
    assert shards_calls == 2


def test_sync_topology_batch_fans_out_to_independent_shards(monkeypatch):
    active = 0
    max_active = 0
    active_lock = threading.Lock()

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            return _two_shard_topology() if args[0] == "SHARDS" else b"OK"

        def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
            nonlocal active, max_active
            with active_lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.03)
                return [(self.url, command[1]) for command in commands]
            finally:
                with active_lock:
                    active -= 1

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

    results = pool.execute_batch(
        [
            ("SET", "leader-a-key", b"a"),
            ("SET", "leader-b-key", b"b"),
        ]
    )

    assert results == [
        ("ferric://leader-a.local:6391", "leader-a-key"),
        ("ferric://leader-b.local:6392", "leader-b-key"),
    ]
    assert max_active == 2


def test_topology_adapter_connect_failure_refreshes_routes(monkeypatch):
    shards_calls = 0

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            nonlocal shards_calls
            if args[0] == "SHARDS":
                shards_calls += 1
                return _single_shard_topology()
            return b"OK"

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        if "leader.local" in url:
            raise OSError("leader connect failed")
        return FakeAdapter(url)

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )

    with pytest.raises(OSError, match="leader connect failed"):
        pool.execute_batch([("SET", "key", b"value")])

    assert shards_calls == 2


def test_topology_affine_session_for_key_uses_routed_leader(monkeypatch):
    created: dict[str, Any] = {}

    class Session:
        pass

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.session = Session()

        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        def acquire_session(self) -> Session:
            return self.session

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )

    session = pool.acquire_session_for_key("tenant:{42}")

    assert session is created["ferric://leader.local:6391"].session


def test_topology_affine_session_preserves_server_route_lane(monkeypatch):
    routed_lanes: list[int] = []
    session = object()

    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology()
                topology["ranges"][0]["lane_id"] = 7
                return topology
            return b"OK"

        def acquire_session(self) -> Any:
            raise AssertionError("routed sessions must preserve the topology lane")

        def acquire_session_on_lane(self, lane_id: int) -> Any:
            routed_lanes.append(lane_id)
            return session

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )

    assert pool.acquire_session_for_key("tenant:{42}") is session
    assert routed_lanes == [7]


def test_async_topology_affine_session_for_key_uses_routed_leader(monkeypatch):
    created: dict[str, Any] = {}

    class Session:
        pass

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.session = Session()

        async def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        async def acquire_session(self) -> Session:
            return self.session

        async def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        session = await pool.acquire_session_for_key("tenant:{42}")
        assert session is created["ferric://leader.local:6391"].session

    asyncio.run(run())


def test_async_topology_affine_session_preserves_server_route_lane(monkeypatch):
    routed_lanes: list[int] = []
    session = object()

    class FakeAdapter:
        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology()
                topology["ranges"][0]["lane_id"] = 7
                return topology
            return b"OK"

        async def acquire_session(self) -> Any:
            raise AssertionError("routed sessions must preserve the topology lane")

        async def acquire_session_on_lane(self, lane_id: int) -> Any:
            routed_lanes.append(lane_id)
            return session

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        assert await pool.acquire_session_for_key("tenant:{42}") is session

    asyncio.run(run())
    assert routed_lanes == [7]


def test_topology_execute_batch_routes_keyed_commands_to_leader(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.batches: list[list[tuple[Any, ...]]] = []

        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[bytes]:
            self.batches.append(list(commands))
            return [b"OK"] * len(commands)

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )

    assert pool.execute_batch([("SET", "key-a", b"a"), ("SET", "key-b", b"b")]) == [
        b"OK",
        b"OK",
    ]
    assert created["ferric://seed.local:6388"].batches == []
    assert created["ferric://leader.local:6391"].batches == [
        [("SET", "key-a", b"a"), ("SET", "key-b", b"b")]
    ]


def test_topology_batch_groups_by_endpoint_and_exact_route_lane(monkeypatch):
    calls: list[tuple[int, list[tuple[Any, ...]]]] = []

    def topology() -> dict[str, Any]:
        payload = _two_shard_topology()
        payload["ranges"][1]["endpoint"] = dict(payload["ranges"][0]["endpoint"])
        payload["ranges"][0]["lane_id"] = 5
        payload["ranges"][1]["lane_id"] = 7
        return payload

    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            return topology() if args[0] == "SHARDS" else b"OK"

        def execute_batch(self, _commands: list[tuple[Any, ...]]) -> list[Any]:
            raise AssertionError("routed batches must preserve the topology lane")

        def execute_batch_on_lane(
            self,
            commands: list[tuple[Any, ...]],
            lane_id: int,
        ) -> list[Any]:
            calls.append((lane_id, list(commands)))
            return [(lane_id, command[1]) for command in commands]

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    key_a = next(
        f"lane-a-{index}"
        for index in range(10_000)
        if RoutingTopology.slot_for_key(f"lane-a-{index}") < 512
    )
    key_b = next(
        f"lane-b-{index}"
        for index in range(10_000)
        if RoutingTopology.slot_for_key(f"lane-b-{index}") >= 512
    )

    assert pool.execute_batch([("SET", key_a, b"a"), ("SET", key_b, b"b")]) == [
        (5, key_a),
        (7, key_b),
    ]
    assert calls == [
        (5, [("SET", key_a, b"a")]),
        (7, [("SET", key_b, b"b")]),
    ]


def test_topology_submit_batch_routes_before_submitting(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.submitted: list[list[tuple[Any, ...]]] = []

        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        def submit_batch(self, commands: list[tuple[Any, ...]]) -> Future[list[Any]]:
            self.submitted.append(list(commands))
            future: Future[list[Any]] = Future()
            future.set_result([b"OK"] * len(commands))
            return future

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )

    result = pool.submit_batch([("SET", "key-a", b"a"), ("SET", "key-b", b"b")])

    assert result.result() == [b"OK", b"OK"]
    assert created["ferric://seed.local:6388"].submitted == []
    assert created["ferric://leader.local:6391"].submitted == [
        [("SET", "key-a", b"a"), ("SET", "key-b", b"b")]
    ]


def test_topology_async_submit_paths_preserve_exact_route_lanes(monkeypatch):
    command_calls: list[tuple[int, list[tuple[Any, ...]]]] = []
    batch_calls: list[tuple[int, list[tuple[Any, ...]]]] = []

    def topology() -> dict[str, Any]:
        payload = _two_shard_topology()
        payload["ranges"][1]["endpoint"] = dict(payload["ranges"][0]["endpoint"])
        payload["ranges"][0]["lane_id"] = 5
        payload["ranges"][1]["lane_id"] = 7
        return payload

    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            return topology() if args[0] == "SHARDS" else b"OK"

        def submit_commands(self, _commands: list[tuple[Any, ...]]) -> list[Future[Any]]:
            raise AssertionError("routed submits must preserve the topology lane")

        def submit_commands_on_lane(
            self,
            commands: list[tuple[Any, ...]],
            lane_id: int,
        ) -> list[Future[Any]]:
            command_calls.append((lane_id, list(commands)))
            return [_future((lane_id, command[1])) for command in commands]

        def submit_batch(self, _commands: list[tuple[Any, ...]]) -> Future[list[Any]]:
            raise AssertionError("routed submits must preserve the topology lane")

        def submit_batch_on_lane(
            self,
            commands: list[tuple[Any, ...]],
            lane_id: int,
        ) -> Future[list[Any]]:
            batch_calls.append((lane_id, list(commands)))
            return _future([(lane_id, command[1]) for command in commands])

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    key_a = next(
        f"submit-a-{index}"
        for index in range(10_000)
        if RoutingTopology.slot_for_key(f"submit-a-{index}") < 512
    )
    key_b = next(
        f"submit-b-{index}"
        for index in range(10_000)
        if RoutingTopology.slot_for_key(f"submit-b-{index}") >= 512
    )
    commands = [("SET", key_a, b"a"), ("SET", key_b, b"b")]

    assert [future.result() for future in pool.submit_commands(commands)] == [
        (5, key_a),
        (7, key_b),
    ]
    assert pool.submit_batch(commands).result() == [(5, key_a), (7, key_b)]
    assert command_calls == [(5, [commands[0]]), (7, [commands[1]])]
    assert batch_calls == [(5, [commands[0]]), (7, [commands[1]])]


def test_topology_specialized_key_submits_preserve_exact_route_lane(monkeypatch):
    calls: list[tuple[tuple[Any, ...], int]] = []

    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                topology = _single_shard_topology()
                topology["ranges"][0]["lane_id"] = 7
                return topology
            return b"OK"

        def submit_command_on_lane(
            self,
            args: tuple[Any, ...],
            lane_id: int,
        ) -> Future[Any]:
            calls.append((args, lane_id))
            return _future(b"OK")

        def submit_mget(self, _keys: Any) -> Future[Any]:
            raise AssertionError("routed MGET must preserve the topology lane")

        def submit_mset_same_value(self, _keys: Any, _value: Any) -> Future[Any]:
            raise AssertionError("routed MSET must preserve the topology lane")

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )

    assert pool.submit_mget(["{tenant}:a", "{tenant}:b"]).result() == b"OK"
    assert pool.submit_mset_same_value(["{tenant}:a", "{tenant}:b"], b"v").result() == b"OK"
    assert calls == [
        (("MGET", "{tenant}:a", "{tenant}:b"), 7),
        (("MSET", "{tenant}:a", b"v", "{tenant}:b", b"v"), 7),
    ]


def test_async_topology_execute_batch_routes_keyed_commands_to_leader(monkeypatch):
    created: dict[str, Any] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.batches: list[list[tuple[Any, ...]]] = []

        async def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[bytes]:
            self.batches.append(list(commands))
            return [b"OK"] * len(commands)

        async def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        assert await pool.execute_batch([("SET", "key-a", b"a"), ("SET", "key-b", b"b")]) == [
            b"OK",
            b"OK",
        ]
        assert created["ferric://seed.local:6388"].batches == []
        assert created["ferric://leader.local:6391"].batches == [
            [("SET", "key-a", b"a"), ("SET", "key-b", b"b")]
        ]

    asyncio.run(run())


def test_async_topology_batch_groups_by_endpoint_and_exact_route_lane(monkeypatch):
    calls: list[tuple[int, list[tuple[Any, ...]]]] = []

    def topology() -> dict[str, Any]:
        payload = _two_shard_topology()
        payload["ranges"][1]["endpoint"] = dict(payload["ranges"][0]["endpoint"])
        payload["ranges"][0]["lane_id"] = 5
        payload["ranges"][1]["lane_id"] = 7
        return payload

    class FakeAdapter:
        async def execute_command(self, *args: Any) -> Any:
            return topology() if args[0] == "SHARDS" else b"OK"

        async def execute_batch(self, _commands: list[tuple[Any, ...]]) -> list[Any]:
            raise AssertionError("routed batches must preserve the topology lane")

        async def execute_batch_on_lane(
            self,
            commands: list[tuple[Any, ...]],
            lane_id: int,
        ) -> list[Any]:
            calls.append((lane_id, list(commands)))
            return [(lane_id, command[1]) for command in commands]

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    key_a = next(
        f"lane-a-{index}"
        for index in range(10_000)
        if RoutingTopology.slot_for_key(f"lane-a-{index}") < 512
    )
    key_b = next(
        f"lane-b-{index}"
        for index in range(10_000)
        if RoutingTopology.slot_for_key(f"lane-b-{index}") >= 512
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        assert await pool.execute_batch([("SET", key_a, b"a"), ("SET", key_b, b"b")]) == [
            (5, key_a),
            (7, key_b),
        ]

    asyncio.run(run())
    assert calls == [
        (5, [("SET", key_a, b"a")]),
        (7, [("SET", key_b, b"b")]),
    ]


def test_async_topology_rejects_cross_shard_multi_key_command(monkeypatch):
    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        async def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"unexpected"

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        with pytest.raises(InvalidCommandError, match="same slot"):
            await pool.execute_command("MGET", "a", "b")

    asyncio.run(run())


def test_async_topology_batch_failure_refreshes_routes_without_replaying_writes(monkeypatch):
    shards_calls = 0
    leader_batch_calls = 0

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        async def execute_command(self, *args: Any) -> Any:
            nonlocal shards_calls
            if args[0] == "SHARDS":
                shards_calls += 1
                return _single_shard_topology()
            return b"OK"

        async def execute_batch(self, _commands: list[tuple[Any, ...]]) -> list[Any]:
            nonlocal leader_batch_calls
            leader_batch_calls += 1
            raise OSError("leader moved")

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        with pytest.raises(OSError, match="leader moved"):
            await pool.execute_batch([("SET", "key", b"value")])
        assert leader_batch_calls == 1
        assert shards_calls == 2

    asyncio.run(run())


def test_async_topology_batch_waits_for_sibling_shards_before_raising(monkeypatch):
    sibling_completed = False

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        async def execute_command(self, *args: Any) -> Any:
            return _two_shard_topology() if args[0] == "SHARDS" else b"OK"

        async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
            nonlocal sibling_completed
            if "leader-a.local" in self.url:
                raise RuntimeError("shard batch failed")
            await asyncio.sleep(0.02)
            sibling_completed = True
            return [b"OK"] * len(commands)

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()

        with pytest.raises(RuntimeError, match="shard batch failed"):
            await pool.execute_batch(
                [
                    ("SET", "leader-a-key", b"a"),
                    ("SET", "leader-b-key", b"b"),
                ]
            )

        assert sibling_completed

    asyncio.run(run())


def test_async_topology_adapter_creation_failure_refreshes_routes(monkeypatch):
    shards_calls = 0

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        async def execute_command(self, *args: Any) -> Any:
            nonlocal shards_calls
            if args[0] == "SHARDS":
                shards_calls += 1
                return _single_shard_topology()
            return b"OK"

        async def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        if "leader.local" in url:
            raise OSError("leader creation failed")
        return FakeAdapter(url)

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        with pytest.raises(OSError, match="leader creation failed"):
            await pool.execute_batch([("SET", "key", b"value")])
        assert shards_calls == 2

    asyncio.run(run())


def test_submitted_batch_future_is_running_before_completion():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger("concurrent.futures")
    old_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)
    try:
        adapter = object.__new__(ProtocolAdapter)
        source: Future[ProtocolResponse] = Future()
        result: Future[list[Any]] = Future()
        result.set_running_or_notify_cancel()
        adapter._complete_batch_future(source, 1, result)
        assert result.cancel() is False

        source.set_result(ProtocolResponse(1, 1, 1, 0, 0, [b"OK"]))

        assert result.result(timeout=1) == [b"OK"]
        assert "InvalidStateError" not in stream.getvalue()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


def test_public_submit_pipeline_payload_future_is_running_on_return():
    adapter = object.__new__(ProtocolAdapter)
    source: Future[ProtocolResponse] = Future()
    adapter._submit_pipeline_payload = lambda *_args, **_kwargs: source

    result = adapter.submit_pipeline_payload(b"payload", 1)

    assert result.cancel() is False


def test_submit_commands_applies_write_timeout_without_joining_entire_batch():
    class FakeSocket:
        def __init__(self) -> None:
            self.previous_timeout = b"previous"
            self.timeout_options: list[bytes] = []
            self.writes: list[bytes] = []

        def getsockopt(self, _level: int, _option: int, _size: int) -> bytes:
            return self.previous_timeout

        def setsockopt(self, _level: int, _option: int, value: bytes) -> None:
            self.timeout_options.append(value)

        def settimeout(self, _value: float | None) -> None:
            pytest.fail("a write deadline must not change the shared read timeout")

        def sendall(self, value: bytes) -> None:
            self.writes.append(value)

    adapter = object.__new__(ProtocolAdapter)
    sock = FakeSocket()
    adapter._sock = sock
    adapter._closed = False
    adapter._connecting = False
    adapter._lock = threading.Lock()
    adapter._request_id = 0
    adapter._lane_cursor = 0
    adapter._pending = {}
    adapter._pending_traces = {}
    adapter._last_activity = 0.0
    adapter.timeout = 0.25
    adapter.compression = None
    adapter.lanes = 1
    adapter._ensure_connected = lambda: None

    adapter.submit_commands([("PING",), ("MULTI",)])

    assert len(sock.timeout_options) == 3
    assert sock.timeout_options[-1] == sock.previous_timeout
    assert len(sock.writes) == 2


def test_send_frames_uses_one_deadline_for_the_entire_batch(monkeypatch):
    now = 100.0

    class FakeSocket:
        def __init__(self) -> None:
            self.previous_timeout = b"previous"
            self.timeout_options: list[bytes] = []

        def getsockopt(self, _level: int, _option: int, _size: int) -> bytes:
            return self.previous_timeout

        def setsockopt(self, _level: int, _option: int, value: bytes) -> None:
            self.timeout_options.append(value)

        def sendall(self, _value: bytes) -> None:
            nonlocal now
            now += 0.04

    monkeypatch.setattr(protocol_module.time, "monotonic", lambda: now)
    sock = FakeSocket()

    protocol_module._send_frames(sock, [b"a", b"b", b"c"], timeout=0.1)

    def seconds(value: bytes) -> float:
        if len(value) == struct.calcsize("@I"):
            return struct.unpack("@I", value)[0] / 1_000
        whole, micros = struct.unpack("@ll", value)
        return whole + micros / 1_000_000

    applied = [seconds(value) for value in sock.timeout_options[:-1]]
    assert applied == pytest.approx([0.1, 0.06, 0.02], abs=0.000_01)
    assert sock.timeout_options[-1] == sock.previous_timeout


def test_protocol_pool_closes_partial_connections_when_construction_fails(monkeypatch):
    class FakeAdapter:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    created: list[FakeAdapter] = []

    def from_url(_url, **_kwargs):
        if created:
            raise OSError("second connection failed")
        adapter = FakeAdapter()
        created.append(adapter)
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", staticmethod(from_url))

    with pytest.raises(OSError, match="second connection failed"):
        ProtocolAdapterPool.from_url("ferric://seed.local:6388", max_connections=2)

    assert len(created) == 1
    assert created[0].closed is True


def test_topology_constructor_closes_seed_adapter_when_discovery_fails(monkeypatch):
    class FakeAdapter:
        def __init__(self) -> None:
            self.closed = False

        def execute_command(self, *_args):
            raise OSError("seed unavailable")

        def close(self) -> None:
            self.closed = True

    created: list[FakeAdapter] = []

    def from_url(_url, **_kwargs):
        adapter = FakeAdapter()
        created.append(adapter)
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", staticmethod(from_url))

    with pytest.raises(FerricStoreError, match="no FerricStore topology endpoint reachable"):
        protocol_module.TopologyProtocolAdapterPool(["ferric://seed.local:6388"])

    assert len(created) == 1
    assert created[0].closed is True


def test_topology_control_uses_last_good_seed_and_rotates_safe_commands(monkeypatch):
    availability = {"seed-a": False, "seed-b": True}
    topology_epoch = 0
    created: dict[str, Any] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.host = "seed-a" if "seed-a" in url else "seed-b"
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args):
            nonlocal topology_epoch
            self.calls.append(args)
            if not availability[self.host]:
                raise OSError(f"{self.host} unavailable")
            if args[0] == "SHARDS":
                topology_epoch += 1
                return _single_shard_topology(
                    self.host,
                    6388,
                    route_epoch=topology_epoch,
                )
            return f"PONG:{self.host}".encode()

        def close(self) -> None:
            pass

    def from_url(url, **_kwargs):
        return created.setdefault(url, FakeAdapter(url))

    monkeypatch.setattr(ProtocolAdapter, "from_url", staticmethod(from_url))
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed-a:6388", "ferric://seed-b:6388"]
    )
    try:
        assert pool.execute_command("PING") == b"PONG:seed-b"

        availability["seed-a"] = True
        availability["seed-b"] = False
        assert pool.execute_command("PING") == b"PONG:seed-a"

        availability["seed-a"] = False
        availability["seed-b"] = True
        with pytest.raises(OSError, match="seed-a unavailable"):
            pool.execute_command("CLIENT.SETNAME", "review-client")
        assert ("CLIENT.SETNAME", "review-client") not in created["ferric://seed-b:6388"].calls
        assert pool.execute_command("PING") == b"PONG:seed-b"
    finally:
        pool.close()


def test_topology_control_fast_path_reuses_last_good_adapter_without_rebuilding_candidates(
    monkeypatch,
):
    class FakeAdapter:
        def execute_command(self, *args):
            if args[0] == "SHARDS":
                return _single_shard_topology("seed.local", 6388)
            return b"PONG"

        def close(self):
            pass

    monkeypatch.setattr(ProtocolAdapter, "from_url", lambda *_args, **_kwargs: FakeAdapter())
    pool = protocol_module.TopologyProtocolAdapterPool(["ferric://seed.local:6388"])
    try:
        monkeypatch.setattr(
            pool,
            "_refresh_candidate_urls",
            lambda: pytest.fail("steady-state control lookup rebuilt topology candidates"),
        )
        assert pool.execute_command("PING") == b"PONG"
    finally:
        pool.close()


def test_async_topology_control_uses_last_good_seed_and_rotates_safe_commands(monkeypatch):
    availability = {"seed-a": False, "seed-b": True}
    topology_epoch = 0
    created: dict[str, Any] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.host = "seed-a" if "seed-a" in url else "seed-b"
            self.calls: list[tuple[Any, ...]] = []

        async def execute_command(self, *args):
            nonlocal topology_epoch
            self.calls.append(args)
            if not availability[self.host]:
                raise OSError(f"{self.host} unavailable")
            if args[0] == "SHARDS":
                topology_epoch += 1
                return _single_shard_topology(
                    self.host,
                    6388,
                    route_epoch=topology_epoch,
                )
            return f"PONG:{self.host}".encode()

        async def close(self) -> None:
            pass

    def from_url(url, **_kwargs):
        return created.setdefault(url, FakeAdapter(url))

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", staticmethod(from_url))

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed-a:6388", "ferric://seed-b:6388"]
        )
        await pool.refresh_topology()
        try:
            assert await pool.execute_command("PING") == b"PONG:seed-b"

            availability["seed-a"] = True
            availability["seed-b"] = False
            assert await pool.execute_command("PING") == b"PONG:seed-a"

            availability["seed-a"] = False
            availability["seed-b"] = True
            with pytest.raises(OSError, match="seed-a unavailable"):
                await pool.execute_command("CLIENT.SETNAME", "review-client")
            assert ("CLIENT.SETNAME", "review-client") not in created["ferric://seed-b:6388"].calls
            assert await pool.execute_command("PING") == b"PONG:seed-b"
        finally:
            await pool.close()

    asyncio.run(run())


def test_async_topology_control_fast_path_reuses_last_good_adapter_without_rebuilding_candidates(
    monkeypatch,
):
    class FakeAdapter:
        async def execute_command(self, *args):
            if args[0] == "SHARDS":
                return _single_shard_topology("seed.local", 6388)
            return b"PONG"

        async def close(self):
            pass

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", lambda *_args, **_kwargs: FakeAdapter())

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(["ferric://seed.local:6388"])
        await pool.refresh_topology()
        try:
            monkeypatch.setattr(
                pool,
                "_refresh_candidate_urls",
                lambda: pytest.fail("steady-state control lookup rebuilt topology candidates"),
            )
            assert await pool.execute_command("PING") == b"PONG"
        finally:
            await pool.close()

    asyncio.run(run())
