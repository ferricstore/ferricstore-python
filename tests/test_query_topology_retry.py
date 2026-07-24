from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, BackpressurePolicy, FlowClient
from ferricstore.backpressure import BackpressureController
from ferricstore.errors import FerricStoreError, OverloadedError
from ferricstore.flow_query_request import (
    _with_flow_query_command_options,
    build_flow_query_args,
)
from ferricstore.flow_query_retry import (
    execute_flow_query_read_with_retry,
    execute_flow_query_read_with_retry_async,
)
from ferricstore.flow_routing import flow_auto_id_routing_key
from ferricstore.protocol_async_pool import AsyncProtocolAdapterPool
from ferricstore.protocol_async_topology import AsyncTopologyProtocolAdapterPool
from ferricstore.protocol_sync_pool import ProtocolAdapterPool
from ferricstore.protocol_sync_topology import TopologyProtocolAdapterPool

QUERY_COMMAND = (
    "FLOW.QUERY",
    "FQL1",
    "FROM runs WHERE run_id = @id RETURN RECORD",
    "id",
    "run-1",
)
QUERY_INDEXES_COMMAND = ("FLOW.QUERY.INDEXES",)
ROUTED_QUERY_COMMAND = tuple(
    _with_flow_query_command_options(
        build_flow_query_args(QUERY_COMMAND[2], {"id": "run-1"}),
        deadline_ms=None,
        routing_key=flow_auto_id_routing_key("run-1"),
    )
)
QUERY_RETRY_COMMANDS = (QUERY_COMMAND, QUERY_INDEXES_COMMAND, ROUTED_QUERY_COMMAND)


def _one_retry_policy() -> BackpressurePolicy:
    return BackpressurePolicy(
        max_retries=1,
        max_elapsed_ms=1_000,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )


def _shared_pressure_controllers() -> tuple[BackpressureController, BackpressureController]:
    policy = BackpressurePolicy(
        max_retries=1,
        max_elapsed_ms=60_000,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=True,
    )
    scope = object()
    return (
        BackpressureController(policy, scope=scope),
        BackpressureController(policy, scope=scope),
    )


def test_sync_query_read_honors_shared_pressure_before_sending() -> None:
    owner, waiter = _shared_pressure_controllers()
    with owner._state.lock:
        owner._state.blocked_until = time.monotonic() + 1.0
    calls = 0

    def operation() -> str:
        nonlocal calls
        calls += 1
        return "sent"

    with pytest.raises(OverloadedError, match="deadline"):
        execute_flow_query_read_with_retry(
            operation,
            waiter,
            deadline_ms=int(time.time() * 1_000) + 10,
        )

    assert calls == 0


def test_async_query_read_honors_shared_pressure_before_sending() -> None:
    async def run() -> int:
        owner, waiter = _shared_pressure_controllers()
        with owner._state.lock:
            owner._state.blocked_until = time.monotonic() + 1.0
        calls = 0

        async def operation() -> str:
            nonlocal calls
            calls += 1
            return "sent"

        with pytest.raises(OverloadedError, match="deadline"):
            await execute_flow_query_read_with_retry_async(
                operation,
                waiter,
                deadline_ms=int(time.time() * 1_000) + 10,
            )
        return calls

    assert asyncio.run(run()) == 0


def test_sync_zero_deadline_disables_retry_and_pressure_deadline_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = BackpressureController(
        BackpressurePolicy(
            max_retries=1,
            max_elapsed_ms=None,
            base_delay_ms=0,
            max_delay_ms=0,
            jitter=0,
            shared=False,
        )
    )
    clock = [0.0]
    sleeps: list[float] = []
    controller._state.blocked_until = 0.1
    retryable = FerricStoreError(
        "reroute",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )
    calls = 0

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise retryable
        return "sent"

    monkeypatch.setattr("ferricstore.backpressure.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("ferricstore.backpressure.time.sleep", sleep)

    assert execute_flow_query_read_with_retry(operation, controller, deadline_ms=0) == "sent"
    assert calls == 2
    assert sleeps == [0.1]


def test_async_zero_deadline_disables_retry_and_pressure_deadline_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> tuple[str, int, list[float]]:
        controller = BackpressureController(
            BackpressurePolicy(
                max_retries=1,
                max_elapsed_ms=None,
                base_delay_ms=0,
                max_delay_ms=0,
                jitter=0,
                shared=False,
            )
        )
        clock = [0.0]
        sleeps: list[float] = []
        controller._state.blocked_until = 0.1
        retryable = FerricStoreError(
            "reroute",
            retryable=True,
            safe_to_retry=True,
            retry_after_ms=0,
        )
        calls = 0

        async def sleep(delay: float) -> None:
            sleeps.append(delay)
            clock[0] += delay

        async def operation() -> str:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise retryable
            return "sent"

        monkeypatch.setattr("ferricstore.backpressure.time.monotonic", lambda: clock[0])
        monkeypatch.setattr(asyncio, "sleep", sleep)
        result = await execute_flow_query_read_with_retry_async(
            operation,
            controller,
            deadline_ms=0,
        )
        return result, calls, sleeps

    assert asyncio.run(run()) == ("sent", 2, [0.1])


def test_sync_shared_pressure_rechecks_absolute_remaining_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = BackpressureController(BackpressurePolicy(max_elapsed_ms=None, shared=False))
    clock = [0.0]
    sleeps: list[float] = []
    controller._state.blocked_until = 0.4

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay
        controller._state.blocked_until = 0.8

    monkeypatch.setattr("ferricstore.backpressure.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("ferricstore.backpressure.time.sleep", sleep)

    assert controller.before_request(remaining_s=0.5) is False
    assert sleeps == [0.4]


def test_async_shared_pressure_rechecks_absolute_remaining_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> tuple[bool, list[float]]:
        controller = BackpressureController(BackpressurePolicy(max_elapsed_ms=None, shared=False))
        clock = [0.0]
        sleeps: list[float] = []
        controller._state.blocked_until = 0.4

        async def sleep(delay: float) -> None:
            sleeps.append(delay)
            clock[0] += delay
            controller._state.blocked_until = 0.8

        monkeypatch.setattr("ferricstore.backpressure.time.monotonic", lambda: clock[0])
        monkeypatch.setattr(asyncio, "sleep", sleep)
        return await controller.before_request_async(remaining_s=0.5), sleeps

    allowed, sleeps = asyncio.run(run())

    assert allowed is False
    assert sleeps == [0.4]


def _topology() -> dict[str, Any]:
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
                    "native_port": 6388,
                },
            }
        ],
    }


@pytest.mark.parametrize("command", QUERY_RETRY_COMMANDS)
def test_sync_topology_retries_safe_query_read_after_refresh(
    monkeypatch: pytest.MonkeyPatch,
    command: tuple[Any, ...],
) -> None:
    state = {"shards": 0, "reads": 0}
    retryable = FerricStoreError("reroute", retryable=True, safe_to_retry=True)

    class Adapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                state["shards"] += 1
                return _topology()
            state["reads"] += 1
            if state["reads"] == 1:
                raise retryable
            return b"query-ok"

        def close(self) -> None:
            pass

    adapter = Adapter()
    monkeypatch.setattr(
        ProtocolAdapterPool,
        "from_url",
        lambda *_args, **_kwargs: adapter,
    )
    pool = TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )
    try:
        assert pool.execute_command(*command) == b"query-ok"
    finally:
        pool.close()

    assert state == {"shards": 2, "reads": 2}


@pytest.mark.parametrize("command", QUERY_RETRY_COMMANDS)
def test_sync_topology_never_replays_query_when_server_marks_retry_unsafe(
    monkeypatch: pytest.MonkeyPatch,
    command: tuple[Any, ...],
) -> None:
    state = {"shards": 0, "reads": 0}
    unsafe = FerricStoreError("reroute", retryable=True, safe_to_retry=False)

    class Adapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                state["shards"] += 1
                return _topology()
            state["reads"] += 1
            raise unsafe

        def close(self) -> None:
            pass

    adapter = Adapter()
    monkeypatch.setattr(
        ProtocolAdapterPool,
        "from_url",
        lambda *_args, **_kwargs: adapter,
    )
    pool = TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
    )
    try:
        with pytest.raises(FerricStoreError, match="reroute") as raised:
            pool.execute_command(*command)
    finally:
        pool.close()

    assert raised.value is unsafe
    assert state["reads"] == 1
    assert state["shards"] >= 2


def test_sync_topology_busy_retry_honors_hint_without_refreshing_topology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"shards": 0, "reads": 0}
    sleeps: list[float] = []
    retryable = FerricStoreError(
        "busy",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=125,
    )

    class Adapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                state["shards"] += 1
                return _topology()
            state["reads"] += 1
            if state["reads"] == 1:
                raise retryable
            return b"query-ok"

        def close(self) -> None:
            pass

    adapter = Adapter()
    monkeypatch.setattr(ProtocolAdapterPool, "from_url", lambda *_a, **_k: adapter)
    monkeypatch.setattr("ferricstore.protocol_sync_topology.time.sleep", sleeps.append)
    pool = TopologyProtocolAdapterPool(["ferric://seed.local:6388"], endpoint_policy="any")
    try:
        assert pool.execute_command(*QUERY_COMMAND) == b"query-ok"
    finally:
        pool.close()

    assert state == {"shards": 1, "reads": 2}
    assert sleeps == [0.125]


def test_sync_client_owns_topology_query_replay_and_deadline_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"shards": 0, "reads": 0}
    retryable = FerricStoreError(
        "query_projection_changed",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )

    class Adapter:
        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                state["shards"] += 1
                return _topology()
            state["reads"] += 1
            raise retryable

        def close(self) -> None:
            pass

    adapter = Adapter()
    monkeypatch.setattr(ProtocolAdapterPool, "from_url", lambda *_a, **_k: adapter)
    pool = TopologyProtocolAdapterPool(["ferric://seed.local:6388"], endpoint_policy="any")
    try:
        client = FlowClient(pool, backpressure=_one_retry_policy())
        with pytest.raises(FerricStoreError, match="query_projection_changed"):
            client.query(QUERY_COMMAND[2], {"id": "run-1"})
        assert state["reads"] == 2

        state["reads"] = 0
        past_deadline_ms = int(time.time() * 1_000) - 1
        with pytest.raises(FerricStoreError, match="query_projection_changed"):
            client.query(QUERY_COMMAND[2], {"id": "run-1"}, deadline_ms=past_deadline_ms)
        assert state["reads"] == 1
    finally:
        pool.close()


@pytest.mark.parametrize("command", QUERY_RETRY_COMMANDS)
def test_async_topology_retries_safe_query_read_after_refresh(
    monkeypatch: pytest.MonkeyPatch,
    command: tuple[Any, ...],
) -> None:
    state = {"shards": 0, "reads": 0}
    retryable = FerricStoreError("reroute", retryable=True, safe_to_retry=True)

    class Adapter:
        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                state["shards"] += 1
                return _topology()
            state["reads"] += 1
            if state["reads"] == 1:
                raise retryable
            return b"query-ok"

        async def close(self) -> None:
            pass

    adapter = Adapter()
    monkeypatch.setattr(
        AsyncProtocolAdapterPool,
        "from_url",
        lambda *_args, **_kwargs: adapter,
    )

    async def run() -> None:
        pool = AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
        )
        await pool.refresh_topology()
        try:
            assert await pool.execute_command(*command) == b"query-ok"
        finally:
            await pool.close()

    asyncio.run(run())
    assert state == {"shards": 2, "reads": 2}


@pytest.mark.parametrize("command", QUERY_RETRY_COMMANDS)
def test_async_topology_never_replays_query_when_server_marks_retry_unsafe(
    monkeypatch: pytest.MonkeyPatch,
    command: tuple[Any, ...],
) -> None:
    state = {"shards": 0, "reads": 0}
    unsafe = FerricStoreError("reroute", retryable=True, safe_to_retry=False)

    class Adapter:
        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                state["shards"] += 1
                return _topology()
            state["reads"] += 1
            raise unsafe

        async def close(self) -> None:
            pass

    adapter = Adapter()
    monkeypatch.setattr(
        AsyncProtocolAdapterPool,
        "from_url",
        lambda *_args, **_kwargs: adapter,
    )

    async def run() -> None:
        pool = AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
        )
        await pool.refresh_topology()
        try:
            with pytest.raises(FerricStoreError, match="reroute") as raised:
                await pool.execute_command(*command)
        finally:
            await pool.close()

        assert raised.value is unsafe

    asyncio.run(run())
    assert state["reads"] == 1
    assert state["shards"] >= 2


def test_async_topology_busy_retry_honors_hint_without_refreshing_topology(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"shards": 0, "reads": 0}
    sleeps: list[float] = []
    retryable = FerricStoreError(
        "busy",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=75,
    )

    class Adapter:
        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                state["shards"] += 1
                return _topology()
            state["reads"] += 1
            if state["reads"] == 1:
                raise retryable
            return b"query-ok"

        async def close(self) -> None:
            pass

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    adapter = Adapter()
    monkeypatch.setattr(AsyncProtocolAdapterPool, "from_url", lambda *_a, **_k: adapter)
    monkeypatch.setattr(asyncio, "sleep", sleep)

    async def run() -> None:
        pool = AsyncTopologyProtocolAdapterPool(["ferric://seed.local:6388"], endpoint_policy="any")
        await pool.refresh_topology()
        try:
            assert await pool.execute_command(*QUERY_COMMAND) == b"query-ok"
        finally:
            await pool.close()

    asyncio.run(run())
    assert state == {"shards": 1, "reads": 2}
    assert sleeps == [0.075]


def test_async_client_owns_topology_query_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"shards": 0, "reads": 0}
    retryable = FerricStoreError(
        "query_projection_changed",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )

    class Adapter:
        async def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                state["shards"] += 1
                return _topology()
            state["reads"] += 1
            raise retryable

        async def close(self) -> None:
            pass

    adapter = Adapter()
    monkeypatch.setattr(AsyncProtocolAdapterPool, "from_url", lambda *_a, **_k: adapter)

    async def run() -> None:
        pool = AsyncTopologyProtocolAdapterPool(["ferric://seed.local:6388"], endpoint_policy="any")
        await pool.refresh_topology()
        try:
            client = AsyncFlowClient(pool, backpressure=_one_retry_policy())
            with pytest.raises(FerricStoreError, match="query_projection_changed"):
                await client.query(QUERY_COMMAND[2], {"id": "run-1"})
            assert state["reads"] == 2
        finally:
            await pool.close()

    asyncio.run(run())
