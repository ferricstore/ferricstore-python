from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, BackpressurePolicy, FlowClient, OverloadedError


def _zero_retry_budget() -> BackpressurePolicy:
    return BackpressurePolicy(
        max_elapsed_ms=0,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )


def test_zero_elapsed_budget_allows_sync_initial_producer_request() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args: Any) -> bytes:
            self.calls.append(args)
            return b"OK"

    executor = Executor()
    client = FlowClient(executor, backpressure=_zero_retry_budget())

    assert client.enqueue("flow-1", type="order", now_ms=1) == b"OK"
    assert len(executor.calls) == 1


def test_zero_elapsed_budget_disables_sync_overload_retry() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls = 0

        def execute_command(self, *_args: Any) -> bytes:
            self.calls += 1
            raise OverloadedError("busy")

    executor = Executor()
    client = FlowClient(executor, backpressure=_zero_retry_budget())

    with pytest.raises(OverloadedError, match="busy"):
        client.enqueue("flow-1", type="order", now_ms=1)
    assert executor.calls == 1


def test_zero_elapsed_budget_allows_async_initial_request_and_disables_retry() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls = 0
            self.overloaded = False

        async def execute_command(self, *_args: Any) -> bytes:
            self.calls += 1
            if self.overloaded:
                raise OverloadedError("busy")
            return b"OK"

    async def run() -> None:
        executor = Executor()
        client = AsyncFlowClient(executor, backpressure=_zero_retry_budget())

        assert await client.enqueue("flow-1", type="order", now_ms=1) == b"OK"
        assert executor.calls == 1

        executor.overloaded = True
        with pytest.raises(OverloadedError, match="busy"):
            await client.enqueue("flow-2", type="order", now_ms=1)
        assert executor.calls == 2

    asyncio.run(run())
