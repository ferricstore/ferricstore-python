from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ferricstore.protocol_async_pool import AsyncProtocolAdapterPool
from ferricstore.protocol_sync_pool import ProtocolAdapterPool


def test_sync_pool_high_contention_preserves_lease_and_activity_invariants() -> None:
    class Adapter:
        def __init__(self) -> None:
            self._lock = threading.Lock()
            self.calls = 0
            self.closed = False
            self.listeners: list[Any] = []

        def add_event_listener(self, listener: Any) -> None:
            self.listeners.append(listener)

        def remove_event_listener(self, listener: Any) -> None:
            self.listeners.remove(listener)

        def execute_command(self, _name: str, value: int) -> int:
            with self._lock:
                self.calls += 1
            time.sleep(0.0001)
            return value

        def close(self) -> None:
            self.closed = True

    adapters = [Adapter() for _ in range(4)]
    pool = ProtocolAdapterPool(adapters)  # type: ignore[arg-type]

    def run(index: int) -> int:
        if index % 19:
            return pool.execute_command("ECHO", index)
        session = pool.acquire_session()
        try:
            return session.execute_command("ECHO", index)
        finally:
            session.close()

    try:
        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = [executor.submit(run, index) for index in range(1_000)]
            results = [future.result(timeout=10) for future in futures]

        assert results == list(range(1_000))
        assert pool._active == [0, 0, 0, 0]
        assert pool._leased == set()
        assert pool._session_waiters == 0
        assert all(adapter.calls > 0 for adapter in adapters)
    finally:
        pool.close()

    assert all(adapter.closed for adapter in adapters)


def test_async_pool_high_contention_and_cancellation_preserve_invariants() -> None:
    class Adapter:
        def __init__(self) -> None:
            self.calls = 0
            self.closed = False
            self.listeners: list[Any] = []

        def add_event_listener(self, listener: Any) -> None:
            self.listeners.append(listener)

        def remove_event_listener(self, listener: Any) -> None:
            self.listeners.remove(listener)

        async def execute_command(self, _name: str, value: int) -> int:
            self.calls += 1
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            return value

        async def close(self) -> None:
            self.closed = True

    async def exercise() -> None:
        adapters = [Adapter() for _ in range(4)]
        pool = AsyncProtocolAdapterPool(adapters)  # type: ignore[arg-type]

        async def run(index: int) -> int:
            if index % 17:
                return await pool.execute_command("ECHO", index)
            session = await pool.acquire_session()
            try:
                return await session.execute_command("ECHO", index)
            finally:
                await session.close()

        tasks = [asyncio.create_task(run(index)) for index in range(800)]
        await asyncio.sleep(0)
        for task in tasks[::13]:
            task.cancel()
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=10,
        )

        assert sum(isinstance(result, asyncio.CancelledError) for result in results) > 0
        assert pool._active == [0, 0, 0, 0]
        assert pool._leased == set()
        assert pool._session_waiters == 0
        assert all(adapter.calls > 0 for adapter in adapters)
        await pool.close()
        assert all(adapter.closed for adapter in adapters)

    asyncio.run(exercise())
