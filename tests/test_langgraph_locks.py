from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import ferricstore.langgraph._locks as lock_module


class RenewalClient:
    def __init__(self) -> None:
        self.owner: str | None = None
        self.commands: list[tuple[Any, ...]] = []
        self.guard = threading.Lock()

    def command(self, *args: Any) -> Any:
        with self.guard:
            self.commands.append(args)
            name = str(args[0]).upper()
            owner = str(args[2])
            if name == "LOCK":
                if self.owner not in (None, owner):
                    return None
                self.owner = owner
                return "OK"
            if name == "EXTEND":
                return int(self.owner == owner)
            if name == "UNLOCK":
                if self.owner != owner:
                    return 0
                self.owner = None
                return 1
            raise AssertionError(f"unexpected command: {args!r}")


class AsyncRenewalClient:
    def __init__(self) -> None:
        self.sync = RenewalClient()

    async def command(self, *args: Any) -> Any:
        return self.sync.command(*args)


def test_sync_lock_is_extended_while_mutation_runs(monkeypatch: Any) -> None:
    monkeypatch.setattr(lock_module, "_LOCK_TTL_MS", 60)
    client = RenewalClient()

    result = lock_module.run_sync_with_locks(
        client,
        ["lock:one"],
        lambda: (time.sleep(0.09), "done")[1],
    )

    assert result == "done"
    assert any(command[0] == "EXTEND" for command in client.commands)
    assert client.owner is None


def test_async_lock_is_extended_while_mutation_runs(monkeypatch: Any) -> None:
    monkeypatch.setattr(lock_module, "_LOCK_TTL_MS", 60)
    client = AsyncRenewalClient()

    async def run() -> str:
        async def operation() -> str:
            await asyncio.sleep(0.09)
            return "done"

        return await lock_module.run_async_with_locks(
            client,
            ["lock:one"],
            operation,
        )

    assert asyncio.run(run()) == "done"
    assert any(command[0] == "EXTEND" for command in client.sync.commands)
    assert client.sync.owner is None
