from __future__ import annotations

import asyncio
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol, TypeVar

from ferricstore.errors import LockHeldError


class _SyncLockClient(Protocol):
    def command(self, *args: Any) -> Any: ...


class _AsyncLockClient(Protocol):
    async def command(self, *args: Any) -> Any: ...


_ResultT = TypeVar("_ResultT")
_LOCK_TTL_MS = 300_000
_LOCK_WAIT_SECONDS = 30.0
_LOCK_RETRY_SECONDS = 0.01
_LOCK_RENEW_DIVISOR = 3


def _lock_acquired(response: Any) -> bool:
    return response in ("OK", b"OK", True)


def _lock_extended(response: Any) -> bool:
    return response in (1, "1", b"1", True)


def _renew_interval_seconds() -> float:
    return max(_LOCK_TTL_MS / 1000 / _LOCK_RENEW_DIVISOR, 0.01)


def _renew_retry_seconds() -> float:
    return min(max(_renew_interval_seconds() / 10, _LOCK_RETRY_SECONDS), 1.0)


def _lease_lost(key: str, reason: str) -> RuntimeError:
    return RuntimeError(f"lost FerricStore lock {key!r} while mutating data: {reason}")


def _lock_keys(keys: Sequence[str]) -> list[str]:
    return sorted(set(keys))


def _try_sync_lock(
    client: _SyncLockClient,
    key: str,
    owner: str,
) -> bool:
    try:
        return _lock_acquired(client.command("LOCK", key, owner, _LOCK_TTL_MS))
    except LockHeldError:
        return False


async def _try_async_lock(
    client: _AsyncLockClient,
    key: str,
    owner: str,
) -> bool:
    try:
        return _lock_acquired(await client.command("LOCK", key, owner, _LOCK_TTL_MS))
    except LockHeldError:
        return False


def run_sync_with_locks(
    client: _SyncLockClient,
    keys: Sequence[str],
    operation: Callable[[], _ResultT],
) -> _ResultT:
    ordered = _lock_keys(keys)
    if not ordered:
        return operation()
    owner = uuid.uuid4().hex
    acquired: list[str] = []
    last_extended: dict[str, float] = {}
    deadline = time.monotonic() + _LOCK_WAIT_SECONDS
    primary_error = False
    heartbeat_error: list[BaseException] = []
    stop_heartbeat = threading.Event()
    heartbeat: threading.Thread | None = None
    try:
        for key in ordered:
            while not _try_sync_lock(client, key, owner):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out acquiring FerricStore lock {key!r}")
                time.sleep(_LOCK_RETRY_SECONDS)
            acquired.append(key)
            last_extended[key] = time.monotonic()

        def renew_locks() -> None:
            wait_seconds = _renew_interval_seconds()
            ttl_seconds = _LOCK_TTL_MS / 1000
            while not stop_heartbeat.wait(wait_seconds):
                now = time.monotonic()
                retry = False
                for key in acquired:
                    try:
                        response = client.command("EXTEND", key, owner, _LOCK_TTL_MS)
                    except BaseException as exc:  # pragma: no cover - transport-specific
                        if now - last_extended[key] >= ttl_seconds:
                            heartbeat_error.append(_lease_lost(key, str(exc)))
                            return
                        retry = True
                        continue
                    if not _lock_extended(response):
                        heartbeat_error.append(_lease_lost(key, "extension was rejected"))
                        return
                    last_extended[key] = now
                wait_seconds = _renew_retry_seconds() if retry else _renew_interval_seconds()

        heartbeat = threading.Thread(
            target=renew_locks,
            name="ferricstore-langgraph-lock-renewal",
            daemon=True,
        )
        heartbeat.start()
        return operation()
    except BaseException:
        primary_error = True
        raise
    finally:
        stop_heartbeat.set()
        if heartbeat is not None:
            heartbeat.join()
        release_error: BaseException | None = None
        for key in reversed(acquired):
            try:
                client.command("UNLOCK", key, owner)
            except BaseException as exc:  # pragma: no cover - transport-specific
                if release_error is None:
                    release_error = exc
        if not primary_error:
            if heartbeat_error:
                raise heartbeat_error[0]
            if release_error is not None:
                raise release_error


async def run_async_with_locks(
    client: _AsyncLockClient,
    keys: Sequence[str],
    operation: Callable[[], Awaitable[_ResultT]],
) -> _ResultT:
    ordered = _lock_keys(keys)
    if not ordered:
        return await operation()
    owner = uuid.uuid4().hex
    acquired: list[str] = []
    last_extended: dict[str, float] = {}
    deadline = time.monotonic() + _LOCK_WAIT_SECONDS
    primary_error = False
    heartbeat_error: list[BaseException] = []
    stop_heartbeat = asyncio.Event()
    heartbeat: asyncio.Task[None] | None = None
    try:
        for key in ordered:
            while not await _try_async_lock(client, key, owner):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out acquiring FerricStore lock {key!r}")
                await asyncio.sleep(_LOCK_RETRY_SECONDS)
            acquired.append(key)
            last_extended[key] = time.monotonic()

        async def renew_locks() -> None:
            wait_seconds = _renew_interval_seconds()
            ttl_seconds = _LOCK_TTL_MS / 1000
            while True:
                try:
                    await asyncio.wait_for(stop_heartbeat.wait(), timeout=wait_seconds)
                    return
                except asyncio.TimeoutError:
                    pass
                now = time.monotonic()
                retry = False
                for key in acquired:
                    try:
                        response = await client.command("EXTEND", key, owner, _LOCK_TTL_MS)
                    except BaseException as exc:  # pragma: no cover - transport-specific
                        if now - last_extended[key] >= ttl_seconds:
                            heartbeat_error.append(_lease_lost(key, str(exc)))
                            return
                        retry = True
                        continue
                    if not _lock_extended(response):
                        heartbeat_error.append(_lease_lost(key, "extension was rejected"))
                        return
                    last_extended[key] = now
                wait_seconds = _renew_retry_seconds() if retry else _renew_interval_seconds()

        heartbeat = asyncio.create_task(renew_locks())
        return await operation()
    except BaseException:
        primary_error = True
        raise
    finally:
        stop_heartbeat.set()
        if heartbeat is not None:
            await heartbeat
        release_error: BaseException | None = None
        for key in reversed(acquired):
            try:
                await client.command("UNLOCK", key, owner)
            except BaseException as exc:  # pragma: no cover - transport-specific
                if release_error is None:
                    release_error = exc
        if not primary_error:
            if heartbeat_error:
                raise heartbeat_error[0]
            if release_error is not None:
                raise release_error
