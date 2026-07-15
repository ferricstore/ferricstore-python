from __future__ import annotations

import asyncio
import contextlib
import inspect
from asyncio import TimeoutError as AsyncioTimeoutError
from collections.abc import Sequence
from typing import Any

from ferricstore.config_validation import validate_string_sequence


class AsyncFlowWakeCoordinator:
    """Own one protocol subscription and broadcast its events to all workers."""

    _MIN_RECOVERY_DELAY_S = 0.01
    _MAX_RECOVERY_DELAY_S = 0.5

    def __init__(
        self,
        client: Any,
        *,
        type: str,
        state: str | None,
        states: Sequence[str] | None,
        partition_key: str | None,
        partition_keys: Sequence[str] | None,
        priority: int | None,
        limit: int,
        enabled: bool,
    ) -> None:
        self._client = client
        self._subscription_client = client
        self._owns_subscription_client = False
        self._subscription_client_acquired = False
        self._type = type
        self._state = state
        self._states = (
            validate_string_sequence(states, name="states", allow_empty=False)
            if states is not None
            else None
        )
        self._partition_key = partition_key
        self._partition_keys = (
            validate_string_sequence(
                partition_keys,
                name="partition_keys",
                allow_empty=False,
            )
            if partition_keys is not None
            else None
        )
        self._priority = priority
        self._limit = limit
        self._requested = enabled
        self._enabled = False
        self._subscribed = False
        self._closed = False
        self._generation = 0
        self._error: BaseException | None = None
        self._subscription_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._condition = asyncio.Condition()
        self._pump_task: asyncio.Task[None] | None = None
        self._activation_task: asyncio.Task[bool] | None = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def subscribed(self) -> bool:
        return self._subscribed

    async def subscribe(self) -> bool:
        if not self._requested or self._closed:
            return False
        async with self._subscription_lock:
            await self._ensure_subscription_client()
            subscribe = getattr(self._subscription_client, "subscribe_flow_wake", None)
            wait_event = getattr(self._subscription_client, "wait_event", None)
            if not callable(subscribe) or not callable(wait_event):
                return False
            task = self._pump_task
            if self._subscribed and task is not None and not task.done():
                return self._enabled
        if not await self._activate_once():
            return False
        async with self._subscription_lock:
            if self._closed:
                return False
            self._subscribed = True
        async with self._subscription_lock:
            if self._closed:
                return False
            task = self._pump_task
            if task is None or task.done():
                self._pump_task = asyncio.create_task(self._pump(wait_event))
        return True

    async def _ensure_subscription_client(self) -> None:
        if self._subscription_client_acquired:
            return
        acquire = getattr(self._client, "_acquire_subscription_client", None)
        if callable(acquire):
            acquired = acquire()
            if inspect.isawaitable(acquired):
                acquired = await acquired
            self._subscription_client, self._owns_subscription_client = acquired
        self._subscription_client_acquired = True

    async def _activate_once(self) -> bool:
        async with self._subscription_lock:
            if self._closed:
                return False
            task = self._activation_task
            if task is None or task.done():
                task = asyncio.create_task(self._activate_and_set_active())
                self._activation_task = task
        try:
            activated = await asyncio.shield(task)
        except asyncio.CancelledError:
            if self._closed:
                return False
            raise
        finally:
            if task.done():
                async with self._subscription_lock:
                    if self._activation_task is task:
                        self._activation_task = None
        return activated and not self._closed

    async def _activate_and_set_active(self) -> bool:
        await self._activate_subscription()
        return await self._set_active()

    async def _activate_subscription(self) -> None:
        subscribe = getattr(self._subscription_client, "subscribe_flow_wake", None)
        if not callable(subscribe):
            raise RuntimeError("FLOW_WAKE subscription is unavailable")
        result = subscribe(
            self._type,
            state=self._state,
            states=list(self._states) if self._states is not None else None,
            partition_key=self._partition_key,
            partition_keys=(
                list(self._partition_keys) if self._partition_keys is not None else None
            ),
            priority=self._priority,
            limit=self._limit,
        )
        if inspect.isawaitable(result):
            await result

    async def _set_active(self) -> bool:
        async with self._condition:
            if self._closed:
                return False
            self._error = None
            self._enabled = True
            self._condition.notify_all()
            return True

    async def wait(self, generation: int, timeout_s: float) -> tuple[bool, int]:
        if not self._enabled or timeout_s <= 0:
            return False, generation
        async with self._condition:
            if self._generation != generation:
                return True, self._generation
            if self._error is not None:
                raise self._error
            if self._closed:
                return False, generation

            def ready() -> bool:
                return self._generation != generation or self._error is not None or self._closed

            try:
                await asyncio.wait_for(self._condition.wait_for(ready), timeout=timeout_s)
            except AsyncioTimeoutError:
                return False, generation
            if self._error is not None:
                raise self._error
            if self._generation != generation:
                return True, self._generation
            return False, generation

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed and not self._owns_subscription_client:
                return
            task: asyncio.Task[None] | None = None
            activation_task: asyncio.Task[bool] | None = None
            if not self._closed:
                self._closed = True
                self._enabled = False
                async with self._subscription_lock:
                    task = self._pump_task
                    activation_task = self._activation_task
                    self._pump_task = None
                    self._activation_task = None
                    self._subscribed = False
            current_task = asyncio.current_task()
            pending = {
                item
                for item in (task, activation_task)
                if item is not None and item is not current_task and not item.done()
            }
            for item in pending:
                item.cancel()
            for item in pending:
                with contextlib.suppress(asyncio.CancelledError):
                    await item
            try:
                if self._owns_subscription_client:
                    close = getattr(self._subscription_client, "close", None)
                    if callable(close):
                        result = close()
                        if inspect.isawaitable(result):
                            await result
                    self._owns_subscription_client = False
            finally:
                async with self._condition:
                    self._condition.notify_all()

    async def _pump(self, wait_event: Any) -> None:
        recovery_delay = self._MIN_RECOVERY_DELAY_S
        while not self._closed:
            try:
                result = wait_event(timeout=None)
                if inspect.isawaitable(result):
                    result = await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                async with self._condition:
                    self._error = exc
                    self._enabled = False
                    self._condition.notify_all()
                recovered, recovery_delay = await self._recover_subscription(recovery_delay)
                if not recovered:
                    return
                continue
            if result is None:
                # A non-blocking/custom transport must not create a hot loop.
                recovery_delay = self._MIN_RECOVERY_DELAY_S
                await asyncio.sleep(self._MIN_RECOVERY_DELAY_S)
                continue
            recovery_delay = self._MIN_RECOVERY_DELAY_S
            async with self._condition:
                self._generation += 1
                self._condition.notify_all()

    async def _recover_subscription(self, delay: float) -> tuple[bool, float]:
        while not self._closed:
            await asyncio.sleep(delay)
            if self._closed:
                return False, delay
            try:
                activated = await self._activate_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                async with self._condition:
                    self._error = exc
                    self._condition.notify_all()
                delay = min(delay * 2, self._MAX_RECOVERY_DELAY_S)
                continue
            if not activated:
                return False, delay
            return True, min(delay * 2, self._MAX_RECOVERY_DELAY_S)
        return False, delay
