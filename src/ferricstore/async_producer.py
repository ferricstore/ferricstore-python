from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import Future as ConcurrentFuture
from typing import Any, TypeVar

from ferricstore.lifecycle_core import AsyncCloseCoordinator

_Result = TypeVar("_Result")


class AsyncProducerLoop:
    """Own one background event loop and one reusable async producer client."""

    def __init__(
        self,
        url: str,
        *,
        client_kwargs: Mapping[str, Any],
        client_factory: Callable[..., Any],
    ) -> None:
        self._url = url
        self._client_kwargs = dict(client_kwargs)
        self._client_factory = client_factory
        self._lock = threading.RLock()
        self._startup: ConcurrentFuture[tuple[asyncio.AbstractEventLoop, Any]] | None = None
        self._thread: threading.Thread | None = None
        self._active: set[ConcurrentFuture[Any]] = set()
        self._closed = False
        self._close_coordinator = AsyncCloseCoordinator()

    async def run(self, send: Callable[[Any], Awaitable[_Result]]) -> _Result:
        startup = self._start()
        # Startup is shared ownership state.  Cancelling one caller must not
        # cancel the concurrent Future that the creator thread still owns.
        loop, client = await asyncio.shield(asyncio.wrap_future(startup))

        async def run_send() -> _Result:
            return await send(client)

        with self._lock:
            if self._closed:
                raise RuntimeError("producer loop is closed")
            operation = asyncio.run_coroutine_threadsafe(run_send(), loop)
            self._active.add(operation)
            operation.add_done_callback(self._operation_finished)
        return await asyncio.wrap_future(operation)

    async def close(self) -> None:
        await self._close_coordinator.run(self._close_once)

    def _start(self) -> ConcurrentFuture[tuple[asyncio.AbstractEventLoop, Any]]:
        with self._lock:
            if self._closed:
                raise RuntimeError("producer loop is closed")
            startup = self._startup
            if startup is not None:
                return startup
            startup = ConcurrentFuture()
            thread = threading.Thread(
                target=self._thread_main,
                args=(startup,),
                name="ferricstore-async-producer",
                daemon=True,
            )
            self._startup = startup
            self._thread = thread
            try:
                thread.start()
            except BaseException:
                self._startup = None
                self._thread = None
                raise
            return startup

    def _thread_main(
        self,
        startup: ConcurrentFuture[tuple[asyncio.AbstractEventLoop, Any]],
    ) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            client = self._client_factory(self._url, **self._client_kwargs)
        except BaseException as exc:
            startup.set_exception(exc)
            loop.close()
            return
        startup.set_result((loop, client))
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    def _operation_finished(self, operation: ConcurrentFuture[Any]) -> None:
        with self._lock:
            self._active.discard(operation)

    async def _close_once(self) -> None:
        with self._lock:
            self._closed = True
            startup = self._startup
            thread = self._thread
            active = tuple(self._active)
        if startup is None:
            return

        try:
            loop, client = await asyncio.wrap_future(startup)
        except BaseException:
            if thread is not None:
                await asyncio.to_thread(thread.join)
            raise

        if active:
            await asyncio.gather(
                *(asyncio.wrap_future(operation) for operation in active),
                return_exceptions=True,
            )

        async def close_client() -> None:
            result = client.close()
            if inspect.isawaitable(result):
                await result

        close_awaitable = close_client()
        try:
            close_future = asyncio.run_coroutine_threadsafe(close_awaitable, loop)
        except BaseException:
            close_awaitable.close()
            raise
        await asyncio.wrap_future(close_future)

        # A failed client close retains the live loop so AsyncCloseCoordinator can
        # make a real retry. Retire the thread only after ownership was released.
        loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            await asyncio.to_thread(thread.join)


__all__ = ["AsyncProducerLoop"]
