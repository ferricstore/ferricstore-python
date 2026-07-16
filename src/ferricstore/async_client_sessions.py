from __future__ import annotations

import builtins
import contextlib
import inspect
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any

from ferricstore.adapters import AsyncCommandExecutor
from ferricstore.batch_core import (
    ordered_batch_executor,
    require_batch_items,
)
from ferricstore.config_validation import validate_string_sequence
from ferricstore.errors import map_exception
from ferricstore.lifecycle_core import await_cancellation_safe, raise_primary_with_cleanup
from ferricstore.types import (
    PubSubMessage,
)

if TYPE_CHECKING:
    from ferricstore.async_client_core import AsyncFlowClient


class _AsyncErrorMappingExecutor:
    def __init__(self, executor: AsyncCommandExecutor) -> None:
        self._executor = executor

    async def execute_command(self, *args: Any) -> Any:
        try:
            result = self._executor.execute_command(*args)
            if not inspect.isawaitable(result):
                raise TypeError("async executor execute_command() must return an awaitable")
            return await result
        except Exception as exc:
            mapped = map_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc

    def __getattr__(self, name: str) -> Any:
        return getattr(self._executor, name)


class AsyncCommandPipeline:
    """Async mixed-command pipeline over the configured FerricStore executor."""

    def __init__(self, client: AsyncFlowClient) -> None:
        self.client = client
        self.commands: builtins.list[tuple[Any, ...]] = []
        self.results: builtins.list[Any] | None = None

    async def __aenter__(self) -> AsyncCommandPipeline:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None and self.results is None:
            await self.execute()

    def command(self, *args: Any) -> AsyncCommandPipeline:
        self.commands.append(args)
        return self

    async def execute(self) -> builtins.list[Any]:
        raw_executor = getattr(self.client.executor, "_executor", self.client.executor)
        execute_batch = ordered_batch_executor(raw_executor)
        if callable(execute_batch):
            try:
                result = execute_batch(self.commands)
                if inspect.isawaitable(result):
                    result = await result
                self.results = require_batch_items(
                    result,
                    len(self.commands),
                    operation="executor batch",
                )
            except Exception as exc:
                mapped = map_exception(exc)
                if mapped is exc:
                    raise
                raise mapped from exc
            return self.results

        self.results = [await self.client.command(*command) for command in self.commands]
        return self.results


class AsyncPubSubSession:
    """High-level async native Pub/Sub session."""

    def __init__(self, client: AsyncFlowClient) -> None:
        self.client = client
        self._active_client: AsyncFlowClient | None = None
        self._owns_client = False

    async def _session_client(self) -> AsyncFlowClient:
        if self._active_client is None:
            self._active_client, self._owns_client = await self.client._acquire_session_client()
        return self._active_client

    async def subscribe(self, *channels: str) -> Any:
        session_client = await self._session_client()
        return await session_client.subscribe(*channels)

    async def unsubscribe(self, *channels: str) -> Any:
        session_client = await self._session_client()
        return await session_client.unsubscribe(*channels)

    async def psubscribe(self, *patterns: str) -> Any:
        session_client = await self._session_client()
        return await session_client.psubscribe(*patterns)

    async def punsubscribe(self, *patterns: str) -> Any:
        session_client = await self._session_client()
        return await session_client.punsubscribe(*patterns)

    async def get_message(
        self,
        *,
        timeout: float | None = None,
        decode: bool = True,
    ) -> PubSubMessage | None:
        session_client = await self._session_client()
        event = await session_client.wait_event(timeout=timeout)
        if event is None:
            return None
        decoder = session_client.codec.decode if decode else None
        return PubSubMessage.from_event(event, decode=decoder)

    async def listen(
        self,
        *,
        timeout: float | None = None,
        decode: bool = True,
    ) -> AsyncIterator[PubSubMessage]:
        while True:
            message = await self.get_message(timeout=timeout, decode=decode)
            if message is None:
                return
            yield message

    async def close(self) -> None:
        if self._active_client is None:
            return
        error: BaseException | None = None
        for cleanup in (self.unsubscribe, self.punsubscribe):
            try:
                await cleanup()
            except BaseException as exc:
                if error is None:
                    error = exc
        if error is not None:
            with contextlib.suppress(BaseException):
                await self._active_client._invalidate_connection()
        try:
            if self._owns_client:
                await self._active_client.close()
        except BaseException as exc:
            if error is None:
                error = exc
        finally:
            self._active_client = None
            self._owns_client = False
        if error is not None:
            raise error


class AsyncTransactionSession:
    """Async transaction context around native MULTI/EXEC/DISCARD."""

    def __init__(
        self,
        client: AsyncFlowClient,
        *,
        key: str | bytes | None = None,
        watch: Sequence[str] | None = None,
    ) -> None:
        self.client = client
        self.key = key
        self.watch_keys = (
            list(validate_string_sequence(watch, name="watch")) if watch is not None else []
        )
        self._active_client: AsyncFlowClient | None = None
        self._owns_client = False
        self.closed = False

    async def __aenter__(self) -> AsyncFlowClient:
        if self.closed:
            raise RuntimeError("transaction session cannot be reused")
        if self._active_client is not None:
            raise RuntimeError("transaction session is already active")
        session_keys = ((self.key,) if self.key is not None else ()) + tuple(self.watch_keys)
        session_client, owns_client = await self.client._acquire_session_client(session_keys)
        self._active_client = session_client
        self._owns_client = owns_client
        try:
            if self.watch_keys:
                await session_client.watch(*self.watch_keys)
            await session_client.multi()
        except BaseException as primary:
            if self.watch_keys:
                with contextlib.suppress(BaseException):
                    await session_client.unwatch()
            cleanup_error: BaseException | None = None
            try:
                await self._release(invalidate=True)
            except BaseException as cleanup:
                cleanup_error = cleanup
            raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        return session_client

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.closed:
            return
        if exc_type is None:
            await self.execute()
        else:
            try:
                await self.discard()
            except BaseException as cleanup:
                raise_primary_with_cleanup(exc, tb, cleanup)

    async def execute(self) -> Any:
        if self._active_client is None:
            raise RuntimeError("transaction session is not active")
        self.closed = True
        session_client = self._active_client
        try:
            result = await session_client.transaction_exec()
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            try:
                await session_client.discard()
            except BaseException as cleanup:
                cleanup_error = cleanup
            try:
                await self._release(invalidate=True)
            except BaseException as cleanup:
                if cleanup_error is None:
                    cleanup_error = cleanup
            raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        await self._release()
        return result

    async def discard(self) -> Any:
        if self._active_client is None:
            raise RuntimeError("transaction session is not active")
        self.closed = True
        session_client = self._active_client
        try:
            result = await session_client.discard()
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            try:
                await self._release(invalidate=True)
            except BaseException as cleanup:
                cleanup_error = cleanup
            raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        await self._release()
        return result

    async def _release(self, *, invalidate: bool = False) -> None:
        active_client = self._active_client
        owns_client = self._owns_client
        self._active_client = None
        self._owns_client = False
        if invalidate and active_client is not None:
            with contextlib.suppress(BaseException):
                await await_cancellation_safe(active_client._invalidate_connection())
        if active_client is not None and owns_client:
            await await_cancellation_safe(active_client.close())
