from __future__ import annotations

import builtins
import contextlib
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from ferricstore.adapters import CommandExecutor
from ferricstore.batch_core import (
    require_batch_items,
)
from ferricstore.errors import map_exception
from ferricstore.lifecycle_core import (
    raise_primary_with_cleanup,
)
from ferricstore.types import (
    PubSubMessage,
)

if TYPE_CHECKING:
    from ferricstore.client_core import FlowClient


class _ErrorMappingExecutor:
    def __init__(self, executor: CommandExecutor) -> None:
        self._executor = executor

    def execute_command(self, *args: Any) -> Any:
        try:
            return self._executor.execute_command(*args)
        except Exception as exc:
            mapped = map_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc

    def __getattr__(self, name: str) -> Any:
        return getattr(self._executor, name)


class CommandPipeline:
    """Small mixed-command pipeline over the configured FerricStore executor."""

    def __init__(self, client: FlowClient) -> None:
        self.client = client
        self.commands: builtins.list[tuple[Any, ...]] = []
        self.results: builtins.list[Any] | None = None

    def __enter__(self) -> CommandPipeline:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None and self.results is None:
            self.execute()

    def command(self, *args: Any) -> CommandPipeline:
        self.commands.append(args)
        return self

    def execute(self) -> builtins.list[Any]:
        raw_executor = getattr(self.client.executor, "_executor", self.client.executor)
        execute_batch = getattr(raw_executor, "execute_batch", None)
        if callable(execute_batch):
            try:
                self.results = require_batch_items(
                    execute_batch(self.commands),
                    len(self.commands),
                    operation="executor batch",
                )
            except Exception as exc:
                mapped = map_exception(exc)
                if mapped is exc:
                    raise
                raise mapped from exc
            return self.results

        self.results = [self.client.command(*command) for command in self.commands]
        return self.results


class PubSubSession:
    """High-level native Pub/Sub session.

    This is the user-facing API for native pushed Pub/Sub messages. It keeps
    ``command(...)`` available as an escape hatch, but normal code should use
    ``client.pubsub_session()`` and ``get_message``/``listen``.
    """

    def __init__(self, client: FlowClient) -> None:
        self.client = client
        self._active_client: FlowClient | None = None
        self._owns_client = False

    def _session_client(self) -> FlowClient:
        if self._active_client is None:
            self._active_client, self._owns_client = self.client._acquire_session_client()
        return self._active_client

    def subscribe(self, *channels: str) -> Any:
        return self._session_client().subscribe(*channels)

    def unsubscribe(self, *channels: str) -> Any:
        return self._session_client().unsubscribe(*channels)

    def psubscribe(self, *patterns: str) -> Any:
        return self._session_client().psubscribe(*patterns)

    def punsubscribe(self, *patterns: str) -> Any:
        return self._session_client().punsubscribe(*patterns)

    def get_message(
        self,
        *,
        timeout: float | None = None,
        decode: bool = True,
    ) -> PubSubMessage | None:
        session_client = self._session_client()
        event = session_client.wait_event(timeout=timeout)
        if event is None:
            return None
        decoder = session_client.codec.decode if decode else None
        return PubSubMessage.from_event(event, decode=decoder)

    def listen(
        self,
        *,
        timeout: float | None = None,
        decode: bool = True,
    ) -> Iterator[PubSubMessage]:
        while True:
            message = self.get_message(timeout=timeout, decode=decode)
            if message is None:
                return
            yield message

    def close(self) -> None:
        if self._active_client is None:
            return
        error: BaseException | None = None
        for cleanup in (self.unsubscribe, self.punsubscribe):
            try:
                cleanup()
            except BaseException as exc:
                if error is None:
                    error = exc
        if error is not None:
            with contextlib.suppress(BaseException):
                self._active_client._invalidate_connection()
        try:
            if self._owns_client:
                self._active_client.close()
        except BaseException as exc:
            if error is None:
                error = exc
        finally:
            self._active_client = None
            self._owns_client = False
        if error is not None:
            raise error


class TransactionSession:
    """Small transaction context around native MULTI/EXEC/DISCARD."""

    def __init__(
        self,
        client: FlowClient,
        *,
        key: str | bytes | None = None,
        watch: Sequence[str] | None = None,
    ) -> None:
        self.client = client
        self.key = key
        self.watch_keys = list(watch or ())
        self._active_client: FlowClient | None = None
        self._owns_client = False
        self.closed = False

    def __enter__(self) -> FlowClient:
        session_keys = ((self.key,) if self.key is not None else ()) + tuple(self.watch_keys)
        session_client, owns_client = self.client._acquire_session_client(session_keys)
        self._active_client = session_client
        self._owns_client = owns_client
        try:
            if self.watch_keys:
                session_client.watch(*self.watch_keys)
            session_client.multi()
        except BaseException as primary:
            if self.watch_keys:
                with contextlib.suppress(BaseException):
                    session_client.unwatch()
            cleanup_error: BaseException | None = None
            try:
                self._release(invalidate=True)
            except BaseException as cleanup:
                cleanup_error = cleanup
            raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        return session_client

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.closed:
            return
        if exc_type is None:
            self.execute()
        else:
            try:
                self.discard()
            except BaseException as cleanup:
                raise_primary_with_cleanup(exc, tb, cleanup)

    def execute(self) -> Any:
        self.closed = True
        session_client = self._active_client or self.client
        try:
            result = session_client.transaction_exec()
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            try:
                session_client.discard()
            except BaseException as cleanup:
                cleanup_error = cleanup
            try:
                self._release(invalidate=True)
            except BaseException as cleanup:
                if cleanup_error is None:
                    cleanup_error = cleanup
            raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        self._release()
        return result

    def discard(self) -> Any:
        self.closed = True
        session_client = self._active_client or self.client
        try:
            result = session_client.discard()
        except BaseException as primary:
            cleanup_error: BaseException | None = None
            try:
                self._release(invalidate=True)
            except BaseException as cleanup:
                cleanup_error = cleanup
            raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        self._release()
        return result

    def _release(self, *, invalidate: bool = False) -> None:
        active_client = self._active_client
        owns_client = self._owns_client
        self._active_client = None
        self._owns_client = False
        if invalidate and active_client is not None:
            with contextlib.suppress(BaseException):
                active_client._invalidate_connection()
        if active_client is not None and owns_client:
            active_client.close()
