from __future__ import annotations

import builtins
import inspect
from collections.abc import Sequence
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.adapters import AsyncCommandExecutor
from ferricstore.async_client_claims import _AsyncClientClaimsMixin
from ferricstore.async_client_effects import _AsyncClientEffectsMixin
from ferricstore.async_client_governance import _AsyncClientGovernanceMixin
from ferricstore.async_client_management import _AsyncClientManagementMixin
from ferricstore.async_client_mutations import _AsyncClientMutationsMixin
from ferricstore.async_client_producer import _AsyncClientProducerMixin
from ferricstore.async_client_queries import _AsyncClientQueriesMixin
from ferricstore.async_client_schedules import _AsyncClientSchedulesMixin
from ferricstore.async_client_sessions import (
    AsyncCommandPipeline,
    AsyncPubSubSession,
    AsyncTransactionSession,
    _AsyncErrorMappingExecutor,
)
from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.async_client_support import _AsyncClientSupportMixin
from ferricstore.async_commands import AsyncDataCommandsMixin
from ferricstore.backpressure import (
    BackpressureController,
    BackpressurePolicy,
    backpressure_scope_for,
)
from ferricstore.client_helpers import (
    _append,
    _ok_response,
    _parse_kv_response,
    _validate_ownership_token,
)
from ferricstore.client_markers import SyncFlowClientMarker
from ferricstore.codecs import Codec, RawCodec
from ferricstore.command_core import normalize_command_name
from ferricstore.config_validation import validate_string_sequence
from ferricstore.errors import InvalidCommandError
from ferricstore.types import (
    FetchOrComputeResult,
    KeyInfo,
    RateLimitResult,
)


class _AsyncClientCoreMixin(_AsyncClientMixinBase):
    def __init__(
        self,
        executor: AsyncCommandExecutor,
        *,
        codec: Codec | None = None,
        backpressure: BackpressurePolicy | None = None,
    ) -> None:
        if isinstance(executor, SyncFlowClientMarker):
            raise TypeError(
                "AsyncFlowClient requires an async executor or URL; "
                "use AsyncFlowClient.from_url(...) instead of passing FlowClient"
            )
        self.executor = _AsyncErrorMappingExecutor(executor)
        self.codec = codec if codec is not None else RawCodec()
        self.backpressure = BackpressureController(
            backpressure,
            scope=backpressure_scope_for(executor),
        )
        self._transaction_mode = False

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        codec: Codec | None = None,
        backpressure: BackpressurePolicy | None = None,
        **kwargs: Any,
    ) -> AsyncFlowClient:
        if urlparse(url).scheme.lower() not in {"ferric", "ferrics"}:
            raise ValueError("FerricStore SDK URLs must use ferric:// or ferrics://")

        from ferricstore.protocol import AsyncProtocolAdapterPool

        return cast(
            "AsyncFlowClient",
            cls(
                AsyncProtocolAdapterPool.from_url(url, **kwargs),
                codec=codec,
                backpressure=backpressure,
            ),
        )

    @classmethod
    def from_urls(
        cls,
        urls: Sequence[str],
        *,
        codec: Codec | None = None,
        backpressure: BackpressurePolicy | None = None,
        **kwargs: Any,
    ) -> AsyncFlowClient:
        resolved_urls = validate_string_sequence(urls, name="urls", allow_empty=False)
        for url in resolved_urls:
            if urlparse(url).scheme.lower() not in {"ferric", "ferrics"}:
                raise ValueError("FerricStore SDK URLs must use ferric:// or ferrics://")

        from ferricstore.protocol import AsyncProtocolAdapterPool

        return cast(
            "AsyncFlowClient",
            cls(
                AsyncProtocolAdapterPool.from_urls(list(resolved_urls), **kwargs),
                codec=codec,
                backpressure=backpressure,
            ),
        )

    async def close(self) -> None:
        if self._transaction_mode:
            self._transaction_mode = False
            await self._set_heartbeat_paused(False)
        close = getattr(self.executor, "close", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result

    async def _invalidate_connection(self) -> None:
        raw_executor = getattr(self.executor, "_executor", self.executor)
        invalidate = getattr(raw_executor, "invalidate", None)
        if not callable(invalidate):
            return
        result = invalidate()
        if inspect.isawaitable(result):
            await result

    async def command(self, *args: Any) -> Any:
        if not args:
            return await self.executor.execute_command(*args)

        name = normalize_command_name(args[0])
        tx_control = name in {"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"}
        raw_executor = getattr(self.executor, "_executor", self.executor)
        if tx_control and getattr(raw_executor, "requires_explicit_session", False):
            raise InvalidCommandError(
                "connection-affine transaction commands require client.transaction()"
            )
        starting_transaction = name == "MULTI" and not self._transaction_mode
        ending_transaction = name in {"EXEC", "DISCARD"} and self._transaction_mode

        if starting_transaction:
            await self._set_heartbeat_paused(True)

        try:
            if self._transaction_mode and not tx_control:
                result = await self.executor.execute_command("COMMAND_EXEC", args[0], *args[1:])
            else:
                result = await self.executor.execute_command(*args)
        except BaseException:
            if starting_transaction:
                await self._set_heartbeat_paused(False)
            raise
        finally:
            if name in {"EXEC", "DISCARD"}:
                self._transaction_mode = False
                if ending_transaction:
                    await self._set_heartbeat_paused(False)

        if name == "MULTI":
            self._transaction_mode = True

        return result

    async def _set_heartbeat_paused(self, paused: bool) -> None:
        method = getattr(
            self.executor,
            "pause_heartbeat" if paused else "resume_heartbeat",
            None,
        )
        if not callable(method):
            return
        result = method()
        if inspect.isawaitable(result):
            await result

    async def refresh_topology(self) -> Any:
        refresh = getattr(self.executor, "refresh_topology", None)
        if not callable(refresh):
            raise RuntimeError("topology refresh requires a topology-aware protocol executor")
        result = refresh()
        if inspect.isawaitable(result):
            return await result
        return result

    async def route(self, key: str | bytes) -> Any:
        route = getattr(self.executor, "route", None)
        if not callable(route):
            raise RuntimeError("route lookup requires a topology-aware protocol executor")
        result = route(key)
        if inspect.isawaitable(result):
            return await result
        return result

    def pipeline(self) -> AsyncCommandPipeline:
        return AsyncCommandPipeline(cast("AsyncFlowClient", self))

    def transaction(
        self,
        key: str | bytes | None = None,
        *,
        watch: Sequence[str] | None = None,
    ) -> AsyncTransactionSession:
        return AsyncTransactionSession(cast("AsyncFlowClient", self), key=key, watch=watch)

    def pubsub_session(self) -> AsyncPubSubSession:
        return AsyncPubSubSession(cast("AsyncFlowClient", self))

    async def _acquire_session_client(
        self,
        keys: Sequence[str | bytes] | None = None,
    ) -> tuple[AsyncFlowClient, bool]:
        raw_executor = getattr(self.executor, "_executor", self.executor)
        session_keys = tuple(keys or ())
        acquire_session = getattr(raw_executor, "acquire_session_for_keys", None)
        if callable(acquire_session) and session_keys:
            session_executor = acquire_session(session_keys)
            if inspect.isawaitable(session_executor):
                session_executor = await session_executor
            session_client = cast(
                "AsyncFlowClient",
                type(self)(session_executor, codec=self.codec),
            )
            session_client.backpressure = self.backpressure
            return session_client, True
        acquire_session_for_key = (
            getattr(raw_executor, "acquire_session_for_key", None) if session_keys else None
        )
        if callable(acquire_session_for_key):
            session_executor = acquire_session_for_key(session_keys[0])
        else:
            acquire_session = getattr(raw_executor, "acquire_session", None)
            if not callable(acquire_session):
                return cast("AsyncFlowClient", self), False
            session_executor = acquire_session()
        if inspect.isawaitable(session_executor):
            session_executor = await session_executor
        session_client = cast(
            "AsyncFlowClient",
            type(self)(session_executor, codec=self.codec),
        )
        session_client.backpressure = self.backpressure
        return session_client, True

    async def _acquire_subscription_client(self) -> tuple[AsyncFlowClient, bool]:
        """Acquire a connection owned for the lifetime of one event subscription."""
        raw_executor = getattr(self.executor, "_executor", self.executor)
        acquire_session = getattr(raw_executor, "acquire_dedicated_session", None)
        if not callable(acquire_session):
            return await self._acquire_session_client()
        session_executor = acquire_session()
        if inspect.isawaitable(session_executor):
            session_executor = await session_executor
        session_client = cast(
            "AsyncFlowClient",
            type(self)(session_executor, codec=self.codec),
        )
        session_client.backpressure = self.backpressure
        return session_client, True

    async def subscribe_flow_wake(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> Any:
        subscribe = getattr(self.executor, "subscribe_flow_wake", None)
        if not callable(subscribe):
            raise RuntimeError("FLOW_WAKE subscriptions require protocol executor event support")
        result = subscribe(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        if inspect.isawaitable(result):
            return await result
        return result

    async def wait_event(self, timeout: float | None = None) -> Any | None:
        wait_event = getattr(self.executor, "wait_event", None)
        if not callable(wait_event):
            return None
        result = wait_event(timeout=timeout)
        if inspect.isawaitable(result):
            return await result
        return result

    async def cas(self, key: str, expected: Any, value: Any, *, ex: int | None = None) -> bool:
        args: builtins.list[Any] = [
            "CAS",
            key,
            self.codec.encode(expected),
            self.codec.encode(value),
        ]
        _append(args, "EX", ex)
        return bool(await self.executor.execute_command(*args))

    async def lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        return _ok_response(await self.executor.execute_command("LOCK", key, owner, ttl_ms))

    async def unlock(self, key: str, owner: str) -> int:
        return int(await self.executor.execute_command("UNLOCK", key, owner))

    async def extend_lock(self, key: str, owner: str, ttl_ms: int) -> int:
        return int(await self.executor.execute_command("EXTEND", key, owner, ttl_ms))

    async def ratelimit_add(
        self,
        key: str,
        *,
        window_ms: int,
        max: int,
        count: int = 1,
    ) -> RateLimitResult:
        return RateLimitResult.from_resp(
            await self.executor.execute_command("RATELIMIT.ADD", key, window_ms, max, count)
        )

    async def key_info(self, key: str) -> KeyInfo:
        return KeyInfo.from_resp(await self.executor.execute_command("FERRICSTORE.KEY_INFO", key))

    async def fetch_or_compute(
        self,
        key: str,
        *,
        ttl_ms: int,
        hint: str | None = None,
    ) -> FetchOrComputeResult:
        args: builtins.list[Any] = ["FETCH_OR_COMPUTE", key, ttl_ms]
        if hint is not None:
            args.append(hint)
        response = await self.executor.execute_command(*args)
        return FetchOrComputeResult.from_resp(response, decode=self.codec.decode)

    async def fetch_or_compute_result(
        self,
        key: str,
        ownership_token: bytes,
        value: Any,
        *,
        ttl_ms: int,
    ) -> bool:
        _validate_ownership_token(ownership_token)
        response = await self.executor.execute_command(
            "FETCH_OR_COMPUTE_RESULT",
            key,
            ownership_token,
            self.codec.encode(value),
            ttl_ms,
        )
        return _ok_response(response)

    async def fetch_or_compute_error(
        self,
        key: str,
        ownership_token: bytes,
        message: str,
    ) -> bool:
        _validate_ownership_token(ownership_token)
        return _ok_response(
            await self.executor.execute_command(
                "FETCH_OR_COMPUTE_ERROR",
                key,
                ownership_token,
                message,
            )
        )

    async def cluster_health(self) -> Any:
        return _parse_kv_response(await self.executor.execute_command("CLUSTER.HEALTH"))

    async def cluster_stats(self) -> Any:
        return _parse_kv_response(await self.executor.execute_command("CLUSTER.STATS"))

    async def cluster_keyslot(self, key: str) -> int:
        return int(await self.executor.execute_command("CLUSTER.KEYSLOT", key))

    async def cluster_slots(self) -> Any:
        return await self.executor.execute_command("CLUSTER.SLOTS")

    async def cluster_status(self) -> Any:
        return _parse_kv_response(await self.executor.execute_command("CLUSTER.STATUS"))

    async def cluster_role(self) -> Any:
        return await self.executor.execute_command("CLUSTER.ROLE")

    async def cluster_join(self, node: str, *, replace: bool = False) -> bool:
        args: builtins.list[Any] = ["CLUSTER.JOIN", node]
        if replace:
            args.append("REPLACE")
        return _ok_response(await self.executor.execute_command(*args))

    async def cluster_leave(self) -> bool:
        return _ok_response(await self.executor.execute_command("CLUSTER.LEAVE"))

    async def cluster_failover(self, shard_index: int, target_node: str) -> bool:
        return _ok_response(
            await self.executor.execute_command("CLUSTER.FAILOVER", shard_index, target_node)
        )

    async def cluster_promote(self, node: str) -> bool:
        return _ok_response(await self.executor.execute_command("CLUSTER.PROMOTE", node))

    async def cluster_demote(self, node: str) -> bool:
        return _ok_response(await self.executor.execute_command("CLUSTER.DEMOTE", node))

    async def ferricstore_config(self, *args: Any) -> Any:
        return await self.executor.execute_command("FERRICSTORE.CONFIG", *args)

    async def ferricstore_hotness(self, *args: Any) -> Any:
        return _parse_kv_response(await self.executor.execute_command("FERRICSTORE.HOTNESS", *args))

    async def ferricstore_metrics(self, *args: Any) -> Any:
        return _parse_kv_response(await self.executor.execute_command("FERRICSTORE.METRICS", *args))

    async def ferricstore_blobgc(self, *args: Any) -> Any:
        return await self.executor.execute_command("FERRICSTORE.BLOBGC", *args)


class AsyncFlowClient(
    _AsyncClientCoreMixin,
    _AsyncClientManagementMixin,
    _AsyncClientProducerMixin,
    _AsyncClientClaimsMixin,
    _AsyncClientMutationsMixin,
    _AsyncClientQueriesMixin,
    _AsyncClientSchedulesMixin,
    _AsyncClientEffectsMixin,
    _AsyncClientGovernanceMixin,
    _AsyncClientSupportMixin,
    AsyncDataCommandsMixin,
):
    """True async FerricFlow client over the FerricStore native protocol."""
