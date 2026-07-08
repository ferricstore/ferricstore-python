from __future__ import annotations

import builtins
import inspect
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.adapters import AsyncCommandExecutor
from ferricstore.backpressure import BackpressureController, BackpressurePolicy
from ferricstore.client import (
    FlowClient,
    _append,
    _append_attributes,
    _append_bool,
    _append_encoded,
    _append_extra_options,
    _append_named_counts,
    _append_named_values,
    _append_payload_read,
    _append_read_options,
    _append_search_state_meta,
    _append_state_meta,
    _append_value_return,
    _auto_partition_key_for_id,
    _command_with_request_context,
    _expand_many_response,
    _flow_return,
    _has_named_item_values,
    _invocation_create_args,
    _invocation_definition_put_args,
    _management_pair_args,
    _management_rule_args,
    _merge_named_map,
    _normalize_admin_response,
    _now_ms,
    _ok_response,
    _parse_kv_response,
    _resolve_include_record,
    _shared_create_many_attributes,
    _shared_create_many_state_meta,
    _split_flow_state_policy,
)
from ferricstore.codecs import Codec, RawCodec
from ferricstore.commands import AsyncDataCommandsMixin
from ferricstore.errors import OverloadedError, map_exception
from ferricstore.types import (
    ApprovalResult,
    BudgetResult,
    ChildSpec,
    CircuitBreakerStatus,
    ClaimedFlow,
    CreateItem,
    EffectResult,
    FencedItem,
    FetchOrComputeResult,
    FlowRecord,
    FlowStatePolicyLike,
    GovernanceOverview,
    KeyInfo,
    PubSubMessage,
    RateLimitResult,
    RetryPolicy,
    ScheduleResult,
    _normalize_ref_meta,
)


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
        execute_batch = getattr(raw_executor, "execute_batch", None)
        if callable(execute_batch):
            try:
                result = execute_batch(self.commands)
                if inspect.isawaitable(result):
                    result = await result
                self.results = list(result)
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

    async def subscribe(self, *channels: str) -> Any:
        return await self.client.subscribe(*channels)

    async def unsubscribe(self, *channels: str) -> Any:
        return await self.client.unsubscribe(*channels)

    async def psubscribe(self, *patterns: str) -> Any:
        return await self.client.psubscribe(*patterns)

    async def punsubscribe(self, *patterns: str) -> Any:
        return await self.client.punsubscribe(*patterns)

    async def get_message(
        self,
        *,
        timeout: float | None = None,
        decode: bool = True,
    ) -> PubSubMessage | None:
        event = await self.client.wait_event(timeout=timeout)
        if event is None:
            return None
        decoder = self.client.codec.decode if decode else None
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
        await self.unsubscribe()
        await self.punsubscribe()


class AsyncTransactionSession:
    """Async transaction context around native MULTI/EXEC/DISCARD."""

    def __init__(self, client: AsyncFlowClient) -> None:
        self.client = client
        self.closed = False

    async def __aenter__(self) -> AsyncFlowClient:
        await self.client.multi()
        return self.client

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.closed:
            return
        if exc_type is None:
            await self.execute()
        else:
            await self.discard()

    async def execute(self) -> Any:
        self.closed = True
        return await self.client.transaction_exec()

    async def discard(self) -> Any:
        self.closed = True
        return await self.client.discard()


class AsyncFlowClient(AsyncDataCommandsMixin):
    """True async FerricFlow client over the FerricStore native protocol."""

    def __init__(
        self,
        executor: AsyncCommandExecutor,
        *,
        codec: Codec | None = None,
        backpressure: BackpressurePolicy | None = None,
    ) -> None:
        if isinstance(executor, FlowClient):
            raise TypeError(
                "AsyncFlowClient requires an async executor or URL; "
                "use AsyncFlowClient.from_url(...) instead of passing FlowClient"
            )
        self.executor = _AsyncErrorMappingExecutor(executor)
        self.codec = codec or RawCodec()
        self.backpressure = BackpressureController(backpressure)
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

        return cls(
            AsyncProtocolAdapterPool.from_url(url, **kwargs),
            codec=codec,
            backpressure=backpressure,
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
        if not urls:
            raise ValueError("FerricStore SDK requires at least one URL")
        for url in urls:
            if urlparse(url).scheme.lower() not in {"ferric", "ferrics"}:
                raise ValueError("FerricStore SDK URLs must use ferric:// or ferrics://")

        from ferricstore.protocol import AsyncProtocolAdapterPool

        return cls(
            AsyncProtocolAdapterPool.from_urls(list(urls), **kwargs),
            codec=codec,
            backpressure=backpressure,
        )

    async def close(self) -> None:
        close = getattr(self.executor, "close", None)
        if callable(close):
            result = close()
            if inspect.isawaitable(result):
                await result

    async def command(self, *args: Any) -> Any:
        if not args:
            return await self.executor.execute_command(*args)

        name = str(args[0]).upper()
        tx_control = name in {"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"}

        try:
            if self._transaction_mode and not tx_control:
                result = await self.executor.execute_command("COMMAND_EXEC", args[0], *args[1:])
            else:
                result = await self.executor.execute_command(*args)
        finally:
            if name in {"EXEC", "DISCARD"}:
                self._transaction_mode = False

        if name == "MULTI":
            self._transaction_mode = True

        return result

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
        return AsyncCommandPipeline(self)

    def transaction(self) -> AsyncTransactionSession:
        return AsyncTransactionSession(self)

    def pubsub_session(self) -> AsyncPubSubSession:
        return AsyncPubSubSession(self)

    async def wait_event(self, timeout: float | None = None) -> Any | None:
        wait_event = getattr(self.executor, "wait_event", None)
        if not callable(wait_event):
            return None
        result = wait_event(timeout=timeout)
        if inspect.isawaitable(result):
            return await result
        return result

    async def subscribe(self, *channels: str) -> Any:
        return await self.command("SUBSCRIBE", *channels)

    async def unsubscribe(self, *channels: str) -> Any:
        return await self.command("UNSUBSCRIBE", *channels)

    async def psubscribe(self, *patterns: str) -> Any:
        return await self.command("PSUBSCRIBE", *patterns)

    async def punsubscribe(self, *patterns: str) -> Any:
        return await self.command("PUNSUBSCRIBE", *patterns)

    async def multi(self) -> Any:
        return await self.command("MULTI")

    async def transaction_exec(self) -> Any:
        return await self.command("EXEC")

    async def discard(self) -> Any:
        return await self.command("DISCARD")

    async def watch(self, *keys: str) -> Any:
        return await self.command("WATCH", *keys)

    async def unwatch(self) -> Any:
        return await self.command("UNWATCH")

    async def blpop(self, *keys: str, timeout: float | int = 0) -> Any:
        return await self.command("BLPOP", *keys, timeout)

    async def brpop(self, *keys: str, timeout: float | int = 0) -> Any:
        return await self.command("BRPOP", *keys, timeout)

    async def blmove(
        self,
        source: str,
        destination: str,
        wherefrom: str,
        whereto: str,
        timeout: float | int = 0,
    ) -> Any:
        return await self.command("BLMOVE", source, destination, wherefrom, whereto, timeout)

    async def blmpop(
        self,
        timeout: float | int,
        keys: builtins.list[str],
        direction: str,
        *,
        count: int | None = None,
    ) -> Any:
        args: builtins.list[Any] = ["BLMPOP", timeout, len(keys), *keys, direction]
        if count is not None:
            args.extend(["COUNT", count])
        return await self.command(*args)

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
        status = response[0].decode() if isinstance(response[0], bytes) else str(response[0])

        if status == "hit":
            return FetchOrComputeResult(status="hit", value=self.codec.decode(response[1]))
        return FetchOrComputeResult(status="compute", compute_token=response[1])

    async def fetch_or_compute_result(self, key: str, value: Any, *, ttl_ms: int) -> bool:
        response = await self.executor.execute_command(
            "FETCH_OR_COMPUTE_RESULT",
            key,
            self.codec.encode(value),
            ttl_ms,
        )
        return _ok_response(response)

    async def fetch_or_compute_error(self, key: str, message: str) -> bool:
        return _ok_response(
            await self.executor.execute_command("FETCH_OR_COMPUTE_ERROR", key, message)
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

    async def capabilities(self) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(
                await self.executor.execute_command("FERRICSTORE.CAPABILITIES")
            ),
        )

    async def acl_set_user(self, username: str, rules: Sequence[Any] | Any) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                "ACL",
                "SETUSER",
                username,
                *_management_rule_args(rules),
            )
        )

    async def acl_del_user(self, username: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("ACL", "DELUSER", username)
        )

    async def acl_get_user(self, username: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("ACL", "GETUSER", username)
        )

    async def acl_list_users(self) -> Any:
        return _normalize_admin_response(await self.executor.execute_command("ACL", "LIST"))

    async def acl_save(self) -> Any:
        return _normalize_admin_response(await self.executor.execute_command("ACL", "SAVE"))

    async def ensure_namespace(
        self,
        prefix: str,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                "FERRICSTORE.NAMESPACE",
                "ENSURE",
                prefix,
                *_management_pair_args(attrs, kwargs),
            )
        )

    async def get_namespace(self, prefix: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.NAMESPACE", "GET", prefix)
        )

    async def list_namespaces(self) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.NAMESPACE", "LIST")
        )

    async def delete_namespace(self, prefix: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.NAMESPACE", "DELETE", prefix)
        )

    async def set_quota(
        self,
        namespace: str,
        quota_spec: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                "FERRICSTORE.QUOTA",
                "SET",
                namespace,
                *_management_pair_args(quota_spec, kwargs),
            )
        )

    async def get_quota(self, namespace: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.QUOTA", "GET", namespace)
        )

    async def quota_usage(self, namespace: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.QUOTA", "USAGE", namespace)
        )

    async def cluster_info(self) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(
                await self.executor.execute_command("FERRICSTORE.TELEMETRY", "CLUSTER_INFO")
            ),
        )

    async def namespace_usage(self, prefix: str) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(
                await self.executor.execute_command(
                    "FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", prefix
                )
            ),
        )

    async def flow_query(
        self,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> builtins.list[Any]:
        return cast(
            builtins.list[Any],
            _normalize_admin_response(
                await self.executor.execute_command(
                    "FERRICSTORE.TELEMETRY",
                    "FLOW_QUERY",
                    *_management_pair_args(attrs, kwargs),
                )
            ),
        )

    async def flow_history(
        self,
        id: str,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> builtins.list[Any]:
        return cast(
            builtins.list[Any],
            _normalize_admin_response(
                await self.executor.execute_command(
                    "FERRICSTORE.TELEMETRY",
                    "FLOW_HISTORY",
                    id,
                    *_management_pair_args(attrs, kwargs),
                )
            ),
        )

    async def invocation_definition_put(
        self,
        definition: Mapping[str, Any] | str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.PUT",
                    _invocation_definition_put_args(definition),
                    request_context,
                )
            )
        )

    async def invocation_definition_get(
        self,
        name: str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.GET",
                    [name],
                    request_context,
                )
            )
        )

    async def invocation_definition_list(
        self,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.LIST",
                    [],
                    request_context,
                )
            )
        )

    async def invocation_create(
        self,
        name: str,
        attrs: Mapping[str, Any],
        *,
        context: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.CREATE",
                    _invocation_create_args(
                        name,
                        attrs,
                        context=context,
                        idempotency_key=idempotency_key,
                    ),
                    request_context,
                )
            )
        )

    async def invocation_get(
        self,
        id: str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context("INVOCATION.GET", [id], request_context)
            )
        )

    async def invocation_partition_list(
        self,
        name: str,
        *,
        scope: str | None = None,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        args: builtins.list[Any] = [name]
        _append(args, "SCOPE", scope)
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context("INVOCATION.PARTITION.LIST", args, request_context)
            )
        )

    async def create(
        self,
        id: str,
        *,
        type: str,
        state: str = "queued",
        payload: Any = None,
        partition_key: str | None = None,
        parent_flow_id: str | None = None,
        root_flow_id: str | None = None,
        correlation_id: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        idempotent: bool | None = None,
        retention_ttl_ms: int | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        now_ms = now_ms if now_ms is not None else _now_ms()
        args: builtins.list[Any] = ["FLOW.CREATE", id, "TYPE", type, "STATE", state, "NOW", now_ms]
        _append(args, "PARTITION", partition_key)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "PARENT_FLOW_ID", parent_flow_id)
        _append(args, "ROOT_FLOW_ID", root_flow_id)
        _append(args, "CORRELATION_ID", correlation_id)
        _append(args, "RUN_AT", run_at_ms if run_at_ms is not None else now_ms)
        _append(args, "PRIORITY", priority)
        _append_bool(args, "IDEMPOTENT", idempotent)
        _append(args, "RETENTION_TTL_MS", retention_ttl_ms)
        _append_attributes(args, attributes=attributes)
        _append_state_meta(args, state_meta)
        _append_named_values(args, self.codec, values=values, value_refs=value_refs)
        response = await self._execute_producer_write(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def enqueue(
        self,
        id: str,
        *,
        type: str,
        payload: Any = None,
        state: str = "queued",
        partition_key: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = 0,
        idempotent: bool | None = None,
        retention_ttl_ms: int | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        return await self.create(
            id,
            type=type,
            state=state,
            payload=payload,
            partition_key=partition_key,
            run_at_ms=run_at_ms,
            now_ms=now_ms,
            priority=priority,
            idempotent=idempotent,
            retention_ttl_ms=retention_ttl_ms,
            attributes=attributes,
            state_meta=state_meta,
            values=values,
            value_refs=value_refs,
            return_record=return_record,
        )

    async def start_and_claim(
        self,
        id: str,
        *,
        type: str,
        initial_state: str,
        worker: str,
        lease_ms: int = 30_000,
        payload: Any = None,
        partition_key: str | None = None,
        parent_flow_id: str | None = None,
        root_flow_id: str | None = None,
        correlation_id: str | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        retention_ttl_ms: int | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
    ) -> FlowRecord:
        now_ms = now_ms if now_ms is not None else _now_ms()
        args: builtins.list[Any] = [
            "FLOW.START_AND_CLAIM",
            id,
            "TYPE",
            type,
            "INITIAL_STATE",
            initial_state,
            "WORKER",
            worker,
            "LEASE_MS",
            lease_ms,
            "NOW",
            now_ms,
        ]
        _append(args, "PARTITION", partition_key)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "PARENT_FLOW_ID", parent_flow_id)
        _append(args, "ROOT_FLOW_ID", root_flow_id)
        _append(args, "CORRELATION_ID", correlation_id)
        _append(args, "PRIORITY", priority)
        _append(args, "RETENTION_TTL_MS", retention_ttl_ms)
        _append_attributes(args, attributes=attributes)
        _append_state_meta(args, state_meta)
        _append_named_values(args, self.codec, values=values, value_refs=value_refs)
        response = await self.executor.execute_command(*args)
        return await self._record_or_get(response, id, partition_key)

    async def enqueue_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str,
        state: str = "queued",
        partition_key: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = 0,
        idempotent: bool | None = None,
        independent: bool | None = True,
        return_ok_on_success: bool = False,
        retention_ttl_ms: int | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
    ) -> builtins.list[Any] | Any:
        if not items:
            return []

        if partition_key is not None or any(item.partition_key is not None for item in items):
            return await self.create_many(
                partition_key,
                items,
                type=type,
                state=state,
                run_at_ms=run_at_ms,
                now_ms=now_ms,
                priority=priority,
                idempotent=idempotent,
                independent=independent,
                return_ok_on_success=return_ok_on_success,
                retention_ttl_ms=retention_ttl_ms,
                values=values,
                value_refs=value_refs,
                attributes=attributes,
                state_meta=state_meta,
            )

        grouped: dict[str, builtins.list[tuple[int, CreateItem]]] = {}
        for idx, item in enumerate(items):
            grouped.setdefault(_auto_partition_key_for_id(item.id), []).append((idx, item))

        results: builtins.list[Any] = [None] * len(items)
        for bucket, indexed_items in grouped.items():
            group_items = [item for _idx, item in indexed_items]
            response = await self.create_many(
                bucket,
                group_items,
                type=type,
                state=state,
                run_at_ms=run_at_ms,
                now_ms=now_ms,
                priority=priority,
                idempotent=idempotent,
                independent=independent,
                return_ok_on_success=return_ok_on_success,
                retention_ttl_ms=retention_ttl_ms,
                values=values,
                value_refs=value_refs,
                attributes=attributes,
                state_meta=state_meta,
            )
            for (idx, _item), item_result in zip(
                indexed_items,
                _expand_many_response(response, len(indexed_items)),
                strict=False,
            ):
                results[idx] = item_result
        return results

    async def create_many(
        self,
        partition_key: str | None,
        items: builtins.list[CreateItem],
        *,
        type: str,
        state: str = "queued",
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        idempotent: bool | None = None,
        independent: bool | None = None,
        return_ok_on_success: bool = False,
        retention_ttl_ms: int | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        attributes = _shared_create_many_attributes(items, attributes)
        state_meta = _shared_create_many_state_meta(items, state_meta)
        now_ms = now_ms if now_ms is not None else _now_ms()
        if partition_key is not None:
            for item in items:
                if item.partition_key is not None and item.partition_key != partition_key:
                    raise ValueError(
                        "create_many item partition_key does not match batch partition_key"
                    )
        mixed = partition_key is None and any(item.partition_key is not None for item in items)
        auto = partition_key is None and not mixed
        wire_partition = "MIXED" if mixed else "AUTO" if auto else partition_key
        args: builtins.list[Any] = [
            "FLOW.CREATE_MANY",
            wire_partition,
            "TYPE",
            type,
            "STATE",
            state,
            "NOW",
            now_ms,
        ]
        _append(args, "RUN_AT", run_at_ms if run_at_ms is not None else now_ms)
        _append(args, "PRIORITY", priority)
        _append_bool(args, "IDEMPOTENT", idempotent)
        _append_bool(args, "INDEPENDENT", independent)
        if return_ok_on_success:
            _append(args, "RETURN", "OK_ON_SUCCESS")
        _append(args, "RETENTION_TTL_MS", retention_ttl_ms)
        _append_attributes(args, attributes=attributes)
        _append_state_meta(args, state_meta)
        extended_items = _has_named_item_values(items) or (
            mixed and any(item.partition_key is None for item in items)
        )

        if extended_items:
            args.extend(["ITEMS_EXT", len(items)])
            for item in items:
                item_partition = item.partition_key if mixed else None
                item_values = _merge_named_map(values, item.values)
                item_refs = _merge_named_map(value_refs, item.value_refs)
                args.extend([item.id, item_partition or "-", self.codec.encode(item.payload)])
                _append_named_counts(args, self.codec, item_values, item_refs)
        else:
            _append_named_values(args, self.codec, values=values, value_refs=value_refs)
            args.append("ITEMS")
            for item in items:
                if mixed:
                    if item.partition_key is None:
                        raise ValueError("mixed create_many items require partition_key")
                    args.extend([item.id, item.partition_key, self.codec.encode(item.payload)])
                else:
                    args.extend([item.id, self.codec.encode(item.payload)])
        return self._records_or_response(await self._execute_producer_write(*args))

    async def value_put(
        self,
        value: Any,
        *,
        partition_key: str | None = None,
        owner_flow_id: str | None = None,
        name: str | None = None,
        override: bool | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> Any:
        args: builtins.list[Any] = [
            "FLOW.VALUE.PUT",
            self.codec.encode(value),
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "OWNER_FLOW_ID", owner_flow_id)
        _append(args, "NAME", name)
        _append_bool(args, "OVERRIDE", override)
        _append(args, "TTL", ttl_ms)
        return await self.executor.execute_command(*args)

    async def value_mget(
        self, refs: builtins.list[str], *, max_bytes: int | None = None
    ) -> builtins.list[Any]:
        if not refs:
            return []
        args: builtins.list[Any] = ["FLOW.VALUE.MGET", *refs]
        _append(args, "MAX_BYTES", max_bytes)
        response = await self.executor.execute_command(*args)
        return [
            _normalize_ref_meta(value)
            if isinstance(value, dict)
            else value
            if value is None
            else self.codec.decode(value)
            for value in response
        ]

    async def signal(
        self,
        id: str,
        *,
        signal: str,
        partition_key: str | None = None,
        idempotency_key: str | None = None,
        if_state: str | builtins.list[str] | tuple[str, ...] | None = None,
        transition_to: str | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
    ) -> Any:
        args: builtins.list[Any] = ["FLOW.SIGNAL", id, "SIGNAL", signal]
        _append(args, "PARTITION", partition_key)
        _append(args, "IDEMPOTENCY", idempotency_key)
        if isinstance(if_state, (list, tuple)):
            for state in if_state:
                _append(args, "IF_STATE", state)
        else:
            _append(args, "IF_STATE", if_state)
        _append(args, "TRANSITION_TO", transition_to)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append(args, "PRIORITY", priority)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        return await self.executor.execute_command(*args)

    async def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)

    async def claim_due(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
        priority: int | None = None,
        now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_record: bool | None = None,
        job_only: bool | None = None,
        payload: bool | None = None,
        payload_max_bytes: int | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedFlow]:
        include_record = _resolve_include_record(include_record, job_only)
        args: builtins.list[Any] = ["FLOW.CLAIM_DUE", type]
        if state is not None and states is not None:
            raise ValueError("state and states are mutually exclusive")
        if states is not None:
            if not states:
                raise ValueError("states must be non-empty")
            for item in states:
                if not isinstance(item, str) or item == "":
                    raise ValueError("states must contain non-empty strings")
                _append(args, "STATE", item)
        else:
            _append(args, "STATE", state)
        args.extend(
            [
                "WORKER",
                worker,
                "LEASE_MS",
                lease_ms,
                "LIMIT",
                limit,
            ]
        )
        _append(args, "NOW", now_ms)
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")
        _append(args, "PARTITION", partition_key)
        if partition_keys is not None:
            if not partition_keys:
                raise ValueError("partition_keys must be non-empty")
            args.extend(["PARTITIONS", len(partition_keys), *partition_keys])
        _append(args, "PRIORITY", priority)
        if not include_record:
            if include_state and include_attributes:
                return_mode = "JOBS_COMPACT_STATE_ATTRS"
            elif include_state:
                return_mode = "JOBS_COMPACT_STATE"
            elif include_attributes:
                return_mode = "JOBS_COMPACT_ATTRS"
            else:
                return_mode = "JOBS_COMPACT"
            _append(args, "RETURN", return_mode)
        _append(args, "BLOCK", block_ms)
        _append_payload_read(args, payload, payload_max_bytes)
        _append_value_return(args, values=values, value_max_bytes=value_max_bytes)
        _append_bool(args, "RECLAIM_EXPIRED", reclaim_expired)
        _append(args, "RECLAIM_RATIO", reclaim_ratio)
        response = await self.executor.execute_command(*args)
        if include_record:
            return self._records(response)
        return ClaimedFlow.from_compact_rows(response)

    async def claim_flows(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        lease_ms: int = 30_000,
        limit: int = 100,
        priority: int | None = 0,
        now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_state: bool = False,
        include_attributes: bool = True,
    ) -> builtins.list[ClaimedFlow]:
        return cast(
            builtins.list[ClaimedFlow],
            await self.claim_due(
                type,
                state=state,
                states=states,
                worker=worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=lease_ms,
                limit=limit,
                priority=priority,
                now_ms=now_ms,
                block_ms=block_ms,
                reclaim_expired=reclaim_expired,
                reclaim_ratio=reclaim_ratio,
                include_record=False,
                include_state=include_state,
                include_attributes=include_attributes,
            ),
        )

    async def claim_jobs(self, *args: Any, **kwargs: Any) -> builtins.list[ClaimedFlow]:
        """Compatibility alias for claim_flows()."""
        return await self.claim_flows(*args, **kwargs)

    async def reclaim(
        self,
        type: str,
        *,
        state: str | None = None,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
        priority: int | None = None,
        now_ms: int | None = None,
        include_record: bool | None = None,
        job_only: bool | None = None,
        payload: bool | None = None,
        payload_max_bytes: int | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        include_attributes: bool = True,
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedFlow]:
        include_record = _resolve_include_record(include_record, job_only)
        if state not in (None, "running"):
            raise ValueError("FLOW.RECLAIM only supports running state")

        args: builtins.list[Any] = [
            "FLOW.RECLAIM",
            type,
            "WORKER",
            worker,
            "LEASE_MS",
            lease_ms,
            "LIMIT",
            limit,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")
        _append(args, "PARTITION", partition_key)
        if partition_keys is not None:
            if not partition_keys:
                raise ValueError("partition_keys must be non-empty")
            args.extend(["PARTITIONS", len(partition_keys), *partition_keys])
        _append(args, "PRIORITY", priority)
        if not include_record:
            _append(args, "RETURN", "JOBS_COMPACT_ATTRS" if include_attributes else "JOBS_COMPACT")
        _append_payload_read(args, payload, payload_max_bytes)
        _append_value_return(args, values=values, value_max_bytes=value_max_bytes)
        response = await self.executor.execute_command(*args)
        if include_record:
            return self._records(response)
        return ClaimedFlow.from_compact_rows(response)

    async def extend_lease(
        self,
        id: str,
        lease_token: bytes,
        *,
        fencing_token: int,
        lease_ms: int,
        partition_key: str | None = None,
        now_ms: int | None = None,
    ) -> FlowRecord:
        args: builtins.list[Any] = [
            "FLOW.EXTEND_LEASE",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "LEASE_MS",
            lease_ms,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        response = await self.executor.execute_command(*args)
        return await self._record_or_get(response, id, partition_key)

    async def transition(
        self,
        id: str,
        *,
        from_state: str,
        to_state: str,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        now_ms = now_ms if now_ms is not None else _now_ms()
        args: builtins.list[Any] = [
            "FLOW.TRANSITION",
            id,
            from_state,
            to_state,
            "LEASE_TOKEN",
            lease_token,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms,
        ]
        _append(args, "PARTITION", partition_key)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "RUN_AT", run_at_ms if run_at_ms is not None else now_ms)
        _append(args, "PRIORITY", priority)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def step_continue(
        self,
        id: str,
        *,
        lease_token: bytes,
        from_state: str,
        to_state: str,
        fencing_token: int,
        lease_ms: int = 30_000,
        partition_key: str | None = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        now_ms: int | None = None,
        worker: str | None = None,
    ) -> FlowRecord:
        args: builtins.list[Any] = [
            "FLOW.STEP_CONTINUE",
            id,
            lease_token,
            from_state,
            to_state,
            "FENCING",
            fencing_token,
            "LEASE_MS",
            lease_ms,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "WORKER", worker)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        response = await self.executor.execute_command(*args)
        return await self._record_or_get(response, id, partition_key)

    async def complete_many(
        self,
        partition_key: str | None,
        items: builtins.list[ClaimedFlow],
        *,
        result: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args: builtins.list[Any] = [
            "FLOW.COMPLETE_MANY",
            "MIXED" if partition_key is None else partition_key,
        ]
        _append_encoded(args, "RESULT", self.codec, result)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_claimed_items(args, partition_key, items, "FLOW.COMPLETE_MANY")
        return self._records_or_response(await self.executor.execute_command(*args))

    async def complete_jobs(
        self,
        jobs: builtins.list[ClaimedFlow],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = True,
    ) -> builtins.list[FlowRecord] | Any:
        if not jobs:
            return []

        first_partition = jobs[0].partition_key
        partition_key = (
            first_partition
            if first_partition is not None
            and all(job.partition_key == first_partition for job in jobs)
            else None
        )
        return await self.complete_many(
            partition_key,
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
        )

    async def complete(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        result: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args: builtins.list[Any] = [
            "FLOW.COMPLETE",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append_encoded(args, "RESULT", self.codec, result)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "TTL", ttl_ms)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def transition_many(
        self,
        partition_key: str | None,
        *,
        from_state: str,
        to_state: str,
        items: builtins.list[FencedItem],
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        mixed = partition_key is None
        wire_partition = "MIXED" if mixed else partition_key
        args: builtins.list[Any] = ["FLOW.TRANSITION_MANY", wire_partition, from_state, to_state]
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "PRIORITY", priority)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_fenced_items(
            args,
            partition_key,
            items,
            "FLOW.TRANSITION_MANY",
            include_lease=True,
        )
        return self._records_or_response(await self.executor.execute_command(*args))

    async def retry(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args: builtins.list[Any] = [
            "FLOW.RETRY",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append_encoded(args, "ERROR", self.codec, error)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "RUN_AT", run_at_ms)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def retry_many(
        self,
        partition_key: str | None,
        items: builtins.list[ClaimedFlow],
        *,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args: builtins.list[Any] = [
            "FLOW.RETRY_MANY",
            "MIXED" if partition_key is None else partition_key,
        ]
        _append(args, "ERROR", self.codec.encode(error) if error is not None else None)
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_claimed_items(args, partition_key, items, "FLOW.RETRY_MANY")
        return self._records_or_response(await self.executor.execute_command(*args))

    async def fail(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args: builtins.list[Any] = [
            "FLOW.FAIL",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append_encoded(args, "ERROR", self.codec, error)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "TTL", ttl_ms)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def fail_many(
        self,
        partition_key: str | None,
        items: builtins.list[ClaimedFlow],
        *,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args: builtins.list[Any] = [
            "FLOW.FAIL_MANY",
            "MIXED" if partition_key is None else partition_key,
        ]
        _append(args, "ERROR", self.codec.encode(error) if error is not None else None)
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_claimed_items(args, partition_key, items, "FLOW.FAIL_MANY")
        return self._records_or_response(await self.executor.execute_command(*args))

    async def cancel(
        self,
        id: str,
        *,
        fencing_token: int,
        lease_token: bytes | None = None,
        partition_key: str | None = None,
        reason: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        attributes_merge: dict[str, Any] | None = None,
        attributes_delete: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args: builtins.list[Any] = [
            "FLOW.CANCEL",
            id,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "PARTITION", partition_key)
        _append(args, "REASON", self.codec.encode(reason) if reason is not None else None)
        _append(args, "TTL", ttl_ms)
        _append_attributes(
            args,
            attributes_merge=attributes_merge,
            attributes_delete=attributes_delete,
        )
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def cancel_many(
        self,
        partition_key: str | None,
        items: builtins.list[FencedItem],
        *,
        reason: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
        state_meta: dict[str, Any] | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = None,
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

        args: builtins.list[Any] = [
            "FLOW.CANCEL_MANY",
            "MIXED" if partition_key is None else partition_key,
        ]
        _append(args, "REASON", self.codec.encode(reason) if reason is not None else None)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_state_meta(args, state_meta)
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_fenced_items(args, partition_key, items, "FLOW.CANCEL_MANY")
        return self._records_or_response(await self.executor.execute_command(*args))

    async def rewind(
        self,
        id: str,
        *,
        to_event: str,
        partition_key: str | None = None,
        expect_state: str | None = None,
        run_at_ms: int | None = None,
        reason_ref: str | None = None,
        now_ms: int | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        args: builtins.list[Any] = [
            "FLOW.REWIND",
            id,
            "TO_EVENT",
            to_event,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "EXPECT_STATE", expect_state)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "REASON_REF", reason_ref)
        response = await self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return await self._record_or_get(response, id, partition_key)

    async def get(
        self,
        id: str,
        *,
        partition_key: str | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
    ) -> FlowRecord | None:
        args: builtins.list[Any] = ["FLOW.GET", id]
        _append(args, "PARTITION", partition_key)
        _append_value_return(args, values=values, value_max_bytes=value_max_bytes)
        value = await self.executor.execute_command(*args)
        if value is None:
            return None
        return self._record(value)

    async def list(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
        count: int | None = None,
        attributes: dict[str, Any] | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.LIST", type]
        _append(args, "STATE", state)
        _append(args, "COUNT", count)
        _append(args, "PARTITION", partition_key)
        _append_attributes(args, attributes=attributes)
        _append_bool(args, "INCLUDE_COLD", include_cold)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return self._records(await self.executor.execute_command(*args))

    async def search(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        terminal_only: bool | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.SEARCH", type]
        _append_read_options(
            args,
            state=state,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            terminal_only=terminal_only,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        _append_attributes(args, attributes=attributes)
        _append_search_state_meta(args, state, state_meta)
        return self._records(await self.executor.execute_command(*args))

    async def stats(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
        count: int | None = None,
        attributes: dict[str, Any] | None = None,
        consistent_projection: bool | None = None,
    ) -> dict[str, Any]:
        args: builtins.list[Any] = ["FLOW.STATS", type]
        _append(args, "STATE", state)
        _append(args, "COUNT", count)
        _append(args, "PARTITION", partition_key)
        _append_attributes(args, attributes=attributes)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return _parse_kv_response(await self.executor.execute_command(*args))

    async def attributes(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
        count: int | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        args: builtins.list[Any] = ["FLOW.ATTRIBUTES", type]
        _append(args, "STATE", state)
        _append(args, "PARTITION", partition_key)
        _append(args, "COUNT", count)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def attribute_values(
        self,
        type: str,
        attribute: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
        count: int | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        args: builtins.list[Any] = ["FLOW.ATTRIBUTE_VALUES", type, attribute]
        _append(args, "STATE", state)
        _append(args, "PARTITION", partition_key)
        _append(args, "COUNT", count)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def terminals(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.TERMINALS", type]
        _append_read_options(
            args,
            state=state,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        return self._records(await self.executor.execute_command(*args))

    async def failures(
        self,
        type: str,
        *,
        partition_key: str | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.FAILURES", type]
        _append_read_options(
            args,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        return self._records(await self.executor.execute_command(*args))

    async def by_parent(self, parent_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self._index_query("FLOW.BY_PARENT", parent_flow_id, **kwargs)

    async def by_root(self, root_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self._index_query("FLOW.BY_ROOT", root_flow_id, **kwargs)

    async def by_correlation(self, correlation_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self._index_query("FLOW.BY_CORRELATION", correlation_id, **kwargs)

    async def info(
        self,
        type: str,
        *,
        partition_key: str | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.INFO", type]
        _append(args, "PARTITION", partition_key)
        _append_bool(args, "INCLUDE_COLD", include_cold)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return dict(await self.executor.execute_command(*args) or {})

    async def stuck(
        self,
        type: str,
        *,
        partition_key: str | None = None,
        count: int | None = None,
        older_than_ms: int | None = None,
        now_ms: int | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.STUCK", type]
        _append(args, "PARTITION", partition_key)
        _append(args, "COUNT", count)
        _append(args, "OLDER_THAN", older_than_ms)
        _append(args, "NOW", now_ms)
        return self._records(await self.executor.execute_command(*args))

    async def history(
        self,
        id: str,
        *,
        partition_key: str | None = None,
        count: int = 100,
        from_event: str | None = None,
        to_event: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        from_version: int | None = None,
        to_version: int | None = None,
        rev: bool | None = None,
        event: str | None = None,
        worker: str | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
        values: bool | None = None,
        payload_max_bytes: int | None = None,
    ) -> builtins.list[Any]:
        args: builtins.list[Any] = ["FLOW.HISTORY", id, "COUNT", count]
        _append(args, "PARTITION", partition_key)
        _append(args, "FROM_EVENT", from_event)
        _append(args, "TO_EVENT", to_event)
        _append(args, "FROM_MS", from_ms)
        _append(args, "TO_MS", to_ms)
        _append(args, "FROM_VERSION", from_version)
        _append(args, "TO_VERSION", to_version)
        _append_bool(args, "REV", rev)
        _append(args, "EVENT", event)
        _append(args, "WORKER", worker)
        _append_bool(args, "INCLUDE_COLD", include_cold)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        _append_bool(args, "VALUES", values)
        _append(args, "PAYLOAD_MAX_BYTES", payload_max_bytes)
        return list(await self.executor.execute_command(*args) or [])

    async def spawn_children(
        self,
        parent_id: str,
        children: builtins.list[ChildSpec],
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        group_id: str = "default",
        wait: str = "all",
        wait_state: str | None = None,
        success: str | None = None,
        failure: str | None = None,
        from_state: str | None = None,
        on_child_failed: str | None = None,
        on_parent_closed: str | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        now_ms: int | None = None,
    ) -> Any:
        args: builtins.list[Any] = [
            "FLOW.SPAWN_CHILDREN",
            parent_id,
            "GROUP",
            group_id,
            "WAIT",
            wait,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "WAIT_STATE", wait_state)
        _append(args, "SUCCESS", success)
        _append(args, "FAILURE", failure)
        _append(args, "FROM_STATE", from_state)
        _append(args, "ON_CHILD_FAILED", on_child_failed)
        _append(args, "ON_PARENT_CLOSED", on_parent_closed)
        mixed = any(child.partition_key is not None for child in children)
        if _has_named_item_values(children):
            args.extend(["ITEMS_EXT", len(children)])
            for child in children:
                if mixed and child.partition_key is None:
                    raise ValueError("mixed spawn_children items require partition_key")
                child_values = _merge_named_map(values, child.values)
                child_refs = _merge_named_map(value_refs, child.value_refs)
                args.extend(
                    [
                        child.id,
                        child.partition_key or "-",
                        child.type,
                        self.codec.encode(child.payload),
                    ]
                )
                _append_named_counts(args, self.codec, child_values, child_refs)
        else:
            _append_named_values(args, self.codec, values=values, value_refs=value_refs)
            args.append("ITEMS")
            if mixed:
                args.append("MIXED")
            for child in children:
                if mixed:
                    if child.partition_key is None:
                        raise ValueError("mixed spawn_children items require partition_key")
                    args.extend(
                        [
                            child.id,
                            child.partition_key,
                            child.type,
                            self.codec.encode(child.payload),
                        ]
                    )
                else:
                    args.extend([child.id, child.type, self.codec.encode(child.payload)])
        return await self.executor.execute_command(*args)

    async def schedule_create(
        self,
        id: str,
        *,
        target: dict[str, Any],
        kind: str | None = None,
        at_ms: int | None = None,
        delay_ms: int | None = None,
        start_at_ms: int | None = None,
        every_ms: int | None = None,
        cron: str | None = None,
        timezone: str | None = None,
        overlap_policy: str | None = None,
        overlap_retry_ms: int | None = None,
        max_fires: int | None = None,
        end_at_ms: int | None = None,
        overwrite: bool | None = None,
        now_ms: int | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.CREATE", id]
        _append(args, "KIND", kind)
        _append(args, "AT_MS", at_ms)
        _append(args, "DELAY_MS", delay_ms)
        _append(args, "START_AT_MS", start_at_ms)
        _append(args, "EVERY_MS", every_ms)
        _append(args, "CRON", cron)
        _append(args, "TIMEZONE", timezone)
        _append(args, "TARGET", target)
        _append(args, "OVERLAP_POLICY", overlap_policy)
        _append(args, "OVERLAP_RETRY_MS", overlap_retry_ms)
        _append(args, "MAX_FIRES", max_fires)
        _append(args, "END_AT_MS", end_at_ms)
        _append_bool(args, "OVERWRITE", overwrite)
        _append(args, "NOW", now_ms)
        _append_extra_options(args, extra_options)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_get(self, id: str, *, now_ms: int | None = None) -> ScheduleResult | None:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.GET", id]
        _append(args, "NOW", now_ms)
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return ScheduleResult.from_resp(response) if response is not None else None

    async def schedule_fire(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.FIRE", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_pause(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.PAUSE", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_resume(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.RESUME", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_delete(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.DELETE", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_fire_due(
        self,
        *,
        now_ms: int | None = None,
        worker: str | None = None,
        block_ms: int | None = None,
        limit: int | None = None,
    ) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.FIRE_DUE"]
        _append(args, "NOW", now_ms)
        _append(args, "WORKER", worker)
        _append(args, "BLOCK", block_ms)
        _append(args, "LIMIT", limit)
        return ScheduleResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def schedule_list(
        self,
        *,
        kind: str | None = None,
        state: str | None = None,
        timezone: str | None = None,
        target_type: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        count: int | None = None,
        rev: bool | None = None,
    ) -> builtins.list[ScheduleResult]:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.LIST"]
        _append(args, "KIND", kind)
        _append(args, "STATE", state)
        _append(args, "TIMEZONE", timezone)
        _append(args, "TARGET_TYPE", target_type)
        _append(args, "FROM_MS", from_ms)
        _append(args, "TO_MS", to_ms)
        _append(args, "COUNT", count)
        _append_bool(args, "REV", rev)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return [ScheduleResult.from_resp(item) for item in response]

    async def install_policy(
        self,
        type: str,
        *,
        retry: RetryPolicy | None = None,
        states: dict[str, FlowStatePolicyLike] | None = None,
        indexed_state_meta: str | None = None,
    ) -> Any:
        args: builtins.list[Any] = ["FLOW.POLICY.SET", type]
        _append(args, "INDEXED_STATE_META", indexed_state_meta)
        if retry is not None:
            self._append_retry_policy(args, retry)
        for state, policy in (states or {}).items():
            args.extend(["STATE", state])
            self._append_state_policy(args, policy)
        return await self.executor.execute_command(*args)

    async def policy_get(self, type: str, *, state: str | None = None) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.POLICY.GET", type]
        _append(args, "STATE", state)
        return dict(await self.executor.execute_command(*args) or {})

    async def effect_reserve(
        self,
        id: str,
        effect_key: str,
        effect_type: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        operation_digest: str,
        idempotency_key: str | None = None,
        governance_scope: str | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        args: builtins.list[Any] = ["FLOW.EFFECT.RESERVE", id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "EFFECT_TYPE", effect_type)
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "OPERATION_DIGEST", operation_digest)
        _append(args, "IDEMPOTENCY_KEY", idempotency_key)
        _append(args, "GOVERNANCE_SCOPE", governance_scope)
        _append(args, "NOW", now_ms)
        return EffectResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def effect_confirm(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return await self._effect_status(
            "FLOW.EFFECT.CONFIRM",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            external_id=external_id,
            latency_ms=latency_ms,
            now_ms=now_ms,
        )

    async def effect_fail(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return await self._effect_status(
            "FLOW.EFFECT.FAIL",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            error=error,
            reason=reason,
            latency_ms=latency_ms,
            now_ms=now_ms,
        )

    async def effect_compensate(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return await self._effect_status(
            "FLOW.EFFECT.COMPENSATE",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            external_id=external_id,
            reason=reason,
            now_ms=now_ms,
        )

    async def effect_get(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
    ) -> EffectResult | None:
        args: builtins.list[Any] = ["FLOW.EFFECT.GET", id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "PARTITION", partition_key)
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return EffectResult.from_resp(response) if response is not None else None

    async def _effect_status(
        self,
        command: str,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        args: builtins.list[Any] = [command, id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "EXTERNAL_ID", external_id)
        _append(args, "ERROR", error)
        _append(args, "REASON", reason)
        _append(args, "LATENCY_MS", latency_ms)
        _append(args, "NOW", now_ms)
        return EffectResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def governance_ledger(
        self,
        id: str,
        *,
        partition_key: str | None = None,
        limit: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        args: builtins.list[Any] = ["FLOW.GOVERNANCE.LEDGER", id]
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        _append(args, "FROM_MS", from_ms)
        _append(args, "TO_MS", to_ms)
        _append_bool(args, "REV", rev)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def approval_request(
        self,
        id: str,
        *,
        flow_id: str,
        scope: str,
        reason: str | None = None,
        requested_by: str | None = None,
        assignees: Sequence[str] | None = None,
        policy_hash: str | None = None,
        policy_version: str | int | None = None,
        timeout_ms: int | None = None,
        expires_at_ms: int | None = None,
        now_ms: int | None = None,
    ) -> ApprovalResult:
        args: builtins.list[Any] = ["FLOW.APPROVAL.REQUEST", id]
        _append(args, "FLOW_ID", flow_id)
        _append(args, "SCOPE", scope)
        _append(args, "REASON", reason)
        _append(args, "REQUESTED_BY", requested_by)
        _append(args, "ASSIGNEES", list(assignees) if assignees is not None else None)
        _append(args, "POLICY_HASH", policy_hash)
        _append(args, "POLICY_VERSION", policy_version)
        _append(args, "TIMEOUT_MS", timeout_ms)
        _append(args, "EXPIRES_AT_MS", expires_at_ms)
        _append(args, "NOW", now_ms)
        return ApprovalResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def approval_approve(
        self,
        id: str,
        *,
        approver: str,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> ApprovalResult:
        return await self._approval_status("FLOW.APPROVAL.APPROVE", id, approver, reason, now_ms)

    async def approval_reject(
        self,
        id: str,
        *,
        approver: str,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> ApprovalResult:
        return await self._approval_status("FLOW.APPROVAL.REJECT", id, approver, reason, now_ms)

    async def _approval_status(
        self,
        command: str,
        id: str,
        approver: str,
        reason: str | None,
        now_ms: int | None,
    ) -> ApprovalResult:
        args: builtins.list[Any] = [command, id]
        _append(args, "APPROVER", approver)
        _append(args, "REASON", reason)
        _append(args, "NOW", now_ms)
        return ApprovalResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def approval_get(self, id: str) -> ApprovalResult | None:
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(await self.executor.execute_command("FLOW.APPROVAL.GET", id)),
        )
        return ApprovalResult.from_resp(response) if response is not None else None

    async def approval_list(
        self,
        *,
        status: str | None = None,
        scope: str | None = None,
        partition_key: str | None = None,
        flow_id: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[ApprovalResult]:
        args: builtins.list[Any] = ["FLOW.APPROVAL.LIST"]
        _append(args, "STATUS", status)
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "FLOW_ID", flow_id)
        _append(args, "LIMIT", limit)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return [ApprovalResult.from_resp(item) for item in response]

    async def governance_overview(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        status: str | None = None,
        flow_id: str | None = None,
        limit: int | None = None,
    ) -> GovernanceOverview:
        args: builtins.list[Any] = ["FLOW.GOVERNANCE.OVERVIEW"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "STATUS", status)
        _append(args, "FLOW_ID", flow_id)
        _append(args, "LIMIT", limit)
        return GovernanceOverview.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def circuit_open(
        self,
        scope: str,
        *,
        open_ms: int | None = None,
        failure_threshold: int | None = None,
        now_ms: int | None = None,
    ) -> CircuitBreakerStatus:
        args: builtins.list[Any] = ["FLOW.CIRCUIT.OPEN", scope]
        _append(args, "OPEN_MS", open_ms)
        _append(args, "FAILURE_THRESHOLD", failure_threshold)
        _append(args, "NOW", now_ms)
        return CircuitBreakerStatus.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def circuit_close(self, scope: str, *, now_ms: int | None = None) -> CircuitBreakerStatus:
        args: builtins.list[Any] = ["FLOW.CIRCUIT.CLOSE", scope]
        _append(args, "NOW", now_ms)
        return CircuitBreakerStatus.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def circuit_get(self, scope: str) -> CircuitBreakerStatus | None:
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(
                await self.executor.execute_command("FLOW.CIRCUIT.GET", scope)
            ),
        )
        return CircuitBreakerStatus.from_resp(response) if response is not None else None

    async def budget_reserve(
        self,
        scope: str,
        amount: int,
        *,
        limit: int | None = None,
        window_ms: int | None = None,
        reservation_id: str | None = None,
        now_ms: int | None = None,
    ) -> BudgetResult:
        args: builtins.list[Any] = ["FLOW.BUDGET.RESERVE", scope]
        _append(args, "AMOUNT", amount)
        _append(args, "LIMIT", limit)
        _append(args, "WINDOW_MS", window_ms)
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def budget_commit(
        self,
        scope: str,
        reservation_id: str,
        actual_amount: int,
        *,
        usage: Mapping[str, Any] | None = None,
        now_ms: int | None = None,
    ) -> BudgetResult:
        args: builtins.list[Any] = ["FLOW.BUDGET.COMMIT", scope]
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "ACTUAL_AMOUNT", actual_amount)
        _append(args, "USAGE", dict(usage) if usage is not None else None)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def budget_release(
        self,
        scope: str,
        reservation_id: str,
        *,
        now_ms: int | None = None,
    ) -> BudgetResult:
        args: builtins.list[Any] = ["FLOW.BUDGET.RELEASE", scope]
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def budget_get(self, scope: str) -> BudgetResult | None:
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(
                await self.executor.execute_command("FLOW.BUDGET.GET", scope)
            ),
        )
        return BudgetResult.from_resp(response) if response is not None else None

    async def budget_list(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[BudgetResult]:
        args: builtins.list[Any] = ["FLOW.BUDGET.LIST"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return [BudgetResult.from_resp(item) for item in response]

    async def limit_lease(
        self,
        scope: str,
        *,
        shard_id: int,
        amount: int,
        ttl_ms: int,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        args: builtins.list[Any] = ["FLOW.LIMIT.LEASE", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "AMOUNT", amount)
        _append(args, "LIMIT", limit)
        _append(args, "TTL_MS", ttl_ms)
        _append(args, "NOW", now_ms)
        return cast(
            dict[str, Any],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def limit_spend(
        self,
        scope: str,
        *,
        shard_id: int,
        amount: int,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        args: builtins.list[Any] = ["FLOW.LIMIT.SPEND", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "AMOUNT", amount)
        _append(args, "NOW", now_ms)
        return cast(
            dict[str, Any],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def limit_release(self, scope: str, *, shard_id: int, amount: int) -> dict[str, Any]:
        args: builtins.list[Any] = ["FLOW.LIMIT.RELEASE", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "AMOUNT", amount)
        return cast(
            dict[str, Any],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def limit_get(self, scope: str, *, now_ms: int | None = None) -> dict[str, Any] | None:
        args: builtins.list[Any] = ["FLOW.LIMIT.GET", scope]
        _append(args, "NOW", now_ms)
        return cast(
            dict[str, Any] | None,
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def limit_list(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> builtins.list[dict[str, Any]]:
        args: builtins.list[Any] = ["FLOW.LIMIT.LIST"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        _append(args, "NOW", now_ms)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def retention_cleanup(
        self,
        *,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.RETENTION_CLEANUP"]
        _append(args, "LIMIT", limit)
        _append(args, "NOW", now_ms)
        return dict(await self.executor.execute_command(*args) or {})

    async def _index_query(
        self, command: str, key: str, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = [command, key]
        _append_read_options(args, **kwargs)
        return self._records(await self.executor.execute_command(*args))

    def _append_claimed_items(
        self,
        args: builtins.list[Any],
        partition_key: str | None,
        items: builtins.list[ClaimedFlow],
        command: str,
    ) -> builtins.list[Any]:
        mixed = partition_key is None
        args.append("ITEMS")
        for item in items:
            if mixed:
                if item.partition_key is None:
                    raise ValueError(f"mixed {command} items require partition_key")
                args.extend([item.id, item.partition_key, item.lease_token, item.fencing_token])
            else:
                if item.partition_key is not None and item.partition_key != partition_key:
                    raise ValueError(
                        f"{command} item partition_key does not match batch partition_key"
                    )
                args.extend([item.id, item.lease_token, item.fencing_token])
        return args

    def _append_fenced_items(
        self,
        args: builtins.list[Any],
        partition_key: str | None,
        items: builtins.list[FencedItem],
        command: str,
        *,
        include_lease: bool = False,
    ) -> builtins.list[Any]:
        mixed = partition_key is None
        args.append("ITEMS")
        for item in items:
            lease = item.lease_token if item.lease_token is not None else "-"
            if mixed:
                if item.partition_key is None:
                    raise ValueError(f"mixed {command} items require partition_key")
                args.extend([item.id, item.partition_key, item.fencing_token])
                if include_lease:
                    args.append(lease)
            else:
                if item.partition_key is not None and item.partition_key != partition_key:
                    raise ValueError(
                        f"{command} item partition_key does not match batch partition_key"
                    )
                args.extend([item.id, item.fencing_token])
                if include_lease:
                    args.append(lease)
        return args

    def _append_retry_policy(self, args: builtins.list[Any], policy: RetryPolicy) -> None:
        args.extend(
            [
                "MAX_RETRIES",
                policy.max_retries,
                "BACKOFF",
                policy.backoff,
                "BASE_MS",
                policy.base_ms,
                "MAX_MS",
                policy.max_ms,
                "JITTER_PCT",
                policy.jitter_pct,
                "EXHAUSTED_TO",
                policy.exhausted_to,
            ]
        )

    def _append_state_policy(self, args: builtins.list[Any], policy: FlowStatePolicyLike) -> None:
        mode, retry = _split_flow_state_policy(policy)
        if mode is not None:
            args.extend(["MODE", mode.upper()])
        if retry is not None:
            self._append_retry_policy(args, retry)

    def _record(self, value: dict[Any, Any]) -> FlowRecord:
        raw_payload = value.get("payload") if "payload" in value else value.get(b"payload")
        raw_values = value.get("values") if "values" in value else value.get(b"values")
        values = None
        if isinstance(raw_values, dict):
            values = {
                (key.decode() if isinstance(key, bytes) else str(key)): self.codec.decode(item)
                for key, item in raw_values.items()
            }
        return FlowRecord.from_resp(value, payload=self.codec.decode(raw_payload), values=values)

    async def _record_or_get(
        self,
        value: Any,
        id: str,
        partition_key: str | None = None,
    ) -> FlowRecord:
        if isinstance(value, dict):
            return self._record(value)
        lookup_partition = (
            _auto_partition_key_for_id(id) if partition_key is None else partition_key
        )
        record = await self.get(id, partition_key=lookup_partition)
        if record is None:
            raise RuntimeError(f"FLOW command succeeded but record {id!r} was not found")
        return record

    def _records(self, values: builtins.list[dict[Any, Any]]) -> builtins.list[FlowRecord]:
        return [self._record(value) for value in values]

    def _records_or_response(self, value: Any) -> builtins.list[FlowRecord] | Any:
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return self._records(value)
        return value

    async def _execute_producer_write(self, *args: Any) -> Any:
        attempt = 0
        while True:
            await self.backpressure.before_request_async()
            try:
                result = await self.executor.execute_command(*args)
                self.backpressure.record_success()
                return result
            except OverloadedError as exc:
                if not self.backpressure.can_retry(attempt):
                    raise
                await self.backpressure.record_overload_async(attempt, exc.retry_after_ms)
                attempt += 1
