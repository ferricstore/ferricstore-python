from __future__ import annotations

import builtins
import inspect
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.adapters import AsyncRedisAdapter, AsyncRedisCommandExecutor
from ferricstore.backpressure import BackpressureController, BackpressurePolicy
from ferricstore.client import (
    FlowClient,
    _append,
    _append_bool,
    _append_encoded,
    _append_named_counts,
    _append_named_values,
    _append_payload_read,
    _append_read_options,
    _append_value_return,
    _auto_partition_key_for_id,
    _expand_many_response,
    _flow_return,
    _has_named_item_values,
    _merge_named_map,
    _now_ms,
    _ok_response,
    _parse_kv_response,
)
from ferricstore.codecs import Codec, RawCodec
from ferricstore.errors import OverloadedError, map_exception
from ferricstore.types import (
    ChildSpec,
    ClaimedItem,
    CreateItem,
    FencedItem,
    FetchOrComputeResult,
    FlowRecord,
    KeyInfo,
    RateLimitResult,
    RetryPolicy,
    _normalize_ref_meta,
)


class _AsyncErrorMappingExecutor:
    def __init__(self, executor: AsyncRedisCommandExecutor) -> None:
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
    """Async mixed-command pipeline over `redis.asyncio` when available."""

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
        redis_client = getattr(raw_executor, "client", None)
        pipeline_factory = getattr(redis_client, "pipeline", None)

        if callable(pipeline_factory):
            pipe = pipeline_factory(transaction=False)
            for command in self.commands:
                pipe.execute_command(*command)
            try:
                self.results = list(await pipe.execute())
            except Exception as exc:
                mapped = map_exception(exc)
                if mapped is exc:
                    raise
                raise mapped from exc
            return self.results

        self.results = [await self.client.command(*command) for command in self.commands]
        return self.results


class AsyncFlowClient:
    """True async FerricFlow client over `redis.asyncio` or any async executor."""

    def __init__(
        self,
        executor: AsyncRedisCommandExecutor,
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

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        codec: Codec | None = None,
        backpressure: BackpressurePolicy | None = None,
        **kwargs: Any,
    ) -> AsyncFlowClient:
        if urlparse(url).scheme.lower() in {"ferric", "ferrics"}:
            from ferricstore.protocol import AsyncProtocolAdapterPool

            return cls(
                AsyncProtocolAdapterPool.from_url(url, **kwargs),
                codec=codec,
                backpressure=backpressure,
            )
        return cls(
            AsyncRedisAdapter.from_url(url, **kwargs),
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
        return await self.executor.execute_command(*args)

    def pipeline(self) -> AsyncCommandPipeline:
        return AsyncCommandPipeline(self)

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
    ) -> builtins.list[FlowRecord] | Any:
        if not items:
            return []

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
        job_only: bool = False,
        payload: bool | None = None,
        payload_max_bytes: int | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        include_state: bool = False,
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedItem]:
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
        if include_state and not job_only:
            raise ValueError("include_state requires job_only=True")
        if job_only:
            _append(args, "RETURN", "JOBS_COMPACT_STATE" if include_state else "JOBS_COMPACT")
        _append(args, "BLOCK", block_ms)
        _append_payload_read(args, payload, payload_max_bytes)
        _append_value_return(args, values=values, value_max_bytes=value_max_bytes)
        _append_bool(args, "RECLAIM_EXPIRED", reclaim_expired)
        _append(args, "RECLAIM_RATIO", reclaim_ratio)
        response = await self.executor.execute_command(*args)
        if job_only:
            return ClaimedItem.from_compact_rows(response)
        return self._records(response)

    async def claim_jobs(
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
    ) -> builtins.list[ClaimedItem]:
        return cast(
            builtins.list[ClaimedItem],
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
                job_only=True,
                include_state=include_state,
            ),
        )

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
        job_only: bool = False,
        payload: bool | None = None,
        payload_max_bytes: int | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedItem]:
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
        if job_only:
            _append(args, "RETURN", "JOBS_COMPACT")
        _append_payload_read(args, payload, payload_max_bytes)
        _append_value_return(args, values=values, value_max_bytes=value_max_bytes)
        response = await self.executor.execute_command(*args)
        if job_only:
            return ClaimedItem.from_compact_rows(response)
        return self._records(response)

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
        items: builtins.list[ClaimedItem],
        *,
        result: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
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
        jobs: builtins.list[ClaimedItem],
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
        items: builtins.list[ClaimedItem],
        *,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
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
        items: builtins.list[ClaimedItem],
        *,
        error: Any = None,
        payload: Any = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        drop_values: builtins.list[str] | None = None,
        override_values: builtins.list[str] | None = None,
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
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.LIST", type]
        _append(args, "STATE", state)
        _append(args, "COUNT", count)
        _append(args, "PARTITION", partition_key)
        _append_bool(args, "INCLUDE_COLD", include_cold)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return self._records(await self.executor.execute_command(*args))

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

    async def install_policy(
        self,
        type: str,
        *,
        retry: RetryPolicy | None = None,
        states: dict[str, RetryPolicy] | None = None,
    ) -> Any:
        args: builtins.list[Any] = ["FLOW.POLICY.SET", type]
        if retry is not None:
            self._append_retry_policy(args, retry)
        for state, policy in (states or {}).items():
            args.extend(["STATE", state])
            self._append_retry_policy(args, policy)
        return await self.executor.execute_command(*args)

    async def policy_get(self, type: str, *, state: str | None = None) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.POLICY.GET", type]
        _append(args, "STATE", state)
        return dict(await self.executor.execute_command(*args) or {})

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
        items: builtins.list[ClaimedItem],
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
