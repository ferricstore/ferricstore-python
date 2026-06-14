from __future__ import annotations

import atexit
import builtins
import contextlib
import threading
import time
import zlib
from concurrent.futures import Future
from dataclasses import dataclass
from collections.abc import Sequence
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.adapters import RedisAdapter, RedisCommandExecutor
from ferricstore.backpressure import BackpressureController, BackpressurePolicy
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

_AUTO_PARTITION_PREFIX = "__flow_auto__:"
_AUTO_PARTITION_BUCKETS = 256


def _flow_return(value: Any) -> FlowRecord | bytes:
    return cast(FlowRecord | bytes, value)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _append(args: builtins.list[Any], name: str, value: Any) -> None:
    if value is not None:
        args.extend([name, value])


def _append_bool(args: builtins.list[Any], name: str, value: bool | None) -> None:
    if value is not None:
        args.extend([name, "true" if value else "false"])


def _append_payload_read(
    args: builtins.list[Any], payload: bool | None, max_bytes: int | None
) -> None:
    if payload is False:
        args.append("NOPAYLOAD")
        return
    if payload is True or max_bytes is not None:
        args.append("PAYLOAD")
    _append(args, "MAXBYTES", max_bytes)


def _append_encoded(args: builtins.list[Any], name: str, codec: Codec, value: Any) -> None:
    if value is not None:
        args.extend([name, codec.encode(value)])


def _append_named_values(
    args: builtins.list[Any],
    codec: Codec,
    *,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
) -> None:
    for name, value in (values or {}).items():
        args.extend(["VALUE", name, codec.encode(value)])
    for name, ref in (value_refs or {}).items():
        args.extend(["VALUE_REF", name, ref])
    for name in drop_values or []:
        args.extend(["DROP_VALUE", name])
    for name in override_values or []:
        args.extend(["OVERRIDE_VALUE", name])


def _merge_named_map(base: dict[str, Any] | None, item: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if base:
        merged.update(base)
    if item:
        merged.update(item)
    return merged


def _has_named_item_values(items: builtins.list[Any]) -> bool:
    return any(getattr(item, "values", None) or getattr(item, "value_refs", None) for item in items)


def _append_named_counts(
    args: builtins.list[Any],
    codec: Codec,
    values: dict[str, Any],
    value_refs: dict[str, str],
) -> None:
    args.append(len(values))
    for name, value in values.items():
        args.extend([name, codec.encode(value)])
    args.append(len(value_refs))
    for name, ref in value_refs.items():
        args.extend([name, ref])


def _append_value_return(
    args: builtins.list[Any],
    *,
    values: builtins.list[str] | None = None,
    value_max_bytes: int | None = None,
) -> None:
    for name in values or []:
        args.extend(["VALUE", name])
    _append(args, "VALUE_MAX_BYTES", value_max_bytes)


def _batch_key_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bytes, int, float, bool)):
        return value
    return repr(value)


def _batch_named_key(
    *,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
) -> tuple[Any, Any, tuple[str, ...], tuple[str, ...]]:
    value_items = tuple(
        sorted((name, _batch_key_value(value)) for name, value in (values or {}).items())
    )
    ref_items = tuple(sorted((value_refs or {}).items()))
    return (
        value_items,
        ref_items,
        tuple(drop_values or ()),
        tuple(override_values or ()),
    )


def _auto_partition_key_for_id(id: str) -> str:
    return f"{_AUTO_PARTITION_PREFIX}{zlib.crc32(id.encode()) % _AUTO_PARTITION_BUCKETS}"


def _expand_many_response(value: Any, count: int) -> builtins.list[Any]:
    if isinstance(value, list) and len(value) == count:
        return value
    return [value] * count


def _append_read_options(
    args: builtins.list[Any],
    *,
    partition_key: str | None = None,
    count: int | None = None,
    from_ms: int | None = None,
    to_ms: int | None = None,
    rev: bool | None = None,
    state: str | None = None,
    terminal_only: bool | None = None,
    include_cold: bool | None = None,
    consistent_projection: bool | None = None,
) -> None:
    _append(args, "COUNT", count)
    _append(args, "PARTITION", partition_key)
    _append(args, "FROM_MS", from_ms)
    _append(args, "TO_MS", to_ms)
    _append_bool(args, "REV", rev)
    _append(args, "STATE", state)
    _append_bool(args, "TERMINAL_ONLY", terminal_only)
    _append_bool(args, "INCLUDE_COLD", include_cold)
    _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)


def _ok_response(value: Any) -> bool:
    return value in ("OK", b"OK", True)


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _parse_kv_response(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {_text(key): item for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        items = list(value)
        if len(items) % 2 == 0:
            return {_text(items[idx]): items[idx + 1] for idx in range(0, len(items), 2)}
    if isinstance(value, (bytes, str)):
        return _parse_text_sections(_text(value))
    return {"value": value}


def _parse_text_sections(text: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    section: dict[str, Any] | None = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line:
            continue

        if line.endswith(":") and not line.startswith(" "):
            key = line[:-1]
            section = {}
            result[key] = section
            continue

        target = section if raw_line.startswith(" ") and section is not None else result
        stripped = line.strip()
        if ":" in stripped:
            key, value = stripped.split(":", 1)
            target[key.strip()] = _coerce_diag_value(value.strip())

    return result


def _coerce_diag_value(value: str) -> Any:
    if value == "":
        return value
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        return value


class _ErrorMappingExecutor:
    def __init__(self, executor: RedisCommandExecutor) -> None:
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
    """Small mixed-command pipeline.

    It accepts any Redis/FerricStore command. When backed by `RedisAdapter`, it
    uses redis-py's pipeline. For custom executors it falls back to sequential
    execution, preserving the same result list shape.
    """

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
        redis_client = getattr(raw_executor, "client", None)
        pipeline_factory = getattr(redis_client, "pipeline", None)

        if callable(pipeline_factory):
            pipe = pipeline_factory(transaction=False)
            for command in self.commands:
                pipe.execute_command(*command)
            try:
                self.results = pipe.execute()
            except Exception as exc:
                mapped = map_exception(exc)
                if mapped is exc:
                    raise
                raise mapped from exc
            return self.results

        self.results = [self.client.command(*command) for command in self.commands]
        return self.results


class FlowClient:
    """FerricFlow client over Redis/FerricStore commands."""

    def __init__(
        self,
        executor: RedisCommandExecutor,
        codec: Codec | None = None,
        *,
        backpressure: BackpressurePolicy | None = None,
    ) -> None:
        self.executor = _ErrorMappingExecutor(executor)
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
    ) -> FlowClient:
        if urlparse(url).scheme.lower() in {"ferric", "ferrics"}:
            from ferricstore.protocol import ProtocolAdapterPool

            return cls(
                ProtocolAdapterPool.from_url(url, **kwargs),
                codec=codec,
                backpressure=backpressure,
            )
        return cls(RedisAdapter.from_url(url, **kwargs), codec=codec, backpressure=backpressure)

    def autobatch(
        self,
        *,
        max_batch: int = 100,
        max_delay_ms: float = 1.0,
    ) -> AutobatchFlowClient:
        return AutobatchFlowClient(self, max_batch=max_batch, max_delay_ms=max_delay_ms)

    def command(self, *args: Any) -> Any:
        return self.executor.execute_command(*args)

    def pipeline(self) -> CommandPipeline:
        return CommandPipeline(self)

    def close(self) -> None:
        close = getattr(self.executor, "close", None)
        if callable(close):
            close()

    def subscribe_flow_wake(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> Any:
        subscribe = getattr(self.executor, "subscribe_flow_wake", None)
        if not callable(subscribe):
            raise RuntimeError("FLOW_WAKE subscriptions require protocol executor event support")
        return subscribe(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )

    def wait_event(self, timeout: float | None = None) -> Any | None:
        wait_event = getattr(self.executor, "wait_event", None)
        if not callable(wait_event):
            return None
        return wait_event(timeout=timeout)

    def cas(self, key: str, expected: Any, value: Any, *, ex: int | None = None) -> bool:
        args: builtins.list[Any] = [
            "CAS",
            key,
            self.codec.encode(expected),
            self.codec.encode(value),
        ]
        _append(args, "EX", ex)
        return bool(self.executor.execute_command(*args))

    def lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        return _ok_response(self.executor.execute_command("LOCK", key, owner, ttl_ms))

    def unlock(self, key: str, owner: str) -> int:
        return int(self.executor.execute_command("UNLOCK", key, owner))

    def extend_lock(self, key: str, owner: str, ttl_ms: int) -> int:
        return int(self.executor.execute_command("EXTEND", key, owner, ttl_ms))

    def ratelimit_add(
        self,
        key: str,
        *,
        window_ms: int,
        max: int,
        count: int = 1,
    ) -> RateLimitResult:
        return RateLimitResult.from_resp(
            self.executor.execute_command("RATELIMIT.ADD", key, window_ms, max, count)
        )

    def key_info(self, key: str) -> KeyInfo:
        return KeyInfo.from_resp(self.executor.execute_command("FERRICSTORE.KEY_INFO", key))

    def fetch_or_compute(
        self,
        key: str,
        *,
        ttl_ms: int,
        hint: str | None = None,
    ) -> FetchOrComputeResult:
        args: builtins.list[Any] = ["FETCH_OR_COMPUTE", key, ttl_ms]
        if hint is not None:
            args.append(hint)
        response = self.executor.execute_command(*args)
        status = response[0].decode() if isinstance(response[0], bytes) else str(response[0])

        if status == "hit":
            return FetchOrComputeResult(status="hit", value=self.codec.decode(response[1]))
        return FetchOrComputeResult(status="compute", compute_token=response[1])

    def fetch_or_compute_result(self, key: str, value: Any, *, ttl_ms: int) -> bool:
        response = self.executor.execute_command(
            "FETCH_OR_COMPUTE_RESULT",
            key,
            self.codec.encode(value),
            ttl_ms,
        )
        return _ok_response(response)

    def fetch_or_compute_error(self, key: str, message: str) -> bool:
        return _ok_response(self.executor.execute_command("FETCH_OR_COMPUTE_ERROR", key, message))

    def cluster_health(self) -> Any:
        return _parse_kv_response(self.executor.execute_command("CLUSTER.HEALTH"))

    def cluster_stats(self) -> Any:
        return _parse_kv_response(self.executor.execute_command("CLUSTER.STATS"))

    def cluster_keyslot(self, key: str) -> int:
        return int(self.executor.execute_command("CLUSTER.KEYSLOT", key))

    def cluster_slots(self) -> Any:
        return self.executor.execute_command("CLUSTER.SLOTS")

    def cluster_status(self) -> Any:
        return _parse_kv_response(self.executor.execute_command("CLUSTER.STATUS"))

    def cluster_role(self) -> Any:
        return self.executor.execute_command("CLUSTER.ROLE")

    def cluster_join(self, node: str, *, replace: bool = False) -> bool:
        args: builtins.list[Any] = ["CLUSTER.JOIN", node]
        if replace:
            args.append("REPLACE")
        return _ok_response(self.executor.execute_command(*args))

    def cluster_leave(self) -> bool:
        return _ok_response(self.executor.execute_command("CLUSTER.LEAVE"))

    def cluster_failover(self, shard_index: int, target_node: str) -> bool:
        return _ok_response(
            self.executor.execute_command("CLUSTER.FAILOVER", shard_index, target_node)
        )

    def cluster_promote(self, node: str) -> bool:
        return _ok_response(self.executor.execute_command("CLUSTER.PROMOTE", node))

    def cluster_demote(self, node: str) -> bool:
        return _ok_response(self.executor.execute_command("CLUSTER.DEMOTE", node))

    def ferricstore_config(self, *args: Any) -> Any:
        return self.executor.execute_command("FERRICSTORE.CONFIG", *args)

    def ferricstore_hotness(self, *args: Any) -> Any:
        return _parse_kv_response(self.executor.execute_command("FERRICSTORE.HOTNESS", *args))

    def ferricstore_metrics(self, *args: Any) -> Any:
        return _parse_kv_response(self.executor.execute_command("FERRICSTORE.METRICS", *args))

    def ferricstore_blobgc(self, *args: Any) -> Any:
        return self.executor.execute_command("FERRICSTORE.BLOBGC", *args)

    def create(
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
        response = self._execute_producer_write(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def enqueue(
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
        """Create a queued flow using the optimized ack-only path by default."""
        return self.create(
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

    def start_and_claim(
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
        response = self.executor.execute_command(*args)
        return self._record_or_get(response, id, partition_key)

    def run_steps_many(
        self,
        items: builtins.list[str | dict[str, Any] | CreateItem],
        *,
        type: str,
        states: Sequence[str] | None = None,
        steps: int | None = None,
        worker: str,
        lease_ms: int = 30_000,
        now_ms: int | None = None,
        payload: Any = None,
        result: Any = None,
        partition_key: str | None = None,
        retention_ttl_ms: int | None = None,
    ) -> bytes:
        """Run deterministic workflow step chains in one durable Flow command.

        This is the low-latency workflow-continuation path: FerricStore writes the
        created/step/completed state and history for each item in one Ra command.
        Use it only when the step chain is deterministic and does not need worker
        code between individual states.
        """
        if not items:
            return b"OK"
        if (states is None) == (steps is None):
            raise ValueError("run_steps_many requires exactly one of states or steps")
        if states is not None and not states:
            raise ValueError("run_steps_many states must be non-empty")
        if steps is not None and steps <= 0:
            raise ValueError("run_steps_many steps must be positive")

        args: builtins.list[Any] = ["FLOW.RUN_STEPS_MANY", "TYPE", type]
        if states is not None:
            args.extend(["STATES", list(states)])
        else:
            args.extend(["STEPS", steps])
        args.extend(["WORKER", worker, "LEASE_MS", lease_ms, "NOW", now_ms or _now_ms()])
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append_encoded(args, "RESULT", self.codec, result)
        _append(args, "RETENTION_TTL_MS", retention_ttl_ms)
        args.extend(["ITEMS", self._run_steps_many_items(items, partition_key)])
        return _flow_return(self.executor.execute_command(*args))

    @staticmethod
    def _run_steps_many_items(
        items: builtins.list[str | dict[str, Any] | CreateItem],
        partition_key: str | None,
    ) -> builtins.list[dict[str, str]]:
        normalized: builtins.list[dict[str, str]] = []
        for item in items:
            if isinstance(item, CreateItem):
                id = item.id
                item_partition = item.partition_key or partition_key
            elif isinstance(item, dict):
                raw_id = item.get("id")
                if not isinstance(raw_id, str) or not raw_id:
                    raise ValueError("run_steps_many item id must be a non-empty string")
                id = raw_id
                raw_partition = item.get("partition_key", partition_key)
                if raw_partition is not None and not isinstance(raw_partition, str):
                    raise ValueError("run_steps_many item partition_key must be a string")
                item_partition = raw_partition
            else:
                if not isinstance(item, str) or not item:
                    raise ValueError("run_steps_many item id must be a non-empty string")
                id = item
                item_partition = partition_key

            normalized_item = {"id": id}
            if item_partition is not None:
                normalized_item["partition_key"] = item_partition
            normalized.append(normalized_item)
        return normalized

    def enqueue_many(
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
        """Create many queued flows, grouping no-partition items by auto bucket."""
        if not items:
            return []

        if partition_key is not None:
            return self.create_many(
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

        if any(item.partition_key is not None for item in items):
            grouped: dict[str, builtins.list[tuple[int, CreateItem]]] = {}
            for idx, item in enumerate(items):
                group_key = item.partition_key or _auto_partition_key_for_id(item.id)
                grouped.setdefault(group_key, []).append((idx, item))

            results: builtins.list[Any] = [None] * len(items)
            for group_key, indexed_items in grouped.items():
                group_items = [item for _idx, item in indexed_items]
                response = self.create_many(
                    group_key,
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

        grouped: dict[str, builtins.list[tuple[int, CreateItem]]] = {}
        for idx, item in enumerate(items):
            grouped.setdefault(_auto_partition_key_for_id(item.id), []).append((idx, item))

        results: builtins.list[Any] = [None] * len(items)
        for bucket, indexed_items in grouped.items():
            group_items = [item for _idx, item in indexed_items]
            response = self.create_many(
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

    def _create_many_args(
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
    ) -> builtins.list[Any]:
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
        return args

    def create_many(
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

        args = self._create_many_args(
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
        return self._records_or_response(self._execute_producer_write(*args))

    def submit_create_many(
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
    ) -> Future[Any]:
        future: Future[Any] = Future()
        if not items:
            future.set_result([])
            return future

        submit_command = getattr(self.executor, "submit_command", None)
        if not callable(submit_command):
            future.set_exception(RuntimeError("submit_create_many requires async executor support"))
            return future

        args = self._create_many_args(
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
        source = submit_command(*args)

        def complete(source_future: Future[Any]) -> None:
            if future.cancelled():
                return
            try:
                future.set_result(self._records_or_response(source_future.result()))
            except Exception as exc:
                mapped = map_exception(exc)
                future.set_exception(mapped if mapped is not exc else exc)

        source.add_done_callback(complete)
        return future

    def value_put(
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
        return self.executor.execute_command(*args)

    def value_mget(
        self, refs: builtins.list[str], *, max_bytes: int | None = None
    ) -> builtins.list[Any]:
        if not refs:
            return []
        args: builtins.list[Any] = ["FLOW.VALUE.MGET", *refs]
        _append(args, "MAX_BYTES", max_bytes)
        response = self.executor.execute_command(*args)
        return [
            _normalize_ref_meta(value)
            if isinstance(value, dict)
            else value
            if value is None
            else self.codec.decode(value)
            for value in response
        ]

    def signal(
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
        return self.executor.execute_command(*args)

    def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return self.signal(id, **kwargs)

    def claim_due(
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
        args = self._claim_due_args(
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
            job_only=job_only,
            payload=payload,
            payload_max_bytes=payload_max_bytes,
            values=values,
            value_max_bytes=value_max_bytes,
            include_state=include_state,
        )
        return self._decode_claim_due_response(self.executor.execute_command(*args), job_only)

    def claim_due_future(
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
    ) -> Future[builtins.list[FlowRecord] | builtins.list[ClaimedItem]]:
        submit_command = getattr(self.executor, "submit_command", None)
        if not callable(submit_command):
            raise RuntimeError("claim_due_future requires protocol executor submit support")
        args = self._claim_due_args(
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
            job_only=job_only,
            payload=payload,
            payload_max_bytes=payload_max_bytes,
            values=values,
            value_max_bytes=value_max_bytes,
            include_state=include_state,
        )
        source = submit_command(*args)
        future: Future[builtins.list[FlowRecord] | builtins.list[ClaimedItem]] = Future()

        def complete(done: Future[Any]) -> None:
            if future.cancelled():
                return
            try:
                future.set_result(self._decode_claim_due_response(done.result(), job_only))
            except Exception as exc:
                mapped = map_exception(exc)
                if not future.cancelled():
                    future.set_exception(mapped if mapped is not exc else exc)

        source.add_done_callback(complete)
        return future

    def _claim_due_args(
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
    ) -> builtins.list[Any]:
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
        return args

    def _decode_claim_due_response(
        self,
        response: Any,
        job_only: bool,
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedItem]:
        if job_only:
            return ClaimedItem.from_compact_rows(response)
        return self._records(response)

    def claim_jobs(
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
        """Claim jobs with the optimized minimal response shape."""
        return cast(
            builtins.list[ClaimedItem],
            self.claim_due(
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

    def claim_jobs_future(
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
    ) -> Future[builtins.list[ClaimedItem]]:
        return cast(
            Future[builtins.list[ClaimedItem]],
            self.claim_due_future(
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

    def reclaim(
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
        response = self.executor.execute_command(*args)
        if job_only:
            return ClaimedItem.from_compact_rows(response)
        return self._records(response)

    def extend_lease(
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
        response = self.executor.execute_command(*args)
        return self._record_or_get(response, id, partition_key)

    def transition(
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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def step_continue(
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
        return_job: bool = False,
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
        if return_job:
            args.extend(["RETURN", "JOBS_COMPACT"])
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        response = self.executor.execute_command(*args)
        if return_job:
            return ClaimedItem.from_resp(response)  # type: ignore[return-value]
        return self._record_or_get(response, id, partition_key)

    def complete_many(
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
        return_ok_on_success: bool = False,
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
        if return_ok_on_success:
            _append(args, "RETURN", "OK_ON_SUCCESS")
        _append_named_values(
            args,
            self.codec,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
        )
        self._append_claimed_items(args, partition_key, items, "FLOW.COMPLETE_MANY")
        return self._records_or_response(self.executor.execute_command(*args))

    def complete_jobs(
        self,
        jobs: builtins.list[ClaimedItem],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = True,
        return_ok_on_success: bool = False,
    ) -> builtins.list[FlowRecord] | Any:
        """Complete claimed jobs, choosing single-partition or mixed batch wire format."""
        if not jobs:
            return []

        first_partition = jobs[0].partition_key
        partition_key = (
            first_partition
            if first_partition is not None
            and all(job.partition_key == first_partition for job in jobs)
            else None
        )
        return self.complete_many(
            partition_key,
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
            return_ok_on_success=return_ok_on_success,
        )

    def _complete_jobs_args(
        self,
        jobs: builtins.list[ClaimedItem],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = True,
    ) -> builtins.list[Any]:
        first_partition = jobs[0].partition_key
        complete_partition_key = (
            first_partition
            if first_partition is not None
            and all(job.partition_key == first_partition for job in jobs)
            else None
        )
        args: builtins.list[Any] = [
            "FLOW.COMPLETE_MANY",
            "MIXED" if complete_partition_key is None else complete_partition_key,
        ]
        _append_encoded(args, "RESULT", self.codec, result)
        _append_encoded(args, "PAYLOAD", self.codec, payload)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
        _append_bool(args, "INDEPENDENT", independent)
        _append_bool(args, "TERMINAL_LOCAL_ONLY", True)
        self._append_claimed_items(args, complete_partition_key, jobs, "FLOW.COMPLETE_MANY")
        return args

    def complete_jobs_and_claim_jobs(
        self,
        jobs: builtins.list[ClaimedItem],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = True,
        type: str,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        lease_ms: int = 30_000,
        limit: int = 100,
        priority: int | None = 0,
        claim_now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_state: bool = False,
    ) -> builtins.list[ClaimedItem]:
        """Complete claimed jobs and claim more jobs in one transport round trip."""
        if not jobs:
            return self.claim_jobs(
                type,
                state=state,
                states=states,
                worker=worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=lease_ms,
                limit=limit,
                priority=priority,
                now_ms=claim_now_ms,
                block_ms=block_ms,
                reclaim_expired=reclaim_expired,
                reclaim_ratio=reclaim_ratio,
                include_state=include_state,
            )

        complete_args = self._complete_jobs_args(
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
        )
        claim_args = self._claim_due_args(
            type,
            state=state,
            states=states,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            lease_ms=lease_ms,
            limit=limit,
            priority=priority,
            now_ms=claim_now_ms,
            block_ms=block_ms,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            job_only=True,
            include_state=include_state,
        )
        complete_response, claim_response = self._execute_command_batch(
            [tuple(complete_args), tuple(claim_args)]
        )
        self._records_or_response(complete_response)
        return cast(
            builtins.list[ClaimedItem],
            self._decode_claim_due_response(claim_response, True),
        )

    def submit_complete_jobs_and_claim_jobs(
        self,
        jobs: builtins.list[ClaimedItem],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
        independent: bool | None = True,
        type: str,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        lease_ms: int = 30_000,
        limit: int = 100,
        priority: int | None = 0,
        claim_now_ms: int | None = None,
        block_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        include_state: bool = False,
    ) -> tuple[Future[int], Future[builtins.list[ClaimedItem]]] | None:
        submit_commands = getattr(self.executor, "submit_commands", None)
        if not callable(submit_commands) or not jobs:
            return None

        complete_args = self._complete_jobs_args(
            jobs,
            result=result,
            payload=payload,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            independent=independent,
        )
        claim_args = self._claim_due_args(
            type,
            state=state,
            states=states,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            lease_ms=lease_ms,
            limit=limit,
            priority=priority,
            now_ms=claim_now_ms,
            block_ms=block_ms,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            job_only=True,
            include_state=include_state,
        )
        source_complete, source_claim = submit_commands([tuple(complete_args), tuple(claim_args)])
        complete_future: Future[int] = Future()
        claim_future: Future[builtins.list[ClaimedItem]] = Future()

        def complete_done(source: Future[Any]) -> None:
            if complete_future.cancelled():
                return
            try:
                self._records_or_response(source.result())
                complete_future.set_result(len(jobs))
            except Exception as exc:
                mapped = map_exception(exc)
                if not complete_future.cancelled():
                    complete_future.set_exception(mapped if mapped is not exc else exc)

        def claim_done(source: Future[Any]) -> None:
            if claim_future.cancelled():
                return
            try:
                claim_future.set_result(
                    cast(
                        builtins.list[ClaimedItem],
                        self._decode_claim_due_response(source.result(), True),
                    )
                )
            except Exception as exc:
                mapped = map_exception(exc)
                if not claim_future.cancelled():
                    claim_future.set_exception(mapped if mapped is not exc else exc)

        source_complete.add_done_callback(complete_done)
        source_claim.add_done_callback(claim_done)
        return complete_future, claim_future

    def _execute_command_batch(self, commands: builtins.list[tuple[Any, ...]]) -> builtins.list[Any]:
        execute_batch = getattr(self.executor, "execute_batch", None)
        if callable(execute_batch):
            try:
                return list(execute_batch(commands))
            except Exception as exc:
                mapped = map_exception(exc)
                if mapped is exc:
                    raise
                raise mapped from exc

        with self.pipeline() as pipe:
            for command in commands:
                pipe.command(*command)
            return pipe.execute()

    def complete(
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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def transition_many(
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
        return self._records_or_response(self.executor.execute_command(*args))

    def retry(
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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def retry_many(
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
        return self._records_or_response(self.executor.execute_command(*args))

    def fail(
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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def fail_many(
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
        return self._records_or_response(self.executor.execute_command(*args))

    def cancel(
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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def cancel_many(
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
        return self._records_or_response(self.executor.execute_command(*args))

    def rewind(
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
        response = self.executor.execute_command(*args)
        if not return_record:
            return _flow_return(response)
        return self._record_or_get(response, id, partition_key)

    def get(
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
        value = self.executor.execute_command(*args)
        if value is None:
            return None
        return self._record(value)

    def list(
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
        return self._records(self.executor.execute_command(*args))

    def terminals(
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
        return self._records(self.executor.execute_command(*args))

    def failures(
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
        return self._records(self.executor.execute_command(*args))

    def by_parent(self, parent_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self._index_query("FLOW.BY_PARENT", parent_flow_id, **kwargs)

    def by_root(self, root_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self._index_query("FLOW.BY_ROOT", root_flow_id, **kwargs)

    def by_correlation(self, correlation_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self._index_query("FLOW.BY_CORRELATION", correlation_id, **kwargs)

    def info(
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
        return dict(self.executor.execute_command(*args) or {})

    def stuck(
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
        return self._records(self.executor.execute_command(*args))

    def history(
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
        return list(self.executor.execute_command(*args) or [])

    def spawn_children(
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
        return self.executor.execute_command(*args)

    def install_policy(
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
        return self.executor.execute_command(*args)

    def policy_get(self, type: str, *, state: str | None = None) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.POLICY.GET", type]
        _append(args, "STATE", state)
        return dict(self.executor.execute_command(*args) or {})

    def retention_cleanup(
        self,
        *,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.RETENTION_CLEANUP"]
        _append(args, "LIMIT", limit)
        _append(args, "NOW", now_ms)
        return dict(self.executor.execute_command(*args) or {})

    def _index_query(self, command: str, key: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = [command, key]
        _append_read_options(args, **kwargs)
        return self._records(self.executor.execute_command(*args))

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

    def _record_or_get(
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
        record = self.get(id, partition_key=lookup_partition)
        if record is None:
            raise RuntimeError(f"FLOW command succeeded but record {id!r} was not found")
        return record

    def _records(self, values: builtins.list[dict[Any, Any]]) -> builtins.list[FlowRecord]:
        return [self._record(value) for value in values]

    def _records_or_response(self, value: Any) -> builtins.list[FlowRecord] | Any:
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return self._records(value)
        return value

    def _execute_producer_write(self, *args: Any) -> Any:
        attempt = 0
        while True:
            self.backpressure.before_request()
            try:
                result = self.executor.execute_command(*args)
                self.backpressure.record_success()
                return result
            except OverloadedError as exc:
                if not self.backpressure.can_retry(attempt):
                    raise
                self.backpressure.record_overload(attempt, exc.retry_after_ms)
                attempt += 1


@dataclass
class _BatchOp:
    kind: str
    key: tuple[Any, ...]
    args: dict[str, Any]
    future: Future[Any]


class AutobatchFlowClient:
    """Thread-safe auto-batching wrapper for hot Flow write commands."""

    def __init__(
        self,
        client: FlowClient,
        *,
        max_batch: int = 100,
        max_delay_ms: float = 1.0,
    ) -> None:
        self.client = client
        self.max_batch = max(1, max_batch)
        self.max_delay_s = max(0.0, max_delay_ms) / 1000.0
        self._condition = threading.Condition()
        self._pending: builtins.list[_BatchOp] = []
        self._closed = False
        self._worker = threading.Thread(
            target=self._run, name="ferricstore-flow-autobatch", daemon=True
        )
        self._worker.start()
        atexit.register(self._close_at_exit)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def close(self, timeout: float | None = 5.0) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        if self._worker.is_alive():
            self._worker.join(timeout=timeout)
        if self._worker.is_alive():
            raise TimeoutError("AutobatchFlowClient close timed out")
        with contextlib.suppress(Exception):
            atexit.unregister(self._close_at_exit)

    def _close_at_exit(self) -> None:
        with contextlib.suppress(TimeoutError):
            self.close(timeout=1.0)

    def flush(self) -> None:
        marker: Future[Any] = Future()
        self._enqueue(_BatchOp("flush", ("flush", id(marker)), {}, marker))
        marker.result()

    def create_async(
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
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> Future[FlowRecord | bytes]:
        future: Future[FlowRecord | bytes] = Future()
        if (
            return_record
            or parent_flow_id is not None
            or root_flow_id is not None
            or correlation_id is not None
        ):
            try:
                future.set_result(
                    self.client.create(
                        id,
                        type=type,
                        state=state,
                        payload=payload,
                        partition_key=partition_key,
                        parent_flow_id=parent_flow_id,
                        root_flow_id=root_flow_id,
                        correlation_id=correlation_id,
                        run_at_ms=run_at_ms,
                        now_ms=now_ms,
                        priority=priority,
                        idempotent=idempotent,
                        values=values,
                        value_refs=value_refs,
                        return_record=return_record,
                    )
                )
            except BaseException as exc:
                future.set_exception(exc)
            return future

        auto_partition = partition_key is None
        batch_partition_key = _auto_partition_key_for_id(id) if auto_partition else partition_key
        batch_key = (
            (
                "create-auto",
                type,
                state,
                run_at_ms,
                now_ms,
                priority,
                idempotent,
                batch_partition_key,
            )
            if auto_partition
            else ("create", type, state, run_at_ms, now_ms, priority, idempotent)
        )
        self._enqueue(
            _BatchOp(
                "create",
                batch_key,
                {
                    "id": id,
                    "type": type,
                    "state": state,
                    "payload": payload,
                    "partition_key": batch_partition_key,
                    "run_at_ms": run_at_ms,
                    "now_ms": now_ms,
                    "priority": priority,
                    "idempotent": idempotent,
                    "values": values,
                    "value_refs": value_refs,
                },
                future,
            )
        )
        return future

    def create(
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
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        return_record: bool = False,
    ) -> FlowRecord | bytes:
        return self.create_async(
            id,
            type=type,
            state=state,
            payload=payload,
            partition_key=partition_key,
            parent_flow_id=parent_flow_id,
            root_flow_id=root_flow_id,
            correlation_id=correlation_id,
            run_at_ms=run_at_ms,
            now_ms=now_ms,
            priority=priority,
            idempotent=idempotent,
            values=values,
            value_refs=value_refs,
            return_record=return_record,
        ).result()

    def complete_async(
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
    ) -> Future[FlowRecord | bytes]:
        future: Future[FlowRecord | bytes] = Future()
        if return_record or partition_key is None:
            try:
                future.set_result(
                    self.client.complete(
                        id,
                        lease_token=lease_token,
                        fencing_token=fencing_token,
                        partition_key=partition_key,
                        result=result,
                        payload=payload,
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                        ttl_ms=ttl_ms,
                        now_ms=now_ms,
                        return_record=return_record,
                    )
                )
            except BaseException as exc:
                future.set_exception(exc)
            return future

        self._enqueue(
            _BatchOp(
                "complete",
                (
                    "complete",
                    _batch_key_value(result),
                    _batch_key_value(payload),
                    ttl_ms,
                    now_ms,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "result": result,
                    "payload": payload,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "ttl_ms": ttl_ms,
                    "now_ms": now_ms,
                },
                future,
            )
        )
        return future

    def complete(
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
        return self.complete_async(
            id,
            lease_token=lease_token,
            fencing_token=fencing_token,
            partition_key=partition_key,
            result=result,
            payload=payload,
            values=values,
            value_refs=value_refs,
            drop_values=drop_values,
            override_values=override_values,
            ttl_ms=ttl_ms,
            now_ms=now_ms,
            return_record=return_record,
        ).result()

    def transition(
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
        if return_record or partition_key is None:
            return self.client.transition(
                id,
                from_state=from_state,
                to_state=to_state,
                lease_token=lease_token,
                fencing_token=fencing_token,
                partition_key=partition_key,
                payload=payload,
                values=values,
                value_refs=value_refs,
                drop_values=drop_values,
                override_values=override_values,
                run_at_ms=run_at_ms,
                now_ms=now_ms,
                priority=priority,
                return_record=return_record,
            )

        future: Future[Any] = Future()
        self._enqueue(
            _BatchOp(
                "transition",
                (
                    "transition",
                    from_state,
                    to_state,
                    _batch_key_value(payload),
                    run_at_ms,
                    now_ms,
                    priority,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "from_state": from_state,
                    "to_state": to_state,
                    "payload": payload,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "run_at_ms": run_at_ms,
                    "now_ms": now_ms,
                    "priority": priority,
                },
                future,
            )
        )
        return _flow_return(future.result())

    def retry(
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
        if return_record or partition_key is None:
            return self.client.retry(
                id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                partition_key=partition_key,
                error=error,
                payload=payload,
                values=values,
                value_refs=value_refs,
                drop_values=drop_values,
                override_values=override_values,
                run_at_ms=run_at_ms,
                now_ms=now_ms,
                return_record=return_record,
            )

        future: Future[Any] = Future()
        self._enqueue(
            _BatchOp(
                "retry",
                (
                    "retry",
                    _batch_key_value(error),
                    _batch_key_value(payload),
                    run_at_ms,
                    now_ms,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "error": error,
                    "payload": payload,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "run_at_ms": run_at_ms,
                    "now_ms": now_ms,
                },
                future,
            )
        )
        return _flow_return(future.result())

    def fail(
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
        if return_record or partition_key is None:
            return self.client.fail(
                id,
                lease_token=lease_token,
                fencing_token=fencing_token,
                partition_key=partition_key,
                error=error,
                payload=payload,
                values=values,
                value_refs=value_refs,
                drop_values=drop_values,
                override_values=override_values,
                ttl_ms=ttl_ms,
                now_ms=now_ms,
                return_record=return_record,
            )

        future: Future[Any] = Future()
        self._enqueue(
            _BatchOp(
                "fail",
                (
                    "fail",
                    _batch_key_value(error),
                    _batch_key_value(payload),
                    ttl_ms,
                    now_ms,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "error": error,
                    "payload": payload,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "ttl_ms": ttl_ms,
                    "now_ms": now_ms,
                },
                future,
            )
        )
        return _flow_return(future.result())

    def cancel(
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
        if return_record or partition_key is None:
            return self.client.cancel(
                id,
                fencing_token=fencing_token,
                lease_token=lease_token,
                partition_key=partition_key,
                reason=reason,
                values=values,
                value_refs=value_refs,
                drop_values=drop_values,
                override_values=override_values,
                ttl_ms=ttl_ms,
                now_ms=now_ms,
                return_record=return_record,
            )

        future: Future[Any] = Future()
        self._enqueue(
            _BatchOp(
                "cancel",
                (
                    "cancel",
                    _batch_key_value(reason),
                    ttl_ms,
                    now_ms,
                    _batch_named_key(
                        values=values,
                        value_refs=value_refs,
                        drop_values=drop_values,
                        override_values=override_values,
                    ),
                ),
                {
                    "id": id,
                    "lease_token": lease_token,
                    "fencing_token": fencing_token,
                    "partition_key": partition_key,
                    "reason": reason,
                    "values": values,
                    "value_refs": value_refs,
                    "drop_values": drop_values,
                    "override_values": override_values,
                    "ttl_ms": ttl_ms,
                    "now_ms": now_ms,
                },
                future,
            )
        )
        return _flow_return(future.result())

    def _enqueue(self, op: _BatchOp) -> None:
        with self._condition:
            if self._closed:
                raise RuntimeError("autobatch client is closed")
            self._pending.append(op)
            self._condition.notify()

    def _run(self) -> None:
        while True:
            ops = self._take_batch()
            if not ops:
                return
            self._flush_ops(ops)

    def _take_batch(self) -> builtins.list[_BatchOp]:
        with self._condition:
            while not self._pending and not self._closed:
                self._condition.wait()
            if not self._pending and self._closed:
                return []

            deadline = time.monotonic() + self.max_delay_s
            while len(self._pending) < self.max_batch and not self._closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)

            ops = self._pending
            self._pending = []
            return ops

    def _flush_ops(self, ops: builtins.list[_BatchOp]) -> None:
        groups: dict[tuple[Any, ...], builtins.list[_BatchOp]] = {}
        for op in ops:
            groups.setdefault(op.key, []).append(op)
        for group in groups.values():
            if group[0].kind == "flush":
                for op in group:
                    op.future.set_result(None)
                continue
            self._flush_group(group)

    def _flush_group(self, group: builtins.list[_BatchOp]) -> None:
        kind = group[0].kind
        try:
            if kind == "create":
                partition_keys = {op.args["partition_key"] for op in group}
                partition_key = next(iter(partition_keys)) if len(partition_keys) == 1 else None
                response = self.client.create_many(
                    partition_key,
                    [
                        CreateItem(
                            op.args["id"],
                            op.args["payload"],
                            partition_key=None
                            if partition_key is not None
                            else op.args["partition_key"],
                            values=op.args.get("values"),
                            value_refs=op.args.get("value_refs"),
                        )
                        for op in group
                    ],
                    type=group[0].args["type"],
                    state=group[0].args["state"],
                    run_at_ms=group[0].args["run_at_ms"],
                    now_ms=group[0].args["now_ms"],
                    priority=group[0].args["priority"],
                    idempotent=group[0].args["idempotent"],
                    independent=True,
                )
            elif kind == "complete":
                response = self.client.complete_many(
                    None,
                    [self._claimed_item(op) for op in group],
                    result=group[0].args["result"],
                    payload=group[0].args["payload"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    ttl_ms=group[0].args["ttl_ms"],
                    now_ms=group[0].args["now_ms"],
                    independent=True,
                )
            elif kind == "transition":
                response = self.client.transition_many(
                    None,
                    from_state=group[0].args["from_state"],
                    to_state=group[0].args["to_state"],
                    items=[self._fenced_item(op, include_lease=True) for op in group],
                    payload=group[0].args["payload"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    run_at_ms=group[0].args["run_at_ms"],
                    now_ms=group[0].args["now_ms"],
                    priority=group[0].args["priority"],
                    independent=True,
                )
            elif kind == "retry":
                response = self.client.retry_many(
                    None,
                    [self._claimed_item(op) for op in group],
                    error=group[0].args["error"],
                    payload=group[0].args["payload"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    run_at_ms=group[0].args["run_at_ms"],
                    now_ms=group[0].args["now_ms"],
                    independent=True,
                )
            elif kind == "fail":
                response = self.client.fail_many(
                    None,
                    [self._claimed_item(op) for op in group],
                    error=group[0].args["error"],
                    payload=group[0].args["payload"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    ttl_ms=group[0].args["ttl_ms"],
                    now_ms=group[0].args["now_ms"],
                    independent=True,
                )
            elif kind == "cancel":
                response = self.client.cancel_many(
                    None,
                    [self._fenced_item(op, include_lease=False) for op in group],
                    reason=group[0].args["reason"],
                    values=group[0].args.get("values"),
                    value_refs=group[0].args.get("value_refs"),
                    drop_values=group[0].args.get("drop_values"),
                    override_values=group[0].args.get("override_values"),
                    ttl_ms=group[0].args["ttl_ms"],
                    now_ms=group[0].args["now_ms"],
                    independent=True,
                )
            else:
                raise RuntimeError(f"unknown batch op {kind!r}")
        except BaseException as exc:
            for op in group:
                op.future.set_exception(exc)
            return

        self._complete_group(group, response)

    def _complete_group(self, group: builtins.list[_BatchOp], response: Any) -> None:
        if isinstance(response, list) and len(response) == len(group):
            for op, item in zip(group, response, strict=False):
                op.future.set_result(item)
            return
        for op in group:
            op.future.set_result(response)

    def _claimed_item(self, op: _BatchOp) -> ClaimedItem:
        return ClaimedItem(
            op.args["id"],
            op.args["lease_token"],
            op.args["fencing_token"],
            partition_key=op.args["partition_key"],
        )

    def _fenced_item(self, op: _BatchOp, *, include_lease: bool) -> FencedItem:
        return FencedItem(
            op.args["id"],
            op.args["fencing_token"],
            op.args["lease_token"] if include_lease else None,
            partition_key=op.args["partition_key"],
        )
