from __future__ import annotations

import time
from typing import Any

from ferricstore.adapters import RedisAdapter, RedisCommandExecutor
from ferricstore.codecs import Codec, RawCodec
from ferricstore.types import ChildSpec, ClaimedItem, CreateItem, FencedItem, FlowRecord, RetryPolicy


def _now_ms() -> int:
    return int(time.time() * 1000)


def _append(args: list[Any], name: str, value: Any) -> None:
    if value is not None:
        args.extend([name, value])


def _append_bool(args: list[Any], name: str, value: bool | None) -> None:
    if value is not None:
        args.extend([name, "true" if value else "false"])


def _append_read_options(
    args: list[Any],
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


class FlowClient:
    """FerricFlow client over Redis/FerricStore commands."""

    def __init__(self, executor: RedisCommandExecutor, codec: Codec | None = None) -> None:
        self.executor = executor
        self.codec = codec or RawCodec()

    @classmethod
    def from_url(cls, url: str, *, codec: Codec | None = None, **kwargs: Any) -> FlowClient:
        return cls(RedisAdapter.from_url(url, **kwargs), codec=codec)

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
    ) -> FlowRecord:
        now_ms = now_ms or _now_ms()
        args: list[Any] = ["FLOW.CREATE", id, "TYPE", type, "STATE", state, "NOW", now_ms]
        _append(args, "PARTITION", partition_key)
        _append(args, "PAYLOAD", self.codec.encode(payload))
        _append(args, "PARENT_FLOW_ID", parent_flow_id)
        _append(args, "ROOT_FLOW_ID", root_flow_id)
        _append(args, "CORRELATION_ID", correlation_id)
        _append(args, "RUN_AT", run_at_ms if run_at_ms is not None else now_ms)
        _append(args, "PRIORITY", priority)
        _append_bool(args, "IDEMPOTENT", idempotent)
        return self._record(self.executor.execute_command(*args))

    def create_many(
        self,
        partition_key: str | None,
        items: list[CreateItem],
        *,
        type: str,
        state: str = "queued",
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
        idempotent: bool | None = None,
    ) -> list[FlowRecord]:
        now_ms = now_ms or _now_ms()
        mixed = partition_key is None
        wire_partition = "MIXED" if mixed else partition_key
        args: list[Any] = [
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
        args.append("ITEMS")
        for item in items:
            if mixed:
                if item.partition_key is None:
                    raise ValueError("mixed create_many items require partition_key")
                args.extend([item.id, item.partition_key, self.codec.encode(item.payload)])
            else:
                args.extend([item.id, self.codec.encode(item.payload)])
        return self._records(self.executor.execute_command(*args))

    def value_put(
        self,
        value: Any,
        *,
        partition_key: str | None = None,
        owner_flow_id: str | None = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> Any:
        args: list[Any] = ["FLOW.VALUE.PUT", self.codec.encode(value), "NOW", now_ms or _now_ms()]
        _append(args, "PARTITION", partition_key)
        _append(args, "OWNER_FLOW_ID", owner_flow_id)
        _append(args, "TTL", ttl_ms)
        return self.executor.execute_command(*args)

    def claim_due(
        self,
        type: str,
        *,
        state: str = "queued",
        worker: str,
        partition_key: str | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
        now_ms: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
    ) -> list[FlowRecord]:
        args: list[Any] = [
            "FLOW.CLAIM_DUE",
            type,
            "STATE",
            state,
            "WORKER",
            worker,
            "LEASE_MS",
            lease_ms,
            "LIMIT",
            limit,
            "NOW",
            now_ms or _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append_bool(args, "RECLAIM_EXPIRED", reclaim_expired)
        _append(args, "RECLAIM_RATIO", reclaim_ratio)
        return self._records(self.executor.execute_command(*args))

    def reclaim(
        self,
        type: str,
        *,
        state: str = "running",
        worker: str,
        partition_key: str | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
        now_ms: int | None = None,
    ) -> list[FlowRecord]:
        args: list[Any] = [
            "FLOW.RECLAIM",
            type,
            "STATE",
            state,
            "WORKER",
            worker,
            "LEASE_MS",
            lease_ms,
            "LIMIT",
            limit,
            "NOW",
            now_ms or _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        return self._records(self.executor.execute_command(*args))

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
        args: list[Any] = [
            "FLOW.EXTEND_LEASE",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "LEASE_MS",
            lease_ms,
            "NOW",
            now_ms or _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        return self._record(self.executor.execute_command(*args))

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
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
    ) -> FlowRecord:
        now_ms = now_ms or _now_ms()
        args: list[Any] = [
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
        _append(args, "PAYLOAD", self.codec.encode(payload))
        _append(args, "RUN_AT", run_at_ms if run_at_ms is not None else now_ms)
        _append(args, "PRIORITY", priority)
        return self._record(self.executor.execute_command(*args))

    def complete_many(
        self,
        partition_key: str | None,
        items: list[ClaimedItem],
        *,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> list[FlowRecord]:
        args: list[Any] = ["FLOW.COMPLETE_MANY", "MIXED" if partition_key is None else partition_key]
        _append(args, "RESULT", self.codec.encode(result) if result is not None else None)
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms or _now_ms())
        self._append_claimed_items(args, partition_key, items, "FLOW.COMPLETE_MANY")
        return self._records(self.executor.execute_command(*args))

    def complete(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        result: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> FlowRecord:
        args: list[Any] = [
            "FLOW.COMPLETE",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms or _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "RESULT", self.codec.encode(result))
        _append(args, "PAYLOAD", self.codec.encode(payload))
        _append(args, "TTL", ttl_ms)
        return self._record(self.executor.execute_command(*args))

    def transition_many(
        self,
        partition_key: str | None,
        *,
        from_state: str,
        to_state: str,
        items: list[FencedItem],
        payload: Any = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
        priority: int | None = None,
    ) -> list[FlowRecord]:
        mixed = partition_key is None
        wire_partition = "MIXED" if mixed else partition_key
        args: list[Any] = ["FLOW.TRANSITION_MANY", wire_partition, from_state, to_state]
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "PRIORITY", priority)
        _append(args, "NOW", now_ms or _now_ms())
        self._append_fenced_items(
            args,
            partition_key,
            items,
            "FLOW.TRANSITION_MANY",
            include_lease=True,
        )
        return self._records(self.executor.execute_command(*args))

    def retry(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        error: Any = None,
        payload: Any = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
    ) -> FlowRecord:
        args: list[Any] = [
            "FLOW.RETRY",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms or _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "ERROR", self.codec.encode(error))
        _append(args, "PAYLOAD", self.codec.encode(payload))
        _append(args, "RUN_AT", run_at_ms)
        return self._record(self.executor.execute_command(*args))

    def retry_many(
        self,
        partition_key: str | None,
        items: list[ClaimedItem],
        *,
        error: Any = None,
        payload: Any = None,
        run_at_ms: int | None = None,
        now_ms: int | None = None,
    ) -> list[FlowRecord]:
        args: list[Any] = ["FLOW.RETRY_MANY", "MIXED" if partition_key is None else partition_key]
        _append(args, "ERROR", self.codec.encode(error) if error is not None else None)
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "NOW", now_ms or _now_ms())
        self._append_claimed_items(args, partition_key, items, "FLOW.RETRY_MANY")
        return self._records(self.executor.execute_command(*args))

    def fail(
        self,
        id: str,
        *,
        lease_token: bytes,
        fencing_token: int,
        partition_key: str | None = None,
        error: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> FlowRecord:
        args: list[Any] = [
            "FLOW.FAIL",
            id,
            lease_token,
            "FENCING",
            fencing_token,
            "NOW",
            now_ms or _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "ERROR", self.codec.encode(error))
        _append(args, "PAYLOAD", self.codec.encode(payload))
        _append(args, "TTL", ttl_ms)
        return self._record(self.executor.execute_command(*args))

    def fail_many(
        self,
        partition_key: str | None,
        items: list[ClaimedItem],
        *,
        error: Any = None,
        payload: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> list[FlowRecord]:
        args: list[Any] = ["FLOW.FAIL_MANY", "MIXED" if partition_key is None else partition_key]
        _append(args, "ERROR", self.codec.encode(error) if error is not None else None)
        _append(args, "PAYLOAD", self.codec.encode(payload) if payload is not None else None)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms or _now_ms())
        self._append_claimed_items(args, partition_key, items, "FLOW.FAIL_MANY")
        return self._records(self.executor.execute_command(*args))

    def cancel(
        self,
        id: str,
        *,
        fencing_token: int,
        lease_token: bytes | None = None,
        partition_key: str | None = None,
        reason: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> FlowRecord:
        args: list[Any] = ["FLOW.CANCEL", id, "FENCING", fencing_token, "NOW", now_ms or _now_ms()]
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "PARTITION", partition_key)
        _append(args, "REASON", self.codec.encode(reason) if reason is not None else None)
        _append(args, "TTL", ttl_ms)
        return self._record(self.executor.execute_command(*args))

    def cancel_many(
        self,
        partition_key: str | None,
        items: list[FencedItem],
        *,
        reason: Any = None,
        ttl_ms: int | None = None,
        now_ms: int | None = None,
    ) -> list[FlowRecord]:
        args: list[Any] = ["FLOW.CANCEL_MANY", "MIXED" if partition_key is None else partition_key]
        _append(args, "REASON", self.codec.encode(reason) if reason is not None else None)
        _append(args, "TTL", ttl_ms)
        _append(args, "NOW", now_ms or _now_ms())
        self._append_fenced_items(args, partition_key, items, "FLOW.CANCEL_MANY")
        return self._records(self.executor.execute_command(*args))

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
    ) -> FlowRecord:
        args: list[Any] = ["FLOW.REWIND", id, "TO_EVENT", to_event, "NOW", now_ms or _now_ms()]
        _append(args, "PARTITION", partition_key)
        _append(args, "EXPECT_STATE", expect_state)
        _append(args, "RUN_AT", run_at_ms)
        _append(args, "REASON_REF", reason_ref)
        return self._record(self.executor.execute_command(*args))

    def get(self, id: str, *, partition_key: str | None = None) -> FlowRecord | None:
        args: list[Any] = ["FLOW.GET", id]
        _append(args, "PARTITION", partition_key)
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
    ) -> list[FlowRecord]:
        args: list[Any] = ["FLOW.LIST", type]
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
    ) -> list[FlowRecord]:
        args: list[Any] = ["FLOW.TERMINALS", type]
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
    ) -> list[FlowRecord]:
        args: list[Any] = ["FLOW.FAILURES", type]
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

    def by_parent(self, parent_flow_id: str, **kwargs: Any) -> list[FlowRecord]:
        return self._index_query("FLOW.BY_PARENT", parent_flow_id, **kwargs)

    def by_root(self, root_flow_id: str, **kwargs: Any) -> list[FlowRecord]:
        return self._index_query("FLOW.BY_ROOT", root_flow_id, **kwargs)

    def by_correlation(self, correlation_id: str, **kwargs: Any) -> list[FlowRecord]:
        return self._index_query("FLOW.BY_CORRELATION", correlation_id, **kwargs)

    def info(
        self,
        type: str,
        *,
        partition_key: str | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> dict[Any, Any]:
        args: list[Any] = ["FLOW.INFO", type]
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
    ) -> list[FlowRecord]:
        args: list[Any] = ["FLOW.STUCK", type]
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
    ) -> list[Any]:
        args: list[Any] = ["FLOW.HISTORY", id, "COUNT", count]
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
        children: list[ChildSpec],
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        group_id: str = "default",
        wait: str = "all",
        wait_state: str | None = None,
        success: str | None = None,
        failure: str | None = None,
        now_ms: int | None = None,
    ) -> Any:
        args: list[Any] = [
            "FLOW.SPAWN_CHILDREN",
            parent_id,
            "GROUP",
            group_id,
            "WAIT",
            wait,
            "NOW",
            now_ms or _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "WAIT_STATE", wait_state)
        _append(args, "SUCCESS", success)
        _append(args, "FAILURE", failure)
        args.append("ITEMS")
        mixed = any(child.partition_key is not None for child in children)
        if mixed:
            args.append("MIXED")
        for child in children:
            if mixed:
                if child.partition_key is None:
                    raise ValueError("mixed spawn_children items require partition_key")
                args.extend([child.id, child.partition_key, child.type, child.payload])
            else:
                args.extend([child.id, child.type, child.payload])
        return self.executor.execute_command(*args)

    def install_policy(
        self,
        type: str,
        *,
        retry: RetryPolicy | None = None,
        states: dict[str, RetryPolicy] | None = None,
    ) -> Any:
        args: list[Any] = ["FLOW.POLICY.SET", type]
        if retry is not None:
            self._append_retry_policy(args, retry)
        for state, policy in (states or {}).items():
            args.extend(["STATE", state])
            self._append_retry_policy(args, policy)
        return self.executor.execute_command(*args)

    def policy_get(self, type: str, *, state: str | None = None) -> dict[Any, Any]:
        args: list[Any] = ["FLOW.POLICY.GET", type]
        _append(args, "STATE", state)
        return dict(self.executor.execute_command(*args) or {})

    def retention_cleanup(
        self,
        *,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> dict[Any, Any]:
        args: list[Any] = ["FLOW.RETENTION_CLEANUP"]
        _append(args, "LIMIT", limit)
        _append(args, "NOW", now_ms)
        return dict(self.executor.execute_command(*args) or {})

    def _index_query(self, command: str, key: str, **kwargs: Any) -> list[FlowRecord]:
        args: list[Any] = [command, key]
        _append_read_options(args, **kwargs)
        return self._records(self.executor.execute_command(*args))

    def _append_claimed_items(
        self,
        args: list[Any],
        partition_key: str | None,
        items: list[ClaimedItem],
        command: str,
    ) -> list[Any]:
        mixed = partition_key is None
        args.append("ITEMS")
        for item in items:
            if mixed:
                if item.partition_key is None:
                    raise ValueError(f"mixed {command} items require partition_key")
                args.extend([item.id, item.partition_key, item.lease_token, item.fencing_token])
            else:
                args.extend([item.id, item.lease_token, item.fencing_token])
        return args

    def _append_fenced_items(
        self,
        args: list[Any],
        partition_key: str | None,
        items: list[FencedItem],
        command: str,
        *,
        include_lease: bool = False,
    ) -> list[Any]:
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
                args.extend([item.id, item.fencing_token])
                if include_lease:
                    args.append(lease)
        return args

    def _append_retry_policy(self, args: list[Any], policy: RetryPolicy) -> None:
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
        return FlowRecord.from_resp(value, payload=self.codec.decode(raw_payload))

    def _records(self, values: list[dict[Any, Any]]) -> list[FlowRecord]:
        return [self._record(value) for value in values]
