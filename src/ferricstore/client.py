from __future__ import annotations

import time
from typing import Any

from ferricstore.adapters import RedisAdapter, RedisCommandExecutor
from ferricstore.codecs import Codec, RawCodec
from ferricstore.types import ChildSpec, FlowRecord, RetryPolicy


def _now_ms() -> int:
    return int(time.time() * 1000)


def _append(args: list[Any], name: str, value: Any) -> None:
    if value is not None:
        args.extend([name, value])


def _append_bool(args: list[Any], name: str, value: bool | None) -> None:
    if value is not None:
        args.extend([name, "true" if value else "false"])


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

    def get(self, id: str, *, partition_key: str | None = None) -> FlowRecord | None:
        args: list[Any] = ["FLOW.GET", id]
        _append(args, "PARTITION", partition_key)
        value = self.executor.execute_command(*args)
        if value is None:
            return None
        return self._record(value)

    def history(self, id: str, *, partition_key: str | None = None, count: int = 100) -> list[Any]:
        args: list[Any] = ["FLOW.HISTORY", id, "COUNT", count]
        _append(args, "PARTITION", partition_key)
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
            if child.partition_key is None:
                args.extend([child.id, child.type, child.payload])
            else:
                args.extend([child.id, child.partition_key, child.type, child.payload])
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
