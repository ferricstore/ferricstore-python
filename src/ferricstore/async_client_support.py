from __future__ import annotations

import builtins
import time
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.client_helpers import (
    _append_read_options,
    _auto_partition_key_for_id,
    _split_flow_state_policy,
)
from ferricstore.errors import OverloadedError
from ferricstore.retry_policy import RetryPolicy
from ferricstore.types import (
    ClaimedFlow,
    FencedItem,
    FlowRecord,
    FlowStatePolicyLike,
)


class _AsyncClientSupportMixin(_AsyncClientMixinBase):
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
        return cast(FlowRecord, record)

    def _records(self, values: builtins.list[dict[Any, Any]]) -> builtins.list[FlowRecord]:
        return [self._record(value) for value in values]

    def _records_or_response(self, value: Any) -> builtins.list[FlowRecord] | Any:
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return self._records(value)
        return value

    async def _execute_producer_write(self, *args: Any) -> Any:
        attempt = 0
        started = time.monotonic()
        while True:
            elapsed_s = time.monotonic() - started
            if not await self.backpressure.before_request_async(elapsed_s=elapsed_s):
                raise OverloadedError("client backpressure wait exceeds max_elapsed_ms")
            try:
                result = await self.executor.execute_command(*args)
                self.backpressure.record_success()
                return result
            except OverloadedError as exc:
                elapsed_s = time.monotonic() - started
                if not self.backpressure.can_retry(
                    attempt,
                    elapsed_s=elapsed_s,
                ):
                    raise
                if not await self.backpressure.record_overload_async(
                    attempt,
                    exc.retry_after_ms,
                    elapsed_s=elapsed_s,
                ):
                    raise
                attempt += 1
