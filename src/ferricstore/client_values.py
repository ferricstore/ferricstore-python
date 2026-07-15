from __future__ import annotations

import builtins
from typing import Any

from ferricstore.batch_core import require_batch_items
from ferricstore.client_helpers import (
    _append,
    _append_bool,
    _append_named_values,
    _append_priority,
    _now_ms,
)
from ferricstore.client_state import _ClientMixinBase
from ferricstore.model_core import _normalize_ref_meta


class _ClientValuesMixin(_ClientMixinBase):
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
        items = require_batch_items(response, len(refs), operation="FLOW.VALUE.MGET")
        return [
            _normalize_ref_meta(value)
            if isinstance(value, dict)
            else value
            if value is None
            else self.codec.decode(value)
            for value in items
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
        _append_priority(args, priority)
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
