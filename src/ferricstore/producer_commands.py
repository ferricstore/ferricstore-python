from __future__ import annotations

import builtins
from typing import Any

from ferricstore.client_helpers import (
    _append,
    _append_attributes,
    _append_bool,
    _append_named_counts,
    _append_named_values,
    _append_priority,
    _append_state_meta,
    _has_named_item_values,
    _merge_named_map,
    _now_ms,
    _shared_create_many_attributes,
    _shared_create_many_state_meta,
)
from ferricstore.codecs import Codec
from ferricstore.config_validation import normalize_optional_max_active_ms
from ferricstore.types import CreateItem


def _create_many_args(
    codec: Codec,
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
    max_active_ms: int | float | str | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    attributes: dict[str, Any] | None = None,
    state_meta: dict[str, Any] | None = None,
) -> builtins.list[Any]:
    """Build the authoritative sync/async ``FLOW.CREATE_MANY`` command."""
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
    _append_priority(args, priority)
    _append_bool(args, "IDEMPOTENT", idempotent)
    _append_bool(args, "INDEPENDENT", independent)
    if return_ok_on_success:
        _append(args, "RETURN", "OK_ON_SUCCESS")
    _append(args, "RETENTION_TTL_MS", retention_ttl_ms)
    _append(args, "MAX_ACTIVE_MS", normalize_optional_max_active_ms(max_active_ms))
    _append_attributes(args, attributes=attributes)
    _append_state_meta(args, state_meta)
    has_item_max_active = any(item.max_active_ms is not None for item in items)
    extended_items = (
        _has_named_item_values(items)
        or has_item_max_active
        or (mixed and any(item.partition_key is None for item in items))
    )

    if extended_items:
        args.extend(["ITEMS_EXT_V2" if has_item_max_active else "ITEMS_EXT", len(items)])
        for item in items:
            item_partition = item.partition_key if mixed else None
            item_values = _merge_named_map(values, item.values)
            item_refs = _merge_named_map(value_refs, item.value_refs)
            args.extend(
                [
                    item.id,
                    item_partition if item_partition is not None else "-",
                    codec.encode(item.payload),
                ]
            )
            if has_item_max_active:
                item_max_active = normalize_optional_max_active_ms(item.max_active_ms)
                args.append(item_max_active if item_max_active is not None else "-")
            _append_named_counts(args, codec, item_values, item_refs)
    else:
        _append_named_values(args, codec, values=values, value_refs=value_refs)
        args.append("ITEMS")
        for item in items:
            if mixed:
                if item.partition_key is None:
                    raise ValueError("mixed create_many items require partition_key")
                args.extend([item.id, item.partition_key, codec.encode(item.payload)])
            else:
                args.extend([item.id, codec.encode(item.payload)])
    return args


__all__ = ["_create_many_args"]
