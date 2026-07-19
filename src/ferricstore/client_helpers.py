from __future__ import annotations

import builtins
import json
import time
from collections.abc import Mapping, Sequence
from typing import Any, cast

from ferricstore.batch_core import (
    BatchValueMatcher,
    queued_batch_fingerprint,
)
from ferricstore.codecs import Codec
from ferricstore.config_validation import (
    validate_nonnegative_int,
    validate_optional_bool,
    validate_optional_flow_priority,
    validate_optional_nonnegative_int,
    validate_positive_int,
    validate_string_sequence,
)
from ferricstore.model_core import _normalize_ref_meta
from ferricstore.mutation_core import JobMutation, MutationKind
from ferricstore.retry_policy import RetryPolicy
from ferricstore.types import (
    ClaimedFlow,
    CreateItem,
    FencedItem,
    FlowRecord,
    FlowStatePolicy,
    FlowStatePolicyLike,
    normalize_flow_state_mode,
)
from ferricstore.worker_core import expand_many_result

_FLOW_MANY_BATCH_LIMIT = 1_000


def _validate_fencing_token(fencing_token: object) -> None:
    validate_nonnegative_int(fencing_token, name="fencing_token")


def _validate_lease_token(lease_token: object) -> None:
    if not isinstance(lease_token, bytes) or not lease_token:
        raise ValueError("lease_token must be non-empty bytes")


def _validate_ownership_token(ownership_token: object) -> None:
    if not isinstance(ownership_token, bytes) or not ownership_token:
        raise ValueError("ownership_token must be non-empty bytes")


def _validate_claim_guard(lease_token: object, fencing_token: object) -> None:
    _validate_lease_token(lease_token)
    _validate_fencing_token(fencing_token)


def _flow_return(value: Any) -> FlowRecord | bytes:
    return cast(FlowRecord | bytes, value)


def _split_flow_state_policy(policy: FlowStatePolicyLike) -> tuple[str | None, RetryPolicy | None]:
    if isinstance(policy, FlowStatePolicy):
        return normalize_flow_state_mode(policy.mode), policy.retry
    if isinstance(policy, RetryPolicy):
        return None, policy
    raise TypeError("state policies must be RetryPolicy or FlowStatePolicy")


def _json_arg(value: Mapping[str, Any] | str) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _command_with_request_context(
    command: str,
    args: Sequence[Any],
    request_context: Mapping[str, Any] | None,
) -> builtins.list[Any]:
    command_args = [command, *args]
    if request_context is not None:
        command_args.extend(["REQUEST_CONTEXT", dict(request_context)])
    return command_args


def _invocation_definition_put_args(definition: Mapping[str, Any] | str) -> builtins.list[Any]:
    return [_json_arg(definition)]


def _invocation_create_args(
    name: str,
    attrs: Mapping[str, Any],
    *,
    context: Mapping[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> builtins.list[Any]:
    envelope: dict[str, Any] = {"attrs": dict(attrs)}
    if context is not None:
        envelope["context"] = dict(context)
    if idempotency_key is not None:
        envelope["idempotency_key"] = idempotency_key
    return [name, _json_arg(envelope)]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _append(args: builtins.list[Any], name: str, value: Any) -> None:
    if value is not None:
        args.extend([name, value])


def _append_priority(args: builtins.list[Any], priority: int | None) -> None:
    priority = validate_optional_flow_priority(priority)
    if priority is not None:
        args.extend(["PRIORITY", priority])


def _append_bool(args: builtins.list[Any], name: str, value: bool | None) -> None:
    value = validate_optional_bool(value, name=name)
    if value is not None:
        args.extend([name, "true" if value else "false"])


def _append_payload_read(
    args: builtins.list[Any], payload: bool | None, max_bytes: int | None
) -> None:
    payload = validate_optional_bool(payload, name="payload")
    max_bytes = validate_optional_nonnegative_int(max_bytes, name="payload_max_bytes")
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


def _append_attributes(
    args: builtins.list[Any],
    *,
    attributes: dict[str, Any] | None = None,
    attributes_merge: dict[str, Any] | None = None,
    attributes_delete: builtins.list[str] | None = None,
) -> None:
    if not attributes and not attributes_merge and not attributes_delete:
        return

    for name, value in (attributes or {}).items():
        args.extend(["ATTRIBUTE", name, value])
    for name, value in (attributes_merge or {}).items():
        args.extend(["ATTRIBUTE_MERGE", name, value])
    for name in attributes_delete or []:
        args.extend(["ATTRIBUTE_DELETE", name])


def _append_state_meta(
    args: builtins.list[Any],
    state_meta: dict[str, Any] | None,
) -> None:
    for name, value in (state_meta or {}).items():
        args.extend(["STATE_META", name, value])


def _append_search_state_meta(
    args: builtins.list[Any],
    state: str | None,
    state_meta: dict[str, Any] | None,
) -> None:
    if not state_meta:
        return

    if all(isinstance(value, Mapping) for value in state_meta.values()):
        for meta_state, values in state_meta.items():
            args.extend(["STATE_META", meta_state, dict(values)])
        return

    if state is None:
        raise ValueError("search state_meta filters require state=... or nested {state: {...}}")

    args.extend(["STATE_META", state, dict(state_meta)])


def _merge_named_map(base: dict[str, Any] | None, item: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    if base:
        merged.update(base)
    if item:
        merged.update(item)
    return merged


def _has_named_item_values(items: builtins.list[Any]) -> bool:
    return any(getattr(item, "values", None) or getattr(item, "value_refs", None) for item in items)


def _shared_create_many_attributes(
    items: builtins.list[CreateItem],
    attributes: dict[str, Any] | None,
) -> dict[str, Any] | None:
    all_item_attrs = [item.attributes or None for item in items]
    populated_attrs = [item_attrs for item_attrs in all_item_attrs if item_attrs is not None]
    if attributes is not None:
        matcher = BatchValueMatcher(attributes)
        if any(not matcher.matches(item_attrs) for item_attrs in populated_attrs):
            raise ValueError(
                "create_many item attributes must match shared attributes when both are provided"
            )
        return attributes
    if not populated_attrs:
        return None

    first = populated_attrs[0]
    matcher = BatchValueMatcher(first)
    if any(not matcher.matches(item_attrs) for item_attrs in all_item_attrs):
        raise ValueError(
            "create_many supports shared attributes only; use attributes=... "
            "or separate create calls for per-item attributes"
        )

    return first


def _shared_create_many_state_meta(
    items: builtins.list[CreateItem],
    state_meta: dict[str, Any] | None,
) -> dict[str, Any] | None:
    all_item_meta = [item.state_meta or None for item in items]
    populated_meta = [item_meta for item_meta in all_item_meta if item_meta is not None]
    if state_meta is not None:
        matcher = BatchValueMatcher(state_meta)
        if any(not matcher.matches(item_meta) for item_meta in populated_meta):
            raise ValueError(
                "create_many item state_meta must match shared state_meta when both are provided"
            )
        return state_meta
    if not populated_meta:
        return None

    first = populated_meta[0]
    matcher = BatchValueMatcher(first)
    if any(not matcher.matches(item_meta) for item_meta in all_item_meta):
        raise ValueError(
            "create_many supports shared state_meta only; use state_meta=... "
            "or separate create calls for per-item state_meta"
        )

    return first


def _run_steps_many_items(
    items: builtins.list[str | dict[str, Any] | CreateItem],
    partition_key: str | bytes | None,
) -> builtins.list[dict[str, str | bytes]]:
    normalized: builtins.list[dict[str, str | bytes]] = []
    for item in items:
        if isinstance(item, CreateItem):
            id = item.id
            item_partition = item.partition_key if item.partition_key is not None else partition_key
        elif isinstance(item, dict):
            raw_id = item.get("id")
            if not isinstance(raw_id, str) or not raw_id:
                raise ValueError("run_steps_many item id must be a non-empty string")
            id = raw_id
            raw_partition = item.get("partition_key", partition_key)
            if raw_partition is not None and not isinstance(raw_partition, (str, bytes)):
                raise ValueError("run_steps_many item partition_key must be string or bytes")
            item_partition = raw_partition
        else:
            if not isinstance(item, str) or not item:
                raise ValueError("run_steps_many item id must be a non-empty string")
            id = item
            item_partition = partition_key

        normalized_item: dict[str, str | bytes] = {"id": id}
        if item_partition is not None:
            normalized_item["partition_key"] = item_partition
        normalized.append(normalized_item)
    return normalized


def _run_steps_many_args(
    codec: Codec,
    items: builtins.list[str | dict[str, Any] | CreateItem],
    *,
    type: str,
    states: Sequence[str] | None,
    steps: int | None,
    worker: str,
    lease_ms: int,
    now_ms: int | None,
    payload: Any,
    result: Any,
    partition_key: str | bytes | None,
    retention_ttl_ms: int | None,
) -> builtins.list[Any]:
    if (states is None) == (steps is None):
        raise ValueError("run_steps_many requires exactly one of states or steps")
    lease_ms = validate_positive_int(lease_ms, name="lease_ms")
    now_ms = validate_optional_nonnegative_int(now_ms, name="now_ms")
    if retention_ttl_ms is not None:
        retention_ttl_ms = validate_positive_int(
            retention_ttl_ms,
            name="retention_ttl_ms",
        )
    resolved_states = (
        validate_string_sequence(states, name="run_steps_many states", allow_empty=False)
        if states is not None
        else None
    )
    if steps is not None:
        steps = validate_positive_int(steps, name="run_steps_many steps")

    args: builtins.list[Any] = ["FLOW.RUN_STEPS_MANY", "TYPE", type]
    if resolved_states is not None:
        args.extend(["STATES", list(resolved_states)])
    else:
        args.extend(["STEPS", steps])
    args.extend(
        [
            "WORKER",
            worker,
            "LEASE_MS",
            lease_ms,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
    )
    _append_encoded(args, "PAYLOAD", codec, payload)
    _append_encoded(args, "RESULT", codec, result)
    _append(args, "RETENTION_TTL_MS", retention_ttl_ms)
    args.extend(["ITEMS", _run_steps_many_items(items, partition_key)])
    return args


def _step_continue_args(
    codec: Codec,
    id: str,
    *,
    lease_token: bytes,
    from_state: str,
    to_state: str,
    fencing_token: int,
    lease_ms: int,
    partition_key: str | bytes | None,
    payload: Any,
    values: dict[str, Any] | None,
    value_refs: dict[str, str] | None,
    drop_values: builtins.list[str] | None,
    override_values: builtins.list[str] | None,
    attributes_merge: dict[str, Any] | None,
    attributes_delete: builtins.list[str] | None,
    state_meta: dict[str, Any] | None,
    now_ms: int | None,
    worker: str | None,
    return_job: bool,
) -> builtins.list[Any]:
    _validate_claim_guard(lease_token, fencing_token)
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
    _append_encoded(args, "PAYLOAD", codec, payload)
    if return_job:
        args.extend(["RETURN", "JOBS_COMPACT"])
    _append_attributes(
        args,
        attributes_merge=attributes_merge,
        attributes_delete=attributes_delete,
    )
    _append_state_meta(args, state_meta)
    _append_named_values(
        args,
        codec,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )
    return args


def _complete_many_args(
    codec: Codec,
    partition_key: str | bytes | None,
    items: builtins.list[ClaimedFlow],
    *,
    result: Any,
    payload: Any,
    values: dict[str, Any] | None,
    value_refs: dict[str, str] | None,
    drop_values: builtins.list[str] | None,
    override_values: builtins.list[str] | None,
    attributes_merge: dict[str, Any] | None,
    attributes_delete: builtins.list[str] | None,
    state_meta: dict[str, Any] | None,
    ttl_ms: int | None,
    now_ms: int | None,
    independent: bool | None,
    return_ok_on_success: bool,
) -> builtins.list[Any]:
    args: builtins.list[Any] = [
        "FLOW.COMPLETE_MANY",
        "MIXED" if partition_key is None else partition_key,
    ]
    _append_encoded(args, "RESULT", codec, result)
    _append_encoded(args, "PAYLOAD", codec, payload)
    _append(args, "TTL", ttl_ms)
    _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
    _append_bool(args, "INDEPENDENT", independent)
    _append_attributes(
        args,
        attributes_merge=attributes_merge,
        attributes_delete=attributes_delete,
    )
    _append_state_meta(args, state_meta)
    if return_ok_on_success:
        _append(args, "RETURN", "OK_ON_SUCCESS")
    _append_named_values(
        args,
        codec,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )
    mixed = partition_key is None
    args.append("ITEMS")
    for item in items:
        _validate_claim_guard(item.lease_token, item.fencing_token)
        if mixed:
            if item.partition_key is None:
                raise ValueError("mixed FLOW.COMPLETE_MANY items require partition_key")
            args.extend([item.id, item.partition_key, item.lease_token, item.fencing_token])
        else:
            if item.partition_key is not None and item.partition_key != partition_key:
                raise ValueError(
                    "FLOW.COMPLETE_MANY item partition_key does not match batch partition_key"
                )
            args.extend([item.id, item.lease_token, item.fencing_token])
    return args


def _append_claimed_items_args(
    args: builtins.list[Any],
    partition_key: str | bytes | None,
    items: builtins.list[ClaimedFlow],
    command: str,
) -> builtins.list[Any]:
    mixed = partition_key is None
    args.append("ITEMS")
    for item in items:
        _validate_claim_guard(item.lease_token, item.fencing_token)
        if mixed:
            if item.partition_key is None:
                raise ValueError(f"mixed {command} items require partition_key")
            args.extend([item.id, item.partition_key, item.lease_token, item.fencing_token])
        else:
            if item.partition_key is not None and item.partition_key != partition_key:
                raise ValueError(f"{command} item partition_key does not match batch partition_key")
            args.extend([item.id, item.lease_token, item.fencing_token])
    return args


def _append_fenced_items_args(
    args: builtins.list[Any],
    partition_key: str | bytes | None,
    items: builtins.list[FencedItem],
    command: str,
    *,
    include_lease: bool = False,
) -> builtins.list[Any]:
    mixed = partition_key is None
    for item in items:
        _validate_fencing_token(item.fencing_token)
        if include_lease:
            try:
                _validate_lease_token(item.lease_token)
            except ValueError as exc:
                raise ValueError(f"{command} item lease_token is required") from exc
        if mixed and item.partition_key is None:
            raise ValueError(f"mixed {command} items require partition_key")
        if not mixed and item.partition_key is not None and item.partition_key != partition_key:
            raise ValueError(f"{command} item partition_key does not match batch partition_key")

    args.append("ITEMS")
    for item in items:
        if mixed:
            args.extend([item.id, item.partition_key, item.fencing_token])
        else:
            args.extend([item.id, item.fencing_token])
        if include_lease:
            args.append(item.lease_token)
    return args


def _complete_jobs_command_args(
    codec: Codec,
    jobs: builtins.list[ClaimedFlow],
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
        if first_partition is not None and all(job.partition_key == first_partition for job in jobs)
        else None
    )
    args: builtins.list[Any] = [
        "FLOW.COMPLETE_MANY",
        "MIXED" if complete_partition_key is None else complete_partition_key,
    ]
    _append_encoded(args, "RESULT", codec, result)
    _append_encoded(args, "PAYLOAD", codec, payload)
    _append(args, "TTL", ttl_ms)
    _append(args, "NOW", now_ms if now_ms is not None else _now_ms())
    _append_bool(args, "INDEPENDENT", independent)
    _append_bool(args, "TERMINAL_LOCAL_ONLY", True)
    return _append_claimed_items_args(
        args,
        complete_partition_key,
        jobs,
        "FLOW.COMPLETE_MANY",
    )


def _complete_command_args(
    codec: Codec,
    id: str,
    *,
    lease_token: bytes,
    fencing_token: int,
    partition_key: str | bytes | None = None,
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
) -> builtins.list[Any]:
    _validate_claim_guard(lease_token, fencing_token)
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
    _append_encoded(args, "RESULT", codec, result)
    _append_encoded(args, "PAYLOAD", codec, payload)
    _append(args, "TTL", ttl_ms)
    _append_attributes(
        args,
        attributes_merge=attributes_merge,
        attributes_delete=attributes_delete,
    )
    _append_state_meta(args, state_meta)
    _append_named_values(
        args,
        codec,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )
    return args


def _transition_command_args(
    codec: Codec,
    id: str,
    *,
    from_state: str,
    to_state: str,
    lease_token: bytes,
    fencing_token: int,
    partition_key: str | bytes | None = None,
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
) -> builtins.list[Any]:
    _validate_claim_guard(lease_token, fencing_token)
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
    _append_encoded(args, "PAYLOAD", codec, payload)
    _append(args, "RUN_AT", run_at_ms if run_at_ms is not None else now_ms)
    _append_priority(args, priority)
    _append_attributes(
        args,
        attributes_merge=attributes_merge,
        attributes_delete=attributes_delete,
    )
    _append_state_meta(args, state_meta)
    _append_named_values(
        args,
        codec,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )
    return args


def _retry_command_args(
    codec: Codec,
    id: str,
    *,
    lease_token: bytes,
    fencing_token: int,
    partition_key: str | bytes | None = None,
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
) -> builtins.list[Any]:
    _validate_claim_guard(lease_token, fencing_token)
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
    _append_encoded(args, "ERROR", codec, error)
    _append_encoded(args, "PAYLOAD", codec, payload)
    _append(args, "RUN_AT", run_at_ms)
    _append_attributes(
        args,
        attributes_merge=attributes_merge,
        attributes_delete=attributes_delete,
    )
    _append_state_meta(args, state_meta)
    _append_named_values(
        args,
        codec,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )
    return args


def _fail_command_args(
    codec: Codec,
    id: str,
    *,
    lease_token: bytes,
    fencing_token: int,
    partition_key: str | bytes | None = None,
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
) -> builtins.list[Any]:
    _validate_claim_guard(lease_token, fencing_token)
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
    _append_encoded(args, "ERROR", codec, error)
    _append_encoded(args, "PAYLOAD", codec, payload)
    _append(args, "TTL", ttl_ms)
    _append_attributes(
        args,
        attributes_merge=attributes_merge,
        attributes_delete=attributes_delete,
    )
    _append_state_meta(args, state_meta)
    _append_named_values(
        args,
        codec,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )
    return args


def _job_mutation_command_args(
    codec: Codec,
    mutation: JobMutation,
    *,
    now_ms: int,
) -> builtins.list[Any]:
    job = mutation.job
    options = dict(mutation.options)
    options.setdefault("now_ms", now_ms)
    common = {
        "lease_token": job.lease_token,
        "fencing_token": job.fencing_token,
        "partition_key": job.partition_key,
    }
    if mutation.kind is MutationKind.COMPLETE:
        return _complete_command_args(codec, job.id, **common, **options)
    if mutation.kind is MutationKind.TRANSITION:
        return _transition_command_args(codec, job.id, **common, **options)
    if mutation.kind is MutationKind.RETRY:
        return _retry_command_args(codec, job.id, **common, **options)
    if mutation.kind is MutationKind.FAIL:
        return _fail_command_args(codec, job.id, **common, **options)
    raise ValueError(f"unsupported job mutation kind {mutation.kind!r}")


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
    resolved_values = validate_string_sequence(values, name="values") if values is not None else ()
    value_max_bytes = validate_optional_nonnegative_int(
        value_max_bytes,
        name="value_max_bytes",
    )
    for name in resolved_values:
        args.extend(["VALUE", name])
    _append(args, "VALUE_MAX_BYTES", value_max_bytes)


def _batch_key_value(value: Any) -> Any:
    return queued_batch_fingerprint(value)


def _batch_named_key(
    *,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
) -> tuple[Any, Any, Any, Any]:
    return (
        _batch_key_value(values),
        _batch_key_value(value_refs),
        _batch_key_value(drop_values),
        _batch_key_value(override_values),
    )


def _expand_many_response(value: Any, count: int) -> builtins.list[Any]:
    return expand_many_result(value, count, operation="Flow many response")


def _append_read_options(
    args: builtins.list[Any],
    *,
    partition_key: str | bytes | None = None,
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


def _normalize_admin_response(value: Any) -> Any:
    """Normalize admin/control-plane responses into Python-native shapes."""

    return _normalize_ref_meta(value)


def _append_extra_options(args: builtins.list[Any], options: dict[str, Any] | None) -> None:
    for name, value in (options or {}).items():
        if value is not None:
            args.extend([name.upper(), value])


def _management_pair_args(
    pairs: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> builtins.list[Any]:
    args: builtins.list[Any] = []
    merged: dict[str, Any] = {}
    if pairs:
        merged.update(dict(pairs))
    if extra:
        merged.update(dict(extra))

    for key, value in merged.items():
        if value is None:
            continue
        args.extend([str(key).upper(), value])
    return args


def _management_rule_args(rules: Sequence[Any] | Any) -> builtins.list[str]:
    values = [rules] if isinstance(rules, (str, bytes)) else list(cast(Sequence[Any], rules))
    return [_text(value) for value in values]


def _coerce_diag_value(value: str) -> Any:
    if value == "":
        return value
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return int(value)
    except ValueError:
        return value
