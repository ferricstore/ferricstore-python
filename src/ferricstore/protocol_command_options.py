from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ferricstore.command_grammar import (
    consume_counted_arguments,
)
from ferricstore.errors import (
    InvalidCommandError,
)
from ferricstore.flow_options import FlowOptionPlan
from ferricstore.protocol_common import (
    _coerce_bool,
    _command_token,
    _map_get,
    _require_arg,
    _text,
    _text_or_none,
)
from ferricstore.protocol_constants import (
    _BOOL_FIELDS,
    _FIELD_NAMES,
    _OP_COMMAND_EXEC,
    ProtocolCommand,
)


def _command_exec_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    command_args = list(args)
    payload: dict[str, Any] = {"command": name, "args": command_args}

    if len(command_args) >= 2 and _command_token(command_args[-2]) == "REQUEST_CONTEXT":
        request_context = _normalize_request_context(command_args[-1])
        payload["args"] = command_args[:-2]
        if request_context:
            payload["request_context"] = request_context

    return ProtocolCommand(_OP_COMMAND_EXEC, payload, 1)


def _normalize_request_context(context: Any) -> dict[str, Any] | None:
    if not isinstance(context, Mapping):
        return None

    payload: dict[str, Any] = {}
    subject = _text_or_none(_map_get(context, "subject"))
    tenant = _text_or_none(_map_get(context, "tenant"))
    scopes = _normalize_request_context_scopes(_map_get(context, "scopes"))

    if subject:
        payload["subject"] = subject
    if tenant:
        payload["tenant"] = tenant
    if scopes:
        payload["scopes"] = scopes
    return payload or None


def _normalize_request_context_scopes(scopes: Any) -> list[str]:
    if scopes is None:
        return []
    if isinstance(scopes, (str, bytes)):
        values = _text(scopes).split()
    elif isinstance(scopes, Sequence):
        values = [_text(value) for value in scopes if value is not None and value != ""]
    else:
        return []
    return list(dict.fromkeys(value for value in values if value))


def _generic_option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    if len(args) % 2 != 0:
        raise InvalidCommandError("protocol options require name/value pairs")
    return {_command_token(args[idx]).lower(): args[idx + 1] for idx in range(0, len(args), 2)}


def _option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    option_plan = FlowOptionPlan(args)
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "NOPAYLOAD":
            payload["payload"] = False
            idx += 1
            continue
        if token == "PAYLOAD":
            if option_plan.payload_is_flag(idx):
                payload["payload"] = True
                idx += 1
            else:
                payload["payload"] = _require_arg(args, idx + 1, "PAYLOAD")
                idx += 2
            continue
        if token == "PARTITIONS":
            segment = consume_counted_arguments(args, idx + 1, label="PARTITIONS")
            payload["partition_keys"] = list(segment.values)
            idx = segment.next_index
            continue
        if token == "STATE":
            value = _require_arg(args, idx + 1, "STATE")
            if "states" in payload:
                payload["states"].append(value)
            elif "state" in payload:
                payload["states"] = [payload.pop("state"), value]
            else:
                payload["state"] = value
            idx += 2
            continue
        if token == "IF_STATE":
            value = _require_arg(args, idx + 1, "IF_STATE")
            if "if_state" in payload:
                existing = payload["if_state"]
                if isinstance(existing, list):
                    existing.append(value)
                else:
                    payload["if_state"] = [existing, value]
            else:
                payload["if_state"] = value
            idx += 2
            continue
        if token == "RETURN":
            return_value = _text(_require_arg(args, idx + 1, "RETURN"))
            if return_value in {"JOBS_COMPACT", "JOBS_COMPACT_STATE"}:
                payload["return"] = return_value.lower()
            else:
                payload["return"] = return_value.lower()
            idx += 2
            continue
        if token == "VALUE":
            name = _text(_require_arg(args, idx + 1, "VALUE"))
            value = _require_arg(args, idx + 2, "VALUE")
            payload.setdefault("values", {})[name] = value
            idx += 3
            continue
        if token == "VALUE_REF":
            name = _text(_require_arg(args, idx + 1, "VALUE_REF"))
            ref = _require_arg(args, idx + 2, "VALUE_REF")
            payload.setdefault("value_refs", {})[name] = ref
            idx += 3
            continue
        if token in {"DROP_VALUE", "OVERRIDE_VALUE"}:
            list_field = "drop_values" if token == "DROP_VALUE" else "override_values"
            payload.setdefault(list_field, []).append(_text(_require_arg(args, idx + 1, token)))
            idx += 2
            continue
        if token in {"ATTRIBUTE", "ATTRIBUTE_MERGE"}:
            map_field = "attributes" if token == "ATTRIBUTE" else "attributes_merge"
            name = _text(_require_arg(args, idx + 1, token))
            value = _require_arg(args, idx + 2, token)
            payload.setdefault(map_field, {})[name] = value
            idx += 3
            continue
        if token == "STATE_META":
            name = _text(_require_arg(args, idx + 1, token))
            value = _require_arg(args, idx + 2, token)
            payload.setdefault("state_meta", {})[name] = value
            idx += 3
            continue
        if token == "ATTRIBUTE_DELETE":
            payload.setdefault("attributes_delete", []).append(
                _text(_require_arg(args, idx + 1, token))
            )
            idx += 2
            continue

        mapped_field = _FIELD_NAMES.get(token)
        if mapped_field is None:
            raise InvalidCommandError(
                f"FerricStore protocol transport does not support option {token}"
            )
        value = _require_arg(args, idx + 1, token)
        payload[mapped_field] = _coerce_bool(value) if mapped_field in _BOOL_FIELDS else value
        idx += 2
    return payload


__all__ = [
    "_command_exec_protocol_command",
    "_generic_option_map",
    "_normalize_request_context",
    "_normalize_request_context_scopes",
    "_option_map",
]
