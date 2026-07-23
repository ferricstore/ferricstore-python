from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ferricstore.errors import (
    InvalidCommandError,
)
from ferricstore.flow_query_request import build_flow_query_payload
from ferricstore.protocol_command_options import (
    _command_exec_protocol_command,
    _option_map,
)
from ferricstore.protocol_common import (
    _coerce_bool,
    _command_token,
    _require_arg,
    _text,
)
from ferricstore.protocol_constants import (
    _BOOL_FIELDS,
    _COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
    _COMPACT_FLOW_COMPLETE_MANY_REQUEST,
    _COMPACT_FLOW_RETRY_MANY_OK_REQUEST,
    _COMPACT_FLOW_RETRY_MANY_REQUEST,
    _FIELD_NAMES,
    _FLAG_CUSTOM_PAYLOAD,
    _FLOW_POLICY_FIELD_NAMES,
    _OPCODES,
    ProtocolCommand,
)
from ferricstore.protocol_flow_codec import (
    _compact_flow_cancel_many_payload,
    _compact_flow_claim_due_payload,
    _compact_flow_claimed_many_payload,
    _compact_flow_create_many_payload,
    _compact_flow_transition_many_payload,
    _compact_flow_value_mget_payload,
)
from ferricstore.protocol_flow_payloads import (
    _collapse_states,
    _find_item_token,
    _flow_claimed_many_payload,
    _flow_create_many_payload,
    _flow_fenced_many_payload,
    _flow_spawn_children_payload,
    _parse_claimed_items,
    _parse_create_items,
    _parse_create_items_ext,
    _parse_fenced_items,
    _parse_spawn_children,
    _parse_spawn_children_ext,
    _split_refs_and_options,
)


def _build_flow_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    command = _build_native_flow_protocol_command(name, args)
    payload = command.payload
    if isinstance(payload, dict) and ("indexed_state_meta" in payload or "state_meta" in payload):
        return _command_exec_protocol_command(name, args)
    return command


_FlowCommandBuilder = Callable[[str, tuple[Any, ...], int], ProtocolCommand]


def _build_flow_creation_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name == "FLOW.CREATE":
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.CREATE_MANY":
        payload = _flow_create_many_payload(args)
        compact = _compact_flow_create_many_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.CLAIM_DUE":
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        _collapse_states(payload)
        compact = _compact_flow_claim_due_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)

    raise InvalidCommandError(f"unsupported flow creation command {name}")


def _build_flow_mutation_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name in {"FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL", "FLOW.EXTEND_LEASE"}:
        payload = {"id": _require_arg(args, 0, name), "lease_token": _require_arg(args, 1, name)}
        payload.update(_option_map(args[2:]))
        if name in {"FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL"}:
            _require_mutation_guards(name, payload, "fencing_token")
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.TRANSITION":
        payload = {
            "id": _require_arg(args, 0, name),
            "from_state": _require_arg(args, 1, name),
            "to_state": _require_arg(args, 2, name),
        }
        payload.update(_option_map(args[3:]))
        _require_mutation_guards(name, payload, "lease_token", "fencing_token")
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.STEP_CONTINUE":
        payload = {
            "id": _require_arg(args, 0, name),
            "lease_token": _require_arg(args, 1, name),
            "from_state": _require_arg(args, 2, name),
            "to_state": _require_arg(args, 3, name),
        }
        payload.update(_option_map(args[4:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.START_AND_CLAIM":
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.RUN_STEPS_MANY":
        payload = _option_map(args)
        if "items" not in payload:
            raise InvalidCommandError("FLOW.RUN_STEPS_MANY requires ITEMS")
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.CANCEL":
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        _require_mutation_guards(name, payload, "fencing_token")
        return ProtocolCommand(opcode, payload)

    raise InvalidCommandError(f"unsupported flow mutation command {name}")


def _require_mutation_guards(name: str, payload: dict[str, Any], *fields: str) -> None:
    missing = [field for field in fields if field not in payload]
    if missing:
        names = ", ".join(missing)
        raise InvalidCommandError(f"{name} requires {names}")


def _build_flow_many_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name in {"FLOW.COMPLETE_MANY", "FLOW.RETRY_MANY", "FLOW.FAIL_MANY"}:
        payload = _flow_claimed_many_payload(name, args)
        if name in {"FLOW.COMPLETE_MANY", "FLOW.FAIL_MANY"}:
            compact = _compact_flow_claimed_many_payload(
                payload,
                request_kind=_COMPACT_FLOW_COMPLETE_MANY_REQUEST,
                ok_request_kind=_COMPACT_FLOW_COMPLETE_MANY_OK_REQUEST,
                extra_allowed={"terminal_local_only"} if name == "FLOW.COMPLETE_MANY" else set(),
            )
            if compact is not None:
                return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        elif name == "FLOW.RETRY_MANY":
            compact = _compact_flow_claimed_many_payload(
                payload,
                request_kind=_COMPACT_FLOW_RETRY_MANY_REQUEST,
                ok_request_kind=_COMPACT_FLOW_RETRY_MANY_OK_REQUEST,
                extra_allowed={"run_at_ms"},
            )
            if compact is not None:
                return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.TRANSITION_MANY":
        payload = {
            "from_state": _require_arg(args, 1, name),
            "to_state": _require_arg(args, 2, name),
        }
        payload.update(_flow_fenced_many_payload(name, args[0:1] + args[3:], include_lease=True))
        compact = _compact_flow_transition_many_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.CANCEL_MANY":
        payload = _flow_fenced_many_payload(name, args, include_lease=False)
        compact = _compact_flow_cancel_many_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)

    raise InvalidCommandError(f"unsupported flow many command {name}")


_FLOW_ID_QUERY_ARGUMENTS = {
    "FLOW.GET": "id",
    "FLOW.HISTORY": "id",
    "FLOW.REWIND": "id",
    "FLOW.SIGNAL": "id",
}


def _build_flow_query_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name == "FLOW.QUERY":
        try:
            return ProtocolCommand(opcode, build_flow_query_payload(args))
        except (TypeError, ValueError) as exc:
            raise InvalidCommandError(str(exc)) from exc
    key = _FLOW_ID_QUERY_ARGUMENTS.get(name)
    if key is not None:
        payload = {key: _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name in {
        "FLOW.STATS",
        "FLOW.INFO",
    }:
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.ATTRIBUTES":
        payload = {"type": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.ATTRIBUTE_VALUES":
        payload = {
            "type": _require_arg(args, 0, name),
            "attribute": _require_arg(args, 1, name),
        }
        payload.update(_option_map(args[2:]))
        return ProtocolCommand(opcode, payload)
    raise InvalidCommandError(f"unsupported flow query command {name}")


def _build_flow_value_policy_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name == "FLOW.VALUE.PUT":
        payload = {"value": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name == "FLOW.VALUE.MGET":
        refs, options = _split_refs_and_options(args)
        payload = {"refs": refs}
        payload.update(options)
        compact = _compact_flow_value_mget_payload(payload)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, payload)
    if name in {"FLOW.POLICY.SET", "FLOW.POLICY.GET", "FLOW.RECLAIM"}:
        payload = {"type": _require_arg(args, 0, name)}
        if name == "FLOW.POLICY.SET":
            payload.update(_flow_policy_set_option_map(args[1:]))
        else:
            payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)

    raise InvalidCommandError(f"unsupported flow value/policy command {name}")


_FLOW_ID_SCOPED_COMMANDS = frozenset(
    {
        "FLOW.SCHEDULE.CREATE",
        "FLOW.SCHEDULE.GET",
        "FLOW.SCHEDULE.FIRE",
        "FLOW.SCHEDULE.PAUSE",
        "FLOW.SCHEDULE.RESUME",
        "FLOW.SCHEDULE.DELETE",
        "FLOW.EFFECT.RESERVE",
        "FLOW.EFFECT.CONFIRM",
        "FLOW.EFFECT.FAIL",
        "FLOW.EFFECT.COMPENSATE",
        "FLOW.EFFECT.GET",
        "FLOW.GOVERNANCE.LEDGER",
        "FLOW.APPROVAL.REQUEST",
        "FLOW.APPROVAL.APPROVE",
        "FLOW.APPROVAL.REJECT",
        "FLOW.APPROVAL.GET",
    }
)

_FLOW_SCOPE_SCOPED_COMMANDS = frozenset(
    {
        "FLOW.CIRCUIT.OPEN",
        "FLOW.CIRCUIT.CLOSE",
        "FLOW.CIRCUIT.GET",
        "FLOW.BUDGET.RESERVE",
        "FLOW.BUDGET.COMMIT",
        "FLOW.BUDGET.RELEASE",
        "FLOW.BUDGET.GET",
        "FLOW.LIMIT.LEASE",
        "FLOW.LIMIT.SPEND",
        "FLOW.LIMIT.RELEASE",
        "FLOW.LIMIT.GET",
    }
)

_FLOW_OPTION_ONLY_COMMANDS = frozenset(
    {
        "FLOW.SCHEDULE.FIRE_DUE",
        "FLOW.SCHEDULE.LIST",
        "FLOW.APPROVAL.LIST",
        "FLOW.GOVERNANCE.OVERVIEW",
        "FLOW.BUDGET.LIST",
        "FLOW.LIMIT.LIST",
        "FLOW.RETENTION_CLEANUP",
    }
)


def _build_flow_governance_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name in _FLOW_ID_SCOPED_COMMANDS:
        payload = {"id": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name in _FLOW_SCOPE_SCOPED_COMMANDS:
        payload = {"scope": _require_arg(args, 0, name)}
        payload.update(_option_map(args[1:]))
        return ProtocolCommand(opcode, payload)
    if name in _FLOW_OPTION_ONLY_COMMANDS:
        return ProtocolCommand(opcode, _option_map(args))
    if name == "FLOW.SPAWN_CHILDREN":
        return ProtocolCommand(opcode, _flow_spawn_children_payload(args))

    raise InvalidCommandError(f"unsupported flow governance command {name}")


_FLOW_COMMAND_FAMILIES: tuple[tuple[frozenset[str], _FlowCommandBuilder], ...] = (
    (
        frozenset({"FLOW.CREATE", "FLOW.CREATE_MANY", "FLOW.CLAIM_DUE"}),
        _build_flow_creation_protocol_command,
    ),
    (
        frozenset(
            {
                "FLOW.COMPLETE",
                "FLOW.RETRY",
                "FLOW.FAIL",
                "FLOW.EXTEND_LEASE",
                "FLOW.TRANSITION",
                "FLOW.STEP_CONTINUE",
                "FLOW.START_AND_CLAIM",
                "FLOW.RUN_STEPS_MANY",
                "FLOW.CANCEL",
            }
        ),
        _build_flow_mutation_protocol_command,
    ),
    (
        frozenset(
            {
                "FLOW.COMPLETE_MANY",
                "FLOW.RETRY_MANY",
                "FLOW.FAIL_MANY",
                "FLOW.TRANSITION_MANY",
                "FLOW.CANCEL_MANY",
            }
        ),
        _build_flow_many_protocol_command,
    ),
    (
        frozenset(
            {
                *_FLOW_ID_QUERY_ARGUMENTS,
                "FLOW.STATS",
                "FLOW.INFO",
                "FLOW.ATTRIBUTES",
                "FLOW.ATTRIBUTE_VALUES",
                "FLOW.QUERY",
            }
        ),
        _build_flow_query_protocol_command,
    ),
    (
        frozenset(
            {
                "FLOW.VALUE.PUT",
                "FLOW.VALUE.MGET",
                "FLOW.POLICY.SET",
                "FLOW.POLICY.GET",
                "FLOW.RECLAIM",
            }
        ),
        _build_flow_value_policy_protocol_command,
    ),
    (
        _FLOW_ID_SCOPED_COMMANDS
        | _FLOW_SCOPE_SCOPED_COMMANDS
        | _FLOW_OPTION_ONLY_COMMANDS
        | {"FLOW.SPAWN_CHILDREN"},
        _build_flow_governance_protocol_command,
    ),
)

_FLOW_COMMAND_BUILDERS = {
    name: builder for names, builder in _FLOW_COMMAND_FAMILIES for name in names
}


def _build_native_flow_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    builder = _FLOW_COMMAND_BUILDERS.get(name)
    if builder is not None:
        return builder(name, args, _OPCODES[name])
    raise InvalidCommandError(f"FerricStore protocol transport does not support command {name}")


def _flow_policy_set_option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    states: dict[str, dict[str, Any]] = {}
    current_state: Any = None
    current_args: list[Any] = []

    def flush_current() -> None:
        nonlocal current_args
        if current_state is None:
            payload.update(_flow_policy_option_map(tuple(current_args)))
        else:
            states[_text(current_state)] = _flow_policy_option_map(tuple(current_args))
        current_args = []

    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "STATE":
            flush_current()
            current_state = _require_arg(args, idx + 1, "STATE")
            idx += 2
            continue
        current_args.extend([args[idx], _require_arg(args, idx + 1, token)])
        idx += 2

    flush_current()
    if states:
        payload["states"] = states
    return payload


def _flow_policy_option_map(args: tuple[Any, ...]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        mapped_field = _FLOW_POLICY_FIELD_NAMES.get(token) or _FIELD_NAMES.get(token)
        if mapped_field is None:
            raise InvalidCommandError(
                f"FerricStore protocol transport does not support option {token}"
            )
        value = _require_arg(args, idx + 1, token)
        payload[mapped_field] = _coerce_bool(value) if mapped_field in _BOOL_FIELDS else value
        idx += 2
    return payload


__all__ = [
    "_build_flow_protocol_command",
    "_collapse_states",
    "_find_item_token",
    "_flow_claimed_many_payload",
    "_flow_create_many_payload",
    "_flow_fenced_many_payload",
    "_flow_policy_option_map",
    "_flow_policy_set_option_map",
    "_flow_spawn_children_payload",
    "_parse_claimed_items",
    "_parse_create_items",
    "_parse_create_items_ext",
    "_parse_fenced_items",
    "_parse_spawn_children",
    "_parse_spawn_children_ext",
    "_split_refs_and_options",
]
