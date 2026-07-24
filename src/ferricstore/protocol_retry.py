from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ferricstore.errors import FerricStoreError, RequestOutcomeUnknownError
from ferricstore.protocol_constants import _OPCODES

_NON_MUTATING_COMMANDS = frozenset(
    {
        "AUTH",
        "BACKPRESSURE",
        "CLIENT.INFO",
        "CLIENT.SETNAME",
        "CLUSTER.HEALTH",
        "CLUSTER.KEYSLOT",
        "CLUSTER.ROLE",
        "CLUSTER.SLOTS",
        "CLUSTER.STATS",
        "CLUSTER.STATUS",
        "FERRICSTORE.HOTNESS",
        "FERRICSTORE.KEY_INFO",
        "FERRICSTORE.METRICS",
        "FLOW.APPROVAL.GET",
        "FLOW.APPROVAL.LIST",
        "FLOW.ATTRIBUTE_VALUES",
        "FLOW.ATTRIBUTES",
        "FLOW.BUDGET.GET",
        "FLOW.BUDGET.LIST",
        "FLOW.CIRCUIT.GET",
        "FLOW.EFFECT.GET",
        "FLOW.GET",
        "FLOW.GOVERNANCE.LEDGER",
        "FLOW.GOVERNANCE.OVERVIEW",
        "FLOW.HISTORY",
        "FLOW.INFO",
        "FLOW.LIMIT.GET",
        "FLOW.LIMIT.LIST",
        "FLOW.POLICY.GET",
        "FLOW.QUERY.INDEXES",
        "FLOW.SCHEDULE.GET",
        "FLOW.SCHEDULE.LIST",
        "FLOW.QUERY",
        "FLOW.STATS",
        "FLOW.VALUE.MGET",
        "GET",
        "HELLO",
        "HGET",
        "HGETALL",
        "HMGET",
        "LRANGE",
        "MGET",
        "OPTIONS",
        "PING",
        "QUIT",
        "ROUTE",
        "SHARDS",
        "SISMEMBER",
        "SMEMBERS",
        "STARTUP",
        "SUBSCRIBE_EVENTS",
        "UNSUBSCRIBE_EVENTS",
        "WINDOW_UPDATE",
        "ZRANGE",
        "ZSCORE",
    }
)

_NON_MUTATING_OPCODES = frozenset(
    _OPCODES[name] for name in _NON_MUTATING_COMMANDS if name in _OPCODES
)


def request_may_mutate(
    opcode: int,
    payload: Mapping[Any, Any] | bytes | None = None,
) -> bool:
    """Conservatively classify unknown/generic opcodes as mutations."""
    return _request_may_mutate(opcode, payload, pipeline_depth=0)


def _request_may_mutate(
    opcode: int,
    payload: Mapping[Any, Any] | bytes | None,
    *,
    pipeline_depth: int,
) -> bool:
    if opcode == _OPCODES["COMMAND_EXEC"] and isinstance(payload, Mapping):
        raw_command = _mapping_value(payload, "command")
        if isinstance(raw_command, bytes):
            try:
                raw_command = raw_command.decode("ascii")
            except UnicodeDecodeError:
                return True
        if isinstance(raw_command, str):
            return raw_command.strip().upper() not in _NON_MUTATING_COMMANDS
    if opcode == _OPCODES["PIPELINE"]:
        if pipeline_depth >= 4 or not isinstance(payload, Mapping):
            return True
        commands = _mapping_value(payload, "commands")
        if not isinstance(commands, (list, tuple)) or not commands:
            return True
        for command in commands:
            if not isinstance(command, Mapping):
                return True
            nested_opcode = _mapping_value(command, "opcode")
            nested_payload = _mapping_value(command, "body")
            if type(nested_opcode) is not int or not isinstance(nested_payload, (Mapping, bytes)):
                return True
            if _request_may_mutate(
                nested_opcode,
                nested_payload,
                pipeline_depth=pipeline_depth + 1,
            ):
                return True
        return False
    return opcode not in _NON_MUTATING_OPCODES


def _mapping_value(mapping: Mapping[Any, Any], field: str) -> Any:
    if field in mapping:
        return mapping[field]
    return mapping.get(field.encode())


def request_outcome_error(
    opcode: int,
    cause: BaseException,
    *,
    payload: Mapping[Any, Any] | bytes | None = None,
    may_mutate: bool | None = None,
    message: str = "protocol request failed after it may have been sent",
) -> FerricStoreError:
    mutation = request_may_mutate(opcode, payload) if may_mutate is None else may_mutate
    if mutation:
        return RequestOutcomeUnknownError(f"{message}; mutation outcome is unknown", raw=cause)
    return FerricStoreError(
        message,
        raw=cause,
        retryable=True,
        safe_to_retry=True,
    )


__all__ = ["request_may_mutate", "request_outcome_error"]
