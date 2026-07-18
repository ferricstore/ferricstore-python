from __future__ import annotations

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
        "FLOW.BY_CORRELATION",
        "FLOW.BY_PARENT",
        "FLOW.BY_ROOT",
        "FLOW.CIRCUIT.GET",
        "FLOW.EFFECT.GET",
        "FLOW.FAILURES",
        "FLOW.GET",
        "FLOW.GOVERNANCE.LEDGER",
        "FLOW.GOVERNANCE.OVERVIEW",
        "FLOW.HISTORY",
        "FLOW.INFO",
        "FLOW.LIMIT.GET",
        "FLOW.LIMIT.LIST",
        "FLOW.LIST",
        "FLOW.POLICY.GET",
        "FLOW.SCHEDULE.GET",
        "FLOW.SCHEDULE.LIST",
        "FLOW.SEARCH",
        "FLOW.STATS",
        "FLOW.STUCK",
        "FLOW.TERMINALS",
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


def request_may_mutate(opcode: int) -> bool:
    """Conservatively classify unknown/generic opcodes as mutations."""
    return opcode not in _NON_MUTATING_OPCODES


def request_outcome_error(
    opcode: int,
    cause: BaseException,
    *,
    message: str = "protocol request failed after it may have been sent",
) -> FerricStoreError:
    if request_may_mutate(opcode):
        return RequestOutcomeUnknownError(f"{message}; mutation outcome is unknown", raw=cause)
    return FerricStoreError(
        message,
        raw=cause,
        retryable=True,
        safe_to_retry=True,
    )


__all__ = ["request_may_mutate", "request_outcome_error"]
