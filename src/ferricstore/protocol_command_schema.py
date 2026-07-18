from __future__ import annotations

from dataclasses import dataclass

from ferricstore.errors import InvalidCommandError


@dataclass(frozen=True, slots=True)
class CommandArity:
    """Argument-count contract for a specialized native command."""

    minimum: int
    maximum: int | None


_SPECIALIZED_COMMAND_ARITIES: dict[str, CommandArity] = {
    "AUTH": CommandArity(2, 2),
    "PING": CommandArity(0, 1),
    "OPTIONS": CommandArity(0, 0),
    "ROUTE": CommandArity(1, 1),
    "ROUTE_BATCH": CommandArity(1, None),
    "SHARDS": CommandArity(0, 0),
    "BACKPRESSURE": CommandArity(0, 0),
    "QUIT": CommandArity(0, 0),
    "CLIENT.SETNAME": CommandArity(1, 1),
    "CLIENT.INFO": CommandArity(0, 0),
    "GET": CommandArity(1, 1),
    "SET": CommandArity(2, None),
    "DEL": CommandArity(1, None),
    "MGET": CommandArity(1, None),
    "MSET": CommandArity(2, None),
    "CAS": CommandArity(3, None),
    "LOCK": CommandArity(3, 3),
    "EXTEND": CommandArity(3, 3),
    "UNLOCK": CommandArity(2, 2),
    "RATELIMIT.ADD": CommandArity(3, 4),
    "FETCH_OR_COMPUTE": CommandArity(2, 3),
    "FETCH_OR_COMPUTE_RESULT": CommandArity(4, 4),
    "FETCH_OR_COMPUTE_ERROR": CommandArity(3, 3),
    "HSET": CommandArity(3, None),
    "HGET": CommandArity(2, 2),
    "HMGET": CommandArity(2, None),
    "HGETALL": CommandArity(1, 1),
    "LPUSH": CommandArity(2, None),
    "RPUSH": CommandArity(2, None),
    "LPOP": CommandArity(1, 2),
    "RPOP": CommandArity(1, 2),
    "LRANGE": CommandArity(3, 3),
    "SADD": CommandArity(2, None),
    "SREM": CommandArity(2, None),
    "SMEMBERS": CommandArity(1, 1),
    "SISMEMBER": CommandArity(2, 2),
    "ZADD": CommandArity(3, None),
    "ZREM": CommandArity(2, None),
    "ZRANGE": CommandArity(3, None),
    "ZSCORE": CommandArity(2, 2),
    "CLUSTER.KEYSLOT": CommandArity(1, 1),
    "FERRICSTORE.KEY_INFO": CommandArity(1, 1),
}
_SPECIALIZED_COMMANDS_WITH_LOCAL_ARITY_VALIDATION = frozenset({"SET"})


def validate_specialized_command_arity(name: str, argument_count: int) -> None:
    contract = _SPECIALIZED_COMMAND_ARITIES.get(name)
    if contract is None:
        return
    if argument_count < contract.minimum or (
        contract.maximum is not None and argument_count > contract.maximum
    ):
        raise InvalidCommandError(f"wrong number of arguments for {name}")


__all__ = ["CommandArity", "validate_specialized_command_arity"]
