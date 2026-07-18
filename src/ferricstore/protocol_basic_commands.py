from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

from ferricstore.errors import (
    InvalidCommandError,
)
from ferricstore.protocol_command_options import (
    _command_exec_protocol_command,
    _generic_option_map,
)
from ferricstore.protocol_common import (
    _command_token,
    _require_arg,
)
from ferricstore.protocol_constants import (
    _FLAG_CUSTOM_PAYLOAD,
    _OPCODES,
    ProtocolCommand,
)
from ferricstore.protocol_pipeline_codec import (
    _compact_kv_keys_payload,
    _compact_kv_set_pairs_payload,
)

_BasicCommandBuilder = Callable[[str, tuple[Any, ...], int], ProtocolCommand]


def _build_control_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name in {"HELLO", "STARTUP", "WINDOW_UPDATE"}:
        return ProtocolCommand(opcode, _generic_option_map(args), 0)
    if name == "AUTH":
        payload = {
            "username": _require_arg(args, 0, name),
            "password": _require_arg(args, 1, name),
        }
        return ProtocolCommand(opcode, payload, 0)
    if name == "PING":
        payload = {"message": args[0]} if args else {}
        return ProtocolCommand(opcode, payload, 0)
    if name == "OPTIONS":
        return ProtocolCommand(opcode, {}, 0)
    if name == "ROUTE":
        return ProtocolCommand(opcode, {"key": _require_arg(args, 0, name)}, 0)
    if name == "ROUTE_BATCH":
        return ProtocolCommand(opcode, {"keys": list(args)}, 0)
    if name == "SHARDS":
        return ProtocolCommand(opcode, {}, 0)
    if name == "BACKPRESSURE":
        return ProtocolCommand(opcode, {}, 0)
    if name == "QUIT":
        return ProtocolCommand(opcode, {}, 0)
    if name == "CLIENT.SETNAME":
        return ProtocolCommand(opcode, {"name": _require_arg(args, 0, name)}, 0)
    return ProtocolCommand(opcode, {}, 0)


def _build_kv_protocol_command(name: str, args: tuple[Any, ...], opcode: int) -> ProtocolCommand:
    if name == "GET":
        return ProtocolCommand(opcode, {"key": _require_arg(args, 0, name)})
    if name == "SET":
        payload = {"key": _require_arg(args, 0, name), "value": _require_arg(args, 1, name)}
        payload.update(_kv_set_options(args[2:]))
        return ProtocolCommand(opcode, payload)
    if name == "DEL":
        return ProtocolCommand(opcode, {"keys": list(args)})
    if name == "MGET":
        compact = _compact_kv_keys_payload(args, 2)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        return ProtocolCommand(opcode, {"keys": list(args)})
    if name == "MSET":
        if not args or len(args) % 2 != 0:
            raise InvalidCommandError("MSET requires key/value pairs")
        compact = _compact_kv_set_pairs_payload(args)
        if compact is not None:
            return ProtocolCommand(opcode, compact, flags=_FLAG_CUSTOM_PAYLOAD)
        pairs = [[args[idx], args[idx + 1]] for idx in range(0, len(args), 2)]
        return ProtocolCommand(opcode, {"pairs": pairs})
    if name == "CAS":
        payload = {
            "key": _require_arg(args, 0, name),
            "expected": _require_arg(args, 1, name),
            "value": _require_arg(args, 2, name),
        }
        idx = 3
        expiry_seen = False
        while idx < len(args):
            token = _command_token(args[idx])
            if token == "EX":
                if expiry_seen:
                    raise InvalidCommandError("protocol CAS accepts only one expiry option")
                payload["ttl"] = (
                    _positive_int_arg(_require_arg(args, idx + 1, "EX"), "CAS EX") * 1000
                )
                expiry_seen = True
                idx += 2
            elif token == "PX":
                if expiry_seen:
                    raise InvalidCommandError("protocol CAS accepts only one expiry option")
                payload["ttl"] = _positive_int_arg(_require_arg(args, idx + 1, "PX"), "CAS PX")
                expiry_seen = True
                idx += 2
            else:
                raise InvalidCommandError(f"protocol CAS does not support option {token}")
        return ProtocolCommand(opcode, payload)

    raise InvalidCommandError(f"unsupported key/value protocol command {name}")


def _build_coordination_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name in {"LOCK", "EXTEND"}:
        return ProtocolCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "owner": _require_arg(args, 1, name),
                "ttl_ms": _require_arg(args, 2, name),
            },
        )
    if name == "UNLOCK":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "owner": _require_arg(args, 1, name)},
        )
    if name == "RATELIMIT.ADD":
        payload = {
            "key": _require_arg(args, 0, name),
            "window_ms": _require_arg(args, 1, name),
            "max": _require_arg(args, 2, name),
        }
        if len(args) > 3:
            payload["count"] = _require_arg(args, 3, name)
        return ProtocolCommand(opcode, payload)
    if name == "FETCH_OR_COMPUTE":
        payload = {"key": _require_arg(args, 0, name), "ttl_ms": _require_arg(args, 1, name)}
        if len(args) > 2:
            payload["hint"] = _require_arg(args, 2, name)
        return ProtocolCommand(opcode, payload)
    if name == "FETCH_OR_COMPUTE_RESULT":
        return ProtocolCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "token": _require_arg(args, 1, name),
                "value": _require_arg(args, 2, name),
                "ttl_ms": _require_arg(args, 3, name),
            },
        )
    if name == "FETCH_OR_COMPUTE_ERROR":
        return ProtocolCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "token": _require_arg(args, 1, name),
                "message": _require_arg(args, 2, name),
            },
        )

    raise InvalidCommandError(f"unsupported coordination protocol command {name}")


def _build_hash_protocol_command(name: str, args: tuple[Any, ...], opcode: int) -> ProtocolCommand:
    if name == "HSET":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "fields": _field_value_map(name, args[1:])},
        )
    if name == "HGET":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "field": _require_arg(args, 1, name)},
        )
    if name == "HMGET":
        _require_values(name, args, 1)
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "fields": list(args[1:])},
        )
    if name == "HGETALL":
        return ProtocolCommand(opcode, {"key": _require_arg(args, 0, name)})

    raise InvalidCommandError(f"unsupported hash protocol command {name}")


def _build_list_protocol_command(name: str, args: tuple[Any, ...], opcode: int) -> ProtocolCommand:
    if name in {"LPUSH", "RPUSH"}:
        _require_values(name, args, 1)
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "values": list(args[1:])},
        )
    if name in {"LPOP", "RPOP"}:
        payload = {"key": _require_arg(args, 0, name)}
        if len(args) > 1:
            payload["count"] = _int_arg(args[1], name)
        return ProtocolCommand(opcode, payload)
    if name == "LRANGE":
        return ProtocolCommand(
            opcode,
            {
                "key": _require_arg(args, 0, name),
                "start": _int_arg(_require_arg(args, 1, name), name),
                "stop": _int_arg(_require_arg(args, 2, name), name),
            },
        )

    raise InvalidCommandError(f"unsupported list protocol command {name}")


def _build_set_protocol_command(name: str, args: tuple[Any, ...], opcode: int) -> ProtocolCommand:
    if name in {"SADD", "SREM"}:
        _require_values(name, args, 1)
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "members": list(args[1:])},
        )
    if name == "SMEMBERS":
        return ProtocolCommand(opcode, {"key": _require_arg(args, 0, name)})
    if name == "SISMEMBER":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "member": _require_arg(args, 1, name)},
        )

    raise InvalidCommandError(f"unsupported set protocol command {name}")


def _build_sorted_set_protocol_command(
    name: str, args: tuple[Any, ...], opcode: int
) -> ProtocolCommand:
    if name == "ZADD":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "items": _zadd_items(args[1:])},
        )
    if name == "ZREM":
        _require_values(name, args, 1)
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "members": list(args[1:])},
        )
    if name == "ZRANGE":
        return ProtocolCommand(opcode, _zrange_payload(args))
    if name == "ZSCORE":
        return ProtocolCommand(
            opcode,
            {"key": _require_arg(args, 0, name), "member": _require_arg(args, 1, name)},
        )

    raise InvalidCommandError(f"unsupported sorted-set protocol command {name}")


def _build_admin_protocol_command(name: str, args: tuple[Any, ...], opcode: int) -> ProtocolCommand:
    if name in {"CLUSTER.KEYSLOT", "FERRICSTORE.KEY_INFO"}:
        key = _require_arg(args, 0, name)
        return ProtocolCommand(opcode, {"key": key, "args": [key]})
    return ProtocolCommand(opcode, {"args": list(args)})


_BASIC_COMMAND_FAMILIES: tuple[tuple[frozenset[str], _BasicCommandBuilder], ...] = (
    (
        frozenset(
            {
                "HELLO",
                "STARTUP",
                "WINDOW_UPDATE",
                "AUTH",
                "PING",
                "OPTIONS",
                "ROUTE",
                "ROUTE_BATCH",
                "SHARDS",
                "BACKPRESSURE",
                "QUIT",
                "CLIENT.SETNAME",
                "CLIENT.INFO",
            }
        ),
        _build_control_protocol_command,
    ),
    (frozenset({"GET", "SET", "DEL", "MGET", "MSET", "CAS"}), _build_kv_protocol_command),
    (
        frozenset(
            {
                "LOCK",
                "EXTEND",
                "UNLOCK",
                "RATELIMIT.ADD",
                "FETCH_OR_COMPUTE",
                "FETCH_OR_COMPUTE_RESULT",
                "FETCH_OR_COMPUTE_ERROR",
            }
        ),
        _build_coordination_protocol_command,
    ),
    (frozenset({"HSET", "HGET", "HMGET", "HGETALL"}), _build_hash_protocol_command),
    (frozenset({"LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE"}), _build_list_protocol_command),
    (frozenset({"SADD", "SREM", "SMEMBERS", "SISMEMBER"}), _build_set_protocol_command),
    (frozenset({"ZADD", "ZREM", "ZRANGE", "ZSCORE"}), _build_sorted_set_protocol_command),
    (
        frozenset(
            {
                "CLUSTER.HEALTH",
                "CLUSTER.STATS",
                "CLUSTER.KEYSLOT",
                "CLUSTER.SLOTS",
                "CLUSTER.STATUS",
                "CLUSTER.JOIN",
                "CLUSTER.LEAVE",
                "CLUSTER.FAILOVER",
                "CLUSTER.PROMOTE",
                "CLUSTER.DEMOTE",
                "CLUSTER.ROLE",
                "FERRICSTORE.KEY_INFO",
                "FERRICSTORE.CONFIG",
                "FERRICSTORE.HOTNESS",
                "FERRICSTORE.METRICS",
                "FERRICSTORE.BLOBGC",
            }
        ),
        _build_admin_protocol_command,
    ),
)

_BASIC_COMMAND_BUILDERS = {
    name: builder for names, builder in _BASIC_COMMAND_FAMILIES for name in names
}


def _build_basic_protocol_command(name: str, args: tuple[Any, ...]) -> ProtocolCommand:
    opcode = _OPCODES[name]
    builder = _BASIC_COMMAND_BUILDERS.get(name)
    if builder is None:
        return _command_exec_protocol_command(name, args)
    return builder(name, args, opcode)


def _kv_set_options(args: tuple[Any, ...]) -> dict[str, Any]:
    if not args:
        return {}
    payload: dict[str, Any] = {}
    seen: set[str] = set()
    expiry: str | None = None
    idx = 0
    while idx < len(args):
        token = _command_token(args[idx])
        if token in seen:
            raise InvalidCommandError(f"protocol SET option {token} may only be specified once")
        seen.add(token)
        if token == "EX":
            if expiry is not None:
                raise InvalidCommandError("protocol SET accepts only one expiry option")
            payload["ttl"] = _positive_int_arg(_require_arg(args, idx + 1, "EX"), "SET EX") * 1000
            expiry = token
            idx += 2
        elif token == "PX":
            if expiry is not None:
                raise InvalidCommandError("protocol SET accepts only one expiry option")
            payload["ttl"] = _positive_int_arg(_require_arg(args, idx + 1, "PX"), "SET PX")
            expiry = token
            idx += 2
        elif token == "EXAT":
            if expiry is not None:
                raise InvalidCommandError("protocol SET accepts only one expiry option")
            payload["exat"] = _positive_int_arg(_require_arg(args, idx + 1, "EXAT"), "SET EXAT")
            expiry = token
            idx += 2
        elif token == "PXAT":
            if expiry is not None:
                raise InvalidCommandError("protocol SET accepts only one expiry option")
            payload["pxat"] = _positive_int_arg(_require_arg(args, idx + 1, "PXAT"), "SET PXAT")
            expiry = token
            idx += 2
        elif token in {"NX", "XX", "GET", "KEEPTTL"}:
            payload[token.lower()] = True
            if token == "KEEPTTL":
                if expiry is not None:
                    raise InvalidCommandError(
                        "protocol SET expiry options and KEEPTTL are mutually exclusive"
                    )
                expiry = token
            idx += 1
        else:
            raise InvalidCommandError(f"protocol SET does not support option {token}")
    if payload.get("nx") and payload.get("xx"):
        raise InvalidCommandError("protocol SET NX and XX are mutually exclusive")
    return payload


def _field_value_map(command: str, args: tuple[Any, ...]) -> dict[Any, Any]:
    if not args or len(args) % 2 != 0:
        raise InvalidCommandError(f"{command} requires field/value pairs")
    if any(not isinstance(args[idx], (str, bytes)) for idx in range(0, len(args), 2)):
        raise InvalidCommandError(f"{command} fields must be strings or bytes")
    return {args[idx]: args[idx + 1] for idx in range(0, len(args), 2)}


def _require_values(command: str, args: tuple[Any, ...], start: int) -> None:
    _require_arg(args, 0, command)
    if len(args) <= start:
        raise InvalidCommandError(f"{command} requires at least one value")


def _int_arg(value: Any, command: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str, bytes)):
        raise InvalidCommandError(f"{command} requires integer arguments")
    try:
        return int(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise InvalidCommandError(f"{command} requires integer arguments") from exc


def _positive_int_arg(value: Any, command: str) -> int:
    parsed = _int_arg(value, command)
    if parsed <= 0:
        raise InvalidCommandError(f"{command} requires positive integer arguments")
    return parsed


def _zadd_items(args: tuple[Any, ...]) -> list[list[Any]]:
    if not args or len(args) % 2 != 0:
        raise InvalidCommandError("ZADD requires score/member pairs")
    items: list[list[Any]] = []
    for idx in range(0, len(args), 2):
        if isinstance(args[idx], bool):
            raise InvalidCommandError("ZADD score must be numeric")
        try:
            score = float(args[idx])
        except (OverflowError, TypeError, ValueError) as exc:
            raise InvalidCommandError("ZADD score must be numeric") from exc
        if not math.isfinite(score):
            raise InvalidCommandError("ZADD score must be finite")
        items.append([score, args[idx + 1]])
    return items


def _zrange_payload(args: tuple[Any, ...]) -> dict[str, Any]:
    payload = {
        "key": _require_arg(args, 0, "ZRANGE"),
        "start": _int_arg(_require_arg(args, 1, "ZRANGE"), "ZRANGE"),
        "stop": _int_arg(_require_arg(args, 2, "ZRANGE"), "ZRANGE"),
    }
    idx = 3
    while idx < len(args):
        token = _command_token(args[idx])
        if token == "WITHSCORES":
            payload["withscores"] = True
            idx += 1
        else:
            raise InvalidCommandError(f"protocol ZRANGE does not support option {token}")
    return payload


__all__ = [
    "_build_basic_protocol_command",
    "_field_value_map",
    "_int_arg",
    "_kv_set_options",
    "_require_values",
    "_zadd_items",
    "_zrange_payload",
]
