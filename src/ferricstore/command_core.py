from __future__ import annotations

import zlib
from collections.abc import Sequence
from typing import Any

from ferricstore.flow_options import flow_option_width

FLOW_AUTO_PARTITION_PREFIX = "__flow_auto__:"
FLOW_AUTO_PARTITION_BUCKETS = 256

_FLOW_POSITIONAL_PARTITION_COMMANDS = {
    "FLOW.CANCEL_MANY",
    "FLOW.COMPLETE_MANY",
    "FLOW.CREATE_MANY",
    "FLOW.FAIL_MANY",
    "FLOW.RETRY_MANY",
    "FLOW.TRANSITION_MANY",
}

_FLOW_AUTO_ID_COMMANDS = {
    "FLOW.CANCEL",
    "FLOW.COMPLETE",
    "FLOW.CREATE",
    "FLOW.EFFECT.COMPENSATE",
    "FLOW.EFFECT.CONFIRM",
    "FLOW.EFFECT.FAIL",
    "FLOW.EFFECT.GET",
    "FLOW.EFFECT.RESERVE",
    "FLOW.EXTEND_LEASE",
    "FLOW.FAIL",
    "FLOW.GET",
    "FLOW.GOVERNANCE.LEDGER",
    "FLOW.HISTORY",
    "FLOW.RETRY",
    "FLOW.REWIND",
    "FLOW.SIGNAL",
    "FLOW.SPAWN_CHILDREN",
    "FLOW.START_AND_CLAIM",
    "FLOW.STEP_CONTINUE",
    "FLOW.TRANSITION",
}

_FLOW_OPTION_STARTS = {
    "FLOW.ATTRIBUTE_VALUES": 2,
    "FLOW.CANCEL": 1,
    "FLOW.CLAIM_DUE": 1,
    "FLOW.COMPLETE": 2,
    "FLOW.CREATE": 1,
    "FLOW.EFFECT.COMPENSATE": 1,
    "FLOW.EFFECT.CONFIRM": 1,
    "FLOW.EFFECT.FAIL": 1,
    "FLOW.EFFECT.GET": 1,
    "FLOW.EFFECT.RESERVE": 1,
    "FLOW.EXTEND_LEASE": 2,
    "FLOW.FAIL": 2,
    "FLOW.GET": 1,
    "FLOW.GOVERNANCE.LEDGER": 1,
    "FLOW.HISTORY": 1,
    "FLOW.LIST": 1,
    "FLOW.RETRY": 2,
    "FLOW.REWIND": 1,
    "FLOW.SEARCH": 1,
    "FLOW.SIGNAL": 1,
    "FLOW.SPAWN_CHILDREN": 1,
    "FLOW.START_AND_CLAIM": 1,
    "FLOW.STEP_CONTINUE": 4,
    "FLOW.TRANSITION": 3,
}

_CONTROL_COMMANDS = {
    "AUTH",
    "BACKPRESSURE",
    "CLIENT",
    "CLIENT.INFO",
    "CLIENT.SETNAME",
    "COMMAND",
    "DBSIZE",
    "DISCARD",
    "ECHO",
    "EXEC",
    "FLUSHALL",
    "FLUSHDB",
    "HELLO",
    "INFO",
    "KEYS",
    "MULTI",
    "OPTIONS",
    "PING",
    "PSUBSCRIBE",
    "PUBSUB",
    "PUBLISH",
    "PUNSUBSCRIBE",
    "QUIT",
    "RANDOMKEY",
    "ROUTE",
    "ROUTE_BATCH",
    "SCAN",
    "SELECT",
    "SHARDS",
    "SLOWLOG",
    "STARTUP",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "UNWATCH",
    "WINDOW_UPDATE",
}

_ALL_KEYS_COMMANDS = {
    "DEL",
    "EXISTS",
    "MGET",
    "PFCOUNT",
    "PFMERGE",
    "SDIFF",
    "SDIFFSTORE",
    "SINTER",
    "SINTERSTORE",
    "SUNION",
    "SUNIONSTORE",
    "UNLINK",
    "WATCH",
}

_PAIR_KEYS_COMMANDS = {"MSET", "MSETNX"}

_TWO_KEYS_COMMANDS = {
    "BLMOVE",
    "COPY",
    "GEOSEARCHSTORE",
    "LMOVE",
    "RENAME",
    "RENAMENX",
    "RPOPLPUSH",
    "SMOVE",
}

_FIRST_KEY_COMMANDS = {
    "APPEND",
    "BF.ADD",
    "BF.CARD",
    "BF.EXISTS",
    "BF.INFO",
    "BF.MADD",
    "BF.MEXISTS",
    "BF.RESERVE",
    "BITCOUNT",
    "BITPOS",
    "CF.ADD",
    "CF.ADDNX",
    "CF.COUNT",
    "CF.DEL",
    "CF.EXISTS",
    "CF.INFO",
    "CF.INSERT",
    "CF.INSERTNX",
    "CF.MEXISTS",
    "CF.RESERVE",
    "CMS.INCRBY",
    "CMS.INFO",
    "CMS.INITBYDIM",
    "CMS.INITBYPROB",
    "CMS.QUERY",
    "DECR",
    "DECRBY",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "GEOADD",
    "GEODIST",
    "GEOHASH",
    "GEOPOS",
    "GEOSEARCH",
    "GET",
    "GETBIT",
    "GETDEL",
    "GETEX",
    "GETRANGE",
    "GETSET",
    "HDEL",
    "HEXISTS",
    "HEXPIRE",
    "HEXPIREAT",
    "HEXPIRETIME",
    "HGET",
    "HGETALL",
    "HGETDEL",
    "HGETEX",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HPEXPIRE",
    "HPEXPIREAT",
    "HPERSIST",
    "HPTTL",
    "HRANDFIELD",
    "HSCAN",
    "HSET",
    "HSETEX",
    "HSETNX",
    "HSTRLEN",
    "HTTL",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LPOP",
    "LPOS",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PFADD",
    "PSETEX",
    "PTTL",
    "RPOP",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCARD",
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SISMEMBER",
    "SMEMBERS",
    "SMISMEMBER",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "STRLEN",
    "TDIGEST.ADD",
    "TDIGEST.BYRANK",
    "TDIGEST.BYREVRANK",
    "TDIGEST.CDF",
    "TDIGEST.CREATE",
    "TDIGEST.INFO",
    "TDIGEST.MAX",
    "TDIGEST.MIN",
    "TDIGEST.QUANTILE",
    "TDIGEST.RANK",
    "TDIGEST.RESET",
    "TDIGEST.REVRANK",
    "TDIGEST.TRIMMED_MEAN",
    "TOPK.ADD",
    "TOPK.COUNT",
    "TOPK.INCRBY",
    "TOPK.INFO",
    "TOPK.LIST",
    "TOPK.QUERY",
    "TOPK.RESERVE",
    "TTL",
    "TYPE",
    "XACK",
    "XADD",
    "XDEL",
    "XLEN",
    "XRANGE",
    "XREVRANGE",
    "XTRIM",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZINCRBY",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANDMEMBER",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZRANK",
    "ZREM",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
}


def command_route_keys(name: str, args: Sequence[Any]) -> tuple[Any, ...]:
    """Extract every routing key for a public command.

    An empty tuple means that the command is control-plane, global, or not yet
    safe to route directly. Returning all keys lets topology code enforce its
    single-slot contract before selecting a leader.
    """
    normalized = normalize_command_name(name)
    values = tuple(args)
    if normalized.startswith("FLOW."):
        flow_keys = _flow_command_route_keys(normalized, values)
        if flow_keys is not None:
            return flow_keys
    if normalized in _CONTROL_COMMANDS:
        return ()
    if normalized in _PAIR_KEYS_COMMANDS:
        return values[0::2]
    if normalized in _ALL_KEYS_COMMANDS:
        return values
    if normalized in _TWO_KEYS_COMMANDS:
        return values[:2]
    if normalized in {"BLPOP", "BRPOP"}:
        return values[:-1]
    if normalized == "BITOP":
        return values[1:]
    if normalized in {"BLMPOP", "SINTERCARD"}:
        offset = 2 if normalized == "BLMPOP" else 1
        count_index = 1 if normalized == "BLMPOP" else 0
        count = _non_negative_int(values[count_index] if len(values) > count_index else None)
        return values[offset : offset + count] if count is not None else ()
    if normalized in {"XREAD", "XREADGROUP"}:
        return _stream_keys(values)
    if normalized in {"CMS.MERGE", "TDIGEST.MERGE"}:
        count = _non_negative_int(values[1] if len(values) > 1 else None)
        return values[:1] + values[2 : 2 + count] if count is not None else ()
    if normalized in {"OBJECT", "XGROUP", "XINFO"}:
        return values[1:2]
    if normalized == "MEMORY":
        return values[1:2] if values and _command_token(values[0]) == "USAGE" else ()
    if normalized in _FIRST_KEY_COMMANDS:
        return values[:1]
    return ()


def flow_auto_partition_key(id: str | bytes) -> str:
    return flow_auto_partition_key_for_index(flow_auto_partition_index(id))


def flow_auto_partition_index(id: str | bytes) -> int:
    encoded = id if isinstance(id, bytes) else id.encode()
    return zlib.crc32(encoded) % FLOW_AUTO_PARTITION_BUCKETS


def flow_auto_partition_key_for_index(index: int) -> str:
    return f"{FLOW_AUTO_PARTITION_PREFIX}{index % FLOW_AUTO_PARTITION_BUCKETS}"


def _flow_command_route_keys(name: str, args: tuple[Any, ...]) -> tuple[Any, ...] | None:
    if name in _FLOW_POSITIONAL_PARTITION_COMMANDS and args:
        marker = normalize_command_name(args[0])
        if marker in {"AUTO", "MIXED", "NONE"}:
            return ()
        return args[:1]

    option_start = _FLOW_OPTION_STARTS.get(name)
    if option_start is not None:
        partition = _flow_partition_option(args, option_start)
        if partition is not None:
            return partition

    if name in _FLOW_AUTO_ID_COMMANDS and args and isinstance(args[0], (str, bytes)):
        return (flow_auto_partition_key(args[0]),)
    return None


def _flow_partition_option(
    args: tuple[Any, ...],
    start: int,
) -> tuple[Any, ...] | None:
    """Read PARTITION only at option boundaries, never from positional data."""
    index = start
    while index < len(args):
        token = normalize_command_name(args[index])
        if token == "PARTITION":
            return args[index + 1 : index + 2]
        if token in {"ITEMS", "ITEMS_EXT"}:
            return None
        width = flow_option_width(args, index)
        if width is None:
            return None
        index += width
    return None


def _stream_keys(args: tuple[Any, ...]) -> tuple[Any, ...]:
    streams_index = next(
        (
            index
            for index, value in enumerate(args)
            if (
                value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
            ).upper()
            == "STREAMS"
        ),
        None,
    )
    if streams_index is None:
        return ()
    remaining = args[streams_index + 1 :]
    return remaining[: len(remaining) // 2]


def _non_negative_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _command_token(value: Any) -> str:
    return normalize_command_name(value)


def normalize_command_name(value: Any) -> str:
    """Normalize string and byte command tokens consistently across SDK layers."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").upper()
    return str(value).upper()
