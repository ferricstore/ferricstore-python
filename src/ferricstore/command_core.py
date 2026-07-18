from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from ferricstore.command_grammar import parse_stream_read
from ferricstore.flow_routing import (
    flow_command_route_keys,
)

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
        return flow_command_route_keys(normalized, values)
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
        return _stream_keys(values, read_group=normalized == "XREADGROUP")
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


def _stream_keys(args: tuple[Any, ...], *, read_group: bool) -> tuple[Any, ...]:
    parsed = parse_stream_read(args, read_group=read_group)
    return parsed.keys if parsed.valid else ()


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
