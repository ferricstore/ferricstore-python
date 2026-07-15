from __future__ import annotations

import builtins
from typing import TYPE_CHECKING, Any

from ferricstore.codecs import Codec
from ferricstore.command_helpers import _expire_args, _getex_args, _set_args, _xread_args

if TYPE_CHECKING:
    from ferricstore.async_commands import AsyncDataCommandsMixin


class DataCommandsMixin:
    """Convenience methods for FerricStore data-structure commands.

    These methods are intentionally thin wrappers over ``command(...)``. Hot
    native protocol commands still use specialized opcodes; commands without a
    specialized opcode use the native generic command envelope.
    """

    codec: Codec

    def command(self, *args: Any) -> Any:  # pragma: no cover - protocol hook
        raise NotImplementedError

    def ping(self, message: Any | None = None) -> Any:
        return self.command("PING", message) if message is not None else self.command("PING")

    def echo(self, message: Any) -> Any:
        return self.command("ECHO", message)

    def set(self, key: str, value: Any, **kwargs: Any) -> Any:
        return self.command(*_set_args(self.codec, key, value, **kwargs))

    def delete(self, *keys: str) -> int:
        return int(self.command("DEL", *keys))

    def kv_get(self, key: str, *, decode: bool = True) -> Any:
        value = self.command("GET", key)
        return self.codec.decode(value) if decode else value

    def kv_set(self, key: str, value: Any, **kwargs: Any) -> Any:
        return self.command(*_set_args(self.codec, key, value, **kwargs))

    def kv_delete(self, *keys: str) -> int:
        return int(self.command("DEL", *keys))

    def exists(self, *keys: str) -> int:
        return int(self.command("EXISTS", *keys))

    def mget(self, *keys: str, decode: bool = True) -> builtins.list[Any]:
        values = list(self.command("MGET", *keys))
        return [self.codec.decode(value) if decode else value for value in values]

    def mset(self, mapping: dict[str, Any], *, encode: bool = True) -> Any:
        args: builtins.list[Any] = ["MSET"]
        for key, value in mapping.items():
            args.extend([key, self.codec.encode(value) if encode else value])
        return self.command(*args)

    def kv_mget(self, *keys: str, decode: bool = True) -> builtins.list[Any]:
        values = list(self.command("MGET", *keys))
        return [self.codec.decode(value) if decode else value for value in values]

    def kv_mset(self, mapping: dict[str, Any], *, encode: bool = True) -> Any:
        args: builtins.list[Any] = ["MSET"]
        for key, value in mapping.items():
            args.extend([key, self.codec.encode(value) if encode else value])
        return self.command(*args)

    def incr(self, key: str) -> int:
        return int(self.command("INCR", key))

    def decr(self, key: str) -> int:
        return int(self.command("DECR", key))

    def incrby(self, key: str, amount: int) -> int:
        return int(self.command("INCRBY", key, amount))

    def decrby(self, key: str, amount: int) -> int:
        return int(self.command("DECRBY", key, amount))

    def incrbyfloat(self, key: str, amount: float) -> Any:
        return self.command("INCRBYFLOAT", key, amount)

    def append(self, key: str, value: Any, *, encode: bool = True) -> int:
        return int(self.command("APPEND", key, self.codec.encode(value) if encode else value))

    def strlen(self, key: str) -> int:
        return int(self.command("STRLEN", key))

    def getdel(self, key: str, *, decode: bool = True) -> Any:
        value = self.command("GETDEL", key)
        return self.codec.decode(value) if decode else value

    def getex(self, key: str, *, decode: bool = True, **kwargs: Any) -> Any:
        value = self.command(*_getex_args(key, **kwargs))
        return self.codec.decode(value) if decode else value

    def setnx(self, key: str, value: Any, *, encode: bool = True) -> int:
        return int(self.command("SETNX", key, self.codec.encode(value) if encode else value))

    def setex(self, key: str, seconds: int, value: Any, *, encode: bool = True) -> Any:
        return self.command("SETEX", key, seconds, self.codec.encode(value) if encode else value)

    def psetex(self, key: str, ms: int, value: Any, *, encode: bool = True) -> Any:
        return self.command("PSETEX", key, ms, self.codec.encode(value) if encode else value)

    def getrange(self, key: str, start: int, end: int) -> Any:
        return self.command("GETRANGE", key, start, end)

    def setrange(self, key: str, offset: int, value: Any, *, encode: bool = True) -> int:
        return int(
            self.command("SETRANGE", key, offset, self.codec.encode(value) if encode else value)
        )

    def msetnx(self, mapping: dict[str, Any], *, encode: bool = True) -> int:
        args: builtins.list[Any] = ["MSETNX"]
        for key, value in mapping.items():
            args.extend([key, self.codec.encode(value) if encode else value])
        return int(self.command(*args))

    def expire(self, key: str, seconds: int, **kwargs: Any) -> int:
        return int(self.command(*_expire_args("EXPIRE", key, seconds, **kwargs)))

    def pexpire(self, key: str, ms: int, **kwargs: Any) -> int:
        return int(self.command(*_expire_args("PEXPIRE", key, ms, **kwargs)))

    def expireat(self, key: str, unix_seconds: int, **kwargs: Any) -> int:
        return int(self.command(*_expire_args("EXPIREAT", key, unix_seconds, **kwargs)))

    def pexpireat(self, key: str, unix_ms: int, **kwargs: Any) -> int:
        return int(self.command(*_expire_args("PEXPIREAT", key, unix_ms, **kwargs)))

    def ttl(self, key: str) -> int:
        return int(self.command("TTL", key))

    def pttl(self, key: str) -> int:
        return int(self.command("PTTL", key))

    def persist(self, key: str) -> int:
        return int(self.command("PERSIST", key))

    def expiretime(self, key: str) -> int:
        return int(self.command("EXPIRETIME", key))

    def pexpiretime(self, key: str) -> int:
        return int(self.command("PEXPIRETIME", key))

    def type(self, key: str) -> Any:
        return self.command("TYPE", key)

    def unlink(self, *keys: str) -> int:
        return int(self.command("UNLINK", *keys))

    def rename(self, key: str, new_key: str) -> Any:
        return self.command("RENAME", key, new_key)

    def renamenx(self, key: str, new_key: str) -> int:
        return int(self.command("RENAMENX", key, new_key))

    def copy(self, source: str, destination: str, *args: Any) -> int:
        return int(self.command("COPY", source, destination, *args))

    def randomkey(self) -> Any:
        return self.command("RANDOMKEY")

    def scan(self, cursor: int = 0, *args: Any) -> Any:
        return self.command("SCAN", cursor, *args)

    def object(self, subcommand: str, *args: Any) -> Any:
        return self.command("OBJECT", subcommand, *args)

    def hset(self, key: str, mapping: dict[str, Any] | None = None, **fields: Any) -> int:
        data = dict(mapping or {})
        data.update(fields)
        args: builtins.list[Any] = ["HSET", key]
        for field, value in data.items():
            args.extend([field, self.codec.encode(value)])
        return int(self.command(*args))

    def hget(self, key: str, field: str, *, decode: bool = True) -> Any:
        value = self.command("HGET", key, field)
        return self.codec.decode(value) if decode else value

    def hmget(self, key: str, *fields: str, decode: bool = True) -> builtins.list[Any]:
        values = list(self.command("HMGET", key, *fields))
        return [self.codec.decode(value) if decode else value for value in values]

    def hgetall(self, key: str, *, decode: bool = True) -> dict[Any, Any]:
        raw = self.command("HGETALL", key)
        if isinstance(raw, dict):
            return {k: self.codec.decode(v) if decode else v for k, v in raw.items()}
        items = list(raw or [])
        return {
            items[i]: self.codec.decode(items[i + 1]) if decode else items[i + 1]
            for i in range(0, len(items), 2)
        }

    def hdel(self, key: str, *fields: str) -> int:
        return int(self.command("HDEL", key, *fields))

    def hexists(self, key: str, field: str) -> int:
        return int(self.command("HEXISTS", key, field))

    def hkeys(self, key: str) -> Any:
        return self.command("HKEYS", key)

    def hvals(self, key: str, *, decode: bool = True) -> builtins.list[Any]:
        values = list(self.command("HVALS", key))
        return [self.codec.decode(value) if decode else value for value in values]

    def hlen(self, key: str) -> int:
        return int(self.command("HLEN", key))

    def hincrby(self, key: str, field: str, amount: int) -> int:
        return int(self.command("HINCRBY", key, field, amount))

    def hincrbyfloat(self, key: str, field: str, amount: float) -> Any:
        return self.command("HINCRBYFLOAT", key, field, amount)

    def hsetnx(self, key: str, field: str, value: Any, *, encode: bool = True) -> int:
        return int(
            self.command("HSETNX", key, field, self.codec.encode(value) if encode else value)
        )

    def hstrlen(self, key: str, field: str) -> int:
        return int(self.command("HSTRLEN", key, field))

    def hrandfield(self, key: str, *args: Any) -> Any:
        return self.command("HRANDFIELD", key, *args)

    def hscan(self, key: str, cursor: int = 0, *args: Any) -> Any:
        return self.command("HSCAN", key, cursor, *args)

    def httl(self, key: str, *fields: str) -> Any:
        return self.command("HTTL", key, "FIELDS", len(fields), *fields)

    def hpttl(self, key: str, *fields: str) -> Any:
        return self.command("HPTTL", key, "FIELDS", len(fields), *fields)

    def hpersist(self, key: str, *fields: str) -> Any:
        return self.command("HPERSIST", key, "FIELDS", len(fields), *fields)

    def hexpire(self, key: str, seconds: int, *fields: str) -> Any:
        return self.command("HEXPIRE", key, seconds, "FIELDS", len(fields), *fields)

    def hpexpire(self, key: str, ms: int, *fields: str) -> Any:
        return self.command("HPEXPIRE", key, ms, "FIELDS", len(fields), *fields)

    def hexpiretime(self, key: str, *fields: str) -> Any:
        return self.command("HEXPIRETIME", key, "FIELDS", len(fields), *fields)

    def hgetdel(self, key: str, *fields: str) -> Any:
        return self.command("HGETDEL", key, "FIELDS", len(fields), *fields)

    def hgetex(self, key: str, *args: Any) -> Any:
        return self.command("HGETEX", key, *args)

    def hsetex(self, key: str, *args: Any) -> Any:
        return self.command("HSETEX", key, *args)

    def lpush(self, key: str, *values: Any, encode: bool = True) -> int:
        return int(
            self.command("LPUSH", key, *[self.codec.encode(v) if encode else v for v in values])
        )

    def rpush(self, key: str, *values: Any, encode: bool = True) -> int:
        return int(
            self.command("RPUSH", key, *[self.codec.encode(v) if encode else v for v in values])
        )

    def lpop(self, key: str, count: int | None = None) -> Any:
        return self.command("LPOP", key, *([] if count is None else [count]))

    def rpop(self, key: str, count: int | None = None) -> Any:
        return self.command("RPOP", key, *([] if count is None else [count]))

    def lrange(self, key: str, start: int, stop: int) -> Any:
        return self.command("LRANGE", key, start, stop)

    def llen(self, key: str) -> int:
        return int(self.command("LLEN", key))

    def lindex(self, key: str, index: int) -> Any:
        return self.command("LINDEX", key, index)

    def lset(self, key: str, index: int, value: Any, *, encode: bool = True) -> Any:
        return self.command("LSET", key, index, self.codec.encode(value) if encode else value)

    def lrem(self, key: str, count: int, value: Any, *, encode: bool = True) -> int:
        return int(self.command("LREM", key, count, self.codec.encode(value) if encode else value))

    def ltrim(self, key: str, start: int, stop: int) -> Any:
        return self.command("LTRIM", key, start, stop)

    def lpos(self, key: str, value: Any, *args: Any, encode: bool = True) -> Any:
        return self.command("LPOS", key, self.codec.encode(value) if encode else value, *args)

    def linsert(self, key: str, where: str, pivot: Any, value: Any, *, encode: bool = True) -> int:
        return int(
            self.command(
                "LINSERT",
                key,
                where,
                self.codec.encode(pivot) if encode else pivot,
                self.codec.encode(value) if encode else value,
            )
        )

    def lmove(self, source: str, destination: str, wherefrom: str, whereto: str) -> Any:
        return self.command("LMOVE", source, destination, wherefrom, whereto)

    def blpop(self, *keys: str, timeout: float | int = 0) -> Any:
        return self.command("BLPOP", *keys, timeout)

    def brpop(self, *keys: str, timeout: float | int = 0) -> Any:
        return self.command("BRPOP", *keys, timeout)

    def blmove(
        self, source: str, destination: str, wherefrom: str, whereto: str, timeout: float | int = 0
    ) -> Any:
        return self.command("BLMOVE", source, destination, wherefrom, whereto, timeout)

    def blmpop(
        self,
        timeout: float | int,
        keys: builtins.list[str],
        direction: str,
        *,
        count: int | None = None,
    ) -> Any:
        args: builtins.list[Any] = ["BLMPOP", timeout, len(keys), *keys, direction]
        if count is not None:
            args.extend(["COUNT", count])
        return self.command(*args)

    def lpushx(self, key: str, *values: Any, encode: bool = True) -> int:
        return int(
            self.command("LPUSHX", key, *[self.codec.encode(v) if encode else v for v in values])
        )

    def rpushx(self, key: str, *values: Any, encode: bool = True) -> int:
        return int(
            self.command("RPUSHX", key, *[self.codec.encode(v) if encode else v for v in values])
        )

    def rpoplpush(self, source: str, destination: str) -> Any:
        return self.command("RPOPLPUSH", source, destination)

    def sadd(self, key: str, *members: Any, encode: bool = True) -> int:
        return int(
            self.command("SADD", key, *[self.codec.encode(m) if encode else m for m in members])
        )

    def srem(self, key: str, *members: Any, encode: bool = True) -> int:
        return int(
            self.command("SREM", key, *[self.codec.encode(m) if encode else m for m in members])
        )

    def smembers(self, key: str) -> Any:
        return self.command("SMEMBERS", key)

    def sismember(self, key: str, member: Any, *, encode: bool = True) -> int:
        return int(self.command("SISMEMBER", key, self.codec.encode(member) if encode else member))

    def smismember(self, key: str, *members: Any, encode: bool = True) -> Any:
        return self.command(
            "SMISMEMBER", key, *[self.codec.encode(m) if encode else m for m in members]
        )

    def scard(self, key: str) -> int:
        return int(self.command("SCARD", key))

    def sinter(self, *keys: str) -> Any:
        return self.command("SINTER", *keys)

    def sunion(self, *keys: str) -> Any:
        return self.command("SUNION", *keys)

    def sdiff(self, *keys: str) -> Any:
        return self.command("SDIFF", *keys)

    def sdiffstore(self, destination: str, *keys: str) -> int:
        return int(self.command("SDIFFSTORE", destination, *keys))

    def sinterstore(self, destination: str, *keys: str) -> int:
        return int(self.command("SINTERSTORE", destination, *keys))

    def sunionstore(self, destination: str, *keys: str) -> int:
        return int(self.command("SUNIONSTORE", destination, *keys))

    def sintercard(self, *args: Any) -> int:
        return int(self.command("SINTERCARD", *args))

    def srandmember(self, key: str, count: int | None = None) -> Any:
        return self.command("SRANDMEMBER", key, *([] if count is None else [count]))

    def spop(self, key: str, count: int | None = None) -> Any:
        return self.command("SPOP", key, *([] if count is None else [count]))

    def smove(self, source: str, destination: str, member: Any, *, encode: bool = True) -> int:
        return int(
            self.command(
                "SMOVE", source, destination, self.codec.encode(member) if encode else member
            )
        )

    def sscan(self, key: str, cursor: int = 0, *args: Any) -> Any:
        return self.command("SSCAN", key, cursor, *args)

    def zadd(self, key: str, mapping: dict[Any, float]) -> int:
        args: builtins.list[Any] = ["ZADD", key]
        for member, score in mapping.items():
            args.extend([score, member])
        return int(self.command(*args))

    def zrem(self, key: str, *members: Any) -> int:
        return int(self.command("ZREM", key, *members))

    def zscore(self, key: str, member: Any) -> Any:
        return self.command("ZSCORE", key, member)

    def zrange(self, key: str, start: int, stop: int, *args: Any) -> Any:
        return self.command("ZRANGE", key, start, stop, *args)

    def zrevrange(self, key: str, start: int, stop: int, *args: Any) -> Any:
        return self.command("ZREVRANGE", key, start, stop, *args)

    def zcard(self, key: str) -> int:
        return int(self.command("ZCARD", key))

    def zincrby(self, key: str, amount: float, member: Any) -> Any:
        return self.command("ZINCRBY", key, amount, member)

    def zcount(self, key: str, min: Any, max: Any) -> int:
        return int(self.command("ZCOUNT", key, min, max))

    def zrank(self, key: str, member: Any) -> Any:
        return self.command("ZRANK", key, member)

    def zrevrank(self, key: str, member: Any) -> Any:
        return self.command("ZREVRANK", key, member)

    def zmscore(self, key: str, *members: Any) -> Any:
        return self.command("ZMSCORE", key, *members)

    def zpopmin(self, key: str, count: int | None = None) -> Any:
        return self.command("ZPOPMIN", key, *([] if count is None else [count]))

    def zpopmax(self, key: str, count: int | None = None) -> Any:
        return self.command("ZPOPMAX", key, *([] if count is None else [count]))

    def zrandmember(self, key: str, *args: Any) -> Any:
        return self.command("ZRANDMEMBER", key, *args)

    def zscan(self, key: str, cursor: int = 0, *args: Any) -> Any:
        return self.command("ZSCAN", key, cursor, *args)

    def zrangebyscore(self, key: str, min: Any, max: Any, *args: Any) -> Any:
        return self.command("ZRANGEBYSCORE", key, min, max, *args)

    def zrevrangebyscore(self, key: str, max: Any, min: Any, *args: Any) -> Any:
        return self.command("ZREVRANGEBYSCORE", key, max, min, *args)

    def setbit(self, key: str, offset: int, value: int) -> int:
        return int(self.command("SETBIT", key, offset, value))

    def getbit(self, key: str, offset: int) -> int:
        return int(self.command("GETBIT", key, offset))

    def bitcount(self, key: str, *args: Any) -> int:
        return int(self.command("BITCOUNT", key, *args))

    def bitpos(self, key: str, bit: int, *args: Any) -> int:
        return int(self.command("BITPOS", key, bit, *args))

    def bitop(self, operation: str, destkey: str, *keys: str) -> int:
        return int(self.command("BITOP", operation, destkey, *keys))

    def pfadd(self, key: str, *elements: Any) -> int:
        return int(self.command("PFADD", key, *elements))

    def pfcount(self, *keys: str) -> int:
        return int(self.command("PFCOUNT", *keys))

    def pfmerge(self, destkey: str, *sourcekeys: str) -> Any:
        return self.command("PFMERGE", destkey, *sourcekeys)

    def geoadd(self, key: str, *longitude_latitude_member: Any) -> int:
        return int(self.command("GEOADD", key, *longitude_latitude_member))

    def geopos(self, key: str, *members: Any) -> Any:
        return self.command("GEOPOS", key, *members)

    def geodist(self, key: str, member1: Any, member2: Any, unit: str | None = None) -> Any:
        return self.command("GEODIST", key, member1, member2, *([] if unit is None else [unit]))

    def geohash(self, key: str, *members: Any) -> Any:
        return self.command("GEOHASH", key, *members)

    def geosearch(self, key: str, *args: Any) -> Any:
        return self.command("GEOSEARCH", key, *args)

    def geosearchstore(self, destination: str, source: str, *args: Any) -> Any:
        return self.command("GEOSEARCHSTORE", destination, source, *args)

    def xadd(
        self, key: str, fields: dict[str, Any], *, id: str = "*", encode: bool = True, **opts: Any
    ) -> Any:
        args: builtins.list[Any] = ["XADD", key]
        if "maxlen" in opts:
            args.extend(["MAXLEN", opts["maxlen"]])
        if "minid" in opts:
            args.extend(["MINID", opts["minid"]])
        args.append(id)
        for field, value in fields.items():
            args.extend([field, self.codec.encode(value) if encode else value])
        return self.command(*args)

    def xlen(self, key: str) -> int:
        return int(self.command("XLEN", key))

    def xrange(self, key: str, start: str = "-", end: str = "+", *args: Any) -> Any:
        return self.command("XRANGE", key, start, end, *args)

    def xrevrange(self, key: str, end: str = "+", start: str = "-", *args: Any) -> Any:
        return self.command("XREVRANGE", key, end, start, *args)

    def xread(
        self, streams: dict[str, str], *, count: int | None = None, block_ms: int | None = None
    ) -> Any:
        return self.command(*_xread_args("XREAD", streams, count=count, block_ms=block_ms))

    def xtrim(self, key: str, *args: Any) -> Any:
        return self.command("XTRIM", key, *args)

    def xdel(self, key: str, *ids: str) -> int:
        return int(self.command("XDEL", key, *ids))

    def xinfo(self, subcommand: str, key: str, *args: Any) -> Any:
        return self.command("XINFO", subcommand, key, *args)

    def xgroup(self, subcommand: str, key: str, group: str, *args: Any) -> Any:
        return self.command("XGROUP", subcommand, key, group, *args)

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block_ms: int | None = None,
    ) -> Any:
        return self.command(
            *_xread_args(
                "XREADGROUP",
                streams,
                count=count,
                block_ms=block_ms,
                group=group,
                consumer=consumer,
            )
        )

    def xack(self, key: str, group: str, *ids: str) -> int:
        return int(self.command("XACK", key, group, *ids))

    def publish(self, channel: str, message: Any, *, encode: bool = True) -> int:
        return int(
            self.command("PUBLISH", channel, self.codec.encode(message) if encode else message)
        )

    def subscribe(self, *channels: str) -> Any:
        return self.command("SUBSCRIBE", *channels)

    def unsubscribe(self, *channels: str) -> Any:
        return self.command("UNSUBSCRIBE", *channels)

    def psubscribe(self, *patterns: str) -> Any:
        return self.command("PSUBSCRIBE", *patterns)

    def punsubscribe(self, *patterns: str) -> Any:
        return self.command("PUNSUBSCRIBE", *patterns)

    def pubsub(self, subcommand: str, *args: Any) -> Any:
        return self.command("PUBSUB", subcommand, *args)

    def multi(self) -> Any:
        return self.command("MULTI")

    def transaction_exec(self) -> Any:
        return self.command("EXEC")

    def discard(self) -> Any:
        return self.command("DISCARD")

    def watch(self, *keys: str) -> Any:
        return self.command("WATCH", *keys)

    def unwatch(self) -> Any:
        return self.command("UNWATCH")

    def bf_reserve(self, key: str, error_rate: float, capacity: int, *args: Any) -> Any:
        return self.command("BF.RESERVE", key, error_rate, capacity, *args)

    def bf_add(self, key: str, item: Any) -> int:
        return int(self.command("BF.ADD", key, item))

    def bf_madd(self, key: str, *items: Any) -> Any:
        return self.command("BF.MADD", key, *items)

    def bf_exists(self, key: str, item: Any) -> int:
        return int(self.command("BF.EXISTS", key, item))

    def bf_mexists(self, key: str, *items: Any) -> Any:
        return self.command("BF.MEXISTS", key, *items)

    def bf_card(self, key: str) -> int:
        return int(self.command("BF.CARD", key))

    def bf_info(self, key: str) -> Any:
        return self.command("BF.INFO", key)

    def cf_reserve(self, key: str, capacity: int, *args: Any) -> Any:
        return self.command("CF.RESERVE", key, capacity, *args)

    def cf_add(self, key: str, item: Any) -> int:
        return int(self.command("CF.ADD", key, item))

    def cf_addnx(self, key: str, item: Any) -> int:
        return int(self.command("CF.ADDNX", key, item))

    def cf_del(self, key: str, item: Any) -> int:
        return int(self.command("CF.DEL", key, item))

    def cf_exists(self, key: str, item: Any) -> int:
        return int(self.command("CF.EXISTS", key, item))

    def cf_mexists(self, key: str, *items: Any) -> Any:
        return self.command("CF.MEXISTS", key, *items)

    def cf_count(self, key: str, item: Any) -> int:
        return int(self.command("CF.COUNT", key, item))

    def cf_info(self, key: str) -> Any:
        return self.command("CF.INFO", key)

    def cms_initbydim(self, key: str, width: int, depth: int) -> Any:
        return self.command("CMS.INITBYDIM", key, width, depth)

    def cms_initbyprob(self, key: str, error: float, probability: float) -> Any:
        return self.command("CMS.INITBYPROB", key, error, probability)

    def cms_incrby(self, key: str, *item_increment_pairs: Any) -> Any:
        return self.command("CMS.INCRBY", key, *item_increment_pairs)

    def cms_query(self, key: str, *items: Any) -> Any:
        return self.command("CMS.QUERY", key, *items)

    def cms_merge(self, dest: str, *args: Any) -> Any:
        return self.command("CMS.MERGE", dest, *args)

    def cms_info(self, key: str) -> Any:
        return self.command("CMS.INFO", key)

    def topk_reserve(self, key: str, k: int, *args: Any) -> Any:
        return self.command("TOPK.RESERVE", key, k, *args)

    def topk_add(self, key: str, *items: Any) -> Any:
        return self.command("TOPK.ADD", key, *items)

    def topk_incrby(self, key: str, *item_increment_pairs: Any) -> Any:
        return self.command("TOPK.INCRBY", key, *item_increment_pairs)

    def topk_query(self, key: str, *items: Any) -> Any:
        return self.command("TOPK.QUERY", key, *items)

    def topk_list(self, key: str, *args: Any) -> Any:
        return self.command("TOPK.LIST", key, *args)

    def topk_count(self, key: str, *items: Any) -> Any:
        return self.command("TOPK.COUNT", key, *items)

    def topk_info(self, key: str) -> Any:
        return self.command("TOPK.INFO", key)

    def tdigest_create(self, key: str, *args: Any) -> Any:
        return self.command("TDIGEST.CREATE", key, *args)

    def tdigest_add(self, key: str, *values: float) -> Any:
        return self.command("TDIGEST.ADD", key, *values)

    def tdigest_reset(self, key: str) -> Any:
        return self.command("TDIGEST.RESET", key)

    def tdigest_quantile(self, key: str, *quantiles: float) -> Any:
        return self.command("TDIGEST.QUANTILE", key, *quantiles)

    def tdigest_cdf(self, key: str, *values: float) -> Any:
        return self.command("TDIGEST.CDF", key, *values)

    def tdigest_rank(self, key: str, *values: float) -> Any:
        return self.command("TDIGEST.RANK", key, *values)

    def tdigest_revrank(self, key: str, *values: float) -> Any:
        return self.command("TDIGEST.REVRANK", key, *values)

    def tdigest_byrank(self, key: str, *ranks: int) -> Any:
        return self.command("TDIGEST.BYRANK", key, *ranks)

    def tdigest_byrevrank(self, key: str, *ranks: int) -> Any:
        return self.command("TDIGEST.BYREVRANK", key, *ranks)

    def tdigest_trimmed_mean(self, key: str, low: float, high: float) -> Any:
        return self.command("TDIGEST.TRIMMED_MEAN", key, low, high)

    def tdigest_min(self, key: str) -> Any:
        return self.command("TDIGEST.MIN", key)

    def tdigest_max(self, key: str) -> Any:
        return self.command("TDIGEST.MAX", key)

    def tdigest_info(self, key: str) -> Any:
        return self.command("TDIGEST.INFO", key)

    def tdigest_merge(self, destination: str, numkeys: int, *args: Any) -> Any:
        return self.command("TDIGEST.MERGE", destination, numkeys, *args)

    def dbsize(self) -> int:
        return int(self.command("DBSIZE"))

    def keys(self, pattern: str = "*") -> Any:
        return self.command("KEYS", pattern)

    def flushdb(self) -> Any:
        return self.command("FLUSHDB")

    def flushall(self) -> Any:
        return self.command("FLUSHALL")

    def server_info(self, section: str | None = None) -> Any:
        return self.command("INFO", *([] if section is None else [section]))

    def command_info(self, *names: str) -> Any:
        return self.command("COMMAND", "INFO", *names)

    def slowlog(self, subcommand: str, *args: Any) -> Any:
        return self.command("SLOWLOG", subcommand, *args)

    def memory(self, subcommand: str, *args: Any) -> Any:
        return self.command("MEMORY", subcommand, *args)

    def config(self, subcommand: str, *args: Any) -> Any:
        return self.command("CONFIG", subcommand, *args)

    def select(self, db: int) -> Any:
        return self.command("SELECT", db)


def __getattr__(name: str) -> Any:
    if name == "AsyncDataCommandsMixin":
        from ferricstore.async_commands import AsyncDataCommandsMixin

        return AsyncDataCommandsMixin
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["AsyncDataCommandsMixin", "DataCommandsMixin"]
