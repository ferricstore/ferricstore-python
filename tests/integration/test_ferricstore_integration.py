from __future__ import annotations

import os
import time
import uuid
from contextlib import suppress
from typing import Any

import pytest

from ferricstore import (
    ChildSpec,
    ClaimedItem,
    CreateItem,
    FencedItem,
    FlowClient,
    JsonCodec,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_INTEGRATION") != "1",
    reason="set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests",
)


def _client() -> FlowClient:
    return FlowClient.from_url(
        os.environ.get("FERRICSTORE_URL", "redis://127.0.0.1:6379/0"),
        codec=JsonCodec(),
    )


def _suffix() -> str:
    return uuid.uuid4().hex


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _ok(value: Any) -> bool:
    return value in (True, b"OK", "OK", 1)


def _decode(client: FlowClient, value: Any) -> Any:
    return client.codec.decode(value) if isinstance(value, bytes) else value


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, value.get(name.encode(), default))
    return default


def _event_id(event: Any) -> str:
    if isinstance(event, (list, tuple)) and event:
        return _text(event[0])
    event_id = _field(event, "event_id", _field(event, "id"))
    if event_id is None:
        raise AssertionError(f"history event does not contain an event id: {event!r}")
    return _text(event_id)


def _fenced(job: ClaimedItem) -> FencedItem:
    return FencedItem(
        id=job.id,
        fencing_token=job.fencing_token,
        lease_token=job.lease_token,
        partition_key=job.partition_key,
    )


def _claim_one(
    client: FlowClient,
    flow_type: str,
    state: str,
    partition: str,
    *,
    worker: str = "py-sdk-integration-worker",
    now_ms: int | None = None,
    lease_ms: int = 30_000,
    include_state: bool = False,
) -> ClaimedItem:
    jobs = client.claim_jobs(
        flow_type,
        state=state,
        worker=worker,
        partition_key=partition,
        limit=1,
        lease_ms=lease_ms,
        now_ms=now_ms,
        priority=None,
        include_state=include_state,
    )
    assert len(jobs) == 1
    return jobs[0]


def _create_and_claim(
    client: FlowClient,
    flow_type: str,
    suffix: str,
    name: str,
    *,
    state: str = "queued",
    now_ms: int | None = None,
    lease_ms: int = 30_000,
) -> tuple[str, str, ClaimedItem]:
    flow_id = f"py-sdk:{name}:{suffix}"
    partition = f"{flow_id}:partition"
    client.create(
        flow_id,
        type=flow_type,
        state=state,
        partition_key=partition,
        payload={"name": name},
        now_ms=now_ms,
        run_at_ms=now_ms,
        idempotent=True,
    )
    return (
        flow_id,
        partition,
        _claim_one(
            client,
            flow_type,
            state,
            partition,
            now_ms=now_ms,
            lease_ms=lease_ms,
            include_state=True,
        ),
    )


def _delete_prefixed_keys(client: FlowClient, prefix: str) -> None:
    with suppress(Exception):
        keys = client.command("KEYS", f"{prefix}*")
        if keys:
            client.command("DEL", *keys)


def test_real_ferricstore_command_and_flow_cycle() -> None:
    client = _client()
    suffix = _suffix()
    key = f"py-sdk:kv:{suffix}"
    flow_id = f"py-sdk:flow:{suffix}"
    flow_type = "py-sdk-integration"

    try:
        assert client.command("SET", key, "value") in (True, b"OK", "OK")
        assert client.command("GET", key) in (b"value", "value")

        client.create(
            flow_id,
            type=flow_type,
            state="queued",
            partition_key=flow_id,
            payload={"hello": "world"},
            idempotent=True,
        )

        job = _claim_one(client, flow_type, "queued", flow_id)
        assert job.id == flow_id
        assert job.partition_key == flow_id
        assert job.lease_token
        assert job.fencing_token > 0

        client.complete(
            job.id,
            lease_token=job.lease_token,
            fencing_token=job.fencing_token,
            partition_key=job.partition_key,
            result={"ok": True},
        )

        record = client.get(flow_id, partition_key=flow_id)
        assert record is not None
        assert record.state == "completed"
    finally:
        with suppress(Exception):
            client.command("DEL", key)
        client.close()


def test_real_ferricstore_native_helpers_and_diagnostics() -> None:
    client = _client()
    suffix = _suffix()
    prefix = f"py-sdk:native:{suffix}:"
    key = f"{prefix}cas"
    lock_key = f"{prefix}lock"
    rate_key = f"{prefix}rate"
    cache_key = f"{prefix}cache"

    try:
        assert client.command("PING") in (b"PONG", "PONG", True)
        assert client.command("ECHO", "hello") in (b"hello", "hello")

        results = (
            client.pipeline()
            .command("SET", key, client.codec.encode("old"))
            .command("GET", key)
            .execute()
        )
        assert _decode(client, results[-1]) == "old"

        assert client.cas(key, "old", "new") is True
        assert _decode(client, client.command("GET", key)) == "new"

        assert client.lock(lock_key, "owner-a", 30_000) is True
        assert client.extend_lock(lock_key, "owner-a", 30_000) == 1
        assert client.unlock(lock_key, "owner-a") == 1

        rate = client.ratelimit_add(rate_key, window_ms=60_000, max=5, count=2)
        assert rate.count >= 1
        assert rate.remaining >= 0

        info = client.key_info(key)
        assert info.type in {"string", "binary", "unknown", ""}
        assert info.raw

        first = client.fetch_or_compute(cache_key, ttl_ms=60_000, hint="integration")
        assert first.should_compute
        assert client.fetch_or_compute_result(cache_key, {"computed": True}, ttl_ms=60_000)
        cached = client.fetch_or_compute(cache_key, ttl_ms=60_000)
        assert cached.hit
        assert cached.value == {"computed": True}

        error_key = f"{prefix}cache-error"
        first_error = client.fetch_or_compute(error_key, ttl_ms=60_000)
        assert first_error.should_compute
        assert client.fetch_or_compute_error(error_key, "boom")

        assert isinstance(client.cluster_health(), dict)
        assert isinstance(client.cluster_stats(), dict)
        assert isinstance(client.cluster_keyslot(key), int)
        assert client.cluster_slots() is not None
        assert isinstance(client.cluster_status(), dict)
        assert client.cluster_role() is not None
        assert client.ferricstore_config("GET", "*") is not None
        assert isinstance(client.ferricstore_metrics(), dict)
        assert isinstance(client.ferricstore_hotness(), dict)
    finally:
        _delete_prefixed_keys(client, prefix)
        client.close()


def test_real_ferricstore_raw_store_command_families() -> None:
    client = _client()
    suffix = _suffix()
    prefix = f"py-sdk:store:{suffix}:"

    try:
        string_key = f"{prefix}string"
        second_key = f"{prefix}string2"
        third_key = f"{prefix}string3"
        assert _ok(client.command("SET", string_key, "abc", "PX", 60_000))
        assert client.command("EXISTS", string_key) == 1
        assert client.command("MGET", string_key, f"{prefix}missing")[0] in (b"abc", "abc")
        assert _ok(client.command("MSET", second_key, "2", third_key, "3"))
        assert client.command("MSETNX", f"{prefix}nx1", "1", f"{prefix}nx2", "2") == 1
        assert client.command("INCR", f"{prefix}counter") == 1
        assert client.command("INCRBY", f"{prefix}counter", 4) == 5
        assert client.command("DECR", f"{prefix}counter") == 4
        assert client.command("DECRBY", f"{prefix}counter", 2) == 2
        assert float(_text(client.command("INCRBYFLOAT", f"{prefix}float", "1.5"))) >= 1.5
        assert client.command("APPEND", f"{prefix}append", "abc") == 3
        assert client.command("STRLEN", f"{prefix}append") == 3
        assert client.command("GETSET", f"{prefix}append", "xyz") in (b"abc", "abc")
        assert client.command("GETRANGE", f"{prefix}append", 0, 1) in (b"xy", "xy")
        assert client.command("SETRANGE", f"{prefix}append", 1, "Q") == 3
        assert client.command("GETEX", f"{prefix}append", "PX", 60_000) in (b"xQz", "xQz")
        assert client.command("TTL", f"{prefix}append") >= 0
        assert client.command("PTTL", f"{prefix}append") >= 0
        assert client.command("PERSIST", f"{prefix}append") in (0, 1)
        assert client.command("EXPIRE", f"{prefix}append", 60) == 1
        assert client.command("PEXPIRE", f"{prefix}append", 60_000) == 1
        assert client.command("EXPIREAT", f"{prefix}append", int(time.time()) + 60) == 1
        assert client.command("PEXPIREAT", f"{prefix}append", int(time.time() * 1000) + 60_000) == 1
        assert client.command("EXPIRETIME", f"{prefix}append") >= 0
        assert client.command("PEXPIRETIME", f"{prefix}append") >= 0
        assert client.command("TYPE", f"{prefix}append") in (b"string", "string")
        assert client.command("SETNX", f"{prefix}setnx", "1") == 1
        assert _ok(client.command("SETEX", f"{prefix}setex", 60, "1"))
        assert _ok(client.command("PSETEX", f"{prefix}psetex", 60_000, "1"))
        assert client.command("COPY", string_key, f"{prefix}copy", "REPLACE") == 1
        assert _ok(client.command("RENAME", f"{prefix}copy", f"{prefix}renamed"))
        assert client.command("RENAMENX", f"{prefix}renamed", f"{prefix}renamed-nx") == 1
        assert client.command("RANDOMKEY") is not None
        assert client.command("KEYS", f"{prefix}*")
        assert client.command("DBSIZE") >= 1
        assert client.command("OBJECT", "ENCODING", string_key) is not None
        assert client.command("OBJECT", "HELP")
        assert client.command("OBJECT", "FREQ", string_key) >= 0
        assert client.command("OBJECT", "IDLETIME", string_key) >= 0
        assert client.command("OBJECT", "REFCOUNT", string_key) == 1
        assert client.command("WAIT", 0, 1) == 0
        assert client.command("WAITAOF", 0, 0, 1) is not None
        assert client.command("MEMORY", "USAGE", string_key) >= 0
        assert client.command("GETDEL", f"{prefix}setnx") in (b"1", "1")
        assert client.command("UNLINK", f"{prefix}nx1") >= 0

        hash_key = f"{prefix}hash"
        assert client.command("HSET", hash_key, "field", "value", "count", "1") >= 1
        assert client.command("HGET", hash_key, "field") in (b"value", "value")
        assert client.command("HMGET", hash_key, "field", "none")[0] in (b"value", "value")
        assert client.command("HGETALL", hash_key)
        assert client.command("HEXISTS", hash_key, "field") == 1
        assert client.command("HKEYS", hash_key)
        assert client.command("HVALS", hash_key)
        assert client.command("HLEN", hash_key) >= 2
        assert client.command("HINCRBY", hash_key, "count", 2) == 3
        assert float(_text(client.command("HINCRBYFLOAT", hash_key, "float", "1.25"))) >= 1.25
        assert client.command("HSETNX", hash_key, "new", "item") == 1
        assert client.command("HSTRLEN", hash_key, "field") == 5
        assert client.command("HRANDFIELD", hash_key, 1, "WITHVALUES")
        assert client.command("HEXPIRE", hash_key, 60, "FIELDS", 1, "field")[0] in (1, -1)
        assert client.command("HTTL", hash_key, "FIELDS", 1, "field")
        assert client.command("HPERSIST", hash_key, "FIELDS", 1, "field")
        assert client.command("HPEXPIRE", hash_key, 60_000, "FIELDS", 1, "field")[0] in (1, -1)
        assert client.command("HPTTL", hash_key, "FIELDS", 1, "field")
        assert client.command("HEXPIRETIME", hash_key, "FIELDS", 1, "field")
        assert client.command("HGETEX", hash_key, "PX", 60_000, "FIELDS", 1, "field")[0] in (
            b"value",
            "value",
        )
        assert client.command("HSETEX", hash_key, 60, "temp", "1") >= 0
        assert client.command("HGETDEL", hash_key, "FIELDS", 1, "temp")[0] in (b"1", "1")
        assert client.command("HDEL", hash_key, "new") == 1

        list_key = f"{prefix}list"
        list_dst = f"{prefix}list-dst"
        assert client.command("LPUSH", list_key, "b", "a") == 2
        assert client.command("RPUSH", list_key, "c") == 3
        assert client.command("LRANGE", list_key, 0, -1)
        assert client.command("LLEN", list_key) == 3
        assert client.command("LINDEX", list_key, 0) in (b"a", "a")
        assert _ok(client.command("LSET", list_key, 1, "bb"))
        assert client.command("LREM", list_key, 0, "bb") == 1
        assert _ok(client.command("LTRIM", list_key, 0, 1))
        assert client.command("LPOS", list_key, "a") == 0
        assert client.command("LINSERT", list_key, "AFTER", "a", "aa") >= 0
        assert client.command("LMOVE", list_key, list_dst, "LEFT", "RIGHT") is not None
        assert client.command("RPOPLPUSH", list_dst, list_key) is not None
        assert client.command("LPUSHX", list_key, "left") >= 1
        assert client.command("RPUSHX", list_key, "right") >= 1
        assert client.command("BLPOP", list_key, 1) is not None
        assert client.command("RPUSH", list_key, "block") >= 1
        assert client.command("BRPOP", list_key, 1) is not None
        assert client.command("RPUSH", list_key, "move") >= 1
        assert client.command("BLMOVE", list_key, list_dst, "LEFT", "RIGHT", 1) is not None
        assert client.command("RPUSH", list_key, "mpop") >= 1
        assert client.command("BLMPOP", 1, 1, list_key, "LEFT", "COUNT", 1) is not None

        set_a = f"{prefix}set-a"
        set_b = f"{prefix}set-b"
        assert client.command("SADD", set_a, "a", "b") == 2
        assert client.command("SADD", set_b, "b", "c") == 2
        assert client.command("SISMEMBER", set_a, "a") == 1
        assert client.command("SMISMEMBER", set_a, "a", "z")
        assert client.command("SCARD", set_a) == 2
        assert client.command("SMEMBERS", set_a)
        assert client.command("SRANDMEMBER", set_a, 1)
        assert client.command("SDIFF", set_a, set_b)
        assert client.command("SINTER", set_a, set_b)
        assert client.command("SUNION", set_a, set_b)
        assert client.command("SDIFFSTORE", f"{prefix}sdiff", set_a, set_b) >= 0
        assert client.command("SINTERSTORE", f"{prefix}sinter", set_a, set_b) >= 0
        assert client.command("SUNIONSTORE", f"{prefix}sunion", set_a, set_b) >= 0
        assert client.command("SINTERCARD", 2, set_a, set_b, "LIMIT", 10) >= 0
        assert client.command("SMOVE", set_a, set_b, "a") in (0, 1)
        assert client.command("SPOP", set_b, 1) is not None
        assert client.command("SREM", set_a, "b") in (0, 1)

        zset = f"{prefix}zset"
        assert client.command("ZADD", zset, 1, "a", 2, "b", 3, "c") == 3
        assert client.command("ZSCORE", zset, "a") is not None
        assert client.command("ZRANK", zset, "a") == 0
        assert client.command("ZREVRANK", zset, "c") == 0
        assert client.command("ZRANGE", zset, 0, -1)
        assert client.command("ZREVRANGE", zset, 0, -1)
        assert client.command("ZCARD", zset) == 3
        assert _text(client.command("ZINCRBY", zset, 1, "a"))
        assert client.command("ZCOUNT", zset, "-inf", "+inf") >= 3
        assert client.command("ZRANDMEMBER", zset, 1, "WITHSCORES")
        assert client.command("ZMSCORE", zset, "a", "none")
        assert client.command("ZRANGEBYSCORE", zset, "-inf", "+inf")
        assert client.command("ZREVRANGEBYSCORE", zset, "+inf", "-inf")
        assert client.command("ZREM", zset, "b") == 1
        assert client.command("ZPOPMIN", zset, 1)
        assert client.command("ZPOPMAX", zset, 1)

        stream = f"{prefix}stream"
        stream_id = client.command("XADD", stream, "*", "field", "value")
        assert stream_id is not None
        assert client.command("XLEN", stream) >= 1
        assert client.command("XRANGE", stream, "-", "+")
        assert client.command("XREVRANGE", stream, "+", "-")
        assert client.command("XINFO", "STREAM", stream)
        group = f"group-{suffix}"
        assert _ok(client.command("XGROUP", "CREATE", stream, group, "0"))
        assert client.command("XACK", stream, group, stream_id) >= 0
        assert client.command("XTRIM", stream, "MAXLEN", "~", 10) >= 0
        assert client.command("XDEL", stream, stream_id) >= 0

        bitmap = f"{prefix}bitmap"
        assert client.command("SETBIT", bitmap, 7, 1) == 0
        assert client.command("GETBIT", bitmap, 7) == 1
        assert client.command("BITCOUNT", bitmap) >= 1
        assert client.command("BITPOS", bitmap, 1) >= 0
        assert client.command("BITOP", "OR", f"{prefix}bitmap-out", bitmap) >= 1

        hll = f"{prefix}hll"
        hll_dst = f"{prefix}hll-dst"
        assert client.command("PFADD", hll, "a", "b") in (0, 1)
        assert client.command("PFCOUNT", hll) >= 1
        assert _ok(client.command("PFMERGE", hll_dst, hll))

        geo = f"{prefix}geo"
        geo_dst = f"{prefix}geo-dst"
        assert client.command("GEOADD", geo, 13.361389, 38.115556, "palermo") == 1
        assert client.command("GEOADD", geo, 15.087269, 37.502669, "catania") == 1
        assert client.command("GEOPOS", geo, "palermo")
        assert client.command("GEODIST", geo, "palermo", "catania", "km") is not None
        assert client.command("GEOHASH", geo, "palermo")
        assert client.command("GEOSEARCH", geo, "FROMMEMBER", "palermo", "BYRADIUS", 200, "km")
        assert (
            client.command(
                "GEOSEARCHSTORE",
                geo_dst,
                geo,
                "FROMMEMBER",
                "palermo",
                "BYRADIUS",
                200,
                "km",
            )
            >= 0
        )

        bloom = f"{prefix}bf"
        assert _ok(client.command("BF.RESERVE", bloom, "0.01", 100))
        assert client.command("BF.ADD", bloom, "a") in (0, 1)
        assert client.command("BF.MADD", bloom, "b", "c")
        assert client.command("BF.EXISTS", bloom, "a") in (0, 1)
        assert client.command("BF.MEXISTS", bloom, "a", "z")
        assert client.command("BF.CARD", bloom) >= 1
        assert client.command("BF.INFO", bloom)

        cuckoo = f"{prefix}cf"
        assert _ok(client.command("CF.RESERVE", cuckoo, 100))
        assert client.command("CF.ADD", cuckoo, "a") in (0, 1)
        assert client.command("CF.ADDNX", cuckoo, "b") in (0, 1)
        assert client.command("CF.EXISTS", cuckoo, "a") in (0, 1)
        assert client.command("CF.MEXISTS", cuckoo, "a", "z")
        assert client.command("CF.COUNT", cuckoo, "a") >= 0
        assert client.command("CF.DEL", cuckoo, "a") in (0, 1)
        assert client.command("CF.INFO", cuckoo)

        cms_a = f"{prefix}cms-a"
        cms_b = f"{prefix}cms-b"
        cms_dst = f"{prefix}cms-dst"
        assert _ok(client.command("CMS.INITBYDIM", cms_a, 20, 4))
        assert _ok(client.command("CMS.INITBYDIM", cms_b, 20, 4))
        assert client.command("CMS.INCRBY", cms_a, "a", 2, "b", 3)
        assert client.command("CMS.INCRBY", cms_b, "a", 1)
        assert client.command("CMS.QUERY", cms_a, "a", "b")
        assert _ok(client.command("CMS.MERGE", cms_dst, 2, cms_a, cms_b))
        assert client.command("CMS.INFO", cms_dst)

        topk = f"{prefix}topk"
        assert _ok(client.command("TOPK.RESERVE", topk, 3))
        assert client.command("TOPK.ADD", topk, "a", "b", "a")
        assert client.command("TOPK.INCRBY", topk, "c", 2)
        assert client.command("TOPK.QUERY", topk, "a", "z")
        assert client.command("TOPK.LIST", topk, "WITHCOUNT")
        assert client.command("TOPK.COUNT", topk, "a", "z")
        assert client.command("TOPK.INFO", topk)

        tdigest = f"{prefix}tdigest"
        tdigest_src = f"{prefix}tdigest-src"
        tdigest_dst = f"{prefix}tdigest-dst"
        assert _ok(client.command("TDIGEST.CREATE", tdigest))
        assert _ok(client.command("TDIGEST.ADD", tdigest, 1, 2, 3, 4))
        assert client.command("TDIGEST.QUANTILE", tdigest, "0.5")
        assert client.command("TDIGEST.CDF", tdigest, 2)
        assert client.command("TDIGEST.RANK", tdigest, 2)
        assert client.command("TDIGEST.REVRANK", tdigest, 2)
        assert client.command("TDIGEST.BYRANK", tdigest, 1)
        assert client.command("TDIGEST.BYREVRANK", tdigest, 1)
        assert client.command("TDIGEST.TRIMMED_MEAN", tdigest, "0.1", "0.9") is not None
        assert client.command("TDIGEST.MIN", tdigest) is not None
        assert client.command("TDIGEST.MAX", tdigest) is not None
        assert client.command("TDIGEST.INFO", tdigest)
        assert _ok(client.command("TDIGEST.CREATE", tdigest_src))
        assert _ok(client.command("TDIGEST.ADD", tdigest_src, 5, 6))
        assert _ok(
            client.command("TDIGEST.MERGE", tdigest_dst, 2, tdigest, tdigest_src, "OVERRIDE")
        )
        assert _ok(client.command("TDIGEST.RESET", tdigest))
    finally:
        _delete_prefixed_keys(client, prefix)
        client.close()


def test_real_ferricstore_flow_state_machine_and_repair_surface() -> None:
    client = _client()
    suffix = _suffix()
    flow_type = f"py-sdk-flow-{suffix}"
    now = int(time.time() * 1000)

    try:
        value_response = client.value_put(
            {"shared": True},
            partition_key=f"py-sdk:value:{suffix}",
            ttl_ms=60_000,
        )
        value_ref = _field(value_response, "ref")
        assert value_ref is not None

        signal_id = f"py-sdk:signal:{suffix}"
        signal_partition = f"{signal_id}:partition"
        client.create(
            signal_id,
            type=flow_type,
            state="created",
            partition_key=signal_partition,
            payload={"step": "created"},
            idempotent=True,
        )
        assert client.signal(
            signal_id,
            signal="approve",
            partition_key=signal_partition,
            if_state="created",
            transition_to="approved",
        )
        signaled = client.get(signal_id, partition_key=signal_partition)
        assert signaled is not None
        assert signaled.state == "approved"

        batch_partition = f"py-sdk:batch:{suffix}:partition"
        batch_items = [
            CreateItem(f"py-sdk:batch:{suffix}:a", {"n": 1}),
            CreateItem(f"py-sdk:batch:{suffix}:b", {"n": 2}),
        ]
        assert client.create_many(
            batch_partition,
            batch_items,
            type=flow_type,
            state="batch",
            now_ms=now,
            run_at_ms=now,
            idempotent=True,
        )
        batch_jobs = client.claim_jobs(
            flow_type,
            state="batch",
            worker="py-sdk-batch-worker",
            partition_key=batch_partition,
            limit=2,
            now_ms=now,
            priority=None,
        )
        assert len(batch_jobs) == 2
        assert client.complete_jobs(batch_jobs, result={"batch": True})

        transition_id, transition_partition, transition_job = _create_and_claim(
            client, flow_type, suffix, "transition"
        )
        extended = client.extend_lease(
            transition_job.id,
            transition_job.lease_token,
            fencing_token=transition_job.fencing_token,
            partition_key=transition_job.partition_key,
            lease_ms=30_000,
        )
        assert extended.id == transition_id
        assert client.transition(
            transition_id,
            from_state=transition_job.state,
            to_state="ready",
            lease_token=transition_job.lease_token,
            fencing_token=transition_job.fencing_token,
            partition_key=transition_partition,
            payload={"step": "ready"},
        )
        ready_job = _claim_one(client, flow_type, "ready", transition_partition)
        assert client.complete(
            ready_job.id,
            lease_token=ready_job.lease_token,
            fencing_token=ready_job.fencing_token,
            partition_key=ready_job.partition_key,
            result={"ok": True},
        )

        retry_id, retry_partition, retry_job = _create_and_claim(
            client, flow_type, suffix, "retry", now_ms=now
        )
        assert client.retry(
            retry_id,
            lease_token=retry_job.lease_token,
            fencing_token=retry_job.fencing_token,
            partition_key=retry_partition,
            error={"retry": True},
            run_at_ms=now,
            now_ms=now,
        )
        retried_job = _claim_one(client, flow_type, "queued", retry_partition, now_ms=now + 1)
        assert client.complete(
            retried_job.id,
            lease_token=retried_job.lease_token,
            fencing_token=retried_job.fencing_token,
            partition_key=retried_job.partition_key,
        )

        fail_id, fail_partition, fail_job = _create_and_claim(client, flow_type, suffix, "fail")
        assert client.fail(
            fail_id,
            lease_token=fail_job.lease_token,
            fencing_token=fail_job.fencing_token,
            partition_key=fail_partition,
            error={"failed": True},
        )
        failed = client.get(fail_id, partition_key=fail_partition)
        assert failed is not None
        assert failed.state == "failed"
        assert client.failures(flow_type, count=20) is not None

        cancel_id, cancel_partition, cancel_job = _create_and_claim(
            client, flow_type, suffix, "cancel"
        )
        assert client.cancel(
            cancel_id,
            lease_token=cancel_job.lease_token,
            fencing_token=cancel_job.fencing_token,
            partition_key=cancel_partition,
            reason={"cancelled": True},
        )
        cancelled = client.get(cancel_id, partition_key=cancel_partition)
        assert cancelled is not None
        assert cancelled.state == "cancelled"
        assert client.terminals(flow_type, count=50) is not None

        many_partition = f"py-sdk:many:{suffix}:partition"
        many_items = [
            CreateItem(f"py-sdk:many:{suffix}:a", {"kind": "transition"}),
            CreateItem(f"py-sdk:many:{suffix}:b", {"kind": "transition"}),
        ]
        assert client.create_many(
            many_partition,
            many_items,
            type=flow_type,
            state="many-transition",
            now_ms=now,
            run_at_ms=now,
        )
        many_jobs = client.claim_jobs(
            flow_type,
            state="many-transition",
            worker="py-sdk-many-worker",
            partition_key=many_partition,
            limit=2,
            now_ms=now,
            priority=None,
        )
        assert len(many_jobs) == 2
        assert client.transition_many(
            many_partition,
            from_state=many_jobs[0].state,
            to_state="many-complete",
            items=[_fenced(job) for job in many_jobs],
            now_ms=now,
        )
        many_complete = client.claim_jobs(
            flow_type,
            state="many-complete",
            worker="py-sdk-many-worker",
            partition_key=many_partition,
            limit=2,
            now_ms=now + 1,
            priority=None,
        )
        assert len(many_complete) == 2

        retry_many_partition = f"py-sdk:retry-many:{suffix}:partition"
        assert client.create_many(
            retry_many_partition,
            [
                CreateItem(f"py-sdk:retry-many:{suffix}:a"),
                CreateItem(f"py-sdk:retry-many:{suffix}:b"),
            ],
            type=flow_type,
            state="retry-many",
            now_ms=now,
            run_at_ms=now,
        )
        retry_many_jobs = client.claim_jobs(
            flow_type,
            state="retry-many",
            worker="py-sdk-retry-many-worker",
            partition_key=retry_many_partition,
            limit=2,
            now_ms=now,
            priority=None,
        )
        assert len(retry_many_jobs) == 2
        assert client.retry_many(
            retry_many_partition,
            retry_many_jobs,
            error={"retry": "many"},
            run_at_ms=now,
            now_ms=now,
        )
        retry_many_again = client.claim_jobs(
            flow_type,
            state="retry-many",
            worker="py-sdk-retry-many-worker",
            partition_key=retry_many_partition,
            limit=2,
            now_ms=now + 1,
            priority=None,
        )
        assert len(retry_many_again) == 2
        assert client.fail_many(retry_many_partition, retry_many_again, error={"done": True})

        cancel_many_partition = f"py-sdk:cancel-many:{suffix}:partition"
        assert client.create_many(
            cancel_many_partition,
            [
                CreateItem(f"py-sdk:cancel-many:{suffix}:a"),
                CreateItem(f"py-sdk:cancel-many:{suffix}:b"),
            ],
            type=flow_type,
            state="cancel-many",
            now_ms=now,
            run_at_ms=now,
        )
        cancel_many_jobs = client.claim_jobs(
            flow_type,
            state="cancel-many",
            worker="py-sdk-cancel-many-worker",
            partition_key=cancel_many_partition,
            limit=2,
            now_ms=now,
            priority=None,
        )
        assert len(cancel_many_jobs) == 2
        assert client.cancel_many(
            cancel_many_partition,
            [_fenced(job) for job in cancel_many_jobs],
            reason={"cancel": "many"},
        )

        reclaim_id = f"py-sdk:reclaim:{suffix}"
        reclaim_partition = f"{reclaim_id}:partition"
        client.create(
            reclaim_id,
            type=flow_type,
            state="reclaim",
            partition_key=reclaim_partition,
            now_ms=1_000,
            run_at_ms=1_000,
        )
        reclaim_initial = _claim_one(
            client,
            flow_type,
            "reclaim",
            reclaim_partition,
            worker="py-sdk-reclaim-initial",
            now_ms=1_000,
            lease_ms=10,
        )
        assert reclaim_initial.id == reclaim_id
        reclaimed = client.reclaim(
            flow_type,
            worker="py-sdk-reclaim-worker",
            partition_key=reclaim_partition,
            limit=1,
            now_ms=2_000,
            lease_ms=30_000,
            job_only=True,
        )
        assert len(reclaimed) == 1
        assert reclaimed[0].id == reclaim_id
        assert isinstance(reclaimed[0], ClaimedItem)
        assert client.complete(
            reclaimed[0].id,
            lease_token=reclaimed[0].lease_token,
            fencing_token=reclaimed[0].fencing_token,
            partition_key=reclaimed[0].partition_key,
        )

        stuck_id = f"py-sdk:stuck:{suffix}"
        stuck_partition = f"{stuck_id}:partition"
        client.create(
            stuck_id,
            type=flow_type,
            state="stuck",
            partition_key=stuck_partition,
            now_ms=1_000,
            run_at_ms=1_000,
        )
        stuck_job = _claim_one(
            client,
            flow_type,
            "stuck",
            stuck_partition,
            now_ms=1_000,
            lease_ms=60_000,
        )
        assert any(
            record.id == stuck_id
            for record in client.stuck(
                flow_type,
                partition_key=stuck_partition,
                count=10,
                older_than_ms=1,
                now_ms=120_000,
            )
        )
        assert client.complete(
            stuck_job.id,
            lease_token=stuck_job.lease_token,
            fencing_token=stuck_job.fencing_token,
            partition_key=stuck_job.partition_key,
        )

        parent_id = f"py-sdk:parent:{suffix}"
        parent_partition = f"{parent_id}:partition"
        client.create(
            parent_id,
            type=flow_type,
            state="dispatch",
            partition_key=parent_partition,
            correlation_id=f"corr:{suffix}",
            root_flow_id=f"root:{suffix}",
            now_ms=now,
            idempotent=True,
        )
        parent = client.get(parent_id, partition_key=parent_partition)
        assert parent is not None
        assert client.spawn_children(
            parent_id,
            [
                ChildSpec(f"py-sdk:child:{suffix}:a", flow_type, {"child": "a"}),
                ChildSpec(f"py-sdk:child:{suffix}:b", flow_type, {"child": "b"}),
            ],
            partition_key=parent_partition,
            fencing_token=parent.fencing_token,
            group_id="fanout",
            wait="any",
            from_state="dispatch",
            wait_state="waiting_children",
            success="children_done",
            failure="children_failed",
        )
        assert any(
            record.id.startswith(f"py-sdk:child:{suffix}:")
            for record in client.by_parent(parent_id)
        )
        assert any(record.id == parent_id for record in client.by_root(f"root:{suffix}"))
        assert any(record.id == parent_id for record in client.by_correlation(f"corr:{suffix}"))

        rewind_id, rewind_partition, rewind_job = _create_and_claim(
            client, flow_type, suffix, "rewind"
        )
        history_before = client.history(rewind_id, partition_key=rewind_partition, count=10)
        assert history_before
        created_event_id = _event_id(history_before[0])
        assert client.complete(
            rewind_job.id,
            lease_token=rewind_job.lease_token,
            fencing_token=rewind_job.fencing_token,
            partition_key=rewind_job.partition_key,
        )
        assert client.rewind(
            rewind_id,
            to_event=created_event_id,
            partition_key=rewind_partition,
            expect_state="completed",
            run_at_ms=now,
        )
        rewound = client.get(rewind_id, partition_key=rewind_partition)
        assert rewound is not None
        assert rewound.state == "queued"

        assert client.list(flow_type, count=100)
        assert isinstance(client.info(flow_type), dict)
        assert isinstance(client.history(signal_id, partition_key=signal_partition, count=5), list)
        assert isinstance(client.retention_cleanup(limit=10), dict)
    finally:
        client.close()
