from __future__ import annotations

import os
import subprocess
import threading
import time
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest

import ferricstore.protocol as protocol_module
from ferricstore import (
    ChildSpec,
    ClaimedFlow,
    CreateItem,
    FencedItem,
    FerricStoreError,
    FlowClient,
    FlowStateMode,
    FlowStatePolicy,
    JsonCodec,
    RetryPolicy,
)
from ferricstore.command_core import normalize_command_name
from ferricstore.commands import DataCommandsMixin

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_INTEGRATION") != "1",
    reason="set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests",
)

_NATIVE_PROTOCOL_COMMANDS: set[str] = set(
    """
    ACL APPEND AUTH BF.ADD BF.CARD BF.EXISTS BF.INFO BF.MADD BF.MEXISTS BF.RESERVE
    BGSAVE BITCOUNT BITOP BITPOS BLMOVE BLMPOP BLPOP BRPOP CAS CF.ADD CF.ADDNX
    CF.COUNT CF.DEL CF.EXISTS CF.INFO CF.MEXISTS CF.RESERVE CLIENT CLUSTER.DEMOTE
    CLUSTER.FAILOVER CLUSTER.HEALTH CLUSTER.JOIN CLUSTER.KEYSLOT CLUSTER.LEAVE
    CLUSTER.PROMOTE CLUSTER.ROLE CLUSTER.SLOTS CLUSTER.STATS CLUSTER.STATUS
    CMS.INCRBY CMS.INFO CMS.INITBYDIM CMS.INITBYPROB CMS.MERGE CMS.QUERY COMMAND
    CONFIG COPY DBSIZE DEBUG DECR DECRBY DEL DISCARD ECHO EXEC EXISTS EXPIRE
    EXPIREAT EXPIRETIME EXTEND FERRICSTORE.BLOBGC FERRICSTORE.CAPABILITIES
    FERRICSTORE.CONFIG FERRICSTORE.DOCTOR FERRICSTORE.HOTNESS FERRICSTORE.KEY_INFO
    FERRICSTORE.METRICS FERRICSTORE.NAMESPACE FERRICSTORE.QUOTA FERRICSTORE.TELEMETRY
    FETCH_OR_COMPUTE FETCH_OR_COMPUTE_ERROR FETCH_OR_COMPUTE_RESULT
    FLOW.APPROVAL.APPROVE FLOW.APPROVAL.GET FLOW.APPROVAL.LIST FLOW.APPROVAL.REJECT
    FLOW.APPROVAL.REQUEST FLOW.ATTRIBUTES FLOW.ATTRIBUTE_VALUES FLOW.BUDGET.COMMIT
    FLOW.BUDGET.GET FLOW.BUDGET.LIST FLOW.BUDGET.RELEASE FLOW.BUDGET.RESERVE
    FLOW.BY_CORRELATION FLOW.BY_PARENT FLOW.BY_ROOT FLOW.CANCEL FLOW.CANCEL_MANY
    FLOW.CIRCUIT.CLOSE FLOW.CIRCUIT.GET FLOW.CIRCUIT.OPEN FLOW.CLAIM_DUE
    FLOW.COMPLETE FLOW.COMPLETE_MANY FLOW.CREATE FLOW.CREATE_MANY
    FLOW.EFFECT.COMPENSATE FLOW.EFFECT.CONFIRM FLOW.EFFECT.FAIL FLOW.EFFECT.GET
    FLOW.EFFECT.RESERVE FLOW.EXTEND_LEASE FLOW.FAIL FLOW.FAILURES FLOW.FAIL_MANY
    FLOW.GET FLOW.GOVERNANCE.LEDGER FLOW.GOVERNANCE.OVERVIEW FLOW.HISTORY
    FLOW.INFO FLOW.LIMIT.GET FLOW.LIMIT.LEASE FLOW.LIMIT.LIST FLOW.LIMIT.RELEASE
    FLOW.LIMIT.SPEND FLOW.LIST FLOW.POLICY.GET FLOW.POLICY.SET FLOW.RECLAIM
    FLOW.RETENTION_CLEANUP FLOW.RETRY FLOW.RETRY_MANY FLOW.REWIND
    FLOW.RUN_STEPS_MANY FLOW.SCHEDULE.CREATE FLOW.SCHEDULE.DELETE FLOW.SEARCH
    FLOW.SCHEDULE.FIRE FLOW.SCHEDULE.FIRE_DUE FLOW.SCHEDULE.GET FLOW.SCHEDULE.LIST
    FLOW.SCHEDULE.PAUSE FLOW.SCHEDULE.RESUME FLOW.SIGNAL FLOW.SPAWN_CHILDREN
    FLOW.START_AND_CLAIM FLOW.STATS FLOW.STEP_CONTINUE FLOW.STUCK FLOW.TERMINALS
    FLOW.TRANSITION FLOW.TRANSITION_MANY FLOW.VALUE.PUT FLUSHALL FLUSHDB GEOADD
    GEODIST GEOHASH GEOPOS GEOSEARCH GEOSEARCHSTORE GET GETBIT GETDEL GETEX
    GETRANGE GETSET HDEL HELLO HEXISTS HEXPIRE HEXPIRETIME HGET HGETALL HGETDEL
    HGETEX HINCRBY HINCRBYFLOAT HKEYS HLEN HMGET HPERSIST HPEXPIRE HPTTL
    HRANDFIELD HSCAN HSET HSETEX HSETNX HSTRLEN HTTL HVALS INCR INCRBY
    INCRBYFLOAT INFO KEY_INFO KEYS LASTSAVE LINDEX LINSERT LLEN LMOVE LOCK LOLWUT
    LPOP LPOS LPUSH LPUSHX LRANGE LREM LSET LTRIM MEMORY MGET MODULE MSET MSETNX
    MULTI OBJECT PERSIST PEXPIRE PEXPIREAT PEXPIRETIME PFADD PFCOUNT PFMERGE PING
    PSETEX PSUBSCRIBE PTTL PUBLISH PUBSUB PUNSUBSCRIBE QUIT RANDOMKEY RATELIMIT.ADD
    RENAME RENAMENX RESET RPOP RPOPLPUSH RPUSH RPUSHX SADD SANDBOX SAVE SCAN SCARD
    SDIFF SDIFFSTORE SELECT SET SETBIT SETEX SETNX SETRANGE SINTER SINTERCARD
    SINTERSTORE SISMEMBER SLOWLOG SMEMBERS SMISMEMBER SMOVE SPOP SRANDMEMBER SREM
    SSCAN STRLEN SUBSCRIBE SUNION SUNIONSTORE TDIGEST.ADD TDIGEST.BYRANK
    TDIGEST.BYREVRANK TDIGEST.CDF TDIGEST.CREATE TDIGEST.INFO TDIGEST.MAX
    TDIGEST.MERGE TDIGEST.MIN TDIGEST.QUANTILE TDIGEST.RANK TDIGEST.RESET
    TDIGEST.REVRANK TDIGEST.TRIMMED_MEAN TOPK.ADD TOPK.COUNT TOPK.INCRBY TOPK.INFO
    TOPK.LIST TOPK.QUERY TOPK.RESERVE TTL TYPE UNLINK UNLOCK UNSUBSCRIBE UNWATCH
    WAIT WAITAOF WATCH XACK XADD XDEL XGROUP XINFO XLEN XRANGE XREAD XREADGROUP
    XREVRANGE XTRIM ZADD ZCARD ZCOUNT ZINCRBY ZMSCORE ZPOPMAX ZPOPMIN
    ZRANDMEMBER ZRANGE ZRANGEBYSCORE ZRANK ZREM ZREVRANGE ZREVRANGEBYSCORE
    ZREVRANK ZSCAN ZSCORE
    """.split()  # noqa: SIM905 - command contract stays readable as copied parser output
)

_NATIVE_PROTOCOL_SHARED_INTEGRATION_EXCLUDED: dict[str, str] = {
    "ACL": "requires protected/security-mode fixture, not the default open integration server",
    "AUTH": "requires protected/security-mode fixture, not the default open integration server",
    "BGSAVE": "admin persistence command; not part of normal SDK app command coverage",
    "CLUSTER.DEMOTE": "mutates cluster topology",
    "CLUSTER.FAILOVER": "mutates cluster topology",
    "CLUSTER.JOIN": "mutates cluster topology",
    "CLUSTER.LEAVE": "mutates cluster topology",
    "CLUSTER.PROMOTE": "mutates cluster topology",
    "DEBUG": "debug/admin command, not normal SDK app surface",
    "FLUSHALL": "destructive for shared integration state",
    "FLUSHDB": "destructive for shared integration state",
    "FERRICSTORE.NAMESPACE": "management command requires namespace control-plane support",
    "FERRICSTORE.QUOTA": "management command requires quota control-plane support",
    "HELLO": "connection handshake command",
    "LASTSAVE": "admin persistence command; not part of normal SDK app command coverage",
    "LOLWUT": "diagnostic compatibility command, not SDK app surface",
    "MODULE": "admin module command; FerricStore does not load modules through SDK tests",
    "QUIT": "connection lifecycle command",
    "RESET": "connection lifecycle command",
    "SANDBOX": "debug/admin command, not normal SDK app surface",
    "SAVE": "admin persistence command; not part of normal SDK app command coverage",
    "SELECT": "single-database compatibility command, not normal SDK app surface",
}

_NATIVE_PROTOCOL_INTEGRATION_OBSERVED: set[str] = set()
_NATIVE_PROTOCOL_INTEGRATION_OBSERVED_LOCK = threading.Lock()


def _observe_command(args: tuple[Any, ...]) -> None:
    if not args:
        return
    try:
        name = normalize_command_name(args[0])
        if name == "COMMAND_EXEC" and len(args) > 1:
            name = normalize_command_name(args[1])
    except (TypeError, ValueError):
        return
    names = {name}
    if name.startswith("CLIENT."):
        names.add("CLIENT")
    with _NATIVE_PROTOCOL_INTEGRATION_OBSERVED_LOCK:
        _NATIVE_PROTOCOL_INTEGRATION_OBSERVED.update(names)


def _observe_commands(commands: list[tuple[Any, ...]]) -> None:
    for command in commands:
        _observe_command(command)


class _ObservedExecutor:
    """Record commands at the client/executor boundary used by live integration tests."""

    _OPTIONAL_CAPABILITIES = frozenset(
        {
            "acquire_dedicated_session",
            "acquire_session",
            "acquire_session_for_key",
            "acquire_session_for_keys",
            "acquire_session_on_lane",
            "execute_batch",
            "execute_batch_on_lane",
            "execute_batch_ordered",
            "execute_command_on_lane",
            "execute_command_with_trace",
            "execute_command_with_trace_on_lane",
            "submit_batch",
            "submit_batch_on_lane",
            "submit_command",
            "submit_command_on_lane",
            "submit_commands",
            "submit_commands_on_lane",
        }
    )

    def __init__(self, executor: Any) -> None:
        self._executor = executor

    def __getattribute__(self, name: str) -> Any:
        if name in object.__getattribute__(self, "_OPTIONAL_CAPABILITIES"):
            executor = object.__getattribute__(self, "_executor")
            if not callable(getattr(executor, name, None)):
                raise AttributeError(name)
        return object.__getattribute__(self, name)

    def execute_command(self, *args: Any) -> Any:
        _observe_command(args)
        return self._executor.execute_command(*args)

    def execute_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
        _observe_command(args)
        return self._executor.execute_command_on_lane(args, lane_id)

    def execute_command_with_trace(self, *args: Any) -> Any:
        _observe_command(args)
        return self._executor.execute_command_with_trace(*args)

    def execute_command_with_trace_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> Any:
        _observe_command(args)
        return self._executor.execute_command_with_trace_on_lane(args, lane_id)

    def submit_command(self, *args: Any) -> Any:
        _observe_command(args)
        return self._executor.submit_command(*args)

    def submit_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
        _observe_command(args)
        return self._executor.submit_command_on_lane(args, lane_id)

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> Any:
        _observe_commands(commands)
        return self._executor.execute_batch(commands)

    def execute_batch_ordered(self, commands: list[tuple[Any, ...]]) -> Any:
        _observe_commands(commands)
        return self._executor.execute_batch_ordered(commands)

    def execute_batch_on_lane(self, commands: list[tuple[Any, ...]], lane_id: int) -> Any:
        _observe_commands(commands)
        return self._executor.execute_batch_on_lane(commands, lane_id)

    def submit_commands(self, commands: list[tuple[Any, ...]]) -> Any:
        _observe_commands(commands)
        return self._executor.submit_commands(commands)

    def submit_commands_on_lane(self, commands: list[tuple[Any, ...]], lane_id: int) -> Any:
        _observe_commands(commands)
        return self._executor.submit_commands_on_lane(commands, lane_id)

    def submit_batch(self, commands: list[tuple[Any, ...]]) -> Any:
        _observe_commands(commands)
        return self._executor.submit_batch(commands)

    def submit_batch_on_lane(self, commands: list[tuple[Any, ...]], lane_id: int) -> Any:
        _observe_commands(commands)
        return self._executor.submit_batch_on_lane(commands, lane_id)

    def acquire_session(self, *args: Any, **kwargs: Any) -> _ObservedExecutor:
        return _ObservedExecutor(self._executor.acquire_session(*args, **kwargs))

    def acquire_session_for_key(self, *args: Any, **kwargs: Any) -> _ObservedExecutor:
        return _ObservedExecutor(self._executor.acquire_session_for_key(*args, **kwargs))

    def acquire_session_for_keys(self, *args: Any, **kwargs: Any) -> _ObservedExecutor:
        return _ObservedExecutor(self._executor.acquire_session_for_keys(*args, **kwargs))

    def acquire_dedicated_session(self, *args: Any, **kwargs: Any) -> _ObservedExecutor:
        return _ObservedExecutor(self._executor.acquire_dedicated_session(*args, **kwargs))

    def acquire_session_on_lane(self, *args: Any, **kwargs: Any) -> _ObservedExecutor:
        return _ObservedExecutor(self._executor.acquire_session_on_lane(*args, **kwargs))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._executor, name)


def _observe_client(client: FlowClient) -> FlowClient:
    executor = client.executor._executor
    if not isinstance(executor, _ObservedExecutor):
        client.executor._executor = _ObservedExecutor(executor)
    return client


def _client() -> FlowClient:
    return _observe_client(
        FlowClient.from_url(
            _integration_url(),
            codec=JsonCodec(),
        )
    )


def _topology_client() -> FlowClient:
    return _observe_client(
        FlowClient.from_urls(
            _integration_urls(),
            codec=JsonCodec(),
            endpoint_policy=os.environ.get("FERRICSTORE_ENDPOINT_POLICY", "any"),
        )
    )


def _integration_url() -> str:
    return os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")


def _integration_urls() -> list[str]:
    raw = os.environ.get("FERRICSTORE_URLS")
    if not raw:
        return [_integration_url()]
    urls = [url.strip() for url in raw.split(",") if url.strip()]
    return urls or [_integration_url()]


def _require_cluster_failure_fixture() -> tuple[str, Path]:
    if os.environ.get("FERRICSTORE_CLUSTER_FAILURE") != "1":
        pytest.skip("set FERRICSTORE_CLUSTER_FAILURE=1 to run node-stop integration tests")
    if len(_integration_urls()) < 3:
        pytest.skip("node-stop integration requires FERRICSTORE_URLS with at least three seeds")

    compose_file = Path(
        os.environ.get("FERRICSTORE_CLUSTER_COMPOSE_FILE", "docker-compose.cluster.yml")
    )
    if not compose_file.exists():
        pytest.skip(f"cluster compose file not found: {compose_file}")

    try:
        subprocess.run(
            ["docker", "compose", "version"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        pytest.skip(f"docker compose is not available: {exc}")

    return os.environ.get("FERRICSTORE_CLUSTER_COMPOSE_PROJECT", "ferricstore-python-cluster"), (
        compose_file
    )


def _docker_compose(project: str, compose_file: Path, *args: str, timeout: int = 60) -> None:
    subprocess.run(
        ["docker", "compose", "-p", project, "-f", str(compose_file), *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _service_from_node(node_name: Any) -> str:
    text = _text(node_name)
    if "@" not in text:
        raise AssertionError(f"cannot derive compose service from node name: {text!r}")
    host = text.split("@", 1)[1]
    service = host.split(".", 1)[0]
    if service not in {"fs0", "fs1", "fs2"}:
        raise AssertionError(f"unexpected cluster service host in node name: {text!r}")
    return service


def _wait_native_url(url: str, *, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client = FlowClient.from_url(url, timeout=1.0)
            try:
                if client.command("PING") in (b"PONG", "PONG", True):
                    return
            finally:
                client.close()
        except Exception as exc:  # pragma: no cover - exercised by Docker integration only
            last_error = exc
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for FerricStore at {url}: {last_error!r}")


def _wait_route_change(
    client: FlowClient,
    key: str,
    old_leader: str,
    *,
    timeout: float = 90.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client.refresh_topology()
            route = client.route(key)
            if _text(route["leader_node"]) != old_leader:
                return route
        except Exception as exc:  # pragma: no cover - exercised by Docker integration only
            last_error = exc
        time.sleep(0.5)
    raise AssertionError(f"route did not move away from {old_leader}: {last_error!r}")


def _cluster_member_count(value: Any) -> int:
    return len([item for item in _text(value).split(",") if item.strip()])


def _wait_cluster_synced(*, minimum_members: int = 3, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    last_status: Any = None
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        client = _topology_client()
        try:
            status = client.cluster_status()
            last_status = status
            shard_members = [
                _field(value, "members")
                for key, value in status.items()
                if str(key).startswith("shard_")
            ]
            if (
                _field(status, "sync_status") == "synced"
                and shard_members
                and all(
                    _cluster_member_count(members) >= minimum_members for members in shard_members
                )
            ):
                return
        except Exception as exc:  # pragma: no cover - exercised by Docker integration only
            last_error = exc
        finally:
            client.close()
        time.sleep(0.5)
    raise AssertionError(
        f"cluster did not settle after node restore: {last_status!r} {last_error!r}"
    )


def _require_protocol_transport() -> None:
    if not _integration_url().startswith(("ferric://", "ferrics://")):
        pytest.skip("native protocol coverage runs with FERRICSTORE_URL=ferric://...")


@pytest.fixture(scope="module", autouse=True)
def _assert_observed_native_protocol_coverage() -> Any:
    with _NATIVE_PROTOCOL_INTEGRATION_OBSERVED_LOCK:
        _NATIVE_PROTOCOL_INTEGRATION_OBSERVED.clear()
    if os.environ.get("FERRICSTORE_SKIP_CATALOG_COVERAGE") == "1":
        yield
        return
    yield

    if not _integration_url().startswith(("ferric://", "ferrics://")):
        return
    client = _client()
    try:
        catalog_names = _command_catalog_names(client.command("COMMAND"))
    finally:
        client.close()

    unknown = catalog_names - _NATIVE_PROTOCOL_COMMANDS
    missing = (
        catalog_names
        - _NATIVE_PROTOCOL_INTEGRATION_OBSERVED
        - set(_NATIVE_PROTOCOL_SHARED_INTEGRATION_EXCLUDED)
    )
    assert unknown == set(), f"server command catalog is missing from SDK contract: {unknown}"
    assert missing == set(), (
        f"native commands were not exercised by live integration tests: {missing}"
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


def _fenced(job: ClaimedFlow) -> FencedItem:
    return FencedItem(
        id=job.id,
        fencing_token=job.fencing_token,
        lease_token=job.lease_token,
        partition_key=job.partition_key,
    )


def _command_catalog_names(value: Any) -> set[str]:
    names: set[str] = set()
    for item in value or []:
        if isinstance(item, (list, tuple)) and item:
            names.add(_text(item[0]).upper())
    return names


def _opcode_value(value: Any) -> int:
    if isinstance(value, int):
        return value
    return int(_text(value), 0)


def _options_opcode_table(value: Any) -> dict[str, int]:
    raw_opcodes = _field(value, "opcodes")
    if not isinstance(raw_opcodes, list):
        raise AssertionError(f"OPTIONS response does not contain opcodes list: {value!r}")

    table: dict[str, int] = {}
    for item in raw_opcodes:
        if isinstance(item, dict):
            name = _field(item, "name")
            opcode = _field(item, "opcode")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            name = item[0]
            opcode = item[1]
        else:
            raise AssertionError(f"unexpected OPTIONS opcode entry: {item!r}")
        if name is None or opcode is None:
            raise AssertionError(f"OPTIONS opcode entry missing name/opcode: {item!r}")
        table[_text(name).upper()] = _opcode_value(opcode)
    return table


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
) -> ClaimedFlow:
    jobs = client.claim_flows(
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


def _wait_flow_state(
    client: FlowClient,
    flow_id: str,
    partition_key: str,
    state: str,
    *,
    timeout: float = 30.0,
) -> Any:
    deadline = time.monotonic() + timeout
    record: Any = None
    while time.monotonic() < deadline:
        record = client.get(flow_id, partition_key=partition_key)
        if record is not None and record.state == state:
            return record
        time.sleep(0.1)
    raise AssertionError(f"flow {flow_id} did not reach state {state!r}; last={record!r}")


def _create_and_claim(
    client: FlowClient,
    flow_type: str,
    suffix: str,
    name: str,
    *,
    state: str = "queued",
    now_ms: int | None = None,
    lease_ms: int = 30_000,
) -> tuple[str, str, ClaimedFlow]:
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


def test_real_ferricstore_native_protocol_command_coverage_contract() -> None:
    _require_protocol_transport()

    client = _client()
    try:
        catalog_names = _command_catalog_names(client.command("COMMAND"))
        assert catalog_names <= _NATIVE_PROTOCOL_COMMANDS
        assert set(_NATIVE_PROTOCOL_SHARED_INTEGRATION_EXCLUDED) <= _NATIVE_PROTOCOL_COMMANDS
    finally:
        client.close()


def test_real_ferricstore_native_options_opcode_table_matches_sdk() -> None:
    _require_protocol_transport()

    client = _client()
    try:
        assert _options_opcode_table(client.command("OPTIONS")) == protocol_module._OPCODES
    finally:
        client.close()


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


def test_real_ferricstore_topology_aware_client_routes_kv_and_flow_commands() -> None:
    _require_protocol_transport()

    client = _topology_client()
    suffix = _suffix()
    key = f"py-sdk:topology:{suffix}"
    flow_type = f"py-sdk-topology-{suffix}"
    flow_id = f"py-sdk:topology-flow:{suffix}"
    partition = f"{{py-sdk-topology:{suffix}}}:partition"
    now = int(time.time() * 1000)

    try:
        topology = client.refresh_topology()
        assert topology.endpoints
        if len(_integration_urls()) >= 3:
            cluster_status = client.cluster_status()
            connected = _field(cluster_status, "connected_nodes")
            assert connected
            shard_members = [
                _field(value, "members")
                for key, value in cluster_status.items()
                if str(key).startswith("shard_")
            ]
            assert any(len(_text(members).split(",")) >= 3 for members in shard_members)

        route = client.route(key)
        assert route["endpoint"]["host"]
        assert route["lane_id"] >= 0

        assert client.kv_set(key, {"routed": True}) in (True, b"OK", "OK")
        assert client.kv_get(key) == {"routed": True}

        assert client.create(
            flow_id,
            type=flow_type,
            state="queued",
            partition_key=partition,
            payload={"routed": True},
            state_meta={"version": "1"},
            now_ms=now,
            run_at_ms=now,
            idempotent=True,
        )
        job = _claim_one(client, flow_type, "queued", partition, now_ms=now + 1)
        assert client.complete(
            flow_id,
            lease_token=job.lease_token,
            fencing_token=job.fencing_token,
            partition_key=partition,
            result={"ok": True},
            state_meta={"version": "2"},
            now_ms=now + 2,
        )
        record = _wait_flow_state(client, flow_id, partition, "completed")
        assert record.state_meta == {
            "queued": {"version": "1"},
            "completed": {"version": "2"},
        }
    finally:
        with suppress(Exception):
            client.command("DEL", key)
        client.close()


def test_real_ferricstore_state_meta_policy_and_flow_cycle() -> None:
    client = _client()
    suffix = _suffix()
    flow_type = f"py-sdk-native-state-meta-{suffix}"
    flow_id = f"py-sdk:state-meta:{suffix}"
    partition = f"{flow_id}:partition"
    now = int(time.time() * 1000)

    try:
        assert client.install_policy(flow_type, indexed_state_meta="version")
        policy = client.policy_get(flow_type)
        assert _text(_field(policy, "indexed_state_meta")) == "version"

        assert client.create(
            flow_id,
            type=flow_type,
            state="accept",
            partition_key=partition,
            state_meta={"version": "1", "owner": "risk"},
            now_ms=now,
            run_at_ms=now,
            idempotent=True,
        )

        record = client.get(flow_id, partition_key=partition)
        assert record.state_meta == {"accept": {"version": "1", "owner": "risk"}}
        assert record.indexed_state_meta == "version"

        claimed = _claim_one(client, flow_type, "accept", partition, now_ms=now + 1)
        assert client.complete(
            flow_id,
            lease_token=claimed.lease_token,
            fencing_token=claimed.fencing_token,
            partition_key=partition,
            state_meta={"version": "3"},
            now_ms=now + 2,
        )

        record = client.get(flow_id, partition_key=partition)
        assert record.state_meta == {
            "accept": {"version": "1", "owner": "risk"},
            "completed": {"version": "3"},
        }
    finally:
        client.close()


def test_real_ferricstore_fifo_state_policy_edges() -> None:
    client = _client()
    suffix = _suffix()
    parallel_type = f"py-sdk-fifo-default-parallel-{suffix}"
    fifo_type = f"py-sdk-fifo-policy-{suffix}"
    partition = f"py-sdk:fifo:{suffix}:partition"
    now = int(time.time() * 1000)

    try:
        for name in ("first", "second"):
            assert client.create(
                f"py-sdk:fifo-default:{suffix}:{name}",
                type=parallel_type,
                state="queued",
                partition_key=partition,
                payload={"name": name},
                priority=1,
                now_ms=now,
                run_at_ms=now,
            )

        default_parallel = client.claim_flows(
            parallel_type,
            state="queued",
            worker="py-sdk-default-parallel-worker",
            partition_key=partition,
            limit=2,
            priority=1,
            now_ms=now + 1,
        )
        assert len(default_parallel) == 2

        assert client.install_policy(
            fifo_type,
            states={
                "queued": FlowStatePolicy(mode=FlowStateMode.FIFO),
                "start": FlowStatePolicy(mode=FlowStateMode.PARALLEL),
            },
        )
        queued_policy = client.policy_get(fifo_type, state="queued")
        start_policy = client.policy_get(fifo_type, state="start")
        assert _text(_field(queued_policy, "mode")).lower() == "fifo"
        assert _text(_field(start_policy, "mode")).lower() == "parallel"

        with pytest.raises(FerricStoreError, match="partition_key is required for fifo state"):
            client.create(
                f"py-sdk:fifo-no-partition:{suffix}",
                type=fifo_type,
                state="queued",
                payload={"bad": "missing-partition"},
                now_ms=now + 10,
                run_at_ms=now + 10,
            )

        with pytest.raises(FerricStoreError, match="priority is not supported for fifo state"):
            client.create(
                f"py-sdk:fifo-priority:{suffix}",
                type=fifo_type,
                state="queued",
                partition_key=partition,
                payload={"bad": "priority"},
                priority=1,
                now_ms=now + 11,
                run_at_ms=now + 11,
            )

        transition_id = f"py-sdk:fifo-transition:{suffix}"
        assert client.create(
            transition_id,
            type=fifo_type,
            state="start",
            payload={"step": "start"},
            now_ms=now + 20,
            run_at_ms=now + 20,
        )
        start_jobs = client.claim_flows(
            fifo_type,
            state="start",
            worker="py-sdk-fifo-transition-worker",
            limit=1,
            priority=None,
            now_ms=now + 21,
        )
        assert len(start_jobs) == 1
        start_job = start_jobs[0]
        assert start_job.partition_key is not None

        with pytest.raises(FerricStoreError, match="partition_key is required for fifo state"):
            client.transition(
                transition_id,
                from_state=start_job.state,
                to_state="queued",
                lease_token=start_job.lease_token,
                fencing_token=start_job.fencing_token,
                priority=None,
                now_ms=now + 22,
                run_at_ms=now + 22,
            )

        assert client.transition(
            transition_id,
            from_state=start_job.state,
            to_state="queued",
            lease_token=start_job.lease_token,
            fencing_token=start_job.fencing_token,
            partition_key=start_job.partition_key,
            priority=None,
            now_ms=now + 23,
            run_at_ms=now + 23,
        )

        queued = client.claim_flows(
            fifo_type,
            state="queued",
            worker="py-sdk-fifo-queued-worker",
            partition_key=start_job.partition_key,
            limit=1,
            priority=None,
            now_ms=now + 24,
        )
        assert [job.id for job in queued] == [transition_id]
    finally:
        client.close()


def test_real_ferricstore_claim_due_partition_keys_decode_claim_data() -> None:
    client = _client()
    suffix = _suffix()
    flow_type = f"py-sdk-claim-partitions-{suffix}"
    partition_a = f"py-sdk:claim:{suffix}:a"
    partition_b = f"py-sdk:claim:{suffix}:b"
    partition_c = f"py-sdk:claim:{suffix}:c"
    now = int(time.time() * 1000)

    try:
        for partition, name in ((partition_a, "a"), (partition_b, "b"), (partition_c, "c")):
            assert client.create(
                f"py-sdk:claim:{suffix}:{name}",
                type=flow_type,
                state="queued",
                partition_key=partition,
                payload={"partition": name},
                now_ms=now,
                run_at_ms=now,
            )

        jobs = client.claim_flows(
            flow_type,
            state="queued",
            worker="py-sdk-claim-partition-worker",
            partition_keys=[partition_a, partition_b],
            limit=10,
            priority=None,
            now_ms=now + 1,
        )

        assert {job.partition_key for job in jobs} == {partition_a, partition_b}
        assert {job.id for job in jobs} == {
            f"py-sdk:claim:{suffix}:a",
            f"py-sdk:claim:{suffix}:b",
        }
        for job in jobs:
            assert isinstance(job.lease_token, bytes)
            assert job.lease_token
            assert isinstance(job.fencing_token, int)
            assert job.fencing_token > 0

        remaining = client.claim_flows(
            flow_type,
            state="queued",
            worker="py-sdk-claim-partition-worker",
            partition_key=partition_c,
            limit=10,
            priority=None,
            now_ms=now + 2,
        )
        assert [job.partition_key for job in remaining] == [partition_c]
    finally:
        client.close()


def test_real_ferricstore_protocol_helpers_and_diagnostics() -> None:
    _require_protocol_transport()

    client = _client()
    suffix = _suffix()
    prefix = f"py-sdk:protocol:{suffix}:"
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
        assert client.fetch_or_compute_result(
            cache_key,
            first.ownership_token,
            {"computed": True},
            ttl_ms=60_000,
        )
        cached = client.fetch_or_compute(cache_key, ttl_ms=60_000)
        assert cached.hit
        assert cached.value == {"computed": True}

        error_key = f"{prefix}cache-error"
        first_error = client.fetch_or_compute(error_key, ttl_ms=60_000)
        assert first_error.should_compute
        assert client.fetch_or_compute_error(error_key, first_error.ownership_token, "boom")

        assert isinstance(client.cluster_health(), dict)
        assert isinstance(client.cluster_stats(), dict)
        assert isinstance(client.cluster_keyslot(key), int)
        assert client.cluster_slots() is not None
        assert isinstance(client.cluster_status(), dict)
        assert client.cluster_role() is not None
        assert client.ferricstore_config("GET", "*") is not None
        assert isinstance(client.ferricstore_metrics(), dict)
        assert isinstance(client.ferricstore_hotness(), dict)
        assert isinstance(client.command("FERRICSTORE.CAPABILITIES"), dict)
        assert isinstance(client.command("FERRICSTORE.TELEMETRY", "CLUSTER_INFO"), dict)
    finally:
        _delete_prefixed_keys(client, prefix)
        client.close()


def test_real_ferricstore_raw_store_command_families() -> None:
    _require_protocol_transport()

    client = _client()
    suffix = _suffix()
    prefix = f"py-sdk:store:{{{suffix}}}:"

    try:
        string_key = f"{prefix}string"
        second_key = f"{prefix}string2"
        third_key = f"{prefix}string3"
        assert _ok(client.command("SET", string_key, "abc", "PX", 60_000))
        assert client.command("EXISTS", string_key) == 1
        assert client.command("MGET", string_key, f"{prefix}missing")[0] in (b"abc", "abc")
        assert _ok(client.command("MSET", second_key, "2", third_key, "3"))
        assert (
            client.command(
                "MSETNX",
                f"{prefix}nx1",
                "1",
                f"{prefix}nx2",
                "2",
            )
            == 1
        )
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


def test_real_ferricstore_native_protocol_store_and_admin_surface() -> None:
    _require_protocol_transport()

    client = _client()
    suffix = _suffix()
    prefix = f"py-sdk:native-store:{{{suffix}}}:"
    keys: list[str] = []

    def key(name: str) -> str:
        value = f"{prefix}{name}"
        keys.append(value)
        return value

    try:
        string_key = key("string")
        second_key = key("string2")
        third_key = key("string3")
        assert client.command("PING") in (b"PONG", "PONG", True)
        assert _ok(client.command("CLIENT.SETNAME", f"py-sdk-native-{suffix}"))
        assert client.command("CLIENT.INFO") is not None
        assert client.command("FERRICSTORE.DOCTOR", "LIST") is not None
        assert _ok(client.command("SET", string_key, "abc", "PX", 60_000))
        assert client.command("GET", string_key) in (b"abc", "abc")
        assert _ok(client.command("MSET", second_key, "2", third_key, "3"))
        mget = client.command("MGET", second_key, third_key, key("missing"))
        assert mget[:2] in ([b"2", b"3"], ["2", "3"])
        assert client.command("DEL", third_key) >= 0

        pipeline_results = (
            client.pipeline()
            .command("SET", key("pipe"), "piped")
            .command("GET", key("pipe"))
            .execute()
        )
        assert pipeline_results[-1] in (b"piped", "piped")

        hash_key = key("hash")
        assert client.command("HSET", hash_key, "field", "value", "count", "1") >= 1
        assert client.command("HGET", hash_key, "field") in (b"value", "value")
        assert client.command("HMGET", hash_key, "field", "none")[0] in (b"value", "value")
        assert client.command("HGETALL", hash_key)

        list_key = key("list")
        assert client.command("LPUSH", list_key, "b", "a") == 2
        assert client.command("RPUSH", list_key, "c") == 3
        assert client.command("LRANGE", list_key, 0, -1)
        assert client.command("LPOP", list_key) in (b"a", "a")
        assert client.command("RPOP", list_key) in (b"c", "c")

        set_key = key("set")
        assert client.command("SADD", set_key, "a", "b") == 2
        assert client.command("SISMEMBER", set_key, "a") == 1
        assert client.command("SMEMBERS", set_key)
        assert client.command("SREM", set_key, "b") == 1

        zset_key = key("zset")
        assert client.command("ZADD", zset_key, 1, "a", 2, "b") == 2
        assert client.command("ZSCORE", zset_key, "a") is not None
        assert client.command("ZRANGE", zset_key, 0, -1)
        assert client.command("ZREM", zset_key, "b") == 1

        counter_key = key("counter")
        assert client.command("INCR", counter_key) == 1
        assert client.command("EXPIRE", counter_key, 60) == 1
        assert client.command("TTL", counter_key) >= 0

        stream_key = key("stream")
        stream_id = client.command("XADD", stream_key, "*", "field", "value")
        assert isinstance(stream_id, (bytes, str))
        assert client.command("XLEN", stream_key) == 1

        hll_key = key("hll")
        assert client.command("PFADD", hll_key, "a", "b") >= 0
        assert client.command("PFCOUNT", hll_key) >= 1

        bloom_key = key("bloom")
        assert _ok(client.command("BF.RESERVE", bloom_key, "0.01", "100"))
        assert client.command("BF.ADD", bloom_key, "member") == 1
        assert client.command("BF.EXISTS", bloom_key, "member") == 1

        cas_key = key("cas")
        assert _ok(client.command("SET", cas_key, client.codec.encode("old")))
        assert client.cas(cas_key, "old", "new") is True
        assert _decode(client, client.command("GET", cas_key)) == "new"

        lock_key = key("lock")
        assert client.lock(lock_key, "owner-a", 30_000) is True
        assert client.extend_lock(lock_key, "owner-a", 30_000) == 1
        assert client.unlock(lock_key, "owner-a") == 1

        rate = client.ratelimit_add(key("rate"), window_ms=60_000, max=5, count=2)
        assert rate.count >= 1
        assert rate.remaining >= 0

        cache_key = key("cache")
        first = client.fetch_or_compute(cache_key, ttl_ms=60_000, hint="native-integration")
        assert first.should_compute
        assert client.fetch_or_compute_result(
            cache_key,
            first.ownership_token,
            {"computed": True},
            ttl_ms=60_000,
        )
        cached = client.fetch_or_compute(cache_key, ttl_ms=60_000)
        assert cached.hit
        assert cached.value == {"computed": True}

        error_cache_key = key("cache-error")
        first_error = client.fetch_or_compute(error_cache_key, ttl_ms=60_000)
        assert first_error.should_compute
        assert client.fetch_or_compute_error(
            error_cache_key,
            first_error.ownership_token,
            "boom",
        )

        info = client.key_info(string_key)
        assert info.raw
        assert isinstance(client.cluster_health(), dict)
        assert isinstance(client.cluster_stats(), dict)
        assert isinstance(client.cluster_keyslot(string_key), int)
        assert client.cluster_slots() is not None
        assert isinstance(client.cluster_status(), dict)
        assert client.cluster_role() is not None
        assert client.ferricstore_config("GET", "*") is not None
        assert isinstance(client.ferricstore_metrics(), dict)
        assert isinstance(client.ferricstore_hotness(), dict)
        assert client.ferricstore_blobgc() is not None
    finally:
        with suppress(Exception):
            if keys:
                client.command("DEL", *keys)
        client.close()


def test_real_ferricstore_native_protocol_flow_admin_surface() -> None:
    _require_protocol_transport()

    client = _client()
    suffix = _suffix()
    flow_type = f"py-sdk-native-admin-{suffix}"
    now = int(time.time() * 1000)
    partition = f"py-sdk:native-admin:{suffix}:partition"

    try:
        assert client.install_policy(
            flow_type,
            retry=RetryPolicy(max_retries=2, base_ms=10, max_ms=100, jitter_pct=0),
            indexed_state_meta="version",
        )
        assert isinstance(client.policy_get(flow_type), dict)

        attr_id = f"py-sdk:native-attr:{suffix}"
        client.create(
            attr_id,
            type=flow_type,
            state="attr",
            partition_key=partition,
            attributes={"tenant": "acme", "tier": "gold"},
            state_meta={"version": "1"},
            now_ms=now,
            run_at_ms=now,
            idempotent=True,
        )
        assert client.list(
            flow_type,
            state="attr",
            partition_key=partition,
            attributes={"tenant": "acme"},
            consistent_projection=True,
        )
        search_matches = client.search(
            flow_type,
            state="attr",
            partition_key=partition,
            state_meta={"version": "1"},
            consistent_projection=True,
        )
        assert any(record.id == attr_id for record in search_matches)
        assert isinstance(
            client.stats(
                flow_type,
                state="attr",
                partition_key=partition,
                attributes={"tenant": "acme"},
                consistent_projection=True,
            ),
            dict,
        )
        assert isinstance(
            client.attributes(
                flow_type,
                state="attr",
                partition_key=partition,
                consistent_projection=True,
            ),
            list,
        )
        assert isinstance(
            client.attribute_values(
                flow_type,
                "tenant",
                state="attr",
                partition_key=partition,
                consistent_projection=True,
            ),
            list,
        )

        started_id = f"py-sdk:native-start:{suffix}"
        started = client.start_and_claim(
            started_id,
            type=flow_type,
            initial_state="step-a",
            worker="py-sdk-native-step-worker",
            partition_key=partition,
            payload={"step": "a"},
            now_ms=now,
        )
        assert started.id == started_id
        continued = client.step_continue(
            started.id,
            lease_token=started.lease_token,
            fencing_token=started.fencing_token,
            from_state="step-a",
            to_state="step-b",
            partition_key=partition,
            worker="py-sdk-native-step-worker",
            payload={"step": "b"},
            return_job=True,
            now_ms=now + 1,
        )
        assert continued.id == started_id
        assert client.complete(
            continued.id,
            lease_token=continued.lease_token,
            fencing_token=continued.fencing_token,
            partition_key=partition,
            result={"done": True},
            now_ms=now + 2,
        )

        assert client.run_steps_many(
            [CreateItem(f"py-sdk:native-run-steps:{suffix}:a", {"n": 1})],
            type=flow_type,
            states=["queued", "done"],
            worker="py-sdk-native-run-worker",
            partition_key=partition,
            now_ms=now,
            result={"done": True},
        )

        schedule_id = f"py-sdk:native-schedule:{suffix}"
        scheduled_flow_id = f"py-sdk:native-scheduled-flow:{suffix}"
        schedule_target = {
            "id": scheduled_flow_id,
            "type": flow_type,
            "state": "scheduled",
            "partition_key": partition,
            "payload": {"scheduled": True},
        }
        assert client.schedule_create(
            schedule_id,
            target=schedule_target,
            kind="one_shot",
            at_ms=now + 60_000,
            overwrite=True,
            now_ms=now,
        )
        assert client.schedule_get(schedule_id) is not None
        assert client.schedule_pause(schedule_id, now_ms=now + 1)
        assert client.schedule_resume(schedule_id, now_ms=now + 2)
        assert isinstance(client.schedule_list(count=10), list)
        delete_schedule_id = f"py-sdk:native-schedule-delete:{suffix}"
        assert client.schedule_create(
            delete_schedule_id,
            target={**schedule_target, "id": f"{scheduled_flow_id}:delete"},
            kind="one_shot",
            at_ms=now + 120_000,
            overwrite=True,
            now_ms=now,
        )
        assert client.schedule_delete(delete_schedule_id, now_ms=now + 3)
        assert client.schedule_fire(schedule_id, now_ms=now + 3)
        assert client.schedule_fire_due(now_ms=now + 4, limit=1) is not None

        gov_flow_id, gov_partition, gov_job = _create_and_claim(
            client, flow_type, suffix, "native-governance", now_ms=now
        )
        effect_key = "send-email"
        reserved = client.effect_reserve(
            gov_flow_id,
            effect_key,
            "email.send",
            partition_key=gov_partition,
            lease_token=gov_job.lease_token,
            fencing_token=gov_job.fencing_token,
            operation_digest="digest-1",
            idempotency_key=f"idem:{suffix}",
            now_ms=now + 10,
        )
        assert reserved is not None
        assert client.effect_confirm(
            gov_flow_id,
            effect_key,
            partition_key=gov_partition,
            lease_token=gov_job.lease_token,
            fencing_token=gov_job.fencing_token,
            external_id="mail-1",
            latency_ms=12,
            now_ms=now + 11,
        )
        assert client.effect_get(gov_flow_id, effect_key, partition_key=gov_partition) is not None
        compensated_effect_key = "send-push"
        assert client.effect_reserve(
            gov_flow_id,
            compensated_effect_key,
            "push.send",
            partition_key=gov_partition,
            lease_token=gov_job.lease_token,
            fencing_token=gov_job.fencing_token,
            operation_digest="digest-1b",
            idempotency_key=f"idem:{suffix}:compensate",
            now_ms=now + 12,
        )
        assert client.effect_compensate(
            gov_flow_id,
            compensated_effect_key,
            partition_key=gov_partition,
            lease_token=gov_job.lease_token,
            fencing_token=gov_job.fencing_token,
            reason="rollback",
            now_ms=now + 13,
        )
        failed_effect_key = "send-sms"
        assert client.effect_reserve(
            gov_flow_id,
            failed_effect_key,
            "sms.send",
            partition_key=gov_partition,
            lease_token=gov_job.lease_token,
            fencing_token=gov_job.fencing_token,
            operation_digest="digest-2",
            idempotency_key=f"idem:{suffix}:fail",
            now_ms=now + 14,
        )
        assert client.effect_fail(
            gov_flow_id,
            failed_effect_key,
            partition_key=gov_partition,
            lease_token=gov_job.lease_token,
            fencing_token=gov_job.fencing_token,
            reason="provider-error",
            latency_ms=20,
            now_ms=now + 15,
        )
        assert isinstance(client.governance_ledger(gov_flow_id, partition_key=gov_partition), list)

        approval_id = f"py-sdk:native-approval:{suffix}"
        approval = client.approval_request(
            approval_id,
            flow_id=gov_flow_id,
            scope=f"approval:{suffix}",
            reason="manual check",
            requested_by="integration",
            assignees=["ops"],
            now_ms=now + 15,
        )
        assert approval is not None
        assert client.approval_get(approval_id) is not None
        assert isinstance(client.approval_list(scope=f"approval:{suffix}", limit=10), list)
        assert client.approval_approve(
            approval_id,
            approver="ops",
            reason="ok",
            now_ms=now + 16,
        )
        rejected_id = f"py-sdk:native-approval-reject:{suffix}"
        assert client.approval_request(
            rejected_id,
            flow_id=gov_flow_id,
            scope=f"approval:{suffix}",
            reason="manual reject",
            requested_by="integration",
            now_ms=now + 17,
        )
        assert client.approval_reject(
            rejected_id,
            approver="ops",
            reason="no",
            now_ms=now + 18,
        )

        circuit_scope = f"circuit:{suffix}"
        assert client.circuit_open(circuit_scope, open_ms=1_000, now_ms=now + 19)
        assert client.circuit_get(circuit_scope) is not None
        assert client.circuit_close(circuit_scope, now_ms=now + 20)

        budget_scope = f"budget:{suffix}"
        assert client.budget_reserve(
            budget_scope,
            5,
            limit=100,
            window_ms=60_000,
            reservation_id=f"reservation:{suffix}:commit",
            now_ms=now + 21,
        )
        assert client.budget_commit(
            budget_scope,
            f"reservation:{suffix}:commit",
            4,
            usage={"tokens": 4},
            now_ms=now + 22,
        )
        assert client.budget_reserve(
            budget_scope,
            3,
            limit=100,
            window_ms=60_000,
            reservation_id=f"reservation:{suffix}:release",
            now_ms=now + 23,
        )
        assert client.budget_release(
            budget_scope,
            f"reservation:{suffix}:release",
            now_ms=now + 24,
        )
        assert client.budget_get(budget_scope) is not None
        assert isinstance(client.budget_list(scope=budget_scope, limit=10), list)

        limit_scope = f"limit:{suffix}"
        assert client.limit_lease(
            limit_scope,
            shard_id=0,
            amount=5,
            ttl_ms=30_000,
            limit=10,
            now_ms=now + 25,
        )
        spent = client.limit_spend(limit_scope, shard_id=0, amount=2, now_ms=now + 26)
        spent_lease = _field(spent, "lease")
        assert _field(spent_lease, "in_use") == 2
        reservation_ids = _field(spent, "reservation_ids")
        assert isinstance(reservation_ids, list)
        assert len(reservation_ids) == 2
        assert client.limit_release(
            limit_scope,
            shard_id=0,
            reservation_ids=reservation_ids,
            now_ms=now + 27,
        )
        assert client.limit_get(limit_scope, now_ms=now + 28) is not None
        assert isinstance(client.limit_list(scope=limit_scope, limit=10, now_ms=now + 29), list)
        assert isinstance(client.governance_overview(limit=10), object)
    finally:
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
        assert client.value_mget([value_ref]) == [{"shared": True}]

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
        batch_jobs = client.claim_flows(
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
        many_jobs = client.claim_flows(
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
        many_complete = client.claim_flows(
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
        retry_many_jobs = client.claim_flows(
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
        retry_many_again = client.claim_flows(
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
        cancel_many_ids = [
            f"py-sdk:cancel-many:{suffix}:a",
            f"py-sdk:cancel-many:{suffix}:b",
        ]
        assert client.create_many(
            cancel_many_partition,
            [CreateItem(flow_id) for flow_id in cancel_many_ids],
            type=flow_type,
            state="cancel-many",
            now_ms=now,
            run_at_ms=now,
        )
        assert client.cancel_many(
            cancel_many_partition,
            [FencedItem(flow_id, 0) for flow_id in cancel_many_ids],
            reason={"cancel": "many"},
            now_ms=now,
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
            include_record=False,
        )
        assert len(reclaimed) == 1
        assert reclaimed[0].id == reclaim_id
        assert isinstance(reclaimed[0], ClaimedFlow)
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

        parent_flow_id = f"py-sdk:parent:{suffix}"
        parent_partition = f"{parent_flow_id}:partition"
        client.create(
            parent_flow_id,
            type=flow_type,
            state="dispatch",
            partition_key=parent_partition,
            correlation_id=f"corr:{suffix}",
            root_flow_id=f"root:{suffix}",
            now_ms=now,
            idempotent=True,
        )
        parent = client.get(parent_flow_id, partition_key=parent_partition)
        assert parent is not None
        assert client.spawn_children(
            parent_flow_id,
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
        assert isinstance(client.by_parent(parent_flow_id), list)
        assert isinstance(client.by_root(f"root:{suffix}"), list)
        assert isinstance(client.by_correlation(f"corr:{suffix}"), list)

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

        assert isinstance(client.list(flow_type, count=100), list)
        assert isinstance(client.info(flow_type), dict)
        assert isinstance(client.history(signal_id, partition_key=signal_partition, count=5), list)
        assert isinstance(client.retention_cleanup(limit=10), dict)
    finally:
        client.close()


def test_real_ferricstore_native_protocol_named_session_and_data_structure_surface() -> None:
    _require_protocol_transport()

    client = _client()
    producer = _client()
    suffix = _suffix()
    keys: list[str] = []

    def key(name: str) -> str:
        value = f"py-sdk:native-named:{suffix}:{name}"
        keys.append(value)
        return value

    try:
        kv_key = key("kv")
        assert _ok(client.kv_set(kv_key, {"answer": 42}))
        assert client.kv_get(kv_key) == {"answer": 42}

        hash_key = key("hash")
        assert client.hset(hash_key, {"field": {"nested": True}}) == 1
        assert client.hget(hash_key, "field") == {"nested": True}

        list_key = key("list")
        assert client.rpush(list_key, {"job": 1}) == 1
        assert _decode(client, client.lpop(list_key)) == {"job": 1}

        set_key = key("set")
        assert client.sadd(set_key, "a", "b") == 2
        assert client.sismember(set_key, "a") == 1

        zset_key = key("zset")
        assert client.zadd(zset_key, {"a": 1.5}) == 1
        assert client.zscore(zset_key, "a") is not None

        stream_key = key("stream")
        assert client.xadd(stream_key, {"field": {"ok": True}})
        assert client.xlen(stream_key) == 1

        tx_key = key("tx")
        with client.transaction() as tx:
            assert tx.kv_set(tx_key, {"tx": True}) in {"QUEUED", b"QUEUED"}
        assert client.kv_get(tx_key) == {"tx": True}

        blocking_key = key("blocking")
        holder: dict[str, Any] = {}

        def blocking_pop() -> None:
            holder["result"] = client.blpop(blocking_key, timeout=3)

        thread = threading.Thread(target=blocking_pop)
        thread.start()
        time.sleep(0.05)
        assert producer.rpush(blocking_key, {"work": 1}) == 1
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert _decode(client, holder["result"][1]) == {"work": 1}

        channel = key("channel")
        pubsub = client.pubsub_session()
        pubsub.subscribe(channel)
        assert producer.publish(channel, {"event": 1}) >= 1
        message = pubsub.get_message(timeout=3)
        assert message is not None
        assert message.channel == channel
        assert message.message == {"event": 1}
        pubsub.unsubscribe(channel)
        pubsub.close()

        pattern_pubsub = client.pubsub_session()
        pattern = f"{channel}:*"
        pattern_channel = f"{channel}:pattern"
        pattern_pubsub.psubscribe(pattern)
        assert producer.publish(pattern_channel, {"event": 2}) >= 1
        pattern_message = pattern_pubsub.get_message(timeout=3)
        assert pattern_message is not None
        assert pattern_message.channel == pattern_channel
        assert pattern_message.message == {"event": 2}
        pattern_pubsub.punsubscribe(pattern)
        pattern_pubsub.close()
    finally:
        with suppress(Exception):
            if keys:
                client.delete(*keys)
        client.close()
        producer.close()


def test_real_ferricstore_named_data_helpers_cover_native_protocol_surface() -> None:
    _require_protocol_transport()

    client = _client()
    suffix = _suffix()
    prefix = f"py-sdk:native-helper:{{{suffix}}}:"
    keys: list[str] = []
    covered: set[str] = set()

    def key(name: str) -> str:
        value = f"{prefix}{name}"
        keys.append(value)
        return value

    def call(name: str, *args: Any, **kwargs: Any) -> Any:
        covered.add(name)
        return getattr(client, name)(*args, **kwargs)

    try:
        string_key = key("string")
        assert _ok(call("set", string_key, "abc", px=60_000, encode=False))
        assert _ok(call("kv_set", key("kv"), {"answer": 42}, px=60_000))
        assert call("kv_get", keys[-1]) == {"answer": 42}
        assert call("exists", string_key) == 1
        assert _ok(
            call(
                "mset",
                {
                    key("mset-a"): "a",
                    key("mset-b"): "b",
                },
                encode=False,
            )
        )
        assert call("mget", keys[-2], keys[-1], decode=False)[:2] in ([b"a", b"b"], ["a", "b"])
        assert _ok(
            call(
                "kv_mset",
                {
                    key("kmset-a"): {"a": 1},
                    key("kmset-b"): {"b": 2},
                },
            )
        )
        assert call("kv_mget", keys[-2], keys[-1]) == [{"a": 1}, {"b": 2}]
        assert (
            call(
                "msetnx",
                {
                    key("msetnx-a"): "a",
                    key("msetnx-b"): "b",
                },
                encode=False,
            )
            == 1
        )
        counter_key = key("counter")
        assert call("incr", counter_key) == 1
        assert call("incrby", counter_key, 4) == 5
        assert call("decr", counter_key) == 4
        assert call("decrby", counter_key, 2) == 2
        assert float(_text(call("incrbyfloat", key("float"), 1.5))) >= 1.5
        append_key = key("append")
        assert call("append", append_key, "abc", encode=False) == 3
        assert call("strlen", append_key) == 3
        assert call("getrange", append_key, 0, 1) in (b"ab", "ab")
        assert call("setrange", append_key, 1, "Q", encode=False) == 3
        assert call("getex", append_key, px=60_000, decode=False) in (b"aQc", "aQc")
        assert call("ttl", append_key) >= 0
        assert call("pttl", append_key) >= 0
        assert call("persist", append_key) in (0, 1)
        assert call("expire", append_key, 60) == 1
        assert call("pexpire", append_key, 60_000) == 1
        assert call("expireat", append_key, int(time.time()) + 60) == 1
        assert call("pexpireat", append_key, int(time.time() * 1000) + 60_000) == 1
        assert call("expiretime", append_key) >= 0
        assert call("pexpiretime", append_key) >= 0
        assert call("type", append_key) in (b"string", "string")
        assert call("setnx", key("setnx"), "1", encode=False) == 1
        assert _ok(call("setex", key("setex"), 60, "1", encode=False))
        assert _ok(call("psetex", key("psetex"), 60_000, "1", encode=False))
        copy_key = key("copy")
        renamed_key = key("renamed")
        renamed_nx_key = key("renamed-nx")
        assert call("copy", string_key, copy_key, "REPLACE") == 1
        assert _ok(call("rename", copy_key, renamed_key))
        assert call("renamenx", renamed_key, renamed_nx_key) == 1
        assert call("randomkey") is not None
        assert call("scan", 0, "MATCH", f"{prefix}*") is not None
        assert call("keys", f"{prefix}*")
        assert call("dbsize") >= 1
        assert call("object", "ENCODING", string_key) is not None
        assert call("memory", "USAGE", string_key) >= 0
        assert call("getdel", key("getdel"), decode=False) is None
        assert call("unlink", key("unlink-missing")) >= 0
        assert call("delete", key("delete-missing")) >= 0
        assert call("kv_delete", key("kv-delete-missing")) >= 0

        hash_key = key("hash")
        assert call("hset", hash_key, {"field": "value", "count": "1"}) >= 1
        assert call("hget", hash_key, "field") == "value"
        assert call("hmget", hash_key, "field", "missing")[0] == "value"
        assert call("hgetall", hash_key)
        assert call("hexists", hash_key, "field") == 1
        assert call("hkeys", hash_key)
        assert call("hvals", hash_key)
        assert call("hlen", hash_key) >= 2
        assert call("hdel", hash_key, "field") == 1

        raw_hash_key = key("hash-raw")
        assert client.command("HSET", raw_hash_key, "field", "value", "count", "1") >= 1
        assert call("hincrby", raw_hash_key, "count", 2) == 3
        assert float(_text(call("hincrbyfloat", raw_hash_key, "float", 1.25))) >= 1.25
        assert call("hsetnx", raw_hash_key, "new", "item", encode=False) == 1
        assert call("hstrlen", raw_hash_key, "field") == 5
        assert call("hrandfield", raw_hash_key, 1, "WITHVALUES")
        assert call("hscan", raw_hash_key, 0)
        assert call("hexpire", raw_hash_key, 60, "field")[0] in (1, -1)
        assert call("httl", raw_hash_key, "field")
        assert call("hpersist", raw_hash_key, "field")
        assert call("hpexpire", raw_hash_key, 60_000, "field")[0] in (1, -1)
        assert call("hpttl", raw_hash_key, "field")
        assert call("hexpiretime", raw_hash_key, "field")
        assert call("hgetex", raw_hash_key, "PX", 60_000, "FIELDS", 1, "field")[0] in (
            b"value",
            "value",
        )
        assert call("hsetex", raw_hash_key, 60, "temp", "1") >= 0
        assert call("hgetdel", raw_hash_key, "temp")[0] in (b"1", "1")
        assert call("hdel", raw_hash_key, "new") == 1

        list_key = key("list")
        list_dst = key("list-dst")
        assert call("lpush", list_key, "b", "a", encode=False) == 2
        assert call("rpush", list_key, "c", encode=False) == 3
        assert call("lrange", list_key, 0, -1)
        assert call("llen", list_key) == 3
        assert call("lindex", list_key, 0) in (b"a", "a")
        assert _ok(call("lset", list_key, 1, "bb", encode=False))
        assert call("lrem", list_key, 0, "bb", encode=False) == 1
        assert _ok(call("ltrim", list_key, 0, 1))
        assert call("lpos", list_key, "a", encode=False) == 0
        assert call("linsert", list_key, "AFTER", "a", "aa", encode=False) >= 0
        assert call("lmove", list_key, list_dst, "LEFT", "RIGHT") is not None
        assert call("rpoplpush", list_dst, list_key) is not None
        assert call("lpushx", list_key, "left", encode=False) >= 1
        assert call("rpushx", list_key, "right", encode=False) >= 1
        assert call("blpop", list_key, timeout=1) is not None
        assert call("rpush", list_key, "block", encode=False) >= 1
        assert call("brpop", list_key, timeout=1) is not None
        assert call("rpush", list_key, "move", encode=False) >= 1
        assert call("blmove", list_key, list_dst, "LEFT", "RIGHT", 1) is not None
        assert call("rpush", list_key, "mpop", encode=False) >= 1
        assert call("blmpop", 1, [list_key], "LEFT", count=1) is not None
        assert call("rpush", list_key, "tail", encode=False) >= 1
        assert call("rpop", list_key) is not None
        assert call("lpop", list_key) is not None

        set_a = key("set-a")
        set_b = key("set-b")
        assert call("sadd", set_a, "a", "b", encode=False) == 2
        assert call("sadd", set_b, "b", "c", encode=False) == 2
        assert call("sismember", set_a, "a", encode=False) == 1
        assert call("smismember", set_a, "a", "z", encode=False)
        assert call("scard", set_a) == 2
        assert call("smembers", set_a)
        assert call("srandmember", set_a, 1)
        assert call("sdiff", set_a, set_b)
        assert call("sinter", set_a, set_b)
        assert call("sunion", set_a, set_b)
        assert call("sdiffstore", key("sdiff"), set_a, set_b) >= 0
        assert call("sinterstore", key("sinter"), set_a, set_b) >= 0
        assert call("sunionstore", key("sunion"), set_a, set_b) >= 0
        assert call("sintercard", 2, set_a, set_b, "LIMIT", 10) >= 0
        assert call("smove", set_a, set_b, "a", encode=False) in (0, 1)
        assert call("sscan", set_a, 0)
        assert call("spop", set_b, 1) is not None
        assert call("srem", set_a, "b", encode=False) in (0, 1)

        zset = key("zset")
        assert call("zadd", zset, {"a": 1, "b": 2, "c": 3}) == 3
        assert call("zscore", zset, "a") is not None
        assert call("zrank", zset, "a") == 0
        assert call("zrevrank", zset, "c") == 0
        assert call("zrange", zset, 0, -1)
        assert call("zrevrange", zset, 0, -1)
        assert call("zcard", zset) == 3
        assert _text(call("zincrby", zset, 1, "a"))
        assert call("zcount", zset, "-inf", "+inf") >= 3
        assert call("zrandmember", zset, 1, "WITHSCORES")
        assert call("zmscore", zset, "a", "missing")
        assert call("zrangebyscore", zset, "-inf", "+inf")
        assert call("zrevrangebyscore", zset, "+inf", "-inf")
        assert call("zscan", zset, 0)
        assert call("zrem", zset, "b") == 1
        assert call("zpopmin", zset, 1)
        assert call("zpopmax", zset, 1)

        bitmap = key("bitmap")
        bitmap_out = key("bitmap-out")
        assert call("setbit", bitmap, 7, 1) == 0
        assert call("getbit", bitmap, 7) == 1
        assert call("bitcount", bitmap) >= 1
        assert call("bitpos", bitmap, 1) >= 0
        assert call("bitop", "OR", bitmap_out, bitmap) >= 1

        hll = key("hll")
        hll_dst = key("hll-dst")
        assert call("pfadd", hll, "a", "b") in (0, 1)
        assert call("pfcount", hll) >= 1
        assert _ok(call("pfmerge", hll_dst, hll))

        geo = key("geo")
        geo_dst = key("geo-dst")
        assert call("geoadd", geo, 13.361389, 38.115556, "palermo") == 1
        assert call("geoadd", geo, 15.087269, 37.502669, "catania") == 1
        assert call("geopos", geo, "palermo")
        assert call("geodist", geo, "palermo", "catania", "km") is not None
        assert call("geohash", geo, "palermo")
        assert call("geosearch", geo, "FROMMEMBER", "palermo", "BYRADIUS", 200, "km")
        assert (
            call(
                "geosearchstore",
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

        stream = key("stream")
        stream_id = call("xadd", stream, {"field": "value"}, encode=False)
        group = f"group-{suffix}"
        assert stream_id is not None
        assert call("xlen", stream) >= 1
        assert call("xrange", stream, "-", "+")
        assert call("xrevrange", stream, "+", "-")
        assert call("xread", {stream: "0"}, count=1) is not None
        assert call("xinfo", "STREAM", stream)
        assert _ok(call("xgroup", "CREATE", stream, group, "0"))
        assert call("xreadgroup", group, "consumer", {stream: ">"}, count=1) is not None
        assert call("xack", stream, group, stream_id) >= 0
        assert call("xtrim", stream, "MAXLEN", "~", 10) >= 0
        assert call("xdel", stream, stream_id) >= 0

        bloom = key("bf")
        assert _ok(call("bf_reserve", bloom, 0.01, 100))
        assert call("bf_add", bloom, "a") in (0, 1)
        assert call("bf_madd", bloom, "b", "c")
        assert call("bf_exists", bloom, "a") in (0, 1)
        assert call("bf_mexists", bloom, "a", "z")
        assert call("bf_card", bloom) >= 1
        assert call("bf_info", bloom)

        cuckoo = key("cf")
        assert _ok(call("cf_reserve", cuckoo, 100))
        assert call("cf_add", cuckoo, "a") in (0, 1)
        assert call("cf_addnx", cuckoo, "b") in (0, 1)
        assert call("cf_exists", cuckoo, "a") in (0, 1)
        assert call("cf_mexists", cuckoo, "a", "z")
        assert call("cf_count", cuckoo, "a") >= 0
        assert call("cf_del", cuckoo, "a") in (0, 1)
        assert call("cf_info", cuckoo)

        cms_a = key("cms-a")
        cms_b = key("cms-b")
        cms_dst = key("cms-dst")
        cms_prob = key("cms-prob")
        assert _ok(call("cms_initbydim", cms_a, 20, 4))
        assert _ok(call("cms_initbydim", cms_b, 20, 4))
        assert _ok(call("cms_initbyprob", cms_prob, 0.01, 0.99))
        assert call("cms_incrby", cms_a, "a", 2, "b", 3)
        assert call("cms_incrby", cms_b, "a", 1)
        assert call("cms_query", cms_a, "a", "b")
        assert _ok(call("cms_merge", cms_dst, 2, cms_a, cms_b))
        assert call("cms_info", cms_dst)

        topk = key("topk")
        assert _ok(call("topk_reserve", topk, 3))
        assert call("topk_add", topk, "a", "b", "a")
        assert call("topk_incrby", topk, "c", 2)
        assert call("topk_query", topk, "a", "z")
        assert call("topk_list", topk, "WITHCOUNT")
        assert call("topk_count", topk, "a", "z")
        assert call("topk_info", topk)

        tdigest = key("tdigest")
        tdigest_src = key("tdigest-src")
        tdigest_dst = key("tdigest-dst")
        assert _ok(call("tdigest_create", tdigest))
        assert _ok(call("tdigest_add", tdigest, 1, 2, 3, 4))
        assert call("tdigest_quantile", tdigest, 0.5)
        assert call("tdigest_cdf", tdigest, 2)
        assert call("tdigest_rank", tdigest, 2)
        assert call("tdigest_revrank", tdigest, 2)
        assert call("tdigest_byrank", tdigest, 1)
        assert call("tdigest_byrevrank", tdigest, 1)
        assert call("tdigest_trimmed_mean", tdigest, 0.1, 0.9) is not None
        assert call("tdigest_min", tdigest) is not None
        assert call("tdigest_max", tdigest) is not None
        assert call("tdigest_info", tdigest)
        assert _ok(call("tdigest_create", tdigest_src))
        assert _ok(call("tdigest_add", tdigest_src, 5, 6))
        assert _ok(call("tdigest_merge", tdigest_dst, 2, tdigest, tdigest_src, "OVERRIDE"))
        assert _ok(call("tdigest_reset", tdigest))

        assert call("ping") in (b"PONG", "PONG", True)
        assert call("echo", "hello") in (b"hello", "hello")
        assert call("server_info") is not None
        assert call("command_info", "GET") is not None
        assert call("slowlog", "GET", 10) is not None
        assert call("config", "GET", "*") is not None
        with client.transaction(watch=[key("watched")]) as transaction:
            assert transaction.set(key("tx-set"), "value", encode=False) in (
                b"QUEUED",
                "QUEUED",
            )
        assert call("publish", key("channel"), {"event": "helper"}) >= 0
        assert call("pubsub", "CHANNELS") is not None

        non_docker_safe_helpers = {
            "command",
            "flushall",
            "flushdb",
            "select",
            "subscribe",
            "unsubscribe",
            "psubscribe",
            "punsubscribe",
        }
        session_only_helpers = {"discard", "multi", "transaction_exec", "unwatch", "watch"}
        public_helpers = {
            name
            for name, value in DataCommandsMixin.__dict__.items()
            if callable(value) and not name.startswith("_")
        }
        assert public_helpers - covered - non_docker_safe_helpers - session_only_helpers == set()
    finally:
        with suppress(Exception):
            if keys:
                client.delete(*keys)
        client.close()


def test_real_ferricstore_topology_client_reroutes_after_leader_node_stop() -> None:
    _require_protocol_transport()
    project, compose_file = _require_cluster_failure_fixture()

    client = _topology_client()
    suffix = _suffix()
    key = f"{{py-sdk-failover:{suffix}}}:kv"
    stopped_service: str | None = None
    stopped_url: str | None = None

    try:
        before_route = client.route(key)
        old_leader = _text(before_route["leader_node"])
        old_endpoint = before_route["endpoint"]
        stopped_service = _service_from_node(old_leader)
        stopped_url = (
            f"ferric://{_field(old_endpoint, 'host')}:{_field(old_endpoint, 'native_port')}"
        )

        assert _ok(client.kv_set(key, {"phase": "before"}))
        assert client.kv_get(key) == {"phase": "before"}

        _docker_compose(project, compose_file, "stop", "-t", "1", stopped_service, timeout=90)

        with pytest.raises((FerricStoreError, OSError, TimeoutError, ConnectionError)):
            client.kv_set(key, {"phase": "stale-route"})

        after_route = _wait_route_change(client, key, old_leader)
        assert _text(after_route["leader_node"]) != old_leader

        assert _ok(client.kv_set(key, {"phase": "after"}))
        assert client.kv_get(key) == {"phase": "after"}
    finally:
        if stopped_service is not None:
            _docker_compose(project, compose_file, "start", stopped_service, timeout=90)
        if stopped_url is not None:
            _wait_native_url(stopped_url, timeout=90)
        if stopped_service is not None:
            # FerricStore removes a stopped voter before it is restarted.  The
            # restored process is reachable again, but the surviving quorum is
            # intentionally a two-voter membership until an administrative
            # rejoin.  That server lifecycle is separate from SDK rerouting.
            _wait_cluster_synced(minimum_members=2, timeout=120)
        with suppress(Exception):
            client.delete(key)
        client.close()
