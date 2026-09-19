from __future__ import annotations

import asyncio
import os
import ssl
import time
import uuid
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, FlowClient, FlowStatePolicy, RetryPolicy
from ferricstore.protocol_codec import decode_value
from ferricstore.protocol_commands import build_protocol_command, encode_frame
from ferricstore.protocol_constants import _HEADER


class _RecordingExecutor:
    def __init__(self, response: Any | None = None) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.response = response if response is not None else {b"type": b"orders", b"generation": 1}

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        return self.response


class _AsyncRecordingExecutor(_RecordingExecutor):
    async def execute_command(self, *args: Any) -> Any:
        return super().execute_command(*args)


@pytest.fixture
def ferricstore_url() -> str:
    if os.environ.get("FERRICSTORE_INTEGRATION") != "1":
        pytest.skip("set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests")
    return os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")


def _client_options(url: str) -> dict[str, Any]:
    if not url.startswith(("http://", "https://")):
        return {}
    options: dict[str, Any] = {}
    username = os.environ.get("FERRICSTORE_USERNAME")
    password = os.environ.get("FERRICSTORE_PASSWORD")
    ca_file = os.environ.get("FERRICSTORE_CA_FILE")
    http2 = os.environ.get("FERRICSTORE_HTTP2")
    if username is not None:
        options["username"] = username
    if password is not None:
        options["password"] = password
    if ca_file is not None:
        context = ssl.create_default_context(cafile=ca_file)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        options["ssl_context"] = context
    if http2 is not None:
        options["http2"] = http2.lower() in {"1", "true", "yes"}
    return options


def _wire_payload(command_args: tuple[Any, ...]) -> dict[bytes, Any]:
    command = build_protocol_command(*command_args)
    frame = encode_frame(
        command.opcode,
        command.lane_id,
        1,
        command.payload,
        command.flags,
    )
    _magic, _version, _flags, _lane, _opcode, _request_id, body_len = _HEADER.unpack(
        frame[: _HEADER.size]
    )
    body = frame[_HEADER.size :]
    assert len(body) == body_len
    payload, remaining = decode_value(body)
    assert remaining == b""
    assert isinstance(payload, dict)
    return payload


def _retry_wire_values(policy: RetryPolicy) -> dict[bytes, Any]:
    return {
        b"max_retries": policy.max_retries,
        b"backoff": {
            b"kind": policy.backoff.encode(),
            b"base_ms": policy.base_ms,
            b"max_ms": policy.max_ms,
            b"jitter_pct": policy.jitter_pct,
        },
        b"exhausted_to": policy.exhausted_to.encode(),
    }


def test_sync_install_policy_wire_payload_nests_type_and_state_retry() -> None:
    executor = _RecordingExecutor()
    client = FlowClient(executor)
    type_policy = RetryPolicy(
        max_retries=5,
        backoff="linear",
        base_ms=17,
        max_ms=211,
        jitter_pct=13,
        exhausted_to="failed",
    )
    state_policy = RetryPolicy(
        max_retries=2,
        backoff="fixed",
        base_ms=7,
        max_ms=19,
        jitter_pct=3,
        exhausted_to="failed",
    )

    client.install_policy(
        "orders",
        retry=type_policy,
        states={"queued": FlowStatePolicy.fifo(retry=state_policy)},
    )

    payload = _wire_payload(executor.calls[-1])
    assert payload[b"retry"] == _retry_wire_values(type_policy)
    assert payload[b"states"][b"queued"][b"mode"] == b"FIFO"
    assert payload[b"states"][b"queued"][b"retry"] == _retry_wire_values(state_policy)
    for field in (
        b"max_retries",
        b"backoff",
        b"base_ms",
        b"max_ms",
        b"jitter_pct",
        b"exhausted_to",
    ):
        assert field not in payload


def test_async_install_policy_wire_payload_nests_type_and_state_retry() -> None:
    async def run() -> None:
        executor = _AsyncRecordingExecutor()
        client = AsyncFlowClient(executor)
        type_policy = RetryPolicy(
            max_retries=5,
            backoff="linear",
            base_ms=17,
            max_ms=211,
            jitter_pct=13,
            exhausted_to="failed",
        )
        state_policy = RetryPolicy(
            max_retries=2,
            backoff="fixed",
            base_ms=7,
            max_ms=19,
            jitter_pct=3,
            exhausted_to="failed",
        )

        await client.install_policy(
            "orders",
            retry=type_policy,
            states={"queued": FlowStatePolicy.fifo(retry=state_policy)},
        )

        payload = _wire_payload(executor.calls[-1])
        assert payload[b"retry"] == _retry_wire_values(type_policy)
        assert payload[b"states"][b"queued"][b"mode"] == b"FIFO"
        assert payload[b"states"][b"queued"][b"retry"] == _retry_wire_values(state_policy)
        for field in (
            b"max_retries",
            b"backoff",
            b"base_ms",
            b"max_ms",
            b"jitter_pct",
            b"exhausted_to",
        ):
            assert field not in payload

    asyncio.run(run())


def test_sync_rewind_wire_payload_uses_encoded_reason() -> None:
    executor = _RecordingExecutor(response=b"OK")
    client = FlowClient(executor)

    client.rewind(
        "flow-1",
        to_event="created-1",
        partition_key="tenant-1",
        expect_state="failed",
        run_at_ms=2_000,
        reason="rewind-reason",
        now_ms=1_000,
    )

    payload = _wire_payload(executor.calls[-1])
    assert payload[b"reason"] == b"rewind-reason"
    assert b"reason_ref" not in payload
    assert b"reason_ref" not in executor.calls[-1]


def test_async_rewind_wire_payload_uses_encoded_reason() -> None:
    async def run() -> None:
        executor = _AsyncRecordingExecutor(response=b"OK")
        client = AsyncFlowClient(executor)

        await client.rewind(
            "flow-1",
            to_event="created-1",
            partition_key="tenant-1",
            expect_state="failed",
            run_at_ms=2_000,
            reason="rewind-reason",
            now_ms=1_000,
        )

        payload = _wire_payload(executor.calls[-1])
        assert payload[b"reason"] == b"rewind-reason"
        assert b"reason_ref" not in payload
        assert b"reason_ref" not in executor.calls[-1]

    asyncio.run(run())


def test_sync_rewind_rejects_unsupported_reason_ref() -> None:
    executor = _RecordingExecutor(response=b"OK")
    client = FlowClient(executor)
    unsupported_kwargs: dict[str, Any] = {"reason_ref": "unsupported"}

    with pytest.raises(TypeError, match="reason_ref"):
        client.rewind("flow-1", to_event="created-1", **unsupported_kwargs)
    assert executor.calls == []


def test_async_rewind_rejects_unsupported_reason_ref() -> None:
    async def run() -> None:
        executor = _AsyncRecordingExecutor(response=b"OK")
        client = AsyncFlowClient(executor)
        unsupported_kwargs: dict[str, Any] = {"reason_ref": "unsupported"}

        with pytest.raises(TypeError, match="reason_ref"):
            await client.rewind("flow-1", to_event="created-1", **unsupported_kwargs)
        assert executor.calls == []

    asyncio.run(run())


def _event_id(event: Any) -> str:
    if isinstance(event, (list, tuple)) and event:
        value = event[0]
    elif isinstance(event, dict):
        value = event.get("event_id", event.get(b"event_id", event.get("id", event.get(b"id"))))
    else:
        raise AssertionError(f"history event does not contain an event id: {event!r}")
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _assert_retry_snapshot(
    retry: dict[str, Any] | None,
    policy: RetryPolicy,
) -> None:
    assert retry == {
        "max_retries": policy.max_retries,
        "backoff": {
            "kind": policy.backoff,
            "base_ms": policy.base_ms,
            "max_ms": policy.max_ms,
            "jitter_pct": policy.jitter_pct,
        },
        "exhausted_to": policy.exhausted_to,
    }


def _policy_roundtrip(
    client: FlowClient,
    type_policy: RetryPolicy,
    state_policy: RetryPolicy,
) -> None:
    flow_type = f"py-sdk-policy-retry-{uuid.uuid4().hex}"
    snapshot = client.install_policy(
        flow_type,
        retry=type_policy,
        states={"queued": FlowStatePolicy.fifo(retry=state_policy)},
        replace=True,
    )
    _assert_retry_snapshot(snapshot.retry, type_policy)
    assert snapshot.states is not None
    assert snapshot.states["queued"]["mode"] == "fifo"
    _assert_retry_snapshot(snapshot.states["queued"]["retry"], state_policy)

    fetched = client.policy_get(flow_type)
    _assert_retry_snapshot(fetched.retry, type_policy)
    assert fetched.states is not None
    _assert_retry_snapshot(fetched.states["queued"]["retry"], state_policy)


def test_sync_policy_retry_roundtrip_preserves_nested_fields(ferricstore_url: str) -> None:
    client = FlowClient.from_url(
        ferricstore_url,
        **_client_options(ferricstore_url),
    )
    try:
        _policy_roundtrip(
            client,
            RetryPolicy(
                max_retries=5,
                backoff="linear",
                base_ms=17,
                max_ms=211,
                jitter_pct=13,
                exhausted_to="failed",
            ),
            RetryPolicy(
                max_retries=2,
                backoff="fixed",
                base_ms=7,
                max_ms=19,
                jitter_pct=3,
                exhausted_to="failed",
            ),
        )
    finally:
        client.close()


def test_async_policy_retry_roundtrip_preserves_nested_fields(ferricstore_url: str) -> None:
    async def run() -> None:
        client = AsyncFlowClient.from_url(
            ferricstore_url,
            **_client_options(ferricstore_url),
        )
        type_policy = RetryPolicy(
            max_retries=5,
            backoff="linear",
            base_ms=17,
            max_ms=211,
            jitter_pct=13,
            exhausted_to="failed",
        )
        state_policy = RetryPolicy(
            max_retries=2,
            backoff="fixed",
            base_ms=7,
            max_ms=19,
            jitter_pct=3,
            exhausted_to="failed",
        )
        try:
            flow_type = f"py-sdk-policy-retry-{uuid.uuid4().hex}"
            snapshot = await client.install_policy(
                flow_type,
                retry=type_policy,
                states={"queued": FlowStatePolicy.fifo(retry=state_policy)},
                replace=True,
            )
            _assert_retry_snapshot(snapshot.retry, type_policy)
            assert snapshot.states is not None
            assert snapshot.states["queued"]["mode"] == "fifo"
            _assert_retry_snapshot(snapshot.states["queued"]["retry"], state_policy)

            fetched = await client.policy_get(flow_type)
            _assert_retry_snapshot(fetched.retry, type_policy)
            assert fetched.states is not None
            _assert_retry_snapshot(fetched.states["queued"]["retry"], state_policy)
        finally:
            await client.close()

    asyncio.run(run())


def _rewind_roundtrip(client: FlowClient, *, reason: str) -> None:
    suffix = uuid.uuid4().hex
    flow_type = f"py-sdk-rewind-{suffix}"
    flow_id = f"py-sdk-rewind-flow-{suffix}"
    partition = f"py-sdk-rewind-partition-{suffix}"
    now_ms = int(time.time() * 1000)

    client.create(
        flow_id,
        type=flow_type,
        state="queued",
        partition_key=partition,
        now_ms=now_ms - 1,
        run_at_ms=now_ms - 1,
        idempotent=True,
    )
    history = client.history(flow_id, partition_key=partition, count=10)
    assert history
    created_event_id = _event_id(history[0])
    claimed = client.claim_due(
        flow_type,
        state="queued",
        worker="py-sdk-rewind-regression-worker",
        partition_key=partition,
        limit=1,
        now_ms=now_ms,
    )
    assert claimed
    job = claimed[0]
    client.transition(
        flow_id,
        from_state="running",
        to_state="ready",
        lease_token=job.lease_token,
        fencing_token=job.fencing_token,
        partition_key=partition,
        now_ms=now_ms + 1,
    )

    client.rewind(
        flow_id,
        to_event=created_event_id,
        partition_key=partition,
        expect_state="ready",
        reason=reason,
        now_ms=now_ms + 2,
    )
    rewound = client.get(flow_id, partition_key=partition)
    assert rewound is not None
    assert rewound.state == "queued"
    assert rewound.raw is not None
    reason_ref = rewound.raw.get("error_ref", rewound.raw.get(b"error_ref"))
    assert reason_ref
    if isinstance(reason_ref, bytes):
        reason_ref = reason_ref.decode()
    assert client.value_mget([str(reason_ref)]) == [reason.encode()]


def test_sync_rewind_reason_roundtrip(ferricstore_url: str) -> None:
    client = FlowClient.from_url(
        ferricstore_url,
        **_client_options(ferricstore_url),
    )
    try:
        _rewind_roundtrip(client, reason="sync-rewind-reason")
    finally:
        client.close()


def test_async_rewind_reason_roundtrip(ferricstore_url: str) -> None:
    async def run() -> None:
        client = AsyncFlowClient.from_url(
            ferricstore_url,
            **_client_options(ferricstore_url),
        )
        try:
            suffix = uuid.uuid4().hex
            flow_type = f"py-sdk-async-rewind-{suffix}"
            flow_id = f"py-sdk-async-rewind-flow-{suffix}"
            partition = f"py-sdk-async-rewind-partition-{suffix}"
            now_ms = int(time.time() * 1000)
            await client.create(
                flow_id,
                type=flow_type,
                state="queued",
                partition_key=partition,
                now_ms=now_ms - 1,
                run_at_ms=now_ms - 1,
                idempotent=True,
            )
            history = await client.history(flow_id, partition_key=partition, count=10)
            assert history
            created_event_id = _event_id(history[0])
            claimed = await client.claim_due(
                flow_type,
                state="queued",
                worker="py-sdk-async-rewind-regression-worker",
                partition_key=partition,
                limit=1,
                now_ms=now_ms,
            )
            assert claimed
            job = claimed[0]
            await client.transition(
                flow_id,
                from_state="running",
                to_state="ready",
                lease_token=job.lease_token,
                fencing_token=job.fencing_token,
                partition_key=partition,
                now_ms=now_ms + 1,
            )
            await client.rewind(
                flow_id,
                to_event=created_event_id,
                partition_key=partition,
                expect_state="ready",
                reason="async-rewind-reason",
                now_ms=now_ms + 2,
            )
            rewound = await client.get(flow_id, partition_key=partition)
            assert rewound is not None
            assert rewound.state == "queued"
            assert rewound.raw is not None
            reason_ref = rewound.raw.get("error_ref", rewound.raw.get(b"error_ref"))
            assert reason_ref
            if isinstance(reason_ref, bytes):
                reason_ref = reason_ref.decode()
            assert await client.value_mget([str(reason_ref)]) == [b"async-rewind-reason"]
        finally:
            await client.close()

    asyncio.run(run())
