from __future__ import annotations

import asyncio
import inspect
import os
import ssl
import time
import uuid
from typing import Any

import pytest

from ferricstore import (
    AsyncFlowClient,
    AsyncWorkflow,
    FlowClient,
    FlowWorkflow,
    JsonCodec,
    StaleLeaseError,
    transition,
)
from ferricstore.durable_step import durable_step_value_name
from ferricstore.types import ClaimedFlow

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_INTEGRATION") != "1",
    reason="set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests",
)

_LEASE_MS = 150


class WorkerStopped(BaseException):
    """Simulate abrupt worker termination without running SDK error recovery."""


class FaultAfterClosureExecutor:
    def __init__(self, executor: Any, *, after_commit: bool) -> None:
        self.executor = executor
        self.after_commit = after_commit
        self.triggered = False

    def execute_command(self, *args: Any) -> Any:
        if args and args[0] == "FLOW.STEP_CONTINUE" and not self.triggered:
            self.triggered = True
            if not self.after_commit:
                raise WorkerStopped()
            self.executor.execute_command(*args)
            raise WorkerStopped()
        return self.executor.execute_command(*args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.executor, name)


class AsyncFaultAfterClosureExecutor:
    def __init__(self, executor: Any, *, after_commit: bool) -> None:
        self.executor = executor
        self.after_commit = after_commit
        self.triggered = False

    async def execute_command(self, *args: Any) -> Any:
        if args and args[0] == "FLOW.STEP_CONTINUE" and not self.triggered:
            self.triggered = True
            if not self.after_commit:
                raise WorkerStopped()
            result = self.executor.execute_command(*args)
            if inspect.isawaitable(result):
                _ = await result
            raise WorkerStopped()
        result = self.executor.execute_command(*args)
        return await result if inspect.isawaitable(result) else result

    def __getattr__(self, name: str) -> Any:
        return getattr(self.executor, name)


class ConnectionErrorAfterCommitExecutor:
    def __init__(self, executor: Any) -> None:
        self.executor = executor
        self.triggered = False
        self.calls: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        result = self.executor.execute_command(*args)
        if args and args[0] == "FLOW.STEP_CONTINUE" and not self.triggered:
            self.triggered = True
            raise ConnectionError("committed STEP_CONTINUE response lost")
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self.executor, name)


class AsyncConnectionErrorAfterCommitExecutor:
    def __init__(self, executor: Any) -> None:
        self.executor = executor
        self.triggered = False
        self.calls: list[tuple[Any, ...]] = []

    async def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        result = self.executor.execute_command(*args)
        result = await result if inspect.isawaitable(result) else result
        if args and args[0] == "FLOW.STEP_CONTINUE" and not self.triggered:
            self.triggered = True
            raise ConnectionError("committed STEP_CONTINUE response lost")
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self.executor, name)


def _url() -> str:
    return os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")


def _client_options() -> dict[str, Any]:
    if not _url().startswith(("http://", "https://")):
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


def _sync_client() -> FlowClient:
    return FlowClient.from_url(_url(), codec=JsonCodec(), **_client_options())


def _async_client() -> AsyncFlowClient:
    return AsyncFlowClient.from_url(_url(), codec=JsonCodec(), **_client_options())


def _create(client: FlowClient, flow_type: str, flow_id: str, partition: str) -> None:
    now_ms = int(time.time() * 1000)
    client.create(
        flow_id,
        type=flow_type,
        state="charge",
        partition_key=partition,
        now_ms=now_ms,
        run_at_ms=now_ms,
    )


async def _create_async(
    client: AsyncFlowClient, flow_type: str, flow_id: str, partition: str
) -> None:
    now_ms = int(time.time() * 1000)
    await client.create(
        flow_id,
        type=flow_type,
        state="charge",
        partition_key=partition,
        now_ms=now_ms,
        run_at_ms=now_ms,
    )


def _run_until_stopped(worker: Any, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = worker.run_once()
        except WorkerStopped:
            return
        if result.claimed == 0:
            time.sleep(0.01)
    raise AssertionError("worker did not claim and stop before the deadline")


async def _run_async_until_stopped(
    workflow: AsyncWorkflow, *, state: str, timeout: float = 5.0
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            result = await workflow.run_once(state=state)
        except WorkerStopped:
            return
        if result.claimed == 0:
            await asyncio.sleep(0.01)
    raise AssertionError("async worker did not claim and stop before the deadline")


def test_sync_worker_retries_uncommitted_step_and_rejects_stale_worker() -> None:
    client = _sync_client()
    suffix = uuid.uuid4().hex
    flow_type = f"py-durable-uncommitted-{suffix}"
    flow_id = f"flow-{suffix}"
    partition = f"partition-{suffix}"
    attempts: list[str] = []
    claims: dict[str, ClaimedFlow] = {}
    refreshed: dict[str, ClaimedFlow] = {}

    try:
        _create(client, flow_type, flow_id, partition)
        workflow_a = FlowWorkflow(client, type=flow_type, initial_state="charge")

        @workflow_a.state("charge", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def worker_a(ctx: Any) -> Any:
            claims["a"] = ctx.job

            def stop_before_result() -> Any:
                attempts.append("a")
                raise WorkerStopped()

            return ctx.client.step(
                ctx.job,
                name="charge-customer:v1",
                run=stop_before_result,
                to_state="warn",
                lease_ms=_LEASE_MS,
            )

        _run_until_stopped(
            workflow_a.worker(worker="worker-a", state="charge", partition_key=partition)
        )
        time.sleep((_LEASE_MS + 75) / 1000)

        workflow_b = FlowWorkflow(client, type=flow_type, initial_state="charge")

        @workflow_b.state("charge", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def worker_b(ctx: Any) -> Any:
            claims["b"] = ctx.job

            def finish() -> dict[str, str]:
                attempts.append("b")
                return {"charge_id": "ch_123"}

            refreshed["b"], _ = ctx.client.step(
                ctx.job,
                name="charge-customer:v1",
                run=finish,
                to_state="warn",
                lease_ms=_LEASE_MS,
            )
            raise WorkerStopped()

        _run_until_stopped(
            workflow_b.worker(worker="worker-b", state="charge", partition_key=partition)
        )

        assert attempts == ["a", "b"]
        assert claims["b"].lease_token != claims["a"].lease_token
        assert claims["b"].fencing_token > claims["a"].fencing_token
        assert claims["b"].run_state == "charge"
        assert refreshed["b"].run_state == "warn"
        assert refreshed["b"].lease_token != claims["b"].lease_token
        assert refreshed["b"].fencing_token > claims["b"].fencing_token

        replayed, result = client.step(
            refreshed["b"],
            name="charge-customer:v1",
            run=lambda: pytest.fail("committed closure ran again"),
            to_state="warn",
            lease_ms=_LEASE_MS,
        )
        assert replayed.run_state == "warn"
        assert result == {"charge_id": "ch_123"}

        with pytest.raises(StaleLeaseError):
            client.advance(claims["a"], to_state="stale-write", lease_ms=_LEASE_MS)
    finally:
        client.close()


def test_sync_worker_context_step_releases_waiting_state_with_refreshed_claim() -> None:
    client = _sync_client()
    suffix = uuid.uuid4().hex
    flow_type = f"py-durable-context-{suffix}"
    flow_id = f"flow-{suffix}"
    partition = f"partition-{suffix}"
    claims: list[ClaimedFlow] = []
    results: list[Any] = []
    wait_until_ms = int(time.time() * 1000) + 60_000

    try:
        _create(client, flow_type, flow_id, partition)
        workflow = FlowWorkflow(client, type=flow_type, initial_state="charge")

        @workflow.state("charge", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def charge(ctx: Any) -> Any:
            claims.append(ctx.job)
            results.append(
                ctx.step(
                    name="charge-customer:v1",
                    run=lambda: {"charge_id": "ch_context"},
                    to_state="prepared",
                    lease_ms=_LEASE_MS,
                )
            )
            return transition("waiting", run_at_ms=wait_until_ms)

        applied = workflow.worker(
            worker="worker-a",
            state="charge",
            partition_key=partition,
        ).run_once()

        assert applied.claimed == 1
        assert applied.applied == 1
        assert results == [{"charge_id": "ch_context"}]
        assert (
            workflow.worker(
                worker="worker-a",
                state="charge",
                partition_key=partition,
            )
            .run_once()
            .claimed
            == 0
        )
        record = client.get(
            flow_id,
            partition_key=partition,
            values=[durable_step_value_name("charge-customer:v1")],
        )
        assert record.state == "waiting"
        assert record.lease_token == b""
        assert record.fencing_token > claims[0].fencing_token
        assert record.values == {
            durable_step_value_name("charge-customer:v1"): {"charge_id": "ch_context"}
        }
    finally:
        client.close()


def test_sync_worker_surfaces_connection_error_after_commit_without_stale_mutation() -> None:
    client_a = _sync_client()
    client_b = _sync_client()
    suffix = uuid.uuid4().hex
    flow_type = f"py-durable-uncertain-{suffix}"
    flow_id = f"flow-{suffix}"
    partition = f"partition-{suffix}"

    try:
        _create(client_b, flow_type, flow_id, partition)
        fault = ConnectionErrorAfterCommitExecutor(  # type: ignore[attr-defined]
            client_a.executor._executor
        )
        client_a.executor._executor = fault  # type: ignore[attr-defined]
        workflow = FlowWorkflow(client_a, type=flow_type, initial_state="charge")

        @workflow.state(
            "charge",
            lease_ms=_LEASE_MS,
            claim_payload=False,
            claim_record=False,
            exception_policy="retry",
        )
        def charge(ctx: Any) -> Any:
            return ctx.step(
                name="charge-customer:v1",
                run=lambda: {"charge_id": "ch_uncertain"},
                to_state="warn",
                lease_ms=_LEASE_MS,
            )

        with pytest.raises(ConnectionError, match="committed STEP_CONTINUE response lost"):
            workflow.worker(
                worker="worker-a",
                state="charge",
                partition_key=partition,
            ).run_once()

        commands = [call[0] for call in fault.calls]
        assert commands.count("FLOW.STEP_CONTINUE") == 1
        assert not {"FLOW.RETRY", "FLOW.FAIL", "FLOW.COMPLETE"}.intersection(commands)
        record = client_b.get(
            flow_id,
            partition_key=partition,
            values=[durable_step_value_name("charge-customer:v1")],
        )
        assert record.run_state == "warn"
        assert record.values == {
            durable_step_value_name("charge-customer:v1"): {"charge_id": "ch_uncertain"}
        }
    finally:
        client_a.close()
        client_b.close()


def test_sync_worker_reuses_provider_idempotency_after_external_success() -> None:
    client_a = _sync_client()
    client_b = _sync_client()
    suffix = uuid.uuid4().hex
    flow_type = f"py-durable-external-{suffix}"
    flow_id = f"flow-{suffix}"
    partition = f"partition-{suffix}"
    provider_effects: dict[str, dict[str, str]] = {}
    provider_calls = 0
    claims: dict[str, ClaimedFlow] = {}

    def charge_provider(key: str) -> dict[str, str]:
        nonlocal provider_calls
        provider_calls += 1
        return provider_effects.setdefault(key, {"charge_id": f"ch_{len(provider_effects) + 1}"})

    try:
        _create(client_b, flow_type, flow_id, partition)
        client_a.executor._executor = FaultAfterClosureExecutor(  # type: ignore[attr-defined]
            client_a.executor._executor,
            after_commit=False,  # type: ignore[attr-defined]
        )
        workflow_a = FlowWorkflow(client_a, type=flow_type, initial_state="charge")

        @workflow_a.state("charge", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def worker_a(ctx: Any) -> Any:
            claims["a"] = ctx.job
            key = f"{ctx.id}:charge-customer:v1"
            return ctx.client.step(
                ctx.job,
                name="charge-customer:v1",
                run=lambda: charge_provider(key),
                to_state="warn",
                lease_ms=_LEASE_MS,
            )

        _run_until_stopped(
            workflow_a.worker(worker="worker-a", state="charge", partition_key=partition)
        )
        time.sleep((_LEASE_MS + 75) / 1000)

        workflow_b = FlowWorkflow(client_b, type=flow_type, initial_state="charge")

        @workflow_b.state("charge", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def worker_b(ctx: Any) -> Any:
            claims["b"] = ctx.job
            key = f"{ctx.id}:charge-customer:v1"
            _job, result = ctx.client.step(
                ctx.job,
                name="charge-customer:v1",
                run=lambda: charge_provider(key),
                to_state="warn",
                lease_ms=_LEASE_MS,
            )
            assert result == {"charge_id": "ch_1"}
            raise WorkerStopped()

        _run_until_stopped(
            workflow_b.worker(worker="worker-b", state="charge", partition_key=partition)
        )

        assert provider_calls == 2
        assert len(provider_effects) == 1
        assert claims["b"].lease_token != claims["a"].lease_token
        assert claims["b"].fencing_token > claims["a"].fencing_token
    finally:
        client_a.close()
        client_b.close()


def test_sync_worker_recovers_after_commit_response_is_lost_without_rerun() -> None:
    client_a = _sync_client()
    client_b = _sync_client()
    suffix = uuid.uuid4().hex
    flow_type = f"py-durable-committed-{suffix}"
    flow_id = f"flow-{suffix}"
    partition = f"partition-{suffix}"
    executions = 0
    claims: dict[str, ClaimedFlow] = {}

    try:
        _create(client_b, flow_type, flow_id, partition)
        client_a.executor._executor = FaultAfterClosureExecutor(  # type: ignore[attr-defined]
            client_a.executor._executor,
            after_commit=True,  # type: ignore[attr-defined]
        )
        workflow_a = FlowWorkflow(client_a, type=flow_type, initial_state="charge")

        @workflow_a.state("charge", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def worker_a(ctx: Any) -> Any:
            nonlocal executions
            claims["a"] = ctx.job

            def finish() -> dict[str, str]:
                nonlocal executions
                executions += 1
                return {"charge_id": "ch_committed"}

            return ctx.client.step(
                ctx.job,
                name="charge-customer:v1",
                run=finish,
                to_state="warn",
                lease_ms=_LEASE_MS,
            )

        _run_until_stopped(
            workflow_a.worker(worker="worker-a", state="charge", partition_key=partition)
        )
        time.sleep((_LEASE_MS + 75) / 1000)

        workflow_b = FlowWorkflow(client_b, type=flow_type, initial_state="warn")

        @workflow_b.state("warn", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def worker_b(ctx: Any) -> Any:
            claims["b"] = ctx.job
            _job, result = ctx.client.step(
                ctx.job,
                name="charge-customer:v1",
                run=lambda: pytest.fail("committed closure ran again"),
                to_state="warn",
                lease_ms=_LEASE_MS,
            )
            assert result == {"charge_id": "ch_committed"}
            raise WorkerStopped()

        _run_until_stopped(
            workflow_b.worker(worker="worker-b", state="warn", partition_key=partition)
        )

        assert executions == 1
        assert claims["b"].run_state == "warn"
        assert claims["b"].lease_token != claims["a"].lease_token
        assert claims["b"].fencing_token > claims["a"].fencing_token
        record = client_b.get(
            flow_id,
            partition_key=partition,
            values=[durable_step_value_name("charge-customer:v1")],
        )
        assert record.values == {
            durable_step_value_name("charge-customer:v1"): {"charge_id": "ch_committed"}
        }
    finally:
        client_a.close()
        client_b.close()


def test_waiting_workflow_releases_claim_and_resumes_on_another_worker() -> None:
    client = _sync_client()
    suffix = uuid.uuid4().hex
    flow_type = f"py-durable-wait-{suffix}"
    flow_id = f"flow-{suffix}"
    partition = f"partition-{suffix}"
    now_ms = int(time.time() * 1000)
    executions = 0
    claims: dict[str, ClaimedFlow] = {}

    try:
        _create(client, flow_type, flow_id, partition)
        workflow_a = FlowWorkflow(client, type=flow_type, initial_state="charge")

        @workflow_a.state("charge", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def wait_for_approval(ctx: Any) -> Any:
            nonlocal executions
            executions += 1
            claims["a"] = ctx.job
            prepared, result = ctx.client.step(
                ctx.job,
                name="prepare-warning:v1",
                run=lambda: {"ok": True},
                to_state="prepared",
                lease_ms=_LEASE_MS,
            )
            assert result == {"ok": True}
            ctx.client.transition(
                prepared.id,
                from_state="running",
                to_state="waiting",
                lease_token=prepared.lease_token,
                fencing_token=prepared.fencing_token,
                partition_key=prepared.partition_key,
                run_at_ms=now_ms + 60_000,
                return_record=True,
            )
            raise WorkerStopped()

        _run_until_stopped(
            workflow_a.worker(worker="worker-a", state="charge", partition_key=partition)
        )
        waiting = client.get(flow_id, partition_key=partition)
        assert waiting.state == "waiting"
        assert waiting.lease_token == b""
        assert durable_step_value_name("prepare-warning:v1") in waiting.value_refs

        client.signal(
            flow_id,
            signal="approved",
            partition_key=partition,
            if_state="waiting",
            transition_to="resume",
            run_at_ms=int(time.time() * 1000),
        )
        workflow_b = FlowWorkflow(client, type=flow_type, initial_state="resume")

        @workflow_b.state("resume", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def resume(ctx: Any) -> Any:
            claims["b"] = ctx.job
            _job, result = ctx.client.step(
                ctx.job,
                name="prepare-warning:v1",
                run=lambda: pytest.fail("completed preparation ran again"),
                to_state="resume",
                lease_ms=_LEASE_MS,
            )
            assert result == {"ok": True}
            raise WorkerStopped()

        _run_until_stopped(
            workflow_b.worker(worker="worker-b", state="resume", partition_key=partition)
        )

        assert executions == 1
        assert claims["b"].run_state == "resume"
        assert claims["b"].lease_token != claims["a"].lease_token
        assert claims["b"].fencing_token > claims["a"].fencing_token
    finally:
        client.close()


def test_waiting_workflows_do_not_pin_sync_worker_capacity() -> None:
    client = _sync_client()
    suffix = uuid.uuid4().hex
    flow_type = f"py-durable-wait-capacity-{suffix}"
    partition = f"partition-{suffix}"
    flow_ids = [f"flow-{suffix}-a", f"flow-{suffix}-b"]
    claims: dict[str, ClaimedFlow] = {}
    handled: list[str] = []

    try:
        for flow_id in flow_ids:
            _create(client, flow_type, flow_id, partition)
        workflow = FlowWorkflow(client, type=flow_type, initial_state="charge")

        @workflow.state("charge", lease_ms=_LEASE_MS, claim_payload=False, claim_record=False)
        def wait(ctx: Any) -> Any:
            claims[ctx.id] = ctx.job
            handled.append(ctx.id)
            return transition("waiting", run_at_ms=int(time.time() * 1000) + 60_000)

        worker = workflow.worker(
            worker="worker-a",
            state="charge",
            partition_key=partition,
            batch_size=2,
        )
        result = worker.run_once()

        assert result.claimed == 2
        assert result.applied == 2
        assert sorted(handled) == sorted(flow_ids)
        assert worker.run_once().claimed == 0
        for flow_id in flow_ids:
            waiting = client.get(flow_id, partition_key=partition)
            assert waiting.state == "waiting"
            assert waiting.lease_token == b""
            assert claims[flow_id].lease_token
    finally:
        client.close()


def test_async_worker_retries_uncommitted_step_with_same_takeover_semantics() -> None:
    async def scenario() -> None:
        client = _async_client()
        suffix = uuid.uuid4().hex
        flow_type = f"py-durable-async-{suffix}"
        flow_id = f"flow-{suffix}"
        partition = f"partition-{suffix}"
        claims: dict[str, ClaimedFlow] = {}
        attempts: list[str] = []

        try:
            await _create_async(client, flow_type, flow_id, partition)
            workflow_a = AsyncWorkflow(
                client,
                type=flow_type,
                states=["charge"],
                initial_state="charge",
                partition_key=partition,
                batch_size=1,
            )

            @workflow_a.state("charge")
            async def worker_a(ctx: Any) -> Any:
                claims["a"] = ctx.job

                async def stop_before_result() -> Any:
                    attempts.append("a")
                    raise WorkerStopped()

                return await ctx.client.step(
                    ctx.job,
                    name="charge-customer:v1",
                    run=stop_before_result,
                    to_state="warn",
                    lease_ms=_LEASE_MS,
                )

            await _run_async_until_stopped(workflow_a, state="charge")
            await asyncio.sleep((_LEASE_MS + 75) / 1000)

            workflow_b = AsyncWorkflow(
                client,
                type=flow_type,
                states=["charge"],
                initial_state="charge",
                partition_key=partition,
                batch_size=1,
            )

            @workflow_b.state("charge")
            async def worker_b(ctx: Any) -> Any:
                claims["b"] = ctx.job

                async def finish() -> dict[str, str]:
                    attempts.append("b")
                    return {"charge_id": "ch_async"}

                await ctx.client.step(
                    ctx.job,
                    name="charge-customer:v1",
                    run=finish,
                    to_state="warn",
                    lease_ms=_LEASE_MS,
                )
                raise WorkerStopped()

            await _run_async_until_stopped(workflow_b, state="charge")

            assert attempts == ["a", "b"]
            assert claims["b"].lease_token != claims["a"].lease_token
            assert claims["b"].fencing_token > claims["a"].fencing_token
            assert claims["b"].run_state == "charge"
            with pytest.raises(StaleLeaseError):
                await client.advance(claims["a"], to_state="stale-write", lease_ms=_LEASE_MS)
        finally:
            await client.close()

    asyncio.run(scenario())


def test_async_worker_context_step_releases_waiting_state_with_refreshed_claim() -> None:
    async def scenario() -> None:
        client = _async_client()
        suffix = uuid.uuid4().hex
        flow_type = f"py-durable-async-context-{suffix}"
        flow_id = f"flow-{suffix}"
        partition = f"partition-{suffix}"
        claims: list[ClaimedFlow] = []
        results: list[Any] = []
        wait_until_ms = int(time.time() * 1000) + 60_000

        try:
            await _create_async(client, flow_type, flow_id, partition)
            workflow = AsyncWorkflow(
                client,
                type=flow_type,
                states=["charge"],
                initial_state="charge",
                partition_key=partition,
                batch_size=1,
            )

            @workflow.state("charge")
            async def charge(ctx: Any) -> Any:
                claims.append(ctx.job)
                results.append(
                    await ctx.step(
                        name="charge-customer:v1",
                        run=lambda: {"charge_id": "ch_async_context"},
                        to_state="prepared",
                        lease_ms=_LEASE_MS,
                    )
                )
                return transition("waiting", run_at_ms=wait_until_ms)

            applied = await workflow.run_once(state="charge")

            assert applied.claimed == 1
            assert applied.applied == 1
            assert results == [{"charge_id": "ch_async_context"}]
            assert (await workflow.run_once(state="charge")).claimed == 0
            record = await client.get(
                flow_id,
                partition_key=partition,
                values=[durable_step_value_name("charge-customer:v1")],
            )
            assert record.state == "waiting"
            assert record.lease_token == b""
            assert record.fencing_token > claims[0].fencing_token
            assert record.values == {
                durable_step_value_name("charge-customer:v1"): {"charge_id": "ch_async_context"}
            }
        finally:
            await client.close()

    asyncio.run(scenario())


def test_async_worker_surfaces_connection_error_after_commit_without_stale_mutation() -> None:
    async def scenario() -> None:
        client_a = _async_client()
        client_b = _async_client()
        suffix = uuid.uuid4().hex
        flow_type = f"py-durable-async-uncertain-{suffix}"
        flow_id = f"flow-{suffix}"
        partition = f"partition-{suffix}"

        try:
            await _create_async(client_b, flow_type, flow_id, partition)
            fault = AsyncConnectionErrorAfterCommitExecutor(  # type: ignore[attr-defined]
                client_a.executor._executor
            )
            client_a.executor._executor = fault  # type: ignore[attr-defined]
            workflow = AsyncWorkflow(
                client_a,
                type=flow_type,
                states=["charge"],
                initial_state="charge",
                partition_key=partition,
                batch_size=1,
            )

            @workflow.state("charge", exception_policy="retry")
            async def charge(ctx: Any) -> Any:
                return await ctx.step(
                    name="charge-customer:v1",
                    run=lambda: {"charge_id": "ch_async_uncertain"},
                    to_state="warn",
                    lease_ms=_LEASE_MS,
                )

            with pytest.raises(ConnectionError, match="committed STEP_CONTINUE response lost"):
                await workflow.run_once(state="charge")

            commands = [call[0] for call in fault.calls]
            assert commands.count("FLOW.STEP_CONTINUE") == 1
            assert not {"FLOW.RETRY", "FLOW.FAIL", "FLOW.COMPLETE"}.intersection(commands)
            record = await client_b.get(
                flow_id,
                partition_key=partition,
                values=[durable_step_value_name("charge-customer:v1")],
            )
            assert record.run_state == "warn"
            assert record.values == {
                durable_step_value_name("charge-customer:v1"): {"charge_id": "ch_async_uncertain"}
            }
        finally:
            await client_a.close()
            await client_b.close()

    asyncio.run(scenario())


def test_async_worker_reuses_provider_idempotency_after_external_success() -> None:
    async def scenario() -> None:
        client_a = _async_client()
        client_b = _async_client()
        suffix = uuid.uuid4().hex
        flow_type = f"py-durable-async-external-{suffix}"
        flow_id = f"flow-{suffix}"
        partition = f"partition-{suffix}"
        provider_effects: dict[str, dict[str, str]] = {}
        provider_calls = 0
        claims: dict[str, ClaimedFlow] = {}

        async def charge_provider(key: str) -> dict[str, str]:
            nonlocal provider_calls
            provider_calls += 1
            return provider_effects.setdefault(
                key,
                {"charge_id": f"ch_{len(provider_effects) + 1}"},
            )

        try:
            await _create_async(client_b, flow_type, flow_id, partition)
            client_a.executor._executor = AsyncFaultAfterClosureExecutor(  # type: ignore[attr-defined]
                client_a.executor._executor,
                after_commit=False,  # type: ignore[attr-defined]
            )
            workflow_a = AsyncWorkflow(
                client_a,
                type=flow_type,
                states=["charge"],
                initial_state="charge",
                partition_key=partition,
                batch_size=1,
            )

            @workflow_a.state("charge")
            async def worker_a(ctx: Any) -> Any:
                claims["a"] = ctx.job
                key = f"{ctx.id}:charge-customer:v1"
                return await ctx.client.step(
                    ctx.job,
                    name="charge-customer:v1",
                    run=lambda: charge_provider(key),
                    to_state="warn",
                    lease_ms=_LEASE_MS,
                )

            await _run_async_until_stopped(workflow_a, state="charge")
            await asyncio.sleep((_LEASE_MS + 75) / 1000)

            workflow_b = AsyncWorkflow(
                client_b,
                type=flow_type,
                states=["charge"],
                initial_state="charge",
                partition_key=partition,
                batch_size=1,
            )

            @workflow_b.state("charge")
            async def worker_b(ctx: Any) -> Any:
                claims["b"] = ctx.job
                key = f"{ctx.id}:charge-customer:v1"
                _job, result = await ctx.client.step(
                    ctx.job,
                    name="charge-customer:v1",
                    run=lambda: charge_provider(key),
                    to_state="warn",
                    lease_ms=_LEASE_MS,
                )
                assert result == {"charge_id": "ch_1"}
                raise WorkerStopped()

            await _run_async_until_stopped(workflow_b, state="charge")

            assert provider_calls == 2
            assert len(provider_effects) == 1
            assert claims["b"].lease_token != claims["a"].lease_token
            assert claims["b"].fencing_token > claims["a"].fencing_token
        finally:
            await client_a.close()
            await client_b.close()

    asyncio.run(scenario())


def test_async_worker_recovers_after_commit_response_is_lost_without_rerun() -> None:
    async def scenario() -> None:
        client_a = _async_client()
        client_b = _async_client()
        suffix = uuid.uuid4().hex
        flow_type = f"py-durable-async-committed-{suffix}"
        flow_id = f"flow-{suffix}"
        partition = f"partition-{suffix}"
        claims: dict[str, ClaimedFlow] = {}
        executions = 0

        try:
            await _create_async(client_b, flow_type, flow_id, partition)
            client_a.executor._executor = AsyncFaultAfterClosureExecutor(  # type: ignore[attr-defined]
                client_a.executor._executor,
                after_commit=True,  # type: ignore[attr-defined]
            )
            workflow_a = AsyncWorkflow(
                client_a,
                type=flow_type,
                states=["charge"],
                initial_state="charge",
                partition_key=partition,
                batch_size=1,
            )

            @workflow_a.state("charge")
            async def worker_a(ctx: Any) -> Any:
                nonlocal executions
                claims["a"] = ctx.job

                async def finish() -> dict[str, str]:
                    nonlocal executions
                    executions += 1
                    return {"charge_id": "ch_async_committed"}

                return await ctx.client.step(
                    ctx.job,
                    name="charge-customer:v1",
                    run=finish,
                    to_state="warn",
                    lease_ms=_LEASE_MS,
                )

            await _run_async_until_stopped(workflow_a, state="charge")
            await asyncio.sleep((_LEASE_MS + 75) / 1000)

            workflow_b = AsyncWorkflow(
                client_b,
                type=flow_type,
                states=["warn"],
                initial_state="warn",
                partition_key=partition,
                batch_size=1,
            )

            @workflow_b.state("warn")
            async def worker_b(ctx: Any) -> Any:
                claims["b"] = ctx.job

                async def must_not_run() -> Any:
                    pytest.fail("committed closure ran again")

                _job, result = await ctx.client.step(
                    ctx.job,
                    name="charge-customer:v1",
                    run=must_not_run,
                    to_state="warn",
                    lease_ms=_LEASE_MS,
                )
                assert result == {"charge_id": "ch_async_committed"}
                raise WorkerStopped()

            await _run_async_until_stopped(workflow_b, state="warn")

            assert executions == 1
            assert claims["b"].run_state == "warn"
            assert claims["b"].lease_token != claims["a"].lease_token
            assert claims["b"].fencing_token > claims["a"].fencing_token
            record = await client_b.get(
                flow_id,
                partition_key=partition,
                values=[durable_step_value_name("charge-customer:v1")],
            )
            assert record.values == {
                durable_step_value_name("charge-customer:v1"): {"charge_id": "ch_async_committed"}
            }
        finally:
            await client_a.close()
            await client_b.close()

    asyncio.run(scenario())


def test_async_waiting_workflows_release_capacity_and_recover_on_another_worker() -> None:
    async def scenario() -> None:
        client = _async_client()
        suffix = uuid.uuid4().hex
        flow_type = f"py-durable-async-wait-{suffix}"
        partition = f"partition-{suffix}"
        flow_ids = [f"flow-{suffix}-a", f"flow-{suffix}-b"]
        claims: dict[str, ClaimedFlow] = {}
        handled: list[str] = []

        try:
            for flow_id in flow_ids:
                await _create_async(client, flow_type, flow_id, partition)
            workflow_a = AsyncWorkflow(
                client,
                type=flow_type,
                states=["charge"],
                initial_state="charge",
                partition_key=partition,
                batch_size=2,
                concurrency=2,
            )

            @workflow_a.state("charge")
            async def wait(ctx: Any) -> Any:
                claims[ctx.id] = ctx.job
                handled.append(ctx.id)
                return transition("waiting", run_at_ms=int(time.time() * 1000) + 60_000)

            result = await workflow_a.run_once(state="charge")
            assert result.claimed == 2
            assert result.applied == 2
            assert sorted(handled) == sorted(flow_ids)
            assert (await workflow_a.run_once(state="charge")).claimed == 0
            for flow_id in flow_ids:
                waiting = await client.get(flow_id, partition_key=partition)
                assert waiting.state == "waiting"
                assert waiting.lease_token == b""

            await client.signal(
                flow_ids[0],
                signal="approved",
                partition_key=partition,
                if_state="waiting",
                transition_to="resume",
                run_at_ms=int(time.time() * 1000),
            )
            workflow_b = AsyncWorkflow(
                client,
                type=flow_type,
                states=["resume"],
                initial_state="resume",
                partition_key=partition,
                batch_size=1,
            )

            @workflow_b.state("resume")
            async def resume(ctx: Any) -> Any:
                claims["b"] = ctx.job
                raise WorkerStopped()

            await _run_async_until_stopped(workflow_b, state="resume")

            assert handled.count(flow_ids[0]) == 1
            assert claims["b"].run_state == "resume"
            assert claims["b"].lease_token != claims[flow_ids[0]].lease_token
            assert claims["b"].fencing_token > claims[flow_ids[0]].fencing_token
        finally:
            await client.close()

    asyncio.run(scenario())
