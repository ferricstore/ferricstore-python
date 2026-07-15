from __future__ import annotations

import asyncio
import threading

import pytest

from ferricstore import AsyncFlowClient, AsyncWorkflow, FlowClient, InvalidCommandError
from ferricstore.async_workflow_budget import AsyncWorkflowBudget
from ferricstore.protocol_async_topology import AsyncTopologyProtocolAdapterPool
from ferricstore.protocol_codec import EncodedValueLimitError, encode_value
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_common import _encode_request_body
from ferricstore.protocol_lifecycle import PendingRequestCapacityError
from ferricstore.protocol_sync_topology import TopologyProtocolAdapterPool
from ferricstore.topology_lifecycle import EndpointAdapterLifecycle
from ferricstore.types import (
    BudgetResult,
    CircuitBreakerStatus,
    ClaimedFlow,
    CreateItem,
    GovernanceOverview,
)
from ferricstore.workflow import Workflow, complete, state


def test_governance_overview_exposes_kv_circuits_as_typed_results() -> None:
    overview = GovernanceOverview.from_resp(
        {
            b"circuits": [
                {
                    b"scope": b"payments",
                    b"status": b"open",
                    b"failure_threshold": 5,
                }
            ],
            b"counts": {b"circuits": 1, b"open_circuits": 1},
        }
    )

    assert overview.circuits == [
        CircuitBreakerStatus(
            scope="payments",
            status="open",
            failure_threshold=5,
            raw={
                "scope": "payments",
                "status": "open",
                "failure_threshold": 5,
            },
        )
    ]
    assert overview.counts == {"circuits": 1, "open_circuits": 1}


def test_flow_boolean_on_matches_the_kv_command_grammar() -> None:
    command = build_protocol_command(
        "FLOW.CREATE",
        "flow-1",
        "TYPE",
        "jobs",
        "STATE",
        "queued",
        "IDEMPOTENT",
        "ON",
    )

    assert command.payload["idempotent"] is True


def test_invalid_flow_boolean_is_not_silently_rewritten_to_false() -> None:
    with pytest.raises(InvalidCommandError, match="boolean"):
        build_protocol_command(
            "FLOW.CREATE",
            "flow-1",
            "TYPE",
            "jobs",
            "STATE",
            "queued",
            "IDEMPOTENT",
            "definitely-not-a-boolean",
        )


def test_workflow_start_and_claim_preserves_an_explicit_empty_initial_state() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.kwargs: dict[str, object] | None = None

        def start_and_claim(self, _id: str, **kwargs: object) -> object:
            self.kwargs = kwargs
            return object()

    class ExampleWorkflow(Workflow):
        type = "jobs"
        initial_state = "queued"

        @state("queued")
        def queued(self, _ctx: object) -> object:
            return complete()

    recorder = Recorder()
    workflow = ExampleWorkflow(recorder)  # type: ignore[arg-type]

    workflow.start_and_claim("flow-1", worker="worker-1", initial_state="")

    assert recorder.kwargs is not None
    assert recorder.kwargs["initial_state"] == ""

    context = workflow.context(
        ClaimedFlow("flow-1", b"lease", 1, run_state="queued"),
        state_name="",
    )
    assert context.logical_state == ""


def test_async_workflow_run_once_does_not_replace_an_explicit_empty_state() -> None:
    class Client:
        def __init__(self) -> None:
            self.claim_calls = 0

        async def claim_flows(self, *_args: object, **_kwargs: object) -> list[object]:
            self.claim_calls += 1
            return []

    async def exercise() -> None:
        client = Client()
        workflow = AsyncWorkflow(client, type="jobs", states=["queued"])

        @workflow.state("queued")
        async def queued(_ctx: object) -> object:
            return complete()

        with pytest.raises(ValueError, match=r"no handler.*''"):
            await workflow.run_once(state="")
        assert client.claim_calls == 0

    asyncio.run(exercise())


def test_protocol_string_byte_limit_is_checked_before_utf8_allocation() -> None:
    class EncodingMustNotRun(str):
        def encode(self, *_args: object, **_kwargs: object) -> bytes:
            raise AssertionError("oversized string was encoded before max_bytes was checked")

    with pytest.raises(EncodedValueLimitError, match="exceeds max_bytes"):
        encode_value(EncodingMustNotRun("oversized"), max_bytes=5)


def test_raw_protocol_bytes_subclass_cannot_lie_about_request_body_size() -> None:
    class MisreportedBytes(bytes):
        def __len__(self) -> int:
            return 0

    with pytest.raises(PendingRequestCapacityError, match="max_pending_request_bytes=3"):
        _encode_request_body(
            MisreportedBytes(b"payload"),
            compression="none",
            max_body_bytes=3,
            pending_limit=3,
        )

    regular = b"payload"
    body, compressed = _encode_request_body(
        regular,
        compression="none",
        max_body_bytes=len(regular),
        pending_limit=len(regular),
    )
    assert body is regular
    assert compressed is False


def test_sync_topology_event_poll_holds_endpoint_lease_until_poll_finishes() -> None:
    lifecycle = EndpointAdapterLifecycle[str](is_idle=lambda _adapter: True)

    class Adapter:
        def __init__(self) -> None:
            self.closed = False

        def wait_event(self, *, timeout: float) -> str:
            assert timeout == 0.0
            ready = lifecycle.install(set(), lambda _key, _adapter: None)
            for adapter in ready:
                adapter.closed = True
            if self.closed:
                raise RuntimeError("topology closed adapter during event poll")
            return "event"

    adapter = Adapter()
    lifecycle.put("node", adapter)
    pool = object.__new__(TopologyProtocolAdapterPool)
    pool._lock = threading.RLock()
    pool._closed = False
    pool._endpoint_lifecycle = lifecycle
    pool._adapters = lifecycle.active

    def release(lease: object) -> None:
        ready = lifecycle.release(lease)  # type: ignore[arg-type]
        if ready is not None:
            ready.closed = True

    pool._release_adapter_lease = release  # type: ignore[method-assign]

    assert pool._take_event() == "event"
    assert adapter.closed is True


def test_async_topology_event_poll_holds_endpoint_lease_until_poll_finishes() -> None:
    async def exercise() -> None:
        lifecycle = EndpointAdapterLifecycle[str](is_idle=lambda _adapter: True)

        class Adapter:
            def __init__(self) -> None:
                self.closed = False

            async def wait_event(self, *, timeout: float) -> str:
                assert timeout == 0.0
                ready = lifecycle.install(set(), lambda _key, _adapter: None)
                for adapter in ready:
                    adapter.closed = True
                if self.closed:
                    raise RuntimeError("topology closed adapter during event poll")
                return "event"

        adapter = Adapter()
        lifecycle.put("node", adapter)
        pool = object.__new__(AsyncTopologyProtocolAdapterPool)
        pool._closed = False
        pool._endpoint_lifecycle = lifecycle
        pool._adapters = lifecycle.active

        def release(lease: object) -> None:
            ready = lifecycle.release(lease)  # type: ignore[arg-type]
            if ready is not None:
                ready.closed = True

        pool._release_adapter_lease = release  # type: ignore[method-assign]

        assert await pool._take_event() == "event"
        assert adapter.closed is True

    asyncio.run(exercise())


def test_async_workflow_budget_commit_can_retry_after_a_failed_attempt() -> None:
    class Client:
        def __init__(self) -> None:
            self.commit_calls = 0

        async def budget_reserve(self, *_args: object, **_kwargs: object) -> BudgetResult:
            return BudgetResult(reservation_id="reservation-1", status="reserved")

        async def budget_commit(self, *_args: object, **_kwargs: object) -> BudgetResult:
            self.commit_calls += 1
            if self.commit_calls == 1:
                raise RuntimeError("transient commit failure")
            return BudgetResult(reservation_id="reservation-1", status="committed")

    class Context:
        def __init__(self) -> None:
            self.client = Client()

        def _record_budget_result(self, *_args: object) -> None:
            pass

    async def exercise() -> None:
        context = Context()
        budget = AsyncWorkflowBudget(context, scope="tenant", amount=1)  # type: ignore[arg-type]
        await budget.__aenter__()

        with pytest.raises(RuntimeError, match="transient commit failure"):
            await budget.commit()

        result = await budget.commit()
        assert result.status == "committed"
        assert context.client.commit_calls == 2

    asyncio.run(exercise())


def test_sync_and_async_create_many_emit_the_same_wire_command() -> None:
    sync_calls: list[tuple[object, ...]] = []
    async_calls: list[tuple[object, ...]] = []

    class SyncExecutor:
        def execute_command(self, *args: object) -> list[bytes]:
            sync_calls.append(args)
            return [b"OK", b"OK"]

    class AsyncExecutor:
        async def execute_command(self, *args: object) -> list[bytes]:
            async_calls.append(args)
            return [b"OK", b"OK"]

    items = [
        CreateItem("flow-1", b"one"),
        CreateItem("flow-2", b"two", partition_key="tenant-2"),
    ]
    kwargs = {
        "type": "jobs",
        "state": "queued",
        "now_ms": 123,
        "priority": 1,
        "idempotent": True,
        "independent": True,
        "values": {"shared": b"value"},
    }

    FlowClient(SyncExecutor()).create_many(None, items, **kwargs)  # type: ignore[arg-type]

    async def exercise() -> None:
        await AsyncFlowClient(AsyncExecutor()).create_many(  # type: ignore[arg-type]
            None,
            items,
            **kwargs,
        )

    asyncio.run(exercise())

    assert async_calls == sync_calls
