import asyncio
import json

import pytest

from ferricstore import (
    AsyncFlowClient,
    AsyncWorkflow,
    ChildGroupResults,
    ChildGroupSummary,
    ChildResult,
    ChildSpec,
    FlowClient,
    JsonCodec,
    WorkflowClient,
    complete,
    transition,
)
from ferricstore.errors import FerricStoreError


def claimed_image(state=b"received"):
    return {
        b"id": b"image-1",
        b"type": b"image",
        b"state": state,
        b"partition_key": b"tenant-a",
        b"lease_token": b"lease-1",
        b"fencing_token": 11,
        b"payload": b"{}",
    }


def parent_with_groups(groups):
    return {
        b"id": b"image-1",
        b"type": b"image",
        b"state": b"resized",
        b"partition_key": b"tenant-a",
        b"child_groups": groups,
    }


RESIZES = {
    b"wait": b"all",
    b"resolved": b"failure",
    b"summary": {b"total": 3, b"completed": 1, b"failed": 1, b"cancelled": 0},
    b"children": {
        b"image-1:small": b"completed",
        b"image-1:large": b"failed",
        b"image-1:huge": b"running",
    },
    b"child_partitions": {
        b"image-1:small": b"tenant-a",
        b"image-1:large": b"tenant-a",
        b"image-1:huge": b"tenant-b",
    },
    b"results": {
        b"image-1:small": {b"status": b"completed", b"result_ref": b"ref-small"},
        b"image-1:large": {b"status": b"failed", b"error_ref": b"ref-large"},
    },
}

EXPECTED_RESIZES = ChildGroupResults(
    group_id="resizes",
    wait="all",
    resolved="failure",
    summary=ChildGroupSummary(total=3, completed=1, failed=1, cancelled=0),
    children=[
        ChildResult(id="image-1:huge", status="running", partition_key="tenant-b"),
        ChildResult(
            id="image-1:large",
            status="failed",
            partition_key="tenant-a",
            error={"message": "too large"},
        ),
        ChildResult(
            id="image-1:small",
            status="completed",
            partition_key="tenant-a",
            result={"url": "s3://small"},
        ),
    ],
)


class Executor:
    def __init__(self, responses=None):
        self.calls = []
        self.responses = dict(responses or {})

    def execute_command(self, *args):
        self.calls.append(args)
        response = self.responses.get(args[0], b"OK")
        if isinstance(response, list) and response and isinstance(response[0], Exception):
            raise response.pop(0)
        if callable(response):
            return response(args)
        return response


class AsyncExecutor(Executor):
    async def execute_command(self, *args):
        return Executor.execute_command(self, *args)


def mget_values(args):
    values = {
        "ref-small": json.dumps({"url": "s3://small"}).encode(),
        "ref-large": json.dumps({"message": "too large"}).encode(),
    }
    return [values.get(ref) for ref in args[1:] if ref != "MAX_BYTES"]


def spawn_args(call):
    names = (
        "GROUP",
        "WAIT",
        "PARTITION",
        "LEASE_TOKEN",
        "FENCING",
        "WAIT_STATE",
        "SUCCESS",
        "FAILURE",
    )
    return {name: call[call.index(name) + 1] for name in names}


def image_workflow(executor, handler):
    client = WorkflowClient(FlowClient(executor, codec=JsonCodec()))
    image = client.workflow(type="image", initial_state="received")
    image.state("received")(handler)
    return image


def test_wait_for_children_spawns_and_parks_the_parent_in_one_command():
    executor = Executor({"FLOW.CLAIM_DUE": [claimed_image()]})

    def received(ctx):
        ctx.wait_for_children(
            [
                ChildSpec(id=f"{ctx.id}:small", type="resize", payload={"size": "small"}),
                ChildSpec(id=f"{ctx.id}:large", type="resize", payload={"size": "large"}),
            ],
            group_id="resizes",
            wait_state="waiting_for_resizes",
            success="resized",
            failure="resize_failed",
        )

    result = image_workflow(executor, received).worker(states=["received"]).run_once()

    assert result.applied == 1
    assert [call[0] for call in executor.calls] == ["FLOW.CLAIM_DUE", "FLOW.SPAWN_CHILDREN"]
    spawn = executor.calls[1]
    assert spawn[1] == "image-1"
    assert spawn_args(spawn) == {
        "GROUP": "resizes",
        "WAIT": "all",
        "PARTITION": "tenant-a",
        "LEASE_TOKEN": b"lease-1",
        "FENCING": 11,
        "WAIT_STATE": "waiting_for_resizes",
        "SUCCESS": "resized",
        "FAILURE": "resize_failed",
    }
    assert "image-1:small" in spawn and "image-1:large" in spawn


def test_wait_for_children_passes_the_child_policies_through():
    executor = Executor({"FLOW.CLAIM_DUE": [claimed_image()]})

    def received(ctx):
        return ctx.wait_for_children(
            [ChildSpec(id="image-1:small", type="resize", payload=None)],
            wait="any",
            wait_state="waiting_for_resizes",
            success="resized",
            failure="resize_failed",
            on_child_failed="ignore",
            on_parent_closed="abandon_children",
        )

    image_workflow(executor, received).worker(states=["received"]).run_once()

    spawn = executor.calls[1]
    assert spawn[spawn.index("WAIT") + 1] == "any"
    assert spawn[spawn.index("ON_CHILD_FAILED") + 1] == "ignore"
    assert spawn[spawn.index("ON_PARENT_CLOSED") + 1] == "abandon_children"


def test_a_handler_cannot_return_another_outcome_after_waiting_for_children():
    executor = Executor({"FLOW.CLAIM_DUE": [claimed_image()]})

    def received(ctx):
        ctx.wait_for_children(
            [ChildSpec(id="image-1:small", type="resize", payload=None)],
            wait_state="waiting_for_resizes",
            success="resized",
            failure="resize_failed",
        )
        return transition("waiting_for_resizes")

    with pytest.raises(FerricStoreError, match="wait_for_children"):
        image_workflow(executor, received).worker(states=["received"]).run_once()
    assert [call[0] for call in executor.calls] == ["FLOW.CLAIM_DUE", "FLOW.SPAWN_CHILDREN"]


def test_wait_for_children_requires_children_and_the_three_states():
    executor = Executor({"FLOW.CLAIM_DUE": [claimed_image()]})
    errors = []
    child = [ChildSpec(id="c", type="t", payload=None)]
    states = {"wait_state": "waiting", "success": "done", "failure": "failed_children"}

    def received(ctx):
        for children, overrides in [
            ([], {}),
            (child, {"wait_state": ""}),
            (child, {"success": ""}),
            (child, {"failure": None}),
            (child, {"wait": "none"}),
        ]:
            try:
                ctx.wait_for_children(children, **{**states, **overrides})
            except ValueError as exc:
                errors.append(str(exc))
        return complete()

    image_workflow(executor, received).worker(states=["received"]).run_once()

    assert errors == [
        "wait_for_children children must be non-empty",
        "wait_for_children wait_state must be a non-empty string",
        "wait_for_children success must be a non-empty string",
        "wait_for_children failure must be a non-empty string",
        "wait_for_children wait must be 'all' or 'any'",
    ]
    assert "FLOW.SPAWN_CHILDREN" not in [call[0] for call in executor.calls]


def test_child_results_decodes_statuses_results_and_errors_in_one_value_read():
    executor = Executor(
        {
            "FLOW.GET": parent_with_groups({b"resizes": RESIZES}),
            "FLOW.VALUE.MGET": mget_values,
        }
    )
    client = FlowClient(executor, codec=JsonCodec())

    assert (
        client.child_results("image-1", partition_key="tenant-a", group_id="resizes")
        == EXPECTED_RESIZES
    )
    assert executor.calls == [
        ("FLOW.GET", "image-1", "PARTITION", "tenant-a"),
        ("FLOW.VALUE.MGET", "ref-large", "ref-small"),
    ]


def test_child_results_returns_none_for_a_missing_parent_or_group():
    client = FlowClient(Executor({"FLOW.GET": None}), codec=JsonCodec())
    assert client.child_results("missing") is None

    client = FlowClient(Executor({"FLOW.GET": parent_with_groups({})}), codec=JsonCodec())
    assert client.child_results("image-1", group_id="resizes") is None


def test_handler_reads_its_own_child_group():
    executor = Executor(
        {
            "FLOW.CLAIM_DUE": [claimed_image(b"resized")],
            "FLOW.GET": parent_with_groups({b"resizes": RESIZES}),
            "FLOW.VALUE.MGET": mget_values,
        }
    )
    seen = []
    client = WorkflowClient(FlowClient(executor, codec=JsonCodec()))
    image = client.workflow(type="image", initial_state="received")

    @image.state("resized")
    def resized(ctx):
        seen.append(ctx.flow.child_results("resizes"))
        return complete()

    image.worker(states=["resized"]).run_once()

    assert seen == [EXPECTED_RESIZES]
    assert executor.calls[1] == ("FLOW.GET", "image-1", "PARTITION", "tenant-a")


def test_handler_child_results_fails_clearly_without_the_group():
    executor = Executor(
        {"FLOW.CLAIM_DUE": [claimed_image(b"resized")], "FLOW.GET": parent_with_groups({})}
    )
    errors = []
    client = WorkflowClient(FlowClient(executor, codec=JsonCodec()))
    image = client.workflow(type="image", initial_state="received")

    @image.state("resized")
    def resized(ctx):
        try:
            ctx.flow.child_results("resizes")
        except FerricStoreError as exc:
            errors.append(str(exc))
        return complete()

    image.worker(states=["resized"]).run_once()

    assert errors == ["Flow image-1 has no child group resizes"]


def test_async_wait_for_children_spawns_and_parks_the_parent_in_one_command():
    async def run():
        executor = AsyncExecutor({"FLOW.CLAIM_DUE": [claimed_image()]})
        workflow = AsyncWorkflow(
            AsyncFlowClient(executor, codec=JsonCodec()),
            type="image",
            states=["received"],
            batch_size=1,
        )

        @workflow.state("received")
        async def received(ctx):
            await ctx.wait_for_children(
                [ChildSpec(id=f"{ctx.id}:small", type="resize", payload={"size": "small"})],
                group_id="resizes",
                wait_state="waiting_for_resizes",
                success="resized",
                failure="resize_failed",
            )

        result = await workflow.run_once()

        assert result.applied == 1
        assert [call[0] for call in executor.calls] == ["FLOW.CLAIM_DUE", "FLOW.SPAWN_CHILDREN"]
        assert spawn_args(executor.calls[1])["LEASE_TOKEN"] == b"lease-1"
        assert spawn_args(executor.calls[1])["WAIT_STATE"] == "waiting_for_resizes"

    asyncio.run(run())


def test_async_child_results_match_the_sync_client():
    async def run():
        executor = AsyncExecutor(
            {
                "FLOW.GET": parent_with_groups({b"resizes": RESIZES}),
                "FLOW.VALUE.MGET": mget_values,
            }
        )
        client = AsyncFlowClient(executor, codec=JsonCodec())
        assert (
            await client.child_results("image-1", partition_key="tenant-a", group_id="resizes")
            == EXPECTED_RESIZES
        )

    asyncio.run(run())
