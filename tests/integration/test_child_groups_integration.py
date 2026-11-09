from __future__ import annotations

import asyncio
import importlib.util
import os
import uuid
from pathlib import Path

import pytest

from ferricstore import (
    AsyncFlowClient,
    AsyncWorkflow,
    ChildSpec,
    FlowClient,
    JsonCodec,
    WorkflowClient,
    complete,
)

pytestmark = [
    pytest.mark.skipif(
        os.environ.get("FERRICSTORE_INTEGRATION") != "1",
        reason="set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests",
    ),
    # Servers before 0.11.19 never make a parent claimable again after its
    # child group resolves, so fan-in cannot finish on them.
    pytest.mark.skipif(
        os.environ.get("FERRICSTORE_SKIP_CHILD_JOIN_RESUME") == "true",
        reason="the server predates claimable child joins (0.11.19)",
    ),
]

TERMINAL = {"completed", "failed", "cancelled"}
EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "fanout_workflow.py"


def _url() -> str:
    return os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")


def test_readme_fanout_waits_for_every_device_and_reads_their_results():
    run_id = uuid.uuid4().hex
    flow = FlowClient.from_url(_url(), codec=JsonCodec())
    client = WorkflowClient(flow)
    campaign = client.workflow(type=f"campaign-{run_id}", initial_state="dispatch")
    device = client.workflow(type=f"device-command-{run_id}", initial_state="queued")
    device_ids = ["d1", "d2", "d3"]
    seen = []

    @campaign.state("dispatch")
    def dispatch(ctx):
        ctx.wait_for_children(
            [
                ChildSpec(
                    id=f"device:{device_id}:cmd:{ctx.id}",
                    type=f"device-command-{run_id}",
                    payload={"device": device_id},
                )
                for device_id in device_ids
            ],
            group_id="devices",
            wait_state="waiting_for_devices",
            success="devices_done",
            failure="device_failed",
        )

    @device.state("queued")
    def command(ctx):
        return complete(result={"acknowledged": ctx.payload["device"]})

    @campaign.state("devices_done")
    def devices_done(ctx):
        devices = ctx.flow.child_results("devices")
        seen.append(devices)
        return complete(result={"acknowledged": len(devices.children)})

    try:
        campaign_id = f"campaign-{run_id}"
        campaign.start(campaign_id, payload={}, idempotent=True)
        assert campaign.worker(states=["dispatch"]).run_once().applied == 1
        assert flow.get(campaign_id).state == "waiting_for_devices"

        for _ in range(20):
            if device.worker(states=["queued"]).run_once().claimed == 0:
                break
        for _ in range(20):
            campaign.worker(states=["devices_done"]).run_once()
            if flow.get(campaign_id).state in TERMINAL:
                break

        assert flow.get(campaign_id).state == "completed"
        [devices] = seen
        assert devices.resolved == "success"
        assert (devices.summary.total, devices.summary.completed) == (3, 3)
        assert [child.result for child in devices.children] == [
            {"acknowledged": device_id} for device_id in device_ids
        ]
    finally:
        flow.close()


def test_async_wait_for_children_parks_the_parent_until_its_children_finish():
    async def run() -> None:
        run_id = uuid.uuid4().hex
        flow = AsyncFlowClient.from_url(_url(), codec=JsonCodec())
        parent = AsyncWorkflow(
            flow, type=f"async-parent-{run_id}", states=["start", "done"], batch_size=10
        )
        child = AsyncWorkflow(flow, type=f"async-child-{run_id}", states=["queued"], batch_size=10)
        seen = []

        @parent.state("start")
        async def start(ctx):
            await ctx.wait_for_children(
                [
                    ChildSpec(id=f"{ctx.id}:{n}", type=f"async-child-{run_id}", payload={"n": n})
                    for n in range(2)
                ],
                wait_state="waiting",
                success="done",
                failure="failed_children",
            )

        # AsyncWorkflow claims without payloads, so the child works from its id.
        @child.state("queued")
        async def queued(ctx):
            return complete(result={"double": int(ctx.id.rsplit(":", 1)[1]) * 2})

        @parent.state("done")
        async def done(ctx):
            group = await ctx.flow.child_results()
            seen.append([item.result for item in group.children])
            return complete()

        try:
            parent_id = f"async-parent-{run_id}"
            await flow.create(parent_id, type=f"async-parent-{run_id}", state="start", payload={})
            await parent.run_once()
            assert (await flow.get(parent_id)).state == "waiting"
            for _ in range(20):
                if (await child.run_once()).claimed == 0:
                    break
            for _ in range(20):
                await parent.run_once()
                if (await flow.get(parent_id)).state in TERMINAL:
                    break
            assert (await flow.get(parent_id)).state == "completed"
            assert seen == [[{"double": 0}, {"double": 2}]]
        finally:
            await flow.close()

    asyncio.run(run())


def test_fanout_example_runs_to_completion():
    spec = importlib.util.spec_from_file_location("fanout_workflow_example", EXAMPLE)
    assert spec is not None and spec.loader is not None
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    flow = FlowClient.from_url(_url(), codec=JsonCodec())
    try:
        campaign_id = f"campaign-{uuid.uuid4().hex}"
        assert example.run_campaign(flow, campaign_id, ["d1", "d2"]) == "completed"
        devices = flow.child_results(
            campaign_id, partition_key=flow.get(campaign_id).partition_key, group_id="devices"
        )
        assert [child.result for child in devices.children] == [
            {"acknowledged": "d1"},
            {"acknowledged": "d2"},
        ]
    finally:
        flow.close()
