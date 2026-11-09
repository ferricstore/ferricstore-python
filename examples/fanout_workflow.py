"""Fan out one command per device, wait for all of them, then read their results.

The campaign Flow parks in ``waiting_for_devices`` without holding a lease or a
worker. FerricStore moves it to ``devices_done`` once every child is terminal,
or to ``device_failed`` at the first failed child.
"""

from __future__ import annotations

import os

from ferricstore import ChildSpec, FlowClient, JsonCodec, WorkflowClient, complete, fail

TERMINAL = {"completed", "failed", "cancelled"}


def define_campaign(client: WorkflowClient, device_ids: list[str]):
    campaign = client.workflow(type="campaign", initial_state="dispatch")
    device = client.workflow(type="device-command", initial_state="queued")

    @campaign.state("dispatch")
    def dispatch(ctx):
        # Creating the children and parking the parent is one fenced command.
        ctx.wait_for_children(
            [
                ChildSpec(
                    id=f"device:{device_id}:cmd:{ctx.id}",
                    type="device-command",
                    payload={"device": device_id},
                )
                for device_id in device_ids
            ],
            group_id="devices",
            wait_state="waiting_for_devices",
            success="devices_done",
            failure="device_failed",
        )

    # Each child is an ordinary Flow with its own lease, retries, and history.
    @device.state("queued")
    def command(ctx):
        return complete(result={"acknowledged": ctx.payload["device"]})

    @campaign.state("devices_done")
    def devices_done(ctx):
        devices = ctx.flow.child_results("devices")
        return complete(result={"acknowledged": [child.result for child in devices.children]})

    @campaign.state("device_failed")
    def device_failed(ctx):
        devices = ctx.flow.child_results("devices")
        failed = [child.id for child in devices.children if child.status == "failed"]
        return fail(error={"failed": failed})

    return campaign, device


def run_campaign(flow: FlowClient, campaign_id: str, device_ids: list[str]) -> str:
    """Drive both workflows in one process until the campaign is terminal.

    Production workers are long-running ``worker().run()`` loops, usually in
    separate services.
    """

    campaign, device = define_campaign(WorkflowClient(flow), device_ids)
    campaign.start(campaign_id, payload={}, idempotent=True)
    campaign_worker = campaign.worker(states=["dispatch", "devices_done", "device_failed"])
    device_worker = device.worker(states=["queued"])
    for _ in range(100):
        record = flow.get(campaign_id)
        if record is not None and record.state in TERMINAL:
            return record.state
        campaign_worker.run_once()
        device_worker.run_once()
    raise RuntimeError(f"campaign {campaign_id} did not finish")


def main() -> None:
    flow = FlowClient.from_url(
        os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388"), codec=JsonCodec()
    )
    try:
        print(run_campaign(flow, "campaign-1", ["d1", "d2", "d3"]))
    finally:
        flow.close()


if __name__ == "__main__":
    main()
