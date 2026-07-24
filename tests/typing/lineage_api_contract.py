from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ferricstore import AsyncFlowClient, FlowClient
from ferricstore.async_workflow_context import AsyncWorkflowFlowCommands
from ferricstore.workflow_models import WorkflowFlowCommands
from ferricstore.workflow_runtime import Workflow


def _options() -> Mapping[str, Any]:
    return {"tenant": "acme"}


def check_sync(
    client: FlowClient,
    workflow: Workflow,
    bound: WorkflowFlowCommands,
) -> None:
    client.by_parent(
        "parent",
        partition_key=b"tenant",
        state="queued",
        count=10,
        from_ms=1,
        to_ms=2,
        rev=True,
        attributes=_options(),
        terminal_only=False,
        include_cold=True,
        consistent_projection=True,
    )
    workflow.by_root("root", partition_key="tenant")
    bound.by_correlation("correlation", partition_key="tenant")

    client.by_parent("parent", partition_key="tenant", typo=True)  # type: ignore[call-arg]
    workflow.by_root("root", partition_key="tenant", typo=True)  # type: ignore[call-arg]
    bound.by_correlation(  # type: ignore[call-arg]
        "correlation", partition_key="tenant", typo=True
    )
    client.by_parent("parent", partition_key=["tenant"])  # type: ignore[arg-type]


async def check_async(
    client: AsyncFlowClient,
    bound: AsyncWorkflowFlowCommands,
) -> None:
    await client.by_parent(
        "parent",
        partition_key=b"tenant",
        state="queued",
        count=10,
        from_ms=1,
        to_ms=2,
        rev=True,
        attributes=_options(),
        terminal_only=False,
        include_cold=True,
        consistent_projection=True,
    )
    await bound.by_root("root", partition_key="tenant")

    await client.by_correlation(  # type: ignore[call-arg]
        "correlation", partition_key="tenant", typo=True
    )
    await bound.by_root("root", partition_key="tenant", typo=True)  # type: ignore[call-arg]
    await client.by_root("root", partition_key="tenant", count="10")  # type: ignore[arg-type]
