from __future__ import annotations

# ruff: noqa: SIM905
from importlib import import_module
from typing import TYPE_CHECKING, Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncFlowClient": ("ferricstore.async_client_core", "AsyncFlowClient"),
    "AsyncQueue": ("ferricstore.async_queue_api", "AsyncQueue"),
    "AsyncQueueClient": ("ferricstore.async_queue_api", "AsyncQueueClient"),
    "AsyncQueueFlow": ("ferricstore.async_queue_api", "AsyncQueueFlow"),
    "AsyncWorkflowBudget": (
        "ferricstore.async_workflow_budget",
        "AsyncWorkflowBudget",
    ),
    "AsyncWorkflowContext": (
        "ferricstore.async_workflow_context",
        "AsyncWorkflowContext",
    ),
    "AsyncWorkflowEffect": (
        "ferricstore.async_workflow_context",
        "AsyncWorkflowEffect",
    ),
    "AsyncWorkflowFlowCommands": (
        "ferricstore.async_workflow_context",
        "AsyncWorkflowFlowCommands",
    ),
    "AsyncWorkflow": ("ferricstore.async_workflow_runtime", "AsyncWorkflow"),
    "AsyncWorkflowClient": (
        "ferricstore.async_workflow_client",
        "AsyncWorkflowClient",
    ),
    "AsyncWorkflowWorkerResult": (
        "ferricstore.async_workflow_types",
        "AsyncWorkflowWorkerResult",
    ),
}
for _name in (
    "ASYNC_QUEUE_WORKER_CONFIG_KEYS ASYNC_WORKFLOW_CONFIG_KEYS FLOW_MANY_BATCH_LIMIT "
    "AsyncErrorMode AsyncFlowBatchHandler AsyncFlowHandler AsyncFlowJob AsyncQueueFlowWorker "
    "AsyncWorkflowHandler"
).split():
    _EXPORTS[_name] = ("ferricstore.async_queue_runtime", _name)
for _name in (
    "AUTO_PARTITION_BUCKETS AUTO_PARTITION_PREFIX SERVER_SLOT_COUNT "
    "_auto_partition_assignments _auto_partition_index_for_id _auto_partition_key "
    "_auto_partition_owner _auto_partition_server_shard _owned_auto_partition_keys"
).split():
    _EXPORTS[_name] = ("ferricstore.async_partitioning", _name)
del _name

__all__ = [name for name in _EXPORTS if not name.startswith("_")]


def __getattr__(name: str) -> Any:
    if name == "asyncio":
        value = import_module("asyncio")
        globals()[name] = value
        return value
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS) | {"asyncio"})


if TYPE_CHECKING:
    import asyncio as asyncio

    from ferricstore.async_client_core import AsyncFlowClient as AsyncFlowClient
    from ferricstore.async_queue_api import AsyncQueue as AsyncQueue
    from ferricstore.async_queue_api import AsyncQueueClient as AsyncQueueClient
    from ferricstore.async_queue_api import AsyncQueueFlow as AsyncQueueFlow
    from ferricstore.async_queue_runtime import AsyncQueueFlowWorker as AsyncQueueFlowWorker
    from ferricstore.async_workflow_budget import (
        AsyncWorkflowBudget as AsyncWorkflowBudget,
    )
    from ferricstore.async_workflow_client import (
        AsyncWorkflowClient as AsyncWorkflowClient,
    )
    from ferricstore.async_workflow_context import (
        AsyncWorkflowContext as AsyncWorkflowContext,
    )
    from ferricstore.async_workflow_context import (
        AsyncWorkflowEffect as AsyncWorkflowEffect,
    )
    from ferricstore.async_workflow_runtime import AsyncWorkflow as AsyncWorkflow
    from ferricstore.async_workflow_types import (
        AsyncWorkflowWorkerResult as AsyncWorkflowWorkerResult,
    )
