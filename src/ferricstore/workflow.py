from __future__ import annotations

# ruff: noqa: SIM905
from importlib import import_module
from typing import TYPE_CHECKING, Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "FlowClient": ("ferricstore.client_core", "FlowClient"),
    "FLOW_MANY_BATCH_LIMIT": ("ferricstore.workflow_models", "FLOW_MANY_BATCH_LIMIT"),
    "WORKFLOW_WORKER_CONFIG_KEYS": (
        "ferricstore.workflow_models",
        "WORKFLOW_WORKER_CONFIG_KEYS",
    ),
    "WorkflowBudget": ("ferricstore.workflow_budget", "WorkflowBudget"),
    "WorkflowContext": ("ferricstore.workflow_models", "WorkflowContext"),
    "WorkflowEffect": ("ferricstore.workflow_models", "WorkflowEffect"),
    "WorkflowFlowCommands": ("ferricstore.workflow_models", "WorkflowFlowCommands"),
    "FlowWorkflow": ("ferricstore.workflow_runtime", "FlowWorkflow"),
    "Workflow": ("ferricstore.workflow_runtime", "Workflow"),
    "WorkflowClient": ("ferricstore.workflow_client", "WorkflowClient"),
    "WorkflowWorker": ("ferricstore.workflow_worker", "WorkflowWorker"),
}
for _name in (
    "Complete Fail Handler Outcome Retry StateConfig Transition WorkflowWorkerResult "
    "complete fail retry state transition"
).split():
    _EXPORTS[_name] = ("ferricstore.workflow_types", _name)
del _name

__all__ = [name for name in _EXPORTS if not name.startswith("_")]


def __getattr__(name: str) -> Any:
    if name == "time":
        value = import_module("time")
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
    return sorted(set(globals()) | set(_EXPORTS) | {"time"})


if TYPE_CHECKING:
    import time as time

    from ferricstore.client_core import FlowClient as FlowClient
    from ferricstore.workflow_budget import WorkflowBudget as WorkflowBudget
    from ferricstore.workflow_client import WorkflowClient as WorkflowClient
    from ferricstore.workflow_models import WorkflowContext as WorkflowContext
    from ferricstore.workflow_models import WorkflowEffect as WorkflowEffect
    from ferricstore.workflow_runtime import FlowWorkflow as FlowWorkflow
    from ferricstore.workflow_runtime import Workflow as Workflow
    from ferricstore.workflow_types import Complete as Complete
    from ferricstore.workflow_types import Fail as Fail
    from ferricstore.workflow_types import Retry as Retry
    from ferricstore.workflow_types import Transition as Transition
    from ferricstore.workflow_types import WorkflowWorkerResult as WorkflowWorkerResult
    from ferricstore.workflow_types import complete as complete
    from ferricstore.workflow_types import fail as fail
    from ferricstore.workflow_types import retry as retry
    from ferricstore.workflow_types import state as state
    from ferricstore.workflow_types import transition as transition
    from ferricstore.workflow_worker import WorkflowWorker as WorkflowWorker
