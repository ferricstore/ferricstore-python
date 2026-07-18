from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncFlowClient": ("ferricstore.async_client_core", "AsyncFlowClient"),
    "AsyncCommandPipeline": (
        "ferricstore.async_client_sessions",
        "AsyncCommandPipeline",
    ),
    "AsyncPubSubSession": ("ferricstore.async_client_sessions", "AsyncPubSubSession"),
    "AsyncTransactionSession": (
        "ferricstore.async_client_sessions",
        "AsyncTransactionSession",
    ),
    "_AsyncErrorMappingExecutor": (
        "ferricstore.async_client_sessions",
        "_AsyncErrorMappingExecutor",
    ),
    "FlowClient": ("ferricstore.client_core", "FlowClient"),
    "_now_ms": ("ferricstore.client_helpers", "_now_ms"),
}

__all__ = [name for name in _EXPORTS if not name.startswith("_")]


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))


if TYPE_CHECKING:
    from ferricstore.async_client_core import AsyncFlowClient as AsyncFlowClient
    from ferricstore.async_client_sessions import AsyncCommandPipeline as AsyncCommandPipeline
    from ferricstore.async_client_sessions import AsyncPubSubSession as AsyncPubSubSession
    from ferricstore.async_client_sessions import (
        AsyncTransactionSession as AsyncTransactionSession,
    )
    from ferricstore.client_core import FlowClient as FlowClient
