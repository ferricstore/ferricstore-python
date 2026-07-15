from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from ferricstore.protocol_compat import COMPAT_EXPORTS

_EXPORTS: dict[str, tuple[str, str]] = {
    "AsyncProtocolAdapter": ("ferricstore.protocol_async", "AsyncProtocolAdapter"),
    "AsyncProtocolAdapterPool": ("ferricstore.protocol_async_pool", "AsyncProtocolAdapterPool"),
    "AsyncProtocolPipeline": ("ferricstore.protocol_pipelines", "AsyncProtocolPipeline"),
    "AsyncTopologyProtocolAdapterPool": (
        "ferricstore.protocol_async_topology",
        "AsyncTopologyProtocolAdapterPool",
    ),
    "FlowWakeSubscriptionRegistry": ("ferricstore.topology_core", "FlowWakeSubscriptionRegistry"),
    "ProtocolAdapter": ("ferricstore.protocol_sync", "ProtocolAdapter"),
    "ProtocolAdapterPool": ("ferricstore.protocol_sync_pool", "ProtocolAdapterPool"),
    "ProtocolCommand": ("ferricstore.protocol_constants", "ProtocolCommand"),
    "ProtocolPipeline": ("ferricstore.protocol_pipelines", "ProtocolPipeline"),
    "ProtocolResponse": ("ferricstore.protocol_constants", "ProtocolResponse"),
    "RoutingTopology": ("ferricstore.protocol_common", "RoutingTopology"),
    "TopologyProtocolAdapterPool": (
        "ferricstore.protocol_sync_topology",
        "TopologyProtocolAdapterPool",
    ),
    "build_protocol_command": ("ferricstore.protocol_commands", "build_protocol_command"),
    "decode_value": ("ferricstore.protocol_codec", "decode_value"),
    "encode_frame": ("ferricstore.protocol_commands", "encode_frame"),
    "encode_value": ("ferricstore.protocol_codec", "encode_value"),
    "_decode_value_at": ("ferricstore.protocol_codec", "decode_value_at"),
    "_decompress_response": ("ferricstore.protocol_framing", "decompress_response"),
    "_send_frames": ("ferricstore.protocol_framing", "send_frames"),
}
for _name, _route in COMPAT_EXPORTS.items():
    _EXPORTS.setdefault(_name, _route)
del _name, _route

__all__ = [name for name in _EXPORTS if not name.startswith("_")]


def __getattr__(name: str) -> Any:
    if name.startswith("__"):
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    if name in {"asyncio", "socket", "time", "zlib"}:
        value = import_module(name)
        globals()[name] = value
        return value
    route = _EXPORTS.get(name)
    if route is not None:
        module_name, attribute_name = route
        value = getattr(import_module(module_name), attribute_name)
        globals()[name] = value
        return value
    if name.startswith("_") and name.upper() == name:
        constants = import_module("ferricstore.protocol_constants")
        if hasattr(constants, name):
            value = getattr(constants, name)
            globals()[name] = value
            return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))


if TYPE_CHECKING:
    from ferricstore.protocol_async import AsyncProtocolAdapter as AsyncProtocolAdapter
    from ferricstore.protocol_async_pool import AsyncProtocolAdapterPool as AsyncProtocolAdapterPool
    from ferricstore.protocol_async_topology import (
        AsyncTopologyProtocolAdapterPool as AsyncTopologyProtocolAdapterPool,
    )
    from ferricstore.protocol_common import RoutingTopology as RoutingTopology
    from ferricstore.protocol_constants import ProtocolCommand as ProtocolCommand
    from ferricstore.protocol_constants import ProtocolResponse as ProtocolResponse
    from ferricstore.protocol_pipelines import AsyncProtocolPipeline as AsyncProtocolPipeline
    from ferricstore.protocol_pipelines import ProtocolPipeline as ProtocolPipeline
    from ferricstore.protocol_sync import ProtocolAdapter as ProtocolAdapter
    from ferricstore.protocol_sync_pool import ProtocolAdapterPool as ProtocolAdapterPool
    from ferricstore.protocol_sync_topology import (
        TopologyProtocolAdapterPool as TopologyProtocolAdapterPool,
    )
