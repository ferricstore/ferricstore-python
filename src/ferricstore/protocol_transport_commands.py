from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, TypeVar
from urllib.parse import unquote, urlparse

from ferricstore.protocol_commands import _build_transport_protocol_command
from ferricstore.protocol_common import _normalize_protocol_url_kwargs, _protocol_url_port
from ferricstore.protocol_compact_budget import transport_compact_encoding_policy
from ferricstore.protocol_constants import _SUPPORTED_SCHEMES, _TLS_SCHEMES, ProtocolCommand
from ferricstore.protocol_lifecycle import DEFAULT_MAX_PENDING_REQUEST_BYTES
from ferricstore.protocol_planning import PreparedCommand, prepare_protocol_command

_FlowManyBuilder = Callable[..., list[tuple[int, bytes, int]] | None]
_Adapter = TypeVar("_Adapter")


class _AdapterCommandConfig(Protocol):
    compression: str
    max_pending_request_bytes: int | None


class _TopologyCommandConfig(Protocol):
    _adapter_kwargs: dict[str, Any]


def _transport_config(adapter: Any) -> tuple[int | None, str]:
    return (
        getattr(adapter, "max_pending_request_bytes", DEFAULT_MAX_PENDING_REQUEST_BYTES),
        getattr(adapter, "compression", "none") or "none",
    )


def adapter_from_url(
    adapter_type: Callable[..., _Adapter],
    url: str,
    kwargs: dict[str, Any],
) -> _Adapter:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    tls = scheme in _TLS_SCHEMES
    if scheme not in _SUPPORTED_SCHEMES:
        raise ValueError(f"unsupported FerricStore URL scheme: {parsed.scheme}")
    host = parsed.hostname or "127.0.0.1"
    port = _protocol_url_port(parsed, tls=tls)
    username = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    _normalize_protocol_url_kwargs(kwargs, tls_from_scheme=tls)
    kwargs.setdefault("username", username)
    kwargs.setdefault("password", password)
    return adapter_type(host, port, **kwargs)


def build_adapter_protocol_command(
    adapter: _AdapterCommandConfig,
    args: tuple[Any, ...],
) -> ProtocolCommand:
    pending_limit, compression = _transport_config(adapter)
    return _build_transport_protocol_command(
        *args,
        max_pending_request_bytes=pending_limit,
        compression=compression,
    )


def prepare_adapter_protocol_command(
    adapter: _AdapterCommandConfig,
    args: tuple[Any, ...],
) -> PreparedCommand:
    return prepare_protocol_command(
        args,
        builder=lambda *values: build_adapter_protocol_command(adapter, values),
    )


def compact_flow_many_for_adapter(
    adapter: _AdapterCommandConfig,
    builder: _FlowManyBuilder,
    commands: list[tuple[Any, ...]],
    protocol_commands: list[ProtocolCommand] | None,
) -> list[tuple[int, bytes, int]] | None:
    pending_limit, compression = _transport_config(adapter)
    with transport_compact_encoding_policy(pending_limit, compression):
        if protocol_commands is None:
            return builder(commands)
        return builder(commands, protocol_commands=protocol_commands)


def build_topology_protocol_command(
    host: _TopologyCommandConfig,
    args: tuple[Any, ...],
) -> ProtocolCommand:
    return _build_transport_protocol_command(
        *args,
        max_pending_request_bytes=host._adapter_kwargs.get(
            "max_pending_request_bytes",
            DEFAULT_MAX_PENDING_REQUEST_BYTES,
        ),
        compression=host._adapter_kwargs.get("compression", "none") or "none",
    )


class TopologyCommandPlanningMixin:
    _adapter_kwargs: dict[str, Any]

    def _build_protocol_command(self, *args: Any) -> ProtocolCommand:
        return build_topology_protocol_command(self, args)

    def _prepare_routed_command(self, args: tuple[Any, ...]) -> PreparedCommand:
        return prepare_protocol_command(args, builder=self._build_protocol_command)


__all__ = [
    "TopologyCommandPlanningMixin",
    "adapter_from_url",
    "build_adapter_protocol_command",
    "build_topology_protocol_command",
    "compact_flow_many_for_adapter",
    "prepare_adapter_protocol_command",
]
