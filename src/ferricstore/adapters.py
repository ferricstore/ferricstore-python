from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CommandExecutor(Protocol):
    """Command executor surface used by the sync SDK.

    The public SDK is FerricStore-native. This protocol remains intentionally
    small so tests and advanced users can inject custom FerricStore-compatible
    executors without depending on a concrete transport implementation.
    """

    def execute_command(self, *args: Any) -> Any:
        """Execute one FerricStore command."""


@runtime_checkable
class AsyncCommandExecutor(Protocol):
    """Async command executor surface used by the async SDK."""

    async def execute_command(self, *args: Any) -> Any:
        """Execute one FerricStore command asynchronously."""


@runtime_checkable
class FlowQueryCommandExecutor(CommandExecutor, Protocol):
    """Optional sync capability for out-of-band FLOW.QUERY transport options."""

    def execute_flow_query_command(
        self,
        *args: Any,
        deadline_ms: int | None = None,
        routing_key: str | bytes | None = None,
    ) -> Any:
        """Execute FLOW.QUERY without adding SDK-only values to command arguments."""


@runtime_checkable
class AsyncFlowQueryCommandExecutor(AsyncCommandExecutor, Protocol):
    """Async counterpart to :class:`FlowQueryCommandExecutor`."""

    async def execute_flow_query_command(
        self,
        *args: Any,
        deadline_ms: int | None = None,
        routing_key: str | bytes | None = None,
    ) -> Any:
        """Execute FLOW.QUERY asynchronously with out-of-band options."""
