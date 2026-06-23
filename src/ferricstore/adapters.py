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
