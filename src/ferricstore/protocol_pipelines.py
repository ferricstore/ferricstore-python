from __future__ import annotations

from typing import Any, cast

from ferricstore.batch_core import ordered_batch_executor
from ferricstore.errors import InvalidCommandError


class ProtocolPipeline:
    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        self.commands: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> ProtocolPipeline:
        self.commands.append(args)
        return self

    def execute(self) -> list[Any]:
        return cast(list[Any], self.adapter.execute_batch(self.commands))


class AsyncProtocolPipeline:
    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        self.commands: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> AsyncProtocolPipeline:
        self.commands.append(args)
        return self

    async def execute(self) -> list[Any]:
        execute_batch = ordered_batch_executor(self.adapter)
        if execute_batch is None:
            raise InvalidCommandError("protocol adapter does not support batch execution")
        return cast(list[Any], await execute_batch(self.commands))


__all__ = ["AsyncProtocolPipeline", "ProtocolPipeline"]
