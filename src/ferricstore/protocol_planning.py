from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_constants import ProtocolCommand
from ferricstore.protocol_pipeline_codec import (
    _blocks_forever,
    _expected_command_collection_items,
)


@dataclass(frozen=True, slots=True)
class PreparedCommand:
    """One parsed command plus execution metadata shared across transport layers."""

    args: tuple[Any, ...]
    command: ProtocolCommand
    expected_collection_items: int | None
    blocks_forever: bool


def prepare_protocol_command(
    args: tuple[Any, ...],
    *,
    builder: Callable[..., ProtocolCommand] = build_protocol_command,
) -> PreparedCommand:
    return PreparedCommand(
        args=args,
        command=builder(*args),
        expected_collection_items=_expected_command_collection_items(args),
        blocks_forever=_blocks_forever(args),
    )


__all__ = ["PreparedCommand", "prepare_protocol_command"]
