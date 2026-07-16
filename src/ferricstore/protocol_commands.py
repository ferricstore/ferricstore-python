from __future__ import annotations

from typing import Any

import ferricstore.protocol_basic_commands as _basic_commands
import ferricstore.protocol_command_options as _command_options
import ferricstore.protocol_compact_commands as _compact_commands
import ferricstore.protocol_flow_commands as _flow_commands
from ferricstore.errors import InvalidCommandError
from ferricstore.protocol_basic_commands import *  # noqa: F403
from ferricstore.protocol_basic_commands import (
    _BASIC_COMMAND_BUILDERS,
    _build_basic_protocol_command,
)
from ferricstore.protocol_codec import encode_value
from ferricstore.protocol_command_options import *  # noqa: F403
from ferricstore.protocol_command_options import (
    _command_exec_protocol_command,
    _option_map,
)
from ferricstore.protocol_command_schema import (
    _SPECIALIZED_COMMANDS_WITH_LOCAL_ARITY_VALIDATION,
    validate_specialized_command_arity,
)
from ferricstore.protocol_common import (
    _command_name,
    _lane_for_opcode,
    _require_arg,
)
from ferricstore.protocol_compact_budget import transport_compact_encoding_policy
from ferricstore.protocol_compact_commands import *  # noqa: F403
from ferricstore.protocol_constants import (
    _HEADER,
    _MAGIC,
    _OPCODES,
    _REQUEST_VERSION,
    ProtocolCommand,
)
from ferricstore.protocol_flow_commands import *  # noqa: F403
from ferricstore.protocol_flow_commands import _build_flow_protocol_command


def build_protocol_command(*args: Any) -> ProtocolCommand:
    try:
        if not args:
            raise InvalidCommandError("protocol command requires command name")

        name = _command_name(args[0])
        if name not in _OPCODES:
            return _command_exec_protocol_command(name, args[1:])

        if name == "COMMAND_EXEC":
            raw_name = _command_name(_require_arg(args, 1, name))
            return _command_exec_protocol_command(raw_name, args[2:])

        if name in _BASIC_COMMAND_BUILDERS:
            if name not in _SPECIALIZED_COMMANDS_WITH_LOCAL_ARITY_VALIDATION:
                validate_specialized_command_arity(name, len(args) - 1)
            return _build_basic_protocol_command(name, args[1:])

        if name.startswith("FLOW."):
            return _build_flow_protocol_command(name, args[1:])

        return ProtocolCommand(
            _OPCODES[name],
            _option_map(args[1:]),
            _lane_for_opcode(_OPCODES[name]),
        )
    except UnicodeEncodeError as exc:
        raise InvalidCommandError("protocol strings must contain valid UTF-8") from exc


def _build_transport_protocol_command(
    *args: Any,
    max_pending_request_bytes: int | None,
    compression: str,
) -> ProtocolCommand:
    """Build with the transport's eager-encoding admission constraints."""

    with transport_compact_encoding_policy(max_pending_request_bytes, compression):
        return build_protocol_command(*args)


def encode_frame(opcode: int, lane_id: int, request_id: int, value: Any, flags: int = 0) -> bytes:
    body = encode_value(value)
    return (
        _HEADER.pack(_MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)) + body
    )


__all__ = [
    *_basic_commands.__all__,
    *_command_options.__all__,
    *_compact_commands.__all__,
    *_flow_commands.__all__,
    "build_protocol_command",
    "encode_frame",
]
