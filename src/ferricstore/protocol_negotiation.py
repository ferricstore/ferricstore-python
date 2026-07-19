from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ferricstore.errors import FerricStoreError
from ferricstore.protocol_common import _map_get

MINIMUM_SERVER_VERSION = "0.9.1"
UNAUTHENTICATED_MAX_FRAME_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class NegotiatedProtocolCapabilities:
    max_response_bytes: int
    compact_response_codecs: dict[int, str]
    auth_required: bool
    flow_policy_set_fields: frozenset[str]


def parse_hello_capabilities(value: Any) -> NegotiatedProtocolCapabilities:
    """Validate and normalize the FerricStore 0.9.1 HELLO capability contract."""
    if not isinstance(value, dict):
        raise _incompatible_server("HELLO response is not a map")
    if _text(_map_get(value, "protocol")) != "ferricstore-native":
        raise _incompatible_server("HELLO protocol is not ferricstore-native")
    if _plain_int(_map_get(value, "version")) != 1:
        raise _incompatible_server("HELLO native protocol version is not 1")

    capabilities = _required_map(value, "capabilities")
    limits = _required_map(capabilities, "limits")
    max_response_bytes = _positive_int(
        _map_get(limits, "max_response_bytes"),
        field="limits.max_response_bytes",
    )
    response_codecs = _required_map(capabilities, "response_codecs")
    compact_opcodes = _required_map(response_codecs, "compact_response_opcodes")
    schemas = _required_map(capabilities, "schemas")
    policy_set_schema = _required_map(schemas, "FLOW.POLICY.SET")
    policy_set_fields = _required_text_set(policy_set_schema, "fields")
    missing_policy_fields = {"expected_generation", "replace"} - policy_set_fields
    if missing_policy_fields:
        missing = ", ".join(sorted(missing_policy_fields))
        raise _incompatible_server(f"FLOW.POLICY.SET schema is missing required fields: {missing}")

    by_opcode: dict[int, str] = {}
    for raw_name, raw_opcodes in compact_opcodes.items():
        name = _text(raw_name)
        if not name or not isinstance(raw_opcodes, (list, tuple)):
            raise _incompatible_server("compact response codec declaration is invalid")
        for raw_opcode in raw_opcodes:
            opcode = _plain_int(raw_opcode)
            if opcode is None or not 0 <= opcode <= 0xFFFF:
                raise _incompatible_server("compact response opcode is invalid")
            previous = by_opcode.setdefault(opcode, name)
            if previous != name:
                raise _incompatible_server(
                    f"opcode {opcode:#06x} is declared by multiple compact response codecs"
                )

    auth_required = _map_get(value, "auth_required") is True
    return NegotiatedProtocolCapabilities(
        max_response_bytes=max_response_bytes,
        compact_response_codecs=by_opcode,
        auth_required=auth_required,
        flow_policy_set_fields=frozenset(policy_set_fields),
    )


def apply_hello_negotiation(adapter: Any, value: Any) -> NegotiatedProtocolCapabilities:
    negotiated = parse_hello_capabilities(value)
    configured_response = getattr(adapter, "_configured_max_response_bytes", None)
    configured_decompressed = getattr(
        adapter,
        "_configured_max_decompressed_response_bytes",
        None,
    )
    adapter.max_response_bytes = _minimum_limit(
        configured_response,
        negotiated.max_response_bytes,
    )
    adapter.max_decompressed_response_bytes = _minimum_limit(
        configured_decompressed,
        negotiated.max_response_bytes,
    )
    adapter._compact_response_codecs = dict(negotiated.compact_response_codecs)
    adapter._auth_required = negotiated.auth_required
    adapter._authenticated = not negotiated.auth_required
    adapter._negotiated_capabilities = negotiated
    _reconfigure_frame_assembler(adapter)
    return negotiated


def reset_hello_negotiation(adapter: Any) -> None:
    adapter.max_response_bytes = getattr(adapter, "_configured_max_response_bytes", None)
    adapter.max_decompressed_response_bytes = getattr(
        adapter,
        "_configured_max_decompressed_response_bytes",
        None,
    )
    adapter._compact_response_codecs = {}
    adapter._auth_required = False
    adapter._authenticated = False
    adapter._negotiated_capabilities = None
    assembler = getattr(adapter, "_response_frame_assembler", None)
    if assembler is not None:
        assembler.clear()
        _reconfigure_frame_assembler(adapter)


def mark_authenticated(adapter: Any) -> None:
    adapter._authenticated = True


def validate_unauthenticated_request_size(adapter: Any, body_bytes: int) -> None:
    if (
        getattr(adapter, "_auth_required", False)
        and not getattr(adapter, "_authenticated", False)
        and body_bytes > UNAUTHENTICATED_MAX_FRAME_BYTES
    ):
        raise FerricStoreError(
            "authenticate before submitting requests larger than the unauthenticated 64 KiB limit"
        )


def _reconfigure_frame_assembler(adapter: Any) -> None:
    assembler = getattr(adapter, "_response_frame_assembler", None)
    if assembler is not None:
        assembler.reconfigure(
            max_body_bytes=adapter.max_response_bytes,
            max_chunks=adapter.max_response_chunks,
        )


def _required_map(value: dict[Any, Any], field: str) -> dict[Any, Any]:
    nested = _map_get(value, field)
    if not isinstance(nested, dict):
        raise _incompatible_server(f"HELLO is missing {field}")
    return nested


def _required_text_set(value: dict[Any, Any], field: str) -> set[str]:
    nested = _map_get(value, field)
    if not isinstance(nested, (list, tuple)):
        raise _incompatible_server(f"HELLO is missing {field}")
    result = {_text(item) for item in nested}
    if "" in result:
        raise _incompatible_server(f"HELLO {field} contains an invalid value")
    return result


def _minimum_limit(configured: int | None, negotiated: int) -> int:
    return negotiated if configured is None else min(configured, negotiated)


def _positive_int(value: Any, *, field: str) -> int:
    parsed = _plain_int(value)
    if parsed is None or parsed <= 0:
        raise _incompatible_server(f"HELLO {field} must be a positive integer")
    return parsed


def _plain_int(value: Any) -> int | None:
    return value if type(value) is int else None


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return ""
    return value if isinstance(value, str) else ""


def _incompatible_server(detail: str) -> FerricStoreError:
    return FerricStoreError(
        f"FerricStore server does not satisfy the minimum 0.9.1 HELLO contract: {detail}"
    )


__all__ = [
    "MINIMUM_SERVER_VERSION",
    "UNAUTHENTICATED_MAX_FRAME_BYTES",
    "NegotiatedProtocolCapabilities",
    "apply_hello_negotiation",
    "mark_authenticated",
    "parse_hello_capabilities",
    "reset_hello_negotiation",
    "validate_unauthenticated_request_size",
]
