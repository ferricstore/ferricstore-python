from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ferricstore.errors import FerricStoreError
from ferricstore.protocol_common import _map_get

MINIMUM_SERVER_VERSION = "0.10.0"
UNAUTHENTICATED_MAX_FRAME_BYTES = 64 * 1024

_FLOW_QUERY_CAPABILITIES = frozenset(
    {
        "flow_query_v1",
        "flow_explain_v1",
        "flow_explain_analyze_v1",
        "flow_composite_index_v1",
        "flow_query_index_status_v1",
    }
)
_FLOW_QUERY_SHAPES = frozenset(
    {
        "runs_by_run_id_record",
        "runs_by_partition_and_run_id_record",
        "runs_by_partition_predicates_ordered_records",
        "runs_by_partition_type_state_ordered_records",
        "runs_by_partition_type_terminals_ordered_records",
        "runs_by_partition_metadata_ordered_records",
        "runs_by_partition_type_running_lease_deadline_ordered_records",
        "runs_by_partition_parent_ordered_records",
        "runs_by_partition_root_ordered_records",
        "runs_by_partition_correlation_ordered_records",
        "runs_by_partition_predicates_count",
        "events_by_run_id_ordered_records",
    }
)


@dataclass(frozen=True, slots=True)
class FlowQueryCapabilities:
    request_contract: str
    result_contract: str
    explain_contract: str
    index_status_contract: str
    capabilities: frozenset[str]
    language_versions: frozenset[str]
    shapes: frozenset[str]


@dataclass(frozen=True, slots=True)
class NegotiatedProtocolCapabilities:
    max_response_bytes: int
    compact_response_codecs: dict[int, str]
    auth_required: bool
    flow_policy_set_fields: frozenset[str]
    flow_query: FlowQueryCapabilities


def parse_hello_capabilities(value: Any) -> NegotiatedProtocolCapabilities:
    """Validate and normalize the FerricStore 0.10.0 HELLO capability contract."""
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
    flow_query = _parse_flow_query_capabilities(capabilities, schemas)

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
        flow_query=flow_query,
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


def _parse_flow_query_capabilities(
    capabilities: dict[Any, Any], schemas: dict[Any, Any]
) -> FlowQueryCapabilities:
    manifest = _required_map(capabilities, "flow_query")
    request_contract = _required_text(manifest, "request_contract")
    result_contract = _required_text(manifest, "result_contract")
    explain_contract = _required_text(manifest, "explain_contract")
    index_status_contract = _required_text(manifest, "index_status_contract")
    expected_contracts = {
        "request_contract": (request_contract, "ferric.flow.query.request/v1"),
        "result_contract": (result_contract, "ferric.flow.query.result/v1"),
        "explain_contract": (explain_contract, "ferric.flow.explain/v1"),
        "index_status_contract": (
            index_status_contract,
            "ferric.flow.query.indexes/v1",
        ),
    }
    for field, (actual, expected) in expected_contracts.items():
        if actual != expected:
            raise _incompatible_server(f"flow_query {field} must be {expected!r}")

    query_capabilities = _bounded_text_set(manifest, "capabilities", maximum=64)
    language_versions = _bounded_text_set(manifest, "language_versions", maximum=16)
    shapes = _bounded_text_set(manifest, "shapes", maximum=128)
    _require_members(query_capabilities, _FLOW_QUERY_CAPABILITIES, "flow_query capability")
    _require_members(language_versions, frozenset({"FQL1"}), "flow_query language")
    _require_members(shapes, _FLOW_QUERY_SHAPES, "flow_query shape")

    query_schema = _required_map(schemas, "FLOW.QUERY")
    fields = _bounded_text_set(query_schema, "fields", maximum=64)
    _require_members(
        fields,
        frozenset({"version", "query", "params", "deadline_ms"}),
        "FLOW.QUERY schema field",
    )
    required = _bounded_text_set(query_schema, "required", maximum=16)
    if required != {"version", "query"}:
        raise _incompatible_server("FLOW.QUERY schema must require exactly version and query")
    return FlowQueryCapabilities(
        request_contract=request_contract,
        result_contract=result_contract,
        explain_contract=explain_contract,
        index_status_contract=index_status_contract,
        capabilities=frozenset(query_capabilities),
        language_versions=frozenset(language_versions),
        shapes=frozenset(shapes),
    )


def _required_text(value: dict[Any, Any], field: str) -> str:
    text = _text(_map_get(value, field))
    if not text or len(text.encode("utf-8")) > 256:
        raise _incompatible_server(f"HELLO flow_query {field} must be bounded non-empty text")
    return text


def _bounded_text_set(value: dict[Any, Any], field: str, *, maximum: int) -> set[str]:
    raw_items = _map_get(value, field)
    if not isinstance(raw_items, (list, tuple)) or not 1 <= len(raw_items) <= maximum:
        raise _incompatible_server(f"HELLO {field} must contain 1..{maximum} entries")
    result: set[str] = set()
    for raw_item in raw_items:
        item = _text(raw_item)
        if not item or len(item.encode("utf-8")) > 256:
            raise _incompatible_server(f"HELLO {field} contains invalid text")
        if item in result:
            raise _incompatible_server(f"HELLO {field} contains duplicate {item!r}")
        result.add(item)
    return result


def _require_members(actual: set[str], required: frozenset[str], context: str) -> None:
    missing = required - actual
    if missing:
        raise _incompatible_server(f"missing {context} {sorted(missing)[0]!r}")


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
        f"FerricStore server does not satisfy the minimum {MINIMUM_SERVER_VERSION} "
        f"HELLO contract: {detail}"
    )


__all__ = [
    "MINIMUM_SERVER_VERSION",
    "UNAUTHENTICATED_MAX_FRAME_BYTES",
    "FlowQueryCapabilities",
    "NegotiatedProtocolCapabilities",
    "apply_hello_negotiation",
    "mark_authenticated",
    "parse_hello_capabilities",
    "reset_hello_negotiation",
    "validate_unauthenticated_request_size",
]
