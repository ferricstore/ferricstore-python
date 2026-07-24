from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ferricstore.flow_query_dsl_types import (
    FlowEventField,
    FlowField,
    FlowOrder,
    FlowPredicate,
    FlowQueryParameter,
    FlowRunMapField,
    _FlowOperand,
)
from ferricstore.flow_query_limits import (
    FLOW_QUERY_MAX_CURSOR_BYTES,
    FLOW_QUERY_MAX_FIELD_VALUE_BYTES,
    FLOW_QUERY_MAX_PARAMETERS,
    FLOW_QUERY_MAX_PARTITION_BYTES,
    FLOW_QUERY_MAX_RUN_ID_BYTES,
    FLOW_QUERY_MAX_TOKENS,
    FLOW_QUERY_MIN_CURSOR_BYTES,
)
from ferricstore.flow_query_request import validate_flow_query_text


def compile_flow_query(
    *,
    source: str,
    predicates: tuple[FlowPredicate, ...],
    orders: tuple[FlowOrder, ...],
    result_limit: int | None,
    page_cursor: _FlowOperand | None,
    return_shape: str,
    projection: tuple[FlowField | FlowEventField | FlowRunMapField, ...],
    bindings: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Compile a validated query shape without mutating its normalized bindings."""

    _validate_generated_parameter_count(predicates, page_cursor, bindings)
    compiler = _FlowQueryCompiler(bindings)
    rendered_predicates = [compiler.predicate(predicate) for predicate in predicates]
    query = f"FROM {source} WHERE {' AND '.join(rendered_predicates)}"
    if orders:
        rendered_orders = ", ".join(f"{order.field.selector} {order.direction}" for order in orders)
        query += f" ORDER BY {rendered_orders}"
    if result_limit is not None:
        query += f" LIMIT {result_limit}"
    if page_cursor is not None:
        query += f" CURSOR {compiler.cursor(page_cursor)}"
    query += f" RETURN {return_shape.upper()}"
    if projection:
        query += " (" + ", ".join(field.selector for field in projection) + ")"

    _validate_generated_token_count(predicates, orders, result_limit, page_cursor, projection)
    validate_flow_query_text(query)
    # Bindings and literal operands are normalized at their public construction
    # boundaries.  The transport request builder validates them once more at the
    # native-wire boundary, so another full normalization pass here is redundant.
    return query, dict(compiler.params)


class _FlowQueryCompiler:
    __slots__ = ("_generated", "params")

    def __init__(self, bindings: Mapping[str, Any]) -> None:
        self.params = dict(bindings)
        self._generated = 0

    def predicate(self, predicate: FlowPredicate) -> str:
        field = predicate.field
        selector = field.selector
        if predicate.operator == "null":
            return f"{selector} IS NULL"
        if predicate.operator == "missing":
            return f"{selector} IS MISSING"

        values = [self._value(operand, field) for operand in predicate.operands]
        if predicate.operator == "eq":
            return f"{selector} = {values[0][0]}"
        if predicate.operator == "in":
            _validate_distinct_in(values)
            return f"{selector} IN ({', '.join(value[0] for value in values)})"

        _validate_range(values[0][1], values[1][1])
        if predicate.operator == "between":
            return f"{selector} BETWEEN {values[0][0]} AND {values[1][0]}"
        return f"{selector} FROM {values[0][0]} TO {values[1][0]}"

    def cursor(self, operand: _FlowOperand) -> str:
        if isinstance(operand, FlowQueryParameter):
            value = self.params[operand.name]
            name = operand.name
        else:
            value = operand.value
            name = "_fql_cursor"
            self.params[name] = value
        _validate_cursor(value)
        return f"@{name}"

    def _value(self, operand: _FlowOperand, query_field: FlowField) -> tuple[str, Any]:
        if isinstance(operand, FlowQueryParameter):
            name = operand.name
            value = self.params[name]
        else:
            name = f"_fql_{self._generated}"
            self._generated += 1
            value = operand.value
            self.params[name] = value
        _validate_field_value(query_field, value, name)
        return f"@{name}", value


def _validate_field_value(query_field: FlowField, value: Any, parameter: str) -> None:
    if query_field.value_kind == "integer":
        if type(value) is not int:
            raise TypeError(
                f"Flow query parameter {parameter!r} is incompatible with integer field "
                f"{query_field.selector!r}"
            )
        return
    if query_field.value_kind == "keyword":
        if not isinstance(value, (str, bytes)):
            raise TypeError(
                f"Flow query parameter {parameter!r} is incompatible with keyword field "
                f"{query_field.selector!r}"
            )
        max_bytes = (
            FLOW_QUERY_MAX_PARTITION_BYTES
            if query_field.name == "partition_key"
            else FLOW_QUERY_MAX_RUN_ID_BYTES
            if query_field.name == "run_id"
            else FLOW_QUERY_MAX_FIELD_VALUE_BYTES
        )
        if len(value) > max_bytes:
            raise ValueError(
                f"Flow query value for {query_field.selector!r} must be 1..{max_bytes} bytes"
            )
        encoded = value if isinstance(value, bytes) else value.encode("utf-8")
        if not encoded or len(encoded) > max_bytes:
            raise ValueError(
                f"Flow query value for {query_field.selector!r} must be 1..{max_bytes} bytes"
            )
        return
    if isinstance(value, (str, bytes)):
        if len(value) > FLOW_QUERY_MAX_FIELD_VALUE_BYTES:
            raise ValueError(
                f"Flow query value for {query_field.selector!r} must not exceed "
                f"{FLOW_QUERY_MAX_FIELD_VALUE_BYTES} bytes"
            )
        encoded = value if isinstance(value, bytes) else value.encode("utf-8")
        if len(encoded) > FLOW_QUERY_MAX_FIELD_VALUE_BYTES:
            raise ValueError(
                f"Flow query value for {query_field.selector!r} must not exceed "
                f"{FLOW_QUERY_MAX_FIELD_VALUE_BYTES} bytes"
            )


def _validate_distinct_in(values: list[tuple[str, Any]]) -> None:
    normalized = [_comparable(value) for _placeholder, value in values]
    if len(normalized) != len(set(normalized)):
        raise ValueError("Flow query IN does not allow a duplicate IN value")


def _validate_range(lower: Any, upper: Any) -> None:
    lower_value = _comparable(lower)
    upper_value = _comparable(upper)
    if lower_value[0] != upper_value[0]:
        raise TypeError("Flow query range bounds have incompatible value types")
    if lower_value[1] > upper_value[1]:
        raise ValueError("Flow query lower bound must not exceed its upper bound")


def _comparable(value: Any) -> tuple[str, Any]:
    if isinstance(value, str):
        return "binary", value.encode("utf-8")
    if isinstance(value, bytes):
        return "binary", value
    if type(value) is bool:
        return "boolean", value
    if type(value) is int:
        return "integer", value
    if type(value) is float:
        return "float", value
    raise TypeError("Flow query value is not comparable")


def _validate_cursor(value: Any) -> None:
    if not isinstance(value, (str, bytes)):
        raise TypeError("Flow query cursor must be text or bytes")
    if len(value) > FLOW_QUERY_MAX_CURSOR_BYTES:
        raise ValueError(
            "Flow query cursor must be "
            f"{FLOW_QUERY_MIN_CURSOR_BYTES}..{FLOW_QUERY_MAX_CURSOR_BYTES} bytes"
        )
    encoded = value if isinstance(value, bytes) else value.encode("utf-8")
    if not FLOW_QUERY_MIN_CURSOR_BYTES <= len(encoded) <= FLOW_QUERY_MAX_CURSOR_BYTES:
        raise ValueError(
            "Flow query cursor must be "
            f"{FLOW_QUERY_MIN_CURSOR_BYTES}..{FLOW_QUERY_MAX_CURSOR_BYTES} bytes"
        )
    if not encoded.startswith(b"fqc1_"):
        raise ValueError("Flow query cursor must begin with 'fqc1_'")


def _validate_generated_token_count(
    predicates: tuple[FlowPredicate, ...],
    orders: tuple[FlowOrder, ...],
    result_limit: int | None,
    cursor: _FlowOperand | None,
    projection: tuple[FlowField | FlowEventField | FlowRunMapField, ...],
) -> None:
    token_count = 3  # FROM <source> WHERE
    token_count += max(0, len(predicates) - 1)
    token_count += sum(_predicate_token_count(predicate) for predicate in predicates)
    if orders:
        token_count += 2 + max(0, len(orders) - 1)
        token_count += sum(_field_token_count(order.field) + 1 for order in orders)
    if result_limit is not None:
        token_count += 2
    if cursor is not None:
        token_count += 2
    token_count += 2
    if projection:
        token_count += 2 + max(0, len(projection) - 1)
        token_count += sum(_projection_field_token_count(field) for field in projection)
    if token_count > FLOW_QUERY_MAX_TOKENS:
        raise ValueError(f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_TOKENS} lexical tokens")


def _validate_generated_parameter_count(
    predicates: tuple[FlowPredicate, ...],
    cursor: _FlowOperand | None,
    bindings: Mapping[str, Any],
) -> None:
    parameter_count = len(bindings)
    parameter_count += sum(
        not isinstance(operand, FlowQueryParameter)
        for predicate in predicates
        for operand in predicate.operands
    )
    if cursor is not None and not isinstance(cursor, FlowQueryParameter):
        parameter_count += 1
    if parameter_count > FLOW_QUERY_MAX_PARAMETERS:
        raise ValueError(f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_PARAMETERS} parameters")


def _predicate_token_count(predicate: FlowPredicate) -> int:
    field_tokens = _field_token_count(predicate.field)
    if predicate.operator == "in":
        return field_tokens + 2 * len(predicate.operands) + 2
    if predicate.operator in {"between", "from_to"}:
        return field_tokens + 4
    return field_tokens + 2


def _field_token_count(query_field: FlowField) -> int:
    if query_field.name == "attribute":
        return 4
    if query_field.name == "state_meta":
        return 7
    return 1


def _projection_field_token_count(
    query_field: FlowField | FlowEventField | FlowRunMapField,
) -> int:
    if isinstance(query_field, FlowRunMapField):
        return 1
    if isinstance(query_field, FlowEventField):
        return 1 if query_field.selector == "fields" else 4
    return _field_token_count(query_field)


__all__ = ["compile_flow_query"]
