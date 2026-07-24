from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Literal, NoReturn

from ferricstore.flow_query_compile import compile_flow_query
from ferricstore.flow_query_dsl_types import (
    MAX_FLOW_QUERY_ORDER_FIELDS,
    MAX_FLOW_QUERY_PREDICATES,
    MAX_FLOW_QUERY_RESULTS,
    FlowEventField,
    FlowField,
    FlowOrder,
    FlowPredicate,
    FlowQueryParameter,
    FlowRunMapField,
    _FlowBoundValue,
    _FlowOperand,
    _operand,
)
from ferricstore.flow_query_limits import FLOW_QUERY_MAX_RETURN_FIELDS
from ferricstore.flow_query_request import (
    FLOW_QUERY_MAX_PARAMETERS,
    normalize_flow_query_params,
)

FlowQuerySource = Literal["runs", "events"]
FlowQueryReturn = Literal["record", "records", "count"]


@dataclass(frozen=True, slots=True)
class FlowQuery:
    """An immutable, composable builder that compiles to parameterized FQL1."""

    source: FlowQuerySource
    predicates: tuple[FlowPredicate, ...] = ()
    orders: tuple[FlowOrder, ...] = ()
    _result_limit: int | None = field(default=None, repr=False)
    _page_cursor: _FlowOperand | None = field(default=None, repr=False)
    _return_shape: FlowQueryReturn | None = field(default=None, repr=False)
    _projection: tuple[FlowField | FlowEventField | FlowRunMapField, ...] = field(
        default=(), repr=False
    )
    _bindings: tuple[tuple[str, Any], ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        if self.source not in {"runs", "events"}:
            raise ValueError("Flow query source must be 'runs' or 'events'")
        object.__setattr__(self, "predicates", tuple(self.predicates))
        object.__setattr__(self, "orders", tuple(self.orders))
        object.__setattr__(self, "_projection", tuple(self._projection))
        object.__setattr__(self, "_bindings", tuple(self._bindings))
        if not all(isinstance(predicate, FlowPredicate) for predicate in self.predicates):
            raise TypeError("FlowQuery predicates must contain only FlowPredicate values")
        if len(self.predicates) > MAX_FLOW_QUERY_PREDICATES:
            raise ValueError(f"FLOW.QUERY accepts at most {MAX_FLOW_QUERY_PREDICATES} predicates")
        if not all(isinstance(order, FlowOrder) for order in self.orders):
            raise TypeError("FlowQuery orders must contain only FlowOrder values")
        if len(self.orders) > MAX_FLOW_QUERY_ORDER_FIELDS:
            raise ValueError(
                f"FLOW.QUERY accepts at most {MAX_FLOW_QUERY_ORDER_FIELDS} order fields"
            )
        selectors = [order.field.selector for order in self.orders]
        if len(selectors) != len(set(selectors)):
            raise ValueError("FLOW.QUERY does not allow a duplicate order field")
        if self._result_limit is not None and (
            type(self._result_limit) is not int
            or not 1 <= self._result_limit <= MAX_FLOW_QUERY_RESULTS
        ):
            raise ValueError(f"FLOW.QUERY limit must be between 1 and {MAX_FLOW_QUERY_RESULTS}")
        if self._page_cursor is not None and not isinstance(
            self._page_cursor, (FlowQueryParameter, _FlowBoundValue)
        ):
            raise TypeError("FlowQuery cursor must be a Flow query value")
        if self._return_shape not in {None, "record", "records", "count"}:
            raise ValueError("Flow query return shape must be record, records, or count")
        _validate_projection(self.source, self._projection)
        if self._return_shape == "count" and self._projection:
            raise ValueError("RETURN COUNT does not accept projection fields")

    @classmethod
    def runs(cls) -> FlowQuery:
        return cls("runs")

    @classmethod
    def events(cls) -> FlowQuery:
        return cls("events")

    def where(self, *predicates: FlowPredicate) -> FlowQuery:
        if not predicates:
            raise ValueError("FlowQuery.where requires at least one predicate")
        if not all(isinstance(predicate, FlowPredicate) for predicate in predicates):
            raise TypeError("FlowQuery.where accepts only FlowPredicate values")
        combined = self.predicates + predicates
        if len(combined) > MAX_FLOW_QUERY_PREDICATES:
            raise ValueError(f"FLOW.QUERY accepts at most {MAX_FLOW_QUERY_PREDICATES} predicates")
        return replace(self, predicates=combined)

    def order_by(self, *orders: FlowOrder) -> FlowQuery:
        if not orders:
            raise ValueError("FlowQuery.order_by requires at least one order")
        if not all(isinstance(order, FlowOrder) for order in orders):
            raise TypeError("FlowQuery.order_by accepts only FlowOrder values")
        combined = self.orders + orders
        if len(combined) > MAX_FLOW_QUERY_ORDER_FIELDS:
            raise ValueError(
                f"FLOW.QUERY accepts at most {MAX_FLOW_QUERY_ORDER_FIELDS} order fields"
            )
        selectors = [order.field.selector for order in combined]
        if len(selectors) != len(set(selectors)):
            raise ValueError("FLOW.QUERY does not allow a duplicate order field")
        return replace(self, orders=combined)

    def limit(self, value: int) -> FlowQuery:
        if type(value) is not int or not 1 <= value <= MAX_FLOW_QUERY_RESULTS:
            raise ValueError(f"FLOW.QUERY limit must be between 1 and {MAX_FLOW_QUERY_RESULTS}")
        return replace(self, _result_limit=value)

    def cursor(self, value: Any) -> FlowQuery:
        """Return the same bound query configured for a subsequent result page."""

        return replace(self, _page_cursor=_operand(value))

    def return_record(self, *projection: FlowField | FlowEventField | FlowRunMapField) -> FlowQuery:
        _validate_projection(self.source, projection)
        return replace(self, _return_shape="record", _projection=projection)

    def return_records(
        self, *projection: FlowField | FlowEventField | FlowRunMapField
    ) -> FlowQuery:
        _validate_projection(self.source, projection)
        return replace(self, _return_shape="records", _projection=projection)

    def return_count(self) -> FlowQuery:
        return replace(self, _return_shape="count", _projection=())

    def bind(
        self,
        params: Mapping[str, Any] | None = None,
        /,
        **values: Any,
    ) -> FlowQuery:
        """Bind named placeholders, retaining prior bindings on the returned query."""

        supplied: dict[str, Any] = {}
        if params is not None:
            if not isinstance(params, Mapping):
                raise TypeError("FlowQuery.bind params must be a mapping")
            supplied = normalize_flow_query_params(params)
        duplicates = supplied.keys() & values.keys()
        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise ValueError(f"Flow query binding provided more than once: {names}")
        normalized_values = normalize_flow_query_params(values)
        if len(supplied) + len(normalized_values) > FLOW_QUERY_MAX_PARAMETERS:
            raise ValueError(f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_PARAMETERS} parameters")
        supplied.update(normalized_values)
        normalized = supplied

        required = self._parameter_names()
        unknown = normalized.keys() - required
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"unknown Flow query binding: {names}")

        current = dict(self._bindings)
        rebound = normalized.keys() & current.keys()
        if rebound:
            names = ", ".join(sorted(rebound))
            raise ValueError(f"Flow query parameter is already bound: {names}")

        current.update(normalized)
        return replace(self, _bindings=tuple(sorted(current.items())))

    def compile(self) -> tuple[str, dict[str, Any]]:
        """Compile the query to FQL1 text and a fresh normalized parameter mapping."""

        self._validate_shape()
        required = self._parameter_names()
        bound = dict(self._bindings)
        missing = required - bound.keys()
        if missing:
            names = ", ".join(sorted(missing))
            raise ValueError(f"missing Flow query binding: {names}")
        unknown = bound.keys() - required
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"unknown Flow query binding: {names}")

        return_shape = self._return_shape
        if return_shape is None:
            _shape_error("a return shape is required")
        return compile_flow_query(
            source=self.source,
            predicates=self.predicates,
            orders=self.orders,
            result_limit=self._result_limit,
            page_cursor=self._page_cursor,
            return_shape=return_shape,
            projection=self._projection,
            bindings=bound,
        )

    def _routing_hint(self) -> tuple[Literal["partition", "auto_id"], str | bytes] | None:
        """Return the validated routing scope after :meth:`compile` succeeds."""

        bindings = dict(self._bindings)
        for field_name in ("partition_key", "run_id"):
            kind: Literal["partition", "auto_id"] = (
                "partition" if field_name == "partition_key" else "auto_id"
            )
            for predicate in self.predicates:
                if predicate.operator != "eq" or predicate.field.name != field_name:
                    continue
                operand = predicate.operands[0]
                value = (
                    bindings.get(operand.name)
                    if isinstance(operand, FlowQueryParameter)
                    else operand.value
                )
                if isinstance(value, (str, bytes)):
                    return kind, value
        return None

    def _parameter_names(self) -> set[str]:
        names = {
            operand.name
            for predicate in self.predicates
            for operand in predicate.operands
            if isinstance(operand, FlowQueryParameter)
        }
        if isinstance(self._page_cursor, FlowQueryParameter):
            names.add(self._page_cursor.name)
        return names

    def _validate_shape(self) -> None:
        if not self.predicates or self._return_shape is None:
            _shape_error("WHERE predicates and a return shape are required")
        if len(self.predicates) > MAX_FLOW_QUERY_PREDICATES:
            _shape_error(f"at most {MAX_FLOW_QUERY_PREDICATES} predicates are supported")

        if self._return_shape == "record":
            self._validate_point_shape()
        elif self._return_shape == "count":
            self._validate_count_shape()
        else:
            self._validate_collection_shape()

    def _validate_point_shape(self) -> None:
        if (
            self.source != "runs"
            or self.orders
            or self._result_limit is not None
            or self._page_cursor is not None
        ):
            _shape_error("RETURN RECORD is only valid for an unpaginated run point read")
        fields = _equality_fields(self.predicates)
        if fields not in [("run_id",), ("partition_key", "run_id"), ("run_id", "partition_key")]:
            _shape_error("a point read requires run_id and optional partition_key equality")

    def _validate_count_shape(self) -> None:
        if (
            self.source != "runs"
            or self.orders
            or self._result_limit is not None
            or self._page_cursor is not None
        ):
            _shape_error("RETURN COUNT supports run predicates without ordering or pagination")
        _require_partition_scope(self.predicates)

    def _validate_collection_shape(self) -> None:
        if not self.orders or self._result_limit is None:
            _shape_error("RETURN RECORDS requires ORDER BY and LIMIT")
        if self.source == "events":
            fields = _equality_fields(self.predicates)
            if fields not in [
                ("run_id",),
                ("partition_key", "run_id"),
                ("run_id", "partition_key"),
            ]:
                _shape_error("event history requires run_id and optional partition_key equality")
            if len(self.orders) != 1 or self.orders[0].field.name != "event_id":
                _shape_error("event history must order by event_id")
            return

        fields = _equality_fields(self.predicates)
        if len(fields) == 2 and set(fields) == {"partition_key", "run_id"}:
            _shape_error("partition_key/run_id point queries must use RETURN RECORD")
        _require_partition_scope(self.predicates)
        if any(order.field.value_kind != "integer" for order in self.orders):
            _shape_error("run collections may order only by integer fields")


def _equality_fields(predicates: tuple[FlowPredicate, ...]) -> tuple[str, ...]:
    if any(predicate.operator != "eq" for predicate in predicates):
        return ()
    return tuple(predicate.field.name for predicate in predicates)


def _require_partition_scope(predicates: tuple[FlowPredicate, ...]) -> None:
    partition_predicates = [
        predicate for predicate in predicates if predicate.field.name == "partition_key"
    ]
    if len(partition_predicates) != 1 or partition_predicates[0].operator != "eq":
        _shape_error("run collections and counts require one partition_key equality")


def _validate_projection(
    source: FlowQuerySource,
    projection: tuple[FlowField | FlowEventField | FlowRunMapField, ...],
) -> None:
    if len(projection) > FLOW_QUERY_MAX_RETURN_FIELDS:
        raise ValueError(
            f"FLOW.QUERY accepts at most {FLOW_QUERY_MAX_RETURN_FIELDS} projection fields"
        )

    selectors: list[str] = []
    for query_field in projection:
        if not isinstance(query_field, (FlowField, FlowEventField, FlowRunMapField)):
            raise TypeError("Flow query projection fields must be Flow field selectors")

        if source == "runs":
            if isinstance(query_field, FlowEventField):
                raise ValueError("runs projection does not support an event field")
            if isinstance(query_field, FlowField) and query_field.name == "event_id":
                raise ValueError("runs projection does not support event_id")
        elif isinstance(query_field, FlowRunMapField):
            raise ValueError(f"events projection does not support {query_field.selector}")
        elif isinstance(query_field, FlowField) and query_field.name != "event_id":
            raise ValueError(f"events projection does not support {query_field.name}")

        selectors.append(query_field.selector)

    if len(selectors) != len(set(selectors)):
        raise ValueError("Flow query return projection contains a duplicate projection field")


def _shape_error(detail: str) -> NoReturn:
    raise ValueError(f"invalid Flow query shape: {detail}")


__all__ = ["FlowQuery", "FlowQueryReturn", "FlowQuerySource"]
