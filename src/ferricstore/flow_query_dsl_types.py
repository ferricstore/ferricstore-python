from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar

from ferricstore.flow_query_limits import (
    FLOW_QUERY_MAX_IN_VALUES as MAX_FLOW_QUERY_IN_VALUES,
)
from ferricstore.flow_query_limits import (
    FLOW_QUERY_MAX_METADATA_NAME_BYTES as MAX_FLOW_QUERY_METADATA_NAME_BYTES,
)
from ferricstore.flow_query_limits import (
    FLOW_QUERY_MAX_ORDER_FIELDS as MAX_FLOW_QUERY_ORDER_FIELDS,
)
from ferricstore.flow_query_limits import (
    FLOW_QUERY_MAX_PREDICATES as MAX_FLOW_QUERY_PREDICATES,
)
from ferricstore.flow_query_limits import (
    FLOW_QUERY_MAX_RESULTS as MAX_FLOW_QUERY_RESULTS,
)
from ferricstore.flow_query_request import (
    normalize_flow_query_parameter,
    validate_flow_query_parameter_name,
)
from ferricstore.flow_query_selectors import (
    flow_metadata_selector,
    normalize_flow_selector_segment,
)

_GENERATED_PARAMETER_PREFIX = "_fql_"

_INTEGER_FIELDS = frozenset(
    {
        "version",
        "priority",
        "created_at_ms",
        "updated_at_ms",
        "next_run_at_ms",
        "lease_deadline_ms",
        "attempts",
        "max_active_ms",
    }
)
_KEYWORD_FIELDS = frozenset(
    {
        "partition_key",
        "run_id",
        "event_id",
        "type",
        "state",
        "run_state",
        "parent_flow_id",
        "root_flow_id",
        "correlation_id",
    }
)
_BUILTIN_FIELDS = _INTEGER_FIELDS | _KEYWORD_FIELDS
_ORDERABLE_FIELDS = _INTEGER_FIELDS | {"event_id"}
_TIME_FIELDS = frozenset({"created_at_ms", "updated_at_ms", "next_run_at_ms", "lease_deadline_ms"})
_PREDICATE_ARITIES: dict[str, int | None] = {
    "eq": 1,
    "in": None,
    "between": 2,
    "from_to": 2,
    "null": 0,
    "missing": 0,
}


@dataclass(frozen=True, slots=True)
class FlowEventField:
    """A validated selector for the event result's ``fields`` map."""

    _metadata_name: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self._metadata_name is not None:
            normalize_flow_selector_segment(
                self._metadata_name,
                "event field",
                maximum_bytes=MAX_FLOW_QUERY_METADATA_NAME_BYTES,
                reject_reserved=True,
            )

    @property
    def selector(self) -> str:
        if self._metadata_name is None:
            return "fields"
        return flow_metadata_selector("fields", self._metadata_name)


@dataclass(frozen=True, slots=True)
class FlowRunMapField:
    """A complete allowlisted metadata map in a projected run result."""

    selector: str

    def __post_init__(self) -> None:
        if not isinstance(self.selector, str):
            raise TypeError("Flow run map field must be text")
        if self.selector not in {"attributes", "state_meta"}:
            raise ValueError(f"unsupported Flow run map field {self.selector!r}")


@dataclass(frozen=True, slots=True)
class FlowQueryParameter:
    """A named FQL parameter whose value is supplied with :meth:`FlowQuery.bind`."""

    name: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, str):
            raise TypeError("Flow query parameter name must be text")
        validate_flow_query_parameter_name(self.name)
        if self.name.startswith(_GENERATED_PARAMETER_PREFIX):
            raise ValueError(
                f"Flow query parameter names beginning with {_GENERATED_PARAMETER_PREFIX!r} "
                "are reserved"
            )


def flow_param(name: str) -> FlowQueryParameter:
    """Create a named parameter placeholder for a composable Flow query."""

    return FlowQueryParameter(name)


@dataclass(frozen=True, slots=True)
class _FlowBoundValue:
    value: Any = field(repr=False)


_FlowOperand = FlowQueryParameter | _FlowBoundValue


@dataclass(frozen=True, slots=True)
class FlowPredicate:
    """An immutable predicate produced by a :class:`FlowField`."""

    field: FlowField
    operator: str
    operands: tuple[_FlowOperand, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.field, FlowField):
            raise TypeError("FlowPredicate field must be a FlowField")
        object.__setattr__(self, "operands", tuple(self.operands))
        if not all(
            isinstance(operand, (FlowQueryParameter, _FlowBoundValue)) for operand in self.operands
        ):
            raise TypeError("FlowPredicate operands must be Flow query values")
        if self.operator not in _PREDICATE_ARITIES:
            raise ValueError(f"unsupported Flow query predicate {self.operator!r}")
        expected = _PREDICATE_ARITIES[self.operator]
        if expected is not None and len(self.operands) != expected:
            raise ValueError(f"Flow query predicate {self.operator!r} has an invalid arity")
        if self.operator == "in" and not 1 <= len(self.operands) <= MAX_FLOW_QUERY_IN_VALUES:
            raise ValueError(f"Flow query IN requires 1..{MAX_FLOW_QUERY_IN_VALUES} values")


@dataclass(frozen=True, slots=True)
class FlowOrder:
    """An immutable FQL ordering expression."""

    field: FlowField
    direction: str

    def __post_init__(self) -> None:
        if not isinstance(self.field, FlowField):
            raise TypeError("FlowOrder field must be a FlowField")
        if self.direction not in {"ASC", "DESC"}:
            raise ValueError("Flow query order direction must be ASC or DESC")
        if not self.field.orderable:
            raise ValueError(f"Flow query field {self.field.selector!r} cannot be ordered")


@dataclass(frozen=True, slots=True)
class FlowField:
    """A validated FQL field selector and predicate factory."""

    name: str
    _state: str | None = field(default=None, repr=False)
    _metadata_name: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.name, str):
            raise TypeError("Flow query field name must be text")
        normalized = self.name.lower()
        if not self.name.isascii():
            raise ValueError(f"unsupported Flow query field {self.name!r}")
        if self._state is None and self._metadata_name is None:
            if normalized not in _BUILTIN_FIELDS:
                raise ValueError(f"unsupported Flow query field {self.name!r}")
            object.__setattr__(self, "name", normalized)
            return
        if normalized == "attribute" and self._state is None:
            normalize_flow_selector_segment(
                self._metadata_name,
                "attribute",
                maximum_bytes=MAX_FLOW_QUERY_METADATA_NAME_BYTES,
                reject_reserved=True,
            )
            object.__setattr__(self, "name", normalized)
            return
        if normalized == "state_meta" and self._state is not None:
            normalize_flow_selector_segment(
                self._state,
                "state_meta state",
                maximum_bytes=MAX_FLOW_QUERY_METADATA_NAME_BYTES,
                reject_reserved=False,
            )
            normalize_flow_selector_segment(
                self._metadata_name,
                "state_meta key",
                maximum_bytes=MAX_FLOW_QUERY_METADATA_NAME_BYTES,
                reject_reserved=True,
            )
            object.__setattr__(self, "name", normalized)
            return
        raise ValueError("invalid Flow query metadata field")

    @classmethod
    def attribute(cls, name: str) -> FlowField:
        return cls("attribute", _metadata_name=name)

    @classmethod
    def state_meta(cls, state: str, name: str) -> FlowField:
        return cls("state_meta", _state=state, _metadata_name=name)

    @property
    def selector(self) -> str:
        if self.name == "attribute":
            metadata_name = self._metadata_name
            if metadata_name is None:
                raise ValueError("invalid Flow query attribute field")
            return flow_metadata_selector("attribute", metadata_name)
        if self.name == "state_meta":
            state = self._state
            metadata_name = self._metadata_name
            if state is None or metadata_name is None:
                raise ValueError("invalid Flow query state_meta field")
            return flow_metadata_selector("state_meta", state, metadata_name)
        return self.name

    @property
    def value_kind(self) -> str:
        if self.name in _INTEGER_FIELDS:
            return "integer"
        if self.name in _KEYWORD_FIELDS:
            return "keyword"
        return "dynamic"

    @property
    def orderable(self) -> bool:
        return self.name in _ORDERABLE_FIELDS and self._metadata_name is None

    @property
    def is_time(self) -> bool:
        return self.name in _TIME_FIELDS and self._metadata_name is None

    @property
    def is_metadata(self) -> bool:
        return self._metadata_name is not None

    def eq(self, value: Any) -> FlowPredicate:
        return FlowPredicate(self, "eq", (_operand(value),))

    def in_(self, *values: Any) -> FlowPredicate:
        if not values:
            raise ValueError("Flow query IN requires at least one value")
        if len(values) > MAX_FLOW_QUERY_IN_VALUES:
            raise ValueError(f"Flow query IN accepts at most {MAX_FLOW_QUERY_IN_VALUES} values")
        return FlowPredicate(self, "in", tuple(_operand(value) for value in values))

    def between(self, lower: Any, upper: Any) -> FlowPredicate:
        return FlowPredicate(self, "between", (_operand(lower), _operand(upper)))

    def from_to(self, lower: Any, upper: Any) -> FlowPredicate:
        if not self.is_time:
            raise ValueError("Flow query FROM/TO requires a timestamp field")
        return FlowPredicate(self, "from_to", (_operand(lower), _operand(upper)))

    def is_null(self) -> FlowPredicate:
        return FlowPredicate(self, "null")

    def is_missing(self) -> FlowPredicate:
        return FlowPredicate(self, "missing")

    def asc(self) -> FlowOrder:
        return FlowOrder(self, "ASC")

    def desc(self) -> FlowOrder:
        return FlowOrder(self, "DESC")


class FlowFields:
    """Namespace containing every FQL1 built-in field and safe metadata selectors."""

    partition_key: ClassVar[FlowField] = FlowField("partition_key")
    run_id: ClassVar[FlowField] = FlowField("run_id")
    event_id: ClassVar[FlowField] = FlowField("event_id")
    type: ClassVar[FlowField] = FlowField("type")
    state: ClassVar[FlowField] = FlowField("state")
    version: ClassVar[FlowField] = FlowField("version")
    priority: ClassVar[FlowField] = FlowField("priority")
    created_at_ms: ClassVar[FlowField] = FlowField("created_at_ms")
    updated_at_ms: ClassVar[FlowField] = FlowField("updated_at_ms")
    next_run_at_ms: ClassVar[FlowField] = FlowField("next_run_at_ms")
    lease_deadline_ms: ClassVar[FlowField] = FlowField("lease_deadline_ms")
    attempts: ClassVar[FlowField] = FlowField("attempts")
    run_state: ClassVar[FlowField] = FlowField("run_state")
    max_active_ms: ClassVar[FlowField] = FlowField("max_active_ms")
    parent_flow_id: ClassVar[FlowField] = FlowField("parent_flow_id")
    root_flow_id: ClassVar[FlowField] = FlowField("root_flow_id")
    correlation_id: ClassVar[FlowField] = FlowField("correlation_id")
    attributes: ClassVar[FlowRunMapField] = FlowRunMapField("attributes")
    state_metadata: ClassVar[FlowRunMapField] = FlowRunMapField("state_meta")
    fields: ClassVar[FlowEventField] = FlowEventField()

    @staticmethod
    def attribute(name: str) -> FlowField:
        return FlowField.attribute(name)

    @staticmethod
    def state_meta(state: str, name: str) -> FlowField:
        return FlowField.state_meta(state, name)

    @staticmethod
    def event_field(name: str) -> FlowEventField:
        return FlowEventField(name)


def _operand(value: Any) -> _FlowOperand:
    if isinstance(value, FlowQueryParameter):
        return value
    return _FlowBoundValue(normalize_flow_query_parameter(value, name="generated"))


__all__ = [
    "MAX_FLOW_QUERY_IN_VALUES",
    "MAX_FLOW_QUERY_ORDER_FIELDS",
    "MAX_FLOW_QUERY_PREDICATES",
    "MAX_FLOW_QUERY_RESULTS",
    "FlowEventField",
    "FlowField",
    "FlowFields",
    "FlowOrder",
    "FlowPredicate",
    "FlowQueryParameter",
    "FlowRunMapField",
    "flow_param",
]
