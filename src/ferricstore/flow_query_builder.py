from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ferricstore.flow_query_request import (
    normalize_flow_query_parameter,
    validate_flow_query_text,
)

MAX_FLOW_QUERY_RESULTS = 100
MAX_FLOW_QUERY_PARTITION_BYTES = 65_535
MAX_FLOW_QUERY_TIME = 9_007_199_254_740_991
MAX_FLOW_QUERY_METADATA_KEY_BYTES = 64
MAX_FLOW_QUERY_STATE_BYTES = 64


@dataclass(slots=True)
class _FlowCollectionQuery:
    partition_key: str | bytes
    limit: int
    reverse: bool
    order_field: str = "updated_at_ms"
    predicates: list[str] = field(default_factory=lambda: ["partition_key = @partition_key"])
    params: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.params["partition_key"] = self.partition_key

    def equality(self, selector: str, parameter: str, value: Any) -> None:
        self.params[parameter] = normalize_flow_query_parameter(value, name=parameter)
        self.predicates.append(f"{selector} = @{parameter}")

    def window(self, from_ms: int | None, to_ms: int | None) -> None:
        if from_ms is None and to_ms is None:
            return
        lower = 0 if from_ms is None else _bounded_time(from_ms, "from_ms")
        upper = MAX_FLOW_QUERY_TIME if to_ms is None else _bounded_time(to_ms, "to_ms")
        if lower > upper:
            raise ValueError("from_ms must not exceed to_ms")
        self.predicates.append("updated_at_ms BETWEEN @from_ms AND @to_ms")
        self.params.update(from_ms=lower, to_ms=upper)

    def metadata(self, root: str, values: Mapping[str, Any]) -> None:
        for index, (name, value) in enumerate(_metadata_entries(values, root)):
            self.equality(_metadata_selector(root, name), f"{root}_{index}", value)

    def state_metadata(self, values: Mapping[str, Mapping[str, Any]]) -> None:
        entries: list[tuple[str, str, Any]] = []
        states: set[str] = set()
        for raw_state, metadata in values.items():
            state = _normalized_state_name(raw_state)
            if state in states:
                raise ValueError("state_meta state is duplicated after normalization")
            states.add(state)
            if not isinstance(metadata, Mapping):
                raise TypeError("state_meta must map states to metadata mappings")
            entries.extend(
                (state, name, value) for name, value in _metadata_entries(metadata, "state_meta")
            )
        for index, (state, name, value) in enumerate(
            sorted(entries, key=lambda entry: (entry[0], entry[1]))
        ):
            self.equality(
                _metadata_selector("state_meta", state, name), f"state_meta_{index}", value
            )

    def build(self) -> tuple[str, dict[str, Any]]:
        if len(self.predicates) > 12:
            raise ValueError("FLOW.QUERY accepts at most 12 predicates")
        direction = "DESC" if self.reverse else "ASC"
        query = (
            f"FROM runs WHERE {' AND '.join(self.predicates)} "
            f"ORDER BY {self.order_field} {direction} LIMIT {self.limit} RETURN RECORDS"
        )
        validate_flow_query_text(query)
        return query, self.params


def build_flow_list_query(
    flow_type: str,
    *,
    partition_key: str | bytes | None,
    state: str | None,
    count: int | None,
    from_ms: int | None,
    to_ms: int | None,
    rev: bool | None,
    attributes: Mapping[str, Any] | None,
    include_cold: bool | None,
    consistent_projection: bool | None,
) -> tuple[str, dict[str, Any]]:
    if flow_type == "any" and not attributes:
        raise ValueError("FLOW.QUERY list requires a concrete flow type or an attribute predicate")
    if state == "any" and not attributes:
        raise ValueError("FLOW.QUERY list state any requires an attribute predicate")
    builder = _builder(
        partition_key,
        count=count,
        rev=rev,
        include_cold=include_cold,
        consistent_projection=consistent_projection,
    )
    _add_type(builder, flow_type)
    if state is None:
        builder.equality("state", "state", "queued")
    elif state != "any":
        builder.equality("state", "state", _required_text(state, "state"))
    builder.metadata("attribute", attributes or {})
    builder.window(from_ms, to_ms)
    return builder.build()


def build_flow_search_query(
    flow_type: str,
    *,
    partition_key: str | bytes | None,
    state: str | None,
    count: int | None,
    from_ms: int | None,
    to_ms: int | None,
    rev: bool | None,
    attributes: Mapping[str, Any] | None,
    state_meta: Mapping[str, Any] | None,
    terminal_only: bool | None,
    include_cold: bool | None,
    consistent_projection: bool | None,
) -> tuple[str, dict[str, Any]]:
    attributes = attributes or {}
    normalized_state_meta = _normalize_state_meta(state_meta, state)
    if not attributes and not normalized_state_meta:
        raise ValueError("FLOW.QUERY search requires an attribute or state_meta predicate")
    if flow_type in {"", "any"} and normalized_state_meta:
        raise ValueError("FLOW.QUERY state_meta predicates require a concrete flow type")
    builder = _builder(
        partition_key,
        count=count,
        rev=rev,
        include_cold=include_cold,
        consistent_projection=consistent_projection,
    )
    if flow_type not in {"", "any"}:
        builder.equality("type", "type", _required_text(flow_type, "flow type"))
    _add_search_state(builder, state, terminal_only)
    builder.metadata("attribute", attributes)
    builder.state_metadata(normalized_state_meta)
    builder.window(from_ms, to_ms)
    return builder.build()


def build_flow_terminal_query(flow_type: str, **options: Any) -> tuple[str, dict[str, Any]]:
    state = options.pop("state", None)
    builder = _builder_from_options(options)
    if _required_text(flow_type, "flow type") == "any":
        raise ValueError("FLOW.QUERY terminals require a concrete flow type")
    _add_type(builder, flow_type)
    _add_terminal_state(builder, state)
    builder.window(options.get("from_ms"), options.get("to_ms"))
    return builder.build()


def build_flow_failure_query(flow_type: str, **options: Any) -> tuple[str, dict[str, Any]]:
    return build_flow_list_query(
        flow_type,
        partition_key=options.get("partition_key"),
        state="failed",
        count=options.get("count"),
        from_ms=options.get("from_ms"),
        to_ms=options.get("to_ms"),
        rev=options.get("rev"),
        attributes=None,
        include_cold=options.get("include_cold"),
        consistent_projection=options.get("consistent_projection"),
    )


def build_flow_lineage_query(
    selector: str,
    identifier: str,
    *,
    partition_key: str | bytes | None,
    state: str | None = None,
    count: int | None = None,
    from_ms: int | None = None,
    to_ms: int | None = None,
    rev: bool | None = None,
    attributes: Mapping[str, Any] | None = None,
    terminal_only: bool | None = None,
    include_cold: bool | None = None,
    consistent_projection: bool | None = None,
) -> tuple[str, dict[str, Any]]:
    if terminal_only not in {None, False}:
        raise ValueError("terminal_only cannot be combined with a lineage query")
    if attributes:
        raise ValueError("FLOW.QUERY lineage does not support attribute predicates")
    builder = _builder(
        partition_key,
        count=count,
        rev=rev,
        include_cold=include_cold,
        consistent_projection=consistent_projection,
    )
    builder.equality(selector, "lineage_id", _required_text(identifier, "lineage id"))
    if state not in {None, "any"}:
        builder.equality("state", "state", _required_text(state, "state"))
    builder.metadata("attribute", attributes or {})
    builder.window(from_ms, to_ms)
    return builder.build()


def build_flow_stuck_query(
    flow_type: str,
    *,
    partition_key: str | bytes | None,
    count: int | None,
    older_than_ms: int | None,
    now_ms: int | None,
) -> tuple[str, dict[str, Any]]:
    flow_type = _required_text(flow_type, "flow type")
    if flow_type == "any":
        raise ValueError("FLOW.QUERY stuck requires a concrete flow type")
    now = int(time.time() * 1000) if now_ms is None else _bounded_time(now_ms, "now_ms")
    older = 0 if older_than_ms is None else _bounded_time(older_than_ms, "older_than_ms")
    cutoff = now - older
    if cutoff < 0:
        raise ValueError("older_than_ms must not exceed now_ms")
    builder = _builder(partition_key, count=count, rev=False)
    builder.order_field = "lease_deadline_ms"
    builder.equality("type", "type", flow_type)
    builder.equality("state", "state", "running")
    builder.predicates.append("lease_deadline_ms BETWEEN @lease_from_ms AND @lease_to_ms")
    builder.params.update(lease_from_ms=0, lease_to_ms=cutoff)
    return builder.build()


def _builder_from_options(options: Mapping[str, Any]) -> _FlowCollectionQuery:
    return _builder(
        options.get("partition_key"),
        count=options.get("count"),
        rev=options.get("rev"),
        include_cold=options.get("include_cold"),
        consistent_projection=options.get("consistent_projection"),
    )


def _builder(
    partition_key: str | bytes | None,
    *,
    count: int | None,
    rev: bool | None,
    include_cold: bool | None = None,
    consistent_projection: bool | None = None,
) -> _FlowCollectionQuery:
    partition = _required_partition(partition_key)
    limit = MAX_FLOW_QUERY_RESULTS if count is None else _bounded_count(count)
    reverse = _optional_bool(rev, "rev") or False
    if _optional_bool(include_cold, "include_cold") is True:
        raise ValueError("FLOW.QUERY does not expose include_cold")
    if _optional_bool(consistent_projection, "consistent_projection") is True:
        raise ValueError("FLOW.QUERY does not expose consistent_projection")
    return _FlowCollectionQuery(partition, limit, reverse)


def _add_type(builder: _FlowCollectionQuery, flow_type: str) -> None:
    flow_type = _required_text(flow_type, "flow type")
    if flow_type != "any":
        builder.equality("type", "type", flow_type)


def _add_search_state(
    builder: _FlowCollectionQuery, state: str | None, terminal_only: bool | None
) -> None:
    terminal = _optional_bool(terminal_only, "terminal_only") or False
    if terminal:
        _add_terminal_state(builder, state)
    elif state not in {None, "", "any"}:
        builder.equality("state", "state", _required_text(state, "state"))


def _add_terminal_state(builder: _FlowCollectionQuery, state: str | None) -> None:
    if state in {None, "", "any"}:
        builder.predicates.append("state IN (@terminal_0, @terminal_1, @terminal_2)")
        builder.params.update(terminal_0="completed", terminal_1="failed", terminal_2="cancelled")
    elif state in {"completed", "failed", "cancelled"}:
        builder.equality("state", "state", state)
    else:
        raise ValueError("terminal state must be completed, failed, cancelled, or any")


def _normalize_state_meta(
    state_meta: Mapping[str, Any] | None, state: str | None
) -> Mapping[str, Mapping[str, Any]]:
    if state_meta is None:
        return {}
    if not isinstance(state_meta, Mapping):
        raise TypeError("state_meta must be a mapping")
    if not state_meta:
        return {}
    nested = [isinstance(value, Mapping) for value in state_meta.values()]
    if all(nested):
        return state_meta
    if any(nested):
        raise TypeError("state_meta cannot mix nested and flat values")
    if state in {None, "", "any"}:
        raise ValueError("flat state_meta requires a concrete state")
    return {_required_text(state, "state"): state_meta}


def _metadata_entries(values: Mapping[str, Any], context: str) -> list[tuple[str, Any]]:
    if not isinstance(values, Mapping):
        raise TypeError("metadata filters must be a mapping")
    entries: list[tuple[str, Any]] = []
    names: set[str] = set()
    for raw_name, value in values.items():
        name = _normalized_metadata_name(raw_name, context)
        if name in names:
            raise ValueError(f"{context} key is duplicated after normalization")
        names.add(name)
        entries.append((name, value))
    return sorted(entries, key=lambda entry: entry[0])


def _normalized_metadata_name(value: Any, context: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{context} metadata keys must be strings")
    name = value.strip()
    try:
        size = len(name.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError(f"{context} metadata key must be valid UTF-8") from exc
    if not 1 <= size <= MAX_FLOW_QUERY_METADATA_KEY_BYTES or name.startswith("__"):
        raise ValueError(f"{context} metadata key is invalid or reserved")
    return name


def _normalized_state_name(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("state_meta state names must be strings")
    state = value.strip()
    try:
        size = len(state.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("state_meta state name must be valid UTF-8") from exc
    if not 1 <= size <= MAX_FLOW_QUERY_STATE_BYTES:
        raise ValueError("state_meta state name must be 1..64 bytes")
    return state


def _metadata_selector(root: str, *names: str) -> str:
    suffix = "".join("['" + name.replace("'", "''") + "']" for name in names)
    return root + suffix


def _required_partition(value: str | bytes | None) -> str | bytes:
    if not isinstance(value, (str, bytes)):
        raise ValueError("FLOW.QUERY convenience methods require partition_key")
    try:
        size = len(value if isinstance(value, bytes) else value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ValueError("partition_key must be valid UTF-8") from exc
    if size == 0 or size > MAX_FLOW_QUERY_PARTITION_BYTES:
        raise ValueError(f"partition_key must be 1..{MAX_FLOW_QUERY_PARTITION_BYTES} bytes")
    return value


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _bounded_count(value: Any) -> int:
    if type(value) is not int or not 1 <= value <= MAX_FLOW_QUERY_RESULTS:
        raise ValueError(f"count must be between 1 and {MAX_FLOW_QUERY_RESULTS}")
    return value


def _bounded_time(value: Any, name: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_FLOW_QUERY_TIME:
        raise ValueError(f"{name} must be between 0 and {MAX_FLOW_QUERY_TIME}")
    return value


def _optional_bool(value: Any, name: str) -> bool | None:
    if value is None or type(value) is bool:
        return value
    raise TypeError(f"{name} must be a boolean")


__all__ = [
    "MAX_FLOW_QUERY_PARTITION_BYTES",
    "MAX_FLOW_QUERY_RESULTS",
    "MAX_FLOW_QUERY_TIME",
    "build_flow_failure_query",
    "build_flow_lineage_query",
    "build_flow_list_query",
    "build_flow_search_query",
    "build_flow_stuck_query",
    "build_flow_terminal_query",
]
