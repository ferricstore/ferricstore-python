from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeAlias

from ferricstore.flow_query_dsl import FlowQuery
from ferricstore.flow_routing import (
    flow_auto_id_routing_key,
    flow_logical_partition_routing_key,
)

FlowQueryInput: TypeAlias = str | FlowQuery


def resolve_flow_query_input(
    query: FlowQueryInput,
    params: Mapping[str, Any] | None,
) -> tuple[str, Mapping[str, Any] | None, str | None]:
    """Resolve raw FQL or a compiled query without allowing two binding sources."""

    if isinstance(query, FlowQuery):
        if params is not None:
            raise ValueError("bind parameters on the FlowQuery instead of passing separate params")
        query_text, bindings = query.compile()
        hint = query._routing_hint()
        if hint is None:
            routing_key = None
        elif hint[0] == "partition":
            routing_key = flow_logical_partition_routing_key(hint[1])
        else:
            routing_key = flow_auto_id_routing_key(hint[1])
        return query_text, bindings, routing_key
    if not isinstance(query, str):
        raise TypeError("FLOW.QUERY query must be text or a FlowQuery")
    return query, params, None


def resolve_flow_explain_input(
    query: FlowQueryInput,
    params: Mapping[str, Any] | None,
) -> tuple[str, Mapping[str, Any] | None, str | None]:
    if isinstance(query, FlowQuery) and query._page_cursor is not None:
        raise ValueError("FlowQuery cursor cannot be used with EXPLAIN")
    return resolve_flow_query_input(query, params)


__all__ = ["FlowQueryInput", "resolve_flow_explain_input", "resolve_flow_query_input"]
