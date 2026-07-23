from __future__ import annotations

import builtins
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from ferricstore.client_helpers import (
    _append,
    _append_attributes,
    _append_bool,
    _append_named_counts,
    _append_named_values,
    _append_value_return,
    _has_named_item_values,
    _merge_named_map,
    _normalize_admin_response,
    _now_ms,
    _parse_kv_response,
)
from ferricstore.client_state import _ClientMixinBase
from ferricstore.config_validation import normalize_optional_max_active_ms
from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_builder import (
    build_flow_failure_query,
    build_flow_lineage_query,
    build_flow_list_query,
    build_flow_search_query,
    build_flow_stuck_query,
    build_flow_terminal_query,
)
from ferricstore.flow_query_request import (
    build_flow_query_args,
    has_explain_prefix,
    validate_flow_query_index_id,
    validate_flow_query_text,
)
from ferricstore.flow_query_response import (
    decode_flow_explain_result,
    decode_flow_query_error,
    decode_flow_query_index_status,
    decode_flow_query_result,
)
from ferricstore.flow_query_types import (
    FlowExplainResult,
    FlowQueryIndexStatus,
    FlowQueryResult,
)
from ferricstore.types import (
    ChildSpec,
    FlowRecord,
)


class _ClientQueriesMixin(_ClientMixinBase):
    if TYPE_CHECKING:
        _index_query: Callable[..., builtins.list[FlowRecord]]
        _record: Callable[[dict[Any, Any]], FlowRecord]
        _records: Callable[[Any], builtins.list[FlowRecord]]

    def get(
        self,
        id: str,
        *,
        partition_key: str | bytes | None = None,
        values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
    ) -> FlowRecord | None:
        args: builtins.list[Any] = ["FLOW.GET", id]
        _append(args, "PARTITION", partition_key)
        _append_value_return(args, values=values, value_max_bytes=value_max_bytes)
        value = self.executor.execute_command(*args)
        if value is None:
            return None
        return self._record(value)

    def query(self, query: str, params: dict[str, Any] | None = None) -> FlowQueryResult:
        if isinstance(query, str) and has_explain_prefix(query):
            raise ValueError("query does not accept EXPLAIN; use explain or explain_analyze")
        return decode_flow_query_result(self._execute_flow_query(query, params))

    def explain(self, query: str, params: dict[str, Any] | None = None) -> FlowExplainResult:
        return self._explain_query("EXPLAIN ", query, params)

    def explain_analyze(
        self, query: str, params: dict[str, Any] | None = None
    ) -> FlowExplainResult:
        return self._explain_query("EXPLAIN ANALYZE ", query, params)

    def query_indexes(self, index_id: str | None = None) -> FlowQueryIndexStatus:
        args: list[Any] = ["FLOW.QUERY.INDEXES"]
        if index_id is not None:
            validate_flow_query_index_id(index_id)
            args.append(index_id)
        try:
            value = self.executor.execute_command(*args)
        except FerricStoreError as exc:
            self._raise_flow_query_error(exc)
        return decode_flow_query_index_status(value)

    def list(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        attributes: dict[str, Any] | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        query, params = build_flow_list_query(
            type,
            partition_key=partition_key,
            state=state,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            attributes=attributes,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        return self._execute_flow_record_query(query, params)

    def search(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        attributes: dict[str, Any] | None = None,
        state_meta: dict[str, Any] | None = None,
        terminal_only: bool | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        query, params = build_flow_search_query(
            type,
            state=state,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            terminal_only=terminal_only,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
            attributes=attributes,
            state_meta=state_meta,
        )
        return self._execute_flow_record_query(query, params)

    def stats(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        attributes: dict[str, Any] | None = None,
        consistent_projection: bool | None = None,
    ) -> dict[str, Any]:
        args: builtins.list[Any] = ["FLOW.STATS", type]
        _append(args, "STATE", state)
        _append(args, "COUNT", count)
        _append(args, "PARTITION", partition_key)
        _append_attributes(args, attributes=attributes)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return _parse_kv_response(self.executor.execute_command(*args))

    def attributes(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        """List projected indexed attribute keys for a workflow type/state."""

        args: builtins.list[Any] = ["FLOW.ATTRIBUTES", type]
        _append(args, "STATE", state)
        _append(args, "PARTITION", partition_key)
        _append(args, "COUNT", count)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(self.executor.execute_command(*args)),
        )

    def attribute_values(
        self,
        type: str,
        attribute: str,
        *,
        state: str | None = None,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        """List top projected values for one indexed attribute key."""

        args: builtins.list[Any] = ["FLOW.ATTRIBUTE_VALUES", type, attribute]
        _append(args, "STATE", state)
        _append(args, "PARTITION", partition_key)
        _append(args, "COUNT", count)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(self.executor.execute_command(*args)),
        )

    def terminals(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        query, params = build_flow_terminal_query(
            type,
            state=state,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        return self._execute_flow_record_query(query, params)

    def failures(
        self,
        type: str,
        *,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        query, params = build_flow_failure_query(
            type,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        return self._execute_flow_record_query(query, params)

    def by_parent(self, parent_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        query, params = build_flow_lineage_query("parent_flow_id", parent_flow_id, **kwargs)
        return self._execute_flow_record_query(query, params)

    def by_root(self, root_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        query, params = build_flow_lineage_query("root_flow_id", root_flow_id, **kwargs)
        return self._execute_flow_record_query(query, params)

    def by_correlation(self, correlation_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        query, params = build_flow_lineage_query("correlation_id", correlation_id, **kwargs)
        return self._execute_flow_record_query(query, params)

    def info(
        self,
        type: str,
        *,
        partition_key: str | bytes | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.INFO", type]
        _append(args, "PARTITION", partition_key)
        _append_bool(args, "INCLUDE_COLD", include_cold)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return dict(self.executor.execute_command(*args) or {})

    def stuck(
        self,
        type: str,
        *,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        older_than_ms: int | None = None,
        now_ms: int | None = None,
    ) -> builtins.list[FlowRecord]:
        query, params = build_flow_stuck_query(
            type,
            partition_key=partition_key,
            count=count,
            older_than_ms=older_than_ms,
            now_ms=now_ms,
        )
        return self._execute_flow_record_query(query, params)

    def _execute_flow_query(self, query: str, params: dict[str, Any] | None) -> Any:
        args = build_flow_query_args(query, params)
        try:
            return self.executor.execute_command(*args)
        except FerricStoreError as exc:
            self._raise_flow_query_error(exc)

    def _execute_flow_record_query(
        self, query: str, params: dict[str, Any]
    ) -> builtins.list[FlowRecord]:
        result = self.query(query, params)
        if result.records is None:
            raise FerricStoreError("FLOW record convenience query returned a count result")
        return self._records(list(result.records))

    def _explain_query(
        self, prefix: str, query: str, params: dict[str, Any] | None
    ) -> FlowExplainResult:
        validate_flow_query_text(query)
        if has_explain_prefix(query):
            raise ValueError("query already contains an EXPLAIN prefix")
        value = self._execute_flow_query(prefix + query.strip(), params)
        return decode_flow_explain_result(value)

    @staticmethod
    def _raise_flow_query_error(exc: FerricStoreError) -> None:
        diagnostic = decode_flow_query_error(exc.raw, raw=exc.raw)
        if diagnostic is not None:
            raise diagnostic from exc
        raise exc

    def history(
        self,
        id: str,
        *,
        partition_key: str | bytes | None = None,
        count: int = 100,
        from_event: str | None = None,
        to_event: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        from_version: int | None = None,
        to_version: int | None = None,
        rev: bool | None = None,
        event: str | None = None,
        worker: str | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
        values: bool | None = None,
        payload_max_bytes: int | None = None,
    ) -> builtins.list[Any]:
        args: builtins.list[Any] = ["FLOW.HISTORY", id, "COUNT", count]
        _append(args, "PARTITION", partition_key)
        _append(args, "FROM_EVENT", from_event)
        _append(args, "TO_EVENT", to_event)
        _append(args, "FROM_MS", from_ms)
        _append(args, "TO_MS", to_ms)
        _append(args, "FROM_VERSION", from_version)
        _append(args, "TO_VERSION", to_version)
        _append_bool(args, "REV", rev)
        _append(args, "EVENT", event)
        _append(args, "WORKER", worker)
        _append_bool(args, "INCLUDE_COLD", include_cold)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        _append_bool(args, "VALUES", values)
        _append(args, "PAYLOAD_MAX_BYTES", payload_max_bytes)
        return list(self.executor.execute_command(*args) or [])

    def spawn_children(
        self,
        parent_flow_id: str,
        children: builtins.list[ChildSpec],
        *,
        partition_key: str | bytes | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        group_id: str = "default",
        wait: str = "all",
        wait_state: str | None = None,
        success: str | None = None,
        failure: str | None = None,
        from_state: str | None = None,
        on_child_failed: str | None = None,
        on_parent_closed: str | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        max_active_ms: int | float | str | None = None,
        now_ms: int | None = None,
    ) -> Any:
        args: builtins.list[Any] = [
            "FLOW.SPAWN_CHILDREN",
            parent_flow_id,
            "GROUP",
            group_id,
            "WAIT",
            wait,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "WAIT_STATE", wait_state)
        _append(args, "SUCCESS", success)
        _append(args, "FAILURE", failure)
        _append(args, "FROM_STATE", from_state)
        _append(args, "ON_CHILD_FAILED", on_child_failed)
        _append(args, "ON_PARENT_CLOSED", on_parent_closed)
        _append(args, "MAX_ACTIVE_MS", normalize_optional_max_active_ms(max_active_ms))
        mixed = any(child.partition_key is not None for child in children)
        has_child_max_active = any(child.max_active_ms is not None for child in children)
        if _has_named_item_values(children) or has_child_max_active:
            args.extend(["ITEMS_EXT_V2" if has_child_max_active else "ITEMS_EXT", len(children)])
            for child in children:
                if mixed and child.partition_key is None:
                    raise ValueError("mixed spawn_children items require partition_key")
                child_values = _merge_named_map(values, child.values)
                child_refs = _merge_named_map(value_refs, child.value_refs)
                args.extend(
                    [
                        child.id,
                        child.partition_key if child.partition_key is not None else "-",
                        child.type,
                        self.codec.encode(child.payload),
                    ]
                )
                if has_child_max_active:
                    child_max_active = normalize_optional_max_active_ms(child.max_active_ms)
                    args.append(child_max_active if child_max_active is not None else "-")
                _append_named_counts(args, self.codec, child_values, child_refs)
        else:
            _append_named_values(args, self.codec, values=values, value_refs=value_refs)
            args.append("ITEMS")
            if mixed:
                args.append("MIXED")
            for child in children:
                if mixed:
                    if child.partition_key is None:
                        raise ValueError("mixed spawn_children items require partition_key")
                    args.extend(
                        [
                            child.id,
                            child.partition_key,
                            child.type,
                            self.codec.encode(child.payload),
                        ]
                    )
                else:
                    args.extend([child.id, child.type, self.codec.encode(child.payload)])
        return self.executor.execute_command(*args)
