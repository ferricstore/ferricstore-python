from __future__ import annotations

import builtins
from collections.abc import Mapping
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.client_helpers import (
    _append,
    _append_attributes,
    _append_bool,
    _append_value_return,
    _normalize_admin_response,
    _parse_kv_response,
)
from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_api import (
    FlowQueryInput,
    resolve_flow_explain_input,
    resolve_flow_query_input,
)
from ferricstore.flow_query_builder import (
    _lineage_query_options,
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
    normalize_flow_query_deadline,
    validate_flow_query_index_id,
    validate_flow_query_text,
)
from ferricstore.flow_query_response import (
    decode_flow_explain_result,
    decode_flow_query_error,
    decode_flow_query_index_status,
    decode_flow_query_result,
)
from ferricstore.flow_query_retry import execute_flow_query_read_with_retry_async
from ferricstore.flow_query_types import (
    FlowExplainResult,
    FlowQueryIndexStatus,
    FlowQueryResult,
)
from ferricstore.flow_routing import flow_logical_partition_routing_key
from ferricstore.types import (
    FlowRecord,
)


class _AsyncClientQueriesMixin(_AsyncClientMixinBase):
    async def get(
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
        value = await self.executor.execute_command(*args)
        if value is None:
            return None
        return self._record(value)

    async def query(
        self,
        query: FlowQueryInput,
        params: Mapping[str, Any] | None = None,
        *,
        deadline_ms: int | None = None,
    ) -> FlowQueryResult:
        query_text, resolved_params, routing_key = resolve_flow_query_input(query, params)
        validate_flow_query_text(query_text)
        if has_explain_prefix(query_text):
            raise ValueError("query does not accept EXPLAIN; use explain or explain_analyze")
        return decode_flow_query_result(
            await self._execute_flow_query(
                query_text,
                resolved_params,
                deadline_ms=deadline_ms,
                routing_key=routing_key,
            )
        )

    async def explain(
        self,
        query: FlowQueryInput,
        params: Mapping[str, Any] | None = None,
        *,
        deadline_ms: int | None = None,
    ) -> FlowExplainResult:
        return await self._explain_query("EXPLAIN ", query, params, deadline_ms=deadline_ms)

    async def explain_analyze(
        self,
        query: FlowQueryInput,
        params: Mapping[str, Any] | None = None,
        *,
        deadline_ms: int | None = None,
    ) -> FlowExplainResult:
        return await self._explain_query("EXPLAIN ANALYZE ", query, params, deadline_ms=deadline_ms)

    async def query_indexes(self, index_id: str | None = None) -> FlowQueryIndexStatus:
        args: list[Any] = ["FLOW.QUERY.INDEXES"]
        if index_id is not None:
            validate_flow_query_index_id(index_id)
            args.append(index_id)
        try:

            async def execute() -> Any:
                return await self.executor.execute_command(*args)

            value = await execute_flow_query_read_with_retry_async(
                execute,
                self.backpressure,
            )
        except FerricStoreError as exc:
            self._raise_flow_query_error(exc)
        return decode_flow_query_index_status(value, expected_id=index_id)

    async def list(
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
        return await self._execute_flow_record_query(query, params)

    async def search(
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
        return await self._execute_flow_record_query(query, params)

    async def stats(
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
        return _parse_kv_response(await self.executor.execute_command(*args))

    async def attributes(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        args: builtins.list[Any] = ["FLOW.ATTRIBUTES", type]
        _append(args, "STATE", state)
        _append(args, "PARTITION", partition_key)
        _append(args, "COUNT", count)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def attribute_values(
        self,
        type: str,
        attribute: str,
        *,
        state: str | None = None,
        partition_key: str | bytes | None = None,
        count: int | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        args: builtins.list[Any] = ["FLOW.ATTRIBUTE_VALUES", type, attribute]
        _append(args, "STATE", state)
        _append(args, "PARTITION", partition_key)
        _append(args, "COUNT", count)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def terminals(
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
        return await self._execute_flow_record_query(query, params)

    async def failures(
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
        return await self._execute_flow_record_query(query, params)

    async def by_parent(
        self,
        parent_flow_id: str,
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
    ) -> builtins.list[FlowRecord]:
        query, params = build_flow_lineage_query(
            "parent_flow_id", parent_flow_id, **_lineage_query_options(locals())
        )
        return await self._execute_flow_record_query(query, params)

    async def by_root(
        self,
        root_flow_id: str,
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
    ) -> builtins.list[FlowRecord]:
        query, params = build_flow_lineage_query(
            "root_flow_id", root_flow_id, **_lineage_query_options(locals())
        )
        return await self._execute_flow_record_query(query, params)

    async def by_correlation(
        self,
        correlation_id: str,
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
    ) -> builtins.list[FlowRecord]:
        query, params = build_flow_lineage_query(
            "correlation_id", correlation_id, **_lineage_query_options(locals())
        )
        return await self._execute_flow_record_query(query, params)

    async def info(
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
        return dict(await self.executor.execute_command(*args) or {})

    async def stuck(
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
        return await self._execute_flow_record_query(query, params)

    async def _execute_flow_query(
        self,
        query: str,
        params: Mapping[str, Any] | None,
        *,
        deadline_ms: int | None = None,
        routing_key: str | bytes | None = None,
    ) -> Any:
        args = build_flow_query_args(query, params)
        deadline = None if deadline_ms is None else normalize_flow_query_deadline(deadline_ms)
        try:

            async def execute() -> Any:
                return await self.executor.execute_flow_query_command(
                    *args,
                    deadline_ms=deadline,
                    routing_key=routing_key,
                )

            return await execute_flow_query_read_with_retry_async(
                execute,
                self.backpressure,
                deadline_ms=deadline,
            )
        except FerricStoreError as exc:
            self._raise_flow_query_error(exc)

    async def _execute_flow_record_query(
        self, query: str, params: dict[str, Any]
    ) -> builtins.list[FlowRecord]:
        partition_key = params.get("partition_key")
        routing_key = flow_logical_partition_routing_key(partition_key)
        result = decode_flow_query_result(
            await self._execute_flow_query(query, params, routing_key=routing_key)
        )
        if result.records is None:
            raise FerricStoreError("FLOW record convenience query returned a count result")
        return self._records(list(result.records))

    async def _explain_query(
        self,
        prefix: str,
        query: FlowQueryInput,
        params: Mapping[str, Any] | None,
        *,
        deadline_ms: int | None = None,
    ) -> FlowExplainResult:
        query_text, resolved_params, routing_key = resolve_flow_explain_input(query, params)
        validate_flow_query_text(query_text)
        if has_explain_prefix(query_text):
            raise ValueError("query already contains an EXPLAIN prefix")
        value = await self._execute_flow_query(
            prefix + query_text,
            resolved_params,
            deadline_ms=deadline_ms,
            routing_key=routing_key,
        )
        return decode_flow_explain_result(value)

    @staticmethod
    def _raise_flow_query_error(exc: FerricStoreError) -> None:
        diagnostic = decode_flow_query_error(exc.raw, raw=exc.raw)
        if diagnostic is not None:
            raise diagnostic from exc
        raise exc

    async def history(
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
        return list(await self.executor.execute_command(*args) or [])
