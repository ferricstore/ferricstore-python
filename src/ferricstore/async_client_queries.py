from __future__ import annotations

import builtins
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.client_helpers import (
    _append,
    _append_attributes,
    _append_bool,
    _append_read_options,
    _append_search_state_meta,
    _append_value_return,
    _normalize_admin_response,
    _parse_kv_response,
)
from ferricstore.types import (
    FlowRecord,
)


class _AsyncClientQueriesMixin(_AsyncClientMixinBase):
    async def get(
        self,
        id: str,
        *,
        partition_key: str | None = None,
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

    async def list(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
        count: int | None = None,
        attributes: dict[str, Any] | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.LIST", type]
        _append(args, "STATE", state)
        _append(args, "COUNT", count)
        _append(args, "PARTITION", partition_key)
        _append_attributes(args, attributes=attributes)
        _append_bool(args, "INCLUDE_COLD", include_cold)
        _append_bool(args, "CONSISTENT_PROJECTION", consistent_projection)
        return self._records(await self.executor.execute_command(*args))

    async def search(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
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
        args: builtins.list[Any] = ["FLOW.SEARCH", type]
        _append_read_options(
            args,
            state=state,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            terminal_only=terminal_only,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        _append_attributes(args, attributes=attributes)
        _append_search_state_meta(args, state, state_meta)
        return self._records(await self.executor.execute_command(*args))

    async def stats(
        self,
        type: str,
        *,
        state: str | None = None,
        partition_key: str | None = None,
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
        partition_key: str | None = None,
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
        partition_key: str | None = None,
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
        partition_key: str | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.TERMINALS", type]
        _append_read_options(
            args,
            state=state,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        return self._records(await self.executor.execute_command(*args))

    async def failures(
        self,
        type: str,
        *,
        partition_key: str | None = None,
        count: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
        include_cold: bool | None = None,
        consistent_projection: bool | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.FAILURES", type]
        _append_read_options(
            args,
            partition_key=partition_key,
            count=count,
            from_ms=from_ms,
            to_ms=to_ms,
            rev=rev,
            include_cold=include_cold,
            consistent_projection=consistent_projection,
        )
        return self._records(await self.executor.execute_command(*args))

    async def by_parent(self, parent_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self._index_query("FLOW.BY_PARENT", parent_flow_id, **kwargs)

    async def by_root(self, root_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self._index_query("FLOW.BY_ROOT", root_flow_id, **kwargs)

    async def by_correlation(self, correlation_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self._index_query("FLOW.BY_CORRELATION", correlation_id, **kwargs)

    async def info(
        self,
        type: str,
        *,
        partition_key: str | None = None,
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
        partition_key: str | None = None,
        count: int | None = None,
        older_than_ms: int | None = None,
        now_ms: int | None = None,
    ) -> builtins.list[FlowRecord]:
        args: builtins.list[Any] = ["FLOW.STUCK", type]
        _append(args, "PARTITION", partition_key)
        _append(args, "COUNT", count)
        _append(args, "OLDER_THAN", older_than_ms)
        _append(args, "NOW", now_ms)
        return self._records(await self.executor.execute_command(*args))

    async def history(
        self,
        id: str,
        *,
        partition_key: str | None = None,
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
