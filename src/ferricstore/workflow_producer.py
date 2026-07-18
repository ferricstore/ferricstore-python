from __future__ import annotations

import builtins
from collections.abc import Sequence
from typing import Any

from ferricstore.client_core import FlowClient
from ferricstore.types import CreateItem, FlowRecord
from ferricstore.workflow_core import pop_workflow_partition_key, workflow_partition_key


class _WorkflowProducerMixin:
    """Creation and routing surface shared by typed workflow runtimes."""

    client: FlowClient
    type: str
    initial_state: str
    partition_by: Sequence[str]

    def create(
        self,
        id: str,
        *,
        payload: Any = None,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> FlowRecord | bytes:
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        partition_key = self._resolve_partition_key(attrs)
        return self.client.create(
            id,
            type=self.type,
            state=attrs.pop("state", self.initial_state),
            payload=payload,
            partition_key=partition_key,
            **attrs,
        )

    def enqueue(
        self,
        id: str,
        *,
        payload: Any = None,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> FlowRecord | bytes:
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        partition_key = self._resolve_partition_key(attrs)
        return self.client.enqueue(
            id,
            type=self.type,
            state=attrs.pop("state", self.initial_state),
            payload=payload,
            partition_key=partition_key,
            **attrs,
        )

    def start_and_claim(
        self,
        id: str,
        *,
        worker: str,
        payload: Any = None,
        initial_state: str | None = None,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> FlowRecord:
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        partition_key = self._resolve_partition_key(attrs)
        return self.client.start_and_claim(
            id,
            type=self.type,
            initial_state=self.initial_state if initial_state is None else initial_state,
            worker=worker,
            payload=payload,
            partition_key=partition_key,
            **attrs,
        )

    def create_many(
        self,
        partition_key: str | None,
        items: builtins.list[CreateItem],
        *,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> builtins.list[FlowRecord]:
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        return self.client.create_many(
            partition_key,
            items,
            type=self.type,
            state=attrs.pop("state", self.initial_state),
            **attrs,
        )

    def partition_key(self, attrs: dict[str, Any]) -> str | None:
        return workflow_partition_key(attrs, self.partition_by)

    def _resolve_partition_key(self, attrs: dict[str, Any]) -> str | None:
        return pop_workflow_partition_key(
            attrs,
            self.partition_by,
            resolver=self.partition_key,
        )
