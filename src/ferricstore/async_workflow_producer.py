from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_producer import AsyncProducerLoop
from ferricstore.types import CreateItem
from ferricstore.worker_core import AsyncWorkerInvocationTracker
from ferricstore.workflow_core import pop_workflow_partition_key


class _AsyncWorkflowProducerMixin:
    """Producer and signal surface for the managed async workflow runtime."""

    client: AsyncFlowClient
    type: str
    initial_state: str
    partition_by: Sequence[str]
    _ensure_open: Callable[[], None]
    _invocations: AsyncWorkerInvocationTracker
    _producer_loop: AsyncProducerLoop | None

    async def enqueue(
        self,
        id: str,
        *,
        payload: Any = None,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> Any:
        self._ensure_open()
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        partition_key = pop_workflow_partition_key(attrs, self.partition_by)
        state = attrs.pop("state", self.initial_state)
        return_record = attrs.pop("return_record", False)

        async def send(client: AsyncFlowClient) -> Any:
            return await client.enqueue(
                id,
                type=self.type,
                state=state,
                payload=payload,
                partition_key=partition_key,
                return_record=return_record,
                **attrs,
            )

        return await self._invocations.run_while_open(
            lambda: self._run_producer(send),
            closed_message="workflow is closed",
        )

    async def start_flow(
        self,
        id: str,
        *,
        payload: Any = None,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> Any:
        return await self.enqueue(
            id,
            payload=payload,
            max_active_ms=max_active_ms,
            **attrs,
        )

    async def signal(self, id: str, **kwargs: Any) -> Any:
        self._ensure_open()
        return await self._invocations.run_while_open(
            lambda: self.client.signal(id, **kwargs),
            closed_message="workflow is closed",
        )

    async def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)

    async def enqueue_many(
        self,
        items: Sequence[CreateItem | tuple[str, Any] | str],
        *,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> Any:
        self._ensure_open()
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        partition_key = pop_workflow_partition_key(attrs, self.partition_by)
        state = attrs.pop("state", self.initial_state)
        independent = attrs.pop("independent", True)
        create_items = [
            item
            if isinstance(item, CreateItem)
            else CreateItem(item[0], item[1])
            if isinstance(item, tuple)
            else CreateItem(item)
            for item in items
        ]

        async def send(client: AsyncFlowClient) -> Any:
            return await client.enqueue_many(
                create_items,
                type=self.type,
                state=state,
                partition_key=partition_key,
                independent=independent,
                **attrs,
            )

        return await self._invocations.run_while_open(
            lambda: self._run_producer(send),
            closed_message="workflow is closed",
        )

    async def _run_producer(self, send: Callable[[AsyncFlowClient], Awaitable[Any]]) -> Any:
        producer_loop = self._producer_loop
        if producer_loop is None:
            return await send(self.client)
        return await producer_loop.run(send)
