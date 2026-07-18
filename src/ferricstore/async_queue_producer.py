from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_producer import AsyncProducerLoop
from ferricstore.types import CreateItem
from ferricstore.worker_core import AsyncWorkerInvocationTracker


class _AsyncQueueProducerMixin:
    """Producer commands shared by the managed async queue runtime."""

    client: AsyncFlowClient
    type: str
    state: str
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
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        state = attrs.pop("state", self.state)
        partition_key = attrs.pop("partition_key", None)
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
            closed_message="queue flow is closed",
        )

    async def signal(self, id: str, **kwargs: Any) -> Any:
        return await self._invocations.run_while_open(
            lambda: self.client.signal(id, **kwargs),
            closed_message="queue flow is closed",
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
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        state = attrs.pop("state", self.state)
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
                independent=independent,
                **attrs,
            )

        return await self._invocations.run_while_open(
            lambda: self._run_producer(send),
            closed_message="queue flow is closed",
        )

    async def _run_producer(self, send: Callable[[AsyncFlowClient], Awaitable[Any]]) -> Any:
        producer_loop = self._producer_loop
        if producer_loop is None:
            return await send(self.client)
        return await producer_loop.run(send)
