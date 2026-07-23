from __future__ import annotations

import threading
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from ferricstore.protocol_common import _flow_wake_payload
from ferricstore.protocol_constants import (
    _OP_SUBSCRIBE_EVENTS,
    _USE_ADAPTER_TIMEOUT,
    ProtocolResponse,
)


class SyncProtocolSubscriptionMixin:
    """Reconnect-stable Flow wake subscriptions for a sync transport host."""

    if TYPE_CHECKING:
        _flow_wake_subscriptions: list[dict[str, Any]]
        _subscription_lock: threading.Lock

        def _request(
            self,
            opcode: int,
            lane_id: int,
            payload: dict[str, Any] | bytes,
            flags: int = 0,
            *,
            timeout: float | object | None = _USE_ADAPTER_TIMEOUT,
            exact_lane: bool = False,
            expected_collection_items: int | None = None,
        ) -> ProtocolResponse: ...

        def _response_value(self, response: ProtocolResponse) -> Any: ...

    def subscribe_flow_wake(
        self,
        type: str,
        *,
        state: str | None = None,
        states: list[str] | None = None,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> Any:
        payload = _flow_wake_payload(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        response = self._request(
            _OP_SUBSCRIBE_EVENTS,
            0,
            payload,
        )
        value = self._response_value(response)
        self.register_flow_wake_subscription(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        return value

    def register_flow_wake_subscription(
        self,
        type: str,
        *,
        state: str | None = None,
        states: list[str] | None = None,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> None:
        payload = _flow_wake_payload(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        with self._subscription_lock:
            self._flow_wake_subscriptions[:] = [payload]


class AsyncProtocolSubscriptionMixin:
    """Reconnect-stable Flow wake subscriptions for an async transport host."""

    if TYPE_CHECKING:
        _flow_wake_subscriptions: list[dict[str, Any]]

        async def _request(
            self,
            opcode: int,
            lane_id: int,
            payload: dict[str, Any] | bytes,
            flags: int = 0,
            *,
            timeout: float | object | None = _USE_ADAPTER_TIMEOUT,
            exact_lane: bool = False,
            expected_collection_items: int | None = None,
        ) -> ProtocolResponse: ...

        def _response_value(self, response: ProtocolResponse) -> Any: ...

    async def subscribe_flow_wake(
        self,
        type: str,
        *,
        state: str | None = None,
        states: list[str] | None = None,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> Any:
        payload = _flow_wake_payload(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        response = await self._request(
            _OP_SUBSCRIBE_EVENTS,
            0,
            payload,
        )
        value = self._response_value(response)
        self.register_flow_wake_subscription(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        return value

    def register_flow_wake_subscription(
        self,
        type: str,
        *,
        state: str | None = None,
        states: list[str] | None = None,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> None:
        payload = _flow_wake_payload(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )
        self._flow_wake_subscriptions[:] = [payload]


__all__ = ["AsyncProtocolSubscriptionMixin", "SyncProtocolSubscriptionMixin"]
