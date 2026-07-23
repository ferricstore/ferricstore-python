from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any

from ferricstore.protocol_constants import (
    _FLAG_TRACE,
    _USE_ADAPTER_TIMEOUT,
    ProtocolResponse,
)
from ferricstore.protocol_planning import PreparedCommand


class SyncPreparedCommandMixin(ABC):
    """Execute an already parsed command without rebuilding transport metadata."""

    @abstractmethod
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

    @abstractmethod
    def _request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | object | None = _USE_ADAPTER_TIMEOUT,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse: ...

    @abstractmethod
    def _submit_request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
        _deadline: float | None = None,
        _expire_at_adapter_timeout: bool = True,
    ) -> tuple[int, Future[ProtocolResponse]]: ...

    @abstractmethod
    def _submit_request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        expected_collection_items: int | None = None,
        _expire_at_adapter_timeout: bool = True,
    ) -> tuple[int, Future[ProtocolResponse]]: ...

    @abstractmethod
    def _value_future(self, response_future: Future[ProtocolResponse]) -> Future[Any]: ...

    @abstractmethod
    def _response_value(self, response: ProtocolResponse) -> Any: ...

    def execute_prepared_command(self, prepared: PreparedCommand) -> Any:
        command = prepared.command
        response = self._request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags,
            timeout=None if prepared.blocks_forever else _USE_ADAPTER_TIMEOUT,
            expected_collection_items=prepared.expected_collection_items,
        )
        return self._response_value(response)

    def execute_prepared_command_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> Any:
        command = prepared.command
        response = self._request_on_lane(
            command.opcode,
            lane_id,
            command.payload,
            command.flags,
            timeout=None if prepared.blocks_forever else _USE_ADAPTER_TIMEOUT,
            expected_collection_items=prepared.expected_collection_items,
        )
        return self._response_value(response)

    def execute_prepared_command_with_trace(
        self,
        prepared: PreparedCommand,
    ) -> dict[str, Any]:
        command = prepared.command
        response = self._request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
            timeout=None if prepared.blocks_forever else _USE_ADAPTER_TIMEOUT,
            expected_collection_items=prepared.expected_collection_items,
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    def execute_prepared_command_with_trace_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> dict[str, Any]:
        command = prepared.command
        response = self._request_on_lane(
            command.opcode,
            lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
            timeout=None if prepared.blocks_forever else _USE_ADAPTER_TIMEOUT,
            expected_collection_items=prepared.expected_collection_items,
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    def submit_prepared_command(
        self,
        prepared: PreparedCommand,
    ) -> Future[Any]:
        command = prepared.command
        _request_id, response_future = self._submit_request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags,
            expected_collection_items=prepared.expected_collection_items,
            _expire_at_adapter_timeout=not prepared.blocks_forever,
        )
        return self._value_future(response_future)

    def submit_prepared_command_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> Future[Any]:
        command = prepared.command
        _request_id, response_future = self._submit_request_on_lane(
            command.opcode,
            lane_id,
            command.payload,
            command.flags,
            expected_collection_items=prepared.expected_collection_items,
            _expire_at_adapter_timeout=not prepared.blocks_forever,
        )
        return self._value_future(response_future)


__all__ = ["SyncPreparedCommandMixin"]
