from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import Future
from dataclasses import replace
from typing import Any

from ferricstore.config_validation import validate_nonnegative_int
from ferricstore.errors import (
    InvalidCommandError,
)
from ferricstore.protocol_commands import (
    _compact_flow_many_payloads_from_raw,
    build_protocol_command,  # noqa: F401 - historical monkeypatch seam
)
from ferricstore.protocol_common import (
    _command_name,
)
from ferricstore.protocol_compact_budget import transport_compact_encoding_policy
from ferricstore.protocol_constants import (
    _FLAG_CUSTOM_PAYLOAD,
    _OP_FLOW_VALUE_MGET,
    _OP_MGET,
    _OPCODES,
    ProtocolCommand,
    ProtocolResponse,
)
from ferricstore.protocol_lifecycle import DEFAULT_MAX_PENDING_REQUEST_BYTES
from ferricstore.protocol_pipeline_codec import (
    _blocks_forever,
    _compact_kv_keys_payload,
    _compact_kv_set_keys_value_payload,
    _expected_command_collection_items,
)
from ferricstore.protocol_pipelines import ProtocolPipeline
from ferricstore.protocol_planning import PreparedCommand
from ferricstore.protocol_responses import (
    _batch_item_value,
    _response_value,
)
from ferricstore.protocol_subscriptions import SyncProtocolSubscriptionMixin
from ferricstore.protocol_sync_batch import SyncProtocolBatchMixin
from ferricstore.protocol_sync_prepared import SyncPreparedCommandMixin
from ferricstore.protocol_sync_transport import SyncProtocolTransportMixin
from ferricstore.protocol_transport_commands import (
    adapter_from_url,
    build_adapter_protocol_command,
    compact_flow_many_for_adapter,
    prepare_adapter_protocol_command,
)


class ProtocolAdapter(
    SyncProtocolBatchMixin,
    SyncProtocolSubscriptionMixin,
    SyncProtocolTransportMixin,
    SyncPreparedCommandMixin,
):
    """FerricStore protocol TCP adapter for the sync SDK.

    The adapter accepts the small `execute_command(*args)` SDK executor shape.
    It encodes supported FerricStore and FerricFlow commands into native protocol
    typed frames.
    """

    client: ProtocolAdapter
    requires_explicit_session = True
    supports_concurrent_fanout = True

    def _build_protocol_command(self, *args: Any) -> ProtocolCommand:
        return build_adapter_protocol_command(self, args)

    def _prepare_protocol_command(self, args: tuple[Any, ...]) -> PreparedCommand:
        return prepare_adapter_protocol_command(self, args)

    def _compact_flow_many_payloads(
        self,
        commands: list[tuple[Any, ...]],
        protocol_commands: list[ProtocolCommand] | None,
    ) -> list[tuple[int, bytes, int]] | None:
        return compact_flow_many_for_adapter(
            self,
            _compact_flow_many_payloads_from_raw,
            commands,
            protocol_commands,
        )

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> ProtocolAdapter:
        return adapter_from_url(cls, url, kwargs)

    def pipeline(self, transaction: bool = False) -> ProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support transaction mode")
        return ProtocolPipeline(self)

    def execute_command(self, *args: Any) -> Any:
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            response = self._request(
                opcode,
                1,
                payload,
                flags,
                expected_collection_items=_expected_command_collection_items(args),
            )
            return self._response_value(response)
        return self.execute_prepared_command(self._prepare_protocol_command(args))

    def execute_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
        """Execute a routed command on the exact topology lane."""
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            return self._response_value(
                self._request_on_lane(
                    opcode,
                    lane_id,
                    payload,
                    flags,
                    expected_collection_items=_expected_command_collection_items(args),
                )
            )
        return self.execute_prepared_command_on_lane(self._prepare_protocol_command(args), lane_id)

    def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        return self.execute_prepared_command_with_trace(self._prepare_protocol_command(args))

    def execute_command_with_trace_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> dict[str, Any]:
        return self.execute_prepared_command_with_trace_on_lane(
            self._prepare_protocol_command(args),
            lane_id,
        )

    def submit_command(self, *args: Any) -> Future[Any]:
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            _request_id, response_future = self._submit_request(
                opcode,
                1,
                payload,
                flags,
                expected_collection_items=_expected_command_collection_items(args),
                _expire_at_adapter_timeout=not _blocks_forever(args),
            )
            return self._value_future(response_future)
        return self.submit_prepared_command(self._prepare_protocol_command(args))

    def submit_command_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> Future[Any]:
        fast = self._fast_bulk_kv_request(args)
        if fast is not None:
            opcode, payload, flags = fast
            _request_id, response_future = self._submit_request_on_lane(
                opcode,
                lane_id,
                payload,
                flags,
                expected_collection_items=_expected_command_collection_items(args),
                _expire_at_adapter_timeout=not _blocks_forever(args),
            )
            return self._value_future(response_future)
        return self.submit_prepared_command_on_lane(self._prepare_protocol_command(args), lane_id)

    def submit_mget(self, keys: Sequence[Any]) -> Future[Any]:
        with transport_compact_encoding_policy(
            getattr(
                self,
                "max_pending_request_bytes",
                DEFAULT_MAX_PENDING_REQUEST_BYTES,
            ),
            getattr(self, "compression", "none"),
        ):
            payload = _compact_kv_keys_payload(keys, 2)
        if payload is None:
            return self.submit_command("MGET", *keys)
        return self.submit_mget_payload(payload)

    def submit_mget_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("MGET payload must be a non-empty compact binary payload")
        _request_id, response_future = self._submit_request(
            _OP_MGET, 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_flow_value_mget_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "FLOW.VALUE.MGET payload must be a non-empty compact binary payload"
            )
        _request_id, response_future = self._submit_request(
            _OP_FLOW_VALUE_MGET, 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_flow_value_mget_payload_on_lane(
        self,
        payload: bytes,
        lane_id: int,
    ) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "FLOW.VALUE.MGET payload must be a non-empty compact binary payload"
            )
        _request_id, response_future = self._submit_request_on_lane(
            _OP_FLOW_VALUE_MGET,
            lane_id,
            payload,
            _FLAG_CUSTOM_PAYLOAD,
        )
        return self._value_future(response_future)

    def submit_mset_same_value(self, keys: Sequence[Any], value: Any) -> Future[Any]:
        with transport_compact_encoding_policy(
            getattr(
                self,
                "max_pending_request_bytes",
                DEFAULT_MAX_PENDING_REQUEST_BYTES,
            ),
            getattr(self, "compression", "none"),
        ):
            payload = _compact_kv_set_keys_value_payload(keys, value)
        if payload is None:
            args = tuple(item for key in keys for item in (key, value))
            return self.submit_command("MSET", *args)
        return self.submit_mset_payload(payload)

    def submit_mset_payload(self, payload: bytes) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("MSET payload must be a non-empty compact binary payload")
        _request_id, response_future = self._submit_request(
            _OPCODES["MSET"], 1, payload, _FLAG_CUSTOM_PAYLOAD
        )
        return self._value_future(response_future)

    def submit_mset_payload_on_lane(self, payload: bytes, lane_id: int) -> Future[Any]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("MSET payload must be a non-empty compact binary payload")
        _request_id, response_future = self._submit_request_on_lane(
            _OPCODES["MSET"],
            lane_id,
            payload,
            _FLAG_CUSTOM_PAYLOAD,
        )
        return self._value_future(response_future)

    def submit_pipeline_payload(self, payload: bytes, count: int) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("PIPELINE payload must be a non-empty compact binary payload")
        count = validate_nonnegative_int(count, name="count")

        future: Future[list[Any]] = Future()
        future.set_running_or_notify_cancel()
        response_future = self._submit_pipeline_payload(payload, count)
        self._complete_batch_future(response_future, count, future)
        return future

    def submit_pipeline_payload_on_lane(
        self,
        payload: bytes,
        count: int,
        lane_id: int,
    ) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError("PIPELINE payload must be a non-empty compact binary payload")
        count = validate_nonnegative_int(count, name="count")
        future: Future[list[Any]] = Future()
        future.set_running_or_notify_cancel()
        response_future = self._submit_pipeline_payload(
            payload,
            count,
            routed_lane=lane_id,
        )
        self._complete_batch_future(response_future, count, future)
        return future

    def submit_flow_many_payload(
        self, command: str, payload: bytes, count: int
    ) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "Flow many payload must be a non-empty compact binary payload"
            )
        count = validate_nonnegative_int(count, name="count")

        name = _command_name(command)
        if name not in {
            "FLOW.CREATE_MANY",
            "FLOW.COMPLETE_MANY",
            "FLOW.TRANSITION_MANY",
            "FLOW.RETRY_MANY",
            "FLOW.FAIL_MANY",
            "FLOW.CANCEL_MANY",
        }:
            raise InvalidCommandError(f"{name} does not support direct Flow many payload submit")

        future: Future[list[Any]] = Future()
        future.set_running_or_notify_cancel()
        self._submit_flow_many_batch([(_OPCODES[name], payload, count)], count, future)
        return future

    def submit_flow_many_payload_on_lane(
        self,
        command: str,
        payload: bytes,
        count: int,
        lane_id: int,
    ) -> Future[list[Any]]:
        if not isinstance(payload, bytes) or not payload:
            raise InvalidCommandError(
                "Flow many payload must be a non-empty compact binary payload"
            )
        count = validate_nonnegative_int(count, name="count")
        name = _command_name(command)
        if name not in {
            "FLOW.CREATE_MANY",
            "FLOW.COMPLETE_MANY",
            "FLOW.TRANSITION_MANY",
            "FLOW.RETRY_MANY",
            "FLOW.FAIL_MANY",
            "FLOW.CANCEL_MANY",
        }:
            raise InvalidCommandError(f"{name} does not support direct Flow many payload submit")
        future: Future[list[Any]] = Future()
        future.set_running_or_notify_cancel()
        self._submit_flow_many_batch(
            [(_OPCODES[name], payload, count)],
            count,
            future,
            routed_lane=lane_id,
        )
        return future

    def _fast_bulk_kv_request(self, args: tuple[Any, ...]) -> tuple[int, bytes, int] | None:
        if not args:
            return None

        try:
            name = _command_name(args[0])
        except Exception:
            return None
        if name not in {"MGET", "MSET"}:
            return None
        command = self._build_protocol_command(*args)
        if isinstance(command.payload, bytes) and command.flags == _FLAG_CUSTOM_PAYLOAD:
            return command.opcode, command.payload, command.flags
        return None

    def _response_value(self, response: ProtocolResponse) -> Any:
        return _response_value(response)

    def _attach_client_trace(
        self, response: ProtocolResponse, client_trace: dict[str, Any] | None
    ) -> ProtocolResponse:
        if not client_trace:
            return response
        trace = dict(response.trace or {})
        client = dict(trace.get("client") or {})
        client.update(client_trace)
        trace["client"] = client
        return replace(response, trace=trace)

    def _batch_item_value(self, item: Any) -> Any:
        return _batch_item_value(item)


__all__ = [
    "ProtocolAdapter",
    "ProtocolPipeline",
]
