from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ferricstore.batch_core import require_batch_items, run_async_fanout
from ferricstore.errors import FerricStoreError
from ferricstore.protocol_commands import (
    _compact_pipeline_payload_from_raw,
)
from ferricstore.protocol_common import _compact_payload_budget
from ferricstore.protocol_constants import (
    _ASYNC_ADAPTER_FANOUT_LIMIT,
    _FLAG_CUSTOM_PAYLOAD,
    _OP_PIPELINE,
    _USE_ADAPTER_TIMEOUT,
    ProtocolCommand,
    ProtocolResponse,
)
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_BATCH_ITEMS,
    DEFAULT_MAX_PENDING_REQUEST_BYTES,
    PendingRequestCapacityError,
    check_batch_item_limit,
)
from ferricstore.protocol_pipeline_codec import _blocks_forever, _pipeline_frame_supported
from ferricstore.protocol_planning import PreparedCommand
from ferricstore.protocol_responses import _flow_many_group_values, _pipeline_pair_list


class AsyncProtocolBatchMixin:
    """Batch execution policy for an async protocol transport host."""

    if TYPE_CHECKING:
        compression: str
        max_batch_items: int | None
        max_pending_request_bytes: int | None

        async def execute_command(self, *args: Any) -> Any: ...

        async def _request(
            self,
            opcode: int,
            lane_id: int,
            payload: dict[str, Any] | bytes,
            flags: int = 0,
            *,
            timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
            exact_lane: bool = False,
            expected_collection_items: int | None = None,
        ) -> ProtocolResponse: ...

        async def _request_on_lane(
            self,
            opcode: int,
            lane_id: int,
            payload: dict[str, Any] | bytes,
            flags: int = 0,
            *,
            timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
            expected_collection_items: int | None = None,
        ) -> ProtocolResponse: ...

        def _response_value(self, response: ProtocolResponse) -> Any: ...

        def _build_protocol_command(self, *args: Any) -> ProtocolCommand: ...

        def _batch_item_value(self, item: Any) -> Any: ...

        def _compact_flow_many_payloads(
            self,
            commands: list[tuple[Any, ...]],
            protocol_commands: list[ProtocolCommand] | None,
        ) -> list[tuple[int, bytes, int]] | None: ...

    def _check_batch_item_count(self, count: int) -> None:
        try:
            check_batch_item_limit(
                count,
                getattr(self, "max_batch_items", DEFAULT_MAX_BATCH_ITEMS),
            )
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc

    def _build_compact_raw_pipeline(
        self,
        commands: list[tuple[Any, ...]],
        *,
        values_only: bool,
        protocol_commands: list[ProtocolCommand] | None = None,
    ) -> bytes | None:
        pending_limit = getattr(
            self,
            "max_pending_request_bytes",
            DEFAULT_MAX_PENDING_REQUEST_BYTES,
        )
        enabled, max_payload_bytes = _compact_payload_budget(
            pending_limit,
            getattr(self, "compression", "none"),
        )
        if not enabled:
            return None
        try:
            return _compact_pipeline_payload_from_raw(
                commands,
                values_only=values_only,
                protocol_commands=protocol_commands,
                max_payload_bytes=max_payload_bytes,
                pending_limit=pending_limit,
            )
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc

    async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return await self._execute_batch(
            commands,
            routed_lane=None,
            concurrent_flow_many=True,
        )

    async def execute_batch_ordered(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return await self._execute_batch(
            commands,
            routed_lane=None,
            concurrent_flow_many=False,
        )

    async def execute_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        return await self._execute_batch(
            commands,
            routed_lane=lane_id,
            concurrent_flow_many=True,
        )

    async def execute_batch_ordered_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        return await self._execute_batch(
            commands,
            routed_lane=lane_id,
            concurrent_flow_many=False,
        )

    async def execute_prepared_batch_on_lane(
        self,
        prepared_commands: list[PreparedCommand],
        lane_id: int,
        *,
        ordered: bool = False,
    ) -> list[Any]:
        self._check_batch_item_count(len(prepared_commands))
        return await self._execute_batch(
            [prepared.args for prepared in prepared_commands],
            routed_lane=lane_id,
            concurrent_flow_many=not ordered,
            prepared_commands=prepared_commands,
        )

    async def _execute_batch(
        self,
        commands: list[tuple[Any, ...]],
        *,
        routed_lane: int | None,
        concurrent_flow_many: bool,
        prepared_commands: list[PreparedCommand] | None = None,
    ) -> list[Any]:
        if not commands:
            return []
        self._check_batch_item_count(len(commands))

        lane_id = 1 if routed_lane is None else routed_lane
        request = self._request if routed_lane is None else self._request_on_lane

        protocol_commands = (
            [prepared.command for prepared in prepared_commands]
            if prepared_commands is not None
            else None
        )
        flow_many_payloads = self._compact_flow_many_payloads(commands, protocol_commands)
        if flow_many_payloads is not None:

            async def request_group(group: tuple[int, bytes, int]) -> list[Any]:
                opcode, payload, count = group
                response = await request(
                    opcode,
                    lane_id,
                    payload,
                    _FLAG_CUSTOM_PAYLOAD,
                )
                group_values = self._response_value(response)
                return _flow_many_group_values(group_values, count)

            groups = await run_async_fanout(
                flow_many_payloads,
                request_group,
                concurrent=concurrent_flow_many,
                max_concurrency=_ASYNC_ADAPTER_FANOUT_LIMIT,
            )
            return [value for group in groups for value in group]

        compact_payload = self._build_compact_raw_pipeline(
            commands,
            values_only=True,
            protocol_commands=protocol_commands,
        )
        if compact_payload is not None:
            response = await request(
                _OP_PIPELINE,
                lane_id,
                compact_payload,
                _FLAG_CUSTOM_PAYLOAD,
            )
            values = require_batch_items(
                self._response_value(response),
                len(commands),
                operation="protocol PIPELINE",
            )
            if _pipeline_pair_list(values):
                return [self._batch_item_value(item) for item in values]
            return values

        if protocol_commands is None:
            protocol_commands = [self._build_protocol_command(*command) for command in commands]
        if not _pipeline_frame_supported(protocol_commands):
            if routed_lane is None:
                return [await self.execute_command(*command) for command in commands]
            values = []
            for index, (raw_command, command) in enumerate(
                zip(commands, protocol_commands, strict=True)
            ):
                command_lane = command.lane_id if routed_lane is None else routed_lane
                blocks_forever = (
                    prepared_commands[index].blocks_forever
                    if prepared_commands is not None
                    else _blocks_forever(raw_command)
                )
                if blocks_forever:
                    response = await request(
                        command.opcode,
                        command_lane,
                        command.payload,
                        command.flags,
                        timeout=None,
                    )
                else:
                    response = await request(
                        command.opcode,
                        command_lane,
                        command.payload,
                        command.flags,
                    )
                values.append(self._response_value(response))
            return values

        batch_commands = [
            {
                "opcode": command.opcode,
                "lane_id": command.lane_id if routed_lane is None else routed_lane,
                "request_id": idx + 1,
                "body": command.payload,
            }
            for idx, command in enumerate(protocol_commands)
        ]
        response = await request(
            _OP_PIPELINE,
            lane_id,
            {"atomicity": "none", "commands": batch_commands, "return": "compact"},
        )

        values = require_batch_items(
            self._response_value(response),
            len(commands),
            operation="protocol PIPELINE",
        )

        return [self._batch_item_value(item) for item in values]


__all__ = ["AsyncProtocolBatchMixin"]
