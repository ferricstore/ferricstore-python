from __future__ import annotations

from collections.abc import Sequence
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, cast

from ferricstore.protocol_mset import _parse_compact_mset_payload


class SyncTopologyMsetMixin:
    """Route optimized MSET submissions without treating their payload as opaque."""

    if TYPE_CHECKING:

        def _leased_batch_target_for_keys(self, keys: Sequence[Any]) -> tuple[Any, Any]: ...

        def _release_adapter_lease(self, lease: Any) -> None: ...

    def submit_mset_same_value(self, keys: Sequence[Any], value: Any) -> Future[Any]:
        target, lease = self._leased_batch_target_for_keys(keys)
        try:
            submit_on_lane = getattr(target.adapter, "submit_command_on_lane", None)
            if target.lane_id is not None and callable(submit_on_lane):
                args: list[Any] = ["MSET"]
                for key in keys:
                    args.extend([key, value])
                return cast(Future[Any], submit_on_lane(tuple(args), target.lane_id))
            return cast(Future[Any], target.adapter.submit_mset_same_value(keys, value))
        finally:
            if lease is not None:
                self._release_adapter_lease(lease)

    def submit_mset_payload(self, payload: bytes) -> Future[Any]:
        keys = _parse_compact_mset_payload(payload)
        target, lease = self._leased_batch_target_for_keys(keys)
        try:
            submit_validated_on_lane = getattr(
                target.adapter,
                "_submit_validated_mset_payload_on_lane",
                None,
            )
            if callable(submit_validated_on_lane):
                return cast(
                    Future[Any],
                    submit_validated_on_lane(payload, target.lane_id),
                )
            submit_on_lane = getattr(target.adapter, "submit_mset_payload_on_lane", None)
            if callable(submit_on_lane):
                return cast(Future[Any], submit_on_lane(payload, target.lane_id))
            return cast(Future[Any], target.adapter.submit_mset_payload(payload))
        finally:
            if lease is not None:
                self._release_adapter_lease(lease)


__all__ = ["SyncTopologyMsetMixin"]
