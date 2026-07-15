from __future__ import annotations

from enum import Enum
from typing import Any

from ferricstore.errors import FerricStoreError
from ferricstore.protocol_constants import (
    _OP_FLOW_CANCEL_MANY,
    _OP_FLOW_COMPLETE_MANY,
    _OP_FLOW_CREATE_MANY,
    _OP_FLOW_FAIL_MANY,
    _OP_FLOW_RETRY_MANY,
    _OP_FLOW_TRANSITION_MANY,
    _OP_FLOW_VALUE_MGET,
    _OP_MGET,
    _OP_MSET,
    _OP_PIPELINE,
    _OP_SET,
)


class _CardinalityPolicy(Enum):
    EXACT_COLLECTION = "exact_collection"
    OK_OR_EXACT_COLLECTION = "ok_or_exact_collection"


_RESPONSE_CARDINALITY_POLICIES = {
    _OP_PIPELINE: _CardinalityPolicy.EXACT_COLLECTION,
    _OP_MGET: _CardinalityPolicy.EXACT_COLLECTION,
    _OP_FLOW_VALUE_MGET: _CardinalityPolicy.EXACT_COLLECTION,
    _OP_SET: _CardinalityPolicy.OK_OR_EXACT_COLLECTION,
    _OP_MSET: _CardinalityPolicy.OK_OR_EXACT_COLLECTION,
    _OP_FLOW_CREATE_MANY: _CardinalityPolicy.OK_OR_EXACT_COLLECTION,
    _OP_FLOW_COMPLETE_MANY: _CardinalityPolicy.OK_OR_EXACT_COLLECTION,
    _OP_FLOW_RETRY_MANY: _CardinalityPolicy.OK_OR_EXACT_COLLECTION,
    _OP_FLOW_TRANSITION_MANY: _CardinalityPolicy.OK_OR_EXACT_COLLECTION,
    _OP_FLOW_FAIL_MANY: _CardinalityPolicy.OK_OR_EXACT_COLLECTION,
    _OP_FLOW_CANCEL_MANY: _CardinalityPolicy.OK_OR_EXACT_COLLECTION,
}


def require_compact_collection_count(
    count: int,
    *,
    max_collection_items: int | None,
    expected_collection_items: int | None = None,
) -> None:
    """Reject an unsafe compact count before allocating its result collection."""
    if max_collection_items is not None and count > max_collection_items:
        raise FerricStoreError("protocol response collection exceeds max_decoded_collection_items")
    if expected_collection_items is not None and count != expected_collection_items:
        raise FerricStoreError(
            f"protocol response collection returned {count} items; "
            f"expected {expected_collection_items}"
        )


def validate_response_cardinality(
    opcode: int,
    value: Any,
    expected_collection_items: int | None,
) -> None:
    """Apply one response-shape contract after compact or generic decoding."""
    if expected_collection_items is None:
        return
    policy = _RESPONSE_CARDINALITY_POLICIES.get(opcode)
    if policy is None:
        return
    if policy is _CardinalityPolicy.OK_OR_EXACT_COLLECTION and _is_ok_scalar(value):
        return
    if not isinstance(value, list):
        raise FerricStoreError(
            f"protocol response returned a scalar; expected {expected_collection_items} items"
        )
    require_compact_collection_count(
        len(value),
        max_collection_items=None,
        expected_collection_items=expected_collection_items,
    )


def _is_ok_scalar(value: Any) -> bool:
    if isinstance(value, bytes):
        return value.lower() == b"ok"
    if isinstance(value, str):
        return value.lower() == "ok"
    return False


__all__ = ["require_compact_collection_count", "validate_response_cardinality"]
