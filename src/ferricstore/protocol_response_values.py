from __future__ import annotations

from typing import Any

from ferricstore.batch_core import is_pipeline_status_batch
from ferricstore.errors import FerricStoreError, OverloadedError, classify_server_error
from ferricstore.protocol_common import _error_message, _map_get, _optional_int, _optional_text
from ferricstore.protocol_constants import _STATUS_BUSY, _STATUS_OK, ProtocolResponse


def _response_value(response: ProtocolResponse) -> Any:
    if response.status == _STATUS_OK:
        return response.value

    message = _error_message(response.value)
    retryable = _optional_bool_field(response.value, "retryable")
    safe_to_retry = _optional_bool_field(response.value, "safe_to_retry")
    retry_after_ms = _optional_int(response.value, "retry_after_ms")
    if response.status == _STATUS_BUSY:
        raise OverloadedError(
            message,
            raw=response.value,
            retry_after_ms=retry_after_ms,
            reason=_optional_text(response.value, "reason"),
            retryable=retryable,
            safe_to_retry=safe_to_retry,
        )
    raise classify_server_error(
        message,
        raw=response.value,
        retryable=retryable,
        safe_to_retry=safe_to_retry,
        retry_after_ms=retry_after_ms,
    )


def _optional_bool_field(value: Any, key: str) -> bool | None:
    raw = _map_get(value, key) if isinstance(value, dict) else None
    return raw if type(raw) is bool else None


def _batch_item_value(item: Any) -> Any:
    if (isinstance(item, list) and len(item) == 2) or (isinstance(item, tuple) and len(item) == 2):
        status = _status_text(item[0]) or "error"
        value = item[1]
        raw: Any = item
    else:
        if not isinstance(item, dict):
            raise FerricStoreError(
                "protocol PIPELINE item is not a map or status pair",
                raw=item,
            )
        status = _optional_text(item, "status") or "error"
        value = _map_get(item, "value")
        raw = item

    if status == "ok":
        return value
    message = _error_message(value)
    if status == "busy":
        raise OverloadedError(
            message,
            raw=raw,
            retryable=_optional_bool_field(value, "retryable"),
            safe_to_retry=_optional_bool_field(value, "safe_to_retry"),
            retry_after_ms=_optional_int(value, "retry_after_ms"),
        )
    raise classify_server_error(
        message,
        raw=raw,
        retryable=_optional_bool_field(value, "retryable"),
        safe_to_retry=_optional_bool_field(value, "safe_to_retry"),
        retry_after_ms=_optional_int(value, "retry_after_ms"),
    )


def _pipeline_pair_list(value: list[Any]) -> bool:
    return is_pipeline_status_batch(value)


def _flow_many_group_values(value: Any, expected_count: int) -> list[Any]:
    if _ok_scalar(value):
        return [value] * expected_count
    if not isinstance(value, list) or len(value) != expected_count:
        raise FerricStoreError("protocol Flow many returned invalid result", raw=value)
    if _pipeline_pair_list(value):
        return [_batch_item_value(item) for item in value]
    return value


def _ok_scalar(value: Any) -> bool:
    if isinstance(value, bytes):
        return value.lower() == b"ok"
    if isinstance(value, str):
        return value.lower() == "ok"
    return False


def _status_text(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError as exc:
            raise FerricStoreError(
                "protocol PIPELINE status is not valid UTF-8",
                raw=value,
            ) from exc
    return None


__all__ = [
    "_batch_item_value",
    "_flow_many_group_values",
    "_ok_scalar",
    "_pipeline_pair_list",
    "_response_value",
    "_status_text",
]
