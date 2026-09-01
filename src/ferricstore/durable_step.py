from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from ferricstore.codecs import Codec
from ferricstore.errors import (
    FerricStoreError,
    HttpError,
    RequestOutcomeUnknownError,
)
from ferricstore.types import ClaimedFlow, FlowRecord

_STEP_VALUE_PREFIX = "__ferricstore_step__:sha256:"
_DEFINITELY_REJECTED_HTTP_STATUSES = {
    400,
    401,
    403,
    404,
    405,
    406,
    411,
    413,
    414,
    415,
    422,
    426,
    431,
}
_DEFINITELY_REJECTED_ERROR_CODES = {
    "auth",
    "unauthorized",
    "noperm",
    "forbidden",
    "bad_request",
    "invalid_command",
    "invalid_request",
    "not_found",
    "flow_not_found",
    "stale_lease",
    "wrong_state",
    "conflict",
    "request_too_large",
}
ClaimedJob = ClaimedFlow | FlowRecord


class DurableStepOutcomeUnknownError(RequestOutcomeUnknownError):
    """Internal signal that STEP_CONTINUE may already have been applied."""

    def __init__(self, original: Exception) -> None:
        super().__init__(
            f"{original}; FLOW.STEP_CONTINUE outcome is unknown and replay with the "
            "stale claim is unsafe",
            raw=original,
        )
        self.original = original


def step_outcome_unknown(exc: Exception) -> DurableStepOutcomeUnknownError:
    if isinstance(exc, DurableStepOutcomeUnknownError):
        return exc
    if isinstance(exc, RequestOutcomeUnknownError) and isinstance(exc.raw, Exception):
        return DurableStepOutcomeUnknownError(exc.raw)
    return DurableStepOutcomeUnknownError(exc)


def durable_mutation_outcome_is_unknown(exc: FerricStoreError) -> bool:
    """Return whether an HTTP mutation error can follow a committed request."""

    if not isinstance(exc, HttpError):
        return False
    if exc.status_code == 408 or exc.error_code == "request_timeout":
        return True
    if exc.safe_to_retry is True:
        return False
    if exc.error_code in _DEFINITELY_REJECTED_ERROR_CODES:
        return False
    return exc.status_code not in _DEFINITELY_REJECTED_HTTP_STATUSES


def durable_step_value_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise ValueError("step name must be a non-empty string")
    try:
        encoded_name = name.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("step name must contain valid Unicode") from exc
    return _STEP_VALUE_PREFIX + hashlib.sha256(encoded_name).hexdigest()


def validate_claimed_job(job: ClaimedJob, *, to_state: str) -> None:
    if not isinstance(job, (ClaimedFlow, FlowRecord)):
        raise TypeError("job must be a ClaimedFlow or claimed FlowRecord")
    if not job.id:
        raise ValueError("job.id must be non-empty")
    if not job.lease_token:
        raise ValueError("job.lease_token must be non-empty")
    if not isinstance(job.fencing_token, int) or isinstance(job.fencing_token, bool):
        raise TypeError("job.fencing_token must be an integer")
    if job.fencing_token <= 0:
        raise ValueError("job.fencing_token must be positive")
    if job.state and job.state != "running":
        raise ValueError("job.state must be running")
    if not job.run_state:
        raise ValueError("job.run_state must be non-empty")
    if not isinstance(to_state, str) or not to_state:
        raise ValueError("to_state must be a non-empty string")


def claimed_from_record(record: FlowRecord, fallback: ClaimedJob) -> ClaimedFlow:
    return ClaimedFlow(
        id=record.id,
        lease_token=record.lease_token,
        fencing_token=record.fencing_token,
        partition_key=record.partition_key,
        type=record.type or fallback.type,
        state=record.state or fallback.state,
        run_state=record.run_state or fallback.run_state,
        payload=record.payload if record.payload is not None else fallback.payload,
        attributes=record.attributes if record.attributes is not None else fallback.attributes,
    )


def validate_step_preflight(job: ClaimedJob, record: FlowRecord) -> None:
    if record.id != job.id or record.partition_key != job.partition_key:
        raise FerricStoreError("FLOW.EXTEND_LEASE returned a different workflow identity")
    if record.lease_token != job.lease_token or record.fencing_token != job.fencing_token:
        raise FerricStoreError("FLOW.EXTEND_LEASE returned a different workflow claim")
    if record.state != "running" or record.run_state != job.run_state:
        raise FerricStoreError("FLOW.EXTEND_LEASE returned a different workflow state")


def refreshed_claim(
    previous: ClaimedJob,
    response: Any,
    *,
    to_state: str,
) -> ClaimedFlow:
    compact = isinstance(response, (list, tuple))
    if not compact:
        if not isinstance(response, Mapping):
            raise FerricStoreError("FLOW.STEP_CONTINUE returned an invalid workflow response")
        missing_state = "state" not in response and b"state" not in response
        missing_run_state = "run_state" not in response and b"run_state" not in response
        if missing_state or missing_run_state:
            raise FerricStoreError("FLOW.STEP_CONTINUE returned an unexpected workflow state")

    fresh = ClaimedFlow.from_resp(response)
    if fresh.id != previous.id or fresh.partition_key != previous.partition_key:
        raise FerricStoreError("FLOW.STEP_CONTINUE returned a different workflow identity")
    if not fresh.lease_token or fresh.lease_token == previous.lease_token:
        raise FerricStoreError("FLOW.STEP_CONTINUE did not refresh the workflow lease")
    if fresh.fencing_token <= previous.fencing_token:
        raise FerricStoreError("FLOW.STEP_CONTINUE did not increase the workflow fencing token")
    if fresh.state != "running" or (fresh.run_state is not None and fresh.run_state != to_state):
        raise FerricStoreError("FLOW.STEP_CONTINUE returned an unexpected workflow state")
    return ClaimedFlow(
        id=fresh.id,
        lease_token=fresh.lease_token,
        fencing_token=fresh.fencing_token,
        partition_key=fresh.partition_key,
        type=fresh.type or previous.type,
        state=fresh.state or previous.state,
        run_state=fresh.run_state or to_state,
        payload=fresh.payload if fresh.payload is not None else previous.payload,
        attributes=fresh.attributes if fresh.attributes is not None else previous.attributes,
    )


def value_ref(record: FlowRecord, name: str) -> str | None:
    if record.raw is not None:
        raw_refs = (
            record.raw["value_refs"]
            if "value_refs" in record.raw
            else record.raw.get(b"value_refs")
        )
        if raw_refs is not None and not isinstance(raw_refs, Mapping):
            raise FerricStoreError("FLOW.EXTEND_LEASE returned invalid value_refs")
    elif record.value_refs is not None and not isinstance(record.value_refs, Mapping):
        raise FerricStoreError("FLOW.EXTEND_LEASE returned invalid value_refs")

    refs = record.value_refs or {}
    if name not in refs:
        return None
    meta = refs[name]
    raw = meta.get("ref") or meta.get(b"ref") if isinstance(meta, Mapping) else meta
    if isinstance(raw, bytes):
        raw = raw.decode()
    if isinstance(raw, str) and raw:
        return raw
    raise FerricStoreError("committed durable step has an invalid result reference")


def decode_committed_result(codec: Codec, value: Any) -> Any:
    if value is None or isinstance(value, Mapping):
        raise FerricStoreError("committed durable step result is missing or omitted")
    if isinstance(value, bytearray):
        value = bytes(value)
    if not isinstance(value, bytes):
        raise FerricStoreError("committed durable step result is invalid")
    return codec.decode(value)


def encode_step_result(codec: Codec, value: Any) -> tuple[bytes, Any]:
    encoded = codec.encode(value)
    if not isinstance(encoded, bytes):
        raise TypeError("codec.encode() must return bytes")
    return encoded, codec.decode(encoded)
