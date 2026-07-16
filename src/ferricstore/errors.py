from __future__ import annotations

import re
from typing import Any


class FerricStoreError(RuntimeError):
    """Raised when FerricStore returns an error or an SDK invariant fails."""

    code = "ferricstore_error"

    def __init__(self, message: str, *, raw: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.raw = raw


class FlowNotFoundError(FerricStoreError):
    code = "flow_not_found"


class FlowWrongStateError(FerricStoreError):
    code = "flow_wrong_state"


class StaleLeaseError(FerricStoreError):
    code = "stale_lease"


class FlowAlreadyExistsError(FerricStoreError):
    code = "flow_already_exists"


class EffectAlreadyReservedError(FerricStoreError):
    """Raised instead of replaying an external effect with an existing reservation."""

    code = "effect_already_reserved"

    def __init__(self, reservation: Any) -> None:
        super().__init__(
            "workflow effect is already reserved; external call was not replayed",
            raw=reservation,
        )
        self.reservation = reservation


class LockHeldError(FerricStoreError):
    code = "lock_held"


class LockNotOwnedError(FerricStoreError):
    code = "lock_not_owned"


class InvalidCommandError(FerricStoreError):
    code = "invalid_command"


class OverloadedError(FerricStoreError):
    code = "overloaded"

    def __init__(
        self,
        message: str,
        *,
        raw: Any = None,
        retry_after_ms: int | None = None,
        reason: str | None = None,
    ) -> None:
        super().__init__(message, raw=raw)
        self.retry_after_ms = retry_after_ms
        self.reason = reason


def classify_server_error(message: str, *, raw: Any = None) -> FerricStoreError:
    lower = message.lower()

    if "overloaded" in lower or "busy" in lower:
        return OverloadedError(
            message,
            raw=raw,
            retry_after_ms=_int_field(lower, "retry_after_ms"),
            reason=_str_field(lower, "reason"),
        )
    if "already exists" in lower:
        return FlowAlreadyExistsError(message, raw=raw)
    if "wrong state" in lower:
        return FlowWrongStateError(message, raw=raw)
    if "stale flow lease" in lower or "stale lease" in lower or "stale token" in lower:
        return StaleLeaseError(message, raw=raw)
    if "not found" in lower or "does not exist" in lower:
        return FlowNotFoundError(message, raw=raw)
    if "lock is held" in lower or "held by another owner" in lower:
        return LockHeldError(message, raw=raw)
    if "not the lock owner" in lower or "caller is not the lock owner" in lower:
        return LockNotOwnedError(message, raw=raw)
    if "wrong number of arguments" in lower or "syntax error" in lower:
        return InvalidCommandError(message, raw=raw)

    return FerricStoreError(message, raw=raw)


def _int_field(message: str, name: str) -> int | None:
    match = re.search(rf"\b{name}=([0-9]+)\b", message)
    if not match:
        return None
    return int(match.group(1))


def _str_field(message: str, name: str) -> str | None:
    match = re.search(rf"\b{name}=([a-z0-9_:-]+)\b", message)
    if not match:
        return None
    return match.group(1)


def map_exception(exc: Exception) -> Exception:
    if isinstance(exc, FerricStoreError):
        if type(exc) is FerricStoreError:
            classified = classify_server_error(exc.message, raw=exc.raw)
            if type(classified) is not FerricStoreError:
                return classified
        return exc

    name = exc.__class__.__name__
    message = str(exc)
    server_like = name == "ResponseError" or message.startswith(("ERR ", "WRONGTYPE ", "DISTLOCK "))

    if not server_like:
        return exc

    return classify_server_error(message, raw=exc)
