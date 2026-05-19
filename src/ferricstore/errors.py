from __future__ import annotations

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


class LockHeldError(FerricStoreError):
    code = "lock_held"


class LockNotOwnedError(FerricStoreError):
    code = "lock_not_owned"


class InvalidCommandError(FerricStoreError):
    code = "invalid_command"


def classify_server_error(message: str, *, raw: Any = None) -> FerricStoreError:
    lower = message.lower()

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


def map_exception(exc: Exception) -> Exception:
    if isinstance(exc, FerricStoreError):
        return exc

    name = exc.__class__.__name__
    message = str(exc)
    server_like = name == "ResponseError" or message.startswith(("ERR ", "WRONGTYPE ", "DISTLOCK "))

    if not server_like:
        return exc

    return classify_server_error(message, raw=exc)
