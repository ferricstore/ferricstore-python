from __future__ import annotations

import contextlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SyncClientPair:
    command: Any
    claim: Any
    owns_command: bool
    owns_claim: bool
    url: str | None


def resolve_sync_client_pair(
    client: Any,
    claim_client: Any | None,
    *,
    from_url: Callable[..., Any],
    command_kwargs: Mapping[str, Any],
    claim_kwargs: Mapping[str, Any],
    rollback: contextlib.ExitStack,
    close: Callable[[Any], None],
) -> SyncClientPair:
    """Resolve worker clients while registering ownership before the next step."""
    url = client if isinstance(client, str) else None
    if url is not None:
        command = from_url(url, **command_kwargs)
        owns_command = True
        rollback.callback(close, command)
    else:
        command = client
        owns_command = False

    if claim_client is None:
        if url is None:
            claim = command
            owns_claim = False
        else:
            claim = from_url(url, **claim_kwargs)
            owns_claim = True
            rollback.callback(close, claim)
    elif isinstance(claim_client, str):
        claim = from_url(claim_client, **claim_kwargs)
        owns_claim = True
        rollback.callback(close, claim)
    else:
        claim = claim_client
        owns_claim = False

    return SyncClientPair(command, claim, owns_command, owns_claim, url)
