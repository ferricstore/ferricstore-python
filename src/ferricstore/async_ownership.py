from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from ferricstore.lifecycle_core import close_resources_async, consume_async_future_exception

_Client = TypeVar("_Client")


@dataclass(frozen=True, slots=True)
class AsyncClientPair(Generic[_Client]):
    """A command/claim client pair with explicit ownership metadata."""

    command: _Client
    claim: _Client
    owns_command: bool
    owns_claim: bool

    def owned_resources(self) -> tuple[Any, ...]:
        resources: list[Any] = []
        if self.owns_command:
            resources.append(self.command)
        if self.owns_claim and self.claim is not self.command:
            resources.append(self.claim)
        return tuple(resources)


@dataclass(frozen=True, slots=True)
class AsyncOwnedClose:
    """One resource plus the ownership transition applied only after close succeeds."""

    resource: Any
    release: Callable[[], None]


async def close_owned_resources_async(
    resources: Sequence[AsyncOwnedClose],
    close_resource: Callable[[Any], Awaitable[None]],
    *,
    max_concurrency: int = 16,
) -> None:
    """Attempt every close while retaining ownership for failed resources."""
    operations: list[Callable[[], Awaitable[None]]] = []
    for owned in resources:

        async def close_one(current: AsyncOwnedClose = owned) -> None:
            await close_resource(current.resource)
            current.release()

        operations.append(close_one)
    await close_resources_async(operations, max_concurrency=max_concurrency)


def _run_async_cleanup(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    try:
        result = close()
    except BaseException:
        return
    if not inspect.isawaitable(result):
        return

    try:
        asyncio.get_running_loop()
    except RuntimeError:

        async def wait_for_cleanup() -> None:
            await result

        with contextlib.suppress(BaseException):
            asyncio.run(wait_for_cleanup())
        return

    task = asyncio.ensure_future(result)
    task.add_done_callback(consume_async_future_exception)


def rollback_async_resources(resources: Iterable[Any]) -> None:
    """Best-effort cleanup that preserves the constructor's primary failure."""
    unique: list[Any] = []
    seen: set[int] = set()
    for resource in resources:
        identity = id(resource)
        if identity not in seen:
            seen.add(identity)
            unique.append(resource)
    for resource in reversed(unique):
        _run_async_cleanup(resource)


def resolve_async_client_pair(
    client: Any,
    claim_client: Any | None,
    *,
    from_url: Callable[..., _Client],
    normalize: Callable[[Any], _Client],
    command_kwargs: Mapping[str, Any],
    claim_kwargs: Mapping[str, Any],
) -> AsyncClientPair[_Client]:
    """Resolve two clients transactionally, rolling back partial URL ownership."""
    created: list[_Client] = []
    try:
        if isinstance(client, str):
            command = from_url(client, **command_kwargs)
            created.append(command)
            owns_command = True
        else:
            command = normalize(client)
            owns_command = False

        if claim_client is None:
            if isinstance(client, str):
                claim = from_url(client, **claim_kwargs)
                created.append(claim)
                owns_claim = True
            else:
                claim = command
                owns_claim = False
        elif isinstance(claim_client, str):
            claim = from_url(claim_client, **claim_kwargs)
            created.append(claim)
            owns_claim = True
        else:
            claim = normalize(claim_client)
            owns_claim = False
    except BaseException:
        rollback_async_resources(created)
        raise

    return AsyncClientPair(command, claim, owns_command, owns_claim)
