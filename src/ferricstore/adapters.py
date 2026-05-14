from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import redis


@runtime_checkable
class RedisCommandExecutor(Protocol):
    """Small adapter surface needed by the SDK."""

    def execute_command(self, *args: Any) -> Any:
        """Execute one Redis/FerricStore command."""


class RedisAdapter:
    """`redis-py` adapter.

    `redis-py` is the default because it is the standard Python Redis client,
    maintained by Redis, and supports RESP3 via `protocol=3`.
    """

    def __init__(self, client: redis.Redis) -> None:
        self.client = client

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> RedisAdapter:
        kwargs.setdefault("protocol", 3)
        kwargs.setdefault("decode_responses", False)
        return cls(redis.Redis.from_url(url, **kwargs))

    def execute_command(self, *args: Any) -> Any:
        return self.client.execute_command(*args)

