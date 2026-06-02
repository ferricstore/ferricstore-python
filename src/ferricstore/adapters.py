from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

from ferricstore.errors import map_exception

if TYPE_CHECKING:
    import redis
    import redis.asyncio as aioredis


@runtime_checkable
class RedisCommandExecutor(Protocol):
    """Small adapter surface needed by the SDK."""

    def execute_command(self, *args: Any) -> Any:
        """Execute one Redis/FerricStore command."""


@runtime_checkable
class AsyncRedisCommandExecutor(Protocol):
    """Async adapter surface needed by the async SDK."""

    async def execute_command(self, *args: Any) -> Any:
        """Execute one Redis/FerricStore command asynchronously."""


class RedisAdapter:
    """`redis-py` adapter.

    `redis-py` is the default because it is the standard Python Redis client,
    maintained by Redis, and supports RESP3 via `protocol=3`.
    """

    def __init__(self, client: redis.Redis) -> None:
        self.client = client

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> RedisAdapter:
        import redis

        kwargs.setdefault("protocol", 3)
        kwargs.setdefault("decode_responses", False)
        return cls(redis.Redis.from_url(url, **kwargs))

    def execute_command(self, *args: Any) -> Any:
        try:
            client = cast(Any, self.client)
            return client.execute_command(*args)
        except Exception as exc:
            mapped = map_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc

    def close(self) -> None:
        self.client.close()


class AsyncRedisAdapter:
    """`redis.asyncio` adapter using RESP3 and raw byte responses."""

    def __init__(self, client: aioredis.Redis) -> None:
        self.client = client

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AsyncRedisAdapter:
        import redis.asyncio as aioredis

        kwargs.setdefault("protocol", 3)
        kwargs.setdefault("decode_responses", False)
        return cls(aioredis.Redis.from_url(url, **kwargs))

    async def execute_command(self, *args: Any) -> Any:
        try:
            client = cast(Any, self.client)
            return await client.execute_command(*args)
        except Exception as exc:
            mapped = map_exception(exc)
            if mapped is exc:
                raise
            raise mapped from exc

    async def close(self) -> None:
        await self.client.aclose()
