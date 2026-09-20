from redis.asyncio import Redis

from application.abstractions.idempotency.idempotent_store import IdempotentStore


class RedisIdempotentStore(IdempotentStore):
    def __init__(self, redis: Redis, *, key_prefix: str = "idempotency:", ttl_seconds: int = 300) -> None:
        self._redis = redis
        self._key_prefix = key_prefix
        self._ttl_seconds = ttl_seconds

    def _key(self, idempotency_key: str) -> str:
        return f"{self._key_prefix}{idempotency_key}"

    async def check_if_exists(self, idempotency_key: str) -> bool:
        return bool(await self._redis.exists(self._key(idempotency_key)))

    async def mark_as_processed(self, idempotency_key: str) -> None:
        await self._redis.set(self._key(idempotency_key), "1", ex=self._ttl_seconds)

    async def unmark_as_processed(self, idempotency_key: str) -> None:
        await self._redis.delete(self._key(idempotency_key))
