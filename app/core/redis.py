"""Redis client for caching."""

import json
from typing import Optional, Any
from redis import asyncio as aioredis
from app.core.config import get_settings

settings = get_settings()


class RedisClient:
    """Redis client for caching report data."""

    _client: Optional[aioredis.Redis] = None

    @classmethod
    async def get_client(cls) -> Optional[aioredis.Redis]:
        """Get Redis client instance."""
        if not settings.REDIS_URL and not settings.VALKEY_PUBLIC_HOST:
            return None

        if cls._client is None:
            try:
                # Use Valkey credentials from settings
                redis_url = settings.REDIS_URL or f"redis://{settings.VALKEY_USER}:{settings.VALKEY_PASSWORD}@{settings.VALKEY_PUBLIC_HOST}:{settings.VALKEY_PUBLIC_PORT}"
                cls._client = await aioredis.from_url(
                    redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
                # Test connection
                await cls._client.ping()
            except Exception as e:
                print(f"Redis connection failed: {e}")
                cls._client = None

        return cls._client

    @classmethod
    async def close(cls):
        """Close Redis connection."""
        if cls._client:
            await cls._client.close()
            cls._client = None


async def get_redis() -> Optional[aioredis.Redis]:
    """Dependency to get Redis client."""
    return await RedisClient.get_client()


async def get_cached_report(cache_key: str) -> Optional[dict]:
    """Get cached report data from Redis."""
    client = await get_redis()
    if not client:
        return None

    try:
        cached_data = await client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)
    except Exception as e:
        print(f"Redis get error: {e}")

    return None


async def cache_report(cache_key: str, data: dict, ttl_seconds: int = 3600):
    """Cache report data in Redis."""
    client = await get_redis()
    if not client:
        return False

    try:
        await client.setex(
            cache_key,
            ttl_seconds,
            json.dumps(data, default=str)  # default=str handles datetime serialization
        )
        return True
    except Exception as e:
        print(f"Redis set error: {e}")
        return False


async def invalidate_report_cache(cache_key: str) -> bool:
    """Invalidate cached report data."""
    client = await get_redis()
    if not client:
        return False

    try:
        await client.delete(cache_key)
        return True
    except Exception as e:
        print(f"Redis delete error: {e}")
        return False
