import json
import os
from typing import Any

import redis.asyncio as redis

_redis_client = None


async def get_redis():
    global _redis_client
    if _redis_client is None:
        url = os.environ.get("REDIS_URL", "redis://localhost:6379")
        _redis_client = redis.from_url(url, decode_responses=True)
    return _redis_client


async def cache_get(key: str) -> dict | None:
    try:
        r = await get_redis()
        val = await r.get(key)
        return json.loads(val) if val else None
    except Exception:
        return None


async def cache_set(key: str, value: dict, ttl_seconds: int = 300):
    try:
        r = await get_redis()
        await r.setex(key, ttl_seconds, json.dumps(value, default=str))
    except Exception:
        return None


async def cache_delete(key: str):
    try:
        r = await get_redis()
        await r.delete(key)
    except Exception:
        return None


async def cache_delete_pattern(pattern: str):
    try:
        r = await get_redis()
        keys = await r.keys(pattern)
        if keys:
            await r.delete(*keys)
    except Exception:
        return None
