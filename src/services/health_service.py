from __future__ import annotations

import asyncio

import redis.asyncio as redis
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

from schemas import HealthCheckResponse

REDIS_TIMEOUT = 2.0
MONGO_TIMEOUT = 2.0
COLLECTION_TIMEOUT = 2.0
CELERY_TIMEOUT = 3.0


async def check_redis(redis_client: redis.Redis) -> str:
    """Ping Redis and report its health."""
    try:
        await asyncio.wait_for(redis_client.ping(), timeout=REDIS_TIMEOUT)
        return "ok"
    except (redis.RedisError, TimeoutError, OSError) as e:
        return f"fail: {type(e).__name__}"


async def check_mongodb(mongo_client: AsyncMongoClient) -> str:
    """Ping MongoDB and report its health."""
    try:
        await asyncio.wait_for(
            mongo_client.admin.command("ping"), timeout=MONGO_TIMEOUT
        )
        return "ok"
    except (PyMongoError, TimeoutError, OSError) as e:
        return f"fail: {type(e).__name__}"


async def check_documents_collection(documents_collection) -> str:
    """Probe the documents collection and report its health."""
    try:
        await asyncio.wait_for(
            documents_collection.find_one({}, projection={"_id": 1}),
            timeout=COLLECTION_TIMEOUT,
        )
        return "ok"
    except (PyMongoError, TimeoutError, OSError) as e:
        return f"fail: {type(e).__name__}"


async def check_celery() -> str:
    """Probe the Celery broker in a worker thread and report its health."""
    try:
        from workers.celery_app import check_celery_broker

        return await asyncio.wait_for(
            asyncio.to_thread(check_celery_broker), timeout=CELERY_TIMEOUT
        )
    except (TimeoutError, OSError, RuntimeError) as e:
        return f"fail: {type(e).__name__}: {e}"
    except Exception as e:  # noqa: BLE001  # keep health endpoint from 500ing
        return f"fail: {type(e).__name__}: {e}"


async def run_health_checks(
    redis_client: redis.Redis,
    mongo_client: AsyncMongoClient,
    documents_collection,
) -> HealthCheckResponse:
    """Run all dependency checks and summarise overall health."""
    checks: dict[str, str] = {
        "redis": await check_redis(redis_client),
        "mongodb": await check_mongodb(mongo_client),
        "documents_collection": await check_documents_collection(documents_collection),
        "celery": await check_celery(),
    }
    ok = all(not value.startswith("fail") for value in checks.values())
    return HealthCheckResponse(status="ok" if ok else "degraded", checks=checks)
