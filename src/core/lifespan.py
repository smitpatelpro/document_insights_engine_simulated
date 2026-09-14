from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError

from core.config import Settings
from db.indexes import aensure_indexes

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Connect Mongo and Redis, ensure indexes, and close them on shutdown."""
    app.state.mongo_client = AsyncMongoClient(
        Settings.MONGODB_URI,
        maxPoolSize=100,
        minPoolSize=10,
        maxIdleTimeMS=30000,
        waitQueueTimeoutMS=5000,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=10000,
        retryWrites=True,
        retryReads=True,
        appName="document-insights-api",
    )
    app.state.db = app.state.mongo_client[Settings.MONGODB_DB]
    app.state.documents_collection = app.state.db.get_collection(
        Settings.DOCUMENTS_COLLECTION
    )

    app.state.redis_pool = redis.ConnectionPool.from_url(
        Settings.REDIS_URL,
        max_connections=50,
        socket_connect_timeout=2.0,
        socket_timeout=2.0,
        retry_on_timeout=True,
        health_check_interval=30,
        decode_responses=True,
    )
    app.state.redis = redis.Redis(connection_pool=app.state.redis_pool)

    try:
        await app.state.mongo_client.admin.command("ping")
        logger.info(
            "MongoDB connected: %s/%s", Settings.MONGODB_URI, Settings.MONGODB_DB
        )
    except PyMongoError as e:
        logger.warning("MongoDB ping failed on startup: %s", e)

    try:
        await aensure_indexes(app.state.documents_collection)
        logger.info("MongoDB indexes ensured")
    except PyMongoError as e:
        logger.warning("MongoDB index creation failed on startup: %s", e)

    try:
        await app.state.redis.ping()
        logger.info("Redis connected: %s", Settings.REDIS_URL)
    except redis.RedisError as e:
        logger.warning("Redis ping failed on startup: %s", e)

    yield

    redis_client: redis.Redis = app.state.redis
    mongo_client: AsyncMongoClient = app.state.mongo_client
    try:
        await redis_client.aclose()
        await redis_client.connection_pool.disconnect()
        logger.info("Redis pool closed")
    except Exception as e:  # noqa: BLE001  # ensure graceful shutdown regardless of close error
        logger.warning("Error closing Redis: %s", e)

    try:
        await mongo_client.close()
        logger.info("MongoDB client closed")
    except Exception as e:  # noqa: BLE001  # ensure graceful shutdown regardless of close error
        logger.warning("Error closing MongoDB: %s", e)
