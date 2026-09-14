from __future__ import annotations

import redis.asyncio as redis
from fastapi import Request
from pymongo import AsyncMongoClient


async def get_redis(request: Request) -> redis.Redis:
    """Return the shared Redis client from app state."""
    return request.app.state.redis


async def get_mongo_client(request: Request) -> AsyncMongoClient:
    """Return the shared Mongo client from app state."""
    return request.app.state.mongo_client


async def get_documents_collection(request: Request):
    """Return the documents Mongo collection from app state."""
    return request.app.state.documents_collection


async def get_db(request: Request):
    """Return the Mongo database handle from app state."""
    return request.app.state.db
