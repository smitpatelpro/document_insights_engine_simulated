from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import redis.asyncio as redis
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException, status
from pymongo.errors import PyMongoError

from core.config import Settings
from schemas import DocumentCreate, DocumentResponse, DocumentStatus, SubmitResponse
from utils import build_summary, hash_content, now_utc
from workers import tasks

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = [DocumentStatus.QUEUED.value, DocumentStatus.PROCESSING.value]

CACHE_KEY_TMPL = "cache:summary:{content_hash}"
SUBMIT_LOCK_KEY_TMPL = "lock:submit:{user_id}:{content_hash}"
ACTIVE_COUNT_KEY_TMPL = "active:{user_id}"


async def get_cached_summary(
    redis_client: redis.Redis, content_hash: str
) -> dict | None:
    """Fetch the shared content summary from Redis, or None on miss/failure."""
    try:
        raw = await redis_client.get(CACHE_KEY_TMPL.format(content_hash=content_hash))
        if raw:
            return json.loads(raw)
    except (
        redis.RedisError,
        OSError,
        TimeoutError,
        ValueError,
        json.JSONDecodeError,
    ) as e:
        logger.warning("cache get failed: %s", e)
    return None


async def acquire_submit_lock(
    redis_client: redis.Redis, user_id: int, content_hash: str
) -> str | None:
    """Take the per-user submit lock; return its token, or None if unavailable."""
    try:
        candidate = uuid.uuid4().hex
        acquired = await redis_client.set(
            SUBMIT_LOCK_KEY_TMPL.format(user_id=user_id, content_hash=content_hash),
            candidate,
            nx=True,
            px=Settings.SUBMIT_LOCK_TTL_MS,
        )
        return candidate if acquired else None
    except (redis.RedisError, OSError, TimeoutError) as e:
        logger.warning("submit lock acquire failed, continuing without lock: %s", e)
        return None


async def release_submit_lock(
    redis_client: redis.Redis, user_id: int, content_hash: str, token: str | None
) -> None:
    """Release the submit lock, but only if the token still matches."""
    if token is None:
        return
    try:
        key = SUBMIT_LOCK_KEY_TMPL.format(user_id=user_id, content_hash=content_hash)
        if await redis_client.get(key) == token:
            await redis_client.delete(key)
    except (redis.RedisError, OSError, TimeoutError) as e:
        logger.warning("submit lock release failed: %s", e)


async def find_inflight(collection, user_id: int, content_hash: str) -> dict | None:
    """Find the user's in-flight (queued/processing) doc with this hash."""
    try:
        return await collection.find_one(
            {
                "user_id": user_id,
                "content_hash": content_hash,
                "status": {"$in": ACTIVE_STATUSES},
            }
        )
    except PyMongoError as e:
        logger.warning("in-flight lookup failed: %s", e)
        return None


async def _redis_active_count(redis_client: redis.Redis, user_id: int) -> bool | None:
    """Track active docs in Redis; False when over cap, None when Redis is down."""
    try:
        key = ACTIVE_COUNT_KEY_TMPL.format(user_id=user_id)
        count = await redis_client.incr(key)
        if count == 1:
            await redis_client.expire(key, Settings.JOB_MAX_SECONDS + 60)
        if count > Settings.MAX_ACTIVE_PER_USER:
            await redis_client.decr(key)
            return False
        return True
    except (redis.RedisError, OSError):
        logger.warning("redis rate limit failed, falling back to mongo count")
        return None


async def _mongo_active_count(collection, user_id: int) -> int:
    """Count active docs in Mongo as a rate-limit fallback."""
    try:
        return await collection.count_documents(
            {"user_id": user_id, "status": {"$in": ACTIVE_STATUSES}}
        )
    except PyMongoError as e:
        logger.warning("mongo count failed, allowing request: %s", e)
        return 0


async def release_rate_limit(redis_client: redis.Redis, user_id: int) -> None:
    """Decrement the user's Redis active-doc counter."""
    try:
        await redis_client.decr(ACTIVE_COUNT_KEY_TMPL.format(user_id=user_id))
    except (redis.RedisError, OSError):
        logger.warning("redis rate limit release failed")


async def ensure_rate_limit(
    redis_client: redis.Redis, collection, user_id: int
) -> None:
    """Raise 429 when the user already has too many active documents."""
    allowed = await _redis_active_count(redis_client, user_id)
    if allowed is None:
        count = await _mongo_active_count(collection, user_id)
        if count >= Settings.MAX_ACTIVE_PER_USER:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"User {user_id} has {count} active documents "
                f"(max {Settings.MAX_ACTIVE_PER_USER})",
            ) from None
    elif not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"User {user_id} has {Settings.MAX_ACTIVE_PER_USER} "
            f"active documents (max {Settings.MAX_ACTIVE_PER_USER})",
        ) from None


def build_completed_doc(
    payload: DocumentCreate, content_hash: str, cached_summary: dict
) -> dict:
    """Build a COMPLETED document dict from a cached summary."""
    now = now_utc()
    return {
        "user_id": payload.user_id,
        "title": payload.title,
        "content": payload.content,
        "content_hash": content_hash,
        "status": DocumentStatus.COMPLETED.value,
        "summary": build_summary(payload.title, payload.content, cached_summary),
        "retries": 0,
        "created_at": now,
        "updated_at": now,
        "started_at": now,
        "completed_at": now,
    }


def build_queued_doc(payload: DocumentCreate, content_hash: str) -> dict:
    """Build a QUEUED document dict ready for persistence."""
    now = now_utc()
    return {
        "user_id": payload.user_id,
        "title": payload.title,
        "content": payload.content,
        "content_hash": content_hash,
        "status": DocumentStatus.QUEUED.value,
        "retries": 0,
        "created_at": now,
        "updated_at": now,
    }


async def store_completed_from_cache(
    collection, payload: DocumentCreate, content_hash: str, cached_summary: dict
) -> SubmitResponse:
    """Persist a cache-hit document and return its submit response."""
    try:
        res = await collection.insert_one(
            build_completed_doc(payload, content_hash, cached_summary)
        )
    except PyMongoError as e:
        logger.error("mongo insert failed (cache path): %s", e)
        raise HTTPException(status_code=500, detail="Failed to store document") from e
    return SubmitResponse(
        document_id=str(res.inserted_id),
        status=DocumentStatus.COMPLETED,
        cached=True,
    )


async def store_queued_and_enqueue(
    collection, redis_client: redis.Redis, payload: DocumentCreate, content_hash: str
) -> SubmitResponse:
    """Persist a QUEUED document and hand it to the background worker."""
    try:
        res = await collection.insert_one(build_queued_doc(payload, content_hash))
    except PyMongoError as e:
        logger.error("mongo insert failed: %s", e)
        await release_rate_limit(redis_client, payload.user_id)
        raise HTTPException(status_code=500, detail="Failed to store document") from e

    document_id = str(res.inserted_id)
    try:
        tasks.process_document.delay(document_id)
        logger.info("enqueued document %s for user %s", document_id, payload.user_id)
    except Exception as e:
        logger.error("celery enqueue failed for %s: %s", document_id, e)
        try:
            await collection.update_one(
                {"_id": res.inserted_id},
                {
                    "$set": {
                        "status": DocumentStatus.FAILED.value,
                        "error": f"enqueue failed: {e}",
                        "updated_at": now_utc(),
                    }
                },
            )
        except PyMongoError:
            pass
        await release_rate_limit(redis_client, payload.user_id)
        raise HTTPException(status_code=500, detail="Failed to enqueue document") from e

    return SubmitResponse(
        document_id=document_id, status=DocumentStatus.QUEUED, cached=False
    )


async def submit_document(
    payload: DocumentCreate, redis_client: redis.Redis, collection
) -> SubmitResponse:
    """Submit a document via cache, dedup, rate-limit, then enqueue."""
    content_hash = hash_content(payload.content)

    cached_summary = await get_cached_summary(redis_client, content_hash)
    if cached_summary is not None:
        return await store_completed_from_cache(
            collection, payload, content_hash, cached_summary
        )

    lock_token = await acquire_submit_lock(redis_client, payload.user_id, content_hash)
    try:
        rechecked = await get_cached_summary(redis_client, content_hash)
        if rechecked is not None:
            return await store_completed_from_cache(
                collection, payload, content_hash, rechecked
            )

        inflight = await find_inflight(collection, payload.user_id, content_hash)
        if inflight is not None:
            logger.info(
                "dedup hit user %s hash %s -> %s",
                payload.user_id,
                content_hash,
                inflight.get("_id"),
            )
            return SubmitResponse(
                document_id=str(inflight.get("_id")),
                status=inflight.get("status", DocumentStatus.QUEUED.value),
                cached=False,
                deduped=True,
            )

        await ensure_rate_limit(redis_client, collection, payload.user_id)
        return await store_queued_and_enqueue(
            collection, redis_client, payload, content_hash
        )
    finally:
        await release_submit_lock(
            redis_client, payload.user_id, content_hash, lock_token
        )


def to_document_response(doc: dict[str, Any]) -> DocumentResponse:
    """Map a Mongo document to its API response shape."""
    status_val = doc.get("status")
    return DocumentResponse.model_validate(
        {
            "_id": str(doc.get("_id")) if doc.get("_id") is not None else None,
            "document_id": str(doc.get("document_id", doc.get("_id"))),
            "user_id": doc.get("user_id"),
            "title": doc.get("title"),
            "content": doc.get("content"),
            "content_hash": doc.get("content_hash"),
            "status": status_val,
            "summary": doc.get("summary")
            if status_val == DocumentStatus.COMPLETED.value
            else None,
            "error": doc.get("error")
            if status_val == DocumentStatus.FAILED.value
            else None,
            "retries": doc.get("retries", 0),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
            "started_at": doc.get("started_at"),
            "completed_at": doc.get("completed_at"),
        }
    )


async def get_document_by_id(collection, document_id: str) -> DocumentResponse:
    """Fetch one document by id, raising 404 when missing or invalid."""
    try:
        oid = ObjectId(document_id)
    except (InvalidId, TypeError, ValueError):
        raise HTTPException(status_code=404, detail="Document not found") from None

    try:
        doc = await collection.find_one({"_id": oid})
    except PyMongoError as e:
        logger.error("find_one failed: %s", e)
        raise HTTPException(status_code=500, detail="Database error") from e

    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return to_document_response(doc)
