from __future__ import annotations

import json
import logging
import random
import time
from typing import Any

import redis
from bson import ObjectId
from bson.errors import InvalidId

from core.config import Settings
from schemas import DocumentStatus
from utils import build_summary, hash_content, mock_summary_content_only, now_utc

logger = logging.getLogger(__name__)

CACHE_KEY_TMPL = "cache:summary:{content_hash}"
LEASE_KEY_TMPL = "processing:{content_hash}"
ACTIVE_COUNT_KEY_TMPL = "active:{user_id}"


def normalize_oid(document_id: str):
    """Convert a string id to ObjectId, falling back to the raw value."""
    try:
        return ObjectId(document_id)
    except (InvalidId, TypeError, ValueError):
        return document_id


def release_active_count(redis_client, user_id: int) -> None:
    """Decrement the user's active-doc counter, ignoring Redis errors."""
    try:
        redis_client.decr(ACTIVE_COUNT_KEY_TMPL.format(user_id=user_id))
    except (redis.RedisError, OSError, TypeError, ValueError) as e:
        logger.warning("active count release failed: %s", e)


def get_cached_content_summary(redis_client, content_hash: str) -> dict | None:
    """Read the shared content summary from Redis, or None on miss/failure."""
    try:
        raw = redis_client.get(CACHE_KEY_TMPL.format(content_hash=content_hash))
        if isinstance(raw, (str, bytes, bytearray)):
            return json.loads(raw)  # type: ignore[arg-type]
        return None
    except (redis.RedisError, OSError, TypeError, ValueError) as e:
        logger.warning("redis cache get failed: %s", e)
    return None


def acquire_compute_lease(redis_client, content_hash: str, owner: str) -> bool:
    """Claim the global compute lease for a hash; True when this worker leads."""
    try:
        ttl = int(Settings.JOB_MAX_SECONDS) + 10
        return bool(
            redis_client.set(
                LEASE_KEY_TMPL.format(content_hash=content_hash), owner, nx=True, ex=ttl
            )
        )
    except (redis.RedisError, OSError, TypeError, ValueError) as e:
        logger.warning("compute lease acquire failed, computing anyway: %s", e)
        return True


def release_compute_lease(redis_client, content_hash: str, owner: str) -> None:
    """Release the compute lease, but only if still owned by this worker."""
    try:
        if redis_client.get(LEASE_KEY_TMPL.format(content_hash=content_hash)) == owner:
            redis_client.delete(LEASE_KEY_TMPL.format(content_hash=content_hash))
    except (redis.RedisError, OSError, TypeError, ValueError) as e:
        logger.warning("compute lease release failed: %s", e)


def write_content_cache(redis_client, content_hash: str, content_only: dict) -> None:
    """Store the content-only summary in Redis for future reuse."""
    try:
        redis_client.setex(
            CACHE_KEY_TMPL.format(content_hash=content_hash),
            Settings.CACHE_TTL_SECONDS,
            json.dumps(content_only),
        )
    except (redis.RedisError, OSError, TypeError, ValueError) as e:
        logger.warning("redis cache set failed: %s", e)


def fanout_siblings(
    coll, content_hash: str, content_only: dict, content: str, skip_oid
) -> None:
    """Mark sibling docs with the same hash COMPLETED from one result."""
    try:
        for sib in coll.find(
            {
                "content_hash": content_hash,
                "status": {"$in": [DocumentStatus.QUEUED, DocumentStatus.PROCESSING]},
                "_id": {"$ne": skip_oid},
            },
            projection={"title": 1},
        ):
            sib_summary = build_summary(sib.get("title", ""), content, content_only)
            coll.update_one(
                {"_id": sib["_id"]},
                {
                    "$set": {
                        "status": DocumentStatus.COMPLETED,
                        "summary": sib_summary,
                        "completed_at": now_utc(),
                        "updated_at": now_utc(),
                    }
                },
            )
    except Exception as e:  # noqa: BLE001
        logger.warning("sibling fan-out failed: %s", e)


def load_and_claim(
    coll, task, oid, document_id: str
) -> tuple[dict | None, dict | None]:
    """Load the doc and claim it for processing, or return an early result."""
    doc = coll.find_one({"_id": oid})
    if doc is None:
        logger.error("process_document: missing %s", document_id)
        return None, {"document_id": document_id, "status": "not_found"}

    claimed = coll.find_one_and_update(
        {"_id": oid, "status": DocumentStatus.QUEUED},
        {
            "$set": {
                "status": DocumentStatus.PROCESSING,
                "started_at": now_utc(),
                "updated_at": now_utc(),
            }
        },
    )
    if claimed is not None:
        return claimed, None

    cur = coll.find_one({"_id": oid}, projection={"status": 1})
    status = (cur or {}).get("status", "unknown")
    if status in (DocumentStatus.COMPLETED, DocumentStatus.FAILED):
        logger.info("document %s already %s, skipping", document_id, status)
        return None, {"document_id": document_id, "status": status, "skipped": True}
    if task.request.retries == 0:
        logger.warning(
            "document %s not queued (status=%s), skipping", document_id, status
        )
        return None, {"document_id": document_id, "status": status, "skipped": True}
    doc = coll.find_one({"_id": oid})
    if doc is None:
        return None, {"document_id": document_id, "status": "not_found"}
    logger.info(
        "retry %s for %s (status=%s)", task.request.retries, document_id, status
    )
    return doc, None


def complete_from_cache(
    coll,
    redis_client,
    task,
    oid,
    doc,
    content_hash: str,
    reused: dict,
    document_id: str,
) -> dict:
    """Complete the doc instantly from a reused cached summary."""
    summary = build_summary(doc.get("title", ""), doc.get("content", ""), reused)
    coll.update_one(
        {"_id": oid},
        {
            "$set": {
                "status": DocumentStatus.COMPLETED,
                "content_hash": content_hash,
                "summary": summary,
                "completed_at": now_utc(),
                "updated_at": now_utc(),
                "retries": task.request.retries,
            }
        },
    )
    logger.info("cache reuse for %s, skipping work", document_id)
    release_active_count(redis_client, doc.get("user_id"))
    return {
        "document_id": document_id,
        "status": DocumentStatus.COMPLETED,
        "summary": summary,
    }


def ensure_single_flight(
    coll, redis_client, task, oid, content_hash: str, document_id: str
) -> None:
    """Let one worker compute per hash; requeue followers via retry."""
    is_leader = acquire_compute_lease(redis_client, content_hash, str(oid))
    if is_leader:
        return
    if task.request.retries >= task.max_retries:
        logger.warning(
            "duplicate compute %s still in-flight, computing anyway (liveness)",
            document_id,
        )
        return
    coll.update_one(
        {"_id": oid},
        {"$set": {"status": DocumentStatus.QUEUED, "updated_at": now_utc()}},
    )
    logger.info(
        "duplicate compute %s in-flight, requeue retry %s",
        document_id,
        task.request.retries + 1,
    )
    raise task.retry(
        exc=RuntimeError("duplicate content already being computed"),
        countdown=5 + task.request.retries * 5,
    )


def run_compute(retries: int) -> None:
    """Simulate document work, failing randomly per settings."""
    logger.debug("simulated processing")
    time.sleep(random.uniform(Settings.JOB_MIN_SECONDS, Settings.JOB_MAX_SECONDS))
    if random.random() < Settings.JOB_FAILURE_RATE or (
        Settings.JOB_PERMANENT_FAILURE_ON_RETRY and retries > 0
    ):
        raise RuntimeError("simulated document processing failure")


def complete_success(
    coll, redis_client, task, oid, doc, content: str, document_id: str
) -> dict:
    """Persist success, publish the cache, fan out, and release counters."""
    content_only = mock_summary_content_only(content)
    summary = build_summary(doc.get("title", ""), content)
    content_hash = hash_content(content)
    coll.update_one(
        {"_id": oid},
        {
            "$set": {
                "status": DocumentStatus.COMPLETED,
                "content_hash": content_hash,
                "summary": summary,
                "completed_at": now_utc(),
                "updated_at": now_utc(),
                "retries": task.request.retries,
            }
        },
    )
    write_content_cache(redis_client, content_hash, content_only)
    fanout_siblings(coll, content_hash, content_only, content, oid)
    release_compute_lease(redis_client, content_hash, str(oid))
    release_active_count(redis_client, doc.get("user_id"))
    logger.info("completed %s", document_id)
    return {
        "document_id": document_id,
        "status": DocumentStatus.COMPLETED,
        "summary": summary,
    }


def handle_failure(
    coll,
    redis_client,
    task,
    oid,
    doc,
    content_hash: str | None,
    document_id: str,
    exc: Exception,
) -> dict:
    """Mark FAILED when retries run out, else requeue and retry."""
    if content_hash is not None:
        release_compute_lease(redis_client, content_hash, str(oid))
    if task.request.retries >= task.max_retries:
        coll.update_one(
            {"_id": oid},
            {
                "$set": {
                    "status": DocumentStatus.FAILED,
                    "error": str(exc),
                    "updated_at": now_utc(),
                    "retries": task.request.retries,
                }
            },
        )
        release_active_count(redis_client, doc.get("user_id"))
        logger.error(
            "document %s failed permanently after %s retries: %s",
            document_id,
            task.request.retries,
            exc,
        )
        return {
            "document_id": document_id,
            "status": DocumentStatus.FAILED,
            "error": str(exc),
        }
    logger.warning(
        "document %s failed, retry %s/%s: %s",
        document_id,
        task.request.retries + 1,
        task.max_retries,
        exc,
    )
    coll.update_one(
        {"_id": oid},
        {"$set": {"status": DocumentStatus.QUEUED, "updated_at": now_utc()}},
    )
    raise task.retry(exc=exc, countdown=2**task.request.retries)


def process_document_task(
    coll: Any, redis_client: Any, task: Any, document_id: str
) -> dict:
    """Run the full worker pipeline for one document."""
    oid = normalize_oid(document_id)

    doc, early_result = load_and_claim(coll, task, oid, document_id)
    if early_result is not None:
        return early_result
    assert doc is not None

    content = doc.get("content", "")
    content_hash = doc.get("content_hash") or hash_content(content)
    reused = get_cached_content_summary(redis_client, content_hash)
    if reused is not None:
        return complete_from_cache(
            coll, redis_client, task, oid, doc, content_hash, reused, document_id
        )

    ensure_single_flight(coll, redis_client, task, oid, content_hash, document_id)

    logger.info("processing %s", document_id)
    try:
        run_compute(task.request.retries)
        content = doc.get("content", "")
        return complete_success(
            coll, redis_client, task, oid, doc, content, document_id
        )
    except Exception as exc:  # noqa: BLE001
        return handle_failure(
            coll, redis_client, task, oid, doc, content_hash, document_id, exc
        )
