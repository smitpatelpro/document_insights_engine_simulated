from __future__ import annotations

import logging

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

INDEXES = {
    "user_status_created": [
        ("user_id", ASCENDING),
        ("status", ASCENDING),
        ("created_at", DESCENDING),
    ],
    "user_created": [("user_id", ASCENDING), ("created_at", DESCENDING)],
    "content_hash": [("content_hash", ASCENDING)],
    "user_content_status": [
        ("user_id", ASCENDING),
        ("content_hash", ASCENDING),
        ("status", ASCENDING),
    ],
}


def ensure_indexes(collection) -> None:
    """Create missing Mongo indexes for the documents collection."""
    try:
        existing = {idx["name"] for idx in collection.list_indexes()}
    except PyMongoError:
        existing = set()
    for name, keys in INDEXES.items():
        if name in existing:
            continue
        collection.create_index(keys, name=name)
        logger.info("created index %s", name)


async def aensure_indexes(collection) -> None:
    """Create missing Mongo indexes using the async driver."""
    try:
        cursor = await collection.list_indexes()
        existing = set()
        async for idx in cursor:
            existing.add(idx["name"])
    except PyMongoError:
        existing = set()
    for name, keys in INDEXES.items():
        if name in existing:
            continue
        await collection.create_index(keys, name=name)
        logger.info("created index %s", name)
