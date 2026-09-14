from __future__ import annotations

import logging

from pymongo import MongoClient
from pymongo.collection import Collection

from core.config import Settings
from db.indexes import ensure_indexes

logger = logging.getLogger(__name__)

_client: MongoClient | None = None


def get_client() -> MongoClient:
    """Return the singleton Mongo client, creating indexes on first use."""
    global _client
    if _client is None:
        _client = MongoClient(Settings.MONGODB_URI, serverSelectionTimeoutMS=3000)
        ensure_indexes(get_client()[Settings.MONGODB_DB][Settings.DOCUMENTS_COLLECTION])
    return _client


def get_collection() -> Collection:
    """Return the documents collection for the worker process."""
    return get_client()[Settings.MONGODB_DB][Settings.DOCUMENTS_COLLECTION]
