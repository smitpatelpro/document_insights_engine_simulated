from __future__ import annotations

import random  # noqa: F401  # re-exported so tests can patch tasks.random
import time  # noqa: F401  # re-exported so tests can patch tasks.time

import redis
from celery import shared_task

from core.config import Settings
from db.client import get_collection
from services import document_processor

_redis_pool = redis.ConnectionPool.from_url(
    Settings.REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=2,
    max_connections=10,
)
_redis_client = redis.Redis(connection_pool=_redis_pool)


@shared_task(
    name="tasks.process_document", bind=True, max_retries=Settings.TASK_MAX_RETRIES
)
def process_document(self, document_id: str) -> dict:
    """Run the document summarization pipeline for one id."""
    return document_processor.process_document_task(
        get_collection(), _redis_client, self, document_id
    )
