from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from bson import ObjectId

from schemas import DocumentStatus
from utils import hash_content
from workers import tasks


class RetrySentinel(Exception):
    pass


class FakeSyncCollection:
    """In-memory sync Mongo double for worker tests."""

    def __init__(self):
        self.docs: dict[str, dict] = {}

    def insert_queued(self, title="T", content="hello world", status="queued"):
        """Insert a queued doc and return its id and handle."""
        oid = ObjectId()
        doc = {
            "_id": oid,
            "user_id": 1,
            "title": title,
            "content": content,
            "status": status,
            "retries": 0,
        }
        self.docs[str(oid)] = doc
        return oid, doc

    def _match(self, doc, filt):
        """Check whether a doc satisfies a simplified Mongo filter."""
        for k, v in filt.items():
            if isinstance(v, dict):
                if "$in" in v:
                    allowed = v["$in"]
                    if doc.get(k) not in allowed and str(doc.get(k)) not in [
                        str(a) for a in allowed
                    ]:
                        # also compare enum members vs raw values
                        doc_val = doc.get(k)
                        doc_cmp = (
                            doc_val.value if hasattr(doc_val, "value") else doc_val
                        )
                        if doc_cmp not in [
                            a.value if hasattr(a, "value") else a for a in allowed
                        ]:
                            return False
                    continue
                if "$ne" in v:
                    if doc.get(k) == v["$ne"]:
                        return False
                    continue
            if doc.get(k) != v:
                return False
        return True

    def find_one(self, filt, projection=None):
        """Find the first doc matching the filter or None."""
        if "_id" in filt and len(filt) == 1:
            return self.docs.get(str(filt["_id"]))
        for d in self.docs.values():
            if self._match(d, filt):
                return d
        return None

    def find_one_and_update(self, filt, update):
        """Atomically claim the first matching doc and apply $set."""
        for d in self.docs.values():
            if self._match(d, filt):
                claimed = dict(d)
                d.update(update.get("$set", {}))
                return claimed
        return None

    def update_one(self, filt, update):
        """Apply a $set update to the first matching doc."""
        doc = None
        if "_id" in filt:
            doc = self.docs.get(str(filt["_id"]))
        else:
            for d in self.docs.values():
                if self._match(d, filt):
                    doc = d
                    break
        if doc is not None:
            doc.update(update.get("$set", {}))
        return MagicMock()

    def find(self, filt, projection=None):
        """Return all docs matching the filter."""
        return [d for d in self.docs.values() if self._match(d, filt)]


@pytest.fixture
def coll():
    """Provide a fresh fake sync collection per test."""
    return FakeSyncCollection()


@pytest.fixture
def task_mocks(coll):
    """Patch I/O + randomness around tasks.process_document.run."""
    mock_redis = MagicMock()
    with (
        patch.object(tasks, "get_collection", return_value=coll),
        patch.object(tasks, "_redis_client", mock_redis),
        patch.object(tasks.time, "sleep", return_value=None),
        patch.object(tasks.random, "uniform", return_value=0),
        patch.object(tasks.random, "random", return_value=1.0),
        patch.object(tasks.Settings, "JOB_FAILURE_RATE", 0.0),
        patch.object(tasks.Settings, "JOB_PERMANENT_FAILURE_ON_RETRY", False),
        patch.object(tasks.redis, "from_url", return_value=mock_redis),
    ):
        yield mock_redis


def run_task(document_id, retries=0, max_retries=3):
    """Run the Celery task inline with a controlled retry context."""
    task = tasks.process_document
    task.push_request(retries=retries)
    orig_max = task.max_retries
    task.max_retries = max_retries
    try:
        return task.run(document_id)
    finally:
        task.pop_request()
        task.max_retries = orig_max


def run_task_expect_retry(document_id, retries=0, max_retries=3):
    """Run the task expecting a retry and return the retry mock."""
    task = tasks.process_document
    task.push_request(retries=retries)
    orig_max = task.max_retries
    task.max_retries = max_retries
    with patch.object(
        task,
        "retry",
        MagicMock(side_effect=RetrySentinel("retry")),
    ) as mock_retry:
        try:
            with pytest.raises(RetrySentinel):
                task.run(document_id)
        finally:
            task.pop_request()
            task.max_retries = orig_max
        return mock_retry


def test_not_found(coll, task_mocks):
    """Verify missing document id returns not_found."""
    res = run_task(str(ObjectId()))
    assert res["status"] == "not_found"


def test_claim_success_completes(coll, task_mocks):
    """Verify queued doc is claimed, computed, cached, and completed."""
    oid, _ = coll.insert_queued(title="My Title", content="hello world")
    res = run_task(str(oid))
    assert res["status"] == DocumentStatus.COMPLETED
    doc = coll.docs[str(oid)]
    assert doc["status"] == DocumentStatus.COMPLETED
    assert doc["content_hash"] == hash_content("hello world")
    assert doc["summary"]["title"] == "My Title"
    assert doc["retries"] == 0
    task_mocks.setex.assert_called_once()
    key, ttl, value = task_mocks.setex.call_args.args
    assert key == f"cache:summary:{doc['content_hash']}"
    assert ttl == tasks.Settings.CACHE_TTL_SECONDS
    assert json.loads(value)["word_count"] == 2


def test_already_completed_skips(coll, task_mocks):
    """Verify already completed doc is skipped without recompute."""
    oid, _ = coll.insert_queued()
    coll.docs[str(oid)]["status"] = DocumentStatus.COMPLETED.value
    res = run_task(str(oid), retries=0)
    assert res.get("skipped") is True


def test_random_failure_resets_to_queued_and_retries(coll, task_mocks):
    """Verify transient failure requeues doc and schedules retry."""
    oid, _ = coll.insert_queued()
    with (
        patch.object(tasks.random, "random", return_value=0.0),
        patch.object(tasks.Settings, "JOB_FAILURE_RATE", 1.0),
    ):
        mock_retry = run_task_expect_retry(str(oid), retries=0, max_retries=3)
    assert coll.docs[str(oid)]["status"] == DocumentStatus.QUEUED
    mock_retry.assert_called_once()
    assert mock_retry.call_args.kwargs["countdown"] == 2**0


def test_retry_exhausted_marks_failed(coll, task_mocks):
    """Verify exhausted retries mark doc failed with error."""
    oid, _ = coll.insert_queued()
    with (
        patch.object(tasks.random, "random", return_value=0.0),
        patch.object(tasks.Settings, "JOB_FAILURE_RATE", 1.0),
    ):
        res = run_task(str(oid), retries=3, max_retries=3)
    assert res["status"] == DocumentStatus.FAILED
    doc = coll.docs[str(oid)]
    assert doc["status"] == DocumentStatus.FAILED
    assert "simulated" in doc["error"]
    assert doc["retries"] == 3


def test_redis_cache_failure_still_completes(coll, task_mocks):
    """Verify Redis cache outage still completes doc via fail-open."""
    import redis as redis_lib

    oid, _ = coll.insert_queued()
    task_mocks.setex.side_effect = redis_lib.RedisError("down")
    res = run_task(str(oid))
    assert res["status"] == DocumentStatus.COMPLETED
    assert coll.docs[str(oid)]["status"] == DocumentStatus.COMPLETED
