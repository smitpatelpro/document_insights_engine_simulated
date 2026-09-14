from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from bson import ObjectId

from schemas import DocumentStatus
from utils import hash_content


@pytest.mark.asyncio
async def test_submit_document_201(ac, mock_enqueue):
    """Verify valid submit returns 201 queued and enqueues worker."""
    resp = await ac.post(
        "/documents",
        json={"user_id": 1, "title": "Hello", "content": "some content here"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "document_id" in data
    assert data["status"] == DocumentStatus.QUEUED.value
    assert data["cached"] is False
    mock_enqueue.delay.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"user_id": 0, "title": "Hello", "content": "some content here"},
        {"user_id": 1, "title": "", "content": "some content here"},
        {"user_id": 1, "title": "   ", "content": "some content here"},
        {"user_id": 1, "title": "Hello", "content": "   \n\t  "},
    ],
)
async def test_submit_validation_422(ac, payload):
    """Verify invalid payloads are rejected with 422."""
    resp = await ac.post("/documents", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_rate_limit_429(ac, mock_enqueue):
    """Verify fourth active doc for same user returns 429."""
    for i in range(3):
        r = await ac.post(
            "/documents",
            json={
                "user_id": 42,
                "title": f"t{i}",
                "content": f"content {i} unique {i}",
            },
        )
        assert r.status_code == 201
    r = await ac.post(
        "/documents",
        json={"user_id": 42, "title": "t3", "content": "content 3 unique 3"},
    )
    assert r.status_code == 429
    assert (
        "max 3" in r.json()["detail"].lower() or "active" in r.json()["detail"].lower()
    )


@pytest.mark.asyncio
async def test_cache_hit_returns_completed(ac, mock_enqueue, fake_redis):
    """Verify cached content completes immediately without enqueue."""
    content = "identical content for caching"
    h = hash_content(content)
    cached = {
        "word_count": 4,
        "character_count": len(content),
        "top_line": content,
        "abstract": content,
    }
    fake_redis.store[f"cache:summary:{h}"] = json.dumps(cached)

    resp = await ac.post(
        "/documents",
        json={"user_id": 99, "title": "New Title", "content": content},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == DocumentStatus.COMPLETED.value
    assert data["cached"] is True
    mock_enqueue.delay.assert_not_called()

    doc_id = data["document_id"]
    resp2 = await ac.get(f"/documents/{doc_id}")
    assert resp2.status_code == 200
    assert resp2.json()["status"] == DocumentStatus.COMPLETED.value
    assert resp2.json()["summary"]["title"] == "New Title"


@pytest.mark.asyncio
async def test_get_document_completed(ac, fake_collection):
    """Verify fetching a completed doc returns summary, missing returns 404."""
    res = await fake_collection.insert_one(
        {
            "user_id": 9,
            "title": "shape",
            "content": "shape content",
            "content_hash": "abc",
            "status": DocumentStatus.COMPLETED.value,
            "summary": {"title": "shape", "word_count": 2},
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )
    resp = await ac.get(f"/documents/{res.inserted_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == DocumentStatus.COMPLETED.value
    assert "summary" in data
    assert "error" not in data
    resp = await ac.get(f"/documents/{ObjectId()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_user_documents_pagination(ac, fake_collection):
    """Verify user listing paginates, isolates users, and filters by status."""
    for i in range(5):
        await fake_collection.insert_one(
            {
                "user_id": 7,
                "title": f"t{i}",
                "content": f"unique content {i} {i * 100}",
                "status": DocumentStatus.QUEUED.value,
                "created_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
            }
        )
    await fake_collection.insert_one(
        {
            "user_id": 8,
            "title": "other",
            "content": "other user content",
            "status": DocumentStatus.QUEUED.value,
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )
    resp = await ac.get("/users/7/documents?page=1&page_size=2")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 5
    assert data["page"] == 1
    assert len(data["documents"]) == 2
    resp = await ac.get("/users/7/documents?page=2&page_size=2")
    assert len(resp.json()["documents"]) == 2
    resp = await ac.get("/users/7/documents?status=" + DocumentStatus.QUEUED.value)
    assert resp.json()["total"] == 5


@pytest.mark.asyncio
async def test_health_endpoint(ac):
    """Verify health endpoint reports ok or degraded."""
    with patch(
        "workers.celery_app.check_celery_broker", return_value="ok (1 worker(s))"
    ):
        resp = await ac.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] in ("ok", "degraded")


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_user_id", [0, -1])
async def test_invalid_user_id_path_422(ac, bad_user_id):
    """Verify non-positive user ids in path return 422."""
    resp = await ac.get(f"/users/{bad_user_id}/documents")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_pagination_overflow_guards(ac):
    """Verify oversized page offsets are rejected with 422 or 400."""
    resp = await ac.get("/users/7/documents?page=1001&page_size=10")
    assert resp.status_code == 422
    resp = await ac.get("/users/7/documents?page=500&page_size=100")
    assert resp.status_code == 400
    assert "too large" in resp.json()["detail"].lower()
