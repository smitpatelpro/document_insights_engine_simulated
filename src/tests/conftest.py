from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from httpx import ASGITransport, AsyncClient

from core.dependencies import (
    get_documents_collection,
    get_mongo_client,
    get_redis,
)
from main import app


class FakeAsyncRedis:
    """In-memory async Redis double for API tests."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.ttls: dict[str, float] = {}

    async def get(self, key: str):
        """Return the stored value for key or None."""
        return self.store.get(key)

    async def set(
        self,
        key: str,
        value: str,
        nx: bool = False,
        px: int | None = None,
        ex: int | None = None,
    ):
        """Store value, honouring NX semantics for locks."""
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def delete(self, *keys: str):
        """Delete keys and return the removal count."""
        removed = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                removed += 1
        return removed

    async def setex(self, key: str, ttl: int, value: str):
        """Store value with TTL, ignoring expiry in the fake."""
        self.store[key] = value

    async def ping(self):
        """Report fake Redis as reachable."""
        return True

    async def aclose(self):
        """No-op close for the fake client."""
        return

    async def incr(self, key: str) -> int:
        """Increment the integer counter stored at key."""
        val = int(self.store.get(key, "0")) + 1
        self.store[key] = str(val)
        return val

    async def decr(self, key: str) -> int:
        """Decrement the counter, removing it at zero."""
        val = int(self.store.get(key, "0")) - 1
        if val <= 0:
            self.store.pop(key, None)
            return 0
        self.store[key] = str(val)
        return val

    async def expire(self, key: str, ttl: int) -> bool:
        """Record TTL for key, returning False when missing."""
        if key in self.store:
            self.ttls[key] = ttl
            return True
        return False

    @property
    def connection_pool(self):
        m = MagicMock()
        m.disconnect = AsyncMock()
        return m


class FakeCollection:
    """In-memory Mongo collection double for API tests."""

    def __init__(self):
        self.docs: dict[str, dict] = {}

    async def insert_one(self, doc: dict):
        """Insert doc with a new ObjectId and return its handle."""
        oid = ObjectId()
        doc["_id"] = oid
        self.docs[str(oid)] = doc
        result = MagicMock()
        result.inserted_id = oid
        return result

    async def find_one(self, filt: dict, projection: dict | None = None):
        """Find the first doc matching a simple equality filter."""
        if "_id" in filt:
            oid = filt["_id"]
            return self.docs.get(str(oid))
        for d in self.docs.values():
            match = True
            for k, v in filt.items():
                if isinstance(v, dict) and "$in" in v:
                    if d.get(k) not in v["$in"]:
                        match = False
                        break
                elif d.get(k) != v:
                    match = False
                    break
            if match:
                return d
        return None

    async def count_documents(self, filt: dict):
        """Count docs matching a simple equality filter."""
        cnt = 0
        for d in self.docs.values():
            match = True
            for k, v in filt.items():
                if isinstance(v, dict) and "$in" in v:
                    if d.get(k) not in v["$in"]:
                        match = False
                        break
                elif d.get(k) != v:
                    match = False
                    break
            if match:
                cnt += 1
        return cnt

    async def update_one(self, filt: dict, update: dict):
        """Apply a $set update to the first matching doc."""
        doc = None
        if "_id" in filt:
            doc = self.docs.get(str(filt["_id"]))
        if doc is not None:
            set_vals = update.get("$set", {})
            doc.update(set_vals)
            doc["updated_at"] = datetime.now(UTC)
        return MagicMock()

    def find(self, filt: dict):
        """Return matching docs newest-first via a fake cursor."""
        matched = []
        for d in self.docs.values():
            ok = True
            for k, v in filt.items():
                if d.get(k) != v:
                    ok = False
                    break
            if ok:
                matched.append(d)
        matched.sort(
            key=lambda x: x.get("created_at", datetime.min.replace(tzinfo=UTC)),
            reverse=True,
        )

        class FakeCursor:
            """Chainable async cursor over pre-sorted fake docs."""

            def __init__(self, docs):
                self.docs = docs
                self._skip = 0
                self._limit: int | None = None

            def sort(self, key, direction):
                """Ignore sort since docs are pre-sorted newest-first."""
                return self

            def skip(self, n):
                """Skip the first n docs for pagination."""
                self._skip = n
                return self

            def limit(self, n):
                """Limit the page to n docs."""
                self._limit = n
                return self

            def __aiter__(self):
                """Yield the paginated docs asynchronously."""
                docs = self.docs[self._skip :]
                if self._limit is not None:
                    docs = docs[: self._limit]

                async def gen():
                    for d in docs:
                        yield d

                return gen().__aiter__()

        return FakeCursor(matched)

    async def delete_many(self, filt: dict):
        """Clear all docs in the fake collection."""
        self.docs.clear()


@pytest.fixture
def fake_redis():
    """Provide a fresh fake Redis per test."""
    return FakeAsyncRedis()


@pytest.fixture
def fake_collection():
    """Provide a fresh fake Mongo collection per test."""
    return FakeCollection()


@pytest.fixture
def fake_mongo(fake_collection):
    """Provide a fake Mongo client with a passing ping."""

    async def fake_admin_command(cmd):
        return {"ok": 1}

    mongo = MagicMock()
    mongo.admin.command = fake_admin_command
    return mongo


@pytest.fixture
def client(fake_redis, fake_collection, fake_mongo):
    """Override FastAPI deps with fakes for the test client."""

    async def _get_redis():
        return fake_redis

    async def _get_collection():
        return fake_collection

    async def _get_mongo():
        return fake_mongo

    app.dependency_overrides[get_redis] = _get_redis
    app.dependency_overrides[get_documents_collection] = _get_collection
    app.dependency_overrides[get_mongo_client] = _get_mongo
    yield (fake_redis, fake_collection)
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def ac(client):
    """Provide an async HTTP client bound to the test app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
def mock_enqueue():
    """Patch Celery enqueue so submits do not need a broker."""
    with patch("workers.tasks.process_document") as mock_task:
        mock_task.delay = MagicMock()
        yield mock_task
