from __future__ import annotations

import logging

from fastapi import HTTPException
from pymongo.errors import PyMongoError

from schemas import DocumentResponse, DocumentStatus, PaginatedDocuments

logger = logging.getLogger(__name__)

MAX_SKIP = 10000


def build_query(user_id: int, status: DocumentStatus | None) -> dict:
    """Build the Mongo filter for a user's documents."""
    query: dict = {"user_id": user_id}
    if status is not None:
        query["status"] = status.value
    return query


def to_list_item(doc: dict) -> DocumentResponse:
    """Map a Mongo document to its list-item response shape."""
    return DocumentResponse.model_validate(
        {
            "document_id": str(doc.get("document_id", doc.get("_id"))),
            "_id": str(doc.get("_id")) if doc.get("_id") is not None else None,
            "user_id": doc.get("user_id"),
            "title": doc.get("title"),
            "content": doc.get("content"),
            "content_hash": doc.get("content_hash"),
            "status": doc.get("status"),
            "summary": doc.get("summary"),
            "error": doc.get("error"),
            "retries": doc.get("retries", 0),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
            "started_at": doc.get("started_at"),
            "completed_at": doc.get("completed_at"),
        }
    )


async def count_total(collection, query: dict) -> int:
    """Count matching documents, raising 500 on database errors."""
    try:
        return await collection.count_documents(query)
    except PyMongoError as e:
        logger.error("count_documents failed: %s", e)
        raise HTTPException(status_code=500, detail="Database error") from e


async def fetch_page(
    collection, query: dict, skip: int, page_size: int
) -> list[DocumentResponse]:
    """Fetch one sorted page of documents, raising 500 on database errors."""
    try:
        cursor = (
            collection.find(query).sort("created_at", -1).skip(skip).limit(page_size)
        )
        items: list[DocumentResponse] = []
        async for d in cursor:
            items.append(to_list_item(d))
        return items
    except PyMongoError as e:
        logger.error("find failed: %s", e)
        raise HTTPException(status_code=500, detail="Database error") from e


async def list_user_documents(
    collection,
    user_id: int,
    page: int,
    page_size: int,
    status: DocumentStatus | None,
) -> PaginatedDocuments:
    """List a user's documents with pagination and optional status filter."""
    skip = (page - 1) * page_size
    if skip > MAX_SKIP:
        raise HTTPException(
            status_code=400, detail="Page offset too large (max skip 10000)"
        )
    query = build_query(user_id, status)
    total = await count_total(collection, query)
    items = await fetch_page(collection, query, skip, page_size)
    return PaginatedDocuments(
        total=total, page=page, page_size=page_size, documents=items
    )
