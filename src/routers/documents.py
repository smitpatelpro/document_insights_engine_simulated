from __future__ import annotations

import redis.asyncio as redis
from fastapi import APIRouter, Depends, Response, status

from core.dependencies import get_documents_collection, get_redis
from schemas import DocumentCreate, DocumentResponse, SubmitResponse
from services import document_service

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post("", response_model=SubmitResponse, status_code=status.HTTP_201_CREATED)
async def submit_document(
    payload: DocumentCreate,
    response: Response,
    redis_client: redis.Redis = Depends(get_redis),  # noqa: B008
    collection=Depends(get_documents_collection),  # noqa: B008
):
    """Submit a document and return 201, or 200 on dedup hit."""
    result = await document_service.submit_document(payload, redis_client, collection)
    if result.deduped:
        response.status_code = status.HTTP_200_OK
    return result


@router.get(
    "/{document_id}", response_model=DocumentResponse, response_model_exclude_none=True
)
async def get_document(
    document_id: str,
    collection=Depends(get_documents_collection),  # noqa: B008
):
    """Fetch one document by id or raise 404."""
    return await document_service.get_document_by_id(collection, document_id)
