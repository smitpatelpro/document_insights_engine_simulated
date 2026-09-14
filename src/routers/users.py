from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query

from core.dependencies import get_documents_collection
from schemas import DocumentStatus, PaginatedDocuments
from services import user_service

router = APIRouter(prefix="/users", tags=["users"])


@router.get(
    "/{user_id}/documents",
    response_model=PaginatedDocuments,
    response_model_exclude_none=True,
)
async def list_user_documents(
    user_id: int = Path(gt=0, description="User identifier, positive integer"),
    page: int = Query(1, ge=1, le=1000, description="Page number, 1-indexed"),
    page_size: int = Query(10, ge=1, le=100, description="Items per page, max 100"),
    status: DocumentStatus | None = Query(None, description="Filter by status"),  # noqa: B008
    collection=Depends(get_documents_collection),  # noqa: B008
):
    """List a user's documents with pagination and status filter."""
    return await user_service.list_user_documents(
        collection, user_id, page, page_size, status
    )
