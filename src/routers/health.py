from __future__ import annotations

import redis.asyncio as redis
from fastapi import APIRouter, Depends, Response, status
from pymongo import AsyncMongoClient

from core.dependencies import get_documents_collection, get_mongo_client, get_redis
from schemas import HealthCheckResponse
from services import health_service

router = APIRouter(tags=["ops"])


@router.get("/health", response_model=HealthCheckResponse)
async def health_check(
    response: Response,
    redis_client: redis.Redis = Depends(get_redis),  # noqa: B008
    mongo_client: AsyncMongoClient = Depends(get_mongo_client),  # noqa: B008
    documents_collection=Depends(get_documents_collection),  # noqa: B008
):
    """Check dependencies and return ok or 503 degraded."""
    result = await health_service.run_health_checks(
        redis_client, mongo_client, documents_collection
    )
    response.status_code = (
        status.HTTP_200_OK
        if result.status == "ok"
        else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    return result
