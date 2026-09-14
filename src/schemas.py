from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DocumentStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class DocumentCreate(BaseModel):
    user_id: int = Field(gt=0, description="User identifier, positive integer")
    title: str = Field(min_length=1, max_length=200, description="Document title")
    content: str = Field(
        min_length=1, max_length=50000, description="Document text content"
    )

    @field_validator("title", "content")
    @classmethod
    def must_not_be_blank(cls, v: str) -> str:
        """Reject blank or whitespace-only titles and contents."""
        if not v.strip():
            raise ValueError("must not be blank or whitespace-only")
        return v


class DocumentStatusResponse(BaseModel):
    document_id: str
    status: DocumentStatus
    summary: dict[str, Any] | None = None
    error: str | None = None


class DocumentResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str | None = Field(default=None, alias="_id")
    document_id: str
    user_id: int
    title: str
    content: str | None = None
    content_hash: str | None = None
    status: DocumentStatus
    summary: dict[str, Any] | None = None
    error: str | None = None
    retries: int = 0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class SubmitResponse(BaseModel):
    document_id: str
    status: DocumentStatus
    cached: bool = False
    deduped: bool = False


class PaginatedDocuments(BaseModel):
    total: int
    page: int
    page_size: int
    documents: list[DocumentResponse]


class HealthCheckResponse(BaseModel):
    status: Literal["ok", "degraded"]
    checks: dict[str, str]
