from __future__ import annotations

import hashlib
from datetime import UTC, datetime


def hash_content(content: str) -> str:
    """Hash document content with SHA-256 for dedup and cache keys."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def now_utc() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(UTC)


def mock_summary_content_only(content: str) -> dict:
    """Build a content-only mock summary without title coupling."""
    words = content.split()
    return {
        "word_count": len(words),
        "character_count": len(content),
        "top_line": content.strip().splitlines()[0] if content.strip() else "",
        "abstract": " ".join(words[:40]),
    }


def build_summary(
    title: str, content: str, cached_content_summary: dict | None = None
) -> dict:
    """Merge a title with a content summary into the response payload."""
    base = cached_content_summary or mock_summary_content_only(content)
    return {"title": title, **base}
