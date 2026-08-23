"""Compatibility helpers for movie-bot MongoDB document schemas.

Older auto-filter bots commonly store the Telegram ``file_id`` as ``_id``.
Newer bots keep MongoDB's ObjectId in ``_id`` and store the Telegram value in
``file_id``.  The transfer pipeline accepts both without mutating the source.
"""
from __future__ import annotations

from typing import Any

SOURCE_PROJECTION = {
    "_id": 1,
    "file_id": 1,
    "file_unique_id": 1,
    "file_name": 1,
    "file_size": 1,
    "file_type": 1,
    "mime_type": 1,
    "caption": 1,
}


def get_file_id(document: dict[str, Any]) -> str | None:
    """Return a usable Telegram file ID from either supported schema."""
    value = document.get("file_id")
    if value is None and isinstance(document.get("_id"), str):
        value = document["_id"]
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def get_dedup_key(document: dict[str, Any]) -> str | None:
    """Prefer Telegram's stable unique ID, then fall back to ``file_id``."""
    unique_id = document.get("file_unique_id")
    if unique_id:
        return f"unique:{str(unique_id).strip().lower()}"
    file_id = get_file_id(document)
    return f"file:{file_id}" if file_id else None


def get_match_keys(document: dict[str, Any]) -> list[str]:
    """Return normalized keys used to match an existing target-channel file."""
    values = [
        get_dedup_key(document),
        get_file_id(document),
        document.get("file_unique_id"),
        document.get("file_name"),
        document.get("caption"),
    ]
    return [str(value).lower().strip() for value in values if value]


def with_resolved_file_id(document: dict[str, Any]) -> dict[str, Any] | None:
    """Copy a source document and expose its Telegram ID consistently."""
    file_id = get_file_id(document)
    if not file_id:
        return None
    resolved = dict(document)
    resolved["resolved_file_id"] = file_id
    resolved["dedup_key"] = get_dedup_key(document)
    return resolved
