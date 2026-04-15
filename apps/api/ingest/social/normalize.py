from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def make_dedupe_hash(
    market_id: str,
    source: str,
    url: str | None,
    title: str | None,
    published_at: datetime | str | None,
) -> str:
    raw = f"{market_id}|{source}|{url or ''}|{title or ''}|{published_at or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None