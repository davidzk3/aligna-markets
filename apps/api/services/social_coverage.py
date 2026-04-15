from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg

from apps.api.db import get_db_dsn


def get_latest_social_row(market_id: str):
    q = """
    SELECT
        market_id,
        day,
        demand_score,
        confidence_score,
        created_at,
        updated_at,
        engine_version
    FROM public.market_social_intelligence_daily
    WHERE market_id = %s
      AND engine_version = 'market_social_intelligence_v3_real'
    ORDER BY day DESC
    LIMIT 1;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            return cur.fetchone()


def market_needs_social_refresh(
    market_id: str,
    max_age_hours: int = 12,
) -> bool:
    row = get_latest_social_row(market_id)
    if not row:
        return True

    updated_at = row[5]
    if updated_at is None:
        return True

    now = datetime.now(timezone.utc)
    age = now - updated_at
    return age > timedelta(hours=max_age_hours)