from __future__ import annotations

import psycopg

from apps.api.db import get_db_dsn


def market_needs_structural_refresh(market_id: str) -> bool:
    q1 = """
    SELECT 1
    FROM public.market_integrity_score_daily
    WHERE market_id = %s
    LIMIT 1;
    """

    q2 = """
    SELECT 1
    FROM public.market_launch_intelligence_daily
    WHERE market_id = %s
    LIMIT 1;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q1, (market_id,))
            integrity_exists = cur.fetchone() is not None

            cur.execute(q2, (market_id,))
            launch_exists = cur.fetchone() is not None

    return not (integrity_exists or launch_exists)