from __future__ import annotations

from datetime import date
from typing import Optional

import psycopg

from apps.api.db import get_db_dsn


def score_market_social_features_daily(
    day: Optional[date] = None,
    market_id: Optional[str] = None,
):
    dsn = get_db_dsn()

    dedupe_q = """
    DELETE FROM public.market_social_features_daily a
USING (
    SELECT
        market_id,
        day,
        MIN(ctid) AS keep_ctid
    FROM public.market_social_features_daily
    WHERE day = COALESCE(%s::date, CURRENT_DATE)
      AND (%s::text IS NULL OR market_id = %s::text)
    GROUP BY market_id, day
    HAVING COUNT(*) > 1
) d
WHERE a.market_id = d.market_id
  AND a.day = d.day
  AND a.ctid <> d.keep_ctid;
    """

    update_q = """
    UPDATE public.market_social_features_daily f
    SET
        hype_score = LEAST(
            100,
            (
                (COALESCE(f.source_concentration, 1) * 35)
                + ((1 - COALESCE(f.source_diversity, 0)) * 25)
                + (CASE WHEN COALESCE(f.attention_durability_score, 0) < 0.30 THEN 20 ELSE 0 END)
                + (CASE WHEN COALESCE(f.trend_velocity_1d, 0) > 1.5 THEN 20 ELSE 0 END)
            ) * 1.0
        ),
        confidence_score = LEAST(
            100,
            (
                (LEAST(COALESCE(f.mention_count, 0), 30) * 1.8)
                + (LEAST(COALESCE(f.source_count, 0), 10) * 6.0)
                + (COALESCE(f.attention_durability_score, 0) * 22.0)
                + ((1 - COALESCE(f.source_concentration, 1)) * 18.0)
            ) * 1.0
        ),
        demand_score = LEAST(
            100,
            (
                (LEAST(COALESCE(f.recency_weighted_volume, 0), 30) * 1.4)
                + (COALESCE(f.source_diversity, 0) * 18.0)
                + (COALESCE(f.attention_durability_score, 0) * 22.0)
                + (LEAST(COALESCE(f.engagement_per_mention, 0), 8) * 3.0)
                + (LEAST(COALESCE(f.trend_velocity_3d, 0), 2) * 8.0)
            ) * 1.0
        ),
        demand_state = CASE
            WHEN LEAST(
                100,
                (
                    (LEAST(COALESCE(f.recency_weighted_volume, 0), 30) * 1.4)
                    + (COALESCE(f.source_diversity, 0) * 18.0)
                    + (COALESCE(f.attention_durability_score, 0) * 22.0)
                    + (LEAST(COALESCE(f.engagement_per_mention, 0), 8) * 3.0)
                    + (LEAST(COALESCE(f.trend_velocity_3d, 0), 2) * 8.0)
                ) * 1.0
            ) >= 70 THEN 'strong'
            WHEN LEAST(
                100,
                (
                    (LEAST(COALESCE(f.recency_weighted_volume, 0), 30) * 1.4)
                    + (COALESCE(f.source_diversity, 0) * 18.0)
                    + (COALESCE(f.attention_durability_score, 0) * 22.0)
                    + (LEAST(COALESCE(f.engagement_per_mention, 0), 8) * 3.0)
                    + (LEAST(COALESCE(f.trend_velocity_3d, 0), 2) * 8.0)
                ) * 1.0
            ) >= 45 THEN 'moderate'
            WHEN LEAST(
                100,
                (
                    (LEAST(COALESCE(f.recency_weighted_volume, 0), 30) * 1.4)
                    + (COALESCE(f.source_diversity, 0) * 18.0)
                    + (COALESCE(f.attention_durability_score, 0) * 22.0)
                    + (LEAST(COALESCE(f.engagement_per_mention, 0), 8) * 3.0)
                    + (LEAST(COALESCE(f.trend_velocity_3d, 0), 2) * 8.0)
                ) * 1.0
            ) > 0 THEN 'weak'
            ELSE 'absent'
        END
    WHERE f.day = COALESCE(%s::date, CURRENT_DATE)
      AND (%s::text IS NULL OR f.market_id = %s::text);
    """

    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(dedupe_q, (day, market_id, market_id))
            cur.execute(update_q, (day, market_id, market_id))
        conn.commit()
    finally:
        conn.close()

    return {
        "status": "ok",
        "day": str(day) if day else None,
        "market_id": market_id,
    }