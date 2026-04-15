from __future__ import annotations

from datetime import date
from typing import Optional

import psycopg

from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_social_intelligence_v5_quality_tightened"


def compute_market_social_intelligence_daily(
    day: Optional[date] = None,
    market_id: Optional[str] = None,
    limit_markets: int = 1000,
):
    dsn = get_db_dsn()

    delete_q = """
DELETE FROM public.market_social_intelligence_daily
WHERE day = COALESCE(%s::date, CURRENT_DATE)
  AND (%s::text IS NULL OR market_id = %s::text);
"""

    insert_q = """
WITH ranked AS (
    SELECT
        f.*,
        ROW_NUMBER() OVER (
            PARTITION BY f.market_id, f.day
            ORDER BY f.created_at DESC NULLS LAST, f.market_id
        ) AS rn
    FROM public.market_social_features_daily f
    WHERE f.day = COALESCE(%s::date, CURRENT_DATE)
      AND (%s::text IS NULL OR f.market_id = %s::text)
),

selected AS (
    SELECT
        r.market_id,
        r.day,
        COALESCE(r.demand_score, 0)::float AS base_demand_score,
        COALESCE(r.confidence_score, 0)::float AS confidence_score,
        COALESCE(r.sentiment_score, 0.5)::float AS sentiment_score,
        COALESCE(r.trend_velocity_1d, 0)::float AS trend_velocity_1d,
        COALESCE(r.mention_count, 0)::int AS mention_count,
        COALESCE(r.source_count, 0)::int AS source_count,
        COALESCE(r.hype_score, 0)::float AS hype_score
    FROM ranked r
    WHERE r.rn = 1
    ORDER BY r.demand_score DESC NULLS LAST, r.market_id
    LIMIT %s
),

scored AS (
    SELECT
        s.market_id,
        s.day,

        (s.base_demand_score / 100.0)::float AS attention_score,
        s.sentiment_score,
        s.base_demand_score AS raw_demand_score,
        s.trend_velocity_1d AS trend_velocity,
        s.mention_count,
        s.source_count,
        s.confidence_score,

        LEAST(100.0, s.base_demand_score)::float AS demand_strength_score,

        LEAST(
            100.0,
            (LN(1 + s.source_count) / LN(1 + 25)) * 100.0
        )::float AS demand_breadth_score,

        GREATEST(
            0.0,
            LEAST(
                100.0,
                (s.confidence_score * 0.7) +
                (CASE
                    WHEN s.source_count >= 8 THEN 20
                    WHEN s.source_count >= 4 THEN 12
                    WHEN s.source_count >= 2 THEN 6
                    ELSE 0
                END)
            )
        )::float AS demand_quality_score,

        GREATEST(
            0.0,
            LEAST(
                100.0,
                (CASE
                    WHEN s.trend_velocity_1d >= 3.0 AND s.source_count >= 5 THEN 85
                    WHEN s.trend_velocity_1d >= 2.0 AND s.source_count >= 4 THEN 70
                    WHEN s.trend_velocity_1d >= 1.0 AND s.source_count >= 3 THEN 55
                    WHEN s.mention_count >= 10 AND s.source_count >= 3 THEN 45
                    WHEN s.mention_count >= 5 THEN 30
                    ELSE 15
                END)
            )
        )::float AS narrative_coherence_score,

        GREATEST(
            0.0,
            LEAST(
                100.0,
                (CASE
                    WHEN s.source_count <= 1 AND s.mention_count >= 6 THEN 85
                    WHEN s.source_count <= 2 AND s.mention_count >= 6 THEN 75
                    WHEN s.hype_score >= 80 THEN 75
                    WHEN s.hype_score >= 60 THEN 60
                    WHEN s.confidence_score < 40 THEN 55
                    ELSE 15
                END)
            )
        )::float AS social_fragility_score
    FROM selected s
),

composed AS (
    SELECT
        sc.market_id,
        sc.day,
        sc.attention_score,
        sc.sentiment_score,
        sc.trend_velocity,
        sc.mention_count,
        sc.source_count,
        sc.confidence_score,

        sc.demand_strength_score,
        sc.demand_breadth_score,
        sc.demand_quality_score,
        sc.narrative_coherence_score,
        sc.social_fragility_score,

        GREATEST(
            0.0,
            LEAST(
                100.0,
                (
                    sc.demand_strength_score * 0.30 +
                    sc.demand_breadth_score * 0.20 +
                    sc.demand_quality_score * 0.25 +
                    sc.narrative_coherence_score * 0.25
                )
                - (sc.social_fragility_score * 0.35)
            )
        )::float AS demand_score
    FROM scored sc
),

final AS (
    SELECT
        c.market_id,
        c.day,
        c.attention_score,
        c.sentiment_score,
        c.demand_score,
        c.trend_velocity,
        c.mention_count,
        c.source_count,
        c.confidence_score,

        c.demand_strength_score,
        c.demand_breadth_score,
        c.demand_quality_score,
        c.narrative_coherence_score,
        c.social_fragility_score,

        CASE
            WHEN c.demand_score >= 85 THEN 'strong'
            WHEN c.demand_score >= 70 THEN 'established'
            WHEN c.demand_score >= 50 THEN 'building'
            WHEN c.demand_score >= 25 THEN 'limited'
            ELSE 'absent'
        END AS demand_state,

        CASE
            WHEN c.demand_score >= 85 THEN 'strong'
            WHEN c.demand_score >= 70 THEN 'established'
            WHEN c.demand_score >= 50 THEN 'building'
            WHEN c.demand_score >= 25 THEN 'limited'
            ELSE 'absent'
        END AS recommendation,

        CASE
            WHEN c.demand_score >= 85 THEN
                'external demand is strong and broadly supported across credible signals'
            WHEN c.demand_score >= 70 THEN
                'external demand is established across multiple credible signals'
            WHEN c.demand_score >= 50 THEN
                'external demand is building but not yet fully mature'
            WHEN c.demand_score >= 25 THEN
                'external demand is present but still limited'
            ELSE
                'external demand remains minimal or absent'
        END AS summary,

        ARRAY_REMOVE(ARRAY[
            CASE WHEN c.demand_score >= 85 THEN 'DEMAND_STRONG' END,
            CASE WHEN c.demand_score >= 70 AND c.demand_score < 85 THEN 'DEMAND_ESTABLISHED' END,
            CASE WHEN c.demand_score >= 50 AND c.demand_score < 70 THEN 'DEMAND_BUILDING' END,
            CASE WHEN c.demand_score >= 25 AND c.demand_score < 50 THEN 'DEMAND_LIMITED' END,
            CASE WHEN c.social_fragility_score >= 65 THEN 'SOCIAL_FRAGILITY_HIGH' END,
            CASE WHEN c.demand_breadth_score < 25 THEN 'NARROW_DEMAND_BASE' END,
            CASE WHEN c.demand_quality_score < 35 THEN 'LOW_DEMAND_QUALITY' END,
            CASE WHEN c.confidence_score < 35 THEN 'LOW_CONFIDENCE_SIGNAL' END
        ], NULL)::text[] AS flags
    FROM composed c
)

INSERT INTO public.market_social_intelligence_daily (
    market_id,
    day,
    attention_score,
    sentiment_score,
    demand_score,
    trend_velocity,
    mention_count,
    source_count,
    confidence_score,
    recommendation,
    summary,
    flags,
    demand_state,
    demand_strength_score,
    demand_breadth_score,
    demand_quality_score,
    narrative_coherence_score,
    social_fragility_score,
    engine_version,
    created_at,
    updated_at
)
SELECT
    market_id,
    day,
    attention_score,
    sentiment_score,
    demand_score,
    trend_velocity,
    mention_count,
    source_count,
    confidence_score,
    recommendation,
    summary,
    flags,
    demand_state,
    demand_strength_score,
    demand_breadth_score,
    demand_quality_score,
    narrative_coherence_score,
    social_fragility_score,
    %s,
    NOW(),
    NOW()
FROM final
ON CONFLICT (market_id, day)
DO UPDATE SET
    attention_score = EXCLUDED.attention_score,
    sentiment_score = EXCLUDED.sentiment_score,
    demand_score = EXCLUDED.demand_score,
    trend_velocity = EXCLUDED.trend_velocity,
    mention_count = EXCLUDED.mention_count,
    source_count = EXCLUDED.source_count,
    confidence_score = EXCLUDED.confidence_score,
    recommendation = EXCLUDED.recommendation,
    summary = EXCLUDED.summary,
    flags = EXCLUDED.flags,
    demand_state = EXCLUDED.demand_state,
    demand_strength_score = EXCLUDED.demand_strength_score,
    demand_breadth_score = EXCLUDED.demand_breadth_score,
    demand_quality_score = EXCLUDED.demand_quality_score,
    narrative_coherence_score = EXCLUDED.narrative_coherence_score,
    social_fragility_score = EXCLUDED.social_fragility_score,
    engine_version = EXCLUDED.engine_version,
    updated_at = NOW();
"""

    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(delete_q, (day, market_id, market_id))
            cur.execute(
                insert_q,
                (day, market_id, market_id, limit_markets, ENGINE_VERSION),
            )
        conn.commit()
    finally:
        conn.close()

    return {
        "status": "ok",
        "day": str(day) if day else None,
        "market_id": market_id,
        "limit_markets": limit_markets,
        "engine_version": ENGINE_VERSION,
    }