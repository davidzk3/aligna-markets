from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

import psycopg

from apps.api.db import get_db_dsn

ENGINE_VERSION = "market_alignment_v9_integrity_source"


def compute_market_alignment_daily(
    day: Optional[date] = None,
    market_id: Optional[str] = None,
    limit_markets: int = 500,
    horizon_mode: Optional[str] = None,
) -> Dict[str, Any]:
    dsn = get_db_dsn()

    effective_horizon_mode = horizon_mode or ("same_day" if day else "mixed_latest")

    if effective_horizon_mode not in {"same_day", "mixed_latest"}:
        raise ValueError("invalid horizon_mode")

    delete_sql = """
        DELETE FROM public.market_alignment_daily
        WHERE (%(market_id)s::text IS NULL OR market_id = %(market_id)s::text)
          AND (%(mode)s = 'mixed_latest' OR day = %(day)s::date);
    """

    sql = """
    WITH base AS (
        SELECT
            m.market_id
        FROM public.markets m
        WHERE (%(market_id)s::text IS NULL OR m.market_id = %(market_id)s::text)
        ORDER BY m.market_id
        LIMIT %(limit)s
    ),

    structural_ranked AS (
        SELECT
            b.market_id,
            md.day AS structural_day,

            CASE
                WHEN md.market_quality_score IS NULL THEN NULL
                WHEN md.market_quality_score <= 1.0 THEN md.market_quality_score * 100.0
                ELSE md.market_quality_score
            END AS structural_score,

            CASE
                WHEN md.concentration_risk_score IS NULL THEN NULL
                WHEN md.concentration_risk_score <= 1.0 THEN md.concentration_risk_score * 100.0
                ELSE md.concentration_risk_score
            END AS structural_risk_score,

            CASE
                WHEN md.market_quality_score IS NULL THEN 'weak'
                WHEN (
                    CASE
                        WHEN md.market_quality_score <= 1.0 THEN md.market_quality_score * 100.0
                        ELSE md.market_quality_score
                    END
                ) >= 80 THEN 'strong'
                WHEN (
                    CASE
                        WHEN md.market_quality_score <= 1.0 THEN md.market_quality_score * 100.0
                        ELSE md.market_quality_score
                    END
                ) >= 60 THEN 'moderate'
                ELSE 'weak'
            END AS structural_state,

            ROW_NUMBER() OVER (
                PARTITION BY b.market_id
                ORDER BY md.day DESC NULLS LAST
            ) AS rn
        FROM base b
        LEFT JOIN public.market_integrity_score_daily md
          ON md.market_id = b.market_id
         AND (
            (%(mode)s = 'same_day' AND md.day = %(day)s::date)
            OR (%(mode)s = 'mixed_latest')
         )
    ),

    structural AS (
        SELECT
            market_id,
            structural_day,
            COALESCE(structural_score, 0) AS structural_score,
            COALESCE(structural_risk_score, 0) AS structural_risk_score,
            structural_state
        FROM structural_ranked
        WHERE rn = 1
    ),

    social_ranked AS (
        SELECT
            b.market_id,
            si.day AS social_day,
            COALESCE(si.demand_score, 0) AS social_score,
            COALESCE(
                si.demand_state,
                CASE
                    WHEN COALESCE(si.demand_score, 0) >= 80 THEN 'strong'
                    WHEN COALESCE(si.demand_score, 0) >= 65 THEN 'established'
                    WHEN COALESCE(si.demand_score, 0) >= 45 THEN 'building'
                    WHEN COALESCE(si.demand_score, 0) >= 20 THEN 'limited'
                    ELSE 'absent'
                END
            ) AS social_state,
            COALESCE(si.social_fragility_score, 0) AS social_fragility_score,
            ROW_NUMBER() OVER (
                PARTITION BY b.market_id
                ORDER BY si.day DESC NULLS LAST
            ) AS rn
        FROM base b
        LEFT JOIN public.market_social_intelligence_daily si
          ON si.market_id = b.market_id
         AND si.engine_version = 'market_social_intelligence_v5_quality_tightened'
         AND (
            (%(mode)s = 'same_day' AND si.day = %(day)s::date)
            OR (%(mode)s = 'mixed_latest')
         )
    ),

    social AS (
        SELECT
            market_id,
            social_day,
            social_score,
            social_state,
            social_fragility_score
        FROM social_ranked
        WHERE rn = 1
    ),

    joined AS (
        SELECT
            COALESCE(s.market_id, so.market_id) AS market_id,
            s.structural_day,
            so.social_day,
            COALESCE(s.structural_state, 'weak') AS structural_state,
            COALESCE(so.social_state, 'absent') AS social_state,
            COALESCE(s.structural_score, 0) AS structural_score,
            COALESCE(s.structural_risk_score, 0) AS structural_risk_score,
            COALESCE(so.social_score, 0) AS social_score,
            COALESCE(so.social_fragility_score, 0) AS social_fragility_score
        FROM structural s
        FULL OUTER JOIN social so
          ON s.market_id = so.market_id
    ),

    scored AS (
        SELECT
            j.*,

            CASE
                -- integrity overrides everything
                WHEN j.structural_risk_score >= 18
                     AND j.social_score >= 0.45
                    THEN 'conviction_mismatch'

                -- true confirmation: both strong AND aligned
                WHEN j.structural_score >= 0.80
                     AND j.social_score >= 0.75
                     AND ABS(j.social_score - j.structural_score) <= 0.10
                    THEN 'confirmed'

                -- demand clearly ahead of structure
                WHEN (j.social_score - j.structural_score) >= 0.10
                    THEN 'conviction_mismatch'

                -- structure ahead of demand
                WHEN (j.structural_score - j.social_score) >= 0.15
                    THEN 'structure_led'

                -- weak both sides
                WHEN j.structural_score < 0.45
                     AND j.social_score < 0.45
                    THEN 'weak'

                -- default fallback
                ELSE 'conviction_mismatch'
            END AS alignment_state,

            ((j.structural_score + j.social_score) / 2.0)::float AS alignment_score,

            (j.social_score - j.structural_score)::float AS gap,

            CASE
                WHEN j.structural_risk_score >= 18
                     AND j.social_state IN ('building', 'established', 'strong')
                    THEN 'integrity'
                WHEN j.structural_state IN ('strong', 'moderate')
                     AND j.social_state IN ('limited', 'absent')
                    THEN 'demand'
                WHEN j.social_state IN ('building', 'established', 'strong')
                    THEN 'structure'
                ELSE NULL
            END AS bottleneck_type,

            CASE
                WHEN j.structural_risk_score >= 18
                     AND j.social_state IN ('building', 'established', 'strong')
                    THEN 'external attention is present, but participation quality does not fully support conviction'
                WHEN j.structural_state = 'strong'
                     AND j.social_state IN ('established', 'strong')
                    THEN 'structure and demand reinforce each other'
                WHEN j.structural_state IN ('strong', 'moderate')
                     AND j.social_state IN ('limited', 'absent')
                    THEN 'structure appears healthier than current demand activation'
                WHEN j.social_state IN ('building', 'established', 'strong')
                    THEN 'demand is present, but participation quality does not yet confirm conviction'
                ELSE 'attention and participation both remain limited'
            END AS summary,

            ARRAY_REMOVE(ARRAY[
                CASE
                    WHEN j.structural_risk_score >= 18
                         AND j.social_state IN ('building', 'established', 'strong')
                        THEN 'STRUCTURAL_FRAGILITY'
                END,
                CASE
                    WHEN j.structural_state IN ('strong', 'moderate')
                         AND j.social_state IN ('limited', 'absent')
                        THEN 'DEMAND_LAGGING_STRUCTURE'
                END,
                CASE
                    WHEN j.social_state IN ('building', 'established', 'strong')
                         AND NOT (
                             j.structural_state = 'strong'
                             AND j.social_state IN ('established', 'strong')
                         )
                        THEN 'DEMAND_AHEAD_OF_STRUCTURE'
                END,
                CASE
                    WHEN ABS(j.social_score - j.structural_score) > 30
                        THEN 'LARGE_ALIGNMENT_GAP'
                END,
                CASE
                    WHEN j.structural_day IS NOT NULL
                         AND j.social_day IS NOT NULL
                         AND j.structural_day <> j.social_day
                        THEN 'mixed_horizon'
                END
            ], NULL)::text[] AS flags
        FROM joined j
    )

    INSERT INTO public.market_alignment_daily (
        market_id,
        day,
        structural_day,
        social_day,
        horizon_mode,
        structural_state,
        social_state,
        alignment_state,
        alignment_score,
        attention_vs_structure_gap,
        bottleneck_type,
        summary,
        flags,
        engine_version,
        updated_at
    )
    SELECT
        market_id,
        COALESCE(structural_day, social_day, %(day)s::date, CURRENT_DATE),
        structural_day,
        social_day,
        %(mode)s,
        structural_state,
        social_state,
        alignment_state,
        alignment_score,
        gap,
        bottleneck_type,
        summary,
        flags,
        %(engine)s,
        NOW()
    FROM scored
    ON CONFLICT (market_id, day)
    DO UPDATE SET
        structural_day = EXCLUDED.structural_day,
        social_day = EXCLUDED.social_day,
        horizon_mode = EXCLUDED.horizon_mode,
        structural_state = EXCLUDED.structural_state,
        social_state = EXCLUDED.social_state,
        alignment_state = EXCLUDED.alignment_state,
        alignment_score = EXCLUDED.alignment_score,
        attention_vs_structure_gap = EXCLUDED.attention_vs_structure_gap,
        bottleneck_type = EXCLUDED.bottleneck_type,
        summary = EXCLUDED.summary,
        flags = EXCLUDED.flags,
        engine_version = EXCLUDED.engine_version,
        updated_at = NOW();
    """

    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                delete_sql,
                {
                    "market_id": market_id,
                    "mode": effective_horizon_mode,
                    "day": day,
                },
            )

            cur.execute(
                sql,
                {
                    "market_id": market_id,
                    "limit": limit_markets,
                    "mode": effective_horizon_mode,
                    "engine": ENGINE_VERSION,
                    "day": day,
                },
            )

        conn.commit()
    finally:
        conn.close()

    return {
        "status": "ok",
        "engine_version": ENGINE_VERSION,
        "horizon_mode": effective_horizon_mode,
        "market_id": market_id,
        "day": str(day) if day else None,
    }