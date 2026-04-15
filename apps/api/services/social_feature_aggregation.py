from __future__ import annotations

from datetime import date
from typing import Optional

import psycopg

from apps.api.db import get_db_dsn


def compute_market_social_features_daily(
    day: Optional[date] = None,
    market_id: Optional[str] = None,
    lookback_days: int = 7,
):
    dsn = get_db_dsn()

    delete_q = """
    DELETE FROM public.market_social_features_daily
    WHERE day = COALESCE(%s::date, CURRENT_DATE)
      AND (%s::text IS NULL OR market_id = %s::text);
    """

    insert_q = """
    WITH params AS (
        SELECT
            COALESCE(%s::date, CURRENT_DATE) AS target_day,
            %s::text AS market_id_filter,
            %s::int AS lookback_days
    ),

    raw_base AS (
        SELECT
            r.market_id,
            r.source,
            COALESCE(NULLIF(r.author, ''), NULLIF(r.source_name, ''), 'unknown') AS author_key,
            COALESCE(NULLIF(r.source_name, ''), 'unknown') AS source_name,
            COALESCE(r.engagement_score, 0)::float AS engagement_score,
            r.published_at::date AS published_day
        FROM public.market_social_raw r
        CROSS JOIN params p
        WHERE r.published_at IS NOT NULL
          AND r.published_at::date BETWEEN (p.target_day - (p.lookback_days - 1)) AND p.target_day
          AND (p.market_id_filter IS NULL OR r.market_id = p.market_id_filter)
    ),

    weighted_base AS (
        SELECT
            b.*,

            -- 🔑 SOURCE WEIGHTING (core improvement)
            CASE
                WHEN b.source = 'news' THEN 1.0
                WHEN b.source = 'twitter' THEN 0.8
                WHEN b.source = 'reddit' THEN 0.6
                ELSE 0.5
            END AS source_weight,

            -- weighted engagement
            b.engagement_score *
            CASE
                WHEN b.source = 'news' THEN 1.2
                WHEN b.source = 'twitter' THEN 1.0
                WHEN b.source = 'reddit' THEN 0.7
                ELSE 0.6
            END AS weighted_engagement

        FROM raw_base b
    ),

    market_rollup AS (
        SELECT
            w.market_id,

            COUNT(*)::int AS mention_count,
            COUNT(DISTINCT w.source_name)::int AS source_count,
            COUNT(DISTINCT w.author_key)::int AS unique_author_count,

            SUM(w.weighted_engagement)::float AS engagement_total,

            SUM(w.source_weight)::float AS weighted_mentions,

            COUNT(*) FILTER (WHERE w.source = 'reddit')::int AS reddit_post_count,
            COUNT(*) FILTER (WHERE w.source = 'news')::int AS news_article_count,

            COUNT(DISTINCT w.published_day)::int AS active_days

        FROM weighted_base w
        GROUP BY w.market_id
    ),

    source_share AS (
        SELECT
            x.market_id,
            MAX(x.source_mentions::float / NULLIF(t.total_mentions, 0))::float AS source_concentration
        FROM (
            SELECT
                market_id,
                source_name,
                COUNT(*)::int AS source_mentions
            FROM raw_base
            GROUP BY market_id, source_name
        ) x
        JOIN (
            SELECT
                market_id,
                COUNT(*)::int AS total_mentions
            FROM raw_base
            GROUP BY market_id
        ) t
          ON t.market_id = x.market_id
        GROUP BY x.market_id
    ),

    daily_mentions AS (
        SELECT
            w.market_id,
            w.published_day,
            SUM(w.source_weight)::float AS mentions
        FROM weighted_base w
        GROUP BY w.market_id, w.published_day
    ),

    trend_calc AS (
        SELECT
            p.target_day AS day,
            dm.market_id,
            COALESCE(MAX(CASE WHEN dm.published_day = p.target_day THEN dm.mentions END), 0)::float
                AS mentions_today,
            COALESCE(AVG(CASE WHEN dm.published_day BETWEEN (p.target_day - 2) AND p.target_day THEN dm.mentions END), 0)::float
                AS avg_3d,
            COALESCE(AVG(CASE WHEN dm.published_day BETWEEN (p.target_day - 6) AND p.target_day THEN dm.mentions END), 0)::float
                AS avg_7d
        FROM daily_mentions dm
        CROSS JOIN params p
        GROUP BY p.target_day, dm.market_id
    ),

    recency_calc AS (
        SELECT
            w.market_id,
            SUM(
                w.source_weight *
                CASE
                    WHEN w.published_day = p.target_day THEN 1.0
                    WHEN w.published_day = p.target_day - 1 THEN 0.85
                    WHEN w.published_day = p.target_day - 2 THEN 0.70
                    WHEN w.published_day = p.target_day - 3 THEN 0.55
                    ELSE 0.35
                END
            )::float AS recency_weighted_volume
        FROM weighted_base w
        CROSS JOIN params p
        GROUP BY w.market_id
    ),

    assembled AS (
        SELECT
            mr.market_id,
            p.target_day AS day,

            mr.mention_count,
            mr.source_count,
            mr.unique_author_count,

            LEAST(1.0, mr.source_count::float / NULLIF(mr.mention_count, 0))::float AS source_diversity,

            mr.engagement_total,
            (mr.engagement_total / NULLIF(mr.weighted_mentions, 0))::float AS engagement_per_mention,

            mr.reddit_post_count,
            mr.news_article_count,

            rc.recency_weighted_volume,

            CASE
                WHEN tc.avg_7d > 0 THEN (tc.mentions_today / tc.avg_7d)::float
                ELSE 0
            END AS trend_velocity_1d,

            CASE
                WHEN tc.avg_7d > 0 THEN (tc.avg_3d / tc.avg_7d)::float
                ELSE 0
            END AS trend_velocity_3d,

            (mr.active_days::float / NULLIF(p.lookback_days, 0))::float AS attention_durability_score,

            COALESCE(ss.source_concentration, 1.0)::float AS source_concentration,

            0.5::float AS sentiment_score,
            0.0::float AS sentiment_dispersion,

            -- 🔥 NEW: hype detection
            CASE
                WHEN mr.source_count <= 2 AND mr.mention_count >= 8 THEN 80
                WHEN mr.source_count <= 3 AND mr.mention_count >= 10 THEN 65
                WHEN tc.avg_3d > 0 AND tc.mentions_today / tc.avg_3d > 2 THEN 70
                ELSE 20
            END::float AS hype_score,

            0.0::float AS confidence_score,
            0.0::float AS demand_score,
            'unknown'::text AS demand_state,

            jsonb_build_object(
                'active_days', mr.active_days,
                'total_mentions', mr.mention_count,
                'weighted_mentions', mr.weighted_mentions
            ) AS features_json

        FROM market_rollup mr
        CROSS JOIN params p
        LEFT JOIN source_share ss ON ss.market_id = mr.market_id
        LEFT JOIN trend_calc tc ON tc.market_id = mr.market_id
        LEFT JOIN recency_calc rc ON rc.market_id = mr.market_id
    ),

    deduped AS (
        SELECT *
        FROM (
            SELECT
                a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY a.market_id, a.day
                    ORDER BY a.market_id
                ) AS rn
            FROM assembled a
        ) z
        WHERE z.rn = 1
    )

    INSERT INTO public.market_social_features_daily (
        market_id,
        day,
        mention_count,
        source_count,
        unique_author_count,
        source_diversity,
        engagement_total,
        engagement_per_mention,
        reddit_post_count,
        news_article_count,
        recency_weighted_volume,
        trend_velocity_1d,
        trend_velocity_3d,
        attention_durability_score,
        source_concentration,
        sentiment_score,
        sentiment_dispersion,
        hype_score,
        confidence_score,
        demand_score,
        demand_state,
        features_json,
        created_at
    )
    SELECT
        market_id,
        day,
        mention_count,
        source_count,
        unique_author_count,
        source_diversity,
        engagement_total,
        engagement_per_mention,
        reddit_post_count,
        news_article_count,
        recency_weighted_volume,
        trend_velocity_1d,
        trend_velocity_3d,
        attention_durability_score,
        source_concentration,
        sentiment_score,
        sentiment_dispersion,
        hype_score,
        confidence_score,
        demand_score,
        demand_state,
        features_json,
        NOW()
    FROM deduped;
    """

    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(delete_q, (day, market_id, market_id))
            cur.execute(insert_q, (day, market_id, lookback_days))
        conn.commit()
    finally:
        conn.close()

    return {
        "status": "ok",
        "day": str(day) if day else None,
        "market_id": market_id,
        "lookback_days": lookback_days,
    }