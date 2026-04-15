from __future__ import annotations

from datetime import date, datetime, timezone
import json
from typing import Any, Dict, Optional

import psycopg

from apps.api.db import get_db_dsn
from apps.api.ingest.social.news_ingest import fetch_news_for_queries
from apps.api.ingest.social.reddit_ingest import fetch_reddit_for_queries
from apps.api.ingest.social.topic_mapper import map_market_topic, topic_map_to_dict
from apps.api.services.social_feature_aggregation import compute_market_social_features_daily
from apps.api.services.social_scoring import score_market_social_features_daily
from apps.api.services.market_social_intelligence import compute_market_social_intelligence_daily


def _ensure_market_social_raw_table(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.market_social_raw (
            market_id text NOT NULL,
            source text NOT NULL,
            source_type text NULL,
            query text NULL,
            external_id text NOT NULL,
            author text NULL,
            source_name text NULL,
            subreddit text NULL,
            title text NULL,
            body text NULL,
            url text NULL,
            language text NULL,
            country text NULL,
            published_at timestamptz NULL,
            ingested_at timestamptz NOT NULL DEFAULT now(),
            engagement_score double precision NULL,
            score double precision NULL,
            comment_count integer NULL,
            upvote_ratio double precision NULL,
            metadata_json jsonb NULL,
            dedupe_hash text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (dedupe_hash)
        );
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_social_raw_market_day
        ON public.market_social_raw (market_id, published_at DESC);
        """
    )


def _load_market_context(cur, market_id: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            market_id,
            title,
            category,
            url,
            protocol,
            chain
        FROM public.markets
        WHERE market_id = %s
        LIMIT 1;
        """,
        (market_id,),
    )
    row = cur.fetchone()
    if not row:
        return None

    return {
        "market_id": row[0],
        "title": row[1],
        "category": row[2],
        "url": row[3],
        "protocol": row[4],
        "chain": row[5],
    }


def _insert_social_rows(cur, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    inserted = 0

    insert_raw_q = """
    INSERT INTO public.market_social_raw (
        market_id,
        source,
        source_type,
        query,
        external_id,
        author,
        source_name,
        subreddit,
        title,
        body,
        url,
        language,
        country,
        published_at,
        ingested_at,
        engagement_score,
        score,
        comment_count,
        upvote_ratio,
        metadata_json,
        dedupe_hash
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s::jsonb, %s
    )
    ON CONFLICT (dedupe_hash) DO NOTHING;
    """

    for row in rows:
        cur.execute(
            insert_raw_q,
            (
                row.get("market_id"),
                row.get("source"),
                row.get("source_type"),
                row.get("query"),
                row.get("external_id"),
                row.get("author"),
                row.get("source_name"),
                row.get("subreddit"),
                row.get("title"),
                row.get("body"),
                row.get("url"),
                row.get("language"),
                row.get("country"),
                row.get("published_at"),
                row.get("engagement_score"),
                row.get("score"),
                row.get("comment_count"),
                row.get("upvote_ratio"),
                json.dumps(row.get("metadata_json") or {}),
                row.get("dedupe_hash"),
            ),
        )
        inserted += cur.rowcount or 0

    return inserted


def ingest_social_for_market(
    market_id: str,
    day: Optional[date] = None,
    include_news: bool = True,
    include_reddit: bool = True,
) -> Dict[str, Any]:
    dsn = get_db_dsn()
    started_at = datetime.now(timezone.utc)

    insert_topic_q = """
    INSERT INTO public.market_topic_map (
        market_id,
        as_of_date,
        title,
        category,
        primary_topic,
        topic_bundle_json,
        query_bundle_json,
        mapping_confidence,
        created_at
    )
    VALUES (%s, COALESCE(%s::date, CURRENT_DATE), %s, %s, %s, %s::jsonb, %s::jsonb, %s, NOW())
    ON CONFLICT (market_id, as_of_date)
    DO UPDATE SET
        title = EXCLUDED.title,
        category = EXCLUDED.category,
        primary_topic = EXCLUDED.primary_topic,
        topic_bundle_json = EXCLUDED.topic_bundle_json,
        query_bundle_json = EXCLUDED.query_bundle_json,
        mapping_confidence = EXCLUDED.mapping_confidence;
    """

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            _ensure_market_social_raw_table(cur)

            market = _load_market_context(cur, market_id)
            if not market:
                return {
                    "status": "error",
                    "market_id": market_id,
                    "error": "market_not_found",
                }

            topic_map = map_market_topic(
                market_id=market_id,
                title=market["title"],
                category=market["category"],
            )

            cur.execute(
                insert_topic_q,
                (
                    market_id,
                    day,
                    market["title"],
                    topic_map.category,
                    topic_map.primary_topic,
                    json.dumps(topic_map_to_dict(topic_map)),
                    json.dumps(topic_map.queries),
                    topic_map.mapping_confidence,
                ),
            )

            news_rows: list[dict[str, Any]] = []
            reddit_rows: list[dict[str, Any]] = []

            if include_news:
                news_rows = fetch_news_for_queries(
                    market_id=market_id,
                    queries=topic_map.queries.get("news", []),
                    category=topic_map.category,
                    entities=topic_map.entities,
                )

            if include_reddit:
                reddit_rows = fetch_reddit_for_queries(
                    market_id=market_id,
                    queries=topic_map.queries.get("reddit", []),
                    category=topic_map.category,
                    entities=topic_map.entities,
                )

            news_inserted = _insert_social_rows(cur, news_rows)
            reddit_inserted = _insert_social_rows(cur, reddit_rows)

        conn.commit()

    finished_at = datetime.now(timezone.utc)

    return {
        "status": "ok",
        "market_id": market_id,
        "day": str(day) if day else None,
        "title": market["title"],
        "category": topic_map.category,
        "primary_topic": topic_map.primary_topic,
        "mapping_confidence": topic_map.mapping_confidence,
        "queries": topic_map.queries,
        "news_rows_fetched": len(news_rows),
        "reddit_rows_fetched": len(reddit_rows),
        "news_rows_inserted": news_inserted,
        "reddit_rows_inserted": reddit_inserted,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "elapsed_seconds": (finished_at - started_at).total_seconds(),
    }


def ingest_social_for_open_markets(
    day: Optional[date] = None,
    limit_markets: int = 25,
):
    dsn = get_db_dsn()

    select_q = """
    SELECT
        market_id,
        title,
        category
    FROM public.markets
    WHERE COALESCE(closed, false) = false
      AND url IS NOT NULL
    ORDER BY updated_at DESC NULLS LAST, inserted_at DESC NULLS LAST
    LIMIT %s;
    """

    insert_topic_q = """
    INSERT INTO public.market_topic_map (
        market_id,
        as_of_date,
        title,
        category,
        primary_topic,
        topic_bundle_json,
        query_bundle_json,
        mapping_confidence,
        created_at
    )
    VALUES (%s, COALESCE(%s::date, CURRENT_DATE), %s, %s, %s, %s::jsonb, %s::jsonb, %s, NOW())
    ON CONFLICT (market_id, as_of_date)
    DO UPDATE SET
        title = EXCLUDED.title,
        category = EXCLUDED.category,
        primary_topic = EXCLUDED.primary_topic,
        topic_bundle_json = EXCLUDED.topic_bundle_json,
        query_bundle_json = EXCLUDED.query_bundle_json,
        mapping_confidence = EXCLUDED.mapping_confidence;
    """

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            _ensure_market_social_raw_table(cur)

            cur.execute(select_q, (limit_markets,))
            markets = cur.fetchall()

            for market_id, title, category in markets:
                topic_map = map_market_topic(
                    market_id=market_id,
                    title=title,
                    category=category,
                )

                cur.execute(
                    insert_topic_q,
                    (
                        market_id,
                        day,
                        title,
                        topic_map.category,
                        topic_map.primary_topic,
                        json.dumps(topic_map_to_dict(topic_map)),
                        json.dumps(topic_map.queries),
                        topic_map.mapping_confidence,
                    ),
                )

                news_rows = fetch_news_for_queries(
                    market_id=market_id,
                    queries=topic_map.queries.get("news", []),
                    category=topic_map.category,
                    entities=topic_map.entities,
                )

                reddit_rows = fetch_reddit_for_queries(
                    market_id=market_id,
                    queries=topic_map.queries.get("reddit", []),
                    category=topic_map.category,
                    entities=topic_map.entities,
                )

                _insert_social_rows(cur, news_rows)
                _insert_social_rows(cur, reddit_rows)

        conn.commit()

    compute_market_social_features_daily(day=day)
    score_market_social_features_daily(day=day)
    compute_market_social_intelligence_daily(day=day, limit_markets=limit_markets)

    return {
        "status": "ok",
        "day": str(day) if day else None,
        "limit_markets": limit_markets,
    }