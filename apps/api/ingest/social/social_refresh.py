from __future__ import annotations

from datetime import datetime, timezone

import psycopg

from apps.api.db import get_db_dsn
from apps.api.ingest.social.topic_mapper import map_market_topic
from apps.api.ingest.social.news_ingest import fetch_news_for_queries
from apps.api.services.social_feature_aggregation import compute_market_social_features_daily
from apps.api.services.social_scoring import score_market_social_features_daily
from apps.api.services.market_social_intelligence import compute_market_social_intelligence_daily
from apps.api.services.market_alignment_intelligence import compute_market_alignment_daily


def get_market_title_and_category(market_id: str):
    q = """
    SELECT market_id, title, category
    FROM public.markets
    WHERE market_id = %s
    LIMIT 1;
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            return cur.fetchone()


def insert_social_raw_rows(rows: list[dict]):
    if not rows:
        return 0

    q = """
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
        engagement_score,
        score,
        comment_count,
        upvote_ratio,
        metadata_json,
        dedupe_hash,
        created_at
    )
    VALUES (
        %(market_id)s,
        %(source)s,
        %(source_type)s,
        %(query)s,
        %(external_id)s,
        %(author)s,
        %(source_name)s,
        %(subreddit)s,
        %(title)s,
        %(body)s,
        %(url)s,
        %(language)s,
        %(country)s,
        %(published_at)s,
        %(engagement_score)s,
        %(score)s,
        %(comment_count)s,
        %(upvote_ratio)s,
        %(metadata_json)s,
        %(dedupe_hash)s,
        NOW()
    )
    ON CONFLICT (dedupe_hash)
    DO NOTHING;
    """

    normalized = []
    for row in rows:
        normalized.append({
            "market_id": row.get("market_id"),
            "source": row.get("source"),
            "source_type": row.get("source_type"),
            "query": row.get("query"),
            "external_id": row.get("external_id"),
            "author": row.get("author"),
            "source_name": row.get("source_name"),
            "subreddit": row.get("subreddit"),
            "title": row.get("title"),
            "body": row.get("body"),
            "url": row.get("url"),
            "language": row.get("language"),
            "country": row.get("country"),
            "published_at": row.get("published_at"),
            "engagement_score": row.get("engagement_score"),
            "score": row.get("score"),
            "comment_count": row.get("comment_count"),
            "upvote_ratio": row.get("upvote_ratio"),
            "metadata_json": row.get("metadata_json"),
            "dedupe_hash": row.get("dedupe_hash"),
        })

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.executemany(q, normalized)
        conn.commit()

    return len(normalized)


def refresh_social_for_market(market_id: str):
    market = get_market_title_and_category(market_id)
    if not market:
        return {"status": "error", "message": "market_not_found", "market_id": market_id}

    _, title, category = market

    topic = map_market_topic(market_id, title, category)
    news_rows = fetch_news_for_queries(
        market_id=market_id,
        queries=topic.queries.get("news", []),
        category=topic.category,
        entities=topic.entities,
    )

    inserted = insert_social_raw_rows(news_rows)
    target_day = datetime.now(timezone.utc).date()

    compute_market_social_features_daily(day=target_day, market_id=market_id, lookback_days=7)
    score_market_social_features_daily(day=target_day, market_id=market_id)
    compute_market_social_intelligence_daily(day=target_day, market_id=market_id, limit_markets=1)
    compute_market_alignment_daily(day=target_day, market_id=market_id, limit_markets=1)

    return {
        "status": "ok",
        "market_id": market_id,
        "inserted_raw_rows": inserted,
        "topic_category": topic.category,
        "queries": topic.queries.get("news", []),
        "target_day": str(target_day),
    }