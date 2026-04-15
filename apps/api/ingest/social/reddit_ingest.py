from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


REDDIT_SEARCH_URL = "https://www.reddit.com/search.json"

GENERIC_BAD_SUBREDDITS = {
    "politics",
    "politics2",
    "peopleagainsttrump",
    "antitrump",
    "neurodiversepolitics",
    "philippines",
    "dailychismisph",
    "politicalreceipts",
    "u_propertyadviceneeded",
    "u_kositoht",
    "freefirehack",
}

GENERIC_BAD_TITLE_TERMS = {
    "govbrief today",
    "fuel crisis",
    "business opportunities senator",
    "hardstuck in div",
    "wettanbieter",
    "app oficial",
}

SPORTS_HINT_TERMS = {
    "villarreal",
    "laliga",
    "la liga",
    "real sociedad",
    "match",
    "prediction",
    "lineup",
    "fixture",
    "table",
    "football",
    "soccer",
}

POLITICS_HINT_TERMS = {
    "colombia",
    "colombian",
    "senate",
    "election",
    "legislative",
    "congress",
    "party",
    "poll",
    "coalition",
    "vote",
}

CRYPTO_HINT_TERMS = {
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "crypto",
    "token",
    "etf",
}


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\sáéíóúñç]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _tokenize(text: str | None) -> list[str]:
    return [t for t in _normalize(text).split() if len(t) >= 3]


def _http_get(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "pm-ops-console/1.0",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _query_has_enough_signal(query: str) -> bool:
    q = query.strip().lower()
    if len(q) < 5:
        return False

    banned_exact = {
        "liga", "mais", "aico", "party i", "party", "election", "politics", "news", "polls"
    }
    return q not in banned_exact


def _build_required_terms(
    category: str | None,
    entities: list[str],
    queries: list[str],
) -> list[str]:
    required: list[str] = []

    if entities:
        required.extend(_tokenize(entities[0])[:3])

    joined = " ".join(queries).lower()

    if category == "sports":
        if "la liga" in joined:
            required.extend(["liga"])
    elif category == "politics":
        if "colombian senate" in joined:
            required.extend(["colombian", "senate"])
        elif "election" in joined:
            required.append("election")
    elif category == "crypto":
        required.append("crypto")

    out = []
    seen = set()
    for t in required:
        if t and t not in seen:
            out.append(t)
            seen.add(t)
    return out


def _relevance_score(
    title: str | None,
    body: str | None,
    subreddit: str | None,
    query: str,
    required_terms: list[str],
    category: str | None,
) -> float:
    hay = _normalize(f"{title or ''} {body or ''} {subreddit or ''} {query}")
    score = 0.0

    for term in required_terms:
        if term in hay:
            score += 1.5

    query_tokens = [t for t in _tokenize(query) if t not in {"news", "polls", "politics", "election"}]
    overlap = sum(1 for t in query_tokens if t in hay)
    score += 0.4 * overlap

    hints: set[str] = set()
    if category == "sports":
        hints = SPORTS_HINT_TERMS
    elif category == "politics":
        hints = POLITICS_HINT_TERMS
    elif category == "crypto":
        hints = CRYPTO_HINT_TERMS

    hint_hits = sum(1 for term in hints if term in hay)
    score += min(hint_hits, 4) * 0.4

    return score


def _passes_relevance(
    title: str | None,
    body: str | None,
    subreddit: str | None,
    query: str,
    category: str | None,
    required_terms: list[str],
) -> bool:
    title_norm = _normalize(title)
    subreddit_norm = _normalize(subreddit)

    if subreddit_norm in GENERIC_BAD_SUBREDDITS:
        return False

    if any(term in title_norm for term in GENERIC_BAD_TITLE_TERMS):
        return False

    score = _relevance_score(
        title=title,
        body=body,
        subreddit=subreddit,
        query=query,
        required_terms=required_terms,
        category=category,
    )

    if category == "sports":
        return score >= 3.0
    if category == "politics":
        return score >= 3.2
    if category == "crypto":
        return score >= 2.5
    return score >= 2.5


def _parse_reddit_ts(created_utc: float | int | None) -> datetime:
    if not created_utc:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(float(created_utc), tz=timezone.utc)


def _dedupe_hash(
    market_id: str,
    source: str,
    url: str | None,
    title: str | None,
    published_at: datetime | str | None,
) -> str:
    import hashlib
    raw = f"{market_id}|{source}|{url or ''}|{title or ''}|{published_at or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _fetch_reddit_posts(query: str, limit: int = 10) -> list[dict[str, Any]]:
    params = {
        "q": query,
        "sort": "new",
        "limit": str(limit),
        "type": "link",
        "restrict_sr": "false",
    }
    url = f"{REDDIT_SEARCH_URL}?{urllib.parse.urlencode(params)}"
    raw = _http_get(url)
    payload = json.loads(raw)

    children = (((payload or {}).get("data") or {}).get("children") or [])
    posts = []
    for child in children:
        data = child.get("data") or {}
        posts.append(data)
    return posts


def fetch_reddit_for_queries(
    market_id: str,
    queries: list[str],
    category: str | None = None,
    entities: list[str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()

    filtered_queries = [q for q in queries[:4] if _query_has_enough_signal(q)]
    required_terms = _build_required_terms(
        category=category,
        entities=entities or [],
        queries=filtered_queries,
    )

    for i, query in enumerate(filtered_queries):
        if i > 0:
            time.sleep(1.0)

        try:
            posts = _fetch_reddit_posts(query=query, limit=10)
            print(f"[reddit_ingest] query={query!r} posts={len(posts)}")
        except Exception as e:
            print(f"[reddit_ingest] query={query!r} error={e}")
            continue

        for post in posts:
            title = post.get("title")
            body = post.get("selftext")
            subreddit = post.get("subreddit")
            permalink = post.get("permalink")
            url = f"https://www.reddit.com{permalink}" if permalink else post.get("url")

            if not _passes_relevance(
                title=title,
                body=body,
                subreddit=subreddit,
                query=query,
                category=category,
                required_terms=required_terms,
            ):
                continue

            published_at = _parse_reddit_ts(post.get("created_utc"))

            dedupe_hash = _dedupe_hash(
                market_id=market_id,
                source="reddit",
                url=url,
                title=title,
                published_at=published_at,
            )

            if dedupe_hash in seen_hashes:
                continue
            seen_hashes.add(dedupe_hash)

            rows.append(
                {
                    "market_id": market_id,
                    "source": "reddit",
                    "source_type": "forum_post",
                    "query": query,
                    "external_id": post.get("id"),
                    "author": post.get("author"),
                    "source_name": subreddit or "reddit",
                    "subreddit": subreddit,
                    "title": title,
                    "body": body,
                    "url": url,
                    "language": "en",
                    "country": None,
                    "published_at": published_at,
                    "engagement_score": float(post.get("num_comments") or 0) + float(post.get("score") or 0),
                    "score": int(post.get("score") or 0),
                    "comment_count": int(post.get("num_comments") or 0),
                    "upvote_ratio": post.get("upvote_ratio"),
                    "metadata_json": {
                        "provider": "reddit_json",
                        "subreddit": subreddit,
                        "query": query,
                        "required_terms": required_terms,
                    },
                    "dedupe_hash": dedupe_hash,
                }
            )

    return rows