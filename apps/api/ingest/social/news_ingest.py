from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any


GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"


def _parse_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)

    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    return datetime.now(timezone.utc)


def _http_get(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "pm-ops-console/1.0",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _fetch_gdelt_articles(query: str, max_records: int = 8) -> list[dict[str, Any]]:
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": str(max_records),
        "format": "json",
        "sort": "datedesc",
    }
    url = f"{GDELT_DOC_API}?{urllib.parse.urlencode(params)}"
    raw = _http_get(url)
    payload = json.loads(raw)
    return payload.get("articles", []) or []


def _fetch_google_news_rss(query: str, max_records: int = 8) -> list[dict[str, Any]]:
    params = {
        "q": query,
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    }
    url = f"{GOOGLE_NEWS_RSS}?{urllib.parse.urlencode(params)}"
    raw = _http_get(url)

    root = ET.fromstring(raw)
    items: list[dict[str, Any]] = []

    channel = root.find("channel")
    if channel is None:
        return items

    for item in channel.findall("item")[:max_records]:
        title = item.findtext("title")
        link = item.findtext("link")
        pub_date = item.findtext("pubDate")
        source = item.find("source")
        source_name = source.text if source is not None else "google_news"

        items.append(
            {
                "title": title,
                "url": link,
                "source": source_name,
                "domain": source_name,
                "sourcecountry": None,
                "seendate": pub_date,
                "language": "en",
                "provider": "google_news_rss",
            }
        )

    return items


def _normalize(text: str | None) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[^\w\sáéíóúñç]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _tokenize(text: str | None) -> list[str]:
    return [t for t in _normalize(text).split() if len(t) >= 3]


def _query_has_enough_signal(query: str) -> bool:
    q = query.strip()
    if len(q) < 5:
        return False

    banned_exact = {
        "liga", "mais", "aico", "party i", "party", "election", "politics", "news", "polls"
    }
    if q.lower() in banned_exact:
        return False

    return True


def _build_required_terms(
    category: str | None,
    entities: list[str],
    queries: list[str],
) -> list[str]:
    required: list[str] = []

    if entities:
        primary = entities[0]
        required.extend(_tokenize(primary)[:3])

    if category == "sports":
        for q in queries:
            if "la liga" in q.lower():
                required.append("liga")
            if "premier league" in q.lower():
                required.extend(["premier", "league"])

    if category == "politics":
        for q in queries:
            qn = _normalize(q)
            if "colombian senate" in qn:
                required.extend(["colombian", "senate"])
            elif "election" in qn:
                required.append("election")

    if category == "crypto":
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
    source_name: str | None,
    query: str,
    required_terms: list[str],
) -> float:
    hay = f"{title or ''} {source_name or ''} {_normalize(query)}"
    hay_norm = _normalize(hay)

    score = 0.0

    for term in required_terms:
        if term in hay_norm:
            score += 1.0

    query_tokens = [t for t in _tokenize(query) if t not in {"news", "polls", "politics", "election"}]
    overlap = sum(1 for t in query_tokens if t in hay_norm)
    score += 0.35 * overlap

    return score


def _passes_relevance(
    title: str | None,
    source_name: str | None,
    query: str,
    category: str | None,
    required_terms: list[str],
) -> bool:
    hay = _normalize(f"{title or ''} {source_name or ''}")

    # reject obvious self / low-value rows
    banned_sources = {
        "polymarket.com",
    }
    if (source_name or "").lower().strip() in banned_sources:
        return False

    banned_title_terms = {
        "prediction",
        "predictions",
        "odds",
    }
    if category == "politics":
        if any(term in hay for term in banned_title_terms):
            return False

    score = _relevance_score(
        title=title,
        source_name=source_name,
        query=query,
        required_terms=required_terms,
    )

    if category == "sports":
        return score >= 2.0
    if category == "politics":
        return score >= 2.25
    if category == "crypto":
        return score >= 1.5
    return score >= 1.5


def _dedupe_hash(
    market_id: str,
    source: str,
    url: str | None,
    title: str | None,
    published_at: datetime | str | None,
) -> str:
    raw = f"{market_id}|{source}|{url or ''}|{title or ''}|{published_at or ''}"
    import hashlib
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_news_for_queries(
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
            time.sleep(1.2)

        articles: list[dict[str, Any]] = []
        provider = None

        try:
            articles = _fetch_gdelt_articles(query=query, max_records=6)
            provider = "gdelt_doc"
            print(f"[news_ingest] provider=gdelt query={query!r} articles={len(articles)}")
        except Exception as e:
            print(f"[news_ingest] provider=gdelt query={query!r} error={e}")

        if not articles:
            try:
                time.sleep(0.5)
                articles = _fetch_google_news_rss(query=query, max_records=6)
                provider = "google_news_rss"
                print(f"[news_ingest] provider=rss query={query!r} articles={len(articles)}")
            except Exception as e:
                print(f"[news_ingest] provider=rss query={query!r} error={e}")
                continue

        for article in articles:
            title = article.get("title")
            url = article.get("url")
            source_name = article.get("source") or article.get("domain") or provider or "news"

            if not _passes_relevance(
                title=title,
                source_name=source_name,
                query=query,
                category=category,
                required_terms=required_terms,
            ):
                continue

            published_at = _parse_datetime(article.get("seendate") or article.get("date"))

            dedupe_hash = _dedupe_hash(
                market_id=market_id,
                source="news",
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
                    "source": "news",
                    "source_type": "article",
                    "query": query,
                    "external_id": url,
                    "author": None,
                    "source_name": source_name,
                    "title": title,
                    "body": None,
                    "url": url,
                    "language": article.get("language") or "en",
                    "country": article.get("sourcecountry"),
                    "published_at": published_at,
                    "engagement_score": 1.0,
                    "comment_count": None,
                    "upvote_ratio": None,
                    "metadata_json": {
                        "provider": provider,
                        "domain": article.get("domain"),
                        "sourcecountry": article.get("sourcecountry"),
                        "query": query,
                        "required_terms": required_terms,
                    },
                    "dedupe_hash": dedupe_hash,
                }
            )

    return rows