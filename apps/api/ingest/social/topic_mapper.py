from __future__ import annotations

import json
import re
from dataclasses import dataclass, asdict
from typing import Any, Optional

import psycopg

from apps.api.db import get_db_dsn


SPORTS_LEAGUES = [
    "premier league",
    "la liga",
    "champions league",
    "serie a",
    "bundesliga",
    "ligue 1",
    "world cup",
    "euros",
    "nba",
    "nfl",
    "mlb",
]

STOP_WORDS = {
    "will", "the", "a", "an", "in", "on", "at", "for", "to", "of", "and",
    "be", "is", "are", "win", "lose", "beat", "vs", "v", "by", "before",
    "after", "during", "over", "under", "season", "game", "match",
    "most", "seats", "election", "senate", "presidential", "general",
    "primary", "party", "coalition", "candidate", "office"
}


@dataclass
class TopicMap:
    market_id: str
    title: str
    category: str | None
    primary_topic: str | None
    entities: list[str]
    aliases: list[str]
    queries: dict[str, list[str]]
    mapping_confidence: float


def _load_override(market_id: str) -> Optional[dict[str, Any]]:
    dsn = get_db_dsn()

    q = """
    select
        market_id,
        primary_topic,
        category,
        entities_json,
        queries_json
    from public.market_topic_overrides
    where market_id = %s
    limit 1;
    """

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            row = cur.fetchone()

    if not row:
        return None

    return {
        "market_id": row[0],
        "primary_topic": row[1],
        "category": row[2],
        "entities_json": row[3] or [],
        "queries_json": row[4] or {},
    }


def infer_category(title: str, category: str | None = None) -> str:
    if category:
        return category.lower()

    t = title.lower()

    if any(x in t for x in SPORTS_LEAGUES):
        return "sports"
    if any(x in t for x in ["election", "president", "governor", "senate", "vote", "war", "ceasefire", "bill"]):
        return "politics"
    if any(x in t for x in ["bitcoin", "btc", "ethereum", "eth", "solana", "crypto", "etf", "token"]):
        return "crypto"

    return "general"


def normalize_spaces(text: str) -> str:
    text = re.sub(r"[^\w\s\-–—'áéíóúñçÁÉÍÓÚÑÇ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_league(title: str) -> str | None:
    t = title.lower()
    for league in SPORTS_LEAGUES:
        if league in t:
            return league.title()
    return None


def extract_sports_entities(title: str) -> list[str]:
    clean = normalize_spaces(title)

    m = re.search(r"will (.+?) win", clean, flags=re.IGNORECASE)
    if m:
        raw = m.group(1).strip()
        raw = re.sub(r"\bthe\b", "", raw, flags=re.IGNORECASE).strip()
        return [raw]

    m = re.search(r"will (.+?) beat (.+)", clean, flags=re.IGNORECASE)
    if m:
        return [m.group(1).strip(), m.group(2).strip()]

    m = re.search(r"(.+?)\s+vs\s+(.+)", clean, flags=re.IGNORECASE)
    if m:
        return [m.group(1).strip(), m.group(2).strip()]

    words = [w for w in clean.split() if w.lower() not in STOP_WORDS and len(w) > 2]
    return words[:3]


def extract_politics_entities(title: str) -> list[str]:
    clean = normalize_spaces(title)

    m = re.search(
        r"will\s+(.+?)\s+win\s+the\s+most\s+seats\s+in\s+the\s+(.+?)\s+election",
        clean,
        flags=re.IGNORECASE,
    )
    if m:
        party = m.group(1).strip()
        election_scope = m.group(2).strip()
        return [party, election_scope]

    m = re.search(r"will\s+(.+?)\s+(win|become|resign|veto|pass)", clean, flags=re.IGNORECASE)
    if m:
        return [m.group(1).strip()]

    parts = [w for w in clean.split() if w.lower() not in STOP_WORDS and len(w) > 2]
    return parts[:4]


def extract_crypto_entities(title: str) -> list[str]:
    clean = normalize_spaces(title)
    tickers = re.findall(r"\b[A-Z]{2,10}\b", title)
    if tickers:
        return list(dict.fromkeys(tickers[:3]))

    return [w for w in clean.split() if w.lower() not in STOP_WORDS][:4]


def extract_entities(title: str, category: str) -> list[str]:
    if category == "sports":
        return extract_sports_entities(title)
    if category == "politics":
        return extract_politics_entities(title)
    if category == "crypto":
        return extract_crypto_entities(title)

    clean = normalize_spaces(title)
    return [w for w in clean.split() if w.lower() not in STOP_WORDS][:4]


def build_aliases(entities: list[str]) -> list[str]:
    out = []
    seen = set()

    for e in entities:
        candidates = [
            e,
            e.replace("FC", "").strip(),
            e.replace("CF", "").strip(),
        ]
        for c in candidates:
            k = c.lower().strip()
            if k and k not in seen:
                out.append(c.strip())
                seen.add(k)

    return out


def build_queries(title: str, entities: list[str], category: str) -> dict[str, list[str]]:
    queries: list[str] = []
    league = extract_league(title)

    if category == "sports":
        if len(entities) >= 1 and league:
            queries.extend([
                f"{entities[0]} {league}",
                f"{entities[0]} {league} title race",
                f"{entities[0]} {league} odds",
            ])
        if len(entities) >= 2:
            queries.extend([
                f"{entities[0]} vs {entities[1]}",
                f"{entities[0]} {entities[1]} match",
            ])
        if len(entities) == 1:
            queries.append(entities[0])

    elif category == "politics":
        party = entities[0] if len(entities) >= 1 else None
        election_scope = entities[1] if len(entities) >= 2 else None

        if party and election_scope:
            queries.extend([
                f"{party} {election_scope}",
                f"{party} {election_scope} election",
                f"{party} {election_scope} polls",
            ])
        elif party:
            queries.extend([
                party,
                f"{party} election",
                f"{party} politics",
            ])

    elif category == "crypto":
        if entities:
            queries.extend([
                entities[0],
                f"{entities[0]} crypto",
                f"{entities[0]} news",
                f"{entities[0]} market",
            ])

    else:
        queries.append(title)
        queries.extend(entities[:2])

    out = []
    seen = set()
    for q in queries:
        q = q.strip()
        k = q.lower()
        if q and k not in seen and len(q) > 3:
            out.append(q)
            seen.add(k)

    return {
        "news": out[:4],
        "reddit": out[:4],
    }


def map_market_topic(market_id: str, title: str, category: str | None = None) -> TopicMap:
    override = _load_override(market_id)
    if override:
        entities = override["entities_json"] or []
        queries = override["queries_json"] or {"news": [], "reddit": []}
        category_val = (override["category"] or category or infer_category(title, category)).lower()

        return TopicMap(
            market_id=market_id,
            title=title,
            category=category_val,
            primary_topic=override["primary_topic"],
            entities=entities,
            aliases=build_aliases(entities),
            queries=queries,
            mapping_confidence=0.99,
        )

    inferred = infer_category(title, category)
    entities = extract_entities(title, inferred)
    aliases = build_aliases(entities)
    queries = build_queries(title, aliases or entities, inferred)

    confidence = 0.35
    if entities:
        confidence += 0.25
    if len(queries.get("news", [])) >= 2:
        confidence += 0.20
    if category:
        confidence += 0.20

    confidence = min(confidence, 0.99)

    return TopicMap(
        market_id=market_id,
        title=title,
        category=inferred,
        primary_topic=entities[0] if entities else None,
        entities=entities,
        aliases=aliases,
        queries=queries,
        mapping_confidence=confidence,
    )


def topic_map_to_dict(topic_map: TopicMap) -> dict[str, Any]:
    return asdict(topic_map)