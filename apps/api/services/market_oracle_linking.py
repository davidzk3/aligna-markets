from __future__ import annotations

from typing import Optional, Dict, Any, List
import re

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn


def _table_exists(schema_name: str, table_name: str) -> bool:
    q = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
    );
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (schema_name, table_name))
            row = cur.fetchone()
            return bool(row[0]) if row else False


def _get_table_columns(schema_name: str, table_name: str) -> List[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s
      AND table_name = %s
    ORDER BY ordinal_position;
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (schema_name, table_name))
            return [r[0] for r in cur.fetchall()]


def _first_existing(columns: List[str], candidates: List[str]) -> Optional[str]:
    cols = set(columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9\s]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _token_overlap_score(a: str, b: str) -> float:
    a_tokens = set(_normalize_text(a).split())
    b_tokens = set(_normalize_text(b).split())

    if not a_tokens or not b_tokens:
        return 0.0

    overlap = len(a_tokens & b_tokens)
    union = len(a_tokens | b_tokens)
    return round(overlap / union, 4) if union else 0.0


def fetch_market_core(market_id: str) -> Optional[Dict[str, Any]]:
    candidate_tables = [
        ("public", "markets"),
        ("core", "markets"),
    ]

    for schema_name, table_name in candidate_tables:
        if not _table_exists(schema_name, table_name):
            continue

        cols = _get_table_columns(schema_name, table_name)
        if not cols:
            continue

        market_id_col = _first_existing(cols, ["market_id", "id"])
        slug_col = _first_existing(cols, ["slug", "market_slug"])
        condition_id_col = _first_existing(cols, ["condition_id", "conditionid"])
        title_col = _first_existing(cols, ["title", "question", "name"])
        description_col = _first_existing(cols, ["description", "rules", "market_description"])

        if not market_id_col:
            continue

        select_map = {
            "market_id": market_id_col,
            "condition_id": condition_id_col or "NULL",
            "slug": slug_col or "NULL",
            "title": title_col or "NULL",
            "description": description_col or "NULL",
        }

        q = f"""
        SELECT
            {select_map["market_id"]} AS market_id,
            {select_map["condition_id"]} AS condition_id,
            {select_map["slug"]} AS slug,
            {select_map["title"]} AS title,
            {select_map["description"]} AS description
        FROM {schema_name}.{table_name}
        WHERE {market_id_col} = %s
        LIMIT 1
        """

        with psycopg.connect(get_db_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(q, (market_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "market_id": row[0],
                        "condition_id": row[1],
                        "slug": row[2],
                        "title": row[3],
                        "description": row[4],
                        "source_table": f"{schema_name}.{table_name}",
                    }

    return None


def fetch_uma_metadata_rows() -> List[Dict[str, Any]]:
    if not _table_exists("public", "market_uma_resolution_metadata"):
        return []

    cols = _get_table_columns("public", "market_uma_resolution_metadata")
    market_id_col = _first_existing(cols, ["market_id"])
    title_col = _first_existing(cols, ["title"])
    description_col = _first_existing(cols, ["description"])

    q = f"""
    SELECT
        {market_id_col or 'NULL'} AS market_id,
        {title_col or 'NULL'} AS title,
        {description_col or 'NULL'} AS description
    FROM public.market_uma_resolution_metadata
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q)
            rows = cur.fetchall()

    return [
        {
            "uma_request_id": r[0],
            "market_id": r[0],
            "title": r[1],
            "description": r[2],
        }
        for r in rows
    ]


def fetch_uma_match_direct_market_id(market_id: str) -> Optional[Dict[str, Any]]:
    if not _table_exists("public", "market_uma_resolution_metadata"):
        return None

    q = """
    SELECT market_id, title, description
    FROM public.market_uma_resolution_metadata
    WHERE market_id = %s
    LIMIT 1
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            row = cur.fetchone()
            if not row:
                return None

            return {
                "candidate_uma_request_id": row[0],
                "title": row[1],
                "description": row[2],
                "link_method": "market_id_direct",
                "confidence": 0.99,
                "evidence_json": {
                    "matched_on": "market_id",
                    "match_value": market_id,
                },
            }


def fetch_uma_request_by_condition(condition_id: str) -> Optional[Dict[str, Any]]:
    if not condition_id:
        return None

    candidate_tables = [
        ("public", "uma_requests"),
        ("public", "market_uma_requests"),
    ]

    for schema_name, table_name in candidate_tables:
        if not _table_exists(schema_name, table_name):
            continue

        cols = _get_table_columns(schema_name, table_name)
        condition_id_col = _first_existing(cols, ["condition_id", "conditionid"])
        request_id_col = _first_existing(cols, ["request_id", "uma_request_id", "id"])

        if not condition_id_col or not request_id_col:
            continue

        q = f"""
        SELECT
            {request_id_col} AS request_id,
            {condition_id_col} AS condition_id
        FROM {schema_name}.{table_name}
        WHERE {condition_id_col} = %s
        ORDER BY 1 DESC
        LIMIT 1
        """

        with psycopg.connect(get_db_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(q, (condition_id,))
                row = cur.fetchone()
                if row:
                    return {
                        "candidate_uma_request_id": row[0],
                        "link_method": "condition_id_match",
                        "confidence": 0.95,
                        "evidence_json": {
                            "matched_on": "condition_id",
                            "match_value": condition_id,
                            "source_table": f"{schema_name}.{table_name}",
                        },
                    }

    return None


def build_slug_candidates(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    slug = market.get("slug")
    if not slug:
        return []

    slug_norm = _normalize_text(slug)
    if not slug_norm:
        return []

    candidates = []
    for row in fetch_uma_metadata_rows():
        title_norm = _normalize_text(row.get("title"))
        if slug_norm and title_norm and slug_norm in title_norm:
            candidates.append(
                {
                    "candidate_uma_request_id": row["uma_request_id"],
                    "link_method": "slug_match",
                    "confidence": 0.72,
                    "evidence_json": {
                        "matched_on": "slug_vs_title_contains",
                        "slug": slug,
                        "uma_title": row.get("title"),
                    },
                }
            )

    return candidates


def build_title_similarity_candidates(market: Dict[str, Any]) -> List[Dict[str, Any]]:
    market_title = market.get("title")
    if not market_title:
        return []

    candidates = []
    for row in fetch_uma_metadata_rows():
        score = _token_overlap_score(market_title, row.get("title"))
        if score >= 0.55:
            candidates.append(
                {
                    "candidate_uma_request_id": row["uma_request_id"],
                    "link_method": "title_similarity_match",
                    "confidence": round(min(0.85, 0.50 + score * 0.4), 4),
                    "evidence_json": {
                        "matched_on": "title_token_overlap",
                        "market_title": market_title,
                        "uma_title": row.get("title"),
                        "token_overlap_score": score,
                    },
                }
            )

    return candidates


def persist_candidates(
    market_id: str,
    condition_id: Optional[str],
    slug: Optional[str],
    candidates: List[Dict[str, Any]],
    selected_candidate_id: Optional[str],
):
    if not candidates:
        return

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            for c in candidates:
                cur.execute(
                    """
                    INSERT INTO public.market_oracle_link_candidates (
                        market_id,
                        condition_id,
                        slug,
                        candidate_uma_request_id,
                        oracle_family,
                        link_method,
                        confidence,
                        evidence_json,
                        is_selected
                    )
                    VALUES (%s, %s, %s, %s, 'uma_oo', %s, %s, %s, %s)
                    ON CONFLICT (market_id, candidate_uma_request_id, link_method)
                    DO UPDATE SET
                        confidence = EXCLUDED.confidence,
                        evidence_json = EXCLUDED.evidence_json,
                        is_selected = EXCLUDED.is_selected
                    """,
                    (
                        market_id,
                        condition_id,
                        slug,
                        c["candidate_uma_request_id"],
                        c["link_method"],
                        c["confidence"],
                        Json(c.get("evidence_json", {})),
                        c["candidate_uma_request_id"] == selected_candidate_id,
                    ),
                )
        conn.commit()


def persist_link(
    market_id: str,
    condition_id: Optional[str],
    slug: Optional[str],
    uma_request_id: Optional[str],
    link_method: str,
    confidence: float,
):
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.market_oracle_links (
                    market_id,
                    condition_id,
                    slug,
                    uma_request_id,
                    link_method,
                    confidence
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id)
                DO UPDATE SET
                    condition_id = EXCLUDED.condition_id,
                    slug = EXCLUDED.slug,
                    uma_request_id = EXCLUDED.uma_request_id,
                    link_method = EXCLUDED.link_method,
                    confidence = EXCLUDED.confidence
                """,
                (
                    market_id,
                    condition_id,
                    slug,
                    uma_request_id,
                    link_method,
                    confidence,
                ),
            )
        conn.commit()


def select_best_candidate(candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda x: (-float(x.get("confidence") or 0.0), x.get("link_method") or ""),
    )[0]


def link_market_to_oracle(market_id: str) -> Dict[str, Any]:
    market = fetch_market_core(market_id)

    if not market:
        direct_uma = fetch_uma_match_direct_market_id(market_id)
        if direct_uma:
            candidates = [direct_uma]
            selected = select_best_candidate(candidates)

            persist_candidates(
                market_id=market_id,
                condition_id=None,
                slug=None,
                candidates=candidates,
                selected_candidate_id=selected["candidate_uma_request_id"],
            )

            persist_link(
                market_id=market_id,
                condition_id=None,
                slug=None,
                uma_request_id=selected["candidate_uma_request_id"],
                link_method=selected["link_method"],
                confidence=selected["confidence"],
            )

            return {
                "market_id": market_id,
                "condition_id": None,
                "slug": None,
                "linked": True,
                "selected_candidate": selected,
                "candidate_count": len(candidates),
                "candidates": candidates,
                "source_table": "public.market_uma_resolution_metadata",
            }

        return {
            "market_id": market_id,
            "linked": False,
            "reason": "market_not_found",
        }

    condition_id = market.get("condition_id")
    slug = market.get("slug")

    candidates: List[Dict[str, Any]] = []

    if condition_id:
        c = fetch_uma_request_by_condition(condition_id)
        if c:
            candidates.append(c)

    direct_uma = fetch_uma_match_direct_market_id(market_id)
    if direct_uma:
        candidates.append(direct_uma)

    candidates.extend(build_slug_candidates(market))
    candidates.extend(build_title_similarity_candidates(market))

    deduped = {}
    for c in candidates:
        key = (c["candidate_uma_request_id"], c["link_method"])
        if key not in deduped or c["confidence"] > deduped[key]["confidence"]:
            deduped[key] = c

    candidates = list(deduped.values())
    selected = select_best_candidate(candidates)

    if not selected:
        return {
            "market_id": market_id,
            "condition_id": condition_id,
            "slug": slug,
            "linked": False,
            "reason": "no_match_found",
            "source_table": market.get("source_table"),
        }

    persist_candidates(
        market_id=market_id,
        condition_id=condition_id,
        slug=slug,
        candidates=candidates,
        selected_candidate_id=selected["candidate_uma_request_id"],
    )

    persist_link(
        market_id=market_id,
        condition_id=condition_id,
        slug=slug,
        uma_request_id=selected["candidate_uma_request_id"],
        link_method=selected["link_method"],
        confidence=selected["confidence"],
    )

    return {
        "market_id": market_id,
        "condition_id": condition_id,
        "slug": slug,
        "linked": True,
        "selected_candidate": selected,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "source_table": market.get("source_table"),
    }