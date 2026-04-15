from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_uma_resolution_intelligence_v1"


def _hours_between(start, end) -> float | None:
    if not start or not end:
        return None
    if isinstance(start, str):
        start = datetime.fromisoformat(start.replace("Z", "+00:00"))
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace("Z", "+00:00"))
    delta = end - start
    return round(delta.total_seconds() / 3600.0, 4)


def _contains_any(text: str, patterns: list[str]) -> bool:
    t = (text or "").lower()
    return any(p.lower() in t for p in patterns)


def _build_flags(
    disputed: bool,
    has_time_ambiguity: bool,
    has_wording_ambiguity: bool,
    has_source_fragility: bool,
    has_bulletin_board_dependency: bool,
    request_to_settlement_hours: float | None,
    dispute_to_settlement_hours: float | None,
) -> list[str]:
    flags: list[str] = []

    if disputed:
        flags.append("DISPUTED")

    if has_time_ambiguity:
        flags.append("TIME_AMBIGUITY")

    if has_wording_ambiguity:
        flags.append("WORDING_AMBIGUITY")

    if has_source_fragility:
        flags.append("SOURCE_FRAGILITY")

    if has_bulletin_board_dependency:
        flags.append("BULLETIN_BOARD_DEPENDENCY")

    if request_to_settlement_hours is not None and request_to_settlement_hours >= 72:
        flags.append("DELAYED_FINALITY")

    if dispute_to_settlement_hours is not None and dispute_to_settlement_hours >= 48:
        flags.append("LONG_POST_DISPUTE_SETTLEMENT")

    return flags


def _resolution_state(
    clarity_score: float,
    oracle_risk_score: float,
    disputed: bool,
) -> str:
    if disputed or oracle_risk_score >= 0.70:
        return "fragile"
    if clarity_score >= 0.80 and oracle_risk_score <= 0.35:
        return "cleanly_resolvable"
    if clarity_score >= 0.60 and oracle_risk_score <= 0.55:
        return "moderate"
    return "complex"


def _resolution_complexity(
    has_time_ambiguity: bool,
    has_wording_ambiguity: bool,
    has_source_fragility: bool,
    has_bulletin_board_dependency: bool,
    disputed: bool,
) -> str:
    score = sum([
        1 if has_time_ambiguity else 0,
        1 if has_wording_ambiguity else 0,
        1 if has_source_fragility else 0,
        1 if has_bulletin_board_dependency else 0,
        1 if disputed else 0,
    ])

    if score >= 4:
        return "high_complexity"
    if score >= 2:
        return "moderate_complexity"
    return "low_complexity"


def _build_summary(
    state: str,
    disputed: bool,
    has_time_ambiguity: bool,
    has_wording_ambiguity: bool,
    has_source_fragility: bool,
    has_bulletin_board_dependency: bool,
) -> str:
    if disputed:
        return "This market entered dispute, indicating meaningful oracle or interpretation friction."

    if state == "cleanly_resolvable":
        return "This market appears cleanly resolvable with limited oracle-path friction."

    if has_time_ambiguity and has_wording_ambiguity:
        return "This market shows both time and wording ambiguity, increasing resolution complexity."

    if has_source_fragility:
        return "This market depends on fragile or consensus-sensitive sourcing, which weakens resolution confidence."

    if has_bulletin_board_dependency:
        return "This market appears dependent on bulletin-board interpretation or amendments, increasing operational resolution risk."

    return "This market shows moderate resolution complexity with some rule or source sensitivity."


def compute_market_uma_resolution_intelligence_daily(
    day: Optional[date] = None,
    market_id: Optional[str] = None,
) -> Dict[str, Any]:
    target_day = day or datetime.now(timezone.utc).date()

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    market_id,
                    oracle_family,
                    oracle_type,
                    oracle_contract,
                    identifier,
                    umip,
                    requester,
                    request_transaction,
                    proposer,
                    proposal_transaction,
                    disputer,
                    dispute_transaction,
                    settlement_recipient,
                    settlement_transaction,
                    requested_time,
                    proposed_time,
                    disputed_time,
                    settled_time,
                    title,
                    description,
                    additional_text_data,
                    bulletin_board_text,
                    res_data,
                    initializer,
                    chain,
                    expiry_type,
                    disputed,
                    settled,
                    outcome_proposed,
                    outcome_settled,
                    raw_payload_json
                FROM public.market_uma_resolution_metadata
                WHERE (%s::text IS NULL OR market_id = %s::text)
                """,
                (market_id, market_id),
            )
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]

    records = [dict(zip(cols, row)) for row in rows]

    upserts = []
    for r in records:
        description = r.get("description") or ""
        bulletin_board_text = r.get("bulletin_board_text") or ""
        additional_text_data = r.get("additional_text_data") or ""

        disputed = bool(r.get("disputed"))
        settled = bool(r.get("settled"))

        requested_time = r.get("requested_time")
        proposed_time = r.get("proposed_time")
        disputed_time = r.get("disputed_time")
        settled_time = r.get("settled_time")

        request_to_proposal_hours = _hours_between(requested_time, proposed_time)
        proposal_to_dispute_hours = _hours_between(proposed_time, disputed_time)
        dispute_to_settlement_hours = _hours_between(disputed_time, settled_time)
        request_to_settlement_hours = _hours_between(requested_time, settled_time)

        has_time_ambiguity = _contains_any(
            description,
            [
                "cannot be confirmed",
                "within 48 hours",
                "by the end of the third calendar date",
                "listed date",
                "specified week",
                "timeframe",
                "israel time",
                "eet",
            ],
        )

        has_wording_ambiguity = _contains_any(
            description,
            [
                "for the purposes of this market",
                "will not count",
                "will not qualify",
                "in the case of ambiguity",
                "otherwise",
                "qualifying",
            ],
        )

        has_source_fragility = _contains_any(
            description,
            [
                "consensus of credible reporting",
                "major international media",
                "national broadcasters",
                "official statements",
                "primary resolution source",
            ],
        )

        has_bulletin_board_dependency = bool(bulletin_board_text) or _contains_any(
            description,
            [
                "bulletin board",
                "updates made by the question creator",
                "should be considered",
            ],
        )

        clarity_score = 1.0
        oracle_risk_score = 0.0
        dispute_propensity_score = 0.10

        if has_time_ambiguity:
            clarity_score -= 0.15
            oracle_risk_score += 0.15
            dispute_propensity_score += 0.10

        if has_wording_ambiguity:
            clarity_score -= 0.20
            oracle_risk_score += 0.20
            dispute_propensity_score += 0.15

        if has_source_fragility:
            clarity_score -= 0.10
            oracle_risk_score += 0.15
            dispute_propensity_score += 0.10

        if has_bulletin_board_dependency:
            clarity_score -= 0.10
            oracle_risk_score += 0.15
            dispute_propensity_score += 0.10

        if disputed:
            clarity_score -= 0.20
            oracle_risk_score += 0.25
            dispute_propensity_score += 0.30

        if request_to_settlement_hours is not None and request_to_settlement_hours >= 72:
            clarity_score -= 0.05
            oracle_risk_score += 0.10

        clarity_score = max(0.0, min(1.0, round(clarity_score, 4)))
        oracle_risk_score = max(0.0, min(1.0, round(oracle_risk_score, 4)))
        dispute_propensity_score = max(0.0, min(1.0, round(dispute_propensity_score, 4)))

        resolution_complexity = _resolution_complexity(
            has_time_ambiguity=has_time_ambiguity,
            has_wording_ambiguity=has_wording_ambiguity,
            has_source_fragility=has_source_fragility,
            has_bulletin_board_dependency=has_bulletin_board_dependency,
            disputed=disputed,
        )

        resolution_state = _resolution_state(
            clarity_score=clarity_score,
            oracle_risk_score=oracle_risk_score,
            disputed=disputed,
        )

        flags = _build_flags(
            disputed=disputed,
            has_time_ambiguity=has_time_ambiguity,
            has_wording_ambiguity=has_wording_ambiguity,
            has_source_fragility=has_source_fragility,
            has_bulletin_board_dependency=has_bulletin_board_dependency,
            request_to_settlement_hours=request_to_settlement_hours,
            dispute_to_settlement_hours=dispute_to_settlement_hours,
        )

        summary = _build_summary(
            state=resolution_state,
            disputed=disputed,
            has_time_ambiguity=has_time_ambiguity,
            has_wording_ambiguity=has_wording_ambiguity,
            has_source_fragility=has_source_fragility,
            has_bulletin_board_dependency=has_bulletin_board_dependency,
        )

        raw_inputs = {
            "title": r.get("title"),
            "description": description,
            "additional_text_data": additional_text_data,
            "bulletin_board_text": bulletin_board_text,
            "identifier": r.get("identifier"),
            "oracle_type": r.get("oracle_type"),
            "disputed": disputed,
            "settled": settled,
        }

        upserts.append({
            "market_id": r["market_id"],
            "day": target_day,
            "oracle_family": r.get("oracle_family") or "uma_oo",
            "resolution_clarity_score": clarity_score,
            "oracle_risk_score": oracle_risk_score,
            "dispute_propensity_score": dispute_propensity_score,
            "resolution_complexity": resolution_complexity,
            "resolution_state": resolution_state,
            "disputed": disputed,
            "settled": settled,
            "requested_time": requested_time,
            "proposed_time": proposed_time,
            "disputed_time": disputed_time,
            "settled_time": settled_time,
            "request_to_proposal_hours": request_to_proposal_hours,
            "proposal_to_dispute_hours": proposal_to_dispute_hours,
            "dispute_to_settlement_hours": dispute_to_settlement_hours,
            "request_to_settlement_hours": request_to_settlement_hours,
            "has_time_ambiguity": has_time_ambiguity,
            "has_wording_ambiguity": has_wording_ambiguity,
            "has_source_fragility": has_source_fragility,
            "has_bulletin_board_dependency": has_bulletin_board_dependency,
            "flags": flags,
            "summary": summary,
            "engine_version": ENGINE_VERSION,
            "raw_inputs_json": raw_inputs,
        })

    if not upserts:
        return {
            "status": "ok",
            "day": str(target_day),
            "market_id": market_id,
            "rows_written": 0,
            "engine_version": ENGINE_VERSION,
        }

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            for row in upserts:
                cur.execute(
                    """
                    INSERT INTO public.market_uma_resolution_intelligence_daily (
                        market_id,
                        day,
                        oracle_family,
                        resolution_clarity_score,
                        oracle_risk_score,
                        dispute_propensity_score,
                        resolution_complexity,
                        resolution_state,
                        disputed,
                        settled,
                        requested_time,
                        proposed_time,
                        disputed_time,
                        settled_time,
                        request_to_proposal_hours,
                        proposal_to_dispute_hours,
                        dispute_to_settlement_hours,
                        request_to_settlement_hours,
                        has_time_ambiguity,
                        has_wording_ambiguity,
                        has_source_fragility,
                        has_bulletin_board_dependency,
                        flags,
                        summary,
                        engine_version,
                        raw_inputs_json,
                        updated_at
                    )
                    VALUES (
                        %(market_id)s,
                        %(day)s,
                        %(oracle_family)s,
                        %(resolution_clarity_score)s,
                        %(oracle_risk_score)s,
                        %(dispute_propensity_score)s,
                        %(resolution_complexity)s,
                        %(resolution_state)s,
                        %(disputed)s,
                        %(settled)s,
                        %(requested_time)s,
                        %(proposed_time)s,
                        %(disputed_time)s,
                        %(settled_time)s,
                        %(request_to_proposal_hours)s,
                        %(proposal_to_dispute_hours)s,
                        %(dispute_to_settlement_hours)s,
                        %(request_to_settlement_hours)s,
                        %(has_time_ambiguity)s,
                        %(has_wording_ambiguity)s,
                        %(has_source_fragility)s,
                        %(has_bulletin_board_dependency)s,
                        %(flags)s,
                        %(summary)s,
                        %(engine_version)s,
                        %(raw_inputs_json)s,
                        NOW()
                    )
                    ON CONFLICT (market_id, day)
                    DO UPDATE SET
                        oracle_family = EXCLUDED.oracle_family,
                        resolution_clarity_score = EXCLUDED.resolution_clarity_score,
                        oracle_risk_score = EXCLUDED.oracle_risk_score,
                        dispute_propensity_score = EXCLUDED.dispute_propensity_score,
                        resolution_complexity = EXCLUDED.resolution_complexity,
                        resolution_state = EXCLUDED.resolution_state,
                        disputed = EXCLUDED.disputed,
                        settled = EXCLUDED.settled,
                        requested_time = EXCLUDED.requested_time,
                        proposed_time = EXCLUDED.proposed_time,
                        disputed_time = EXCLUDED.disputed_time,
                        settled_time = EXCLUDED.settled_time,
                        request_to_proposal_hours = EXCLUDED.request_to_proposal_hours,
                        proposal_to_dispute_hours = EXCLUDED.proposal_to_dispute_hours,
                        dispute_to_settlement_hours = EXCLUDED.dispute_to_settlement_hours,
                        request_to_settlement_hours = EXCLUDED.request_to_settlement_hours,
                        has_time_ambiguity = EXCLUDED.has_time_ambiguity,
                        has_wording_ambiguity = EXCLUDED.has_wording_ambiguity,
                        has_source_fragility = EXCLUDED.has_source_fragility,
                        has_bulletin_board_dependency = EXCLUDED.has_bulletin_board_dependency,
                        flags = EXCLUDED.flags,
                        summary = EXCLUDED.summary,
                        engine_version = EXCLUDED.engine_version,
                        raw_inputs_json = EXCLUDED.raw_inputs_json,
                        updated_at = NOW()
                    """,
                    {
                        **row,
                        "flags": Json(row["flags"]),
                        "raw_inputs_json": Json(row["raw_inputs_json"]),
                    },
                )
            conn.commit()

    return {
        "status": "ok",
        "day": str(target_day),
        "market_id": market_id,
        "rows_written": len(upserts),
        "engine_version": ENGINE_VERSION,
    }