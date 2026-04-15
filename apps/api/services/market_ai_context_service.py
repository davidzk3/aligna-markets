import json
import os
from datetime import date, datetime
from decimal import Decimal

import psycopg

from apps.api.db import get_db_dsn


MODEL_NAME = "gpt-4o-mini"
PROMPT_VERSION = "v7_fallback_reasoning"

SYSTEM_PROMPT = """
You are a prediction market context interpreter.

You analyze relationship between:
- structure (market quality)
- demand (external attention)
- alignment (relationship between them)

You DO NOT:
- give trading advice
- recommend actions
- assume demand should convert into structure

You MUST:
- explain whether demand reflects real conviction
- explain whether mismatch is meaningful or expected
- abstain if unclear

Return ONLY valid JSON.
"""


def make_json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [make_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [make_json_safe(v) for v in value]
    return value


def build_user_prompt(packet):
    safe_packet = make_json_safe(packet)
    return f"""
Analyze this market:

{json.dumps(safe_packet, indent=2)}

Return JSON with:

{{
  "interpretation": "...",
  "interpretation_type": "confirmed | conviction_mismatch | structure_led | weak | unclear | abstain",
  "confidence": 0.0,
  "does_demand_reflect_conviction": true,
  "does_alignment_imply_convergence": false,
  "drivers": [
    {{
      "key": "...",
      "label": "...",
      "impact": "high | medium | low",
      "direction": "positive | negative | fragility | uncertainty | neutral"
    }}
  ],
  "caution_flags": ["..."],
  "abstain": false,
  "abstain_reason": null
}}
"""

def normalize_driver_key(driver: str) -> str:
    mapping = {
        "demand supported by participation": "demand_supported_by_participation",
        "structure is limiting demand conversion": "structure_limits_conversion",
        "demand is insufficient for structure": "demand_insufficient_for_structure",
        "market dominated by large participants": "market_dominated_by_large_participants",
        "speculative activity dominating participation": "speculative_activity_dominating_participation",
        "lack of balanced participation base": "lack_of_balanced_participation_base",
        "extreme concentration in trading activity": "extreme_concentration_in_trading_activity",
        "attention concentrated in very few sources": "attention_concentrated_in_few_sources",
        "inconsistent or fragmented narrative signals": "fragmented_narrative_signals",
        "attention is unstable or short-lived": "unstable_attention",
        "structure and demand measured on different time horizons": "mixed_horizon",
        "demand leading structure": "demand_leading_structure",
        "insufficient structural coverage": "insufficient_structural_coverage",
        "limited combined evidence from structure and demand": "limited_combined_evidence",
        "clear demand-structure mismatch": "clear_demand_structure_mismatch",
        "severe demand-structure mismatch": "severe_demand_structure_mismatch",
        "demand not fully supported by participation quality": "demand_not_supported_by_participation_quality",
        "thin maker participation": "thin_maker_participation",
        "fragile resolution path": "fragile_resolution_path",
        "market entered dispute": "market_entered_dispute",
        "wording ambiguity increases resolution risk": "wording_ambiguity_increases_resolution_risk",
        "time ambiguity increases resolution risk": "time_ambiguity_increases_resolution_risk",
        "source fragility weakens settlement confidence": "source_fragility_weakens_settlement_confidence",
        "resolution depends on bulletin board interpretation": "bulletin_board_dependency",
        "high oracle risk": "high_oracle_risk",
        "low resolution clarity": "low_resolution_clarity",
    }
    return mapping.get(driver, driver.strip().lower().replace(" ", "_").replace("-", "_"))

def driver_label_from_key(key: str) -> str:
    mapping = {
        "demand_supported_by_participation": "demand supported by participation",
        "structure_limits_conversion": "structure limits demand conversion",
        "demand_insufficient_for_structure": "demand insufficient for structure",
        "market_dominated_by_large_participants": "market dominated by large participants",
        "speculative_activity_dominating_participation": "speculative activity dominating participation",
        "lack_of_balanced_participation_base": "lack of balanced participation base",
        "extreme_concentration_in_trading_activity": "extreme concentration in trading activity",
        "attention_concentrated_in_few_sources": "attention concentrated in few sources",
        "fragmented_narrative_signals": "fragmented narrative signals",
        "unstable_attention": "unstable attention",
        "mixed_horizon": "mixed horizon",
        "demand_leading_structure": "demand leading structure",
        "insufficient_structural_coverage": "insufficient structural coverage",
        "limited_combined_evidence": "limited combined evidence",
        "clear_demand_structure_mismatch": "clear demand-structure mismatch",
        "severe_demand_structure_mismatch": "severe demand-structure mismatch",
        "demand_not_supported_by_participation_quality": "demand not fully supported by participation quality",
        "thin_maker_participation": "thin maker participation",
        "fragile_resolution_path": "fragile resolution path",
        "market_entered_dispute": "market entered dispute",
        "wording_ambiguity_increases_resolution_risk": "wording ambiguity increases resolution risk",
        "time_ambiguity_increases_resolution_risk": "time ambiguity increases resolution risk",
        "source_fragility_weakens_settlement_confidence": "source fragility weakens settlement confidence",
        "bulletin_board_dependency": "resolution depends on bulletin board interpretation",
        "high_oracle_risk": "high oracle risk",
        "low_resolution_clarity": "low resolution clarity",
    }
    return mapping.get(key, key.replace("_", " "))

def score_driver_key(key: str) -> dict:
    mapping = {
        "demand_supported_by_participation": ("high", "positive"),
        "structure_limits_conversion": ("high", "negative"),
        "demand_insufficient_for_structure": ("high", "negative"),
        "market_dominated_by_large_participants": ("medium", "fragility"),
        "speculative_activity_dominating_participation": ("medium", "fragility"),
        "lack_of_balanced_participation_base": ("high", "fragility"),
        "extreme_concentration_in_trading_activity": ("high", "fragility"),
        "attention_concentrated_in_few_sources": ("medium", "uncertainty"),
        "fragmented_narrative_signals": ("medium", "uncertainty"),
        "unstable_attention": ("medium", "fragility"),
        "mixed_horizon": ("medium", "uncertainty"),
        "demand_leading_structure": ("high", "negative"),
        "insufficient_structural_coverage": ("medium", "uncertainty"),
        "limited_combined_evidence": ("medium", "uncertainty"),
        "clear_demand_structure_mismatch": ("high", "negative"),
        "severe_demand_structure_mismatch": ("high", "negative"),
        "demand_not_supported_by_participation_quality": ("high", "negative"),
        "thin_maker_participation": ("medium", "fragility"),
        "fragile_resolution_path": ("high", "fragility"),
        "market_entered_dispute": ("high", "fragility"),
        "wording_ambiguity_increases_resolution_risk": ("medium", "uncertainty"),
        "time_ambiguity_increases_resolution_risk": ("medium", "uncertainty"),
        "source_fragility_weakens_settlement_confidence": ("medium", "uncertainty"),
        "bulletin_board_dependency": ("medium", "uncertainty"),
        "high_oracle_risk": ("high", "fragility"),
        "low_resolution_clarity": ("high", "uncertainty"),
    }

    impact, direction = mapping.get(key, ("low", "neutral"))
    return {
        "key": key,
        "label": driver_label_from_key(key),
        "impact": impact,
        "direction": direction,
    }


def rank_scored_drivers(scored_drivers: list[dict]) -> list[dict]:
    impact_rank = {"high": 0, "medium": 1, "low": 2}
    direction_rank = {
        "negative": 0,
        "fragility": 1,
        "uncertainty": 2,
        "positive": 3,
        "neutral": 4,
    }

    return sorted(
        scored_drivers,
        key=lambda d: (
            impact_rank.get(d.get("impact", "low"), 9),
            direction_rank.get(d.get("direction", "neutral"), 9),
            d.get("label", ""),
        ),
    )


def score_and_rank_drivers(drivers: list[str]) -> list[dict]:
    normalized = []
    seen = set()

    for driver in drivers or []:
        key = normalize_driver_key(driver)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(score_driver_key(key))

    return rank_scored_drivers(normalized)


def driver_keys(scored_drivers: list[dict]) -> list[str]:
    return [d.get("key") for d in scored_drivers if d.get("key")]


def driver_labels(scored_drivers: list[dict]) -> list[str]:
    return [d.get("label") for d in scored_drivers if d.get("label")]


def build_contextual_summary(ai_context: dict) -> str | None:
    if not ai_context:
        return None

    interpretation_type = ai_context.get("interpretation_type")
    abstain = ai_context.get("abstain")
    drivers = ai_context.get("drivers") or []
    labels = driver_labels(drivers)

    if abstain or interpretation_type == "abstain":
        return "Context insufficient to determine whether attention is converting into credible participation."

    if interpretation_type == "confirmed":
        return "Attention appears to be supported by credible participation."

    if interpretation_type == "conviction_mismatch":
        return "Demand appears ahead of participation quality."

    if interpretation_type == "structure_led":
        return "Structure appears healthier than current demand activation."

    if interpretation_type == "weak":
        return "Attention and participation both remain limited."

    if interpretation_type == "unclear":
        if labels:
            return f"Context remains mixed: {labels[0]}."
        return "Context remains mixed."

    return None


def build_fallback_interpretation(
    alignment_state: str | None,
    scored_drivers: list[dict],
    category: str,
) -> str:
    if alignment_state == "resolution_risk":
        return (
            "The resolution path is operationally fragile. "
            "Dispute activity and settlement risk reduce confidence in a clean, low-friction resolution."
        )

    keys = set(driver_keys(scored_drivers))

    is_sports = category == "sports"
    is_politics = category in {"politics", "elections", "government"}

    if alignment_state == "confirmed":
        if "lack_of_balanced_participation_base" in keys:
            return (
                "Demand and participation are aligned overall, but the market still shows a thinner balanced base "
                "than ideal. Attention is converting into participation, though the structure is not fully broad."
            )
        return (
            "Demand and participation are aligned. Market activity appears to reflect credible conviction rather "
            "than transient attention alone."
        )

    if alignment_state == "conviction_mismatch":
        if (
            "severe_demand_structure_mismatch" in keys
            and "lack_of_balanced_participation_base" in keys
        ):
            return (
                "External demand is materially ahead of market structure. "
                "Participation remains too narrow to support attention as durable conviction."
            )

        if (
            "severe_demand_structure_mismatch" in keys
            and "market_dominated_by_large_participants" in keys
        ):
            return (
                "External demand is materially ahead of market structure. "
                "Participation is present, but remains too concentrated to treat attention as fully durable."
            )

        if "severe_demand_structure_mismatch" in keys:
            return (
                "External demand is materially ahead of market structure. "
                "Attention is present, but participation quality is not deep enough to support it."
            )

        if (
            "clear_demand_structure_mismatch" in keys
            and "market_dominated_by_large_participants" in keys
        ):
            return (
                "Demand is ahead of structure, but participation remains too concentrated "
                "to treat current attention as broad conviction."
            )

        if "clear_demand_structure_mismatch" in keys:
            return (
                "Demand is ahead of structure. "
                "Attention is present, but participation quality remains slightly behind."
            )

        if "demand_leading_structure" in keys:
            return (
                "Demand is arriving faster than the market can absorb it. "
                "Attention is outpacing the current participation base."
            )

        if "structure_limits_conversion" in keys:
            return (
                "Demand is present, but market structure is limiting conversion into durable participation."
            )

        if "demand_not_supported_by_participation_quality" in keys:
            return (
                "Attention appears real, but participation quality does not yet confirm broad conviction."
            )

        if "lack_of_balanced_participation_base" in keys:
            return (
                "Demand is visible, but participation remains too narrow to treat the signal as fully durable."
            )

        return (
            "Demand appears ahead of participation quality. "
            "External attention is not yet translating into durable market support."
        )

    if alignment_state == "structure_led":
        if "demand_insufficient_for_structure" in keys:
            return (
                "Structure appears stable, but external demand is not yet keeping pace. "
                "The market may be better prepared than current attention levels suggest."
            )
        return (
            "Structure appears healthier than current demand activation. "
            "Market quality is ahead of external attention."
        )

    if (
        "fragile_resolution_path" in keys
        or "market_entered_dispute" in keys
        or "high_oracle_risk" in keys
    ):
        return (
            "The resolution path is operationally fragile. "
            "Dispute activity and settlement risk reduce confidence in a clean, low-friction resolution."
        )

    if alignment_state == "weak":
        if "limited_combined_evidence" in keys:
            return (
                "Signals remain weak across both demand and participation. "
                "There is not enough combined evidence to treat current activity as robust."
            )
        return (
            "Attention and participation both remain limited. "
            "The current setup does not support a strong claim of durable conviction."
        )

    return (
        "Signals remain mixed, with no clear convergence between demand and participation."
    )


def build_fallback_confidence(
    alignment_state: str | None,
    scored_drivers: list[dict],
    caution_flags: list[str],
) -> float:
    confidence = {
        "confirmed": 0.78,
        "conviction_mismatch": 0.70,
        "structure_led": 0.66,
        "weak": 0.60,
        "unclear": 0.56,
        "abstain": 0.45,
    }.get(alignment_state or "unclear", 0.56)

    keys = set(driver_keys(scored_drivers))

    if "severe_demand_structure_mismatch" in keys:
        confidence += 0.05
    elif "clear_demand_structure_mismatch" in keys:
        confidence += 0.03

    if "mixed_horizon" in caution_flags:
        confidence -= 0.03

    if "thin_neutral_base" in caution_flags and alignment_state != "confirmed":
        confidence -= 0.03

    if "fragile_demand" in caution_flags:
        confidence -= 0.02

    if "insufficient_structural_coverage" in keys:
        confidence -= 0.04

    confidence = max(0.35, min(0.86, confidence))
    return round(confidence, 2)

def infer_fallback_alignment_state(packet, scored_drivers: list[dict]) -> str:

    keys = set(driver_keys(scored_drivers))

    # 🔥 NEW: resolution-first classification
    if (
        "fragile_resolution_path" in keys
        or "market_entered_dispute" in keys
        or "high_oracle_risk" in keys
    ):
        return "resolution_risk"

    alignment = packet.get("alignment", {}) or {}
    demand = packet.get("demand", {}) or {}
    participants = packet.get("participants", {}) or {}

    raw_alignment_state = (alignment.get("state") or "").strip().lower()
    structural_state = (
        (alignment.get("structural_state") or packet.get("market", {}).get("structural_state") or "")
        .strip()
        .lower()
    )
    demand_state = (demand.get("state") or "").strip().lower()

    whale_concentration = float(participants.get("whale_concentration") or 0.0)
    organic_share = float(participants.get("organic_share") or 0.0)

    keys = set(driver_keys(scored_drivers))

    strong_demand = demand_state in {"strong", "established", "building"}
    strong_structure = structural_state == "strong"
    moderate_structure = structural_state == "moderate"

    severe_mismatch = (
        "severe_demand_structure_mismatch" in keys
        or "clear_demand_structure_mismatch" in keys
        or "demand_leading_structure" in keys
        or "demand_not_supported_by_participation_quality" in keys
        or "structure_limits_conversion" in keys
    )

    fragility_only = (
        ("lack_of_balanced_participation_base" in keys or "market_dominated_by_large_participants" in keys)
        and not severe_mismatch
    )

    if strong_structure and demand_state in {"strong", "established"}:
        return "confirmed"

    if strong_structure and strong_demand and fragility_only:
        return "confirmed"

    if moderate_structure and demand_state in {"absent", "limited"}:
        return "structure_led"

    if strong_demand and moderate_structure:
        return "conviction_mismatch"

    if severe_mismatch:
        return "conviction_mismatch"

    if raw_alignment_state in {"confirmed", "conviction_mismatch", "structure_led", "weak"}:
        return raw_alignment_state

    return "weak"


def rebalance_fallback_drivers(
    packet,
    scored_drivers: list[dict],
    interpretation_type: str,
) -> list[dict]:
    participants = packet.get("participants", {}) or {}
    meta = packet.get("meta", {}) or {}

    whale_concentration = float(participants.get("whale_concentration") or 0.0)
    organic_share = float(participants.get("organic_share") or 0.0)
    neutral_share = 1.0 - organic_share - whale_concentration

    existing_keys = set(driver_keys(scored_drivers))
    final_drivers = list(scored_drivers)

    def append_driver(key: str):
        if key not in existing_keys:
            final_drivers.append(score_driver_key(key))
            existing_keys.add(key)

    if interpretation_type == "confirmed":
        final_drivers = [
            d
            for d in final_drivers
            if d.get("key")
            not in {
                "demand_leading_structure",
                "demand_not_supported_by_participation_quality",
                "clear_demand_structure_mismatch",
                "severe_demand_structure_mismatch",
                "structure_limits_conversion",
            }
        ]
        existing_keys = set(driver_keys(final_drivers))

        append_driver("demand_supported_by_participation")

        if neutral_share <= 0.20:
            append_driver("lack_of_balanced_participation_base")

        if whale_concentration >= 0.40:
            append_driver("market_dominated_by_large_participants")

    elif interpretation_type == "conviction_mismatch":
        final_drivers = [
            d
            for d in final_drivers
            if d.get("key") != "demand_supported_by_participation"
        ]
        existing_keys = set(driver_keys(final_drivers))

        if "demand_leading_structure" not in existing_keys and "structure_limits_conversion" not in existing_keys:
            append_driver("demand_not_supported_by_participation_quality")
            append_driver("structure_limits_conversion")

    elif interpretation_type == "structure_led":
        final_drivers = [
            d
            for d in final_drivers
            if d.get("key")
            not in {
                "demand_supported_by_participation",
                "demand_leading_structure",
                "clear_demand_structure_mismatch",
                "severe_demand_structure_mismatch",
            }
        ]
        existing_keys = set(driver_keys(final_drivers))
        append_driver("demand_insufficient_for_structure")

    elif interpretation_type == "weak":
        final_drivers = [
            d
            for d in final_drivers
            if d.get("key") != "demand_supported_by_participation"
        ]
        existing_keys = set(driver_keys(final_drivers))
        append_driver("limited_combined_evidence")

    if meta.get("is_mixed_horizon"):
        append_driver("mixed_horizon")

    return rank_scored_drivers(final_drivers)


def prioritize_scored_drivers(
    scored_drivers: list[dict],
    interpretation_type: str,
) -> list[dict]:
    if not scored_drivers:
        return []

    if interpretation_type == "confirmed":
        key_priority = {
            "demand_supported_by_participation": 0,
            "lack_of_balanced_participation_base": 1,
            "market_dominated_by_large_participants": 2,
            "speculative_activity_dominating_participation": 3,
            "extreme_concentration_in_trading_activity": 4,
            "unstable_attention": 5,
            "insufficient_structural_coverage": 6,
            "mixed_horizon": 7,
            "attention_concentrated_in_few_sources": 8,
            "fragmented_narrative_signals": 9,
            "limited_combined_evidence": 10,
        }
    elif interpretation_type == "conviction_mismatch":
        key_priority = {
            "fragile_resolution_path": 0,
            "market_entered_dispute": 1,
            "high_oracle_risk": 2,
            "low_resolution_clarity": 3,
            "severe_demand_structure_mismatch": 4,
            "clear_demand_structure_mismatch": 5,
            "demand_leading_structure": 6,
            "demand_not_supported_by_participation_quality": 7,
            "structure_limits_conversion": 8,
            "lack_of_balanced_participation_base": 9,
            "thin_maker_participation": 10,
            "market_dominated_by_large_participants": 11,
            "wording_ambiguity_increases_resolution_risk": 12,
            "time_ambiguity_increases_resolution_risk": 13,
            "source_fragility_weakens_settlement_confidence": 14,
            "bulletin_board_dependency": 15,
            "mixed_horizon": 16,
        }
    elif interpretation_type == "structure_led":
        key_priority = {
            "demand_insufficient_for_structure": 0,
            "lack_of_balanced_participation_base": 1,
            "market_dominated_by_large_participants": 2,
            "insufficient_structural_coverage": 3,
            "mixed_horizon": 4,
        }
    elif interpretation_type == "weak":
        key_priority = {
            "limited_combined_evidence": 0,
            "insufficient_structural_coverage": 1,
            "mixed_horizon": 2,
        }
    else:
        key_priority = {}

    impact_rank = {"high": 0, "medium": 1, "low": 2}

    return sorted(
        scored_drivers,
        key=lambda d: (
            key_priority.get(d.get("key"), 50),
            impact_rank.get(d.get("impact", "low"), 9),
            d.get("label", ""),
        ),
    )

def fallback_ai_context(packet, drivers):
    alignment = packet.get("alignment", {}) or {}
    demand = packet.get("demand", {}) or {}
    resolution = packet.get("resolution", {}) or {}
    participants = packet.get("participants", {}) or {}
    meta = packet.get("meta", {}) or {}
    market = packet.get("market", {}) or {}

    demand_state = (demand.get("state") or "absent").strip().lower()
    category = (market.get("category") or "").lower()

    whale_concentration = float(participants.get("whale_concentration") or 0.0)
    organic_share = float(participants.get("organic_share") or 0.0)
    neutral_share = max(0.0, 1.0 - whale_concentration - organic_share)
    fragility = float(demand.get("social_fragility_score") or 0.0)

    # 🔥 NEW: strict resolution-only detection
    resolution_only_mode = (
        not packet.get("structure")
        and not packet.get("demand")
        and bool(packet.get("resolution"))
    )

    resolution_only_mode = (
        not packet.get("structure")
        or packet.get("structure", {}).get("state") in {None, ""}
    ) and bool(packet.get("resolution"))
    caution_flags = []

    if meta.get("is_mixed_horizon"):
        caution_flags.append("mixed_horizon")

    if whale_concentration >= 0.40:
        caution_flags.append("elevated_whale_participation")

    if neutral_share <= 0.20:
        caution_flags.append("thin_organic_participation")

    if fragility >= 60:
        caution_flags.append("fragile_demand")

    if resolution.get("disputed") is True:
        caution_flags.append("resolution_disputed")

    if resolution.get("has_wording_ambiguity") is True:
        caution_flags.append("resolution_wording_ambiguity")

    if resolution.get("has_time_ambiguity") is True:
        caution_flags.append("resolution_time_ambiguity")

    scored_drivers = score_and_rank_drivers(drivers)

        # 🔥 NEW: filter drivers for resolution-only markets
    if resolution_only_mode:
        scored_drivers = [
            d for d in scored_drivers
            if d.get("key") in {
                "fragile_resolution_path",
                "market_entered_dispute",
                "high_oracle_risk",
                "low_resolution_clarity",
                "wording_ambiguity_increases_resolution_risk",
                "time_ambiguity_increases_resolution_risk",
            }
        ]

    interpretation_type = infer_fallback_alignment_state(packet, scored_drivers)

    if not resolution_only_mode:
        scored_drivers = rebalance_fallback_drivers(
            packet,
            scored_drivers,
            interpretation_type,
        )

    scored_drivers = prioritize_scored_drivers(
        scored_drivers,
        interpretation_type,
    )

    abstain = False
    abstain_reason = None

    if (
        interpretation_type == "unclear"
        and not scored_drivers
        and demand_state == "absent"
    ):
        abstain = True
        abstain_reason = "insufficient_context"
        interpretation_type = "abstain"

    interpretation = build_fallback_interpretation(
        alignment_state=interpretation_type,
        scored_drivers=scored_drivers,
        category=category,
    )

    confidence = build_fallback_confidence(
        alignment_state=interpretation_type,
        scored_drivers=scored_drivers,
        caution_flags=caution_flags,
    )

    if abstain:
        confidence = 0.45

    does_demand_reflect_conviction = interpretation_type == "confirmed"
    does_alignment_imply_convergence = interpretation_type == "confirmed"

    if interpretation_type in {"conviction_mismatch", "weak", "unclear", "abstain"}:
        does_alignment_imply_convergence = False

    if interpretation_type in {"structure_led", "weak", "unclear", "abstain"}:
        does_demand_reflect_conviction = False

    return {
        "interpretation": interpretation,
        "interpretation_type": interpretation_type,
        "confidence": confidence,
        "does_demand_reflect_conviction": does_demand_reflect_conviction,
        "does_alignment_imply_convergence": does_alignment_imply_convergence,
        "drivers": scored_drivers,
        "caution_flags": caution_flags,
        "abstain": abstain,
        "abstain_reason": abstain_reason,
        "model_provider": "fallback",
        "model_name": "rule_based_v2",
    }


def call_openai_json(system_prompt: str, user_prompt: str) -> dict:
    from openai import OpenAI

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
        response_format={"type": "json_object"},
    )

    content = response.choices[0].message.content
    return json.loads(content)


def persist(market_id, packet, output, packet_hash):
    safe_packet = make_json_safe(packet)
    safe_output = make_json_safe(output)

    model_name = safe_output.get("model_name", "fallback_rules_v1")

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.market_ai_interpretations (
                    market_id,
                    structural_day,
                    social_day,
                    alignment_day,
                    snapshot_hash,
                    input_packet_json,
                    ai_output_json,
                    interpretation_type,
                    confidence,
                    abstain,
                    abstain_reason,
                    model_provider,
                    model_name,
                    prompt_version,
                    is_mixed_horizon
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (market_id, snapshot_hash, prompt_version, model_name)
                DO NOTHING
                RETURNING id
                """,
                (
                    market_id,
                    safe_packet["meta"].get("structural_day"),
                    safe_packet["meta"].get("social_day"),
                    safe_packet["meta"].get("alignment_day"),
                    packet_hash,
                    json.dumps(safe_packet),
                    json.dumps(safe_output),
                    safe_output.get("interpretation_type"),
                    safe_output.get("confidence"),
                    safe_output.get("abstain"),
                    safe_output.get("abstain_reason"),
                    safe_output.get("model_provider", "fallback"),
                    model_name,
                    PROMPT_VERSION,
                    safe_packet["meta"].get("is_mixed_horizon"),
                ),
            )

            row = cur.fetchone()

            if row and row[0]:
                conn.commit()
                return row[0]

            cur.execute(
                """
                SELECT id
                FROM public.market_ai_interpretations
                WHERE market_id = %s
                  AND snapshot_hash = %s
                  AND prompt_version = %s
                  AND model_name = %s
                LIMIT 1
                """,
                (
                    market_id,
                    packet_hash,
                    PROMPT_VERSION,
                    model_name,
                ),
            )

            existing = cur.fetchone()

            if existing:
                conn.commit()
                return existing[0]

            conn.commit()
            raise Exception("Failed to persist or retrieve AI interpretation")


def load_cached_market_ai_context(market_id, packet_hash, model_name):
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    ai_output_json
                FROM public.market_ai_interpretations
                WHERE market_id = %s
                  AND snapshot_hash = %s
                  AND prompt_version = %s
                  AND model_name = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (market_id, packet_hash, PROMPT_VERSION, model_name),
            )
            row = cur.fetchone()
            if not row:
                return None

            audit_id, ai_output_json = row
            if not isinstance(ai_output_json, dict):
                return None

            return {
                **ai_output_json,
                "audit_id": audit_id,
            }


def generate_market_ai_context(packet, packet_hash, drivers, _db=None):
    safe_packet = make_json_safe(packet)
    market_id = safe_packet["market"]["market_id"]

    use_fallback = os.getenv("AI_CONTEXT_MODE", "").lower() == "fallback"
    fallback_model_name = "rule_based_v2"
    active_model_name = fallback_model_name if use_fallback else MODEL_NAME

    cached = load_cached_market_ai_context(
        market_id,
        packet_hash,
        active_model_name,
    )
    if cached:
        merged = {
            **cached,
            "cache_hit": True,
        }
        merged["contextual_summary"] = build_contextual_summary(merged)
        return merged

    if use_fallback:
        output = fallback_ai_context(safe_packet, drivers)
        output["model_provider"] = "fallback"
        output["model_name"] = fallback_model_name
    else:
        try:
            user_prompt = build_user_prompt(safe_packet)
            output = call_openai_json(SYSTEM_PROMPT, user_prompt)
            output["model_provider"] = "openai"
            output["model_name"] = MODEL_NAME

            if not output.get("drivers"):
                output["drivers"] = score_and_rank_drivers(drivers)
        except Exception:
            output = fallback_ai_context(safe_packet, drivers)
            output["model_provider"] = "fallback"
            output["model_name"] = fallback_model_name

    audit_id = persist(
        market_id,
        safe_packet,
        output,
        packet_hash,
    )

    merged = {
        **output,
        "audit_id": audit_id,
        "cache_hit": False,
    }
    merged["contextual_summary"] = build_contextual_summary(merged)
    return merged