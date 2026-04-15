import json
import hashlib
from typing import Any, Dict, Tuple, List


def build_participants_payload(
    market: Dict[str, Any],
    impact: Dict[str, Any] | None,
) -> Dict[str, Any]:
    recent = (impact or {}).get("recent_cohort_share") or []
    recent_map = {(row.get("cohort") or "").upper(): row for row in recent}

    dominant_whale_share = float(
        (recent_map.get("DOMINANT_WHALE") or {}).get("notional_share") or 0.0
    )
    large_influential_share = float(
        (recent_map.get("LARGE_INFLUENTIAL") or {}).get("notional_share") or 0.0
    )
    coordinated_actor_share = float(
        (recent_map.get("COORDINATED_ACTOR") or {}).get("notional_share") or 0.0
    )
    active_speculator_share = float(
        (recent_map.get("ACTIVE_SPECULATOR") or {}).get("notional_share") or 0.0
    )
    directional_speculator_share = float(
        (recent_map.get("DIRECTIONAL_SPECULATOR") or {}).get("notional_share") or 0.0
    )
    maker_like_share = float(
        (recent_map.get("MAKER_LIKE") or {}).get("notional_share") or 0.0
    )
    organic_participant_share = float(
        (recent_map.get("ORGANIC_PARTICIPANT") or {}).get("notional_share") or 0.0
    )

    whale_concentration = dominant_whale_share + large_influential_share
    speculative_share = coordinated_actor_share + active_speculator_share + directional_speculator_share
    organic_share = maker_like_share + organic_participant_share

    participant_flags: List[str] = []

    if dominant_whale_share > 0.35:
        participant_flags.append("dominant_whale_concentration")

    if whale_concentration > 0.50:
        participant_flags.append("elevated_whale_participation")

    if speculative_share > 0.35:
        participant_flags.append("elevated_speculative_participation")

    if organic_share < 0.20:
        participant_flags.append("thin_organic_participation")

    if maker_like_share < 0.10:
        participant_flags.append("thin_maker_layer")

    return {
        "dominant_whale_share": dominant_whale_share,
        "large_influential_share": large_influential_share,
        "coordinated_actor_share": coordinated_actor_share,
        "active_speculator_share": active_speculator_share,
        "directional_speculator_share": directional_speculator_share,
        "maker_like_share": maker_like_share,
        "organic_participant_share": organic_participant_share,
        "whale_concentration": whale_concentration,
        "speculative_share": speculative_share,
        "organic_share": organic_share,
        "participant_flags": participant_flags,
    }


def compute_alignment_drivers(
    market: Dict[str, Any],
    social: Dict[str, Any],
    alignment: Dict[str, Any],
    resolution: Dict[str, Any],
    participants: Dict[str, Any],
    meta: Dict[str, Any],
) -> List[str]:
    drivers: List[str] = []

    structural_state = (
        (alignment.get("structural_state") or market.get("structural_state") or "")
        .strip()
        .lower()
    )
    alignment_state = (alignment.get("state") or "").strip().lower()
    demand_state = (social.get("demand_state") or "absent").strip().lower()

    dominant_whale_share = float(participants.get("dominant_whale_share") or 0.0)
    whale_concentration = float(participants.get("whale_concentration") or 0.0)
    spec_share = float(participants.get("speculative_share") or 0.0)
    organic_share = float(participants.get("organic_share") or 0.0)
    maker_like_share = float(participants.get("maker_like_share") or 0.0)

    strong_demand = demand_state in {"strong", "established"}
    any_demand = demand_state in {"strong", "established", "building"}
    low_demand = demand_state in {"absent", "limited"}

    strong_structure = structural_state == "strong"
    moderate_structure = structural_state == "moderate"
    weak_structure = structural_state == "weak"

    gap = alignment.get("gap")
    gap_val = None
    if gap is not None:
        try:
            gap_val = float(gap)
        except Exception:
            gap_val = None

    # --- PRIMARY STATE DRIVERS ---
    if alignment_state == "confirmed":
        drivers.append("demand supported by participation")
    elif alignment_state == "conviction_mismatch":
        drivers.append("demand not fully supported by participation quality")
    elif alignment_state == "structure_led":
        drivers.append("demand is insufficient for structure")
    elif alignment_state == "weak":
        drivers.append("limited combined evidence from structure and demand")

    # --- GAP / MISMATCH ---
    if gap_val is not None and any_demand and not strong_structure:
        if gap_val >= 0.20:
            drivers.append("severe demand-structure mismatch")
        elif gap_val >= 0.10:
            drivers.append("clear demand-structure mismatch")

    # --- BOTTLENECK ---
    bottleneck = (alignment.get("bottleneck_type") or "").strip().lower()
    if bottleneck == "structure" and any_demand and not strong_structure:
        drivers.append("structure is limiting demand conversion")
    elif bottleneck == "demand" and (strong_structure or moderate_structure) and low_demand:
        drivers.append("demand is insufficient for structure")
    elif bottleneck == "integrity":
        drivers.append("participation quality reduces confidence in demand")

    # --- PARTICIPATION QUALITY ---
    if dominant_whale_share >= 0.35:
        drivers.append("dominant whale concentration")

    elif whale_concentration >= 0.40:
        drivers.append("market dominated by large participants")

    if spec_share >= 0.40:
        drivers.append("speculative activity dominating participation")

    if organic_share <= 0.20:
        drivers.append("lack of balanced participation base")

    if maker_like_share <= 0.10:
        drivers.append("thin maker participation")

    # --- STRUCTURE QUALITY ---
    hhi = market.get("concentration_hhi")
    if hhi is not None:
        try:
            if float(hhi) >= 0.85:
                drivers.append("extreme concentration in trading activity")
        except Exception:
            pass

    integrity_band = (market.get("integrity_band") or "").lower()
    if integrity_band in {"fragile", "weak"}:
        drivers.append("structural integrity remains weak")

    # --- DEMAND QUALITY ---
    source_count = social.get("source_count")
    if source_count is not None:
        try:
            if int(source_count) <= 2:
                drivers.append("attention concentrated in very few sources")
        except Exception:
            pass

    coherence = social.get("narrative_coherence_score")
    if coherence is not None:
        try:
            if float(coherence) < 45:
                drivers.append("inconsistent or fragmented narrative signals")
        except Exception:
            pass

    fragility = social.get("social_fragility_score")
    if fragility is not None:
        try:
            if float(fragility) >= 60:
                drivers.append("attention is unstable or short-lived")
        except Exception:
            pass

    # --- RESOLUTION ---
    resolution_state = (resolution.get("state") or "").strip().lower()
    oracle_risk_score = resolution.get("oracle_risk_score")
    clarity_score = resolution.get("clarity_score")

    try:
        oracle_risk_score_val = float(oracle_risk_score) if oracle_risk_score is not None else None
    except Exception:
        oracle_risk_score_val = None

    try:
        clarity_score_val = float(clarity_score) if clarity_score is not None else None
    except Exception:
        clarity_score_val = None

    if resolution_state == "fragile":
        drivers.append("fragile resolution path")

    if resolution.get("disputed") is True:
        drivers.append("market entered dispute")

    if resolution.get("has_wording_ambiguity") is True:
        drivers.append("wording ambiguity increases resolution risk")

    if resolution.get("has_time_ambiguity") is True:
        drivers.append("time ambiguity increases resolution risk")

    if resolution.get("has_source_fragility") is True:
        drivers.append("source fragility weakens settlement confidence")

    if resolution.get("has_bulletin_board_dependency") is True:
        drivers.append("resolution depends on bulletin board interpretation")

    if oracle_risk_score_val is not None and oracle_risk_score_val >= 0.70:
        drivers.append("high oracle risk")

    if clarity_score_val is not None and clarity_score_val <= 0.50:
        drivers.append("low resolution clarity")

    # --- META ---
    if meta.get("is_mixed_horizon"):
        drivers.append("structure and demand measured on different time horizons")

    # --- ALIGNMENT FLAGS ---
    for flag in alignment.get("flags") or []:
        if flag == "DEMAND_AHEAD_OF_STRUCTURE" and any_demand and not strong_structure:
            drivers.append("demand leading structure")
        elif flag == "DEMAND_LAGGING_STRUCTURE" and (strong_structure or moderate_structure):
            drivers.append("demand is insufficient for structure")
        elif flag == "STRUCTURAL_COVERAGE_MISSING":
            drivers.append("insufficient structural coverage")
        elif flag == "LOW_CONFIDENCE_PROXY":
            drivers.append("demand signal confidence remains limited")

    # --- STATE-SENSITIVE CLEANUP ---
    if strong_structure and strong_demand:
        drivers = [
            d for d in drivers
            if d not in {
                "severe demand-structure mismatch",
                "clear demand-structure mismatch",
                "demand leading structure",
                "demand not fully supported by participation quality",
                "structure is limiting demand conversion",
            }
        ]
        if "demand supported by participation" not in drivers:
            drivers.insert(0, "demand supported by participation")

    elif (moderate_structure or strong_structure) and low_demand:
        drivers = [
            d for d in drivers
            if d not in {
                "severe demand-structure mismatch",
                "clear demand-structure mismatch",
                "demand leading structure",
                "demand supported by participation",
            }
        ]
        if "demand is insufficient for structure" not in drivers:
            drivers.insert(0, "demand is insufficient for structure")

    elif weak_structure and any_demand:
        if "demand not fully supported by participation quality" not in drivers:
            drivers.insert(0, "demand not fully supported by participation quality")
        if "structure is limiting demand conversion" not in drivers:
            drivers.append("structure is limiting demand conversion")

    # --- DEDUPE ---
    seen = set()
    out: List[str] = []
    for driver in drivers:
        if driver not in seen:
            seen.add(driver)
            out.append(driver)

    return out


def hash_context_packet(packet: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(packet, sort_keys=True, default=str).encode()
    ).hexdigest()


def build_market_ai_context_packet(
    market: Dict[str, Any] | None,
    social_intelligence: Dict[str, Any] | None,
    alignment_intelligence: Dict[str, Any] | None,
    resolution_intelligence: Dict[str, Any] | None,
    impact: Dict[str, Any] | None,
    meta: Dict[str, Any] | None,
) -> Tuple[Dict[str, Any], str, List[str]]:
    market = market or {}
    social_intelligence = social_intelligence or {}
    alignment_intelligence = alignment_intelligence or {}
    resolution_intelligence = resolution_intelligence or {}
    impact = impact or {}
    meta = meta or {}

    if social_intelligence:
        social_intelligence["demand_state"] = (
            social_intelligence.get("demand_state")
            or social_intelligence.get("recommendation")
            or social_intelligence.get("social_state")
            or "absent"
        )

    has_market_coverage = bool(market)
    has_social_coverage = bool(social_intelligence)
    has_alignment_coverage = bool(alignment_intelligence)
    has_resolution_coverage = bool(resolution_intelligence)

    resolution_only_mode = (
        has_resolution_coverage
        and not has_market_coverage
        and not has_social_coverage
        and not has_alignment_coverage
    )

    participants = (
        {}
        if resolution_only_mode
        else build_participants_payload(market, impact)
    )

    packet_alignment = {
        "state": (
            None
            if resolution_only_mode
            else (
                alignment_intelligence.get("alignment_state")
                or alignment_intelligence.get("state")
                or "weak"
            )
        ),
        "structural_state": (
            None
            if resolution_only_mode
            else (
                alignment_intelligence.get("structural_state")
                or market.get("structural_state")
                or "weak"
            )
        ),
        "demand_state": (
            None
            if resolution_only_mode
            else (
                social_intelligence.get("demand_state")
                or social_intelligence.get("recommendation")
                or social_intelligence.get("social_state")
                or "absent"
            )
        ),
        "score": alignment_intelligence.get("alignment_score"),
        "gap": alignment_intelligence.get("attention_vs_structure_gap"),
        "bottleneck_type": alignment_intelligence.get("bottleneck_type"),
        "flags": alignment_intelligence.get("flags", []),
        "summary": alignment_intelligence.get("summary"),
    }

    packet_resolution = {
        "state": resolution_intelligence.get("resolution_state"),
        "clarity_score": resolution_intelligence.get("resolution_clarity_score"),
        "oracle_risk_score": resolution_intelligence.get("oracle_risk_score"),
        "dispute_propensity_score": resolution_intelligence.get("dispute_propensity_score"),
        "resolution_complexity": resolution_intelligence.get("resolution_complexity"),
        "disputed": resolution_intelligence.get("disputed"),
        "settled": resolution_intelligence.get("settled"),
        "flags": resolution_intelligence.get("flags", []),
        "summary": resolution_intelligence.get("summary"),
        "has_time_ambiguity": resolution_intelligence.get("has_time_ambiguity"),
        "has_wording_ambiguity": resolution_intelligence.get("has_wording_ambiguity"),
        "has_source_fragility": resolution_intelligence.get("has_source_fragility"),
        "has_bulletin_board_dependency": resolution_intelligence.get("has_bulletin_board_dependency"),
        "request_to_settlement_hours": resolution_intelligence.get("request_to_settlement_hours"),
        "dispute_to_settlement_hours": resolution_intelligence.get("dispute_to_settlement_hours"),
    }
    
    drivers = compute_alignment_drivers(
        market,
        social_intelligence,
        packet_alignment,
        packet_resolution,
        participants,
        meta,
    )

    packet: Dict[str, Any] = {
"market": {
    "market_id": (
        market.get("market_id")
        or meta.get("market_id")
    ),
            "title": market.get("title") or market.get("question"),
            "category": market.get("category"),
            "url": market.get("url"),
            "structural_state": (
                alignment_intelligence.get("structural_state")
                or market.get("structural_state")
            ),
            "concentration_hhi": market.get("concentration_hhi"),
            "integrity_band": market.get("integrity_band"),
            "market_quality_score": market.get("market_quality_score"),
            "liquidity_health_score": market.get("liquidity_health_score"),
            "concentration_risk_score": market.get("concentration_risk_score"),
        },
        "structure": (
            {}
            if resolution_only_mode
            else {
                "state": (
                    alignment_intelligence.get("structural_state")
                    or market.get("structural_state")
                ),
                "score": market.get("market_quality_score"),
                "concentration_hhi": market.get("concentration_hhi"),
                "market_quality_score": market.get("market_quality_score"),
                "liquidity_health_score": market.get("liquidity_health_score"),
                "concentration_risk_score": market.get("concentration_risk_score"),
                "integrity_band": market.get("integrity_band"),
            }
        ),
        "demand": (
            {}
            if resolution_only_mode
            else {
                "state": (
                    social_intelligence.get("demand_state")
                    or social_intelligence.get("recommendation")
                    or social_intelligence.get("social_state")
                    or "absent"
                ),
                "score": social_intelligence.get("demand_score"),
                "attention_score": social_intelligence.get("attention_score"),
                "mention_count": social_intelligence.get("mention_count"),
                "source_count": social_intelligence.get("source_count"),
                "strength_score": social_intelligence.get("demand_strength_score"),
                "breadth_score": social_intelligence.get("demand_breadth_score"),
                "quality_score": social_intelligence.get("demand_quality_score"),
                "narrative_coherence_score": social_intelligence.get("narrative_coherence_score"),
                "social_fragility_score": social_intelligence.get("social_fragility_score"),
                "flags": social_intelligence.get("flags", []),
                "summary": social_intelligence.get("summary"),
            }
        ),
        "alignment": {
            **packet_alignment,
            "drivers": drivers,
        },
        "resolution": packet_resolution,
        "participants": participants if not resolution_only_mode else {},
        "meta": {
            "is_mixed_horizon": bool(meta.get("is_mixed_horizon")),
            "structural_day": meta.get("structural_day"),
            "social_day": meta.get("social_day"),
            "alignment_day": meta.get("alignment_day"),
            "resolution_day": meta.get("resolution_day"),
        },
    }

    return packet, hash_context_packet(packet), drivers