from __future__ import annotations

import hashlib
import json
from typing import Any, Dict

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn
from apps.api.services.market_design_intelligence import (
    extract_design_features,
    score_design,
)
from apps.api.services.market_design_rewrite_intelligence import (
    evaluate_market_design_rewrite,
)

ENGINE_VERSION = "market_resolution_simulation_v1"


def _sha(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def persist_resolution_simulation_run(
    payload: Dict[str, Any],
    simulation: Dict[str, Any],
    design_scores: Dict[str, Any],
    rewrite_eval: Dict[str, Any],
    rewrites: list[Dict[str, Any]],
) -> int:
    q = """
    INSERT INTO public.market_resolution_simulation_runs (
        input_hash,
        title,
        category,
        market_type,
        protocol,
        oracle_family,
        resolution_path,
        expected_settlement_hours,
        confidence,
        design_scores,
        rewrite_eval,
        rewrite_actions,
        request_payload
    )
    VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s
    )
    RETURNING id;
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                q,
                (
                    _sha(payload),
                    payload.get("title"),
                    payload.get("category"),
                    payload.get("market_type"),
                    payload.get("protocol"),
                    payload.get("oracle_family"),
                    simulation.get("resolution_path"),
                    simulation.get("expected_settlement_hours"),
                    simulation.get("confidence"),
                    Json(design_scores),
                    Json(rewrite_eval),
                    Json(rewrites),
                    Json(payload),
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row[0])


def simulate_resolution(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Core simulation layer:
    - evaluates design
    - evaluates rewrite
    - simulates oracle + dispute behavior
    - persists run for later learning
    """

    # --- DESIGN ---
    features = extract_design_features(payload)
    design_scores = score_design(features)

    clarity = float(design_scores.get("design_clarity_score", 0.5) or 0.5)
    risk = float(design_scores.get("expected_resolution_risk_score", 0.5) or 0.5)
    dispute = float(
        design_scores.get("expected_dispute_propensity_score", 0.5) or 0.5
    )

    # --- REWRITE ---
    rewrite_eval = evaluate_market_design_rewrite(payload)
    rewrites = rewrite_eval.get("top_rewrite_actions") or []

    # --- SIMULATION LOGIC ---
    if clarity >= 0.75 and risk <= 0.25 and dispute <= 0.25:
        resolution_path = "clean_resolution"
    elif dispute >= 0.5:
        resolution_path = "disputed_then_resolved"
    elif risk >= 0.5:
        resolution_path = "ambiguous_resolution"
    else:
        resolution_path = "operator_intervention"

    if resolution_path == "clean_resolution":
        settlement_hours = 2
    elif resolution_path == "disputed_then_resolved":
        settlement_hours = 24
    elif resolution_path == "ambiguous_resolution":
        settlement_hours = 12
    else:
        settlement_hours = 6

    simulation = {
        "resolution_path": resolution_path,
        "expected_settlement_hours": settlement_hours,
        "confidence": round(clarity * (1 - risk), 4),
    }

    simulation_run_id = persist_resolution_simulation_run(
        payload=payload,
        simulation=simulation,
        design_scores=design_scores,
        rewrite_eval=rewrite_eval,
        rewrites=rewrites,
    )

    return {
        "simulation_run_id": simulation_run_id,
        "engine_version": ENGINE_VERSION,
        "simulation": simulation,
        "design_scores": design_scores,
        "rewrite_eval": rewrite_eval,
        "rewrite_actions": rewrites,
    }