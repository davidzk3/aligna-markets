from __future__ import annotations

from typing import Any, Dict, Optional

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _is_dispute_path(path: Optional[str]) -> bool:
    if not path:
        return False
    return path in {
        "disputed_then_resolved",
        "ambiguous_resolution",
        "operator_intervention",
    }


def get_simulation_run(simulation_run_id: int) -> Optional[Dict[str, Any]]:
    q = """
    SELECT
        id,
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
        request_payload,
        created_at
    FROM public.market_resolution_simulation_runs
    WHERE id = %s
    LIMIT 1;
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (simulation_run_id,))
            row = cur.fetchone()
            if not row:
                return None

            cols = [d.name for d in cur.description]
            return dict(zip(cols, row))


def persist_resolution_outcome(payload: Dict[str, Any]) -> int:
    q = """
    INSERT INTO public.market_resolution_outcomes (
        market_id,
        condition_id,
        resolved_at,
        outcome,
        actual_resolution_path,
        was_disputed,
        dispute_count,
        settlement_hours,
        oracle_source,
        raw_resolution_data
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                q,
                (
                    payload.get("market_id"),
                    payload.get("condition_id"),
                    payload.get("resolved_at"),
                    payload.get("outcome"),
                    payload.get("actual_resolution_path"),
                    payload.get("actual_dispute"),
                    payload.get("dispute_count"),
                    _safe_float(payload.get("actual_settlement_hours")),
                    payload.get("oracle_source"),
                    Json(payload),
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row[0])


def persist_resolution_evaluation(
    simulation_run_id: int,
    market_id: Optional[str],
    actual_resolution_path: str,
    predicted_resolution_path: str,
    prediction_correct: bool,
    predicted_settlement_hours: Optional[float],
    actual_settlement_hours: Optional[float],
    error_hours: Optional[float],
    predicted_dispute: bool,
    actual_dispute: bool,
    evaluation_summary: str,
    raw_actual_payload: Dict[str, Any],
) -> int:
    q = """
    INSERT INTO public.market_resolution_evaluations (
        simulation_run_id,
        market_id,
        actual_resolution_path,
        predicted_resolution_path,
        prediction_correct,
        predicted_settlement_hours,
        actual_settlement_hours,
        error_hours,
        predicted_dispute,
        actual_dispute,
        evaluation_summary,
        raw_actual_payload
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                q,
                (
                    simulation_run_id,
                    market_id,
                    actual_resolution_path,
                    predicted_resolution_path,
                    prediction_correct,
                    predicted_settlement_hours,
                    actual_settlement_hours,
                    error_hours,
                    predicted_dispute,
                    actual_dispute,
                    evaluation_summary,
                    Json(raw_actual_payload),
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row[0])


def evaluate_resolution_outcome(payload: Dict[str, Any]) -> Dict[str, Any]:
    simulation_run_id = int(payload["simulation_run_id"])
    actual_resolution_path = str(payload["actual_resolution_path"])
    actual_dispute = bool(payload["actual_dispute"])
    actual_settlement_hours = _safe_float(payload.get("actual_settlement_hours"))

    sim = get_simulation_run(simulation_run_id)
    if not sim:
        raise ValueError(f"Simulation run {simulation_run_id} not found")

    predicted_resolution_path = sim["resolution_path"]
    predicted_settlement_hours = _safe_float(sim.get("expected_settlement_hours"))
    predicted_dispute = _is_dispute_path(predicted_resolution_path)

    prediction_correct = predicted_resolution_path == actual_resolution_path

    error_hours = None
    if predicted_settlement_hours is not None and actual_settlement_hours is not None:
        error_hours = round(abs(predicted_settlement_hours - actual_settlement_hours), 4)

    dispute_match = predicted_dispute == actual_dispute

    summary_parts = []

    if prediction_correct:
        summary_parts.append("Predicted resolution path matched actual outcome")
    else:
        summary_parts.append(
            f"Predicted {predicted_resolution_path} but actual path was {actual_resolution_path}"
        )

    if dispute_match:
        summary_parts.append("dispute expectation matched actual outcome")
    else:
        summary_parts.append(
            f"predicted dispute={predicted_dispute} but actual dispute={actual_dispute}"
        )

    if error_hours is not None:
        summary_parts.append(f"settlement timing error was {error_hours:.2f} hours")

    evaluation_summary = "; ".join(summary_parts) + "."

    outcome_id = persist_resolution_outcome(payload)

    evaluation_id = persist_resolution_evaluation(
        simulation_run_id=simulation_run_id,
        market_id=payload.get("market_id"),
        actual_resolution_path=actual_resolution_path,
        predicted_resolution_path=predicted_resolution_path,
        prediction_correct=prediction_correct,
        predicted_settlement_hours=predicted_settlement_hours,
        actual_settlement_hours=actual_settlement_hours,
        error_hours=error_hours,
        predicted_dispute=predicted_dispute,
        actual_dispute=actual_dispute,
        evaluation_summary=evaluation_summary,
        raw_actual_payload=payload,
    )

    return {
        "evaluation_id": evaluation_id,
        "outcome_id": outcome_id,
        "simulation_run_id": simulation_run_id,
        "predicted_resolution_path": predicted_resolution_path,
        "actual_resolution_path": actual_resolution_path,
        "prediction_correct": prediction_correct,
        "predicted_dispute": predicted_dispute,
        "actual_dispute": actual_dispute,
        "predicted_settlement_hours": predicted_settlement_hours,
        "actual_settlement_hours": actual_settlement_hours,
        "error_hours": error_hours,
        "evaluation_summary": evaluation_summary,
    }