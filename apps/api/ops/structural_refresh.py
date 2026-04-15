from __future__ import annotations

from datetime import date, datetime, timezone

import psycopg

from apps.api.db import get_db_dsn
from apps.api.services.microstructure import compute_microstructure_daily
from apps.api.services.microstructure_features import compute_microstructure_features_daily
from apps.api.services.market_regime_v2 import compute_market_regime_daily_v2
from apps.api.services.market_risk_radar import compute_market_risk_radar_daily
from apps.api.services.market_manipulation import compute_market_manipulation_daily
from apps.api.services.market_integrity import compute_market_integrity_daily
from apps.api.services.market_launch_intelligence import compute_market_launch_intelligence_daily
from apps.api.services.market_alignment_intelligence import compute_market_alignment_daily


def _resolve_structural_day_for_market(market_id: str) -> date:
    sql = """
    WITH latest_micro AS (
        SELECT MAX(day)::date AS day
        FROM public.market_microstructure_daily
        WHERE market_id = %(market_id)s
    ),
    latest_trade AS (
        SELECT MAX(ts)::date AS day
        FROM core.trades
        WHERE market_id = %(market_id)s
    )
    SELECT COALESCE(
        (SELECT day FROM latest_micro),
        (SELECT day FROM latest_trade),
        %(today)s::date
    )::date AS target_day
    """

    today = datetime.now(timezone.utc).date()

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"market_id": market_id, "today": today})
            row = cur.fetchone()

    if row and row[0]:
        return row[0]
    return today


def refresh_structural_for_market(market_id: str):
    target_day = _resolve_structural_day_for_market(market_id)

    microstructure_result = compute_microstructure_daily(
        day=target_day,
        market_id=market_id,
        limit_markets=1,
    )

    microstructure_features_result = compute_microstructure_features_daily(
        day=target_day,
        market_id=market_id,
        window_hours=24,
        limit_markets=1,
    )

    regime_result = compute_market_regime_daily_v2(
        day=target_day,
        market_id=market_id,
        limit_markets=1,
    )

    radar_result = compute_market_risk_radar_daily(
        day=target_day,
        market_id=market_id,
        limit_markets=1,
    )

    manipulation_result = compute_market_manipulation_daily(
        day=target_day,
        market_id=market_id,
        limit_markets=1,
    )

    integrity_result = compute_market_integrity_daily(
        day=target_day,
        market_id=market_id,
        limit_markets=1,
    )

    launch_result = compute_market_launch_intelligence_daily(
        day=target_day,
        market_id=market_id,
        limit_markets=1,
    )

    alignment_result = compute_market_alignment_daily(
        day=target_day,
        market_id=market_id,
        limit_markets=1,
        horizon_mode="same_day",
    )

    return {
        "status": "ok",
        "market_id": market_id,
        "target_day": str(target_day),
        "microstructure_result": microstructure_result,
        "microstructure_features_result": microstructure_features_result,
        "regime_result": regime_result,
        "radar_result": radar_result,
        "manipulation_result": manipulation_result,
        "integrity_result": integrity_result,
        "launch_result": launch_result,
        "alignment_result": alignment_result,
    }