from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg

from apps.api.db import get_db_dsn


ENGINE_VERSION = "microstructure_features_v2_flags_array_2026_03_03"


def compute_microstructure_features_daily(
    day: Optional[date] = None,
    window_hours: int = 24,
    limit_markets: int = 500,
    market_id: Optional[str] = None,
) -> Dict[str, Any]:
    if day is None:
        day = date.today()

    end_ts = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
    start_ts = end_ts - timedelta(hours=window_hours)

    sql = """
with eligible_markets as (
  select m.market_id
  from public.market_microstructure_daily m
  where m.day = %(day)s
    and m.window_hours = %(window_hours)s
    and (%(market_id)s::text is null or m.market_id = %(market_id)s::text)
  order by m.market_id
  limit %(limit_markets)s
),

base as (
  select
    m.market_id,
    m.day,
    m.window_hours,
    coalesce(m.volume, 0)::double precision as volume,
    coalesce(m.trades, 0)::int as trades,
    coalesce(m.unique_traders, 0)::int as unique_traders,
    coalesce(m.avg_spread, 0)::double precision as avg_spread,
    coalesce(m.price_volatility, 0)::double precision as price_volatility,
    coalesce(m.bbo_ticks, 0)::int as bbo_ticks,
    coalesce(m.top1_trader_share, 0)::double precision as top1_trader_share,
    coalesce(m.top5_trader_share, 0)::double precision as top5_trader_share,
    coalesce(m.hhi, 0)::double precision as hhi,
    coalesce(m.burst_score, 0)::double precision as burst_score,
    coalesce(m.suspicious_burst_flag, false) as suspicious_burst_flag,
    coalesce(m.structural_score, 0)::double precision as structural_score,
    coalesce(m.identity_coverage, 0)::double precision as identity_coverage,
    coalesce(m.identity_blind, false) as identity_blind
  from public.market_microstructure_daily m
  join eligible_markets e
    on e.market_id = m.market_id
  where m.day = %(day)s
    and m.window_hours = %(window_hours)s
),

scored as (
  select
    b.market_id,
    b.day,
    b.window_hours,
    b.trades,
    b.unique_traders,
    b.volume,
    b.avg_spread,
    b.price_volatility,
    b.bbo_ticks,
    b.top1_trader_share,
    b.top5_trader_share,
    b.hhi,
    b.burst_score,
    b.suspicious_burst_flag,
    b.structural_score,
    b.identity_coverage,
    b.identity_blind,

    case
      when b.avg_spread <= 0.005 then 1.00
      when b.avg_spread <= 0.010 then 0.85
      when b.avg_spread <= 0.020 then 0.65
      when b.avg_spread <= 0.040 then 0.40
      else 0.15
    end as spread_score,

    case
      when b.unique_traders >= 100 then 1.00
      when b.unique_traders >= 50 then 0.80
      when b.unique_traders >= 20 then 0.60
      when b.unique_traders >= 8 then 0.35
      else 0.10
    end as breadth_score,

    case
      when b.volume >= 100000 then 1.00
      when b.volume >= 25000 then 0.80
      when b.volume >= 5000 then 0.60
      when b.volume >= 1000 then 0.35
      else 0.10
    end as volume_score,

    case
      when b.hhi <= 0.08 then 1.00
      when b.hhi <= 0.15 then 0.75
      when b.hhi <= 0.25 then 0.45
      else 0.15
    end as concentration_health_score,

    case
      when b.price_volatility <= 0.010 then 1.00
      when b.price_volatility <= 0.025 then 0.80
      when b.price_volatility <= 0.050 then 0.55
      else 0.25
    end as stability_score
  from base b
),

final as (
  select
    s.market_id,
    s.day,
    s.window_hours,

    round((
      0.30 * s.spread_score
      + 0.20 * s.volume_score
      + 0.20 * s.breadth_score
      + 0.15 * s.concentration_health_score
      + 0.15 * s.stability_score
    )::numeric, 6)::double precision as market_quality_score,

    round((
      0.45 * s.spread_score
      + 0.35 * s.volume_score
      + 0.20 * case
        when s.bbo_ticks >= 200 then 1.00
        when s.bbo_ticks >= 50 then 0.70
        when s.bbo_ticks >= 10 then 0.40
        else 0.10
      end
    )::numeric, 6)::double precision as liquidity_health_score,

    round((1.0 - s.concentration_health_score)::numeric, 6)::double precision as concentration_risk_score,

    case
      when s.trades < 3 or s.unique_traders < 3 then array['THIN_ACTIVITY']::text[]
      when s.suspicious_burst_flag then array['BURST_RISK']::text[]
      when s.hhi >= 0.25 then array['CONCENTRATED_FLOW']::text[]
      else array[]::text[]
    end as flags
  from scored s
),

upserted as (
  insert into public.market_microstructure_features_daily (
    market_id,
    day,
    window_hours,
    market_quality_score,
    liquidity_health_score,
    concentration_risk_score,
    flags,
    engine_version,
    created_at,
    updated_at
  )
  select
    f.market_id,
    f.day,
    f.window_hours,
    f.market_quality_score,
    f.liquidity_health_score,
    f.concentration_risk_score,
    f.flags,
    %(engine_version)s,
    now(),
    now()
  from final f
  on conflict (market_id, day) do update set
    window_hours = excluded.window_hours,
    market_quality_score = excluded.market_quality_score,
    liquidity_health_score = excluded.liquidity_health_score,
    concentration_risk_score = excluded.concentration_risk_score,
    flags = excluded.flags,
    engine_version = excluded.engine_version,
    updated_at = now()
  returning market_id
)

select count(*)::int as markets_written
from upserted;
"""

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                create table if not exists public.market_microstructure_features_daily (
                    market_id text not null,
                    day date not null,
                    window_hours integer not null default 24,
                    market_quality_score double precision null,
                    liquidity_health_score double precision null,
                    concentration_risk_score double precision null,
                    flags text[] null,
                    engine_version text null,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now(),
                    primary key (market_id, day)
                );
                """
            )

            cur.execute(
                """
                alter table public.market_microstructure_features_daily
                add column if not exists flags text[] null;
                """
            )

            cur.execute(
                """
                alter table public.market_microstructure_features_daily
                add column if not exists engine_version text null;
                """
            )

            cur.execute(
                """
                alter table public.market_microstructure_features_daily
                add column if not exists created_at timestamptz not null default now();
                """
            )

            cur.execute(
                """
                alter table public.market_microstructure_features_daily
                add column if not exists updated_at timestamptz not null default now();
                """
            )

            cur.execute(
                """
                create index if not exists idx_market_microstructure_features_daily_day
                on public.market_microstructure_features_daily (day desc);
                """
            )

            cur.execute(
                """
                create index if not exists idx_market_microstructure_features_daily_market_day
                on public.market_microstructure_features_daily (market_id, day desc);
                """
            )

            cur.execute(
                sql,
                {
                    "day": day,
                    "window_hours": window_hours,
                    "limit_markets": limit_markets,
                    "market_id": market_id,
                    "engine_version": ENGINE_VERSION,
                },
            )
            row = cur.fetchone()
            conn.commit()

    return {
        "engine_version": ENGINE_VERSION,
        "day": str(day),
        "window_hours": window_hours,
        "limit_markets": limit_markets,
        "market_id": market_id,
        "markets_written": row[0] if row else 0,
        "start_ts": start_ts.isoformat(),
        "end_ts": end_ts.isoformat(),
        "status": "ok",
    }