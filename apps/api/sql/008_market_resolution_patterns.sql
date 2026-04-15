CREATE TABLE IF NOT EXISTS public.market_resolution_patterns_daily (
    day DATE NOT NULL,
    pattern_type TEXT NOT NULL,
    pattern_value TEXT NOT NULL,

    markets_count INTEGER NOT NULL DEFAULT 0,
    disputed_count INTEGER NOT NULL DEFAULT 0,
    settled_count INTEGER NOT NULL DEFAULT 0,

    dispute_rate DOUBLE PRECISION NULL,
    avg_resolution_clarity_score DOUBLE PRECISION NULL,
    avg_oracle_risk_score DOUBLE PRECISION NULL,
    avg_dispute_propensity_score DOUBLE PRECISION NULL,
    avg_request_to_settlement_hours DOUBLE PRECISION NULL,
    avg_dispute_to_settlement_hours DOUBLE PRECISION NULL,

    engine_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (day, pattern_type, pattern_value)
);

CREATE INDEX IF NOT EXISTS idx_market_resolution_patterns_daily_day
    ON public.market_resolution_patterns_daily (day DESC);

CREATE INDEX IF NOT EXISTS idx_market_resolution_patterns_daily_type
    ON public.market_resolution_patterns_daily (pattern_type, day DESC);