CREATE TABLE IF NOT EXISTS public.market_design_rewrite_intelligence_evaluations (
    id SERIAL PRIMARY KEY,

    input_hash TEXT NOT NULL UNIQUE,

    title TEXT NOT NULL,
    description TEXT NULL,
    category TEXT NULL,
    market_type TEXT NULL,
    protocol TEXT NOT NULL DEFAULT 'polymarket',
    oracle_family TEXT NOT NULL DEFAULT 'uma_oo',

    rewrite_readiness TEXT NOT NULL,
    rewrite_priority TEXT NOT NULL,

    problem_clauses JSONB NOT NULL DEFAULT '[]'::jsonb,
    rewrite_actions JSONB NOT NULL DEFAULT '[]'::jsonb,

    revised_title TEXT NULL,
    revised_description TEXT NULL,

    split_recommendation JSONB NOT NULL DEFAULT '{}'::jsonb,

    summary TEXT NULL,
    engine_version TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_design_rewrite_intelligence_created_at
    ON public.market_design_rewrite_intelligence_evaluations (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_design_rewrite_intelligence_priority
    ON public.market_design_rewrite_intelligence_evaluations (rewrite_priority, created_at DESC);
