CREATE TABLE IF NOT EXISTS public.market_snapshot_audit (
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    structural_day DATE NULL,
    social_day DATE NULL,
    alignment_day DATE NULL,

    structural_score DOUBLE PRECISION NULL,
    social_score DOUBLE PRECISION NULL,

    structural_state TEXT NULL,
    social_state TEXT NULL,
    alignment_state TEXT NULL,
    integrity_band TEXT NULL,

    contextual_summary TEXT NULL,

    is_mixed_horizon BOOLEAN NOT NULL DEFAULT FALSE,

    snapshot_hash TEXT NULL,
    source TEXT NOT NULL DEFAULT 'snapshot_endpoint',

    snapshot_json JSONB NOT NULL,
    ai_context_json JSONB NULL,
    drivers_json JSONB NULL,
    caution_flags_json JSONB NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_audit_market_id
    ON public.market_snapshot_audit (market_id);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_audit_captured_at
    ON public.market_snapshot_audit (captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_snapshot_audit_market_captured
    ON public.market_snapshot_audit (market_id, captured_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_market_snapshot_audit_market_hash_source
    ON public.market_snapshot_audit (market_id, snapshot_hash, source)
    WHERE snapshot_hash IS NOT NULL;


CREATE TABLE IF NOT EXISTS public.market_uma_resolution_metadata (
    id BIGSERIAL PRIMARY KEY,
    market_id TEXT NOT NULL,

    oracle_family TEXT NOT NULL DEFAULT 'uma_oo',
    oracle_type TEXT NULL,
    oracle_contract TEXT NULL,
    identifier TEXT NULL,
    umip TEXT NULL,

    requester TEXT NULL,
    request_transaction TEXT NULL,

    proposer TEXT NULL,
    proposal_transaction TEXT NULL,

    disputer TEXT NULL,
    dispute_transaction TEXT NULL,

    settlement_recipient TEXT NULL,
    settlement_transaction TEXT NULL,

    requested_time TIMESTAMPTZ NULL,
    proposed_time TIMESTAMPTZ NULL,
    disputed_time TIMESTAMPTZ NULL,
    settled_time TIMESTAMPTZ NULL,

    title TEXT NULL,
    description TEXT NULL,
    additional_text_data TEXT NULL,
    bulletin_board_text TEXT NULL,

    res_data TEXT NULL,
    initializer TEXT NULL,

    chain TEXT NULL,
    expiry_type TEXT NULL,
    disputed BOOLEAN NOT NULL DEFAULT FALSE,
    settled BOOLEAN NOT NULL DEFAULT FALSE,

    outcome_proposed TEXT NULL,
    outcome_settled TEXT NULL,

    raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,

    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (market_id)
);

CREATE INDEX IF NOT EXISTS idx_market_uma_resolution_metadata_market_id
    ON public.market_uma_resolution_metadata (market_id);

CREATE INDEX IF NOT EXISTS idx_market_uma_resolution_metadata_requested_time
    ON public.market_uma_resolution_metadata (requested_time DESC);

CREATE INDEX IF NOT EXISTS idx_market_uma_resolution_metadata_disputed
    ON public.market_uma_resolution_metadata (disputed);

CREATE INDEX IF NOT EXISTS idx_market_uma_resolution_metadata_settled
    ON public.market_uma_resolution_metadata (settled);