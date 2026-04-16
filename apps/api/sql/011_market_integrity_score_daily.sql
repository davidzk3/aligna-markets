--
-- PostgreSQL database dump
--

\restrict QN7xzdbHrhr5GdaUjTs97p1DpnUrD5aXMVgpb8sUa88wB1Yxa8Yf7NaYmZZYVc4

-- Dumped from database version 16.11 (Debian 16.11-1.pgdg13+1)
-- Dumped by pg_dump version 16.11 (Debian 16.11-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: market_integrity_score_daily; Type: TABLE; Schema: public; Owner: pmops
--

CREATE TABLE public.market_integrity_score_daily (
    market_id text NOT NULL,
    day date NOT NULL,
    title text,
    url text,
    category text,
    regime text,
    regime_reason text,
    trades integer,
    unique_traders integer,
    market_quality_score double precision,
    liquidity_health_score double precision,
    concentration_risk_score double precision,
    whale_volume_share double precision,
    radar_risk_score double precision,
    manipulation_score double precision,
    manipulation_signal text,
    whale_role_share double precision,
    speculator_role_share double precision,
    possible_farmer_count integer,
    integrity_score double precision,
    integrity_band text,
    review_priority text,
    primary_reason text,
    needs_operator_review boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    has_regime_data boolean DEFAULT false NOT NULL,
    has_radar_data boolean DEFAULT false NOT NULL,
    has_manipulation_data boolean DEFAULT false NOT NULL,
    data_completeness_score double precision,
    is_partial_coverage boolean DEFAULT false NOT NULL,
    neutral_role_share double precision
);


ALTER TABLE public.market_integrity_score_daily OWNER TO pmops;

--
-- Name: market_integrity_score_daily market_integrity_score_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: pmops
--

ALTER TABLE ONLY public.market_integrity_score_daily
    ADD CONSTRAINT market_integrity_score_daily_pkey PRIMARY KEY (market_id, day);


--
-- Name: idx_integrity_market_day; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX idx_integrity_market_day ON public.market_integrity_score_daily USING btree (market_id, day);


--
-- Name: idx_market_integrity_score_daily_band; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX idx_market_integrity_score_daily_band ON public.market_integrity_score_daily USING btree (day DESC, integrity_band);


--
-- Name: idx_market_integrity_score_daily_day; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX idx_market_integrity_score_daily_day ON public.market_integrity_score_daily USING btree (day DESC);


--
-- Name: idx_market_integrity_score_daily_partial; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX idx_market_integrity_score_daily_partial ON public.market_integrity_score_daily USING btree (day DESC, is_partial_coverage, data_completeness_score);


--
-- Name: idx_market_integrity_score_daily_review; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX idx_market_integrity_score_daily_review ON public.market_integrity_score_daily USING btree (day DESC, needs_operator_review, integrity_score);


--
-- PostgreSQL database dump complete
--

\unrestrict QN7xzdbHrhr5GdaUjTs97p1DpnUrD5aXMVgpb8sUa88wB1Yxa8Yf7NaYmZZYVc4

