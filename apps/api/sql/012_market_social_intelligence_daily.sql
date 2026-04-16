--
-- PostgreSQL database dump
--

\restrict jop5dhi4dyPlAAxhqq2ydAJpa7LIM4zMMvjWaIrPa12NXvIRzYKuWOG13WQBXQY

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
-- Name: market_social_intelligence_daily; Type: TABLE; Schema: public; Owner: pmops
--

CREATE TABLE public.market_social_intelligence_daily (
    market_id text NOT NULL,
    day date NOT NULL,
    attention_score double precision,
    sentiment_score double precision,
    demand_score double precision,
    trend_velocity double precision,
    mention_count integer,
    source_count integer,
    confidence_score double precision,
    recommendation text,
    summary text,
    flags text[],
    engine_version text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    demand_state text,
    demand_strength_score double precision,
    demand_breadth_score double precision,
    demand_quality_score double precision,
    narrative_coherence_score double precision,
    social_fragility_score double precision
);


ALTER TABLE public.market_social_intelligence_daily OWNER TO pmops;

--
-- Name: market_social_intelligence_daily market_social_intelligence_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: pmops
--

ALTER TABLE ONLY public.market_social_intelligence_daily
    ADD CONSTRAINT market_social_intelligence_daily_pkey PRIMARY KEY (market_id, day);


--
-- PostgreSQL database dump complete
--

\unrestrict jop5dhi4dyPlAAxhqq2ydAJpa7LIM4zMMvjWaIrPa12NXvIRzYKuWOG13WQBXQY

