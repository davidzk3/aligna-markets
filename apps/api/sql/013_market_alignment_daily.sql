--
-- PostgreSQL database dump
--

\restrict nvgyPzu4iP46MUZ3LGSpYPaghU0a43modplYb2rYm4bMb4J9FYNG4TuYaPBxHEe

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
-- Name: market_alignment_daily; Type: TABLE; Schema: public; Owner: pmops
--

CREATE TABLE public.market_alignment_daily (
    market_id text NOT NULL,
    day date NOT NULL,
    structural_state text,
    social_state text,
    alignment_state text,
    alignment_score double precision,
    attention_vs_structure_gap double precision,
    bottleneck_type text,
    summary text,
    flags text[],
    engine_version text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    structural_day date,
    social_day date,
    horizon_mode text
);


ALTER TABLE public.market_alignment_daily OWNER TO pmops;

--
-- Name: market_alignment_daily market_alignment_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: pmops
--

ALTER TABLE ONLY public.market_alignment_daily
    ADD CONSTRAINT market_alignment_daily_pkey PRIMARY KEY (market_id, day);


--
-- Name: idx_market_alignment_daily_market_day; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX idx_market_alignment_daily_market_day ON public.market_alignment_daily USING btree (market_id, day DESC);


--
-- Name: idx_market_alignment_daily_mode_day; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX idx_market_alignment_daily_mode_day ON public.market_alignment_daily USING btree (horizon_mode, day DESC);


--
-- PostgreSQL database dump complete
--

\unrestrict nvgyPzu4iP46MUZ3LGSpYPaghU0a43modplYb2rYm4bMb4J9FYNG4TuYaPBxHEe

