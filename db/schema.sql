\restrict dbmate

-- Dumped from database version 18.6 (Debian 18.6-1.pgdg13+2)
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
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
-- Name: inverter_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.inverter_settings (
    soc bigint NOT NULL,
    device_mode text NOT NULL,
    grid_charge text NOT NULL,
    state text NOT NULL,
    load_first_stop_discharge bigint DEFAULT 10 NOT NULL
);


--
-- Name: optimization_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.optimization_results (
    optimization text NOT NULL,
    "time" timestamp with time zone NOT NULL,
    p_pv double precision NOT NULL,
    p_load double precision NOT NULL,
    p_grid_pos double precision NOT NULL,
    p_grid_neg double precision NOT NULL,
    p_grid double precision NOT NULL,
    p_batt double precision NOT NULL,
    soc_opt double precision NOT NULL,
    unit_load_cost double precision NOT NULL,
    unit_prod_price double precision NOT NULL,
    cost_profit double precision NOT NULL,
    cost_fun_profit double precision NOT NULL,
    optim_status text NOT NULL,
    a_soc double precision DEFAULT 0 NOT NULL
);


--
-- Name: schedule; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schedule (
    "time" timestamp with time zone NOT NULL,
    optimization text NOT NULL,
    work_mode text NOT NULL,
    grid_charge text NOT NULL,
    stop_discharge_soc double precision NOT NULL,
    target_soc double precision NOT NULL
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: inverter_settings inverter_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.inverter_settings
    ADD CONSTRAINT inverter_settings_pkey PRIMARY KEY (state);


--
-- Name: optimization_results optimization_results_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.optimization_results
    ADD CONSTRAINT optimization_results_pkey PRIMARY KEY (optimization, "time");


--
-- Name: schedule schedule_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schedule
    ADD CONSTRAINT schedule_pkey PRIMARY KEY ("time");


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- PostgreSQL database dump complete
--

\unrestrict dbmate


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20261020000001');
