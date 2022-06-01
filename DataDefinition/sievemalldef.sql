--
-- PostgreSQL database dump
--

-- Dumped from database version 14.2 (Ubuntu 14.2-1.pgdg20.04+1)
-- Dumped by pg_dump version 14.2 (Ubuntu 14.2-1.pgdg20.04+1)

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

--
-- Name: mall; Type: SCHEMA; Schema: -; Owner: sieve
--

CREATE SCHEMA mall;


ALTER SCHEMA mall OWNER TO sieve;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: mall_observation; Type: TABLE; Schema: public; Owner: sieve
--

CREATE TABLE public.mall_observation (
    id character varying(50) NOT NULL,
    shop_name character varying(20) NOT NULL,
    obs_date date NOT NULL,
    obs_time time without time zone NOT NULL,
    user_interest character varying(20),
    device_id integer NOT NULL
);


ALTER TABLE public.mall_observation OWNER TO sieve;

--
-- Name: user_guard_expression; Type: TABLE; Schema: public; Owner: sieve
--

CREATE TABLE public.user_guard_expression (
    id character varying(128) NOT NULL,
    querier character varying(16) NOT NULL,
    purpose character varying(16) NOT NULL,
    enforcement_action character varying(8),
    last_updated timestamp with time zone NOT NULL,
    dirty character varying(16) NOT NULL,
    caridinality double precision
);


ALTER TABLE public.user_guard_expression OWNER TO sieve;

--
-- Name: user_guard_parts; Type: TABLE; Schema: public; Owner: sieve
--

CREATE TABLE public.user_guard_parts (
    id character varying(128) NOT NULL,
    guard_exp_id character varying(128) NOT NULL,
    ownereq integer,
    profeq character varying(16),
    groupeq character varying(16),
    loceq character varying(24),
    datege date,
    datele date,
    timege time without time zone,
    timele time without time zone,
    cardinality double precision
);


ALTER TABLE public.user_guard_parts OWNER TO sieve;

--
-- Name: user_guard_to_policy; Type: TABLE; Schema: public; Owner: sieve
--

CREATE TABLE public.user_guard_to_policy (
    policy_id character varying(128) NOT NULL,
    guard_id character varying(128) NOT NULL
);


ALTER TABLE public.user_guard_to_policy OWNER TO sieve;

--
-- Name: user_policy; Type: TABLE; Schema: public; Owner: sieve
--

CREATE TABLE public.user_policy (
    policy_id integer NOT NULL,
    id character varying(255) NOT NULL,
    querier character varying(255) NOT NULL,
    purpose character varying(255) NOT NULL,
    ttl integer NOT NULL,
    origin character varying(255) NOT NULL,
    objection character varying(255) NOT NULL,
    sharing character varying(255) NOT NULL,
    enforcement_action character varying(255),
    inserted_at timestamp with time zone NOT NULL,
    device_id integer NOT NULL,
    key character varying(128) NOT NULL
);


ALTER TABLE public.user_policy OWNER TO sieve;

--
-- Name: user_policy_object_condition; Type: TABLE; Schema: public; Owner: sieve
--

CREATE TABLE public.user_policy_object_condition (
    id integer NOT NULL,
    policy_id character varying(255) NOT NULL,
    attribute character varying(255) NOT NULL,
    attribute_type character varying(255) NOT NULL,
    operator character varying(255) NOT NULL,
    comp_value character varying(255)
);


ALTER TABLE public.user_policy_object_condition OWNER TO sieve;

--
-- Name: user_policy_object_condition_id_seq; Type: SEQUENCE; Schema: public; Owner: sieve
--

CREATE SEQUENCE public.user_policy_object_condition_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.user_policy_object_condition_id_seq OWNER TO sieve;

--
-- Name: user_policy_object_condition_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: sieve
--

ALTER SEQUENCE public.user_policy_object_condition_id_seq OWNED BY public.user_policy_object_condition.id;


--
-- Name: user_policy_policy_id_seq; Type: SEQUENCE; Schema: public; Owner: sieve
--

CREATE SEQUENCE public.user_policy_policy_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.user_policy_policy_id_seq OWNER TO sieve;

--
-- Name: user_policy_policy_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: sieve
--

ALTER SEQUENCE public.user_policy_policy_id_seq OWNED BY public.user_policy.policy_id;


--
-- Name: user_policy policy_id; Type: DEFAULT; Schema: public; Owner: sieve
--

ALTER TABLE ONLY public.user_policy ALTER COLUMN policy_id SET DEFAULT nextval('public.user_policy_policy_id_seq'::regclass);


--
-- Name: user_policy_object_condition id; Type: DEFAULT; Schema: public; Owner: sieve
--

ALTER TABLE ONLY public.user_policy_object_condition ALTER COLUMN id SET DEFAULT nextval('public.user_policy_object_condition_id_seq'::regclass);

