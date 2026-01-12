--
-- PostgreSQL database dump
--

-- Dumped from database version 13.20
-- Dumped by pg_dump version 13.20

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
-- Name: get_stock_dp01(date, numeric, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_dp01(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000, p_pool_count numeric DEFAULT 25) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
select a.code, a.name --, (close_price-open_price)/close_price*100 ratio, trade_value
from stockmain a join stock_ma b on a.trade_date = b.trade_date and a.code = b.code
join stockfdt_pbr_v c on a.trade_date = c.trade_date and a.code = c.code
where a.trade_date = p_trade_date

and a.close_price < p_max_price

and (a.close_price-a.open_price)/a.close_price*100 > 0.1
and (a.close_price-a.open_price)/a.close_price*100 < 3.1

--and market_cap < 2000000000000   --2조
and market_cap < 5000000000000   --5조
--and market_cap > 300000000000   --3천억
and market_cap > 500000000000   --5천억

--and (LEAST(a.open_price,a.close_price) - a.low_price) >= (a.high_price - GREATEST(a.open_price,a.close_price))*1.1

and a.close_price > ma5
and ma5 > ma10
and ma10 > ma20
--and ma20 > ma40
--and ma40 > ma60
--and ma60 > ma90
--and ma90 > ma120

and c.pbr < 1.50
--and c.pbr < 1.10
and c.per < 15.0
and c.roe > 5.0

--and trade_value > 1000000000   --십억
and trade_value > 1500000000   --십5억
--and trade_value > 3000000000   --3십억

order by trade_value desc
limit p_pool_count
$$;


ALTER FUNCTION public.get_stock_dp01(p_trade_date date, p_max_price numeric, p_pool_count numeric) OWNER TO postgres;

--
-- Name: get_stock_filter_results(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_filter_results(p_trade_date date) RETURNS TABLE(code text, name text, close_price integer, ratio numeric, trade_value_uk integer, market_cap_chunuk integer, pbr numeric, per numeric, dividend_per_share numeric, dividend_yield numeric, roe numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.code::text,
        a.name::text,
        a.close_price::int,
        ((a.close_price - a.open_price) / a.close_price * 100)::numeric(5,2) AS ratio,
        (a.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (a.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        c.pbr::numeric,
        c.per::numeric,
        c.dividend_per_share::numeric,
        c.dividend_yield::numeric,
        (c.pbr/c.per*100)::decimal(10,2)
    FROM stockmain a
    JOIN stock_ma b
      ON a.trade_date = b.trade_date AND a.code = b.code
    JOIN stockfdt c
      ON a.trade_date = c.trade_date AND a.code = c.code
    WHERE a.trade_date = p_trade_date
--      AND a.close_price < 300000
      AND a.market_cap > 100000000000   --천억
      AND c.pbr < 1.00
      AND c.per < 10.00
      AND a.trade_value > 1000000000

--and (a.close_price-a.open_price)/a.close_price*100 >= 0.3
--and (a.close_price-a.open_price)/a.close_price*100 <= 3.0
--and market_cap < 2000000000000

--and (LEAST(a.open_price,a.close_price) - a.low_price) >= (a.high_price - GREATEST(a.open_price,a.close_price))*1.1

--and a.close_price > ma5
--and ma5 > ma10
--and ma10 > ma20
--and ma20 > ma40

--and ma40 > ma60
--and ma60 > ma90
--and ma90 > ma120

    ORDER BY a.trade_value DESC
    LIMIT 200;
END;
$$;


ALTER FUNCTION public.get_stock_filter_results(p_trade_date date) OWNER TO postgres;

--
-- Name: get_stock_filter_results_dividend(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_filter_results_dividend(p_trade_date date) RETURNS TABLE(code text, name text, close_price integer, ratio numeric, trade_value_uk integer, market_cap_chunuk integer, pbr numeric, per numeric, dividend_per_share numeric, dividend_yield numeric, roe numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.code::text,
        a.name::text,
        a.close_price::int,
        ((a.close_price - a.open_price) / a.close_price * 100)::numeric(5,2) AS ratio,
        (a.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (a.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        c.pbr::numeric,
        c.per::numeric,
        c.dividend_per_share::numeric,
        c.dividend_yield::numeric,
        (c.pbr/c.per*100)::decimal(10,2) as roe
    FROM stockmain a
    JOIN stock_ma b
      ON a.trade_date = b.trade_date AND a.code = b.code
    JOIN stockfdt c
      ON a.trade_date = c.trade_date AND a.code = c.code
    WHERE a.trade_date = p_trade_date
--      AND a.close_price < 300000
      AND a.market_cap > 1000000000000   --1조
--and c.pbr < 1.0
--and (c.pbr/c.per*100)::decimal(10,2) > 5.0

--      AND c.pbr < 1.00
--      AND c.per < 5.00
--      AND a.trade_value > 1000000000

--and (a.close_price-a.open_price)/a.close_price*100 >= 0.3
--and (a.close_price-a.open_price)/a.close_price*100 <= 3.0
--and market_cap < 2000000000000

--and (LEAST(a.open_price,a.close_price) - a.low_price) >= (a.high_price - GREATEST(a.open_price,a.close_price))*1.1

--and a.close_price > ma5
--and ma5 > ma10
--and ma10 > ma20
--and ma20 > ma40

--and ma40 > ma60
--and ma60 > ma90
--and ma90 > ma120

    ORDER BY c.dividend_yield DESC
    LIMIT 300;
END;
$$;


ALTER FUNCTION public.get_stock_filter_results_dividend(p_trade_date date) OWNER TO postgres;

--
-- Name: get_stock_filter_results_dividend_range(date, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_filter_results_dividend_range(p_trade_date date, p_code character varying) RETURNS TABLE(trade_date date, code text, name text, close_price integer, ratio numeric, trade_value_uk integer, market_cap_chunuk integer, pbr numeric, per numeric, dividend_per_share numeric, dividend_yield numeric, roe numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT a.trade_date,
        a.code::text,
        a.name::text,
        a.close_price::int,
        ((a.close_price - a.open_price) / a.close_price * 100)::numeric(5,2) AS ratio,
        (a.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (a.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        c.pbr::numeric,
        c.per::numeric,
        c.dividend_per_share::numeric,
        c.dividend_yield::numeric,
        (c.pbr/c.per*100)::decimal(10,2) as roe
    FROM stockmain a
--    JOIN stock_ma b
--      ON a.trade_date = b.trade_date AND a.code = b.code
    JOIN stockfdt c
      ON a.trade_date = c.trade_date AND a.code = c.code
    WHERE a.trade_date >= p_trade_date
    and a.code = p_code
--      AND a.close_price < 300000
--      AND a.market_cap > 1000000000000   --1조
--and c.pbr < 1.0
--and (c.pbr/c.per*100)::decimal(10,2) > 8.0
--      AND c.pbr < 1.00
--      AND c.per < 5.00
--      AND a.trade_value > 1000000000

--and (a.close_price-a.open_price)/a.close_price*100 >= 0.3
--and (a.close_price-a.open_price)/a.close_price*100 <= 3.0
--and market_cap < 2000000000000

--and (LEAST(a.open_price,a.close_price) - a.low_price) >= (a.high_price - GREATEST(a.open_price,a.close_price))*1.1

--and a.close_price > ma5
--and ma5 > ma10
--and ma10 > ma20
--and ma20 > ma40

--and ma40 > ma60
--and ma60 > ma90
--and ma90 > ma120

    ORDER BY a.trade_date ASC;
END;
$$;


ALTER FUNCTION public.get_stock_filter_results_dividend_range(p_trade_date date, p_code character varying) OWNER TO postgres;

--
-- Name: get_stock_filter_results_pbr(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_filter_results_pbr(p_trade_date date) RETURNS TABLE(code text, name text, close_price integer, ratio numeric, trade_value_uk integer, market_cap_chunuk integer, pbr numeric, per numeric, dividend_per_share numeric, dividend_yield numeric, roe numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.code::text,
        a.name::text,
        a.close_price::int,
        ((a.close_price - a.open_price) / a.close_price * 100)::numeric(5,2) AS ratio,
        (a.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (a.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        c.pbr::numeric,
        c.per::numeric,
        c.dividend_per_share::numeric,
        c.dividend_yield::numeric,
        (c.pbr/c.per*100)::decimal(10,2)
    FROM stockmain a
    JOIN stock_ma b
      ON a.trade_date = b.trade_date AND a.code = b.code
    JOIN stockfdt c
      ON a.trade_date = c.trade_date AND a.code = c.code
    WHERE a.trade_date = p_trade_date
--      AND a.close_price < 300000
      AND a.market_cap > 1000000000000   --1조
--      AND c.pbr < 1.00
--      AND c.per < 5.00
--      AND a.trade_value > 1000000000

--and (a.close_price-a.open_price)/a.close_price*100 >= 0.3
--and (a.close_price-a.open_price)/a.close_price*100 <= 3.0
--and market_cap < 2000000000000

--and (LEAST(a.open_price,a.close_price) - a.low_price) >= (a.high_price - GREATEST(a.open_price,a.close_price))*1.1

--and a.close_price > ma5
--and ma5 > ma10
--and ma10 > ma20
--and ma20 > ma40

--and ma40 > ma60
--and ma60 > ma90
--and ma90 > ma120

    ORDER BY c.pbr ASC
    LIMIT 300;
END;
$$;


ALTER FUNCTION public.get_stock_filter_results_pbr(p_trade_date date) OWNER TO postgres;

--
-- Name: get_stock_ma10_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma10_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma10,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma5,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_5,
lag(ma10,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma10,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma10,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma10,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma10,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma10,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma10,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma10,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma10,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        10)     over    (partition      by      code    order   by      trade_date)     as      price_prev10
from stockmain
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date >= p_trade_date
and     prev10  >       prev9
and     prev10  >       prev8
and     prev10  >       prev7
and     prev10  >       prev6
and     prev10  >       prev5
and     prev10  >       prev4
and     prev10  >       prev3

and prev1 < ma40

and close_price > ma5
and close_price > ma10
and ma5 > ma10
and prev1_5 < prev1

and close_price < p_max_price
and sm.market_cap > 300000000000   --3천억

and sfv.pbr < 3.0

order by trade_date
$$;


ALTER FUNCTION public.get_stock_ma10_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma120(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma120(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma120,      1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_90,
lag(ma120,      2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma120,      3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma120,      4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma120,      5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma120,      6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma120,      7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma120,      8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma120,      9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma120,      10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma120,      11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma120,      12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma120,      13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma120,      14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma120,      15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma120,      16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma120,      17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma120,      18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma120,      19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma120,      20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma120,      21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma120,      22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma120,      23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma120,      24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma120,      25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma120,      26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma120,      27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma120,      28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma120,      29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma120,      30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma120,      31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma120,      32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma120,      33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma120,      34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma120,      35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma120,      36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma120,      37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma120,      38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma120,      39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma120,      40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma120,      41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma120,      42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma120,      43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma120,      44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma120,      45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma120,      46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma120,      47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma120,      48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma120,      49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma120,      50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma120,      51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma120,      52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma120,      53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma120,      54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma120,      55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma120,      56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma120,      57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma120,      58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma120,      59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma120,      60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
lag(ma120,      61)     over    (partition      by      code    order   by      trade_date)     as      prev61,
lag(ma120,      62)     over    (partition      by      code    order   by      trade_date)     as      prev62,
lag(ma120,      63)     over    (partition      by      code    order   by      trade_date)     as      prev63,
lag(ma120,      64)     over    (partition      by      code    order   by      trade_date)     as      prev64,
lag(ma120,      65)     over    (partition      by      code    order   by      trade_date)     as      prev65,
lag(ma120,      66)     over    (partition      by      code    order   by      trade_date)     as      prev66,
lag(ma120,      67)     over    (partition      by      code    order   by      trade_date)     as      prev67,
lag(ma120,      68)     over    (partition      by      code    order   by      trade_date)     as      prev68,
lag(ma120,      69)     over    (partition      by      code    order   by      trade_date)     as      prev69,
lag(ma120,      70)     over    (partition      by      code    order   by      trade_date)     as      prev70,
lag(ma120,      71)     over    (partition      by      code    order   by      trade_date)     as      prev71,
lag(ma120,      72)     over    (partition      by      code    order   by      trade_date)     as      prev72,
lag(ma120,      73)     over    (partition      by      code    order   by      trade_date)     as      prev73,
lag(ma120,      74)     over    (partition      by      code    order   by      trade_date)     as      prev74,
lag(ma120,      75)     over    (partition      by      code    order   by      trade_date)     as      prev75,
lag(ma120,      76)     over    (partition      by      code    order   by      trade_date)     as      prev76,
lag(ma120,      77)     over    (partition      by      code    order   by      trade_date)     as      prev77,
lag(ma120,      78)     over    (partition      by      code    order   by      trade_date)     as      prev78,
lag(ma120,      79)     over    (partition      by      code    order   by      trade_date)     as      prev79,
lag(ma120,      80)     over    (partition      by      code    order   by      trade_date)     as      prev80,
lag(ma120,      81)     over    (partition      by      code    order   by      trade_date)     as      prev81,
lag(ma120,      82)     over    (partition      by      code    order   by      trade_date)     as      prev82,
lag(ma120,      83)     over    (partition      by      code    order   by      trade_date)     as      prev83,
lag(ma120,      84)     over    (partition      by      code    order   by      trade_date)     as      prev84,
lag(ma120,      85)     over    (partition      by      code    order   by      trade_date)     as      prev85,
lag(ma120,      86)     over    (partition      by      code    order   by      trade_date)     as      prev86,
lag(ma120,      87)     over    (partition      by      code    order   by      trade_date)     as      prev87,
lag(ma120,      88)     over    (partition      by      code    order   by      trade_date)     as      prev88,
lag(ma120,      89)     over    (partition      by      code    order   by      trade_date)     as      prev89,
lag(ma120,      90)     over    (partition      by      code    order   by      trade_date)     as      prev90,
lag(ma120,      91)     over    (partition      by      code    order   by      trade_date)     as      prev91,
lag(ma120,      92)     over    (partition      by      code    order   by      trade_date)     as      prev92,
lag(ma120,      93)     over    (partition      by      code    order   by      trade_date)     as      prev93,
lag(ma120,      94)     over    (partition      by      code    order   by      trade_date)     as      prev94,
lag(ma120,      95)     over    (partition      by      code    order   by      trade_date)     as      prev95,
lag(ma120,      96)     over    (partition      by      code    order   by      trade_date)     as      prev96,
lag(ma120,      97)     over    (partition      by      code    order   by      trade_date)     as      prev97,
lag(ma120,      98)     over    (partition      by      code    order   by      trade_date)     as      prev98,
lag(ma120,      99)     over    (partition      by      code    order   by      trade_date)     as      prev99,
lag(ma120,      100)    over    (partition      by      code    order   by      trade_date)     as      prev100,
lag(ma120,      101)    over    (partition      by      code    order   by      trade_date)     as      prev101,
lag(ma120,      102)    over    (partition      by      code    order   by      trade_date)     as      prev102,
lag(ma120,      103)    over    (partition      by      code    order   by      trade_date)     as      prev103,
lag(ma120,      104)    over    (partition      by      code    order   by      trade_date)     as      prev104,
lag(ma120,      105)    over    (partition      by      code    order   by      trade_date)     as      prev105,
lag(ma120,      106)    over    (partition      by      code    order   by      trade_date)     as      prev106,
lag(ma120,      107)    over    (partition      by      code    order   by      trade_date)     as      prev107,
lag(ma120,      108)    over    (partition      by      code    order   by      trade_date)     as      prev108,
lag(ma120,      109)    over    (partition      by      code    order   by      trade_date)     as      prev109,
lag(ma120,      110)    over    (partition      by      code    order   by      trade_date)     as      prev110,
lag(ma120,      111)    over    (partition      by      code    order   by      trade_date)     as      prev111,
lag(ma120,      112)    over    (partition      by      code    order   by      trade_date)     as      prev112,
lag(ma120,      113)    over    (partition      by      code    order   by      trade_date)     as      prev113,
lag(ma120,      114)    over    (partition      by      code    order   by      trade_date)     as      prev114,
lag(ma120,      115)    over    (partition      by      code    order   by      trade_date)     as      prev115,
lag(ma120,      116)    over    (partition      by      code    order   by      trade_date)     as      prev116,
lag(ma120,      117)    over    (partition      by      code    order   by      trade_date)     as      prev117,
lag(ma120,      118)    over    (partition      by      code    order   by      trade_date)     as      prev118,
lag(ma120,      119)    over    (partition      by      code    order   by      trade_date)     as      prev119,
lag(ma120,      120)    over    (partition      by      code    order   by      trade_date)     as      prev120,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 240) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,      120)    over    (partition      by      code    order   by      trade_date)     as      price_prev120
--from stockmain
--)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date
and     prev120 >       prev119
and     prev119 >       prev118
and     prev118 >       prev117
and     prev117 >       prev116
and     prev116 >       prev115
and     prev115 >       prev114
and     prev114 >       prev113
and     prev113 >       prev112
and     prev112 >       prev111
and     prev111 >       prev110
and     prev110 >       prev109
and     prev109 >       prev108
and     prev108 >       prev107
and     prev107 >       prev106
and     prev106 >       prev105
and     prev105 >       prev104
and     prev104 >       prev103
and     prev103 >       prev102
and     prev102 >       prev101
and     prev101 >       prev100
and     prev100 >       prev99
and     prev99  >       prev98
and     prev98  >       prev97
and     prev97  >       prev96
and     prev96  >       prev95
and     prev95  >       prev94
and     prev94  >       prev93
and     prev93  >       prev92
and     prev92  >       prev91
and     prev91  >       prev90
and     prev90  >       prev89
and     prev89  >       prev88
and     prev88  >       prev87
and     prev87  >       prev86
and     prev86  >       prev85
and     prev85  >       prev84
and     prev84  >       prev83
and     prev83  >       prev82
and     prev82  >       prev81
and     prev81  >       prev80
and     prev80  >       prev79
and     prev79  >       prev78
and     prev78  >       prev77
and     prev77  >       prev76
and     prev76  >       prev75
and     prev75  >       prev74
and     prev74  >       prev73
and     prev73  >       prev72
and     prev72  >       prev71
and     prev71  >       prev70
and     prev70  >       prev69
and     prev69  >       prev68
and     prev68  >       prev67
and     prev67  >       prev66
and     prev66  >       prev65
and     prev65  >       prev64
and     prev64  >       prev63
and     prev63  >       prev62
and     prev62  >       prev61
and     prev61  >       prev60
--and   prev60  >       prev59
--and   prev59  >       prev58
--and   prev58  >       prev57
--and   prev57  >       prev56
--and   prev56  >       prev55
--and   prev55  >       prev54
--and   prev54  >       prev53
--and   prev53  >       prev52
--and   prev52  >       prev51
--and     prev51  >       prev50
--and     prev50  >       prev49
--and     prev49  >       prev48
--and     prev48  >       prev47
--and     prev47  >       prev46
--and     prev46  >       prev45
--and     prev45  >       prev44
--and     prev44  >       prev43
--and     prev43  >       prev42
--and     prev42  >       prev41
--and     prev41  >       prev40
--and     prev40  >       prev39
--and     prev39  >       prev38
--and     prev38  >       prev37
--and     prev37  >       prev36
--and     prev36  >       prev35
--and     prev35  >       prev34
--and     prev34  >       prev33
--and     prev33  >       prev32
--and     prev32  >       prev31
--and     prev31  >       prev30
--and     prev30  >       prev29
--and     prev29  >       prev28
--and     prev28  >       prev27
--and     prev27  >       prev26
--and     prev26  >       prev25
and prev1 < ma120

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and close_price > ma90
and close_price > ma120
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and ma60 > ma90
and ma90 > ma120
and prev1_90 < prev1

and close_price < (p_max_price - 800000)
and sm.market_cap > 300000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma120(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma120_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma120_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma120,      1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_90,
lag(ma120,      2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma120,      3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma120,      4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma120,      5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma120,      6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma120,      7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma120,      8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma120,      9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma120,      10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma120,      11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma120,      12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma120,      13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma120,      14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma120,      15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma120,      16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma120,      17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma120,      18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma120,      19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma120,      20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma120,      21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma120,      22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma120,      23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma120,      24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma120,      25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma120,      26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma120,      27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma120,      28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma120,      29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma120,      30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma120,      31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma120,      32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma120,      33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma120,      34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma120,      35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma120,      36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma120,      37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma120,      38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma120,      39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma120,      40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma120,      41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma120,      42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma120,      43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma120,      44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma120,      45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma120,      46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma120,      47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma120,      48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma120,      49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma120,      50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma120,      51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma120,      52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma120,      53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma120,      54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma120,      55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma120,      56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma120,      57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma120,      58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma120,      59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma120,      60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
lag(ma120,      61)     over    (partition      by      code    order   by      trade_date)     as      prev61,
lag(ma120,      62)     over    (partition      by      code    order   by      trade_date)     as      prev62,
lag(ma120,      63)     over    (partition      by      code    order   by      trade_date)     as      prev63,
lag(ma120,      64)     over    (partition      by      code    order   by      trade_date)     as      prev64,
lag(ma120,      65)     over    (partition      by      code    order   by      trade_date)     as      prev65,
lag(ma120,      66)     over    (partition      by      code    order   by      trade_date)     as      prev66,
lag(ma120,      67)     over    (partition      by      code    order   by      trade_date)     as      prev67,
lag(ma120,      68)     over    (partition      by      code    order   by      trade_date)     as      prev68,
lag(ma120,      69)     over    (partition      by      code    order   by      trade_date)     as      prev69,
lag(ma120,      70)     over    (partition      by      code    order   by      trade_date)     as      prev70,
lag(ma120,      71)     over    (partition      by      code    order   by      trade_date)     as      prev71,
lag(ma120,      72)     over    (partition      by      code    order   by      trade_date)     as      prev72,
lag(ma120,      73)     over    (partition      by      code    order   by      trade_date)     as      prev73,
lag(ma120,      74)     over    (partition      by      code    order   by      trade_date)     as      prev74,
lag(ma120,      75)     over    (partition      by      code    order   by      trade_date)     as      prev75,
lag(ma120,      76)     over    (partition      by      code    order   by      trade_date)     as      prev76,
lag(ma120,      77)     over    (partition      by      code    order   by      trade_date)     as      prev77,
lag(ma120,      78)     over    (partition      by      code    order   by      trade_date)     as      prev78,
lag(ma120,      79)     over    (partition      by      code    order   by      trade_date)     as      prev79,
lag(ma120,      80)     over    (partition      by      code    order   by      trade_date)     as      prev80,
lag(ma120,      81)     over    (partition      by      code    order   by      trade_date)     as      prev81,
lag(ma120,      82)     over    (partition      by      code    order   by      trade_date)     as      prev82,
lag(ma120,      83)     over    (partition      by      code    order   by      trade_date)     as      prev83,
lag(ma120,      84)     over    (partition      by      code    order   by      trade_date)     as      prev84,
lag(ma120,      85)     over    (partition      by      code    order   by      trade_date)     as      prev85,
lag(ma120,      86)     over    (partition      by      code    order   by      trade_date)     as      prev86,
lag(ma120,      87)     over    (partition      by      code    order   by      trade_date)     as      prev87,
lag(ma120,      88)     over    (partition      by      code    order   by      trade_date)     as      prev88,
lag(ma120,      89)     over    (partition      by      code    order   by      trade_date)     as      prev89,
lag(ma120,      90)     over    (partition      by      code    order   by      trade_date)     as      prev90,
lag(ma120,      91)     over    (partition      by      code    order   by      trade_date)     as      prev91,
lag(ma120,      92)     over    (partition      by      code    order   by      trade_date)     as      prev92,
lag(ma120,      93)     over    (partition      by      code    order   by      trade_date)     as      prev93,
lag(ma120,      94)     over    (partition      by      code    order   by      trade_date)     as      prev94,
lag(ma120,      95)     over    (partition      by      code    order   by      trade_date)     as      prev95,
lag(ma120,      96)     over    (partition      by      code    order   by      trade_date)     as      prev96,
lag(ma120,      97)     over    (partition      by      code    order   by      trade_date)     as      prev97,
lag(ma120,      98)     over    (partition      by      code    order   by      trade_date)     as      prev98,
lag(ma120,      99)     over    (partition      by      code    order   by      trade_date)     as      prev99,
lag(ma120,      100)    over    (partition      by      code    order   by      trade_date)     as      prev100,
lag(ma120,      101)    over    (partition      by      code    order   by      trade_date)     as      prev101,
lag(ma120,      102)    over    (partition      by      code    order   by      trade_date)     as      prev102,
lag(ma120,      103)    over    (partition      by      code    order   by      trade_date)     as      prev103,
lag(ma120,      104)    over    (partition      by      code    order   by      trade_date)     as      prev104,
lag(ma120,      105)    over    (partition      by      code    order   by      trade_date)     as      prev105,
lag(ma120,      106)    over    (partition      by      code    order   by      trade_date)     as      prev106,
lag(ma120,      107)    over    (partition      by      code    order   by      trade_date)     as      prev107,
lag(ma120,      108)    over    (partition      by      code    order   by      trade_date)     as      prev108,
lag(ma120,      109)    over    (partition      by      code    order   by      trade_date)     as      prev109,
lag(ma120,      110)    over    (partition      by      code    order   by      trade_date)     as      prev110,
lag(ma120,      111)    over    (partition      by      code    order   by      trade_date)     as      prev111,
lag(ma120,      112)    over    (partition      by      code    order   by      trade_date)     as      prev112,
lag(ma120,      113)    over    (partition      by      code    order   by      trade_date)     as      prev113,
lag(ma120,      114)    over    (partition      by      code    order   by      trade_date)     as      prev114,
lag(ma120,      115)    over    (partition      by      code    order   by      trade_date)     as      prev115,
lag(ma120,      116)    over    (partition      by      code    order   by      trade_date)     as      prev116,
lag(ma120,      117)    over    (partition      by      code    order   by      trade_date)     as      prev117,
lag(ma120,      118)    over    (partition      by      code    order   by      trade_date)     as      prev118,
lag(ma120,      119)    over    (partition      by      code    order   by      trade_date)     as      prev119,
lag(ma120,      120)    over    (partition      by      code    order   by      trade_date)     as      prev120,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        120)    over    (partition      by      code    order   by      trade_date)     as      price_prev120
from stockmain
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date >= p_trade_date
and     prev120 >       prev119
and     prev119 >       prev118
and     prev118 >       prev117
and     prev117 >       prev116
and     prev116 >       prev115
and     prev115 >       prev114
and     prev114 >       prev113
and     prev113 >       prev112
and     prev112 >       prev111
and     prev111 >       prev110
and     prev110 >       prev109
and     prev109 >       prev108
and     prev108 >       prev107
and     prev107 >       prev106
and     prev106 >       prev105
and     prev105 >       prev104
and     prev104 >       prev103
and     prev103 >       prev102
and     prev102 >       prev101
and     prev101 >       prev100
and     prev100 >       prev99
and     prev99  >       prev98
and     prev98  >       prev97
and     prev97  >       prev96
and     prev96  >       prev95
and     prev95  >       prev94
and     prev94  >       prev93
and     prev93  >       prev92
and     prev92  >       prev91
and     prev91  >       prev90
and     prev90  >       prev89
and     prev89  >       prev88
and     prev88  >       prev87
and     prev87  >       prev86
and     prev86  >       prev85
and     prev85  >       prev84
and     prev84  >       prev83
and     prev83  >       prev82
and     prev82  >       prev81
and     prev81  >       prev80
and     prev80  >       prev79
and     prev79  >       prev78
and     prev78  >       prev77
and     prev77  >       prev76
and     prev76  >       prev75
and     prev75  >       prev74
and     prev74  >       prev73
and     prev73  >       prev72
and     prev72  >       prev71
and     prev71  >       prev70
and     prev70  >       prev69
and     prev69  >       prev68
and     prev68  >       prev67
and     prev67  >       prev66
and     prev66  >       prev65
and     prev65  >       prev64
and     prev64  >       prev63
and     prev63  >       prev62
and     prev62  >       prev61
and     prev61  >       prev60
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and prev1 < ma120

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and close_price > ma90
and close_price > ma120
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and ma60 > ma90
and ma90 > ma120
and prev1_90 < prev1

and close_price < p_max_price
and sm.market_cap > 300000000000

and sfv.pbr < 3.0

order by trade_date
$$;


ALTER FUNCTION public.get_stock_ma120_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma20(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma20(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma10,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_10,
lag(ma20,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma20,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma20,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma20,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma20,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma20,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma20,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma20,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma20,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma20,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma20,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma20,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma20,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma20,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma20,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma20,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma20,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma20,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma20,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 40) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,        20)     over    (partition      by      code    order   by      trade_date)     as      price_prev20
--from stockmain
--)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and     prev13  >       prev12
and     prev12  >       prev11
and     prev11  >       prev10
--and     prev10  >       prev9
--and     prev9   >       prev8
--and     prev8   >       prev7
--and     prev9   >       prev6
--and     prev6   >       prev5
and prev1 < ma20

and close_price > ma5
and close_price > ma10
and close_price > ma20
and ma5 > ma10
and ma10 > ma20
and prev1_10 < prev1

and close_price < p_max_price
and sm.market_cap > 300000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma20(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma20_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma20_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma10,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_10,
lag(ma20,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma20,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma20,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma20,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma20,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma20,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma20,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma20,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma20,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma20,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma20,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma20,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma20,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma20,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma20,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma20,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma20,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma20,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma20,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        20)     over    (partition      by      code    order   by      trade_date)     as      price_prev20
from stockmain
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date >= p_trade_date
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and     prev13  >       prev12
and     prev12  >       prev11
and     prev11  >       prev10
and     prev10  >       prev9
and     prev9   >       prev8
and     prev8   >       prev7
and     prev9   >       prev6
and     prev6   >       prev5
and prev1 < ma20

and close_price > ma5
and close_price > ma10
and close_price > ma20
and ma5 > ma10
and ma10 > ma20
and prev1_10 < prev1

and close_price < p_max_price
and sm.market_cap > 300000000000

and sfv.pbr < 3.0

order by trade_date
$$;


ALTER FUNCTION public.get_stock_ma20_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma40(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma40(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_20,
lag(ma40,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma40,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma40,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma40,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma40,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma40,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma40,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma40,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma40,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma40,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma40,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma40,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma40,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma40,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma40,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma40,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma40,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma40,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma40,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma40,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma40,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma40,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma40,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma40,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma40,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma40,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma40,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma40,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma40,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma40,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma40,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma40,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma40,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma40,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma40,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma40,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma40,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma40,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma40,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 80) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,      40)     over    (partition      by      code    order   by      trade_date)     as      price_prev40
--from stockmain
--)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
--and   prev20  >       prev19
--and   prev19  >       prev18
--and   prev18  >       prev17
--and   prev17  >       prev16
--and   prev16  >       prev15
--and   prev15  >       prev14
--and   prev14  >       prev13
--and   prev13  >       prev12
--and   prev12  >       prev11
--and   prev11  >       prev10
--and   prev10  >       prev9
and prev1 < ma40

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and prev1_20 < prev1

and close_price < (p_max_price - 200000)
and sm.market_cap > 300000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma40(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma40_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma40_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_20,
lag(ma40,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma40,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma40,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma40,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma40,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma40,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma40,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma40,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma40,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma40,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma40,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma40,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma40,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma40,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma40,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma40,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma40,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma40,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma40,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma40,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma40,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma40,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma40,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma40,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma40,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma40,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma40,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma40,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma40,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma40,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma40,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma40,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma40,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma40,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma40,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma40,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma40,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma40,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma40,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        40)     over    (partition      by      code    order   by      trade_date)     as      price_prev40
from stockmain
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date >= p_trade_date
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and     prev13  >       prev12
and     prev12  >       prev11
and     prev11  >       prev10
and     prev10  >       prev9
and prev1 < ma40

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and prev1_20 < prev1

and close_price < p_max_price
and sm.market_cap > 300000000000

and sfv.pbr < 3.0

order by trade_date
$$;


ALTER FUNCTION public.get_stock_ma40_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma60(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma60(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_40,
lag(ma60,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma60,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma60,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma60,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma60,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma60,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma60,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma60,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma60,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma60,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma60,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma60,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma60,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma60,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma60,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma60,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma60,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma60,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma60,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma60,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma60,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma60,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma60,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma60,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma60,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma60,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma60,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma60,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma60,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma60,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma60,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma60,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma60,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma60,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma60,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma60,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma60,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma60,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma60,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma60,       41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma60,       42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma60,       43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma60,       44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma60,       45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma60,       46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma60,       47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma60,       48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma60,       49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma60,       50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma60,       51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma60,       52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma60,       53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma60,       54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma60,       55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma60,       56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma60,       57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma60,       58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma60,       59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma60,       60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 120) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,      60)     over    (partition      by      code    order   by      trade_date)     as      price_prev60
--from stockmain
--)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
--and   prev30  >       prev29
--and   prev29  >       prev28
--and   prev28  >       prev27
--and   prev27  >       prev26
--and   prev26  >       prev25
--and   prev25  >       prev24
--and   prev24  >       prev23
--and   prev23  >       prev22
--and   prev22  >       prev21
--and   prev21  >       prev20
--and   prev20  >       prev19
--and   prev19  >       prev18
--and   prev18  >       prev17
--and   prev17  >       prev16
--and   prev16  >       prev15
--and   prev15  >       prev14
--and   prev14  >       prev13
and prev1 < ma60

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and prev1_40 < prev1

and close_price < (p_max_price - 400000)
and sm.market_cap > 300000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma60(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma60_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma60_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_40,
lag(ma60,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma60,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma60,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma60,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma60,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma60,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma60,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma60,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma60,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma60,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma60,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma60,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma60,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma60,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma60,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma60,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma60,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma60,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma60,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma60,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma60,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma60,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma60,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma60,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma60,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma60,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma60,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma60,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma60,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma60,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma60,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma60,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma60,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma60,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma60,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma60,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma60,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma60,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma60,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma60,       41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma60,       42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma60,       43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma60,       44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma60,       45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma60,       46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma60,       47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma60,       48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma60,       49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma60,       50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma60,       51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma60,       52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma60,       53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma60,       54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma60,       55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma60,       56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma60,       57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma60,       58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma60,       59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma60,       60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        60)     over    (partition      by      code    order   by      trade_date)     as      price_prev60
from stockmain
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date >= p_trade_date
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and prev1 < ma60

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and prev1_40 < prev1

and close_price < p_max_price
and sm.market_cap > 300000000000

and sfv.pbr < 3.0

order by trade_date
$$;


ALTER FUNCTION public.get_stock_ma60_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma90(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma90(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_60,
lag(ma90,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma90,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma90,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma90,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma90,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma90,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma90,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma90,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma90,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma90,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma90,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma90,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma90,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma90,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma90,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma90,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma90,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma90,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma90,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma90,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma90,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma90,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma90,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma90,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma90,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma90,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma90,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma90,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma90,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma90,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma90,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma90,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma90,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma90,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma90,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma90,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma90,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma90,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma90,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma90,       41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma90,       42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma90,       43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma90,       44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma90,       45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma90,       46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma90,       47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma90,       48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma90,       49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma90,       50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma90,       51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma90,       52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma90,       53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma90,       54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma90,       55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma90,       56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma90,       57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma90,       58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma90,       59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma90,       60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
lag(ma90,       61)     over    (partition      by      code    order   by      trade_date)     as      prev61,
lag(ma90,       62)     over    (partition      by      code    order   by      trade_date)     as      prev62,
lag(ma90,       63)     over    (partition      by      code    order   by      trade_date)     as      prev63,
lag(ma90,       64)     over    (partition      by      code    order   by      trade_date)     as      prev64,
lag(ma90,       65)     over    (partition      by      code    order   by      trade_date)     as      prev65,
lag(ma90,       66)     over    (partition      by      code    order   by      trade_date)     as      prev66,
lag(ma90,       67)     over    (partition      by      code    order   by      trade_date)     as      prev67,
lag(ma90,       68)     over    (partition      by      code    order   by      trade_date)     as      prev68,
lag(ma90,       69)     over    (partition      by      code    order   by      trade_date)     as      prev69,
lag(ma90,       70)     over    (partition      by      code    order   by      trade_date)     as      prev70,
lag(ma90,       71)     over    (partition      by      code    order   by      trade_date)     as      prev71,
lag(ma90,       72)     over    (partition      by      code    order   by      trade_date)     as      prev72,
lag(ma90,       73)     over    (partition      by      code    order   by      trade_date)     as      prev73,
lag(ma90,       74)     over    (partition      by      code    order   by      trade_date)     as      prev74,
lag(ma90,       75)     over    (partition      by      code    order   by      trade_date)     as      prev75,
lag(ma90,       76)     over    (partition      by      code    order   by      trade_date)     as      prev76,
lag(ma90,       77)     over    (partition      by      code    order   by      trade_date)     as      prev77,
lag(ma90,       78)     over    (partition      by      code    order   by      trade_date)     as      prev78,
lag(ma90,       79)     over    (partition      by      code    order   by      trade_date)     as      prev79,
lag(ma90,       80)     over    (partition      by      code    order   by      trade_date)     as      prev80,
lag(ma90,       81)     over    (partition      by      code    order   by      trade_date)     as      prev81,
lag(ma90,       82)     over    (partition      by      code    order   by      trade_date)     as      prev82,
lag(ma90,       83)     over    (partition      by      code    order   by      trade_date)     as      prev83,
lag(ma90,       84)     over    (partition      by      code    order   by      trade_date)     as      prev84,
lag(ma90,       85)     over    (partition      by      code    order   by      trade_date)     as      prev85,
lag(ma90,       86)     over    (partition      by      code    order   by      trade_date)     as      prev86,
lag(ma90,       87)     over    (partition      by      code    order   by      trade_date)     as      prev87,
lag(ma90,       88)     over    (partition      by      code    order   by      trade_date)     as      prev88,
lag(ma90,       89)     over    (partition      by      code    order   by      trade_date)     as      prev89,
lag(ma90,       90)     over    (partition      by      code    order   by      trade_date)     as      prev90,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 180) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,      90)     over    (partition      by      code    order   by      trade_date)     as      price_prev90
--from stockmain
--)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date
and     prev90  >       prev89
and     prev89  >       prev88
and     prev88  >       prev87
and     prev87  >       prev86
and     prev86  >       prev85
and     prev85  >       prev84
and     prev84  >       prev83
and     prev83  >       prev82
and     prev82  >       prev81
and     prev81  >       prev80
and     prev80  >       prev79
and     prev79  >       prev78
and     prev78  >       prev77
and     prev77  >       prev76
and     prev76  >       prev75
and     prev75  >       prev74
and     prev74  >       prev73
and     prev73  >       prev72
and     prev72  >       prev71
and     prev71  >       prev70
and     prev70  >       prev69
and     prev69  >       prev68
and     prev68  >       prev67
and     prev67  >       prev66
and     prev66  >       prev65
and     prev65  >       prev64
and     prev64  >       prev63
and     prev63  >       prev62
and     prev62  >       prev61
and     prev61  >       prev60
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
--and     prev45  >       prev44
--and     prev44  >       prev43
--and     prev43  >       prev42
--and     prev42  >       prev41
--and     prev41  >       prev40
--and     prev40  >       prev39
--and     prev39  >       prev38
--and     prev38  >       prev37
--and     prev37  >       prev36
--and     prev36  >       prev35
--and     prev35  >       prev34
--and     prev34  >       prev33
--and     prev33  >       prev32
--and     prev32  >       prev31
--and     prev31  >       prev30
--and     prev30  >       prev29
--and     prev29  >       prev28
--and     prev28  >       prev27
--and     prev27  >       prev26
--and     prev26  >       prev25
--and     prev25  >       prev24
--and     prev24  >       prev23
--and     prev23  >       prev22
--and     prev22  >       prev21
--and     prev21  >       prev20
--and     prev20  >       prev19
and prev1 < ma90

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and close_price > ma90
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and ma60 > ma90
and prev1_60 < prev1

and close_price < (p_max_price - 600000)
and sm.market_cap > 300000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma90(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma90_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma90_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_60,
lag(ma90,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma90,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma90,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma90,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma90,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma90,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma90,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma90,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma90,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma90,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma90,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma90,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma90,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma90,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma90,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma90,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma90,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma90,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma90,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma90,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma90,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma90,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma90,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma90,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma90,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma90,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma90,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma90,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma90,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma90,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma90,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma90,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma90,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma90,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma90,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma90,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma90,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma90,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma90,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma90,       41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma90,       42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma90,       43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma90,       44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma90,       45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma90,       46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma90,       47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma90,       48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma90,       49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma90,       50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma90,       51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma90,       52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma90,       53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma90,       54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma90,       55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma90,       56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma90,       57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma90,       58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma90,       59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma90,       60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
lag(ma90,       61)     over    (partition      by      code    order   by      trade_date)     as      prev61,
lag(ma90,       62)     over    (partition      by      code    order   by      trade_date)     as      prev62,
lag(ma90,       63)     over    (partition      by      code    order   by      trade_date)     as      prev63,
lag(ma90,       64)     over    (partition      by      code    order   by      trade_date)     as      prev64,
lag(ma90,       65)     over    (partition      by      code    order   by      trade_date)     as      prev65,
lag(ma90,       66)     over    (partition      by      code    order   by      trade_date)     as      prev66,
lag(ma90,       67)     over    (partition      by      code    order   by      trade_date)     as      prev67,
lag(ma90,       68)     over    (partition      by      code    order   by      trade_date)     as      prev68,
lag(ma90,       69)     over    (partition      by      code    order   by      trade_date)     as      prev69,
lag(ma90,       70)     over    (partition      by      code    order   by      trade_date)     as      prev70,
lag(ma90,       71)     over    (partition      by      code    order   by      trade_date)     as      prev71,
lag(ma90,       72)     over    (partition      by      code    order   by      trade_date)     as      prev72,
lag(ma90,       73)     over    (partition      by      code    order   by      trade_date)     as      prev73,
lag(ma90,       74)     over    (partition      by      code    order   by      trade_date)     as      prev74,
lag(ma90,       75)     over    (partition      by      code    order   by      trade_date)     as      prev75,
lag(ma90,       76)     over    (partition      by      code    order   by      trade_date)     as      prev76,
lag(ma90,       77)     over    (partition      by      code    order   by      trade_date)     as      prev77,
lag(ma90,       78)     over    (partition      by      code    order   by      trade_date)     as      prev78,
lag(ma90,       79)     over    (partition      by      code    order   by      trade_date)     as      prev79,
lag(ma90,       80)     over    (partition      by      code    order   by      trade_date)     as      prev80,
lag(ma90,       81)     over    (partition      by      code    order   by      trade_date)     as      prev81,
lag(ma90,       82)     over    (partition      by      code    order   by      trade_date)     as      prev82,
lag(ma90,       83)     over    (partition      by      code    order   by      trade_date)     as      prev83,
lag(ma90,       84)     over    (partition      by      code    order   by      trade_date)     as      prev84,
lag(ma90,       85)     over    (partition      by      code    order   by      trade_date)     as      prev85,
lag(ma90,       86)     over    (partition      by      code    order   by      trade_date)     as      prev86,
lag(ma90,       87)     over    (partition      by      code    order   by      trade_date)     as      prev87,
lag(ma90,       88)     over    (partition      by      code    order   by      trade_date)     as      prev88,
lag(ma90,       89)     over    (partition      by      code    order   by      trade_date)     as      prev89,
lag(ma90,       90)     over    (partition      by      code    order   by      trade_date)     as      prev90,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        90)     over    (partition      by      code    order   by      trade_date)     as      price_prev90
from stockmain
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date >= p_trade_date
and     prev90  >       prev89
and     prev89  >       prev88
and     prev88  >       prev87
and     prev87  >       prev86
and     prev86  >       prev85
and     prev85  >       prev84
and     prev84  >       prev83
and     prev83  >       prev82
and     prev82  >       prev81
and     prev81  >       prev80
and     prev80  >       prev79
and     prev79  >       prev78
and     prev78  >       prev77
and     prev77  >       prev76
and     prev76  >       prev75
and     prev75  >       prev74
and     prev74  >       prev73
and     prev73  >       prev72
and     prev72  >       prev71
and     prev71  >       prev70
and     prev70  >       prev69
and     prev69  >       prev68
and     prev68  >       prev67
and     prev67  >       prev66
and     prev66  >       prev65
and     prev65  >       prev64
and     prev64  >       prev63
and     prev63  >       prev62
and     prev62  >       prev61
and     prev61  >       prev60
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and prev1 < ma90

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and close_price > ma90
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and ma60 > ma90
and prev1_60 < prev1

and close_price < p_max_price
and sm.market_cap > 300000000000

and sfv.pbr < 3.0

order by trade_date
$$;


ALTER FUNCTION public.get_stock_ma90_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_sell(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_sell(p_trade_date date DEFAULT '2025-07-01'::date) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
select
    sm.code,
    sm.name
from stockmain sm join stock_ma mc
on sm.trade_date = mc.trade_date and sm.code = mc.code
--where ma20 > ma10
where ma10 > ma5
and sm.trade_date = p_trade_date
--and sm.code not in (
--'376930' --노을
--,'018880' --한온시스템
--)
$$;


ALTER FUNCTION public.get_stock_sell(p_trade_date date) OWNER TO postgres;

--
-- Name: get_stocketf_ma120(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma120(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma120,      1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_90,
lag(ma120,      2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma120,      3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma120,      4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma120,      5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma120,      6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma120,      7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma120,      8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma120,      9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma120,      10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma120,      11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma120,      12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma120,      13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma120,      14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma120,      15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma120,      16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma120,      17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma120,      18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma120,      19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma120,      20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma120,      21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma120,      22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma120,      23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma120,      24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma120,      25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma120,      26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma120,      27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma120,      28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma120,      29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma120,      30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma120,      31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma120,      32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma120,      33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma120,      34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma120,      35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma120,      36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma120,      37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma120,      38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma120,      39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma120,      40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma120,      41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma120,      42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma120,      43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma120,      44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma120,      45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma120,      46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma120,      47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma120,      48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma120,      49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma120,      50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma120,      51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma120,      52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma120,      53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma120,      54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma120,      55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma120,      56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma120,      57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma120,      58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma120,      59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma120,      60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
lag(ma120,      61)     over    (partition      by      code    order   by      trade_date)     as      prev61,
lag(ma120,      62)     over    (partition      by      code    order   by      trade_date)     as      prev62,
lag(ma120,      63)     over    (partition      by      code    order   by      trade_date)     as      prev63,
lag(ma120,      64)     over    (partition      by      code    order   by      trade_date)     as      prev64,
lag(ma120,      65)     over    (partition      by      code    order   by      trade_date)     as      prev65,
lag(ma120,      66)     over    (partition      by      code    order   by      trade_date)     as      prev66,
lag(ma120,      67)     over    (partition      by      code    order   by      trade_date)     as      prev67,
lag(ma120,      68)     over    (partition      by      code    order   by      trade_date)     as      prev68,
lag(ma120,      69)     over    (partition      by      code    order   by      trade_date)     as      prev69,
lag(ma120,      70)     over    (partition      by      code    order   by      trade_date)     as      prev70,
lag(ma120,      71)     over    (partition      by      code    order   by      trade_date)     as      prev71,
lag(ma120,      72)     over    (partition      by      code    order   by      trade_date)     as      prev72,
lag(ma120,      73)     over    (partition      by      code    order   by      trade_date)     as      prev73,
lag(ma120,      74)     over    (partition      by      code    order   by      trade_date)     as      prev74,
lag(ma120,      75)     over    (partition      by      code    order   by      trade_date)     as      prev75,
lag(ma120,      76)     over    (partition      by      code    order   by      trade_date)     as      prev76,
lag(ma120,      77)     over    (partition      by      code    order   by      trade_date)     as      prev77,
lag(ma120,      78)     over    (partition      by      code    order   by      trade_date)     as      prev78,
lag(ma120,      79)     over    (partition      by      code    order   by      trade_date)     as      prev79,
lag(ma120,      80)     over    (partition      by      code    order   by      trade_date)     as      prev80,
lag(ma120,      81)     over    (partition      by      code    order   by      trade_date)     as      prev81,
lag(ma120,      82)     over    (partition      by      code    order   by      trade_date)     as      prev82,
lag(ma120,      83)     over    (partition      by      code    order   by      trade_date)     as      prev83,
lag(ma120,      84)     over    (partition      by      code    order   by      trade_date)     as      prev84,
lag(ma120,      85)     over    (partition      by      code    order   by      trade_date)     as      prev85,
lag(ma120,      86)     over    (partition      by      code    order   by      trade_date)     as      prev86,
lag(ma120,      87)     over    (partition      by      code    order   by      trade_date)     as      prev87,
lag(ma120,      88)     over    (partition      by      code    order   by      trade_date)     as      prev88,
lag(ma120,      89)     over    (partition      by      code    order   by      trade_date)     as      prev89,
lag(ma120,      90)     over    (partition      by      code    order   by      trade_date)     as      prev90,
lag(ma120,      91)     over    (partition      by      code    order   by      trade_date)     as      prev91,
lag(ma120,      92)     over    (partition      by      code    order   by      trade_date)     as      prev92,
lag(ma120,      93)     over    (partition      by      code    order   by      trade_date)     as      prev93,
lag(ma120,      94)     over    (partition      by      code    order   by      trade_date)     as      prev94,
lag(ma120,      95)     over    (partition      by      code    order   by      trade_date)     as      prev95,
lag(ma120,      96)     over    (partition      by      code    order   by      trade_date)     as      prev96,
lag(ma120,      97)     over    (partition      by      code    order   by      trade_date)     as      prev97,
lag(ma120,      98)     over    (partition      by      code    order   by      trade_date)     as      prev98,
lag(ma120,      99)     over    (partition      by      code    order   by      trade_date)     as      prev99,
lag(ma120,      100)    over    (partition      by      code    order   by      trade_date)     as      prev100,
lag(ma120,      101)    over    (partition      by      code    order   by      trade_date)     as      prev101,
lag(ma120,      102)    over    (partition      by      code    order   by      trade_date)     as      prev102,
lag(ma120,      103)    over    (partition      by      code    order   by      trade_date)     as      prev103,
lag(ma120,      104)    over    (partition      by      code    order   by      trade_date)     as      prev104,
lag(ma120,      105)    over    (partition      by      code    order   by      trade_date)     as      prev105,
lag(ma120,      106)    over    (partition      by      code    order   by      trade_date)     as      prev106,
lag(ma120,      107)    over    (partition      by      code    order   by      trade_date)     as      prev107,
lag(ma120,      108)    over    (partition      by      code    order   by      trade_date)     as      prev108,
lag(ma120,      109)    over    (partition      by      code    order   by      trade_date)     as      prev109,
lag(ma120,      110)    over    (partition      by      code    order   by      trade_date)     as      prev110,
lag(ma120,      111)    over    (partition      by      code    order   by      trade_date)     as      prev111,
lag(ma120,      112)    over    (partition      by      code    order   by      trade_date)     as      prev112,
lag(ma120,      113)    over    (partition      by      code    order   by      trade_date)     as      prev113,
lag(ma120,      114)    over    (partition      by      code    order   by      trade_date)     as      prev114,
lag(ma120,      115)    over    (partition      by      code    order   by      trade_date)     as      prev115,
lag(ma120,      116)    over    (partition      by      code    order   by      trade_date)     as      prev116,
lag(ma120,      117)    over    (partition      by      code    order   by      trade_date)     as      prev117,
lag(ma120,      118)    over    (partition      by      code    order   by      trade_date)     as      prev118,
lag(ma120,      119)    over    (partition      by      code    order   by      trade_date)     as      prev119,
lag(ma120,      120)    over    (partition      by      code    order   by      trade_date)     as      prev120,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
where trade_date >= (date(p_trade_date) - 240) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,      120)    over    (partition      by      code    order   by      trade_date)     as      price_prev120
--from stocketf
--)
select
    sm.code,
    sm.name
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date = p_trade_date
and     prev120 >       prev119
and     prev119 >       prev118
and     prev118 >       prev117
and     prev117 >       prev116
and     prev116 >       prev115
and     prev115 >       prev114
and     prev114 >       prev113
and     prev113 >       prev112
and     prev112 >       prev111
and     prev111 >       prev110
and     prev110 >       prev109
and     prev109 >       prev108
and     prev108 >       prev107
and     prev107 >       prev106
and     prev106 >       prev105
and     prev105 >       prev104
and     prev104 >       prev103
and     prev103 >       prev102
and     prev102 >       prev101
and     prev101 >       prev100
and     prev100 >       prev99
and     prev99  >       prev98
and     prev98  >       prev97
and     prev97  >       prev96
and     prev96  >       prev95
and     prev95  >       prev94
and     prev94  >       prev93
and     prev93  >       prev92
and     prev92  >       prev91
and     prev91  >       prev90
and     prev90  >       prev89
and     prev89  >       prev88
and     prev88  >       prev87
and     prev87  >       prev86
and     prev86  >       prev85
and     prev85  >       prev84
and     prev84  >       prev83
and     prev83  >       prev82
and     prev82  >       prev81
and     prev81  >       prev80
and     prev80  >       prev79
and     prev79  >       prev78
and     prev78  >       prev77
and     prev77  >       prev76
and     prev76  >       prev75
and     prev75  >       prev74
and     prev74  >       prev73
and     prev73  >       prev72
and     prev72  >       prev71
and     prev71  >       prev70
and     prev70  >       prev69
and     prev69  >       prev68
and     prev68  >       prev67
and     prev67  >       prev66
and     prev66  >       prev65
and     prev65  >       prev64
and     prev64  >       prev63
and     prev63  >       prev62
and     prev62  >       prev61
and     prev61  >       prev60
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and prev1 < ma120

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and close_price > ma90
and close_price > ma120
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and ma60 > ma90
and ma90 > ma120
and prev1_90 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
$$;


ALTER FUNCTION public.get_stocketf_ma120(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma120_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma120_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma120,      1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_90,
lag(ma120,      2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma120,      3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma120,      4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma120,      5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma120,      6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma120,      7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma120,      8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma120,      9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma120,      10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma120,      11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma120,      12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma120,      13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma120,      14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma120,      15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma120,      16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma120,      17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma120,      18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma120,      19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma120,      20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma120,      21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma120,      22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma120,      23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma120,      24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma120,      25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma120,      26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma120,      27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma120,      28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma120,      29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma120,      30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma120,      31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma120,      32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma120,      33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma120,      34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma120,      35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma120,      36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma120,      37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma120,      38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma120,      39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma120,      40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma120,      41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma120,      42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma120,      43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma120,      44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma120,      45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma120,      46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma120,      47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma120,      48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma120,      49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma120,      50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma120,      51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma120,      52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma120,      53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma120,      54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma120,      55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma120,      56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma120,      57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma120,      58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma120,      59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma120,      60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
lag(ma120,      61)     over    (partition      by      code    order   by      trade_date)     as      prev61,
lag(ma120,      62)     over    (partition      by      code    order   by      trade_date)     as      prev62,
lag(ma120,      63)     over    (partition      by      code    order   by      trade_date)     as      prev63,
lag(ma120,      64)     over    (partition      by      code    order   by      trade_date)     as      prev64,
lag(ma120,      65)     over    (partition      by      code    order   by      trade_date)     as      prev65,
lag(ma120,      66)     over    (partition      by      code    order   by      trade_date)     as      prev66,
lag(ma120,      67)     over    (partition      by      code    order   by      trade_date)     as      prev67,
lag(ma120,      68)     over    (partition      by      code    order   by      trade_date)     as      prev68,
lag(ma120,      69)     over    (partition      by      code    order   by      trade_date)     as      prev69,
lag(ma120,      70)     over    (partition      by      code    order   by      trade_date)     as      prev70,
lag(ma120,      71)     over    (partition      by      code    order   by      trade_date)     as      prev71,
lag(ma120,      72)     over    (partition      by      code    order   by      trade_date)     as      prev72,
lag(ma120,      73)     over    (partition      by      code    order   by      trade_date)     as      prev73,
lag(ma120,      74)     over    (partition      by      code    order   by      trade_date)     as      prev74,
lag(ma120,      75)     over    (partition      by      code    order   by      trade_date)     as      prev75,
lag(ma120,      76)     over    (partition      by      code    order   by      trade_date)     as      prev76,
lag(ma120,      77)     over    (partition      by      code    order   by      trade_date)     as      prev77,
lag(ma120,      78)     over    (partition      by      code    order   by      trade_date)     as      prev78,
lag(ma120,      79)     over    (partition      by      code    order   by      trade_date)     as      prev79,
lag(ma120,      80)     over    (partition      by      code    order   by      trade_date)     as      prev80,
lag(ma120,      81)     over    (partition      by      code    order   by      trade_date)     as      prev81,
lag(ma120,      82)     over    (partition      by      code    order   by      trade_date)     as      prev82,
lag(ma120,      83)     over    (partition      by      code    order   by      trade_date)     as      prev83,
lag(ma120,      84)     over    (partition      by      code    order   by      trade_date)     as      prev84,
lag(ma120,      85)     over    (partition      by      code    order   by      trade_date)     as      prev85,
lag(ma120,      86)     over    (partition      by      code    order   by      trade_date)     as      prev86,
lag(ma120,      87)     over    (partition      by      code    order   by      trade_date)     as      prev87,
lag(ma120,      88)     over    (partition      by      code    order   by      trade_date)     as      prev88,
lag(ma120,      89)     over    (partition      by      code    order   by      trade_date)     as      prev89,
lag(ma120,      90)     over    (partition      by      code    order   by      trade_date)     as      prev90,
lag(ma120,      91)     over    (partition      by      code    order   by      trade_date)     as      prev91,
lag(ma120,      92)     over    (partition      by      code    order   by      trade_date)     as      prev92,
lag(ma120,      93)     over    (partition      by      code    order   by      trade_date)     as      prev93,
lag(ma120,      94)     over    (partition      by      code    order   by      trade_date)     as      prev94,
lag(ma120,      95)     over    (partition      by      code    order   by      trade_date)     as      prev95,
lag(ma120,      96)     over    (partition      by      code    order   by      trade_date)     as      prev96,
lag(ma120,      97)     over    (partition      by      code    order   by      trade_date)     as      prev97,
lag(ma120,      98)     over    (partition      by      code    order   by      trade_date)     as      prev98,
lag(ma120,      99)     over    (partition      by      code    order   by      trade_date)     as      prev99,
lag(ma120,      100)    over    (partition      by      code    order   by      trade_date)     as      prev100,
lag(ma120,      101)    over    (partition      by      code    order   by      trade_date)     as      prev101,
lag(ma120,      102)    over    (partition      by      code    order   by      trade_date)     as      prev102,
lag(ma120,      103)    over    (partition      by      code    order   by      trade_date)     as      prev103,
lag(ma120,      104)    over    (partition      by      code    order   by      trade_date)     as      prev104,
lag(ma120,      105)    over    (partition      by      code    order   by      trade_date)     as      prev105,
lag(ma120,      106)    over    (partition      by      code    order   by      trade_date)     as      prev106,
lag(ma120,      107)    over    (partition      by      code    order   by      trade_date)     as      prev107,
lag(ma120,      108)    over    (partition      by      code    order   by      trade_date)     as      prev108,
lag(ma120,      109)    over    (partition      by      code    order   by      trade_date)     as      prev109,
lag(ma120,      110)    over    (partition      by      code    order   by      trade_date)     as      prev110,
lag(ma120,      111)    over    (partition      by      code    order   by      trade_date)     as      prev111,
lag(ma120,      112)    over    (partition      by      code    order   by      trade_date)     as      prev112,
lag(ma120,      113)    over    (partition      by      code    order   by      trade_date)     as      prev113,
lag(ma120,      114)    over    (partition      by      code    order   by      trade_date)     as      prev114,
lag(ma120,      115)    over    (partition      by      code    order   by      trade_date)     as      prev115,
lag(ma120,      116)    over    (partition      by      code    order   by      trade_date)     as      prev116,
lag(ma120,      117)    over    (partition      by      code    order   by      trade_date)     as      prev117,
lag(ma120,      118)    over    (partition      by      code    order   by      trade_date)     as      prev118,
lag(ma120,      119)    over    (partition      by      code    order   by      trade_date)     as      prev119,
lag(ma120,      120)    over    (partition      by      code    order   by      trade_date)     as      prev120,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        120)    over    (partition      by      code    order   by      trade_date)     as      price_prev120
from stocketf
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date >= p_trade_date
and     prev120 >       prev119
and     prev119 >       prev118
and     prev118 >       prev117
and     prev117 >       prev116
and     prev116 >       prev115
and     prev115 >       prev114
and     prev114 >       prev113
and     prev113 >       prev112
and     prev112 >       prev111
and     prev111 >       prev110
and     prev110 >       prev109
and     prev109 >       prev108
and     prev108 >       prev107
and     prev107 >       prev106
and     prev106 >       prev105
and     prev105 >       prev104
and     prev104 >       prev103
and     prev103 >       prev102
and     prev102 >       prev101
and     prev101 >       prev100
and     prev100 >       prev99
and     prev99  >       prev98
and     prev98  >       prev97
and     prev97  >       prev96
and     prev96  >       prev95
and     prev95  >       prev94
and     prev94  >       prev93
and     prev93  >       prev92
and     prev92  >       prev91
and     prev91  >       prev90
and     prev90  >       prev89
and     prev89  >       prev88
and     prev88  >       prev87
and     prev87  >       prev86
and     prev86  >       prev85
and     prev85  >       prev84
and     prev84  >       prev83
and     prev83  >       prev82
and     prev82  >       prev81
and     prev81  >       prev80
and     prev80  >       prev79
and     prev79  >       prev78
and     prev78  >       prev77
and     prev77  >       prev76
and     prev76  >       prev75
and     prev75  >       prev74
and     prev74  >       prev73
and     prev73  >       prev72
and     prev72  >       prev71
and     prev71  >       prev70
and     prev70  >       prev69
and     prev69  >       prev68
and     prev68  >       prev67
and     prev67  >       prev66
and     prev66  >       prev65
and     prev65  >       prev64
and     prev64  >       prev63
and     prev63  >       prev62
and     prev62  >       prev61
and     prev61  >       prev60
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and prev1 < ma120

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and close_price > ma90
and close_price > ma120
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and ma60 > ma90
and ma90 > ma120
and prev1_90 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
order by trade_date
$$;


ALTER FUNCTION public.get_stocketf_ma120_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma20(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma20(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma10,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_10,
lag(ma20,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma20,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma20,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma20,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma20,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma20,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma20,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma20,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma20,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma20,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma20,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma20,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma20,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma20,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma20,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma20,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma20,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma20,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma20,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
where trade_date >= (date(p_trade_date) - 40) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,      20)     over    (partition      by      code    order   by      trade_date)     as      price_prev20
--from stocketf
--)
select
    sm.code,
    sm.name
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date = p_trade_date
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and     prev13  >       prev12
and     prev12  >       prev11
and     prev11  >       prev10
and     prev10  >       prev9
and     prev9   >       prev8
and     prev8   >       prev7
and     prev9   >       prev6
and     prev6   >       prev5
and prev1 < ma20

and close_price > ma5
and close_price > ma10
and close_price > ma20
and ma5 > ma10
and ma10 > ma20
and prev1_10 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
$$;


ALTER FUNCTION public.get_stocketf_ma20(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma20_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma20_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma10,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_10,
lag(ma20,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma20,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma20,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma20,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma20,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma20,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma20,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma20,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma20,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma20,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma20,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma20,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma20,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma20,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma20,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma20,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma20,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma20,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma20,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        20)     over    (partition      by      code    order   by      trade_date)     as      price_prev20
from stocketf
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date >= p_trade_date
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and     prev13  >       prev12
and     prev12  >       prev11
and     prev11  >       prev10
and     prev10  >       prev9
and     prev9   >       prev8
and     prev8   >       prev7
and     prev9   >       prev6
and     prev6   >       prev5
and prev1 < ma20

and close_price > ma5
and close_price > ma10
and close_price > ma20
and ma5 > ma10
and ma10 > ma20
and prev1_10 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
order by trade_date
$$;


ALTER FUNCTION public.get_stocketf_ma20_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma40(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma40(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_20,
lag(ma40,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma40,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma40,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma40,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma40,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma40,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma40,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma40,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma40,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma40,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma40,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma40,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma40,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma40,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma40,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma40,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma40,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma40,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma40,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma40,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma40,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma40,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma40,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma40,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma40,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma40,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma40,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma40,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma40,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma40,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma40,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma40,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma40,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma40,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma40,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma40,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma40,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma40,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma40,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
where trade_date >= (date(p_trade_date) - 80) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,        40)     over    (partition      by      code    order   by      trade_date)     as      price_prev40
--from stocketf
--)
select
    sm.code,
    sm.name
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date = p_trade_date
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and     prev13  >       prev12
and     prev12  >       prev11
and     prev11  >       prev10
and     prev10  >       prev9
and prev1 < ma40

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and prev1_20 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
$$;


ALTER FUNCTION public.get_stocketf_ma40(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma40_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma40_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_20,
lag(ma40,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma40,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma40,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma40,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma40,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma40,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma40,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma40,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma40,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma40,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma40,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma40,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma40,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma40,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma40,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma40,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma40,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma40,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma40,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma40,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma40,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma40,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma40,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma40,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma40,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma40,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma40,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma40,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma40,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma40,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma40,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma40,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma40,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma40,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma40,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma40,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma40,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma40,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma40,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        40)     over    (partition      by      code    order   by      trade_date)     as      price_prev40
from stocketf
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date >= p_trade_date
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and     prev13  >       prev12
and     prev12  >       prev11
and     prev11  >       prev10
and     prev10  >       prev9
and prev1 < ma40

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and prev1_20 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
order by trade_date
$$;


ALTER FUNCTION public.get_stocketf_ma40_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma60(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma60(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_40,
lag(ma60,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma60,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma60,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma60,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma60,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma60,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma60,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma60,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma60,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma60,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma60,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma60,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma60,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma60,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma60,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma60,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma60,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma60,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma60,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma60,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma60,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma60,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma60,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma60,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma60,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma60,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma60,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma60,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma60,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma60,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma60,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma60,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma60,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma60,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma60,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma60,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma60,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma60,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma60,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma60,       41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma60,       42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma60,       43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma60,       44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma60,       45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma60,       46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma60,       47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma60,       48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma60,       49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma60,       50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma60,       51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma60,       52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma60,       53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma60,       54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma60,       55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma60,       56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma60,       57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma60,       58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma60,       59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma60,       60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
where trade_date >= (date(p_trade_date) - 120) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,      60)     over    (partition      by      code    order   by      trade_date)     as      price_prev60
--from stocketf
--)
select
    sm.code,
    sm.name
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date = p_trade_date
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and prev1 < ma60

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and prev1_40 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
$$;


ALTER FUNCTION public.get_stocketf_ma60(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma60_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma60_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_40,
lag(ma60,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma60,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma60,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma60,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma60,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma60,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma60,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma60,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma60,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma60,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma60,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma60,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma60,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma60,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma60,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma60,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma60,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma60,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma60,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma60,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma60,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma60,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma60,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma60,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma60,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma60,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma60,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma60,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma60,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma60,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma60,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma60,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma60,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma60,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma60,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma60,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma60,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma60,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma60,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma60,       41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma60,       42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma60,       43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma60,       44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma60,       45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma60,       46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma60,       47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma60,       48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma60,       49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma60,       50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma60,       51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma60,       52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma60,       53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma60,       54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma60,       55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma60,       56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma60,       57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma60,       58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma60,       59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma60,       60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        60)     over    (partition      by      code    order   by      trade_date)     as      price_prev60
from stocketf
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date >= p_trade_date
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and     prev19  >       prev18
and     prev18  >       prev17
and     prev17  >       prev16
and     prev16  >       prev15
and     prev15  >       prev14
and     prev14  >       prev13
and prev1 < ma60

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and prev1_40 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
order by trade_date
$$;


ALTER FUNCTION public.get_stocketf_ma60_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma90(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma90(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_60,
lag(ma90,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma90,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma90,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma90,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma90,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma90,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma90,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma90,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma90,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma90,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma90,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma90,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma90,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma90,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma90,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma90,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma90,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma90,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma90,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma90,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma90,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma90,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma90,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma90,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma90,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma90,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma90,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma90,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma90,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma90,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma90,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma90,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma90,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma90,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma90,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma90,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma90,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma90,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma90,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma90,       41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma90,       42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma90,       43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma90,       44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma90,       45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma90,       46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma90,       47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma90,       48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma90,       49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma90,       50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma90,       51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma90,       52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma90,       53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma90,       54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma90,       55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma90,       56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma90,       57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma90,       58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma90,       59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma90,       60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
lag(ma90,       61)     over    (partition      by      code    order   by      trade_date)     as      prev61,
lag(ma90,       62)     over    (partition      by      code    order   by      trade_date)     as      prev62,
lag(ma90,       63)     over    (partition      by      code    order   by      trade_date)     as      prev63,
lag(ma90,       64)     over    (partition      by      code    order   by      trade_date)     as      prev64,
lag(ma90,       65)     over    (partition      by      code    order   by      trade_date)     as      prev65,
lag(ma90,       66)     over    (partition      by      code    order   by      trade_date)     as      prev66,
lag(ma90,       67)     over    (partition      by      code    order   by      trade_date)     as      prev67,
lag(ma90,       68)     over    (partition      by      code    order   by      trade_date)     as      prev68,
lag(ma90,       69)     over    (partition      by      code    order   by      trade_date)     as      prev69,
lag(ma90,       70)     over    (partition      by      code    order   by      trade_date)     as      prev70,
lag(ma90,       71)     over    (partition      by      code    order   by      trade_date)     as      prev71,
lag(ma90,       72)     over    (partition      by      code    order   by      trade_date)     as      prev72,
lag(ma90,       73)     over    (partition      by      code    order   by      trade_date)     as      prev73,
lag(ma90,       74)     over    (partition      by      code    order   by      trade_date)     as      prev74,
lag(ma90,       75)     over    (partition      by      code    order   by      trade_date)     as      prev75,
lag(ma90,       76)     over    (partition      by      code    order   by      trade_date)     as      prev76,
lag(ma90,       77)     over    (partition      by      code    order   by      trade_date)     as      prev77,
lag(ma90,       78)     over    (partition      by      code    order   by      trade_date)     as      prev78,
lag(ma90,       79)     over    (partition      by      code    order   by      trade_date)     as      prev79,
lag(ma90,       80)     over    (partition      by      code    order   by      trade_date)     as      prev80,
lag(ma90,       81)     over    (partition      by      code    order   by      trade_date)     as      prev81,
lag(ma90,       82)     over    (partition      by      code    order   by      trade_date)     as      prev82,
lag(ma90,       83)     over    (partition      by      code    order   by      trade_date)     as      prev83,
lag(ma90,       84)     over    (partition      by      code    order   by      trade_date)     as      prev84,
lag(ma90,       85)     over    (partition      by      code    order   by      trade_date)     as      prev85,
lag(ma90,       86)     over    (partition      by      code    order   by      trade_date)     as      prev86,
lag(ma90,       87)     over    (partition      by      code    order   by      trade_date)     as      prev87,
lag(ma90,       88)     over    (partition      by      code    order   by      trade_date)     as      prev88,
lag(ma90,       89)     over    (partition      by      code    order   by      trade_date)     as      prev89,
lag(ma90,       90)     over    (partition      by      code    order   by      trade_date)     as      prev90,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
where trade_date >= (date(p_trade_date) - 180) and trade_date <= p_trade_date
)--, close_price_check as (
--select
--trade_date, code,
--lag(close_price,      90)     over    (partition      by      code    order   by      trade_date)     as      price_prev90
--from stocketf
--)
select
    sm.code,
    sm.name
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date = p_trade_date
and     prev90  >       prev89
and     prev89  >       prev88
and     prev88  >       prev87
and     prev87  >       prev86
and     prev86  >       prev85
and     prev85  >       prev84
and     prev84  >       prev83
and     prev83  >       prev82
and     prev82  >       prev81
and     prev81  >       prev80
and     prev80  >       prev79
and     prev79  >       prev78
and     prev78  >       prev77
and     prev77  >       prev76
and     prev76  >       prev75
and     prev75  >       prev74
and     prev74  >       prev73
and     prev73  >       prev72
and     prev72  >       prev71
and     prev71  >       prev70
and     prev70  >       prev69
and     prev69  >       prev68
and     prev68  >       prev67
and     prev67  >       prev66
and     prev66  >       prev65
and     prev65  >       prev64
and     prev64  >       prev63
and     prev63  >       prev62
and     prev62  >       prev61
and     prev61  >       prev60
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and prev1 < ma90

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and close_price > ma90
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and ma60 > ma90
and prev1_60 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
$$;


ALTER FUNCTION public.get_stocketf_ma90(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_ma90_test(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_ma90_test(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(trade_date date, code character varying, name character varying, ma5 numeric, ma10 numeric, ma20 numeric, ma40 numeric, ma60 numeric, ma90 numeric, ma120 numeric)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_60,
lag(ma90,       2)      over    (partition      by      code    order   by      trade_date)     as      prev2,
lag(ma90,       3)      over    (partition      by      code    order   by      trade_date)     as      prev3,
lag(ma90,       4)      over    (partition      by      code    order   by      trade_date)     as      prev4,
lag(ma90,       5)      over    (partition      by      code    order   by      trade_date)     as      prev5,
lag(ma90,       6)      over    (partition      by      code    order   by      trade_date)     as      prev6,
lag(ma90,       7)      over    (partition      by      code    order   by      trade_date)     as      prev7,
lag(ma90,       8)      over    (partition      by      code    order   by      trade_date)     as      prev8,
lag(ma90,       9)      over    (partition      by      code    order   by      trade_date)     as      prev9,
lag(ma90,       10)     over    (partition      by      code    order   by      trade_date)     as      prev10,
lag(ma90,       11)     over    (partition      by      code    order   by      trade_date)     as      prev11,
lag(ma90,       12)     over    (partition      by      code    order   by      trade_date)     as      prev12,
lag(ma90,       13)     over    (partition      by      code    order   by      trade_date)     as      prev13,
lag(ma90,       14)     over    (partition      by      code    order   by      trade_date)     as      prev14,
lag(ma90,       15)     over    (partition      by      code    order   by      trade_date)     as      prev15,
lag(ma90,       16)     over    (partition      by      code    order   by      trade_date)     as      prev16,
lag(ma90,       17)     over    (partition      by      code    order   by      trade_date)     as      prev17,
lag(ma90,       18)     over    (partition      by      code    order   by      trade_date)     as      prev18,
lag(ma90,       19)     over    (partition      by      code    order   by      trade_date)     as      prev19,
lag(ma90,       20)     over    (partition      by      code    order   by      trade_date)     as      prev20,
lag(ma90,       21)     over    (partition      by      code    order   by      trade_date)     as      prev21,
lag(ma90,       22)     over    (partition      by      code    order   by      trade_date)     as      prev22,
lag(ma90,       23)     over    (partition      by      code    order   by      trade_date)     as      prev23,
lag(ma90,       24)     over    (partition      by      code    order   by      trade_date)     as      prev24,
lag(ma90,       25)     over    (partition      by      code    order   by      trade_date)     as      prev25,
lag(ma90,       26)     over    (partition      by      code    order   by      trade_date)     as      prev26,
lag(ma90,       27)     over    (partition      by      code    order   by      trade_date)     as      prev27,
lag(ma90,       28)     over    (partition      by      code    order   by      trade_date)     as      prev28,
lag(ma90,       29)     over    (partition      by      code    order   by      trade_date)     as      prev29,
lag(ma90,       30)     over    (partition      by      code    order   by      trade_date)     as      prev30,
lag(ma90,       31)     over    (partition      by      code    order   by      trade_date)     as      prev31,
lag(ma90,       32)     over    (partition      by      code    order   by      trade_date)     as      prev32,
lag(ma90,       33)     over    (partition      by      code    order   by      trade_date)     as      prev33,
lag(ma90,       34)     over    (partition      by      code    order   by      trade_date)     as      prev34,
lag(ma90,       35)     over    (partition      by      code    order   by      trade_date)     as      prev35,
lag(ma90,       36)     over    (partition      by      code    order   by      trade_date)     as      prev36,
lag(ma90,       37)     over    (partition      by      code    order   by      trade_date)     as      prev37,
lag(ma90,       38)     over    (partition      by      code    order   by      trade_date)     as      prev38,
lag(ma90,       39)     over    (partition      by      code    order   by      trade_date)     as      prev39,
lag(ma90,       40)     over    (partition      by      code    order   by      trade_date)     as      prev40,
lag(ma90,       41)     over    (partition      by      code    order   by      trade_date)     as      prev41,
lag(ma90,       42)     over    (partition      by      code    order   by      trade_date)     as      prev42,
lag(ma90,       43)     over    (partition      by      code    order   by      trade_date)     as      prev43,
lag(ma90,       44)     over    (partition      by      code    order   by      trade_date)     as      prev44,
lag(ma90,       45)     over    (partition      by      code    order   by      trade_date)     as      prev45,
lag(ma90,       46)     over    (partition      by      code    order   by      trade_date)     as      prev46,
lag(ma90,       47)     over    (partition      by      code    order   by      trade_date)     as      prev47,
lag(ma90,       48)     over    (partition      by      code    order   by      trade_date)     as      prev48,
lag(ma90,       49)     over    (partition      by      code    order   by      trade_date)     as      prev49,
lag(ma90,       50)     over    (partition      by      code    order   by      trade_date)     as      prev50,
lag(ma90,       51)     over    (partition      by      code    order   by      trade_date)     as      prev51,
lag(ma90,       52)     over    (partition      by      code    order   by      trade_date)     as      prev52,
lag(ma90,       53)     over    (partition      by      code    order   by      trade_date)     as      prev53,
lag(ma90,       54)     over    (partition      by      code    order   by      trade_date)     as      prev54,
lag(ma90,       55)     over    (partition      by      code    order   by      trade_date)     as      prev55,
lag(ma90,       56)     over    (partition      by      code    order   by      trade_date)     as      prev56,
lag(ma90,       57)     over    (partition      by      code    order   by      trade_date)     as      prev57,
lag(ma90,       58)     over    (partition      by      code    order   by      trade_date)     as      prev58,
lag(ma90,       59)     over    (partition      by      code    order   by      trade_date)     as      prev59,
lag(ma90,       60)     over    (partition      by      code    order   by      trade_date)     as      prev60,
lag(ma90,       61)     over    (partition      by      code    order   by      trade_date)     as      prev61,
lag(ma90,       62)     over    (partition      by      code    order   by      trade_date)     as      prev62,
lag(ma90,       63)     over    (partition      by      code    order   by      trade_date)     as      prev63,
lag(ma90,       64)     over    (partition      by      code    order   by      trade_date)     as      prev64,
lag(ma90,       65)     over    (partition      by      code    order   by      trade_date)     as      prev65,
lag(ma90,       66)     over    (partition      by      code    order   by      trade_date)     as      prev66,
lag(ma90,       67)     over    (partition      by      code    order   by      trade_date)     as      prev67,
lag(ma90,       68)     over    (partition      by      code    order   by      trade_date)     as      prev68,
lag(ma90,       69)     over    (partition      by      code    order   by      trade_date)     as      prev69,
lag(ma90,       70)     over    (partition      by      code    order   by      trade_date)     as      prev70,
lag(ma90,       71)     over    (partition      by      code    order   by      trade_date)     as      prev71,
lag(ma90,       72)     over    (partition      by      code    order   by      trade_date)     as      prev72,
lag(ma90,       73)     over    (partition      by      code    order   by      trade_date)     as      prev73,
lag(ma90,       74)     over    (partition      by      code    order   by      trade_date)     as      prev74,
lag(ma90,       75)     over    (partition      by      code    order   by      trade_date)     as      prev75,
lag(ma90,       76)     over    (partition      by      code    order   by      trade_date)     as      prev76,
lag(ma90,       77)     over    (partition      by      code    order   by      trade_date)     as      prev77,
lag(ma90,       78)     over    (partition      by      code    order   by      trade_date)     as      prev78,
lag(ma90,       79)     over    (partition      by      code    order   by      trade_date)     as      prev79,
lag(ma90,       80)     over    (partition      by      code    order   by      trade_date)     as      prev80,
lag(ma90,       81)     over    (partition      by      code    order   by      trade_date)     as      prev81,
lag(ma90,       82)     over    (partition      by      code    order   by      trade_date)     as      prev82,
lag(ma90,       83)     over    (partition      by      code    order   by      trade_date)     as      prev83,
lag(ma90,       84)     over    (partition      by      code    order   by      trade_date)     as      prev84,
lag(ma90,       85)     over    (partition      by      code    order   by      trade_date)     as      prev85,
lag(ma90,       86)     over    (partition      by      code    order   by      trade_date)     as      prev86,
lag(ma90,       87)     over    (partition      by      code    order   by      trade_date)     as      prev87,
lag(ma90,       88)     over    (partition      by      code    order   by      trade_date)     as      prev88,
lag(ma90,       89)     over    (partition      by      code    order   by      trade_date)     as      prev89,
lag(ma90,       90)     over    (partition      by      code    order   by      trade_date)     as      prev90,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stocketf_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,        90)     over    (partition      by      code    order   by      trade_date)     as      price_prev90
from stocketf
)
select sm.trade_date,
    sm.code,
    sm.name,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40, mc.ma60, mc.ma90, mc.ma120
from stocketf sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date >= p_trade_date
and     prev90  >       prev89
and     prev89  >       prev88
and     prev88  >       prev87
and     prev87  >       prev86
and     prev86  >       prev85
and     prev85  >       prev84
and     prev84  >       prev83
and     prev83  >       prev82
and     prev82  >       prev81
and     prev81  >       prev80
and     prev80  >       prev79
and     prev79  >       prev78
and     prev78  >       prev77
and     prev77  >       prev76
and     prev76  >       prev75
and     prev75  >       prev74
and     prev74  >       prev73
and     prev73  >       prev72
and     prev72  >       prev71
and     prev71  >       prev70
and     prev70  >       prev69
and     prev69  >       prev68
and     prev68  >       prev67
and     prev67  >       prev66
and     prev66  >       prev65
and     prev65  >       prev64
and     prev64  >       prev63
and     prev63  >       prev62
and     prev62  >       prev61
and     prev61  >       prev60
and     prev60  >       prev59
and     prev59  >       prev58
and     prev58  >       prev57
and     prev57  >       prev56
and     prev56  >       prev55
and     prev55  >       prev54
and     prev54  >       prev53
and     prev53  >       prev52
and     prev52  >       prev51
and     prev51  >       prev50
and     prev50  >       prev49
and     prev49  >       prev48
and     prev48  >       prev47
and     prev47  >       prev46
and     prev46  >       prev45
and     prev45  >       prev44
and     prev44  >       prev43
and     prev43  >       prev42
and     prev42  >       prev41
and     prev41  >       prev40
and     prev40  >       prev39
and     prev39  >       prev38
and     prev38  >       prev37
and     prev37  >       prev36
and     prev36  >       prev35
and     prev35  >       prev34
and     prev34  >       prev33
and     prev33  >       prev32
and     prev32  >       prev31
and     prev31  >       prev30
and     prev30  >       prev29
and     prev29  >       prev28
and     prev28  >       prev27
and     prev27  >       prev26
and     prev26  >       prev25
and     prev25  >       prev24
and     prev24  >       prev23
and     prev23  >       prev22
and     prev22  >       prev21
and     prev21  >       prev20
and     prev20  >       prev19
and prev1 < ma90

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and close_price > ma60
and close_price > ma90
and ma5 > ma10
and ma10 > ma20
and ma20 > ma40
and ma40 > ma60
and ma60 > ma90
and prev1_60 < prev1

and close_price < p_max_price
and sm.trade_value > 500000000
order by trade_date
$$;


ALTER FUNCTION public.get_stocketf_ma90_test(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stocketf_sell(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stocketf_sell(p_trade_date date DEFAULT '2025-07-01'::date) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
select
    sm.code,
    sm.name
from stocketf sm join stocketf_ma mc
on sm.trade_date = mc.trade_date and sm.code = mc.code
where ma20 > ma10
and sm.trade_date = p_trade_date
$$;


ALTER FUNCTION public.get_stocketf_sell(p_trade_date date) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: stock_ma; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stock_ma (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    ma5 numeric(15,2),
    ma10 numeric(15,2),
    ma20 numeric(15,2),
    ma40 numeric(15,2),
    ma60 numeric(15,2),
    ma90 numeric(15,2),
    ma120 numeric(15,2),
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.stock_ma OWNER TO postgres;

--
-- Name: stocketf; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stocketf (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    name character varying(100),
    close_price numeric(15,2),
    change_price numeric(15,2),
    change_rate numeric(7,4),
    open_price numeric(15,2),
    high_price numeric(15,2),
    low_price numeric(15,2),
    volume bigint,
    trade_value bigint,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.stocketf OWNER TO postgres;

--
-- Name: TABLE stocketf; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.stocketf IS '일별 ETF(상장지수펀드) 시세 데이터 테이블';


--
-- Name: COLUMN stocketf.trade_date; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.trade_date IS '거래일 (YYYY-MM-DD 형식)';


--
-- Name: COLUMN stocketf.code; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.code IS 'ETF 종목 코드 (KRX 기준)';


--
-- Name: COLUMN stocketf.name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.name IS 'ETF 명칭 (예: KODEX 200, TIGER 미국나스닥100 등)';


--
-- Name: COLUMN stocketf.close_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.close_price IS '종가 (해당 거래일 장 마감 시 최종 가격)';


--
-- Name: COLUMN stocketf.change_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.change_price IS '전일 대비 가격 변화 (종가 - 전일 종가)';


--
-- Name: COLUMN stocketf.change_rate; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.change_rate IS '전일 대비 등락률 ((종가 - 전일 종가) / 전일 종가 * 100)';


--
-- Name: COLUMN stocketf.open_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.open_price IS '시가 (장 시작 시 첫 거래 가격)';


--
-- Name: COLUMN stocketf.high_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.high_price IS '고가 (장중 최고 거래 가격)';


--
-- Name: COLUMN stocketf.low_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.low_price IS '저가 (장중 최저 거래 가격)';


--
-- Name: COLUMN stocketf.volume; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.volume IS '거래량 (해당 거래일 동안의 총 거래 수량)';


--
-- Name: COLUMN stocketf.trade_value; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.trade_value IS '거래대금 (해당 거래일 동안의 총 거래 금액, 단위: 원)';


--
-- Name: COLUMN stocketf.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stocketf.created_at IS '데이터 생성 시각 (레코드 입력 시 자동 등록)';


--
-- Name: stocketf_ma; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stocketf_ma (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    ma5 numeric(15,2),
    ma10 numeric(15,2),
    ma20 numeric(15,2),
    ma40 numeric(15,2),
    ma60 numeric(15,2),
    ma90 numeric(15,2),
    ma120 numeric(15,2),
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.stocketf_ma OWNER TO postgres;

--
-- Name: stockfdt; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stockfdt (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    name character varying(100),
    close_price integer,
    change_price integer,
    change_rate numeric(10,2),
    eps numeric(15,2),
    per numeric(10,2),
    forward_eps numeric(15,2),
    forward_per numeric(10,2),
    bps numeric(15,2),
    pbr numeric(10,2),
    dividend_per_share integer,
    dividend_yield numeric(10,2),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.stockfdt OWNER TO postgres;

--
-- Name: TABLE stockfdt; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.stockfdt IS 'KRX 종목별 재무지표 데이터 (PER, PBR, 배당수익률 등)';


--
-- Name: COLUMN stockfdt.trade_date; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.trade_date IS '거래일자';


--
-- Name: COLUMN stockfdt.code; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.code IS '종목코드 (6자리)';


--
-- Name: COLUMN stockfdt.name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.name IS '종목명';


--
-- Name: COLUMN stockfdt.close_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.close_price IS '종가 (원)';


--
-- Name: COLUMN stockfdt.change_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.change_price IS '전일 대비 가격 변동 (원)';


--
-- Name: COLUMN stockfdt.change_rate; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.change_rate IS '등락률 (%)';


--
-- Name: COLUMN stockfdt.eps; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.eps IS 'EPS - 주당순이익 (Earnings Per Share)';


--
-- Name: COLUMN stockfdt.per; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.per IS 'PER - 주가수익비율 (Price Earnings Ratio)';


--
-- Name: COLUMN stockfdt.forward_eps; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.forward_eps IS '선행 EPS - 미래 예상 주당순이익';


--
-- Name: COLUMN stockfdt.forward_per; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.forward_per IS '선행 PER - 미래 예상 주가수익비율';


--
-- Name: COLUMN stockfdt.bps; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.bps IS 'BPS - 주당순자산가치 (Book-value Per Share)';


--
-- Name: COLUMN stockfdt.pbr; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.pbr IS 'PBR - 주가순자산비율 (Price Book-value Ratio)';


--
-- Name: COLUMN stockfdt.dividend_per_share; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.dividend_per_share IS '주당배당금 (원)';


--
-- Name: COLUMN stockfdt.dividend_yield; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.dividend_yield IS '배당수익률 (%)';


--
-- Name: COLUMN stockfdt.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdt.created_at IS '데이터 최초 생성 시각';


--
-- Name: stockfdt_pbr_v; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.stockfdt_pbr_v AS
 SELECT stockfdt.trade_date,
    stockfdt.code,
    stockfdt.pbr,
    stockfdt.per,
    (((stockfdt.pbr / stockfdt.per) * (100)::numeric))::numeric(10,2) AS roe
   FROM public.stockfdt;


ALTER TABLE public.stockfdt_pbr_v OWNER TO postgres;

--
-- Name: stockmain; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stockmain (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    name character varying(100),
    close_price numeric(15,2),
    change_price numeric(15,2),
    change_rate numeric(7,4),
    open_price numeric(15,2),
    high_price numeric(15,2),
    low_price numeric(15,2),
    volume bigint,
    trade_value bigint,
    market_cap bigint,
    shares_out bigint,
    sector character varying(50),
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.stockmain OWNER TO postgres;

--
-- Name: TABLE stockmain; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.stockmain IS '일별 종목별 주가 및 거래 기본 데이터 테이블';


--
-- Name: COLUMN stockmain.trade_date; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.trade_date IS '거래일 (YYYY-MM-DD 형식)';


--
-- Name: COLUMN stockmain.code; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.code IS '종목 코드 (KRX 기준 종목코드)';


--
-- Name: COLUMN stockmain.name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.name IS '종목명';


--
-- Name: COLUMN stockmain.close_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.close_price IS '종가 (거래일 기준 최종 가격)';


--
-- Name: COLUMN stockmain.change_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.change_price IS '전일 대비 가격 변화 (종가 - 전일 종가)';


--
-- Name: COLUMN stockmain.change_rate; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.change_rate IS '전일 대비 등락률 ((종가 - 전일 종가) / 전일 종가 * 100)';


--
-- Name: COLUMN stockmain.open_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.open_price IS '시가 (장 시작 시 첫 거래 가격)';


--
-- Name: COLUMN stockmain.high_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.high_price IS '고가 (장중 최고 거래 가격)';


--
-- Name: COLUMN stockmain.low_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.low_price IS '저가 (장중 최저 거래 가격)';


--
-- Name: COLUMN stockmain.volume; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.volume IS '거래량 (해당 거래일 동안의 총 거래 주식 수)';


--
-- Name: COLUMN stockmain.trade_value; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.trade_value IS '거래대금 (해당 거래일 동안의 총 거래 금액, 단위: 원)';


--
-- Name: COLUMN stockmain.market_cap; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.market_cap IS '시가총액 (종가 × 상장주식수, 단위: 원)';


--
-- Name: COLUMN stockmain.shares_out; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.shares_out IS '상장주식수 (해당 거래일 기준 총 발행 주식 수)';


--
-- Name: COLUMN stockmain.sector; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.sector IS '업종명 또는 섹터 구분 (예: 반도체, 2차전지 등)';


--
-- Name: COLUMN stockmain.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmain.created_at IS '데이터 생성 시각 (레코드 입력 시 자동 등록)';


--
-- Name: stock_ma stock_ma_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stock_ma
    ADD CONSTRAINT stock_ma_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stocketf_ma stocketf_ma_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stocketf_ma
    ADD CONSTRAINT stocketf_ma_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stocketf stocketf_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stocketf
    ADD CONSTRAINT stocketf_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stockfdt stockfdt_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stockfdt
    ADD CONSTRAINT stockfdt_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stockmain stockmain_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stockmain
    ADD CONSTRAINT stockmain_pkey PRIMARY KEY (trade_date, code);


--
-- PostgreSQL database dump complete
--
