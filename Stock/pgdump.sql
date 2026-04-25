--
-- PostgreSQL database dump
--

\restrict KSc0Ewf66wvmfigrm6gavkfurVvO3Ac9q3diT6JYVbhRaqyM2cnogwqDCb8niLK

-- Dumped from database version 13.23
-- Dumped by pg_dump version 13.23

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

CREATE FUNCTION public.get_stock_filter_results_dividend(p_trade_date date) RETURNS TABLE(code text, name text, close_price integer, change_rate numeric, trade_value_uk integer, market_cap_chunuk integer, pbr numeric, per numeric, forward_per numeric, dividend_per_share numeric, dividend_yield numeric, roe numeric, forward_roe numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.code::text,
        a.name::text,
        a.close_price::int,
        a.change_rate::numeric(5,2),
        --((a.close_price - a.open_price) / a.close_price * 100)::numeric(5,2) AS ratio,
        (a.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (a.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        c.pbr::numeric,
        c.per::numeric,
        c.forward_per::numeric,
        c.dividend_per_share::numeric,
        c.dividend_yield::numeric,
        (c.pbr/c.per*100)::decimal(10,2) as roe,
        (c.pbr/c.forward_per*100)::decimal(10,2) as forward_roe
    FROM stockmain a
    JOIN stock_ma b
      ON a.trade_date = b.trade_date AND a.code = b.code
    JOIN stockfdt c
      ON a.trade_date = c.trade_date AND a.code = c.code
    WHERE a.trade_date = p_trade_date
--      AND a.close_price < 300000
      --AND a.market_cap > 1000000000000   --1조
      AND a.market_cap > 500000000000   --5천억
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

CREATE FUNCTION public.get_stock_filter_results_pbr(p_trade_date date) RETURNS TABLE(code text, name text, close_price integer, change_rate numeric, trade_value_uk integer, market_cap_chunuk integer, pbr numeric, per numeric, forward_per numeric, dividend_per_share numeric, dividend_yield numeric, roe numeric, forward_roe numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.code::text,
        a.name::text,
        a.close_price::int,
        a.change_rate::numeric(5,2),
        --((a.close_price - a.open_price) / a.close_price * 100)::numeric(5,2) AS ratio,
        (a.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (a.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        c.pbr::numeric,
        c.per::numeric,
        c.forward_per::numeric,
        c.dividend_per_share::numeric,
        c.dividend_yield::numeric,
        (c.pbr/c.per*100)::decimal(10,2) as roe,
        (c.pbr/c.forward_per*100)::decimal(10,2) as forward_roe
    FROM stockmain a
    JOIN stock_ma b
      ON a.trade_date = b.trade_date AND a.code = b.code
    JOIN stockfdt c
      ON a.trade_date = c.trade_date AND a.code = c.code
    WHERE a.trade_date = p_trade_date
--      AND a.close_price < 300000
      --AND a.market_cap > 1000000000000   --1조
      AND a.market_cap > 500000000000   --5천억
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
-- Name: get_stock_filter_results_roe(date); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_filter_results_roe(p_trade_date date) RETURNS TABLE(code text, name text, close_price integer, change_rate numeric, trade_value_uk integer, market_cap_chunuk integer, pbr numeric, per numeric, forward_per numeric, dividend_per_share numeric, dividend_yield numeric, roe numeric, forward_roe numeric)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        a.code::text,
        a.name::text,
        a.close_price::int,
        a.change_rate::numeric(5,2),
        --((a.close_price - a.open_price) / a.close_price * 100)::numeric(5,2) AS ratio,
        (a.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (a.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        c.pbr::numeric,
        c.per::numeric,
        c.forward_per::numeric,
        c.dividend_per_share::numeric,
        c.dividend_yield::numeric,
        (c.pbr/c.per*100)::decimal(10,2) as roe,
        (c.pbr/c.forward_per*100)::decimal(10,2) as forward_roe
    FROM stockmain a
    JOIN stock_ma b
      ON a.trade_date = b.trade_date AND a.code = b.code
    JOIN stockfdt c
      ON a.trade_date = c.trade_date AND a.code = c.code
    WHERE a.trade_date = p_trade_date
--      AND a.close_price < 300000
      --AND a.market_cap > 1000000000000   --1조
      AND a.market_cap > 500000000000   --5천억
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

and c.pbr is not null
and c.per is not null

    ORDER BY roe DESC
    LIMIT 300;
END;
$$;


ALTER FUNCTION public.get_stock_filter_results_roe(p_trade_date date) OWNER TO postgres;

--
-- Name: get_stock_filter_results_total(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_filter_results_total() RETURNS TABLE("종목코드" character varying, "종목명" character varying, "종가" numeric, "등락률(%)" numeric, "거래량" bigint, "거래량배수(5일평균比)" numeric, "거래대금" text, "시가총액" text, "PER" numeric, "선행PER" numeric, "PBR" numeric, "ROE(%)" numeric, "선행ROE(%)" numeric, "배당수익률(%)" numeric, "종합점수" integer, "등급" text)
    LANGUAGE sql STABLE
    AS $$

WITH latest_date AS (
    SELECT MAX(trade_date) AS last_date
    FROM stockmain
),

base AS (
    SELECT m.*
    FROM stockmain m
    JOIN latest_date ld ON m.trade_date = ld.last_date
),

base_ma AS (
    SELECT ma.*
    FROM stock_ma ma
    JOIN latest_date ld ON ma.trade_date = ld.last_date
),

base_fdt AS (
    SELECT f.*
    FROM stockfdt f
    JOIN latest_date ld ON f.trade_date = ld.last_date
),

base_pbr_v AS (
    SELECT v.*
    FROM stockfdt_pbr_v v
    JOIN latest_date ld ON v.trade_date = ld.last_date
),

avg_vol_5d AS (
    SELECT
        s.code,
        AVG(s.volume) AS avg_vol_5d,
        MAX(s.volume) AS max_vol_5d
    FROM stockmain s
    JOIN latest_date ld
      ON s.trade_date BETWEEN ld.last_date - INTERVAL '7 days'
                          AND ld.last_date - INTERVAL '1 day'
    GROUP BY s.code
),

scoring AS (
    SELECT
        m.code,
        m.name,
        m.sector,
        m.close_price                                             AS 종가,
        ROUND(m.change_rate, 2)                                   AS 등락률,
        m.volume                                                  AS 거래량,
        ROUND(m.volume / NULLIF(av.avg_vol_5d, 0), 2)            AS 거래량배수,
        ROUND(m.trade_value / 100000000.0, 0)                    AS 거래대금_억,
        ROUND(m.market_cap  / 100000000.0, 0)                    AS 시가총액_억,
        ma.ma5,
        ma.ma20,
        ma.ma60,
        ma.ma120,
        f.per,
        f.forward_per,
        f.pbr,
        f.dividend_yield,
        ROUND(v.roe, 2)                                           AS roe,
        ROUND(v.forward_roe, 2)                                   AS forward_roe,
        av.avg_vol_5d,

        -- ★ 멀티팩터 점수 산출 (최대 17점)

        -- [기술-1] MA 정배열 (최대 3점)
        CASE
            WHEN m.close_price > ma.ma5
             AND ma.ma5        > ma.ma20
             AND ma.ma20       > ma.ma60  THEN 3
            WHEN m.close_price > ma.ma5
             AND ma.ma5        > ma.ma20  THEN 1
            ELSE 0
        END

        -- [기술-2] 장기 추세 (1점)
        + CASE
            WHEN ma.ma120 IS NOT NULL
             AND m.close_price > ma.ma120 THEN 1
            ELSE 0
          END

        -- [기술-3] 거래량 급증 (최대 3점)
        + CASE
            WHEN av.avg_vol_5d > 0 AND m.volume >= av.avg_vol_5d * 2.0 THEN 3
            WHEN av.avg_vol_5d > 0 AND m.volume >= av.avg_vol_5d * 1.8 THEN 2
            WHEN av.avg_vol_5d > 0 AND m.volume >= av.avg_vol_5d * 1.4 THEN 1
            ELSE 0
          END

        -- [기술-4] 캔들 강도 (최대 2점)
        + CASE
            WHEN m.close_price > m.open_price
             AND m.close_price = m.high_price                          THEN 2
            WHEN m.close_price > m.open_price
             AND (m.high_price - m.close_price)
                 <= (m.close_price - m.open_price) * 0.3               THEN 1
            ELSE 0
          END

        -- [기술-5] 과열 없는 건강한 상승 (1점)
        + CASE
            WHEN m.change_rate BETWEEN 0.5 AND 3.0 THEN 1
            ELSE 0
          END

        -- [가치-1] PBR 저평가 (최대 3점)
        + CASE
            WHEN f.pbr IS NOT NULL AND f.pbr > 0 AND f.pbr < 0.8      THEN 3
            WHEN f.pbr IS NOT NULL AND f.pbr BETWEEN 0.8  AND 1.2     THEN 2
            WHEN f.pbr IS NOT NULL AND f.pbr BETWEEN 1.2  AND 1.5     THEN 1
            ELSE 0
          END

        -- [가치-2] ROE 수익성 (최대 3점)
        + CASE
            WHEN v.roe IS NOT NULL AND v.roe >= 20                     THEN 3
            WHEN v.roe IS NOT NULL AND v.roe BETWEEN 15 AND 20        THEN 2
            WHEN v.roe IS NOT NULL AND v.roe BETWEEN 10 AND 15        THEN 1
            ELSE 0
          END

        -- [가치-3] 이익 성장 기대 (최대 2점)
        + CASE
            WHEN f.forward_per IS NOT NULL AND f.per IS NOT NULL
             AND f.forward_per > 0 AND f.per > 0
             AND f.forward_per < f.per * 0.8                           THEN 2
            WHEN f.forward_per IS NOT NULL AND f.per IS NOT NULL
             AND f.forward_per > 0 AND f.per > 0
             AND f.forward_per < f.per                                 THEN 1
            ELSE 0
          END

        AS total_score

    FROM base m
    JOIN  base_ma    ma ON m.code = ma.code
    LEFT JOIN base_fdt   f  ON m.code = f.code
    LEFT JOIN base_pbr_v v  ON m.code = v.code
    LEFT JOIN avg_vol_5d av ON m.code = av.code

    WHERE
        m.market_cap  >= 150000000000          -- 시총 1,500억 이상
        --AND m.market_cap  <= 5000000000000     -- 시총 5조 이하
        AND m.trade_value >= 5000000000        -- 거래대금 50억 이상
        AND f.per > 0 AND f.per <= 15          -- PER 0 초과 15 이하
        AND f.pbr > 0 AND f.pbr <= 3.0        -- PBR 0 초과 3 이하
        AND v.roe >= 8.0                       -- ROE 8% 이상
        AND m.close_price > ma.ma20            -- 종가 > MA20
        AND ma.ma5        >= ma.ma20           -- MA5 >= MA20 (정배열)
)

SELECT
    code::VARCHAR(20)                                          AS 종목코드,
    name::VARCHAR(100)                                         AS 종목명,
    --sector::VARCHAR(50)                                        AS 섹터,
    종가,
    등락률,
    거래량,
    거래량배수,
    TO_CHAR(거래대금_억, 'FM999,999,999') || ' 억'            AS 거래대금,
    TO_CHAR(시가총액_억, 'FM999,999,999') || ' 억'            AS 시가총액,
    --ma5,
    --ma20,
    --ma60,
    --ma120,
    per,
    forward_per,
    pbr,
    roe,
    forward_roe,
    dividend_yield,
    total_score,
    CASE
        WHEN total_score >= 14 THEN '★★★ 최우선'
        WHEN total_score >= 11 THEN '★★  우선'
        WHEN total_score >= 8  THEN '★   관심'
        ELSE                        '△  대기'
    END                                                        AS 등급

FROM scoring
WHERE total_score >= 8
ORDER BY
    total_score DESC,
    거래량배수  DESC,
    roe         DESC
LIMIT 30;

$$;


ALTER FUNCTION public.get_stock_filter_results_total() OWNER TO postgres;

--
-- Name: get_stock_filter_results_total(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_filter_results_total(p_date text) RETURNS TABLE("종목코드" character varying, "종목명" character varying, "종가" numeric, "등락률(%)" numeric, "거래량" bigint, "거래량배수(5일평균比)" numeric, "거래대금" text, "시가총액" text, "PER" numeric, "선행PER" numeric, "PBR" numeric, "ROE(%)" numeric, "선행ROE(%)" numeric, "배당수익률(%)" numeric, "종합점수" integer, "등급" text)
    LANGUAGE plpgsql STABLE
    AS $_$
DECLARE
    v_target_date DATE;
    v_actual_date DATE;  -- 입력일이 휴장일일 경우 직전 거래일로 대체
BEGIN
    -- ── 입력값 파싱: 'YYYYMMDD' 또는 'YYYY-MM-DD' 모두 허용
    BEGIN
        v_target_date := TO_DATE(p_date,
            CASE
                WHEN p_date ~ '^\d{8}$'         THEN 'YYYYMMDD'   -- 20260304
                WHEN p_date ~ '^\d{4}-\d{2}-\d{2}$' THEN 'YYYY-MM-DD' -- 2026-03-04
                ELSE NULL
            END
        );
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '날짜 형식 오류: "%" → YYYYMMDD 또는 YYYY-MM-DD 형식으로 입력하세요.', p_date;
    END;

    -- ── 입력일이 휴장일(데이터 없음)이면 직전 거래일 자동 대체
    SELECT MAX(trade_date) INTO v_actual_date
    FROM stockmain
    WHERE trade_date <= v_target_date;

    IF v_actual_date IS NULL THEN
        RAISE EXCEPTION '입력일(%) 이전의 거래 데이터가 없습니다.', p_date;
    END IF;

    IF v_actual_date < v_target_date THEN
        RAISE NOTICE '입력일(%)은 휴장일입니다. 직전 거래일(%)로 대체합니다.',
            v_target_date, v_actual_date;
    END IF;

    -- ── 메인 쿼리 반환
    RETURN QUERY
    WITH

    base AS (
        SELECT m.*
        FROM stockmain m
        WHERE m.trade_date = v_actual_date
    ),

    base_ma AS (
        SELECT ma.*
        FROM stock_ma ma
        WHERE ma.trade_date = v_actual_date
    ),

    base_fdt AS (
        SELECT f.*
        FROM stockfdt f
        WHERE f.trade_date = v_actual_date
    ),

    base_pbr_v AS (
        SELECT v.*
        FROM stockfdt_pbr_v v
        WHERE v.trade_date = v_actual_date
    ),

    -- 직전 5 거래일 평균 거래량 (입력 기준일 제외)
    avg_vol_5d AS (
        SELECT
            s.code,
            AVG(s.volume) AS avg_vol_5d,
            MAX(s.volume) AS max_vol_5d
        FROM stockmain s
        WHERE s.trade_date BETWEEN v_actual_date - INTERVAL '7 days'
                                AND v_actual_date - INTERVAL '1 day'
        GROUP BY s.code
    ),

    scoring AS (
        SELECT
            m.code,
            m.name,
            m.sector,
            m.close_price                                         AS 종가,
            ROUND(m.change_rate, 2)                               AS 등락률,
            m.volume                                              AS 거래량,
            ROUND(m.volume / NULLIF(av.avg_vol_5d, 0), 2)        AS 거래량배수,
            ROUND(m.trade_value / 100000000.0, 0)                AS 거래대금_억,
            ROUND(m.market_cap  / 100000000.0, 0)                AS 시가총액_억,
            ma.ma5,
            ma.ma20,
            ma.ma60,
            ma.ma120,
            f.per,
            f.forward_per,
            f.pbr,
            f.dividend_yield,
            ROUND(v.roe, 2)                                       AS roe,
            ROUND(v.forward_roe, 2)                               AS forward_roe,
            av.avg_vol_5d,

            -- ── 멀티팩터 점수 산출 (최대 17점) ──────────────────────

            -- [기술-1] MA 정배열 (최대 3점)
            CASE
                WHEN m.close_price > ma.ma5
                 AND ma.ma5        > ma.ma20
                 AND ma.ma20       > ma.ma60  THEN 3
                WHEN m.close_price > ma.ma5
                 AND ma.ma5        > ma.ma20  THEN 1
                ELSE 0
            END
            -- [기술-2] 장기 추세 (1점)
            + CASE
                WHEN ma.ma120 IS NOT NULL
                 AND m.close_price > ma.ma120 THEN 1
                ELSE 0
              END
            -- [기술-3] 거래량 급증 (최대 3점)
            + CASE
                WHEN av.avg_vol_5d > 0 AND m.volume >= av.avg_vol_5d * 2.0 THEN 3
                WHEN av.avg_vol_5d > 0 AND m.volume >= av.avg_vol_5d * 1.8 THEN 2
                WHEN av.avg_vol_5d > 0 AND m.volume >= av.avg_vol_5d * 1.4 THEN 1
                ELSE 0
              END
            -- [기술-4] 캔들 강도 (최대 2점)
            + CASE
                WHEN m.close_price > m.open_price
                 AND m.close_price = m.high_price                        THEN 2
                WHEN m.close_price > m.open_price
                 AND (m.high_price - m.close_price)
                     <= (m.close_price - m.open_price) * 0.3             THEN 1
                ELSE 0
              END
            -- [기술-5] 과열 없는 건강한 상승 (1점)
            + CASE
                WHEN m.change_rate BETWEEN 0.5 AND 3.0 THEN 1
                ELSE 0
              END
            -- [가치-1] PBR 저평가 (최대 3점)
            + CASE
                WHEN f.pbr IS NOT NULL AND f.pbr > 0 AND f.pbr < 0.8     THEN 3
                WHEN f.pbr IS NOT NULL AND f.pbr BETWEEN 0.8  AND 1.2    THEN 2
                WHEN f.pbr IS NOT NULL AND f.pbr BETWEEN 1.2  AND 1.5    THEN 1
                ELSE 0
              END
            -- [가치-2] ROE 수익성 (최대 3점)
            + CASE
                WHEN v.roe IS NOT NULL AND v.roe >= 20                    THEN 3
                WHEN v.roe IS NOT NULL AND v.roe BETWEEN 15 AND 20       THEN 2
                WHEN v.roe IS NOT NULL AND v.roe BETWEEN 10 AND 15       THEN 1
                ELSE 0
              END
            -- [가치-3] 이익 성장 기대 (최대 2점)
            + CASE
                WHEN f.forward_per IS NOT NULL AND f.per IS NOT NULL
                 AND f.forward_per > 0 AND f.per > 0
                 AND f.forward_per < f.per * 0.8                          THEN 2
                WHEN f.forward_per IS NOT NULL AND f.per IS NOT NULL
                 AND f.forward_per > 0 AND f.per > 0
                 AND f.forward_per < f.per                                THEN 1
                ELSE 0
              END

            AS total_score

        FROM base m
        JOIN  base_ma    ma ON m.code = ma.code
        LEFT JOIN base_fdt   f  ON m.code = f.code
        LEFT JOIN base_pbr_v v  ON m.code = v.code
        LEFT JOIN avg_vol_5d av ON m.code = av.code

        WHERE
            m.market_cap  >= 150000000000       -- 시총 1,500억 이상
            --AND m.market_cap  <= 5000000000000  -- 시총 5조 이하
            AND m.trade_value >= 5000000000     -- 거래대금 50억 이상
            AND f.per > 0 AND f.per <= 15       -- PER 0 초과 15 이하
            AND f.pbr > 0 AND f.pbr <= 3.0     -- PBR 0 초과 3 이하
            AND v.roe >= 8.0                    -- ROE 8% 이상
            AND m.close_price > ma.ma20         -- 종가 > MA20
            AND ma.ma5        >= ma.ma20        -- MA5 >= MA20 (정배열)
    )

    SELECT
        s.code::VARCHAR(20),
        s.name::VARCHAR(100),
        --s.sector::VARCHAR(50),
        s.종가,
        s.등락률,
        s.거래량,
        s.거래량배수,
        TO_CHAR(s.거래대금_억, 'FM999,999,999') || ' 억',
        TO_CHAR(s.시가총액_억, 'FM999,999,999') || ' 억',
        --s.ma5,
        --s.ma20,
        --s.ma60,
        --s.ma120,
        s.per,
        s.forward_per,
        s.pbr,
        s.roe,
        s.forward_roe,
        s.dividend_yield,
        s.total_score,
        CASE
            WHEN s.total_score >= 14 THEN '★★★ 최우선'
            WHEN s.total_score >= 11 THEN '★★  우선'
            WHEN s.total_score >= 8  THEN '★   관심'
            ELSE                          '△  대기'
        END
        --v_actual_date                            -- 실제 적용된 거래일 반환

    FROM scoring s
    WHERE s.total_score >= 8
    ORDER BY
        s.total_score DESC,
        s.거래량배수  DESC,
        s.roe         DESC
    LIMIT 30;

END;
$_$;


ALTER FUNCTION public.get_stock_filter_results_total(p_date text) OWNER TO postgres;

--
-- Name: get_stock_ma10(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma10(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
WITH ma_check AS (
    SELECT trade_date, code,
        LAG(ma10, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1,
        LAG(ma5,  1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1_5,
        ma5, ma10, ma20, ma30, ma40, ma50, ma60, ma70, ma80, ma90, ma100, ma110, ma120
    FROM stock_ma
    WHERE trade_date >= (DATE(p_trade_date) - 15) AND trade_date <= p_trade_date
)
SELECT sm.code, sm.name
FROM stockmain sm
JOIN ma_check mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
JOIN stockfdt_pbr_v sfv ON sm.trade_date = sfv.trade_date AND sm.code = sfv.code
WHERE sm.trade_date = p_trade_date

-- 1. 골든크로스 조건 (어제는 5일선이 10일선 아래)
AND mc.prev1_5 < mc.prev1

-- 2. 핵심 정배열 유지
AND sm.close_price > mc.ma5
AND mc.ma5 > mc.ma10

-- 3. 억제기 (아직 20일선은 위에 있음)
AND mc.ma20 > mc.ma10

-- 4. 수급 및 가치 필터
AND sm.close_price < p_max_price
AND sm.market_cap > 500000000000
AND sm.change_rate < 5.0
AND sm.trade_value > 5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 20.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15
AND (sm.close_price >= mc.ma5 * 1.015 OR sm.change_rate >= 3.0)
AND sm.change_rate > 0.0
$$;


ALTER FUNCTION public.get_stock_ma10(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma100(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma100(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma100,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma90,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_90,
ma5,ma10,ma20,ma30,ma40,ma50,ma60,ma70,ma80,ma90,ma100,ma110,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 15) and trade_date <= p_trade_date
)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date

and prev1 < ma100

and close_price > ma5
and ma5 > ma10
and ma10 > ma20
and ma20 > ma30
and ma30 > ma40
and ma40 > ma50
and ma50 > ma60
and ma60 > ma70
and ma70 > ma80
and ma80 > ma90
and ma90 > ma100
and prev1_90 < prev1

and ma110 > ma100

and close_price < p_max_price
and sm.market_cap > 500000000000
and sm.change_rate < 15.0
and sm.trade_value > 5000000000

and ((sfv.pbr >= 0.0 and sfv.pbr < 1.0) or (
sfv.pbr >= 0.0 and sfv.pbr < 3.0
and sfv.per >= 0.0 and sfv.per < 25.0
and sfv.roe > 5.0
))

-- 새로 추가된 안전장치 3개
-- 1. 추격 매수 금지 (단기 과열 방지): 모든 함수 동일하게 ma20 사용!
-- (120일 정배열이든 60일 정배열이든, 최근 20일선 대비 15% 이상 붕 떠있으면 단기 고점이므로 안 산다)
AND sm.close_price <= mc.ma20 * 1.15
-- 2. 미세 돌파 속임수 방지 (당일 모멘텀 확인): 당일 단기 추세인 ma5를 기준!
-- (개선 아이디어) 5일선 대비 1% 이상 높거나, '당일 상승률이 2% 이상'으로 힘 있게 올라갔다면 인정!
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)
-- 3. 당일 양봉/상승 마감 확정
AND sm.change_rate > 0.0

$$;


ALTER FUNCTION public.get_stock_ma100(p_trade_date date, p_max_price numeric) OWNER TO postgres;

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
-- Name: get_stock_ma110(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma110(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma110,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma100,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_100,
ma5,ma10,ma20,ma30,ma40,ma50,ma60,ma70,ma80,ma90,ma100,ma110,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 15) and trade_date <= p_trade_date
)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date

and prev1 < ma110

and close_price > ma5
and ma5 > ma10
and ma10 > ma20
and ma20 > ma30
and ma30 > ma40
and ma40 > ma50
and ma50 > ma60
and ma60 > ma70
and ma70 > ma80
and ma80 > ma90
and ma90 > ma100
and ma100 > ma110
and prev1_100 < prev1

and ma120 > ma110

and close_price < p_max_price
and sm.market_cap > 500000000000
and sm.change_rate < 15.0
and sm.trade_value > 5000000000

and ((sfv.pbr >= 0.0 and sfv.pbr < 1.0) or (
sfv.pbr >= 0.0 and sfv.pbr < 3.0
and sfv.per >= 0.0 and sfv.per < 25.0
and sfv.roe > 5.0
))

-- 새로 추가된 안전장치 3개
-- 1. 추격 매수 금지 (단기 과열 방지): 모든 함수 동일하게 ma20 사용!
-- (120일 정배열이든 60일 정배열이든, 최근 20일선 대비 15% 이상 붕 떠있으면 단기 고점이므로 안 산다)
AND sm.close_price <= mc.ma20 * 1.15
-- 2. 미세 돌파 속임수 방지 (당일 모멘텀 확인): 당일 단기 추세인 ma5를 기준!
-- (개선 아이디어) 5일선 대비 1% 이상 높거나, '당일 상승률이 2% 이상'으로 힘 있게 올라갔다면 인정!
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)
-- 3. 당일 양봉/상승 마감 확정
AND sm.change_rate > 0.0

$$;


ALTER FUNCTION public.get_stock_ma110(p_trade_date date, p_max_price numeric) OWNER TO postgres;

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
lag(ma120,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma110,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_110,
ma5,ma10,ma20,ma30,ma40,ma50,ma60,ma70,ma80,ma90,ma100,ma110,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 15) and trade_date <= p_trade_date
)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date

and prev1 < ma120

and close_price > ma5
and ma5 > ma10
and ma10 > ma20
and ma20 > ma30
and ma30 > ma40
and ma40 > ma50
and ma50 > ma60
and ma60 > ma70
and ma70 > ma80
and ma80 > ma90
and ma90 > ma100
and ma100 > ma110
and ma110 > ma120
and prev1_110 < prev1

--and ma130 > ma120

and close_price < p_max_price
and sm.market_cap > 500000000000
and sm.change_rate < 15.0
and sm.trade_value > 5000000000

and ((sfv.pbr >= 0.0 and sfv.pbr < 1.0) or (
sfv.pbr >= 0.0 and sfv.pbr < 3.0
and sfv.per >= 0.0 and sfv.per < 25.0
and sfv.roe > 5.0
))

-- 새로 추가된 안전장치 3개
-- 1. 추격 매수 금지 (단기 과열 방지): 모든 함수 동일하게 ma20 사용!
-- (120일 정배열이든 60일 정배열이든, 최근 20일선 대비 15% 이상 붕 떠있으면 단기 고점이므로 안 산다)
AND sm.close_price <= mc.ma20 * 1.15
-- 2. 미세 돌파 속임수 방지 (당일 모멘텀 확인): 당일 단기 추세인 ma5를 기준!
-- (개선 아이디어) 5일선 대비 1% 이상 높거나, '당일 상승률이 2% 이상'으로 힘 있게 올라갔다면 인정!
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)
-- 3. 당일 양봉/상승 마감 확정
AND sm.change_rate > 0.0

$$;


ALTER FUNCTION public.get_stock_ma120(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma120_origin(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma120_origin(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
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
and sm.market_cap > 500000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma120_origin(p_trade_date date, p_max_price numeric) OWNER TO postgres;

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
WITH ma_check AS (
    SELECT trade_date, code,
        LAG(ma20, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1,
        LAG(ma10, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1_10,
        ma5, ma10, ma20, ma30, ma40, ma50, ma60, ma70, ma80, ma90, ma100, ma110, ma120
    FROM stock_ma
    WHERE trade_date >= (DATE(p_trade_date) - 15) AND trade_date <= p_trade_date
)
SELECT sm.code, sm.name
FROM stockmain sm
JOIN ma_check mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
JOIN stockfdt_pbr_v sfv ON sm.trade_date = sfv.trade_date AND sm.code = sfv.code
WHERE sm.trade_date = p_trade_date

-- 1. 골든크로스 조건 (어제는 10일선이 20일선 아래)
AND mc.prev1_10 < mc.prev1

-- 2. 핵심 정배열 유지
AND sm.close_price > mc.ma5
AND mc.ma5 > mc.ma10
AND mc.ma10 > mc.ma20

-- 3. 억제기 (아직 40일선은 위에 있음 - 30일선 제외)
AND mc.ma40 > mc.ma20

-- 4. 수급 및 가치 필터
AND sm.close_price < p_max_price
AND sm.market_cap > 500000000000
AND sm.change_rate < 5.0
AND sm.trade_value > 5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 20.0 AND sfv.roe > 7.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15
AND (sm.close_price >= mc.ma5 * 1.015 OR sm.change_rate >= 3.0)
AND sm.change_rate > 0.0
$$;


ALTER FUNCTION public.get_stock_ma20(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma20_origin(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma20_origin(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
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
and sm.market_cap > 500000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma20_origin(p_trade_date date, p_max_price numeric) OWNER TO postgres;

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
-- Name: get_stock_ma30(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma30(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma30,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma20,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_20,
ma5,ma10,ma20,ma30,ma40,ma50,ma60,ma70,ma80,ma90,ma100,ma110,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 15) and trade_date <= p_trade_date
)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date

and prev1 < ma30

and close_price > ma5
and ma5 > ma10
and ma10 > ma20
and ma20 > ma30
and prev1_20 < prev1

and ma40 > ma30

and close_price < p_max_price
and sm.market_cap > 500000000000
and sm.change_rate < 15.0
and sm.trade_value > 5000000000

and ((sfv.pbr >= 0.0 and sfv.pbr < 1.0) or (
sfv.pbr >= 0.0 and sfv.pbr < 3.0
and sfv.per >= 0.0 and sfv.per < 25.0
and sfv.roe > 5.0
))

-- 새로 추가된 안전장치 3개
-- 1. 추격 매수 금지 (단기 과열 방지): 모든 함수 동일하게 ma20 사용!
-- (120일 정배열이든 60일 정배열이든, 최근 20일선 대비 15% 이상 붕 떠있으면 단기 고점이므로 안 산다)
AND sm.close_price <= mc.ma20 * 1.15
-- 2. 미세 돌파 속임수 방지 (당일 모멘텀 확인): 당일 단기 추세인 ma5를 기준!
-- (개선 아이디어) 5일선 대비 1% 이상 높거나, '당일 상승률이 2% 이상'으로 힘 있게 올라갔다면 인정!
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)
-- 3. 당일 양봉/상승 마감 확정
AND sm.change_rate > 0.0

$$;


ALTER FUNCTION public.get_stock_ma30(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma40(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma40(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
WITH ma_check AS (
    SELECT trade_date, code,
        LAG(ma40, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1,
        LAG(ma20, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1_20,
        ma5, ma10, ma20, ma30, ma40, ma50, ma60, ma70, ma80, ma90, ma100, ma110, ma120
    FROM stock_ma
    WHERE trade_date >= (DATE(p_trade_date) - 15) AND trade_date <= p_trade_date
)
SELECT sm.code, sm.name
FROM stockmain sm
JOIN ma_check mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
JOIN stockfdt_pbr_v sfv ON sm.trade_date = sfv.trade_date AND sm.code = sfv.code
WHERE sm.trade_date = p_trade_date

-- 1. 골든크로스 조건 (어제는 20일선이 40일선 아래)
AND mc.prev1_20 < mc.prev1

-- 2. 핵심 정배열 유지 (30일선 조건 삭제)
AND sm.close_price > mc.ma5
AND mc.ma5 > mc.ma10
AND mc.ma10 > mc.ma20
AND mc.ma20 > mc.ma40

-- 3. 억제기 (아직 60일선은 위에 있음 - 50일선 제외)
AND mc.ma60 > mc.ma40

-- 4. 수급 및 가치 필터
AND sm.close_price < p_max_price
AND sm.market_cap > 500000000000
AND sm.change_rate < 5.0
AND sm.trade_value > 5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 20.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15
AND (sm.close_price >= mc.ma5 * 1.015 OR sm.change_rate >= 3.0)
AND sm.change_rate > 0.0
$$;


ALTER FUNCTION public.get_stock_ma40(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma40_origin(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma40_origin(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
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
and sm.market_cap > 500000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma40_origin(p_trade_date date, p_max_price numeric) OWNER TO postgres;

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
-- Name: get_stock_ma50(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma50(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma50,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma40,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_40,
ma5,ma10,ma20,ma30,ma40,ma50,ma60,ma70,ma80,ma90,ma100,ma110,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 15) and trade_date <= p_trade_date
)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date

and prev1 < ma50

and close_price > ma5
and ma5 > ma10
and ma10 > ma20
and ma20 > ma30
and ma30 > ma40
and ma40 > ma50
and prev1_40 < prev1

and ma60 > ma50

and close_price < p_max_price
and sm.market_cap > 500000000000
and sm.change_rate < 15.0
and sm.trade_value > 5000000000

and ((sfv.pbr >= 0.0 and sfv.pbr < 1.0) or (
sfv.pbr >= 0.0 and sfv.pbr < 3.0
and sfv.per >= 0.0 and sfv.per < 25.0
and sfv.roe > 5.0
))

-- 새로 추가된 안전장치 3개
-- 1. 추격 매수 금지 (단기 과열 방지): 모든 함수 동일하게 ma20 사용!
-- (120일 정배열이든 60일 정배열이든, 최근 20일선 대비 15% 이상 붕 떠있으면 단기 고점이므로 안 산다)
AND sm.close_price <= mc.ma20 * 1.15
-- 2. 미세 돌파 속임수 방지 (당일 모멘텀 확인): 당일 단기 추세인 ma5를 기준!
-- (개선 아이디어) 5일선 대비 1% 이상 높거나, '당일 상승률이 2% 이상'으로 힘 있게 올라갔다면 인정!
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)
-- 3. 당일 양봉/상승 마감 확정
AND sm.change_rate > 0.0

$$;


ALTER FUNCTION public.get_stock_ma50(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma60(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma60(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
WITH ma_check AS (
    SELECT trade_date, code,
        LAG(ma60, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1,
        LAG(ma40, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1_40,
        ma5, ma10, ma20, ma30, ma40, ma50, ma60, ma70, ma80, ma90, ma100, ma110, ma120
    FROM stock_ma
    WHERE trade_date >= (DATE(p_trade_date) - 15) AND trade_date <= p_trade_date
)
SELECT sm.code, sm.name
FROM stockmain sm
JOIN ma_check mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
JOIN stockfdt_pbr_v sfv ON sm.trade_date = sfv.trade_date AND sm.code = sfv.code
WHERE sm.trade_date = p_trade_date

-- 1. 골든크로스 조건 (어제는 40일선이 60일선 아래)
AND mc.prev1_40 < mc.prev1

-- 2. 핵심 정배열 유지 (30, 50일선 조건 삭제)
AND sm.close_price > mc.ma5
AND mc.ma5 > mc.ma10
AND mc.ma10 > mc.ma20
AND mc.ma20 > mc.ma40
AND mc.ma40 > mc.ma60

-- 3. 억제기 (가장 무거운 장기 이평선 120일선은 아직 위에 있음)
AND mc.ma120 > mc.ma60

-- 4. 수급 및 가치 필터
AND sm.close_price < p_max_price
AND sm.market_cap > 500000000000
AND sm.change_rate < 5.0
AND sm.trade_value > 5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 20.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15
AND (sm.close_price >= mc.ma5 * 1.015 OR sm.change_rate >= 3.0)
AND sm.change_rate > 0.0
$$;


ALTER FUNCTION public.get_stock_ma60(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma60_origin(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma60_origin(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
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
and sm.market_cap > 500000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma60_origin(p_trade_date date, p_max_price numeric) OWNER TO postgres;

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
-- Name: get_stock_ma70(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma70(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma70,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma60,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_60,
ma5,ma10,ma20,ma30,ma40,ma50,ma60,ma70,ma80,ma90,ma100,ma110,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 15) and trade_date <= p_trade_date
)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date

and prev1 < ma70

and close_price > ma5
and ma5 > ma10
and ma10 > ma20
and ma20 > ma30
and ma30 > ma40
and ma40 > ma50
and ma50 > ma60
and ma60 > ma70
and prev1_60 < prev1

and ma80 > ma70

and close_price < p_max_price
and sm.market_cap > 500000000000
and sm.change_rate < 15.0
and sm.trade_value > 5000000000

and ((sfv.pbr >= 0.0 and sfv.pbr < 1.0) or (
sfv.pbr >= 0.0 and sfv.pbr < 3.0
and sfv.per >= 0.0 and sfv.per < 25.0
and sfv.roe > 5.0
))

-- 새로 추가된 안전장치 3개
-- 1. 추격 매수 금지 (단기 과열 방지): 모든 함수 동일하게 ma20 사용!
-- (120일 정배열이든 60일 정배열이든, 최근 20일선 대비 15% 이상 붕 떠있으면 단기 고점이므로 안 산다)
AND sm.close_price <= mc.ma20 * 1.15
-- 2. 미세 돌파 속임수 방지 (당일 모멘텀 확인): 당일 단기 추세인 ma5를 기준!
-- (개선 아이디어) 5일선 대비 1% 이상 높거나, '당일 상승률이 2% 이상'으로 힘 있게 올라갔다면 인정!
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)
-- 3. 당일 양봉/상승 마감 확정
AND sm.change_rate > 0.0

$$;


ALTER FUNCTION public.get_stock_ma70(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma80(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma80(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
    LANGUAGE sql
    AS $$
with ma_check as (
select
trade_date,
code,
lag(ma80,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1,
lag(ma70,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_70,
ma5,ma10,ma20,ma30,ma40,ma50,ma60,ma70,ma80,ma90,ma100,ma110,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 15) and trade_date <= p_trade_date
)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date

and prev1 < ma80

and close_price > ma5
and ma5 > ma10
and ma10 > ma20
and ma20 > ma30
and ma30 > ma40
and ma40 > ma50
and ma50 > ma60
and ma60 > ma70
and ma70 > ma80
and prev1_70 < prev1

and ma90 > ma80

and close_price < p_max_price
and sm.market_cap > 500000000000
and sm.change_rate < 15.0
and sm.trade_value > 5000000000

and ((sfv.pbr >= 0.0 and sfv.pbr < 1.0) or (
sfv.pbr >= 0.0 and sfv.pbr < 3.0
and sfv.per >= 0.0 and sfv.per < 25.0
and sfv.roe > 5.0
))

-- 새로 추가된 안전장치 3개
-- 1. 추격 매수 금지 (단기 과열 방지): 모든 함수 동일하게 ma20 사용!
-- (120일 정배열이든 60일 정배열이든, 최근 20일선 대비 15% 이상 붕 떠있으면 단기 고점이므로 안 산다)
AND sm.close_price <= mc.ma20 * 1.15
-- 2. 미세 돌파 속임수 방지 (당일 모멘텀 확인): 당일 단기 추세인 ma5를 기준!
-- (개선 아이디어) 5일선 대비 1% 이상 높거나, '당일 상승률이 2% 이상'으로 힘 있게 올라갔다면 인정!
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)
-- 3. 당일 양봉/상승 마감 확정
AND sm.change_rate > 0.0

$$;


ALTER FUNCTION public.get_stock_ma80(p_trade_date date, p_max_price numeric) OWNER TO postgres;

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
lag(ma80,       1)      over    (partition      by      code    order   by      trade_date)     as      prev1_80,
ma5,ma10,ma20,ma30,ma40,ma50,ma60,ma70,ma80,ma90,ma100,ma110,ma120
from stock_ma
where trade_date >= (date(p_trade_date) - 15) and trade_date <= p_trade_date
)
select
    sm.code,
    sm.name
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
--    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
    join stockfdt_pbr_v sfv on sm.trade_date = sfv.trade_date and sm.code = sfv.code
where
    sm.trade_date = p_trade_date

and prev1 < ma90

and close_price > ma5
and ma5 > ma10
and ma10 > ma20
and ma20 > ma30
and ma30 > ma40
and ma40 > ma50
and ma50 > ma60
and ma60 > ma70
and ma70 > ma80
and ma80 > ma90
and prev1_80 < prev1

and ma100 > ma90

and close_price < p_max_price
and sm.market_cap > 500000000000
and sm.change_rate < 15.0
and sm.trade_value > 5000000000

and ((sfv.pbr >= 0.0 and sfv.pbr < 1.0) or (
sfv.pbr >= 0.0 and sfv.pbr < 3.0
and sfv.per >= 0.0 and sfv.per < 25.0
and sfv.roe > 5.0
))

-- 새로 추가된 안전장치 3개
-- 1. 추격 매수 금지 (단기 과열 방지): 모든 함수 동일하게 ma20 사용!
-- (120일 정배열이든 60일 정배열이든, 최근 20일선 대비 15% 이상 붕 떠있으면 단기 고점이므로 안 산다)
AND sm.close_price <= mc.ma20 * 1.15
-- 2. 미세 돌파 속임수 방지 (당일 모멘텀 확인): 당일 단기 추세인 ma5를 기준!
-- (개선 아이디어) 5일선 대비 1% 이상 높거나, '당일 상승률이 2% 이상'으로 힘 있게 올라갔다면 인정!
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)
-- 3. 당일 양봉/상승 마감 확정
AND sm.change_rate > 0.0

$$;


ALTER FUNCTION public.get_stock_ma90(p_trade_date date, p_max_price numeric) OWNER TO postgres;

--
-- Name: get_stock_ma90_origin(date, numeric); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_stock_ma90_origin(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000) RETURNS TABLE(code character varying, name character varying)
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
and sm.market_cap > 500000000000
and sm.change_rate < 15.0

and ((sfv.pbr < 1.0) or (
sfv.pbr > 0.1 and sfv.pbr < 3.0
and sfv.per > 1.0 and sfv.per < 30.0
and sfv.roe > 3.0
))
$$;


ALTER FUNCTION public.get_stock_ma90_origin(p_trade_date date, p_max_price numeric) OWNER TO postgres;

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
SELECT
    sm.code,
    sm.name
FROM stockmain sm
JOIN stock_ma mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
WHERE sm.trade_date = p_trade_date
  -- 매도 핵심 로직: 오늘 종가가 10일선 대비 확실하게 3% 이상 뚫고 내려갔을 때 (단기 지지선 붕괴 확정)
  AND sm.close_price < mc.ma10 * 0.97
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

--
-- Name: run_backtest(text, text, bigint); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.run_backtest(p_start_date text DEFAULT '20260101'::text, p_end_date text DEFAULT '20260315'::text, p_invest_per bigint DEFAULT 1000000) RETURNS TABLE("매수일" date, "종목코드" character varying, "종목명" character varying, "섹터" character varying, "등급" text, "매수가" numeric, "매수주수" integer, "실투자금" bigint, "매도일" date, "매도가" numeric, "회수금" bigint, "손익금액" bigint, "수익률_pct" numeric, "보유일수" integer, "매도사유" text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_start       DATE;
    v_end         DATE;
    v_db_last     DATE;     -- ★ DB에 존재하는 실제 마지막 거래일
    v_date        DATE;
    v_shares      INTEGER;
    v_actual_inv  BIGINT;
    r_pos         RECORD;
    r_buy         RECORD;
    r_sell        RECORD;
    v_sold        BOOLEAN;
BEGIN
    v_start := TO_DATE(p_start_date,
        CASE WHEN p_start_date ~ '^\d{8}$' THEN 'YYYYMMDD' ELSE 'YYYY-MM-DD' END);
    v_end   := TO_DATE(p_end_date,
        CASE WHEN p_end_date   ~ '^\d{8}$' THEN 'YYYYMMDD' ELSE 'YYYY-MM-DD' END);

    -- ★ DB 실제 마지막 거래일 조회
    SELECT MAX(trade_date) INTO v_db_last FROM stockmain;

    IF v_db_last IS NULL THEN
        RAISE EXCEPTION 'stockmain 테이블에 데이터가 없습니다.';
    END IF;

    DROP TABLE IF EXISTS _bt_positions;
    CREATE TEMP TABLE _bt_positions (
        buy_date    DATE,
        code        VARCHAR(20),
        name        VARCHAR(100),
        sector      VARCHAR(50),
        grade       TEXT,
        buy_price   NUMERIC(15,2),
        shares      INTEGER,
        actual_inv  BIGINT
    );

    -- ================================================================
    -- PHASE 1. p_start_date ~ p_end_date : 매수 + 매도 동시 진행
    -- ================================================================
    FOR v_date IN
        SELECT DISTINCT trade_date
        FROM   stockmain
        WHERE  trade_date BETWEEN v_start AND v_end
        ORDER  BY trade_date
    LOOP

        -- ── 보유 종목 매도 조건 체크
        FOR r_pos IN SELECT * FROM _bt_positions LOOP

            SELECT sm.close_price
            INTO   r_sell
            FROM   stockmain sm
            JOIN   stock_ma  ma ON sm.trade_date = ma.trade_date
                               AND sm.code       = ma.code
            WHERE  sm.trade_date = v_date
              AND  sm.code       = r_pos.code
              AND  sm.close_price < ma.ma10 * 0.97;

            IF FOUND THEN
                매수일     := r_pos.buy_date;
                종목코드   := r_pos.code;
                종목명     := r_pos.name;
                섹터       := r_pos.sector;
                등급       := r_pos.grade;
                매수가     := r_pos.buy_price;
                매수주수   := r_pos.shares;
                실투자금   := r_pos.actual_inv;
                매도일     := v_date;
                매도가     := r_sell.close_price;
                회수금     := (r_sell.close_price * r_pos.shares)::BIGINT;
                손익금액   := 회수금 - r_pos.actual_inv;
                수익률_pct := ROUND(손익금액::NUMERIC / r_pos.actual_inv * 100, 2);
                보유일수   := v_date - r_pos.buy_date;
                매도사유   := 'MA10 -3% 이탈';
                RETURN NEXT;
                DELETE FROM _bt_positions WHERE code = r_pos.code;
            END IF;

        END LOOP;

        -- ── 신규 매수 신호 탐색 (p_end_date 이내에서만 매수)
        FOR r_buy IN
            SELECT f.종목코드, f.종목명, f.섹터, f.등급, f.종가
            FROM   get_stock_filter_results_total(TO_CHAR(v_date, 'YYYYMMDD')) f
            WHERE  f.등급 IN ('★★★ 최우선', '★★  우선')
              AND  NOT EXISTS (
                      SELECT 1 FROM _bt_positions p
                      WHERE  p.code = f.종목코드
                  )
        LOOP
            v_shares     := FLOOR(p_invest_per::NUMERIC / NULLIF(r_buy.종가, 0));
            v_actual_inv := (v_shares * r_buy.종가)::BIGINT;
            CONTINUE WHEN v_shares <= 0;

            INSERT INTO _bt_positions
                (buy_date, code, name, sector, grade, buy_price, shares, actual_inv)
            VALUES
                (v_date, r_buy.종목코드, r_buy.종목명,
                 r_buy.섹터, r_buy.등급, r_buy.종가,
                 v_shares, v_actual_inv);
        END LOOP;

    END LOOP;  -- PHASE 1 END

    -- ================================================================
    -- ★ PHASE 2. p_end_date 이후 ~ DB 마지막 날까지
    --            매수는 없고 보유 종목 매도 조건만 계속 체크
    -- ================================================================
    FOR v_date IN
        SELECT DISTINCT trade_date
        FROM   stockmain
        WHERE  trade_date > v_end           -- p_end_date 다음날부터
          AND  trade_date <= v_db_last      -- DB 마지막 날까지
        ORDER  BY trade_date
    LOOP

        -- 보유 종목이 없으면 루프 조기 종료
        EXIT WHEN NOT EXISTS (SELECT 1 FROM _bt_positions);

        FOR r_pos IN SELECT * FROM _bt_positions LOOP

            SELECT sm.close_price
            INTO   r_sell
            FROM   stockmain sm
            JOIN   stock_ma  ma ON sm.trade_date = ma.trade_date
                               AND sm.code       = ma.code
            WHERE  sm.trade_date = v_date
              AND  sm.code       = r_pos.code
              AND  sm.close_price < ma.ma10 * 0.97;

            IF FOUND THEN
                매수일     := r_pos.buy_date;
                종목코드   := r_pos.code;
                종목명     := r_pos.name;
                섹터       := r_pos.sector;
                등급       := r_pos.grade;
                매수가     := r_pos.buy_price;
                매수주수   := r_pos.shares;
                실투자금   := r_pos.actual_inv;
                매도일     := v_date;
                매도가     := r_sell.close_price;
                회수금     := (r_sell.close_price * r_pos.shares)::BIGINT;
                손익금액   := 회수금 - r_pos.actual_inv;
                수익률_pct := ROUND(손익금액::NUMERIC / r_pos.actual_inv * 100, 2);
                보유일수   := v_date - r_pos.buy_date;
                매도사유   := 'MA10 -3% 이탈';   -- ★ 매수 기간 이후라도 정상 매도
                RETURN NEXT;
                DELETE FROM _bt_positions WHERE code = r_pos.code;
            END IF;

        END LOOP;

    END LOOP;  -- PHASE 2 END

    -- ================================================================
    -- ★ PHASE 3. DB 마지막 날까지 매도 조건 못 만난 종목만 강제청산
    --            → DB 마지막 거래일(v_db_last) 종가로 청산
    -- ================================================================
    FOR r_pos IN SELECT * FROM _bt_positions LOOP

        SELECT sm.close_price
        INTO   r_sell
        FROM   stockmain sm
        WHERE  sm.trade_date = v_db_last
          AND  sm.code       = r_pos.code;

        매수일     := r_pos.buy_date;
        종목코드   := r_pos.code;
        종목명     := r_pos.name;
        섹터       := r_pos.sector;
        등급       := r_pos.grade;
        매수가     := r_pos.buy_price;
        매수주수   := r_pos.shares;
        실투자금   := r_pos.actual_inv;
        매도일     := v_db_last;
        매도가     := COALESCE(r_sell.close_price, r_pos.buy_price);
        회수금     := (COALESCE(r_sell.close_price, r_pos.buy_price)
                       * r_pos.shares)::BIGINT;
        손익금액   := 회수금 - r_pos.actual_inv;
        수익률_pct := ROUND(손익금액::NUMERIC / r_pos.actual_inv * 100, 2);
        보유일수   := v_db_last - r_pos.buy_date;
        매도사유   := '강제청산(DB마지막날)';   -- ★ 사유 명확히 구분
        RETURN NEXT;

    END LOOP;

    DROP TABLE IF EXISTS _bt_positions;
    RETURN;

EXCEPTION WHEN OTHERS THEN
    DROP TABLE IF EXISTS _bt_positions;
    RAISE;
END;
$_$;


ALTER FUNCTION public.run_backtest(p_start_date text, p_end_date text, p_invest_per bigint) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: mytrade; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mytrade (
    code character varying(20) NOT NULL,
    trade_div character varying(10),
    trade_status smallint,
    trade_expected_cagr numeric(10,2),
    trade_dividend numeric(10,2),
    trade_per numeric(10,2),
    trade_eps_ratio numeric(10,2),
    trade_roe numeric(10,2),
    trade_min_roe_ever numeric(10,2),
    trade_pbr numeric(10,2),
    trade_hist_avg_pbr numeric(10,2),
    trade_close_price integer,
    trade_name character varying(100),
    remark character varying(100),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.mytrade OWNER TO postgres;

--
-- Name: COLUMN mytrade.trade_div; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.mytrade.trade_div IS 'bond1:bpb_1, bond2:bpr_avg';


--
-- Name: COLUMN mytrade.trade_status; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.mytrade.trade_status IS '0:예정, 1:매수, 2:매도';


--
-- Name: mytradeus; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mytradeus (
    code character varying(20) NOT NULL,
    trade_status smallint,
    trade_dividend numeric(10,2),
    trade_per numeric(10,2),
    trade_roe numeric(10,2),
    trade_pbr numeric(10,2),
    trade_close_price numeric(15,2),
    trade_name character varying(100),
    remark character varying(100),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.mytradeus OWNER TO postgres;

--
-- Name: stock_debt; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stock_debt (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    name character varying(100),
    net_debt numeric(15,2),
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.stock_debt OWNER TO postgres;

--
-- Name: stock_debtus; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stock_debtus (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    name character varying(100),
    net_debt numeric(20,2),
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.stock_debtus OWNER TO postgres;

--
-- Name: TABLE stock_debtus; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.stock_debtus IS '미국 주식 순부채(Net Debt) 데이터';


--
-- Name: COLUMN stock_debtus.code; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stock_debtus.code IS '티커 (예: AAPL)';


--
-- Name: COLUMN stock_debtus.name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stock_debtus.name IS '종목명';


--
-- Name: COLUMN stock_debtus.net_debt; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stock_debtus.net_debt IS '순부채 ($) (총부채 - 총현금)';


--
-- Name: COLUMN stock_debtus.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stock_debtus.created_at IS '데이터 생성 시각';


--
-- Name: stock_ma; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stock_ma (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    ma5 numeric(15,2),
    ma10 numeric(15,2),
    ma20 numeric(15,2),
    ma30 numeric(15,2),
    ma40 numeric(15,2),
    ma50 numeric(15,2),
    ma60 numeric(15,2),
    ma70 numeric(15,2),
    ma80 numeric(15,2),
    ma90 numeric(15,2),
    ma100 numeric(15,2),
    ma110 numeric(15,2),
    ma120 numeric(15,2),
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.stock_ma OWNER TO postgres;

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
    stockfdt.name,
    stockfdt.change_rate,
    stockfdt.dividend_yield,
    stockfdt.pbr,
    stockfdt.per,
    (((stockfdt.pbr / stockfdt.per) * (100)::numeric))::numeric(10,2) AS roe,
    stockfdt.forward_per,
    (((stockfdt.pbr / stockfdt.forward_per) * (100)::numeric))::numeric(10,2) AS forward_roe,
    stockfdt.close_price,
    (((stockfdt.close_price)::numeric / stockfdt.pbr))::integer AS bps,
    (((stockfdt.close_price)::numeric / stockfdt.per))::integer AS eps
   FROM public.stockfdt;


ALTER TABLE public.stockfdt_pbr_v OWNER TO postgres;

--
-- Name: stockfdtus; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stockfdtus (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    name character varying(100),
    close_price numeric(15,2),
    change_price numeric(15,2),
    change_rate numeric(10,2),
    eps numeric(15,2),
    per numeric(10,2),
    forward_eps numeric(15,2),
    forward_per numeric(10,2),
    bps numeric(15,2),
    pbr numeric(10,2),
    dividend_per_share numeric(10,4),
    dividend_yield numeric(10,2),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.stockfdtus OWNER TO postgres;

--
-- Name: TABLE stockfdtus; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.stockfdtus IS '미국 주식 일별 재무/가치 지표';


--
-- Name: COLUMN stockfdtus.trade_date; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.trade_date IS '거래일자';


--
-- Name: COLUMN stockfdtus.code; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.code IS '티커 (예: AAPL)';


--
-- Name: COLUMN stockfdtus.name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.name IS '종목명';


--
-- Name: COLUMN stockfdtus.close_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.close_price IS '종가 ($)';


--
-- Name: COLUMN stockfdtus.change_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.change_price IS '전일 대비 가격 변동 ($)';


--
-- Name: COLUMN stockfdtus.change_rate; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.change_rate IS '등락률 (%)';


--
-- Name: COLUMN stockfdtus.eps; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.eps IS 'EPS ($)';


--
-- Name: COLUMN stockfdtus.per; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.per IS 'PER';


--
-- Name: COLUMN stockfdtus.forward_eps; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.forward_eps IS '선행 EPS ($)';


--
-- Name: COLUMN stockfdtus.forward_per; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.forward_per IS '선행 PER';


--
-- Name: COLUMN stockfdtus.bps; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.bps IS 'BPS ($)';


--
-- Name: COLUMN stockfdtus.pbr; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.pbr IS 'PBR';


--
-- Name: COLUMN stockfdtus.dividend_per_share; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.dividend_per_share IS '주당배당금 ($)';


--
-- Name: COLUMN stockfdtus.dividend_yield; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.dividend_yield IS '배당수익률 (%)';


--
-- Name: COLUMN stockfdtus.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockfdtus.created_at IS '데이터 최초 생성 시각';


--
-- Name: stockfdtus_pbr_v; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.stockfdtus_pbr_v AS
 SELECT stockfdtus.trade_date,
    stockfdtus.code,
    stockfdtus.name,
    stockfdtus.change_rate,
    stockfdtus.dividend_yield,
    stockfdtus.pbr,
    stockfdtus.per,
    (((stockfdtus.pbr / NULLIF(stockfdtus.per, (0)::numeric)) * (100)::numeric))::numeric(10,2) AS roe,
    stockfdtus.forward_per,
    (((stockfdtus.pbr / NULLIF(stockfdtus.forward_per, (0)::numeric)) * (100)::numeric))::numeric(10,2) AS forward_roe,
    stockfdtus.close_price,
    (((stockfdtus.close_price)::numeric / NULLIF(stockfdtus.pbr, (0)::numeric)))::numeric(15,2) AS bps,
    (((stockfdtus.close_price)::numeric / NULLIF(stockfdtus.per, (0)::numeric)))::numeric(15,2) AS eps
   FROM public.stockfdtus;


ALTER TABLE public.stockfdtus_pbr_v OWNER TO postgres;

--
-- Name: stockmain; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stockmain (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    name character varying(100),
    close_price numeric(15,2),
    change_price numeric(15,2),
    change_rate numeric(15,4),
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
-- Name: stockmainus; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.stockmainus (
    trade_date date NOT NULL,
    code character varying(20) NOT NULL,
    name character varying(100),
    close_price numeric(15,2),
    change_price numeric(15,2),
    change_rate numeric(15,4),
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


ALTER TABLE public.stockmainus OWNER TO postgres;

--
-- Name: TABLE stockmainus; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE public.stockmainus IS '미국 주식 일별 시세 데이터';


--
-- Name: COLUMN stockmainus.trade_date; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.trade_date IS '거래일 (YYYY-MM-DD 형식)';


--
-- Name: COLUMN stockmainus.code; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.code IS '티커 (미국 종목코드, 예: AAPL)';


--
-- Name: COLUMN stockmainus.name; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.name IS '종목명';


--
-- Name: COLUMN stockmainus.close_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.close_price IS '종가 ($)';


--
-- Name: COLUMN stockmainus.change_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.change_price IS '전일 대비 가격 변화 ($)';


--
-- Name: COLUMN stockmainus.change_rate; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.change_rate IS '전일 대비 등락률 (%)';


--
-- Name: COLUMN stockmainus.open_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.open_price IS '시가 ($)';


--
-- Name: COLUMN stockmainus.high_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.high_price IS '고가 ($)';


--
-- Name: COLUMN stockmainus.low_price; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.low_price IS '저가 ($)';


--
-- Name: COLUMN stockmainus.volume; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.volume IS '거래량';


--
-- Name: COLUMN stockmainus.trade_value; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.trade_value IS '거래대금 ($)';


--
-- Name: COLUMN stockmainus.market_cap; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.market_cap IS '시가총액 ($)';


--
-- Name: COLUMN stockmainus.shares_out; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.shares_out IS '상장주식수';


--
-- Name: COLUMN stockmainus.sector; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.sector IS '섹터 (예: Technology)';


--
-- Name: COLUMN stockmainus.created_at; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN public.stockmainus.created_at IS '데이터 생성 시각';


--
-- Name: mytrade mytrade_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mytrade
    ADD CONSTRAINT mytrade_pkey PRIMARY KEY (code, created_at);


--
-- Name: mytradeus mytradeus_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mytradeus
    ADD CONSTRAINT mytradeus_pkey PRIMARY KEY (code, created_at);


--
-- Name: stock_debt stock_debt_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stock_debt
    ADD CONSTRAINT stock_debt_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stock_debtus stock_debtus_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stock_debtus
    ADD CONSTRAINT stock_debtus_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stock_ma stock_ma_new_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stock_ma
    ADD CONSTRAINT stock_ma_new_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stockfdt stockfdt_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stockfdt
    ADD CONSTRAINT stockfdt_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stockfdtus stockfdtus_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stockfdtus
    ADD CONSTRAINT stockfdtus_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stockmain stockmain_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stockmain
    ADD CONSTRAINT stockmain_pkey PRIMARY KEY (trade_date, code);


--
-- Name: stockmainus stockmainus_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.stockmainus
    ADD CONSTRAINT stockmainus_pkey PRIMARY KEY (trade_date, code);


--
-- PostgreSQL database dump complete
--

\unrestrict KSc0Ewf66wvmfigrm6gavkfurVvO3Ac9q3diT6JYVbhRaqyM2cnogwqDCb8niLK
