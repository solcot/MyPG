-- =====================================================================
-- 함수 생성: get_stock_filter_results_total()
-- 호출: SELECT * FROM get_stock_filter_results_total();
-- =====================================================================

CREATE OR REPLACE FUNCTION get_stock_filter_results_total()
RETURNS TABLE (
    종목코드        VARCHAR(20),
    종목명          VARCHAR(100),
    섹터            VARCHAR(50),
    종가            NUMERIC(15,2),
    "등락률(%)"     NUMERIC(15,4),
    거래량          BIGINT,
    "거래량배수(5일평균比)" NUMERIC(10,2),
    거래대금        TEXT,
    시가총액        TEXT,
    ma5             NUMERIC(15,2),
    ma20            NUMERIC(15,2),
    ma60            NUMERIC(15,2),
    ma120           NUMERIC(15,2),
    "PER"           NUMERIC(10,2),
    "선행PER"       NUMERIC(10,2),
    "PBR"           NUMERIC(10,2),
    "ROE(%)"        NUMERIC(10,2),
    "배당수익률(%)" NUMERIC(10,2),
    종합점수        INTEGER,
    등급            TEXT
)
LANGUAGE sql
STABLE
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
        AND m.market_cap  <= 5000000000000     -- 시총 5조 이하
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
    sector::VARCHAR(50)                                        AS 섹터,
    종가,
    등락률,
    거래량,
    거래량배수,
    TO_CHAR(거래대금_억, 'FM999,999,999') || ' 억'            AS 거래대금,
    TO_CHAR(시가총액_억, 'FM999,999,999') || ' 억'            AS 시가총액,
    ma5,
    ma20,
    ma60,
    ma120,
    per,
    forward_per,
    pbr,
    roe,
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

