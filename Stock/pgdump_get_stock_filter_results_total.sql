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




-- =====================================================================
-- 함수 생성: get_stock_filter_results_total(p_date TEXT)
-- 호출: SELECT * FROM get_stock_filter_results_total('20260304');
--       SELECT * FROM get_stock_filter_results_total('2026-03-04');
-- =====================================================================

CREATE OR REPLACE FUNCTION get_stock_filter_results_total(p_date TEXT)
RETURNS TABLE (
    종목코드              VARCHAR(20),
    종목명                VARCHAR(100),
    섹터                  VARCHAR(50),
    종가                  NUMERIC(15,2),
    "등락률(%)"           NUMERIC(15,4),
    거래량                BIGINT,
    "거래량배수(5일평균比)" NUMERIC(10,2),
    거래대금              TEXT,
    시가총액              TEXT,
    ma5                   NUMERIC(15,2),
    ma20                  NUMERIC(15,2),
    ma60                  NUMERIC(15,2),
    ma120                 NUMERIC(15,2),
    "PER"                 NUMERIC(10,2),
    "선행PER"             NUMERIC(10,2),
    "PBR"                 NUMERIC(10,2),
    "ROE(%)"              NUMERIC(10,2),
    "배당수익률(%)"       NUMERIC(10,2),
    종합점수              INTEGER,
    등급                  TEXT,
    기준일                DATE           -- ★ 어떤 날짜 기준인지 확인용
)
LANGUAGE plpgsql
STABLE
AS $$
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
            AND m.market_cap  <= 5000000000000  -- 시총 5조 이하
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
        s.sector::VARCHAR(50),
        s.종가,
        s.등락률,
        s.거래량,
        s.거래량배수,
        TO_CHAR(s.거래대금_억, 'FM999,999,999') || ' 억',
        TO_CHAR(s.시가총액_억, 'FM999,999,999') || ' 억',
        s.ma5,
        s.ma20,
        s.ma60,
        s.ma120,
        s.per,
        s.forward_per,
        s.pbr,
        s.roe,
        s.dividend_yield,
        s.total_score,
        CASE
            WHEN s.total_score >= 14 THEN '★★★ 최우선'
            WHEN s.total_score >= 11 THEN '★★  우선'
            WHEN s.total_score >= 8  THEN '★   관심'
            ELSE                          '△  대기'
        END,
        v_actual_date                            -- 실제 적용된 거래일 반환

    FROM scoring s
    WHERE s.total_score >= 8
    ORDER BY
        s.total_score DESC,
        s.거래량배수  DESC,
        s.roe         DESC
    LIMIT 30;

END;
$$;


