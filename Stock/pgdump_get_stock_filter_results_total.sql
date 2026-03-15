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



CREATE OR REPLACE FUNCTION run_backtest(
    p_start_date   TEXT    DEFAULT '20260101',
    p_end_date     TEXT    DEFAULT '20260315',
    p_invest_per   BIGINT  DEFAULT 1000000
)
RETURNS TABLE (
    매수일          DATE,
    종목코드        VARCHAR(20),
    종목명          VARCHAR(100),
    섹터            VARCHAR(50),
    등급            TEXT,
    매수가          NUMERIC(15,2),
    매수주수        INTEGER,
    실투자금        BIGINT,
    매도일          DATE,
    매도가          NUMERIC(15,2),
    회수금          BIGINT,
    손익금액        BIGINT,
    수익률_pct      NUMERIC(10,2),
    보유일수        INTEGER,
    매도사유        TEXT
)
LANGUAGE plpgsql
AS $$
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
$$;







-- =====================================================================
-- 공통 CTE: 백테스트 결과를 한 번만 실행해 재사용
-- =====================================================================
WITH bt AS (
    SELECT * FROM run_backtest('20260101', '20260315', 1000000)
)


-- =====================================================================
-- 1. 전체 거래 내역 (금액 포함)
-- =====================================================================
SELECT
    매수일,
    종목코드,
    종목명,
    등급,
    매수가,
    매수주수,
    TO_CHAR(실투자금, 'FM999,999,999') || ' 원'     AS 실투자금,
    매도일,
    매도가,
    TO_CHAR(회수금,   'FM999,999,999') || ' 원'     AS 회수금,
    TO_CHAR(손익금액, 'FM999,999,999') || ' 원'     AS 손익금액,
    수익률_pct                                      AS "수익률(%)",
    보유일수,
    매도사유
FROM bt
ORDER BY 매수일, 수익률_pct DESC;


-- =====================================================================
-- 2. 종합 성과 요약 (금액 포함)
-- =====================================================================
SELECT
    COUNT(*)                                                    AS 총거래수,
    COUNT(*) FILTER (WHERE 수익률_pct > 0)                      AS 수익거래수,
    COUNT(*) FILTER (WHERE 수익률_pct <= 0)                     AS 손실거래수,
    ROUND(
        COUNT(*) FILTER (WHERE 수익률_pct > 0)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                           AS "승률(%)",

    -- 투자금 관련
    TO_CHAR(SUM(실투자금), 'FM999,999,999,999') || ' 원'        AS 총투자금_합산,
    TO_CHAR(SUM(손익금액), 'FM999,999,999,999') || ' 원'        AS 총손익금액,
    TO_CHAR(SUM(손익금액) FILTER (WHERE 손익금액 > 0),
            'FM999,999,999,999') || ' 원'                       AS 총수익금액,
    TO_CHAR(SUM(손익금액) FILTER (WHERE 손익금액 <= 0),
            'FM999,999,999,999') || ' 원'                       AS 총손실금액,

    -- 수익률 관련
    ROUND(AVG(수익률_pct), 2)                                    AS "평균수익률(%)",
    ROUND(MAX(수익률_pct), 2)                                    AS "최대수익(%)",
    ROUND(MIN(수익률_pct), 2)                                    AS "최대손실(%)",
    ROUND(
        AVG(수익률_pct) FILTER (WHERE 수익률_pct > 0), 2
    )                                                           AS "평균수익(수익거래,%)",
    ROUND(
        AVG(수익률_pct) FILTER (WHERE 수익률_pct <= 0), 2
    )                                                           AS "평균손실(손실거래,%)",
    ROUND(
        AVG(수익률_pct) FILTER (WHERE 수익률_pct > 0)
        / ABS(NULLIF(AVG(수익률_pct) FILTER (WHERE 수익률_pct <= 0), 0)),
    2)                                                          AS "손익비",
    ROUND(AVG(보유일수), 1)                                      AS "평균보유일수"
FROM bt;


-- =====================================================================
-- 3. 등급별 성과 비교 (금액 포함)
-- =====================================================================
SELECT
    등급,
    COUNT(*)                                                    AS 거래수,
    ROUND(
        COUNT(*) FILTER (WHERE 수익률_pct > 0)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                           AS "승률(%)",
    TO_CHAR(SUM(손익금액), 'FM999,999,999,999') || ' 원'        AS 총손익금액,
    TO_CHAR(AVG(손익금액)::BIGINT, 'FM999,999,999') || ' 원'    AS 거래당평균손익,
    ROUND(AVG(수익률_pct), 2)                                    AS "평균수익률(%)",
    ROUND(MAX(수익률_pct), 2)                                    AS "최대수익(%)",
    ROUND(MIN(수익률_pct), 2)                                    AS "최대손실(%)"
FROM bt
GROUP BY 등급
ORDER BY 등급;


-- =====================================================================
-- 4. 섹터별 성과 비교 (금액 포함)
-- =====================================================================
SELECT
    섹터,
    COUNT(*)                                                    AS 거래수,
    ROUND(AVG(수익률_pct), 2)                                    AS "평균수익률(%)",
    TO_CHAR(SUM(손익금액), 'FM999,999,999,999') || ' 원'        AS 섹터총손익,
    TO_CHAR(AVG(손익금액)::BIGINT, 'FM999,999,999') || ' 원'    AS 거래당평균손익,
    ROUND(
        COUNT(*) FILTER (WHERE 수익률_pct > 0)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                           AS "승률(%)"
FROM bt
GROUP BY 섹터
HAVING COUNT(*) >= 2
ORDER BY SUM(손익금액) DESC;


-- =====================================================================
-- 5. 매도 사유별 통계 (금액 포함)
-- =====================================================================
SELECT
    매도사유,
    COUNT(*)                                                    AS 거래수,
    TO_CHAR(SUM(손익금액), 'FM999,999,999,999') || ' 원'        AS 총손익금액,
    TO_CHAR(AVG(손익금액)::BIGINT, 'FM999,999,999') || ' 원'    AS 거래당평균손익,
    ROUND(AVG(수익률_pct), 2)                                    AS "평균수익률(%)",
    ROUND(AVG(보유일수), 1)                                      AS "평균보유일수"
FROM bt
GROUP BY 매도사유;


-- =====================================================================
-- 6. 월별 수익률 및 손익 금액 추이
-- =====================================================================
SELECT
    TO_CHAR(매도일, 'YYYY-MM')                                  AS 월,
    COUNT(*)                                                    AS 거래수,
    ROUND(AVG(수익률_pct), 2)                                    AS "평균수익률(%)",
    TO_CHAR(SUM(손익금액), 'FM999,999,999,999') || ' 원'        AS 월별총손익,
    TO_CHAR(SUM(손익금액) FILTER (WHERE 손익금액 > 0),
            'FM999,999,999,999') || ' 원'                       AS 월별수익합,
    TO_CHAR(SUM(손익금액) FILTER (WHERE 손익금액 <= 0),
            'FM999,999,999,999') || ' 원'                       AS 월별손실합
FROM bt
GROUP BY TO_CHAR(매도일, 'YYYY-MM')
ORDER BY 월;


-- =====================================================================
-- 7. 종목별 누적 손익 랭킹 (동일 종목 여러 번 거래 시 합산)
-- =====================================================================
SELECT
    종목코드,
    종목명,
    섹터,
    COUNT(*)                                                    AS 거래횟수,
    TO_CHAR(SUM(손익금액), 'FM999,999,999,999') || ' 원'        AS 누적손익,
    ROUND(AVG(수익률_pct), 2)                                    AS "평균수익률(%)",
    ROUND(AVG(보유일수), 1)                                      AS "평균보유일수"
FROM bt
GROUP BY 종목코드, 종목명, 섹터
ORDER BY SUM(손익금액) DESC
LIMIT 20;


