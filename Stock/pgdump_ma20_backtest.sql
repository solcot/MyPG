WITH ma_base AS (
    -- 1. 매수 및 매도 계산을 위한 이평선 데이터 추출 (12월~2월 말까지 넉넉히)
    SELECT trade_date, code,
        LAG(ma20, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1,
        LAG(ma10, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1_10,
        ma5, ma10, ma20, ma40, ma60, ma120
    FROM stock_ma
    WHERE trade_date >= '2021-01-15' AND trade_date <= '2026-02-28'
),
buy_signals AS (
    -- 2. 2026년 1월 중 [ma20 로직] 매수 신호가 발생한 종목 및 날짜 찾기
    SELECT 
        sm.trade_date AS buy_date, 
        sm.code, 
        sm.name, 
        sm.close_price AS buy_price
    FROM stockmain sm
    JOIN ma_base mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
    JOIN stockfdt_pbr_v sfv ON sm.trade_date = sfv.trade_date AND sm.code = sfv.code
    WHERE sm.trade_date >= '2021-02-01' AND sm.trade_date <= '2025-11-30'

-- 1. 골든크로스 조건 (어제는 10일선이 20일선 아래)
AND mc.prev1_10 < mc.prev1

-- 2. 핵심 정배열 유지
AND sm.close_price > mc.ma5
AND mc.ma5 > mc.ma10
AND mc.ma10 > mc.ma20

-- 3. 억제기 (아직 40일선은 위에 있음 - 30일선 제외)
AND mc.ma40 > mc.ma20

-- 4. 수급 및 가치 필터
AND sm.close_price < 1000000
AND sm.market_cap > 500000000000 --500000000000
AND sm.change_rate < 15.0
AND sm.trade_value > 5000000000 --5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 25.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15   
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)  
AND sm.change_rate > 0.0

),
sell_candidates AS (
    -- 3. 매수한 날짜 '이후'로 매도 조건(< 10일선 1% 이탈)을 만족하는 날짜 모두 찾기
    SELECT 
        b.code, b.buy_date, b.buy_price,
        sm.trade_date AS sell_date, 
        sm.close_price AS sell_price,
        -- 매수일 이후 첫 번째로 걸리는 날짜를 찾기 위해 순번(ROW_NUMBER) 매기기
        ROW_NUMBER() OVER (PARTITION BY b.code, b.buy_date ORDER BY sm.trade_date) as rn
    FROM buy_signals b
    JOIN stockmain sm ON sm.code = b.code AND sm.trade_date > b.buy_date
    JOIN ma_base mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
    WHERE sm.close_price < mc.ma10 * 0.97
    --WHERE mc.ma5 < mc.ma10  
      AND sm.trade_date <= '2026-02-28'
),
sell_signals AS (
    -- 4. 가장 처음 만난 매도 날짜 딱 하루만 확정
    SELECT code, buy_date, sell_date, sell_price
    FROM sell_candidates
    WHERE rn = 1
),
last_prices AS (
    -- 5. 아직 매도되지 않은 종목을 평가하기 위한 2월 마지막 거래일 종가
    SELECT code, close_price 
    FROM stockmain 
    WHERE trade_date = (SELECT MAX(trade_date) FROM stockmain WHERE trade_date <= '2026-02-28')
)

-- 6. 최종 결과 및 100만 원 투자 시 손익 계산
SELECT 
    b.buy_date AS "매수 신호일",
    b.code AS "종목코드",
    b.name AS "종목명",
    b.buy_price AS "매수단가",
    COALESCE(s.sell_date::text, '보유중(평가)') AS "매도 신호일",
    COALESCE(s.sell_price, lp.close_price) AS "매도/평가 단가",
    
    -- 수익률(%) = (매도가 - 매수가) / 매수가 * 100
    ROUND((COALESCE(s.sell_price, lp.close_price) - b.buy_price) / b.buy_price * 100, 2) AS "수익률(%)",
    
    -- 100만 원 투자 시 손익(원) = 100만원 * 수익률
    ROUND(1000000 * (COALESCE(s.sell_price, lp.close_price) - b.buy_price) / b.buy_price, 0) AS "예상 손익(원)"
FROM buy_signals b
LEFT JOIN sell_signals s ON b.code = s.code AND b.buy_date = s.buy_date
LEFT JOIN last_prices lp ON b.code = lp.code
ORDER BY b.buy_date, b.code;

---------------------------------------------------------------------------------------------------------------------------------------------

WITH ma_base AS (
    -- 1. 매수 및 매도 계산을 위한 이평선 데이터 추출 (12월~2월 말까지 넉넉히)
    SELECT trade_date, code,
        LAG(ma20, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1,
        LAG(ma10, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1_10,
        ma5, ma10, ma20, ma40, ma60, ma120
    FROM stock_ma
    WHERE trade_date >= '2021-01-15' AND trade_date <= '2026-02-28'
),
buy_signals AS (
    -- 2. 2026년 1월 중 [ma20 로직] 매수 신호가 발생한 종목 및 날짜 찾기
    SELECT 
        sm.trade_date AS buy_date, 
        sm.code, 
        sm.name, 
        sm.close_price AS buy_price
    FROM stockmain sm
    JOIN ma_base mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
    JOIN stockfdt_pbr_v sfv ON sm.trade_date = sfv.trade_date AND sm.code = sfv.code
    WHERE sm.trade_date >= '2021-02-01' AND sm.trade_date <= '2025-11-30'

-- 1. 골든크로스 조건 (어제는 10일선이 20일선 아래)
AND mc.prev1_10 < mc.prev1

-- 2. 핵심 정배열 유지
AND sm.close_price > mc.ma5
AND mc.ma5 > mc.ma10
AND mc.ma10 > mc.ma20

-- 3. 억제기 (아직 40일선은 위에 있음 - 30일선 제외)
AND mc.ma40 > mc.ma20

-- 4. 수급 및 가치 필터
AND sm.close_price < 1000000
AND sm.market_cap > 500000000000 --500000000000
AND sm.change_rate < 15.0
AND sm.trade_value > 5000000000 --5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 25.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15   
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)  
AND sm.change_rate > 0.0

),
sell_candidates AS (
    -- 3. 매수한 날짜 '이후'로 매도 조건(< 10일선 1% 이탈)을 만족하는 날짜 모두 찾기
    SELECT 
        b.code, b.buy_date, b.buy_price,
        sm.trade_date AS sell_date, 
        sm.close_price AS sell_price,
        -- 매수일 이후 첫 번째로 걸리는 날짜를 찾기 위해 순번(ROW_NUMBER) 매기기
        ROW_NUMBER() OVER (PARTITION BY b.code, b.buy_date ORDER BY sm.trade_date) as rn
    FROM buy_signals b
    JOIN stockmain sm ON sm.code = b.code AND sm.trade_date > b.buy_date
    JOIN ma_base mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
    WHERE sm.close_price < mc.ma10 * 0.97
    --WHERE mc.ma5 < mc.ma10  
      AND sm.trade_date <= '2026-02-28'
),
sell_signals AS (
    -- 4. 가장 처음 만난 매도 날짜 딱 하루만 확정
    SELECT code, buy_date, sell_date, sell_price
    FROM sell_candidates
    WHERE rn = 1
),
last_prices AS (
    -- 5. 아직 매도되지 않은 종목을 평가하기 위한 2월 마지막 거래일 종가
    SELECT code, close_price 
    FROM stockmain 
    WHERE trade_date = (SELECT MAX(trade_date) FROM stockmain WHERE trade_date <= '2026-02-28')
)

-- 6. 최종 합산 요약 계산
SELECT 
    COUNT(*) AS "총 매매 횟수",
    SUM(CASE WHEN COALESCE(s.sell_price, lp.close_price) > b.buy_price THEN 1 ELSE 0 END) AS "익절 횟수",
    SUM(CASE WHEN COALESCE(s.sell_price, lp.close_price) <= b.buy_price THEN 1 ELSE 0 END) AS "손절 횟수",
    ROUND(AVG((COALESCE(s.sell_price, lp.close_price) - b.buy_price) / b.buy_price * 100), 2) AS "평균 수익률(%)",
    SUM(ROUND(1000000 * (COALESCE(s.sell_price, lp.close_price) - b.buy_price) / b.buy_price, 0)) AS "총 합산 손익금(원)"
FROM buy_signals b
LEFT JOIN sell_signals s ON b.code = s.code AND b.buy_date = s.buy_date
LEFT JOIN last_prices lp ON b.code = lp.code;


