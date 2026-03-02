-- ======================================================================
-- [1단계: 정찰병] 5일선이 10일선을 돌파 (다음 저항: 20일선)
-- ======================================================================
CREATE OR REPLACE FUNCTION public.get_stock_ma10(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000)
 RETURNS TABLE(code character varying, name character varying)
 LANGUAGE sql
AS $function$
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
AND sm.change_rate < 5.0   -- 15.0
AND sm.trade_value > 5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 25.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15   
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)  
AND sm.change_rate > 0.0
$function$;


-- ======================================================================
-- [2단계: 메인 승부] 10일선이 20일선을 돌파 (다음 저항: 40일선)
-- ======================================================================
CREATE OR REPLACE FUNCTION public.get_stock_ma20(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000)
 RETURNS TABLE(code character varying, name character varying)
 LANGUAGE sql
AS $function$
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
AND sm.change_rate < 5.0   -- 15.0
AND sm.trade_value > 5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 25.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15   
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)  
AND sm.change_rate > 0.0
$function$;


-- ======================================================================
-- [3단계: 1차 불타기] 20일선이 40일선을 돌파 (다음 저항: 60일선)
-- ======================================================================
CREATE OR REPLACE FUNCTION public.get_stock_ma40(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000)
 RETURNS TABLE(code character varying, name character varying)
 LANGUAGE sql
AS $function$
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
AND sm.change_rate < 5.0   -- 15.0
AND sm.trade_value > 5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 25.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15   
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)  
AND sm.change_rate > 0.0
$function$;


-- ======================================================================
-- [4단계: 마지막 탑승] 40일선이 60일선을 돌파 (장기 저항: 120일선)
-- ======================================================================
CREATE OR REPLACE FUNCTION public.get_stock_ma60(p_trade_date date DEFAULT '2025-07-01'::date, p_max_price numeric DEFAULT 500000)
 RETURNS TABLE(code character varying, name character varying)
 LANGUAGE sql
AS $function$
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
AND sm.change_rate < 5.0   -- 15.0
AND sm.trade_value > 5000000000
AND ((sfv.pbr >= 0.0 AND sfv.pbr < 1.0) OR (sfv.pbr >= 0.0 AND sfv.pbr < 3.0 AND sfv.per >= 0.0 AND sfv.per < 25.0 AND sfv.roe > 5.0))

-- 5. 안전장치 3개
AND sm.close_price <= mc.ma20 * 1.15   
AND (sm.close_price >= mc.ma5 * 1.01 OR sm.change_rate >= 2.0)  
AND sm.change_rate > 0.0
$function$;
