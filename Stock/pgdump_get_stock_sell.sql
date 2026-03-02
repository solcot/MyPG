CREATE OR REPLACE FUNCTION public.get_stock_sell(p_trade_date date DEFAULT '2025-07-01'::date)
 RETURNS TABLE(code character varying, name character varying)
 LANGUAGE sql
AS $function$
SELECT
    sm.code,
    sm.name
FROM stockmain sm
JOIN stock_ma mc ON sm.trade_date = mc.trade_date AND sm.code = mc.code
WHERE sm.trade_date = p_trade_date
  -- 매도 핵심 로직: 오늘 종가가 10일선 대비 확실하게 3% 이상 뚫고 내려갔을 때 (단기 지지선 붕괴 확정)
  AND sm.close_price < mc.ma10 * 0.97
$function$;


