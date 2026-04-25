"
select code,to_char(trade_date, 'YYYY') as year,avg(roe)::int roe,max(per) per,max(pbr) pbr,max(name)
from stockfdt_pbr_v
where code='316140'
and trade_date >= '20160101'
GROUP BY code, to_char(trade_date, 'YYYY')
order by year;

select code,trade_date,roe,per,pbr,name
from stockfdt_pbr_v
where code='316140' 
and trade_date >= '20160101'
order by trade_date;
"



"    
WITH basic_number AS (
select
28126 as bps,
16.0 as min_roe_ever,
10 as years,
1.3 as hist_avg_pbr,
39883 as close_price,
12.0 as target_cagr
)
select 
a.*,
':::::' as div,
-- 10년 후 예상 BPS (장부 가치)
ROUND((a.bps * POWER(1 + a.min_roe_ever / 100.0, a.years)) * 1) AS future_bps,
-- 10년 후 투자 승수 = (미래 예상 주가 / 현재 주가) [0 나누기 방어 추가]
ROUND(((a.bps * POWER(1 + a.min_roe_ever / 100.0, a.years)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
-- 최종 예상 연평균 복리 수익률 (CAGR)
ROUND(
    (POWER(
        ((a.bps * POWER(1 + a.min_roe_ever / 100.0, a.years)) * 1) / NULLIF(a.close_price, 0),  
        1.0 / a.years                                                  
    ) - 1) * 100, 
2) AS expected_cagr,
-- target_carg에 도달하기 위한 최저 목표 매수가
ROUND(
        ((a.bps * POWER(1 + a.min_roe_ever / 100.0, a.years)) * 1) 
        / POWER(1 + a.target_cagr / 100.0, a.years)
    ) AS target_buy_price_bond1,
    
-- 10년 후 예상 주가 (미래 BPS * 역대 평균 PBR)
ROUND((a.bps * POWER(1 + a.min_roe_ever / 100.0, a.years)) * a.hist_avg_pbr) AS future_expected_price,
-- 10년 후 투자 승수 = (미래 예상 주가 / 현재 주가) [0 나누기 방어 추가]
ROUND(((a.bps * POWER(1 + a.min_roe_ever / 100.0, a.years)) * a.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
-- 최종 예상 연평균 복리 수익률 (CAGR)
ROUND(
    (POWER(
        ((a.bps * POWER(1 + a.min_roe_ever / 100.0, a.years)) * a.hist_avg_pbr) / NULLIF(a.close_price, 0),  
        1.0 / a.years                                                  
    ) - 1) * 100, 
2) AS fep_expected_cagr,
-- target_carg에 도달하기 위한 최저 목표 매수가
ROUND(
        ((a.bps * POWER(1 + a.min_roe_ever / 100.0, a.years)) * a.hist_avg_pbr) 
        / POWER(1 + a.target_cagr / 100.0, a.years)
    ) AS target_buy_price_bond2
    
from basic_number a
;
"



#-- 1. 꾸준히 수익 창출하는 기업
#-- 2. 저평가 종목 
#-- 3. 소형주도 대상에 포함
#-- 4. 소형주라도 최소 거래량 충족해야 함
#-- 5. 성장성 저평가 종목
#-- 6. 순부채가 없는 종목 
#-- 7. 순자산이 계속적으로 증가하는 기업
#-- 8. 최소 배당 조건 만족 
cat > bond_1_2.sql <<'EEOFF'
WITH max_date_cte AS (
    -- 💡 [추가] 쿼리 수행일 기준 가장 최신(현재) 날짜를 한 번만 추출하여 성능 최적화
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    -- 💡 [추가] 1년 전 기준 앞뒤 3일(총 1주일) 동안의 평균 EPS 계산 (휴장일 및 데이터 누락 방어)
    SELECT 
        code,
        AVG(eps) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    -- 1단계: 종목(code)별, 3개월 단위 평균 ROE 및 평균 PBR 계산
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        avg(b.market_cap/a.pbr)::bigint AS avg_market_cap,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a join stockmain b on a.trade_date = b.trade_date and a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    -- 2단계: 최악의 ROE 찾기 및 NULL 빵꾸 이력 추적
    SELECT 
        code,
        year,
        avg_roe,
        avg_market_cap,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        BOOL_OR(avg_roe IS NULL) OVER (PARTITION BY code) AS has_null_roe,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    -- 3단계: 불량 종목(5% 미만 or NULL 이력) 싹 다 제거!
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        (avg_market_cap/10000000000)::bigint AS avg_market_cap_bakuk,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    WHERE min_roe_ever >= 5.0 and avg_roe_ever >= 8.0   -- 1. 꾸준히 수익 창출하는 기업
      --AND has_null_roe = false
),
pivot_data AS (
    -- 4단계: 2015-1Q부터 2026-1Q까지 45분기 절대 시간 피벗 전개!
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,

-- [ROE 피벗]
        MAX(avg_roe) FILTER (WHERE year = '2016') AS aaa_roe,
        MAX(avg_roe) FILTER (WHERE year = '2017') AS bbb_roe,
        MAX(avg_roe) FILTER (WHERE year = '2018') AS ccc_roe,
        MAX(avg_roe) FILTER (WHERE year = '2019') AS ddd_roe,
        MAX(avg_roe) FILTER (WHERE year = '2020') AS eee_roe,
        MAX(avg_roe) FILTER (WHERE year = '2021') AS fff_roe,
        MAX(avg_roe) FILTER (WHERE year = '2022') AS ggg_roe,
        MAX(avg_roe) FILTER (WHERE year = '2023') AS hhh_roe,
        MAX(avg_roe) FILTER (WHERE year = '2024') AS iii_roe,
        MAX(avg_roe) FILTER (WHERE year = '2025') AS jjj_roe,
        MAX(avg_roe) FILTER (WHERE year = '2026') AS kkk_roe,
        
        '***' AS ddiivv,
        
        -- [Market Cap 피벗]
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2016') AS aaa_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2017') AS bbb_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2018') AS ccc_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2019') AS ddd_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2020') AS eee_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2021') AS fff_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2022') AS ggg_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2023') AS hhh_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2024') AS iii_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2025') AS jjj_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2026') AS kkk_pcap_bakuk,
        
        '***' AS dddiiivvv,
        
        -- [dividend 피벗]
        MAX(avg_dividend) FILTER (WHERE year = '2016') AS aaa_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2017') AS bbb_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2018') AS ccc_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2019') AS ddd_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2020') AS eee_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2021') AS fff_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2022') AS ggg_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2023') AS hhh_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2024') AS iii_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2025') AS jjj_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2026') AS kkk_dividend
        
    FROM filtered_data
    GROUP BY code
),
last_data AS (
    SELECT 
        -- 💡 [추가] 1년 전 대비 EPS 증가율 계산 (%)
        -- (현재 EPS - 1년전 EPS) / |1년전 EPS| * 100
        -- NULLIF 방어코드로 분모가 0일 때의 에러(ZeroDivision) 방지
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,

        -- 💡 1. 현재 bps 기준 10년 후 예상 BPS (장부 가치)
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        
        -- 💡 3. 10년 후 투자 승수 = (미래 예상 주가 / 현재 주가) [0 나누기 방어 추가]
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        
        -- 💡 4. 최종 예상 연평균 복리 수익률 (CAGR)
        ROUND(
            (POWER(
                ((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0),  
                1.0 / 10.0                                                  
            ) - 1) * 100, 
        2) AS expected_cagr,

        -- 💡 2. 현재 bps 기준 10년 후 예상 주가 (미래 BPS * 역대 평균 PBR)
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        
        -- 💡 3. 10년 후 투자 승수 = (미래 예상 주가 / 현재 주가)
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        
        -- 💡 4. 최종 예상 연평균 복리 수익률 (CAGR)
        ROUND(
            (POWER(
                ((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0),  
                1.0 / 10.0                                                  
            ) - 1) * 100, 
        2) AS fep_expected_cagr,
        
        * -- USING(code)로 합쳐진 a, b, p의 모든 컬럼 (code는 단 1번만 출력됨)
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)  -- 💡 [추가] 1년 전 EPS 데이터 조인
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
SELECT  (b.trade_value::numeric / 10000000)::int AS trade_value_chunman,
        (b.market_cap::numeric / 10000000000)::int AS market_cap_bakuk,      
        b.sector,
        z.net_debt,
        a.* -- 이 자리에 기존 eps 컬럼과 함께 앞에서 정의한 eps_ratio가 포함되어 출력됩니다.
FROM last_data a 
JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code 
    AND ((a.expected_cagr >= 12.0 and a.fep_expected_cagr >= 10) OR (a.expected_cagr >= 8.0 and a.fep_expected_cagr >= 15))   -- 2. 저평가 종목 
join (select * from stock_debt where trade_date = (select max(trade_date) from stock_debt)) z on a.code = z.code
WHERE b.market_cap > 30000000000   -- 3. 소형주도 대상에 포함
    and b.trade_value > 100000000   -- 4. 소형주라도 최소 거래량 충족해야 함
    and a.eps_ratio > a.per   -- 5. 성장성 저평가 종목
    and (z.net_debt < 0.0 or z.net_debt = 'NaN')   -- 6. 순부채가 없는 종목 
    AND ggg_pcap_bakuk <= iii_pcap_bakuk and iii_pcap_bakuk <= kkk_pcap_bakuk    -- 7. 순자산이 증가하는 기업
    and a.dividend_yield >= 3.0   -- 8. 최소 배당 조건 만족 
    --AND a.eps_ratio < 100            -- [방어코드 추가] 1년 만에 이익이 100% 이상 폭증한 것은 일회성 기저효과일 확률이 높으므로 제외
    AND a.per > 0                    -- [방어코드 추가] 적자 기업(PER N/A 처리 등) 방지
    and a.code not in (SELECT code FROM mytrade WHERE trade_status = 1) -- 이미 매수한 종목 제외
ORDER BY a.expected_cagr DESC;
EEOFF



# bond_1 쿼리에 담긴 진짜 속마음(전제조건)은 이겁니다.
#     "이 회사는 역대 최악의 시절에도 ROE를 10%나 냈어. 
#     앞으로 10년 동안 장사를 기가 막히게 잘해서 내 자산(BPS)을 엄청 불려주겠지? 
#     정상적이라면 10년 뒤에 사람들이 권리금(PBR 1.5)을 얹어서 비싸게 사줘야 해.
# 
#     하지만... 주식 시장은 미쳤으니까, 
#     10년 뒤에 운이 더럽게 없어서 시장 폭락장이 오거나 이 회사가 인기가 없어져서 
#     사람들이 권리금을 단 1원도 안 쳐주고 딱 장부 가치(PBR 1.0)에만 사겠다고 헐값에 후려친다고 가정해 보자.
# 
#     그래도 내가 10년 동안 연평균 15% 수익(CAGR)을 먹을 수 있을까? 어? 먹을 수 있네? 그럼 당장 사야지!"
# 
# [DBA의 최종 요약]
# 즉, "확률이 높아서" PBR을 1로 잡은 것이 아닙니다.
# **"최악의 상황(권리금 0원)을 가정하고 후려쳐서 계산해도 내가 원하는 목표 수익률이 나오는 
# 찐 알짜배기 주식"**을 찾기 위해 일부러 가혹하게 PBR을 1로 눌러버린 것입니다.



# bond_2 쿼리는 벤저민 그레이엄의 '전통적 가치투자(PBR 1.0 회귀)'의 한계를 부수고, 
# 피터 린치나 필립 피셔 같은 대가들이 쓰는 
# **'성장주(Growth Stock) 프리미엄 모델'**로 진화하는 완벽한 질문입니다!
# 
# "지금 PBR이 3.0인 잘 나가는 회사가 10년 뒤에도 여전히 업계 1위라서 
# 최소한 PBR 2.0은 받을 텐데, 왜 굳이 PBR 1.0으로 후려쳐서 수익률을 깎아 먹어야 해?"
# 
# 이 억울함을 달래주기 위한 해결책, 아주 시원하게 팩트 폭격을 날려드립니다!
# 
# "10년 뒤의 순자산(Future BPS)에다가, 
# 시장이 이 종목에 지난 10년간 평균적으로 부여했던 
# '적정 권리금(Historical Average PBR)'을 곱해서 
# '미래의 진짜 예상 주가'를 산출해 주면 완벽하게 해결됩니다!"



cat > bond_1_2_mytrade.sql <<'EEOFF'
WITH max_date_cte AS (
    -- 💡 [추가] 쿼리 수행일 기준 가장 최신(현재) 날짜를 한 번만 추출하여 성능 최적화
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    -- 💡 [추가] 1년 전 기준 앞뒤 3일(총 1주일) 동안의 평균 EPS 계산 (휴장일 및 데이터 누락 방어)
    SELECT 
        code,
        AVG(eps) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    -- 1단계: 종목(code)별, 3개월 단위 평균 ROE 및 평균 PBR 계산
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        avg(b.market_cap/a.pbr)::bigint AS avg_market_cap,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a join stockmain b on a.trade_date = b.trade_date and a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    -- 2단계: 최악의 ROE 찾기 및 NULL 빵꾸 이력 추적
    SELECT 
        code,
        year,
        avg_roe,
        avg_market_cap,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        BOOL_OR(avg_roe IS NULL) OVER (PARTITION BY code) AS has_null_roe,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    -- 3단계: 불량 종목(5% 미만 or NULL 이력) 싹 다 제거!
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        (avg_market_cap/10000000000)::bigint AS avg_market_cap_bakuk,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    --WHERE min_roe_ever >= 5.0   -- 1. 꾸준히 수익 창출하는 기업
      --AND has_null_roe = false
),
pivot_data AS (
    -- 4단계: 2015-1Q부터 2026-1Q까지 45분기 절대 시간 피벗 전개!
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,

-- [ROE 피벗]
        MAX(avg_roe) FILTER (WHERE year = '2016') AS aaa_roe,
        MAX(avg_roe) FILTER (WHERE year = '2017') AS bbb_roe,
        MAX(avg_roe) FILTER (WHERE year = '2018') AS ccc_roe,
        MAX(avg_roe) FILTER (WHERE year = '2019') AS ddd_roe,
        MAX(avg_roe) FILTER (WHERE year = '2020') AS eee_roe,
        MAX(avg_roe) FILTER (WHERE year = '2021') AS fff_roe,
        MAX(avg_roe) FILTER (WHERE year = '2022') AS ggg_roe,
        MAX(avg_roe) FILTER (WHERE year = '2023') AS hhh_roe,
        MAX(avg_roe) FILTER (WHERE year = '2024') AS iii_roe,
        MAX(avg_roe) FILTER (WHERE year = '2025') AS jjj_roe,
        MAX(avg_roe) FILTER (WHERE year = '2026') AS kkk_roe,
        
        '***' AS ddiivv,
        
        -- [Market Cap 피벗]
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2016') AS aaa_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2017') AS bbb_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2018') AS ccc_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2019') AS ddd_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2020') AS eee_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2021') AS fff_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2022') AS ggg_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2023') AS hhh_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2024') AS iii_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2025') AS jjj_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2026') AS kkk_pcap_bakuk,
        
        '***' AS dddiiivvv,
        
        -- [dividend 피벗]
        MAX(avg_dividend) FILTER (WHERE year = '2016') AS aaa_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2017') AS bbb_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2018') AS ccc_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2019') AS ddd_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2020') AS eee_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2021') AS fff_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2022') AS ggg_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2023') AS hhh_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2024') AS iii_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2025') AS jjj_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2026') AS kkk_dividend
        
    FROM filtered_data
    GROUP BY code
),
last_data AS (
    SELECT 
        -- 💡 [추가] 1년 전 대비 EPS 증가율 계산 (%)
        -- (현재 EPS - 1년전 EPS) / |1년전 EPS| * 100
        -- NULLIF 방어코드로 분모가 0일 때의 에러(ZeroDivision) 방지
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,

        -- 💡 1. 10년 후 예상 BPS (장부 가치)
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        
        -- 💡 3. 10년 후 투자 승수 = (미래 예상 주가 / 현재 주가) [0 나누기 방어 추가]
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        
        -- 💡 4. 최종 예상 연평균 복리 수익률 (CAGR)
        ROUND(
            (POWER(
                ((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0),  
                1.0 / 10.0                                                  
            ) - 1) * 100, 
        2) AS expected_cagr,

        -- 💡 2. 10년 후 예상 주가 (미래 BPS * 역대 평균 PBR)
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        
        -- 💡 3. 10년 후 투자 승수 = (미래 예상 주가 / 현재 주가)
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        
        -- 💡 4. 최종 예상 연평균 복리 수익률 (CAGR)
        ROUND(
            (POWER(
                ((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0),  
                1.0 / 10.0                                                  
            ) - 1) * 100, 
        2) AS fep_expected_cagr,
        
        * -- USING(code)로 합쳐진 a, b, p의 모든 컬럼 (code는 단 1번만 출력됨)
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)  -- 💡 [추가] 1년 전 EPS 데이터 조인
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
SELECT  a.trade_date,a.code,a.name
   ,b.trade_div
   ,trade_expected_cagr
   ,trade_dividend
   ,trade_per
   ,trade_eps_ratio
   ,trade_roe
   ,trade_min_roe_ever
   ,trade_pbr
   ,trade_hist_avg_pbr
   ,trade_close_price
   ,remark
   ,((y.close_price / b.trade_close_price - 1) * 100)::numeric(10,2) first_profit_ratio
   ,':::' div
   ,case when b.trade_div = 'bond1' then a.expected_cagr - trade_expected_cagr end bond1_diff
   ,case when b.trade_div = 'bond1' then a.expected_cagr - 10 end bond1_sell
   ,case when b.trade_div = 'bond1' then a.expected_cagr - 8 end bond1_end
   ,case when b.trade_div = 'bond2' then a.fep_expected_cagr - trade_expected_cagr end bond2_diff   
   ,case when b.trade_div = 'bond2' then a.fep_expected_cagr - 13 end bond2_sell   
   ,case when b.trade_div = 'bond2' then a.fep_expected_cagr - 10 end bond2_end   
   ,':::' divv
   ,a.eps_ratio
   ,1 hist_pbr
   ,a.expected_cagr
   ,a.hist_avg_pbr
   ,a.fep_expected_cagr
   ,a.close_price
   ,a.change_rate
   ,a.dividend_yield
   ,a.pbr
   ,a.per
   ,a.roe
   ,a.min_roe_ever
   ,a.avg_roe_ever
   ,a.max_roe_ever
   ,a.hist_avg_pbr
   ,z.net_debt
   ,(y.trade_value::numeric / 10000000)::int AS trade_value_chunman
   ,(y.market_cap::numeric / 10000000000)::int AS market_cap_bakuk
FROM last_data a join mytrade b on a.code = b.code
join (select * from stock_debt where trade_date = (select max(trade_date) from stock_debt)) z on a.code = z.code
join stockmain y on a.trade_date = y.trade_date and a.code = y.code
where b.trade_status = 1
order by b.trade_div,a.code
EEOFF



#===============================================================================> bond_1 / bond_2 insert

cat > bond_insert.sql <<'EEOFF'
insert into mytrade
WITH max_date_cte AS (
    -- 💡 [추가] 쿼리 수행일 기준 가장 최신(현재) 날짜를 한 번만 추출하여 성능 최적화
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    -- 💡 [추가] 1년 전 기준 앞뒤 3일(총 1주일) 동안의 평균 EPS 계산 (휴장일 및 데이터 누락 방어)
    SELECT 
        code,
        AVG(eps) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    -- 1단계: 종목(code)별, 3개월 단위 평균 ROE 및 평균 PBR 계산
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        avg(b.market_cap/a.pbr)::bigint AS avg_market_cap,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a join stockmain b on a.trade_date = b.trade_date and a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    -- 2단계: 최악의 ROE 찾기 및 NULL 빵꾸 이력 추적
    SELECT 
        code,
        year,
        avg_roe,
        avg_market_cap,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        BOOL_OR(avg_roe IS NULL) OVER (PARTITION BY code) AS has_null_roe,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    -- 3단계: 불량 종목(5% 미만 or NULL 이력) 싹 다 제거!
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        (avg_market_cap/10000000000)::bigint AS avg_market_cap_bakuk,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    --WHERE min_roe_ever >= 5.0 and avg_roe_ever >= 8.0   -- 1. 꾸준히 수익 창출하는 기업
      --AND has_null_roe = false
),
pivot_data AS (
    -- 4단계: 2015-1Q부터 2026-1Q까지 45분기 절대 시간 피벗 전개!
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,

-- [ROE 피벗]
        MAX(avg_roe) FILTER (WHERE year = '2016') AS aaa_roe,
        MAX(avg_roe) FILTER (WHERE year = '2017') AS bbb_roe,
        MAX(avg_roe) FILTER (WHERE year = '2018') AS ccc_roe,
        MAX(avg_roe) FILTER (WHERE year = '2019') AS ddd_roe,
        MAX(avg_roe) FILTER (WHERE year = '2020') AS eee_roe,
        MAX(avg_roe) FILTER (WHERE year = '2021') AS fff_roe,
        MAX(avg_roe) FILTER (WHERE year = '2022') AS ggg_roe,
        MAX(avg_roe) FILTER (WHERE year = '2023') AS hhh_roe,
        MAX(avg_roe) FILTER (WHERE year = '2024') AS iii_roe,
        MAX(avg_roe) FILTER (WHERE year = '2025') AS jjj_roe,
        MAX(avg_roe) FILTER (WHERE year = '2026') AS kkk_roe,
        
        '***' AS ddiivv,
        
        -- [Market Cap 피벗]
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2016') AS aaa_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2017') AS bbb_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2018') AS ccc_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2019') AS ddd_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2020') AS eee_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2021') AS fff_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2022') AS ggg_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2023') AS hhh_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2024') AS iii_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2025') AS jjj_pcap_bakuk,
        MAX(avg_market_cap_bakuk) FILTER (WHERE year = '2026') AS kkk_pcap_bakuk,
        
        '***' AS dddiiivvv,
        
        -- [dividend 피벗]
        MAX(avg_dividend) FILTER (WHERE year = '2016') AS aaa_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2017') AS bbb_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2018') AS ccc_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2019') AS ddd_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2020') AS eee_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2021') AS fff_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2022') AS ggg_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2023') AS hhh_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2024') AS iii_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2025') AS jjj_dividend,
        MAX(avg_dividend) FILTER (WHERE year = '2026') AS kkk_dividend
        
    FROM filtered_data
    GROUP BY code
),
last_data AS (
select 
    -- 1년간 eps 상승률
    ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,
    -- 최종 예상 연평균 복리 수익률 (CAGR)
    ROUND(
        (POWER(
            ((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0),  
            1.0 / 10.0                                                  
        ) - 1) * 100, 
    2) AS expected_cagr,
    ROUND(
        (POWER(
            ((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0),  
            1.0 / 10.0                                                  
        ) - 1) * 100, 
    2) AS fep_expected_cagr,    
    * -- USING(code)로 합쳐진 a와 b의 모든 컬럼 (code는 단 1번만 출력됨)
FROM stockfdt_pbr_v a 
JOIN pivot_data b USING (code)
LEFT JOIN past_eps_cte p USING (code)  -- 💡 [추가] 1년 전 EPS 데이터 조인
CROSS JOIN max_date_cte
WHERE a.trade_date = max_date_cte.max_date
AND a.bps > 0           
AND a.close_price > 0  
)
select  a.code,
        CASE 
            WHEN a.expected_cagr >= 15.0 THEN 'bond1' 
            WHEN a.fep_expected_cagr >= 15.0 THEN 'bond2'
            WHEN a.expected_cagr >= 12.0 THEN 'bond1'
        END,
        '1',
        CASE 
            WHEN a.expected_cagr >= 15.0 THEN a.expected_cagr 
            WHEN a.fep_expected_cagr >= 15.0 THEN a.fep_expected_cagr
            WHEN a.expected_cagr >= 12.0 THEN a.expected_cagr
        END,
        a.dividend_yield,
        a.per,
        a.eps_ratio,
        a.roe,
        a.min_roe_ever,
        a.pbr,
        a.hist_avg_pbr,
        a.close_price,
        a.name,
        ' ' remark,
        current_timestamp
from last_data a
where a.code in (
 '036670'
,''
,''
,''
,''
)
EEOFF




#=========================================================================================> US

cat > bondus.sql <<'EEOFF'
WITH LatestDate AS (
    SELECT MAX(trade_date) AS max_date 
    FROM public.stockmainus
),
-- 💡 섹터별 특성을 반영하여 필터링된 유니버스를 임시 테이블로 만듭니다.
FilteredStocks AS (
    SELECT 
        v.trade_date,
        v.code,
        v.name,
        v.close_price,
        v.change_rate,
        v.pbr,
        v.per,
        v.forward_per,
        v.roe,
        v.forward_roe,
        v.dividend_yield,
        TRUNC(m.market_cap::numeric / 10000000) AS market_cap_bakuk,
        TRUNC(m.trade_value::numeric / 100000) AS trade_value_uk,
        CASE 
            WHEN d.net_debt IS NULL OR d.net_debt = 'NaN' THEN NULL 
            ELSE TRUNC(d.net_debt::numeric / 10000000) 
        END AS net_debt_bakuk,
        m.sector,
        
        -- 💡 13개 섹터를 3개의 거대한 전략 그룹으로 재분류합니다.
        CASE 
            WHEN m.sector IN ('Technology', 'Healthcare', 'Communication Services') THEN 'INNOVATION (혁신성장)'
            WHEN m.sector IN ('Financial Services', 'Real Estate', 'Utilities') THEN 'HIGH_YIELD (고배당가치)'
            ELSE 'CORE_COMPOUNDER (전통복리)' -- Industrials, Consumer, Energy, Materials
        END AS strategy_type

    FROM public.stockfdtus_pbr_v v
    JOIN public.stockmainus m 
        ON v.trade_date = m.trade_date 
       AND v.code = m.code
    LEFT JOIN (select * from stock_debtus where trade_date = (select max(trade_date) from stock_debtus)) d 
        ON v.code = d.code
    WHERE v.trade_date = (SELECT max_date FROM LatestDate)

    -- ✅ 1. 공통 체급 및 기본 방어막 (잡주, 적자기업 배제)
    AND m.market_cap >= 3000000000
    AND m.trade_value >= 3000000
    AND v.eps > 0
    AND v.forward_per > 0
    AND v.forward_per < v.per  -- 🌟 공통 진리: 내년 이익이 무조건 성장해야 함!
    
    -- ✅ 2. 쓰레기 주식 및 리스크 국가 배제 (기존 철벽 유지)
    AND m.sector NOT IN ('Unknown', '')
    AND v.name NOT ILIKE ANY (ARRAY[
        '%China%', '%Hong Kong%', '%Holdings Ltd%', '%Group Ltd%', '%Holdings Limited%', '%Group Limited%', '%ADR%'
    ])
    AND v.name NOT ILIKE ANY (ARRAY[
        '%Fund%', '%Trust%', '%ETF%', '%SPAC%', '%Acquisition%',
        '%Depositary%', '%Depository%', '%Dep Shs%', '%Preferred%', '%Pref%', '%Series%'
    ])
    AND v.code NOT LIKE '%-%P%'
    AND v.code NOT IN (SELECT code FROM mytradeus WHERE trade_status = 1)

    -- 💥 3. 핵심: 섹터별 맞춤형(Tailored) 펀더멘탈 필터
    AND (
        -- 🔵 [그룹 1] 고배당 가치 (금융, 리츠, 유틸리티)
        -- 평가 기준: PBR이 낮고 배당이 높아야 함. ROE 허들은 낮춤(리츠 특성 반영).
        (
            m.sector IN ('Financial Services', 'Real Estate', 'Utilities')
            AND v.pbr BETWEEN 0.3 AND 2.5
            AND v.per BETWEEN 5 AND 18
            AND v.roe >= 5.0                 -- 리츠 평균(4.4%)을 감안해 5%로 하향
            AND v.dividend_yield >= 3.5      -- 대신 배당은 3.5% 이상 강력히 요구
            AND (v.dividend_yield * v.per) <= 85 -- 배당 성향 85% 이하 (유틸/리츠 감안)
        )
        OR
        -- 🟢 [그룹 2] 혁신 성장 (테크, 헬스케어, 커뮤니케이션)
        -- 평가 기준: 무형자산(기술/특허) 가치를 인정하여 PBR/PER 상한을 대폭 열어줌.
        (
            m.sector IN ('Technology', 'Healthcare', 'Communication Services')
            AND v.pbr BETWEEN 1.0 AND 12.0   -- 테크주는 PBR 10배도 흔함
            AND v.per BETWEEN 10 AND 35      -- PER 35배까지 성장 프리미엄 허용
            AND v.forward_roe >= 15.0        -- 대신 내년 자본수익률(ROE)이 15% 이상으로 압도적일 것
            AND v.dividend_yield >= 0.1      -- 밈주식 방지용 (아주 적더라도 배당을 주는 근본 기업만)
            AND (d.net_debt IS NULL OR d.net_debt = 'NaN' OR d.net_debt::numeric < m.market_cap * 0.3) -- 부채 비율 엄격
        )
        OR
        -- 🟠 [그룹 3] 전통 복리 (산업재, 필수/경기소비재, 에너지, 소재)
        -- 평가 기준: 성장과 배당의 중간 밸런스 유지.
        (
            m.sector IN ('Industrials', 'Consumer Cyclical', 'Consumer Defensive', 'Basic Materials', 'Energy')
            AND v.pbr BETWEEN 0.5 AND 5.0
            AND v.per BETWEEN 8 AND 25
            AND v.forward_roe >= 12.0
            AND v.dividend_yield >= 1.5      -- 인플레 방어 이상의 배당 요구
            AND (d.net_debt IS NULL OR d.net_debt = 'NaN' OR d.net_debt::numeric < m.market_cap * 0.6)
        )
    )
)

-- ✅ 4. 최종 정렬 출력
SELECT * FROM FilteredStocks
ORDER BY 
    strategy_type,              -- 1순위: 그룹별로 묶어서 보기
    dividend_yield DESC,        -- 2순위: 배당 높은 순
    forward_per ASC;            -- 3순위: 내년 이익 대비 저평가 순
EEOFF



cat > bondus_mytrade.sql <<'EEOFF'
SELECT z.trade_dividend, z.trade_per, z.trade_roe, z.trade_pbr, z.trade_close_price, z.remark,
        v.trade_date,
        v.code,
        v.name,
        v.close_price,
        v.change_rate,
        v.pbr,
        v.per,
        v.forward_per,
        v.roe,
        v.forward_roe,
        v.dividend_yield,
        TRUNC(m.market_cap::numeric / 10000000) AS market_cap_bakuk,
        TRUNC(m.trade_value::numeric / 100000) AS trade_value_uk,
        CASE 
            WHEN d.net_debt IS NULL OR d.net_debt = 'NaN' THEN NULL 
            ELSE TRUNC(d.net_debt::numeric / 10000000) 
        END AS net_debt_bakuk,
        m.sector,
        
        -- 💡 13개 섹터를 3개의 거대한 전략 그룹으로 재분류합니다.
        CASE 
            WHEN m.sector IN ('Technology', 'Healthcare', 'Communication Services') THEN 'INNOVATION (혁신성장)'
            WHEN m.sector IN ('Financial Services', 'Real Estate', 'Utilities') THEN 'HIGH_YIELD (고배당가치)'
            ELSE 'CORE_COMPOUNDER (전통복리)' -- Industrials, Consumer, Energy, Materials
        END AS strategy_type
FROM public.stockfdtus_pbr_v v
JOIN public.stockmainus m ON v.trade_date = m.trade_date AND v.code = m.code
LEFT JOIN (select * from stock_debtus where trade_date = (select max(trade_date) from stock_debtus)) d ON v.code = d.code 
join mytradeus z on v.code = z.code and z.trade_status = 1
WHERE v.trade_date = (SELECT MAX(trade_date) FROM public.stockmainus)
ORDER BY 
    strategy_type,              -- 1순위: 그룹별로 묶어서 보기
    dividend_yield DESC,        -- 2순위: 배당 높은 순
    forward_per ASC;            -- 3순위: 내년 이익 대비 저평가 순
EEOFF



cat > bondus_insert.sql <<'EEOFF'
insert into mytradeus
SELECT 
    v.code,
    1,
    v.dividend_yield,
    v.per,    
    v.roe,    
    v.pbr,
    v.close_price,
    v.name,
    ' ',
    current_timestamp
FROM public.stockfdtus_pbr_v v
JOIN public.stockmainus m ON v.trade_date = m.trade_date AND v.code = m.code
LEFT JOIN (select * from stock_debtus where trade_date = (select max(trade_date) from stock_debtus)) d ON v.code = d.code 
WHERE v.trade_date = (SELECT MAX(trade_date) FROM public.stockmainus)
and v.code in (
'ACN'  
,'DOX' 
,'' 
,'' 
,'' 
,'' 
,'' 
)
EEOFF





----------------------------------------------------------------------> US 기업 AI 질문
WITH LatestDate AS (
    SELECT MAX(trade_date) AS max_date 
    FROM public.stockmainus
),
-- 💡 섹터별 특성을 반영하여 필터링된 유니버스를 임시 테이블로 만듭니다.
FilteredStocks AS (
    SELECT 
        v.trade_date,
        v.code,
        v.name,
        v.close_price,
        v.change_rate,
        v.pbr,
        v.per,
        v.forward_per,
        v.roe,
        v.forward_roe,
        v.dividend_yield,
        TRUNC(m.market_cap::numeric / 10000000) AS market_cap_bakuk,
        TRUNC(m.trade_value::numeric / 100000) AS trade_value_uk,
        CASE 
            WHEN d.net_debt IS NULL OR d.net_debt = 'NaN' THEN NULL 
            ELSE TRUNC(d.net_debt::numeric / 10000000) 
        END AS net_debt_bakuk,
        m.sector,
        
        -- 💡 13개 섹터를 3개의 거대한 전략 그룹으로 재분류합니다.
        CASE 
            WHEN m.sector IN ('Technology', 'Healthcare', 'Communication Services') THEN 'INNOVATION (혁신성장)'
            WHEN m.sector IN ('Financial Services', 'Real Estate', 'Utilities') THEN 'HIGH_YIELD (고배당가치)'
            ELSE 'CORE_COMPOUNDER (전통복리)' -- Industrials, Consumer, Energy, Materials
        END AS strategy_type

    FROM public.stockfdtus_pbr_v v
    JOIN public.stockmainus m 
        ON v.trade_date = m.trade_date 
       AND v.code = m.code
    LEFT JOIN (select * from stock_debtus where trade_date = (select max(trade_date) from stock_debtus)) d 
        ON v.code = d.code
    WHERE v.trade_date = (SELECT max_date FROM LatestDate)

    -- ✅ 1. 공통 체급 및 기본 방어막 (잡주, 적자기업 배제)
    AND m.market_cap >= 3000000000
    AND m.trade_value >= 3000000
    AND v.eps > 0
    AND v.forward_per > 0
    AND v.forward_per < v.per  -- 🌟 공통 진리: 내년 이익이 무조건 성장해야 함!
    
    -- ✅ 2. 쓰레기 주식 및 리스크 국가 배제 (기존 철벽 유지)
    AND m.sector NOT IN ('Unknown', '')
    AND v.name NOT ILIKE ANY (ARRAY[
        '%China%', '%Hong Kong%', '%Holdings Ltd%', '%Group Ltd%', '%Holdings Limited%', '%Group Limited%', '%ADR%'
    ])
    AND v.name NOT ILIKE ANY (ARRAY[
        '%Fund%', '%Trust%', '%ETF%', '%SPAC%', '%Acquisition%',
        '%Depositary%', '%Depository%', '%Dep Shs%', '%Preferred%', '%Pref%', '%Series%'
    ])
    AND v.code NOT LIKE '%-%P%'
    AND v.code NOT IN (SELECT code FROM mytradeus WHERE trade_status = 1)

    -- 💥 3. 핵심: 섹터별 맞춤형(Tailored) 펀더멘탈 필터
    AND (
        -- 🔵 [그룹 1] 고배당 가치 (금융, 리츠, 유틸리티)
        -- 평가 기준: PBR이 낮고 배당이 높아야 함. ROE 허들은 낮춤(리츠 특성 반영).
        (
            m.sector IN ('Financial Services', 'Real Estate', 'Utilities')
            AND v.pbr BETWEEN 0.3 AND 2.5
            AND v.per BETWEEN 5 AND 18
            AND v.roe >= 5.0                 -- 리츠 평균(4.4%)을 감안해 5%로 하향
            AND v.dividend_yield >= 3.5      -- 대신 배당은 3.5% 이상 강력히 요구
            AND (v.dividend_yield * v.per) <= 85 -- 배당 성향 85% 이하 (유틸/리츠 감안)
        )
        OR
        -- 🟢 [그룹 2] 혁신 성장 (테크, 헬스케어, 커뮤니케이션)
        -- 평가 기준: 무형자산(기술/특허) 가치를 인정하여 PBR/PER 상한을 대폭 열어줌.
        (
            m.sector IN ('Technology', 'Healthcare', 'Communication Services')
            AND v.pbr BETWEEN 1.0 AND 12.0   -- 테크주는 PBR 10배도 흔함
            AND v.per BETWEEN 10 AND 35      -- PER 35배까지 성장 프리미엄 허용
            AND v.forward_roe >= 15.0        -- 대신 내년 자본수익률(ROE)이 15% 이상으로 압도적일 것
            AND v.dividend_yield >= 0.1      -- 밈주식 방지용 (아주 적더라도 배당을 주는 근본 기업만)
            AND (d.net_debt IS NULL OR d.net_debt = 'NaN' OR d.net_debt::numeric < m.market_cap * 0.3) -- 부채 비율 엄격
        )
        OR
        -- 🟠 [그룹 3] 전통 복리 (산업재, 필수/경기소비재, 에너지, 소재)
        -- 평가 기준: 성장과 배당의 중간 밸런스 유지.
        (
            m.sector IN ('Industrials', 'Consumer Cyclical', 'Consumer Defensive', 'Basic Materials', 'Energy')
            AND v.pbr BETWEEN 0.5 AND 5.0
            AND v.per BETWEEN 8 AND 25
            AND v.forward_roe >= 12.0
            AND v.dividend_yield >= 1.5      -- 인플레 방어 이상의 배당 요구
            AND (d.net_debt IS NULL OR d.net_debt = 'NaN' OR d.net_debt::numeric < m.market_cap * 0.6)
        )
    )
)

-- ✅ 4. 최종 정렬 출력
SELECT * FROM FilteredStocks
ORDER BY 
    strategy_type,              -- 1순위: 그룹별로 묶어서 보기
    dividend_yield DESC,        -- 2순위: 배당 높은 순
    forward_per ASC;            -- 3순위: 내년 이익 대비 저평가 순



위 쿼리는 미국에 상장된 기업중 저평가된 가치주를 선별하기 위해 만든 쿼리인데...
위 쿼리 수행 결과가 아래와 같거든...
여기서 다른 기업 정보 즉 roe나 배당이 꾸준히 증가하는지? 기업에 해자가 있는지?
등도 추가로 파악해서...
향후 AI 시대에도 경쟁력이 있고 안정적으로 배당을 받거나 아니면 안정적으로 주가가 우상향할 수 있는
오래 보유할 장기 복리 가치주 TOP3 선별해줘...
만약 리스트된 종목 모두 장기 보유에 적합한 가치주가 아니라면 솔직하게 모두 적합하지 않다고 답변해줘...



 trade_date | code  |              name               | close_price | change_rate |  pbr  |  per  | forward_per |  roe  | forward_roe | dividend_yield | market_cap_bakuk | trade_value_uk | net_debt_bakuk |         sector         |       strategy_type
------------+-------+---------------------------------+-------------+-------------+-------+-------+-------------+-------+-------------+----------------+------------------+----------------+----------------+------------------------+----------------------------
 2026-04-21 | ARLP  | Alliance Resource Partners, L.P |       24.79 |        0.81 |  1.73 | 10.33 |        9.09 | 16.75 |       19.03 |          10.08 |              318 |            113 |             39 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | WES   | Western Midstream Partners, LP  |       40.58 |       -0.27 |  4.12 | 13.62 |       10.92 | 30.25 |       37.73 |           9.17 |             1655 |            668 |            799 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | ABEV  | Ambev S.A.                      |        3.07 |        0.00 |  2.73 | 15.35 |       14.64 | 17.79 |       18.65 |           9.12 |             4788 |            921 |          -1693 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | MPLX  | MPLX LP                         |       55.47 |       -0.40 |  3.94 | 11.51 |       11.25 | 34.23 |       35.02 |           7.77 |             5641 |           1522 |           2402 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | EPD   | Enterprise Products Partners L. |       37.21 |        0.81 |  2.70 | 13.99 |       11.88 | 19.30 |       22.73 |           5.89 |             8043 |            875 |           3391 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | BBY   | Best Buy Co., Inc.              |       66.59 |       -0.52 |  4.70 | 13.21 |        9.49 | 35.58 |       49.53 |           5.77 |             1395 |           1780 |            224 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | FRO   | Frontline Plc                   |       35.38 |       -4.30 |  3.14 | 20.81 |       12.36 | 15.09 |       25.40 |           4.97 |              787 |           1294 |            281 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | KVUE  | Kenvue Inc.                     |       17.30 |       -1.59 |  3.08 | 22.76 |       14.35 | 13.53 |       21.46 |           4.80 |             3321 |           2552 |            761 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | EMN   | Eastman Chemical Company        |       72.46 |       -1.60 |  1.39 | 17.67 |       10.64 |  7.87 |       13.06 |           4.61 |              828 |            599 |            453 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | REYN  | Reynolds Consumer Products Inc. |       20.98 |       -3.01 |  1.96 | 14.67 |       12.53 | 13.36 |       15.64 |           4.39 |              442 |            259 |            155 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | AM    | Antero Midstream Corporation    |       20.61 |       -1.29 |  4.95 | 23.97 |       13.89 | 20.65 |       35.64 |           4.37 |              981 |            450 |            304 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | SFD   | Smithfield Foods, Inc.          |       29.00 |        1.54 |  1.68 | 11.55 |       11.08 | 14.55 |       15.16 |           4.31 |             1141 |            285 |             85 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | B     | Barrick Mining Corporation      |       40.45 |       -5.84 |  2.55 | 13.81 |        9.10 | 18.46 |       28.02 |           4.15 |             6793 |           4784 |           -149 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | TTE   | TotalEnergies SE                |       88.37 |        1.21 |  1.65 | 15.29 |        9.62 | 10.79 |       17.15 |           4.14 |            18845 |           1034 |           3220 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | EQNR  | Equinor ASA                     |       37.65 |        4.55 |  4.66 | 19.41 |        9.75 | 24.01 |       47.79 |           4.14 |             9382 |           2045 |           1188 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | RIO   | Rio Tinto Plc                   |       97.72 |       -2.11 |  2.55 | 16.07 |       11.44 | 15.87 |       22.29 |           4.11 |            15890 |           1681 |           1432 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | LKQ   | LKQ Corporation                 |       31.24 |        1.13 |  1.22 | 13.52 |        9.44 |  9.02 |       12.92 |           3.84 |              797 |            806 |            474 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | BVN   | Buenaventura Mining Company Inc |       31.76 |       -9.46 |  1.99 |  9.62 |        9.39 | 20.69 |       21.19 |           3.59 |              806 |            622 |             17 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | KDP   | Keurig Dr Pepper Inc.           |       26.44 |       -0.23 |  1.41 | 17.28 |       10.57 |  8.16 |       13.34 |           3.48 |             3592 |           4315 |           1820 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | TGT   | Target Corporation              |      132.10 |        1.47 |  3.70 | 16.23 |       15.51 | 22.80 |       23.86 |           3.45 |             5982 |           7580 |           1480 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | BHP   | BHP Group Limited               |       77.69 |       -2.39 |  3.91 | 19.28 |       14.89 | 20.28 |       26.26 |           3.42 |            19735 |           2300 |           1568 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | EXE   | Expand Energy Corporation       |       94.26 |       -1.27 |  1.21 | 12.45 |        9.84 |  9.72 |       12.30 |           3.38 |             2265 |           4345 |            449 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | SHEL  | Shell PLC                       |       88.66 |        0.75 |  1.45 | 14.78 |       10.20 |  9.81 |       14.22 |           3.36 |            24767 |           4189 |           4547 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | IPAR  | Interparfums, Inc.              |       95.20 |       -2.87 |  3.47 | 18.17 |       17.12 | 19.10 |       20.27 |           3.36 |              305 |            159 |             -8 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | KFY   | Korn Ferry                      |       66.93 |        0.45 |  1.72 | 13.28 |       11.61 | 12.95 |       14.81 |           3.29 |              349 |            196 |            -39 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | JD    | JD.com, Inc.                    |       30.52 |       -2.71 |  1.27 | 16.15 |        7.28 |  7.86 |       17.45 |           3.28 |             4518 |           2151 |         -10612 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | MGA   | Magna International, Inc.       |       61.67 |       -0.61 |  1.38 | 21.05 |        8.11 |  6.56 |       17.02 |           3.21 |             1717 |            790 |            507 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | PSX   | Phillips 66                     |      159.38 |        2.33 |  2.20 | 14.77 |       10.60 | 14.90 |       20.75 |           3.19 |             6390 |           2359 |           2046 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | MZTI  | The Marzetti Company            |      127.13 |       -4.59 |  3.38 | 19.47 |       17.29 | 17.36 |       19.55 |           3.15 |              349 |            524 |            -16 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | EOG   | EOG Resources, Inc.             |      132.43 |        2.53 |  2.39 | 14.52 |        9.50 | 16.46 |       25.16 |           3.08 |             7104 |           4601 |            573 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | PR    | Permian Resources Corporation   |       19.91 |        1.58 |  1.44 | 15.55 |       10.17 |  9.26 |       14.16 |           3.06 |             1747 |           2517 |            354 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | TGS   | Transportadora de Gas del Sur S |       31.06 |        0.26 |  2.04 | 15.23 |       11.40 | 13.39 |       17.89 |           3.06 |              481 |             60 |         -10256 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | TS    | Tenaris S.A.                    |       60.75 |        1.86 |  3.70 | 16.60 |       15.89 | 22.29 |       23.29 |           2.93 |             3066 |           1086 |           -243 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | ALV   | Autoliv, Inc.                   |      117.69 |       -3.02 |  3.34 | 12.68 |        9.85 | 26.34 |       33.91 |           2.80 |              881 |            946 |            191 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | COP   | ConocoPhillips                  |      120.26 |        3.27 |  2.28 | 18.94 |       14.62 | 12.04 |       15.60 |           2.79 |            14699 |           8438 |           1741 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | SU    | Suncor Energy  Inc.             |       62.55 |        1.44 |  2.30 | 17.77 |       12.04 | 12.94 |       19.10 |           2.78 |             7447 |           2518 |           1121 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | XOM   | Exxon Mobil Corporation         |      148.36 |        0.46 |  2.39 | 22.14 |       14.74 | 10.79 |       16.21 |           2.78 |            61666 |          20590 |           3980 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | CTRA  | Coterra Energy Inc.             |       31.85 |        1.37 |  1.63 | 14.22 |       10.83 | 11.46 |       15.05 |           2.76 |             2418 |           1838 |            389 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | ZTO   | ZTO Express (Cayman) Inc.       |       25.52 |        0.91 |  2.10 | 15.56 |       11.76 | 13.50 |       17.86 |           2.70 |             1957 |            374 |          -1434 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | GAP   | Gap, Inc. (The)                 |       26.65 |       -2.27 |  2.61 | 12.51 |       10.22 | 20.86 |       25.54 |           2.63 |              991 |           1265 |            260 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | STZ   | Constellation Brands, Inc.      |      156.95 |       -1.80 |  3.53 | 16.33 |       12.59 | 21.62 |       28.04 |           2.63 |             2732 |           1741 |           1046 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | AEO   | American Eagle Outfitters, Inc. |       19.31 |       -2.87 |  1.93 | 17.72 |        9.78 | 10.89 |       19.73 |           2.59 |              321 |            958 |            150 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | FBIN  | Fortune Brands Innovations, Inc |       40.40 |       -2.37 |  2.03 | 16.36 |       10.32 | 12.41 |       19.67 |           2.57 |              484 |            442 |            255 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | PPG   | PPG Industries, Inc.            |      110.92 |       -3.32 |  3.12 | 16.03 |       12.91 | 19.46 |       24.17 |           2.56 |             2482 |           1751 |            568 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | MTDR  | Matador Resources Company       |       58.97 |        4.48 |  1.30 |  9.68 |        7.17 | 13.43 |       18.13 |           2.54 |              732 |           1091 |            353 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | SNA   | Snap-On Incorporated            |      383.58 |       -0.85 |  3.35 | 19.99 |       18.00 | 16.76 |       18.61 |           2.54 |             1996 |           1289 |            -33 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | CRC   | California Resources Corporatio |       63.92 |        2.80 |  1.54 | 15.40 |       12.74 | 10.00 |       12.09 |           2.53 |              567 |            355 |            122 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | NFG   | National Fuel Gas Company       |       86.26 |       -0.47 |  2.28 | 12.03 |       10.46 | 18.95 |       21.80 |           2.48 |              819 |            587 |            250 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | CCEP  | Coca-Cola Europacific Partners  |       95.88 |       -1.46 |  4.66 | 19.14 |       16.68 | 24.35 |       27.94 |           2.45 |             4271 |           1562 |            982 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | LEA   | Lear Corporation                |      128.47 |       -0.33 |  1.29 | 15.76 |        7.60 |  8.19 |       16.97 |           2.40 |              651 |            371 |            246 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | LEVI  | Levi Strauss & Co               |       23.36 |        1.08 |  4.07 | 17.18 |       13.91 | 23.69 |       29.26 |           2.40 |              898 |            721 |            150 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | YUMC  | Yum China Holdings, Inc.        |       48.50 |       -0.39 |  3.19 | 19.32 |       14.95 | 16.51 |       21.34 |           2.39 |             1702 |            574 |             94 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | CNI   | Canadian National Railway Compa |      110.17 |       -0.93 |  4.29 | 20.07 |       17.22 | 21.38 |       24.91 |           2.39 |             6744 |           1198 |           2127 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | CBT   | Cabot Corporation               |       76.02 |        0.68 |  2.52 | 13.29 |       11.08 | 18.96 |       22.74 |           2.37 |              396 |            198 |             89 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | PKG   | Packaging Corporation of Americ |      210.53 |       -1.35 |  4.06 | 24.54 |       17.64 | 16.54 |       23.02 |           2.37 |             1878 |           1161 |            376 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | AROC  | Archrock, Inc.                  |       35.19 |       -1.90 |  4.13 | 19.23 |       15.36 | 21.48 |       26.89 |           2.36 |              616 |            326 |            242 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | HMY   | Harmony Gold Mining Company Lim |       17.25 |       -5.94 |  3.40 | 10.92 |        5.20 | 31.14 |       65.38 |           2.32 |             1089 |           1353 |            594 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | SLB   | SLB Limited                     |       52.77 |        1.09 |  3.02 | 22.46 |       15.80 | 13.45 |       19.11 |           2.24 |             7921 |           7170 |            824 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | CVE   | Cenovus Energy Inc              |       25.60 |        2.77 |  2.10 | 16.41 |       10.60 | 12.80 |       19.81 |           2.23 |             4811 |           1642 |           1146 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | OVV   | Ovintiv Inc. (DE)               |       54.91 |        2.73 |  1.24 | 11.49 |        7.00 | 10.79 |       17.71 |           2.19 |             1555 |           1002 |            610 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | DKS   | Dick's Sporting Goods Inc       |      228.81 |       -0.78 |  3.67 | 22.97 |       14.18 | 15.98 |       25.88 |           2.19 |             2058 |           1513 |            639 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | AOS   | A.O. Smith Corporation          |       64.99 |       -0.93 |  4.85 | 16.88 |       15.00 | 28.73 |       32.33 |           2.18 |              898 |            423 |              1 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | LEN-B | Lennar Corporation              |       92.22 |        0.28 |  1.03 | 13.27 |        5.69 |  7.76 |       18.10 |           2.17 |             2271 |             47 |           -346 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | MGY   | Magnolia Oil & Gas Corporation  |       28.71 |        2.65 |  2.68 | 16.60 |       11.27 | 16.14 |       23.78 |           2.16 |              548 |            338 |             15 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | EDU   | New Oriental Education & Techno |       56.36 |       -3.66 |  2.30 | 23.48 |       13.40 |  9.80 |       17.16 |           2.13 |              939 |            344 |           -415 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | SUNB  | Sunbelt Rentals Holdings, Inc.  |       70.57 |       -2.45 |  3.85 | 21.65 |       16.46 | 17.78 |       23.39 |           2.13 |             2916 |           1765 |           1042 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | DVN   | Devon Energy Corporation        |       45.60 |        1.47 |  1.83 | 10.94 |        8.51 | 16.73 |       21.50 |           2.11 |             2833 |           4526 |            730 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | GNTX  | Gentex Corporation              |       22.74 |        0.04 |  1.97 | 13.07 |       10.57 | 15.07 |       18.64 |           2.11 |              489 |            463 |            -13 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | RPM   | RPM International Inc.          |      107.29 |       -2.21 |  4.36 | 20.67 |       18.30 | 21.09 |       23.83 |           2.01 |             1374 |            679 |            260 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | MMS   | Maximus, Inc.                   |       67.92 |       -1.05 |  2.15 | 10.42 |        7.45 | 20.63 |       28.86 |           1.94 |              370 |            234 |            153 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | DG    | Dollar General Corporation      |      124.11 |       -1.86 |  3.21 | 18.12 |       15.55 | 17.72 |       20.64 |           1.90 |             2733 |           2078 |           1458 | Consumer Defensive     | CORE_COMPOUNDER (전통복리)
 2026-04-21 | COLM  | Columbia Sportswear Company     |       63.97 |       -0.09 |  2.00 | 19.74 |       16.61 | 10.13 |       12.04 |           1.88 |              344 |            274 |            -31 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | GD    | General Dynamics Corporation    |      325.52 |       -1.99 |  3.43 | 21.08 |       18.11 | 16.27 |       18.94 |           1.87 |             8816 |           4303 |            745 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | KBR   | KBR, Inc.                       |       36.71 |        0.69 |  3.09 | 10.52 |        8.74 | 29.37 |       35.35 |           1.80 |              465 |            383 |            238 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | NSC   | Norfolk Southern Corporation    |      302.22 |       -0.63 |  4.36 | 23.72 |       22.57 | 18.38 |       19.32 |           1.79 |             6787 |           2725 |           1630 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | MPC   | Marathon Petroleum Corporation  |      220.35 |        2.91 |  3.75 | 16.68 |       11.56 | 22.48 |       32.44 |           1.73 |             6494 |           3226 |           3068 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | BTG   | B2Gold Corp                     |        4.72 |       -4.45 |  1.76 | 16.86 |        3.98 | 10.44 |       44.22 |           1.69 |              634 |           2771 |             21 | Basic Materials        | CORE_COMPOUNDER (전통복리)
 2026-04-21 | GIL   | Gildan Activewear, Inc.         |       60.51 |        1.37 |  3.15 | 23.54 |       11.27 | 13.38 |       27.95 |           1.65 |             1121 |            908 |            435 | Consumer Cyclical      | CORE_COMPOUNDER (전통복리)
 2026-04-21 | BKR   | Baker Hughes Company            |       60.25 |        1.83 |  3.16 | 23.17 |       21.51 | 13.64 |       14.69 |           1.53 |             5975 |           3292 |            298 | Energy                 | CORE_COMPOUNDER (전통복리)
 2026-04-21 | OSK   | Oshkosh Corporation (Holding Co |      150.11 |       -1.11 |  2.07 | 14.98 |       10.54 | 13.82 |       19.64 |           1.52 |              940 |            554 |             89 | Industrials            | CORE_COMPOUNDER (전통복리)
 2026-04-21 | OMF   | OneMain Holdings, Inc.          |       58.91 |       -1.69 |  2.03 |  8.98 |        6.66 | 22.61 |       30.48 |           7.13 |              693 |            556 |           2190 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | AEG   | Aegon Ltd. New York Registry Sh |        8.02 |       -0.50 |  1.37 | 11.30 |        8.66 | 12.12 |       15.82 |           5.86 |             1214 |            612 |            480 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | NWG   | NatWest Group plc               |       16.11 |       -2.54 |  1.25 |  8.85 |        7.51 | 14.12 |       16.64 |           5.46 |             6420 |            498 |          -7259 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | BBD   | Banco Bradesco Sa               |        4.07 |       -2.86 |  1.21 |  9.47 |        7.65 | 12.78 |       15.82 |           5.16 |             4302 |           1492 |          54581 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | COLB  | Columbia Banking System, Inc.   |       29.09 |       -1.99 |  1.10 | 12.65 |        8.53 |  8.70 |       12.90 |           5.02 |              859 |            533 |            173 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | LNC   | Lincoln National Corporation    |       36.69 |       -1.19 |  0.70 |  6.29 |        4.35 | 11.13 |       16.09 |           4.91 |              701 |            926 |          -3800 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | AVA   | Avista Corporation              |       40.36 |       -2.20 |  1.22 | 16.96 |       14.45 |  7.19 |        8.44 |           4.86 |              333 |            204 |            329 | Utilities              | HIGH_YIELD (고배당가치)
 2026-04-21 | BBVA  | Banco Bilbao Vizcaya Argentaria |       22.47 |       -3.15 |  1.89 | 10.86 |       10.02 | 17.40 |       18.86 |           4.81 |            12699 |            485 |            424 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | ES    | Eversource Energy (D/B/A)       |       66.82 |       -2.47 |  1.55 | 14.65 |       13.20 | 10.58 |       11.74 |           4.71 |             2511 |           1091 |           3010 | Utilities              | HIGH_YIELD (고배당가치)
 2026-04-21 | ING   | ING Group, N.V.                 |       28.49 |       -1.42 |  1.30 | 11.44 |        9.19 | 11.36 |       14.15 |           4.49 |             8205 |            639 |          10375 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | D     | Dominion Energy, Inc.           |       61.09 |       -1.82 |  1.91 | 17.61 |       16.03 | 10.85 |       11.92 |           4.37 |             5369 |           1613 |           4989 | Utilities              | HIGH_YIELD (고배당가치)
 2026-04-21 | WSBC  | WesBanco, Inc.                  |       35.73 |       -1.62 |  0.90 | 16.02 |        8.67 |  5.62 |       10.38 |           4.25 |              343 |            253 |             74 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | BNS   | Bank Nova Scotia Halifax Pfd 3  |       75.87 |       -1.19 |  1.46 | 15.52 |       11.35 |  9.41 |       12.86 |           4.19 |             9387 |           1305 |         -17635 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | UGI   | UGI Corporation                 |       36.52 |       -0.81 |  1.57 | 13.58 |       10.79 | 11.56 |       14.55 |           4.11 |              784 |            260 |            696 | Utilities              | HIGH_YIELD (고배당가치)
 2026-04-21 | TFC   | Truist Financial Corporation    |       51.07 |        0.45 |  1.07 | 12.64 |        9.98 |  8.47 |       10.72 |           4.07 |             6362 |           3677 |           2358 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | CNA   | CNA Financial Corporation       |       48.17 |        1.03 |  1.12 | 10.27 |       10.04 | 10.91 |       11.16 |           3.99 |             1303 |            212 |             44 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | WF    | Woori Financial Group Inc.      |       71.60 |       -2.27 |  2.44 |  8.65 |        7.86 | 28.21 |       31.04 |           3.94 |             1744 |             87 |        4522141 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | BSAC  | Banco Santander - Chile         |       34.16 |       -3.69 |  1.40 | 13.83 |       11.05 | 10.12 |       12.67 |           3.89 |             1609 |            112 |                | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | FHB   | First Hawaiian, Inc.            |       26.87 |       -1.03 |  1.19 | 12.21 |       11.28 |  9.75 |       10.55 |           3.87 |              330 |            618 |           -142 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | IFS   | Intercorp Financial Services In |       47.70 |       -2.63 |  1.48 |  9.86 |        8.12 | 15.01 |       18.23 |           3.77 |              529 |            109 |            860 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | SLF   | Sun Life Financial Inc.         |       70.32 |       -0.13 |  2.33 | 15.77 |       11.28 | 14.77 |       20.66 |           3.75 |             3896 |            308 |          -7681 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | RF    | Regions Financial Corporation   |       28.35 |        0.14 |  1.39 | 11.76 |        9.93 | 11.82 |       14.00 |           3.74 |             2421 |           2154 |           -480 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | KEY   | KeyCorp                         |       22.10 |       -0.41 |  1.36 | 13.56 |       10.29 | 10.03 |       13.22 |           3.71 |             2402 |           3169 |           1514 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | FG    | F&G Annuities & Life, Inc.      |       27.11 |        0.15 |  0.77 | 14.42 |        4.48 |  5.34 |       17.19 |           3.69 |              367 |             90 |            -34 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | LYG   | Lloyds Banking Group Plc        |        5.42 |       -3.56 |  1.41 | 14.65 |       10.19 |  9.62 |       13.84 |           3.69 |             7958 |           1185 |          -2300 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | HBAN  | Huntington Bancshares Incorpora |       16.97 |       -0.53 |  1.23 | 12.21 |        8.87 | 10.07 |       13.87 |           3.65 |             3456 |           2382 |            489 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | EXC   | Exelon Corporation              |       46.27 |       -0.28 |  1.64 | 16.95 |       15.21 |  9.68 |       10.78 |           3.63 |             4734 |           4490 |           4961 | Utilities              | HIGH_YIELD (고배당가치)
 2026-04-21 | DB    | Deutsche Bank AG                |       32.57 |       -2.46 |  0.65 |  8.95 |        6.04 |  7.26 |       10.76 |           3.62 |             6317 |            695 |         -22060 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | USB   | U.S. Bancorp                    |       56.84 |       -0.28 |  1.51 | 11.92 |       10.09 | 12.67 |       14.97 |           3.62 |             8838 |           3589 |           3079 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | BOH   | Bank of Hawaii Corporation      |       77.78 |       -2.85 |  2.05 | 15.68 |       11.24 | 13.07 |       18.24 |           3.60 |              308 |            385 |            -23 | Financial Services     | HIGH_YIELD (고배당가치)
 2026-04-21 | WIT   | Wipro Limited                   |        2.13 |       -0.47 |  2.33 | 15.21 |       13.38 | 15.32 |       17.41 |           8.92 |             2278 |            288 |         -34032 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | PAYX  | Paychex, Inc.                   |       93.68 |        0.63 |  8.36 | 20.68 |       15.87 | 40.43 |       52.68 |           4.61 |             3356 |           3037 |            323 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | UMC   | United Microelectronics Corpora |       12.33 |       -2.45 |  2.57 | 23.26 |       16.52 | 11.05 |       15.56 |           3.89 |             3129 |           1612 |          -4939 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | CHT   | Chunghwa Telecom Co., Ltd.      |       43.10 |       -0.90 | 10.91 | 27.28 |       25.90 | 39.99 |       42.12 |           3.87 |             3343 |             42 |          -1948 | Communication Services | INNOVATION (혁신성장)
 2026-04-21 | PHG   | Koninklijke Philips N.V. NY Reg |       28.48 |       -2.43 |  2.10 | 26.13 |       13.90 |  8.04 |       15.11 |           3.55 |             2730 |            307 |            529 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | MDT   | Medtronic plc.                  |       82.00 |       -3.53 |  2.15 | 22.91 |       13.53 |  9.38 |       15.89 |           3.46 |            10527 |           7488 |           1968 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | DOX   | Amdocs Limited                  |       66.37 |       -0.88 |  2.09 | 12.84 |        8.20 | 16.28 |       25.49 |           3.44 |              716 |            558 |             70 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | ACN   | Accenture plc                   |      194.42 |       -0.33 |  3.83 | 15.95 |       13.04 | 24.01 |       29.37 |           3.35 |            11965 |           6194 |           -105 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | NVS   | Novartis AG                     |      147.97 |       -2.00 |  6.12 | 20.67 |       15.24 | 29.61 |       40.16 |           3.20 |            28561 |           3133 |           2392 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | GSK   | GSK plc                         |       56.12 |       -2.14 |  5.07 | 14.93 |       10.87 | 33.96 |       46.64 |           3.17 |            11205 |           2244 |           1436 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | MRK   | Merck & Company, Inc.           |      112.56 |       -3.88 |  5.29 | 15.46 |       11.56 | 34.22 |       45.76 |           3.02 |            27829 |          11776 |           3596 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | ABT   | Abbott Laboratories             |       92.72 |       -3.42 |  3.09 | 25.97 |       15.30 | 11.90 |       20.20 |           2.72 |            16111 |          15876 |            519 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | QCOM  | QUALCOMM Incorporated           |      135.56 |       -1.43 |  6.31 | 27.33 |       12.27 | 23.09 |       51.43 |           2.71 |            14478 |          16058 |            299 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | IBM   | International Business Machines |      255.68 |        0.78 |  7.34 | 22.95 |       19.10 | 31.98 |       38.43 |           2.63 |            23993 |          14251 |           5018 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | UNH   | UnitedHealth Group Incorporated |      346.01 |        6.96 |  3.33 | 26.15 |       17.20 | 12.73 |       19.36 |           2.55 |            31406 |          90080 |           5488 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | GILD  | Gilead Sciences, Inc.           |      133.29 |       -1.90 |  7.29 | 19.66 |       13.85 | 37.08 |       52.64 |           2.46 |            16544 |           7428 |           1592 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | BR    | Broadridge Financial Solutions, |      161.87 |        0.37 |  6.56 | 17.89 |       15.64 | 36.67 |       41.94 |           2.41 |             1889 |           1533 |            300 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | JNJ   | Johnson & Johnson               |      226.16 |       -1.96 |  6.68 | 20.50 |       17.80 | 32.59 |       37.53 |           2.37 |            54502 |          25990 |           2923 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | CTSH  | Cognizant Technology Solutions  |       60.45 |        0.32 |  1.93 | 13.26 |        9.86 | 14.56 |       19.57 |           2.18 |             2917 |           3273 |            -74 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | G     | Genpact Limited                 |       37.03 |        1.06 |  2.47 | 11.83 |        8.35 | 20.88 |       29.58 |           2.03 |              629 |            837 |             55 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | RPRX  | Royalty Pharma plc              |       49.48 |        0.37 |  3.27 | 27.80 |        8.95 | 11.76 |       36.54 |           1.90 |             2933 |           1261 |            833 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | CSCO  | Cisco Systems, Inc.             |       89.70 |        2.27 |  7.42 | 32.27 |       19.88 | 22.99 |       37.32 |           1.87 |            35441 |          16717 |           1596 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | NXPI  | NXP Semiconductors N.V.         |      224.50 |        1.43 |  5.64 | 28.24 |       13.45 | 19.97 |       41.93 |           1.81 |             5672 |           7547 |            924 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | DGX   | Quest Diagnostics Incorporated  |      205.04 |        4.45 |  3.15 | 23.41 |       17.86 | 13.46 |       17.64 |           1.68 |             2269 |           4972 |            616 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | SAP   | SAP  SE                         |      176.44 |       -1.40 |  3.94 | 24.57 |       17.94 | 16.04 |       21.96 |           1.68 |            20747 |           5126 |           -206 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | LOGI  | Logitech International S.A. - R |       98.17 |       -1.84 |  6.17 | 20.58 |       17.08 | 29.98 |       36.12 |           1.62 |             1447 |            922 |           -172 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | AZN   | AstraZeneca PLC                 |      195.78 |       -2.45 |  6.24 | 29.94 |       24.64 | 20.84 |       25.32 |           1.61 |            30351 |           4171 |           2398 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | GRMN  | Garmin Ltd.                     |      265.49 |       -0.76 |  5.70 | 30.91 |       26.03 | 18.44 |       21.90 |           1.58 |             5110 |           1110 |           -254 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | JKHY  | Jack Henry & Associates, Inc.   |      155.50 |        1.06 |  5.09 | 22.31 |       22.24 | 22.81 |       22.89 |           1.51 |             1125 |           1675 |              4 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | ATR   | AptarGroup, Inc.                |      126.69 |       -3.23 |  3.05 | 21.51 |       19.90 | 14.18 |       15.33 |           1.47 |              815 |            310 |            113 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | DLO   | DLocal Limited                  |       13.30 |       -1.70 |  6.89 | 20.46 |       12.17 | 33.68 |       56.61 |           1.43 |              392 |            126 |            -75 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | BMI   | Badger Meter, Inc.              |      120.89 |        6.60 |  5.12 | 27.35 |       23.78 | 18.72 |       21.53 |           1.27 |              352 |           2082 |            -20 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | BZ    | KANZHUN LIMITED - American Depo |       13.82 |       -1.92 |  2.18 | 15.89 |        9.45 | 13.72 |       23.07 |           1.23 |              634 |            616 |          -1984 | Communication Services | INNOVATION (혁신성장)
 2026-04-21 | PAYC  | Paycom Software, Inc.           |      130.44 |       -1.91 |  4.03 | 16.12 |       11.20 | 25.00 |       35.98 |           1.15 |              707 |           1070 |            -27 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | INTU  | Intuit Inc.                     |      404.85 |        0.00 |  5.90 | 26.32 |       15.29 | 22.42 |       38.59 |           1.15 |            11265 |          12669 |            391 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | STE   | STERIS plc (Ireland)            |      219.45 |       -3.07 |  3.01 | 30.56 |       19.81 |  9.85 |       15.19 |           1.12 |             2153 |           2641 |            162 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | LDOS  | Leidos Holdings, Inc.           |      153.20 |       -1.05 |  3.94 | 13.74 |       11.63 | 28.68 |       33.88 |           1.08 |             1936 |           1073 |            423 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | RMD   | ResMed Inc.                     |      221.22 |       -2.18 |  5.10 | 21.88 |       18.16 | 23.31 |       28.08 |           1.08 |             3229 |           1815 |            -56 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | INGM  | Ingram Micro Holding Corporatio |       30.86 |        1.81 |  1.71 | 22.20 |        8.68 |  7.70 |       19.70 |           1.07 |              725 |            299 |            179 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | LH    | Labcorp Holdings Inc.           |      273.42 |        1.57 |  2.61 | 26.11 |       14.18 | 10.00 |       18.41 |           1.05 |             2252 |           1559 |            599 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | FOX   | Fox Corporation                 |       57.63 |       -0.62 |  2.24 | 13.82 |       11.60 | 16.21 |       19.31 |           0.97 |             2690 |            405 |            547 | Communication Services | INNOVATION (혁신성장)
 2026-04-21 | CRM   | Salesforce, Inc.                |      187.11 |        0.45 |  2.94 | 23.99 |       12.53 | 12.26 |       23.46 |           0.94 |            17270 |          22496 |            814 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | SNX   | TD SYNNEX Corporation           |      215.19 |       -1.23 |  1.96 | 17.92 |       11.92 | 10.94 |       16.44 |           0.89 |             1734 |           1033 |            359 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | FOXA  | Fox Corporation                 |       64.31 |       -0.68 |  2.50 | 15.42 |       12.47 | 16.21 |       20.05 |           0.87 |             2735 |           1554 |            547 | Communication Services | INNOVATION (혁신성장)
 2026-04-21 | MSFT  | Microsoft Corporation           |      424.16 |        1.46 |  8.06 | 26.56 |       22.44 | 30.35 |       35.92 |           0.86 |           315251 |         134796 |           3381 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | A     | Agilent Technologies, Inc.      |      122.09 |        0.93 |  5.00 | 26.89 |       18.55 | 18.59 |       26.95 |           0.84 |             3453 |           1699 |            179 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | EHC   | Encompass Health Corporation    |      102.77 |       -3.29 |  4.21 | 18.52 |       15.87 | 22.73 |       26.53 |           0.74 |             1034 |            585 |            264 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | ADEA  | Adeia Inc.                      |       29.72 |        0.68 |  6.74 | 30.02 |       19.86 | 22.45 |       33.94 |           0.67 |              329 |            504 |             29 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | GIB   | CGI Inc.                        |       76.80 |       -0.18 |  2.27 | 14.17 |       10.80 | 16.02 |       21.02 |           0.65 |             1650 |            399 |            358 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | QGEN  | Qiagen N.V.                     |       40.10 |       -2.29 |  2.19 | 19.66 |       14.56 | 11.14 |       15.04 |           0.65 |              826 |            409 |             73 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | CHE   | Chemed Corp                     |      373.79 |       -3.11 |  5.23 | 20.40 |       14.04 | 25.64 |       37.25 |           0.62 |              514 |           1253 |              6 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | REGN  | Regeneron Pharmaceuticals, Inc. |      747.36 |       -0.27 |  2.45 | 18.00 |       14.26 | 13.61 |       17.18 |           0.50 |             7901 |           5286 |           -563 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | TMO   | Thermo Fisher Scientific Inc    |      524.57 |       -0.28 |  3.70 | 29.59 |       19.33 | 12.50 |       19.14 |           0.34 |            19494 |           9053 |           3125 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | PEGA  | Pegasystems Inc.                |       39.29 |      -10.30 |  8.50 | 18.45 |       12.82 | 46.07 |       66.30 |           0.31 |              666 |           2098 |            -34 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | META  | Meta Platforms, Inc.            |      668.84 |       -0.31 |  7.79 | 28.49 |       18.78 | 27.34 |       41.48 |           0.31 |           169779 |          57440 |            348 | Communication Services | INNOVATION (혁신성장)
 2026-04-21 | GOOG  | Alphabet Inc.                   |      330.47 |       -1.47 |  9.62 | 30.60 |       24.51 | 31.44 |       39.25 |           0.25 |           399769 |          47625 |          -5984 | Communication Services | INNOVATION (혁신성장)
 2026-04-21 | GOOGL | Alphabet Inc.                   |      332.29 |       -1.52 |  9.67 | 30.77 |       24.65 | 31.43 |       39.23 |           0.25 |           401971 |          76626 |          -5984 | Communication Services | INNOVATION (혁신성장)
 2026-04-21 | GEHC  | GE HealthCare Technologies Inc. |       72.26 |       -2.55 |  3.17 | 15.88 |       12.99 | 19.96 |       24.40 |           0.19 |             3297 |           1944 |            597 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | BDC   | Belden Inc                      |      132.63 |        0.65 |  4.09 | 22.48 |       14.90 | 18.19 |       27.45 |           0.15 |              516 |            316 |            102 | Technology             | INNOVATION (혁신성장)
 2026-04-21 | ENSG  | The Ensign Group, Inc.          |      191.74 |       -2.94 |  4.95 | 32.78 |       23.27 | 15.10 |       21.27 |           0.14 |             1120 |           1414 |            163 | Healthcare             | INNOVATION (혁신성장)
 2026-04-21 | MU    | Micron Technology, Inc.         |      449.38 |        0.21 |  7.00 | 21.19 |        4.45 | 33.03 |      157.30 |           0.13 |            50678 |         120785 |           -379 | Technology             | INNOVATION (혁신성장)
(171 rows)


-- grok

-- claud

-- qwen

-- chatgpt

-- gemini

| grep -Ee 'trade_date|VICI|CMCSA|PRU|TROW|INGR|CI|OZK'

