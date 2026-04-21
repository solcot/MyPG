"
drop view stockfdt_pbr_v;
CREATE VIEW public.stockfdt_pbr_v AS
 SELECT trade_date,
    code,
    name,
    change_rate,
    dividend_yield,
    pbr,
    per,
    (pbr / per * 100::numeric)::numeric(10,2) AS roe,
    forward_per,
    (pbr / forward_per * 100::numeric)::numeric(10,2) AS forward_roe,
    close_price,
    (close_price/pbr)::int bps,
    (close_price/per)::int eps
   FROM stockfdt;
"

"
CREATE TABLE public.stock_debt (
    code character varying(20) NOT NULL primary key,
    name character varying(100),
    net_debt numeric(15,2),
    created_at timestamp without time zone DEFAULT now() NOT NULL
);
"

"
drop table mytrade;
create table mytrade
(
code varchar(20) not null,
trade_div varchar(10),
trade_status smallint,
trade_expected_cagr numeric(10,2),
trade_dividend numeric(10,2)
,trade_per numeric(10,2)
,trade_eps_ratio numeric(10,2)
,trade_roe numeric(10,2)
,trade_min_roe_ever numeric(10,2)
,trade_pbr numeric(10,2)
,trade_hist_avg_pbr numeric(10,2)
,trade_close_price integer
,trade_name varchar(100)
,remark varchar(100),
created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE ONLY mytrade
    ADD CONSTRAINT mytrade_pkey PRIMARY KEY (code,created_at);

COMMENT ON COLUMN public.mytrade.trade_div IS 'bond1:bpb_1, bond2:bpr_avg';
COMMENT ON COLUMN public.mytrade.trade_status IS '0:예정, 1:매수, 2:매도';
"



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
join stock_debt z on a.code = z.code
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
   ,((1- y.close_price / b.trade_close_price) * 100)::numeric(10,2) first_profit_ratio
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
join stock_debt z on a.code = z.code
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
'023910' 
,'004590' 
,'002810' 
,'108320' 
,'049720' 
,'337930' 
,'376180' 
,'030000' 
)
EEOFF




#=========================================================================================> US

cat > bondus.sql <<'EEOFF'
WITH LatestDate AS (
    SELECT MAX(trade_date) AS max_date 
    FROM public.stockmainus
)

SELECT 
    v.trade_date,
    v.code,
    v.name,
    v.close_price,
    v.change_rate,

    -- 밸류
    v.pbr,
    v.per,
    v.forward_per,

    -- 수익성
    v.roe,
    v.forward_roe,

    -- 배당
    v.dividend_yield,

    -- 규모 / 유동성
    TRUNC(m.market_cap::numeric / 10000000) AS market_cap_bakuk,
    TRUNC(m.trade_value::numeric / 100000) AS trade_value_uk,

    -- 부채
    CASE 
        WHEN d.net_debt IS NULL OR d.net_debt = 'NaN' THEN NULL 
        ELSE TRUNC(d.net_debt::numeric / 10000000) 
    END AS net_debt_bakuk,

    m.sector,

    -- 💡 어떤 트랙으로 뽑혔는지 표시
    CASE 
        WHEN v.dividend_yield >= 3 THEN 'DIVIDEND'
        ELSE 'GROWTH'
    END AS strategy_type

FROM public.stockfdtus_pbr_v v
JOIN public.stockmainus m 
    ON v.trade_date = m.trade_date 
   AND v.code = m.code
LEFT JOIN public.stock_debtus d 
    ON v.code = d.code

WHERE v.trade_date = (SELECT max_date FROM LatestDate)

-- ✅ 1. 체급 (초우량주/대형주 위주)
AND m.market_cap >= 3000000000
AND m.trade_value >= 3000000

-- ✅ 2. 공통 저평가 기본 (극단적 수치/쓰레기 주식 배제)
AND v.pbr BETWEEN 0.5 AND 2.0
AND v.per BETWEEN 5 AND 20
AND v.eps > 0

-- ✅ 3. 공통 성장 필터
AND v.forward_per > 0
AND v.forward_roe >= 10

-- 💥 4. 핵심: 2-Track 조건 분기
AND (
    
    -- 🔵 [Track A] 배당 안정주 (흔들리지 않는 현금흐름)
    (
        v.dividend_yield BETWEEN 3 AND 5.5
        AND v.forward_per < v.per
        AND v.roe >= 10
        
        -- 배당 지속성 (번 돈의 60% 이하만 배당으로 지급 = 배당컷 위험 제로)
        AND (v.dividend_yield * v.per) <= 60
        
        -- 금융 섹터는 건전성 확인을 위해 PER을 더 엄격하게 제한
        AND (
            m.sector != 'Financial Services'
            OR v.per <= 9
        )
    )

    OR

    -- 🟢 [Track B] 성장 가치주 (자본 복리 증식)
    (
        v.dividend_yield >= 2   -- 배당은 방어력 제공용
        AND v.forward_roe >= 15 -- 폭발적인 내년 자본 수익률
        AND v.forward_per < v.per
        
        -- 성장주는 프리미엄 허용
        AND v.per <= 20
    )
)

-- ✅ 5. 부채 리스크 방어 (금융주 특수성 반영)
AND (
    m.sector IN ('Financial Services') 
    OR d.net_debt IS NULL
    OR d.net_debt = 'NaN'
    OR d.net_debt::numeric < m.market_cap * 0.6
)

-- ✅ 6. 섹터 필터 (핵심 우량 산업)
AND m.sector IN (
    'Technology',
    'Healthcare',
    'Consumer Defensive',
    'Industrials',
    'Financial Services'
)

-- 👉 경기 민감 산업 완전 제거 (Buy & Sleep 확보)
AND m.sector NOT IN ('Energy', 'Basic Materials')

-- ✅ 7. 국가/지정학적 리스크 완벽 제거 (ADR 및 VIE 지주사 필터 추가)
AND v.name NOT ILIKE ANY (ARRAY[
    '%China%', '%Hong Kong%', 
    '%Holdings Ltd%', '%Group Ltd%', '%Holdings Limited%', '%Group Limited%', 
    '%ADR%'
])

-- ✅ 8. 쓰레기 주식 및 껍데기(우선주/펀드) 제거
AND v.name NOT ILIKE ANY (ARRAY[
    '%Fund%', '%Trust%', '%ETF%', '%SPAC%', '%Acquisition%',
    '%Depositary%', '%Depository%', '%Dep Shs%',
    '%Preferred%', '%Pref%', '%Series%'
])
AND v.code NOT LIKE '%-%P%'

-- ✅ 9. 기존 보유 종목(마이 포트폴리오) 제외
AND v.code NOT IN (
    SELECT code FROM mytradeus WHERE trade_status = 1
)

ORDER BY 
    strategy_type,              -- 💡 배당(DIVIDEND) / 성장(GROWTH) 그룹별 정렬
    v.dividend_yield DESC,      -- 1순위: 배당 높은 순
    v.forward_per ASC,          -- 2순위: 내년 이익 대비 싼 순 (성장 폭이 큰 순)
    v.forward_roe DESC;         -- 3순위: 자본을 잘 굴리는 순
EEOFF



cat > bondus_mytrade.sql <<'EEOFF'
SELECT z.trade_dividend, z.trade_per, z.trade_roe, z.trade_pbr, z.trade_close_price, z.remark,
    v.trade_date,
    v.code,
    v.name,
    v.close_price,
    v.change_rate,

    -- 밸류
    v.pbr,
    v.per,
    v.forward_per,

    -- 수익성
    v.roe,
    v.forward_roe,

    -- 배당
    v.dividend_yield,

    -- 규모 / 유동성
    TRUNC(m.market_cap::numeric / 10000000) AS market_cap_bakuk,
    TRUNC(m.trade_value::numeric / 100000) AS trade_value_uk,

    -- 부채
    CASE 
        WHEN d.net_debt IS NULL OR d.net_debt = 'NaN' THEN NULL 
        ELSE TRUNC(d.net_debt::numeric / 10000000) 
    END AS net_debt_bakuk,

    m.sector,

    -- 💡 어떤 트랙으로 뽑혔는지 표시
    CASE 
        WHEN v.dividend_yield >= 3 THEN 'DIVIDEND'
        ELSE 'GROWTH'
    END AS strategy_type
FROM public.stockfdtus_pbr_v v
JOIN public.stockmainus m ON v.trade_date = m.trade_date AND v.code = m.code
LEFT JOIN public.stock_debtus d ON v.code = d.code 
join mytradeus z on v.code = z.code and z.trade_status = 1
WHERE v.trade_date = (SELECT MAX(trade_date) FROM public.stockmainus)
ORDER BY 
    v.dividend_yield DESC,     -- 1순위: 최고 배당률
    v.pbr ASC,                 -- 2순위: 가장 저평가
    v.roe DESC;                -- 3순위: 최고 수익성
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
LEFT JOIN public.stock_debtus d ON v.code = d.code 
WHERE v.trade_date = (SELECT MAX(trade_date) FROM public.stockmainus)
and v.code in (
'TROW'  
,'INGR' 
,'' 
,'' 
,'' 
,'' 
,'' 
)
EEOFF





---------------> US 기업 AI 질문
WITH LatestDate AS (
    SELECT MAX(trade_date) AS max_date 
    FROM public.stockmainus
)

SELECT 
    v.trade_date,
    v.code,
    v.name,
    v.close_price,
    v.change_rate,

    -- 밸류
    v.pbr,
    v.per,
    v.forward_per,

    -- 수익성
    v.roe,
    v.forward_roe,

    -- 배당
    v.dividend_yield,

    -- 규모 / 유동성
    TRUNC(m.market_cap::numeric / 10000000) AS market_cap_bakuk,
    TRUNC(m.trade_value::numeric / 100000) AS trade_value_uk,

    -- 부채
    CASE 
        WHEN d.net_debt IS NULL OR d.net_debt = 'NaN' THEN NULL 
        ELSE TRUNC(d.net_debt::numeric / 10000000) 
    END AS net_debt_bakuk,

    m.sector,

    -- 💡 어떤 트랙으로 뽑혔는지 표시
    CASE 
        WHEN v.dividend_yield >= 3 THEN 'DIVIDEND'
        ELSE 'GROWTH'
    END AS strategy_type

FROM public.stockfdtus_pbr_v v
JOIN public.stockmainus m 
    ON v.trade_date = m.trade_date 
   AND v.code = m.code
LEFT JOIN public.stock_debtus d 
    ON v.code = d.code

WHERE v.trade_date = (SELECT max_date FROM LatestDate)

-- ✅ 1. 체급 (초우량주/대형주 위주)
AND m.market_cap >= 3000000000
AND m.trade_value >= 3000000

-- ✅ 2. 공통 저평가 기본 (극단적 수치/쓰레기 주식 배제)
AND v.pbr BETWEEN 0.5 AND 2.0
AND v.per BETWEEN 5 AND 20
AND v.eps > 0

-- ✅ 3. 공통 성장 필터
AND v.forward_per > 0
AND v.forward_roe >= 10

-- 💥 4. 핵심: 2-Track 조건 분기
AND (
    
    -- 🔵 [Track A] 배당 안정주 (흔들리지 않는 현금흐름)
    (
        v.dividend_yield BETWEEN 3 AND 5.5
        AND v.forward_per < v.per
        AND v.roe >= 10
        
        -- 배당 지속성 (번 돈의 60% 이하만 배당으로 지급 = 배당컷 위험 제로)
        AND (v.dividend_yield * v.per) <= 60
        
        -- 금융 섹터는 건전성 확인을 위해 PER을 더 엄격하게 제한
        AND (
            m.sector != 'Financial Services'
            OR v.per <= 9
        )
    )

    OR

    -- 🟢 [Track B] 성장 가치주 (자본 복리 증식)
    (
        v.dividend_yield >= 2   -- 배당은 방어력 제공용
        AND v.forward_roe >= 15 -- 폭발적인 내년 자본 수익률
        AND v.forward_per < v.per
        
        -- 성장주는 프리미엄 허용
        AND v.per <= 20
    )
)

-- ✅ 5. 부채 리스크 방어 (금융주 특수성 반영)
AND (
    m.sector IN ('Financial Services') 
    OR d.net_debt IS NULL
    OR d.net_debt = 'NaN'
    OR d.net_debt::numeric < m.market_cap * 0.6
)

-- ✅ 6. 섹터 필터 (핵심 우량 산업)
AND m.sector IN (
    'Technology',
    'Healthcare',
    'Consumer Defensive',
    'Industrials',
    'Financial Services'
)

-- 👉 경기 민감 산업 완전 제거 (Buy & Sleep 확보)
AND m.sector NOT IN ('Energy', 'Basic Materials')

-- ✅ 7. 국가/지정학적 리스크 완벽 제거 (ADR 및 VIE 지주사 필터 추가)
AND v.name NOT ILIKE ANY (ARRAY[
    '%China%', '%Hong Kong%', 
    '%Holdings Ltd%', '%Group Ltd%', '%Holdings Limited%', '%Group Limited%', 
    '%ADR%'
])

-- ✅ 8. 쓰레기 주식 및 껍데기(우선주/펀드) 제거
AND v.name NOT ILIKE ANY (ARRAY[
    '%Fund%', '%Trust%', '%ETF%', '%SPAC%', '%Acquisition%',
    '%Depositary%', '%Depository%', '%Dep Shs%',
    '%Preferred%', '%Pref%', '%Series%'
])
AND v.code NOT LIKE '%-%P%'

-- ✅ 9. 기존 보유 종목(마이 포트폴리오) 제외
AND v.code NOT IN (
    SELECT code FROM mytradeus WHERE trade_status = 1
)

ORDER BY 
    strategy_type,              -- 💡 배당(DIVIDEND) / 성장(GROWTH) 그룹별 정렬
    v.dividend_yield DESC,      -- 1순위: 배당 높은 순
    v.forward_per ASC,          -- 2순위: 내년 이익 대비 싼 순 (성장 폭이 큰 순)
    v.forward_roe DESC;         -- 3순위: 자본을 잘 굴리는 순



위 쿼리는 미국에 상장된 기업중 저평가된 가치주를 선별하기 위해 만든 쿼리인데...
위 쿼리 수행 결과가 아래와 같거든...
여기서 다른 기업 정보 
즉 roe나 배당이 꾸준히 증가하는지? 기업에 해자가 있는지?
등도 추가로 파악해서...
안정적으로 배당을 받으면서 주가도 우상향할 수 있는 오래 보유할 가치주 TOP3 선별해줘...
만약 리스트된 종목 모두 장기 보유에 적합한 가치주가 아니라면 솔직하게 모두 적당하지 않다고 답변해줘...



 trade_date | code |              name               | close_price | change_rate | pbr  |  per  | forward_per |  roe  | forward_roe | dividend_yield | market_cap_bakuk | trade_value_uk | net_debt_bakuk |       sector       | strategy_type
------------+------+---------------------------------+-------------+-------------+------+-------+-------------+-------+-------------+----------------+------------------+----------------+----------------+--------------------+---------------
 2026-04-20 | PAGS | PagSeguro Digital Ltd.          |       11.32 |        0.53 | 1.08 |  7.97 |        5.83 | 13.55 |       18.52 |           9.19 |              316 |            327 |             19 | Technology         | DIVIDEND
 2026-04-20 | AEG  | Aegon Ltd. New York Registry Sh |        8.06 |       -0.62 | 1.37 | 11.35 |        8.70 | 12.07 |       15.75 |           5.83 |             1220 |            443 |            480 | Financial Services | DIVIDEND
 2026-04-20 | CPA  | Copa Holdings, S.A.             |      125.30 |       -0.22 | 1.86 |  7.70 |        6.45 | 24.16 |       28.84 |           5.46 |              515 |            391 |             96 | Industrials        | DIVIDEND
 2026-04-20 | NWG  | NatWest Group plc               |       16.53 |       -2.19 | 1.29 |  9.03 |        7.71 | 14.29 |       16.73 |           5.32 |             6588 |            965 |          -7259 | Financial Services | DIVIDEND
 2026-04-20 | TROW | T. Rowe Price Group, Inc.       |       98.10 |        1.15 | 1.97 | 10.62 |       10.26 | 18.55 |       19.20 |           5.30 |             2139 |           1590 |           -290 | Financial Services | DIVIDEND
 2026-04-20 | BBD  | Banco Bradesco Sa               |        4.19 |       -0.48 | 1.24 |  9.74 |        7.87 | 12.73 |       15.76 |           5.01 |             4429 |           1034 |          54581 | Financial Services | DIVIDEND
 2026-04-20 | LNC  | Lincoln National Corporation    |       37.13 |        0.65 | 0.71 |  6.37 |        4.40 | 11.15 |       16.14 |           4.85 |              709 |            463 |          -3800 | Financial Services | DIVIDEND
 2026-04-20 | BBVA | Banco Bilbao Vizcaya Argentaria |       23.20 |       -2.73 | 1.95 | 11.15 |       10.35 | 17.49 |       18.84 |           4.66 |            13070 |            519 |            424 | Financial Services | DIVIDEND
 2026-04-20 | SFD  | Smithfield Foods, Inc.          |       28.56 |       -1.52 | 1.65 | 11.38 |       10.92 | 14.50 |       15.11 |           4.38 |             1123 |            596 |             85 | Consumer Defensive | DIVIDEND
 2026-04-20 | OZK  | Bank OZK                        |       49.24 |        1.05 | 0.94 |  7.97 |        7.65 | 11.79 |       12.29 |           3.70 |              550 |            789 |           -219 | Financial Services | DIVIDEND
 2026-04-20 | FG   | F&G Annuities & Life, Inc.      |       27.07 |       -0.15 | 0.76 | 14.40 |        4.47 |  5.28 |       17.00 |           3.69 |              367 |            101 |            -34 | Financial Services | DIVIDEND
 2026-04-20 | IFS  | Intercorp Financial Services In |       48.99 |       -0.41 | 1.52 | 10.00 |        8.34 | 15.20 |       18.23 |           3.67 |              544 |            169 |            860 | Financial Services | DIVIDEND
 2026-04-20 | USB  | U.S. Bancorp                    |       57.00 |        0.12 | 1.52 | 11.95 |       10.12 | 12.72 |       15.02 |           3.61 |             8863 |           4723 |           3079 | Financial Services | DIVIDEND
 2026-04-20 | FBP  | First BanCorp. New              |       23.53 |        1.29 | 1.86 | 10.94 |       10.05 | 17.00 |       18.51 |           3.40 |              368 |            189 |            -29 | Financial Services | DIVIDEND
 2026-04-20 | MFC  | Manulife Financial Corporation  |       38.56 |       -0.64 | 1.83 | 17.21 |       10.73 | 10.63 |       17.05 |           3.40 |             6448 |            569 |           -673 | Financial Services | DIVIDEND
 2026-04-20 | KFY  | Korn Ferry                      |       66.63 |        1.32 | 1.71 | 13.22 |       11.56 | 12.93 |       14.79 |           3.30 |              347 |            162 |            -39 | Industrials        | DIVIDEND
 2026-04-20 | PFG  | Principal Financial Group Inc   |       96.19 |        0.02 | 1.76 | 18.32 |        9.45 |  9.61 |       18.62 |           3.25 |             2085 |           1145 |           -136 | Financial Services | DIVIDEND
 2026-04-20 | IX   | ORIX Corporation                |       31.87 |       -1.12 | 1.22 | 12.16 |        2.71 | 10.03 |       45.02 |           3.14 |             3500 |             79 |         540635 | Financial Services | DIVIDEND
 2026-04-20 | FITB | Fifth Third Bancorp             |       50.98 |        1.27 | 1.69 | 17.16 |       10.39 |  9.85 |       16.27 |           3.14 |             4617 |           3500 |           1083 | Financial Services | DIVIDEND
 2026-04-20 | MET  | MetLife, Inc.                   |       77.70 |       -0.49 | 1.79 | 16.50 |        7.10 | 10.85 |       25.21 |           2.92 |             5119 |           1443 |           3972 | Financial Services | GROWTH
 2026-04-20 | RDN  | Radian Group Inc.               |       35.25 |       -0.34 | 1.00 |  8.03 |        6.55 | 12.45 |       15.27 |           2.89 |              480 |            142 |            -43 | Financial Services | GROWTH
 2026-04-20 | INGR | Ingredion Incorporated          |      114.21 |       -0.90 | 1.68 | 10.22 |        9.55 | 16.44 |       17.59 |           2.87 |              720 |            462 |             93 | Consumer Defensive | GROWTH
 2026-04-20 | EWBC | East West Bancorp, Inc.         |      119.09 |        0.91 | 1.84 | 12.51 |       10.80 | 14.71 |       17.04 |           2.69 |             1638 |           1131 |           -132 | Financial Services | GROWTH
 2026-04-20 | VCTR | Victory Capital Holdings, Inc.  |       74.53 |        0.89 | 1.97 | 18.27 |       10.00 | 10.78 |       19.70 |           2.63 |              477 |            352 |             85 | Financial Services | GROWTH
 2026-04-20 | ALLY | Ally Financial Inc.             |       46.30 |        2.07 | 1.08 | 19.54 |        7.18 |  5.53 |       15.04 |           2.59 |             1431 |           1578 |           1185 | Financial Services | GROWTH
 2026-04-20 | VOYA | Voya Financial, Inc.            |       75.50 |        1.41 | 1.43 | 12.00 |        6.79 | 11.92 |       21.06 |           2.44 |              700 |            673 |            329 | Financial Services | GROWTH
 2026-04-20 | EG   | Everest Group, Ltd.             |      350.64 |       -0.24 | 0.92 |  9.28 |        5.75 |  9.91 |       16.00 |           2.28 |             1416 |           1091 |            -52 | Financial Services | GROWTH
 2026-04-20 | STT  | State Street Corporation        |      150.18 |        3.27 | 1.73 | 15.25 |       10.99 | 11.34 |       15.74 |           2.24 |             4185 |           4575 |                | Financial Services | GROWTH
 2026-04-20 | CI   | The Cigna Group                 |      279.92 |        0.46 | 1.77 | 12.62 |        8.37 | 14.03 |       21.15 |           2.23 |             7477 |           2928 |           2273 | Healthcare         | GROWTH
 2026-04-20 | SAN  | Banco Santander, S.A. Sponsored |       12.68 |       -1.55 | 1.56 | 12.81 |        9.65 | 12.18 |       16.17 |           2.21 |            18388 |            762 |          -3098 | Financial Services | GROWTH
 2026-04-20 | CTSH | Cognizant Technology Solutions  |       60.26 |       -1.70 | 1.92 | 13.21 |        9.83 | 14.53 |       19.53 |           2.19 |             2908 |           4581 |            -74 | Technology         | GROWTH
 2026-04-20 | CBSH | Commerce Bancshares, Inc.       |       51.40 |        0.53 | 1.87 | 12.72 |       11.93 | 14.70 |       15.67 |           2.14 |              757 |            729 |           -143 | Financial Services | GROWTH
 2026-04-20 | WAL  | Western Alliance Bancorporation |       79.45 |        0.08 | 1.17 |  9.10 |        6.76 | 12.86 |       17.31 |           2.11 |              874 |            656 |            242 | Financial Services | GROWTH
 2026-04-20 | THG  | Hanover Insurance Group Inc     |      179.99 |       -1.02 | 1.78 |  9.94 |        9.91 | 17.91 |       17.96 |           2.11 |              633 |            288 |              9 | Financial Services | GROWTH
 2026-04-20 | BPOP | Popular, Inc.                   |      148.07 |        0.80 | 1.56 | 12.04 |        9.19 | 12.96 |       16.97 |           2.03 |              963 |            679 |           -348 | Financial Services | GROWTH
(35 rows)


-- grok
1. TROW - T. Rowe Price Group, Inc. (Financial Services, market_cap ≈ $21.39B, DIVIDEND track)
2. INGR - Ingredion Incorporated (Consumer Defensive, market_cap ≈ $7.2B, GROWTH track)
3. CI - The Cigna Group (Healthcare, market_cap ≈ $74.77B, GROWTH track)

-- claud


-- qwen
🥇 1위: **U.S. Bancorp **(USB) - 미국 지역 은행
🥈 2위: **T. Rowe Price **(TROW) - 자산운용사
🥉 3위: **Ingredion **(INGR) - 식품 소재 기업

-- chatgpt
TROW → 40% (핵심)
CTSH → 30% (성장)
INGR → 30% (방어)

-- gemini
🥇 1. 티 로우 프라이스 그룹 (TROW - T. Rowe Price Group)
🥈 2. 더 시그나 그룹 (CI - The Cigna Group)
🥉 3. 뱅크 오즈케이 (OZK - Bank OZK)

| grep -Ee 'trade_date|VICI|CMCSA|PRU|TROW|INGR'
