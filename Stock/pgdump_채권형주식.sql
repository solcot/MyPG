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
trade_dividend numeric(10,2),
remark varchar(100),
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
    WHERE min_roe_ever >= 5.0   -- 1. 꾸준히 수익 창출하는 기업
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
SELECT  c.trade_div, c.trade_status, c.trade_expected_cagr, c.trade_dividend, remark,
        (b.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (b.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        b.sector,
        z.net_debt,
        a.* -- 이 자리에 기존 eps 컬럼과 함께 앞에서 정의한 eps_ratio가 포함되어 출력됩니다.
FROM last_data a 
JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code 
    AND ((a.expected_cagr >= 12.0 and a.fep_expected_cagr >= 10) OR (a.expected_cagr >= 8.0 and a.fep_expected_cagr >= 15))   -- 2. 저평가 종목 
join stock_debt z on a.code = z.code
FULL OUTER JOIN (SELECT * FROM mytrade WHERE trade_status = 1) c ON b.code = c.code
WHERE b.market_cap > 30000000000   -- 3. 소형주도 대상에 포함
    and b.trade_value > 100000000   -- 4. 소형주라도 최소 거래량 충족해야 함
    and a.eps_ratio > a.per   -- 5. 성장성 저평가 종목
    and (z.net_debt < 0.0 or z.net_debt = 'NaN')   -- 6. 순부채가 없는 종목 
    AND ggg_pcap_bakuk <= iii_pcap_bakuk and iii_pcap_bakuk <= kkk_pcap_bakuk    -- 7. 순자산이 증가하는 기업
    and a.dividend_yield >= 3.0   -- 8. 최소 배당 조건 만족 
    --AND a.eps_ratio < 100            -- 💡 [방어코드 추가] 1년 만에 이익이 100% 이상 폭증한 것은 일회성 기저효과일 확률이 높으므로 제외
    AND a.per > 0                    -- 💡 [방어코드 추가] 적자 기업(PER N/A 처리 등) 방지
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
   ,remark
   ,':::' div
   ,case when b.trade_div = 'bond1' then a.expected_cagr - trade_expected_cagr end bond1_diff
   ,case when b.trade_div = 'bond2' then a.fep_expected_cagr - trade_expected_cagr end bond2_diff   
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
FROM last_data a join mytrade b on a.code = b.code
where trade_status = 1
order by b.trade_div,a.code
EEOFF



#===============================================================================> bond_1 / bond_2 insert

cat > bond_1_insert.sql <<'EEOFF'
insert into mytrade
WITH calc_yearly_data AS (
    -- 1단계: 종목(code)별, 3개월 단위 평균 ROE 및 평균 PBR 계산
    SELECT 
        code,
        to_char(trade_date, 'YYYY') AS year,
        AVG(roe) AS avg_roe,
        AVG(pbr) AS avg_pbr  -- 💡 [추가] 과거 시장 평가(권리금)를 추적하기 위한 PBR 평균
    FROM stockfdt_pbr_v
    WHERE trade_date >= '20160101'
    GROUP BY code, to_char(trade_date, 'YYYY')
),
find_min_roe AS (
    -- 2단계: 최악의 ROE 찾기 및 NULL 빵꾸 이력 추적
    SELECT 
        code,
        year,
        avg_roe,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        BOOL_OR(avg_roe IS NULL) OVER (PARTITION BY code) AS has_null_roe,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr -- 💡 [추가] 10년 평균 PBR 산출
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
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, -- 💡 [추가] 다음 단계로 전달
        ROUND(avg_roe, 2) AS avg_roe
    FROM find_min_roe
    --WHERE min_roe_ever >= 5
      --AND has_null_roe = false
),
pivot_data AS (
    -- 4단계: 2015-1Q부터 2026-1Q까지 45분기 절대 시간 피벗 전개!
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr, -- 💡 [추가] 최종 계산을 위해 메인 쿼리로 전달
        -- [2016년]
        MAX(avg_roe) FILTER (WHERE year = '2016') AS "2016",
        -- [2017년]
        MAX(avg_roe) FILTER (WHERE year = '2017') AS "2017",
        -- [2018년]
        MAX(avg_roe) FILTER (WHERE year = '2018') AS "2018",
        -- [2019년]
        MAX(avg_roe) FILTER (WHERE year = '2019') AS "2019",
        -- [2020년]
        MAX(avg_roe) FILTER (WHERE year = '2020') AS "2020",
        -- [2021년]
        MAX(avg_roe) FILTER (WHERE year = '2021') AS "2021",
        -- [2022년]
        MAX(avg_roe) FILTER (WHERE year = '2022') AS "2022",
        -- [2023년]
        MAX(avg_roe) FILTER (WHERE year = '2023') AS "2023",
        -- [2024년]
        MAX(avg_roe) FILTER (WHERE year = '2024') AS "2024",
        -- [2025년]
        MAX(avg_roe) FILTER (WHERE year = '2025') AS "2025",
        -- [2026년]
        MAX(avg_roe) FILTER (WHERE year = '2026') AS "2026"
    FROM filtered_data
    GROUP BY code
),
last_data AS (
select 
    -- 💡 1. 10년 후 예상 BPS (장부 가치)
    ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
    
    -- 💡 2. 10년 후 예상 주가 (미래 BPS * 역대 평균 PBR)
    ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
    
    -- 💡 3. 10년 후 투자 승수 = (미래 예상 주가 / 현재 주가) [0 나누기 방어 추가]
    ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
    
    -- 💡 4. 최종 예상 연평균 복리 수익률 (CAGR)
    ROUND(
        (POWER(
            ((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0),  
            1.0 / 10.0                                                  
        ) - 1) * 100, 
    2) AS expected_cagr,
    
    * -- USING(code)로 합쳐진 a와 b의 모든 컬럼 (code는 단 1번만 출력됨)
from stockfdt_pbr_v a join pivot_data b USING (code)
where a.trade_date = (select max(trade_date) from stockfdt_pbr_v)
AND a.bps > 0           -- 💡 [방어코드] 자본잠식 기업 에러 방지
AND a.close_price > 0   -- 💡 [방어코드] 거래정지(0원) 에러 방지
)
select  --c.trade_status, c.trade_expected_cagr, c.trade_dividend, remark,
        --(b.trade_value::numeric / 100000000)::int AS trade_value_uk,
        --(b.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        --b.sector,
        --a.*
        a.code,'bond1','1',a.expected_cagr,a. dividend_yield,a.name || ': ' || a.close_price
from last_data a join stockmain b on a.trade_date = b.trade_date and a.code = b.code
full outer join (select * from mytrade where trade_div = 'bond1') c on b.code = c.code
where a.code in (
'023590' 
,'' 
,''
,''
,''
)
order by a.expected_cagr desc;
EEOFF



cat > bond_2_insert.sql <<'EEOFF'
insert into mytrade
WITH calc_yearly_data AS (
    -- 1단계: 종목(code)별, 3개월 단위 평균 ROE 및 평균 PBR 계산
    SELECT 
        code,
        to_char(trade_date, 'YYYY') AS year,
        AVG(roe) AS avg_roe,
        AVG(pbr) AS avg_pbr  -- 💡 [추가] 과거 시장 평가(권리금)를 추적하기 위한 PBR 평균
    FROM stockfdt_pbr_v
    WHERE trade_date >= '20160101'
    GROUP BY code, to_char(trade_date, 'YYYY')
),
find_min_roe AS (
    -- 2단계: 최악의 ROE 찾기, NULL 이력 추적 및 '역대 평균 PBR' 산출
    SELECT 
        code,
        year,
        avg_roe,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        BOOL_OR(avg_roe IS NULL) OVER (PARTITION BY code) AS has_null_roe,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr -- 💡 [추가] 10년 평균 PBR 산출
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
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, -- 💡 [추가] 다음 단계로 전달
        ROUND(avg_roe, 2) AS avg_roe
    FROM find_min_roe
    --WHERE min_roe_ever >= 5
      --AND has_null_roe = false
),
pivot_data AS (
    -- 4단계: 45분기 절대 시간 피벗 전개 및 종목별 메타데이터 집계
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr, -- 💡 [추가] 최종 계산을 위해 메인 쿼리로 전달
        -- [2016년]
        MAX(avg_roe) FILTER (WHERE year = '2016') AS "2016",
        -- [2017년]
        MAX(avg_roe) FILTER (WHERE year = '2017') AS "2017",
        -- [2018년]
        MAX(avg_roe) FILTER (WHERE year = '2018') AS "2018",
        -- [2019년]
        MAX(avg_roe) FILTER (WHERE year = '2019') AS "2019",
        -- [2020년]
        MAX(avg_roe) FILTER (WHERE year = '2020') AS "2020",
        -- [2021년]
        MAX(avg_roe) FILTER (WHERE year = '2021') AS "2021",
        -- [2022년]
        MAX(avg_roe) FILTER (WHERE year = '2022') AS "2022",
        -- [2023년]
        MAX(avg_roe) FILTER (WHERE year = '2023') AS "2023",
        -- [2024년]
        MAX(avg_roe) FILTER (WHERE year = '2024') AS "2024",
        -- [2025년]
        MAX(avg_roe) FILTER (WHERE year = '2025') AS "2025",
        -- [2026년]
        MAX(avg_roe) FILTER (WHERE year = '2026') AS "2026"
    FROM filtered_data
    GROUP BY code
),
last_data AS (
    -- 5단계: 재무 가치평가 (Valuation) 및 미래 주가 산출
    SELECT 
        -- 💡 1. 10년 후 예상 BPS (장부 가치)
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        
        -- 💡 2. 10년 후 예상 주가 (미래 BPS * 역대 평균 PBR)
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        
        -- 💡 3. 10년 후 투자 승수 = (미래 예상 주가 / 현재 주가) [0 나누기 방어 추가]
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        
        -- 💡 4. 최종 예상 연평균 복리 수익률 (CAGR)
        ROUND(
            (POWER(
                ((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0),  
                1.0 / 10.0                                                  
            ) - 1) * 100, 
        2) AS expected_cagr,
        
        * -- USING(code)로 합쳐진 a와 b의 모든 컬럼 (code는 단 1번만 출력됨)
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    WHERE a.trade_date = (SELECT MAX(trade_date) FROM stockfdt_pbr_v)
      AND a.bps > 0           -- 💡 [방어코드] 자본잠식 기업 에러 방지
      AND a.close_price > 0   -- 💡 [방어코드] 거래정지(0원) 에러 방지
)
-- 6단계: 거래대금/시가총액 조인 및 최종 결과 추출
SELECT  
    --c.trade_status, c.trade_expected_cagr, c.trade_dividend, remark,
    --(b.trade_value::numeric / 100000000)::int AS trade_value_uk,
    --(b.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
    --b.sector,
    --a.*
    a.code,'bond2','1',a.expected_cagr,a. dividend_yield,a.name || ': ' || a.close_price
from last_data a join stockmain b on a.trade_date = b.trade_date and a.code = b.code --and a.expected_cagr >= 8.0
full outer join (select * from mytrade where trade_div = 'bond2') c on b.code = c.code
where a.code in (
'337930' 
,'376180' 
,'036800' 
,'108320'
,'049720'
,'030000'
)
ORDER BY a.expected_cagr DESC;
EEOFF


