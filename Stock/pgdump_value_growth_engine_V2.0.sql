ace 구글밸류체인엑티브
plus 글로벌휴머노이드로봇액티브
koact 글로벌친환경전력인프라액티브
time 글로벌우주테크&방산액티브
koact 글로벌AI메모리반도체액티브

*******************************************************************************
[트랙 A] 가치주 퀀트 시스템 (Value Engine v2.1)
*******************************************************************************
목표: 하락장 방어, 딥밸류(Deep Value) 포착, 배당을 통한 복리 재투자

Phase 1. 신규 매수 (Entry)
- 사용 쿼리: value_engine.sql
- 액션: 쿼리에 등장한 종목을 선정하여 1차 매수 (예: 500만 원)

Phase 2. 물타기 방어선 (Scale-In)
- 조건: 내 평단가 대비 -15% / -30% 하락 시
- 사용 쿼리: value_engine.sql (재조회)
- 액션: 쿼리에 '여전히 존재하면' 추가 매수. 만약 사라졌다면 물타기 즉시 중단.

Phase 3. 수익 실현 (Take Profit)
- 조건: 계산된 기대수익률(CAGR) 도달 시
- 액션: 쿼리 조회 생략. 기계적 분할 매도.
  > 1차 익절: CAGR 12% (또는 FEP 17%) 도달 시 1/3 매도
  > 2차 익절: CAGR 11% (또는 FEP 16%) 도달 시 남은 물량 1/2 매도
  > 전량 익절: CAGR 10% (또는 FEP 15%) 도달 시 전량 청산

Phase 4. 펀더멘털 손절 (Stop Loss)
- 사용 쿼리: value_holding_monitor.sql (실적 시즌 주기적 확인)
- 액션: 보유 종목이 모니터 쿼리에서 '사라지면(적자 전환 등)' 
  > 수익/손실 무관하게 현재가 전량 즉시 매도 (방출)

Phase 5. 시간 통제 (Time Stop)
- 액션: 최초 매수일로부터 1.5년 ~ 2년 경과 시점까지 +7% 이상 수익이 없다면 
  > 펀더멘털 무관 전량 매도 후 자본 재배치
*******************************************************************************    



#-- 1. 꾸준히 수익 창출하는 기업 (과거 ROE 기반)
#-- 2. 저평가 종목 (기대 CAGR 기반)
#-- 3. 소형주도 대상에 포함 하되
#-- 4. 소형주라도 최소 거래량 충족 (유동성 방어)
#-- 5. 성장성 저평가 종목 (1년 전 대비 EPS 성장)
#-- 6. [NEW] 완벽한 무차입 경영 (현금이 부채보다 많음)
#-- 7. BPS 가 계속적으로 증가하는 기업
#-- 8. 최소 배당 조건 만족 (주주 환원)
#-- 9. [NEW] 본업의 질적 우수성 (영업이익률 5% 이상 및 순이익 흑자)
cat > value_engine.sql <<'EEOFF'
WITH max_date_cte AS (
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    SELECT 
        code,
        (AVG(eps))::numeric(10,2) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        -- 기존: avg(b.market_cap/a.pbr)::bigint AS avg_market_cap,
        -- 변경: 시가총액 대신 진짜 자산인 BPS(주당순자산)를 계산합니다.
        (AVG(a.bps))::bigint AS avg_bps,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a 
    JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    SELECT 
        code,
        year,
        avg_roe,
        avg_bps,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        avg_bps AS avg_bps,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    WHERE avg_roe_ever >= 10.0 AND min_roe_ever >= 0.0   -- 1. 꾸준히 수익 창출하는 기업
),
pivot_data AS (
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,
        -- 기존 시가총액 피벗 삭제 후 BPS 피벗으로 교체
        MAX(avg_bps) FILTER (WHERE year = '2022') AS ggg_bps,
        MAX(avg_bps) FILTER (WHERE year = '2024') AS iii_bps,
        MAX(avg_bps) FILTER (WHERE year = '2026') AS kkk_bps
    FROM filtered_data
    GROUP BY code
),
-- 💡 [NEW] 재무 데이터(stock_debt)의 가장 최근 날짜 스냅샷만 추출
latest_debt_cte AS (
    SELECT *
    FROM stock_debt
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_debt)
),
last_data AS (
    SELECT 
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS expected_cagr,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS fep_expected_cagr,
        *
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
SELECT  
    (m.trade_value::numeric / 10000000)::int AS trade_value_chunman,
    (m.market_cap::numeric / 10000000000)::int AS market_cap_bakuk,     
    m.sector,
    -- 💡 [NEW] 재무제표 출력 항목 추가
    d.revenue AS 매출액,
    d.operating_income AS 영업이익,
    ROUND((d.operating_income / NULLIF(d.revenue, 0)) * 100, 2) AS 영업이익률_pct,
    d.net_debt AS 순부채,
    a.* 
FROM last_data a 
JOIN stockmain m ON a.trade_date = m.trade_date AND a.code = m.code 
-- 💡 [NEW] 실적 데이터(INNER JOIN을 통해 재무 데이터가 있는 확실한 기업만 필터링)
JOIN latest_debt_cte d ON a.code = d.code
WHERE 1=1
    -- 2. 저평가 종목 
    AND (a.expected_cagr >= 15.0 OR a.fep_expected_cagr >= 20.0)
    -- 3, 4. 유동성 및 규모 하한선 (시장 소외주 방어)
    AND m.market_cap > 100000000000   
    AND m.trade_value > 1000000000    
    -- 5. 성장성 검증
    AND a.eps_ratio > a.per AND a.eps_ratio < 100.0   
    -- 7. BPS 성장 검증
    AND ggg_bps <= iii_bps AND iii_bps <= kkk_bps
    -- 8. 배당수익률 최소 3% 이상
    AND a.dividend_yield >= 3.0   
    -- 기타 방어 코드
    AND (a.per > 0 AND a.per < 15.0)   

    --------------------------------------------------------------------------------
    -- 🚀 [NEW] stock_debt 테이블을 활용한 퀄리티 펀더멘털 필터 3종 세트
    --------------------------------------------------------------------------------
    -- 9-1. 장사를 해서 돈을 벌고 있는가? (일회성 이익 속임수 방어)
    AND d.operating_income > 0
    AND d.net_income > 0 
    -- 9-2. 경제적 해자가 존재하는가? (영업이익률 최소 5% 이상)
    AND (d.operating_income / NULLIF(d.revenue, 0)) >= 0.05
    -- 6. 진짜 현금 부자인가? (보유 현금이 이자 발생 부채보다 많아야 함)
    AND (d.net_debt <= 0)
    --------------------------------------------------------------------------------

ORDER BY a.expected_cagr DESC;
EEOFF



cat > value_engine_insert.sql <<'EEOFF'
insert into mytrade
WITH max_date_cte AS (
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    SELECT 
        code,
        (AVG(eps))::numeric(10,2) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        -- 기존: avg(b.market_cap/a.pbr)::bigint AS avg_market_cap,
        -- 변경: 시가총액 대신 진짜 자산인 BPS(주당순자산)를 계산합니다.
        (AVG(a.bps))::bigint AS avg_bps,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a 
    JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    SELECT 
        code,
        year,
        avg_roe,
        avg_bps,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        avg_bps AS avg_bps,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    WHERE avg_roe_ever >= 10.0 AND min_roe_ever >= 0.0   -- 1. 꾸준히 수익 창출하는 기업
),
pivot_data AS (
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,
        -- 기존 시가총액 피벗 삭제 후 BPS 피벗으로 교체
        MAX(avg_bps) FILTER (WHERE year = '2022') AS ggg_bps,
        MAX(avg_bps) FILTER (WHERE year = '2024') AS iii_bps,
        MAX(avg_bps) FILTER (WHERE year = '2026') AS kkk_bps
    FROM filtered_data
    GROUP BY code
),
-- 💡 [NEW] 재무 데이터(stock_debt)의 가장 최근 날짜 스냅샷만 추출
latest_debt_cte AS (
    SELECT *
    FROM stock_debt
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_debt)
),
last_data AS (
    SELECT 
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS expected_cagr,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS fep_expected_cagr,
        *
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
select  a.code,
        d.revenue AS 매출액,
        d.operating_income AS 영업이익,
        ROUND((d.operating_income / NULLIF(d.revenue, 0)) * 100, 2) AS 영업이익률_pct,
        d.net_debt AS 순부채,
        CASE 
            WHEN a.expected_cagr >= 15.0 THEN 'bond' 
            WHEN a.fep_expected_cagr >= 20.0 THEN 'value'
        END as div,
        '1' as status,
        CASE 
            WHEN a.expected_cagr >= 15.0 THEN a.expected_cagr 
            WHEN a.fep_expected_cagr >= 20.0 THEN a.fep_expected_cagr
        END as cagr,
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
FROM last_data a 
JOIN stockmain m ON a.trade_date = m.trade_date AND a.code = m.code 
-- 💡 [NEW] 실적 데이터(INNER JOIN을 통해 재무 데이터가 있는 확실한 기업만 필터링)
JOIN latest_debt_cte d ON a.code = d.code
where a.code in (
 '023590'
,''
,''
,''
,''
,''
)
EEOFF




#-- [value_holding_monitor.sql] 가치주 보유 종목 감시 엔진
#-- 가격(PER, CAGR) 조건은 제외하고, 회사의 펀더멘털(실적, 재무) 훼손 여부만 감시
cat > value_holding_monitor.sql <<'EEOFF'
WITH max_date_cte AS (
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    SELECT 
        code,
        (AVG(eps))::numeric(10,2) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        -- 기존: avg(b.market_cap/a.pbr)::bigint AS avg_market_cap,
        -- 변경: 시가총액 대신 진짜 자산인 BPS(주당순자산)를 계산합니다.
        (AVG(a.bps))::bigint AS avg_bps,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a 
    JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    SELECT 
        code,
        year,
        avg_roe,
        avg_bps,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        avg_bps AS avg_bps,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    WHERE avg_roe_ever >= 10.0 AND min_roe_ever >= 0.0   -- 1. 꾸준히 수익 창출하는 기업
),
pivot_data AS (
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,
        -- 기존 시가총액 피벗 삭제 후 BPS 피벗으로 교체
        MAX(avg_bps) FILTER (WHERE year = '2022') AS ggg_bps,
        MAX(avg_bps) FILTER (WHERE year = '2024') AS iii_bps,
        MAX(avg_bps) FILTER (WHERE year = '2026') AS kkk_bps
    FROM filtered_data
    GROUP BY code
),
-- 💡 [NEW] 재무 데이터(stock_debt)의 가장 최근 날짜 스냅샷만 추출
latest_debt_cte AS (
    SELECT *
    FROM stock_debt
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_debt)
),
last_data AS (
    SELECT 
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS expected_cagr,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS fep_expected_cagr,
        *
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
SELECT  
    (m.trade_value::numeric / 10000000)::int AS trade_value_chunman,
    (m.market_cap::numeric / 10000000000)::int AS market_cap_bakuk,     
    m.sector,
    -- 💡 [NEW] 재무제표 출력 항목 추가
    d.revenue AS 매출액,
    d.operating_income AS 영업이익,
    ROUND((d.operating_income / NULLIF(d.revenue, 0)) * 100, 2) AS 영업이익률_pct,
    d.net_debt AS 순부채,
    a.* 
FROM last_data a 
JOIN stockmain m ON a.trade_date = m.trade_date AND a.code = m.code 
-- 💡 [NEW] 실적 데이터(INNER JOIN을 통해 재무 데이터가 있는 확실한 기업만 필터링)
JOIN latest_debt_cte d ON a.code = d.code
WHERE 1=1
    -- 3, 4. 유동성 및 규모 하한선 (시장 소외주 방어)
    AND m.market_cap > 100000000000   
    AND m.trade_value > 1000000000      
    -- 7. BPS 성장 검증
    AND ggg_bps <= iii_bps AND iii_bps <= kkk_bps   

    --------------------------------------------------------------------------------
    -- 🚀 [NEW] stock_debt 테이블을 활용한 퀄리티 펀더멘털 필터 3종 세트
    --------------------------------------------------------------------------------
    -- 9-1. 장사를 해서 돈을 벌고 있는가? (일회성 이익 속임수 방어)
    AND d.operating_income > 0
    AND d.net_income > 0 
    -- 9-2. 경제적 해자가 존재하는가? (영업이익률 최소 5% 이상)
    AND (d.operating_income / NULLIF(d.revenue, 0)) >= 0.05
    -- 6. 진짜 현금 부자인가? (보유 현금이 이자 발생 부채보다 많아야 함)
    AND (d.net_debt <= 0)
    --------------------------------------------------------------------------------

ORDER BY a.expected_cagr DESC;
EEOFF




*******************************************************************************
[트랙 B] 성장주 퀀트 시스템 (Growth Engine v2.1)
*******************************************************************************
목표: 상승장 아웃퍼폼, 폭발적 이익 모멘텀 탑승, 자본 차익 극대화

Phase 1. 신규 매수 (Entry)
- 사용 쿼리: growth_engine.sql
- 액션: 쿼리에 등장한 종목을 선정하여 1차 매수 (정해진 예산 분배)

Phase 2. 물타기 방어선 (Scale-In)
- 조건: 내 평단가 대비 -15% / -30% 하락 시
- 사용 쿼리: growth_engine.sql (재조회)
- 액션: 쿼리에 '여전히 존재하면' 기계적 2차, 3차 매수. 만약 사라졌다면 즉시 중단.

Phase 3. 수익 실현 (Take Profit)
- 조건: 내 평단가 대비 단순 수익률 도달 시
- 액션: 쿼리 조회 생략. 대시세를 먹기 위해 기계적 분할 매도.
  > 1차 익절: +50% 도달 시 1/3 매도
  > 2차 익절: +100% 도달 시 남은 물량 1/2 매도 (이후 끝까지 추세 추종)

Phase 4. 펀더멘털 손절 (Stop Loss)
- 사용 쿼리: growth_holding_monitor.sql (실적 시즌 주기적 확인)
- 액션: 보유 종목이 모니터 쿼리에서 '사라지면(EPS 성장 꺾임 등)' 
  > 수익/손실 무관하게 현재가 전량 즉시 매도 (방출)

Phase 5. 시간 통제 (Time Stop)
- 액션: 최초 매수일로부터 1년 ~ 1.5년 경과 시점까지 +10% 이상 수익이 없다면 
  > 펀더멘털 무관 전량 매도 후 자본 재배치 (성장주는 반응이 느리면 즉시 교체)       
*******************************************************************************



#-- 1. [상향] 자본 효율성 극대화 (평균 ROE 12% 이상)
#-- 2. [완화] 성장 프리미엄 용인 (기대 CAGR 8% 이상)
#-- 3. [유지] 소형주도 대상에 포함 (시총 1,000억 이상)
#-- 4. [완화] 성장하는 강소기업 포착 (거래대금 10억 이상)
#-- 5. [핵심] 폭발적 실적 성장 (1년 전 대비 EPS 15% 이상 성장)
#-- 6. [완화] 합리적 레버리지 용인 (순부채가 시총의 20% 이하)
#-- 7. [유지] BPS가 계속적으로 증가하는 기업
#-- 8. [삭제] 배당 조건 전면 해제 (재투자 기업 선호)
#-- 9. [상향] 강력한 경제적 해자 (영업이익률 8% 이상)
cat > growth_engine.sql <<'EEOFF'
WITH max_date_cte AS (
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    SELECT 
        code,
        (AVG(eps))::numeric(10,2) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        (AVG(a.bps))::bigint AS avg_bps,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a 
    JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    SELECT 
        code,
        year,
        avg_roe,
        avg_bps,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        avg_bps AS avg_bps,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    WHERE avg_roe_ever >= 12.0 AND min_roe_ever >= 0.0   -- [상향] 성장주 기준: 평균 자본효율성(ROE) 12% 이상
),
pivot_data AS (
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,
        MAX(avg_bps) FILTER (WHERE year = '2022') AS ggg_bps,
        MAX(avg_bps) FILTER (WHERE year = '2024') AS iii_bps,
        MAX(avg_bps) FILTER (WHERE year = '2026') AS kkk_bps
    FROM filtered_data
    GROUP BY code
),
latest_debt_cte AS (
    SELECT *
    FROM stock_debt
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_debt)
),
last_data AS (
    SELECT 
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS expected_cagr,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS fep_expected_cagr,
        *
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
SELECT  
    (m.trade_value::numeric / 10000000)::int AS trade_value_chunman,
    (m.market_cap::numeric / 10000000000)::int AS market_cap_bakuk,     
    m.sector,
    d.revenue AS 매출액,
    d.operating_income AS 영업이익,
    ROUND((d.operating_income / NULLIF(d.revenue, 0)) * 100, 2) AS 영업이익률_pct,
    d.net_debt AS 순부채,
    a.* 
FROM last_data a 
JOIN stockmain m ON a.trade_date = m.trade_date AND a.code = m.code 
JOIN latest_debt_cte d ON a.code = d.code
WHERE 1=1
    -- 2. [완화] 저평가 기준: 성장주의 프리미엄 가격 반영 
    AND (a.expected_cagr >= 8.0 OR a.fep_expected_cagr >= 12.0)
    
    -- 3, 4. [유지/미세조정] 규모 및 유동성 조건 (거래대금 하한선을 10억으로 소폭 낮춰 폭발력 있는 중소형주 포착)
    AND m.market_cap > 100000000000   
    AND m.trade_value > 1000000000    
    
    -- 5. [핵심] 강력한 실적 모멘텀: 1년 전 대비 이익성장률 15% 이상 필수 검증
    AND a.eps_ratio >= 15.0 AND a.eps_ratio < 150.0   
    
    -- 7. [유지] 순자산(BPS) 우상향 기조 검증
    AND a.ggg_bps <= a.iii_bps AND a.iii_bps <= a.kkk_bps
    
    -- 8. [삭제] 배당 조건 삭제: 성장주는 버는 돈을 배당 대신 R&D와 시설 투자에 재투자해야 함
    AND a.dividend_yield >= 0.0   
    
    -- [성장주 핵심] 밸류에이션 상한선 확장: 시장에서 성장 프리미엄을 받는 고멀티플 용인 (PER 15 -> 35로 상향)
    AND (a.per > 0 AND a.per < 35.0)   

    --------------------------------------------------------------------------------
    -- 🚀 [NEW] stock_debt 테이블을 활용한 질적 우수성 조건 재설계
    --------------------------------------------------------------------------------
    -- 9-1. 본업 흑자 유지
    AND d.operating_income > 0
    AND d.net_income > 0 
    
    -- 9-2. [상향] 경제적 해자 강화: 독점적 지위나 가격 결정력이 있는 고마진 기업 (영업이익률 5% -> 8%로 상향)
    AND (d.operating_income / NULLIF(d.revenue, 0)) >= 0.08
    
    -- 6. [완화] 무차입 경영 조건 완화: 빚을 내서 효율적인 투자를 하는 합리적 레버리지 용인 (순부채 시가총액의 20% 이하)
    AND (d.net_debt <= m.market_cap * 0.2)
    --------------------------------------------------------------------------------

ORDER BY a.eps_ratio DESC; -- 기대 CAGR 대신 이익 성장률(Momentum)이 높은 순으로 정렬
EEOFF



cat > growth_engine_insert.sql <<'EEOFF'
insert into mytrade
WITH max_date_cte AS (
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    SELECT 
        code,
        (AVG(eps))::numeric(10,2) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        -- 기존: avg(b.market_cap/a.pbr)::bigint AS avg_market_cap,
        -- 변경: 시가총액 대신 진짜 자산인 BPS(주당순자산)를 계산합니다.
        (AVG(a.bps))::bigint AS avg_bps,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a 
    JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    SELECT 
        code,
        year,
        avg_roe,
        avg_bps,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        avg_bps AS avg_bps,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    WHERE avg_roe_ever >= 10.0 AND min_roe_ever >= 0.0   -- 1. 꾸준히 수익 창출하는 기업
),
pivot_data AS (
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,
        -- 기존 시가총액 피벗 삭제 후 BPS 피벗으로 교체
        MAX(avg_bps) FILTER (WHERE year = '2022') AS ggg_bps,
        MAX(avg_bps) FILTER (WHERE year = '2024') AS iii_bps,
        MAX(avg_bps) FILTER (WHERE year = '2026') AS kkk_bps
    FROM filtered_data
    GROUP BY code
),
-- 💡 [NEW] 재무 데이터(stock_debt)의 가장 최근 날짜 스냅샷만 추출
latest_debt_cte AS (
    SELECT *
    FROM stock_debt
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_debt)
),
last_data AS (
    SELECT 
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS expected_cagr,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS fep_expected_cagr,
        *
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
select  a.code,
        d.revenue AS 매출액,
        d.operating_income AS 영업이익,
        ROUND((d.operating_income / NULLIF(d.revenue, 0)) * 100, 2) AS 영업이익률_pct,
        d.net_debt AS 순부채,
        'growth' as div,
        '1' as status,
        a.fep_expected_cagr as cagr,
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
FROM last_data a 
JOIN stockmain m ON a.trade_date = m.trade_date AND a.code = m.code 
-- 💡 [NEW] 실적 데이터(INNER JOIN을 통해 재무 데이터가 있는 확실한 기업만 필터링)
JOIN latest_debt_cte d ON a.code = d.code
where a.code in (
 '272110'
,'206650'
,'035900'
,'353810'
,'200670'
,''
)
EEOFF




#-- 1. [상향] 자본 효율성 극대화 (평균 ROE 12% 이상)
#-- 3. [유지] 소형주도 대상에 포함 (시총 1,000억 이상)
#-- 4. [완화] 거래대금 10억 이상
#-- 5. [핵심] 폭발적 실적 성장 유지 검증 (1년 전 대비 EPS 15% 이상 성장)
#-- 6. [완화] 합리적 레버리지 유지 검증 (순부채 시총 20% 이하)
#-- 7. [유지] BPS 우상향
#-- 9. [상향] 강력한 경제적 해자 유지 검증 (영업이익률 8% 이상)
cat > growth_holding_monitor.sql <<'EEOFF'
WITH max_date_cte AS (
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    SELECT 
        code,
        (AVG(eps))::numeric(10,2) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        (AVG(a.bps))::bigint AS avg_bps,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a 
    JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    SELECT 
        code,
        year,
        avg_roe,
        avg_bps,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        avg_bps AS avg_bps,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    WHERE avg_roe_ever >= 12.0 AND min_roe_ever >= 0.0   
),
pivot_data AS (
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,
        MAX(avg_bps) FILTER (WHERE year = '2022') AS ggg_bps,
        MAX(avg_bps) FILTER (WHERE year = '2024') AS iii_bps,
        MAX(avg_bps) FILTER (WHERE year = '2026') AS kkk_bps
    FROM filtered_data
    GROUP BY code
),
latest_debt_cte AS (
    SELECT *
    FROM stock_debt
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_debt)
),
last_data AS (
    SELECT 
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS expected_cagr,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS fep_expected_cagr,
        *
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
SELECT  
    (m.trade_value::numeric / 10000000)::int AS trade_value_chunman,
    (m.market_cap::numeric / 10000000000)::int AS market_cap_bakuk,     
    m.sector,
    d.revenue AS 매출액,
    d.operating_income AS 영업이익,
    ROUND((d.operating_income / NULLIF(d.revenue, 0)) * 100, 2) AS 영업이익률_pct,
    d.net_debt AS 순부채,
    a.* FROM last_data a 
JOIN stockmain m ON a.trade_date = m.trade_date AND a.code = m.code 
JOIN latest_debt_cte d ON a.code = d.code
WHERE 1=1
    --------------------------------------------------------------------------------
    -- 🚨 [삭제] 밸류에이션(가격) 상한선 전면 삭제
    -- 이유: 보유 종목은 주가가 폭등하여 고평가(PER 35 초과, CAGR 8% 미달)가 되더라도
    -- 대시세를 즐겨야 하므로 가격 때문에 탈락시키지 않음
    -- AND (a.expected_cagr >= 8.0 OR a.fep_expected_cagr >= 12.0)  <-- 주석 처리
    -- AND (a.per > 0 AND a.per < 35.0)                             <-- 주석 처리
    --------------------------------------------------------------------------------
    
    -- 3, 4. 규모 및 유동성 조건 (유지)
    AND m.market_cap > 100000000000   
    AND m.trade_value > 1000000000    
    
    -- 5. [핵심] 실적 모멘텀 유지 검증
    -- 분기 실적 발표 후 성장이 꺾이면(15% 미만) 여기서 가차 없이 탈락됨
    AND a.eps_ratio >= 15.0 AND a.eps_ratio < 150.0   
    
    -- 7. 순자산(BPS) 우상향 기조 검증 (유지)
    AND a.ggg_bps <= a.iii_bps AND a.iii_bps <= a.kkk_bps
    
    -- 8. 배당 조건 (유지)
    AND a.dividend_yield >= 0.0   
    
    -- 9-1. 본업 흑자 유지 (적자 전환 시 탈락)
    AND d.operating_income > 0
    AND d.net_income > 0 
    
    -- 9-2. 경제적 해자 강화 (영업이익률 8% 붕괴 시 탈락)
    AND (d.operating_income / NULLIF(d.revenue, 0)) >= 0.08
    
    -- 6. 재무 건전성 유지 (빚이 과도하게 늘어나면 탈락)
    AND (d.net_debt <= m.market_cap * 0.2)

ORDER BY a.eps_ratio DESC;
EEOFF






cat > value_growth_mytrade.sql <<'EEOFF'
WITH max_date_cte AS (
    SELECT MAX(trade_date) AS max_date FROM stockfdt_pbr_v
),
past_eps_cte AS (
    SELECT 
        code,
        (AVG(eps))::numeric(10,2) AS past_eps
    FROM stockfdt_pbr_v
    CROSS JOIN max_date_cte
    WHERE trade_date BETWEEN max_date - INTERVAL '1 year' - INTERVAL '3 days' 
                         AND max_date - INTERVAL '1 year' + INTERVAL '3 days'
    GROUP BY code
),
calc_yearly_data AS (
    SELECT 
        a.code,
        to_char(a.trade_date, 'YYYY') AS year,
        AVG(a.roe) AS avg_roe,
        AVG(a.pbr) AS avg_pbr,  
        -- 기존: avg(b.market_cap/a.pbr)::bigint AS avg_market_cap,
        -- 변경: 시가총액 대신 진짜 자산인 BPS(주당순자산)를 계산합니다.
        (AVG(a.bps))::bigint AS avg_bps,
        avg(a.dividend_yield) as avg_dividend
    FROM stockfdt_pbr_v a 
    JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code
    WHERE a.trade_date >= '20160101'
    GROUP BY a.code, to_char(a.trade_date, 'YYYY')
),
find_min_roe AS (
    SELECT 
        code,
        year,
        avg_roe,
        avg_bps,
        avg_dividend,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr
    FROM calc_yearly_data
),
filtered_data AS (
    SELECT 
        code,
        year,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, 
        ROUND(avg_roe, 2) AS avg_roe,
        avg_bps AS avg_bps,
        round(avg_dividend, 2) as avg_dividend
    FROM find_min_roe
    WHERE avg_roe_ever >= 10.0 AND min_roe_ever >= 0.0   -- 1. 꾸준히 수익 창출하는 기업
),
pivot_data AS (
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr,
        -- 기존 시가총액 피벗 삭제 후 BPS 피벗으로 교체
        MAX(avg_bps) FILTER (WHERE year = '2022') AS ggg_bps,
        MAX(avg_bps) FILTER (WHERE year = '2024') AS iii_bps,
        MAX(avg_bps) FILTER (WHERE year = '2026') AS kkk_bps
    FROM filtered_data
    GROUP BY code
),
-- 💡 [NEW] 재무 데이터(stock_debt)의 가장 최근 날짜 스냅샷만 추출
latest_debt_cte AS (
    SELECT *
    FROM stock_debt
    WHERE trade_date = (SELECT MAX(trade_date) FROM stock_debt)
),
last_data AS (
    SELECT 
        ROUND(((a.eps - p.past_eps) / NULLIF(ABS(p.past_eps), 0)) * 100, 2) AS eps_ratio,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) AS future_bps,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 2) AS return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * 1) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS expected_cagr,
        ROUND((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) AS future_expected_price,
        ROUND(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 2) AS fep_return_multiple,
        ROUND((POWER(((a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) * b.hist_avg_pbr) / NULLIF(a.close_price, 0), 1.0 / 10.0) - 1) * 100, 2) AS fep_expected_cagr,
        *
    FROM stockfdt_pbr_v a 
    JOIN pivot_data b USING (code)
    LEFT JOIN past_eps_cte p USING (code)
    CROSS JOIN max_date_cte
    WHERE a.trade_date = max_date_cte.max_date
    AND a.bps > 0           
    AND a.close_price > 0   
)
select  a.trade_date,a.code,a.name
    ,b.revenue
    ,b.operating_income
    ,b.operating_income_ratio
    ,b.net_debt
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
    ,':::' div1
    ,case when b.trade_div = 'bond' then a.expected_cagr - trade_expected_cagr end bond_diff
    ,case when b.trade_div = 'bond' then a.expected_cagr - 12 end bond_sell_1
    ,case when b.trade_div = 'bond' then a.expected_cagr - 11 end bond_sell_2
    ,case when b.trade_div = 'bond' then a.expected_cagr - 10 end bond_sell_3
    ,':::' div2
    ,case when b.trade_div = 'value' then a.fep_expected_cagr - trade_expected_cagr end value_diff   
    ,case when b.trade_div = 'value' then a.expected_cagr - 17 end value_sell_1
    ,case when b.trade_div = 'value' then a.expected_cagr - 16 end value_sell_2
    ,case when b.trade_div = 'value' then a.expected_cagr - 15 end value_sell_3
    ,':::' div3
    ,case when b.trade_div = 'growth' then a.fep_expected_cagr - trade_expected_cagr end growth_diff   
    ,case when b.trade_div = 'growth' then '50%' end growth_sell_1
    ,case when b.trade_div = 'growth' then '100%' end growth_sell_2
    ,case when b.trade_div = 'growth' then 'growth_engine_exit' end growth_sell_3
    ,':::' div4
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
    ,(y.trade_value::numeric / 10000000)::int AS trade_value_chunman
    ,(y.market_cap::numeric / 10000000000)::int AS market_cap_bakuk
FROM last_data a join mytrade b on a.code = b.code
JOIN stockmain m ON a.trade_date = m.trade_date AND a.code = m.code 
join stockmain y on a.trade_date = y.trade_date and a.code = y.code
where b.trade_status = 1
order by b.trade_div,bond_sell_1,value_sell_1,growth_sell_1
EEOFF



