cat > bond1.sql <<'EEOFF'
WITH calc_quarterly_data AS (
    -- 1단계: 종목(code)별, 3개월 단위 평균 ROE 및 평균 PBR 계산
    SELECT 
        code,
        to_char(trade_date, 'YYYY-Q"Q"') AS quarter,
        AVG(roe) AS avg_roe,
        AVG(pbr) AS avg_pbr  -- 💡 [추가] 과거 시장 평가(권리금)를 추적하기 위한 PBR 평균
    FROM stockfdt_pbr_v
    WHERE trade_date >= '20160101'
    GROUP BY code, to_char(trade_date, 'YYYY-Q"Q"')
),
find_min_roe AS (
    -- 2단계: 최악의 ROE 찾기 및 NULL 빵꾸 이력 추적
    SELECT 
        code,
        quarter,
        avg_roe,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        BOOL_OR(avg_roe IS NULL) OVER (PARTITION BY code) AS has_null_roe,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr -- 💡 [추가] 10년 평균 PBR 산출
    FROM calc_quarterly_data
),
filtered_data AS (
    -- 3단계: 불량 종목(5% 미만 or NULL 이력) 싹 다 제거!
    SELECT 
        code,
        quarter,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, -- 💡 [추가] 다음 단계로 전달
        ROUND(avg_roe, 2) AS avg_roe
    FROM find_min_roe
    WHERE min_roe_ever >= 5
      AND has_null_roe = false
),
pivot_data AS (
    -- 4단계: 2015-1Q부터 2026-1Q까지 45분기 절대 시간 피벗 전개!
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr, -- 💡 [추가] 최종 계산을 위해 메인 쿼리로 전달
        -- [2015년]
        --MAX(avg_roe) FILTER (WHERE quarter = '2015-1Q') AS "2015-1Q",
        --MAX(avg_roe) FILTER (WHERE quarter = '2015-2Q') AS "2015-2Q",
        --MAX(avg_roe) FILTER (WHERE quarter = '2015-3Q') AS "2015-3Q",
        --MAX(avg_roe) FILTER (WHERE quarter = '2015-4Q') AS "2015-4Q",
        -- [2016년]
        MAX(avg_roe) FILTER (WHERE quarter = '2016-1Q') AS "2016-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2016-2Q') AS "2016-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2016-3Q') AS "2016-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2016-4Q') AS "2016-4Q",
        -- [2017년]
        MAX(avg_roe) FILTER (WHERE quarter = '2017-1Q') AS "2017-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2017-2Q') AS "2017-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2017-3Q') AS "2017-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2017-4Q') AS "2017-4Q",
        -- [2018년]
        MAX(avg_roe) FILTER (WHERE quarter = '2018-1Q') AS "2018-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2018-2Q') AS "2018-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2018-3Q') AS "2018-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2018-4Q') AS "2018-4Q",
        -- [2019년]
        MAX(avg_roe) FILTER (WHERE quarter = '2019-1Q') AS "2019-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2019-2Q') AS "2019-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2019-3Q') AS "2019-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2019-4Q') AS "2019-4Q",
        -- [2020년]
        MAX(avg_roe) FILTER (WHERE quarter = '2020-1Q') AS "2020-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2020-2Q') AS "2020-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2020-3Q') AS "2020-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2020-4Q') AS "2020-4Q",
        -- [2021년]
        MAX(avg_roe) FILTER (WHERE quarter = '2021-1Q') AS "2021-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2021-2Q') AS "2021-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2021-3Q') AS "2021-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2021-4Q') AS "2021-4Q",
        -- [2022년]
        MAX(avg_roe) FILTER (WHERE quarter = '2022-1Q') AS "2022-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2022-2Q') AS "2022-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2022-3Q') AS "2022-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2022-4Q') AS "2022-4Q",
        -- [2023년]
        MAX(avg_roe) FILTER (WHERE quarter = '2023-1Q') AS "2023-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2023-2Q') AS "2023-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2023-3Q') AS "2023-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2023-4Q') AS "2023-4Q",
        -- [2024년]
        MAX(avg_roe) FILTER (WHERE quarter = '2024-1Q') AS "2024-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2024-2Q') AS "2024-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2024-3Q') AS "2024-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2024-4Q') AS "2024-4Q",
        -- [2025년]
        MAX(avg_roe) FILTER (WHERE quarter = '2025-1Q') AS "2025-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2025-2Q') AS "2025-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2025-3Q') AS "2025-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2025-4Q') AS "2025-4Q",
        -- [2026년]
        MAX(avg_roe) FILTER (WHERE quarter = '2026-1Q') AS "2026-1Q"
    FROM filtered_data
    GROUP BY code
),
last_data AS (
select 
    ROUND(bps * POWER(1 + min_roe_ever / 100.0, 10)) AS future_bps,
    ROUND((bps * POWER(1 + min_roe_ever / 100.0, 10)) / close_price, 2) AS return_multiple,
    ROUND(
        (POWER(
            (bps * POWER(1 + min_roe_ever / 100.0, 10)) / close_price,  -- (미래가치 / 현재가치)
            1.0 / 10.0                                                  -- ^ (1/10)
        ) - 1) * 100, 
    2) AS expected_cagr,
*
from stockfdt_pbr_v a join pivot_data b USING (code)
where a.trade_date = (select max(trade_date) from stockfdt_pbr_v)
AND a.bps > 0           -- 💡 [방어코드] 자본잠식 기업 에러 방지
AND a.close_price > 0   -- 💡 [방어코드] 거래정지(0원) 에러 방지
)
select  (b.trade_value::numeric / 100000000)::int AS trade_value_uk,
        (b.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
        b.sector,
        a.*
from last_data a join stockmain b on a.trade_date = b.trade_date and a.code = b.code
where expected_cagr >= 10.0
order by expected_cagr desc;
EEOFF




select 
    ROUND(bps * POWER(1 + min_roe_ever / 100.0, 10)) AS future_bps,
    ROUND((bps * POWER(1 + min_roe_ever / 100.0, 10)) / close_price, 2) AS return_multiple,
    ROUND(
        (POWER(
            (bps * POWER(1 + min_roe_ever / 100.0, 10)) / close_price,  -- (미래가치 / 현재가치)
            1.0 / 10.0                                                  -- ^ (1/10)
        ) - 1) * 100, 
    2) AS expected_cagr
;

select 
    ROUND(85756 * POWER(1 + 5.6 / 100.0, 10)) AS future_bps,
    ROUND((85756 * POWER(1 + 5.6 / 100.0, 10)) / 23500, 2) AS return_multiple,
    ROUND(
        (POWER(
            (85756 * POWER(1 + 5.6 / 100.0, 10)) / 23500,  -- (미래가치 / 현재가치)
            1.0 / 10.0                                                  -- ^ (1/10)
        ) - 1) * 100, 
    2) AS expected_cagr
;



위 쿼리에 담긴 진짜 속마음(전제조건)은 이겁니다.
    "이 회사는 역대 최악의 시절에도 ROE를 10%나 냈어. 
    앞으로 10년 동안 장사를 기가 막히게 잘해서 내 자산(BPS)을 엄청 불려주겠지? 
    정상적이라면 10년 뒤에 사람들이 권리금(PBR 1.5)을 얹어서 비싸게 사줘야 해.

    하지만... 주식 시장은 미쳤으니까, 
    10년 뒤에 운이 더럽게 없어서 시장 폭락장이 오거나 이 회사가 인기가 없어져서 
    사람들이 권리금을 단 1원도 안 쳐주고 딱 장부 가치(PBR 1.0)에만 사겠다고 헐값에 후려친다고 가정해 보자.

    그래도 내가 10년 동안 연평균 15% 수익(CAGR)을 먹을 수 있을까? 어? 먹을 수 있네? 그럼 당장 사야지!"

[DBA의 최종 요약]
즉, "확률이 높아서" PBR을 1로 잡은 것이 아닙니다.
**"최악의 상황(권리금 0원)을 가정하고 후려쳐서 계산해도 내가 원하는 목표 수익률이 나오는 
찐 알짜배기 주식"**을 찾기 위해 일부러 가혹하게 PBR을 1로 눌러버린 것입니다.




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
    bps::int bps
   FROM stockfdt;




벤저민 그레이엄의 '전통적 가치투자(PBR 1.0 회귀)'의 한계를 부수고, 
피터 린치나 필립 피셔 같은 대가들이 쓰는 
**'성장주(Growth Stock) 프리미엄 모델'**로 진화하는 완벽한 질문입니다!

"지금 PBR이 3.0인 잘 나가는 회사가 10년 뒤에도 여전히 업계 1위라서 
최소한 PBR 2.0은 받을 텐데, 왜 굳이 PBR 1.0으로 후려쳐서 수익률을 깎아 먹어야 해?"

이 억울함을 달래주기 위한 해결책, 아주 시원하게 팩트 폭격을 날려드립니다!

"10년 뒤의 순자산(Future BPS)에다가, 
시장이 이 종목에 지난 10년간 평균적으로 부여했던 
'적정 권리금(Historical Average PBR)'을 곱해서 
'미래의 진짜 예상 주가'를 산출해 주면 완벽하게 해결됩니다!"


cat > bond2.sql <<'EEOFF'
WITH calc_quarterly_data AS (
    -- 1단계: 종목(code)별, 3개월 단위 평균 ROE 및 평균 PBR 계산
    SELECT 
        code,
        to_char(trade_date, 'YYYY-Q"Q"') AS quarter,
        AVG(roe) AS avg_roe,
        AVG(pbr) AS avg_pbr  -- 💡 [추가] 과거 시장 평가(권리금)를 추적하기 위한 PBR 평균
    FROM stockfdt_pbr_v
    WHERE trade_date >= '20160101'
    GROUP BY code, to_char(trade_date, 'YYYY-Q"Q"')
),
find_min_roe AS (
    -- 2단계: 최악의 ROE 찾기, NULL 이력 추적 및 '역대 평균 PBR' 산출
    SELECT 
        code,
        quarter,
        avg_roe,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        AVG(avg_roe) OVER (PARTITION BY code) AS avg_roe_ever,
        MAX(avg_roe) OVER (PARTITION BY code) AS max_roe_ever,
        BOOL_OR(avg_roe IS NULL) OVER (PARTITION BY code) AS has_null_roe,
        AVG(avg_pbr) OVER (PARTITION BY code) AS hist_avg_pbr -- 💡 [추가] 10년 평균 PBR 산출
    FROM calc_quarterly_data
),
filtered_data AS (
    -- 3단계: 불량 종목(5% 미만 or NULL 이력) 싹 다 제거!
    SELECT 
        code,
        quarter,
        ROUND(min_roe_ever, 2) AS min_roe_ever,
        ROUND(avg_roe_ever, 2) AS avg_roe_ever,
        ROUND(max_roe_ever, 2) AS max_roe_ever,
        ROUND(hist_avg_pbr, 2) AS hist_avg_pbr, -- 💡 [추가] 다음 단계로 전달
        ROUND(avg_roe, 2) AS avg_roe
    FROM find_min_roe
    WHERE min_roe_ever >= 5
      AND has_null_roe = false
),
pivot_data AS (
    -- 4단계: 45분기 절대 시간 피벗 전개 및 종목별 메타데이터 집계
    SELECT 
        code,
        MAX(min_roe_ever) AS min_roe_ever,
        MAX(avg_roe_ever) AS avg_roe_ever,
        MAX(max_roe_ever) AS max_roe_ever,
        MAX(hist_avg_pbr) AS hist_avg_pbr, -- 💡 [추가] 최종 계산을 위해 메인 쿼리로 전달
        
        -- [2015년] (주석 처리된 부분은 그대로 유지하셨군요. 좋습니다!)
        --MAX(avg_roe) FILTER (WHERE quarter = '2015-1Q') AS "2015-1Q",
        --MAX(avg_roe) FILTER (WHERE quarter = '2015-2Q') AS "2015-2Q",
        --MAX(avg_roe) FILTER (WHERE quarter = '2015-3Q') AS "2015-3Q",
        --MAX(avg_roe) FILTER (WHERE quarter = '2015-4Q') AS "2015-4Q",
        
        -- [2016년]
        MAX(avg_roe) FILTER (WHERE quarter = '2016-1Q') AS "2016-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2016-2Q') AS "2016-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2016-3Q') AS "2016-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2016-4Q') AS "2016-4Q",
        -- [2017년]
        MAX(avg_roe) FILTER (WHERE quarter = '2017-1Q') AS "2017-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2017-2Q') AS "2017-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2017-3Q') AS "2017-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2017-4Q') AS "2017-4Q",
        -- [2018년]
        MAX(avg_roe) FILTER (WHERE quarter = '2018-1Q') AS "2018-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2018-2Q') AS "2018-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2018-3Q') AS "2018-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2018-4Q') AS "2018-4Q",
        -- [2019년]
        MAX(avg_roe) FILTER (WHERE quarter = '2019-1Q') AS "2019-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2019-2Q') AS "2019-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2019-3Q') AS "2019-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2019-4Q') AS "2019-4Q",
        -- [2020년]
        MAX(avg_roe) FILTER (WHERE quarter = '2020-1Q') AS "2020-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2020-2Q') AS "2020-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2020-3Q') AS "2020-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2020-4Q') AS "2020-4Q",
        -- [2021년]
        MAX(avg_roe) FILTER (WHERE quarter = '2021-1Q') AS "2021-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2021-2Q') AS "2021-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2021-3Q') AS "2021-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2021-4Q') AS "2021-4Q",
        -- [2022년]
        MAX(avg_roe) FILTER (WHERE quarter = '2022-1Q') AS "2022-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2022-2Q') AS "2022-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2022-3Q') AS "2022-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2022-4Q') AS "2022-4Q",
        -- [2023년]
        MAX(avg_roe) FILTER (WHERE quarter = '2023-1Q') AS "2023-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2023-2Q') AS "2023-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2023-3Q') AS "2023-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2023-4Q') AS "2023-4Q",
        -- [2024년]
        MAX(avg_roe) FILTER (WHERE quarter = '2024-1Q') AS "2024-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2024-2Q') AS "2024-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2024-3Q') AS "2024-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2024-4Q') AS "2024-4Q",
        -- [2025년]
        MAX(avg_roe) FILTER (WHERE quarter = '2025-1Q') AS "2025-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2025-2Q') AS "2025-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2025-3Q') AS "2025-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2025-4Q') AS "2025-4Q",
        -- [2026년]
        MAX(avg_roe) FILTER (WHERE quarter = '2026-1Q') AS "2026-1Q"
    FROM filtered_data
    GROUP BY code
),
last_data AS (
    -- 5단계: 재무 가치평가 (Valuation) 및 미래 주가 산출
    SELECT 
        -- 💡 1. 10년 후 예상 BPS (장부 가치)
        ROUND(a.bps * POWER(1 + b.min_roe_ever / 100.0, 10)) AS future_bps,
        
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
    (b.trade_value::numeric / 100000000)::int AS trade_value_uk,
    (b.market_cap::numeric / 100000000000)::int AS market_cap_chunuk,
    b.sector,
    a.*
FROM last_data a 
JOIN stockmain b ON a.trade_date = b.trade_date AND a.code = b.code
WHERE a.expected_cagr >= 10.0
ORDER BY a.expected_cagr DESC;
EEOFF

