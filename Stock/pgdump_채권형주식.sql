WITH calc_quarterly_roe AS (
    -- 1단계: 종목(code)별, 3개월 단위 평균 ROE 계산
    SELECT 
        code,
        to_char(trade_date, 'YYYY-Q"Q"') AS quarter,
        AVG(roe) AS avg_roe
    FROM stockfdt_pbr_v
    GROUP BY code, to_char(trade_date, 'YYYY-Q"Q"')
),
find_min_roe AS (
    -- 2단계: 최악의 ROE 찾기 및 NULL 빵꾸 이력 추적
    SELECT 
        code,
        quarter,
        avg_roe,
        MIN(avg_roe) OVER (PARTITION BY code) AS min_roe_ever,
        BOOL_OR(avg_roe IS NULL) OVER (PARTITION BY code) AS has_null_roe
    FROM calc_quarterly_roe
),
filtered_data AS (
    -- 3단계: 불량 종목(5% 미만 or NULL 이력) 싹 다 제거!
    SELECT 
        code,
        quarter,
        ROUND(avg_roe, 2) AS avg_roe
    FROM find_min_roe
    WHERE min_roe_ever >= 10
      AND has_null_roe = false
),
pivot_data AS (
    -- 4단계: 2015-1Q부터 2026-1Q까지 45분기 절대 시간 피벗 전개!
    SELECT 
        code,
        -- [2015년]
        MAX(avg_roe) FILTER (WHERE quarter = '2015-1Q') AS "2015-1Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2015-2Q') AS "2015-2Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2015-3Q') AS "2015-3Q",
        MAX(avg_roe) FILTER (WHERE quarter = '2015-4Q') AS "2015-4Q",
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
)
select * 
from pivot_data
ORDER BY code
;
