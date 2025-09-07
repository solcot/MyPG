# Stock Folder
pip install requests
pip install pyyaml
pip install pandas
pip install lxml
pip install html5lib
pip install beautifulsoup4
pip install holidayskr
pip install psycopg2-binary   # pg 연동시 필요

🚀 : “출발!”, “시작!” 느낌

📉 하락/손실/경고
📉 🔻 🚨 ❌ 🛑

📈 상승/수익/성공
📈 🔺 💰 ✅ 🟢

🧠 정보/분석/계산
🧮 📊 📋 🧾

⏰ 시간 관련
⏰ ⏳

🛠️ 시스템/기타
🛠️ 🔄 🧪

기쁨을 표현하는 이모티콘
😊: 미소 짓는 얼굴
😄: 활짝 웃는 얼굴
😂: 기쁨의 눈물을 흘리며 웃는 얼굴
🥳: 파티를 즐기는 얼굴 (축하, 기쁨)
✨: 반짝임 (긍정적인 감정, 기쁨)

슬픔을 표현하는 이모티콘
😢: 우는 얼굴
😥: 슬픔을 느끼는 얼굴
😭: 흐느껴 우는 얼굴 (아주 슬플 때)
😞: 실망한 얼굴

--------------------------------------------------------------------------
CREATE TABLE stockmain (
    trade_date    DATE NOT NULL,        -- 거래일
    code          VARCHAR(20) NOT NULL, -- 종목코드
    name          VARCHAR(100),         -- 종목명
    close_price   NUMERIC(15,2),        -- 종가
    change_price  NUMERIC(15,2),        -- 전일 대비
    change_rate   NUMERIC(7,4),         -- 등락률 (%)
    open_price    NUMERIC(15,2),        -- 시가
    high_price    NUMERIC(15,2),        -- 고가
    low_price     NUMERIC(15,2),        -- 저가
    volume        BIGINT,               -- 거래량
    trade_value   BIGINT,               -- 거래대금
    market_cap    BIGINT,               -- 시가총액
    shares_out    BIGINT,               -- 상장주식수
    sector        VARCHAR(100),          -- 소속부
    created_at    TIMESTAMP NOT NULL DEFAULT now(), -- 데이터 입력 시간
    PRIMARY KEY (trade_date, code)
);

CREATE TABLE stock_ma (
    trade_date DATE NOT NULL,             -- 거래일
    code       VARCHAR(20) NOT NULL,      -- 종목코드
    ma5       NUMERIC(15,2),             -- 5일 이동평균
    ma10      NUMERIC(15,2),             -- 10일 이동평균
    ma20      NUMERIC(15,2),             -- 20일 이동평균
    ma60      NUMERIC(15,2),             -- 60일 이동평균
    ma120     NUMERIC(15,2),             -- 120일 이동평균
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, code)
);

select a.close_price, b.* 
from stockmain a join stock_ma b 
    on a.trade_date = b.trade_date
    and a.code = b.code
where a.trade_date = '2025-09-05' and a.code = '005930';

select a.name, a.close_price
    , b.* 
from stockmain a join stock_ma b 
    on a.trade_date = b.trade_date
    and a.code = b.code
where a.trade_date = '2025-09-05' 
and a.close_price >= b.ma5
and a.close_price >= b.ma10
and a.close_price >= b.ma20
and a.close_price >= b.ma60
and a.close_price >= b.ma120

and b.ma5 >= b.ma10
and b.ma10 >= b.ma20
and b.ma20 >= b.ma60
and b.ma60 >= b.ma120
;

WITH ma5_check AS (
    SELECT
        code,
        trade_date,
        ma5,
        LAG(ma5, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1,
        LAG(ma5, 2) OVER (PARTITION BY code ORDER BY trade_date) AS prev2,
        LAG(ma5, 3) OVER (PARTITION BY code ORDER BY trade_date) AS prev3,
        LAG(ma5, 4) OVER (PARTITION BY code ORDER BY trade_date) AS prev4,
        LAG(ma5, 5) OVER (PARTITION BY code ORDER BY trade_date) AS prev5,
        ma10, ma20, ma60, ma120
    FROM stock_ma
)
SELECT
    sm.trade_date,
    sm.name,
    m.code,
    sm.close_price,
    m.ma5, m.ma10, m.ma20,
    (abs(sm.close_price - m.ma5) / sm.close_price
     + abs(sm.close_price - m.ma10) / sm.close_price
     + abs(sm.close_price - m.ma20) / sm.close_price
     + abs(sm.close_price - m.ma60) / sm.close_price
     + abs(sm.close_price - m.ma120) / sm.close_price
    ) AS total_gap_ratio_per_closeprice,
    (sm.close_price - sm.open_price) / sm.close_price * 100 AS today_change_rate
FROM ma5_check m
JOIN stockmain sm
    ON sm.code = m.code AND sm.trade_date = m.trade_date
WHERE
    m.trade_date = '2025-09-04'
    AND prev4 > prev3
    AND prev3 > prev2
    AND prev2 > prev1
    AND prev1 < ma5
    AND sm.close_price > m.ma5
    AND sm.close_price > m.ma10
    AND sm.close_price > m.ma20
    AND sm.trade_value > 1000000000
    AND ((close_price - open_price) / close_price * 100) > 0.3
    AND ((close_price - open_price) / close_price * 100) < 3.0
    AND close_price < 350000
    AND market_cap > 100000000000 
    AND     (abs(sm.close_price - m.ma5) / sm.close_price
     + abs(sm.close_price - m.ma10) / sm.close_price
     + abs(sm.close_price - m.ma20) / sm.close_price
     + abs(sm.close_price - m.ma60) / sm.close_price
     + abs(sm.close_price - m.ma120) / sm.close_price
    ) < 0.10
ORDER BY total_gap_ratio_per_closeprice ASC;


WITH ma10_check AS (
    SELECT
        code,
        trade_date,
        LAG(ma60, 1) OVER (PARTITION BY code ORDER BY trade_date) AS prev1,
        LAG(ma60, 2) OVER (PARTITION BY code ORDER BY trade_date) AS prev2,
        LAG(ma60, 3) OVER (PARTITION BY code ORDER BY trade_date) AS prev3,
        LAG(ma60, 4) OVER (PARTITION BY code ORDER BY trade_date) AS prev4,
        LAG(ma60, 5) OVER (PARTITION BY code ORDER BY trade_date) AS prev5,
        LAG(ma60, 6) OVER (PARTITION BY code ORDER BY trade_date) AS prev6,
        LAG(ma60, 7) OVER (PARTITION BY code ORDER BY trade_date) AS prev7,
        LAG(ma60, 8) OVER (PARTITION BY code ORDER BY trade_date) AS prev8,
        LAG(ma60, 9) OVER (PARTITION BY code ORDER BY trade_date) AS prev9,
        LAG(ma60, 10) OVER (PARTITION BY code ORDER BY trade_date) AS prev10,
        LAG(ma60, 11) OVER (PARTITION BY code ORDER BY trade_date) AS prev11,
        LAG(ma60, 12) OVER (PARTITION BY code ORDER BY trade_date) AS prev12,
        LAG(ma60, 13) OVER (PARTITION BY code ORDER BY trade_date) AS prev13,
        LAG(ma60, 14) OVER (PARTITION BY code ORDER BY trade_date) AS prev14,
        LAG(ma60, 15) OVER (PARTITION BY code ORDER BY trade_date) AS prev15,
        LAG(ma60, 16) OVER (PARTITION BY code ORDER BY trade_date) AS prev16,
        LAG(ma60, 17) OVER (PARTITION BY code ORDER BY trade_date) AS prev17,
        LAG(ma60, 18) OVER (PARTITION BY code ORDER BY trade_date) AS prev18,
        LAG(ma60, 19) OVER (PARTITION BY code ORDER BY trade_date) AS prev19,
        LAG(ma60, 20) OVER (PARTITION BY code ORDER BY trade_date) AS prev20,
        LAG(ma60, 21) OVER (PARTITION BY code ORDER BY trade_date) AS prev21,
        LAG(ma60, 22) OVER (PARTITION BY code ORDER BY trade_date) AS prev22,
        LAG(ma60, 23) OVER (PARTITION BY code ORDER BY trade_date) AS prev23,
        LAG(ma60, 24) OVER (PARTITION BY code ORDER BY trade_date) AS prev24,
        LAG(ma60, 25) OVER (PARTITION BY code ORDER BY trade_date) AS prev25,
        LAG(ma60, 26) OVER (PARTITION BY code ORDER BY trade_date) AS prev26,
        LAG(ma60, 27) OVER (PARTITION BY code ORDER BY trade_date) AS prev27,
        LAG(ma60, 28) OVER (PARTITION BY code ORDER BY trade_date) AS prev28,
        LAG(ma60, 29) OVER (PARTITION BY code ORDER BY trade_date) AS prev29,
        LAG(ma60, 30) OVER (PARTITION BY code ORDER BY trade_date) AS prev30,
        LAG(ma60, 31) OVER (PARTITION BY code ORDER BY trade_date) AS prev31,
        LAG(ma60, 32) OVER (PARTITION BY code ORDER BY trade_date) AS prev32,
        LAG(ma60, 33) OVER (PARTITION BY code ORDER BY trade_date) AS prev33,
        LAG(ma60, 34) OVER (PARTITION BY code ORDER BY trade_date) AS prev34,
        LAG(ma60, 35) OVER (PARTITION BY code ORDER BY trade_date) AS prev35,
        LAG(ma60, 36) OVER (PARTITION BY code ORDER BY trade_date) AS prev36,
        LAG(ma60, 37) OVER (PARTITION BY code ORDER BY trade_date) AS prev37,
        LAG(ma60, 38) OVER (PARTITION BY code ORDER BY trade_date) AS prev38,
        LAG(ma60, 39) OVER (PARTITION BY code ORDER BY trade_date) AS prev39,
        LAG(ma60, 40) OVER (PARTITION BY code ORDER BY trade_date) AS prev40,
        LAG(ma60, 41) OVER (PARTITION BY code ORDER BY trade_date) AS prev41,
        LAG(ma60, 42) OVER (PARTITION BY code ORDER BY trade_date) AS prev42,
        LAG(ma60, 43) OVER (PARTITION BY code ORDER BY trade_date) AS prev43,
        LAG(ma60, 44) OVER (PARTITION BY code ORDER BY trade_date) AS prev44,
        LAG(ma60, 45) OVER (PARTITION BY code ORDER BY trade_date) AS prev45,
        ma5, ma10, ma20, ma60, ma120
    FROM stock_ma
)
SELECT
    sm.trade_date,
    sm.name,
    m.code,
    sm.close_price,
    ((close_price - open_price) / close_price * 100)::decimal(7,2) today_up_ratio,
    (abs(sm.close_price - m.ma5) / sm.close_price
     + abs(sm.close_price - m.ma10) / sm.close_price
     + abs(sm.close_price - m.ma20) / sm.close_price
     + abs(sm.close_price - m.ma60) / sm.close_price
     + abs(sm.close_price - m.ma120) / sm.close_price
    )::decimal(7,2) AS total_gap_ratio_per_closeprice,
    m.ma5, m.ma10, m.ma20
FROM ma10_check m
JOIN stockmain sm
    ON sm.code = m.code AND sm.trade_date = m.trade_date
WHERE
    m.trade_date = '2025-09-05'
    AND prev45 > prev44
    AND prev44 > prev43
    AND prev43 > prev42
    AND prev42 > prev41
    AND prev41 > prev40
    AND prev40 > prev39
    AND prev39 > prev38
    AND prev38 > prev37
    AND prev37 > prev36
    AND prev36 > prev35
    AND prev35 > prev34
    AND prev34 > prev33
    AND prev33 > prev32
    AND prev32 > prev31
    AND prev31 > prev30
    AND prev30 > prev29
    AND prev29 > prev28
    AND prev28 > prev27
    AND prev27 > prev26
    AND prev26 > prev25
    AND prev25 > prev24
    AND prev24 > prev23
    AND prev23 > prev22
    AND prev22 > prev21
    AND prev21 > prev20
    AND prev20 > prev19
    AND prev19 > prev18
    AND prev18 > prev17
    AND prev17 > prev16
    AND prev16 > prev15
    AND prev15 > prev14
    AND prev14 > prev13
    AND prev13 > prev12
    AND prev12 > prev11
    AND prev11 > prev10
    AND prev10 > prev9
    AND prev9 > prev8
    AND prev8 > prev7
    AND prev7 > prev6
    AND prev6 > prev5
    AND prev5 > prev4
    AND prev4 > prev3
    AND prev3 > prev2
    AND prev2 > prev1
    AND prev1 < ma60

    AND sm.close_price > m.ma5
    AND sm.close_price > m.ma10
    AND sm.close_price > m.ma20
    AND sm.trade_value > 1000000000
--    AND ((close_price - open_price) / close_price * 100) >= 0.0
--    AND ((close_price - open_price) / close_price * 100) < 7.0
    AND close_price < 350000
    AND market_cap > 100000000000 
    AND (abs(sm.close_price - m.ma5) / sm.close_price
     + abs(sm.close_price - m.ma10) / sm.close_price
     + abs(sm.close_price - m.ma20) / sm.close_price
     + abs(sm.close_price - m.ma60) / sm.close_price
     + abs(sm.close_price - m.ma120) / sm.close_price
    ) < 0.23
;

--ORDER BY trade_date DESC;

