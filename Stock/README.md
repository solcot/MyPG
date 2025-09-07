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
--ORDER BY trade_date DESC;

