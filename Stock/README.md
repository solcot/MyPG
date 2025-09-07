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

