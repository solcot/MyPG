# Stock Folder
pip install requests
pip install pyyaml
pip install pandas
pip install lxml
pip install html5lib
pip install beautifulsoup4
pip install holidayskr
pip install psycopg2-binary   # pg 연동시 필요
pip install pandas_market_calendars
pip install selenium webdriver-manager
--------------------------------------------------
pip install pyupbit    # CoinPy 에 필요
--------------------------------------------------
pip install yfinance   # USStock 에 필요
--------------------------------------------------


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
    ma40      NUMERIC(15,2),             -- 40일 이동평균
    ma60      NUMERIC(15,2),             -- 60일 이동평균
    ma90      NUMERIC(15,2),             -- 90일 이동평균
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

with ma_check as (
select
trade_date,
code,
lag(ma40,	1)	over	(partition	by	code	order	by	trade_date)	as	prev1,
lag(ma40,	2)	over	(partition	by	code	order	by	trade_date)	as	prev2,
lag(ma40,	3)	over	(partition	by	code	order	by	trade_date)	as	prev3,
lag(ma40,	4)	over	(partition	by	code	order	by	trade_date)	as	prev4,
lag(ma40,	5)	over	(partition	by	code	order	by	trade_date)	as	prev5,
lag(ma40,	6)	over	(partition	by	code	order	by	trade_date)	as	prev6,
lag(ma40,	7)	over	(partition	by	code	order	by	trade_date)	as	prev7,
lag(ma40,	8)	over	(partition	by	code	order	by	trade_date)	as	prev8,
lag(ma40,	9)	over	(partition	by	code	order	by	trade_date)	as	prev9,
lag(ma40,	10)	over	(partition	by	code	order	by	trade_date)	as	prev10,
lag(ma40,	11)	over	(partition	by	code	order	by	trade_date)	as	prev11,
lag(ma40,	12)	over	(partition	by	code	order	by	trade_date)	as	prev12,
lag(ma40,	13)	over	(partition	by	code	order	by	trade_date)	as	prev13,
lag(ma40,	14)	over	(partition	by	code	order	by	trade_date)	as	prev14,
lag(ma40,	15)	over	(partition	by	code	order	by	trade_date)	as	prev15,
lag(ma40,	16)	over	(partition	by	code	order	by	trade_date)	as	prev16,
lag(ma40,	17)	over	(partition	by	code	order	by	trade_date)	as	prev17,
lag(ma40,	18)	over	(partition	by	code	order	by	trade_date)	as	prev18,
lag(ma40,	19)	over	(partition	by	code	order	by	trade_date)	as	prev19,
lag(ma40,	20)	over	(partition	by	code	order	by	trade_date)	as	prev20,
lag(ma40,	21)	over	(partition	by	code	order	by	trade_date)	as	prev21,
lag(ma40,	22)	over	(partition	by	code	order	by	trade_date)	as	prev22,
lag(ma40,	23)	over	(partition	by	code	order	by	trade_date)	as	prev23,
lag(ma40,	24)	over	(partition	by	code	order	by	trade_date)	as	prev24,
lag(ma40,	25)	over	(partition	by	code	order	by	trade_date)	as	prev25,
lag(ma40,	26)	over	(partition	by	code	order	by	trade_date)	as	prev26,
lag(ma40,	27)	over	(partition	by	code	order	by	trade_date)	as	prev27,
lag(ma40,	28)	over	(partition	by	code	order	by	trade_date)	as	prev28,
lag(ma40,	29)	over	(partition	by	code	order	by	trade_date)	as	prev29,
lag(ma40,	30)	over	(partition	by	code	order	by	trade_date)	as	prev30,
lag(ma40,	31)	over	(partition	by	code	order	by	trade_date)	as	prev31,
lag(ma40,	32)	over	(partition	by	code	order	by	trade_date)	as	prev32,
lag(ma40,	33)	over	(partition	by	code	order	by	trade_date)	as	prev33,
lag(ma40,	34)	over	(partition	by	code	order	by	trade_date)	as	prev34,
lag(ma40,	35)	over	(partition	by	code	order	by	trade_date)	as	prev35,
lag(ma40,	36)	over	(partition	by	code	order	by	trade_date)	as	prev36,
lag(ma40,	37)	over	(partition	by	code	order	by	trade_date)	as	prev37,
lag(ma40,	38)	over	(partition	by	code	order	by	trade_date)	as	prev38,
lag(ma40,	39)	over	(partition	by	code	order	by	trade_date)	as	prev39,
lag(ma40,	40)	over	(partition	by	code	order	by	trade_date)	as	prev40,
lag(ma40,	41)	over	(partition	by	code	order	by	trade_date)	as	prev41,
lag(ma40,	42)	over	(partition	by	code	order	by	trade_date)	as	prev42,
lag(ma40,	43)	over	(partition	by	code	order	by	trade_date)	as	prev43,
lag(ma40,	44)	over	(partition	by	code	order	by	trade_date)	as	prev44,
lag(ma40,	45)	over	(partition	by	code	order	by	trade_date)	as	prev45,
lag(ma40,	46)	over	(partition	by	code	order	by	trade_date)	as	prev46,
lag(ma40,	47)	over	(partition	by	code	order	by	trade_date)	as	prev47,
lag(ma40,	48)	over	(partition	by	code	order	by	trade_date)	as	prev48,
lag(ma40,	49)	over	(partition	by	code	order	by	trade_date)	as	prev49,
lag(ma40,	50)	over	(partition	by	code	order	by	trade_date)	as	prev50,
lag(ma40,	51)	over	(partition	by	code	order	by	trade_date)	as	prev51,
lag(ma40,	52)	over	(partition	by	code	order	by	trade_date)	as	prev52,
lag(ma40,	53)	over	(partition	by	code	order	by	trade_date)	as	prev53,
lag(ma40,	54)	over	(partition	by	code	order	by	trade_date)	as	prev54,
lag(ma40,	55)	over	(partition	by	code	order	by	trade_date)	as	prev55,
lag(ma40,	56)	over	(partition	by	code	order	by	trade_date)	as	prev56,
lag(ma40,	57)	over	(partition	by	code	order	by	trade_date)	as	prev57,
lag(ma40,	58)	over	(partition	by	code	order	by	trade_date)	as	prev58,
lag(ma40,	59)	over	(partition	by	code	order	by	trade_date)	as	prev59,
lag(ma40,	60)	over	(partition	by	code	order	by	trade_date)	as	prev60,
ma5,ma10,ma20,ma40,ma60,ma90,ma120
from stock_ma
), close_price_check as (
select
trade_date, code,
lag(close_price,	40)	over	(partition	by	code	order	by	trade_date)	as	price_prev40
from stockmain
)
select
    sm.trade_date,
    sm.code,
    sm.name,
    sm.close_price,
    mc.ma5, mc.ma10, mc.ma20, mc.ma40
from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
    join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
where
    sm.trade_date > '2025-08-05'
--and	prev59	>	prev58
--and	prev58	>	prev57
--and	prev57	>	prev56
--and	prev56	>	prev55
--and	prev55	>	prev54
--and	prev54	>	prev53
--and	prev53	>	prev52
--and	prev52	>	prev51
--and	prev51	>	prev50
--and	prev50	>	prev49
--and	prev49	>	prev48
--and	prev48	>	prev47
--and	prev47	>	prev46
--and	prev46	>	prev45
--and	prev45	>	prev44
--and	prev44	>	prev43
--and	prev43	>	prev42
--and	prev42	>	prev41
--and	prev41	>	prev40
and	prev40	>	prev39
and	prev39	>	prev38
and	prev38	>	prev37
and	prev37	>	prev36
and	prev36	>	prev35
and	prev35	>	prev34
and	prev34	>	prev33
and	prev33	>	prev32
and	prev32	>	prev31
and	prev31	>	prev30
and	prev30	>	prev29
and	prev29	>	prev28
and	prev28	>	prev27
and	prev27	>	prev26
and	prev26	>	prev25
and	prev25	>	prev24
and	prev24	>	prev23
and	prev23	>	prev22
and	prev22	>	prev21
and	prev21	>	prev20
and	prev20	>	prev19
and	prev19	>	prev18
and	prev18	>	prev17
and	prev17	>	prev16
and	prev16	>	prev15
and	prev15	>	prev14
and	prev14	>	prev13
and	prev13	>	prev12
and	prev12	>	prev11
and	prev11	>	prev10
and	prev10	>	prev9
and	prev9	>	prev8
and	prev8	>	prev7
and	prev7	>	prev6
and	prev6	>	prev5
and	prev5	>	prev4
--and	prev4	>	prev3
--and	prev3	>	prev2
--and	prev2	>	prev1
and prev1 < ma40
and price_prev40 < close_price

and close_price > ma5
and close_price > ma10
and close_price > ma20
and close_price > ma40
and ma5 > ma10
and ma10 > ma20
and ma10 > ma40

and (close_price - ma40)/ma40*100 < 15.0  --ma40과의 간격 %

and ((close_price - open_price) / close_price * 100) < 15.0  --등락율 %
and close_price > 1500
and close_price < 350000
and market_cap > 50000000000  --5백억
and trade_value > 1000000000  --10억
;


