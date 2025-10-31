import requests
import pandas as pd
import random
import yaml
from io import BytesIO
from datetime import datetime, timedelta
from holidayskr import is_holiday
import psycopg2
from psycopg2.extras import execute_batch

with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
DISCORD_WEBHOOK_URL_MAIN = _cfg['DISCORD_WEBHOOK_URL_MAIN']

def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    try:
        requests.post(DISCORD_WEBHOOK_URL, data=message, timeout=5)
    except Exception as e:
        print(f"❌ Discord 전송 실패: {e}", flush=True)
    print(message, flush=True)

def send_message_main(msg):
    """디스코드 메세지 전송"""
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    try:
        requests.post(DISCORD_WEBHOOK_URL_MAIN, data=message, timeout=5)
    except Exception as e:
        print(f"❌ Discord 전송 실패: {e}", flush=True)
    #print(message, flush=True)

def get_last_trading_day():
    day = datetime.today() - timedelta(days=1)
    while day.weekday() >= 5 or is_holiday(day.strftime("%Y-%m-%d")):
        day -= timedelta(days=1)
    return day.strftime('%Y%m%d')

def get_all_symbols(p_trade_date='20250901'):
    #trade_date = get_last_trading_day()
    #trade_date = '20250826'
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with psycopg2.connect(
            host="192.168.1.33",
            dbname="postgres",
            user="postgres",
            password="1dlsvmfk)(!@"
        ) as conn:
            with conn.cursor() as cur:
                sql = """
                    with ma_check as (
                    select
                    trade_date,
                    code,
                    lag(ma40,	1)	over	(partition	by	code	order	by	trade_date)	as	prev1,
                    lag(ma20,	1)	over	(partition	by	code	order	by	trade_date)	as	prev1_20,
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
                    ma5,ma10,ma20,ma40,ma60,ma90,ma120
                    from stock_ma
                    ), close_price_check as (
                    select
                    trade_date, code,
                    lag(close_price,	40)	over	(partition	by	code	order	by	trade_date)	as	price_prev40
                    from stockmain
                    )
                    select
                        --sm.trade_date,
                        sm.code,
                        sm.name
                        --sm.close_price,
                        --mc.ma5, mc.ma10, mc.ma20, mc.ma40
                    from stockmain sm join ma_check mc on sm.trade_date = mc.trade_date and sm.code = mc.code
                        join close_price_check cc on sm.trade_date = cc.trade_date and sm.code = cc.code
                    where
                        sm.trade_date = %s
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
                    and prev1 < ma40

                    and close_price > ma5
                    and close_price > ma10
                    and close_price > ma20
                    and close_price > ma40
                    and ma5 > ma10
                    and ma10 > ma20
                    and ma20 > ma40
                    and prev1_20 < prev1

                    and (close_price - ma40)/ma40*100 < 25.0
                """
                cur.execute(sql, (trade_date,))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        print(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)} 매수종목 반환")
        #print(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        print(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols_sell(p_trade_date='20250901'):
    #trade_date = get_last_trading_day()
    #trade_date = '20250826'
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with psycopg2.connect(
            host="192.168.1.33",
            dbname="postgres",
            user="postgres",
            password="1dlsvmfk)(!@"
        ) as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT sm.code, sm.name
                    FROM stockmain sm
                    JOIN stock_ma mc 
                        ON sm.trade_date = mc.trade_date
                        AND sm.code = mc.code
                    WHERE mc.ma40 > mc.ma20
                        AND sm.trade_date = %s
                """
                cur.execute(sql, (trade_date,))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        print(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)} 매도종목 반환")
        #print(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        print(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def fetch_krx_pbr_data(trade_date, mktId='ALL'):
    """
    KRX에서 개별종목의 PER/PBR/배당수익률 데이터를 가져오는 함수
    
    Parameters:
    -----------
    trade_date : str
        조회일자 (YYYYMMDD 형식, 예: '20240930')
    mktId : str
        시장 구분 ('STK': 코스피, 'KSQ': 코스닥, 'ALL': 전체, 기본값: 'ALL')
        
    Returns:
    --------
    pandas.DataFrame
        PBR, PER, 배당수익률 등이 포함된 데이터프레임
        주요 컬럼: 종목명, 종목코드, 종가, EPS, PER, BPS, PBR, 배당수익률 등
    """
    
    # Step 1: OTP 코드 생성
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_form_data = {
        'locale': 'ko_KR',
        'mktId': mktId,           # 'STK', 'KSQ', 'ALL'
        'trdDd': trade_date,      # 거래일자
        'money': '1',             # 원 단위
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03501',  # PBR 데이터 URL
    }
    
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    print(f"OTP 코드 생성 요청 중... 시장: {mktId}, 날짜: {trade_date}")
    otp_response = requests.post(otp_url, data=otp_form_data, headers=headers)
    
    if otp_response.status_code != 200:
        print(f"OTP 요청 실패: 상태 코드 {otp_response.status_code}")
        print(otp_response.text)
        return None
    
    otp_code = otp_response.text
    print(f"OTP 코드 생성 완료")
    
    # Step 2: CSV 파일 다운로드
    csv_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    csv_response = requests.post(csv_url, data={'code': otp_code}, headers=headers)
    
    if csv_response.status_code != 200:
        print(f"CSV 다운로드 실패: 상태 코드 {csv_response.status_code}")
        print(csv_response.text)
        return None
    
    # Step 3: CSV 파싱
    try:
        df = pd.read_csv(BytesIO(csv_response.content), encoding='euc-kr')
        print(f"데이터 로드 완료: {len(df)}개 종목")
        return df
    except Exception as e:
        print(f"CSV 파싱 오류: {e}")
        return None


def fetch_krx_all_stocks_pbr(trade_date, mktId='ALL'):
    """
    특정 날짜의 전체 종목 PBR 데이터를 가져오는 함수
    
    Parameters:
    -----------
    trade_date : str
        조회일자 (YYYYMMDD 형식, 예: '20240930')
    mktId : str, optional
        시장 구분 ('STK': 코스피, 'KSQ': 코스닥, 'ALL': 전체, 기본값: 'ALL')
        
    Returns:
    --------
    pandas.DataFrame
        전체 종목의 PBR, PER 등 투자지표 데이터
    """
    
    result = fetch_krx_pbr_data(trade_date, mktId)
    
    return result

if __name__ == "__main__":
#    pool_count = 300
#    trade_date = get_last_trading_day()
#    #trade_date = '20250905'
#    symbols_buy_pool = get_all_symbols(p_trade_date=trade_date)  # 금일 매수 종목
#    symbols_sell_pool = get_all_symbols_sell(p_trade_date=trade_date)  # 금일 매도 종목 <- 현재 계좌에 있다면...

    # 사용 예시
    # 예시 1: 특정 날짜의 전체 종목 PBR 데이터
    df_all = fetch_krx_all_stocks_pbr('20251002')
    if df_all is not None:
        print("\n전체 종목 PBR 데이터:")
        print(df_all.head())
        print(f"\n컬럼 목록: {df_all.columns.tolist()}")
