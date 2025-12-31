import requests
from requests.exceptions import ConnectTimeout, ReadTimeout, Timeout, ConnectionError
import json
import time
import yaml
import random
import math
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from holidayskr import is_holiday
import configparser # 추가
import os # 파일 존재 여부 확인 및 삭제를 위해 os 모듈 추가
import psycopg2
from psycopg2.extras import execute_batch
import pandas_market_calendars as mcal
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
DISCORD_WEBHOOK_URL_MAIN = _cfg['DISCORD_WEBHOOK_URL_MAIN']
HOST = _cfg['HOST']
DBNAME = _cfg['DBNAME']
USER = _cfg['USER']
PASSWORD = _cfg['PASSWORD']

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

def get_db_connection():
    """데이터베이스 연결 객체를 반환하는 함수"""
    return psycopg2.connect(
        host=HOST,
        dbname=DBNAME,
        user=USER,
        password=PASSWORD
    )

def load_settings():
    """Setting.ini 파일에서 설정을 읽어옵니다."""
    config = configparser.ConfigParser()
    config_path = 'C:\\StockPy\\Setting.ini'

    try:
        config.read(config_path, encoding='utf-8')
        send_message(f"✅ 설정 파일 '{config_path}'을(를) 성공적으로 읽었습니다.")
    except Exception as e:
        send_message(f"❌ 설정 파일 '{config_path}' 읽기 실패: {e}")
        send_message("기본 설정값을 사용합니다.")
        return {
            'ACCOUNT_AMT': 7000000,
            'EXCLUDE_LIST': [],
            'TARGET_BUY_COUNT': 25,
            'T_9_TIME': {'hour': 9, 'minute': 0, 'second': 15},
            'T_START_TIME': {'hour': 9, 'minute': 3, 'second': 0},
            'T_SELL_TIME': {'hour': 14, 'minute': 3, 'second': 0},
            'T_EXIT_TIME': {'hour': 14, 'minute': 8, 'second': 0},
            'AMOUNT_TO_BUY': 350000,
            'STOP_ADD_LOSE_PCT': -5.0,
            'MAX_MOOLING': 5,
            'SLIPPAGE_LIMIT': 1.015,
            'STOP_LOSE_PCT': -3.0,
            'STOP_TRAILING_REBOUND': 1.0,
            'STOP_ABS_LOSE_PCT': -5.0,
            'BREAK_EVEN_PCT1' : 3.0,
            'BREAK_EVEN_LOSE_PCT1' : 2.0,
            'BURN_IN_RATIO' : 0.5,
            'BREAK_EVEN_PCT2' : 5.0,
            'BREAK_EVEN_LOSE_PCT2' : 2.0,
            'BREAK_EVEN_PCT3' : 7.0,
            'BREAK_EVEN_LOSE_PCT3' : 2.0,
            'TAKE_PROFIT_PCT': 9.0,
            'TAKE_PROFIT_LOSE_PCT': 2.0,
            'AMOUNT_LIMIT1_TIME': {'hour': 13, 'minute': 0, 'second': 0},
            'AMOUNT_LIMIT1': 0.7,
            'AMOUNT_LIMIT2_TIME': {'hour': 13, 'minute': 30, 'second': 0},
            'AMOUNT_LIMIT2': 0.5,
            'TARGET_K1': 0.7,
            'TARGET_K2_TIME': {'hour': 13, 'minute': 0, 'second': 0},
            'TARGET_K2': 0.5,
            'TARGET_K3_TIME': {'hour': 13, 'minute': 30, 'second': 0},
            'TARGET_K3': 0.3,
            'TOTAL_LOSE_EXIT_PCT' : -2.2,
            'POOL_COUNT' : 15
        }

    settings = {}
    try:
        settings['ACCOUNT_AMT'] = config.getint('General', 'ACCOUNT_AMT')
        exclude_list_str = config.get('General', 'EXCLUDE_LIST', fallback='')
        settings['EXCLUDE_LIST'] = [item.strip() for item in exclude_list_str.split(',') if item.strip()] if exclude_list_str else []
        settings['TARGET_BUY_COUNT'] = config.getint('General', 'TARGET_BUY_COUNT')

        def parse_time_setting(config_obj, prefix):
            hour = config_obj.getint('TimeSettings', f'{prefix}_HOUR')
            minute = config_obj.getint('TimeSettings', f'{prefix}_MINUTE')
            second = config_obj.getint('TimeSettings', f'{prefix}_SECOND')
            return {'hour': hour, 'minute': minute, 'second': second}

        settings['T_9_TIME'] = parse_time_setting(config, 'T_9')
        settings['T_START_TIME'] = parse_time_setting(config, 'T_START')
        settings['T_SELL_TIME'] = parse_time_setting(config, 'T_SELL')
        settings['T_EXIT_TIME'] = parse_time_setting(config, 'T_EXIT')
        settings['AMOUNT_LIMIT1_TIME'] = parse_time_setting(config, 'AMOUNT_LIMIT1')
        settings['AMOUNT_LIMIT2_TIME'] = parse_time_setting(config, 'AMOUNT_LIMIT2')
        settings['TARGET_K2_TIME'] = parse_time_setting(config, 'TARGET_K2')
        settings['TARGET_K3_TIME'] = parse_time_setting(config, 'TARGET_K3')

        settings['AMOUNT_TO_BUY'] = config.getfloat('StrategyParameters', 'AMOUNT_TO_BUY')
        settings['STOP_ADD_LOSE_PCT'] = config.getfloat('StrategyParameters', 'STOP_ADD_LOSE_PCT')
        settings['MAX_MOOLING'] = config.getfloat('StrategyParameters', 'MAX_MOOLING')
        settings['SLIPPAGE_LIMIT'] = config.getfloat('StrategyParameters', 'SLIPPAGE_LIMIT')
        settings['STOP_LOSE_PCT'] = config.getfloat('StrategyParameters', 'STOP_LOSE_PCT')
        settings['STOP_TRAILING_REBOUND'] = config.getfloat('StrategyParameters', 'STOP_TRAILING_REBOUND')
        settings['STOP_ABS_LOSE_PCT'] = config.getfloat('StrategyParameters', 'STOP_ABS_LOSE_PCT')
        settings['BREAK_EVEN_PCT1'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_PCT1')
        settings['BREAK_EVEN_LOSE_PCT1'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_LOSE_PCT1')
        settings['BURN_IN_RATIO'] = config.getfloat('StrategyParameters', 'BURN_IN_RATIO')
        settings['BREAK_EVEN_PCT2'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_PCT2')
        settings['BREAK_EVEN_LOSE_PCT2'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_LOSE_PCT2')
        settings['BREAK_EVEN_PCT3'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_PCT3')
        settings['BREAK_EVEN_LOSE_PCT3'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_LOSE_PCT3')
        settings['TAKE_PROFIT_PCT'] = config.getfloat('StrategyParameters', 'TAKE_PROFIT_PCT')
        settings['TAKE_PROFIT_LOSE_PCT'] = config.getfloat('StrategyParameters', 'TAKE_PROFIT_LOSE_PCT')
        settings['AMOUNT_LIMIT1'] = config.getfloat('StrategyParameters', 'AMOUNT_LIMIT1')
        settings['AMOUNT_LIMIT2'] = config.getfloat('StrategyParameters', 'AMOUNT_LIMIT2')
        settings['TARGET_K1'] = config.getfloat('StrategyParameters', 'TARGET_K1')
        settings['TARGET_K2'] = config.getfloat('StrategyParameters', 'TARGET_K2')
        settings['TARGET_K3'] = config.getfloat('StrategyParameters', 'TARGET_K3')
        settings['TOTAL_LOSE_EXIT_PCT'] = config.getfloat('StrategyParameters', 'TOTAL_LOSE_EXIT_PCT')
        settings['POOL_COUNT'] = config.getint('StrategyParameters', 'POOL_COUNT')

    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        send_message(f"❌ 설정 파일 파싱 오류: {e}. 설정 값 확인이 필요합니다.")
        return {
            'ACCOUNT_AMT': 7000000,
            'EXCLUDE_LIST': [],
            'TARGET_BUY_COUNT': 25,
            'T_9_TIME': {'hour': 9, 'minute': 0, 'second': 15},
            'T_START_TIME': {'hour': 9, 'minute': 3, 'second': 0},
            'T_SELL_TIME': {'hour': 14, 'minute': 3, 'second': 0},
            'T_EXIT_TIME': {'hour': 14, 'minute': 8, 'second': 0},
            'AMOUNT_TO_BUY': 350000,
            'STOP_ADD_LOSE_PCT': -5.0,
            'MAX_MOOLING': 5,
            'SLIPPAGE_LIMIT': 1.015,
            'STOP_LOSE_PCT': -3.0,
            'STOP_TRAILING_REBOUND': 1.0,
            'STOP_ABS_LOSE_PCT': -5.0,
            'BREAK_EVEN_PCT1' : 3.0,
            'BREAK_EVEN_LOSE_PCT1' : 2.0,
            'BURN_IN_RATIO' : 0.5,
            'BREAK_EVEN_PCT2' : 5.0,
            'BREAK_EVEN_LOSE_PCT2' : 2.0,
            'BREAK_EVEN_PCT3' : 7.0,
            'BREAK_EVEN_LOSE_PCT3' : 2.0,
            'TAKE_PROFIT_PCT': 9.0,
            'TAKE_PROFIT_LOSE_PCT': 2.0,
            'AMOUNT_LIMIT1_TIME': {'hour': 12, 'minute': 0, 'second': 0},
            'AMOUNT_LIMIT1': 0.7,
            'AMOUNT_LIMIT2_TIME': {'hour': 13, 'minute': 0, 'second': 0},
            'AMOUNT_LIMIT2': 0.5,
            'TARGET_K1': 0.7,
            'TARGET_K2_TIME': {'hour': 12, 'minute': 0, 'second': 0},
            'TARGET_K2': 0.5,
            'TARGET_K3_TIME': {'hour': 13, 'minute': 0, 'second': 0},
            'TARGET_K3': 0.3,
            'TOTAL_LOSE_EXIT_PCT' : -2.2,
            'POOL_COUNT' : 15
        }

    return settings

########################################################
# PostgreSQL insert
########################################################
def save_moving_average_by_date(conn, trade_date):
    """
    trade_date 기준으로 stockmain에 있는 모든 종목의 
    5/10/20/40/60/90/120일 이동평균을 계산하여 stock_ma 테이블에 저장
    :param conn: psycopg2 connection 객체
    :param trade_date: 'YYYYMMDD' 문자열
    """
    # trade_date → datetime.date 변환
    trade_date_obj = pd.to_datetime(trade_date, format='%Y%m%d').date()

    with conn.cursor() as cur:
        # stockmain에서 trade_date 기준으로 모든 종목 조회
        cur.execute("""
            SELECT DISTINCT code
            FROM stockmain
            WHERE trade_date = %s
        """, (trade_date_obj,))
        codes = [row[0] for row in cur.fetchall()]

        if not codes:
            print(f"❌ {trade_date} 기준 stockmain 데이터 없음")
            return

        # 필요한 최근 200일 데이터만 조회 (trade_date 포함)
        cur.execute("""
            SELECT code, trade_date, close_price
            FROM stockmain
            WHERE code = ANY(%s)
            AND trade_date <= %s
            AND trade_date >= %s::date - interval '200 day'
            ORDER BY code, trade_date
        """, (codes, trade_date_obj, trade_date_obj))
        rows = cur.fetchall()

    if not rows:
        print(f"❌ {trade_date} 기준 이동평균 계산용 데이터 없음")
        return

    # DataFrame 변환
    df = pd.DataFrame(rows, columns=['code', 'trade_date', 'close_price'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    # 이동평균 계산 (5, 10, 20, 40, 60, 90, 120)
    ma_days = [5, 10, 20, 40, 60, 90, 120]
    ma_list = []

    for code, group in df.groupby('code'):
        group = group.sort_values('trade_date').copy()
        ma_dict = {'code': code, 'trade_date': trade_date_obj}
        for days in ma_days:
            ma_value = group['close_price'].tail(days).mean() if len(group) >= days else None
            ma_dict[f'ma{days}'] = float(ma_value) if ma_value is not None else None
        ma_list.append(ma_dict)

    ma_df = pd.DataFrame(ma_list)

    # DB 저장용 튜플 (Python 기본 타입으로 변환)
    values = []
    for row in ma_df.itertuples(index=False):
        values.append((
            row.trade_date,
            row.code,
            row.ma5,
            row.ma10,
            row.ma20,
            row.ma40,
            row.ma60,
            row.ma90,
            row.ma120,
        ))

    sql = """
        INSERT INTO stock_ma (
            trade_date, code, ma5, ma10, ma20, ma40, ma60, ma90, ma120
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (trade_date, code) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=1000)
    conn.commit()
    send_message(f"✅ {trade_date} stock_ma 이동평균 저장 완료 ({len(values)} 종목)")
    send_message_main(f"✅ {trade_date} stock_ma 이동평균 저장 완료 ({len(values)} 종목)")

def save_to_postgres(df, trade_date, conn):
    """
    stockmain 테이블에 DataFrame 저장
    """

    # trade_date → Python datetime.date 변환
    trade_date = pd.to_datetime(trade_date, format='%Y%m%d').date()

    # trade_date 컬럼 추가
    df["trade_date"] = trade_date

    # ✅ 숫자 컬럼은 Python 기본 타입으로 변환
    num_cols = ["종가","대비","등락률","시가","고가","저가","거래량","거래대금","시가총액","상장주식수"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(object)

    # DB 저장용 데이터 튜플 변환
    values = [
        (
            row.trade_date,
            str(row.종목코드),
            str(row.종목명),
            float(row.종가) if row.종가 is not None else None,
            float(row.대비) if row.대비 is not None else None,
            float(row.등락률) if row.등락률 is not None else None,
            float(row.시가) if row.시가 is not None else None,
            float(row.고가) if row.고가 is not None else None,
            float(row.저가) if row.저가 is not None else None,
            int(row.거래량) if row.거래량 is not None else None,
            int(row.거래대금) if row.거래대금 is not None else None,
            int(row.시가총액) if row.시가총액 is not None else None,
            int(row.상장주식수) if row.상장주식수 is not None else None,
            str(row.소속부)
        )
        for row in df.itertuples(index=False)
    ]

    # INSERT 구문
    sql = """
        INSERT INTO stockmain (
            trade_date, code, name, close_price, change_price, change_rate,
            open_price, high_price, low_price, volume, trade_value,
            market_cap, shares_out, sector
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (trade_date, code) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=1000)
    conn.commit()
    send_message(f"✅ {trade_date} stockmain 마스터 저장 완료 ({len(values)} 종목)")
    send_message_main(f"✅ {trade_date} stockmain 마스터 저장 완료 ({len(values)} 종목)")

def get_last_trading_day():
    day = datetime.today() - timedelta(days=1)
    while day.weekday() >= 5 or is_holiday(day.strftime("%Y-%m-%d")):
        day -= timedelta(days=1)
    return day.strftime('%Y%m%d')

#--def is_trading_day(trade_date: str) -> bool:
#--    """
#--    YYYYMMDD 문자열 기준으로 거래일 여부 반환
#--    """
#--    dt = datetime.strptime(trade_date, "%Y%m%d")
#--    # 주말 체크 (토요일=5, 일요일=6)
#--    if dt.weekday() >= 5:
#--        return False
#--    # 공휴일 체크
#--    if is_holiday(dt.strftime("%Y-%m-%d")):
#--        return False
#--    return True

# KRX 달력을 한 번만 생성 (함수 호출 시마다 생성하면 느려질 수 있음)
krx_cal = mcal.get_calendar('XKRX')

def is_trading_day(p_date):
    """
    특정 날짜가 장 개장일인지 확인하는 함수
    :param p_date: datetime 객체
    :return: True (개장일), False (휴장일)
    """
    # 날짜를 문자열로 변환 (YYYY-MM-DD)
    target_date = p_date.strftime('%Y-%m-%d')
    
    # 해당 날짜의 스케줄 조회
    schedule = krx_cal.schedule(start_date=target_date, end_date=target_date)
    
    # schedule 데이터프레임이 비어있지 않으면 개장일임
    return not schedule.empty

def fetch_krx_data(mktId, trade_date):
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_form_data = {
        'locale': 'ko_KR',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01501',  # 이 부분이 핵심
        'mktId': mktId,            # 'STK', 'KSQ'
        'trdDd': trade_date,
        'money': '1',              # 원 단위
        'csvxls_isNo': 'false'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'User-Agent': 'Mozilla/5.0'
    }

    print(f"OTP 코드 생성 요청 중... 시장: {mktId}, 날짜: {trade_date}")
    otp_response = requests.post(otp_url, data=otp_form_data, headers=headers)
    if otp_response.status_code != 200:
        print(f"OTP 요청 실패: 상태 코드 {otp_response.status_code}")
        print(otp_response.text)
        return None
    otp_code = otp_response.text

    print(f"CSV 파일 다운로드 중... 시장: {mktId}")
    csv_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    csv_response = requests.post(csv_url, data={'code': otp_code}, headers=headers)
    if csv_response.status_code != 200:
        print(f"CSV 다운로드 실패: 상태 코드 {csv_response.status_code}")
        print(csv_response.text)
        return None

    try:
        df = pd.read_csv(BytesIO(csv_response.content), encoding='euc-kr')
        return df
    except Exception as e:
        print(f"CSV 파싱 오류: {e}")
        return None

def insert_all_symbols(p_trade_date='20250901'):
    trade_date = p_trade_date

    print(f"✅ 거래일은 {trade_date} 입니다.")

    df_kospi = fetch_krx_data('STK', trade_date)
    df_kosdaq = fetch_krx_data('KSQ', trade_date)

    if df_kospi is None and df_kosdaq is None:
        print("❌ KOSPI와 KOSDAQ 데이터 모두 가져오기 실패")
        return []
    elif df_kospi is None:
        df = df_kosdaq
    elif df_kosdaq is None:
        df = df_kospi
    else:
        df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)

    if df is None or df.empty:
        print("❌ 데이터 로드 실패: 데이터프레임이 비어 있습니다.")
        return []

    send_message(f"✅ 전체 종목 수: {len(df)}")
    send_message_main(f"✅ 전체 종목 수: {len(df)}")
    #send_message("\n✅ 열 이름:")
    #send_message(df.columns.tolist()) # ['종목코드', '종목명', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수', '소속부']
    #print("\n✅ 원본 상위 5개 샘플:")
    #print(df.head(5))

    # >>> 이 위치에서 PostgreSQL 저장 호출
    with get_db_connection() as conn:
        save_to_postgres(df, trade_date, conn)  # 함수 내부에서 commit 까지 수행
    # 여기서 자동으로 conn.close() 호출됨

    with get_db_connection() as conn:
        save_moving_average_by_date(conn, trade_date)

def get_all_symbols20(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stock_ma20(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 20일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 20일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols40(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stock_ma40(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 40일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 40일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols60(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stock_ma60(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 60일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 60일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols90(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stock_ma90(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 90일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 90일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols120(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stock_ma120(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 120일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 120일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def save_to_postgres_fdt(df, trade_date, conn):
    """
    KRX 재무지표 (EPS, PER, PBR 등) 데이터를 stockfdt 테이블에 저장
    """

    # ✅ 거래일을 date 타입으로 변환
    trade_date = pd.to_datetime(trade_date, format='%Y%m%d').date()

    # ✅ trade_date 컬럼 추가
    df["trade_date"] = trade_date

    # ✅ 숫자형 컬럼 변환 (NaN → None)
    num_cols = ["종가", "대비", "등락률", "EPS", "PER", "선행 EPS", "선행 PER", "BPS", "PBR", "주당배당금", "배당수익률"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ✅ DB 저장용 데이터 튜플 변환
    values = [
        (
            row.trade_date,
            str(row.종목코드),
            str(row.종목명),
            int(row.종가) if pd.notna(row.종가) else None,
            int(row.대비) if pd.notna(row.대비) else None,
            float(row.등락률) if pd.notna(row.등락률) else None,
            float(row.EPS) if pd.notna(row.EPS) else None,
            float(row.PER) if pd.notna(row.PER) else None,
            float(row._7) if hasattr(row, '_7') else (
                float(row._6) if '선행 EPS' in df.columns else None
            ),  # 안전장치
            float(row._8) if hasattr(row, '_8') else (
                float(row._7) if '선행 PER' in df.columns else None
            ),
            float(row.BPS) if pd.notna(row.BPS) else None,
            float(row.PBR) if pd.notna(row.PBR) else None,
            int(row.주당배당금) if pd.notna(row.주당배당금) else None,
            float(row.배당수익률) if pd.notna(row.배당수익률) else None
        )
        for row in df.itertuples(index=False)
    ]

    # ✅ INSERT SQL
    sql = """
        INSERT INTO stockfdt (
            trade_date, code, name, close_price, change_price, change_rate,
            eps, per, forward_eps, forward_per,
            bps, pbr, dividend_per_share, dividend_yield
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        ON CONFLICT (trade_date, code) DO NOTHING;
    """

    # ✅ DB 저장
    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=1000)
    conn.commit()

    send_message(f"✅ {trade_date} stockfdt 저장 완료 ({len(values)} 종목)")
    send_message_main(f"✅ {trade_date} stockfdt 저장 완료 ({len(values)} 종목)")
    
def fetch_krx_pbr_data(mktId='ALL', trade_date='20250901'):
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
        
def insert_all_symbols_fdt(p_trade_date='20250901'):
    trade_date = p_trade_date

    print(f"✅ 거래일은 {trade_date} 입니다.")

    df_all_fdt = fetch_krx_pbr_data('ALL', trade_date)

    if df_all_fdt is None:
        print("❌ KOSPI와 KOSDAQ 데이터 모두 가져오기 실패")
        return []
    else:
        df = df_all_fdt

    if df is None or df.empty:
        print("❌ 데이터 로드 실패: 데이터프레임이 비어 있습니다.")
        return []

    send_message(f"✅ 전체 종목 수: {len(df)}")
    send_message_main(f"✅ 전체 종목 수: {len(df)}")
    #send_message("\n✅ 열 이름:")
    #send_message(df.columns.tolist()) # ['종목코드', '종목명', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수', '소속부']
    #print("\n✅ 원본 상위 5개 샘플:")
    #print(df.head(5))

    # >>> 이 위치에서 PostgreSQL 저장 호출
    with get_db_connection() as conn:
        save_to_postgres_fdt(df, trade_date, conn)  # 함수 내부에서 commit 까지 수행
    # 여기서 자동으로 conn.close() 호출됨

def save_moving_average_by_date_etf(conn, trade_date):
    """
    trade_date 기준으로 stocketf에 있는 모든 종목의 
    5/10/20/40/60/90/120일 이동평균을 계산하여 stocketf_ma 테이블에 저장
    :param conn: psycopg2 connection 객체
    :param trade_date: 'YYYYMMDD' 문자열
    """
    # trade_date → datetime.date 변환
    trade_date_obj = pd.to_datetime(trade_date, format='%Y%m%d').date()

    with conn.cursor() as cur:
        # stocketf에서 trade_date 기준으로 모든 종목 조회
        cur.execute("""
            SELECT DISTINCT code
            FROM stocketf
            WHERE trade_date = %s
        """, (trade_date_obj,))
        codes = [row[0] for row in cur.fetchall()]

        if not codes:
            print(f"❌ {trade_date} 기준 stocketf 데이터 없음")
            return

        # 필요한 최근 200일 데이터만 조회 (trade_date 포함)
        cur.execute("""
            SELECT code, trade_date, close_price
            FROM stocketf
            WHERE code = ANY(%s)
            AND trade_date <= %s
            AND trade_date >= %s::date - interval '200 day'
            ORDER BY code, trade_date
        """, (codes, trade_date_obj, trade_date_obj))
        rows = cur.fetchall()

    if not rows:
        print(f"❌ {trade_date} 기준 이동평균 계산용 데이터 없음")
        return

    # DataFrame 변환
    df = pd.DataFrame(rows, columns=['code', 'trade_date', 'close_price'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])

    # 이동평균 계산 (5, 10, 20, 40, 60, 90, 120)
    ma_days = [5, 10, 20, 40, 60, 90, 120]
    ma_list = []

    for code, group in df.groupby('code'):
        group = group.sort_values('trade_date').copy()
        ma_dict = {'code': code, 'trade_date': trade_date_obj}
        for days in ma_days:
            ma_value = group['close_price'].tail(days).mean() if len(group) >= days else None
            ma_dict[f'ma{days}'] = float(ma_value) if ma_value is not None else None
        ma_list.append(ma_dict)

    ma_df = pd.DataFrame(ma_list)

    # DB 저장용 튜플 (Python 기본 타입으로 변환)
    values = []
    for row in ma_df.itertuples(index=False):
        values.append((
            row.trade_date,
            row.code,
            row.ma5,
            row.ma10,
            row.ma20,
            row.ma40,
            row.ma60,
            row.ma90,
            row.ma120,
        ))

    sql = """
        INSERT INTO stocketf_ma (
            trade_date, code, ma5, ma10, ma20, ma40, ma60, ma90, ma120
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (trade_date, code) DO NOTHING
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=1000)
    conn.commit()
    send_message(f"✅ {trade_date} stocketf_ma 이동평균 저장 완료 ({len(values)} 종목)")
    send_message_main(f"✅ {trade_date} stocketf_ma 이동평균 저장 완료 ({len(values)} 종목)")

def save_to_postgres_etf(df, trade_date, conn):
    """
    stocketf 테이블에 DataFrame 저장
    """

    # trade_date → Python datetime.date 변환
    trade_date = pd.to_datetime(trade_date, format='%Y%m%d').date()

    # trade_date 컬럼 추가
    df["trade_date"] = trade_date

    # ✅ 숫자 컬럼은 Python 기본 타입으로 변환
    num_cols = ["종가","대비","등락률","시가","고가","저가","거래량","거래대금"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(object)

    # DB 저장용 데이터 튜플 변환
    values = [
        (
            row.trade_date,
            str(row.종목코드),
            str(row.종목명),
            float(row.종가) if row.종가 is not None else None,
            float(row.대비) if row.대비 is not None else None,
            float(row.등락률) if row.등락률 is not None else None,
            float(row.시가) if row.시가 is not None else None,
            float(row.고가) if row.고가 is not None else None,
            float(row.저가) if row.저가 is not None else None,
            int(row.거래량) if row.거래량 is not None else None,
            int(row.거래대금) if row.거래대금 is not None else None
        )
        for row in df.itertuples(index=False)
    ]

    # INSERT 구문
    sql = """
        INSERT INTO stocketf (
            trade_date, code, name, close_price, change_price, change_rate,
            open_price, high_price, low_price, volume, trade_value
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (trade_date, code) DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=1000)
    conn.commit()
    send_message(f"✅ {trade_date} stocketf 마스터 저장 완료 ({len(values)} 종목)")
    send_message_main(f"✅ {trade_date} stocketf 마스터 저장 완료 ({len(values)} 종목)")

def fetch_etf_data(mktId, trade_date):
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_form_data = {
        "locale": "ko_KR",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT04301",  # 올바른 ETF URL
        "trdDd": trade_date,
        "etfTabGubun": "1",
        "money": "1",
        "csvxls_isNo": "false"
    }
    headers = {
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201040101",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"OTP 코드 생성 요청 중... 시장: {mktId}, 날짜: {trade_date}")
    otp_response = requests.post(otp_url, data=otp_form_data, headers=headers)
    if otp_response.status_code != 200:
        print(f"OTP 요청 실패: 상태 코드 {otp_response.status_code}")
        print(otp_response.text)
        return None
    otp_code = otp_response.text

    print(f"CSV 파일 다운로드 중... 시장: {mktId}")
    csv_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    csv_response = requests.post(csv_url, data={'code': otp_code}, headers=headers)
    if csv_response.status_code != 200:
        print(f"CSV 다운로드 실패: 상태 코드 {csv_response.status_code}")
        print(csv_response.text)
        return None

    try:
        df = pd.read_csv(BytesIO(csv_response.content), encoding='euc-kr')

        # 필요한 컬럼만 선택
        required_columns = ['종목코드', '종목명', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금']
        available_columns = [col for col in required_columns if col in df.columns]
        
        if not available_columns:
            print("❌ 필요한 컬럼을 찾을 수 없습니다.")
            print(f"사용 가능한 컬럼: {list(df.columns)}")
            return None

        # 필요한 컬럼만 추출
        result_df = df[available_columns].copy()
        return result_df

    except Exception as e:
        print(f"CSV 파싱 오류: {e}")
        return None

def insert_all_symbols_etf(p_trade_date='20250901'):
    trade_date = p_trade_date

    print(f"✅ 거래일은 {trade_date} 입니다.")

    df_etf = fetch_etf_data('ETF', trade_date)

    if df_etf is None:
        print("❌ ETF 데이터 가져오기 실패")
        return []
    else:
        df = df_etf

    if df is None or df.empty:
        print("❌ 데이터 로드 실패: 데이터프레임이 비어 있습니다.")
        return []

    send_message(f"✅ ETF 전체 종목 수: {len(df)}")
    send_message_main(f"✅ ETF 전체 종목 수: {len(df)}")
    #send_message("\n✅ 열 이름:")
    #send_message(df.columns.tolist()) # ['종목코드', '종목명', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수', '소속부']
    #print("\n✅ 원본 상위 5개 샘플:")
    #print(df.head(5))

    # >>> 이 위치에서 PostgreSQL 저장 호출
    with get_db_connection() as conn:
        save_to_postgres_etf(df, trade_date, conn)  # 함수 내부에서 commit 까지 수행
    # 여기서 자동으로 conn.close() 호출됨

    with get_db_connection() as conn:
        save_moving_average_by_date_etf(conn, trade_date)

def get_all_symbols20_etf(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stocketf_ma20(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 20일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 20일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols40_etf(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stocketf_ma40(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 40일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 40일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols60_etf(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stocketf_ma60(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 60일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 60일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols90_etf(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stocketf_ma90(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 90일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 90일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols120_etf(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stocketf_ma120(%s, %s);
                """
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 120일 이평 매수종목 반환")
        #send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 120일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        #send_message_main(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

if __name__ == "__main__":
    trade_date_p = datetime.now()
    trade_date = datetime.now().strftime('%Y%m%d')
    #trade_date = '20251017'

    settings = load_settings()
    AMOUNT_TO_BUY = settings['AMOUNT_TO_BUY']
    MAX_BUY_PRICE = AMOUNT_TO_BUY

    # daily stockfdt insert ****************************************************************
    if is_trading_day(trade_date_p):
        insert_all_symbols_fdt(p_trade_date=trade_date)
    else:
        send_message(f"⏩ {trade_date}는 거래일이 아니므로 stockfdt insert 처리 스킵")
        send_message_main(f"⏩ {trade_date}는 거래일이 아니므로 stockfdt insert 처리 스킵")

    # daily stockmain,stock_ma insert ****************************************************************
    if is_trading_day(trade_date_p):
        insert_all_symbols(p_trade_date=trade_date)

        symbols_buy_pool20 = get_all_symbols20(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 20
        symbols_buy_pool40 = get_all_symbols40(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 40
        symbols_buy_pool60 = get_all_symbols60(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 60
        symbols_buy_pool90 = get_all_symbols90(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 90
        symbols_buy_pool120 = get_all_symbols120(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 120
        symbols_buy_pool = {
            **symbols_buy_pool20,
            **symbols_buy_pool40,
            **symbols_buy_pool60,
            **symbols_buy_pool90,
            **symbols_buy_pool120
        }
        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_buy_pool)}건 이평 매수종목 반환")
        send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_buy_pool)}건 이평 매수종목 반환")
        send_message(symbols_buy_pool)
        send_message_main(symbols_buy_pool)
    else:
        send_message(f"⏩ {trade_date}는 거래일이 아니므로 stockmain/stock_ma insert 처리 스킵")
        send_message_main(f"⏩ {trade_date}는 거래일이 아니므로 stockmain/stock_ma insert 처리 스킵")

    # daily stocketf,stocketf_ma insert ****************************************************************
    if is_trading_day(trade_date_p):
        insert_all_symbols_etf(p_trade_date=trade_date)

        symbols_buy_pool20_etf = get_all_symbols20_etf(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 20
        symbols_buy_pool40_etf = get_all_symbols40_etf(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 40
        symbols_buy_pool60_etf = get_all_symbols60_etf(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 60
        symbols_buy_pool90_etf = get_all_symbols90_etf(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 90
        symbols_buy_pool120_etf = get_all_symbols120_etf(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)  # 금일 매수 종목 120
        symbols_buy_pool_etf = {
            **symbols_buy_pool20_etf,            
            **symbols_buy_pool40_etf,
            **symbols_buy_pool60_etf,
            **symbols_buy_pool90_etf,
            **symbols_buy_pool120_etf
        }
        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_buy_pool_etf)}건 이평 매수종목 반환")
        send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_buy_pool_etf)}건 이평 매수종목 반환")
        send_message(symbols_buy_pool_etf)
        send_message_main(symbols_buy_pool_etf)
    else:
        send_message(f"⏩ {trade_date}는 거래일이 아니므로 stocketf/stocketf_ma insert 처리 스킵")
        send_message_main(f"⏩ {trade_date}는 거래일이 아니므로 stocketf/stocketf_ma insert 처리 스킵")
