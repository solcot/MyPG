import requests
from requests.exceptions import ConnectTimeout, ReadTimeout, Timeout, ConnectionError
import json
import time
import yaml
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from holidayskr import is_holiday
import configparser
import os
import psycopg2
from psycopg2.extras import execute_batch
import pandas_market_calendars as mcal
import warnings

# Selenium 관련 임포트
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

warnings.filterwarnings('ignore', category=UserWarning)

# =========================================================
# 설정 파일 로드
# =========================================================
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
    """디스코드 메세지 전송 (Main 채널)"""
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    try:
        requests.post(DISCORD_WEBHOOK_URL_MAIN, data=message, timeout=5)
    except Exception as e:
        print(f"❌ Discord 전송 실패: {e}", flush=True)

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
        return {'ACCOUNT_AMT': 7000000} # 기본값

    settings = {}
    try:
        settings['ACCOUNT_AMT'] = config.getint('General', 'ACCOUNT_AMT')
        exclude_list_str = config.get('General', 'EXCLUDE_LIST', fallback='')
        settings['EXCLUDE_LIST'] = [item.strip() for item in exclude_list_str.split(',') if item.strip()] if exclude_list_str else []
        settings['TARGET_BUY_COUNT'] = config.getint('General', 'TARGET_BUY_COUNT')

        # TimeSettings 및 StrategyParameters 파싱
        # (기존 로직 유지)
        settings['AMOUNT_TO_BUY'] = config.getfloat('StrategyParameters', 'AMOUNT_TO_BUY')
        # ... 필요한 다른 설정들 ...
        
    except Exception as e:
        send_message(f"❌ 설정 파일 파싱 오류: {e}")
        settings['ACCOUNT_AMT'] = 7000000 # Fallback
        settings['AMOUNT_TO_BUY'] = 350000.0

    return settings

# =================================================================================
# [핵심] 로그인 세션 생성 함수 (한 번만 실행)
# =================================================================================
def get_authenticated_session():
    """
    Selenium을 이용하여 반자동 로그인을 수행하고,
    인증된 requests.Session 객체를 반환합니다.
    """
    print("\n" + "="*70)
    print("🚀 [로그인 프로세스 시작] 브라우저가 열리면 로그인을 진행해주세요.")
    print("="*70)

    # 1. Selenium 옵션
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1280,800")
    
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    chrome_options.add_argument(f'user-agent={user_agent}')

    # 크롬 바이너리 위치 자동 찾기
    path_candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
    ]
    for path in path_candidates:
        if os.path.exists(path):
            chrome_options.binary_location = path
            break

    driver = None
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # 2. KRX 페이지 접속 (로그인 유도용 - PER 화면)
        target_url = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'
        driver.get(target_url)
        time.sleep(3) 

        # 3. 알림창 처리
        try:
            driver.switch_to.alert.accept()
        except:
            pass

        print("\n" + "="*60)
        print("🛑 [사용자 개입 필요]")
        print("   1. 열린 크롬 창에서 '로그인' 버튼을 눌러 로그인을 완료하세요.")
        print("   2. 로그인이 완료되어 화면이 정상적으로 보이면,")
        print("   👉 여기 터미널에서 [Enter] 키를 누르세요.")
        print("="*60 + "\n")
        input("⌨️ 로그인을 완료했다면 엔터를 누르세요...")

        # 4. 쿠키 추출 및 세션 생성
        sess = requests.Session()
        selenium_cookies = driver.get_cookies()
        for cookie in selenium_cookies:
            sess.cookies.set(cookie['name'], cookie['value'])
        
        # 기본 헤더 설정
        sess.headers.update({'User-Agent': user_agent})
        
        print("✅ 인증된 세션 확보 완료! 브라우저를 종료합니다.")
        return sess

    except Exception as e:
        print(f"❌ 로그인 세션 생성 실패: {e}")
        return None
    finally:
        if driver:
            driver.quit()

# =================================================================================
# 데이터 수집 함수 (Session 인자 사용)
# =================================================================================

def fetch_krx_pbr_data(date_str, session):
    """
    [PER/PBR 데이터]
    내부적으로 STK(코스피)와 KSQ(코스닥)을 각각 조회하여 합친 뒤 반환합니다.
    (KONEX 제외 목적)
    """
    # Referer 설정
    session.headers.update({'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'})

    # 가져올 시장 리스트 (KONEX 제외)
    target_markets = ['STK', 'KSQ'] 
    dfs = []

    for mkt in target_markets:
        print(f"DEBUG: PBR 데이터 요청 중... ({mkt})")
        
        otp_params = {
            'locale': 'ko_KR',
            'mktId': mkt,      # 'ALL' 대신 'STK', 'KSQ' 순차 대입
            'trdDd': date_str,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03501'
        }
        
        try:
            # 1. OTP 요청
            otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
            otp_code = session.post(otp_url, data=otp_params).text.strip()

            if "LOGOUT" in otp_code or "error" in otp_code.lower():
                print(f"❌ PBR OTP 실패 (LOGOUT) - Market: {mkt}")
                continue # 다음 시장으로 넘어감

            # 2. 다운로드 요청
            down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
            res = session.post(down_url, data={'code': otp_code})

            # 3. DataFrame 변환 및 리스트 추가
            df_part = pd.read_csv(BytesIO(res.content), encoding='euc-kr')
            dfs.append(df_part)
            
            # 너무 빠른 연속 요청 방지
            time.sleep(0.5)

        except Exception as e:
            print(f"❌ PBR 데이터 요청 중 에러({mkt}): {e}")
            continue

    # 두 시장의 데이터를 합쳐서 반환
    if dfs:
        result_df = pd.concat(dfs, ignore_index=True)
        return result_df
    else:
        return None

def fetch_krx_data(trade_date, session):
    """
    [KOSPI/KOSDAQ 전종목 시세 데이터]
    내부적으로 STK(코스피)와 KSQ(코스닥)을 각각 조회하여 합친 뒤 반환합니다.
    (KONEX 자동 제외)
    """
    # Referer 설정
    session.headers.update({'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101'})

    target_markets = ['STK', 'KSQ']
    dfs = []

    for mkt in target_markets:
        print(f"DEBUG: 시세 데이터 요청 중... ({mkt})") 
        
        otp_params = {
            'locale': 'ko_KR',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'mktId': mkt,       # STK, KSQ 순차 대입
            'trdDd': trade_date,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false'
        }

        try:
            # 1. OTP 요청
            otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
            otp_code = session.post(otp_url, data=otp_params).text.strip()

            if "LOGOUT" in otp_code or "error" in otp_code.lower():
                print(f"❌ 시세 OTP 실패 (LOGOUT) - Market: {mkt}")
                continue

            # 2. 다운로드 요청
            down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
            csv_response = session.post(down_url, data={'code': otp_code})
            
            # 3. 데이터프레임 변환
            df_part = pd.read_csv(BytesIO(csv_response.content), encoding='euc-kr')
            dfs.append(df_part)
            
            time.sleep(0.5) # 서버 부하 방지용 딜레이

        except Exception as e:
            print(f"❌ 시세 데이터 요청 중 에러({mkt}): {e}")
            continue

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return None

# =================================================================================
# Insert Controller 함수들 (Session 인자 추가)
# =================================================================================

def insert_all_symbols_fdt(p_trade_date, session):
    trade_date = p_trade_date
    print(f"✅ [FDT] 거래일: {trade_date} 데이터 수집 시작")

    df = fetch_krx_pbr_data(trade_date, session)

    if df is None or df.empty:
        print("❌ FDT 데이터 로드 실패")
        return

    send_message(f"✅ FDT 종목 수: {len(df)}")
    send_message_main(f"✅ FDT 종목 수: {len(df)}")

    with get_db_connection() as conn:
        save_to_postgres_fdt(df, trade_date, conn)

def insert_all_symbols(trade_date, session):
    print(f"✅ [StockMain] 거래일: {trade_date} 데이터 수집 시작")

    # 1. 내부에서 STK+KSQ만 합쳐서 가져옴
    df = fetch_krx_data(trade_date, session)

    if df is None or df.empty:
        print("❌ StockMain 데이터 로드 실패 (데이터 없음)")
        return

    # (이전의 KONEX 필터링 로직 삭제됨 - 이제 필요 없음)

    send_message(f"✅ StockMain 전체 종목 수: {len(df)}")
    send_message_main(f"✅ StockMain 전체 종목 수: {len(df)}")

    # 2. DB 저장
    with get_db_connection() as conn:
        save_to_postgres(df, trade_date, conn)
    
    # 3. 이평선 계산
    with get_db_connection() as conn:
        save_moving_average_by_date(conn, trade_date)

# =================================================================================
# DB 저장 및 계산 함수들 (기존 로직 유지)
# =================================================================================

def save_moving_average_by_date(conn, trade_date):
    """
    trade_date 기준으로 stockmain에 있는 모든 종목의 
    5/10/20/40/60/90/120일 이동평균을 계산하여 stock_ma 테이블에 저장
    """
    trade_date_obj = pd.to_datetime(trade_date, format='%Y%m%d').date()

    with conn.cursor() as cur:
        # stockmain에서 trade_date 기준으로 모든 종목 조회
        cur.execute("SELECT DISTINCT code FROM stockmain WHERE trade_date = %s", (trade_date_obj,))
        codes = [row[0] for row in cur.fetchall()]

        if not codes:
            print(f"❌ {trade_date} 기준 stockmain 데이터 없음")
            return

        # 필요한 최근 200일 데이터만 조회
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
        return

    df = pd.DataFrame(rows, columns=['code', 'trade_date', 'close_price'])
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    # 종가 데이터를 미리 float으로 변환하여 NumPy 타입 이슈 방지
    df['close_price'] = df['close_price'].astype(float)

    ma_days = [5, 10, 20, 40, 60, 90, 120]
    values = []

    # 종목별 Loop
    for code, group in df.groupby('code'):
        group = group.sort_values('trade_date')
        
        # 오늘 날짜 데이터가 마지막에 있어야 함
        if group.iloc[-1]['trade_date'].date() != trade_date_obj:
            continue

        ma_vals = {}
        for days in ma_days:
            if len(group) >= days:
                # [핵심 수정] .mean() 결과를 float()으로 명시적 형변환
                val = group['close_price'].tail(days).mean()
                ma_vals[days] = float(val)
            else:
                ma_vals[days] = None
        
        values.append((
            trade_date_obj, code,
            ma_vals[5], ma_vals[10], ma_vals[20], 
            ma_vals[40], ma_vals[60], ma_vals[90], ma_vals[120]
        ))

    sql = """
        INSERT INTO stock_ma (trade_date, code, ma5, ma10, ma20, ma40, ma60, ma90, ma120)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_date, code) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=1000)
    conn.commit()
    send_message(f"✅ {trade_date} stock_ma 이동평균 저장 완료 ({len(values)} 종목)")
    send_message_main(f"✅ {trade_date} stock_ma 이동평균 저장 완료 ({len(values)} 종목)")

def save_to_postgres(df, trade_date, conn):
    """stockmain 테이블에 DataFrame 저장"""
    trade_date = pd.to_datetime(trade_date, format='%Y%m%d').date()
    df["trade_date"] = trade_date

    num_cols = ["종가","대비","등락률","시가","고가","저가","거래량","거래대금","시가총액","상장주식수"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype(object)

    values = [
        (
            row.trade_date, str(row.종목코드), str(row.종목명),
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
        ) for row in df.itertuples(index=False)
    ]

    sql = """
        INSERT INTO stockmain (
            trade_date, code, name, close_price, change_price, change_rate,
            open_price, high_price, low_price, volume, trade_value,
            market_cap, shares_out, sector
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_date, code) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=1000)
    conn.commit()
    send_message(f"✅ {trade_date} stockmain 마스터 저장 완료 ({len(values)} 종목)")

def save_to_postgres_fdt(df, trade_date, conn):
    """stockfdt 테이블에 재무지표 저장"""
    trade_date = pd.to_datetime(trade_date, format='%Y%m%d').date()
    df["trade_date"] = trade_date

    num_cols = ["종가", "대비", "등락률", "EPS", "PER", "선행 EPS", "선행 PER", "BPS", "PBR", "주당배당금", "배당수익률"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    values = []
    for row in df.itertuples(index=False):
        # 선행 지표 컬럼명 처리 (KRX csv 컬럼명이 _숫자로 올 때가 있음)
        f_eps = getattr(row, '_6', None) if '선행 EPS' in df.columns else None
        if hasattr(row, '_7'): f_eps = row._7
        
        f_per = getattr(row, '_7', None) if '선행 PER' in df.columns else None
        if hasattr(row, '_8'): f_per = row._8

        values.append((
            row.trade_date, str(row.종목코드), str(row.종목명),
            int(row.종가) if pd.notna(row.종가) else None,
            int(row.대비) if pd.notna(row.대비) else None,
            float(row.등락률) if pd.notna(row.등락률) else None,
            float(row.EPS) if pd.notna(row.EPS) else None,
            float(row.PER) if pd.notna(row.PER) else None,
            float(f_eps) if f_eps else None,
            float(f_per) if f_per else None,
            float(row.BPS) if pd.notna(row.BPS) else None,
            float(row.PBR) if pd.notna(row.PBR) else None,
            int(row.주당배당금) if pd.notna(row.주당배당금) else None,
            float(row.배당수익률) if pd.notna(row.배당수익률) else None
        ))

    sql = """
        INSERT INTO stockfdt (
            trade_date, code, name, close_price, change_price, change_rate,
            eps, per, forward_eps, forward_per,
            bps, pbr, dividend_per_share, dividend_yield
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_date, code) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, values, page_size=1000)
    conn.commit()
    send_message(f"✅ {trade_date} stockfdt 저장 완료 ({len(values)} 종목)")

def is_trading_day(p_date):
    """장 개장일 여부 확인"""
    # 전역 변수 krx_cal 사용 (Main에서 초기화됨)
    target_date = p_date.strftime('%Y-%m-%d')
    schedule = krx_cal.schedule(start_date=target_date, end_date=target_date)
    return not schedule.empty

# =================================================================================
# 매수 종목 Pool 조회 함수들
# =================================================================================

def get_all_symbols20(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = "select * from get_stock_ma20(%s, %s);"
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()
                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}
        
        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 20일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        return symbols_name_dict
    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols40(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = "select * from get_stock_ma40(%s, %s);"
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()
                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}
        
        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 40일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        return symbols_name_dict
    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols60(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = "select * from get_stock_ma60(%s, %s);"
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()
                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}
        
        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 60일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        return symbols_name_dict
    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols90(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = "select * from get_stock_ma90(%s, %s);"
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()
                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}
        
        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 90일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        return symbols_name_dict
    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

def get_all_symbols120(p_trade_date='20250901', p_max_price=500000):
    trade_date = p_trade_date
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = "select * from get_stock_ma120(%s, %s);"
                cur.execute(sql, (trade_date,p_max_price))
                rows = cur.fetchall()
                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}
        
        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 120일 이평 매수종목 반환")
        send_message(symbols_name_dict)
        return symbols_name_dict
    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}


# =================================================================================
# Main Execution
# =================================================================================
if __name__ == "__main__":
    trade_date_p = datetime.now()
    trade_date = trade_date_p.strftime('%Y%m%d')
    # trade_date = '20260105' # 테스트시 주석 해제

    settings = load_settings()
    AMOUNT_TO_BUY = settings['AMOUNT_TO_BUY']
    MAX_BUY_PRICE = AMOUNT_TO_BUY
    
    krx_cal = mcal.get_calendar('XKRX') # 캘린더 초기화

    if is_trading_day(trade_date_p):
        
        # 1. [핵심] 통합 로그인 수행 (여기서 딱 한 번 로그인)
        session = get_authenticated_session()

        if session is not None:
            # 2. 데이터 수집 및 저장 (로그인된 세션 전달)
            insert_all_symbols_fdt(trade_date, session)
            insert_all_symbols(trade_date, session)
            # insert_all_symbols_etf는 제거되었습니다.
            
            # 3. 매수 풀 계산 (DB 조회 로직)
            symbols_buy_pool20 = get_all_symbols20(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)
            symbols_buy_pool40 = get_all_symbols40(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)
            symbols_buy_pool60 = get_all_symbols60(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)
            symbols_buy_pool90 = get_all_symbols90(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)
            symbols_buy_pool120 = get_all_symbols120(p_trade_date=trade_date, p_max_price=MAX_BUY_PRICE)
            
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
            print("❌ 로그인을 하지 못해 작업을 중단합니다.")
            
    else:
        send_message(f"⏩ {trade_date}는 거래일이 아니므로 처리를 스킵합니다.")
        send_message_main(f"⏩ {trade_date}는 거래일이 아니므로 처리를 스킵합니다.")


