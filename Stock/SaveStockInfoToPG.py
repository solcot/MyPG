import requests
from requests.exceptions import ConnectTimeout, ReadTimeout, Timeout, ConnectionError
import json
import time
import yaml
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
# from holidayskr import is_holiday # 필요시 주석 해제
import configparser
import os
import psycopg2
from psycopg2.extras import execute_batch
import pandas_market_calendars as mcal
import warnings
import pickle  # [추가] 쿠키 저장을 위한 모듈

# Selenium 관련 임포트
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By # [추가] 명시적 대기 등을 위해 필요할 수 있음

warnings.filterwarnings('ignore', category=UserWarning)

# =========================================================
# 설정 파일 로드
# =========================================================
# 경로가 다르다면 본인 환경에 맞게 수정해주세요.
try:
    with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
        _cfg = yaml.load(f, Loader=yaml.FullLoader)
    DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
    DISCORD_WEBHOOK_URL_MAIN = _cfg['DISCORD_WEBHOOK_URL_MAIN']
    HOST = _cfg['HOST']
    DBNAME = _cfg['DBNAME']
    USER = _cfg['USER']
    PASSWORD = _cfg['PASSWORD']
except Exception as e:
    print(f"⚠️ 설정 파일 로드 실패 (기본값 사용 불가): {e}")
    # 테스트를 위해 임시 변수 처리 (실제 환경에선 위에서 에러나면 종료 권장)
    DISCORD_WEBHOOK_URL = ""
    HOST = ""

def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    if DISCORD_WEBHOOK_URL:
        try:
            requests.post(DISCORD_WEBHOOK_URL, data=message, timeout=5)
        except Exception as e:
            print(f"❌ Discord 전송 실패: {e}", flush=True)
    print(message, flush=True)

def send_message_main(msg):
    """디스코드 메세지 전송 (Main 채널)"""
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    if DISCORD_WEBHOOK_URL_MAIN:
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
        return {'ACCOUNT_AMT': 7000000} 

    settings = {}
    try:
        settings['ACCOUNT_AMT'] = config.getint('General', 'ACCOUNT_AMT', fallback=7000000)
        exclude_list_str = config.get('General', 'EXCLUDE_LIST', fallback='')
        settings['EXCLUDE_LIST'] = [item.strip() for item in exclude_list_str.split(',') if item.strip()] if exclude_list_str else []
        settings['TARGET_BUY_COUNT'] = config.getint('General', 'TARGET_BUY_COUNT', fallback=10)
        settings['AMOUNT_TO_BUY'] = config.getfloat('StrategyParameters', 'AMOUNT_TO_BUY', fallback=350000.0)
    except Exception as e:
        send_message(f"❌ 설정 파일 파싱 오류: {e}")
        settings['ACCOUNT_AMT'] = 7000000 
        settings['AMOUNT_TO_BUY'] = 350000.0

    return settings

# =================================================================================
# DB 저장 및 계산 함수들 (덮어쓰기 모드로 수정됨)
# =================================================================================

def save_moving_average_by_date(conn, trade_date):
    """
    [수정됨] 해당 날짜의 기존 이평선 데이터를 삭제 후 다시 계산하여 저장
    """
    trade_date_obj = pd.to_datetime(trade_date, format='%Y%m%d').date()

    with conn.cursor() as cur:
        # 1. stockmain에서 데이터 조회 (이전 로직 동일)
        cur.execute("SELECT DISTINCT code FROM stockmain WHERE trade_date = %s", (trade_date_obj,))
        codes = [row[0] for row in cur.fetchall()]

        if not codes:
            print(f"❌ {trade_date} 기준 stockmain 데이터 없음")
            return

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
    df['close_price'] = df['close_price'].astype(float)

    ma_days = [5, 10, 20, 40, 60, 90, 120]
    values = []

    for code, group in df.groupby('code'):
        group = group.sort_values('trade_date')
        
        if group.iloc[-1]['trade_date'].date() != trade_date_obj:
            continue

        ma_vals = {}
        for days in ma_days:
            if len(group) >= days:
                val = group['close_price'].tail(days).mean()
                ma_vals[days] = float(val)
            else:
                ma_vals[days] = None
        
        values.append((
            trade_date_obj, code,
            ma_vals[5], ma_vals[10], ma_vals[20], 
            ma_vals[40], ma_vals[60], ma_vals[90], ma_vals[120]
        ))

    # [핵심 수정] DELETE 후 INSERT
    with conn.cursor() as cur:
        # 1. 기존 데이터 삭제
        cur.execute("DELETE FROM stock_ma WHERE trade_date = %s", (trade_date_obj,))
        
        # 2. 데이터 삽입 (ON CONFLICT 제거)
        sql = """
            INSERT INTO stock_ma (trade_date, code, ma5, ma10, ma20, ma40, ma60, ma90, ma120)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cur, sql, values, page_size=1000)
        
    conn.commit()
    send_message(f"✅ {trade_date} stock_ma 이동평균 재계산 및 덮어쓰기 완료 ({len(values)} 종목)")
    send_message_main(f"✅ {trade_date} stock_ma 이동평균 재계산 및 덮어쓰기 완료 ({len(values)} 종목)")

def save_to_postgres(df, trade_date, conn):
    """
    [수정됨] 해당 날짜의 stockmain 데이터를 모두 삭제 후 Insert
    """
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

    # [핵심 수정] DELETE 후 INSERT
    with conn.cursor() as cur:
        # 1. 해당 날짜 데이터 전체 삭제
        cur.execute("DELETE FROM stockmain WHERE trade_date = %s", (trade_date,))
        print(f"🗑️ {trade_date} stockmain 기존 데이터 삭제 완료")

        # 2. 데이터 삽입 (ON CONFLICT 제거)
        sql = """
            INSERT INTO stockmain (
                trade_date, code, name, close_price, change_price, change_rate,
                open_price, high_price, low_price, volume, trade_value,
                market_cap, shares_out, sector
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cur, sql, values, page_size=1000)
        
    conn.commit()
    send_message(f"✅ {trade_date} stockmain 덮어쓰기 완료 ({len(values)} 종목)")

def save_to_postgres_fdt(df, trade_date, conn):
    """
    [수정됨] 해당 날짜의 stockfdt 데이터를 모두 삭제 후 Insert
    """
    trade_date = pd.to_datetime(trade_date, format='%Y%m%d').date()
    df["trade_date"] = trade_date

    num_cols = ["종가", "대비", "등락률", "EPS", "PER", "선행 EPS", "선행 PER", "BPS", "PBR", "주당배당금", "배당수익률"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    values = []
    for row in df.itertuples(index=False):
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

    # [핵심 수정] DELETE 후 INSERT
    with conn.cursor() as cur:
        # 1. 해당 날짜 데이터 전체 삭제
        cur.execute("DELETE FROM stockfdt WHERE trade_date = %s", (trade_date,))
        print(f"🗑️ {trade_date} stockfdt 기존 데이터 삭제 완료")

        # 2. 데이터 삽입 (ON CONFLICT 제거)
        sql = """
            INSERT INTO stockfdt (
                trade_date, code, name, close_price, change_price, change_rate,
                eps, per, forward_eps, forward_per,
                bps, pbr, dividend_per_share, dividend_yield
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cur, sql, values, page_size=1000)
        
    conn.commit()
    send_message(f"✅ {trade_date} stockfdt 덮어쓰기 완료 ({len(values)} 종목)")

# =================================================================================
# [핵심] 로그인 세션 생성 함수 (자동 복구 기능 포함)
# =================================================================================
def get_authenticated_session():
    """
    1. 'krx_session.pkl' 로드 시도 및 유효성 검사.
    2. 유효하면 즉시 세션 반환.
    3. 파일이 없거나, 로드 중 에러가 나거나, 유효성 검사 실패 시(세션 만료)
       -> 자동으로 Selenium 브라우저를 띄워 재로그인 프로세스로 진입.
    """
    cookie_filename = 'krx_session.pkl'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    
    sess = requests.Session()
    sess.headers.update({'User-Agent': user_agent})
    
    # -------------------------------------------------------
    # 1. 저장된 쿠키 로드 및 유효성 테스트
    # -------------------------------------------------------
    need_login = True  # 기본적으로 로그인이 필요하다고 가정

    if os.path.exists(cookie_filename):
        print(f"📂 저장된 세션 파일('{cookie_filename}') 발견. 유효성 검사 중...")
        try:
            with open(cookie_filename, 'rb') as f:
                cookies = pickle.load(f)
                sess.cookies.update(cookies)
            
            # 테스트 요청 (가벼운 마이페이지 혹은 메뉴 호출)
            test_url = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'
            res = sess.get(test_url, timeout=5)
            
            # KRX는 세션 만료 시 보통 200 OK를 주더라도 내용물에 '로그인' 버튼이 생기거나
            # 리다이렉트 스크립트가 포함됨. 
            # 여기서는 간단히 길이가 너무 짧거나(에러 페이지), 특정 키워드가 없으면 만료로 판단.
            if res.status_code == 200 and "MDC" in res.text and len(res.text) > 2000:
                print("✅ 저장된 세션이 유효합니다! 자동 로그인 성공.")
                need_login = False  # 로그인 불필요
                return sess
            else:
                print("⚠️ 저장된 세션이 만료되었습니다. (재로그인 필요)")
        except Exception as e:
            print(f"⚠️ 세션 로드 중 오류 발생({e}). 재로그인을 진행합니다.")
    else:
        print("ℹ️ 저장된 세션 파일이 없습니다. 새 로그인을 진행합니다.")

    # -------------------------------------------------------
    # 2. Selenium으로 수동 로그인 진행 (need_login이 True일 때만 실행)
    # -------------------------------------------------------
    if need_login:
        print("\n" + "="*70)
        print("🚀 [로그인 갱신 필요] 브라우저가 열리면 로그인을 진행해주세요.")
        print("="*70)

        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1280,800")
        chrome_options.add_argument(f'user-agent={user_agent}')

        # 크롬 바이너리 위치
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

            # 로그인 화면 접속
            target_url = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'
            driver.get(target_url)
            time.sleep(3) 

            # 팝업 닫기
            try:
                driver.switch_to.alert.accept()
            except:
                pass

            print("\n" + "="*60)
            print("🛑 [사용자 개입 필요]")
            print("   1. 열린 크롬 창에서 '로그인' 버튼을 눌러 로그인을 완료하세요.")
            print("   2. 로그인이 완료되면, 👉 여기 터미널에서 [Enter] 키를 누르세요.")
            print("="*60 + "\n")
            input("⌨️ 로그인을 완료했다면 엔터를 누르세요...")

            # 로그인 후 쿠키 가져오기
            sess = requests.Session() # 새 세션 시작
            selenium_cookies = driver.get_cookies()
            for cookie in selenium_cookies:
                sess.cookies.set(cookie['name'], cookie['value'])
            
            sess.headers.update({'User-Agent': user_agent})
            
            # 새 쿠키 저장
            with open(cookie_filename, 'wb') as f:
                pickle.dump(sess.cookies, f)
            
            print(f"💾 새로운 로그인 정보를 '{cookie_filename}'에 갱신했습니다.")
            return sess

        except Exception as e:
            print(f"❌ 로그인 프로세스 실패: {e}")
            return None
        finally:
            if driver:
                driver.quit()

# =================================================================================
# 데이터 수집 함수 (Session 인자 사용)
# =================================================================================

def fetch_krx_pbr_data(date_str, session):
    """ [PER/PBR 데이터] """
    session.headers.update({'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'})

    target_markets = ['STK', 'KSQ'] 
    dfs = []

    for mkt in target_markets:
        print(f"DEBUG: PBR 데이터 요청 중... ({mkt})")
        
        otp_params = {
            'locale': 'ko_KR',
            'mktId': mkt,
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
                print(f"❌ PBR OTP 실패 (LOGOUT/Error) - Market: {mkt}")
                continue 

            # 2. 다운로드 요청
            down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
            res = session.post(down_url, data={'code': otp_code})

            # 3. DataFrame 변환
            df_part = pd.read_csv(BytesIO(res.content), encoding='euc-kr')
            dfs.append(df_part)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"❌ PBR 데이터 요청 중 에러({mkt}): {e}")
            continue

    if dfs:
        result_df = pd.concat(dfs, ignore_index=True)
        return result_df
    else:
        return None

def fetch_krx_data(trade_date, session):
    """ [KOSPI/KOSDAQ 전종목 시세 데이터] """
    session.headers.update({'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101'})

    target_markets = ['STK', 'KSQ']
    dfs = []

    for mkt in target_markets:
        print(f"DEBUG: 시세 데이터 요청 중... ({mkt})") 
        
        otp_params = {
            'locale': 'ko_KR',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'mktId': mkt,
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
                print(f"❌ 시세 OTP 실패 (LOGOUT/Error) - Market: {mkt}")
                continue

            # 2. 다운로드 요청
            down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
            csv_response = session.post(down_url, data={'code': otp_code})
            
            # 3. 데이터프레임 변환
            df_part = pd.read_csv(BytesIO(csv_response.content), encoding='euc-kr')
            dfs.append(df_part)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"❌ 시세 데이터 요청 중 에러({mkt}): {e}")
            continue

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return None

# =================================================================================
# Insert Controller 함수들
# =================================================================================

def insert_all_symbols_fdt(p_trade_date, session):
    trade_date = p_trade_date
    print(f"✅ [FDT] 거래일: {trade_date} 데이터 수집 시작")

    df = fetch_krx_pbr_data(trade_date, session)

    if df is None or df.empty:
        print("❌ FDT 데이터 로드 실패 (혹은 휴장일/데이터 없음)")
        return

    send_message(f"✅ FDT 종목 수: {len(df)}")
    send_message_main(f"✅ FDT 종목 수: {len(df)}")

    with get_db_connection() as conn:
        save_to_postgres_fdt(df, trade_date, conn)

def insert_all_symbols(trade_date, session):
    print(f"✅ [StockMain] 거래일: {trade_date} 데이터 수집 시작")

    df = fetch_krx_data(trade_date, session)

    if df is None or df.empty:
        print("❌ StockMain 데이터 로드 실패")
        return

    send_message(f"✅ StockMain 전체 종목 수: {len(df)}")
    send_message_main(f"✅ StockMain 전체 종목 수: {len(df)}")

    # 2. DB 저장
    with get_db_connection() as conn:
        save_to_postgres(df, trade_date, conn)
    
    # 3. 이평선 계산
    with get_db_connection() as conn:
        save_moving_average_by_date(conn, trade_date)

def is_trading_day(p_date):
    target_date = p_date.strftime('%Y-%m-%d')
    schedule = krx_cal.schedule(start_date=target_date, end_date=target_date)
    return not schedule.empty

# =================================================================================
# 매수 종목 Pool 조회 함수들
# =================================================================================
# 편의상 하나로 뭉쳐서 처리하거나, 기존처럼 개별 함수 유지 가능
# 여기서는 기존 코드 구조를 유지합니다.

def get_all_symbols_common(trade_date, max_price, days):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = f"select * from get_stock_ma{days}(%s, %s);"
                cur.execute(sql, (trade_date, max_price))
                rows = cur.fetchall()
                symbols = {str(code).zfill(6): name for code, name in rows}
        
        send_message(f"✅ [{trade_date}] {days}일 이평 매수종목: {len(symbols)}건")
        
        # [보완] 내용이 너무 길면 잘라서 보내거나 생략
        str_symbols = str(symbols)
        if len(str_symbols) > 1900:
             send_message(f"⚠️ 종목 리스트가 너무 길어 출력을 생략합니다. (총 {len(symbols)}개)")
        else:
             send_message(symbols)
             
        return symbols
    except Exception as e:
        send_message(f"❌ DB 조회 오류 ({days}일): {e}")
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
    
    krx_cal = mcal.get_calendar('XKRX') 

    if is_trading_day(trade_date_p):
        
        # 1. [핵심] 로그인 처리 (최초 1회 수동, 이후 자동)
        session = get_authenticated_session()

        if session is not None:
            # 2. 데이터 수집 및 저장
            insert_all_symbols_fdt(trade_date, session)
            insert_all_symbols(trade_date, session)
            
            # 3. 매수 풀 계산
            # 코드를 줄이기 위해 루프 사용 가능하지만 기존 스타일 유지
            pool = {}
            for d in [20, 40, 60, 90, 120]:
                pool.update(get_all_symbols_common(trade_date, MAX_BUY_PRICE, d))
            
            send_message(f"✅ [{trade_date}] 최종 합산 매수종목: {len(pool)}건")
            send_message_main(f"✅ [{trade_date}] 최종 합산 매수종목: {len(pool)}건")
            send_message(pool)
            send_message_main(pool)
            
        else:
            print("❌ 로그인을 하지 못해 작업을 중단합니다.")
            
    else:
        send_message(f"⏩ {trade_date}는 거래일이 아니므로 처리를 스킵합니다.")
        send_message_main(f"⏩ {trade_date}는 거래일이 아니므로 처리를 스킵합니다.")


