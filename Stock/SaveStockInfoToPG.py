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
    (5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120일)
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

    # [수정1] 30, 50, 70, 80, 100, 110일 추가
    ma_days = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
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
        
        # [수정2] values 목록에 새로 추가된 이평선 값 반영
        values.append((
            trade_date_obj, code,
            ma_vals[5], ma_vals[10], ma_vals[20], ma_vals[30],
            ma_vals[40], ma_vals[50], ma_vals[60], ma_vals[70], 
            ma_vals[80], ma_vals[90], ma_vals[100], ma_vals[110], ma_vals[120]
        ))

    # [핵심 수정] DELETE 후 INSERT
    with conn.cursor() as cur:
        # 1. 기존 데이터 삭제
        cur.execute("DELETE FROM stock_ma WHERE trade_date = %s", (trade_date_obj,))
        
        # 2. 데이터 삽입 (ON CONFLICT 제거)
        # [수정3] INSERT 쿼리에 신규 컬럼명 및 %s 바인딩 개수 수정 (총 15개)
        sql = """
            INSERT INTO stock_ma (
                trade_date, code, ma5, ma10, ma20, ma30, ma40, ma50, 
                ma60, ma70, ma80, ma90, ma100, ma110, ma120
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
# [수정] 로그인 세션 생성 함수 (SessionManager와 동기화 완료)
# =================================================================================
def get_authenticated_session():
    """
    1. 'C:\\StockPy\\krx_session.pkl' 경로를 고정하여 SessionManager와 파일을 공유합니다.
    2. SessionManager와 동일한 타임아웃(15초)과 검증 로직(길이 체크)을 사용합니다.
    3. 세션이 유효하면 즉시 반환하고, 정말 문제가 있을 때만 비상용으로 Selenium을 켭니다.
    """
    # [수정] 경로를 절대 경로로 고정하여 프로그램 실행 위치에 상관없이 동일한 파일을 보게 합니다.
    cookie_filename = r'C:\StockPy\krx_session.pkl' 
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    
    sess = requests.Session()
    sess.headers.update({'User-Agent': user_agent})
    
    need_login = True

    # -------------------------------------------------------
    # 1. 저장된 쿠키 로드 및 유효성 테스트 (SessionManager와 동기화)
    # -------------------------------------------------------
    if os.path.exists(cookie_filename):
        print(f"📂 저장된 세션 파일('{cookie_filename}') 로드 및 검증 중...")
        try:
            with open(cookie_filename, 'rb') as f:
                cookies = pickle.load(f)
                sess.cookies.update(cookies)
            
            # [수정] 타임아웃을 15초로 늘려 서버 지연으로 인한 오작동을 방지합니다.
            test_url = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'
            res = sess.get(test_url, timeout=15)
            
            # [수정] SessionManager와 동일하게 '응답 코드'와 '내용 길이'만으로 깔끔하게 판단합니다.
            if res.status_code == 200 and len(res.text) > 2000:
                print("✅ 세션이 유효합니다! (SessionManager 동기화 성공)")
                need_login = False 
                return sess
            else:
                print(f"⚠️ 세션 검증 실패: 코드 {res.status_code}, 길이 {len(res.text)}")
        except Exception as e:
            print(f"⚠️ 세션 파일 읽기 오류: {e}")
    else:
        print(f"ℹ️ 세션 파일이 존재하지 않습니다: {cookie_filename}")

    # -------------------------------------------------------
    # 2. Selenium으로 비상 로그인 (SessionManager가 꺼져있을 때만 실행됨)
    # -------------------------------------------------------
    if need_login:
        print("\n" + "="*70)
        print("🚨 [비상] 유효한 세션이 없습니다. 수동 로그인을 진행합니다.")
        print("   (SessionManager.py가 켜져 있는지 확인해 주세요!)")
        print("="*70)

        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1280,800")
        chrome_options.add_argument(f'user-agent={user_agent}')

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

            target_url = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'
            driver.get(target_url)
            time.sleep(3) 

            try:
                driver.switch_to.alert.accept()
            except:
                pass

            print("\n🛑 [사용자 개입 필요] 로그인을 완료하고 엔터를 누르세요.")
            input("⌨️ 엔터 키 대기 중...")

            # 로그인 정보 추출 및 저장
            sess = requests.Session()
            selenium_cookies = driver.get_cookies()
            for cookie in selenium_cookies:
                sess.cookies.set(cookie['name'], cookie['value'])
            
            sess.headers.update({'User-Agent': user_agent})
            
            # [수정] 저장 시에도 절대 경로를 사용합니다.
            with open(cookie_filename, 'wb') as f:
                pickle.dump(sess.cookies, f)
            
            print(f"💾 새로운 세션이 저장되었습니다: {cookie_filename}")
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


        # [수정] 결과가 1건 이상일 때만 디스코드/로그 메시지 전송
        if len(symbols) > 0:
            send_message(f"✅ [{trade_date}] {days}일 이평 매수종목: {len(symbols)}건 (기준가: {max_price:,.0f}원)")

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
    #trade_date_p = datetime.strptime('20260227', "%Y%m%d")
    #trade_date = trade_date_p.strftime('%Y%m%d')

    settings = load_settings()
    AMOUNT_TO_BUY = settings['AMOUNT_TO_BUY']
    MAX_BUY_PRICE = AMOUNT_TO_BUY  # 기본 기준가
    
    # ✅ [수정됨] 핵심 이평선 4단계로 압축 (10, 20, 40, 60)
    #ma_list = [10, 20, 40, 60]
    ma_list = [20]
    # ✅ [수정됨] 피라미딩 자금 관리에 맞춘 차감액(Deduction) 설정
    deduction_map = {
        20: 0        # 100만 -  0만 = 100만 원 (메인 비중 탑재 - 생명선 돌파)
    }
    #---deduction_map = {
    #---    10: 800000,  # 100만 - 80만 = 20만 원 (1차 정찰병 진입)
    #---    20: 0,       # 100만 -  0만 = 100만 원 (메인 비중 탑재 - 생명선 돌파)
    #---    40: 600000,  # 100만 - 60만 = 40만 원 (1차 불타기 - 추세 확인)
    #---    60: 800000   # 100만 - 80만 = 20만 원 (마지막 불타기 - 중기 추세 돌파)
    #---}
    #---deduction_map = {
    #---    10: 1600000,  # 200만 - 160만 = 40만 원 (정찰병)
    #---    20: 0,        # 200만 -   0만 = 200만 원 (메인 비중 - 생명선)
    #---    40: 1200000,  # 200만 - 120만 = 80만 원 (1차 불타기)
    #---    60: 1600000   # 200만 - 160만 = 40만 원 (마지막 불타기)
    #---}

    krx_cal = mcal.get_calendar('XKRX') 

    if is_trading_day(trade_date_p):
        
        # 1. [핵심] 로그인 처리 (최초 1회 수동, 이후 자동)
        session = get_authenticated_session()

        if session is not None:
            # 2. 데이터 수집 및 저장
            insert_all_symbols_fdt(trade_date, session)
            insert_all_symbols(trade_date, session)
            
            # 3. 매수 풀 계산
            pool = {}
            # ma_list를 순회하며 각 이평선별 차감된 기준가 적용
            for d in ma_list:
                # 차감 금액 계산 (map에 없을 경우 0원 차감)
                current_deduction = deduction_map.get(d, 0)
                calculated_max_price = MAX_BUY_PRICE - current_deduction
                
                # 계산된 기준가가 0보다 클 때만 조회 수행
                if calculated_max_price > 0:
                    #print(f"🔍 [{trade_date}] {d}일 이평 조회 시작 (차감가 적용: {calculated_max_price:,.0f}원)")
                    pool.update(get_all_symbols_common(trade_date, calculated_max_price, d))
                else:
                    print(f"⏩ [{trade_date}] {d}일 이평 스킵 (차감 후 기준가가 0원 이하: {calculated_max_price:,.0f}원)")
            
            send_message(f"✅ [{trade_date}] 최종 합산 매수종목: {len(pool)}건")
            send_message_main(f"✅ [{trade_date}] 최종 합산 매수종목: {len(pool)}건")
            
            # 종목 리스트 출력 (내용이 너무 길 경우에 대한 안전 처리)
            str_pool = str(pool)
            if len(str_pool) > 1900:
                send_message(f"⚠️ 종목 리스트가 너무 길어 상세 출력을 생략합니다. (총 {len(pool)}개)")
            else:
                send_message(pool)
                send_message_main(pool)
            
        else:
            print("❌ 로그인을 하지 못해 작업을 중단합니다.")
            
    else:
        send_message(f"⏩ {trade_date}는 거래일이 아니므로 처리를 스킵합니다.")
        send_message_main(f"⏩ {trade_date}는 거래일이 아니므로 처리를 스킵합니다.")



#-----if __name__ == "__main__":
#-----    # 1. 환경 설정 및 캘린더 로드
#-----    settings = load_settings()
#-----    AMOUNT_TO_BUY = settings['AMOUNT_TO_BUY']
#-----    MAX_BUY_PRICE = AMOUNT_TO_BUY
#-----    krx_cal = mcal.get_calendar('XKRX') 
#-----
#-----    # 2. 시작 날짜와 종료 날짜 설정
#-----    start_date = datetime(2026, 2, 1)
#-----    end_date = datetime(2026, 3, 1)
#-----    
#-----    # 3. 로그인 처리 (루프 밖에서 한 번만 수행하여 세션 유지)
#-----    session = get_authenticated_session()
#-----    
#-----    if session is None:
#-----        print("❌ 로그인을 하지 못해 작업을 중단합니다.")
#-----    else:
#-----        # 4. 시작일부터 종료일까지 루프 수행
#-----        current_date = start_date
#-----        while current_date <= end_date:
#-----            trade_date = current_date.strftime('%Y%m%d')
#-----            
#-----            # 영업일 여부 체크
#-----            if is_trading_day(current_date):
#-----                print(f"\n🚀 {trade_date} 데이터 수집 시작...")
#-----                
#-----                # 데이터 수집 및 저장
#-----                try:
#-----                    insert_all_symbols_fdt(trade_date, session)
#-----                    insert_all_symbols(trade_date, session)
#-----                    
#-----                except Exception as e:
#-----                    print(f"❌ {trade_date} 처리 중 에러 발생: {e}")
#-----            else:
#-----                print(f"⏩ {trade_date}는 휴장일이므로 스킵합니다.")
#-----            
#-----            # 다음 날짜로 이동
#-----            current_date += timedelta(days=1)
#-----            
#-----            # 과도한 API 요청 방지를 위해 루프 사이 짧은 휴식 (권장)
#-----            time.sleep(1)
#-----
#-----        send_message(f"✅ {start_date.strftime('%Y%m%d')} ~ {end_date.strftime('%Y%m%d')} 기간 수집 완료")


