import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import re
import time
import yaml
import math
import psycopg2
from psycopg2.extras import execute_batch
import threading
from concurrent.futures import ThreadPoolExecutor
import requests  # 디스코드 전송을 위해 추가
from datetime import datetime  # 디스코드 시간 표시를 위해 추가

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By 

# =========================================================
# 1. 설정 파일 로드 및 DB 연결 함수
# =========================================================
try:
    with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
        _cfg = yaml.load(f, Loader=yaml.FullLoader)
    HOST = _cfg['HOST']
    DBNAME = _cfg['DBNAME']
    USER = _cfg['USER']
    PASSWORD = _cfg['PASSWORD']
    # 🌟 디스코드 웹훅 URL 로드 추가
    DISCORD_WEBHOOK_URL = _cfg.get('DISCORD_WEBHOOK_URL', '') 
except Exception as e:
    print(f"⚠️ 설정 파일 로드 실패: {e}")
    HOST = ""
    DISCORD_WEBHOOK_URL = ""

def get_db_connection():
    return psycopg2.connect(host=HOST, dbname=DBNAME, user=USER, password=PASSWORD)

# 🌟 디스코드 메시지 전송 함수 추가
def send_message(msg):
    """디스코드 메세지 전송"""
    if not DISCORD_WEBHOOK_URL:
        return
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    try:
        requests.post(DISCORD_WEBHOOK_URL, data=message, timeout=5)
    except Exception as e:
        print(f"❌ Discord 전송 실패: {e}", flush=True)

# =========================================================
# 2. 멀티스레딩용 크롬 드라이버 관리 (주기적 재부팅 적용)
# =========================================================
thread_local = threading.local()
active_drivers = [] 

def get_driver():
    """각 스레드마다 크롬을 할당하되, 50번 쓰면 껐다 켜서 메모리를 확보합니다."""
    if hasattr(thread_local, "driver") and hasattr(thread_local, "usage_count"):
        if thread_local.usage_count > 50:
            try:
                thread_local.driver.quit()
                if thread_local.driver in active_drivers:
                    active_drivers.remove(thread_local.driver)
            except:
                pass
            del thread_local.driver

    if not hasattr(thread_local, "driver"):
        chrome_options = Options()
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--headless") 
        chrome_options.add_argument("--disable-dev-shm-usage") 
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/100.0.4896.75 Safari/537.36")
        
        chrome_options.page_load_strategy = 'eager' 
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.fonts": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        thread_local.driver = driver
        thread_local.usage_count = 0
        active_drivers.append(driver)
        
    thread_local.usage_count += 1
    return thread_local.driver

# =========================================================
# 3. 크롤링 로직 (에러 발생 시 브라우저 강제 폐기 로직 포함)
# =========================================================
def get_wisereport_recent_net_debt(stock_code):
    driver = get_driver()
    url = f"https://comp.wisereport.co.kr/company/c1030001.aspx?cmp_cd={stock_code}&cn="
    
    try:
        driver.get(url)
        time.sleep(1) 
        
        elements = driver.find_elements(By.XPATH, "//*[contains(text(), '재무상태표')]")
        for el in elements:
            if el.is_displayed() and el.tag_name not in ['title', 'h1', 'h2', 'script', 'style', 'html', 'head']:
                try:
                    driver.execute_script("arguments[0].click();", el)
                    break 
                except:
                    continue
                    
        time.sleep(1.5) 
        
        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        
    except Exception as e:
        print(f"[{stock_code}] 크롤링/파싱 중 에러: {e}")
        try:
            driver.quit()
            if driver in active_drivers:
                active_drivers.remove(driver)
        except:
            pass
        if hasattr(thread_local, 'driver'):
            del thread_local.driver
        return None

    # --- 데이터 정밀 매칭 및 추출 로직 ---
    for df in tables:
        if isinstance(df.columns, pd.MultiIndex):
            flat_cols = []
            for col_tuple in df.columns:
                clean_tuple = [str(c) for c in col_tuple if 'Unnamed' not in str(c)]
                flat_cols.append(' '.join(clean_tuple).strip())
            df.columns = flat_cols
        else:
            df.columns = df.columns.astype(str)

        if not any('(최근분기)' in col for col in df.columns):
            continue

        target_idx = None
        for i in range(len(df)):
            row_str = ''.join(df.iloc[i].astype(str).values).replace(' ', '')
            if '순부채' in row_str and '차입금' not in row_str:
                target_idx = i
                break

        if target_idx is not None:
            target_col = None
            for col in df.columns:
                if '(최근분기)' in col and '(E)' not in col:
                    target_col = col
                    break
            if not target_col:
                for col in df.columns:
                    if '(최근분기)' in col:
                        target_col = col
                        break

            if target_col:
                raw_val = df.loc[target_idx, target_col]
                val_str = str(raw_val).replace(',', '').strip()
                
                if val_str in ['-', '', 'NaN', 'nan']:
                    return None
                    
                try:
                    return float(val_str)
                except ValueError:
                    return None
                    
    return None

def process_stock(stock_info):
    """멀티스레딩에서 개별 종목을 처리할 래퍼 함수"""
    code, name = stock_info
    net_debt_val = get_wisereport_recent_net_debt(code)
    return (code, name, net_debt_val)

# =========================================================
# 4. Main 실행: 멀티스레딩 병렬 처리 및 DB 연동
# =========================================================
if __name__ == "__main__":
    # 🌟 시작 알림 (터미널 출력 + 디스코드 전송)
    start_msg = "🚀 [순부채 수집 시스템 - 고속 병렬 버전] 작동 시작..."
    print(start_msg)
    send_message(start_msg)
    
    conn = get_db_connection()
    codes_to_fetch = []
    
    with conn.cursor() as cur:
        sql_select = """
            SELECT code, name 
            FROM stockmain 
            WHERE trade_date = (SELECT max(trade_date) FROM stockmain);
        """
        cur.execute(sql_select)
        codes_to_fetch = cur.fetchall() 
        
    print(f"✅ DB에서 수집 대상 {len(codes_to_fetch)}개 종목을 불러왔습니다.")

    insert_values = []
    success_cnt = 0
    fail_cnt = 0

    print("\n🌐 [병렬 수집 시작] 5개의 크롬이 동시에 데이터를 수집합니다...")
    
    MAX_WORKERS = 5 
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for idx, result in enumerate(executor.map(process_stock, codes_to_fetch), 1):
            code, name, net_debt_val = result
            
            if net_debt_val is None or math.isnan(net_debt_val):
                db_val = 'NaN'
                fail_cnt += 1
            else:
                db_val = net_debt_val
                success_cnt += 1
                
            insert_values.append((code, name, db_val))
            
            if idx % 10 == 0:
                print(f"⏳ 진행중... {idx}/{len(codes_to_fetch)} 완료 (최근 완료: {name}, 순부채: {db_val})")

            if len(insert_values) >= 100:
                with conn.cursor() as cur:
                    sql_insert = """
                        INSERT INTO stock_debt (code, name, net_debt)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (code) DO UPDATE 
                        SET name = EXCLUDED.name,
                            net_debt = EXCLUDED.net_debt,
                            created_at = now();
                    """
                    execute_batch(cur, sql_insert, insert_values, page_size=100)
                    conn.commit()
                insert_values.clear()

    if insert_values:
        with conn.cursor() as cur:
            sql_insert = """
                INSERT INTO stock_debt (code, name, net_debt)
                VALUES (%s, %s, %s)
                ON CONFLICT (code) DO UPDATE 
                SET name = EXCLUDED.name,
                    net_debt = EXCLUDED.net_debt,
                    created_at = now();
            """
            execute_batch(cur, sql_insert, insert_values, page_size=100)
            conn.commit()
            
    for d in active_drivers:
        try:
            d.quit()
        except:
            pass
            
    conn.close()
    
    # 🌟 최종 결과 알림 (터미널 출력 + 디스코드 전송)
    end_msg = (
        f"🎉 [순부채 수집 시스템 작업 완료]\n"
        f"📊 총 {len(codes_to_fetch)}개 대상 중 성공: {success_cnt}건 / 데이터 없음(NaN): {fail_cnt}건\n"
        f"✅ stock_debt 테이블에 데이터 반영이 완료되었습니다."
    )
    print("\n" + end_msg)
    send_message(end_msg)


