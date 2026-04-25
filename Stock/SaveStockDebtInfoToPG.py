import os
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
import requests  
from datetime import datetime  

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By 

# =========================================================
# 🌟 [추가됨] 좀비 프로세스 완벽 청소 함수
# =========================================================
def kill_zombie_processes():
    """백그라운드에 꼬여있는 크롬 드라이버를 강제로 싹 지웁니다."""
    try:
        os.system("taskkill /f /im chromedriver.exe /T >nul 2>&1")
    except:
        pass

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
    DISCORD_WEBHOOK_URL = _cfg.get('DISCORD_WEBHOOK_URL', '') 
except Exception as e:
    print(f"⚠️ 설정 파일 로드 실패: {e}")
    HOST = ""
    DISCORD_WEBHOOK_URL = ""

def get_db_connection():
    return psycopg2.connect(host=HOST, dbname=DBNAME, user=USER, password=PASSWORD)

def send_message(msg):
    if not DISCORD_WEBHOOK_URL: return
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    try: requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
    except Exception as e: print(f"❌ Discord 전송 실패: {e}", flush=True)

# =========================================================
# 2. 멀티스레딩용 크롬 드라이버 관리 
# =========================================================
thread_local = threading.local()
active_drivers = [] 

def get_driver():
    if hasattr(thread_local, "driver") and hasattr(thread_local, "usage_count"):
        if thread_local.usage_count > 50:
            try:
                thread_local.driver.quit()
                if thread_local.driver in active_drivers:
                    active_drivers.remove(thread_local.driver)
            except: pass
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
        
        # 🌟 [추가됨] 무한 대기 멈춤 방지: 15초 안에 응답 없으면 강제 에러 발생!
        driver.set_page_load_timeout(15)
        
        thread_local.driver = driver
        thread_local.usage_count = 0
        active_drivers.append(driver)
        
    thread_local.usage_count += 1
    return thread_local.driver

# =========================================================
# 3. 크롤링 로직 (무한 대기 방지 및 폐기 로직 포함)
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
                except: continue
                    
        time.sleep(1.5) 
        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        
    except Exception as e:
        print(f"⚠️ [{stock_code}] 접속 지연 또는 파싱 에러 (드라이버 강제 폐기)")
        try:
            driver.quit()
            if driver in active_drivers:
                active_drivers.remove(driver)
        except: pass
        if hasattr(thread_local, 'driver'): del thread_local.driver
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
                
                if val_str in ['-', '', 'NaN', 'nan']: return None
                try: return float(val_str)
                except ValueError: return None
    return None

def process_stock(stock_info):
    code, name = stock_info
    net_debt_val = get_wisereport_recent_net_debt(code)
    return (code, name, net_debt_val)

# =========================================================
# 4. Main 실행: [청크 분할] 및 DB 연동
# =========================================================
if __name__ == "__main__":
    # 시작 전 청소
    kill_zombie_processes()
    
    start_msg = "🚀 [순부채 수집 시스템 - 고속 병렬 버전] 작동 시작..."
    print(start_msg)
    send_message(start_msg)
    
    conn = get_db_connection()
    codes_to_fetch = []
    
    # 🌟 1. 당일 날짜 설정 및 DB 초기화
    current_date = datetime.now().date()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stock_debt WHERE trade_date = %s", (current_date,))
        conn.commit()
        print(f"🗑️ 오늘 날짜({current_date}) 기존 데이터를 초기화했습니다.")
    except Exception as e:
        print(f"⚠️ 기존 데이터 초기화 중 오류 발생: {e}")

    with conn.cursor() as cur:
        sql_select = """
            SELECT code, name 
            FROM stockmain 
            WHERE trade_date = (SELECT max(trade_date) FROM stockmain);
        """
        cur.execute(sql_select)
        codes_to_fetch = cur.fetchall() 
        
    print(f"✅ DB에서 수집 대상 {len(codes_to_fetch)}개 종목을 불러왔습니다.")

    success_cnt = 0
    fail_cnt = 0
    MAX_WORKERS = 5 
    
    # 🌟 2. 회원님 아이디어 적용: 500개 단위 청크(Chunk) 분할
    chunk_size = 500
    chunks = [codes_to_fetch[i:i + chunk_size] for i in range(0, len(codes_to_fetch), chunk_size)]

    print("\n🌐 [병렬 수집 시작] 500개씩 쪼개어 안전하게 수집을 진행합니다...")

    for chunk_idx, chunk in enumerate(chunks):
        print(f"\n📦 === 청크 {chunk_idx + 1}/{len(chunks)} 시작 (총 {len(chunk)}개) ===")
        insert_values = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for idx, result in enumerate(executor.map(process_stock, chunk), 1):
                code, name, net_debt_val = result
                
                if net_debt_val is None or math.isnan(net_debt_val):
                    db_val = 'NaN'
                    fail_cnt += 1
                else:
                    db_val = net_debt_val
                    success_cnt += 1
                    
                insert_values.append((current_date, code, name, db_val))
                
                # 진행률 표시
                if idx % 10 == 0:
                    current_total = (chunk_idx * chunk_size) + idx
                    print(f"⏳ 전체 진행중... {current_total}/{len(codes_to_fetch)} 완료 (최근: {name})")

                # DB 저장 (100개마다)
                if len(insert_values) >= 100:
                    with conn.cursor() as cur:
                        sql_insert = """
                            INSERT INTO stock_debt (trade_date, code, name, net_debt)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (trade_date, code) DO UPDATE 
                            SET name = EXCLUDED.name,
                                net_debt = EXCLUDED.net_debt,
                                created_at = now();
                        """
                        execute_batch(cur, sql_insert, insert_values, page_size=100)
                        conn.commit()
                    insert_values.clear()

        # 청크 종료 후 남은 자투리 DB 저장
        if insert_values:
            with conn.cursor() as cur:
                sql_insert = """
                    INSERT INTO stock_debt (trade_date, code, name, net_debt)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (trade_date, code) DO UPDATE 
                    SET name = EXCLUDED.name,
                        net_debt = EXCLUDED.net_debt,
                        created_at = now();
                """
                execute_batch(cur, sql_insert, insert_values, page_size=100)
                conn.commit()

        # 🌟 3. 청크(500개)가 끝날 때마다 크롬 완벽 초기화 & 휴식
        print(f"💤 청크 {chunk_idx + 1} 완료. 메모리 정리를 위해 브라우저를 모두 닫고 3초 휴식합니다.")
        for d in active_drivers:
            try: d.quit()
            except: pass
        active_drivers.clear()
        
        kill_zombie_processes() # 확실하게 죽임
        time.sleep(3) # 컴퓨터와 네트워크 휴식
        
    conn.close()
    
    # 🌟 최종 결과 알림
    end_msg = (
        f"🎉 [순부채 수집 시스템 작업 완료]\n"
        f"📊 총 {len(codes_to_fetch)}개 대상 중 성공: {success_cnt}건 / 데이터 없음(NaN): {fail_cnt}건\n"
        f"✅ stock_debt 테이블에 데이터 반영이 완료되었습니다. (기준일: {current_date})"
    )
    print("\n" + end_msg)
    send_message(end_msg)


