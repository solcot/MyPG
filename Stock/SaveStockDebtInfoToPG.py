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
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import requests  
from datetime import datetime  

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By 

# =========================================================
# 🌟 좀비 프로세스 완벽 청소 함수
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
        
        driver.set_page_load_timeout(15)
        driver.set_script_timeout(15)
        
        thread_local.driver = driver
        thread_local.usage_count = 0
        active_drivers.append(driver)
        
    thread_local.usage_count += 1
    return thread_local.driver

# =========================================================
# 3. 크롤링 헬퍼 함수 및 파싱 로직
# =========================================================
def click_tab(driver, tab_name):
    """지정된 텍스트를 가진 탭을 안전하게 찾아 클릭하는 헬퍼 함수"""
    elements = driver.find_elements(By.XPATH, f"//*[contains(text(), '{tab_name}')]")
    for el in elements:
        if el.is_displayed() and el.tag_name not in ['title', 'h1', 'h2', 'script', 'style', 'html', 'head']:
            try:
                driver.execute_script("arguments[0].click();", el)
                return True
            except: 
                continue
    return False

def parse_value_from_tables(tables, target_keywords, exclude_keywords=None):
    """테이블 리스트에서 원하는 행의 '(최근분기)' 데이터를 찾아 반환하는 범용 함수"""
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
            
            if any(kw in row_str for kw in target_keywords):
                if exclude_keywords and any(ex_kw in row_str for ex_kw in exclude_keywords):
                    continue
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
                    return float('nan')
                try: 
                    return float(val_str)
                except ValueError: 
                    return float('nan')
    return float('nan')

def get_wisereport_financials(stock_code):
    driver = get_driver()
    url = f"https://comp.wisereport.co.kr/company/c1030001.aspx?cmp_cd={stock_code}&cn="
    
    result = {
        'revenue': float('nan'),
        'gross_profit': float('nan'),
        'operating_income': float('nan'),
        'net_income': float('nan'),
        'total_assets': float('nan'),
        'net_debt': float('nan')
    }
    
    try:
        driver.get(url)
        time.sleep(1) # 기본 페이지 로드 대기
        
        # 💡 [핵심 수정] 1. '포괄손익계산서' 탭을 명시적으로 클릭하고 대기
        clicked_is = click_tab(driver, '포괄손익계산서')
        if clicked_is:
            time.sleep(1.5) # 데이터 렌더링 대기
            
        html_is = driver.page_source
        tables_is = pd.read_html(StringIO(html_is))
        
        result['revenue'] = parse_value_from_tables(tables_is, ['매출액'])
        result['gross_profit'] = parse_value_from_tables(tables_is, ['매출총이익'])
        result['operating_income'] = parse_value_from_tables(tables_is, ['영업이익'])
        result['net_income'] = parse_value_from_tables(tables_is, ['당기순이익'])
        
        # 💡 2. '재무상태표' 탭을 클릭하고 대기
        clicked_bs = click_tab(driver, '재무상태표')
        if clicked_bs:
            time.sleep(1.5) 
            html_bs = driver.page_source
            tables_bs = pd.read_html(StringIO(html_bs))
        else:
            tables_bs = tables_is 
            
        result['total_assets'] = parse_value_from_tables(tables_bs, ['자산총계'])
        result['net_debt'] = parse_value_from_tables(tables_bs, ['순부채'], exclude_keywords=['차입금'])
        
    except Exception as e:
        print(f"⚠️ [{stock_code}] 접속 지연 또는 파싱 에러 (드라이버 강제 폐기)")
        try:
            driver.quit()
            if driver in active_drivers:
                active_drivers.remove(driver)
        except: pass
        if hasattr(thread_local, 'driver'): del thread_local.driver
        return result

    return result

def process_stock(stock_info):
    code, name = stock_info
    fin = get_wisereport_financials(code)
    return (code, name, fin['revenue'], fin['gross_profit'], fin['operating_income'], fin['net_income'], fin['total_assets'], fin['net_debt'])

def safe_db_val(val):
    """DB Insert를 위해 안전한 문자열 'NaN' 또는 숫자 반환"""
    return 'NaN' if val is None or math.isnan(val) else val

# =========================================================
# 4. Main 실행: [청크 분할] 및 DB 연동
# =========================================================
if __name__ == "__main__":
    kill_zombie_processes()
    
    start_msg = "🚀 [재무 지표 수집 시스템 - 고속 병렬 버전] 작동 시작..."
    print(start_msg)
    send_message(start_msg)
    
    conn = get_db_connection()
    codes_to_fetch = []
    
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
    MAX_WORKERS = 3 
    
    chunk_size = 500
    chunks = [codes_to_fetch[i:i + chunk_size] for i in range(0, len(codes_to_fetch), chunk_size)]

    print(f"\n🌐 [병렬 수집 시작] 500개씩 쪼개어 안전하게 수집을 진행합니다... (워커 수: {MAX_WORKERS})")

    for chunk_idx, chunk in enumerate(chunks):
        print(f"\n📦 === 청크 {chunk_idx + 1}/{len(chunks)} 시작 (총 {len(chunk)}개) ===")
        insert_values = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_stock, item): item for item in chunk}
            
            for idx, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    code, name, rev, gp, op, ni, ta, nd = future.result()
                except Exception as e:
                    code, name = futures[future]
                    print(f"⚠️ [{name}] 스레드 예외 발생: 패스합니다.")
                    rev = gp = op = ni = ta = nd = float('nan')
                
                db_rev = safe_db_val(rev)
                db_gp = safe_db_val(gp)
                db_op = safe_db_val(op)
                db_ni = safe_db_val(ni)
                db_ta = safe_db_val(ta)
                db_nd = safe_db_val(nd)

                if db_nd == 'NaN': fail_cnt += 1
                else: success_cnt += 1
                    
                insert_values.append((current_date, code, name, db_rev, db_gp, db_op, db_ni, db_ta, db_nd))
                
                # 💡 [핵심 수정] 진행률 표시에 6개의 모든 지표를 이쁘게 출력합니다!
                if idx % 10 == 0:
                    current_total = (chunk_idx * chunk_size) + idx
                    log_msg = (f"⏳ 진행중... {current_total}/{len(codes_to_fetch)} 완료 (최근: {name} | "
                               f"매출: {db_rev}, 총이익: {db_gp}, 영업익: {db_op}, "
                               f"순이익: {db_ni}, 자산: {db_ta}, 순부채: {db_nd})")
                    print(log_msg)

                if len(insert_values) >= 100:
                    with conn.cursor() as cur:
                        sql_insert = """
                            INSERT INTO stock_debt (
                                trade_date, code, name, revenue, gross_profit, operating_income, net_income, total_assets, net_debt
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (trade_date, code) DO UPDATE 
                            SET name = EXCLUDED.name,
                                revenue = EXCLUDED.revenue,
                                gross_profit = EXCLUDED.gross_profit,
                                operating_income = EXCLUDED.operating_income,
                                net_income = EXCLUDED.net_income,
                                total_assets = EXCLUDED.total_assets,
                                net_debt = EXCLUDED.net_debt,
                                created_at = now();
                        """
                        execute_batch(cur, sql_insert, insert_values, page_size=100)
                        conn.commit()
                    insert_values.clear()

        if insert_values:
            with conn.cursor() as cur:
                sql_insert = """
                    INSERT INTO stock_debt (
                        trade_date, code, name, revenue, gross_profit, operating_income, net_income, total_assets, net_debt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, code) DO UPDATE 
                    SET name = EXCLUDED.name,
                        revenue = EXCLUDED.revenue,
                        gross_profit = EXCLUDED.gross_profit,
                        operating_income = EXCLUDED.operating_income,
                        net_income = EXCLUDED.net_income,
                        total_assets = EXCLUDED.total_assets,
                        net_debt = EXCLUDED.net_debt,
                        created_at = now();
                """
                execute_batch(cur, sql_insert, insert_values, page_size=100)
                conn.commit()

        print(f"💤 청크 {chunk_idx + 1} 완료. 메모리 정리를 위해 브라우저를 모두 닫고 3초 휴식합니다.")
        for d in active_drivers:
            try: d.quit()
            except: pass
        active_drivers.clear()
        
        kill_zombie_processes() 
        time.sleep(3) 
        
    conn.close()
    
    end_msg = (
        f"🎉 [재무 지표 수집 시스템 작업 완료]\n"
        f"📊 총 {len(codes_to_fetch)}개 대상 중 성공: {success_cnt}건 / 데이터 없음(NaN): {fail_cnt}건\n"
        f"✅ stock_debt 테이블에 데이터 반영이 완료되었습니다. (기준일: {current_date})"
    )
    print("\n" + end_msg)
    send_message(end_msg)


