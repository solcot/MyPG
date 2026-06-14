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
    try:
        os.system("taskkill /f /im chromedriver.exe /T >nul 2>&1")
        os.system("taskkill /f /im chrome.exe /T >nul 2>&1")
    except:
        pass

# =========================================================
# 1. 설정 파일 로드 및 DB 연결
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
    except: pass

# =========================================================
# 2. 멀티스레딩용 크롬 드라이버 관리 
# =========================================================
thread_local = threading.local()
active_drivers = [] 

def get_driver():
    if hasattr(thread_local, "driver") and hasattr(thread_local, "usage_count"):
        # 💡 [안정성] 15번만 사용하고 무조건 새 브라우저로 교체하여 찌꺼기 방지
        if thread_local.usage_count > 15:
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
        # 💡 [속도 향상] 불필요한 이미지 로딩 차단
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/100.0.4896.75 Safari/537.36")
        
        chrome_options.page_load_strategy = 'eager' 
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 💡 타임아웃을 20초로 늘려 서버 응답 지연에 대비
        driver.set_page_load_timeout(20)
        driver.set_script_timeout(20)
        
        thread_local.driver = driver
        thread_local.usage_count = 0
        active_drivers.append(driver)
        
    thread_local.usage_count += 1
    return thread_local.driver

# =========================================================
# 3. 크롤링 헬퍼 함수 및 파싱 로직
# =========================================================
def click_tab(driver, tab_name):
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
        'revenue': float('nan'), 'gross_profit': float('nan'),
        'operating_income': float('nan'), 'net_income': float('nan'),
        'total_assets': float('nan'), 'net_debt': float('nan')
    }
    
    try:
        driver.get(url)
        time.sleep(1) 
        
        if click_tab(driver, '포괄손익계산서'):
            time.sleep(1.5) 
            
        html_is = driver.page_source
        tables_is = pd.read_html(StringIO(html_is))
        
        result['revenue'] = parse_value_from_tables(tables_is, ['매출액'])
        result['gross_profit'] = parse_value_from_tables(tables_is, ['매출총이익'])
        result['operating_income'] = parse_value_from_tables(tables_is, ['영업이익'])
        result['net_income'] = parse_value_from_tables(tables_is, ['당기순이익'])
        
        if click_tab(driver, '재무상태표'):
            time.sleep(1.5) 
            html_bs = driver.page_source
            tables_bs = pd.read_html(StringIO(html_bs))
        else:
            tables_bs = tables_is 
            
        result['total_assets'] = parse_value_from_tables(tables_bs, ['자산총계'])
        result['net_debt'] = parse_value_from_tables(tables_bs, ['순부채'], exclude_keywords=['차입금'])
        
    except Exception as e:
        if hasattr(thread_local, 'driver'): 
            del thread_local.driver
        # 💡 에러 발생 시 명시적으로 예외를 던져서 재시도(Retry) 큐로 보냅니다.
        raise Exception(f"크롤링 에러: {str(e)}")

    return result

def process_stock(stock_info):
    code, name = stock_info
    fin = get_wisereport_financials(code)
    return (code, name, fin['revenue'], fin['gross_profit'], fin['operating_income'], fin['net_income'], fin['total_assets'], fin['net_debt'])

def safe_db_val(val):
    return 'NaN' if val is None or math.isnan(val) else val

# =========================================================
# 4. Main 실행 (무손실 회전초밥 아키텍처)
# =========================================================
if __name__ == "__main__":
    kill_zombie_processes()
    
    start_msg = "🚀 [재무 지표 수집 - 무손실 100% 완주 버전] 작동 시작..."
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
    except Exception as e: pass

    with conn.cursor() as cur:
        cur.execute("SELECT code, name FROM stockmain WHERE trade_date = (SELECT max(trade_date) FROM stockmain)")
        codes_to_fetch = cur.fetchall() 
        
    print(f"✅ DB에서 수집 대상 {len(codes_to_fetch)}개 종목을 불러왔습니다.")

    success_cnt = 0
    fail_cnt = 0
    MAX_WORKERS = 3 
    CHUNK_SIZE = 100
    
    # 💡 [핵심] 실패 횟수를 추적하기 위한 딕셔너리와 대기열(Queue) 리스트
    retry_counts = {item[0]: 0 for item in codes_to_fetch}
    unprocessed_queue = codes_to_fetch.copy()

    total_target = len(unprocessed_queue)
    processed_count = 0
    chunk_idx = 0

    print(f"\n🌐 [병렬 수집 시작] 멈춤 현상 원천 차단! 대기열 시스템을 가동합니다...")

    # 대기열에 종목이 남아있는 한 계속 돕니다.
    while unprocessed_queue:
        chunk_idx += 1
        # 대기열 앞에서부터 100개씩 뽑아옵니다.
        current_chunk = unprocessed_queue[:CHUNK_SIZE]
        del unprocessed_queue[:CHUNK_SIZE]
        
        print(f"\n📦 === 청크 {chunk_idx} 시작 (남은 대기열: {len(unprocessed_queue)}개) ===")
        
        executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        futures = {executor.submit(process_stock, item): item for item in current_chunk}
        insert_values = []
        
        # 💡 [수정됨] 딱 300초(5분)만 기다립니다. 네트워크가 느려도 충분히 커버 가능한 시간입니다.
        done, not_done = concurrent.futures.wait(futures, timeout=300)
        
        # 1. 정상적으로 완료된 녀석들 처리
        for future in done:
            item = futures[future]
            try:
                code, name, rev, gp, op, ni, ta, nd = future.result()
                db_rev, db_gp = safe_db_val(rev), safe_db_val(gp)
                db_op, db_ni = safe_db_val(op), safe_db_val(ni)
                db_ta, db_nd = safe_db_val(ta), safe_db_val(nd)
                
                insert_values.append((current_date, code, name, db_rev, db_gp, db_op, db_ni, db_ta, db_nd))
                
                if db_nd == 'NaN': fail_cnt += 1
                else: success_cnt += 1
                
                processed_count += 1
                if processed_count % 10 == 0:
                    print(f"⏳ 진행중... {processed_count}/{total_target} (최근: {name} | 순부채: {db_nd})")

            except Exception as e:
                # 💡 코드가 터졌을 경우 재시도 큐로 보냄
                code, name = item
                retry_counts[code] += 1
                # 💡 [수정됨] 최대 5번까지 재시도합니다.
                if retry_counts[code] <= 5:
                    print(f"⚠️ [{name}] 파싱 에러! (재시도 횟수: {retry_counts[code]}/5) -> 대기열로 복귀합니다.")
                    unprocessed_queue.append(item)
                else:
                    print(f"❌ [{name}] 5회 연속 실패. 데이터 없음을 간주하고 NaN으로 포기합니다.")
                    insert_values.append((current_date, code, name, 'NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN'))
                    fail_cnt += 1
                    processed_count += 1

        # 2. 300초가 지났는데도 멈춰있는(프리징) 녀석들 처리
        if not_done:
            print(f"🚨 [경고] {len(not_done)}개의 종목에서 무한 대기(프리징) 감지! 브라우저를 죽이고 다음 청크에서 재시도합니다.")
            for future in not_done:
                stuck_item = futures[future]
                code = stuck_item[0]
                retry_counts[code] += 1
                
                # 💡 [수정됨] 최대 5번까지 재시도합니다.
                if retry_counts[code] <= 5:
                    unprocessed_queue.append(stuck_item) # 대기열 맨 뒤로 다시 집어넣습니다!
                else:
                    insert_values.append((current_date, stuck_item[0], stuck_item[1], 'NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN'))
                    fail_cnt += 1
                    processed_count += 1
        
        # 💡 완료 여부 상관없이 멈춰있는 스레드와 브라우저를 모조리 강제 사살합니다.
        executor.shutdown(wait=False, cancel_futures=True)
        kill_zombie_processes()
        time.sleep(3) # 강제 종료 후 숨 고르기
        
        # DB 일괄 저장
        if insert_values:
            with conn.cursor() as cur:
                sql_insert = """
                    INSERT INTO stock_debt (
                        trade_date, code, name, revenue, gross_profit, operating_income, net_income, total_assets, net_debt
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trade_date, code) DO UPDATE 
                    SET name = EXCLUDED.name, revenue = EXCLUDED.revenue, gross_profit = EXCLUDED.gross_profit,
                        operating_income = EXCLUDED.operating_income, net_income = EXCLUDED.net_income,
                        total_assets = EXCLUDED.total_assets, net_debt = EXCLUDED.net_debt, created_at = now();
                """
                execute_batch(cur, sql_insert, insert_values, page_size=100)
                conn.commit()

    conn.close()
    
    end_msg = (
        f"🎉 [재무 지표 수집 시스템 작업 완료]\n"
        f"📊 총 {total_target}개 대상 중 성공: {success_cnt}건 / 데이터 없음(NaN): {fail_cnt}건\n"
        f"✅ 단 하나의 누락 없이 stock_debt 테이블 반영이 완료되었습니다. (기준일: {current_date})"
    )
    print("\n" + end_msg)
    send_message(end_msg)


