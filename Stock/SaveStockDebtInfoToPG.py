import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import re
import time
import yaml
import math
import psycopg2
from psycopg2.extras import execute_batch

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
except Exception as e:
    print(f"⚠️ 설정 파일 로드 실패: {e}")
    HOST = ""

def get_db_connection():
    return psycopg2.connect(host=HOST, dbname=DBNAME, user=USER, password=PASSWORD)

# =========================================================
# 2. 크롤링 로직 (브라우저를 매번 켜지 않고 재사용하도록 변경)
# =========================================================
def get_wisereport_recent_net_debt(driver, stock_code):
    """넘겨받은 크롬 드라이버를 재사용하여 순부채 값을 가져옵니다."""
    url = f"https://comp.wisereport.co.kr/company/c1030001.aspx?cmp_cd={stock_code}&cn="
    
    try:
        driver.get(url)
        time.sleep(2) # 기본 렌더링 대기
        
        # '재무상태표' 탭 찾아서 자동 클릭
        elements = driver.find_elements(By.XPATH, "//*[contains(text(), '재무상태표')]")
        for el in elements:
            if el.is_displayed() and el.tag_name not in ['title', 'h1', 'h2', 'script', 'style', 'html', 'head']:
                try:
                    driver.execute_script("arguments[0].click();", el)
                    break 
                except:
                    continue
                    
        time.sleep(3) # 데이터 불러오기 대기 (속도 개선을 위해 약간 단축)
        
        html = driver.page_source
        tables = pd.read_html(StringIO(html))
    except Exception as e:
        print(f"[{stock_code}] 크롤링/파싱 중 에러: {e}")
        return None

    # 데이터 추출 (이전과 동일한 완벽 정렬 로직)
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

# =========================================================
# 3. Main 실행: DB 연동 및 루프 처리
# =========================================================
if __name__ == "__main__":
    print("🚀 [순부채 수집 시스템] 작동 시작...")
    
    # --- 1) DB에서 최신 날짜의 전 종목 읽어오기 ---
    conn = get_db_connection()
    codes_to_fetch = []
    
    with conn.cursor() as cur:
        # stockmain 테이블에서 가장 최근 거래일의 code, name 조회
        sql_select = """
            SELECT code, name 
            FROM stockmain 
            WHERE trade_date = (SELECT max(trade_date) FROM stockmain);
        """
        cur.execute(sql_select)
        codes_to_fetch = cur.fetchall()  # [(code1, name1), (code2, name2), ...]
        
    print(f"✅ DB에서 수집 대상 {len(codes_to_fetch)}개 종목을 불러왔습니다.")

    # --- 2) 크롬 브라우저 초기화 (단 1번만 실행!) ---
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless") # 백그라운드 실행
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/100.0.4896.75 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # --- 3) 루프 돌면서 크롤링 및 DB 저장 ---
    # 배치 저장을 위해 빈 리스트 생성
    insert_values = []
    success_cnt = 0
    fail_cnt = 0

    print("\n🌐 웹 크롤링 및 DB 저장을 시작합니다... (시간이 다소 소요됩니다)")
    
    for idx, (code, name) in enumerate(codes_to_fetch, 1):
        # 크롤링 함수 호출 (driver를 파라미터로 넘김)
        net_debt_val = get_wisereport_recent_net_debt(driver, code)
        
        # 조건: None이거나 숫자가 아니면 'NaN' 문자열로 처리
        if net_debt_val is None or math.isnan(net_debt_val):
            db_val = 'NaN'
            fail_cnt += 1
        else:
            db_val = net_debt_val
            success_cnt += 1
            
        insert_values.append((code, name, db_val))
        
        # 진행률 표시 (10개마다 출력)
        if idx % 10 == 0:
            print(f"⏳ 진행중... {idx}/{len(codes_to_fetch)} 완료 (최근 종목: {name}, 순부채: {db_val})")

        # [안전장치] 데이터 유실 방지를 위해 100개마다 DB에 중간 저장 (배치 처리)
        if len(insert_values) >= 100:
            with conn.cursor() as cur:
                # PK(code) 충돌 시 name, net_debt, created_at만 업데이트하는 UPSERT 구문
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
            insert_values.clear() # 저장 후 리스트 비우기

    # --- 4) 루프 종료 후 남은 데이터 최종 DB 저장 ---
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
            
    # 마무리 정리
    driver.quit()
    conn.close()
    
    print("\n🎉 [모든 작업 완료]")
    print(f"📊 총 {len(codes_to_fetch)}개 중 성공: {success_cnt}건 / 없음(NaN): {fail_cnt}건")
    print("✅ stock_debt 테이블에 데이터 반영이 완료되었습니다.")
    
