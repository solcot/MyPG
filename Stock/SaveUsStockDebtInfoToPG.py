import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import yaml
import requests
import math
import time
import warnings
import concurrent.futures

warnings.filterwarnings('ignore', category=UserWarning)

# =========================================================
# 1. 설정 및 공통 함수
# =========================================================
try:
    with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
        _cfg = yaml.load(f, Loader=yaml.FullLoader)
    DISCORD_WEBHOOK_URL = _cfg.get('DISCORD_WEBHOOK_URL', '')
    HOST = _cfg['HOST']
    DBNAME = _cfg['DBNAME']
    USER = _cfg['USER']
    PASSWORD = _cfg['PASSWORD']
except Exception as e:
    print(f"⚠️ 설정 로드 실패: {e}")
    exit()

def send_message(msg):
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    if DISCORD_WEBHOOK_URL:
        try: requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
        except: pass
    print(message['content'], flush=True)

def get_db_connection():
    return psycopg2.connect(host=HOST, dbname=DBNAME, user=USER, password=PASSWORD)

def safe_float(val):
    if val is None or pd.isna(val) or math.isnan(val) or math.isinf(val): return None
    return float(val)

def safe_str(val, max_length=100):
    if val is None or pd.isna(val): return None
    return str(val)[:max_length]

# =========================================================
# 2. 유효 종목만 DB에서 뽑아오기 (메인 테이블 연동)
# =========================================================
def get_valid_tickers_from_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 방금 9378개 수집 성공한 그 '진짜' 종목들만 타겟으로 삼습니다!
                cur.execute("""
                    SELECT DISTINCT code 
                    FROM stockmainus 
                    WHERE trade_date = (SELECT MAX(trade_date) FROM stockmainus)
                """)
                return [row[0] for row in cur.fetchall()]
    except Exception as e:
        send_message(f"❌ DB 티커 로드 실패: {e}")
        return []

# =========================================================
# 3. 단일 종목 순부채 처리 (순정 스텔스 모드 + NaN 보장)
# =========================================================
def process_debt(ticker):
    # 💡 [핵심] 1초 매너 타임
    time.sleep(1.0) 
    
    try:
        # 🚨 순정 yfinance 호출로 복구
        stock = yf.Ticker(ticker)
        info = stock.info
        name = safe_str(info.get('shortName', ticker), 100)
        
        total_debt = safe_float(info.get('totalDebt'))
        total_cash = safe_float(info.get('totalCash'))
        
        # 💡 데이터가 아예 없으면 NaN 처리
        if total_debt is None and total_cash is None:
            return (ticker, name if name else ticker, float('nan'))
        
        td = total_debt if total_debt is not None else 0.0
        tc = total_cash if total_cash is not None else 0.0
        net_debt = td - tc
        
        return (ticker, name, net_debt)
        
    except Exception as e:
        # 통신 에러가 나더라도 DB 이빨이 빠지지 않게 NaN으로 자리 유지
        if "401" in str(e) or "429" in str(e):
            print(f"⚠️ [{ticker}] 야후 차단 에러 발생: {e}")
        return (ticker, ticker, float('nan'))

# =========================================================
# 4. 병렬 실행(청크) 및 DB 저장
# =========================================================
def fetch_and_insert_us_debt():
    tickers = get_valid_tickers_from_db()
    if not tickers:
        send_message("❌ 처리할 티커가 없습니다. 메인 수집 코드를 먼저 실행하세요.")
        return

    send_message(f"🚀 [순정 스텔스 모드] 순부채 갱신 시작 (타겟: {len(tickers)} 종목)")
    
    debt_values = []
    
    # 💡 300개 단위 청크
    chunk_size = 300
    ticker_chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]
    
    processed_count = 0
    
    # 💡 3명의 일꾼으로 가장 안전하게 접근
    for chunk_idx, chunk in enumerate(ticker_chunks):
        print(f"\n📦 청크 {chunk_idx + 1}/{len(ticker_chunks)} 처리 시작... (총 {len(chunk)}개)")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_debt, t): t for t in chunk}
            
            for future in concurrent.futures.as_completed(futures):
                processed_count += 1
                if processed_count % 100 == 0: 
                    print(f"⏳ 진행 중... ({processed_count}/{len(tickers)})")
                
                result = future.result()
                if result: 
                    debt_values.append(result)
                    
        if chunk_idx < len(ticker_chunks) - 1:
            print(f"💤 300개 수집 완료. (현재: {len(debt_values)}개) 서버 휴식을 위해 10초 대기합니다...")
            time.sleep(10)

    if not debt_values:
        send_message("❌ 수집된 순부채 데이터가 없습니다.")
        return

    # =========================================================
    # 🚨 DB Insert (trade_date 반영 및 당일 데이터 삭제 후 삽입)
    # =========================================================
    try:
        # 1. 실행되는 현재 시점의 날짜(trade_date) 구하기
        current_date = datetime.now().date()
        
        # 2. 기존 (code, name, net_debt) 구조 맨 앞에 current_date 추가
        insert_data = [(current_date, item[0], item[1], item[2]) for item in debt_values]

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 3. 해당 날짜 데이터만 완벽하게 초기화 (Truncate 효과)
                cur.execute("DELETE FROM stock_debtus WHERE trade_date = %s", (current_date,))
                
                # 4. 새로운 스키마에 맞춰 일괄 Insert
                sql_insert = """
                    INSERT INTO stock_debtus (trade_date, code, name, net_debt) 
                    VALUES (%s, %s, %s, %s)
                """
                execute_batch(cur, sql_insert, insert_data, page_size=1000)
                
            conn.commit()
            
        nan_count = sum(1 for item in debt_values if math.isnan(item[2]))
        valid_count = len(debt_values) - nan_count
        
        send_message(f"✅ 미국 주식 순부채(Net Debt) DB 갱신 완료 [기준일: {current_date}] (정상: {valid_count}개 / 미제공(NaN): {nan_count}개)")
        
    except Exception as e:
        send_message(f"❌ 순부채 DB 오류: {e}")

if __name__ == "__main__":
    fetch_and_insert_us_debt()


