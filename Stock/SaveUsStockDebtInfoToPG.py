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
    if val is None or pd.isna(val) or math.isnan(val) or math.isinf(val): 
        return float('nan')
    return float(val)

def safe_str(val, max_length=100):
    if val is None or pd.isna(val): return None
    return str(val)[:max_length]

def safe_db_val(val):
    """DB Insert를 위해 안전한 문자열 'NaN' 또는 숫자 반환"""
    return 'NaN' if val is None or math.isnan(val) else val

# =========================================================
# 2. 유효 종목만 DB에서 뽑아오기 (메인 테이블 연동)
# =========================================================
def get_valid_tickers_from_db():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
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
# 3. 단일 종목 재무 지표 추출 로직 (분기 데이터 활용)
# =========================================================
def process_financials(ticker):
    # 💡 [핵심] 1초 매너 타임 (yfinance 다중 호출 시 차단 방지)
    time.sleep(1.0) 
    
    # 6개 지표를 NaN으로 초기화
    res = {
        'rev': float('nan'), 'gp': float('nan'), 'op': float('nan'), 
        'ni': float('nan'), 'ta': float('nan'), 'nd': float('nan')
    }
    name = ticker

    try:
        stock = yf.Ticker(ticker)
        
        # 1. 이름 추출
        info = stock.info
        name = safe_str(info.get('shortName', ticker), 100)
        
        # 2. 포괄손익계산서 (최근 분기)
        q_fin = stock.quarterly_financials
        if not q_fin.empty:
            recent_fin = q_fin.iloc[:, 0]  # 최신 분기 데이터 (0번째 열)
            res['rev'] = safe_float(recent_fin.get('Total Revenue'))
            res['gp'] = safe_float(recent_fin.get('Gross Profit'))
            res['op'] = safe_float(recent_fin.get('Operating Income'))
            res['ni'] = safe_float(recent_fin.get('Net Income'))
            
        # 3. 재무상태표 (최근 분기)
        q_bs = stock.quarterly_balance_sheet
        if not q_bs.empty:
            recent_bs = q_bs.iloc[:, 0]
            res['ta'] = safe_float(recent_bs.get('Total Assets'))
            
            # 순부채(Net Debt)가 명시적으로 있으면 사용하고, 없으면 (Total Debt - Cash) 계산
            nd = safe_float(recent_bs.get('Net Debt'))
            if math.isnan(nd):
                td = safe_float(recent_bs.get('Total Debt'))
                cash = safe_float(recent_bs.get('Cash And Cash Equivalents'))
                if not math.isnan(td) and not math.isnan(cash):
                    res['nd'] = td - cash
            else:
                res['nd'] = nd
                
        return (ticker, name, res['rev'], res['gp'], res['op'], res['ni'], res['ta'], res['nd'])
        
    except Exception as e:
        if "401" in str(e) or "429" in str(e):
            print(f"⚠️ [{ticker}] 야후 차단 에러 발생: {e}")
        return (ticker, name, res['rev'], res['gp'], res['op'], res['ni'], res['ta'], res['nd'])

# =========================================================
# 4. 병렬 실행(청크) 및 DB 저장
# =========================================================
def fetch_and_insert_us_debt():
    tickers = get_valid_tickers_from_db()
    if not tickers:
        send_message("❌ 처리할 티커가 없습니다. 메인 수집 코드를 먼저 실행하세요.")
        return

    send_message(f"🚀 [재무 지표 수집] 미국 주식 갱신 시작 (타겟: {len(tickers)} 종목)")
    
    current_date = datetime.now().date()
    
    # 💡 데이터베이스 초기화 (오늘 날짜)
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM stock_debtus WHERE trade_date = %s", (current_date,))
            conn.commit()
            print(f"🗑️ 오늘 날짜({current_date}) 기존 데이터를 초기화했습니다.")
    except Exception as e:
        print(f"⚠️ 기존 데이터 초기화 중 오류 발생: {e}")

    # 💡 300개 단위 청크
    chunk_size = 300
    ticker_chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]
    
    success_cnt = 0
    fail_cnt = 0
    
    for chunk_idx, chunk in enumerate(ticker_chunks):
        print(f"\n📦 청크 {chunk_idx + 1}/{len(ticker_chunks)} 처리 시작... (총 {len(chunk)}개)")
        insert_values = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_financials, t): t for t in chunk}
            
            for idx, future in enumerate(concurrent.futures.as_completed(futures), 1):
                try:
                    code, name, rev, gp, op, ni, ta, nd = future.result()
                except Exception as e:
                    code = futures[future]
                    name = code
                    rev = gp = op = ni = ta = nd = float('nan')
                    print(f"⚠️ [{code}] 스레드 예외 발생: 패스합니다.")

                # DB Insert를 위해 NaN 텍스트 변환
                db_rev = safe_db_val(rev)
                db_gp = safe_db_val(gp)
                db_op = safe_db_val(op)
                db_ni = safe_db_val(ni)
                db_ta = safe_db_val(ta)
                db_nd = safe_db_val(nd)

                if db_nd == 'NaN': fail_cnt += 1
                else: success_cnt += 1
                
                insert_values.append((current_date, code, name, db_rev, db_gp, db_op, db_ni, db_ta, db_nd))
                
                # 💡 [핵심 수정] 진행률 표시에 6개의 모든 지표 출력
                if idx % 10 == 0:
                    current_total = (chunk_idx * chunk_size) + idx
                    log_msg = (f"⏳ 진행중... {current_total}/{len(tickers)} 완료 (최근: {name} | "
                               f"매출: {db_rev}, 총이익: {db_gp}, 영업익: {db_op}, "
                               f"순이익: {db_ni}, 자산: {db_ta}, 순부채: {db_nd})")
                    print(log_msg)

                # 100개 단위 DB 삽입
                if len(insert_values) >= 100:
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            sql_insert = """
                                INSERT INTO stock_debtus (
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

        # 자투리 DB 삽입
        if insert_values:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    sql_insert = """
                        INSERT INTO stock_debtus (
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

        if chunk_idx < len(ticker_chunks) - 1:
            print(f"💤 300개 수집 완료. 야후 API 서버 휴식을 위해 10초 대기합니다...")
            time.sleep(10)

    end_msg = (
        f"🎉 [미국 주식 재무 지표 수집 작업 완료]\n"
        f"📊 총 {len(tickers)}개 대상 중 성공: {success_cnt}건 / 데이터 없음(NaN): {fail_cnt}건\n"
        f"✅ stock_debtus 테이블에 데이터 반영이 완료되었습니다. (기준일: {current_date})"
    )
    print("\n" + end_msg)
    send_message(end_msg)

if __name__ == "__main__":
    fetch_and_insert_us_debt()


