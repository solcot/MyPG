import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
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

def safe_int(val):
    if val is None or pd.isna(val) or math.isnan(val) or math.isinf(val): return None
    return int(val)

def safe_str(val, max_length=100):
    if val is None or pd.isna(val): return None
    return str(val)[:max_length]

# =========================================================
# 💡 [신규] 휴장일/주말 사전 차단용 정찰병 함수
# =========================================================
def is_market_open(target_date_str):
    """애플(AAPL) 주가 유무로 해당 날짜가 휴장일인지 1초 만에 판별합니다."""
    try:
        t_date = pd.to_datetime(target_date_str).date()
        s_date = t_date - timedelta(days=5)
        e_date = t_date + timedelta(days=1)
        
        # 대장주 애플 딱 1개만 조회해봅니다.
        hist = yf.Ticker("AAPL").history(start=s_date, end=e_date)
        
        # 애플 데이터가 없거나, 타겟 날짜가 목록에 없다면 휴장일!
        if hist.empty or t_date not in hist.index.date:
            return False
        return True
    except:
        return False

# 🚨 문제의 원인이었던 yf_session (위장 세션) 코드를 완전히 삭제했습니다!

# =========================================================
# 2. 티커 리스트 수집
# =========================================================
def get_us_tickers():
    try:
        # 💡 [수정] SEC 규정에 맞게 '앱 이름'과 '이메일'을 명시한 정직한 신분증으로 복구
        headers = {'User-Agent': 'StockPy_Project/1.0 (my_personal_email@gmail.com)'}
        res = requests.get('https://www.sec.gov/files/company_tickers.json', headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()
        return list(set([v['ticker'].replace('.', '-') for k, v in data.items()]))
    except Exception as e:
        send_message(f"❌ 티커 수집 실패: {e}")
        return ['AAPL', 'MSFT', 'NVDA']

# =========================================================
# 3. 개별 종목 수집 
# =========================================================
def process_single_ticker(ticker, target_date, start_date, end_date):
    # 💡 1초 매너 타임 유지
    time.sleep(1.0)
    
    try:
        # 🚨 순정 yfinance 호출로 복구 (session 파라미터 삭제)
        stock = yf.Ticker(ticker)
        
        hist = stock.history(start=start_date, end=end_date)
        if hist.empty or target_date not in hist.index.date: return None, None
            
        past_data = hist[hist.index.date <= target_date]
        if len(past_data) < 1: return None, None
            
        today_row = past_data.iloc[-1]
        prev_row = past_data.iloc[-2] if len(past_data) > 1 else today_row

        close_p = safe_float(today_row['Close'])
        open_p = safe_float(today_row['Open'])
        high_p = safe_float(today_row['High'])
        low_p = safe_float(today_row['Low'])
        vol = safe_int(today_row['Volume'])
        prev_close = safe_float(prev_row['Close'])
        
        chg_p = close_p - prev_close if close_p and prev_close else 0.0
        chg_r = (chg_p / prev_close * 100) if prev_close and prev_close != 0 else 0.0

        info = stock.info
        name = safe_str(info.get('shortName', ticker), 100)
        sector = safe_str(info.get('sector', 'Unknown'), 50)
        shares = safe_int(info.get('sharesOutstanding'))
        mcap = safe_int(info.get('marketCap'))
        if mcap is None and shares and close_p: mcap = int(close_p * shares)
        t_val = int(close_p * vol) if close_p and vol else 0

        main_val = (target_date, ticker, name, close_p, chg_p, chg_r, open_p, high_p, low_p, vol, t_val, mcap, shares, sector)

        eps = safe_float(info.get('trailingEps'))
        per = safe_float(info.get('trailingPE'))
        f_eps = safe_float(info.get('forwardEps'))
        f_per = safe_float(info.get('forwardPE'))
        bps = safe_float(info.get('bookValue'))
        pbr = safe_float(info.get('priceToBook'))
        
        div_rate = safe_float(info.get('dividendRate', 0.0))
        if div_rate and close_p and close_p > 0:
            div_yield = round((div_rate / close_p) * 100, 2)
        else:
            div_yield = 0.0

        fdt_val = (target_date, ticker, name, close_p, chg_p, chg_r, eps, per, f_eps, f_per, bps, pbr, div_rate, div_yield)

        return main_val, fdt_val
    
    except Exception as e: 
        # 만약 진짜 에러(401 등)가 나면 조용히 넘어가지 않고 콘솔에 출력해서 원인을 알려줍니다.
        if "401" in str(e) or "429" in str(e):
            print(f"⚠️ [{ticker}] 야후 차단 에러 발생: {e}")
        return None, None

# =========================================================
# 4. 병렬 실행 및 청크(Chunk) 휴식 로직
# =========================================================
def run_collection(trade_date_str):
    # 💡 [핵심] 1만 개 돌기 전에 정찰병 출동! 휴장일이면 즉시 스킵
    print(f"🔍 [{trade_date_str}] 휴장일 여부 사전 검사 중...")
    if not is_market_open(trade_date_str):
        send_message(f"⏸️ [{trade_date_str}] 미국장 휴일(주말/공휴일)입니다. 1만 개 수집을 스킵하고 즉시 종료합니다.")
        return

    # 휴장일이 아니면 정상 수집 시작
    t_date = pd.to_datetime(trade_date_str).date()
    s_date, e_date = t_date - timedelta(days=7), t_date + timedelta(days=1)
    tickers = get_us_tickers()
    
    send_message(f"🚀 [순정 스텔스 모드] {trade_date_str} 미국주식 {len(tickers)}개 수집 시작")
    
    main_list, fdt_list = [], []
    
    chunk_size = 300
    ticker_chunks = [tickers[i:i + chunk_size] for i in range(0, len(tickers), chunk_size)]
    
    processed_count = 0
    
    for chunk_idx, chunk in enumerate(ticker_chunks):
        print(f"\n📦 청크 {chunk_idx + 1}/{len(ticker_chunks)} 처리 시작... (총 {len(chunk)}개)")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_single_ticker, t, t_date, s_date, e_date): t for t in chunk}
            
            for future in concurrent.futures.as_completed(futures):
                processed_count += 1
                if processed_count % 100 == 0: 
                    print(f"⏳ 진행 중... {processed_count}/{len(tickers)}")
                    
                m, f = future.result()
                if m: 
                    main_list.append(m)
                    fdt_list.append(f)
        
        if chunk_idx < len(ticker_chunks) - 1:
            print(f"💤 300개 수집 완료. (현재까지 성공: {len(main_list)}개) 10초간 휴식합니다...")
            time.sleep(10)

    if not main_list:
        send_message("❌ 수집 데이터 없음 (미국 휴장일 또는 아직 차단이 안 풀렸습니다)")
        return

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM stockmainus WHERE trade_date = %s", (t_date,))
                execute_batch(cur, """INSERT INTO stockmainus (trade_date, code, name, close_price, change_price, change_rate, open_price, high_price, low_price, volume, trade_value, market_cap, shares_out, sector) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", main_list, page_size=1000)
                
                cur.execute("DELETE FROM stockfdtus WHERE trade_date = %s", (t_date,))
                execute_batch(cur, """INSERT INTO stockfdtus (trade_date, code, name, close_price, change_price, change_rate, eps, per, forward_eps, forward_per, bps, pbr, dividend_per_share, dividend_yield) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", fdt_list, page_size=1000)
            conn.commit()
        send_message(f"✅ [{trade_date_str}] 미국 주식 DB 덮어쓰기 완료 (총 {len(main_list)} 종목 성공)")
    except Exception as e: send_message(f"❌ DB 오류: {e}")

if __name__ == "__main__":
    target = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    #trade_date_p = datetime.strptime('20260416', "%Y%m%d")
    #target = trade_date_p.strftime('%Y-%m-%d')

    run_collection(target)


