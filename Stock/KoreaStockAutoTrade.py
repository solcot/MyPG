import sys
import requests
from requests.exceptions import ConnectTimeout, ReadTimeout, Timeout, ConnectionError
import json
import time
import yaml
import random
import math
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from holidayskr import is_holiday
import configparser # 추가
import os # 파일 존재 여부 확인 및 삭제를 위해 os 모듈 추가
import psycopg2
from psycopg2.extras import execute_batch
import pandas_market_calendars as mcal
import warnings
warnings.filterwarnings('ignore', category=UserWarning)

with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
DISCORD_WEBHOOK_URL_MAIN = _cfg['DISCORD_WEBHOOK_URL_MAIN']
URL_BASE = _cfg['URL_BASE']
HOST = _cfg['HOST']
DBNAME = _cfg['DBNAME']
USER = _cfg['USER']
PASSWORD = _cfg['PASSWORD']

# SettingReload.ini 파일을 위한 ConfigParser 객체 전역 선언 (또는 함수 바깥)
RELOAD_CONFIG_PATH = 'C:\\StockPy\\SettingReload.ini'
RELOAD_CONFIG = configparser.ConfigParser()

def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    try:
        requests.post(DISCORD_WEBHOOK_URL, data=message, timeout=5)
    except Exception as e:
        print(f"❌ Discord 전송 실패: {e}", flush=True)
    print(message, flush=True)

def send_message_main(msg):
    """디스코드 메세지 전송"""
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    try:
        requests.post(DISCORD_WEBHOOK_URL_MAIN, data=message, timeout=5)
    except Exception as e:
        print(f"❌ Discord 전송 실패: {e}", flush=True)
    #print(message, flush=True)

def get_db_connection():
    """데이터베이스 연결 객체를 반환하는 함수"""
    return psycopg2.connect(
        host=HOST,
        dbname=DBNAME,
        user=USER,
        password=PASSWORD
    )

def get_access_token():
    """토큰 발급"""
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
    "appkey":APP_KEY, 
    "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    ACCESS_TOKEN = res.json()["access_token"]
    return ACCESS_TOKEN
    
def hashkey(datas):
    """암호화"""
    PATH = "uapi/hashkey"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
    'content-Type' : 'application/json',
    'appKey' : APP_KEY,
    'appSecret' : APP_SECRET,
    }
    res = requests.post(URL, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]
    return hashkey

def get_last_trading_day():
    day = datetime.today() - timedelta(days=1)
    while day.weekday() >= 5 or is_holiday(day.strftime("%Y-%m-%d")):
        day -= timedelta(days=1)
    return day.strftime('%Y%m%d')

# 1. 공통 함수 (기존 로직 유지)
def get_all_symbols_by_ma(p_trade_date, p_max_price, p_ma):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # DB의 프로시저 호출 시 차감된 p_max_price가 전달됩니다.
                sql = f"select * from get_stock_ma{p_ma}(%s, %s);"
                cur.execute(sql, (p_trade_date, p_max_price))
                rows = cur.fetchall()
                res_dict = {str(code).zfill(6): name for code, name in rows}
        
        send_message(f"✅ [{p_trade_date}] {p_ma}일 이평 매수종목: {len(res_dict)}건 (기준가: {p_max_price:,.0f}원)")
        return res_dict
    except Exception as e:
        send_message(f"❌ {p_ma}일 DB 조회 중 오류: {e}")
        return {}

def get_all_symbols_sell(p_trade_date='20250901'):
    trade_date = p_trade_date

    # PostgreSQL 접속 후 쿼리 실행
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = """
                    select * from get_stock_sell(%s);
                """
                cur.execute(sql, (trade_date,))
                rows = cur.fetchall()

                symbols_name_dict = {str(code).zfill(6): name for code, name in rows}

        send_message(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 매도종목 반환")
        send_message_main(f"✅ [{trade_date}]일 DB 조회 완료: {len(symbols_name_dict)}건 매도종목 반환")
        #print(symbols_name_dict)
        return symbols_name_dict

    except Exception as e:
        send_message(f"❌ DB 조회 중 오류 발생: {e}")
        return {}

# --- ✨ 손절 (Trailing Stop) 로직 함수 ✨ ---
def check_trailing_stop_loss(
    stock_dict, 
    trailing_losses, 
    blocked_symbols,   # 🚨 추가: 물타기 제한에 걸린 종목 관리용 set
    stop_loss_threshold=-7.0, 
    trailing_rebound=1.0, 
    stop_abs_loss_threshold=-15.0, 
    add_lose_pct=-5.0, 
    max_mooling=5
):
    """
    손절 감시 (물타기 적용):
    1. 지속적인 하락 중 -5% 초과 시 무조건 손절 (현재 비활성화됨)
    2. 손실이 줄었다가 다시 악화되면 트레일링 손절 대신 물타기 신호
    3. 물타기 횟수 제한 (max_mooling, 기본 5회)
       → 제한 도달 시 🚫 메시지는 딱 1번만 출력
    """
    stopped = []

    for sym, info in stock_dict.items():
        current_price = get_current_price(sym)
        bought_price = info.get("매수가")
        # ✅ 방어 코드 추가
        if current_price is None or bought_price is None or bought_price == 0:
            send_message(f"⚠️ [check_trailing_stop_loss] {info.get('종목명')}({sym}) 매수가 비정상 (bought_price={bought_price}) → 계산 건너뜀")
            continue

        profit_pct = round(((current_price / bought_price) - 1) * 100, 2)

        # ✅ 동적 손절 임계치 계산
        buy_count = count_buy_record(sym)
        mool_count = buy_count - 1
        adjusted_threshold = stop_loss_threshold + (mool_count * add_lose_pct)

        #--- # 1️⃣ -5% 이상 손실 시 무조건 손절
        #--- if profit_pct <= stop_abs_loss_threshold:
        #---     send_message(f"😭 [손절]{info.get('종목명')}({sym}) 손실 {stop_abs_loss_threshold:.2f}% 초과! 강제손절 (손절률 {profit_pct:.2f}%)")
        #---     send_message_main(f"😭 [손절]{info.get('종목명')}({sym}) 손실 {stop_abs_loss_threshold:.2f}% 초과! 강제손절 (손절률 {profit_pct:.2f}%)")
        #---     stopped.append(sym)
        #---     continue  # 더 이상 체크할 필요 없음
                                                             
        # 2️⃣ 트레일링 손절 조건 확인
        if profit_pct < 0:
            # 최저 손실 갱신
            if sym not in trailing_losses or profit_pct > trailing_losses[sym]:
                trailing_losses[sym] = profit_pct

            # 손실 반등 후 재하락 감지
            if trailing_losses[sym] - profit_pct >= trailing_rebound and profit_pct <= adjusted_threshold:

                # 물타기 횟수 제한 확인
                if mool_count >= max_mooling:
                    if sym not in blocked_symbols:   # 🚫 이미 로그 찍은 종목은 생략
                        send_message(f"🚫 [물타기중단]{info.get('종목명')}({sym}) "
                                     f"물타기 {max_mooling}회 도달 → 추가 물타기 불가")
                        send_message_main(f"🚫 [물타기중단]{info.get('종목명')}({sym}) "
                                          f"물타기 {max_mooling}회 도달 → 추가 물타기 불가")
                        blocked_symbols.add(sym)  # ✅ 상태 기록
                else:
                    send_message(f"🟢 [물타기신호 {mool_count+1}회차]{info.get('종목명')}({sym}) "
                                 f"손실률 {profit_pct:.2f}% (임계치 {adjusted_threshold:.2f}%) → 물타기 대상")
                    send_message_main(f"🟢 [물타기신호 {mool_count+1}회차]{info.get('종목명')}({sym}) "
                                      f"손실률 {profit_pct:.2f}% (임계치 {adjusted_threshold:.2f}%) → 물타기 대상")
                    stopped.append(sym)
        else:
            # 손실이 아닌 경우 기록 제거
            if sym in trailing_losses:
                trailing_losses.pop(sym, None)

    return stopped

# --- ✨ 익절 (Trailing Stop) 로직 함수 + 불타기 ✨ ---
def check_profit_taking_with_trailing_stop(
    stock_dict,
    trailing_peak_prices,
    break_even_pct1,
    break_even_lose_pct1,
    break_even_pct2,
    break_even_lose_pct2,
    break_even_pct3,
    break_even_lose_pct3,
    take_profit_pct,
    take_profit_lose_pct
):
    """
    4단계 트레일링 스탑 로직 + 불타기 신호 생성
    """
    profited = []
    burn_in_list = []

    for sym, info in stock_dict.items():
        current_price = get_current_price(sym)
        if current_price is None:
            continue

        bought_price = info.get('매수가')
        if bought_price is None or bought_price == 0:
            continue

        profit_pct = round(((current_price / bought_price) - 1) * 100, 2)
        sym_name = info.get('종목명', sym)

        # 초기 상태 등록
        if sym not in trailing_peak_prices:
            if profit_pct >= break_even_pct1:
                send_message(f"🟡 {sym_name}({sym}) {break_even_pct1}% 도달 → 1단계 트레일링 시작")
                send_message_main(f"🟡 {sym_name}({sym}) {break_even_pct1}% 도달 → 1단계 트레일링 시작")
                trailing_peak_prices[sym] = {'stage': 1, 'peak_price': current_price}
                burn_in_list.append(sym)  # 1단계 도달시만 불타기 수행
            continue

        # 상태 불러오기
        stage = trailing_peak_prices[sym]['stage']
        peak_price = trailing_peak_prices[sym]['peak_price']

        # 최고가 갱신
        if current_price > peak_price:
            trailing_peak_prices[sym]['peak_price'] = current_price
            peak_price = current_price

        # --- 1단계 트레일링 ---
        if stage == 1:
            if profit_pct >= break_even_pct2:
                send_message(f"🟡🟡 {sym_name}({sym}) {break_even_pct2}% 도달 → 2단계 트레일링 시작")
                send_message_main(f"🟡🟡 {sym_name}({sym}) {break_even_pct2}% 도달 → 2단계 트레일링 시작")
                trailing_peak_prices[sym] = {'stage': 2, 'peak_price': current_price}
            elif current_price <= peak_price * (1 - abs(break_even_lose_pct1) / 100):
                send_message(f"😄 [단계1]{sym_name}({sym}) 1단계 최고가 대비 {abs(break_even_lose_pct1)}% 하락! (익절률 {profit_pct:.2f}%)")
                send_message_main(f"😄 [단계1]{sym_name}({sym}) 1단계 최고가 대비 {abs(break_even_lose_pct1)}% 하락! (익절률 {profit_pct:.2f}%)")
                profited.append(sym)

        # --- 2단계 트레일링 ---
        elif stage == 2:
            if profit_pct >= break_even_pct3:
                send_message(f"🟡🟡🟡 {sym_name}({sym}) {break_even_pct3}% 도달 → 3단계 트레일링 시작")
                send_message_main(f"🟡🟡🟡 {sym_name}({sym}) {break_even_pct3}% 도달 → 3단계 트레일링 시작")
                trailing_peak_prices[sym] = {'stage': 3, 'peak_price': current_price}
            elif current_price <= peak_price * (1 - abs(break_even_lose_pct2) / 100):
                send_message(f"😄😄 [단계2]{sym_name}({sym}) 2단계 최고가 대비 {abs(break_even_lose_pct2)}% 하락! (익절률 {profit_pct:.2f}%)")
                send_message_main(f"😄😄 [단계2]{sym_name}({sym}) 2단계 최고가 대비 {abs(break_even_lose_pct2)}% 하락! (익절률 {profit_pct:.2f}%)")
                profited.append(sym)

        # --- 3단계 트레일링 ---
        elif stage == 3:
            if profit_pct >= take_profit_pct:
                send_message(f"🟡🟡🟡🟡 {sym_name}({sym}) {take_profit_pct}% 도달 → 4단계 트레일링 시작")
                send_message_main(f"🟡🟡🟡🟡 {sym_name}({sym}) {take_profit_pct}% 도달 → 4단계 트레일링 시작")
                trailing_peak_prices[sym] = {'stage': 4, 'peak_price': current_price}
            elif current_price <= peak_price * (1 - abs(break_even_lose_pct3) / 100):
                send_message(f"😄😄😄 [단계3]{sym_name}({sym}) 3단계 최고가 대비 {abs(break_even_lose_pct3)}% 하락! (익절률 {profit_pct:.2f}%)")
                send_message_main(f"😄😄😄 [단계3]{sym_name}({sym}) 3단계 최고가 대비 {abs(break_even_lose_pct3)}% 하락! (익절률 {profit_pct:.2f}%)")
                profited.append(sym)

        # --- 4단계 트레일링 ---
        elif stage == 4:
            if current_price <= peak_price * (1 - abs(take_profit_lose_pct) / 100):
                send_message(f"😄😄😄😄 [단계4]{sym_name}({sym}) 4단계 최고가 대비 {abs(take_profit_lose_pct)}% 하락! (익절률 {profit_pct:.2f}%)")
                send_message_main(f"😄😄😄😄 [단계4]{sym_name}({sym}) 4단계 최고가 대비 {abs(take_profit_lose_pct)}% 하락! (익절률 {profit_pct:.2f}%)")
                profited.append(sym)

    return profited, burn_in_list

def get_current_price(code="005930"):
    """
    현재가 조회 함수 (재시도 포함)
    - 최대 3번까지 재시도
    - 각 재시도마다 대기시간 증가 (1초 → 2초 → 3초)
    """
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": "FHKST01010100"
    }
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": code,
    }

    time.sleep(0.05)
    for i in range(3):  # 최대 3회 재시도
        try:
            res = requests.get(URL, headers=headers, params=params, timeout=5)

            if res.status_code == 200:
                result = res.json()
                price_str = result.get('output', {}).get('stck_prpr')

                if price_str is None:
                    send_message(f"[{code}] 현재가 응답에 가격 정보 없음")
                    return None

                current_price = int(price_str)
                return current_price
            else:
                send_message(f"[{code}] 현재가 조회 실패 (HTTP {res.status_code}) - 재시도 {i+1}/3")
                time.sleep(1 * (i + 1))  # 1초 → 2초 → 3초 대기

        except Exception as e:
            send_message(f"[{code}] 현재가 조회 예외 발생: {e} - 재시도 {i+1}/3")
            time.sleep(1 * (i + 1))

    send_message(f"[{code}] ❌ 현재가 조회 최종 실패. 해당 종목은 건너뜁니다.")
    return None

def get_price_info(code="005930", k_base=0.5, gap_threshold=0.03):
    """
    변동성 돌파 전략 목표가 + 당일 시가를 함께 반환
    전일 종가 대비 일정 % 이상 갭하락 시 매수 제외
    :param code: 종목 코드 (6자리 문자열)
    :param k_base: 변동성 계수 (기본값 0.5)
    :param gap_threshold: 갭 하락 허용 한도 (기본값 3% -> 0.03)
    :return: (target_price, open_price) or (None, None)
    """
    PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"FHKST01010400"
    }
    params = {
        "fid_cond_mrkt_div_code":"J",
        "fid_input_iscd":code,
        "fid_org_adj_prc":"1",
        "fid_period_div_code":"D"
    }

    time.sleep(0.05)
    try:
        res = requests.get(URL, headers=headers, params=params)
        if res.status_code != 200:
            send_message(f"[{code}] 가격 정보 조회 실패 (HTTP {res.status_code})")
            return None, None

        output = res.json().get("output")
        if not output or len(output) < 2:
            send_message(f"[{code}] 일봉 데이터 부족 또는 없음.")
            return None, None

        # 1. 필요한 가격들 정의
        # 오늘 시가
        open_price = int(output[0]['stck_oprc'])
        stock_name = output[0].get('hts_kor_isnm', '')  # 종목명 추가
        # 전일 시가/종가/고가/저가
        prev_open  = int(output[1]['stck_oprc'])
        prev_close = int(output[1]['stck_clpr'])
        prev_high  = int(output[1]['stck_hgpr'])
        prev_low   = int(output[1]['stck_lwpr'])

        # 변동성 돌파 목표가 계산
        total_range = prev_high - prev_low
        kplusvalue = total_range * k_base
        target_price = int(open_price + kplusvalue)

        # -------------------------------
        # 📌 갭 하락/갭 상승 필터 (전일 종가 대비 % 기준)
        # -------------------------------
        gap_rate = (open_price - prev_close) / prev_close

        # 갭 하락
        if gap_rate <= -gap_threshold:
            msg = f"[{code}] {stock_name} 갭하락 {gap_rate*100:.2f}% 발생 -> 매수풀에서 제거"
            send_message(msg)
            send_message_main(msg)
            return None, None

        # 갭 상승
        if gap_rate >= gap_threshold:
            msg = f"[{code}] {stock_name} 갭상승 {gap_rate*100:.2f}% 발생 -> 매수풀에서 제거"
            send_message(msg)
            send_message_main(msg)
            return None, None

        return target_price, open_price

    except (ConnectTimeout, ReadTimeout, Timeout, ConnectionError) as e:
        send_message(f"[{code}] 네트워크 연결 오류/타임아웃: {e}")
        return None, None
        
    except (KeyError, ValueError) as e:
        send_message(f"[{code}] 가격 정보 파싱 오류: {e}")
        return None, None
        
    except Exception as e:
        send_message(f"[{code}] 예상치 못한 오류: {e}")
        return None, None

def format_krw(val):
    """숫자(또는 숫자 문자열)를 천 단위 콤마로 포맷하여 문자열 반환.
       숫자가 아니면 'N/A' 반환 (단위은 함수에서 붙임)."""
    if val is None:
        return "N/A"
    # 문자열이면 쉼표/공백 제거 후 숫자 변환 시도
    try:
        if isinstance(val, str):
            s = val.strip()
            if s == "":
                return "N/A"
            s = s.replace(",", "")         # "1,000" 같은 경우 제거
            num = float(s)
        else:
            num = float(val)
    except Exception:
        return "N/A"

    # 정수이면 정수 형태로 포맷, 아니면 소수 2자리로 포맷 (필요하면 조정)
    if num.is_integer():
        return f"{int(num):,}원"
    else:
        return f"{num:,.2f}원"

def get_stock_balance():
    """주식 잔고조회 - 연속 조회 지원"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"

    stock_dict = {}
    stock_info_list = []
    item_count = 0
    evaluation = []  # <- 추가

    # 초기 헤더/파라미터
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": "TTTC8434R",
        "tr_cont": "",       # 첫 호출 시 공백
        "custtype": "P",
    }

    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",  # 첫 호출 시 공백
        "CTX_AREA_NK100": ""   # 첫 호출 시 공백
    }

    page = 1
    max_pages = 100  # 무한루프 방지용

    while True:
        #time.sleep(0.3)  # <-- API 호출 제한 회피용 딜레이
        res = requests.get(URL, headers=headers, params=params)
        if res.status_code != 200:
            msg1 = res.json().get('msg1', '')
            if '초당 거래건수' in msg1:
                send_message("[WARN] 초당 거래건수 초과 — 1초 대기 후 재시도")
                time.sleep(1)
                continue  # 재시도
            else:
                send_message(f"[ERROR] 주식 잔고 조회 실패: {msg1}")
                sys.exit(1)  # ← 프로그램 강제 종료, #return stock_dict  #break

        response_data = res.json()
        stock_list = response_data.get('output1', [])
        evaluation = response_data.get('output2', [])

        ctx_fk = response_data.get('ctx_area_fk100', '')
        ctx_nk = response_data.get('ctx_area_nk100', '')

        # 받은 데이터 처리
        for stock in stock_list:
            symbol = stock.get('pdno')
            hldg_qty = int(stock.get('hldg_qty', 0))
            buy_price = float(stock.get('pchs_avg_pric', 0))
            product_name = stock.get('prdt_name')
            if hldg_qty > 0:
                item_count += 1
                stock_dict[symbol] = {
                    '종목명': product_name,
                    '현재수량': hldg_qty,
                    '매수가': buy_price
                }
                stock_info_list.append(f"{item_count:02d}.{product_name}({symbol})")

        # 다음 페이지 여부 확인
        tr_cont = res.headers.get('tr_cont', '')
        if tr_cont in ['F', 'M'] and page < max_pages:
            headers['tr_cont'] = "N"   # 연속 조회용
            params['CTX_AREA_FK100'] = ctx_fk
            params['CTX_AREA_NK100'] = ctx_nk
            page += 1
        else:
            break  # 마지막 페이지 또는 최대 페이지 도달

    # 결과 메시지 출력
    send_message(f"====주식 보유잔고====")
    if item_count > 0:
        send_message(f"📋 전체 보유 주식: {item_count}건")
        send_message_main(f"📋 전체 보유 주식: {item_count}건")
        # 원하는 경우 종목 리스트도 출력
        # send_message(f"{':'.join(stock_info_list)}")
    else:
        send_message("📋 현재 보유 주식은 없습니다.")

    if evaluation:
        scts = evaluation[0].get('scts_evlu_amt')
        evlu = evaluation[0].get('evlu_pfls_smtl_amt')
        tot  = evaluation[0].get('tot_evlu_amt')
        send_message(f"💰 주식 평가 금액: {format_krw(scts)}")
        send_message(f"💰 평가 손익 합계: {format_krw(evlu)}")
        send_message_main(f"💰 평가 손익 합계: {format_krw(evlu)}")
        send_message(f"💰 총 평가 금액: {format_krw(tot)}")
    else:
        send_message("평가 정보가 없습니다.")
    send_message("=================")

    return stock_dict

def get_balance(pdno="005930", ord_unpr="65500"):
    """최대주문가능금액 조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8908R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": pdno,
        "ORD_UNPR": str(ord_unpr),   # 주문가격
        "ORD_DVSN": "01",            # 지정가
        "CMA_EVLU_AMT_ICLD_YN": "N",
        "OVRS_ICLD_YN": "N"
    }
    res = requests.get(URL, headers=headers, params=params)
    if res.status_code != 200:
        send_message(f"최대주문가능금액 조회 실패(HTTP {res.status_code})")
        return 0
    
    j = res.json()
    output = j.get('output', {})
    
    # 최대주문가능금액 필드들
    max_ord_psbl_amt = output.get('max_buy_amt')  # 최대매수가능금액
    if max_ord_psbl_amt is None:
        max_ord_psbl_amt = output.get('ord_psbl_amt')  # 주문가능금액
    
    if max_ord_psbl_amt is None:
        send_message("최대주문가능금액 응답에 값 없음")
        return 0
    
    try:
        return int(max_ord_psbl_amt)
    except:
        return 0

def buy(code="005930", qty="1"):
    """주식 시장가 매수"""  
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    data = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": str(int(qty)),
        "ORD_UNPR": "0",
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC0802U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    
    time.sleep(0.05)
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json()['rt_cd'] == '0':
        send_message(f"[매수 성공]{str(res.json())}")
        return True
    else:
        send_message(f"[매수 실패]{str(res.json())}")
        return False

def safe_buy(sym, buy_amount, current_price, stock_name):
    """
    주문 가능 금액을 확인하고 안전하게 매수
    - 최초 주문은 버퍼 0% (1.00) 적용
    - 실패할 때마다 3%씩 더 보수적으로 줄여서 재시도 (최대 6회)
    """
    if current_price is None or current_price <= 0:
        send_message(f"⚠️ {sym} 매수 불가: 현재가 오류 ({current_price}), 매수풀에서 제거")
        return False

    # 실제 사용 가능한 현금 조회
    account_cash = get_balance()
    if account_cash is None or account_cash <= 0:
        send_message(f"⚠️ {sym} 매수 불가: 주문가능금액이 0원으로 조회됨, 매수풀에서 제거")
        return False

    # 매수하려는 금액이 계좌 잔고를 초과하면 cap
    max_cash = int(min(buy_amount, account_cash))
    if max_cash <= 0:
        send_message(f"⚠️ {sym} 매수 불가: 매수금액 0원, 매수풀에서 제거")
        return False

    attempts = 0
    base_ratio = 1.00  # 첫 시도는 100% 버퍼
    while attempts < 6:
        # 시도 횟수에 따라 버퍼를 점점 늘려감 (예: 100% -> 97% → 94% → 91% ...)
        ratio = base_ratio - (attempts * 0.03)

        safe_cash = int(min(buy_amount, max_cash) * ratio)
        qty_to_buy = int(safe_cash // current_price)
        total_buy_amt = qty_to_buy * current_price

        if qty_to_buy <= 0:
            send_message(f"⚠️ {sym} 매수 불가: (safe_cash {safe_cash}원, 현재가 {current_price}원), 매수풀에서 제거")
            return False

        send_message(f"🟢 {sym} 주문시도({attempts+1}회차): 수량={qty_to_buy}, 단가={current_price}, 총액={qty_to_buy*current_price:,}원, 잔고={buy_amount:,}원")
        ok = buy(sym, qty_to_buy)
        if ok:
            add_buy_record(sym, qty_to_buy, current_price, total_buy_amt, stock_name)
            return True

        # 실패 → 다음 루프에서 더 보수적으로 줄여서 재시도
        attempts += 1
        time.sleep(0.2)  # API 호출 간격 확보

    send_message(f"⚠️ {sym} 매수 실패(6회 재시도 후). 매수풀에서 제거")
    return False

def sell(code="005930", qty="1", stock_dict_cache=None, div="모름"):
    """주식 시장가 매도 (매도 전 보유정보 스냅샷 → 매도 후 기록)"""
    # ✅ 1) 매도 직전 스냅샷 확보
    pos = get_position_snapshot(code, stock_dict_cache=stock_dict_cache)
    pre_buy_price = pos['매수가']
    pre_stock_name = pos['종목명']

    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    data = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": qty,
        "ORD_UNPR": "0",
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC0801U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }

    time.sleep(0.05)
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json().get('rt_cd') == '0':
        send_message(f"[매도 성공]{str(res.json())}")

        # ✅ 2) 체결단가는 주문응답에 정확히 없으니, 기존대로 현재가로 기록 (원하면 체결조회 API로 대체 가능)
        sell_price = get_current_price(code) or 0

        # ✅ 3) 매도 이력 기록 (스냅샷 사용)
        write_sell_history(
            code=code,
            qty=qty,
            sell_price=sell_price,
            buy_price=pre_buy_price,
            stock_name=pre_stock_name,
            div=div
        )

        # ✅ 4) BuyDate.ini 에서 제거
        remove_sell_record(code)
        return True
    else:
        send_message(f"[매도 실패]{str(res.json())}")
        return False

def load_settings():
    """Setting.ini 파일에서 설정을 읽어옵니다."""
    config = configparser.ConfigParser()
    config_path = 'C:\\StockPy\\Setting.ini'

    try:
        config.read(config_path, encoding='utf-8')
        send_message(f"✅ 설정 파일 '{config_path}'을(를) 성공적으로 읽었습니다.")
    except Exception as e:
        send_message(f"❌ 설정 파일 '{config_path}' 읽기 실패: {e}")
        send_message("기본 설정값을 사용합니다.")
        return {
            'ACCOUNT_AMT': 7000000,
            'EXCLUDE_LIST': [],
            'TARGET_BUY_COUNT': 25,
            'T_9_TIME': {'hour': 9, 'minute': 0, 'second': 15},
            'T_START_TIME': {'hour': 9, 'minute': 3, 'second': 0},
            'T_SELL_TIME': {'hour': 14, 'minute': 3, 'second': 0},
            'T_EXIT_TIME': {'hour': 14, 'minute': 8, 'second': 0},
            'AMOUNT_TO_BUY': 350000,
            'STOP_ADD_LOSE_PCT': -5.0,
            'MAX_MOOLING': 5,
            'SLIPPAGE_LIMIT': 1.015,
            'STOP_LOSE_PCT': -3.0,
            'STOP_TRAILING_REBOUND': 1.0,
            'STOP_ABS_LOSE_PCT': -5.0,
            'BREAK_EVEN_PCT1' : 3.0,
            'BREAK_EVEN_LOSE_PCT1' : 2.0,
            'BURN_IN_RATIO' : 0.5,
            'BREAK_EVEN_PCT2' : 5.0,
            'BREAK_EVEN_LOSE_PCT2' : 2.0,
            'BREAK_EVEN_PCT3' : 7.0,
            'BREAK_EVEN_LOSE_PCT3' : 2.0,
            'TAKE_PROFIT_PCT': 9.0,
            'TAKE_PROFIT_LOSE_PCT': 2.0,
            'AMOUNT_LIMIT1_TIME': {'hour': 13, 'minute': 0, 'second': 0},
            'AMOUNT_LIMIT1': 0.7,
            'AMOUNT_LIMIT2_TIME': {'hour': 13, 'minute': 30, 'second': 0},
            'AMOUNT_LIMIT2': 0.5,
            'TARGET_K1': 0.7,
            'TARGET_K2_TIME': {'hour': 13, 'minute': 0, 'second': 0},
            'TARGET_K2': 0.5,
            'TARGET_K3_TIME': {'hour': 13, 'minute': 30, 'second': 0},
            'TARGET_K3': 0.3,
            'TOTAL_LOSE_EXIT_PCT' : -2.2,
            'POOL_COUNT' : 15
        }

    settings = {}
    try:
        settings['ACCOUNT_AMT'] = config.getint('General', 'ACCOUNT_AMT')
        exclude_list_str = config.get('General', 'EXCLUDE_LIST', fallback='')
        settings['EXCLUDE_LIST'] = [item.strip() for item in exclude_list_str.split(',') if item.strip()] if exclude_list_str else []
        settings['TARGET_BUY_COUNT'] = config.getint('General', 'TARGET_BUY_COUNT')

        def parse_time_setting(config_obj, prefix):
            hour = config_obj.getint('TimeSettings', f'{prefix}_HOUR')
            minute = config_obj.getint('TimeSettings', f'{prefix}_MINUTE')
            second = config_obj.getint('TimeSettings', f'{prefix}_SECOND')
            return {'hour': hour, 'minute': minute, 'second': second}

        settings['T_9_TIME'] = parse_time_setting(config, 'T_9')
        settings['T_START_TIME'] = parse_time_setting(config, 'T_START')
        settings['T_SELL_TIME'] = parse_time_setting(config, 'T_SELL')
        settings['T_EXIT_TIME'] = parse_time_setting(config, 'T_EXIT')
        settings['AMOUNT_LIMIT1_TIME'] = parse_time_setting(config, 'AMOUNT_LIMIT1')
        settings['AMOUNT_LIMIT2_TIME'] = parse_time_setting(config, 'AMOUNT_LIMIT2')
        settings['TARGET_K2_TIME'] = parse_time_setting(config, 'TARGET_K2')
        settings['TARGET_K3_TIME'] = parse_time_setting(config, 'TARGET_K3')

        settings['AMOUNT_TO_BUY'] = config.getfloat('StrategyParameters', 'AMOUNT_TO_BUY')
        settings['STOP_ADD_LOSE_PCT'] = config.getfloat('StrategyParameters', 'STOP_ADD_LOSE_PCT')
        settings['MAX_MOOLING'] = config.getint('StrategyParameters', 'MAX_MOOLING')
        settings['SLIPPAGE_LIMIT'] = config.getfloat('StrategyParameters', 'SLIPPAGE_LIMIT')
        settings['STOP_LOSE_PCT'] = config.getfloat('StrategyParameters', 'STOP_LOSE_PCT')
        settings['STOP_TRAILING_REBOUND'] = config.getfloat('StrategyParameters', 'STOP_TRAILING_REBOUND')
        settings['STOP_ABS_LOSE_PCT'] = config.getfloat('StrategyParameters', 'STOP_ABS_LOSE_PCT')
        settings['BREAK_EVEN_PCT1'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_PCT1')
        settings['BREAK_EVEN_LOSE_PCT1'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_LOSE_PCT1')
        settings['BURN_IN_RATIO'] = config.getfloat('StrategyParameters', 'BURN_IN_RATIO')
        settings['BREAK_EVEN_PCT2'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_PCT2')
        settings['BREAK_EVEN_LOSE_PCT2'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_LOSE_PCT2')
        settings['BREAK_EVEN_PCT3'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_PCT3')
        settings['BREAK_EVEN_LOSE_PCT3'] = config.getfloat('StrategyParameters', 'BREAK_EVEN_LOSE_PCT3')
        settings['TAKE_PROFIT_PCT'] = config.getfloat('StrategyParameters', 'TAKE_PROFIT_PCT')
        settings['TAKE_PROFIT_LOSE_PCT'] = config.getfloat('StrategyParameters', 'TAKE_PROFIT_LOSE_PCT')
        settings['AMOUNT_LIMIT1'] = config.getfloat('StrategyParameters', 'AMOUNT_LIMIT1')
        settings['AMOUNT_LIMIT2'] = config.getfloat('StrategyParameters', 'AMOUNT_LIMIT2')
        settings['TARGET_K1'] = config.getfloat('StrategyParameters', 'TARGET_K1')
        settings['TARGET_K2'] = config.getfloat('StrategyParameters', 'TARGET_K2')
        settings['TARGET_K3'] = config.getfloat('StrategyParameters', 'TARGET_K3')
        settings['TOTAL_LOSE_EXIT_PCT'] = config.getfloat('StrategyParameters', 'TOTAL_LOSE_EXIT_PCT')
        settings['POOL_COUNT'] = config.getint('StrategyParameters', 'POOL_COUNT')

    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        send_message(f"❌ 설정 파일 파싱 오류: {e}. 설정 값 확인이 필요합니다.")
        return {
            'ACCOUNT_AMT': 7000000,
            'EXCLUDE_LIST': [],
            'TARGET_BUY_COUNT': 25,
            'T_9_TIME': {'hour': 9, 'minute': 0, 'second': 15},
            'T_START_TIME': {'hour': 9, 'minute': 3, 'second': 0},
            'T_SELL_TIME': {'hour': 14, 'minute': 3, 'second': 0},
            'T_EXIT_TIME': {'hour': 14, 'minute': 8, 'second': 0},
            'AMOUNT_TO_BUY': 350000,
            'STOP_ADD_LOSE_PCT': -5.0,
            'MAX_MOOLING': 5,
            'SLIPPAGE_LIMIT': 1.015,
            'STOP_LOSE_PCT': -3.0,
            'STOP_TRAILING_REBOUND': 1.0,
            'STOP_ABS_LOSE_PCT': -5.0,
            'BREAK_EVEN_PCT1' : 3.0,
            'BREAK_EVEN_LOSE_PCT1' : 2.0,
            'BURN_IN_RATIO' : 0.5,
            'BREAK_EVEN_PCT2' : 5.0,
            'BREAK_EVEN_LOSE_PCT2' : 2.0,
            'BREAK_EVEN_PCT3' : 7.0,
            'BREAK_EVEN_LOSE_PCT3' : 2.0,
            'TAKE_PROFIT_PCT': 9.0,
            'TAKE_PROFIT_LOSE_PCT': 2.0,
            'AMOUNT_LIMIT1_TIME': {'hour': 12, 'minute': 0, 'second': 0},
            'AMOUNT_LIMIT1': 0.7,
            'AMOUNT_LIMIT2_TIME': {'hour': 13, 'minute': 0, 'second': 0},
            'AMOUNT_LIMIT2': 0.5,
            'TARGET_K1': 0.7,
            'TARGET_K2_TIME': {'hour': 12, 'minute': 0, 'second': 0},
            'TARGET_K2': 0.5,
            'TARGET_K3_TIME': {'hour': 13, 'minute': 0, 'second': 0},
            'TARGET_K3': 0.3,
            'TOTAL_LOSE_EXIT_PCT' : -2.2,
            'POOL_COUNT' : 15
        }

    return settings

def load_reload_setting():
    """SettingReload.ini 파일에서 RELOAD 값을 읽어옵니다."""
    RELOAD_CONFIG.read(RELOAD_CONFIG_PATH, encoding='utf-8')
    try:
        return RELOAD_CONFIG.getboolean('General', 'RELOAD', fallback=False)
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        send_message(f"❌ SettingReload.ini 읽기 오류: {e}. 기본값 FALSE를 사용합니다.")
        return False

def write_reload_setting(value):
    """SettingReload.ini 파일의 RELOAD 값을 씁니다."""
    if not RELOAD_CONFIG.has_section('General'):
        RELOAD_CONFIG.add_section('General')
    RELOAD_CONFIG.set('General', 'RELOAD', str(value).upper()) # TRUE/FALSE로 저장
    try:
        with open(RELOAD_CONFIG_PATH, 'w', encoding='utf-8') as f:
            RELOAD_CONFIG.write(f)
        send_message(f"✅ SettingReload.ini RELOAD 값을 {value}로 업데이트했습니다.")
    except Exception as e:
        send_message(f"❌ SettingReload.ini 쓰기 오류: {e}")



BUYDATE_FILE = "C:\\StockPy\\BuyDate.ini"
BUYDATE_HISTORY_FILE = "C:\\StockPy\\BuyDate_History.ini"
SELLHISTORY_FILE = "C:\\StockPy\\SellHistory.ini"

def count_buy_record(sym):
    """BUYDATE_FILE에서 특정 symbol이 몇 개 있는지 count"""
    if not os.path.exists(BUYDATE_FILE):
        return 1

    count = 0
    with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            parts = line.strip().split(maxsplit=2)  # 날짜 / 심볼 / 종목명
            if len(parts) >= 2 and parts[1] == sym:
                count += 1
    # count가 0이면 1을 return
    return count if count > 0 else 1

def get_position_snapshot(code, stock_dict_cache=None):
    """
    매도 전에 보유정보 스냅샷을 확보.
    - stock_dict_cache 가 있으면 그걸 우선 사용(루프에서 이미 조회한 최신 보유목록 전달)
    - 없으면 get_stock_balance()로 조회
    """
    stock_dict = stock_dict_cache if stock_dict_cache is not None else get_stock_balance()
    info = stock_dict.get(code, {}) if isinstance(stock_dict, dict) else {}

    # 키 명 보호적으로 처리 (환경 따라 '평균단가' 등일 수 있어 백업 키도 확인)
    buy_price = (
        info.get('매수가')
    )
    stock_name = (
        info.get('종목명')
    )
    hold_qty = info.get('현재수량')

    # 정수/문자 혼합 안전 처리
    try:
        buy_price = int(buy_price)
    except Exception:
        buy_price = 0
    try:
        hold_qty = int(hold_qty)
    except Exception:
        hold_qty = 0

    return {
        '종목명': stock_name,
        '매수가': buy_price,
        '현재수량': hold_qty,
    }

def write_sell_history(code, qty, sell_price, buy_price, stock_name, div):
    """
    매도 이력 기록 (사전에 캡처한 보유정보를 인자로 받음)
    매수일/매도일/보유일 + 기존 항목 기록
    """
    today_str = datetime.now().strftime("%Y%m%d")  # 매도일
    
    # ✅ BuyDate.ini 에서 매수일 찾기
    buy_date_str = None
    if os.path.exists(BUYDATE_FILE):
        with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(maxsplit=2)
                if len(parts) >= 2 and parts[1] == code:
                    buy_date_str = parts[0]  # 첫 번째 컬럼이 매수일
                    break

    # 매수일이 없으면 오늘로 처리
    if not buy_date_str:
        buy_date_str = "20200101"

    # ✅ 보유일 계산 (매도일 - 매수일)
    try:
        d_buy = datetime.strptime(buy_date_str, "%Y%m%d")
        d_sell = datetime.strptime(today_str, "%Y%m%d")
        hold_days = (d_sell - d_buy).days
    except Exception:
        hold_days = 0

    try:
        qty = int(qty)
    except Exception:
        qty = 0
    try:
        sell_price = int(sell_price) if sell_price is not None else 0
    except Exception:
        sell_price = 0
    try:
        buy_price = int(buy_price) if buy_price is not None else 0
    except Exception:
        buy_price = 0

    buy_total = buy_price * qty
    sell_total = sell_price * qty
    profit_rate = ( (sell_price / buy_price - 1) * 100 ) if buy_price > 0 else 0.0
    profit_amt = sell_total - buy_total

    # ✅ 매도 로그 기록: [매수일 매도일 보유일 ... 기존 항목]
    with open(SELLHISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(
            f"{buy_date_str} {today_str} {hold_days} {code} {qty:,} {buy_price:,} {buy_total:,} "
            f"{sell_price:,} {sell_total:,} {profit_amt:,} {profit_rate:.2f}% {div} {stock_name}\n"
        )

def add_buy_record(sym, qty_to_buy, current_price, total_buy_amt, stock_name):
    """매수 기록 추가 (무조건 마지막에 추가)"""
    today_str = datetime.now().strftime("%Y%m%d")
    
    # 파일 존재 여부와 관계없이 무조건 마지막에 기록
    with open(BUYDATE_FILE, "a", encoding="utf-8") as f:
        f.write(f"{today_str} {sym} {qty_to_buy:,} {current_price:,} {total_buy_amt:,} {stock_name}\n")

# --- ★★★ 요청사항이 반영된 수정된 함수 ★★★ ---
def remove_sell_record(sym):
    """매도 시 BuyDate.ini에서 해당 종목을 삭제하고, 삭제된 내역은 BuyDate_History.ini에 기록합니다."""
    if not os.path.exists(BUYDATE_FILE):
        print(f"경고: 원본 파일({BUYDATE_FILE})이 존재하지 않습니다.")
        return

    try:
        # 1. 원본 파일(BuyDate.ini)의 모든 내용을 읽어옵니다.
        with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # 2. 원본 파일은 '덮어쓰기(w)' 모드로, 백업 파일은 '추가(a)' 모드로 엽니다.
        #    이렇게 하면 파일을 한 번만 순회하면서 두 가지 작업을 동시에 처리할 수 있습니다.
        with open(BUYDATE_FILE, "w", encoding="utf-8") as f_buy_new, \
             open(BUYDATE_HISTORY_FILE, "a", encoding="utf-8") as f_history:
            
            # 3. 읽어온 모든 라인을 하나씩 확인합니다.
            for line in lines:
                if not line.strip():  # 빈 줄은 건너뜁니다.
                    continue
                
                parts = line.strip().split(maxsplit=2)
                
                # 4. 라인에서 분리한 두 번째 요소(종목 코드)가 매도한 종목(sym)과 일치하는지 확인합니다.
                if len(parts) >= 2 and parts[1] == sym:
                    # 5. 일치하면, 해당 라인을 BuyDate_History.ini에 씁니다. (백업)
                    f_history.write(line)
                    # continue를 통해 아래 f_buy_new.write(line)가 실행되지 않도록 하여
                    # BuyDate.ini에서는 해당 라인이 삭제되는 효과를 줍니다.
                    continue
                
                # 6. 매도한 종목이 아니면, 해당 라인을 BuyDate.ini에 다시 씁니다. (유지)
                f_buy_new.write(line)

        #print(f"'{sym}' 종목을 {BUYDATE_FILE}에서 삭제하고 {BUYDATE_HISTORY_FILE}에 백업했습니다.")

    except IOError as e:
        print(f"파일 처리 중 오류가 발생했습니다: {e}")

def get_old_symbols(days=5):
    """
    BUYDATE_FILE에서 days일 이상 보유한 종목 조회
    """
    old_symbols = []
    six_days_ago = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

    if not os.path.exists(BUYDATE_FILE):
        return old_symbols

    with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            buy_date = parts[0]
            symbol = parts[1]
            stock_name = " ".join(parts[2:])  # 띄어쓰기 있는 종목명 합치기
            if buy_date <= six_days_ago:
                old_symbols.append((symbol, stock_name))

    return old_symbols

def can_additional_buy():
    """계좌 잔고 확인 후 추가매수 가능 여부 반환"""
    balance = get_balance()
    if balance is None:
        return False
    return balance >= AMOUNT_TO_BUY

def print_today_sell_history():
    """SellHistory.ini 에서 매도일이 오늘인 내역과 총 수익금 출력"""
    today_str = datetime.now().strftime("%Y%m%d")
    if not os.path.exists(SELLHISTORY_FILE):
        send_message("📂 SellHistory.ini 파일이 존재하지 않습니다.")
        return

    total_cnt = 0
    total_profit = 0
    found = False
    with open(SELLHISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            parts = line.strip().split(maxsplit=12)  # 종목명 포함
            if len(parts) < 11:
                continue
            sell_date = parts[1]  # 두 번째 컬럼이 매도일
            if sell_date == today_str:
                send_message("📑 오늘 매도 내역: " + line.strip())
                try:
                    profit_amt = int(parts[9].replace(",", ""))  # 10번째 컬럼 (수익금)
                    total_cnt += 1
                    total_profit += profit_amt
                except Exception:
                    pass
                found = True

    if found:
        send_message(f"📊 오늘 총 수익금: {total_cnt:,}건 {total_profit:,}원")
        send_message("=================")
        send_message_main(f"📊 오늘 총 수익금: {total_cnt:,}건 {total_profit:,}원")
    else:
        send_message("📑 오늘 매도 내역 없음")
        send_message("=================")
        send_message_main("📑 오늘 매도 내역 없음")

def print_month_sell_history():
    """SellHistory.ini 에서 올해 1월부터 현재 월까지의 매도 내역과 총 수익금 출력"""
    if not os.path.exists(SELLHISTORY_FILE):
        send_message("📂 SellHistory.ini 파일이 존재하지 않습니다.")
        return

    now = datetime.now()
    current_year = now.year
    current_month = now.month

    # 1. 1월부터 현재 월까지의 데이터를 담을 딕셔너리 초기화
    monthly_stats = {}
    for m in range(1, current_month + 1):
        month_str = f"{current_year}{m:02d}"  # YYYYMM 형식 (예: 202601, 202602)
        monthly_stats[month_str] = {'cnt': 0, 'profit': 0, 'found': False}

    # 2. 파일 1회 순회하며 해당 월별 데이터 집계
    with open(SELLHISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            parts = line.strip().split(maxsplit=12)  # 종목명 포함
            if len(parts) < 11:
                continue
            
            sell_date = parts[1]  # 두 번째 컬럼이 매도일
            sell_month = sell_date[:6]  # YYYYMM 추출
            
            # 추출한 월(sell_month)이 1월~현재 월 범위에 포함될 경우 누적
            if sell_month in monthly_stats:
                try:
                    profit_amt = int(parts[9].replace(",", ""))  # 10번째 컬럼 (수익금)
                    monthly_stats[sell_month]['cnt'] += 1
                    monthly_stats[sell_month]['profit'] += profit_amt
                    monthly_stats[sell_month]['found'] = True
                except Exception:
                    pass

    # 3. 1월부터 현재 월까지 순서대로 출력 (기존 출력 형식 유지)
    for m in range(1, current_month + 1):
        month_str = f"{current_year}{m:02d}"
        stats = monthly_stats[month_str]
        
        if stats['found']:
            send_message(f"📊 {month_str}월 총 수익금: {stats['cnt']:,}건 {stats['profit']:,}원")
            send_message("=================")
            send_message_main(f"📊 {month_str}월 총 수익금: {stats['cnt']:,}건 {stats['profit']:,}원")
        else:
            send_message(f"📑 {month_str}월 매도 내역 없음")
            send_message("=================")
            send_message_main(f"📑 {month_str}월 매도 내역 없음")

def print_year_sell_history():
    """SellHistory.ini 에서 올해 매도 내역과 총 수익금 출력"""
    today_str = datetime.now().strftime("%Y%m%d")
    this_year = today_str[:4]  # YYYY 형식
    if not os.path.exists(SELLHISTORY_FILE):
        send_message("📂 SellHistory.ini 파일이 존재하지 않습니다.")
        return

    total_cnt = 0
    total_profit = 0
    found = False
    with open(SELLHISTORY_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            parts = line.strip().split(maxsplit=12)  # 종목명 포함
            if len(parts) < 11:
                continue
            sell_date = parts[1]  # 두 번째 컬럼이 매도일
            if sell_date.startswith(this_year):
                try:
                    profit_amt = int(parts[9].replace(",", ""))  # 10번째 컬럼 (수익금)
                    total_cnt += 1
                    total_profit += profit_amt
                except Exception:
                    pass
                found = True

    if found:
        send_message(f"📊 {this_year}년 총 수익금: {total_cnt:,}건 {total_profit:,}원")
        send_message("=================")
        send_message_main(f"📊 {this_year}년 총 수익금: {total_cnt:,}건 {total_profit:,}원")
    else:
        send_message(f"📑 {this_year}년 매도 내역 없음")
        send_message("=================")
        send_message_main(f"📑 {this_year}년 매도 내역 없음")

def check_market_status_pre_market():
    # 1. 한국 거래소(XKRX) 달력 가져오기
    krx = mcal.get_calendar('XKRX')
    
    # 2. 오늘 날짜 확인
    now = datetime.now()
    today_str = now.strftime('%Y-%m-%d')
    
    # 3. 넉넉하게 전후 20일간의 스케줄 조회
    # 종료일을 오늘(now)까지로 제한하거나, 필터링 로직을 강화해야 합니다.
    schedule = krx.schedule(start_date=(now - timedelta(days=20)).strftime('%Y-%m-%d'), 
                            end_date=today_str)
    
    # 4. 오늘이 개장일인지 확인
    is_market_open = today_str in schedule.index.strftime('%Y-%m-%d')
    
    # 5. 마지막 거래일(Last Trading Day) 계산
    # '오늘보다 이전인' 거래일들만 필터링합니다.
    past_trades = schedule[schedule.index < today_str]
    
    if not past_trades.empty:
        last_trading_day = past_trades.index[-1].strftime('%Y%m%d')
    else:
        # 혹시 모를 에러 방지 (데이터가 없을 경우)
        last_trading_day = None

    return is_market_open, last_trading_day
#***********************************************************************************************************
# 자동매매 시작
try:
    msg = f"🚀🚀🚀🚀🚀🚀🚀 StockPy 자동매매 시작"
    send_message(msg)
    send_message_main(msg)

    ACCESS_TOKEN = get_access_token()
    #send_message(f"✅-----[{ACCESS_TOKEN}]-----")
    #ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjkxN2ZlNjRhLTk3ODMtNGI0Yy04ZWZiLTQwY2Y2Y2QxYThhYiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTc2NzMwOTMzMywiaWF0IjoxNzY3MjIyOTMzLCJqdGkiOiJQU1kxaUxka1BsTGE5ajhkYTNJZDZENGlHU3g5REVkU3I4Uk8ifQ.e7gsdKo8tiDKUZIWzaz7cycXp0G7eBeSV2PVbopAwH7PiyWl8-QHf_G6xrtrJuTtG58HACBW4rYeXwdzUOCf8A"

    # --- ✨ 메인 자동매매 루프 시작 ✨ ---
    # 외부 루프: 설정 재로드를 위해 전체 로직을 감쌈
    while True:
        # 루프 시작 직후 추가
        old_sell_done = False  # 5일 이상 보유 종목 매도 플래그

        # --- 설정 파일에서 값 로드 ---------------------------------------------------------------------------------------------
        settings = load_settings()

        POOL_COUNT = settings['POOL_COUNT']
        AMOUNT_TO_BUY = settings['AMOUNT_TO_BUY']
        #trade_date = get_last_trading_day()
        #trade_date = '20250909'
        is_open, trade_date = check_market_status_pre_market()
        #send_message(f"✅ [{is_open},{trade_date}] ---")
        MAX_BUY_PRICE = AMOUNT_TO_BUY

        #ma_list = [20, 40, 60, 90, 120]
        ma_list = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
        # [수정] 이평선별 차감 금액 설정 (키값을 숫자로 두어 계산 편의성 제공)
        #---deduction_map = {
        #---    20: 0,
        #---    40: 200000,
        #---    60: 400000,
        #---    90: 600000,
        #---    120: 800000
        #---}
        deduction_map = {
            10: 800000,
            20: 0,
            30: 100000,
            40: 200000,
            50: 300000,
            60: 400000,
            70: 500000,
            80: 600000,
            90: 700000,
            100: 800000,
            110: 900000,
            120: 950000
        }
        # 각 이평선별 결과를 따로 담을 저장소
        ma_results = {}
        # 전체를 합칠 최종 딕셔너리
        symbols_buy_pool = {}
        for ma in ma_list:
            # [수정] 차감된 매수 기준가 계산
            # deduction_map에 정의되지 않은 ma가 올 경우를 대비해 기본값 0 설정
            current_max_price = MAX_BUY_PRICE - deduction_map.get(ma, 0)
            
            # 데이터 조회 시 차감된 가격(current_max_price) 전달
            pool_part = get_all_symbols_by_ma(trade_date, current_max_price, ma)
            
            # [할당] 개별 저장 (하위 로직용)
            ma_results[str(ma)] = pool_part
            
            # [통합] 전체 풀에 합치기
            symbols_buy_pool.update(pool_part)

        # 필요시 아래 사용
        # pool_20 = ma_results.get('20', {})
        # pool_60 = ma_results.get('60', {})

        # 최종 결과 보고
        send_message(f"📊 최종 통합 매수 종목 총 {len(symbols_buy_pool)}건 이평 매수종목 반환")
        send_message_main(f"📊 최종 통합 매수 종목 총 {len(symbols_buy_pool)}건 이평 매수종목 반환")
        send_message(symbols_buy_pool)
        send_message_main(symbols_buy_pool)

        symbols_sell_pool = get_all_symbols_sell(p_trade_date=trade_date)  # 금일 매도 종목 <- 현재 계좌에 있다면...

        #symbols_buy_pool = {
        #    "005930": "삼성전자",
        #    "035720": "카카오",
        #    "000660": "SK하이닉스",
        #    "068270": "셀트리온",
        #    "005380": "현대차",
        #    # 필요에 따라 더 많은 종목을 여기에 추가합니다.
        #    # "종목코드": "종목명" 형식으로 추가하세요.
        #}

        ACCOUNT_AMT = settings['ACCOUNT_AMT']    #**************** ACCOUNT_AMT/TARGET_BUY_COUNT/df['종가'] 는 항상 같이 고려되야 함....
        #--- # --- ✨ 09시 이전 EXCLUDE_LIST 초기화 로직 ✨ ---
        #--- t_now_check = datetime.now()
        #--- t_9_oclock = t_now_check.replace(hour=9, minute=0, second=0, microsecond=0)
#--- 
        #--- # 09:00:00 이전이면 EXCLUDE_LIST를 강제로 빈 리스트로 설정
        #--- if t_now_check < t_9_oclock:
        #---     send_message("✅ 09시 이전이므로 EXCLUDE_LIST를 초기화합니다.")
        #---     EXCLUDE_LIST = []
        #--- else:
        #---     EXCLUDE_LIST = settings['EXCLUDE_LIST']
        EXCLUDE_LIST = settings['EXCLUDE_LIST']
        TARGET_BUY_COUNT = settings['TARGET_BUY_COUNT']

        T_9_TIME = settings['T_9_TIME']
        T_START_TIME = settings['T_START_TIME']
        T_SELL_TIME = settings['T_SELL_TIME']
        T_EXIT_TIME = settings['T_EXIT_TIME']

        #AMOUNT_TO_BUY = settings['AMOUNT_TO_BUY']   #윗부분에서 처리
        STOP_ADD_LOSE_PCT = settings['STOP_ADD_LOSE_PCT']
        MAX_MOOLING = settings['MAX_MOOLING']

        SLIPPAGE_LIMIT = settings['SLIPPAGE_LIMIT']
        STOP_LOSE_PCT = settings['STOP_LOSE_PCT']
        STOP_TRAILING_REBOUND = settings['STOP_TRAILING_REBOUND']
        STOP_ABS_LOSE_PCT = settings['STOP_ABS_LOSE_PCT']

        BREAK_EVEN_PCT1 = settings['BREAK_EVEN_PCT1']
        BREAK_EVEN_LOSE_PCT1 = settings['BREAK_EVEN_LOSE_PCT1']
        BURN_IN_RATIO = settings['BURN_IN_RATIO']
        BREAK_EVEN_PCT2 = settings['BREAK_EVEN_PCT2']
        BREAK_EVEN_LOSE_PCT2 = settings['BREAK_EVEN_LOSE_PCT2']
        BREAK_EVEN_PCT3 = settings['BREAK_EVEN_PCT3']
        BREAK_EVEN_LOSE_PCT3 = settings['BREAK_EVEN_LOSE_PCT3']
        TAKE_PROFIT_PCT = settings['TAKE_PROFIT_PCT']
        TAKE_PROFIT_LOSE_PCT = settings['TAKE_PROFIT_LOSE_PCT']

        AMOUNT_LIMIT1_TIME = settings['AMOUNT_LIMIT1_TIME']
        AMOUNT_LIMIT1 = settings['AMOUNT_LIMIT1']
        AMOUNT_LIMIT2_TIME = settings['AMOUNT_LIMIT2_TIME']
        AMOUNT_LIMIT2 = settings['AMOUNT_LIMIT2']

        TARGET_K1 = settings['TARGET_K1']
        TARGET_K2_TIME = settings['TARGET_K2_TIME']
        TARGET_K2 = settings['TARGET_K2']
        TARGET_K3_TIME = settings['TARGET_K3_TIME']
        TARGET_K3 = settings['TARGET_K3']

        TOTAL_LOSE_EXIT_PCT = settings['TOTAL_LOSE_EXIT_PCT']
        # --- 설정 파일 로드 끝 ---------------------------------------------------------------------------------------------

        # 1. 제외 리스트가 존재할 경우에만 필터링 수행
        if EXCLUDE_LIST and len(EXCLUDE_LIST) > 0:
            # ma_results 딕셔너리에 저장된 모든 이평선(20, 40, 60, 90, 120)을 순회
            for ma in ma_list:
                ma_str = str(ma)
                
                # 현재 이평선에 해당하는 종목 풀 가져오기
                target_pool = ma_results.get(ma_str, {})
                
                if target_pool:
                    # ✅ 필터링: EXCLUDE_LIST에 없는 종목들로만 새로운 딕셔너리 구성
                    filtered_pool = {
                        sym: name
                        for sym, name in target_pool.items()
                        if sym not in EXCLUDE_LIST
                    }
                    
                    # 필터링된 결과로 기존 ma_results 데이터 업데이트
                    ma_results[ma_str] = filtered_pool
                    
                    # (선택 사항) 필터링 결과 로그 출력
                    diff_count = len(target_pool) - len(filtered_pool)
                    if diff_count > 0:
                        print(f"🚫 {ma}일 이평 풀에서 제외 종목 {diff_count}건 필터링 완료")

            # 2. 전체 통합 풀(symbols_buy_pool)도 필터링된 데이터로 재구성
            #symbols_buy_pool = {}
            #for ma in ma_list:
            #    symbols_buy_pool.update(ma_results.get(str(ma), {}))

        bought_list = [] # 매수 완료된 종목 리스트
        total_cash = get_balance() # 보유 현금 조회 (10,000원 제외)
        if total_cash < 0: # 잔액이 마이너스가 되는 경우 방지
            total_cash = 0
        stock_dict = get_stock_balance() # 보유 주식 조회
        # ACCOUNT_AMT 계산
        total_buy_value = int(sum(
            stock_dict[sym]['현재수량'] * stock_dict[sym]['매수가']
            for sym in stock_dict
        ))
        ACCOUNT_AMT = total_cash + total_buy_value  # 초기 계좌 금액 설정
        #send_message(f"📋 프로그램 시작: ACCOUNT_AMT = {ACCOUNT_AMT:,}원 (현금: {total_cash:,}원, 주식구매가격: {total_buy_value:,}원)")
        for sym in stock_dict.keys():
            bought_list.append(sym)

        t_now = datetime.now()

        # 주식 매수/매도 시간
        t_9 = t_now.replace(**T_9_TIME)
        t_start = t_now.replace(**T_START_TIME)

        t_notbuy = t_now.replace(hour=14, minute=30, second=0,microsecond=0)
        t_oldstocksell = t_now.replace(hour=15, minute=0, second=0, microsecond=0)
        t_notstoploss = t_now.replace(hour=15, minute=10, second=0,microsecond=0)
        #t_notbuy = t_now.replace(hour=15, minute=30, second=0,microsecond=0)
        #t_oldstocksell = t_now.replace(hour=16, minute=0, second=0, microsecond=0)
        #t_notstoploss = t_now.replace(hour=16, minute=10, second=0,microsecond=0)

        t_sell = t_now.replace(**T_SELL_TIME)
        t_exit = t_now.replace(**T_EXIT_TIME)

        #---# 이미 매수한 종목 수를 고려하여 buy_percent 계산
        #---remaining_buy_count = TARGET_BUY_COUNT - len(bought_list)
        #---if remaining_buy_count <= 0:
        #---    buy_percent = 0 # 더 이상 매수할 종목이 없으면 비율을 0으로 설정
        #---else:
        #---    # 소수점 셋째 자리까지 유지하고 넷째 자리부터 버림
        #---    buy_percent = math.floor((100 / remaining_buy_count) * 0.01 * 1000) / 1000
        #---
        #---# 종목별 주문 금액 완화 로직 추가
        #---if t_now >= t_now.replace(**AMOUNT_LIMIT2_TIME):
        #---    buy_amount = int(total_cash * buy_percent * AMOUNT_LIMIT2)  # 매수 비중 줄임
        #---elif t_now >= t_now.replace(**AMOUNT_LIMIT1_TIME):
        #---    buy_amount = int(total_cash * buy_percent * AMOUNT_LIMIT1)  # 매수 비중 줄임
        #---else:
        #---    buy_amount = int(total_cash * buy_percent)

        soldout = False

        send_message("🚀 국내 주식 자동매매 프로그램을 시작합니다.")
        send_message_main("🚀 국내 주식 자동매매 프로그램을 시작합니다.")
        last_stop_loss_check_time = datetime.now() - timedelta(seconds=15) # 손절 초기값 설정 
        last_profit_taking_check_time = datetime.now() - timedelta(seconds=45) # 익절 초기값 설정 
        last_balance_check_time = datetime.now() - timedelta(minutes=15)  # 초기화: 과거로 설정해서 15분후에 출력되도록 이후는 30분마다
        last_heartbeat = datetime.now() - timedelta(minutes=10)
        last_reload_check_time = datetime.now() - timedelta(seconds=10)
        last_can_buy1_flag_time = datetime.now() - timedelta(minutes=11)
        last_can_buy2_flag_time = datetime.now() - timedelta(minutes=12)
        last_can_buy3_flag_time = datetime.now() - timedelta(minutes=13)
        # 슬리피지 초과 감시용 변수들 (초기화 부분)
        slippage_count = {}
        slippage_last_logged = {}
        # 추가: 휴일 종료 플래그
        program_exit = False
        # ✨ 추가: 익절 변동(Trailing Stop)을 위한 딕셔너리
        trailing_peaks = {} 
        # ✨ 추가: 손절 변동(Trailing Stop)을 위한 딕셔너리
        trailing_losses = {}  # 예: {'005930': -1.2}
        blocked_symbols = set()   # 🚨 반드시 초기화 필요

        while True:
            t_now = datetime.now()

            can_buy_flag = can_additional_buy()
            #if not can_buy_flag:
            #    time.sleep(300)            

            # 10분마다 heartbeat 출력
            if (t_now - last_heartbeat).total_seconds() >= 600:
                send_message("✅ 시스템 정상 작동 중입니다.")
                last_heartbeat = t_now

            #today = datetime.today().weekday()
            today = datetime.today()
            #if today.weekday() >= 5 or is_holiday(today.strftime("%Y-%m-%d")):  # 토요일/일요일/휴일 이면 자동 종료
            if not is_open:
                send_message("🛑 휴일이므로 프로그램을 종료합니다.")
                send_message_main("🛑 휴일이므로 프로그램을 종료합니다.")
                program_exit = True # ✨ 플래그 설정 ✨
                break

            # --- ✨ SettingReload.ini 확인 및 재로드 로직 ✨ ---
            # 특정 시간(예: 매분 00초) 또는 주기적으로 재로드 플래그 확인
            if (t_now - last_reload_check_time).total_seconds() >= 60: # 60초가 지났으면 수행
                if load_reload_setting(): # RELOAD = TRUE 인 경우
                    send_message("🔄 SettingReload.ini RELOAD = TRUE 감지! 설정을 재로드합니다.")
                    write_reload_setting(False) # RELOAD를 FALSE로 되돌림
                    break # 내부 while 루프를 종료하고 외부 while 루프로 이동하여 설정 재로드
                last_reload_check_time = t_now # 재로드 체크 후 시간 업데이트
            # --- ✨ 재로드 로직 끝 ✨ ---

            if t_9 < t_now < t_start and soldout == False: # # AM 09:00 ~ AM 09:03 : 잔여 수량 매도
                ##### 장시작시 매수종목중 이평선 역배열에 만난 종목 매도
                for sym, details in stock_dict.items():
                    qty = details.get('현재수량', '0')
                    sell_name = details.get('종목명')
                    if int(qty) > 0:
                        if sym in symbols_sell_pool.keys():
                            send_message(f"✨ [장시작매도]{sell_name}({sym}) 이평선 역배열 매도 수행")
                            send_message_main(f"✨ [장시작매도]{sell_name}({sym}) 이평선 역배열 매도 수행")
                            result = sell(sym, qty, stock_dict_cache=stock_dict, div="장시작이평선역배열")
                            if result:
                                time.sleep(1.5)              
                bought_list = []
                stock_dict = get_stock_balance()
                for sym in stock_dict.keys():
                    bought_list.append(sym)

                ##### 장시작시 이평선 정배열인 종목 신규(or 추가) 매수
                if not can_buy_flag:
                    send_message(f"🚫 장시작 신규매수 중단: 계좌 잔고 부족(<{AMOUNT_TO_BUY:,}원)")
                    send_message_main(f"🚫 장시작 신규매수 중단: 계좌 잔고 부족(<{AMOUNT_TO_BUY:,}원)")     
                else:
                    # ma_list 순서대로 루프 실행 (20 -> 40 -> 60 -> 90 -> 120)
                    for ma in ma_list:
                        target_pool = ma_results.get(ma, {})
                        
                        # 해당 이평선 풀에 종목이 없으면 스킵
                        if not target_pool:
                            continue
                            
                        for sym, stock_name in target_pool.items():
                            # TARGET_BUY_COUNT 도달 여부 체크 (기존 로직 유지)
                            remaining_buy_count = TARGET_BUY_COUNT - len(bought_list)
                            
                            if remaining_buy_count > 1:
                                # [중복 매수 방지] 이미 매수한 종목이면 스킵
                                #if sym in bought_list:
                                #    continue

                                current_price = get_current_price(sym)
                                if current_price is None:
                                    send_message(f"[{stock_name}({sym})] 가격수신실패. 다음 종목으로 넘어갑니다.")
                                    continue 

                                send_message(f"📈 {stock_name}({sym})({current_price}) [{ma}일 이평] 장시작 매수를 시도합니다.")
                                send_message_main(f"📈 {stock_name}({sym})({current_price}) [{ma}일 이평] 장시작 매수를 시도합니다.")
                                
                                # 이평선별로 차감된 금액 계산
                                buy_amount = AMOUNT_TO_BUY - deduction_map.get(ma, 0)
                                
                                # 매수 실행
                                result = safe_buy(sym, buy_amount, current_price, stock_name)
                                
                                if result:
                                    # 매수 성공 시 bought_list에 추가 (실제 safe_buy 내부에서 추가한다면 생략 가능)
                                    # bought_list.append(sym) 
                                    time.sleep(1.5)

                bought_list = []
                stock_dict = get_stock_balance()
                for sym in stock_dict.keys():
                    bought_list.append(sym)

                soldout = True

            if t_start < t_now < t_sell:  # AM 09:03 ~ PM 02:58 : 물타기/익절 감시     
            
                #send_message("루프 시작..................") #루프 시간 측정용

                # 물타기 감시 로직 -------------------------------------------------------  
                if t_notstoploss < t_now < t_sell:  # PM 03:10 ~ PM 03:23 : BREAK_EVEN_PCT 조정
                    STOP_LOSE_PCT = -3000.0
                    STOP_TRAILING_REBOUND = 1.0
                    STOP_ABS_LOSE_PCT = -5000.0
                if (t_now - last_stop_loss_check_time).total_seconds() >= 15: # 15초마다 체크
                    if STOP_LOSE_PCT < 0:
                        stopped = check_trailing_stop_loss(
                            stock_dict=stock_dict,
                            trailing_losses=trailing_losses,
                            blocked_symbols=blocked_symbols,   # ✅ set으로 미리 선언 필요
                            stop_loss_threshold=STOP_LOSE_PCT,
                            trailing_rebound=STOP_TRAILING_REBOUND,
                            stop_abs_loss_threshold=STOP_ABS_LOSE_PCT,
                            add_lose_pct=STOP_ADD_LOSE_PCT,
                            max_mooling=MAX_MOOLING
                        )
                        if stopped:
                            if not can_buy_flag:
                                if (t_now - last_can_buy1_flag_time).total_seconds() >= 900:
                                    send_message(f"🚫 물타기 중단: 계좌 잔고 부족(<{AMOUNT_TO_BUY:,}원)")
                                    send_message_main(f"🚫 물타기 중단: 계좌 잔고 부족(<{AMOUNT_TO_BUY:,}원)")
                                    last_can_buy1_flag_time = t_now
                            else:
                                for sym in stopped:
                                    remaining_buy_count = 100   # TARGET_BUY_COUNT - len(bought_list)
                                    if remaining_buy_count > 0:
                                        current_price = get_current_price(sym)
                                        info = stock_dict.get(sym, {})
                                        if current_price is None or not info:
                                            continue

                                        bought_name = info.get('종목명')
                                        bought_qty = info.get('현재수량', 0)
                                        bought_price = info.get('매수가')

                                        total_cash = get_balance()
                                        buy_percent = math.floor((100 / remaining_buy_count) * 0.01 * 1000) / 1000

                                        # 종목별 주문 금액 완화 로직 추가
                                        if t_now >= t_now.replace(**AMOUNT_LIMIT2_TIME):
                                            buy_amount = int(total_cash * buy_percent * AMOUNT_LIMIT2)  # 매수 비중 줄임
                                        elif t_now >= t_now.replace(**AMOUNT_LIMIT1_TIME):
                                            buy_amount = int(total_cash * buy_percent * AMOUNT_LIMIT1)  # 매수 비중 줄임
                                        else:
                                            buy_amount = int(total_cash * buy_percent)
                                        
                                        if AMOUNT_TO_BUY > 0:
                                            buy_amount = AMOUNT_TO_BUY
                                        #buy_qty = int(buy_amount // current_price)
                                        if buy_amount > 0:
                                            send_message(f"💧 {bought_name}({sym}) 손절 대신 추가 물타기 진행 ({buy_amount:,}원)")
                                            send_message_main(f"💧 {bought_name}({sym}) 손절 대신 추가 물타기 진행 ({buy_amount:,}원)")
                                            result = safe_buy(sym, buy_amount, current_price, stock_name=bought_name)
                                            if result:
                                                soldout = False

                                                if sym in trailing_losses:
                                                    trailing_losses.pop(sym, None)
                                                if sym in trailing_peaks:
                                                    trailing_peaks.pop(sym, None)
                                                                            
                                                time.sleep(0.1)
                                                bought_list = []
                                                stock_dict = get_stock_balance() # 보유 주식 조회
                                                for sym in stock_dict.keys():
                                                    bought_list.append(sym)

                    last_stop_loss_check_time = t_now # 마지막 체크 시간 업데이트
                # 물타기 감시 로직 끝 ------------------------------------------------------------------
                # 익절 감시 로직 -----------------------------------------------------------                
                #---if t_notbuy < t_now < t_sell:  # PM 02:30 ~ PM 03:23 : BREAK_EVEN_PCT 조정
                #---    BREAK_EVEN_PCT1 = 3.0
                #---    BREAK_EVEN_LOSE_PCT1 = 0.3
                #---    BREAK_EVEN_PCT2 = 5.0
                #---    BREAK_EVEN_LOSE_PCT2 = 0.3
                #---    BREAK_EVEN_PCT3 = 7.0
                #---    BREAK_EVEN_LOSE_PCT3 = 0.3
                #---    TAKE_PROFIT_PCT = 9.0
                #---    TAKE_PROFIT_LOSE_PCT = 0.3
                if (t_now - last_profit_taking_check_time).total_seconds() >= 15: # 15초마다 체크
                    profited_flag = 0
                    burn_in_list_flag = 0
                    if BREAK_EVEN_PCT1 > 0:
                        profited, burn_in_list = check_profit_taking_with_trailing_stop(
                            stock_dict=stock_dict,
                            trailing_peak_prices=trailing_peaks,
                            break_even_pct1=BREAK_EVEN_PCT1,
                            break_even_lose_pct1=BREAK_EVEN_LOSE_PCT1,
                            break_even_pct2=BREAK_EVEN_PCT2,
                            break_even_lose_pct2=BREAK_EVEN_LOSE_PCT2,
                            break_even_pct3=BREAK_EVEN_PCT3,
                            break_even_lose_pct3=BREAK_EVEN_LOSE_PCT3,
                            take_profit_pct=TAKE_PROFIT_PCT,
                            take_profit_lose_pct=TAKE_PROFIT_LOSE_PCT
                        )
                        if profited:
                            profited_flag = 1
                            for sym in profited:
                                qty = stock_dict.get(sym, {}).get('현재수량', 0)
                                if qty > 0:
                                    result = sell(sym, qty, stock_dict_cache=stock_dict, div="익절")  # ← 캐시 전달
                                    if result:
                                        if sym in trailing_peaks:
                                            trailing_peaks.pop(sym, None)
                                        if sym in trailing_losses:
                                            trailing_losses.pop(sym, None)
                        if BURN_IN_RATIO > 0:
                            if burn_in_list:
                                if not can_buy_flag:
                                    if (t_now - last_can_buy2_flag_time).total_seconds() >= 900:
                                        send_message(f"🚫 불타기 중단: 계좌 잔고 부족(<{AMOUNT_TO_BUY:,}원)")
                                        send_message_main(f"🚫 불타기 중단: 계좌 잔고 부족(<{AMOUNT_TO_BUY:,}원)")
                                        last_can_buy2_flag_time = t_now
                                else:
                                    burn_in_list_flag = 1
                                    for sym in burn_in_list:
                                        ratio = BURN_IN_RATIO
                                        current_price = get_current_price(sym)
                                        info = stock_dict.get(sym, {})
                                        if current_price is None or not info:
                                            continue

                                        bought_name = info.get('종목명')
                                        bought_qty = info.get('현재수량', 0)
                                        bought_price = info.get('매수가')

                                        if bought_price and bought_qty:
                                            invested = bought_price * bought_qty
                                            amount_to_buy = int(invested * ratio)
                                            #qty_to_buy = int(amount_to_buy // current_price)
                                            if amount_to_buy > 0:
                                                send_message(f"🔥 {bought_name}({sym}) 불타기 매수 진행 ({ratio*100:.1f}%, {amount_to_buy:,}원)")
                                                send_message_main(f"🔥 {bought_name}({sym}) 불타기 매수 진행 ({ratio*100:.1f}%, {amount_to_buy:,}원)")
                                                result = safe_buy(sym, amount_to_buy, current_price, stock_name=bought_name)
                                                if result:
                                                    soldout = False
                        if profited_flag > 0 or burn_in_list_flag > 0:
                            time.sleep(0.1)
                            bought_list = []
                            stock_dict = get_stock_balance() # 보유 주식 조회
                            for sym in stock_dict.keys():
                                bought_list.append(sym)
                    
                    last_profit_taking_check_time = t_now # 마지막 체크 시간 업데이트
                # 익절 감시 로직 끝 -------------------------------------------------------------





                time.sleep(7)





                # ✅ 10분마다 잔고 확인 (예: 09:15, 09:25, 09:35 ...), HTS에서 직접 매수/매도 종목 최신화
                if (t_now - last_balance_check_time).total_seconds() >= 600:  # 1800초 = 30분
                    bought_list = []
                    stock_dict = get_stock_balance() # 보유 주식 조회
                    for sym in stock_dict.keys():
                        bought_list.append(sym)
                    # ✨ 일일 손실 한도 체크 로직 추가 ✨
                    if stock_dict:
                        total_cash = get_balance()  # 현금 잔고 조회 (10,000원 제외)
                        if total_cash < 0:
                            total_cash = 0
                        # 보유 주식의 현재 평가 금액 계산
                        total_stock_value = int(sum(
                            stock_dict[sym]['현재수량'] * get_current_price(sym) 
                            for sym in stock_dict 
                            if get_current_price(sym) is not None
                        ))
                        # 계좌 전체 금액 = 현금 + 주식 평가 금액
                        total_account_value = total_cash + total_stock_value
                        # 초기 계좌 금액 대비 손실률 계산
                        loss_pct = ((total_account_value - ACCOUNT_AMT) / ACCOUNT_AMT) * 100
                        if loss_pct <= TOTAL_LOSE_EXIT_PCT:
                            send_message(f"🚨 계좌 전체 금액 손실 한도({TOTAL_LOSE_EXIT_PCT}%) 도달! 현재 손실률: {loss_pct:.2f}% | 보유 주식 전량 매도 후 프로그램을 종료합니다.")
                            send_message_main(f"🚨 계좌 전체 금액 손실 한도({TOTAL_LOSE_EXIT_PCT}%) 도달! 현재 손실률: {loss_pct:.2f}% | 보유 주식 전량 매도 후 프로그램을 종료합니다.")
                            # 보유 주식 전량 매도
                            #~~~ for sym, details in stock_dict.items():
                            #~~~     qty = details.get('현재수량', '0')
                            #~~~     if int(qty) > 0:
                            #~~~         sell(sym, qty, stock_dict_cache=stock_dict, div="손실한도초과")  # ← 캐시 전달
                            #~~~         time.sleep(1)
                            soldout = True
                            #~~~ bought_list = []
                            program_exit = True # ✨ 플래그 설정 ✨
                            break  # 내부 루프 종료
                    last_balance_check_time = t_now

                #send_message("루프 끝..................") #루프 시간 측정용

            # 루프 내부, t_sell < t_now < t_exit 직전에 추가
            if not old_sell_done and t_oldstocksell < t_now < t_sell:
                old_syms = get_old_symbols(days=1095)
                if old_syms:
                    send_message(f"⏰ 1095일 이상 보유 종목 매도 실행: {len(old_syms)}개")
                    send_message_main(f"⏰ 1095일 이상 보유 종목 매도 실행: {len(old_syms)}개")
                    for sym, stock_name in old_syms:
                        qty = stock_dict.get(sym, {}).get('현재수량', 0)
                        if qty and int(qty) > 0:
                            result = sell(sym, qty, stock_dict_cache=stock_dict, div="보유기간초과")  # ← 캐시 전달
                            time.sleep(1)
                            if result:
                                send_message(f"📉 {stock_name}({sym}) 보유 1095일 경과 → 전량 매도 완료")
                                send_message_main(f"📉 {stock_name}({sym}) 보유 1095일 경과 → 전량 매도 완료")
                # ✅ 매도 직후 보유 최신화
                bought_list = []
                stock_dict = get_stock_balance() # 보유 주식 조회
                for sym in stock_dict.keys():
                    bought_list.append(sym)
                old_sell_done = True

            if t_sell < t_now < t_exit:  # PM 02:58 ~ PM 03:03 : 일괄 매도
                time.sleep(3)

            if t_exit < t_now:  # PM 03:03 ~ :프로그램 종료
                send_message("종료시점 보유주식 조회내역은 아래와 같습니다.")
                get_stock_balance()
                print_today_sell_history()  # ✨ 오늘 매도 내역 출력
                print_month_sell_history()   # ✨ 이번달 매도 내역 출력
                print_year_sell_history()   # ✨ 이번해 매도 내역 출력
                send_message("🛑 운영시간이 아니므로 프로그램을 종료합니다.")
                send_message_main("🛑 운영시간이 아니므로 프로그램을 종료합니다.")
                break

        # 내부 루프가 break로 종료되었을 때 처리
        if program_exit: # ✨ 플래그 확인 ✨
            break # 외부 루프도 종료하여 프로그램 완전히 끝냄
        elif t_exit > t_now: # 프로그램 종료 시간이 아닌데 break 되었다면 (즉, 재로드 때문)
            send_message("🛠️ 설정 재로드를 위해 메인 루프를 다시 시작합니다.")
            send_message_main("🛠️ 설정 재로드를 위해 메인 루프를 다시 시작합니다.")
            continue # 외부 while True 루프의 다음 반복으로 이동
        else: # 프로그램 종료 시간이라면 외부 루프도 종료
            break

except Exception as e:
    send_message(f"[오류 발생]{e}")
    time.sleep(1)

#-- 디버그 필요시 -- except Exception as e:
#-- 디버그 필요시 --     import traceback
#-- 디버그 필요시 --     error_msg = f"[오류 발생] {e}\n{traceback.format_exc()}"
#-- 디버그 필요시 --     send_message(error_msg)
#-- 디버그 필요시 --     print(error_msg)
#-- 디버그 필요시 --     time.sleep(1)
