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

with open('C:\\StockPy2\\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
DISCORD_WEBHOOK_URL_MAIN = _cfg['DISCORD_WEBHOOK_URL_MAIN']
URL_BASE = _cfg['URL_BASE']

# SettingReload.ini 파일을 위한 ConfigParser 객체 전역 선언 (또는 함수 바깥)
RELOAD_CONFIG_PATH = 'C:\\StockPy2\\SettingReload.ini'
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

def fetch_krx_data(mktId, trade_date):
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_form_data = {
        'locale': 'ko_KR',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01501',  # 이 부분이 핵심
        'mktId': mktId,            # 'STK', 'KSQ'
        'trdDd': trade_date,
        'money': '1',              # 원 단위
        'csvxls_isNo': 'false'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'User-Agent': 'Mozilla/5.0'
    }

    send_message(f"OTP 코드 생성 요청 중... 시장: {mktId}, 날짜: {trade_date}")
    otp_response = requests.post(otp_url, data=otp_form_data, headers=headers)
    if otp_response.status_code != 200:
        send_message(f"OTP 요청 실패: 상태 코드 {otp_response.status_code}")
        send_message(otp_response.text)
        return None
    otp_code = otp_response.text

    send_message(f"CSV 파일 다운로드 중... 시장: {mktId}")
    csv_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    csv_response = requests.post(csv_url, data={'code': otp_code}, headers=headers)
    if csv_response.status_code != 200:
        send_message(f"CSV 다운로드 실패: 상태 코드 {csv_response.status_code}")
        send_message(csv_response.text)
        return None

    try:
        df = pd.read_csv(BytesIO(csv_response.content), encoding='euc-kr')
        return df
    except Exception as e:
        send_message(f"CSV 파싱 오류: {e}")
        return None

def get_all_symbols():
    trade_date = get_last_trading_day()
    #trade_date = '20250804'
    send_message(f"✅ 최종 거래일은 {trade_date} 입니다.")
    send_message_main(f"✅ 최종 거래일은 {trade_date} 입니다.")

    df_kospi = fetch_krx_data('STK', trade_date)
    df_kosdaq = fetch_krx_data('KSQ', trade_date)

    if df_kospi is None and df_kosdaq is None:
        send_message("❌ KOSPI와 KOSDAQ 데이터 모두 가져오기 실패")
        return []
    elif df_kospi is None:
        df = df_kosdaq
    elif df_kosdaq is None:
        df = df_kospi
    else:
        df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)

    if df is None or df.empty:
        send_message("❌ 데이터 로드 실패: 데이터프레임이 비어 있습니다.")
        return []

    send_message(f"✅ 전체 종목 수: {len(df)}")
    send_message_main(f"✅ 전체 종목 수: {len(df)}")
    #print("\n✅ 열 이름:")
    #print(df.columns.tolist())
    #print("\n✅ 원본 상위 10개 샘플:")
    #print(df.head(10))

    try:
        df['등락률'] = df['등락률'].astype(str).str.replace('%', '', regex=False).astype(float)
        df['종가'] = pd.to_numeric(df['종가'], errors='coerce')
        df['시가'] = pd.to_numeric(df['시가'], errors='coerce')
        df['고가'] = pd.to_numeric(df['고가'], errors='coerce')
        df['저가'] = pd.to_numeric(df['저가'], errors='coerce')
        df['시가총액'] = pd.to_numeric(df['시가총액'], errors='coerce')
        df['거래량'] = pd.to_numeric(df['거래량'], errors='coerce')
        df['거래대금'] = pd.to_numeric(df['거래대금'], errors='coerce')
    except KeyError as e:
        send_message(f"❌ 열 이름 오류: {e}")
        send_message("사용 가능한 열:", df.columns.tolist())
        return []

    # 거래대금 단위가 억/천 단위일 수 있으므로 조정 확인 필요
    #print("\n✅ 거래대금 단위 확인 (상위 5개):")
    #print(df['거래대금'].head(5))

    # 추가 컬럼 계산
    #df['전일변동폭비율'] = (df['고가'] - df['저가']) / df['저가']
    df['금일등락률'] = (df['종가'] - df['시가']) / df['종가'] * 100

    #*************************************************************************************************************
    # [1번계좌] 대형주
    filtered = df[
        (df['금일등락률'] >= 0.5) &          # -3~7 등락률 범위를 소폭 확장하여 더 많은 잠재 후보군을 포함
        (df['금일등락률'] <= 1.5) &
        (df['종가'] <= 300000) &          # 동전주를 회피하는 최소 가격
        (df['시가총액'] >= 200e10) &      # 시가총액 2조 이상 (너무 작은 종목 제외)
        #(df['시가총액'] < 200e10) &       # 시가총액 2조 이하 (너무 무거운 대형주 제외, 중소형주 집중)
        (df['거래대금'] >= 175e8)         # 거래대금 175억 이상 (최소한의 유동성 확보)
        #(df['전일변동폭비율'] >= 0.03) &   # 전일 변동폭이 최소 5% 이상인 종목
        #(df['전일변동폭비율'] <= 0.20)    # 전일 변동폭이 20% 이하 (지나치게 과열된 종목 제외)
    ].copy()
    #*************************************************************************************************************

    #filtered['점수'] = filtered['전일변동폭비율'] * filtered['거래대금'] * (1 + filtered['등락률'] / 100)
    # 안정성 점수 계산 (기존 공격적 점수 대신)
    filtered['안정성점수'] = (
        filtered['시가총액'] * 0.3 +  # 시총 가중치
        filtered['거래대금'] * 0.3 +   # 유동성 가중치  
        (1 / (abs(filtered['금일등락률']) + 1)) * filtered['거래대금'] * 0.4  # 안정성 가중치
    )

    #top_filtered = filtered.sort_values(by='점수', ascending=False).head(150)
    #top_filtered = filtered.sort_values(by='점수', ascending=False)
    # 점수 기준 정렬
    top_filtered = filtered.sort_values(by='안정성점수', ascending=False)
 
    send_message(f"✅ 최종 선정 종목 수: {len(top_filtered)}")
    send_message_main(f"✅ 최종 선정 종목 수: {len(top_filtered)}")
    #print("\n✅ 상위 점수 종목 샘플:")
    #print(top_filtered[['종목명', '종목코드', '종가', '전일변동폭비율', '거래대금', '점수']].head(10))
    #print(top_filtered)

    # **여기부터 변경 시작:** 종목코드를 키로, 종목명을 값으로 하는 딕셔너리 생성
    symbols_name_dict = {} # 새로운 딕셔너리 생성
    for _, row in top_filtered.iterrows():
        symbol = str(row['종목코드']).zfill(6) # 종목코드를 가져와 6자리 문자열로 만듭니다.
        name = row['종목명'] # 종목명을 가져옵니다.
        symbols_name_dict[symbol] = name # 딕셔너리에 '종목코드': '종목명' 형태로 저장합니다.

    return symbols_name_dict # **변경 끝:** 이 딕셔너리를 반환합니다.

# --- ✨ 손절 (Trailing Stop) 로직 함수 ✨ ---
def check_trailing_stop_loss(stock_dict, trailing_losses, stop_loss_threshold=-3.0, trailing_rebound=1.0, stop_abs_loss_threshold=-5.0):
    """
    손절 감시:
    1. 지속적인 하락 중 -5% 초과 시 무조건 손절
    2. 손실이 줄었다가 다시 악화되면 트레일링 손절
    """
    stopped = []

    for sym, info in stock_dict.items():
        current_price = get_current_price(sym)
        bought_price = info.get('매수가')
        if current_price is None or bought_price is None:
            continue

        profit_pct = round(((current_price / bought_price) - 1) * 100, 2)

        # 1️⃣ -5% 이상 손실 시 무조건 손절
        if profit_pct <= stop_abs_loss_threshold:
            send_message(f"😭😭 [손절2]{info.get('종목명')}({sym}) 손실 {stop_abs_loss_threshold:.2f}% 초과! 강제손절 (손절률 {profit_pct:.2f}%)")
            send_message_main(f"😭😭 [손절2]{info.get('종목명')}({sym}) 손실 {stop_abs_loss_threshold:.2f}% 초과! 강제손절 (손절률 {profit_pct:.2f}%)")
            stopped.append(sym)
            continue  # 더 이상 체크할 필요 없음

        # 2️⃣ 트레일링 손절 조건 확인
        if profit_pct < 0:
            # 최저 손실 갱신
            if sym not in trailing_losses or profit_pct > trailing_losses[sym]:
                trailing_losses[sym] = profit_pct

            # 손실 반등 후 재하락 감지
            if trailing_losses[sym] - profit_pct >= trailing_rebound and profit_pct <= stop_loss_threshold:
                #send_message(f"😭 [손절1]{info.get('종목명')}({sym}) 트레일링 손절 (손절률 {profit_pct:.2f}%)")
                send_message(f"😭 [손절1]{info.get('종목명')}({sym}) 트레일링 손절 (반등률 {trailing_losses[sym]:.2f}%)-(손절률 {profit_pct:.2f}%)")
                #send_message_main(f"😭 [손절1]{info.get('종목명')}({sym}) 트레일링 손절 (손절률 {profit_pct:.2f}%)")
                send_message_main(f"😭 [손절1]{info.get('종목명')}({sym}) 트레일링 손절 (반등률 {trailing_losses[sym]:.2f}%)-(손절률 {profit_pct:.2f}%)")
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

        # 오늘 시가
        open_price = int(output[0]['stck_oprc'])
        # 전일 종가
        prev_close = int(output[1]['stck_clpr'])
        # 전일 고가/저가
        prev_high  = int(output[1]['stck_hgpr'])
        prev_low   = int(output[1]['stck_lwpr'])

        # 변동성 돌파 목표가 계산
        total_range = prev_high - prev_low
        kplusvalue = total_range * k_base
        target_price = int(open_price + kplusvalue)

        # -------------------------------
        # 📌 갭 하락 필터 (전일 종가 대비 % 기준)
        # -------------------------------
        gap_rate = (open_price - prev_close) / prev_close
        if gap_rate <= -gap_threshold:
            send_message(f"[{code}] 갭하락 {gap_rate*100:.2f}% 발생 -> 매수풀에서 제거")
            send_message_main(f"[{code}] 갭하락 {gap_rate*100:.2f}% 발생 -> 매수풀에서 제거")
            selected_symbols_map.pop(code, None)            
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

def get_stock_balance():
    """주식 잔고조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8434R",
        "custtype":"P",
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
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    res = requests.get(URL, headers=headers, params=params)

    if res.status_code != 200:
        send_message(f"주식 잔고 조회 실패: {res.json().get('msg1', '알 수 없는 오류')}")
        return {}

    response_data = res.json()
    stock_list = response_data.get('output1', []) 
    evaluation = response_data.get('output2', [])

    stock_dict = {}
    send_message(f"====주식 보유잔고====")
    
    item_count = 0
    stock_info_list = []  # 주식 정보를 저장할 리스트
    for idx, stock in enumerate(stock_list, start=1):
        # API에서 받은 데이터에서 필요한 정보 추출
        symbol = stock.get('pdno')
        hldg_qty = int(stock.get('hldg_qty', 0))
        buy_price = float(stock.get('pchs_avg_pric', 0))
        product_name = stock.get('prdt_name')

        if hldg_qty > 0: 
            item_count += 1
            # ✨ 매수가를 포함한 상세 정보를 딕셔너리로 저장
            stock_dict[symbol] = {
                '종목명': product_name,
                '현재수량': hldg_qty,
                '매수가': buy_price
            }
            #send_message(f"{item_count:02d}.{product_name}({symbol}): {hldg_qty}주, 매수가:{buy_price:,.2f}원")
            # 리스트 형태로 저장
            stock_info_list.append(f"{item_count:02d}.{product_name}({symbol})")

    # 수정: 보유 주식 건수를 요약해서 한 번만 메시지 전송
    if item_count > 0:
        # 보유 주식 리스트를 콜론으로 구분하여 출력
        stock_list_str = ":".join(stock_info_list)
        send_message(f"📋 현재 보유 주식은 {item_count:02d}건 입니다.\n{stock_list_str}")
        send_message_main(f"📋 현재 보유 주식은 {item_count:02d}건 입니다.")
    else:
        send_message("📋 현재 보유 주식은 없습니다.")

    if evaluation:
        send_message(f"💰 주식 평가 금액: {evaluation[0].get('scts_evlu_amt', 'N/A')}원")
        send_message(f"💰 평가 손익 합계: {evaluation[0].get('evlu_pfls_smtl_amt', 'N/A')}원")
        send_message_main(f"💰 평가 손익 합계: {evaluation[0].get('evlu_pfls_smtl_amt', 'N/A')}원")
        send_message(f"💰 총 평가 금액: {evaluation[0].get('tot_evlu_amt', 'N/A')}원")
    else:
        send_message("평가 정보가 없습니다.")
    send_message(f"=================")

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

def safe_buy(sym, buy_amount, current_price):
    """
    주문 가능 금액을 확인하고 안전하게 매수
    - 최초 주문은 버퍼 0% (1.00) 적용
    - 실패할 때마다 3%씩 더 보수적으로 줄여서 재시도 (최대 6회)
    """
    if current_price is None or current_price <= 0:
        send_message(f"⚠️ {sym} 매수 불가: 현재가 오류 ({current_price}), 매수풀에서 제거")
        selected_symbols_map.pop(sym, None)
        return False

    # 최초 주문가능금액 조회
    #max_cash = get_balance(pdno=sym, ord_unpr=current_price)
    max_cash = buy_amount
    if max_cash <= 0:
        send_message(f"⚠️ {sym} 매수 불가: 주문가능금액이 0원으로 조회됨, 매수풀에서 제거")
        selected_symbols_map.pop(sym, None)
        return False

    attempts = 0
    base_ratio = 1.00  # 첫 시도는 100% 버퍼
    while attempts < 6:
        # 시도 횟수에 따라 버퍼를 점점 늘려감 (예: 100% -> 97% → 94% → 91% ...)
        ratio = base_ratio - (attempts * 0.03)

        safe_cash = int(min(buy_amount, max_cash) * ratio)
        qty_to_buy = safe_cash // current_price

        if qty_to_buy <= 0:
            send_message(f"⚠️ {sym} 매수 불가: (safe_cash {safe_cash}원, 현재가 {current_price}원), 매수풀에서 제거")
            selected_symbols_map.pop(sym, None)
            return False

        send_message(f"🟢 {sym} 주문시도({attempts+1}회차): 수량={qty_to_buy}, 단가={current_price}, 총액={qty_to_buy*current_price:,}원, 잔고={buy_amount:,}원")
        ok = buy(sym, qty_to_buy)
        if ok:
            return True

        # 실패 → 다음 루프에서 더 보수적으로 줄여서 재시도
        attempts += 1
        time.sleep(0.2)  # API 호출 간격 확보

    send_message(f"⚠️ {sym} 매수 실패(6회 재시도 후). 매수풀에서 제거")
    selected_symbols_map.pop(sym, None)
    return False

def sell(code="005930", qty="1"):
    """주식 시장가 매도"""
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
    if res.json()['rt_cd'] == '0':
        send_message(f"[매도 성공]{str(res.json())}")
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
            'TOTAL_LOSE_EXIT_PCT' : -2.2
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
            'TOTAL_LOSE_EXIT_PCT' : -2.2
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

#***********************************************************************************************************
# 자동매매 시작
try:
    #ACCESS_TOKEN = get_access_token()
    ACCESS_TOKEN = "ey......WJ3oHQ"
    #print(f"\n📋 ACCESS_TOKEN: {ACCESS_TOKEN}")

    total_cash = get_balance() # 보유 현금 조회 (10,000원 제외)
    #total_cash = get_max_order_cash()
    print(f"\n📋 total_cash: {total_cash:,}")

except Exception as e:
    print(f"\n[오류 발생]{e}")
