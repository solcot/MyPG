import requests
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

with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
URL_BASE = _cfg['URL_BASE']

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
    send_message(f"✅ 최종 거래일은 {trade_date} 입니다.")

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

    # 변동폭 비율 계산
    df['전일변동폭비율'] = (df['고가'] - df['저가']) / df['저가']

    #*************************************************************************************************************
    # 약 100~150개 정도 필터됨
    filtered = df[
        #(df['등락률'] >= -5.0) & 
        #(df['등락률'] >= -5.0) & (df['등락률'] <= 15.0) & 
        (df['등락률'] >= -5.0) & (df['등락률'] <= 10.0) & 
        #(df['종가'] >= 2500) & (df['종가'] <= 99000) &
        #(df['종가'] >= 2500) & (df['종가'] <= 199000) &
        #(df['종가'] >= 2500) & (df['종가'] <= 239000) &
        #(df['종가'] >= 2500) & (df['종가'] <= 466000) &
        #(df['종가'] >= 2500) & (df['종가'] <= 300000) &
        (df['종가'] >= 2500) & (df['종가'] <= 150000) &
        #(df['시가총액'] >= 5e10) &
        (df['시가총액'] >= 5e10) & (df['시가총액'] <= 7e12) &
        (df['거래량'] >= 30000) &
        #(df['거래량'] >= 50000) &
        (df['거래대금'] >= 3e9) &
        #(df['거래대금'] >= 5e9) &
        #(df['전일변동폭비율'] >= 0.05)
        (df['전일변동폭비율'] >= 0.06)
    ].copy()
    #*************************************************************************************************************

    ## 필터 조건
    #filtered = df[
    #    (df['등락률'] >= -1) & (df['등락률'] <= 0.5) &
    #    (df['종가'] >= 3000) & (df['종가'] <= 30000) &
    #    (df['시가총액'] >= 1e11) & (df['시가총액'] <= 1e12) &
    #    (df['거래대금'] >= 1e9)  # 10억 원 이상
    #]

    #print(f"\n✅ 조건 만족 종목 수: {len(filtered)}")
    #print("\n✅ 조건 만족 상위 10개 샘플:")
    #print(filtered[['종목명', '종목코드', '종가', '등락률', '시가총액', '거래대금']].head(10))
    #
    ## 종목코드 리스트 생성
    #symbols = filtered['종목코드'].astype(str).str.zfill(6).tolist()
    #random.shuffle(symbols)

    #print("\n✅ 예시 종목코드:", symbols[:5])
    #return symbols


    # 기존 필터 이후 추가
    #filtered['점수'] = filtered['전일변동폭비율'] * filtered['거래대금']   # 전일에 가격도 크게 움직이고, 돈도 많이 몰린 종목을 추리기 위해
    filtered['점수'] = filtered['전일변동폭비율'] * filtered['거래대금'] * (1 + filtered['등락률'] / 100)

    # 점수 기준 정렬 → 상위 150개 추출
    #top_filtered = filtered.sort_values(by='점수', ascending=False).head(150)
    top_filtered = filtered.sort_values(by='점수', ascending=False)

    send_message(f"✅ 최종 선정 종목 수: {len(top_filtered)}")
    #print("\n✅ 상위 점수 종목 샘플:")
    #print(top_filtered[['종목명', '종목코드', '종가', '전일변동폭비율', '거래대금', '점수']].head(10))
    #print(top_filtered)

    # 종목코드 리스트 생성 (정렬 순서 유지)
    symbols = top_filtered['종목코드'].astype(str).str.zfill(6).tolist()
    global symbol_name_map
    symbol_name_map = dict(zip(
        top_filtered['종목코드'].astype(str).str.zfill(6),
        top_filtered['종목명']
    ))
    #print(f"\n✅ 최종 선정 종목코드 수: {len(symbols)}")
    #print("\n✅ 예시 종목코드:", symbols)

    return symbols

# --- ✨ 손절 로직 함수 (수정) ✨ ---
def check_stop_loss(stock_dict, threshold=-3.0):
    """
    보유 종목 중 손절 기준 이하인 종목을 찾아서 리스트로 반환
    :param stock_dict: 보유 주식 정보 딕셔너리
    :param threshold: 손절 기준 수익률 (%)
    :return: 손절 매도 대상 종목 리스트
    """
    stopped_out = []
    
    # API 호출 대신 전달받은 stock_dict 활용
    for sym, stock in stock_dict.items():
        qty = int(stock.get('현재수량', 0))
        buy_price = float(stock.get('매수가', 0))
        
        current_price = get_current_price(sym)
        if qty == 0 or buy_price == 0 or current_price is None:
            continue
        
        profit_pct = ((current_price - buy_price) / buy_price) * 100
        if profit_pct <= threshold:
            send_message(f"😭 손절매 발동! {stock.get('종목명')}({sym}) 수익률 {profit_pct:.2f}% → 매도")
            stopped_out.append(sym)
            
    return stopped_out

# --- ✨ 익절 변동손절 (Trailing Stop) 로직 함수 ✨ ---
def check_profit_taking_with_trailing_stop(stock_dict, profit_threshold, trailing_stop_percent, trailing_peak_prices):
    """
    익절 기준(threshold)을 넘어선 종목들에 대해
    최고가 대비 일정 비율(trailing_stop_percent) 하락 시 매도합니다.
    """
    profited = []
    
    # 보유 종목의 현재가 조회
    for sym in stock_dict.keys():
        current_price = get_current_price(sym)
        if current_price is None:
            continue
            
        bought_price = stock_dict.get(sym, {}).get('매수가', None)
        if bought_price is None:
            continue
            
        profit_pct = ((current_price / bought_price) - 1) * 100
        
        sym_name = stock_dict.get(sym, {}).get('종목명', None)

        # 1) 이전에 5% 목표를 달성한 적이 없는 종목인 경우
        if sym not in trailing_peak_prices:
            # 현재 수익률이 익절 기준치(5%)를 넘었을 경우
            if profit_pct >= profit_threshold:
                send_message(f"😄 {sym_name}({sym}) 익절 기준({profit_threshold}%) 달성! 최고가 추적 시작.")
                trailing_peak_prices[sym] = current_price # 최고가로 기록
            
        # 2) 이미 5% 목표를 달성하여 최고가 추적 중인 종목인 경우
        else:
            # 새로운 현재가가 이전 최고가보다 높으면 갱신
            if current_price > trailing_peak_prices[sym]:
                trailing_peak_prices[sym] = current_price
            
            # 현재가가 최고가 대비 일정 비율(2%) 이상 하락했는지 확인
            if current_price <= trailing_peak_prices[sym] * (1 - trailing_stop_percent / 100):
                send_message(f"✨ {sym_name}({sym}) 최고가({trailing_peak_prices[sym]:.2f}) 대비 {trailing_stop_percent}% 하락. {profit_pct:,.2f}% 로 익절 매도합니다.")
                profited.append(sym)
                
    return profited

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

def get_price_info(code="005930", k=0.5):
    """
    변동성 돌파 전략 목표가 + 당일 시가를 함께 반환
    :param code: 종목 코드 (6자리 문자열)
    :param k: 변동성 계수 (기본값 0.5)
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
        ### 아래 참고 #######################
        ### stck_oprc: 시가 (Open Price)
        ### stck_hgpr: 고가 (High Price)
        ### stck_lwpr: 저가 (Low Price)
        ### stck_clpr: 종가 (Close Price)
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

        # 전일 고가/저가로 변동폭 계산
        prev_high = int(output[1]['stck_hgpr'])
        prev_low  = int(output[1]['stck_lwpr'])

        # 목표가 = 오늘 시가 + (전일 고가 - 전일 저가) * k
        target_price = open_price + (prev_high - prev_low) * k

        return target_price, open_price

    except (KeyError, ValueError) as e:
        send_message(f"[{code}] 가격 정보 파싱 오류: {e}")
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

    # 수정: 보유 주식 건수를 요약해서 한 번만 메시지 전송
    if item_count > 0:
        send_message(f"📋 현재 보유 주식은 {item_count:02d}건 입니다.")
    else:
        send_message("📋 현재 보유 주식은 없습니다.")

    if evaluation:
        send_message(f"💰 주식 평가 금액: {evaluation[0].get('scts_evlu_amt', 'N/A')}원")
        send_message(f"💰 평가 손익 합계: {evaluation[0].get('evlu_pfls_smtl_amt', 'N/A')}원")
        send_message(f"💰 총 평가 금액: {evaluation[0].get('tot_evlu_amt', 'N/A')}원")
    else:
        send_message("평가 정보가 없습니다.")
    send_message(f"=================")

    return stock_dict

def get_balance():
    """현금 잔고조회"""
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
        "PDNO": "005930",
        "ORD_UNPR": "65500",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "Y",
        "OVRS_ICLD_YN": "Y"
    }
    res = requests.get(URL, headers=headers, params=params)
    cash = res.json()['output']['ord_psbl_cash']
    send_message(f"💰 주문 가능 현금 잔고: {cash}원")
    return int(cash)

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
            'TAKE_PROFIT_PCT': 7.0,
            'TAKE_PROFIT_LOSE_PCT': 3.0,
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
            'TAKE_PROFIT_PCT': 7.0,
            'TAKE_PROFIT_LOSE_PCT': 3.0,
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
#***********************************************************************************************************
#***********************************************************************************************************
#***********************************************************************************************************
#***********************************************************************************************************
# 자동매매 시작
try:
    ACCESS_TOKEN = get_access_token()

    symbol_list = get_all_symbols()  # 거래량, 시총, 조건 필터링된 종목들

    # --- ✨ 메인 자동매매 루프 시작 ✨ ---
    # 외부 루프: 설정 재로드를 위해 전체 로직을 감쌈
    while True:
        # --- 설정 파일에서 값 로드 ---------------------------------------------------------------------------------------------
        settings = load_settings()

        ## --- ✨ 테스트 출력 시작 ✨ ---
        #send_message("--- [setting.ini] 로드된 설정 값 ---")
        #for key, value in settings.items():
        #    if isinstance(value, dict): # 시간 설정 (딕셔너리)은 보기 좋게 출력
        #        time_str = f"{{'hour': {value['hour']}, 'minute': {value['minute']}, 'second': {value['second']}}}"
        #        send_message(f"- {key}: {time_str}")
        #    elif isinstance(value, list): # 리스트는 join으로 출력
        #        send_message(f"- {key}: {', '.join(value)}")
        #    else:
        #        send_message(f"- {key}: {value}")
        #send_message("--- [setting.ini] 로드된 설정 값 끝 ---")
        ## --- ✨ 테스트 출력 끝 ✨ ---

        ACCOUNT_AMT = settings['ACCOUNT_AMT']    #**************** ACCOUNT_AMT/TARGET_BUY_COUNT/df['종가'] 는 항상 같이 고려되야 함....
        # --- ✨ 09시 이전 EXCLUDE_LIST 초기화 로직 ✨ ---
        t_now_check = datetime.now()
        t_9_oclock = t_now_check.replace(hour=9, minute=0, second=0, microsecond=0)

        # 09:00:00 이전이면 EXCLUDE_LIST를 강제로 빈 리스트로 설정
        if t_now_check < t_9_oclock:
            send_message("✅ 09시 이전이므로 EXCLUDE_LIST를 초기화합니다.")
            EXCLUDE_LIST = []
        else:
            EXCLUDE_LIST = settings['EXCLUDE_LIST']
        #EXCLUDE_LIST = settings['EXCLUDE_LIST']
        TARGET_BUY_COUNT = settings['TARGET_BUY_COUNT']

        T_9_TIME = settings['T_9_TIME']
        T_START_TIME = settings['T_START_TIME']
        T_SELL_TIME = settings['T_SELL_TIME']
        T_EXIT_TIME = settings['T_EXIT_TIME']

        SLIPPAGE_LIMIT = settings['SLIPPAGE_LIMIT']

        STOP_LOSE_PCT = settings['STOP_LOSE_PCT']
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

        if EXCLUDE_LIST and len(EXCLUDE_LIST) > 0:
            symbol_list = [sym for sym in symbol_list if sym not in EXCLUDE_LIST]

        bought_list = [] # 매수 완료된 종목 리스트
        total_cash = get_balance() - 10000 # 보유 현금 조회 (10,000원 제외)
        if total_cash < 0: # 잔액이 마이너스가 되는 경우 방지
            total_cash = 0
        stock_dict = get_stock_balance() # 보유 주식 조회
        # ACCOUNT_AMT 계산
        total_buy_value = sum(
            stock_dict[sym]['현재수량'] * stock_dict[sym]['매수가']
            for sym in stock_dict
        )
        ACCOUNT_AMT = total_cash + total_buy_value  # 초기 계좌 금액 설정
        send_message(f"📋 프로그램 시작: ACCOUNT_AMT = {ACCOUNT_AMT:,}원 (현금: {total_cash:,}원, 주식구매가격: {total_buy_value:,}원)")
        for sym in stock_dict.keys():
            bought_list.append(sym)

        t_now = datetime.now()

        # 주식 매수/매도 시간
        t_9 = t_now.replace(**T_9_TIME)
        t_start = t_now.replace(**T_START_TIME)
        t_sell = t_now.replace(**T_SELL_TIME)
        t_exit = t_now.replace(**T_EXIT_TIME)

        # 이미 매수한 종목 수를 고려하여 buy_percent 계산
        remaining_buy_count = TARGET_BUY_COUNT - len(bought_list)
        if remaining_buy_count <= 0:
            buy_percent = 0 # 더 이상 매수할 종목이 없으면 비율을 0으로 설정
        else:
            # 소수점 셋째 자리까지 유지하고 넷째 자리부터 버림
            buy_percent = math.floor((100 / remaining_buy_count) * 0.01 * 1000) / 1000
        
        # 종목별 주문 금액 완화 로직 추가
        if t_now >= t_now.replace(**AMOUNT_LIMIT2_TIME):
            buy_amount = min(int(total_cash * buy_percent * AMOUNT_LIMIT2), total_cash * 0.06)  # 매수 비중 줄임
        elif t_now >= t_now.replace(**AMOUNT_LIMIT1_TIME):
            buy_amount = min(int(total_cash * buy_percent * AMOUNT_LIMIT1), total_cash * 0.08)  # 매수 비중 줄임
        else:
            buy_amount = min(int(total_cash * buy_percent), total_cash * 0.1)

        soldout = False

        send_message("===국내 주식 자동매매 프로그램을 시작합니다===")
        last_stop_loss_check_time = datetime.now() - timedelta(seconds=15) # 손절 초기값 설정 
        last_profit_taking_check_time = datetime.now() - timedelta(seconds=45) # 익절 초기값 설정 
        last_balance_check_time = datetime.now() - timedelta(minutes=15)  # 초기화: 과거로 설정해서 15분후에 출력되도록 이후는 30분마다
        last_heartbeat = datetime.now() - timedelta(minutes=10)
        last_reload_check_time = datetime.now() - timedelta(seconds=10)
        # 슬리피지 초과 감시용 변수들 (초기화 부분)
        slippage_count = {}
        slippage_last_logged = {}
        # 추가: 휴일 종료 플래그
        program_exit = False
        # ✨ 추가: 익절 변동손절(Trailing Stop)을 위한 딕셔너리
        trailing_peaks = {} 

        while True:
            t_now = datetime.now()

            # 10분마다 heartbeat 출력
            if (t_now - last_heartbeat).total_seconds() >= 600:
                send_message("✅ 시스템 정상 작동 중입니다.")
                last_heartbeat = t_now

            #today = datetime.today().weekday()
            today = datetime.today()
            if today.weekday() >= 5 or is_holiday(today.strftime("%Y-%m-%d")):  # 토요일/일요일/휴일 이면 자동 종료
                send_message("휴일이므로 프로그램을 종료합니다.")
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
                for sym, details in stock_dict.items():
                    qty = details.get('현재수량', '0') # '현재수량'을 추출하여 qty에 할당
                    if int(qty) > 0: # 수량이 0보다 큰 경우에만 매도 실행
                        sell(sym, qty)
                soldout = True
                bought_list = []
                stock_dict = get_stock_balance()

            if t_start < t_now < t_sell:  # AM 09:03 ~ PM 02:58 : 매수     
            
                #send_message("루프 시작..................") #루프 시간 측정용

                # 손절 감시 로직 -------------------------------------------------------       
                if (t_now - last_stop_loss_check_time).total_seconds() >= 30: # 30초마다 체크
                    stopped = check_stop_loss(stock_dict=stock_dict, threshold=STOP_LOSE_PCT)
                    if stopped:
                        for sym in stopped:
                            qty = stock_dict.get(sym, {}).get('현재수량', 0)
                            if qty > 0:
                                result = sell(sym, qty)
                                if result:
                                    if sym in bought_list:
                                        bought_list.remove(sym)
                                    if sym in symbol_list:
                                        symbol_list.remove(sym)
                        stock_dict = get_stock_balance() # 손절 후 계좌 정보 최신화
                        
                        # ✨ 손절 후 buy_amount 재계산 로직
                        time.sleep(5) # 급격한 재매수 방지용
                        remaining_buy_count = TARGET_BUY_COUNT - len(bought_list)
                        if remaining_buy_count > 0:
                            buy_percent = math.floor((100 / remaining_buy_count) * 0.01 * 1000) / 1000
                            total_cash = get_balance() - 10000
                            if total_cash < 0:
                                total_cash = 0
                            # 종목별 주문 금액 완화 로직 추가
                            if t_now >= t_now.replace(**AMOUNT_LIMIT2_TIME):
                                buy_amount = min(int(total_cash * buy_percent * AMOUNT_LIMIT2), total_cash * 0.06)  # 매수 비중 줄임
                            elif t_now >= t_now.replace(**AMOUNT_LIMIT1_TIME):
                                buy_amount = min(int(total_cash * buy_percent * AMOUNT_LIMIT1), total_cash * 0.08)  # 매수 비중 줄임
                            else:
                                buy_amount = min(int(total_cash * buy_percent), total_cash * 0.1)
                        else:
                            buy_amount = 0

                    last_stop_loss_check_time = t_now # 마지막 체크 시간 업데이트
                # 손절 감시 로직 끝 ------------------------------------------------------------------
                # 익절 감시 로직 -----------------------------------------------------------
                if (t_now - last_profit_taking_check_time).total_seconds() >= 30: # 30초마다 체크
                    profited = check_profit_taking_with_trailing_stop(
                        stock_dict=stock_dict, 
                        profit_threshold=TAKE_PROFIT_PCT,
                        trailing_stop_percent=TAKE_PROFIT_LOSE_PCT,
                        trailing_peak_prices=trailing_peaks
                    )
                    if profited:
                        for sym in profited:
                            qty = stock_dict.get(sym, {}).get('현재수량', 0)
                            if qty > 0:
                                result = sell(sym, qty)
                                if result:
                                    if sym in bought_list:
                                        bought_list.remove(sym)
                                    if sym in symbol_list:
                                        symbol_list.remove(sym)
                                    if sym in trailing_peaks:
                                        del trailing_peaks[sym]
                        stock_dict = get_stock_balance() # 익절 후 계좌 정보 최신화

                        # ✨ 익절 후 buy_amount 재계산 로직
                        time.sleep(5) # 급격한 재매수 방지용
                        remaining_buy_count = TARGET_BUY_COUNT - len(bought_list)
                        if remaining_buy_count > 0:
                            buy_percent = math.floor((100 / remaining_buy_count) * 0.01 * 1000) / 1000
                            total_cash = get_balance() - 10000
                            if total_cash < 0:
                                total_cash = 0
                            # 종목별 주문 금액 완화 로직 추가
                            if t_now >= t_now.replace(**AMOUNT_LIMIT2_TIME):
                                buy_amount = min(int(total_cash * buy_percent * AMOUNT_LIMIT2), total_cash * 0.06)  # 매수 비중 줄임
                            elif t_now >= t_now.replace(**AMOUNT_LIMIT1_TIME):
                                buy_amount = min(int(total_cash * buy_percent * AMOUNT_LIMIT1), total_cash * 0.08)  # 매수 비중 줄임
                            else:
                                buy_amount = min(int(total_cash * buy_percent), total_cash * 0.1)
                        else:
                            buy_amount = 0
                    
                    last_profit_taking_check_time = t_now # 마지막 체크 시간 업데이트
                # 익절 감시 로직 끝 -------------------------------------------------------------

                for sym in symbol_list:
                    if len(bought_list) < TARGET_BUY_COUNT:
                        if sym in bought_list:
                            continue

                        # 🔁 k값 점진적 완화 로직 추가
                        if len(bought_list) < TARGET_BUY_COUNT:
                            if t_now >= t_now.replace(**TARGET_K3_TIME):
                                k = TARGET_K3
                            elif t_now >= t_now.replace(**TARGET_K2_TIME):
                                k = TARGET_K2
                            else:
                                k = TARGET_K1
                        else:
                            k = TARGET_K1

                        target_price, open_price = get_price_info(sym, k)
                        #time.sleep(0.1)
                        current_price = get_current_price(sym)
                        if open_price is None or target_price is None or current_price is None: # 가격을 가져오지 못했으면 다음 종목으로 넘어감
                            send_message(f"[{sym}] 가격 수신 실패. 다음 종목으로 넘어갑니다.")
                            #time.sleep(1) # API 호출 빈도 조절
                            continue 

                        # 갭상승 제외하고, 진짜 장중 돌파만 매수
                        #if open_price < target_price < current_price:
                        # 갭상승(or NXT) 포함해서 target_price 돌파 매수
                        if target_price < current_price:
                            stock_name = symbol_name_map.get(sym, "Unknown")

                            # 돌파 조건은 만족했지만 슬리피지 체크
                            if current_price > target_price * SLIPPAGE_LIMIT:
                                # 슬리피지 횟수 기록
                                if sym not in slippage_count:
                                    slippage_count[sym] = 1
                                else:
                                    slippage_count[sym] += 1
                                # 3회 이하까지는 무조건 출력
                                if slippage_count[sym] <= 3:
                                    send_message(f"🔄 {stock_name}({sym}) 슬리피지 초과 {slippage_count[sym]}회 (현재가:{current_price:.2f} > 허용가:{target_price * SLIPPAGE_LIMIT:.2f})")
                                else:
                                    # 마지막으로 출력한 시간이 10분 지났으면 다시 출력
                                    last_log_time = slippage_last_logged.get(sym)
                                    if last_log_time is None or (t_now - last_log_time).total_seconds() >= 600:
                                        send_message(f"🔄 {stock_name}({sym}) 슬리피지 반복 초과 중... (현재가:{current_price:.2f} > 허용가:{target_price * SLIPPAGE_LIMIT:.2f})")
                                        slippage_last_logged[sym] = t_now
                                continue  # 슬리피지 초과 종목은 매수하지 않음
                            else:
                                buy_qty = 0  # 매수할 수량 초기화  

                                # 종목별 주문 금액 완화 로직 추가
                                if t_now >= t_now.replace(**AMOUNT_LIMIT2_TIME):
                                    buy_amount = min(int(total_cash * buy_percent * AMOUNT_LIMIT2), total_cash * 0.06)  # 매수 비중 줄임
                                elif t_now >= t_now.replace(**AMOUNT_LIMIT1_TIME):
                                    buy_amount = min(int(total_cash * buy_percent * AMOUNT_LIMIT1), total_cash * 0.08)  # 매수 비중 줄임
                                else:
                                    buy_amount = min(int(total_cash * buy_percent), total_cash * 0.1)

                                buy_qty = int(buy_amount // current_price)
                                if buy_qty > 0:
                                    send_message(f"📈 {stock_name}({sym}) 목표가 달성({target_price} < {current_price}) 매수를 시도합니다.")
                                    result = buy(sym, buy_qty)
                                    if result:
                                        soldout = False
                                        bought_list.append(sym)
                                        stock_dict = get_stock_balance()
                        time.sleep(0.025)
                time.sleep(0.025)

                # ✅ 30분마다 잔고 확인 (예: 09:15, 09:45, 10:15 ...)
                if (t_now - last_balance_check_time).total_seconds() >= 1800:  # 1800초 = 30분
                    stock_dict = get_stock_balance()  # 잔고 조회
                    # ✨ 일일 손실 한도 체크 로직 추가 ✨
                    if stock_dict:
                        total_cash = get_balance() - 10000  # 현금 잔고 조회 (10,000원 제외)
                        if total_cash < 0:
                            total_cash = 0
                        # 보유 주식의 현재 평가 금액 계산
                        total_stock_value = sum(
                            stock_dict[sym]['현재수량'] * get_current_price(sym) 
                            for sym in stock_dict 
                            if get_current_price(sym) is not None
                        )
                        # 계좌 전체 금액 = 현금 + 주식 평가 금액
                        total_account_value = total_cash + total_stock_value
                        # 초기 계좌 금액 대비 손실률 계산
                        loss_pct = ((total_account_value - ACCOUNT_AMT) / ACCOUNT_AMT) * 100
                        if loss_pct <= TOTAL_LOSE_EXIT_PCT:
                            send_message(f"🚨 계좌 전체 금액 손실 한도({TOTAL_LOSE_EXIT_PCT}%) 도달! 현재 손실률: {loss_pct:.2f}% | 보유 주식 전량 매도 후 프로그램을 종료합니다.")
                            # 보유 주식 전량 매도
                            for sym, details in stock_dict.items():
                                qty = details.get('현재수량', '0')
                                if int(qty) > 0:
                                    sell(sym, qty)
                                    time.sleep(1)
                            soldout = True
                            bought_list = []
                            program_exit = True # ✨ 플래그 설정 ✨
                            break  # 내부 루프 종료
                    last_balance_check_time = t_now

                #send_message("루프 끝..................") #루프 시간 측정용

            if t_sell < t_now < t_exit:  # PM 02:58 ~ PM 03:03 : 일괄 매도
                if soldout == False:
                    stock_dict = get_stock_balance()
                    for sym, details in stock_dict.items():
                        qty = details.get('현재수량', '0') # '현재수량'을 추출하여 qty에 할당
                        if int(qty) > 0: # 수량이 0보다 큰 경우에만 매도 실행
                            sell(sym, qty)
                            time.sleep(1)
                    soldout = True
                    bought_list = []
                    time.sleep(1)
            if t_exit < t_now:  # PM 03:03 ~ :프로그램 종료
                send_message("종료시점 보유주식 조회내역은 아래와 같습니다.")
                get_stock_balance()
                send_message("프로그램을 종료합니다.")
                break

        # 내부 루프가 break로 종료되었을 때 처리
        if program_exit: # ✨ 플래그 확인 ✨
            break # 외부 루프도 종료하여 프로그램 완전히 끝냄
        elif t_exit > t_now: # 프로그램 종료 시간이 아닌데 break 되었다면 (즉, 재로드 때문)
            send_message("🔄 설정 재로드를 위해 메인 루프를 다시 시작합니다.")
            continue # 외부 while True 루프의 다음 반복으로 이동
        else: # 프로그램 종료 시간이라면 외부 루프도 종료
            break

except Exception as e:
    send_message(f"[오류 발생]{e}")
    time.sleep(1)
