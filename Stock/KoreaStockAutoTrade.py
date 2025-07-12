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

with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
URL_BASE = _cfg['URL_BASE']

def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print(message)

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

    send_message(f"\n✅ 전체 종목 수: {len(df)}")
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

    ## 필터링 (기본:43개, 수정:97개)
    filtered = df[
        #(df['등락률'] >= -1) & (df['등락률'] <= 1) &           # 전일 등락률이 -1% ~ +1% 범위: 과하게 급등/급락하지 않은 종목
        (df['등락률'] >= -1.5) & (df['등락률'] <= 1.5) &           # 전일 등락률이 -1% ~ +1% 범위: 과하게 급등/급락하지 않은 종목
        #(df['종가'] >= 3000) & (df['종가'] <= 30000) &         # 전일 종가가 3,000원 이상 30,000원 이하: 저가/고가 extremes 제외
        (df['종가'] >= 3000) & (df['종가'] <= 70000) &         # 전일 종가가 3,000원 이상 30,000원 이하: 저가/고가 extremes 제외
        #(df['시가총액'] >= 1e11) & (df['시가총액'] <= 1e12) &  # 시가총액이 1,000억 원 ~ 1조 원: 너무 작지도 크지도 않은 종목군
        (df['시가총액'] >= 1e11) & (df['시가총액'] <= 2e12) &  # 시가총액이 1,000억 원 ~ 1조 원: 너무 작지도 크지도 않은 종목군
        (df['거래량'] >= 50000) &                              # 전일 거래량 5만 주 이상: 유동성이 충분한 종목
        (df['거래대금'] >= 2e9) &                              # 전일 거래대금 20억 원 이상: 자금이 어느 정도 몰린 종목
        (df['전일변동폭비율'] >= 0.03)                         # 전일 고가/저가 차이가 3% 이상: 변동성이 있었던 종목
    ].copy()  # .copy()는 SettingWithCopyWarning 방지를 위한 명시적 복사

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
    filtered['점수'] = filtered['전일변동폭비율'] * filtered['거래대금']   # 전일에 가격도 크게 움직이고, 돈도 많이 몰린 종목을 추리기 위해

    # 점수 기준 정렬 → 상위 30개 추출
    #top_filtered = filtered.sort_values(by='점수', ascending=False).head(30)
    top_filtered = filtered.sort_values(by='점수', ascending=False)

    send_message(f"\n✅ 최종 선정 종목 수: {len(top_filtered)}")
    #print("\n✅ 상위 점수 종목 샘플:")
    #print(top_filtered[['종목명', '종목코드', '종가', '전일변동폭비율', '거래대금', '점수']].head(10))
    #print(top_filtered)

    # 종목코드 리스트 생성 (정렬 순서 유지)
    symbols = top_filtered['종목코드'].astype(str).str.zfill(6).tolist()
    #print(f"\n✅ 최종 선정 종목코드 수: {len(symbols)}")
    #print("\n✅ 예시 종목코드:", symbols)

    return symbols
    
def get_current_price(code="005930"):
    """현재가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {ACCESS_TOKEN}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output']['stck_prpr'])

def get_target_price(code="005930"):
    """변동성 돌파 전략으로 매수 목표가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"FHKST01010400"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    "fid_org_adj_prc":"1",
    "fid_period_div_code":"D"
    }
    res = requests.get(URL, headers=headers, params=params)
    # 1. API 응답 상태 코드 확인
    if res.status_code != 200:
        send_message(f"[{code}] 일봉 조회 실패 (HTTP {res.status_code}): {res.json().get('msg1', '알 수 없는 오류')}")
        return None # 오류 발생 시 목표가를 반환하지 않고 None을 반환

    response_data = res.json()
    output = response_data.get('output') # 'output' 키가 없을 수도 있으므로 get() 사용

    # 2. 'output' 데이터 유효성 검사 (list index out of range 방지)
    # 최소한 어제 데이터 (인덱스 1)까지 있어야 하므로 길이가 2 이상이어야 함
    if not output or len(output) < 2:
        send_message(f"[{code}] 일봉 데이터 부족 또는 없음. API 응답 output: {output}")
        return None # 데이터가 부족하면 목표가를 계산할 수 없으므로 None 반환
        
    try:
        ### 아래 참고 #######################
        ### stck_oprc: 시가 (Open Price)
        ### stck_hgpr: 고가 (High Price)
        ### stck_lwpr: 저가 (Low Price)
        ### stck_clpr: 종가 (Close Price)
        stck_oprc = int(output[0]['stck_oprc']) # 오늘 시가
        
        #prev_day_open = int(output[1]['stck_oprc']) # 전일 시가
        prev_day_open = int(output[1]['stck_lwpr']) # 전일 저가

        #prev_day_close = int(output[1]['stck_clpr']) # 전일 종가
        prev_day_close = int(output[1]['stck_hgpr']) # 전일 고가

        # 전일 시가(or저가)와 종가(or고가) 중 높은 가격을 전일 고가로, 낮은 가격을 전일 저가로 설정
        stck_hgpr_adjusted = max(prev_day_open, prev_day_close)
        stck_lwpr_adjusted = min(prev_day_open, prev_day_close)

        target_price = stck_oprc + (stck_hgpr_adjusted - stck_lwpr_adjusted) * 0.5   # 0.5 보다 다른 수치도 필요시 적용해 볼 것
        
        return target_price
    except KeyError as e:
        send_message(f"[{code}] API 응답에서 필요한 키 누락: {e}. 응답 전문: {output}")
        return None
    except ValueError as e:
        send_message(f"[{code}] 가격 데이터 형변환 오류: {e}. 응답 전문: {output}")
        return None

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

    # API 응답 성공 여부 확인 및 데이터 유효성 검사 (이전 답변에서 제안된 내용)
    if res.status_code != 200:
        send_message(f"주식 잔고 조회 실패: {res.json().get('msg1', '알 수 없는 오류')}")
        return {} # 빈 딕셔너리 반환 또는 예외 처리

    response_data = res.json()
    stock_list = response_data.get('output1', []) 
    evaluation = response_data.get('output2', [])

    stock_dict = {}
    send_message(f"====주식 보유잔고====")
    
    # enumerate를 사용하여 순번(idx)과 함께 종목 정보를 가져옵니다.
    # 시작 순번을 1로 설정합니다 (start=1)
    item_count = 0 # 실제로 보유한 종목 수를 세기 위한 변수 추가
    for idx, stock in enumerate(stock_list, start=1):
        if int(stock.get('hldg_qty', 0)) > 0: 
            item_count += 1 # 보유 종목일 경우 카운트 증가
            stock_dict[stock.get('pdno')] = stock.get('hldg_qty')
            # f-string 포맷팅을 사용하여 순번을 두 자리 숫자로 표시합니다 (예: 01, 02)
            send_message(f"{item_count:02d}.{stock.get('prdt_name', '알 수 없음')}({stock.get('pdno', '알 수 없음')}): {stock.get('hldg_qty', 0)}주")
            time.sleep(0.1)
    
    if evaluation:
        send_message(f"주식 평가 금액: {evaluation[0].get('scts_evlu_amt', 'N/A')}원")
        time.sleep(0.1)
        send_message(f"평가 손익 합계: {evaluation[0].get('evlu_pfls_smtl_amt', 'N/A')}원")
        time.sleep(0.1)
        send_message(f"총 평가 금액: {evaluation[0].get('tot_evlu_amt', 'N/A')}원")
        time.sleep(0.1)
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
    send_message(f"주문 가능 현금 잔고: {cash}원")
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
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json()['rt_cd'] == '0':
        send_message(f"[매도 성공]{str(res.json())}")
        return True
    else:
        send_message(f"[매도 실패]{str(res.json())}")
        return False

# 자동매매 시작
try:
    ACCESS_TOKEN = get_access_token()

    symbol_list = get_all_symbols()
    # send_message(f"\n✅ 구매 예정 종목코드: {symbol_list}")
    bought_list = [] # 매수 완료된 종목 리스트
    total_cash = get_balance() - 10000 # 보유 현금 조회 (10,000원 제외)
    if total_cash < 0: # 잔액이 마이너스가 되는 경우 방지
        total_cash = 0
    stock_dict = get_stock_balance() # 보유 주식 조회
    for sym in stock_dict.keys():
        bought_list.append(sym)
    target_buy_count = 15 # 매수할 종목 수

    # 이미 매수한 종목 수를 고려하여 buy_percent 계산
    remaining_buy_count = target_buy_count - len(bought_list)
    if remaining_buy_count <= 0:
        buy_percent = 0 # 더 이상 매수할 종목이 없으면 비율을 0으로 설정
    else:
        # 소수점 셋째 자리까지 유지하고 넷째 자리부터 버림
        buy_percent = math.floor((100 / remaining_buy_count) * 0.01 * 1000) / 1000
    
    buy_amount = total_cash * buy_percent  # 종목별 주문 금액 계산
    soldout = False

    send_message("===국내 주식 자동매매 프로그램을 시작합니다===")
    while True:
        t_now = datetime.now()
        t_9 = t_now.replace(hour=9, minute=0, second=15, microsecond=0)
        t_start = t_now.replace(hour=9, minute=3, second=0, microsecond=0)
        #t_sell = t_now.replace(hour=15, minute=15, second=0, microsecond=0)
        #t_exit = t_now.replace(hour=15, minute=20, second=0,microsecond=0)
        t_sell = t_now.replace(hour=14, minute=58, second=0, microsecond=0)
        t_exit = t_now.replace(hour=15, minute=3, second=0,microsecond=0)
        today = datetime.today().weekday()
        if today == 5 or today == 6:  # 토요일이나 일요일이면 자동 종료
            send_message("주말이므로 프로그램을 종료합니다.")
            break
        if t_9 < t_now < t_start and soldout == False: # # AM 09:00 ~ AM 09:03 : 잔여 수량 매도
            for sym, qty in stock_dict.items():
                sell(sym, qty)
            soldout = True
            bought_list = []
            stock_dict = get_stock_balance()
        if t_start < t_now < t_sell:  # AM 09:03 ~ PM 02:58 : 매수
            for sym in symbol_list:
                if len(bought_list) < target_buy_count:
                    if sym in bought_list:
                        continue
                    target_price = get_target_price(sym)
                    if target_price is None: # 목표가를 가져오지 못했으면 다음 종목으로 넘어감
                        send_message(f"[{sym}] 목표가 계산 실패. 다음 종목으로 넘어갑니다.")
                        time.sleep(1) # API 호출 빈도 조절
                        continue 
                    current_price = get_current_price(sym)
                    if target_price < current_price:
                        buy_qty = 0  # 매수할 수량 초기화
                        buy_qty = int(buy_amount // current_price)
                        if buy_qty > 0:
                            send_message(f"{sym} 목표가 달성({target_price} < {current_price}) 매수를 시도합니다.")
                            result = buy(sym, buy_qty)
                            if result:
                                soldout = False
                                bought_list.append(sym)
                                get_stock_balance()
                    time.sleep(1)
            time.sleep(1)
            if t_now.minute == 30 and t_now.second <= 5: 
                get_stock_balance()
                time.sleep(5)
        if t_sell < t_now < t_exit:  # PM 02:58 ~ PM 03:03 : 일괄 매도
            if soldout == False:
                stock_dict = get_stock_balance()
                for sym, qty in stock_dict.items():
                    sell(sym, qty)
                soldout = True
                bought_list = []
                time.sleep(1)
        if t_exit < t_now:  # PM 03:03 ~ :프로그램 종료
            send_message("종료시점 보유주식 조회내역은 아래와 같습니다.")
            get_stock_balance()
            send_message("프로그램을 종료합니다.")
            break
except Exception as e:
    send_message(f"[오류 발생]{e}")
    time.sleep(1)
