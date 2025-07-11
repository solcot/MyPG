import requests
import json
import datetime
import time
import yaml
import random
import math

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
    now = datetime.datetime.now()
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

    symbol_list = [
        "048870", "032680", "071200", "014440", "115440",
        "128540", "130580", "187270", "265560", "339950",
        "356890", "234080", "284740", "450950", "001060",
        "090850", "237880", "072870", "206650", "200710",
        "234340", "181710", "002710", "158430", "469070",
        "030190", "083500", "102970", "037030", "200880",
        "403870", "001680", "452160", "002350", "376900",
        "013990", "457550", "005880", "082740", "222800",
        "058610", "445290", "028670", "487240", "102970",
        "475150", "006800", "028050", "130660", "173130",
        "290270", "319400", "038500", "001440", "031820",
        "054920", "010280", "144960", "032820", "032350",
        "064850", "215600", "007340", "214320", "030000",
        "036640", "083450", "319660", "009470", "005810",
        "000240", "003490", "023590", "194480", "020760",
        "012340", "009290", "011200", "022100", "067280",
        "004990", "001230", "017900", "042670", "174360",
        "063570", "122900", "136480", "251270", "286940",
        "034310", "109740", "068760", "105630", "475560",
        "357880", "413640", "089590", "298690", "267980",
        "014970", "088390", "048410", "307870", "115310"
    ] # 매수 희망 종목 리스트
    #### 시너지이노베이션,소프트센,인피니트헬스케어,영보화학,우리넷
    #### 에코캡,나이스디앤비,신화콘텍,영화테크,아이비김영
    #### 싸이버원,JW생명과학,쿠쿠홈시스,아스테라시스,JW중외제약
    #### 현대이지웰,클리오,메가스터디,유바이오로직스,에이디테크놀로지
    #### 헥토파이낸셜,NHN,TCC스틸,아톤,RISE AI&로봇
    #### NICE평가정보,에프엔에스테크,KODEX 증권,파워넷,서연이화
    #### HPSP,대상,제이엔비,넥센타이어,로킷헬스케어
    #### 아가방컴퍼니,우진엔텍,대한해운,한화엔진,심텍
    #### 에스피지,KODEX 로봇액티브,팬오션,KODEX AI전력핵심설비,KODEX 증권
    #### SK이터닉스,미래에셋증권,삼성E&A,한전산업,오파스넷
    #### 휴네시온,현대무벡스,삼표시멘트,대한전선,아이티센씨티에스
    #### 한컴위드,아이티센엔텍,뉴파워프라즈마,우리기술,롯데관광개발
    #### 에프엔가이드,신라젠,DN오토모티브,이노션,제일기획
    #### HRS,GST,피에스케이,삼화전기,풍산홀딩스
    #### 한국앤컴퍼니,대한항공,다우기술,데브시스터즈,일진디스플
    #### 뉴인텍,광동제약,HMM,포스코DX,멀티캠퍼스
    #### 롯데지주,동국홀딩스,광전자,HD현대인프라코어,RISE 중국본토대형주CSI100
    #### NICE인프라,아이마켓코리아,하림,넷마블,롯데이노베이트
    #### NICE,디에스케이,셀트리온제약,한세실업,더본코리아
    #### SKAI,비아이매트릭스,제주항공,에어부산,매일유업
    #### 삼륭물산,이녹스,현대바이오,비투엔,인포바인
    random.shuffle(symbol_list)
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
        t_now = datetime.datetime.now()
        t_9 = t_now.replace(hour=9, minute=5, second=0, microsecond=0)
        #t_start = t_now.replace(hour=9, minute=5, second=0, microsecond=0)
        #t_sell = t_now.replace(hour=15, minute=15, second=0, microsecond=0)
        #t_exit = t_now.replace(hour=15, minute=20, second=0,microsecond=0)
        t_start = t_now.replace(hour=9, minute=30, second=0, microsecond=0)
        t_sell = t_now.replace(hour=15, minute=0, second=0, microsecond=0)
        t_exit = t_now.replace(hour=15, minute=5, second=0,microsecond=0)
        today = datetime.datetime.today().weekday()
        if today == 5 or today == 6:  # 토요일이나 일요일이면 자동 종료
            send_message("주말이므로 프로그램을 종료합니다.")
            break
        if t_9 < t_now < t_start and soldout == False: # 잔여 수량 매도
            for sym, qty in stock_dict.items():
                sell(sym, qty)
            soldout = True
            bought_list = []
            #----------------------매도 실패로 남아있는 주식이 있을수 있으므로-------------------------------
            time.sleep(60)
            total_cash = get_balance() - 10000 # 보유 현금 조회 (10,000원 제외)
            if total_cash < 0: # 잔액이 마이너스가 되는 경우 방지
                total_cash = 0
            stock_dict = get_stock_balance() # 보유 주식 조회
            for sym in stock_dict.keys():
                bought_list.append(sym)

            # 이미 매수한 종목 수를 고려하여 buy_percent 계산
            remaining_buy_count = target_buy_count - len(bought_list)
            if remaining_buy_count <= 0:
                buy_percent = 0 # 더 이상 매수할 종목이 없으면 비율을 0으로 설정
            else:
                # 소수점 셋째 자리까지 유지하고 넷째 자리부터 버림
                buy_percent = math.floor((100 / remaining_buy_count) * 0.01 * 1000) / 1000
            
            buy_amount = total_cash * buy_percent  # 종목별 주문 금액 계산
            #--------------------------------------------------------------------------------------------
            #stock_dict = get_stock_balance()
        if t_start < t_now < t_sell :  # AM 09:05 ~ PM 03:15 : 매수
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
        if t_sell < t_now < t_exit:  # PM 03:15 ~ PM 03:20 : 일괄 매도
            if soldout == False:
                stock_dict = get_stock_balance()
                for sym, qty in stock_dict.items():
                    sell(sym, qty)
                soldout = True
                bought_list = []
                time.sleep(1)
        if t_exit < t_now:  # PM 03:20 ~ :프로그램 종료
            send_message("프로그램을 종료합니다.")
            break
except Exception as e:
    send_message(f"[오류 발생]{e}")
    time.sleep(1)
