import requests
import pandas as pd
import random
import yaml
from io import BytesIO
from datetime import datetime, timedelta
from holidayskr import is_holiday

with open('C:\\StockPy\\config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
DISCORD_WEBHOOK_URL_MAIN = _cfg['DISCORD_WEBHOOK_URL_MAIN']

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

    print(f"OTP 코드 생성 요청 중... 시장: {mktId}, 날짜: {trade_date}")
    otp_response = requests.post(otp_url, data=otp_form_data, headers=headers)
    if otp_response.status_code != 200:
        print(f"OTP 요청 실패: 상태 코드 {otp_response.status_code}")
        print(otp_response.text)
        return None
    otp_code = otp_response.text

    print(f"CSV 파일 다운로드 중... 시장: {mktId}")
    csv_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    csv_response = requests.post(csv_url, data={'code': otp_code}, headers=headers)
    if csv_response.status_code != 200:
        print(f"CSV 다운로드 실패: 상태 코드 {csv_response.status_code}")
        print(csv_response.text)
        return None

    try:
        df = pd.read_csv(BytesIO(csv_response.content), encoding='euc-kr')
        return df
    except Exception as e:
        print(f"CSV 파싱 오류: {e}")
        return None

def get_all_symbols(p_pool_count=15):
    #trade_date = get_last_trading_day()
    trade_date = '20250827'

    #send_message(f"✅ 최종 거래일은 {trade_date} 입니다.")
    #send_message_main(f"✅ 최종 거래일은 {trade_date} 입니다.")
    print(f"✅ 최종 거래일은 {trade_date} 입니다.")

    df_kospi = fetch_krx_data('STK', trade_date)
    df_kosdaq = fetch_krx_data('KSQ', trade_date)

    if df_kospi is None and df_kosdaq is None:
        print("❌ KOSPI와 KOSDAQ 데이터 모두 가져오기 실패")
        return []
    elif df_kospi is None:
        df = df_kosdaq
    elif df_kosdaq is None:
        df = df_kospi
    else:
        df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)

    if df is None or df.empty:
        print("❌ 데이터 로드 실패: 데이터프레임이 비어 있습니다.")
        return []

    #send_message(f"✅ 전체 종목 수: {len(df)}")
    #send_message_main(f"✅ 전체 종목 수: {len(df)}")
    print(f"\n✅ 전체 종목 수: {len(df)}")
    print("\n✅ 열 이름:")
    print(df.columns.tolist()) # ['종목코드', '종목명', '종가', '대비', '등락률', '시가', '고가', '저가', '거래량', '거래대금', '시가총액', '상장주식수', '소속부']
    print("\n✅ 원본 상위 10개 샘플:")
    print(df.head(10))

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
        print(f"❌ 열 이름 오류: {e}")
        print("사용 가능한 열:", df.columns.tolist())
        return []

    # 거래대금 단위가 억/천 단위일 수 있으므로 조정 확인 필요
    print("\n✅ 거래대금 단위 확인 (상위 5개):")
    print(df['거래대금'].head(5))

    ##📌 [A] 장기 투자용 필터 (우량 + 성장성)
    ##목적: 장기 보유, 분할 매수, 저평가/성장 기업 탐색
    #filtered = df[
    #    (df['종가'] >= 10000) & (df['종가'] <= 100000) &   # 너무 싼 주식 제외, 고평가 제거
    #    (df['시가총액'] >= 3e11) &                         # 최소 3천억 이상: 대형/중견
    #    (df['거래대금'] >= 1e9) &                          # 어느 정도 거래 활발
    #    (df['등락률'].abs() <= 5)                          # 과도한 변동성 제거
    #].copy()

    #📌 [B] 단기 매매 - 안정성 중심 (보수적)
    #목적: 큰 리스크 없이 꾸준한 소폭 수익 추구
    #filtered = df[
    #    (df['등락률'] >= -0.5) & (df['등락률'] <= 0.5) &
    #    (df['종가'] >= 5000) & (df['종가'] <= 50000) &
    #    (df['시가총액'] >= 3e11) & (df['시가총액'] <= 2e12) &
    #    (df['거래량'] >= 100000) &
    #    (df['거래대금'] >= 5e9) &
    #    (df['전일변동폭비율'] >= 0.015) & (df['전일변동폭비율'] <= 0.05)  # 너무 폭발적인 변동성 제거
    #].copy()

    #📌 [C] 단기 매매 - 수익성 중심 (공격적)
    #목적: 큰 변동을 활용해 단기 수익 노림 (스캘핑/단타)
    #filtered = df[
    #    (df['등락률'] >= -2) & (df['등락률'] <= 2) &
    #    (df['종가'] >= 1000) & (df['종가'] <= 50000) &
    #    (df['시가총액'] >= 5e10) &
    #    (df['거래량'] >= 100000) &
    #    (df['거래대금'] >= 10e9) &
    #    (df['전일변동폭비율'] >= 0.04)                         # 높은 변동성
    #].copy()

    # 필터링 (기본:43개, 수정:97개)
    #filtered = df[
    #    #(df['등락률'] >= -1) & (df['등락률'] <= 1) &           # 전일 등락률이 -1% ~ +1% 범위: 과하게 급등/급락하지 않은 종목
    #    (df['등락률'] >= -1.5) & (df['등락률'] <= 1.5) &           # 전일 등락률이 -1% ~ +1% 범위: 과하게 급등/급락하지 않은 종목
    #    #(df['종가'] >= 3000) & (df['종가'] <= 30000) &         # 전일 종가가 3,000원 이상 30,000원 이하: 저가/고가 extremes 제외
    #    (df['종가'] >= 3000) & (df['종가'] <= 70000) &         # 전일 종가가 3,000원 이상 30,000원 이하: 저가/고가 extremes 제외
    #    #(df['시가총액'] >= 1e11) & (df['시가총액'] <= 1e12) &  # 시가총액이 1,000억 원 ~ 1조 원: 너무 작지도 크지도 않은 종목군
    #    (df['시가총액'] >= 1e11) & (df['시가총액'] <= 2e12) &  # 시가총액이 1,000억 원 ~ 1조 원: 너무 작지도 크지도 않은 종목군
    #    (df['거래량'] >= 50000) &                              # 전일 거래량 5만 주 이상: 유동성이 충분한 종목
    #    #(df['거래대금'] >= 2e9) &                              # 전일 거래대금 20억 원 이상: 자금이 어느 정도 몰린 종목
    #    (df['거래대금'] >= 5e9) &                              # 전일 거래대금 20억 원 이상: 자금이 어느 정도 몰린 종목
    #    #(df['전일변동폭비율'] >= 0.03)                         # 전일 고가/저가 차이가 3% 이상: 변동성이 있었던 종목
    #    (df['전일변동폭비율'] >= 0.05)                         # 전일 고가/저가 차이가 3% 이상: 변동성이 있었던 종목
    #].copy()  # .copy()는 SettingWithCopyWarning 방지를 위한 명시적 복사

    ## 약 120개 정도 필터됨
    #filtered = df[
    #    (df['등락률'] >= -1.5) & (df['등락률'] <= 70.0) &   # 0.5
    #    (df['종가'] >= 2000) & (df['종가'] <= 333000) &   # 2500
    #    (df['시가총액'] >= 5e10) & (df['시가총액'] <= 500e12) &   # 5e10
    #    (df['거래량'] >= 100000) &   # 300000
    #    (df['거래대금'] >= 3e9) &   # 7e9
    #    (df['전일변동폭비율'] >= 0.05)   # 0.07
    #].copy()

    #####df['전일변동폭비율'] = (df['고가'] - df['저가']) / df['시가']
    ####### 약 60개 정도 필터됨
    #####filtered = df[
    #####    (df["등락률"] > 0) &                                        # 당일 양봉 종목만
    #####    (df["시가총액"] >= 7e9) &                                    # 시가총액 70억 이상
    #####    (df["거래대금"] >= 7e9) &                                    # 거래대금 70억 이상
    #####    (df['전일변동폭비율'] >= 0.03) &                              # 전일 변동폭이 시가 대비 3% 이상
    #####    (df["종가"] > (df["시가"] + (df["고가"] - df["저가"]) * 0.3))  # 목표가 돌파 (k=0.3)
    #####].copy()
    #### 거래대금 + 양봉 : 약 40
    ###filtered = df[
    ###    (df['등락률'] > 0) & 
    ###    (df['종가'] >= 1500) & 
    ###    (df['시가총액'] >= 5e10) &
    ###    (df['거래량'] >= 1000000) & 
    ###    (df['거래대금'] >= 1e10) & 
    ###    (df['전일변동폭비율'] >= 0.07) 
    ###].copy()
    #***# 개선된 필터링 조건
    #***filtered = df[
    #***    (df['등락률'] >= -5) &  # 소폭 하락 ~ 상승 종목 (과열 방지)
    #***    (df['등락률'] <= 10) &   # 과도한 상승 종목 제외
    #***    (df['종가'] >= 2000) &  # 최소 주가 상향 조정
    #***    (df['시가총액'] >= 5e10) &  # 더 안정적인 대형주 위주
    #***    (df['거래량'] >= 500000) & 
    #***    (df['거래대금'] >= 8e9) &   # 거래대금 기준 하향 조정
    #***    (df['전일변동폭비율'] >= 0.05) &  # 변동폭 기준 완화
    #***    (df['전일변동폭비율'] <= 0.2)    # 과도한 변동성 제외
    #***].copy()

    #***# 현재 필터링의 문제점 개선
    #***filtered = df[
    #***    (df['등락률'] >= -2) &      # -5에서 -2로 좁힘
    #***    (df['등락률'] <= 3) &       # 10에서 3으로 좁힘 (과열 방지)
    #***    (df['종가'] >= 5000) &      # 2000에서 5000으로 상향
    #***    (df['시가총액'] >= 1e11) &  # 더 대형주 위주
    #***    (df['거래대금'] >= 2e10) &   # 거래대금 기준 상향
    #***    (df['전일변동폭비율'] >= 0.03) &  # 적절한 변동성만
    #***    (df['전일변동폭비율'] <= 0.12)    # 과도한 변동성 제외
    #***].copy()

    #+++ # [1번계좌] 대형주
    #+++ filtered = df[
    #+++     (df['등락률'] >= 0.2) &          # -3~7 등락률 범위를 소폭 확장하여 더 많은 잠재 후보군을 포함
    #+++     (df['등락률'] <= 12) &
    #+++     (df['종가'] >= 3000) &          # 동전주를 회피하는 최소 가격
    #+++     (df['시가총액'] >= 5e11) &      # 시가총액 5천억 이상 (너무 작은 종목 제외)
    #+++     (df['시가총액'] <= 15e12) &      # 시가총액 15조 이하 (너무 무거운 대형주 제외, 중소형주 집중)
    #+++     (df['거래대금'] >= 1e10) &       # 거래대금 100억 이상 (최소한의 유동성 확보)
    #+++     (df['전일변동폭비율'] >= 0.05) &  # 전일 변동폭이 최소 5% 이상인 종목
    #+++     (df['전일변동폭비율'] <= 0.20)    # 전일 변동폭이 20% 이하 (지나치게 과열된 종목 제외)
    #+++ ].copy()

    #!!! # [1번계좌] 미니주
    #!!! filtered = df[
    #!!!     (df['등락률'] >= 0.5) &          # -3~7 등락률 범위를 소폭 확장하여 더 많은 잠재 후보군을 포함
    #!!!     (df['등락률'] <= 3.5) &
    #!!!     #(df['종가'] >= 100) &          # 동전주를 회피하는 최소 가격
    #!!!     (df['시가총액'] >= 1e9) &      # 시가총액 10억 이상 (너무 작은 종목 제외)
    #!!!     (df['시가총액'] < 10e10) &       # 시가총액 1천억 이하 (너무 무거운 대형주 제외, 중소형주 집중)
    #!!!     (df['거래대금'] >= 17e8)         # 거래대금 17억 이상 (최소한의 유동성 확보)
    #!!!     #(df['전일변동폭비율'] >= 0.03) &   # 전일 변동폭이 최소 5% 이상인 종목
    #!!!     #(df['전일변동폭비율'] <= 0.20)    # 전일 변동폭이 20% 이하 (지나치게 과열된 종목 제외)
    #!!! ].copy()

    #*** # [1번계좌] 중형주
    #*** filtered = df[
    #***     (df['등락률'] >= 0.5) &          # -3~7 등락률 범위를 소폭 확장하여 더 많은 잠재 후보군을 포함
    #***     (df['등락률'] <= 1.5) &
    #***     #(df['종가'] >= 100) &          # 동전주를 회피하는 최소 가격
    #***     (df['시가총액'] >= 50e10) &      # 시가총액 5천억 이상 (너무 작은 종목 제외)
    #***     (df['시가총액'] < 200e10) &       # 시가총액 2조 이하 (너무 무거운 대형주 제외, 중소형주 집중)
    #***     (df['거래대금'] >= 70e8)         # 거래대금 70억 이상 (최소한의 유동성 확보)
    #***     #(df['전일변동폭비율'] >= 0.03) &   # 전일 변동폭이 최소 5% 이상인 종목
    #***     #(df['전일변동폭비율'] <= 0.20)    # 전일 변동폭이 20% 이하 (지나치게 과열된 종목 제외)
    #*** ].copy()

    # 추가 컬럼 계산
    #df['전일변동폭비율'] = (df['고가'] - df['저가']) / df['저가']
    df['금일등락률'] = (df['종가'] - df['시가']) / df['종가'] * 100

    # [1번계좌] 중소형주
    filtered = df[
        (df['금일등락률'] >= 0.5) &          # -3~7 등락률 범위를 소폭 확장하여 더 많은 잠재 후보군을 포함
        (df['금일등락률'] <= 1.5) &
        (df['종가'] <= 300000) &          # 동전주를 회피하는 최소 가격
        #(df['시가총액'] >= 50e10) &      # 시가총액 5천억 이상 (너무 작은 종목 제외)
        (df['시가총액'] < 200e10)        # 시가총액 2조 이하 (너무 무거운 대형주 제외, 중소형주 집중)
        #(df['거래대금'] >= 67e8)         # 거래대금 67억 이상 (최소한의 유동성 확보)
        #(df['전일변동폭비율'] >= 0.03) &   # 전일 변동폭이 최소 5% 이상인 종목
        #(df['전일변동폭비율'] <= 0.20)    # 전일 변동폭이 20% 이하 (지나치게 과열된 종목 제외)
    ].copy()

    top_filtered = filtered.sort_values(by='거래대금', ascending=False).head(p_pool_count)

    # 안정성 점수 계산 (기존 공격적 점수 대신)
    top_filtered['안정성점수'] = (
        top_filtered['시가총액'] * 0.3 +  # 시총 가중치
        top_filtered['거래대금'] * 0.3 +   # 유동성 가중치  
        (1 / (abs(top_filtered['금일등락률']) + 1)) * top_filtered['거래대금'] * 0.4  # 안정성 가중치
    )
  
    return_filtered = top_filtered.sort_values(by='안정성점수', ascending=False)

    print(f"\n✅ 최종 선정 종목 수: {len(return_filtered)}")
    print("\n✅ 상위 점수 종목 샘플:")
    print(return_filtered)

    # **여기부터 변경 시작:** 종목코드를 키로, 종목명을 값으로 하는 딕셔너리 생성
    symbols_name_dict = {} # 새로운 딕셔너리 생성
    for _, row in return_filtered.iterrows():
        symbol = str(row['종목코드']).zfill(6) # 종목코드를 가져와 6자리 문자열로 만듭니다.
        name = row['종목명'] # 종목명을 가져옵니다.
        symbols_name_dict[symbol] = name # 딕셔너리에 '종목코드': '종목명' 형태로 저장합니다.

    return symbols_name_dict # **변경 끝:** 이 딕셔너리를 반환합니다.

if __name__ == "__main__":
    pool_count = 20
    symbols = get_all_symbols(p_pool_count=pool_count)
