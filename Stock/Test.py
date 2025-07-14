import requests
import pandas as pd
import random
from io import BytesIO
from datetime import datetime, timedelta
from holidayskr import is_holiday

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

def get_all_symbols():
    #trade_date = get_last_trading_day()
    trade_date = '20250707'
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

    print(f"\n✅ 전체 종목 수: {len(df)}")
    print("\n✅ 열 이름:")
    print(df.columns.tolist())
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

    # 변동폭 비율 계산
    df['전일변동폭비율'] = (df['고가'] - df['저가']) / df['저가']


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

    filtered = df[
        (df['등락률'] >= -5.0) & 
        (df['종가'] >= 2500) & (df['종가'] <= 99000) &
        (df['시가총액'] >= 5e10) & (df['시가총액'] <= 7e12) &
        (df['거래량'] >= 25000) &
        (df['거래대금'] >= 3e9) &
        (df['전일변동폭비율'] >= 0.055)
    ].copy()

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
    top_filtered = filtered.sort_values(by='점수', ascending=False).head(150)
    #top_filtered = filtered.sort_values(by='점수', ascending=False)

    print(f"\n✅ 최종 선정 종목 수: {len(top_filtered)}")
    print("\n✅ 상위 점수 종목 샘플:")
    #print(top_filtered[['종목명', '종목코드', '종가', '전일변동폭비율', '거래대금', '점수']].head(10))
    #pd.set_option('display.max_rows', None)      # 모든 행 출력
    #pd.set_option('display.max_columns', None)   # 모든 열 출력
    print(top_filtered)

    # 종목코드 리스트 생성 (정렬 순서 유지)
    symbols = top_filtered['종목코드'].astype(str).str.zfill(6).tolist()
    print(f"\n✅ 최종 선정 종목코드 수: {len(symbols)}")
    print("\n✅ 예시 종목코드:", symbols)

    return symbols

if __name__ == "__main__":
    symbols = get_all_symbols()
