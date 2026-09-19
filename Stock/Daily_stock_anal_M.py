# pip install paramiko google-genai
import time
import paramiko
import smtplib
from email.mime.text import MIMEText
from google import genai
from google.genai import types

# 1. 서버 접속 및 PostgreSQL 쿼리 실행
def get_postgres_data():
    host = "192...."
    port = 22
    username = "postgres"
    password = "1d..."
    
    print("1. 서버에 접속하여 DB 쿼리를 실행 중입니다...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port=port, username=username, password=password)
    
    stdin, stdout, stderr = client.exec_command("psql -P pager=off -f value_engine.sql_see")
    result = stdout.read().decode('utf-8')
    # 결과를 줄 단위로 쪼갠 후 슬라이싱[:10]으로 10줄만 가져와서 출력
    #print('\n'.join(result.split('\n')[:10]))
    client.close()
    
    return result

# 2. 제미나이 데이터 분석 (최신 genai SDK 적용)
def analyze_with_gemini_gems(data):
    print("2. 제미나이(Gems)가 데이터를 분석 중입니다...")
    
    client = genai.Client(api_key="AQ.Ab...")
    
    gems_instruction = """
    당신은 월스트리트 최고의 퀀트 투자 분석가입니다. 
    제공된 데이터베이스 쿼리 결과를 꼼꼼히 분석하여, 투자 가치가 가장 높은 Top 5 종목을 선정하세요.
    선정시 ROE를 가장 중요한 지표로 사용하고, 필수는 아니나 10년간 꾸준히 ROE가 지속되는지도 확인해주세요.
    추가로 할인율과 CAGR 및 영업이익률도 참고해 주세요.
    기타 쿼리 결과에 나온 지표외에 분석가로서의 미래예측 및 AI 시대에 해당 기업의 경제적 해자가 있는지도 검토해주세요.

    쿼리 결과로 나온 컬럼에 대한 설명은 아래와 같습니다.
    ----------------------------------------------------------------------------------------
1. 기본 정보 및 시장 지표 (Basic Info & Market Indicators)
    - code / name / sector: 종목코드, 종목명, 해당 기업이 속한 산업 섹터
    - trade_date / max_date: 기준일(현재 주가 및 최신 재무 데이터 스냅샷 기준일)
    - trade_value_천만: 최근 거래대금 (단위: 1,000만 원)
    - market_cap_백억: 시가총액 (단위: 100억 원)
    - curr_price / close_price: 현재 주가 (종가)
    - change_rate: 전일 대비 주가 등락률(%)
2. 최신 재무 및 수익성 지표 (Latest Financials & Profitability)
    - 매출액_억 / 영업이익_억 / 순부채_억: 가장 최근 스냅샷 기준 재무제표 항목 (단위: 억 원)
    - 영업이익률_pct: 매출액 대비 영업이익 비율(%)
    - psr (Price-to-Sales Ratio): 시가총액을 매출액으로 나눈 값. (매출 대비 주가 수준)
    - pbr / per / roe / dividend_yield: 현재 시점의 PBR, PER, ROE, 배당수익률
    - forward_per / forward_roe: 향후 12개월 예상 순이익 및 자본 기준의 PER, ROE
    - bps / eps: 현재 주당순자산(BPS) 및 주당순이익(EPS)
    - past_eps: 1년 전 시점의 EPS (PEG 계산에 활용)
    - peg (Price/Earnings-to-Growth): PER을 EPS 성장률(최근 1년)로 나눈 값. 낮을수록 이익 성장성 대비 주가가 저평가됨을 의미.
3. 내재가치 및 안전마진 지표 (보수적 시나리오: 최저 ROE & PBR 1 회귀 가정)
   이 그룹은 10년 내 '가장 낮았던 ROE'를 향후 10년간 유지하고, 10년 뒤 PBR이 1배가 된다고 가정한 매우 보수적인 지표입니다.
    - expected_price: 10년 뒤 예상 주가. (현재 BPS * (1 + 최저ROE)^10 * PBR 1배)
    - ep_curr_10 / ep_curr_15: 10년 뒤 예상 주가를 각각 연 10%, 연 15%의 할인율(요구수익률)로 당겨온 현재 적정가치.
    - ep_curr_10_dratio / ep_curr_15_dratio: 현재가 대비 할인율(안전마진). [(10% 또는 15% 할인 적정가치 - 현재가) / 적정가치 * 100]. 값이 클수록 저평가.
    - return_multiple: 10년 뒤 예상 주가가 현재가 대비 몇 배가 되는지를 나타내는 배수.
    - expected_cagr: 10년간 주가 상승의 연평균 복리 수익률(CAGR)에 과거 평균 배당수익률을 더한 총 기대수익률.
4. 내재가치 및 안전마진 지표 (중립 시나리오: 평균 ROE & PBR 1 회귀 가정)
   위 3번 항목과 계산 로직은 동일하나, '최저 ROE' 대신 '과거 10년 평균 ROE(avg_roe_ever)'를 적용한 지표입니다.
    - expected_price_avgroe / epa_curr_10 / epa_curr_15: 평균 ROE를 가정한 10년 뒤 주가와 이를 10%, 15%로 할인한 현재 적정가치.
    - epa_curr_10_dratio / epa_curr_15_dratio: 평균 ROE 가정 시의 안전마진(할인율).
    - return_multiple_avgroe / expected_cagr_avgroe: 평균 ROE 가정 시의 10년 투자 배수 및 총 기대수익률(CAGR).
5. 내재가치 및 안전마진 지표 (역사적 PBR 시나리오: 최저/평균 ROE & 과거 평균 PBR 유지 가정)
   10년 뒤 주가가 PBR 1배가 아닌, 기업 고유의 과거 평균 PBR(최대 3배 한도)로 평가받는다고 가정한 현실적 지표입니다.
    - fep_expected_price / fepa_expected_price_avgroe: 과거 평균 PBR을 적용한 10년 뒤 예상 주가. (최저 ROE 및 평균 ROE 기준)
    - fep_curr_10 / fepa_curr_10 등: 위 예상 주가를 할인율 10%, 15%로 당겨온 현재 적정가.
    - fep_curr_10_dratio / fepa_curr_10_dratio 등: 해당 가치 대비 현재 주가의 안전마진(할인율).
    - fep_expected_cagr / fep_expected_cagr_avgroe: 역사적 PBR 가정 시의 예상 총 복리 수익률(CAGR).
6. 10년 역사적 통계 및 연도별 피벗 데이터 (Historical Trend)
    - min_roe_ever / avg_roe_ever / max_roe_ever: 과거 10년간 기록한 ROE의 최솟값, 평균값, 최댓값.
    - hist_avg_pbr / hist_avg_dividend: 과거 10년간 평균 PBR 및 평균 배당수익률.
    - aaa_roe ~ kkk_roe: 10년전(aaa)부터 현재(kkk)까지 연도별 ROE 추이.
    - aaa_bps ~ kkk_bps: 연도별 BPS 추이 (자본의 축적 과정 확인).
    - aaa_eps ~ kkk_eps: 연도별 EPS 추이 (이익 창출력 확인).
    - aaa_dividend ~ kkk_dividend: 연도별 배당수익률 추이.
    - bbb_eps_ratio ~ kkk_eps_ratio: 연도별 EPS 전년 대비 증감률(YoY 성장률).
    ----------------------------------------------------------------------------------------

    결과는 다음 양식을 지켜주세요:
    RANK_1. [종목명:코드]
    - 선정 이유 (핵심 지표 위주로 2~3줄 요약)
    - 모닝스타 방식의 기업의 경제적 해자: 넓다 / 좁다 / 없다 중 하나 표시
    * 선정된 종목의 아래 지표 표시 (지표값은 굵게 빨강색으로 표시)
    - market_cap_백억 / trade_value_천만 / 영업이익률_pct / change_rate / curr_price
    - dividend_yield / pbr / per / roe / forward_per / forward_roe
    - psr / peg
    - ep_curr_10_dratio / epa_curr_10_dratio / fep_curr_10_dratio / fepa_curr_10_dratio
    - expected_cagr / expected_cagr_avgroe / fep_expected_cagr / fep_expected_cagr_avgroe
    - aaa_roe ~ kkk_roe
    - aaa_bps / fff_bps / kkk_bps
    - aaa_eps / fff_eps / kkk_eps
    - aaa_dividend / fff_dividend / kkk_dividend
    RANK_2. ...
    RANK_3. ...
    RANK_4. ...
    RANK_5. ...
    """
    prompt = f"다음 쿼리 결과 데이터를 분석해 줘:\n\n{data}"
    
    max_retries = 60 # 최대 3번까지 재시도
    
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model='gemini-3.8-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=gems_instruction,
                )
            )
            return response.text # 성공하면 결과 반환
            
        except Exception as e:
            error_msg = str(e)
            if "503" in error_msg or "UNAVAILABLE" in error_msg:
                if attempt < max_retries - 1:
                    print(f"⚠️ 구글 서버 과부하 발생. 60초 후 재시도합니다... ({attempt+1}/{max_retries})")
                    time.sleep(60) # 60초 대기
                else:
                    raise Exception("서버 지연이 계속되어 분석을 실패했습니다.")
            else:
                # 503 외의 다른 에러는 즉시 중단
                raise e

# 3. 분석 결과를 이메일로 전송
def send_email(content):
    print("3. 분석 결과를 이메일로 전송 중입니다...")
    
    sender_email = "gso..." 
    receiver_email = "gsol..." 
    password = "nb..." 
    
    msg = MIMEText(content)
    msg['Subject'] = "📈 [M]오늘의 Top 5 추천 종목 (AI 분석)"
    msg['From'] = sender_email
    msg['To'] = receiver_email
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender_email, password)
        server.send_message(msg)
        
    print("이메일 전송 성공!")

if __name__ == "__main__":
    try:
        raw_data = get_postgres_data()
        
        if not raw_data.strip():
            print("❌ 쿼리 결과가 비어있습니다. 서버나 쿼리문을 확인해주세요.")
        else:
            analysis_result = analyze_with_gemini_gems(raw_data)
            send_email(analysis_result)
            print("🎉 모든 자동화 작업이 성공적으로 완료되었습니다!")
            
    except Exception as e:
        print(f"❌ 작업 중 오류가 발생했습니다: {e}")


