import time
import pickle
import os
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# =========================================================
# 설정
# =========================================================
COOKIE_FILENAME = r'C:\StockPy\krx_session.pkl'
CHECK_INTERVAL = 900  # 15분 (너무 자주 하면 차단될 수 있으므로 15분 권장)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def check_session_validity():
    """
    단순 GET이 아니라, 실제 OTP 생성을 시도하여 세션의 '데이터 권한'을 검증합니다.
    """
    if not os.path.exists(COOKIE_FILENAME):
        return False

    try:
        sess = requests.Session()
        sess.headers.update({'User-Agent': USER_AGENT})
        
        with open(COOKIE_FILENAME, 'rb') as f:
            cookies = pickle.load(f)
            sess.cookies.update(cookies)
        
        # 1. 실제 데이터 요청 시 필요한 Referer 설정
        headers = {
            'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101',
            'X-Requested-With': 'XMLHttpRequest'
        }

        # 2. 가장 가벼운 OTP 요청 (삼성전자 1종목 시세 조회용 OTP)
        # 이 요청이 성공하면 'POST' 권한이 살아있는 것입니다.
        otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        otp_params = {
            'locale': 'ko_KR',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501', # 전종목 시세 화면
            'mktId': 'STK',
            'trdDd': datetime.now().strftime('%Y%m%d'), # 오늘 날짜
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false'
        }

        res = sess.post(otp_url, data=otp_params, headers=headers, timeout=10)
        otp_code = res.text.strip()

        # [검증 로직] OTP 코드가 정상적으로(문자열로) 오고 LOGOUT 문구가 없어야 함
        if res.status_code == 200 and len(otp_code) > 10 and "LOGOUT" not in otp_code:
            return True
        else:
            print(f"[{get_timestamp()}] ⚠️ 세션 권한 만료 (OTP 생성 실패: {otp_code[:20]})")
            return False

    except Exception as e:
        print(f"[{get_timestamp()}] ❌ 검증 중 에러 발생: {e}")
        return False

def perform_manual_login():
    """ Selenium을 통한 수동 로그인 및 쿠키 저장 """
    print("\n" + "="*60)
    print(f"[{get_timestamp()}] 🚀 세션 갱신 브라우저 실행")
    print("="*60)

    chrome_options = Options()
    chrome_options.add_argument(f'user-agent={USER_AGENT}')
    chrome_options.add_argument("--window-size=1280,800")
    
    path_candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
    ]
    for path in path_candidates:
        if os.path.exists(path):
            chrome_options.binary_location = path
            break

    driver = None
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # 로그인 유도 페이지
        driver.get('http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506')
        time.sleep(3)

        try:
            driver.switch_to.alert.accept()
        except:
            pass

        print("\n🛑 로그인을 완료하고 [Enter] 키를 누르세요...")
        input() 

        sess = requests.Session()
        for cookie in driver.get_cookies():
            sess.cookies.set(cookie['name'], cookie['value'])
        
        with open(COOKIE_FILENAME, 'wb') as f:
            pickle.dump(sess.cookies, f)
        
        print(f"[{get_timestamp()}] 💾 쿠키 저장 완료!")

    except Exception as e:
        print(f"[{get_timestamp()}] ❌ 로그인 실패: {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    print(f"[{get_timestamp()}] 🕒 KRX 강력 세션 매니저 시작 (주기: {CHECK_INTERVAL}초)")
    
    while True:
        if check_session_validity():
            print(f"[{get_timestamp()}] ✅ 세션 POST 권한 유효 (Keep-Alive)")
        else:
            perform_manual_login()
        
        time.sleep(CHECK_INTERVAL)


