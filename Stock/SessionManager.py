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
COOKIE_FILENAME = 'krx_session.pkl'
CHECK_INTERVAL = 3600  # 1시간 (3600초)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
TARGET_URL = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'

def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def check_session_validity():
    """
    저장된 쿠키 파일을 로드하여 유효한지 테스트합니다.
    True: 유효함 / False: 만료됨(혹은 파일 없음)
    """
    if not os.path.exists(COOKIE_FILENAME):
        print(f"[{get_timestamp()}] ⚠️ 쿠키 파일이 없습니다.")
        return False

    try:
        sess = requests.Session()
        sess.headers.update({'User-Agent': USER_AGENT})
        
        with open(COOKIE_FILENAME, 'rb') as f:
            cookies = pickle.load(f)
            sess.cookies.update(cookies)
        
        # 테스트 요청 (타임아웃 10초 설정)
        res = sess.get(TARGET_URL, timeout=10)
        
        # [검증 로직]
        # KRX는 세션이 만료되어도 200 OK를 줄 때가 많지만,
        # 정상 로그인 상태라면 페이지 내용(Content-Length)이 충분히 깁니다.
        if res.status_code == 200 and len(res.text) > 2000:
            return True
        else:
            print(f"[{get_timestamp()}] ⚠️ 세션 만료 감지 (응답 길이/코드 이상)")
            return False

    except Exception as e:
        print(f"[{get_timestamp()}] ❌ 검증 중 에러 발생: {e}")
        return False

def perform_manual_login():
    """
    Selenium을 띄워 사용자가 로그인하게 한 뒤, 새 쿠키를 저장합니다.
    """
    print("\n" + "="*60)
    print(f"[{get_timestamp()}] 🚀 세션 갱신 프로세스 시작")
    print("브라우저가 열리면 로그인을 진행해주세요.")
    print("="*60)

    chrome_options = Options()
    # chrome_options.add_argument("--headless") # 로그인을 해야 하므로 헤드리스 금지
    chrome_options.add_argument(f'user-agent={USER_AGENT}')
    chrome_options.add_argument("--window-size=1280,800")
    
    # 크롬 설치 경로 자동 탐색 (필요 시)
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

        driver.get(TARGET_URL)
        time.sleep(3)

        # 팝업 닫기 시도
        try:
            driver.switch_to.alert.accept()
        except:
            pass

        print("\n🛑 [대기 중] 브라우저에서 로그인을 완료하세요.")
        print("👉 로그인이 완료되면 이 창에서 [Enter] 키를 누르세요.")
        input() # 사용자 입력 대기

        # 쿠키 저장
        sess = requests.Session()
        for cookie in driver.get_cookies():
            sess.cookies.set(cookie['name'], cookie['value'])
        
        with open(COOKIE_FILENAME, 'wb') as f:
            pickle.dump(sess.cookies, f)
        
        print(f"[{get_timestamp()}] 💾 쿠키 갱신 및 저장 완료!")

    except Exception as e:
        print(f"[{get_timestamp()}] ❌ 로그인 프로세스 실패: {e}")
    finally:
        if driver:
            driver.quit()

# =========================================================
# 메인 루프 (1시간마다 실행)
# =========================================================
if __name__ == "__main__":
    print(f"[{get_timestamp()}] 🕒 KRX 세션 관리자가 시작되었습니다. (주기: {CHECK_INTERVAL}초)")
    
    while True:
        print(f"\n[{get_timestamp()}] 🔍 세션 상태 점검 중...")
        
        is_valid = check_session_validity()
        
        if is_valid:
            print(f"[{get_timestamp()}] ✅ 세션이 유효합니다. (다음 점검까지 대기)")
        else:
            print(f"[{get_timestamp()}] 🔄 세션 갱신이 필요합니다.")
            perform_manual_login()
        
        # 1시간 대기
        time.sleep(CHECK_INTERVAL)


