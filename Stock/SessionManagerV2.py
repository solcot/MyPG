import os
import sys
import time
import pickle
import requests
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoAlertPresentException,
    UnexpectedAlertPresentException,
)
from webdriver_manager.chrome import ChromeDriverManager

# =========================================================
# 설정
# =========================================================
COOKIE_FILENAME = r'C:\StockPy\krx_session.pkl'
CHECK_INTERVAL = 900          # 15분
USER_AGENT = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')

# 계정 정보는 코드에 직접 쓰지 말고 환경변수로 관리하세요.
#   Windows(cmd):  setx KRX_ID "내아이디"   /   setx KRX_PW "내비밀번호"
#   (setx 후에는 새 터미널을 열어야 적용됩니다)
KRX_ID = os.environ.get('KRX_ID', '')
KRX_PW = os.environ.get('KRX_PW', '')

LOGIN_URL = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506'
LOGIN_PAGE_KEY = 'MDCCOMS001'  # 로그인 화면 URL에 포함된 문자열 (로그인 성공 여부 판단용)

MAX_LOGIN_RETRY = 3           # 한 주기당 자동 로그인 최대 시도 횟수 (계정 잠금 방지용으로 작게 유지)
HEADLESS = False              # True로 하면 창이 안 뜸. KRX가 막을 수 있으니 우선 False로 테스트
WAIT_SEC = 15


class LoginFatal(Exception):
    """아이디/비밀번호 오류 등 재시도하면 안 되는 로그인 실패"""


def get_timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def log(msg):
    print(f"[{get_timestamp()}] {msg}", flush=True)


# =========================================================
# 세션 검증 (기존 로직 그대로)
# =========================================================
def check_session_validity():
    if not os.path.exists(COOKIE_FILENAME):
        return False

    try:
        sess = requests.Session()
        sess.headers.update({'User-Agent': USER_AGENT})

        with open(COOKIE_FILENAME, 'rb') as f:
            cookies = pickle.load(f)
            sess.cookies.update(cookies)

        headers = {
            'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101',
            'X-Requested-With': 'XMLHttpRequest'
        }

        otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        otp_params = {
            'locale': 'ko_KR',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'mktId': 'STK',
            'trdDd': datetime.now().strftime('%Y%m%d'),
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false'
        }

        res = sess.post(otp_url, data=otp_params, headers=headers, timeout=10)
        otp_code = res.text.strip()

        if res.status_code == 200 and len(otp_code) > 10 and "LOGOUT" not in otp_code:
            return True
        else:
            log(f"⚠️ 세션 권한 만료 (OTP 생성 실패: {otp_code[:20]})")
            return False

    except Exception as e:
        log(f"❌ 검증 중 에러 발생: {e}")
        return False


# =========================================================
# 자동 로그인 관련 헬퍼
# =========================================================
def drain_alerts(driver, timeout=2):
    """떠 있는 alert/confirm을 모두 수락하고 문구 목록을 반환"""
    texts = []
    end = time.time() + timeout
    while time.time() < end:
        try:
            alert = driver.switch_to.alert
            texts.append(alert.text)
            alert.accept()
            time.sleep(0.3)
        except NoAlertPresentException:
            time.sleep(0.2)
    return texts


def set_value(driver, element, value):
    """send_keys로 입력하고, 값이 안 들어가면 JS로 강제 입력"""
    element.click()
    element.clear()
    element.send_keys(value)
    if element.get_attribute('value') != value:
        driver.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
            element, value)


def build_driver():
    opts = Options()
    opts.add_argument(f'user-agent={USER_AGENT}')
    opts.add_argument("--window-size=1280,800")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    if HEADLESS:
        opts.add_argument("--headless=new")

    for path in (r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                 r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"):
        if os.path.exists(path):
            opts.binary_location = path
            break

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)


def save_cookies(driver):
    sess = requests.Session()
    for c in driver.get_cookies():
        sess.cookies.set(c['name'], c['value'],
                         domain=c.get('domain'), path=c.get('path', '/'))
    with open(COOKIE_FILENAME, 'wb') as f:
        pickle.dump(sess.cookies, f)
    log("💾 쿠키 저장 완료!")


DEBUG_DIR = r'C:\StockPy\krx_debug'

ID_SELECTORS = ["input[placeholder*='아이디']", "#mbrId", "input[name='mbrId']", "input[type='text']"]
PW_SELECTORS = ["input[type='password']", "#pw", "input[name='pw']"]


class PageLayoutError(Exception):
    """로그인 입력창을 못 찾음 - 재시도해도 소용없으므로 중단"""


def find_anywhere(driver, selectors, timeout=WAIT_SEC):
    """메인 문서와 모든 iframe을 돌면서 보이는 요소를 찾는다. 찾으면 해당 frame 안에 머문다."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            driver.switch_to.default_content()
            frames = [None] + driver.find_elements(By.CSS_SELECTOR, "iframe, frame")
        except Exception:
            time.sleep(0.5)
            continue
        for fr in frames:
            try:
                driver.switch_to.default_content()
                if fr is not None:
                    driver.switch_to.frame(fr)
                for sel in selectors:
                    for el in driver.find_elements(By.CSS_SELECTOR, sel):
                        if el.is_displayed():
                            return el
            except Exception:
                continue
        time.sleep(0.5)
    driver.switch_to.default_content()
    return None


def debug_dump(driver, tag):
    """입력창을 못 찾았을 때 화면/HTML/요소 목록을 남긴다 (비밀번호 입력 전이라 민감정보 없음)"""
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        driver.switch_to.default_content()
        png = os.path.join(DEBUG_DIR, f'{ts}_{tag}.png')
        html = os.path.join(DEBUG_DIR, f'{ts}_{tag}.html')
        driver.save_screenshot(png)
        with open(html, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        log(f"   [debug] 현재 URL: {driver.current_url}")
        frames = driver.find_elements(By.CSS_SELECTOR, "iframe, frame")
        log(f"   [debug] iframe 개수: {len(frames)}")
        for i, fr in enumerate(frames):
            log(f"   [debug] iframe[{i}] id={fr.get_attribute('id')!r} name={fr.get_attribute('name')!r} src={fr.get_attribute('src')!r}")
        for el in driver.find_elements(By.TAG_NAME, "input"):
            log("   [debug] input id=%r name=%r type=%r placeholder=%r displayed=%s" % (
                el.get_attribute('id'), el.get_attribute('name'), el.get_attribute('type'),
                el.get_attribute('placeholder'), el.is_displayed()))
        log(f"   [debug] 저장: {png} / {html}")
    except Exception as e:
        log(f"   [debug] 덤프 실패: {type(e).__name__}: {e}")


ALERT_FATAL_KEYWORDS = ('일치', '잘못', '확인해', '잠', '5회', '틀렸')   # JS alert용
PAGE_FATAL_KEYWORDS = ('일치하지', '일치 하지', '잘못 입력', '잘못된', '틀렸', '잠겼')  # 화면 문구용(오탐 방지로 좁게)
CONFIRM_XPATH = ("//*[(self::button or self::a or self::span or self::div or self::input)"
                 " and (normalize-space(.)='확인' or @value='확인')]")


def _visible(driver, xpath):
    try:
        return [e for e in driver.find_elements(By.XPATH, xpath) if e.is_displayed()]
    except UnexpectedAlertPresentException:
        raise
    except Exception:
        return []


def text_visible(driver, keywords):
    """현재 컨텍스트에서 해당 문구를 '직접' 포함한 요소가 화면에 보이는지"""
    for kw in keywords:
        xp = ("//*[not(self::script) and not(self::style) "
              f"and text()[contains(.,'{kw}')]]")
        if _visible(driver, xp):
            return True
    return False


def click_el(driver, el):
    try:
        el.click()
    except Exception:
        driver.execute_script("arguments[0].click();", el)


def iter_contexts(driver, max_depth=2):
    """최상위 문서 -> 모든 (중첩) iframe 순으로 컨텍스트를 전환하며 path를 yield"""
    def walk(path):
        try:
            driver.switch_to.default_content()
            for idx in path:
                driver.switch_to.frame(idx)
        except UnexpectedAlertPresentException:
            raise
        except Exception:
            return
        yield path
        if len(path) < max_depth:
            try:
                driver.switch_to.default_content()
                for idx in path:
                    driver.switch_to.frame(idx)
                n = len(driver.find_elements(By.CSS_SELECTOR, "iframe, frame"))
            except UnexpectedAlertPresentException:
                raise
            except Exception:
                return
            for i in range(n):
                yield from walk(path + [i])
    yield from walk([])


def find_confirm_buttons(driver):
    """현재 컨텍스트에서 보이는 [확인] 버튼 (jQuery UI dialog 우선, 없으면 일반 탐색)"""
    try:
        btns = [e for e in driver.find_elements(
                    By.CSS_SELECTOR, ".ci-dialog .btn-confirm, .ci-dialog .ui-dialog-buttonpane button")
                if e.is_displayed() and e.text.strip() == '확인']
    except UnexpectedAlertPresentException:
        raise
    except Exception:
        btns = []
    return btns or _visible(driver, CONFIRM_XPATH)


def dump_dialogs(driver):
    """실패 시 현재 떠 있는 팝업(dialog)의 문구/HTML을 로그와 스크린샷으로 남긴다"""
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        driver.switch_to.default_content()
        driver.save_screenshot(os.path.join(DEBUG_DIR, f'{ts}_post_login.png'))
        log(f"   [debug] URL: {driver.current_url}")
        for path in iter_contexts(driver):
            dlgs = [e for e in driver.find_elements(
                        By.CSS_SELECTOR, ".ci-dialog, [role='dialog'], .ui-dialog")
                    if e.is_displayed()]
            for d in dlgs:
                log(f"   [debug] frame{path} 팝업 문구: {d.text.strip()[:150]!r}")
                log(f"   [debug] frame{path} 팝업 HTML: {(d.get_attribute('outerHTML') or '')[:800]}")
        driver.switch_to.default_content()
    except Exception as e:
        log(f"   [debug] 팝업 덤프 실패: {type(e).__name__}: {e}")


def handle_post_login(driver, timeout=WAIT_SEC):
    """
    로그인 버튼 클릭 이후 처리 (로그인 폼과 팝업은 iframe 안에 있으므로 모든 iframe을 확인):
      - JS alert -> 수락 (오류 문구면 LoginFatal)
      - '이미 로그인된 계정입니다' 레이어 팝업 -> [확인] 클릭 (팝업이 없으면 그냥 통과)
      - 아이디/비번 오류 문구 -> LoginFatal (재시도로 인한 계정 잠금 방지)
      - 로그인 화면(URL)을 벗어나면 성공으로 간주하고 return
    """
    end = time.time() + timeout
    dup_clicks = 0

    while time.time() < end:
        # 1) 네이티브 alert
        try:
            alert = driver.switch_to.alert
            txt = alert.text
            log(f"   alert: {txt}")
            alert.accept()
            if any(k in txt for k in ALERT_FATAL_KEYWORDS) and '이미 로그인' not in txt:
                raise LoginFatal(f"로그인 거부 메시지 감지: {txt}")
            continue
        except NoAlertPresentException:
            pass

        try:
            # 2) 로그인 화면 이탈 여부 (최상위 문서 URL 기준)
            driver.switch_to.default_content()
            if LOGIN_PAGE_KEY not in driver.current_url:
                return

            # 3) 모든 iframe을 돌며 팝업 확인
            clicked = False
            for _ in iter_contexts(driver):
                try:
                    if text_visible(driver, PAGE_FATAL_KEYWORDS):
                        raise LoginFatal("화면에 로그인 오류 문구가 표시됨 (아이디/비밀번호 확인 필요)")
                    if dup_clicks < 3 and text_visible(driver, ['이미 로그인']):
                        btns = find_confirm_buttons(driver)
                        if btns:
                            log("   '이미 로그인된 계정' 팝업 감지 -> [확인] 클릭")
                            click_el(driver, btns[0])
                            dup_clicks += 1
                            clicked = True
                            break
                except (LoginFatal, UnexpectedAlertPresentException):
                    raise
                except Exception:
                    continue
            if clicked:
                time.sleep(1.0)
                continue
        except UnexpectedAlertPresentException:
            continue  # 다음 루프에서 alert 처리

        time.sleep(0.5)

    dump_dialogs(driver)
    raise RuntimeError("로그인 화면에서 벗어나지 못함 (로그인 실패 가능)")


def perform_auto_login():
    """Selenium으로 자동 로그인 후 쿠키 저장. 아이디/비번 오류면 LoginFatal 발생"""
    if not KRX_ID or not KRX_PW:
        raise LoginFatal("환경변수 KRX_ID / KRX_PW 가 설정되지 않았습니다.")

    log("🚀 자동 로그인 시작")
    driver = None
    try:
        driver = build_driver()
        driver.get(LOGIN_URL)

        # 페이지가 늦게 띄우는 alert + 로그인 화면으로의 redirect를 충분히 기다림
        for t in drain_alerts(driver, timeout=6):
            log(f"   초기 alert: {t}")
        try:
            WebDriverWait(driver, 20).until(lambda d: LOGIN_PAGE_KEY in d.current_url)
        except (TimeoutException, UnexpectedAlertPresentException):
            drain_alerts(driver, timeout=2)
            log(f"   로그인 화면 URL로 이동하지 않음: {driver.current_url}")
        time.sleep(1.5)

        id_input = find_anywhere(driver, ID_SELECTORS)
        if id_input is None:
            debug_dump(driver, 'no_id_input')
            raise PageLayoutError("아이디 입력창을 찾지 못함")
        # find_anywhere가 해당 frame 안에 머무르므로 같은 컨텍스트에서 비밀번호 창 탐색
        pw_input = None
        for sel in PW_SELECTORS:
            els = [e for e in driver.find_elements(By.CSS_SELECTOR, sel) if e.is_displayed()]
            if els:
                pw_input = els[0]
                break
        if pw_input is None:
            debug_dump(driver, 'no_pw_input')
            raise PageLayoutError("비밀번호 입력창을 찾지 못함")

        set_value(driver, id_input, KRX_ID)
        set_value(driver, pw_input, KRX_PW)

        try:
            login_btns = driver.find_elements(By.CSS_SELECTOR, "a.jsLoginBtn")
            if login_btns:
                click_el(driver, login_btns[0])
            else:
                pw_input.send_keys(Keys.ENTER)   # 비밀번호 칸에서 Enter = 로그인
        except Exception:
            pw_input.send_keys(Keys.ENTER)

        handle_post_login(driver)

        time.sleep(2)
        save_cookies(driver)

    finally:
        if driver:
            driver.quit()


def refresh_session():
    """자동 로그인을 최대 MAX_LOGIN_RETRY회 시도. 성공하면 True"""
    for attempt in range(1, MAX_LOGIN_RETRY + 1):
        log(f"🔁 세션 갱신 시도 {attempt}/{MAX_LOGIN_RETRY}")
        try:
            perform_auto_login()
        except (LoginFatal, PageLayoutError) as e:
            log(f"⛔ 재시도 중단: {e}")
            return False
        except Exception as e:
            log(f"❌ 로그인 중 에러: {type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}")

        if check_session_validity():
            log("✅ 세션 갱신 성공")
            return True
        time.sleep(10)
    return False


# =========================================================
# 메인 루프
# =========================================================
if __name__ == "__main__":
    log(f"🕒 KRX 세션 매니저(자동 로그인) 시작 (주기: {CHECK_INTERVAL}초)")

    while True:
        if check_session_validity():
            log("✅ 세션 POST 권한 유효 (Keep-Alive)")
        else:
            if not refresh_session():
                log("🚨 자동 갱신 실패 - 다음 주기에 다시 시도합니다 (계정 잠금 방지)")

        time.sleep(CHECK_INTERVAL)
