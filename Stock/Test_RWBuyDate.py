import os
from datetime import datetime, timedelta

# 테스트용 BUYDATE_FILE
BUYDATE_FILE = "C:\\StockPy\\BuyDate_test.ini"

# 기존 함수들 그대로 사용 (add_buy_record, remove_sell_record, get_old_symbols)
def add_buy_record(sym, stock_name):
    """매수 기록 추가"""
    today_str = datetime.now().strftime("%Y%m%d")
    with open(BUYDATE_FILE, "a", encoding="utf-8") as f:
        f.write(f"{today_str} {sym} {stock_name}\n")

def remove_sell_record(sym):
    """매도 시 해당 종목 기록 삭제"""
    if not os.path.exists(BUYDATE_FILE):
        return
    lines = []
    with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()
    with open(BUYDATE_FILE, "w", encoding="utf-8") as f:
        for line in lines:
            if not line.strip():
                continue
            parts = line.strip().split()
            if len(parts) >= 2 and parts[1] == sym:
                continue  # 매도 종목은 건너뛰기
            f.write(line)

def get_old_symbols(days=5):
    """
    BUYDATE_FILE에서 days일 이상 보유한 종목 조회
    """
    old_symbols = []
    six_days_ago = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

    if not os.path.exists(BUYDATE_FILE):
        return old_symbols

    with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            buy_date = parts[0]
            symbol = parts[1]
            stock_name = " ".join(parts[2:])  # 띄어쓰기 있는 종목명 합치기
            if buy_date <= six_days_ago:
                old_symbols.append((symbol, stock_name))

    return old_symbols

# ---------------------------
# 테스트용 코드 시작
# ---------------------------
# 테스트 파일 초기화
if os.path.exists(BUYDATE_FILE):
    os.remove(BUYDATE_FILE)

print("=== BUYDATE_FILE 테스트 시작 ===")

# 1. 기록 추가 테스트
add_buy_record("005930", "            삼성 전자 ")
add_buy_record("000660", " SK 하     이&닉 스 ")
add_buy_record("035720", " 카 카 오     ")

print("\n[1] 기록 추가 후 내용 확인:")
with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
    print(f.read())

# 2. 기록 삭제 테스트
remove_sell_record("000660")  # SK하이닉스 삭제
print("\n[2] '000660' 삭제 후 내용 확인:")
with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
    print(f.read())

# 3. 오래된 기록 조회 테스트
# 파일에 임의로 6일 전 기록 추가
six_days_ago = (datetime.now() - timedelta(days=6)).strftime("%Y%m%d")
with open(BUYDATE_FILE, "a", encoding="utf-8") as f:
    f.write(f"{six_days_ago} 068270     셀*트& 리 온\n")
print("\n[3] 6일 전 기록 추가 후 내용 확인:")
with open(BUYDATE_FILE, "r", encoding="utf-8") as f:
    print(f.read())

old_symbols = get_old_symbols(days=5)
print("\n[3] 5일 이상 보유 종목 조회 결과:")
print(old_symbols)

# 테스트 종료
print("\n=== 테스트 종료 ===")
