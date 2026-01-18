import pyupbit
import time
import yaml
import pandas as pd
from datetime import datetime, timedelta
import os
import requests

# =========================================================
# 1. 설정 및 초기화
# =========================================================
class UpbitAutoTrade:
    def __init__(self):
        self.config = self.load_config()
        self.access = self.config['access_key']
        self.secret = self.config['secret_key']
        self.discord_url = self.config.get('DISCORD_WEBHOOK_URL', '') 
        
        self.upbit = pyupbit.Upbit(self.access, self.secret)
        
        self.LOOP_TIME = int(self.config.get('LOOP_TIME', 30))
        self.AMOUNT_TO_BUY = int(self.config.get('AMOUNT_TO_BUY', 100000))
        self.TRADE_VALUE = int(self.config.get('TRADE_VALUE', 3000000000))
        
        # [설정] 캔들 간격 읽어오기
        self.CANDLE_INTERVAL = self.config.get('CANDLE_INTERVAL', 'day')
        
        # [설정] 매도 전략 읽어오기 (기본값: 5-10)
        self.SELL_STRATEGY = self.config.get('SELL_STRATEGY', '5-10')
        
        start_msg = (f"🤖 자동매매 봇 초기화 완료\n"
                     f"- 주기: {self.LOOP_TIME}분\n"
                     f"- 캔들: {self.CANDLE_INTERVAL}\n"
                     f"- 매수금: {self.AMOUNT_TO_BUY}원\n"
                     f"- 매도전략: {self.SELL_STRATEGY} 데드크로스")
        print(start_msg)
        self.send_discord_message(start_msg)

    def load_config(self):
        try:
            with open('config.yaml', encoding='UTF-8') as f:
                return yaml.load(f, Loader=yaml.FullLoader)
        except Exception as e:
            print(f"⚠️ 설정 파일 로드 실패: {e}")
            return {}

    def log_to_file(self, filename, data_list):
        log_str = ",".join(map(str, data_list))
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(f"{log_str}\n")

    def send_discord_message(self, msg):
        if not self.discord_url: return
        try:
            requests.post(self.discord_url, data={"content": msg})
        except Exception as e:
            print(f"❌ 디스코드 전송 실패: {e}")

    def get_market_snapshot(self, tickers):
        """전 종목 시세 조회 (Chunking)"""
        url = "https://api.upbit.com/v1/ticker"
        headers = {"accept": "application/json"}
        result_list = []
        for i in range(0, len(tickers), 100):
            chunk = tickers[i:i+100]
            if not chunk: break
            markets_str = ",".join(chunk)
            try:
                response = requests.get(url, params={"markets": markets_str}, headers=headers)
                data = response.json()
                if isinstance(data, list):
                    result_list.extend(data)
                time.sleep(0.5) 
            except Exception as e:
                print(f"❌ API 조회 중 에러: {e}")
        return result_list

    # =========================================================
    # 보유 코인 목록
    # =========================================================
    def get_my_coins(self):
        """현재 보유 중인 코인(ticker) 집합 반환"""
        coins = set()
        try:
            balances = self.upbit.get_balances()
            for b in balances:
                if b['currency'] != 'KRW' and float(b['balance']) > 0:
                    coins.add("KRW-" + b['currency'])
        except Exception as e:
            print(f"⚠️ 보유 코인 조회 실패: {e}")
        return coins

    # =========================================================
    # 2. 핵심 분석 로직
    # =========================================================
    def get_ma_status(self, ticker):
        try:
            candles_ago = max(1, self.LOOP_TIME // self._get_interval_minutes())
            count_needed = 30 + candles_ago + 5
            
            df = pyupbit.get_ohlcv(ticker, interval=self.CANDLE_INTERVAL, count=count_needed)
            if df is None or len(df) < 25: 
                return None
            
            curr_ma5 = df['close'].rolling(5).mean().iloc[-1]
            curr_ma10 = df['close'].rolling(10).mean().iloc[-1]
            curr_ma20 = df['close'].rolling(20).mean().iloc[-1]
            
            if pd.isna(curr_ma5) or pd.isna(curr_ma10) or pd.isna(curr_ma20):
                return None
            
            past_idx = -1 - candles_ago
            if len(df) < abs(past_idx):
                return None
            
            past_ma10 = df['close'].rolling(10).mean().iloc[past_idx]
            past_ma20 = df['close'].rolling(20).mean().iloc[past_idx]
            
            if pd.isna(past_ma10) or pd.isna(past_ma20):
                return None
            
            curr_price = pyupbit.get_current_price(ticker)
            if curr_price is None: 
                return None

            return {
                'curr_price': curr_price,
                'curr_ma5': curr_ma5,
                'curr_ma10': curr_ma10,
                'curr_ma20': curr_ma20,
                'past_ma10': past_ma10,
                'past_ma20': past_ma20,
                'name': ticker
            }
        except Exception as e:
            print(f"⚠️ [{ticker}] MA 계산 실패: {e}")
            return None

    def _get_interval_minutes(self):
        if self.CANDLE_INTERVAL == "day":
            return 1440
        elif self.CANDLE_INTERVAL == "minute240":
            return 240
        elif self.CANDLE_INTERVAL == "minute60":
            return 60
        elif self.CANDLE_INTERVAL == "minute10":
            return 10
        return 1440

    # =========================================================
    # 4. 매수 로직 (보유 코인 재매수 방지)
    # =========================================================
    def execute_buy_logic(self):
        print("\n🔴 [매수 검증] 시작...")
        try:
            krw_balance = self.upbit.get_balance("KRW")
            if krw_balance < self.AMOUNT_TO_BUY:
                print(f"⚠️ 잔고 부족({krw_balance:,.0f}원)으로 매수를 건너뜁니다.")
                return

            my_coins = self.get_my_coins()
            if my_coins:
                print(f"   📦 현재 보유 중인 코인: {', '.join(sorted(my_coins))}")

            tickers = pyupbit.get_tickers(fiat="KRW")
            all_tickers_data = self.get_market_snapshot(tickers)
            
            candidates = []
            for info in all_tickers_data:
                if info['acc_trade_price_24h'] >= self.TRADE_VALUE:
                    candidates.append(info['market'])
            
            print(f"   🔎 1차 필터링 통과: {len(candidates)}개")

            for ticker in candidates:
                if ticker in my_coins:
                    print(f"   🔒 [{ticker}] 이미 보유 중 → 매수 스킵")
                    continue

                status = self.get_ma_status(ticker)
                if not status:
                    continue

                cond_now = (status['curr_price'] > status['curr_ma5'] > status['curr_ma10'] > status['curr_ma20'])
                cond_past = (status['past_ma10'] < status['past_ma20'])

                if cond_now and cond_past:
                    print(f"      🚀 [매수 진입] {ticker}")
                    buy_res = self.upbit.buy_market_order(ticker, self.AMOUNT_TO_BUY)
                    if buy_res:
                        self.send_discord_message(f"🚀 매수 체결: {ticker}")
                        curr_krw = self.upbit.get_balance("KRW")
                        if curr_krw < self.AMOUNT_TO_BUY:
                            break

                time.sleep(0.5)

        except Exception as e:
            print(f"❌ 매수 로직 에러: {e}")

    # =========================================================
    # 5. 메인 루프
    # =========================================================
    def run(self):
        print(f"🔥 AutoTrade 시작... [Loop Time: {self.LOOP_TIME}분]")
        while True:
            self.execute_buy_logic()
            time.sleep(self.LOOP_TIME * 60)

if __name__ == "__main__":
    bot = UpbitAutoTrade()
    bot.run()
