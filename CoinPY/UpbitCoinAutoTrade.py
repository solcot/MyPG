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
    # 2. 핵심 분석 로직
    # =========================================================
    def get_ma_status(self, ticker):
        """이평선 분석 (에러 발생 시 None 반환하여 건너뜀)"""
        try:
            # 설정된 CANDLE_INTERVAL 적용
            df = pyupbit.get_ohlcv(ticker, interval=self.CANDLE_INTERVAL, count=30)
            if df is None or len(df) < 25: return None
            
            curr_ma5 = df['close'].rolling(5).mean().iloc[-1]
            curr_ma10 = df['close'].rolling(10).mean().iloc[-1]
            curr_ma20 = df['close'].rolling(20).mean().iloc[-1]
            
            # 현재가 조회
            curr_price = pyupbit.get_current_price(ticker)
            if curr_price is None: return None

            # 과거 시점 데이터 조회 (타임머신 로직)
            past_time = datetime.now() - timedelta(minutes=self.LOOP_TIME)
            df_past_min = pyupbit.get_ohlcv(ticker, interval="minute1", to=past_time, count=1)
            
            if df_past_min is None or df_past_min.empty: return None
            past_price = df_past_min['close'].iloc[-1]
            
            # 현재 캔들(마지막 행)을 과거 가격으로 대체하여 과거 MA 계산
            past_series = pd.concat([df['close'].iloc[:-1], pd.Series([past_price])])
            past_ma10 = past_series.rolling(10).mean().iloc[-1]
            past_ma20 = past_series.rolling(20).mean().iloc[-1]

            return {
                'curr_price': curr_price,
                'curr_ma5': curr_ma5,
                'curr_ma10': curr_ma10,
                'curr_ma20': curr_ma20,
                'past_ma10': past_ma10,
                'past_ma20': past_ma20,
                'name': ticker
            }
        except Exception:
            return None

    def report_account_status(self):
        """계좌 리포트"""
        try:
            balances = self.upbit.get_balances()
            krw_balance = 0
            coin_reports = []
            
            for b in balances:
                if b['currency'] == 'KRW':
                    krw_balance = float(b['balance'])
                    continue

                avg_price = float(b['avg_buy_price'])
                vol = float(b['balance'])
                valuation_raw = avg_price * vol 
                
                # 1만원 미만 소액은 리포트에서 제외
                if valuation_raw < 10000:
                    continue

                coin_name = b['currency']
                ticker = f"KRW-{coin_name}"
                
                try:
                    curr_price = pyupbit.get_current_price(ticker)
                    if curr_price is None: continue
                except Exception:
                    continue
                
                yield_rate = (curr_price - avg_price) / avg_price * 100
                current_valuation = curr_price * vol
                
                coin_reports.append(
                    f"- **{coin_name}**: {yield_rate:+.2f}% (평가금: {current_valuation:,.0f}원)"
                )
            
            report_msg = (
                f"📊 **[계좌 현황 리포트]**\n"
                f"💰 **매수 가능 현금:** {krw_balance:,.0f} KRW\n"
            )
            
            if coin_reports:
                report_msg += "📦 **보유 코인(1만원 이상):**\n" + "\n".join(coin_reports)
            else:
                report_msg += "📦 **보유 코인:** 없음"
            
            print(report_msg.replace("**", ""))
            self.send_discord_message(report_msg)
                
        except Exception as e:
            print(f"⚠️ 리포트 생성 실패: {e}")

    # =========================================================
    # 3. 매도 로직 (전략 선택 적용)
    # =========================================================
    def execute_sell_logic(self):
        print("\n🔵 [매도 검증] 시작...")
        try:
            balances = self.upbit.get_balances()
            checked_count = 0
            
            for b in balances:
                currency = b['currency']
                if currency == 'KRW': continue
                
                balance_amt = float(b['balance'])
                avg_buy_price = float(b['avg_buy_price'])
                
                # 1만원 미만 소액은 매도 검증 제외
                if balance_amt * avg_buy_price < 10000: 
                    continue
                
                ticker = f"KRW-{currency}"
                
                status = self.get_ma_status(ticker)
                if not status: continue
                
                checked_count += 1
                curr_price = status['curr_price']
                yield_rate = (curr_price - avg_buy_price) / avg_buy_price * 100
                
                # [수정] 설정값에 따른 매도 조건 판단
                is_sell_signal = False
                strategy_msg = ""
                
                if self.SELL_STRATEGY == "10-20":
                    # MA10 < MA20 일 때 매도
                    is_sell_signal = (status['curr_ma10'] < status['curr_ma20'])
                    strategy_msg = f"MA10({status['curr_ma10']:,.0f}) vs MA20({status['curr_ma20']:,.0f})"
                else:
                    # 기본값: MA5 < MA10 일 때 매도
                    is_sell_signal = (status['curr_ma5'] < status['curr_ma10'])
                    strategy_msg = f"MA5({status['curr_ma5']:,.0f}) vs MA10({status['curr_ma10']:,.0f})"
                
                print(f"   👉 [{currency}] 수익률:{yield_rate:+.2f}% | {strategy_msg} | "
                      f"상태:{'📉매도조건' if is_sell_signal else '👌홀딩'}")

                # 매도 실행
                if is_sell_signal:
                    print(f"      🚨 {ticker} 매도 실행합니다! (조건: {self.SELL_STRATEGY})")
                    sell_res = self.upbit.sell_market_order(ticker, balance_amt)
                    
                    if sell_res:
                        time.sleep(1)
                        sell_price = float(status['curr_price'])
                        diff = sell_price - avg_buy_price
                        
                        log_data = [
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            ticker, avg_buy_price, sell_price, 
                            diff, f"{yield_rate:.2f}%", ticker
                        ]
                        self.log_to_file('SellHistory.ini', log_data)
                        
                        discord_msg = (
                            f"📉 **[매도 체결 알림]** {ticker}\n"
                            f"• 전략: {self.SELL_STRATEGY} 데드크로스\n"
                            f"• 수익률: **{yield_rate:+.2f}%**\n"
                            f"• 차익: {diff:,.0f}원\n"
                            f"• 매도가: {sell_price:,.0f}원"
                        )
                        self.send_discord_message(discord_msg)
                        print(f"      ✅ 시장가 매도 및 알림 완료!")
                
                time.sleep(0.5)

            if checked_count == 0:
                print("   (매도 검증할 1만원 이상 보유 코인이 없습니다)")

        except Exception as e:
            print(f"❌ 매도 로직 에러: {e}")

    # =========================================================
    # 4. 매수 로직
    # =========================================================
    def execute_buy_logic(self):
        print("\n🔴 [매수 검증] 시작...")
        try:
            krw_balance = self.upbit.get_balance("KRW")
            if krw_balance < self.AMOUNT_TO_BUY:
                print(f"⚠️ 잔고 부족({krw_balance:,.0f}원)으로 매수를 건너뜁니다.")
                return

            tickers = pyupbit.get_tickers(fiat="KRW")
            all_tickers_data = self.get_market_snapshot(tickers)
            
            candidates = []
            for info in all_tickers_data:
                if info['acc_trade_price_24h'] >= self.TRADE_VALUE:
                    candidates.append(info['market'])
            
            print(f"   🔎 1차 필터링(거래대금 {self.TRADE_VALUE//100000000}억↑) 통과: {len(candidates)}개 (전체 검증 시작)")

            for ticker in candidates:
                status = self.get_ma_status(ticker)
                
                if not status:
                    print(f"   😶 [{ticker}] 데이터 부족 또는 조회 실패 (Pass)")
                    time.sleep(0.5) 
                    continue

                cond_now = (status['curr_price'] > status['curr_ma5'] > status['curr_ma10'] > status['curr_ma20'])
                cond_past = (status['past_ma10'] < status['past_ma20'])

                print(f"   👁️ [{ticker}] {status['curr_price']:,.2f}원 | "
                      f"정배열(P>5>10>20):{'⭕' if cond_now else '❌'} | "
                      f"과거(10<20):{'⭕' if cond_past else '❌'}")

                if cond_now and cond_past:
                    print(f"      🚀 [매수 진입] 조건 만족: {ticker} (추가 매수 가능)")
                    buy_res = self.upbit.buy_market_order(ticker, self.AMOUNT_TO_BUY)
                    
                    if buy_res:
                        log_data = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ticker, self.AMOUNT_TO_BUY, ticker]
                        self.log_to_file('BuyDate.ini', log_data)
                        
                        discord_msg = (
                            f"🚀 **[매수 체결 알림]** {ticker}\n"
                            f"• 매수금액: {self.AMOUNT_TO_BUY:,.0f}원\n"
                            f"• 현재가: {status['curr_price']:,.0f}원 (Approx)\n"
                            f"• 이평선 정배열 + 골든크로스 (추가매수 허용)"
                        )
                        self.send_discord_message(discord_msg)
                        print(f"      ✅ 매수 주문 및 알림 완료!")
                        
                        curr_krw = self.upbit.get_balance("KRW")
                        if curr_krw < self.AMOUNT_TO_BUY: break
                
                time.sleep(0.5)

        except Exception as e:
            print(f"❌ 매수 로직 에러: {e}")

    # =========================================================
    # 5. 메인 루프
    # =========================================================
    def run(self):
        print(f"🔥 AutoTrade 시작... [Loop Time: {self.LOOP_TIME}분]")
        self.send_discord_message(f"🔥 **AutoTrade 서비스 시작** (Loop: {self.LOOP_TIME}분 / 캔들: {self.CANDLE_INTERVAL} / 매도전략: {self.SELL_STRATEGY})")
        
        while True:
            start_time = datetime.now()
            print("="*60)
            print(f"⏰ 루프 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            self.report_account_status()
            self.execute_sell_logic()
            self.execute_buy_logic()

            end_time = datetime.now()
            elapsed = (end_time - start_time).seconds
            sleep_sec = (self.LOOP_TIME * 60) - elapsed
            
            print(f"\n💤 {sleep_sec}초 대기 후 다음 루프 실행...")
            if sleep_sec > 0:
                time.sleep(sleep_sec)
            else:
                print("⚠️ 로직 수행 시간이 루프 타임보다 깁니다. 즉시 재시작합니다.")

if __name__ == "__main__":
    bot = UpbitAutoTrade()
    bot.run()
