import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import time

class ReboundAnalyzer:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.results = {
            'volume_drop': [],  # 거래량 급감 전략
            'ma45': [],         # 45일선 전략
            'ma360': []         # 360일선 전략
        }
    
    def get_historical_data(self, code, days=400):
        """종목의 과거 데이터를 가져오는 함수"""
        url = f"https://finance.naver.com/item/sise_day.naver?code={code}&page=1"
        
        try:
            # 첫 페이지로 총 페이지 수 계산
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 페이지 네비게이션에서 마지막 페이지 찾기
            last_page_elem = soup.select_one('table.Nnavi td.pgRR a')
            if last_page_elem:
                last_page = int(last_page_elem['href'].split('page=')[1])
            else:
                last_page = 10  # 기본값
            
            # 필요한 페이지 수 계산 (1페이지당 10일치 데이터)
            required_pages = min((days // 10) + 1, last_page)
            
            data = []
            for page in range(1, required_pages + 1):
                page_url = f"https://finance.naver.com/item/sise_day.naver?code={code}&page={page}"
                response = requests.get(page_url, headers=self.headers)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                rows = soup.select('table.type2 tr[onmouseover]')
                for row in rows:
                    cols = row.select('td span')
                    if len(cols) >= 7:
                        try:
                            date = cols[0].text.strip()
                            close = float(cols[1].text.strip().replace(',', ''))
                            open_price = float(cols[2].text.strip().replace(',', ''))
                            high = float(cols[3].text.strip().replace(',', ''))
                            low = float(cols[4].text.strip().replace(',', ''))
                            volume = int(cols[5].text.strip().replace(',', ''))
                            
                            data.append({
                                'date': datetime.strptime(date, '%Y.%m.%d'),
                                'open': open_price,
                                'high': high,
                                'low': low,
                                'close': close,
                                'volume': volume
                            })
                        except:
                            continue
                
                time.sleep(0.1)  # 서버 부하 방지
            
            df = pd.DataFrame(data)
            if not df.empty:
                df = df.sort_values('date').reset_index(drop=True)
                # 이동평균선 계산
                df['MA45'] = df['close'].rolling(window=45).mean()
                df['MA360'] = df['close'].rolling(window=360).mean()
                # 일일 변화율 계산
                df['price_change'] = ((df['close'] - df['open']) / df['open']) * 100
                df['volume_change'] = df['volume'].pct_change() * 100
            
            return df
            
        except Exception as e:
            print(f"과거 데이터 수집 중 오류 발생 ({code}): {str(e)}")
            return None

    def analyze_volume_drop(self, stock_data, historical_data):
        """거래량 급감 전략 분석
        핵심 조건:
        1. 전일 대비 거래량 500~1000% 이상 증가 (폭등)
        2. 다음 날 거래량 전일의 20% 이하 (이상적으로 12% 이하)
        3. 1% 이상 하락 음봉 발생
        4. 음봉 고점 돌파 시 매수 타이밍
        """
        try:
            if historical_data is None or len(historical_data) < 3:
                return False
            
            # 최근 3일 데이터 (0: 오늘, 1: 어제, 2: 그저께)
            recent_data = historical_data.tail(3).reset_index(drop=True)
            
            if len(recent_data) < 3:
                return False
            
            today = recent_data.iloc[2]      # 가장 최근
            yesterday = recent_data.iloc[1]   # 어제
            day_before = recent_data.iloc[0]  # 그저께
            
            # 조건 1: 어제 거래량이 그저께 대비 500% 이상 증가
            volume_surge = (yesterday['volume'] / day_before['volume']) >= 5.0  # 500% 이상
            
            # 조건 2: 오늘 거래량이 어제의 20% 이하 (이상적으로 12% 이하)
            volume_drop_ratio = today['volume'] / yesterday['volume']
            volume_drop = volume_drop_ratio <= 0.20  # 20% 이하
            ideal_drop = volume_drop_ratio <= 0.12   # 12% 이하 (더 강한 신호)
            
            # 조건 3: 오늘 1% 이상 하락 음봉
            red_candle = today['price_change'] <= -1.0
            
            # 조건 4: 5일선과의 이격 확인 (10% 이내)
            if len(historical_data) >= 5:
                ma5 = historical_data.tail(5)['close'].mean()
                gap_from_ma5 = abs((today['close'] - ma5) / ma5) * 100
                close_to_ma5 = gap_from_ma5 <= 10  # 5일선과 10% 이내
            else:
                close_to_ma5 = True
            
            # 최종 판단
            basic_signal = volume_surge and volume_drop and red_candle
            strong_signal = volume_surge and ideal_drop and red_candle and close_to_ma5
            
            if strong_signal:
                signal_strength = "강함"
            elif basic_signal:
                signal_strength = "보통"
            else:
                return False
            
            # 결과 저장
            result = {
                'code': stock_data.get('종목코드', ''),
                'name': stock_data.get('종목명', ''),
                'strategy': '거래량급감',
                'signal_strength': signal_strength,
                'current_price': today['close'],
                'volume_surge_ratio': (yesterday['volume'] / day_before['volume']),
                'volume_drop_ratio': volume_drop_ratio,
                'price_change': today['price_change'],
                'buy_point': today['high'],  # 음봉 고점 돌파 시 매수
                'analysis_date': today['date'].strftime('%Y-%m-%d')
            }
            
            return result
            
        except Exception as e:
            print(f"거래량 급감 분석 중 오류 발생: {str(e)}")
            return False

    def analyze_ma45(self, stock_data, historical_data):
        """45일선(세력선) 전략 분석
        핵심 조건:
        1. 20% 이상 급등한 날이 존재
        2. 이후 하락 추세 전환
        3. 거래량이 감소한 상태
        4. 처음으로 45일선에 도달 시 매수 기회
        """
        try:
            if historical_data is None or len(historical_data) < 60:
                return False
            
            # 최근 60일 데이터로 분석
            recent_data = historical_data.tail(60).reset_index(drop=True)
            current = recent_data.iloc[-1]
            
            # 조건 1: 과거 60일 내 20% 이상 급등한 날 찾기
            surge_days = []
            for i in range(1, len(recent_data)):
                daily_change = ((recent_data.iloc[i]['close'] - recent_data.iloc[i-1]['close']) / recent_data.iloc[i-1]['close']) * 100
                if daily_change >= 20:
                    surge_days.append(i)
            
            if not surge_days:
                return False
            
            # 가장 최근 급등일 찾기
            last_surge_day = max(surge_days)
            surge_date = recent_data.iloc[last_surge_day]['date']
            surge_price = recent_data.iloc[last_surge_day]['close']
            
            # 조건 2: 급등 이후 하락 추세 확인
            post_surge_data = recent_data.iloc[last_surge_day:]
            if len(post_surge_data) < 5:
                return False
            
            # 하락 추세: 최고점 대비 현재가 하락
            max_price_after_surge = post_surge_data['high'].max()
            decline_from_peak = ((current['close'] - max_price_after_surge) / max_price_after_surge) * 100
            downtrend = decline_from_peak <= -5  # 5% 이상 하락
            
            # 조건 3: 거래량 감소 확인
            recent_avg_volume = post_surge_data.tail(5)['volume'].mean()
            surge_avg_volume = post_surge_data.head(5)['volume'].mean()
            volume_decreased = recent_avg_volume < surge_avg_volume * 0.7  # 30% 이상 감소
            
            # 조건 4: 45일선 첫 도달 확인
            if pd.isna(current['MA45']):
                return False
            
            ma45_distance = ((current['close'] - current['MA45']) / current['MA45']) * 100
            near_ma45 = -2 <= ma45_distance <= 2  # 45일선 ±2% 이내
            
            # 과거 45일선 터치 이력 확인 (첫 도달인지)
            touched_before = False
            for i in range(last_surge_day, len(recent_data) - 1):
                if not pd.isna(recent_data.iloc[i]['MA45']):
                    past_distance = ((recent_data.iloc[i]['close'] - recent_data.iloc[i]['MA45']) / recent_data.iloc[i]['MA45']) * 100
                    if -2 <= past_distance <= 2:
                        touched_before = True
                        break
            
            first_touch = near_ma45 and not touched_before
            
            # 최종 판단
            if downtrend and volume_decreased and first_touch:
                result = {
                    'code': stock_data.get('종목코드', ''),
                    'name': stock_data.get('종목명', ''),
                    'strategy': '45일선',
                    'current_price': current['close'],
                    'ma45_price': current['MA45'],
                    'surge_date': surge_date.strftime('%Y-%m-%d'),
                    'surge_price': surge_price,
                    'decline_from_peak': decline_from_peak,
                    'volume_decrease_ratio': (recent_avg_volume / surge_avg_volume),
                    'ma45_distance': ma45_distance,
                    'analysis_date': current['date'].strftime('%Y-%m-%d')
                }
                return result
            
            return False
            
        except Exception as e:
            print(f"45일선 분석 중 오류 발생: {str(e)}")
            return False

    def analyze_ma360(self, stock_data, historical_data):
        """360일선(최후의 수비선) 전략 분석
        핵심 조건:
        1. 360일선이 우상향 중
        2. 주가가 360일선을 강하게 이탈
        3. 이후 반드시 360일선까지 회복하려는 경향
        4. 돌파 후 지지 받으면 급등 가능성 매우 높음
        """
        try:
            if historical_data is None or len(historical_data) < 380:
                return False
            
            # 최근 데이터
            current = historical_data.iloc[-1]
            
            if pd.isna(current['MA360']):
                return False
            
            # 조건 1: 360일선 우상향 확인
            ma360_recent = historical_data.tail(20)['MA360'].dropna()
            if len(ma360_recent) < 10:
                return False
            
            # 360일선 기울기 계산 (최근 20일간)
            ma360_slope = (ma360_recent.iloc[-1] - ma360_recent.iloc[0]) / len(ma360_recent)
            upward_trend = ma360_slope > 0
            
            # 조건 2: 과거 360일선 강한 이탈 이력 찾기
            breakthrough_found = False
            breakthrough_date = None
            recovery_started = False
            
            # 최근 60일 내에서 이탈 패턴 찾기
            recent_60days = historical_data.tail(60)
            
            for i in range(len(recent_60days) - 10):
                row = recent_60days.iloc[i]
                if not pd.isna(row['MA360']):
                    # 360일선 대비 -10% 이상 이탈
                    distance = ((row['close'] - row['MA360']) / row['MA360']) * 100
                    if distance <= -10:  # 강한 이탈
                        breakthrough_found = True
                        breakthrough_date = row['date']
                        
                        # 이탈 이후 회복 시도 확인
                        post_breakthrough = recent_60days.iloc[i:]
                        for j in range(1, len(post_breakthrough)):
                            recovery_row = post_breakthrough.iloc[j]
                            if not pd.isna(recovery_row['MA360']):
                                recovery_distance = ((recovery_row['close'] - recovery_row['MA360']) / recovery_row['MA360']) * 100
                                if recovery_distance >= -3:  # 360일선 3% 이내로 회복
                                    recovery_started = True
                                    break
                        break
            
            # 조건 3: 현재 360일선 지지 확인
            current_distance = ((current['close'] - current['MA360']) / current['MA360']) * 100
            near_support = -5 <= current_distance <= 3  # 360일선 근처
            
            # 조건 4: 지지 확인 (최근 거래량 증가)
            recent_volume = historical_data.tail(5)['volume'].mean()
            past_volume = historical_data.tail(20).head(15)['volume'].mean()
            volume_support = recent_volume > past_volume * 1.2  # 20% 이상 증가
            
            # 최종 판단
            if upward_trend and breakthrough_found and recovery_started and near_support:
                signal_strength = "강함" if volume_support else "보통"
                
                result = {
                    'code': stock_data.get('종목코드', ''),
                    'name': stock_data.get('종목명', ''),
                    'strategy': '360일선',
                    'signal_strength': signal_strength,
                    'current_price': current['close'],
                    'ma360_price': current['MA360'],
                    'ma360_slope': ma360_slope,
                    'breakthrough_date': breakthrough_date.strftime('%Y-%m-%d') if breakthrough_date else '',
                    'current_distance': current_distance,
                    'volume_support_ratio': (recent_volume / past_volume),
                    'analysis_date': current['date'].strftime('%Y-%m-%d')
                }
                return result
            
            return False
            
        except Exception as e:
            print(f"360일선 분석 중 오류 발생: {str(e)}")
            return False

    def analyze_stock(self, stock_data):
        """개별 종목에 대한 리바운드 전략 분석 (거래량 급감 전략만 실행)"""
        try:
            code = stock_data.get('종목코드', '')
            name = stock_data.get('종목명', '')
            
            if not code:
                return
            
            print(f"  📊 {name}({code}) 리바운드 분석 중...")
            
            # 과거 데이터 조회
            historical_data = self.get_historical_data(code, days=400)
            if historical_data is None or historical_data.empty:
                return
            
            # 거래량 급감 전략만 실행
            volume_result = self.analyze_volume_drop(stock_data, historical_data)
            if volume_result:
                self.results['volume_drop'].append(volume_result)
                print(f"    ✅ 거래량 급감 신호 발견: {name}")
        except Exception as e:
            print(f"종목 분석 중 오류 발생 ({stock_data.get('종목명', 'Unknown')}): {str(e)}")

    def get_results(self):
        """분석 결과 반환"""
        return self.results 