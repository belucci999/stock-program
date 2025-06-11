import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

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
            last_page = int(soup.select_one('table.Nnavi td.pgRR a')['href'].split('page=')[1])
            
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
                            volume = int(cols[5].text.strip().replace(',', ''))
                            
                            data.append({
                                'date': datetime.strptime(date, '%Y.%m.%d'),
                                'close': close,
                                'volume': volume
                            })
                        except:
                            continue
            
            return pd.DataFrame(data)
            
        except Exception as e:
            print(f"과거 데이터 수집 중 오류 발생 ({code}): {str(e)}")
            return None

    def analyze_volume_drop(self, stock_data):
        """거래량 급감 전략 분석
        - 전일 대비 거래량 폭증 (500~1000% 이상)
        - 다음 날 거래량 급감 (전일의 25% 이하, 이상적으로 12% 이하)
        - 음봉 발생
        - 매수 타이밍: 음봉 고점 돌파 시
        """
        try:
            # 필요한 데이터 추출
            current_volume = stock_data.get('current_volume', 0)
            prev_volume = stock_data.get('prev_volume', 0)
            current_price = stock_data.get('current_price', 0)
            prev_price = stock_data.get('prev_price', 0)
            
            # 거래량 변화율 계산
            volume_change = ((current_volume - prev_volume) / prev_volume) * 100 if prev_volume > 0 else 0
            
            # 가격 변화 확인 (음봉 여부)
            price_change = ((current_price - prev_price) / prev_price) * 100 if prev_price > 0 else 0
            is_down_candle = price_change < 0
            
            # 과거 데이터 확인 (더 정교한 패턴 분석)
            code = stock_data.get('code')
            historical_data = self.get_historical_data(code, days=30)  # 30일 데이터 가져오기
            
            if historical_data is not None and len(historical_data) >= 3:
                # 최근 3일 데이터
                day0 = historical_data.iloc[0]  # 오늘
                day1 = historical_data.iloc[1]  # 어제
                day2 = historical_data.iloc[2]  # 그저께
                
                # 폭증 후 급감 패턴 체크 (그저께 → 어제 폭증, 어제 → 오늘 급감)
                volume_surge = ((day1['volume'] - day2['volume']) / day2['volume']) * 100 if day2['volume'] > 0 else 0
                volume_drop = ((day0['volume'] - day1['volume']) / day1['volume']) * 100 if day1['volume'] > 0 else 0
                
                # 조건 확인
                surge_condition = volume_surge >= 500  # 500% 이상 폭증
                drop_condition = volume_drop <= -75   # 75% 이상 급감 (전일의 25% 이하)
                down_candle = day0['close'] < day1['close']  # 음봉 발생
                
                # 추가 조건: 음봉의 크기가 5% 이상이면 신뢰도 상승
                significant_down = ((day0['close'] - day1['close']) / day1['close']) * 100 <= -5 if day1['close'] > 0 else False
                
                # 결과 저장 (종합 조건)
                if surge_condition and drop_condition and down_candle:
                    result = {
                        'code': stock_data.get('code'),
                        'name': stock_data.get('name'),
                        'strategy': '거래량급감',
                        'current_price': current_price,
                        'volume_surge': volume_surge,
                        'volume_drop': volume_drop,
                        'down_candle': down_candle,
                        'significant_down': significant_down,
                        'reliability': 'HIGH' if significant_down else 'MEDIUM'
                    }
                    return result
                
        except Exception as e:
            print(f"거래량 급감 분석 중 오류 발생: {str(e)}")
        
        return None

    def analyze_ma45(self, historical_data, stock_data):
        """45일선(세력선) 전략 분석
        - 20% 이상 급등한 날이 존재
        - 이후 하락 추세 전환
        - 거래량이 감소한 상태에서
        - 처음으로 45일선에 도달 시 매수 기회
        """
        try:
            if len(historical_data) < 60:  # 최소 60일 데이터 필요 (급등 패턴 찾기 위해)
                return None
                
            # 45일 이동평균선 계산
            historical_data['MA45'] = historical_data['close'].rolling(window=45).mean()
            
            # 20일 이동평균선 추가 (추세 확인용)
            historical_data['MA20'] = historical_data['close'].rolling(window=20).mean()
            
            # 최근 데이터
            latest = historical_data.iloc[0]  # 가장 최근 거래일
            
            # 현재가가 45일선 근처에 있는지 확인 (±2% 이내)
            price_near_ma45 = 0.98 <= (latest['close'] / latest['MA45']) <= 1.02
            
            if not price_near_ma45:
                return None
            
            # 1. 과거 20% 이상 급등 찾기 (최근 60일 이내)
            surge_days = []
            for i in range(5, min(60, len(historical_data))):
                current = historical_data.iloc[i]
                prev = historical_data.iloc[i+1]
                daily_change = ((current['close'] - prev['close']) / prev['close']) * 100
                
                if daily_change >= 20:
                    surge_days.append(i)
            
            if not surge_days:
                return None  # 급등일 없음
                
            # 2. 급등 이후 하락 추세 확인
            # 가장 최근의 급등일 이후 데이터 확인
            latest_surge = surge_days[0]
            
            # 급등일 이후부터 현재까지 하락 추세 확인
            downtrend = False
            for i in range(latest_surge-1, -1, -1):
                if i >= len(historical_data)-1:
                    continue
                current = historical_data.iloc[i]
                next_day = historical_data.iloc[i-1] if i > 0 else current
                
                # 하락 추세 정의: 45일선 아래로 내려간 적이 있음
                if current['close'] < current['MA45']:
                    downtrend = True
                    break
            
            if not downtrend:
                return None  # 하락 추세 아님
            
            # 3. 거래량 감소 확인 (최근 5일 평균 거래량이 급등일 대비 감소)
            surge_volume = historical_data.iloc[latest_surge]['volume']
            recent_avg_volume = historical_data.iloc[0:5]['volume'].mean()
            
            volume_decreased = recent_avg_volume < surge_volume * 0.7  # 급등일 거래량의 70% 미만
            
            if not volume_decreased:
                return None  # 거래량 감소 아님
            
            # 4. 최근에 처음으로 45일선에 도달했는지 확인
            first_touch = True
            for i in range(1, min(10, len(historical_data))):
                prev = historical_data.iloc[i]
                prev_near_ma45 = 0.98 <= (prev['close'] / prev['MA45']) <= 1.02
                
                if prev_near_ma45:
                    first_touch = False
                    break
            
            if not first_touch:
                return None  # 처음 터치가 아님
            
            # 모든 조건 충족
            result = {
                'code': stock_data.get('code'),
                'name': stock_data.get('name'),
                'strategy': '45일선',
                'current_price': stock_data.get('current_price', latest['close']),
                'ma45_price': latest['MA45'],
                'days_since_surge': latest_surge,
                'surge_pct': ((historical_data.iloc[latest_surge]['close'] - 
                              historical_data.iloc[latest_surge+1]['close']) / 
                              historical_data.iloc[latest_surge+1]['close']) * 100,
                'volume_decreased_pct': ((recent_avg_volume - surge_volume) / surge_volume) * 100
            }
            
            return result
            
        except Exception as e:
            print(f"45일선 분석 중 오류 발생: {str(e)}")
            return None

    def analyze_ma360(self, historical_data, stock_data):
        """360일선(최후의 수비선) 전략 분석
        - 360일선이 우상향 중일 때
        - 주가가 360일선을 이탈했다가 다시 회복하는 패턴
        - 지지 확인 후 급등 가능성
        """
        try:
            if len(historical_data) < 400:  # 최소 400일 데이터 필요
                return None
                
            # 360일 이동평균선 계산
            historical_data['MA360'] = historical_data['close'].rolling(window=360).mean()
            
            # 최근 데이터
            latest = historical_data.iloc[0]
            
            # 1. 현재가가 360일선 근처인지 확인 (±3% 이내)
            price_near_ma360 = 0.97 <= (latest['close'] / latest['MA360']) <= 1.03
            
            if not price_near_ma360:
                return None
            
            # 2. 360일선이 우상향인지 확인
            ma360_slope = (latest['MA360'] - historical_data.iloc[30]['MA360']) / 30
            is_uptrend = ma360_slope > 0
            
            if not is_uptrend:
                return None  # 우상향 아님
            
            # 3. 최근 이탈 후 회복 패턴 확인
            # 과거 90일 동안 360일선 아래로 이탈했다가 다시 위로 올라온 패턴 찾기
            break_and_recovery = False
            for i in range(10, min(90, len(historical_data))):
                if historical_data.iloc[i]['close'] < historical_data.iloc[i]['MA360'] * 0.9:  # 10% 이상 이탈
                    # 이탈 이후 회복 확인
                    recovery_found = False
                    for j in range(i-1, -1, -1):
                        if historical_data.iloc[j]['close'] >= historical_data.iloc[j]['MA360']:
                            recovery_found = True
                            break
                    
                    if recovery_found:
                        break_and_recovery = True
                        break
            
            if not break_and_recovery:
                return None  # 이탈 후 회복 패턴 없음
            
            # 4. 최근 지지 확인 (현재가가 360일선 위에 있고, 최근에 360일선에 닿았다가 올라온 패턴)
            support_confirmed = False
            for i in range(1, min(20, len(historical_data))):
                day_low_near_ma360 = False
                
                # 해당 거래일의 저가가 360일선 근처였는지 가정 (실제로는 일봉 데이터만 있으므로 근사치 계산)
                if historical_data.iloc[i]['close'] < historical_data.iloc[i]['MA360'] * 1.02:
                    day_low_near_ma360 = True
                
                if day_low_near_ma360 and historical_data.iloc[i-1]['close'] > historical_data.iloc[i]['MA360']:
                    support_confirmed = True
                    break
            
            if not support_confirmed:
                return None  # 최근 지지 확인 안됨
            
            # 모든 조건 충족
            result = {
                'code': stock_data.get('code'),
                'name': stock_data.get('name'),
                'strategy': '360일선',
                'current_price': stock_data.get('current_price', latest['close']),
                'ma360_price': latest['MA360'],
                'ma360_slope': ma360_slope,
                'recovery_strength': ((latest['close'] / latest['MA360']) - 1) * 100  # 회복 강도 (%)
            }
            
            return result
            
        except Exception as e:
            print(f"360일선 분석 중 오류 발생: {str(e)}")
            return None

    def analyze_stock(self, stock_data):
        """개별 종목에 대한 리바운드 전략 분석"""
        try:
            code = stock_data['code']
            name = stock_data['name']
            
            # 과거 데이터 조회
            historical_data = self.get_historical_data(code)
            if historical_data is None or historical_data.empty:
                return
            
            # 1. 거래량 급감 전략 분석
            volume_drop_result = self.analyze_volume_drop(stock_data)
            if volume_drop_result:
                self.results['volume_drop'].append(volume_drop_result)
            
            # 2. 45일선 전략 분석
            ma45_result = self.analyze_ma45(historical_data, stock_data)
            if ma45_result:
                self.results['ma45'].append(ma45_result)
            
            # 3. 360일선 전략 분석
            ma360_result = self.analyze_ma360(historical_data, stock_data)
            if ma360_result:
                self.results['ma360'].append(ma360_result)
                    
        except Exception as e:
            print(f"종목 분석 중 오류 발생 ({stock_data.get('name', 'Unknown')}): {str(e)}")

    def get_results(self):
        """분석 결과 반환"""
        return self.results 