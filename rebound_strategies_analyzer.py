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
        - 전일 대비 거래량 85% 이상 감소
        - 주가는 하락
        """
        try:
            current_volume = stock_data['current_volume']
            prev_volume = stock_data['prev_volume']
            
            # 거래량 변화율 계산
            volume_change = ((current_volume - prev_volume) / prev_volume) * 100
            
            # 조건 확인: 거래량 85% 이상 감소
            if volume_change <= -85:
                return True
                
        except Exception as e:
            print(f"거래량 급감 분석 중 오류 발생: {str(e)}")
        
        return False

    def analyze_ma45(self, historical_data):
        """45일선(세력선) 전략 분석
        - 현재가가 45일 이동평균선 근처에서 지지
        - 거래량 증가
        """
        try:
            if len(historical_data) < 45:
                return False
                
            # 45일 이동평균선 계산
            historical_data['MA45'] = historical_data['close'].rolling(window=45).mean()
            
            # 최근 데이터
            latest = historical_data.iloc[0]
            prev = historical_data.iloc[1]
            
            # 조건 확인
            price_near_ma = 0.98 <= (latest['close'] / latest['MA45']) <= 1.02  # 2% 이내
            volume_increase = latest['volume'] > prev['volume'] * 1.5  # 거래량 50% 이상 증가
            
            return price_near_ma and volume_increase
            
        except Exception as e:
            print(f"45일선 분석 중 오류 발생: {str(e)}")
            return False

    def analyze_ma360(self, historical_data):
        """360일선(최후의 수비선) 전략 분석
        - 현재가가 360일 이동평균선 근처에서 지지
        - 과거 360일선 지지 이력 확인
        """
        try:
            if len(historical_data) < 360:
                return False
                
            # 360일 이동평균선 계산
            historical_data['MA360'] = historical_data['close'].rolling(window=360).mean()
            
            # 최근 데이터
            latest = historical_data.iloc[0]
            
            # 현재가가 360일선 근처인지 확인 (3% 이내)
            price_near_ma = 0.97 <= (latest['close'] / latest['MA360']) <= 1.03
            
            # 과거 360일선 지지 이력 확인
            if price_near_ma:
                # 과거 데이터에서 360일선 지지 횟수 계산
                support_count = 0
                for i in range(1, len(historical_data) - 5):
                    window = historical_data.iloc[i:i+5]
                    if any(0.97 <= (price / ma) <= 1.03 for price, ma in zip(window['close'], window['MA360'])):
                        support_count += 1
                
                # 최소 2회 이상의 지지 이력이 있어야 함
                return support_count >= 2
                
            return False
            
        except Exception as e:
            print(f"360일선 분석 중 오류 발생: {str(e)}")
            return False

    def analyze_stock(self, stock_data):
        """개별 종목에 대한 리바운드 전략 분석"""
        try:
            code = stock_data['code']
            name = stock_data['name']
            
            # 1. 거래량 급감 전략 분석
            if self.analyze_volume_drop(stock_data):
                self.results['volume_drop'].append({
                    'code': code,
                    'name': name,
                    'strategy': '거래량급감',
                    'current_price': stock_data['current_price'],
                    'volume_change': ((stock_data['current_volume'] - stock_data['prev_volume']) / stock_data['prev_volume']) * 100
                })
            
            # 과거 데이터 조회
            historical_data = self.get_historical_data(code)
            if historical_data is not None and not historical_data.empty:
                # 2. 45일선 전략 분석
                if self.analyze_ma45(historical_data):
                    self.results['ma45'].append({
                        'code': code,
                        'name': name,
                        'strategy': '45일선',
                        'current_price': stock_data['current_price'],
                        'ma45_price': historical_data.iloc[0]['MA45']
                    })
                
                # 3. 360일선 전략 분석
                if self.analyze_ma360(historical_data):
                    self.results['ma360'].append({
                        'code': code,
                        'name': name,
                        'strategy': '360일선',
                        'current_price': stock_data['current_price'],
                        'ma360_price': historical_data.iloc[0]['MA360']
                    })
                    
        except Exception as e:
            print(f"종목 분석 중 오류 발생 ({stock_data.get('name', 'Unknown')}): {str(e)}")

    def get_results(self):
        """분석 결과 반환"""
        return self.results 