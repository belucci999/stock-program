import os
import time
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
import schedule
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'credentials.json'
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
BASE_URL = "https://finance.naver.com/sise/sise_market_sum.naver"

class StockCrawler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    @staticmethod
    def is_regular_stock(stock_name):
        """주식 종목 필터링 함수"""
        # 금융상품 접두어/접미어 패턴 (증권사 이름 + 상품 식별자)
        product_patterns = [
            # 증권사 이름
            '삼성', 'SAMSUNG', '키움', 'KIWOOM',
            '미래에셋', '한국투자', 'NH투자', 'KB',
            '메리츠', '신한', '하나', 'SK',
            '대신', '유안타', '현대차', '교보',
            'IBK', '유진', '이베스트', '신영',
            'DB', '한화', 'BNK', '부국',
            '케이프', '카카오페이', '토스', '다올',
            'KTB', '하이', 'DS', '리딩',
            '비엔케이', '아이비케이'
        ]
        
        # 금융상품 식별자
        product_identifiers = [
            'ETF', 'ETN', '채권', '펀드',
            '인버스', '레버리지', '선물', '옵션',
            'KOSPI', 'KOSDAQ', '나스닥',
            '배당주', '액티브', '합성',
            'TIGER', 'KODEX', 'KBSTAR', 'ARIRANG', 'HANARO',
            'KOSEF', 'KINDEX', 'TIMEFOLIO', 'MASTER'
        ]
        
        # 채권 관련 키워드
        bond_keywords = [
            '국고채', '국채', '통안채', '회사채', '공채', '국공채'
        ]
        
        stock_name = stock_name.upper()
        
        # 1. 채권 관련 상품 제외
        if any(keyword.upper() in stock_name for keyword in bond_keywords):
            return False
            
        # 2. 증권사 금융상품 패턴 확인
        # 증권사 이름과 상품 식별자가 모두 포함된 경우에만 제외
        for company in product_patterns:
            if company.upper() in stock_name:
                if any(identifier.upper() in stock_name for identifier in product_identifiers):
                    return False
        
        # 3. 일반적인 금융상품 식별자 확인
        if any(identifier.upper() in stock_name for identifier in product_identifiers):
            return False
        
        return True

    def get_financial_ratios(self, stock_code):
        """개별 종목의 PER, PBR, ROE, 영업이익, 영업이익증가율 정보를 가져오는 함수"""
        url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
        
        try:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 재무비율 데이터 초기화
            financial_data = {
                'PER': 'N/A',
                'PBR': 'N/A',
                'ROE': 'N/A',
                '영업이익': 'N/A',
                '영업이익증가율': 'N/A'
            }
            
            # PER, PBR, ROE 추출
            table = soup.select_one('table.tb_type1.tb_num')
            if table:
                rows = table.select('tr')
                for row in rows:
                    th = row.select_one('th')
                    td = row.select_one('td')
                    if th and td:
                        if 'PER' in th.text:
                            financial_data['PER'] = td.text.strip()
                        elif 'PBR' in th.text:
                            financial_data['PBR'] = td.text.strip()
                        elif 'ROE' in th.text:
                            financial_data['ROE'] = td.text.strip()
            
            # 영업이익 정보 추출
            table = soup.select_one('div.section.cop_analysis div.sub_section')
            if table:
                rows = table.select('tr')
                for row in rows:
                    th = row.select_one('th')
                    if th and '영업이익' in th.text:
                        tds = row.select('td')
                        if len(tds) >= 2:
                            current_profit = tds[0].text.strip()
                            prev_profit = tds[1].text.strip()
                            financial_data['영업이익'] = current_profit
                            
                            # 영업이익증가율 계산
                            try:
                                current = float(current_profit.replace(',', ''))
                                prev = float(prev_profit.replace(',', ''))
                                if prev != 0:
                                    growth_rate = ((current - prev) / abs(prev)) * 100
                                    financial_data['영업이익증가율'] = f"{growth_rate:.2f}%"
                            except:
                                pass
            
            return financial_data
        except:
            return {
                'PER': 'N/A',
                'PBR': 'N/A',
                'ROE': 'N/A',
                '영업이익': 'N/A',
                '영업이익증가율': 'N/A'
            }

    def get_daily_trading_data(self, stock_code):
        """일별 시세 데이터를 가져오는 함수"""
        url = f"https://finance.naver.com/item/sise_day.naver?code={stock_code}"
        
        try:
            response = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 일별 시세 테이블 찾기
            table = soup.find('table', {'class': 'type2'})
            if table:
                rows = table.find_all('tr')
                volumes = []
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 7:  # 날짜, 종가, 전일비, 시가, 고가, 저가, 거래량
                        try:
                            volume_text = cols[6].text.strip()
                            if volume_text:  # 거래량이 비어있지 않은 경우만
                                volume = int(volume_text.replace(',', ''))
                                volumes.append(volume)
                        except:
                            continue
                
                if len(volumes) >= 2:
                    return {
                        'current_volume': volumes[0],  # 당일 거래량
                        'prev_volume': volumes[1]      # 전일 거래량
                    }
                else:
                    print(f"  - 충분한 거래량 데이터가 없음 (발견된 데이터: {len(volumes)}개)")
        
        except Exception as e:
            print(f"  - 거래량 데이터 수집 중 오류: {str(e)}")
        
        return None

    def collect_all_stocks(self):
        """네이버 금융에서 주식 정보를 크롤링하는 함수"""
        all_data = []
        filtered_data = []
        markets = ['&sosok=0', '&sosok=1']  # 코스피(0)와 코스닥(1)
        
        print("\n데이터 수집 시작...")
        for market_index, market in enumerate(markets):
            market_name = "코스피" if market_index == 0 else "코스닥"
            print(f"\n[{market_name} 시장 수집 시작]")
            page = 1
            while True:
                url = f"{BASE_URL}?{market}&page={page}"
                print(f"\n{market_name} 페이지 {page} 처리 중...")
                
                response = requests.get(url, headers=self.headers)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # 테이블에서 종목 데이터 추출
                table = soup.select_one('table.type_2')
                if not table:
                    print(f"{market_name} 마지막 페이지 도달")
                    break
                    
                rows = table.select('tbody > tr')
                if not rows:
                    print(f"{market_name} 데이터 없음")
                    break
                
                processed_count = 0
                for row in rows:
                    cols = row.select('td')
                    if len(cols) <= 1:
                        continue
                        
                    try:
                        # 종목 정보 추출
                        name = cols[1].select_one('a').text.strip()
                        code = cols[1].select_one('a')['href'].split('code=')[1]
                        print(f"처리 중: [{code}] {name}")
                        
                        current_price = float(cols[2].text.strip().replace(',', ''))
                        
                        # 거래량 데이터 수집
                        print(f"  - 거래량 데이터 수집 중...")
                        volume_data = self.get_daily_trading_data(code)
                        if not volume_data:
                            print(f"  - 거래량 데이터 없음, 건너뜀")
                            continue
                            
                        # 재무비율 데이터 수집
                        print(f"  - 재무비율 데이터 수집 중...")
                        financial_data = self.get_financial_ratios(code)
                        
                        # 종목 데이터 구성
                        stock_data = {
                            'code': code,
                            'name': name,
                            'current_price': current_price,
                            'current_volume': volume_data['current_volume'],
                            'prev_volume': volume_data['prev_volume'],
                            **financial_data
                        }
                        
                        all_data.append(stock_data)
                        processed_count += 1
                        
                        # 일반 주식 필터링
                        if self.is_regular_stock(name):
                            filtered_data.append(stock_data)
                            print(f"  - 일반 주식으로 필터링됨")
                        else:
                            print(f"  - 제외됨 (ETF/ETN 등)")
                            
                    except Exception as e:
                        print(f"  - 오류 발생: {str(e)}")
                        continue
                
                print(f"\n{market_name} 페이지 {page} 완료 (처리된 종목: {processed_count}개)")
                page += 1
                print("잠시 대기 중... (서버 부하 방지)")
                time.sleep(1)  # 서버 부하 방지
        
        print(f"\n전체 수집 완료!")
        print(f"- 전체 종목 수: {len(all_data)}개")
        print(f"- 필터링된 종목 수: {len(filtered_data)}개")
        
        return {
            'all': all_data,
            'filtered': filtered_data
        }

def update_google_sheets(df_all, df_filtered):
    """구글 시트에 데이터를 업로드하는 함수"""
    try:
        # Google Sheets API 인증
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)
        
        # 시트 업데이트
        def update_sheet(data, range_name):
            body = {
                'values': [data.columns.tolist()] + data.values.tolist()
            }
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
        
        # 전체 종목 데이터 업데이트
        update_sheet(df_all, 'AllStocks!A1')
        
        # 필터링된 종목 데이터 업데이트
        update_sheet(df_filtered, 'FilteredStocks!A1')
        
        print("구글 시트 업데이트 완료")
        
    except Exception as e:
        print(f"구글 시트 업데이트 중 오류 발생: {str(e)}")

def main():
    """메인 실행 함수"""
    try:
        # 데이터 수집
        crawler = StockCrawler()
        stock_data = crawler.collect_all_stocks()
        
        # 데이터프레임 변환
        df_all = pd.DataFrame(stock_data['all'])
        df_filtered = pd.DataFrame(stock_data['filtered'])
        
        # 구글 시트 업데이트
        update_google_sheets(df_all, df_filtered)
        
    except Exception as e:
        print(f"프로그램 실행 중 오류 발생: {str(e)}")

def schedule_job():
    """스케줄링된 작업 실행 함수"""
    print(f"작업 시작: {datetime.now()}")
    main()
    print(f"작업 완료: {datetime.now()}")

if __name__ == "__main__":
    # 매일 장 마감 후 실행 (15:30)
    schedule.every().day.at("15:30").do(schedule_job)
    
    while True:
        schedule.run_pending()
        time.sleep(60)