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

def get_financial_ratios(stock_code):
    """개별 종목의 PER, PBR, ROE, 영업이익, 영업이익증가율 정보를 가져오는 함수"""
    url = f"https://finance.naver.com/item/main.naver?code={stock_code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
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

def get_daily_trading_data(stock_code):
    """일별 시세 데이터를 가져오는 함수"""
    url = f"https://finance.naver.com/item/sise_day.naver?code={stock_code}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 일별 시세 테이블 찾기
        table = soup.find('table', {'class': 'type2'})
        if table:
            rows = table.find_all('tr', align='center')
            volumes = []
            
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 6:  # 날짜, 종가, 전일비, 시가, 고가, 저가, 거래량
                    try:
                        volume = int(cols[5].text.strip().replace(',', ''))
                        volumes.append(volume)
                    except:
                        continue
            
            if len(volumes) >= 2:
                return {
                    'current_volume': volumes[0],  # 당일 거래량
                    'prev_volume': volumes[1]      # 전일 거래량
                }
    
    except Exception as e:
        print(f"일별 시세 데이터 수집 중 오류 발생 ({stock_code}): {str(e)}")
    
    return None

def get_stock_data():
    """네이버 금융에서 주식 정보를 크롤링하는 함수"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # 전체 페이지를 순회하며 데이터 수집
    all_data = []
    filtered_data = []
    markets = ['&sosok=0', '&sosok=1']  # 코스피(0)와 코스닥(1)
    
    for market in markets:
        page = 1
        while True:
            url = f"{BASE_URL}?{market}&page={page}"
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 테이블에서 종목 데이터 추출
            table = soup.select_one('table.type_2')
            if not table:
                break
                
            rows = table.select('tbody > tr')
            
            for row in rows:
                cols = row.select('td')
                if len(cols) <= 1:
                    continue
                    
                try:
                    stock_name = cols[1].select_one('a').text.strip()
                    
                    # 일반 주식이 아닌 종목 필터링
                    if not is_regular_stock(stock_name):
                        continue
                        
                    stock_code = cols[1].select_one('a')['href'].split('=')[-1]
                    
                    # 일별 시세에서 거래량 데이터 가져오기
                    volume_data = get_daily_trading_data(stock_code)
                    if volume_data:
                        current_volume = volume_data['current_volume']
                        prev_volume = volume_data['prev_volume']
                        
                        # 거래량 변화율 계산
                        volume_change_pct = 0
                        if prev_volume and prev_volume > 0:
                            volume_change_pct = ((current_volume - prev_volume) / prev_volume) * 100
                        
                        stock_data = {
                            '종목명': stock_name,
                            '종목코드': stock_code,
                            '시장구분': '코스피' if market == '&sosok=0' else '코스닥',
                            '현재가': cols[2].text.strip().replace(',', ''),
                            '전일비': cols[3].text.strip().replace(',', ''),
                            '거래량': str(current_volume),
                            '전일거래량': str(prev_volume),
                            '거래대금': cols[6].text.strip().replace(',', ''),
                            '시가총액': cols[7].text.strip().replace(',', ''),  # 시가총액 추가
                            '거래량변화율': f"{volume_change_pct:.2f}%",
                            'PER': 'N/A',
                            'PBR': 'N/A',
                            'ROE': 'N/A',
                            '영업이익': 'N/A',
                            '영업이익증가율': 'N/A'
                        }
                        
                        # 재무비율 정보 추가
                        financial_data = get_financial_ratios(stock_code)
                        stock_data.update(financial_data)
                        
                        # 모든 종목 데이터 저장
                        all_data.append(stock_data)
                        
                        # 거래량 -85% 이하이고 음봉인 종목 필터링
                        price_change = float(cols[3].text.strip().replace(',', ''))
                        if volume_change_pct <= -85 and price_change < 0:
                            filtered_data.append(stock_data)
                            print(f"조건 충족 종목 발견: {stock_data['종목명']}")
                            
                except Exception as e:
                    print(f"데이터 처리 중 오류 발생: {str(e)}")
                    continue
            
            page += 1
            time.sleep(1)  # 서버 부하 방지를 위한 지연
    
    return pd.DataFrame(all_data), pd.DataFrame(filtered_data)

def update_google_sheets(df_all, df_filtered):
    """Google Sheets API를 사용하여 데이터를 업데이트하는 함수"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)
        
        # 현재 날짜로 시트 이름 생성
        date_str = datetime.now().strftime('%Y-%m-%d')
        all_sheet_name = f"{date_str}_전체"
        filtered_sheet_name = f"{date_str}_거래량급감"
        
        # 새로운 시트들 생성
        body = {
            'requests': [
                {
                    'addSheet': {
                        'properties': {
                            'title': all_sheet_name
                        }
                    }
                },
                {
                    'addSheet': {
                        'properties': {
                            'title': filtered_sheet_name
                        }
                    }
                }
            ]
        }
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=body).execute()
        
        # 전체 데이터 업데이트
        values_all = [df_all.columns.tolist()] + df_all.values.tolist()
        body_all = {
            'values': values_all
        }
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{all_sheet_name}!A1',
            valueInputOption='RAW',
            body=body_all
        ).execute()
        
        # 필터링된 데이터 업데이트
        values_filtered = [df_filtered.columns.tolist()] + df_filtered.values.tolist()
        body_filtered = {
            'values': values_filtered
        }
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{filtered_sheet_name}!A1',
            valueInputOption='RAW',
            body=body_filtered
        ).execute()
        
        print(f"데이터가 성공적으로 업데이트되었습니다. (전체: {all_sheet_name}, 거래량급감: {filtered_sheet_name})")
    except Exception as e:
        print(f"Google Sheets 업데이트 중 오류 발생: {str(e)}")

def main():
    """메인 실행 함수"""
    print("주식 데이터 크롤링 시작...")
    df_all, df_filtered = get_stock_data()
    print(f"총 {len(df_all)}개 종목 중 {len(df_filtered)}개 조건 충족")
    update_google_sheets(df_all, df_filtered)

def schedule_job():
    """매일 장 마감 후 실행될 작업 예약"""
    schedule.every().day.at("15:40").do(main)  # 장 마감 후 실행
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    print("프로그램 시작...")
    print("매일 15:40에 데이터를 수집하도록 설정되었습니다.")
    schedule_job()