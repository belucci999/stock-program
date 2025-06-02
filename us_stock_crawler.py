import os
import time
from datetime import datetime
import yfinance as yf
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv
import schedule
import requests

# Load environment variables
load_dotenv()

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'credentials.json'
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

def get_sp500_symbols():
    """S&P 500 종목 리스트를 가져오는 함수"""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]
    return df['Symbol'].tolist()

def get_nasdaq100_symbols():
    """나스닥 100 종목 리스트를 가져오는 함수"""
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    tables = pd.read_html(url)
    df = tables[4]  # 나스닥 100 종목 테이블의 인덱스
    return df['Ticker'].tolist()

def get_stock_data(symbols):
    """야후 파이낸스에서 주식 데이터를 가져오는 함수"""
    all_data = []
    
    for symbol in symbols:
        try:
            # 주식 정보 가져오기
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # 거래량 데이터 가져오기
            hist = stock.history(period="2d")
            
            if len(hist) >= 2:
                current_volume = hist['Volume'].iloc[-1]
                prev_volume = hist['Volume'].iloc[-2]
                volume_change = ((current_volume - prev_volume) / prev_volume * 100) if prev_volume > 0 else 0
                
                stock_data = {
                    '종목명': info.get('longName', 'N/A'),
                    '종목코드': symbol,
                    '시장구분': 'NASDAQ' if '^NDX' in info.get('symbol', '') else 'S&P500',
                    '현재가': info.get('currentPrice', 'N/A'),
                    '전일비': info.get('regularMarketChange', 'N/A'),
                    '거래량': current_volume,
                    '전일거래량': prev_volume,
                    '거래대금': info.get('regularMarketVolume', 'N/A') * info.get('currentPrice', 0),
                    '전일거래대금': prev_volume * info.get('previousClose', 0),
                    '거래량변화율': f"{volume_change:.2f}%",
                    'PER': info.get('forwardPE', 'N/A'),
                    'PBR': info.get('priceToBook', 'N/A'),
                    'ROE': info.get('returnOnEquity', 'N/A'),
                    '영업이익': info.get('operatingMargins', 'N/A'),
                    '영업이익증가율': 'N/A',  # 필요시 계산 로직 추가
                    '수집일자': datetime.now().strftime('%Y-%m-%d')
                }
                
                all_data.append(stock_data)
                print(f"데이터 수집 완료: {symbol}")
                
            time.sleep(1)  # API 호출 제한 방지
            
        except Exception as e:
            print(f"Error collecting data for {symbol}: {str(e)}")
            continue
    
    return pd.DataFrame(all_data)

def update_google_sheets(df):
    """Google Sheets API를 사용하여 데이터를 업데이트하는 함수"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)
        
        # 현재 날짜로 시트 이름 생성
        date_str = datetime.now().strftime('%Y-%m-%d')
        sheet_name = f"미국주식_{date_str}"
        
        # 새로운 시트 생성
        body = {
            'requests': [
                {
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }
            ]
        }
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=body).execute()
        
        # 데이터 업데이트
        values = [df.columns.tolist()] + df.values.tolist()
        body = {
            'values': values
        }
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"데이터가 성공적으로 업데이트되었습니다. (시트: {sheet_name})")
    except Exception as e:
        print(f"Google Sheets 업데이트 중 오류 발생: {str(e)}")

def main():
    """메인 실행 함수"""
    print("미국 주식 데이터 크롤링 시작...")
    
    # S&P 500과 나스닥 100 종목 리스트 가져오기
    sp500_symbols = get_sp500_symbols()
    nasdaq100_symbols = get_nasdaq100_symbols()
    
    # 중복 제거
    all_symbols = list(set(sp500_symbols + nasdaq100_symbols))
    
    # 데이터 수집 및 저장
    df = get_stock_data(all_symbols)
    print(f"총 {len(df)}개 종목 데이터 수집 완료")
    
    # Google Sheets 업데이트
    update_google_sheets(df)

def schedule_job():
    """매일 장 마감 후 실행될 작업 예약"""
    # 미국 장 마감 후 실행 (한국 시간 기준 다음날 아침 6:00)
    schedule.every().day.at("06:00").do(main)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    print("프로그램 시작...")
    print("매일 06:00에 데이터를 수집하도록 설정되었습니다.")
    schedule_job()