import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime

def test_google_sheets():
    """구글 시트 연결 테스트"""
    
    try:
        # 1. 인증 설정
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        gc = gspread.authorize(creds)
        
        print("✅ 구글 시트 인증 성공!")
        
        # 2. 테스트 스프레드시트 생성
        spreadsheet_name = f"테스트_시트_{datetime.now().strftime('%Y%m%d_%H%M')}"
        spreadsheet = gc.create(spreadsheet_name)
        
        print(f"✅ 스프레드시트 생성 성공: {spreadsheet_name}")
        print(f"🔗 URL: {spreadsheet.url}")
        
        # 3. 데이터 입력 테스트
        worksheet = spreadsheet.sheet1
        
        test_data = [
            ['종목명', '현재가', '변화율', '시가총액'],
            ['삼성전자', '71000', '+1.2%', '4230000'],
            ['SK하이닉스', '89000', '-0.8%', '650000'],
            ['NAVER', '180000', '+2.1%', '300000']
        ]
        
        worksheet.update('A1', test_data)
        
        print("✅ 데이터 입력 성공!")
        print("📱 구글 시트에서 확인해보세요!")
        
        return True
        
    except FileNotFoundError:
        print("❌ credentials.json 파일을 찾을 수 없습니다.")
        print("💡 파일이 현재 디렉토리에 있는지 확인하세요.")
        return False
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        return False

if __name__ == "__main__":
    test_google_sheets()
