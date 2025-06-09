import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime
import os
import time

def test_google_sheets_fixed():
    """구글 시트 + 드라이브 API 테스트"""
    
    print("🔍 구글 시트 연결 재테스트 중...")
    
    try:
        # 구글 시트 + 드라이브 API 연결
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        gc = gspread.authorize(creds)
        
        print("✅ 구글 시트 인증 성공!")
        print("⏳ Drive API 테스트 중...")
        
        # 테스트 스프레드시트 생성 (Drive API 사용)
        test_name = f"주식분석_테스트_{datetime.now().strftime('%Y%m%d_%H%M')}"
        
        # Drive API가 활성화되어 있는지 테스트
        spreadsheet = gc.create(test_name)
        
        print("✅ Google Drive API 작동 확인!")
        print(f"✅ 테스트 스프레드시트 생성: {test_name}")
        print(f"🔗 URL: {spreadsheet.url}")
        
        # 데이터 입력 테스트
        worksheet = spreadsheet.sheet1
        worksheet.update('A1', [
            ['테스트 항목', '결과'],
            ['Google Sheets API', '✅ 성공'],
            ['Google Drive API', '✅ 성공'],
            ['테스트 시간', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
            ['상태', '모든 API 준비 완료!']
        ])
        
        print("✅ 모든 테스트 성공!")
        print("🎉 구글 시트 자동화 준비 완료!")
        print("📱 구글 드라이브에서 결과를 확인해보세요!")
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ 테스트 실패: {error_msg}")
        
        if "Google Drive API" in error_msg:
            print("\n💡 해결 방법:")
            print("1. Google Cloud Console에서 Google Drive API 활성화")
            print("2. 2-3분 기다린 후 다시 테스트")
            print("3. 링크: https://console.developers.google.com/apis/api/drive.googleapis.com/overview?project=457931181501")
        
        return False

if __name__ == "__main__":
    test_google_sheets_fixed()