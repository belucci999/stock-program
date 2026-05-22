import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime
import os
import time

def test_google_sheets_fixed():
    """구글 시트 권한 부여 및 찾기"""
    
    print("🔍 기존 구글 시트 찾기 및 권한 부여...")
    
    try:
        # 구글 시트 + 드라이브 API 연결
        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file('credentials.json', scopes=scope)
        gc = gspread.authorize(creds)
        
        from googleapiclient.discovery import build
        drive_service = build('drive', 'v3', credentials=creds)
        
        print("✅ 구글 시트 인증 성공!")
        
        # 기존 스프레드시트 찾기
        print("🔍 서비스 계정 드라이브에서 스프레드시트 검색 중...")
        results = drive_service.files().list(
            q="mimeType='application/vnd.google-apps.spreadsheet'",
            fields="files(id, name, createdTime)"
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            print("❌ 기존 구글 시트를 찾을 수 없습니다. 새로 생성합니다...")
            # 새 시트 생성
            test_name = f"주식분석_데이터_{datetime.now().strftime('%Y%m%d_%H%M')}"
            spreadsheet = gc.create(test_name)
            sheet_id = spreadsheet.id
            sheet_name = test_name
        else:
            print(f"📋 발견된 스프레드시트 ({len(files)}개):")
            for i, file in enumerate(files):
                print(f"{i+1}. {file['name']} (ID: {file['id']})")
            
            # 가장 최근 생성된 시트 선택
            latest_sheet = max(files, key=lambda x: x['createdTime'])
            sheet_id = latest_sheet['id']
            sheet_name = latest_sheet['name']
            print(f"✅ 최신 시트 선택: {sheet_name}")
        
        # hyjkoo17@gmail.com에게 편집 권한 부여
        print("📧 hyjkoo17@gmail.com에게 편집 권한 부여 중...")
        permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': 'hyjkoo17@gmail.com'
        }
        
        result = drive_service.permissions().create(
            fileId=sheet_id,
            body=permission,
            sendNotificationEmail=True
        ).execute()
        
        print("🎉 성공! hyjkoo17@gmail.com에게 편집 권한 부여 완료")
        print("📧 알림 이메일이 발송되었습니다")
        print(f"🔗 구글 시트 링크: https://docs.google.com/spreadsheets/d/{sheet_id}")
        
        # 환경변수 파일 생성
        with open('.env', 'w') as f:
            f.write(f"SPREADSHEET_ID={sheet_id}\n")
        print("📝 .env 파일에 SPREADSHEET_ID 저장 완료")
        
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