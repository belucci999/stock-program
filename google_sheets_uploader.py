import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from datetime import datetime
import os

class GoogleSheetsUploader:
    def __init__(self, credentials_file='credentials.json'):
        """구글 시트 업로더 초기화"""
        self.credentials_file = credentials_file
        self.gc = None
        self.setup_connection()
    
    def setup_connection(self):
        """구글 시트 연결 설정"""
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            if not os.path.exists(self.credentials_file):
                print(f"❌ {self.credentials_file} 파일이 없습니다.")
                return False
            
            creds = Credentials.from_service_account_file(self.credentials_file, scopes=scope)
            self.gc = gspread.authorize(creds)
            print("✅ 구글 시트 연결 성공!")
            return True
            
        except Exception as e:
            print(f"❌ 구글 시트 연결 실패: {str(e)}")
            return False
    
    def create_or_get_spreadsheet(self, spreadsheet_name):
        """스프레드시트 생성 또는 가져오기"""
        try:
            # 기존 스프레드시트 찾기
            try:
                spreadsheet = self.gc.open(spreadsheet_name)
                print(f"📋 기존 스프레드시트 사용: {spreadsheet_name}")
                return spreadsheet
            except gspread.SpreadsheetNotFound:
                # 새 스프레드시트 생성
                spreadsheet = self.gc.create(spreadsheet_name)
                print(f"📝 새 스프레드시트 생성: {spreadsheet_name}")
                return spreadsheet
                
        except Exception as e:
            print(f"❌ 스프레드시트 생성/접근 실패: {str(e)}")
            return None
    
    def clean_dataframe(self, df):
        """DataFrame에서 NaN 값을 처리하여 JSON 호환 가능하게 만들기"""
        # DataFrame 복사
        df_clean = df.copy()
        
        # NaN 값을 빈 문자열로 변경
        df_clean = df_clean.fillna('')
        
        # inf, -inf 값도 처리
        df_clean = df_clean.replace([np.inf, -np.inf], '')
        
        # 모든 값을 문자열로 변환하여 JSON 호환성 보장
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            # 'nan' 문자열도 빈 문자열로 변경
            df_clean[col] = df_clean[col].replace('nan', '')
        
        return df_clean
    
    def upload_dataframe(self, df, spreadsheet_name, sheet_name):
        """데이터프레임을 구글 시트에 업로드 (NaN 값 처리 포함)"""
        try:
            spreadsheet = self.create_or_get_spreadsheet(spreadsheet_name)
            if not spreadsheet:
                return False
            
            # 시트가 있는지 확인하고 없으면 생성
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                # 기존 데이터 삭제
                worksheet.clear()
            except gspread.WorksheetNotFound:
                # 새 시트 생성
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=30)
            
            # 데이터 업로드
            if len(df) > 0:
                # DataFrame 정리 (NaN 값 처리)
                df_clean = self.clean_dataframe(df)
                
                # 헤더와 데이터를 함께 업로드
                data = [df_clean.columns.tolist()] + df_clean.values.tolist()
                
                # 수정된 update 방식
                worksheet.update(values=data, range_name='A1')
                
                print(f"✅ '{sheet_name}' 시트에 {len(df)}개 행 업로드 완료!")
                return True
            else:
                print(f"⚠️ '{sheet_name}' 시트: 업로드할 데이터가 없습니다.")
                return True
                
        except Exception as e:
            print(f"❌ 시트 업로드 실패 ({sheet_name}): {str(e)}")
            return False
    
    def get_spreadsheet_url(self, spreadsheet_name):
        """스프레드시트 URL 가져오기"""
        try:
            spreadsheet = self.gc.open(spreadsheet_name)
            return spreadsheet.url
        except:
            return None

    def format_sheet_headers(self, spreadsheet_name, sheet_name):
        """시트 헤더 포맷 설정 (선택사항)"""
        try:
            spreadsheet = self.gc.open(spreadsheet_name)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            # 헤더 행 굵게 만들기
            worksheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            print(f"✅ '{sheet_name}' 시트 헤더 포맷 적용 완료!")
            return True
            
        except Exception as e:
            print(f"⚠️ 헤더 포맷 적용 실패 ({sheet_name}): {str(e)}")
            return False