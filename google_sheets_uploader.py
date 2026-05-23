import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from datetime import datetime
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv

from credentials_path import resolve_credentials_path

# Load environment variables
load_dotenv()

# 업로드 시 숫자로 유지할 컬럼 (서식 API 호출 없음, 값만 숫자 타입으로 전달)
NUMERIC_COLUMNS = frozenset({
    '현재가', '전일종가', '거래량', '전일거래량', '거래대금', '전일거래대금',
    '시가총액', '매출액', '영업이익', '당기순이익', '52주최고', '52주최저', '배당금',
    'PER', 'PBR', 'ROE', '부채비율', '유보율', '배당수익률', '영업이익률', '순이익률',
    '거래량증감율', '거래대금증감율', '가격변화율', '거래량변화율', '외국인비율', '기관비율', '베타', '투자점수',
})


class GoogleSheetsUploader:
    def __init__(self):
        # 환경 변수 로드
        load_dotenv()
        
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        self.CREDENTIALS_FILE = resolve_credentials_path()
        self.SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
        
        if not os.path.exists(self.CREDENTIALS_FILE):
            print(f"[경고] {self.CREDENTIALS_FILE} 파일이 없습니다.")
            return

        if not self.SPREADSHEET_ID:
            print("[경고] SPREADSHEET_ID가 설정되지 않았습니다. (.env 파일 확인)")
            return

        # Google Sheets API + gspread 연결
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                self.CREDENTIALS_FILE, scopes=self.SCOPES)
            self.service = build('sheets', 'v4', credentials=self.credentials)
            self.setup_connection()
            print("[OK] 구글 시트 API 인증 성공")
        except Exception as e:
            print(f"[오류] 구글 시트 API 인증 실패: {str(e)}")
            self.service = None
            self.gc = None
    
    def create_sheet(self, sheet_name):
        """새로운 시트 생성"""
        try:
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': sheet_name
                        }
                    }
                }]
            }
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.SPREADSHEET_ID,
                body=body
            ).execute()
            print(f"시트 생성 완료: {sheet_name}")
            
        except Exception as e:
            if "already exists" not in str(e):
                print(f"시트 생성 중 오류 발생: {str(e)}")
    
    def update_sheet(self, data, sheet_name, include_header=True):
        """시트 데이터 업데이트"""
        try:
            if isinstance(data, list):
                if not data:  # 빈 리스트 체크
                    print(f"데이터가 없습니다: {sheet_name}")
                    return
                    
                # 리스트를 데이터프레임으로 변환
                df = pd.DataFrame(data)
            else:
                df = data
            
            # 데이터 준비
            if include_header:
                values = [df.columns.tolist()] + df.values.tolist()
            else:
                values = df.values.tolist()
            
            body = {
                'values': values
            }
            
            # 데이터 업데이트
            self.service.spreadsheets().values().update(
                spreadsheetId=self.SPREADSHEET_ID,
                range=f'{sheet_name}!A1',
                valueInputOption='RAW',
                body=body
            ).execute()
            
            print(f"데이터 업데이트 완료: {sheet_name}")
            
        except Exception as e:
            print(f"데이터 업데이트 중 오류 발생: {str(e)}")
    
    def upload_rebound_signals(self, results):
        """리바운드 신호 업로드"""
        try:
            # 현재 날짜로 시트 이름 생성
            date_str = datetime.now().strftime('%Y-%m-%d')
            
            # 전략별 시트 생성 및 업데이트
            strategies = {
                'volume_drop': '거래량급감',
                'ma45': '45일선',
                'ma360': '360일선'
            }
            
            for key, name in strategies.items():
                sheet_name = f"{date_str}_{name}"
                self.create_sheet(sheet_name)
                
                if results[key]:
                    df = pd.DataFrame(results[key])
                    self.update_sheet(df, sheet_name)
                else:
                    print(f"신호 없음: {name}")
            
            # 통합 시트 업데이트
            all_signals = []
            for key in strategies.keys():
                all_signals.extend(results[key])
            
            if all_signals:
                sheet_name = f"{date_str}_전체"
                self.create_sheet(sheet_name)
                df_all = pd.DataFrame(all_signals)
                self.update_sheet(df_all, sheet_name)
            
            print("리바운드 신호 업로드 완료")
            
        except Exception as e:
            print(f"리바운드 신호 업로드 중 오류 발생: {str(e)}")

    def setup_connection(self):
        """구글 시트 연결 설정"""
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            if not os.path.exists(self.CREDENTIALS_FILE):
                print(f"[오류] {self.CREDENTIALS_FILE} 파일이 없습니다.")
                return False
            
            creds = Credentials.from_service_account_file(self.CREDENTIALS_FILE, scopes=scope)
            self.gc = gspread.authorize(creds)
            print("[OK] 구글 시트 연결 성공")
            return True
            
        except Exception as e:
            print(f"[오류] 구글 시트 연결 실패: {str(e)}")
            return False
    
    def create_or_get_spreadsheet(self, spreadsheet_name=None):
        """스프레드시트 열기 (.env SPREADSHEET_ID 우선)"""
        if not getattr(self, "gc", None):
            if not self.setup_connection():
                return None

        try:
            if self.SPREADSHEET_ID:
                spreadsheet = self.gc.open_by_key(self.SPREADSHEET_ID)
                print(f"[연결] 스프레드시트: {spreadsheet.title}")
                return spreadsheet

            if not spreadsheet_name:
                print("[오류] SPREADSHEET_ID 또는 스프레드시트 이름이 필요합니다.")
                return None

            try:
                spreadsheet = self.gc.open(spreadsheet_name)
                print(f"[연결] 기존 스프레드시트: {spreadsheet_name}")
                return spreadsheet
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.gc.create(spreadsheet_name)
                print(f"[생성] 새 스프레드시트: {spreadsheet_name}")
                return spreadsheet

        except Exception as e:
            print(f"[오류] 스프레드시트 접근 실패: {str(e)}")
            print("       시트를 서비스 계정 이메일(client_email)과 편집자로 공유했는지 확인하세요.")
            return None

    def get_or_create_worksheet(self, sheet_name, rows=8000, cols=40):
        """날짜 탭을 가져오거나 생성. 이미 있으면 내용을 비워 같은 날 재실행 시 덮어씀."""
        spreadsheet = self.create_or_get_spreadsheet()
        if not spreadsheet:
            return None

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()
            print(f"[덮어쓰기] 탭 '{sheet_name}' 기존 내용 삭제 후 갱신")
            return worksheet
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=rows, cols=cols)
            print(f"[생성] 탭 '{sheet_name}' 새로 추가")
            return worksheet

    def upload_sections_to_daily_tab(self, date_tab_name, sections):
        """
        하루치 결과를 하나의 탭에 섹션별로 업로드.
        sections: [(섹션제목, DataFrame 또는 None), ...]
        """
        try:
            worksheet = self.get_or_create_worksheet(date_tab_name)
            if not worksheet:
                return False

            rows = []
            for section_title, df in sections:
                rows.append([section_title])
                rows.append([])
                if df is not None and len(df) > 0:
                    df_upload = self.prepare_dataframe_for_upload(df)
                    rows.append(df_upload.columns.tolist())
                    for row in df_upload.itertuples(index=False, name=None):
                        rows.append(list(row))
                else:
                    rows.append(["(데이터 없음)"])
                rows.append([])
                rows.append([])

            if rows:
                worksheet.update(
                    values=rows,
                    range_name="A1",
                    value_input_option="USER_ENTERED",
                )

            section_count = sum(1 for _, df in sections if df is not None and len(df) > 0)
            print(f"[OK] 탭 '{date_tab_name}' 업로드 완료 (섹션 {section_count}개)")
            return True

        except Exception as e:
            print(f"[오류] 탭 '{date_tab_name}' 업로드 실패: {str(e)}")
            return False

    def prepare_dataframe_for_upload(self, df):
        """업로드용 DataFrame: NaN 처리, 숫자 컬럼은 숫자 값으로 전달 (서식 API 미사용)."""
        out = df.copy()
        out = out.replace([np.inf, -np.inf], np.nan)

        for col in out.columns:
            if col in NUMERIC_COLUMNS:
                out[col] = pd.to_numeric(
                    out[col].astype(str).str.replace(',', '', regex=False),
                    errors='coerce',
                )

        out = out.where(pd.notna(out), '')
        return out

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
                worksheet.update(
                    values=data,
                    range_name='A1',
                    value_input_option='USER_ENTERED',
                )
                
                print(f"[OK] '{sheet_name}' 시트에 {len(df)}개 행 업로드 완료")
                return True
            else:
                print(f"[안내] '{sheet_name}' 시트: 업로드할 데이터가 없습니다.")
                return True
                
        except Exception as e:
            print(f"[오류] 시트 업로드 실패 ({sheet_name}): {str(e)}")
            return False
    
    def get_spreadsheet_url(self, spreadsheet_name=None):
        """스프레드시트 URL 가져오기"""
        try:
            spreadsheet = self.create_or_get_spreadsheet(spreadsheet_name)
            return spreadsheet.url if spreadsheet else None
        except Exception:
            return None

    def format_sheet_headers(self, spreadsheet_name, sheet_name):
        """시트 헤더 포맷 설정 (선택사항)"""
        try:
            spreadsheet = self.create_or_get_spreadsheet(spreadsheet_name)
            if not spreadsheet:
                return False
            worksheet = spreadsheet.worksheet(sheet_name)
            
            # 헤더 행 굵게 만들기
            worksheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            print(f"[OK] '{sheet_name}' 헤더 포맷 적용")
            return True
            
        except Exception as e:
            print(f"[경고] 헤더 포맷 적용 실패 ({sheet_name}): {str(e)}")
            return False