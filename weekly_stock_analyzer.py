import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
from google.oauth2 import service_account
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Google Sheets 설정
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = 'credentials.json'
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

def clean_numeric_data(value):
    """문자열 숫자 데이터를 float로 변환"""
    if pd.isna(value) or value == '' or value == 'N/A':
        return np.nan
    
    if isinstance(value, str):
        value = value.replace(',', '')
        if value.startswith('-'):
            try:
                return -float(value[1:])
            except:
                return np.nan
        try:
            return float(value)
        except:
            return np.nan
    
    return float(value) if not pd.isna(value) else np.nan

def calculate_volume_change(current_volume, prev_volume):
    """거래량 변화율 계산"""
    try:
        current = clean_numeric_data(current_volume)
        prev = clean_numeric_data(prev_volume)
        
        if pd.isna(current) or pd.isna(prev) or prev == 0:
            return np.nan
        
        change_rate = ((current - prev) / prev) * 100
        return change_rate
    except:
        return np.nan

def calculate_price_change(current_price, prev_price):
    """주가 변화율 계산"""
    try:
        current = clean_numeric_data(current_price)
        prev = clean_numeric_data(prev_price)
        
        if pd.isna(current) or pd.isna(prev) or prev == 0:
            return np.nan
        
        change_rate = ((current - prev) / prev) * 100
        return change_rate
    except:
        return np.nan

def analyze_contrarian_enhanced(df):
    """Contrarian 투자 기회 분석 (전체 종목 점수순 정렬)"""
    print(f"🎯 Contrarian 분석 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 데이터 클리닝
    df['PER_clean'] = df['PER'].apply(clean_numeric_data)
    df['ROE_clean'] = df['ROE'].apply(clean_numeric_data)
    df['current_price_clean'] = df['현재가'].apply(clean_numeric_data)
    df['volume_change'] = df.apply(lambda x: calculate_volume_change(x['거래량'], x['전일거래량']), axis=1)
    df['price_change'] = df.apply(lambda x: calculate_price_change(x['현재가'], x['전일종가']), axis=1)
    
    print(f"📊 전체 분석 대상: {len(df)}개 종목")
    
    # Contrarian 점수 계산
    def calculate_contrarian_score(row):
        score = 0
        
        # 거래량 감소 점수 (최대 40점)
        if not pd.isna(row['volume_change']):
            if row['volume_change'] <= -95:
                score += 40
            elif row['volume_change'] <= -90:
                score += 35
            elif row['volume_change'] <= -85:
                score += 30
            elif row['volume_change'] <= -80:
                score += 25
            elif row['volume_change'] <= -70:
                score += 20
            elif row['volume_change'] <= -50:
                score += 15
        
        # 작은 음봉 점수 (최대 30점)
        if not pd.isna(row['price_change']):
            if -3 <= row['price_change'] <= -1:
                score += 30
            elif -5 <= row['price_change'] <= -1:
                score += 25
            elif -7 <= row['price_change'] <= -1:
                score += 20
            elif -10 <= row['price_change'] <= -1:
                score += 15
        
        # ROE 점수 (최대 20점)
        if not pd.isna(row['ROE_clean']) and row['ROE_clean'] > 0:
            if row['ROE_clean'] >= 20:
                score += 20
            elif row['ROE_clean'] >= 15:
                score += 17
            elif row['ROE_clean'] >= 10:
                score += 14
            elif row['ROE_clean'] >= 5:
                score += 11
            elif row['ROE_clean'] >= 1:
                score += 8
        
        # PER 보정 점수 (±10점)
        if not pd.isna(row['PER_clean']):
            if row['PER_clean'] < 0:
                if not pd.isna(row['ROE_clean']) and row['ROE_clean'] > 0:
                    score += 5  # 현대에이치티형
                else:
                    score -= 5
            elif 5 <= row['PER_clean'] <= 20:
                score += 10
            elif 1 <= row['PER_clean'] <= 30:
                score += 7
            elif row['PER_clean'] > 50:
                score -= 5
        
        return score
    
    # 전체 종목에 점수 계산
    df['contrarian_score'] = df.apply(calculate_contrarian_score, axis=1)
    
    # 등급 분류
    def classify_grade(row):
        if (row['volume_change'] <= -85 and 
            -7 <= row['price_change'] <= -1 and 
            row['ROE_clean'] > 0 and 
            row['contrarian_score'] >= 50):
            return "🏆S"
        elif (row['volume_change'] <= -70 and 
              row['price_change'] <= -1 and 
              row['ROE_clean'] > 0 and 
              row['contrarian_score'] >= 30):
            return "🥈A"
        elif (row['volume_change'] <= -50 and 
              row['price_change'] <= 0 and 
              row['ROE_clean'] > 0 and 
              row['contrarian_score'] >= 15):
            return "🥉B"
        else:
            return "C"
    
    df['grade'] = df.apply(classify_grade, axis=1)
    
    # 통계 출력
    grade_counts = df['grade'].value_counts()
    roe_positive_per_negative = ((df['ROE_clean'] > 0) & (df['PER_clean'] < 0)).sum()
    
    print(f"✅ 등급별 분포:")
    for grade in ["🏆S", "🥈A", "🥉B", "C"]:
        count = grade_counts.get(grade, 0)
        print(f"   {grade}등급: {count}개")
    print(f"   현대에이치티형: {roe_positive_per_negative}개")
    
    return df

def upload_to_google_sheets(df):
    """구글 시트에 데이터 업로드"""
    if not SPREADSHEET_ID:
        print("⚠️  SPREADSHEET_ID가 설정되지 않아 구글 시트 업로드를 건너뜁니다.")
        return False
        
    try:
        print("📊 구글 시트 업로드 시작...")
        
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)
        
        # 현재 날짜로 시트 이름 생성
        date_str = datetime.now().strftime('%Y-%m-%d')
        main_sheet_name = f"{date_str}_Contrarian전체"
        s_sheet_name = f"{date_str}_S등급"
        a_sheet_name = f"{date_str}_A등급"
        hyundai_sheet_name = f"{date_str}_현대에이치티형"
        
        # 1. 전체 종목 데이터 준비
        df_main = df[[
            'grade', '종목명', '종목코드', '시장구분', '현재가', '전일종가',
            'volume_change', 'price_change', 'ROE_clean', 'PER_clean',
            'contrarian_score'
        ]].copy()
        
        df_main.columns = [
            '등급', '종목명', '종목코드', '시장구분', '현재가', '전일종가',
            '거래량변화(%)', '주가변화(%)', 'ROE(%)', 'PER', 'Contrarian점수'
        ]
        
        df_main = df_main.sort_values('Contrarian점수', ascending=False)
        
        # 숫자 데이터 반올림 및 NaN 처리
        for col in ['거래량변화(%)', '주가변화(%)', 'ROE(%)', 'PER', 'Contrarian점수']:
            if col in df_main.columns:
                df_main[col] = df_main[col].round(2)
        
        df_main = df_main.fillna('')
        
        # 새 시트들 생성
        requests = [{'addSheet': {'properties': {'title': main_sheet_name}}}]
        
        # S등급 데이터가 있으면 시트 추가
        s_grade = df[df['grade'] == "🏆S"]
        if len(s_grade) > 0:
            requests.append({'addSheet': {'properties': {'title': s_sheet_name}}})
        
        # A등급 데이터가 있으면 시트 추가
        a_grade = df[df['grade'] == "🥈A"]
        if len(a_grade) > 0:
            requests.append({'addSheet': {'properties': {'title': a_sheet_name}}})
        
        # 현대에이치티형 데이터가 있으면 시트 추가
        hyundai_type = df[(df['ROE_clean'] > 0) & (df['PER_clean'] < 0)]
        if len(hyundai_type) > 0:
            requests.append({'addSheet': {'properties': {'title': hyundai_sheet_name}}})
        
        # 시트들 생성
        try:
            body = {'requests': requests}
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body=body).execute()
            print("✅ 새 시트들 생성 완료")
        except Exception as e:
            print(f"⚠️  시트 생성 오류 (이미 존재할 수 있음): {str(e)}")
        
        # 메인 데이터 업로드
        values_main = [df_main.columns.tolist()] + df_main.values.tolist()
        body_main = {'values': values_main}
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{main_sheet_name}!A1',
            valueInputOption='RAW',
            body=body_main
        ).execute()
        print(f"✅ 메인 데이터 업로드 완료: {len(df_main)}개 종목")
        
        # S등급 업로드
        if len(s_grade) > 0:
            s_data = s_grade[['종목명', '종목코드', '시장구분', '현재가', 'volume_change', 'price_change', 'ROE_clean', 'PER_clean', 'contrarian_score']].copy()
            s_data.columns = ['종목명', '종목코드', '시장구분', '현재가', '거래량변화(%)', '주가변화(%)', 'ROE(%)', 'PER', 'Contrarian점수']
            s_data = s_data.fillna('')
            values_s = [s_data.columns.tolist()] + s_data.values.tolist()
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{s_sheet_name}!A1',
                valueInputOption='RAW',
                body={'values': values_s}
            ).execute()
            print(f"✅ S등급 업로드 완료: {len(s_grade)}개")
        
        # A등급 업로드
        if len(a_grade) > 0:
            a_data = a_grade[['종목명', '종목코드', '시장구분', '현재가', 'volume_change', 'price_change', 'ROE_clean', 'PER_clean', 'contrarian_score']].copy()
            a_data.columns = ['종목명', '종목코드', '시장구분', '현재가', '거래량변화(%)', '주가변화(%)', 'ROE(%)', 'PER', 'Contrarian점수']
            a_data = a_data.fillna('')
            values_a = [a_data.columns.tolist()] + a_data.values.tolist()
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{a_sheet_name}!A1',
                valueInputOption='RAW',
                body={'values': values_a}
            ).execute()
            print(f"✅ A등급 업로드 완료: {len(a_grade)}개")
        
        # 현대에이치티형 업로드
        if len(hyundai_type) > 0:
            hyundai_data = hyundai_type[['grade', '종목명', '종목코드', '시장구분', '현재가', 'volume_change', 'price_change', 'ROE_clean', 'PER_clean', 'contrarian_score']].copy()
            hyundai_data.columns = ['등급', '종목명', '종목코드', '시장구분', '현재가', '거래량변화(%)', '주가변화(%)', 'ROE(%)', 'PER', 'Contrarian점수']
            hyundai_data = hyundai_data.fillna('')
            values_hyundai = [hyundai_data.columns.tolist()] + hyundai_data.values.tolist()
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{hyundai_sheet_name}!A1',
                valueInputOption='RAW',
                body={'values': values_hyundai}
            ).execute()
            print(f"✅ 현대에이치티형 업로드 완료: {len(hyundai_type)}개")
        
        print(f"🎉 구글 시트 업로드 완료!")
        print(f"📊 메인 시트: {main_sheet_name}")
        return True
        
    except Exception as e:
        print(f"❌ 구글 시트 업로드 오류: {str(e)}")
        return False

def find_latest_stock_data():
    """가장 최신 주식 데이터 파일 찾기"""
    patterns = ["full_stock_data*.xlsx", "*.xlsx"]
    
    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            latest_file = max(files, key=os.path.getctime)
            return latest_file
    
    return None

def main():
    """메인 실행 함수"""
    try:
        print("=" * 60)
        print(f"🚀 일일 Contrarian 분석 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # 1. 최신 데이터 파일 찾기
        latest_file = find_latest_stock_data()
        
        if not latest_file:
            print("❌ 주식 데이터 파일을 찾을 수 없습니다.")
            return
        
        print(f"✅ 데이터 파일: {latest_file}")
        
        # 2. 데이터 로드
        df = pd.read_excel(latest_file)
        print(f"✅ 데이터 로드 완료: {len(df)}개 종목")
        
        # 3. Contrarian 분석
        analyzed_df = analyze_contrarian_enhanced(df)
        
        # 4. 로컬 Excel 저장
        current_time = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f'contrarian_analysis_{current_time}.xlsx'
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # 전체 종목 점수순 정렬
            df_main = analyzed_df[[
                'grade', '종목명', '종목코드', '시장구분', '현재가', '전일종가',
                'volume_change', 'price_change', 'ROE_clean', 'PER_clean',
                'contrarian_score'
            ]].copy()
            
            df_main.columns = [
                '등급', '종목명', '종목코드', '시장구분', '현재가', '전일종가',
                '거래량변화(%)', '주가변화(%)', 'ROE(%)', 'PER', 'Contrarian점수'
            ]
            
            df_main = df_main.sort_values('Contrarian점수', ascending=False)
            
            # 숫자 데이터 반올림
            for col in ['거래량변화(%)', '주가변화(%)', 'ROE(%)', 'PER', 'Contrarian점수']:
                if col in df_main.columns:
                    df_main[col] = df_main[col].round(2)
            
            df_main.to_excel(writer, sheet_name='전체종목_점수순', index=False)
            
            # S등급, A등급 시트 추가
            s_grade = analyzed_df[analyzed_df['grade'] == "🏆S"]
            if len(s_grade) > 0:
                s_grade.to_excel(writer, sheet_name='S등급', index=False)
                
            a_grade = analyzed_df[analyzed_df['grade'] == "🥈A"]
            if len(a_grade) > 0:
                a_grade.to_excel(writer, sheet_name='A등급', index=False)
        
        print(f"📁 로컬 Excel 저장: {filename}")
        
        # 5. 구글 시트 업로드
        upload_to_google_sheets(analyzed_df)
        
        print("🎉 일일 Contrarian 분석 완료!")
        print(f"⏰ 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ 분석 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()