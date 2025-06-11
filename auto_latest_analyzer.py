import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import re

def find_latest_stock_data_file():
    """가장 최신 주식 데이터 파일을 찾기"""
    
    # 패턴에 맞는 모든 파일 찾기
    patterns = [
        'full_stock_data_*.xlsx',
        'stock_data_*.xlsx', 
        '*stock_data*.xlsx'
    ]
    
    all_files = []
    for pattern in patterns:
        files = glob.glob(pattern)
        all_files.extend(files)
    
    if not all_files:
        print("❌ 주식 데이터 파일을 찾을 수 없습니다.")
        print("다음 형식의 파일이 필요합니다:")
        print("  - full_stock_data_YYYYMMDD_HHMM.xlsx")
        return None
    
    # 중복 제거
    unique_files = list(set(all_files))
    
    # 파일명에서 날짜/시간 추출하여 정렬
    file_info = []
    for file in unique_files:
        # 날짜시간 패턴 찾기 (YYYYMMDD_HHMM)
        match = re.search(r'(\d{8}_\d{4})', file)
        if match:
            datetime_str = match.group(1)
            try:
                file_datetime = datetime.strptime(datetime_str, '%Y%m%d_%H%M')
                file_info.append((file, file_datetime))
            except:
                continue
    
    if not file_info:
        print(f"❌ 날짜 형식을 인식할 수 있는 파일이 없습니다.")
        print(f"발견된 파일들: {unique_files}")
        return None
    
    # 날짜순으로 정렬 (최신이 맨 마지막)
    file_info.sort(key=lambda x: x[1])
    latest_file = file_info[-1][0]
    
    print(f"📁 최신 데이터 파일 발견: {latest_file}")
    print(f"📅 수집 시간: {file_info[-1][1].strftime('%Y년 %m월 %d일 %H시 %M분')}")
    return latest_file

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

def analyze_stocks():
    """주식 분석 실행"""
    
    print("🔍 최신 주식 데이터 파일을 찾는 중...")
    
    # 최신 파일 찾기
    latest_file = find_latest_stock_data_file()
    if not latest_file:
        return
    
    print(f"\n📊 데이터 로딩 중...")
    
    try:
        # 데이터 읽기
        df = pd.read_excel(latest_file)
        print(f"✅ 총 {len(df)}개 종목 데이터 로드 완료")
        
        # 숫자 데이터 정리
        df['PER'] = df['PER'].apply(clean_numeric_data)
        df['PBR'] = df['PBR'].apply(clean_numeric_data)
        df['ROE'] = df['ROE'].apply(clean_numeric_data)
        df['현재가'] = df['현재가'].apply(clean_numeric_data)
        df['전일종가'] = df['전일종가'].apply(clean_numeric_data)
        df['거래량'] = df['거래량'].apply(clean_numeric_data)
        df['전일거래량'] = df['전일거래량'].apply(clean_numeric_data)
        
        # 가격 변화율 계산
        df['가격변화율'] = ((df['현재가'] - df['전일종가']) / df['전일종가'] * 100).round(2)
        
        # 거래량 변화율 계산
        df['거래량변화율'] = ((df['거래량'] - df['전일거래량']) / df['전일거래량'] * 100).round(2)
        
        print(f"\n=== 1주일 상승 가능성 분석 ===")
        
        # 필터링 조건
        valid_data = df[
            (df['PER'].notna()) & 
            (df['ROE'].notna()) & 
            (df['거래량변화율'].notna()) &
            (df['가격변화율'].notna())
        ].copy()
        
        print(f"📊 분석 가능한 종목: {len(valid_data)}개")
        
        # 조건별 점수 계산
        def calculate_score(row):
            score = 0
            
            # 1. 거래량 급증 (관심도 증가)
            if row['거래량변화율'] >= 200:  # 200% 이상 급증
                score += 30
            elif row['거래량변화율'] >= 100:  # 100% 이상 급증
                score += 25
            elif row['거래량변화율'] >= 50:   # 50% 이상 급증
                score += 20
            elif row['거래량변화율'] >= 30:   # 30% 이상 급증
                score += 15
            
            # 2. 낮은 PER (저평가)
            if 0 < row['PER'] <= 5:      # 매우 저평가
                score += 25
            elif 5 < row['PER'] <= 10:   # 저평가
                score += 20
            elif 10 < row['PER'] <= 15:  # 적정
                score += 15
            elif 15 < row['PER'] <= 20:  # 보통
                score += 10
            
            # 3. 높은 ROE (수익성)
            if row['ROE'] >= 20:         # 매우 우수
                score += 25
            elif row['ROE'] >= 15:       # 우수
                score += 20
            elif row['ROE'] >= 10:       # 양호
                score += 15
            elif row['ROE'] >= 5:        # 보통
                score += 10
            elif row['ROE'] >= 3:        # 최소 수준
                score += 5
            
            # 4. 가격 조정 후 반등 (역발상)
            if -3 <= row['가격변화율'] <= -0.5:  # 소폭 하락
                score += 15
            elif -7 <= row['가격변화율'] < -3:   # 중간 하락
                score += 10
            elif 0 <= row['가격변화율'] <= 3:    # 보합~소폭상승
                score += 5
            
            return score
        
        # 점수 계산
        valid_data['투자점수'] = valid_data.apply(calculate_score, axis=1)
        
        # 점수순 정렬
        result = valid_data.sort_values('투자점수', ascending=False)
        
        # 상위 종목 출력
        print(f"\n🎯 1주일 상승 가능성 TOP 20:")
        print("=" * 100)
        
        top_stocks = result.head(20)
        for idx, row in top_stocks.iterrows():
            print(f"{row['종목명']:10} | "
                  f"점수: {row['투자점수']:2.0f} | "
                  f"가격변화: {row['가격변화율']:6.2f}% | "
                  f"거래량변화: {row['거래량변화율']:8.1f}% | "
                  f"PER: {row['PER']:6.1f} | "
                  f"ROE: {row['ROE']:6.1f}%")
        
        # 결과를 Excel로 저장
        output_filename = f"stock_analysis_result_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            # 전체 결과
            result.to_excel(writer, sheet_name='전체분석결과', index=False)
            
            # 상위 50개
            result.head(50).to_excel(writer, sheet_name='추천종목_TOP50', index=False)
            
            # 등급별 분류
            grade_a = result[result['투자점수'] >= 60]  # A등급
            grade_b = result[(result['투자점수'] >= 40) & (result['투자점수'] < 60)]  # B등급
            grade_c = result[(result['투자점수'] >= 20) & (result['투자점수'] < 40)]  # C등급
            
            if len(grade_a) > 0:
                grade_a.to_excel(writer, sheet_name='A등급_매우추천', index=False)
            if len(grade_b) > 0:
                grade_b.to_excel(writer, sheet_name='B등급_추천', index=False)
            if len(grade_c) > 0:
                grade_c.to_excel(writer, sheet_name='C등급_관심', index=False)
        
        print(f"\n💾 분석 결과가 '{output_filename}' 파일로 저장되었습니다!")
        print(f"📊 등급별 종목 수:")
        print(f"   A등급 (60점 이상): {len(grade_a)}개")
        print(f"   B등급 (40-59점): {len(grade_b)}개")
        print(f"   C등급 (20-39점): {len(grade_c)}개")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    analyze_stocks()