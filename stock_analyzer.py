import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob

def clean_numeric_data(value):
    """문자열 숫자 데이터를 float로 변환"""
    if pd.isna(value) or value == '' or value == 'N/A':
        return np.nan
    
    # 문자열인 경우 처리
    if isinstance(value, str):
        # 콤마 제거
        value = value.replace(',', '')
        # 음수 처리
        if value.startswith('-'):
            try:
                return -float(value[1:])
            except:
                return np.nan
        # 양수 처리
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

def analyze_contrarian_stocks(df):
    """역발상 투자 전략 분석"""
    print("=== 역발상 투자 전략 분석 시작 ===\n")
    
    # 데이터 클리닝
    df['PER_clean'] = df['PER'].apply(clean_numeric_data)
    df['PBR_clean'] = df['PBR'].apply(clean_numeric_data)
    df['ROE_clean'] = df['ROE'].apply(clean_numeric_data)
    
    # 거래량 변화율 계산
    df['volume_change'] = df.apply(lambda x: calculate_volume_change(x['거래량'], x['전일거래량']), axis=1)
    
    # 주가 변화율 계산
    df['price_change'] = df.apply(lambda x: calculate_price_change(x['현재가'], x['전일종가']), axis=1)
    
    print("1. 전체 데이터 현황:")
    print(f"   - 총 종목 수: {len(df)}")
    print(f"   - PER 데이터 있는 종목: {df['PER_clean'].notna().sum()}")
    print(f"   - PBR 데이터 있는 종목: {df['PBR_clean'].notna().sum()}")
    print(f"   - ROE 데이터 있는 종목: {df['ROE_clean'].notna().sum()}")
    print(f"   - 거래량 변화율 계산 가능: {df['volume_change'].notna().sum()}")
    print(f"   - 주가 변화율 계산 가능: {df['price_change'].notna().sum()}\n")
    
    # 기본 통계
    print("2. 주요 지표 통계:")
    print(f"   - PER 평균: {df['PER_clean'].mean():.2f}, 중간값: {df['PER_clean'].median():.2f}")
    print(f"   - PBR 평균: {df['PBR_clean'].mean():.2f}, 중간값: {df['PBR_clean'].median():.2f}")
    print(f"   - ROE 평균: {df['ROE_clean'].mean():.2f}%, 중간값: {df['ROE_clean'].median():.2f}%")
    print(f"   - 거래량 변화율 평균: {df['volume_change'].mean():.2f}%")
    print(f"   - 주가 변화율 평균: {df['price_change'].mean():.2f}%\n")
    
    # 역발상 투자 조건 적용
    print("3. 역발상 투자 조건 분석:")
    
    # 조건 1: 거래량 85% 이상 감소 (관심도 하락)
    volume_condition = df['volume_change'] <= -85
    volume_candidates = df[volume_condition & df['volume_change'].notna()]
    print(f"   - 거래량 85% 이상 감소: {len(volume_candidates)}개 종목")
    
    # 조건 2: 주가 소폭 하락 (-7% ~ -0.3%)
    price_condition = (df['price_change'] >= -7) & (df['price_change'] <= -0.3)
    price_candidates = df[price_condition & df['price_change'].notna()]
    print(f"   - 주가 소폭 하락(-7%~-0.3%): {len(price_candidates)}개 종목")
    
    # 조건 3: PER 하위 40% (저평가)
    per_40th_percentile = df['PER_clean'].quantile(0.4)
    per_condition = (df['PER_clean'] <= per_40th_percentile) & (df['PER_clean'] > 0)
    per_candidates = df[per_condition]
    print(f"   - PER 하위 40% (PER ≤ {per_40th_percentile:.2f}): {len(per_candidates)}개 종목")
    
    # 조건 4: ROE 3% 이상 (합리적 수익성)
    roe_condition = df['ROE_clean'] >= 3
    roe_candidates = df[roe_condition]
    print(f"   - ROE 3% 이상: {len(roe_candidates)}개 종목\n")
    
    # 모든 조건 만족하는 종목
    all_conditions = volume_condition & price_condition & per_condition & roe_condition
    final_candidates = df[all_conditions & 
                         df['volume_change'].notna() & 
                         df['price_change'].notna() & 
                         df['PER_clean'].notna() & 
                         df['ROE_clean'].notna()]
    
    print("4. 최종 역발상 투자 후보:")
    print(f"   - 모든 조건 만족: {len(final_candidates)}개 종목\n")
    
    if len(final_candidates) > 0:
        print("=== 최종 투자 후보 종목 ===")
        for idx, stock in final_candidates.iterrows():
            print(f"종목명: {stock['종목명']}")
            print(f"  - 현재가: {stock['현재가']}원")
            print(f"  - 주가변화: {stock['price_change']:.2f}%")
            print(f"  - 거래량변화: {stock['volume_change']:.2f}%")
            print(f"  - PER: {stock['PER_clean']:.2f}")
            print(f"  - PBR: {stock['PBR_clean']:.2f}")
            print(f"  - ROE: {stock['ROE_clean']:.2f}%")
            print(f"  - 시장: {stock['시장구분']}")
            print()
    else:
        print("조건을 만족하는 종목이 없습니다.")
        print("\n=== 개별 조건별 우수 종목 ===")
        
        # 거래량 감소 상위 3개
        if len(volume_candidates) > 0:
            print("거래량 감소 상위 3개:")
            top_volume = volume_candidates.nsmallest(3, 'volume_change')
            for idx, stock in top_volume.iterrows():
                print(f"  {stock['종목명']}: 거래량 {stock['volume_change']:.1f}% 감소")
        
        # PER 낮은 상위 3개
        if len(per_candidates) > 0:
            print("\nPER 낮은 상위 3개:")
            top_per = per_candidates.nsmallest(3, 'PER_clean')
            for idx, stock in top_per.iterrows():
                print(f"  {stock['종목명']}: PER {stock['PER_clean']:.2f}")
        
        # ROE 높은 상위 3개
        if len(roe_candidates) > 0:
            print("\nROE 높은 상위 3개:")
            top_roe = roe_candidates.nlargest(3, 'ROE_clean')
            for idx, stock in top_roe.iterrows():
                print(f"  {stock['종목명']}: ROE {stock['ROE_clean']:.2f}%")

def main():
    print("역발상 투자 분석기 시작...\n")
    
    # 가장 최신 데이터 파일 찾기
    excel_files = glob.glob('stock_data_*.xlsx')
    if not excel_files:
        print("Excel 데이터 파일을 찾을 수 없습니다.")
        print("먼저 quick_stock_check.py를 실행하여 데이터를 수집해주세요.")
        return
    
    latest_file = max(excel_files, key=os.path.getctime)
    print(f"분석 대상 파일: {latest_file}")
    
    try:
        df = pd.read_excel(latest_file)
        print(f"데이터 로드 완료: {len(df)}개 종목\n")
        
        analyze_contrarian_stocks(df)
        
    except Exception as e:
        print(f"데이터 분석 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()