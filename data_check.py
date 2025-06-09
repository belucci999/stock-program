import pandas as pd
import numpy as np

def check_data(excel_file):
    """데이터 상태 확인"""
    print(f"=== {excel_file} 데이터 확인 ===")
    
    df = pd.read_excel(excel_file)
    print(f"전체 행 수: {len(df)}")
    print(f"컬럼 목록: {list(df.columns)}")
    print("\n첫 5행 데이터:")
    print(df.head())
    
    print("\n각 컬럼별 데이터 타입:")
    print(df.dtypes)
    
    print("\n각 컬럼별 null/빈값 개수:")
    for col in df.columns:
        null_count = df[col].isnull().sum()
        empty_count = (df[col] == '').sum()
        print(f"{col}: null {null_count}개, 빈값 {empty_count}개")
    
    print("\nPER 컬럼 샘플:")
    print(df['PER'].head(10))
    
    print("\n거래량 컬럼 샘플:")
    print(df['거래량'].head(10))

# 최근 파일 확인
import os
excel_files = [f for f in os.listdir('.') if f.startswith('stock_data_') and f.endswith('.xlsx')]
if excel_files:
    latest_file = sorted(excel_files)[-1]
    check_data(latest_file)
else:
    print("분석할 파일이 없습니다.")
