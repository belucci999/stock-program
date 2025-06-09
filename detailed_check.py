import pandas as pd

def detailed_check(excel_file):
    """더 자세한 데이터 확인"""
    df = pd.read_excel(excel_file)
    
    print("=== ROE 데이터 상세 확인 ===")
    print("ROE 샘플 20개:")
    print(df['ROE'].head(20).tolist())
    
    print("\n=== PER 데이터 상세 확인 ===")
    print("PER 샘플 20개:")
    print(df['PER'].head(20).tolist())
    
    print("\n=== 거래량 변화율 확인 ===")
    df['거래량변화율'] = ((df['거래량'] - df['전일거래량']) / df['전일거래량'] * 100)
    print("거래량 변화율 샘플 10개:")
    print(df['거래량변화율'].head(10).tolist())
    
    print(f"\n거래량 감소 85% 이상인 종목 수: {len(df[df['거래량변화율'] <= -85])}")
    print(f"거래량 감소 80% 이상인 종목 수: {len(df[df['거래량변화율'] <= -80])}")
    print(f"거래량 감소 70% 이상인 종목 수: {len(df[df['거래량변화율'] <= -70])}")

# 실행
import os
excel_files = [f for f in os.listdir('.') if f.startswith('stock_data_') and f.endswith('.xlsx')]
if excel_files:
    latest_file = sorted(excel_files)[-1]
    detailed_check(latest_file)
    