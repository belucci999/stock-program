import os
from datetime import datetime
import pandas as pd
from quick_stock_check import get_stock_data, is_regular_stock  # 기존 코드 활용
from rebound_strategies_analyzer import ReboundAnalyzer
from google_sheets_uploader import GoogleSheetsUploader

def save_to_excel(results):
    """분석 결과를 엑셀 파일로 저장"""
    try:
        # 저장할 디렉토리 생성
        output_dir = 'results'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 현재 날짜로 파일명 생성
        date_str = datetime.now().strftime('%Y-%m-%d')
        excel_file = os.path.join(output_dir, f'리바운드분석_{date_str}.xlsx')
        
        # 엑셀 파일 생성
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # 전략별 시트 생성
            strategies = {
                'volume_drop': '거래량급감',
                'ma45': '45일선',
                'ma360': '360일선'
            }
            
            for key, name in strategies.items():
                if results[key]:
                    df = pd.DataFrame(results[key])
                    df.to_excel(writer, sheet_name=name, index=False)
            
            # 통합 시트 생성
            all_signals = []
            for key in strategies.keys():
                all_signals.extend(results[key])
            
            if all_signals:
                df_all = pd.DataFrame(all_signals)
                df_all.to_excel(writer, sheet_name='전체', index=False)
        
        print(f"✅ 엑셀 파일 저장 완료: {excel_file}")
        
    except Exception as e:
        print(f"❌ 엑셀 파일 저장 중 오류 발생: {str(e)}")



def main():
    print(f"🚀 일일 리바운드 분석 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    try:
        # 데이터 수집 (quick_stock_check.py의 함수 사용)
        print("1. 주가 데이터 수집 중...")
        stock_data = get_stock_data()
        
        # 리바운드 분석
        print("2. 리바운드 신호 분석 중...")
        analyzer = ReboundAnalyzer()
        for data in stock_data:
            if is_regular_stock(data['종목명']):  # 일반 주식만 분석
                analyzer.analyze_stock(data)
        results = analyzer.get_results()
        
        # 구글 시트 업로드
        print("3. 구글 시트 업로드 중...")
        uploader = GoogleSheetsUploader()
        uploader.upload_rebound_signals(results)
        
        # 엑셀 파일 저장
        print("4. 엑셀 파일 저장 중...")
        save_to_excel(results)
        
        print("✅ 분석 완료!")
        
        # 결과 요약 출력
        print("\n📊 분석 결과 요약:")
        print(f"- 거래량 급감 전략: {len(results['volume_drop'])}개 종목")
        print(f"- 45일선 전략: {len(results['ma45'])}개 종목")
        print(f"- 360일선 전략: {len(results['ma360'])}개 종목")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main() 