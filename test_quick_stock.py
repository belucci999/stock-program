import pandas as pd
from datetime import datetime
import time
import sys
import os

# quick_stock_check.py 모듈의 함수 import
from quick_stock_check import get_individual_stock_data, get_stock_list, GoogleSheetsUploader

def test_individual_stock():
    """개별 종목 데이터 수집 테스트"""
    print("🔍 개별 종목 데이터 수집 테스트")
    
    # 테스트할 종목 (삼성전자)
    code = "005930"
    name = "삼성전자"
    
    print(f"테스트 종목: {name} ({code})")
    
    # 데이터 수집
    start_time = time.time()
    data = get_individual_stock_data(code, name)
    end_time = time.time()
    
    print(f"소요시간: {end_time - start_time:.2f}초")
    
    # 결과 출력
    print("\n📊 수집된 데이터:")
    for key, value in data.items():
        print(f"{key}: {value}")
    
    # 중요 필드 확인
    important_fields = ['업종', '현재가', '전일종가', '거래량', '전일거래량', '거래량증감률', 'PER', 'PBR', 'ROE']
    
    print("\n🔎 중요 필드 확인:")
    for field in important_fields:
        status = "✅" if data.get(field) else "❌"
        print(f"{status} {field}: {data.get(field, '')}")
    
    return data

def test_multiple_stocks(limit=10):
    """여러 종목 데이터 수집 테스트"""
    print(f"\n🔍 다중 종목 데이터 수집 테스트 (최대 {limit}개)")
    
    # 종목 리스트 가져오기 (제한된 수만큼)
    all_stocks = get_stock_list()[:limit]
    
    if not all_stocks:
        print("❌ 종목 리스트를 가져올 수 없습니다.")
        return
    
    print(f"총 {len(all_stocks)}개 종목 테스트")
    
    # 데이터 수집
    detailed_data = []
    
    start_time = time.time()
    for i, stock in enumerate(all_stocks):
        print(f"\n[{i+1}/{len(all_stocks)}] {stock['name']} ({stock['code']}) 처리 중...")
        
        # 개별 종목 데이터 수집
        stock_data = get_individual_stock_data(stock['code'], stock['name'])
        
        # 필요한 필드 추출
        업종 = stock_data.pop('업종', '')
        현재가 = stock_data.pop('현재가', '')
        전일종가 = stock_data.pop('전일종가', '')
        거래량 = stock_data.pop('거래량', '')
        전일거래량 = stock_data.pop('전일거래량', '')
        거래량증감률 = stock_data.pop('거래량증감률', '')
        
        # 기본 정보와 상세 데이터 결합
        combined_data = {
            '종목명': stock['name'],
            '종목코드': stock['code'],
            '시장구분': stock['market'],
            '업종': 업종,
            '현재가': 현재가,
            '전일종가': 전일종가,
            '거래량': 거래량,
            '전일거래량': 전일거래량,
            '거래량증감률': 거래량증감률,
            **stock_data
        }
        
        detailed_data.append(combined_data)
        
        # 진행상황 출력
        print(f"  업종: {업종}")
        print(f"  현재가: {현재가}")
        print(f"  전일종가: {전일종가}")
        print(f"  거래량: {거래량}")
        print(f"  전일거래량: {전일거래량}")
        print(f"  거래량증감률: {거래량증감률}")
        
        time.sleep(0.3)
    
    end_time = time.time()
    print(f"\n총 소요시간: {end_time - start_time:.2f}초")
    
    # 데이터프레임 생성
    df = pd.DataFrame(detailed_data)
    
    # 컬럼 순서 정리
    column_order = [
        '종목명', '종목코드', '시장구분', '업종', '현재가', '전일종가', '거래량', '전일거래량', '거래량증감률',
        'PER', 'PBR', 'ROE', '시가총액', '거래대금',
        '매출액', '영업이익', '당기순이익', '부채비율', '유보율',
        '배당수익률', '배당금', '52주최고', '52주최저', '베타',
        '외국인비율', '기관비율'
    ]
    
    for col in column_order:
        if col not in df.columns:
            df[col] = ''
    
    df = df[column_order]
    
    # Excel 파일 저장
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    filename = f'test_result_{len(all_stocks)}_{timestamp}.xlsx'
    df.to_excel(filename, index=False)
    
    print(f"\n✅ 테스트 결과가 {filename}에 저장되었습니다.")
    
    # 데이터 품질 체크
    print("\n📊 데이터 품질:")
    quality_fields = ['업종', '현재가', '전일종가', '거래량', '전일거래량', '거래량증감률', 'PER', 'PBR', 'ROE']
    for field in quality_fields:
        non_empty = df[df[field] != ''].shape[0]
        print(f"{field} 데이터 있는 종목: {non_empty}/{len(df)}")
    
    return df

def test_google_sheets_upload(df):
    """구글 시트 업로드 테스트"""
    print("\n🔍 구글 시트 업로드 테스트")
    
    if df is None or df.empty:
        print("❌ 업로드할 데이터가 없습니다.")
        return False
    
    uploader = GoogleSheetsUploader()
    
    if uploader.gc:
        spreadsheet_name = "테스트"
        current_date = datetime.now().strftime('%Y-%m-%d')
        sheet_name = f"🧪_테스트_{current_date}_{datetime.now().strftime('%H%M')}"
        
        success = uploader.upload_dataframe(df, spreadsheet_name, sheet_name)
        
        if success:
            uploader.format_headers(success, sheet_name)
            print(f"📊 구글 시트 링크: {success}")
            print("✅ 구글 시트 업로드 테스트 완료!")
            print(f"📋 시트명: {sheet_name}")
            return True
        else:
            print("❌ 구글 시트 업로드에 실패했습니다.")
            return False
    else:
        print("❌ 구글 시트 연결에 실패했습니다.")
        return False

def main():
    """테스트 메인 함수"""
    print("=" * 50)
    print("📝 quick_stock_check.py 테스트")
    print("=" * 50)
    
    try:
        # 개별 종목 테스트
        individual_data = test_individual_stock()
        
        # 사용자 입력 요청
        test_count = input("\n테스트할 종목 수를 입력하세요 (기본값: 5): ")
        if test_count.strip() and test_count.isdigit():
            limit = int(test_count)
        else:
            limit = 5
        
        # 다중 종목 테스트
        df = test_multiple_stocks(limit)
        
        # 구글 시트 업로드 테스트 여부 확인
        test_upload = input("\n구글 시트 업로드 테스트를 진행할까요? (y/n, 기본값: n): ")
        if test_upload.lower() == 'y':
            test_google_sheets_upload(df)
        
        print("\n✅ 모든 테스트가 완료되었습니다.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")

if __name__ == "__main__":
    main() 