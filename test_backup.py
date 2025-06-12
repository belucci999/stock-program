from quick_stock_check import get_individual_stock_data

# 인덱스 오류가 발생했던 종목들
error_codes = ['000220', '001290', '001470', '002795']

for code in error_codes:
    print(f"\n===== {code} 테스트 =====")
    data = get_individual_stock_data(code, f"테스트_{code}")
    
    print(f"현재가: {data.get('현재가', 'N/A')}")
    print(f"거래량: {data.get('거래량', 'N/A')}")
    print(f"전일거래량: {data.get('전일거래량', 'N/A')}")
    print(f"거래량증감률: {data.get('거래량증감률', 'N/A')}")
    print(f"시가총액: {data.get('시가총액', 'N/A')}") 