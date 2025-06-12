from quick_stock_check import get_individual_stock_data

# 삼성전자 종목 테스트
code = "005930"
name = "삼성전자"

print(f"===== {name}({code}) 테스트 =====")
data = get_individual_stock_data(code, name)

print(f"현재가: {data.get('현재가', 'N/A')}")
print(f"거래량: {data.get('거래량', 'N/A')}")
print(f"전일거래량: {data.get('전일거래량', 'N/A')}")
print(f"거래량증감률: {data.get('거래량증감률', 'N/A')}")
print(f"시가총액: {data.get('시가총액', 'N/A')}") 