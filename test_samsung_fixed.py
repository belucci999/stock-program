from quick_stock_check import get_individual_stock_data

# 삼성전자 종목 테스트
code = "005930"
name = "삼성전자"

print(f"===== {name}({code}) 상세 정보 테스트 =====")
data = get_individual_stock_data(code, name)

# 모든 수집된 데이터 출력
print("\n[수집된 모든 데이터]")
for key, value in data.items():
    print(f"{key}: {value}")

# 특히 영업이익과 당기순이익 확인
print("\n[중요 재무 데이터]")
print(f"현재가: {data.get('현재가', 'N/A')}")
print(f"영업이익: {data.get('영업이익', 'N/A')}")
print(f"당기순이익: {data.get('당기순이익', 'N/A')}")
print(f"시가총액: {data.get('시가총액', 'N/A')}")
print(f"거래량: {data.get('거래량', 'N/A')}")
print(f"전일거래량: {data.get('전일거래량', 'N/A')}") 