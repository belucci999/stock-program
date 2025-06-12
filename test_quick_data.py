from quick_stock_check import get_individual_stock_data

# 삼성전자 종목 데이터 테스트
code = "005930"
name = "삼성전자"

print(f"✅ {name}({code}) 데이터 추출 테스트")
data = get_individual_stock_data(code, name)

print("\n--- 추출된 데이터 ---")
print(f"현재가: {data.get('현재가', 'N/A')}")
print(f"거래량: {data.get('거래량', 'N/A')}")
print(f"전일거래량: {data.get('전일거래량', 'N/A')}")

# 거래량 증감률 계산
try:
    curr_vol = int(data.get('거래량', '0'))
    prev_vol = int(data.get('전일거래량', '0'))
    if prev_vol > 0:
        volume_change_pct = ((curr_vol - prev_vol) / prev_vol) * 100
        print(f"거래량증감률: {volume_change_pct:.2f}%")
    else:
        print("거래량증감률: 계산 불가 (전일거래량 0)")
except Exception as e:
    print(f"거래량증감률 계산 오류: {e}")

print("\n--- 기타 재무 데이터 ---")
fields = ['PER', 'PBR', 'ROE', '시가총액', '매출액', '영업이익', '당기순이익']
for field in fields:
    print(f"{field}: {data.get(field, 'N/A')}") 