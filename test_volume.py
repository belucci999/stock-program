import requests
from bs4 import BeautifulSoup

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
url = 'https://finance.naver.com/item/main.naver?code=005930'
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

# 현재가 추출 테스트
current_price = soup.select_one('div.rate_info div.today span.blind')
print(f'현재가: {current_price.text if current_price else None}')

# 전체 거래량 관련 요소 분석
print("\n--- 거래량 관련 분석 ---")
tds = soup.select('div.rate_info table.no_info td')
print(f'전체 TD 개수: {len(tds)}')

for i, td in enumerate(tds):
    blind = td.select_one('span.blind')
    print(f'TD {i}: {blind.text if blind else None}')

# 거래량 관련 모든 span.blind 검색
print("\n--- 모든 span.blind 분석 ---")
all_blinds = soup.select('table.no_info span.blind')
for i, blind in enumerate(all_blinds):
    print(f'Blind {i}: {blind.text}')

# 다른 방식으로 거래량 추출 시도
print("\n--- 다른 방식으로 거래량 추출 ---")
try:
    # 거래량은 일반적으로 3번째 TD에 있을 수 있음
    volume_elem = soup.select('table.no_info td')[1].select_one('span.blind')
    print(f'추정 거래량: {volume_elem.text if volume_elem else None}')
except Exception as e:
    print(f'오류: {e}')

# 다른 방식으로 전일 거래량 추출 시도
try:
    prev_volume_elem = soup.select('table.no_info td')[2].select_one('span.blind')
    print(f'추정 전일 거래량: {prev_volume_elem.text if prev_volume_elem else None}')
except Exception as e:
    print(f'오류: {e}') 