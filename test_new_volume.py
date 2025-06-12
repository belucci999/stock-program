import requests
from bs4 import BeautifulSoup
import re

def get_stock_volumes(code):
    """네이버 금융 외국인 거래 페이지에서 거래량 정보 추출"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        # 외국인 거래 페이지 URL
        url = f'https://finance.naver.com/item/frgn.naver?code={code}'
        response = requests.get(url, headers=headers)
        
        # EUC-KR 인코딩 처리
        response.encoding = 'euc-kr'  
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 거래량 데이터가 있는 테이블 찾기 (4번째 테이블, 인덱스 3)
        tables = soup.select('table')
        if len(tables) < 4:
            print("필요한 테이블을 찾을 수 없습니다.")
            return None
        
        volume_table = tables[3]  # 4번째 테이블 (인덱스 3)
        
        # 데이터 행 찾기
        rows = volume_table.select('tr:not(:first-child)')  # 헤더 행 제외
        
        # 빈 행이나 데이터가 없는 행 필터링
        data_rows = []
        for row in rows:
            cells = row.select('td')
            if len(cells) > 4:  # 최소 5개의 셀이 있어야 유효한 데이터 행
                # 첫 번째 셀(날짜)이 비어있지 않은지 확인
                date_text = cells[0].text.strip()
                if date_text and re.match(r'\d{4}\.\d{2}\.\d{2}', date_text):
                    data_rows.append(cells)
        
        # 충분한 데이터 행이 있는지 확인
        if len(data_rows) < 2:
            print(f"충분한 데이터 행을 찾을 수 없습니다. 찾은 행 수: {len(data_rows)}")
            return None
        
        # 최근 5일간의 거래량 데이터 추출
        volumes_data = []
        for i, cells in enumerate(data_rows[:5]):
            date = cells[0].text.strip()
            price = cells[1].text.strip()
            change = re.sub(r'\s+', ' ', cells[2].text.strip())
            change_pct = cells[3].text.strip()
            volume = cells[4].text.strip().replace(',', '')
            
            volumes_data.append({
                'date': date,
                'price': price,
                'change': change,
                'change_pct': change_pct,
                'volume': volume
            })
            
        # 결과 출력
        print(f"\n[{code} 최근 거래량 정보]")
        for data in volumes_data:
            print(f"{data['date']}: 종가 {data['price']}, 변동 {data['change']} ({data['change_pct']}), 거래량 {data['volume']}")
        
        # 오늘과 전일 거래량 반환
        if len(volumes_data) >= 2:
            return {
                'current_date': volumes_data[0]['date'],
                'current_volume': volumes_data[0]['volume'],
                'prev_date': volumes_data[1]['date'],
                'prev_volume': volumes_data[1]['volume']
            }
        else:
            return None
        
    except Exception as e:
        print(f"오류 발생: {e}")
        return None

# 테스트
test_codes = ['005930', '000660', '051910']  # 삼성전자, SK하이닉스, LG화학

for code in test_codes:
    print(f"\n===== {code} 거래량 테스트 =====")
    result = get_stock_volumes(code)
    if result:
        print(f"\n[결과 요약]")
        print(f"현재 날짜: {result['current_date']}")
        print(f"현재 거래량: {result['current_volume']}")
        print(f"전일 날짜: {result['prev_date']}")
        print(f"전일 거래량: {result['prev_volume']}")
        
        # 거래량 증감률 계산
        try:
            curr_vol = int(result['current_volume'])
            prev_vol = int(result['prev_volume'])
            if prev_vol > 0:
                volume_change_pct = ((curr_vol - prev_vol) / prev_vol) * 100
                print(f"거래량 증감률: {volume_change_pct:.2f}%")
        except:
            pass 