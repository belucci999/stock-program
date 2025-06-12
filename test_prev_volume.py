import requests
from bs4 import BeautifulSoup

def get_previous_volume(code):
    """네이버 금융 외국인 거래 페이지에서 전일거래량을 추출"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        # 외국인 거래 페이지 URL
        url = f'https://finance.naver.com/item/frgn.naver?code={code}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 거래량 데이터가 있는 테이블 찾기
        table = soup.select_one('table.type2')
        if not table:
            print("거래량 테이블을 찾을 수 없습니다.")
            return None
        
        # 데이터 행 찾기 (첫 번째 행은 현재 날짜, 두 번째 행이 전일)
        rows = table.select('tr[onmouseover]')
        if len(rows) < 2:
            print("충분한 데이터 행을 찾을 수 없습니다.")
            return None
        
        # 첫 번째 행 (오늘 데이터)
        today_row = rows[0]
        today_cells = today_row.select('td')
        if len(today_cells) < 5:
            print("오늘 행에 충분한 셀이 없습니다.")
            return None
        
        today_date = today_cells[0].text.strip()
        today_volume = today_cells[4].text.strip().replace(',', '')
        
        # 두 번째 행 (전일 데이터)
        prev_row = rows[1]
        prev_cells = prev_row.select('td')
        if len(prev_cells) < 5:
            print("전일 행에 충분한 셀이 없습니다.")
            return None
        
        prev_date = prev_cells[0].text.strip()
        prev_volume = prev_cells[4].text.strip().replace(',', '')
        
        print(f"[데이터 확인]")
        print(f"오늘 ({today_date}) 거래량: {today_volume}")
        print(f"전일 ({prev_date}) 거래량: {prev_volume}")
        
        # 추가로 여러 일자의 거래량 확인
        print("\n[최근 5일 거래량]")
        for i, row in enumerate(rows[:5]):
            cells = row.select('td')
            if len(cells) >= 5:
                date = cells[0].text.strip()
                price = cells[1].text.strip()
                change = cells[2].text.strip()
                pct = cells[3].text.strip()
                volume = cells[4].text.strip()
                print(f"{date}: 종가 {price}, 변동 {change} ({pct}), 거래량 {volume}")
        
        return {
            'today_date': today_date,
            'today_volume': today_volume,
            'prev_date': prev_date,
            'prev_volume': prev_volume
        }
        
    except Exception as e:
        print(f"오류 발생: {e}")
        return None

# 테스트
test_codes = ['005930', '000660', '051910']  # 삼성전자, SK하이닉스, LG화학

for code in test_codes:
    print(f"\n===== {code} 거래량 테스트 =====")
    get_previous_volume(code) 