import requests
from bs4 import BeautifulSoup
import re
from quick_stock_check import get_stock_volumes

def get_main_page_volume(code):
    """메인 페이지에서 거래량 추출 시도"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f'https://finance.naver.com/item/main.naver?code={code}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 모든 td 요소 가져오기
        td_elements = soup.select('table.no_info td')
        
        # 현재 거래량
        current_volume = None
        if len(td_elements) > 2:
            volume_elem = td_elements[2].select_one('span.blind')
            if volume_elem:
                current_volume = volume_elem.text.replace(',', '')
        
        # 전일 거래량
        previous_volume = None
        if len(td_elements) > 10:
            prev_volume_elem = td_elements[10].select_one('span.blind')
            if prev_volume_elem:
                previous_volume = prev_volume_elem.text.replace(',', '')
        
        return {
            'current_volume': current_volume,
            'previous_volume': previous_volume
        }
    except Exception as e:
        print(f"메인 페이지 거래량 추출 오류: {e}")
        return None

def get_daily_volume(code):
    """일별 시세 페이지에서 거래량 추출 시도"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f'https://finance.naver.com/item/sise_day.naver?code={code}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 데이터 행 찾기
        rows = soup.select('table.type2 tr[onmouseover]')
        
        if len(rows) < 2:
            return None
        
        # 첫 번째와 두 번째 행에서 거래량 추출
        volumes = []
        for i, row in enumerate(rows[:2]):
            cells = row.select('td')
            if len(cells) >= 6:
                date = cells[0].text.strip()
                volume = cells[5].text.strip().replace(',', '')
                volumes.append({'date': date, 'volume': volume})
        
        if len(volumes) >= 2:
            return {
                'current_volume': volumes[0]['volume'],
                'previous_volume': volumes[1]['volume']
            }
        
        return None
    except Exception as e:
        print(f"일별 시세 거래량 추출 오류: {e}")
        return None

# 테스트할 종목 목록
test_codes = ['005930', '000220', '001290', '001470', '002795']

for code in test_codes:
    print(f"\n===== {code} 거래량 정보 =====")
    
    # 1. 외국인 거래 페이지 (현재 quick_stock_check.py에서 사용 중)
    print("[방법 1] 외국인 거래 페이지")
    frgn_volumes = get_stock_volumes(code)
    if frgn_volumes:
        print(f"  현재 거래량: {frgn_volumes.get('current_volume', 'N/A')}")
        print(f"  전일 거래량: {frgn_volumes.get('prev_volume', 'N/A')}")
    else:
        print("  데이터 추출 실패")
    
    # 2. 메인 페이지 방법 (백업 방법)
    print("\n[방법 2] 메인 페이지")
    main_volumes = get_main_page_volume(code)
    if main_volumes:
        print(f"  현재 거래량: {main_volumes.get('current_volume', 'N/A')}")
        print(f"  전일 거래량: {main_volumes.get('previous_volume', 'N/A')}")
    else:
        print("  데이터 추출 실패")
    
    # 3. 일별 시세 페이지
    print("\n[방법 3] 일별 시세 페이지")
    daily_volumes = get_daily_volume(code)
    if daily_volumes:
        print(f"  현재 거래량: {daily_volumes.get('current_volume', 'N/A')}")
        print(f"  전일 거래량: {daily_volumes.get('previous_volume', 'N/A')}")
    else:
        print("  데이터 추출 실패") 