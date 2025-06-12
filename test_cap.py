import requests
from bs4 import BeautifulSoup
import re

def extract_market_cap(code):
    """종목의 실제 시가총액 추출"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    url = f'https://finance.naver.com/item/main.naver?code={code}'
    
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 시가총액 추출 (1)
        print("방법 1: 기본 테이블 검색")
        for table in soup.select('table'):
            for row in table.select('tr'):
                th = row.select_one('th')
                if th and '시가총액' in th.text:
                    td = row.select_one('td')
                    if td:
                        print(f"  원본: {td.text.strip()}")
        
        # 시가총액 추출 (2)
        print("\n방법 2: ID 속성 활용")
        # 종목분석에서 시가총액 데이터
        cap_elem = soup.select_one('div#time div.new_totalinfo dl.blind')
        if cap_elem:
            print("  시총 요소 발견 (div#time)")
            dts = cap_elem.select('dt')
            dds = cap_elem.select('dd')
            
            for i, (dt, dd) in enumerate(zip(dts, dds)):
                dt_text = dt.text.strip()
                dd_text = dd.text.strip()
                if '시가총액' in dt_text:
                    print(f"  시가총액 발견: {dd_text}")
                    
                    # 숫자만 추출 (억원 단위)
                    cap_match = re.search(r'(\d+)억원', dd_text)
                    if cap_match:
                        cap_value = cap_match.group(1)
                        print(f"  추출 값: {cap_value}억원")
                
        # 시가총액 추출 (3)
        print("\n방법 3: 종목 정보 테이블")
        stock_info_table = soup.select_one('div.first table.tb_type1')
        if stock_info_table:
            rows = stock_info_table.select('tr')
            for row in rows:
                th = row.select_one('th')
                if th and '시가총액' in th.text:
                    td = row.select_one('td')
                    if td:
                        cap_text = td.text.strip()
                        print(f"  원본: {cap_text}")
                        
                        # n억원 패턴
                        cap_match1 = re.search(r'([0-9,]+)억원', cap_text)
                        if cap_match1:
                            print(f"  추출 값: {cap_match1.group(1)}억원")
                            
                        # n조 m억원 패턴
                        cap_match2 = re.search(r'([0-9,]+)조\s*([0-9,]*)억원', cap_text)
                        if cap_match2:
                            trillions = int(cap_match2.group(1).replace(',', ''))
                            billions = int(cap_match2.group(2).replace(',', '') or '0')
                            total_billions = trillions * 10000 + billions
                            print(f"  변환 값: {total_billions}억원")
        
    except Exception as e:
        print(f"오류: {e}")

# 테스트
codes = ["005930", "000660", "051910"]  # 삼성전자, SK하이닉스, LG화학
for code in codes:
    print(f"\n===== {code} 시가총액 테스트 =====")
    extract_market_cap(code) 