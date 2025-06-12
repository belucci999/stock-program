import requests
from bs4 import BeautifulSoup
import re

def extract_financial_value(value):
    """재무 데이터에서 숫자 추출 (마이너스 처리 포함)"""
    if not value or value.strip() == '':
        return ''
    
    value = value.strip()
    
    # 1. 괄호로 둘러싸인 경우 (마이너스 의미)
    bracket_match = re.search(r'\(([\d,\.]+)\)', value)
    if bracket_match:
        number = bracket_match.group(1).replace(',', '')
        return f'-{number}' if number else ''
    
    # 2. 이미 마이너스 기호가 있는 경우
    minus_match = re.search(r'-\s*([\d,\.]+)', value)
    if minus_match:
        number = minus_match.group(1).replace(',', '')
        return f'-{number}' if number else ''
    
    # 3. 양수인 경우
    plus_match = re.search(r'([\d,\.]+)', value)
    if plus_match:
        number = plus_match.group(1).replace(',', '')
        return number if number else ''
    
    return ''

def check_samsung_profit_data():
    """삼성전자 영업이익 및 당기순이익 데이터 확인"""
    code = "005930"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    url = f'https://finance.naver.com/item/main.naver?code={code}'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 재무지표 영역의 모든 테이블 검색
    print("삼성전자(005930) 재무지표 확인")
    print("=" * 50)
    
    # 모든 테이블 검색
    tables = soup.select('table')
    
    # 영업이익 데이터 추적
    found_tables = 0
    for i, table in enumerate(tables):
        profit_data = []
        net_income_data = []
        
        rows = table.select('tr')
        for row in rows:
            cells = row.select('th, td')
            if len(cells) >= 2:
                header = cells[0].get_text(strip=True)
                
                if '영업이익' in header:
                    print(f"\n테이블 {i+1} - 영업이익 행 발견")
                    print(f"헤더: {header}")
                    
                    # 모든 값들 출력
                    values = [cell.get_text(strip=True) for cell in cells[1:]]
                    raw_values = values
                    print(f"원본 값들: {raw_values}")
                    
                    # 값 추출 결과
                    extracted_values = [extract_financial_value(val) for val in values]
                    print(f"추출된 값들: {extracted_values}")
                    
                    profit_data = extracted_values
                    found_tables += 1
                
                if '당기순이익' in header:
                    print(f"\n테이블 {i+1} - 당기순이익 행 발견")
                    print(f"헤더: {header}")
                    
                    # 모든 값들 출력
                    values = [cell.get_text(strip=True) for cell in cells[1:]]
                    raw_values = values
                    print(f"원본 값들: {raw_values}")
                    
                    # 값 추출 결과
                    extracted_values = [extract_financial_value(val) for val in values]
                    print(f"추출된 값들: {extracted_values}")
                    
                    net_income_data = extracted_values
        
        # 이 테이블에서 영업이익과 당기순이익 데이터를 모두 찾았으면 비교
        if profit_data and net_income_data:
            print("\n영업이익과 당기순이익 비교:")
            print(f"영업이익: {profit_data}")
            print(f"당기순이익: {net_income_data}")
    
    if found_tables == 0:
        print("영업이익 데이터를 찾을 수 없습니다.")

if __name__ == "__main__":
    check_samsung_profit_data() 