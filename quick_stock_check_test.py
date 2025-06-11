import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re

def is_regular_stock(name):
    exclude_keywords = [
        'ETF', 'ETN', 'KODEX', 'TIGER', 'KBSTAR', 'KOSEF', 'KINDEX', 'ARIRANG', 'HANARO',
        'REIT', '리츠',
        '국고채', '회사채', '채권',
        '배당주', '액티브', '인덱스', '합성', '선물', '옵션', '인버스', '레버리지',
        'NH투자', 'SK증권', '메리츠', '미래에셋', '삼성선물', '신한투자',
        '유안타', '유진투자', '키움', '하나금융', '한화투자',
        'KB증권', 'IBK투자', '교보증권', '대신증권', '현대차증권'
    ]
    
    if any(keyword in name for keyword in exclude_keywords) and not ('증권' in name or '금융' in name):
        return False
    return True

def get_individual_stock_data(code, name):
    """개별 종목 페이지에서 PER, PBR, ROE 데이터 수집"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    
    try:
        url = f'https://finance.naver.com/item/main.naver?code={code}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        per = ''
        pbr = ''
        roe = ''
        
        # 전체 텍스트에서 정규식으로 PER, PBR, ROE 찾기
        page_text = soup.get_text()
        
        # PER 추출 (PER 12.34배 형식)
        per_match = re.search(r'PER[^\d]*?([+-]?\d+\.?\d*)배', page_text)
        if per_match:
            per = per_match.group(1)
        
        # PBR 추출 (PBR 1.23배 형식)
        pbr_match = re.search(r'PBR[^\d]*?([+-]?\d+\.?\d*)배', page_text)
        if pbr_match:
            pbr = pbr_match.group(1)
        
        # ROE 추출 (ROE 12.34% 형식)
        roe_match = re.search(r'ROE[^\d]*?([+-]?\d+\.?\d*)%?', page_text)
        if roe_match:
            roe = roe_match.group(1)
        
        # 더 구체적인 테이블 기반 추출 시도
        if not per or not pbr or not roe:
            # 모든 테이블 행 검사
            for table in soup.find_all('table'):
                for row in table.find_all('tr'):
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        for i, cell in enumerate(cells[:-1]):
                            cell_text = cell.get_text(strip=True)
                            next_cell_text = cells[i+1].get_text(strip=True)
                            
                            if 'PER' in cell_text and not per:
                                per_val = re.search(r'([+-]?\d+\.?\d*)', next_cell_text)
                                if per_val:
                                    per = per_val.group(1)
                            
                            if 'PBR' in cell_text and not pbr:
                                pbr_val = re.search(r'([+-]?\d+\.?\d*)', next_cell_text)
                                if pbr_val:
                                    pbr = pbr_val.group(1)
                            
                            if 'ROE' in cell_text and not roe:
                                roe_val = re.search(r'([+-]?\d+\.?\d*)', next_cell_text)
                                if roe_val:
                                    roe = roe_val.group(1)
        
        print(f"{name} - PER: {per}, PBR: {pbr}, ROE: {roe}%")
        return per, pbr, roe
        
    except Exception as e:
        print(f"{name} 데이터 수집 오류: {str(e)}")
        return '', '', ''

def get_stock_data():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    stock_data = []
    
    # 테스트를 위해 일단 첫 2페이지만 수집
    for market_type in [0, 1]:
        market_name = "코스피" if market_type == 0 else "코스닥"
        print(f"\n{market_name} 데이터 수집 시작...")
        
        for page in range(1, 3):  # 첫 2페이지만
            print(f"페이지 {page} 수집 중...")
            url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={market_type}&page={page}'
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            rows = soup.select('table.type_2 tr')[1:]
            if not rows:
                break
            
            collected = 0
            for row in rows:
                cols = row.select('td')
                if len(cols) <= 1:
                    continue
                
                try:
                    name_element = cols[1].select_one('a')
                    if not name_element:
                        continue
                    
                    name = name_element.text.strip()
                    if not is_regular_stock(name):
                        continue
                    
                    code = name_element['href'].split('=')[-1]
                    current_price = cols[2].text.strip().replace(',', '')
                    current_volume = cols[9].text.strip().replace(',', '') if len(cols) > 9 else ''
                    
                    # 개별 종목 페이지에서 PER, PBR, ROE 데이터 수집
                    time.sleep(0.5)  # 요청 간격 늘림
                    per, pbr, roe = get_individual_stock_data(code, name)
                    
                    # 일별 시세 페이지에서 전일 데이터 가져오기
                    time.sleep(0.1)
                    daily_url = f'https://finance.naver.com/item/sise_day.naver?code={code}'
                    daily_response = requests.get(daily_url, headers=headers)
                    daily_soup = BeautifulSoup(daily_response.text, 'html.parser')
                    
                    daily_rows = daily_soup.select('table.type2 tr')
                    valid_rows = [r for r in daily_rows if r.select('td.num')]
                    
                    prev_close = ''
                    prev_volume = ''
                    if len(valid_rows) >= 2:
                        yesterday_row = valid_rows[1]
                        yesterday_cols = yesterday_row.select('td.num')
                        if len(yesterday_cols) >= 6:
                            prev_close = yesterday_cols[0].text.strip().replace(',', '')
                            prev_volume = yesterday_cols[5].text.strip().replace(',', '')
                    
                    if all([name, code, current_price]):
                        stock_data.append({
                            '종목명': name,
                            '종목코드': code,
                            '시장구분': market_name,
                            '현재가': current_price,
                            '전일종가': prev_close,
                            '거래량': current_volume,
                            '전일거래량': prev_volume,
                            'PER': per,
                            'PBR': pbr,
                            'ROE': roe,
                            '수집일자': datetime.now().strftime('%Y-%m-%d')
                        })
                        collected += 1
                
                except Exception as e:
                    print(f"오류 발생 - 종목: {name if 'name' in locals() else '알 수 없음'}, 오류: {str(e)}")
                    continue
            
            print(f"페이지 {page}에서 {collected}개 종목 수집 완료")
            if len(stock_data) >= 10:  # 테스트용으로 10개만 수집
                break
        
        if len(stock_data) >= 10:
            break
    
    return stock_data

def main():
    print("=== 수집된 데이터 미리보기 ===")
    stock_data = get_stock_data()
    
    if stock_data:
        df = pd.DataFrame(stock_data)
        filename = f'stock_data_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
        df.to_excel(filename, index=False)
        print(f"\n수집 완료! 총 {len(stock_data)}개 종목의 데이터가 {filename}에 저장되었습니다.")
        
        # 데이터 미리보기
        print("\n=== 수집된 데이터 요약 ===")
        for _, row in df.iterrows():
            print(f"{row['종목명']} - PER: {row['PER']}, PBR: {row['PBR']}, ROE: {row['ROE']}%")
    else:
        print("\n수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()