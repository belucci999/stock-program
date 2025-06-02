import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

def get_stock_data():
    """네이버 금융에서 주식 정보를 크롤링하는 함수"""
    url = "https://finance.naver.com/sise/sise_market_sum.naver"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    all_data = []
    markets = ['&sosok=0', '&sosok=1']  # 코스피(0)와 코스닥(1)
    
    for market_index, market in enumerate(markets):
        market_name = "코스피" if market == "&sosok=0" else "코스닥"
        print(f"\n{market_name} 시장 수집 시작...")
        
        page = 1
        while True:
            current_url = f"{url}?{market}&page={page}"
            print(f"페이지 {page} 수집 중...")
            
            try:
                response = requests.get(current_url, headers=headers)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                table = soup.select_one('table.type_2')
                if not table:
                    print(f"{market_name} 마지막 페이지 도달")
                    break
                    
                rows = table.select('tbody > tr')
                page_data_count = 0
                
                for row in rows:
                    cols = row.select('td')
                    if len(cols) <= 1:
                        continue
                        
                    try:
                        stock_name = cols[1].select_one('a').text.strip()
                        stock_code = cols[1].select_one('a')['href'].split('=')[-1]
                        
                        stock_data = {
                            '종목명': stock_name,
                            '종목코드': stock_code,
                            '시장구분': market_name,
                            '현재가': cols[2].text.strip(),
                            '전일비': cols[3].text.strip(),
                            '거래량': cols[5].text.strip(),
                            '거래대금': cols[6].text.strip(),
                            '시가총액': cols[7].text.strip(),
                            '수집시각': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        all_data.append(stock_data)
                        page_data_count += 1
                        
                    except Exception as e:
                        print(f"종목 데이터 수집 실패: {str(e)}")
                        continue
                
                print(f"페이지 {page}에서 {page_data_count}개 종목 수집 완료")
                page += 1
                time.sleep(1)  # 서버 부하 방지
                
            except Exception as e:
                print(f"페이지 수집 실패: {str(e)}")
                break
    
    print(f"\n전체 {len(all_data)}개 종목 수집 완료!")
    return pd.DataFrame(all_data)

if __name__ == "__main__":
    print("데이터 수집 시작...")
    start_time = datetime.now()
    
    try:
        df = get_stock_data()
        
        # 결과를 엑셀 파일로 저장
        current_time = datetime.now().strftime('%Y%m%d_%H%M')
        excel_filename = f'stock_data_{current_time}.xlsx'
        df.to_excel(excel_filename, index=False)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"\n작업 완료!")
        print(f"소요 시간: {duration:.1f}초")
        print(f"파일 저장됨: {excel_filename}")
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")