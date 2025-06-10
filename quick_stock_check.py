import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re

def is_regular_stock(name):
    """
    일반 주식인지 판단하는 함수
    ETF, 펀드, 채권 등을 제외하고 순수 주식 종목만 선별
    """
    
    # 명백한 ETF/펀드 브랜드명들
    etf_brands = [
        'KODEX', 'TIGER', 'KBSTAR', 'KOSEF', 'KINDEX', 'ARIRANG', 'HANARO',
        'RISE', 'ACE', 'KIWOOM', 'PLUS', 'SOL', 'WON', '1Q', 'ITF', 'BNK',
        'FOCUS', 'TREX', 'HK', '파워', '마이티', 'DAISHIN343', '아이엠에셋',
        'KCGI', 'KB발해인프라', '맥쿼리인프라', '맵스리얼티', '한국ANKOR유전'
    ]
    
    # ETF/펀드 관련 키워드
    fund_keywords = [
        'ETF', 'ETN', 'REIT', '리츠', '펀드',
        '채권', '통안채', '물가채', '금융채', '국고채', '회사채',
        '단기자금', '단기통안채', '단기금융채', '액티브',
        '스팩', 'SPAC'  # SPAC(특수목적인수회사) 추가
    ]
    
    # 투자 관련 키워드 (일반 주식에는 없는)
    investment_keywords = [
        '인덱스', '합성', '선물', '옵션', '인버스', '레버리지',
        '커버드콜', '위클리커버드콜', '데일리커버드콜', '고정커버드콜',
        'TOP', 'Plus', 'TR', '포커스', '테마', '밸류', '성장', '소부장'
    ]
    
    # 지수 관련 키워드
    index_keywords = ['S&P', 'MSCI', 'CSI', 'FTSE', 'Nikkei', 'DAX', 'NASDAQ', 'SOLACTIVE', 'KRX']
    
    # 국가/지역 관련 (해외투자 ETF)
    country_keywords = ['미국', '중국', '일본', '독일', '인도', '글로벌', '아시아', '유로']
    
    # 1. ETF 브랜드명 체크
    for brand in etf_brands:
        if name.startswith(brand):
            return False
    
    # 2. 펀드/채권 키워드 체크
    for keyword in fund_keywords:
        if keyword in name:
            return False
    
    # 3. 투자 관련 키워드 체크
    for keyword in investment_keywords:
        if keyword in name:
            return False
    
    # 4. 지수 관련 키워드 체크
    for keyword in index_keywords:
        if keyword in name:
            return False
    
    # 5. 국가/지역 키워드 체크 (단, 실제 기업명에 포함된 경우는 예외)
    for keyword in country_keywords:
        if keyword in name:
            # 실제 기업명의 일부가 아니라 투자 상품명인 경우만 제외
            if any(invest_word in name for invest_word in ['배당', '성장', '테크', '반도체', '나스닥']):
                return False
    
    # 6. 증권사 운용 상품 (실제 증권회사 주식은 제외하지 않음)
    securities_products = [
        'NH투자', 'SK증권', '메리츠', '미래에셋', '삼성선물', '신한투자',
        '유안타', '유진투자', '키움', '하나금융', '한화투자',
        'KB증권', 'IBK투자', '교보증권', '대신증권', '현대차증권'
    ]
    
    for keyword in securities_products:
        if keyword in name and not ('증권' in name or '금융' in name or '은행' in name):
            return False
    
    # 7. 고배당, 배당 관련 상품 (개별 주식이 아닌 테마 상품)
    if '고배당' in name or ('배당' in name and any(word in name for word in ['TOP', 'Plus', '성장', '킹'])):
        return False
    
    # 8. SPAC (특수목적인수회사) 관련 패턴
    spac_patterns = [
        '스팩', 'SPAC', '목적', '인수회사', '특수목적'
    ]
    
    for pattern in spac_patterns:
        if pattern in name:
            return False
    
    # SPAC 명명 패턴: "회사명스팩숫자호" (예: 엔에이치스팩29호)
    if '스팩' in name and ('호' in name or any(char.isdigit() for char in name)):
        return False
    
    # 9. 기타 투자회사 패턴
    if '투자회사' in name or '자산운용' in name:
        return False
    
    return True

def get_individual_stock_data(code, name):
    """개별 종목 페이지에서 상세 재무 데이터 수집"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    
    try:
        url = f'https://finance.naver.com/item/main.naver?code={code}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 초기화 (18개 데이터 필드)
        data = {
            'PER': '', 'PBR': '', 'ROE': '', '시가총액': '',
            '매출액': '', '영업이익': '', '당기순이익': '', 
            '부채비율': '', '유보율': '', '배당수익률': '', '배당금': '',
            '52주최고': '', '52주최저': '', '거래대금': '',
            '외국인비율': '', '기관비율': '', '베타': '', '업종': ''
        }
        
        # 전체 텍스트에서 정규식으로 데이터 찾기
        page_text = soup.get_text()
        
        # 기본 지표 추출
        patterns = {
            'PER': r'PER[^\d]*?([+-]?\d+\.?\d*)배',
            'PBR': r'PBR[^\d]*?([+-]?\d+\.?\d*)배', 
            'ROE': r'ROE[^\d]*?([+-]?\d+\.?\d*)%?',
            '시가총액': r'시가총액[^\d]*?([,\d]+)억',
            '배당수익률': r'배당수익률[^\d]*?([+-]?\d+\.?\d*)%',
            '52주최고': r'52주최고[^\d]*?([,\d]+)',
            '52주최저': r'52주최저[^\d]*?([,\d]+)',
            '베타': r'베타[^\d]*?([+-]?\d+\.?\d*)',
            '외국인비율': r'외국인[^\d]*?([+-]?\d+\.?\d*)%',
            '기관비율': r'기관[^\d]*?([+-]?\d+\.?\d*)%'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, page_text)
            if match:
                data[key] = match.group(1).replace(',', '') if ',' in match.group(1) else match.group(1)
        
        # 업종 정보 추출 (종목명 근처에서)
        sector_element = soup.select_one('.wrap_company h2 a')
        if sector_element:
            data['업종'] = sector_element.get('title', '')
        
        # 테이블 기반 상세 추출
        for table in soup.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    for i, cell in enumerate(cells[:-1]):
                        cell_text = cell.get_text(strip=True)
                        next_cell_text = cells[i+1].get_text(strip=True)
                        
                        # 재무 지표 매핑
                        field_mapping = {
                            'PER': ['PER', 'per'],
                            'PBR': ['PBR', 'pbr'],
                            'ROE': ['ROE', 'roe'],
                            '시가총액': ['시가총액'],
                            '매출액': ['매출액', '매출'],
                            '영업이익': ['영업이익'],
                            '당기순이익': ['당기순이익', '순이익'],
                            '부채비율': ['부채비율'],
                            '유보율': ['유보율'],
                            '배당수익률': ['배당수익률', '배당률'],
                            '배당금': ['배당금', '현금배당'],
                            '52주최고': ['52주최고', '52주 최고'],
                            '52주최저': ['52주최저', '52주 최저'],
                            '거래대금': ['거래대금'],
                            '외국인비율': ['외국인', '외국인 비율'],
                            '기관비율': ['기관', '기관 비율'],
                            '베타': ['베타', 'Beta']
                        }
                        
                        for field, keywords in field_mapping.items():
                            if any(keyword in cell_text for keyword in keywords) and not data[field]:
                                # 숫자 추출
                                if field in ['매출액', '영업이익', '당기순이익']:
                                    # 억원 단위 처리
                                    value_match = re.search(r'([,\d]+)억?원?', next_cell_text)
                                    if value_match:
                                        data[field] = value_match.group(1).replace(',', '')
                                elif field in ['52주최고', '52주최저', '거래대금']:
                                    # 일반 숫자 (쉼표 제거)
                                    value_match = re.search(r'([,\d]+)', next_cell_text)
                                    if value_match:
                                        data[field] = value_match.group(1).replace(',', '')
                                elif field in ['부채비율', '유보율', '배당수익률', '외국인비율', '기관비율']:
                                    # 퍼센트 값
                                    value_match = re.search(r'([+-]?\d+\.?\d*)%?', next_cell_text)
                                    if value_match:
                                        data[field] = value_match.group(1)
                                elif field in ['PER', 'PBR', 'ROE', '베타']:
                                    # 배수나 비율
                                    value_match = re.search(r'([+-]?\d+\.?\d*)', next_cell_text)
                                    if value_match:
                                        data[field] = value_match.group(1)
                                elif field == '배당금':
                                    # 배당금 (원)
                                    value_match = re.search(r'([,\d]+)원?', next_cell_text)
                                    if value_match:
                                        data[field] = value_match.group(1).replace(',', '')
        
        # 추가: 기업개요 페이지에서 업종 정보 더 정확히 가져오기
        if not data['업종']:
            try:
                company_url = f'https://finance.naver.com/item/coinfo.naver?code={code}'
                company_response = requests.get(company_url, headers=headers)
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                
                # 업종 정보 추출
                sector_info = company_soup.select_one('table.gline tr td')
                if sector_info:
                    sector_text = sector_info.get_text(strip=True)
                    if '업종' in sector_text:
                        data['업종'] = sector_text.split('업종')[-1].strip()
            except:
                pass  # 실패해도 계속 진행
        
        return tuple(data.values())
        
    except Exception as e:
        print(f"{name} 데이터 수집 오류: {str(e)}")
        return ('',) * 18  # 18개 빈 값 반환

def get_stock_data():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    stock_data = []
    
    for market_type in [0, 1]:
        market_name = "코스피" if market_type == 0 else "코스닥"
        print(f"\n{market_name} 데이터 수집 시작...")
        
        # 전체 페이지 수 확인
        url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={market_type}&page=1'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        max_page = 1
        page_nav = soup.select('td.pgRR > a')
        if page_nav:
            max_page = int(page_nav[0]['href'].split('=')[-1])
        
        print(f"{market_name} 전체 페이지 수: {max_page}")
        
        # 전체 페이지 수집
        for page in range(1, max_page + 1):
            print(f"페이지 {page}/{max_page} 수집 중...")
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
                    
                    # 개별 종목 페이지에서 상세 데이터 수집
                    time.sleep(0.3)  # 서버 부하 방지
                    (per, pbr, roe, market_cap, sales, operating_profit, net_income, 
                     debt_ratio, retention_ratio, dividend_yield, dividend,
                     high_52w, low_52w, trading_value, foreign_ratio, 
                     institutional_ratio, beta, sector) = get_individual_stock_data(code, name)
                    
                    # ETF/펀드 추가 필터링: PER, PBR, ROE가 모두 비어있으면 제외
                    if not per and not pbr and not roe:
                        print(f"  - {name}: PER/PBR/ROE 데이터 없음 (ETF/펀드로 추정) - 제외")
                        continue
                    
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
                            '업종': sector,
                            '현재가': current_price,
                            '전일종가': prev_close,
                            '거래량': current_volume,
                            '전일거래량': prev_volume,
                            '거래대금': trading_value,
                            'PER': per,
                            'PBR': pbr,
                            'ROE': roe,
                            '시가총액': market_cap,
                            '매출액': sales,
                            '영업이익': operating_profit,
                            '당기순이익': net_income,
                            '부채비율': debt_ratio,
                            '유보율': retention_ratio,
                            '배당수익률': dividend_yield,
                            '배당금': dividend,
                            '52주최고': high_52w,
                            '52주최저': low_52w,
                            '외국인비율': foreign_ratio,
                            '기관비율': institutional_ratio,
                            '베타': beta,
                            '수집일자': datetime.now().strftime('%Y-%m-%d')
                        })
                        collected += 1
                        
                        # 진행상황 출력
                        if collected % 10 == 0:
                            print(f"  - {collected}개 종목 수집 완료 (최근: {name} - 업종: {sector})")
                
                except Exception as e:
                    print(f"오류 발생 - 종목: {name if 'name' in locals() else '알 수 없음'}, 오류: {str(e)}")
                    continue
            
            print(f"페이지 {page}에서 {collected}개 종목 수집 완료")
            
            # 중간 저장 (100페이지마다)
            if page % 100 == 0:
                temp_df = pd.DataFrame(stock_data)
                temp_filename = f'temp_stock_data_{datetime.now().strftime("%Y%m%d_%H%M")}_{page}.xlsx'
                temp_df.to_excel(temp_filename, index=False)
                print(f"중간 저장: {temp_filename}")
    
    return stock_data

def main():
    print("=== 전체 종목 상세 데이터 수집 시작 ===")
    print("수집 데이터: 25개 필드 (재무지표, 투자자정보, 배당정보 등)")
    print("예상 소요시간: 3-4시간 (전체 종목 약 2000-3000개)")
    print("주의: 수집 중 중단하지 마세요. 100페이지마다 중간 저장됩니다.\n")
    
    start_time = datetime.now()
    stock_data = get_stock_data()
    end_time = datetime.now()
    
    if stock_data:
        df = pd.DataFrame(stock_data)
        filename = f'full_stock_data_detailed_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
        df.to_excel(filename, index=False)
        
        print(f"\n🎉 수집 완료!")
        print(f"총 {len(stock_data)}개 종목의 상세 데이터가 {filename}에 저장되었습니다.")
        print(f"소요시간: {end_time - start_time}")
        
        # 데이터 품질 확인
        print(f"\n📊 데이터 품질:")
        print(f"PER 데이터 있는 종목: {df['PER'].notna().sum()}/{len(df)}")
        print(f"PBR 데이터 있는 종목: {df['PBR'].notna().sum()}/{len(df)}")
        print(f"ROE 데이터 있는 종목: {df['ROE'].notna().sum()}/{len(df)}")
        print(f"시가총액 데이터 있는 종목: {df['시가총액'].notna().sum()}/{len(df)}")
        print(f"업종 데이터 있는 종목: {df['업종'].notna().sum()}/{len(df)}")
        print(f"매출액 데이터 있는 종목: {df['매출액'].notna().sum()}/{len(df)}")
        print(f"배당수익률 데이터 있는 종목: {df['배당수익률'].notna().sum()}/{len(df)}")
        
        # 시가총액별 분포 확인
        df['시가총액'] = pd.to_numeric(df['시가총액'], errors='coerce')
        print(f"\n📈 시가총액 분포:")
        print(f"10조 이상 대형주: {(df['시가총액'] >= 100000).sum()}개")
        print(f"1조~10조 중형주: {((df['시가총액'] >= 10000) & (df['시가총액'] < 100000)).sum()}개")
        print(f"1조 미만 소형주: {(df['시가총액'] < 10000).sum()}개")
        
        # 업종별 분포 확인
        print(f"\n🏢 업종별 분포 (상위 10개):")
        sector_counts = df['업종'].value_counts().head(10)
        for sector, count in sector_counts.items():
            print(f"{sector}: {count}개")
            
    else:
        print("\n수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()