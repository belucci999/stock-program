import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
import gspread
from google.oauth2.service_account import Credentials
import numpy as np
import os

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

class GoogleSheetsUploader:
    def __init__(self, credentials_file='credentials.json'):
        """구글 시트 업로더 초기화"""
        self.credentials_file = credentials_file
        self.gc = None
        self.setup_connection()
    
    def setup_connection(self):
        """구글 시트 연결 설정"""
        try:
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            if not os.path.exists(self.credentials_file):
                print(f"❌ {self.credentials_file} 파일이 없습니다.")
                return False
            
            creds = Credentials.from_service_account_file(self.credentials_file, scopes=scope)
            self.gc = gspread.authorize(creds)
            print("✅ 구글 시트 연결 성공!")
            return True
            
        except Exception as e:
            print(f"❌ 구글 시트 연결 실패: {str(e)}")
            return False
    
    def create_or_get_spreadsheet(self, spreadsheet_name):
        """스프레드시트 생성 또는 가져오기"""
        try:
            # 기존 스프레드시트 찾기
            try:
                spreadsheet = self.gc.open(spreadsheet_name)
                print(f"📋 기존 스프레드시트 사용: {spreadsheet_name}")
                return spreadsheet
            except gspread.SpreadsheetNotFound:
                # 새 스프레드시트 생성
                spreadsheet = self.gc.create(spreadsheet_name)
                print(f"📝 새 스프레드시트 생성: {spreadsheet_name}")
                return spreadsheet
                
        except Exception as e:
            print(f"❌ 스프레드시트 생성/접근 실패: {str(e)}")
            return None
    
    def clean_dataframe(self, df):
        """DataFrame에서 NaN 값을 처리하여 JSON 호환 가능하게 만들기"""
        # DataFrame 복사
        df_clean = df.copy()
        
        # NaN 값을 빈 문자열로 변경
        df_clean = df_clean.fillna('')
        
        # inf, -inf 값도 처리
        df_clean = df_clean.replace([np.inf, -np.inf], '')
        
        # 모든 값을 문자열로 변환하여 JSON 호환성 보장
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            # 'nan' 문자열도 빈 문자열로 변경
            df_clean[col] = df_clean[col].replace('nan', '')
        
        return df_clean
    
    def upload_dataframe(self, df, spreadsheet_name, sheet_name):
        """데이터프레임을 구글 시트에 업로드 (NaN 값 처리 포함)"""
        try:
            spreadsheet = self.create_or_get_spreadsheet(spreadsheet_name)
            if not spreadsheet:
                return False
            
            # 시트가 있는지 확인하고 없으면 생성
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                # 기존 데이터 삭제
                worksheet.clear()
            except gspread.WorksheetNotFound:
                # 새 시트 생성
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=30)
            
            # 데이터 업로드
            if len(df) > 0:
                # DataFrame 정리 (NaN 값 처리)
                df_clean = self.clean_dataframe(df)
                
                # 헤더와 데이터를 함께 업로드
                data = [df_clean.columns.tolist()] + df_clean.values.tolist()
                
                # 수정된 update 방식
                worksheet.update(values=data, range_name='A1')
                
                print(f"✅ '{sheet_name}' 시트에 {len(df)}개 행 업로드 완료!")
                return True
            else:
                print(f"⚠️ '{sheet_name}' 시트: 업로드할 데이터가 없습니다.")
                return True
                
        except Exception as e:
            print(f"❌ 시트 업로드 실패 ({sheet_name}): {str(e)}")
            return False
    
    def get_spreadsheet_url(self, spreadsheet_name):
        """스프레드시트 URL 가져오기"""
        try:
            spreadsheet = self.gc.open(spreadsheet_name)
            return spreadsheet.url
        except:
            return None

    def format_sheet_headers(self, spreadsheet_name, sheet_name):
        """시트 헤더 포맷 설정 (선택사항)"""
        try:
            spreadsheet = self.gc.open(spreadsheet_name)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            # 헤더 행 굵게 만들기
            worksheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            print(f"✅ '{sheet_name}' 시트 헤더 포맷 적용 완료!")
            return True
            
        except Exception as e:
            print(f"⚠️ 헤더 포맷 적용 실패 ({sheet_name}): {str(e)}")
            return False

def calculate_volume_change_rate(current_volume, prev_volume):
    """거래량 전일비 증감율 계산"""
    try:
        if not current_volume or not prev_volume or prev_volume == '0':
            return ''
        
        current = float(str(current_volume).replace(',', ''))
        previous = float(str(prev_volume).replace(',', ''))
        
        if previous == 0:
            return ''
        
        change_rate = ((current - previous) / previous) * 100
        return f"{change_rate:.2f}"
    except:
        return ''

def get_individual_stock_data(code, name):
    """개별 종목 페이지에서 상세 재무 데이터 수집"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
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
        
        # 업종 정보 추출 (여러 방법 시도)
        # 방법 1: 종목명 옆 링크에서
        sector_link = soup.select_one('.wrap_company h2 a')
        if sector_link and sector_link.get('title'):
            data['업종'] = sector_link.get('title').strip()
        
        # 방법 2: 기업개요 페이지에서
        if not data['업종']:
            try:
                company_url = f'https://finance.naver.com/item/coinfo.naver?code={code}'
                company_response = requests.get(company_url, headers=headers)
                company_soup = BeautifulSoup(company_response.text, 'html.parser')
                
                # 업종 테이블에서 추출
                tables = company_soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        for i, cell in enumerate(cells[:-1]):
                            if '업종' in cell.get_text(strip=True) and i+1 < len(cells):
                                data['업종'] = cells[i+1].get_text(strip=True)
                                break
                        if data['업종']:
                            break
                    if data['업종']:
                        break
            except:
                pass
        
        # 거래대금과 시가총액 추출 (메인 페이지의 요약 정보에서)
        today_data = soup.select('.today .blind')
        for elem in today_data:
            text = elem.get_text(strip=True)
            # 거래대금 추출
            if '거래대금' in text:
                match = re.search(r'거래대금[^\d]*([,\d]+)', text)
                if match:
                    data['거래대금'] = match.group(1).replace(',', '')
        
        # 시가총액은 보통 요약 정보 테이블에 있음
        summary_table = soup.select_one('table.no_info')
        if summary_table:
            for row in summary_table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                for i, cell in enumerate(cells[:-1]):
                    cell_text = cell.get_text(strip=True)
                    if '시가총액' in cell_text and i+1 < len(cells):
                        next_text = cells[i+1].get_text(strip=True)
                        # 시가총액에서 숫자만 추출 (억원 단위)
                        match = re.search(r'([,\d]+)억?원?', next_text)
                        if match:
                            data['시가총액'] = match.group(1).replace(',', '')
        
        # 베타 정보는 투자정보 섹션에서 추출
        invest_info = soup.select('.section.invest_info')
        for section in invest_info:
            text = section.get_text()
            beta_match = re.search(r'베타[^\d]*([+-]?\d+\.?\d*)', text)
            if beta_match:
                data['베타'] = beta_match.group(1)
                break
        
        # 추가: 전체 페이지 텍스트에서 정규식으로 누락된 데이터 보완
        page_text = soup.get_text()
        
        # 더 포괄적인 패턴으로 재시도
        if not data['거래대금']:
            trading_match = re.search(r'거래대금[^\d]*([,\d]+)백만원?|거래대금[^\d]*([,\d]+)억원?', page_text)
            if trading_match:
                value = trading_match.group(1) or trading_match.group(2)
                data['거래대금'] = value.replace(',', '') if value else ''
        
        if not data['시가총액']:
            market_cap_match = re.search(r'시가총액[^\d]*([,\d]+)억원?', page_text)
            if market_cap_match:
                data['시가총액'] = market_cap_match.group(1).replace(',', '')
        
        if not data['베타']:
            beta_match = re.search(r'베타[^\d]*([+-]?\d+\.?\d*)', page_text)
            if beta_match:
                data['베타'] = beta_match.group(1)
        
        # 기존 PER, PBR, ROE 등 기본 지표 추출
        patterns = {
            'PER': r'PER[^\d]*?([+-]?\d+\.?\d*)배',
            'PBR': r'PBR[^\d]*?([+-]?\d+\.?\d*)배', 
            'ROE': r'ROE[^\d]*?([+-]?\d+\.?\d*)%?',
            '배당수익률': r'배당수익률[^\d]*?([+-]?\d+\.?\d*)%',
            '52주최고': r'52주최고[^\d]*?([,\d]+)',
            '52주최저': r'52주최저[^\d]*?([,\d]+)',
            '외국인비율': r'외국인[^\d]*?([+-]?\d+\.?\d*)%',
            '기관비율': r'기관[^\d]*?([+-]?\d+\.?\d*)%'
        }
        
        for key, pattern in patterns.items():
            if not data[key]:  # 아직 값이 없는 경우만
                match = re.search(pattern, page_text)
                if match:
                    data[key] = match.group(1).replace(',', '') if ',' in match.group(1) else match.group(1)
        
        # 테이블 기반 상세 추출 (기존 로직 유지하되 개선)
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
                                elif field == '시가총액':
                                    # 시가총액 (억원)
                                    value_match = re.search(r'([,\d]+)억?원?', next_cell_text)
                                    if value_match:
                                        data[field] = value_match.group(1).replace(',', '')
                                elif field == '배당금':
                                    # 배당금 (원)
                                    value_match = re.search(r'([,\d]+)원?', next_cell_text)
                                    if value_match:
                                        data[field] = value_match.group(1).replace(',', '')
        
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
                    
                    # 거래량 증감율 계산
                    volume_change_rate = calculate_volume_change_rate(current_volume, prev_volume)
                    
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
                            '거래량증감율': volume_change_rate,
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
    print("수집 데이터: 26개 필드 (재무지표, 투자자정보, 배당정보, 거래량증감율 등)")
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
        print(f"거래량증감율 데이터 있는 종목: {df['거래량증감율'].notna().sum()}/{len(df)}")
        
        # 시가총액별 분포 확인
        df['시가총액'] = pd.to_numeric(df['시가총액'], errors='coerce')
        print(f"\n📈 시가총액 분포:")
        print(f"10조 이상 대형주: {(df['시가총액'] >= 100000).sum()}개")
        print(f"1조~10조 중형주: {((df['시가총액'] >= 10000) & (df['시가총액'] < 100000)).sum()}개")
        print(f"1조 미만 소형주: {(df['시가총액'] < 10000).sum()}개")
        
        # 거래량 증감율 통계
        df['거래량증감율_num'] = pd.to_numeric(df['거래량증감율'], errors='coerce')
        volume_stats = df['거래량증감율_num'].describe()
        print(f"\n📊 거래량 증감율 통계:")
        print(f"평균: {volume_stats['mean']:.2f}%")
        print(f"중앙값: {volume_stats['50%']:.2f}%")
        print(f"최대: {volume_stats['max']:.2f}%")
        print(f"최소: {volume_stats['min']:.2f}%")
        
        # 업종별 분포 확인
        print(f"\n🏢 업종별 분포 (상위 10개):")
        sector_counts = df['업종'].value_counts().head(10)
        for sector, count in sector_counts.items():
            print(f"{sector}: {count}개")
        
        # 구글 시트 업로드
        print(f"\n📤 구글 시트 업로드 시작...")
        uploader = GoogleSheetsUploader()
        
        if uploader.gc:  # 구글 시트 연결이 성공한 경우
            # 기존 Stock Analyzer와 동일한 스프레드시트 사용
            today = datetime.now().strftime('%Y-%m-%d')
            spreadsheet_name = f"주식분석결과_{today}"
            sheet_name = f"💾_전체종목데이터_{datetime.now().strftime('%H%M')}"
            
            success = uploader.upload_dataframe(df, spreadsheet_name, sheet_name)
            
            if success:
                # 헤더 포맷 적용
                uploader.format_sheet_headers(spreadsheet_name, sheet_name)
                
                # 스프레드시트 URL 출력
                url = uploader.get_spreadsheet_url(spreadsheet_name)
                if url:
                    print(f"📊 구글 시트 링크: {url}")
                    
                print(f"✅ 구글 시트 업로드 완료!")
                print(f"📋 시트명: {sheet_name}")
                print(f"📝 기존 역발상 투자 데이터와 동일한 스프레드시트에 새 시트로 추가되었습니다.")
            else:
                print(f"❌ 구글 시트 업로드 실패")
        else:
            print(f"⚠️ 구글 시트 연결 실패로 인해 업로드를 건너뜁니다.")
            print(f"💾 엑셀 파일로만 저장됨: {filename}")
            
    else:
        print("\n수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()