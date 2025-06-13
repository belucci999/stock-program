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
    """개별 종목 페이지에서 상세 재무 데이터 수집 (실제 페이지 구조 반영)"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        # 메인 페이지에서 기본 정보 수집 (가장 많은 데이터가 있음)
        main_url = f'https://finance.naver.com/item/main.naver?code={code}'
        main_response = requests.get(main_url, headers=headers)
        main_soup = BeautifulSoup(main_response.text, 'html.parser')
        
        # 투자자별 매매동향 페이지에서 투자자 비율 수집
        investor_url = f'https://finance.naver.com/item/frgn.naver?code={code}'
        investor_response = requests.get(investor_url, headers=headers)
        investor_soup = BeautifulSoup(investor_response.text, 'html.parser')
        
        # 재무정보 페이지에서 상세 재무 데이터 수집
        finance_url = f'https://finance.naver.com/item/coinfo.naver?code={code}&target=finsum_more'
        finance_response = requests.get(finance_url, headers=headers)
        finance_soup = BeautifulSoup(finance_response.text, 'html.parser')
        
        # 초기화 (확장된 데이터 필드)
        data = {
            'PER': '', 'PBR': '', 'ROE': '', '시가총액': '',
            '매출액': '', '영업이익': '', '당기순이익': '', 
            '부채비율': '', '유보율': '', '배당수익률': '', '배당금': '',
            '52주최고': '', '52주최저': '', '거래대금': '',
            '외국인비율': '', '기관비율': '', '베타': '', '업종': '',
            '영업이익률': '', '순이익률': ''  # 추가 지표
        }
        
        # 1. 메인 페이지에서 기본 데이터 추출 (PER, PBR, ROE 최신 데이터 포함)
        extract_main_page_data(main_soup, data)
        
        # 3. 투자자 페이지에서 투자자 비율 추출
        extract_investor_data(investor_soup, data)
        
        # 4. 재무정보 페이지에서 재무 데이터 추출
        extract_financial_data(finance_soup, data)
        
        # 5. 메인 페이지에서 추가 재무 정보 추출 (백업)
        extract_additional_finance_data(main_soup, data)
        
        # 정확한 순서로 데이터 반환 (메인 수집 로직과 정확히 일치)
        return (
            data['PER'],        # 0: PER
            data['PBR'],        # 1: PBR  
            data['ROE'],        # 2: ROE
            data['시가총액'],    # 3: 시가총액
            data['매출액'],      # 4: 매출액
            data['영업이익'],    # 5: 영업이익
            data['당기순이익'],  # 6: 당기순이익
            data['부채비율'],    # 7: 부채비율
            data['유보율'],      # 8: 유보율
            data['배당수익률'],  # 9: 배당수익률
            data['배당금'],      # 10: 배당금
            data['52주최고'],    # 11: 52주최고
            data['52주최저'],    # 12: 52주최저
            data['거래대금'],    # 13: 거래대금
            data['외국인비율'],  # 14: 외국인비율
            data['기관비율'],    # 15: 기관비율
            data['베타'],        # 16: 베타
            data['업종']         # 17: 업종
        )
        
    except Exception as e:
        print(f"{name} 데이터 수집 오류: {str(e)}")
        return ('',) * 18  # 18개 빈 값 반환

def extract_investment_indicators(soup, data):
    """종목정보 페이지의 투자지표 테이블에서 최신 데이터 추출 (올바른 위치에서)"""
    try:
        tables = soup.find_all('table')
        
        for table in tables:
            table_text = table.get_text()
            
            # PER/PBR 전용 테이블 찾기 (테이블 10번 - PER/EPS 박스)
            if 'PERlEPS' in table_text and 'PBRlBPS' in table_text:
                rows = table.find_all('tr')
                
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        first_cell_text = cells[0].get_text(strip=True)
                        second_cell_text = cells[1].get_text(strip=True)
                        
                        # PER 추출 (PERlEPS 행에서)
                        if 'PERlEPS' in first_cell_text and '추정' not in first_cell_text and not data['PER']:
                            # "11.53배l5,162원" 형태에서 PER 값 추출
                            per_match = re.search(r'([+-]?\d+\.?\d*)배', second_cell_text)
                            if per_match:
                                per_value = float(per_match.group(1))
                                if 0 < per_value < 1000:  # 합리적인 PER 범위
                                    data['PER'] = per_match.group(1)
                                    print(f"✅ 올바른 PER 추출: {data['PER']}배")
                        
                        # PBR 추출 (PBRlBPS 행에서)
                        elif 'PBRlBPS' in first_cell_text and not data['PBR']:
                            # "1.01배l59,059원" 형태에서 PBR 값 추출
                            pbr_match = re.search(r'([+-]?\d+\.?\d*)배', second_cell_text)
                            if pbr_match:
                                pbr_value = float(pbr_match.group(1))
                                if 0 < pbr_value < 100:  # 합리적인 PBR 범위
                                    data['PBR'] = pbr_match.group(1)
                                    print(f"✅ 올바른 PBR 추출: {data['PBR']}배")
                        
                        # 배당수익률 추출
                        elif '배당수익률' in first_cell_text and not data['배당수익률']:
                            # "2.43%" 형태
                            dividend_match = re.search(r'([+-]?\d+\.?\d*)%', second_cell_text)
                            if dividend_match:
                                data['배당수익률'] = dividend_match.group(1)
                                print(f"✅ 배당수익률 추출: {data['배당수익률']}%")
            
            # 동종업종비교 테이블에서 시가총액 추출 (정확한 위치)
            elif '동종업종비교' in table_text and '삼성전자' in table_text:
                rows = table.find_all('tr')
                
                # 삼성전자가 첫 번째 컬럼에 있는 테이블에서 시가총액 추출
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        first_cell_text = cells[0].get_text(strip=True)
                        
                        # 시가총액(억) 행 찾기
                        if '시가총액' in first_cell_text and '억' in first_cell_text:
                            # 삼성전자는 두 번째 컬럼 (인덱스 1)
                            if len(cells) > 1 and not data['시가총액']:
                                samsung_cap = cells[1].get_text(strip=True).replace(',', '')
                                # 숫자만 추출
                                cap_match = re.search(r'^([0-9,]+)', samsung_cap)
                                if cap_match:
                                    data['시가총액'] = cap_match.group(1).replace(',', '')
                                    print(f"✅ 올바른 시가총액 추출: {data['시가총액']}억원")
    
    except Exception as e:
        print(f"투자지표 추출 오류: {str(e)}")

def extract_latest_financial_ratios(soup, data):
    """투자정보 테이블에서 최신 PER, PBR, ROE 추출 (2025.03 기준)"""
    try:
        tables = soup.find_all('table')
        
        for table in tables:
            table_text = table.get_text()
            
            # 2025.03이 포함된 투자정보 테이블 찾기
            if 'PER' in table_text and 'PBR' in table_text and '2025.03' in table_text:
                rows = table.find_all('tr')
                
                # 헤더에서 최신 컬럼 위치 찾기 (2025.06 우선, 없으면 2025.03)
                latest_col_index = -1
                if len(rows) >= 2:
                    header_cells = rows[1].find_all(['td', 'th'])  # 2번째 행이 년도 행
                    
                    # 1순위: 2025.06 찾기
                    for i, cell in enumerate(header_cells):
                        if '2025.06' in cell.get_text(strip=True):
                            latest_col_index = i
                            print(f"✅ 2025.06 컬럼 발견 (컬럼 {i})")
                            break
                    
                    # 2순위: 2025.06이 없으면 2025.03 찾기  
                    if latest_col_index == -1:
                        for i, cell in enumerate(header_cells):
                            if '2025.03' in cell.get_text(strip=True):
                                latest_col_index = i
                                print(f"✅ 2025.03 컬럼 발견 (컬럼 {i})")
                                break
                
                if latest_col_index == -1:
                    continue
                
                # 각 행에서 PER, PBR, ROE, 부채비율, 유보율 추출
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) > latest_col_index:
                        first_cell_text = cells[0].get_text(strip=True)
                        latest_value = cells[latest_col_index].get_text(strip=True)
                        
                        # PER 추출 (PER(배) 행)
                        if 'PER' in first_cell_text and '배' in first_cell_text and not data['PER']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                try:
                                    per_value = float(latest_value)
                                    if 0 < per_value < 1000:
                                        data['PER'] = latest_value
                                        print(f"✅ 최신 PER 추출 (2025.03): {data['PER']}")
                                except:
                                    pass
                        
                        # PBR 추출 (PBR(배) 행)
                        elif 'PBR' in first_cell_text and '배' in first_cell_text and not data['PBR']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                try:
                                    pbr_value = float(latest_value)
                                    if 0 < pbr_value < 100:
                                        data['PBR'] = latest_value
                                        print(f"✅ 최신 PBR 추출 (2025.03): {data['PBR']}")
                                except:
                                    pass
                        
                        # ROE 추출 (ROE(지배주주) 행)
                        elif 'ROE' in first_cell_text and not data['ROE']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                try:
                                    roe_value = float(latest_value)
                                    if -100 <= roe_value <= 100:
                                        data['ROE'] = latest_value
                                        print(f"✅ 최신 ROE 추출 (2025.03): {data['ROE']}")
                                except:
                                    pass
                        
                        # 매출액 추출
                        elif '매출액' in first_cell_text and not data['매출액']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                data['매출액'] = latest_value
                                print(f"✅ 최신 매출액 추출 (2025.03): {data['매출액']}")
                        
                        # 영업이익 추출
                        elif '영업이익' in first_cell_text and not data['영업이익']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                data['영업이익'] = latest_value
                                print(f"✅ 최신 영업이익 추출 (2025.03): {data['영업이익']}")
                        
                        # 당기순이익 추출
                        elif '당기순이익' in first_cell_text and not data['당기순이익']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                data['당기순이익'] = latest_value
                                print(f"✅ 최신 당기순이익 추출 (2025.03): {data['당기순이익']}")
                        
                        # 영업이익률 추출
                        elif '영업이익률' in first_cell_text and not data.get('영업이익률'):
                            if latest_value and latest_value != '' and latest_value != '-':
                                data['영업이익률'] = latest_value
                                print(f"✅ 최신 영업이익률 추출 (2025.03): {data['영업이익률']}")
                        
                        # 순이익률 추출
                        elif '순이익률' in first_cell_text and not data.get('순이익률'):
                            if latest_value and latest_value != '' and latest_value != '-':
                                data['순이익률'] = latest_value
                                print(f"✅ 최신 순이익률 추출 (2025.03): {data['순이익률']}")
                        
                        # 부채비율 추출 ⭐ 추가
                        elif '부채비율' in first_cell_text and not data['부채비율']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                try:
                                    debt_value = float(latest_value)
                                    if 0 <= debt_value <= 1000:  # 합리적인 부채비율 범위
                                        data['부채비율'] = latest_value
                                        print(f"✅ 최신 부채비율 추출 (2025.03): {data['부채비율']}%")
                                except:
                                    pass
                        
                        # 유보율 추출 ⭐ 추가
                        elif '유보율' in first_cell_text and not data['유보율']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                try:
                                    retention_value = float(latest_value.replace(',', ''))
                                    if retention_value > 0:  # 유보율은 보통 큰 값
                                        data['유보율'] = latest_value
                                        print(f"✅ 최신 유보율 추출 (2025.03): {data['유보율']}%")
                                except:
                                    pass
                        
                        # 배당금 추출 ⭐ 추가
                        elif '주당배당금' in first_cell_text and not data['배당금']:
                            if latest_value and latest_value != '' and latest_value != '-':
                                try:
                                    dividend_value = float(latest_value.replace(',', ''))
                                    if dividend_value > 0:
                                        data['배당금'] = latest_value
                                        print(f"✅ 최신 배당금 추출 (2025.03): {data['배당금']}원")
                                except:
                                    pass
    
    except Exception as e:
        print(f"재무비율 추출 오류: {str(e)}")

def extract_main_page_data(soup, data):
    """메인 페이지에서 모든 주요 정보 추출 (PER, PBR, ROE, 시가총액, 52주 최고/최저, 배당수익률)"""
    try:
        # 업종 정보 추출 (여러 위치에서 시도)
        if not data['업종']:
            # 시도 1: h2 태그 내 em 태그
            sector_elements = soup.select('div.wrap_company h2 em')
            if sector_elements:
                data['업종'] = sector_elements[0].get_text(strip=True)
                print(f"✅ 업종 추출: {data['업종']}")
            
            # 시도 2: 업종 테이블에서 찾기
            if not data['업종']:
                page_text = soup.get_text()
                sector_match = re.search(r'업종[^\w]*([가-힣\w\s]+?)(?:\s|업종|분류)', page_text)
                if sector_match:
                    sector = sector_match.group(1).strip()
                    if sector and not re.match(r'^\d', sector):  # 숫자로 시작하지 않는 경우만
                        data['업종'] = sector
                        print(f"✅ 업종 추출 (텍스트): {data['업종']}")
        
        # 1. 우측 투자정보 박스에서 최신 투자지표 추출 (우선순위 높음)
        extract_investment_indicators(soup, data)
        
        # 2. 재무정보 테이블에서 최신 PER, PBR, ROE 추출 (백업)
        extract_latest_financial_ratios(soup, data)
        
        # 3. 전체 페이지 텍스트에서 52주 최고/최저, 시가총액, 거래대금 추출
        page_text = soup.get_text()
        
        # 52주 최고/최저 추출 (강화된 패턴) ⭐ 개선
        if not data['52주최고'] or not data['52주최저']:
            patterns = [
                # 패턴 1: "52주최고l최저: 88,800l49,900" 형태
                r'52주최고[^\d]*?최저[^\d]*?([,\d]+)[^\d]*?([,\d]+)',
                # 패턴 2: "52주 최고 88,800 최저 49,900" 형태  
                r'52주\s*최고[^\d]*?([,\d]+)[^\d]*?최저[^\d]*?([,\d]+)',
                # 패턴 3: "최고 88,800 최저 49,900" 형태
                r'최고[^\d]*?([,\d]+)[^\d]*?최저[^\d]*?([,\d]+)',
                # 패턴 4: "88,800l49,900" 간단한 형태 (52주 관련 텍스트 근처에서만)
                r'([,\d]{5,})l([,\d]{5,})',
                # 패턴 5: 세로 막대 구분자
                r'([,\d]{5,})\|([,\d]{5,})',
            ]
            
            for i, pattern in enumerate(patterns):
                if data['52주최고'] and data['52주최저']:
                    break
                    
                matches = re.finditer(pattern, page_text)
                for match in matches:
                    high = match.group(1).replace(',', '')
                    low = match.group(2).replace(',', '')
                    
                    # 합리적인 값인지 확인
                    try:
                        high_val = int(high)
                        low_val = int(low)
                        
                        # 조건: 4자리 이상, high > low, 현실적인 주가 범위
                        if (len(high) >= 4 and len(low) >= 4 and 
                            high_val > low_val and 
                            100 <= low_val <= 1000000 and 
                            100 <= high_val <= 1000000):
                            
                            data['52주최고'] = high
                            data['52주최저'] = low
                            print(f"✅ 52주 최고/최저 추출 (패턴{i+1}): {data['52주최고']}/{data['52주최저']}")
                            break
                    except:
                        continue
        
        # 테이블에서 52주 최고/최저 직접 찾기 (백업) ⭐ 추가
        if not data['52주최고'] or not data['52주최저']:
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    for cell in cells:
                        cell_text = cell.get_text(strip=True)
                        
                        # "88,800l49,900" 같은 패턴 찾기
                        if 'l' in cell_text and len(cell_text) < 20:  # 너무 긴 텍스트는 제외
                            parts = cell_text.split('l')
                            if len(parts) == 2:
                                try:
                                    high = parts[0].replace(',', '').strip()
                                    low = parts[1].replace(',', '').strip()
                                    
                                    if (high.isdigit() and low.isdigit() and 
                                        len(high) >= 4 and len(low) >= 4 and
                                        int(high) > int(low)):
                                        
                                        data['52주최고'] = high
                                        data['52주최저'] = low
                                        print(f"✅ 52주 최고/최저 추출 (테이블): {data['52주최고']}/{data['52주최저']}")
                                        break
                                except:
                                    continue
                if data['52주최고'] and data['52주최저']:
                    break
        
        # 시가총액 추출
        if not data['시가총액']:
            # 1. '조'와 '억'이 모두 있는 경우
            jo_ok_match = re.search(r'시가총액[\s:]*([0-9,]+)조\s*([0-9,]+)?억원', soup.get_text())
            if jo_ok_match:
                jo = int(jo_ok_match.group(1).replace(',', ''))
                ok = int(jo_ok_match.group(2).replace(',', '')) if jo_ok_match.group(2) else 0
                data['시가총액'] = str(jo * 10000 + ok)
                print(f"✅ 시가총액 추출(조+억): {jo}조 {ok}억 → {data['시가총액']}억")
            else:
                # 2. '억원'만 있는 경우
                market_cap_match = re.search(r'시가총액[^\d]*?([,\d]+)억원', soup.get_text())
                if market_cap_match:
                    data['시가총액'] = market_cap_match.group(1).replace(',', '')
                    print(f"✅ 시가총액 추출: {data['시가총액']}억원")
        
        # 거래대금 추출
        if not data['거래대금']:
            # "1,057,637백만원" 패턴
            trading_match = re.search(r'거래대금[^\d]*?([,\d]+)백만원', page_text)
            if trading_match:
                data['거래대금'] = trading_match.group(1).replace(',', '')
                print(f"✅ 거래대금 추출: {data['거래대금']}백만원")
        
        # 4. 테이블에서 추가 정보 추출
        tables = soup.find_all('table')
        
        for table in tables:
            table_text = table.get_text()
            
            # 시가총액/거래대금 테이블 찾기 (백업)
            if '시가총액' in table_text:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        first_cell = cells[0].get_text(strip=True)
                        second_cell = cells[1].get_text(strip=True)
                        
                        if '시가총액' in first_cell and not data['시가총액']:
                            # "3,522,184억원" → "3522184"
                            cap_match = re.search(r'([0-9,]+)억원', second_cell)
                            if cap_match:
                                data['시가총액'] = cap_match.group(1).replace(',', '')
                        
                        elif '거래대금' in first_cell and not data['거래대금']:
                            # "1,057,637백만원" → "1057637"
                            amount_match = re.search(r'([0-9,]+)백만원', second_cell)
                            if amount_match:
                                data['거래대금'] = amount_match.group(1).replace(',', '')
        
        # 5. 메인 페이지에서 배당수익률 추출 (백업)
        if not data['배당수익률']:
            dividend_pattern = re.search(r'배당수익률[:\s]*([0-9.]+)%', soup.get_text())
            if dividend_pattern:
                data['배당수익률'] = dividend_pattern.group(1)
                print(f"✅ 메인페이지 배당수익률 추출: {data['배당수익률']}%")
    
    except Exception as e:
        print(f"메인페이지 데이터 추출 오류: {str(e)}")

def extract_financial_data(soup, data):
    """재무정보 페이지에서 재무 데이터 추출"""
    try:
        # 재무제표 테이블에서 최신 연도 데이터 추출
        tables = soup.find_all('table')
        for table in tables:
            table_text = table.get_text()
            
            # 재무제표 테이블 찾기
            if '매출액' in table_text or '영업이익' in table_text:
                rows = table.find_all('tr')
                if len(rows) > 1:
                    # 헤더에서 최신 연도 컬럼 찾기 (2024년 우선)
                    header_row = rows[0]
                    year_cells = header_row.find_all(['th', 'td'])
                    latest_col_index = 1  # 기본값
                    
                    for i, cell in enumerate(year_cells[1:], 1):
                        cell_text = cell.get_text(strip=True)
                        if '2024' in cell_text or '24/12' in cell_text:
                            latest_col_index = i
                            break
                        elif '2023' in cell_text or '23/12' in cell_text and latest_col_index == 1:
                            latest_col_index = i
                    
                    # 각 재무 항목 추출
                    for row in rows[1:]:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) > latest_col_index:
                            item_name = cells[0].get_text(strip=True)
                            latest_value = cells[latest_col_index].get_text(strip=True)
                            
                            # 매출액
                            if '매출액' in item_name and not data['매출액']:
                                value_match = re.search(r'([,\d]+)', latest_value.replace(',', ''))
                                if value_match:
                                    data['매출액'] = value_match.group(1)
                            
                            # 영업이익
                            elif '영업이익' in item_name and not data['영업이익']:
                                value_match = re.search(r'([+-]?[,\d]+)', latest_value.replace(',', ''))
                                if value_match:
                                    data['영업이익'] = value_match.group(1)
                            
                            # 당기순이익
                            elif ('당기순이익' in item_name or '순이익' in item_name) and not data['당기순이익']:
                                value_match = re.search(r'([+-]?[,\d]+)', latest_value.replace(',', ''))
                                if value_match:
                                    data['당기순이익'] = value_match.group(1)
                            
                            # ROE
                            elif 'ROE' in item_name and not data['ROE']:
                                value_match = re.search(r'([+-]?\d+\.?\d*)', latest_value)
                                if value_match:
                                    data['ROE'] = value_match.group(1)
                            
                            # 부채비율
                            elif '부채비율' in item_name and not data['부채비율']:
                                value_match = re.search(r'([+-]?\d+\.?\d*)', latest_value)
                                if value_match:
                                    data['부채비율'] = value_match.group(1)
                            
                            # 유보율
                            elif '유보율' in item_name and not data['유보율']:
                                value_match = re.search(r'([+-]?\d+\.?\d*)', latest_value)
                                if value_match:
                                    data['유보율'] = value_match.group(1)
                            
                            # 배당금
                            elif '배당금' in item_name and not data['배당금']:
                                value_match = re.search(r'([,\d]+)', latest_value.replace(',', ''))
                                if value_match:
                                    data['배당금'] = value_match.group(1)
    
    except Exception as e:
        print(f"재무 데이터 추출 오류: {str(e)}")

def extract_investor_data(soup, data):
    """투자자별 매매동향 페이지에서 투자자 비율 및 거래 데이터 추출"""
    try:
        # 전체 텍스트에서 외국인/기관 비율 추출
        page_text = soup.get_text()
        
        # 외국인 소진율 또는 보유율
        foreign_patterns = [
            r'외국인소진율[^\d]*?([+-]?\d+\.?\d*)%',
            r'외국인보유율[^\d]*?([+-]?\d+\.?\d*)%',
            r'외국인[^\d]*?([+-]?\d+\.?\d*)%'
        ]
        
        for pattern in foreign_patterns:
            foreign_match = re.search(pattern, page_text)
            if foreign_match and not data['외국인비율']:
                data['외국인비율'] = foreign_match.group(1)
                break
        
        # 테이블에서 정확한 투자자 비율 찾기
        tables = soup.find_all('table')
        for table in tables:
            if '외국인' in table.get_text():
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        for i, cell in enumerate(cells[:-1]):
                            cell_text = cell.get_text(strip=True)
                            next_cell_text = cells[i+1].get_text(strip=True)
                            
                            # 외국인 비율
                            if '외국인' in cell_text and '%' in next_cell_text and not data['외국인비율']:
                                value_match = re.search(r'([+-]?\d+\.?\d*)', next_cell_text)
                                if value_match:
                                    data['외국인비율'] = value_match.group(1)
                            
                            # 기관 비율
                            elif '기관' in cell_text and '%' in next_cell_text and not data['기관비율']:
                                value_match = re.search(r'([+-]?\d+\.?\d*)', next_cell_text)
                                if value_match:
                                    data['기관비율'] = value_match.group(1)
    
    except Exception as e:
        print(f"투자자 데이터 추출 오류: {str(e)}")

def extract_price_data(soup, data):
    """메인 페이지에서 기본 시세 데이터 추출"""
    try:
        # 52주 최고/최저, 시가총액, 베타 등 추출
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    for i, cell in enumerate(cells[:-1]):
                        cell_text = cell.get_text(strip=True)
                        next_cell_text = cells[i+1].get_text(strip=True)
                        
                        # 52주 최고
                        if '52주최고' in cell_text and not data['52주최고']:
                            value_match = re.search(r'([,\d]+)', next_cell_text)
                            if value_match:
                                data['52주최고'] = value_match.group(1).replace(',', '')
                        
                        # 52주 최저
                        elif '52주최저' in cell_text and not data['52주최저']:
                            value_match = re.search(r'([,\d]+)', next_cell_text)
                            if value_match:
                                data['52주최저'] = value_match.group(1).replace(',', '')
                        
                        # 시가총액
                        elif '시가총액' in cell_text and not data['시가총액']:
                            value_match = re.search(r'([,\d]+)', next_cell_text)
                            if value_match:
                                data['시가총액'] = value_match.group(1).replace(',', '')
                        
                        # 베타
                        elif '베타' in cell_text and not data['베타']:
                            value_match = re.search(r'([+-]?\d+\.?\d*)', next_cell_text)
                            if value_match:
                                data['베타'] = value_match.group(1)
        
        # 거래대금 정보 (아직 없다면)
        if not data['거래대금']:
            page_text = soup.get_text()
            trading_match = re.search(r'거래대금[^\d]*([,\d]+)', page_text)
            if trading_match:
                data['거래대금'] = trading_match.group(1).replace(',', '')
    
    except Exception as e:
        print(f"시세 데이터 추출 오류: {str(e)}")

def extract_additional_finance_data(soup, data):
    """메인 페이지에서 추가 재무 정보 추출 (백업용)"""
    try:
        page_text = soup.get_text()
        
        # ROE 패턴 찾기
        if not data['ROE']:
            roe_patterns = [
                r'ROE[^\d]*?([+-]?\d+\.?\d*)%?',
                r'자기자본이익률[^\d]*?([+-]?\d+\.?\d*)%?'
            ]
            for pattern in roe_patterns:
                roe_match = re.search(pattern, page_text)
                if roe_match:
                    value = float(roe_match.group(1))
                    if -100 <= value <= 100:  # 합리적인 ROE 범위
                        data['ROE'] = roe_match.group(1)
                        break
        
        # 베타 정보 재시도
        if not data['베타']:
            # 테이블에서 베타 찾기
            tables = soup.find_all('table')
            for table in tables:
                if '베타' in table.get_text():
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all(['td', 'th'])
                        for i, cell in enumerate(cells[:-1]):
                            if '베타' in cell.get_text() and i+1 < len(cells):
                                beta_text = cells[i+1].get_text(strip=True)
                                beta_match = re.search(r'([+-]?\d+\.?\d*)', beta_text)
                                if beta_match:
                                    data['베타'] = beta_match.group(1)
                                    break
        
        # 시가총액 정정 (조 단위 변환 재확인)
        if data['시가총액']:
            # 현재 값이 조 단위인지 확인하고 변환
            cap_value = data['시가총액'].replace(',', '')
            if len(cap_value) <= 4:  # 조 단위로 추정
                data['시가총액'] = str(int(cap_value) * 10000)
    
    except Exception as e:
        print(f"추가 재무 데이터 추출 오류: {str(e)}")

def get_stock_data():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
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
            
            # 중간 저장 (5페이지마다, 로컬에만)
            if page % 5 == 0:
                temp_df = pd.DataFrame(stock_data)
                temp_filename = f'temp_stock_data_{datetime.now().strftime("%Y%m%d_%H%M")}_{page}.xlsx'
                temp_df.to_excel(temp_filename, index=False)
                print(f"중간 저장 (로컬): {temp_filename}")
    
    return stock_data

def main():
    print("=== 전체 종목 상세 데이터 수집 시작 ===")
    print("수집 데이터: 26개 필드 (재무지표, 투자자정보, 배당정보, 거래량증감율 등)")
    print("예상 소요시간: 3-4시간 (전체 종목 약 2000-3000개)")
    print("주의: 수집 중 중단하지 마세요. 5페이지마다 로컬에 중간 저장됩니다.\n")
    
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