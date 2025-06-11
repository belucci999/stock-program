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
        'TOP', 'Plus', '커버드콜', '인버스', '레버리지',
        'ESG', 'MZ', 'AI', 'K-', '글로벌', '미국', '중국', '일본', '유럽',
        'S&P', 'MSCI', 'NASDAQ', 'NYSE', 'FTSE', 'STOXX'
    ]
    
    # 1. ETF/펀드 브랜드명 체크
    for brand in etf_brands:
        if brand in name:
            return False
    
    # 2. 펀드 키워드 체크
    for keyword in fund_keywords:
        if keyword in name:
            return False
    
    return True

def get_stock_list():
    """네이버 금융에서 전체 주식 종목 리스트 가져오기"""
    
    kospi_stocks = []
    kosdaq_stocks = []
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    # KOSPI 종목들
    print("KOSPI 종목 수집 중...")
    page = 1
    while True:
        url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok=0&page={page}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = soup.select('table.type_2 tr')
        page_stocks = []
        
        for row in rows[2:]:  # 헤더 제외
            cells = row.select('td')
            if len(cells) > 1:
                name_cell = cells[1].select_one('a')
                if name_cell:
                    name = name_cell.text.strip()
                    href = name_cell.get('href', '')
                    
                    if 'code=' in href:
                        code = href.split('code=')[1].split('&')[0]
                        
                        if is_regular_stock(name):
                            kospi_stocks.append({'name': name, 'code': code, 'market': 'KOSPI'})
                            page_stocks.append(name)
        
        if not page_stocks:
            break
        
        print(f"  KOSPI 페이지 {page}: {len(page_stocks)}개 종목")
        page += 1
        time.sleep(0.1)
    
    # KOSDAQ 종목들
    print("\nKOSDAQ 종목 수집 중...")
    page = 1
    while True:
        url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page={page}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rows = soup.select('table.type_2 tr')
        page_stocks = []
        
        for row in rows[2:]:  # 헤더 제외
            cells = row.select('td')
            if len(cells) > 1:
                name_cell = cells[1].select_one('a')
                if name_cell:
                    name = name_cell.text.strip()
                    href = name_cell.get('href', '')
                    
                    if 'code=' in href:
                        code = href.split('code=')[1].split('&')[0]
                        
                        if is_regular_stock(name):
                            kosdaq_stocks.append({'name': name, 'code': code, 'market': 'KOSDAQ'})
                            page_stocks.append(name)
        
        if not page_stocks:
            break
        
        print(f"  KOSDAQ 페이지 {page}: {len(page_stocks)}개 종목")
        page += 1
        time.sleep(0.1)
    
    all_stocks = kospi_stocks + kosdaq_stocks
    print(f"\n총 {len(all_stocks)}개 종목 수집 완료")
    print(f"KOSPI: {len(kospi_stocks)}개, KOSDAQ: {len(kosdaq_stocks)}개")
    
    return all_stocks

def get_individual_stock_data(code, name):
    """개별 종목 페이지에서 상세 재무 데이터 수집 - 마이너스 값 처리 개선"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        url = f'https://finance.naver.com/item/main.naver?code={code}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 초기화 (17개 데이터 필드 - 업종 제외)
        data = {
            'PER': '', 'PBR': '', 'ROE': '', '시가총액': '',
            '매출액': '', '영업이익': '', '당기순이익': '', 
            '부채비율': '', '유보율': '', '배당수익률': '', '배당금': '',
            '52주최고': '', '52주최저': '', '베타': '',
            '외국인비율': '', '기관비율': '', '거래대금': '', '거래량증감률': '',
            '업종': '', '현재가': '', '전일종가': '', '거래량': ''
        }
        
        # 업종 정보 추출
        try:
            industry_info = soup.select_one('div.trade_compare > h4 > div > span.txt')
            if industry_info:
                data['업종'] = industry_info.text.strip()
        except Exception as e:
            print(f"  업종 정보 추출 중 오류: {e}")
            
        # 현재가, 전일종가, 거래량 추출
        try:
            # 현재가
            current_price = soup.select_one('div.rate_info div.today span.blind')
            if current_price:
                data['현재가'] = current_price.text.replace(',', '')
                
            # 전일종가
            prev_price = soup.select_one('table.no_info td.first span.blind')
            if prev_price:
                data['전일종가'] = prev_price.text.replace(',', '')
                
            # 거래량
            volume = soup.select_one('div.rate_info table.no_info td span.blind')
            if volume:
                data['거래량'] = volume.text.replace(',', '')
                
            # 거래량증감률 계산
            if data['거래량'] and data['전일종가']:
                # 이전 거래량 추출
                prev_volume_elem = soup.select('table.no_info td')[2].select_one('span.blind')
                if prev_volume_elem:
                    prev_volume = prev_volume_elem.text.replace(',', '')
                    try:
                        curr_vol = int(data['거래량'])
                        prev_vol = int(prev_volume)
                        if prev_vol > 0:
                            volume_change_pct = ((curr_vol - prev_vol) / prev_vol) * 100
                            data['거래량증감률'] = f"{volume_change_pct:.2f}%"
                    except:
                        pass
        except Exception as e:
            print(f"  가격/거래량 정보 추출 중 오류: {e}")
        
        # 재무 데이터 추출 - 마이너스 값 처리 개선
        invest_tables = soup.select('table')
        for table in invest_tables:
            rows = table.select('tr')
            for row in rows:
                cells = row.select('td, th')
                if len(cells) >= 2:
                    header = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    
                    # 재무 지표 매핑 (마이너스 값 처리 개선)
                    if 'PER' in header and 'PER' not in value:
                        extracted = extract_financial_value(value)
                        if extracted and extracted != '-':
                            data['PER'] = extracted
                    elif 'PBR' in header and 'PBR' not in value:
                        extracted = extract_financial_value(value)
                        if extracted and extracted != '-':
                            data['PBR'] = extracted
                    elif 'ROE' in header and 'ROE' not in value:
                        extracted = extract_financial_value(value)
                        if extracted and extracted != '-':
                            data['ROE'] = extracted + '%' if extracted else ''
                    elif '시가총액' in header:
                        data['시가총액'] = value
                    elif '매출액' in header:
                        data['매출액'] = extract_financial_value(value)
                    elif '영업이익' in header:
                        data['영업이익'] = extract_financial_value(value)
                    elif '당기순이익' in header:
                        data['당기순이익'] = extract_financial_value(value)
                    elif '부채비율' in header:
                        data['부채비율'] = extract_financial_value(value)
                    elif '유보율' in header:
                        data['유보율'] = extract_financial_value(value)
                    elif '배당수익률' in header:
                        data['배당수익률'] = extract_financial_value(value)
                    elif '배당금' in header:
                        data['배당금'] = extract_financial_value(value)
                    elif '52주최고' in header:
                        data['52주최고'] = extract_financial_value(value)
                    elif '52주최저' in header:
                        data['52주최저'] = extract_financial_value(value)
                    elif '외국인' in header and '비율' in header:
                        data['외국인비율'] = extract_financial_value(value)
                    elif '기관' in header and '비율' in header:
                        data['기관비율'] = extract_financial_value(value)
                    elif '거래대금' in header:
                        data['거래대금'] = value
        
        return data
        
    except Exception as e:
        print(f"  오류 발생 ({name}): {e}")
        return {k: '' for k in data.keys()}

def clean_dataframe(df):
    """데이터프레임의 NaN 값을 빈 문자열로 변환"""
    return df.fillna('')

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
            
            if os.path.exists(self.credentials_file):
                creds = Credentials.from_service_account_file(self.credentials_file, scopes=scope)
                self.gc = gspread.authorize(creds)
                print("✅ 구글 시트 연결 성공!")
                return True
            else:
                print(f"❌ 인증 파일을 찾을 수 없습니다: {self.credentials_file}")
                return False
                
        except Exception as e:
            print(f"❌ 구글 시트 연결 실패: {e}")
            return False
    
    def upload_dataframe(self, df, spreadsheet_name, sheet_name):
        """데이터프레임을 구글 시트에 업로드"""
        try:
            if not self.gc:
                print("❌ 구글 시트 연결이 없습니다.")
                return False
            
            # 기존 스프레드시트 열기 또는 생성
            try:
                spreadsheet = self.gc.open(spreadsheet_name)
            except gspread.SpreadsheetNotFound:
                spreadsheet = self.gc.create(spreadsheet_name)
                print(f"📝 새 스프레드시트 생성: {spreadsheet_name}")
            
            # 새 시트 추가
            try:
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=len(df)+10, cols=len(df.columns)+5)
            except Exception as e:
                try:
                    worksheet = spreadsheet.worksheet(sheet_name)
                    worksheet.clear()
                except:
                    sheet_name = f"{sheet_name}_{datetime.now().strftime('%H%M%S')}"
                    worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=len(df)+10, cols=len(df.columns)+5)
            
            # 데이터프레임 정리
            clean_df = clean_dataframe(df)
            headers = clean_df.columns.tolist()
            data_rows = clean_df.values.tolist()
            
            # 모든 데이터를 문자열로 변환
            string_data_rows = []
            for row in data_rows:
                string_row = [str(cell) if cell is not None else '' for cell in row]
                string_data_rows.append(string_row)
            
            # 헤더 업로드
            worksheet.update('A1', [headers])
            
            # 데이터 업로드
            if string_data_rows:
                worksheet.update(f'A2:Z{len(string_data_rows)+1}', string_data_rows)
            
            print(f"✅ '{sheet_name}' 시트에 {len(df)}개 행 업로드 완료!")
            return spreadsheet.url
            
        except Exception as e:
            print(f"❌ 구글 시트 업로드 실패: {e}")
            return False
    
    def format_headers(self, spreadsheet_url, sheet_name):
        """헤더 포맷 적용"""
        try:
            spreadsheet = self.gc.open_by_url(spreadsheet_url)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            worksheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            print(f"✅ '{sheet_name}' 시트 헤더 포맷 적용 완료!")
            return True
            
        except Exception as e:
            print(f"❌ 헤더 포맷 적용 실패: {e}")
            return False

def get_stock_data():
    """전체 주식 데이터 수집 메인 함수 - 마이너스 값 처리 개선"""
    
    print("🚀 전체 종목 데이터 수집을 시작합니다!")
    print("📝 마이너스 값 처리가 개선된 버전입니다 (괄호 형태 포함)")
    print("예상 소요시간: 30-40분 (전체 종목 약 2500-3000개)")
    print("=" * 50)
    
    start_time = datetime.now()
    
    # 1. 전체 종목 리스트 가져오기
    all_stocks = get_stock_list()
    
    if not all_stocks:
        print("❌ 종목 리스트를 가져올 수 없습니다.")
        return
    
    # 2. 각 종목의 상세 데이터 수집
    detailed_data = []
    total_stocks = len(all_stocks)
    
    stocks_per_page = 60
    current_page = 0
    processed_count = 0
    
    print(f"\n📊 총 {total_stocks}개 종목의 상세 데이터 수집 시작...")
    
    for i, stock in enumerate(all_stocks):
        try:
            if i % stocks_per_page == 0:
                current_page = i // stocks_per_page + 1
                total_pages = (total_stocks + stocks_per_page - 1) // stocks_per_page
                print(f"\n페이지 {current_page}/{total_pages} 수집 중...")
            
            # 개별 종목 데이터 수집
            stock_data = get_individual_stock_data(stock['code'], stock['name'])
            
            # 필요한 필드 추출
            업종 = stock_data.pop('업종', '')
            현재가 = stock_data.pop('현재가', '')
            전일종가 = stock_data.pop('전일종가', '')
            거래량 = stock_data.pop('거래량', '')
            거래량증감률 = stock_data.pop('거래량증감률', '')
            
            # 기본 정보와 상세 데이터 결합
            combined_data = {
                '종목명': stock['name'],
                '종목코드': stock['code'],
                '시장구분': stock['market'],
                '업종': 업종,
                '현재가': 현재가,
                '전일종가': 전일종가,
                '거래량': 거래량,
                '거래량증감률': 거래량증감률,
                **stock_data
            }
            
            detailed_data.append(combined_data)
            processed_count += 1
            
            # 진행상황 출력 (마이너스 값 확인용)
            if processed_count % 10 == 0:
                profit = stock_data.get('영업이익', '')
                net_income = stock_data.get('당기순이익', '')
                print(f"  - {processed_count}개 종목 수집 완료 (최근: {stock['name']} - 영업이익: {profit}, 당기순이익: {net_income})")
            
            # 10페이지(600개 종목)마다 임시 파일 저장
            if processed_count % 600 == 0 and processed_count > 0:
                temp_df = pd.DataFrame(detailed_data)
                
                # 컬럼 순서 정리
                column_order = [
                    '종목명', '종목코드', '시장구분', '업종', '현재가', '전일종가', '거래량', '거래량증감률',
                    'PER', 'PBR', 'ROE', '시가총액', '거래대금',
                    '매출액', '영업이익', '당기순이익', '부채비율', '유보율',
                    '배당수익률', '배당금', '52주최고', '52주최저', '베타',
                    '외국인비율', '기관비율'
                ]
                
                for col in column_order:
                    if col not in temp_df.columns:
                        temp_df[col] = ''
                
                temp_df = temp_df[column_order]
                
                # 임시 파일 저장
                temp_timestamp = datetime.now().strftime('%Y%m%d_%H%M')
                temp_filename = f'stock_data_temp_{processed_count}_{temp_timestamp}.xlsx'
                temp_df.to_excel(temp_filename, index=False)
                print(f"\n💾 임시 파일 저장 완료: {temp_filename} ({processed_count}개 종목)")
            
            time.sleep(0.3)
            
        except Exception as e:
            print(f"  오류 발생 ({stock['name']}): {e}")
            continue
    
    if not detailed_data:
        print("❌ 수집된 데이터가 없습니다.")
        return
    
    # 3. 데이터프레임 생성
    df = pd.DataFrame(detailed_data)
    
    # 4. 컬럼 순서 정리
    column_order = [
        '종목명', '종목코드', '시장구분', '업종', '현재가', '전일종가', '거래량', '거래량증감률',
        'PER', 'PBR', 'ROE', '시가총액', '거래대금',
        '매출액', '영업이익', '당기순이익', '부채비율', '유보율',
        '배당수익률', '배당금', '52주최고', '52주최저', '베타',
        '외국인비율', '기관비율'
    ]
    
    for col in column_order:
        if col not in df.columns:
            df[col] = ''
    
    df = df[column_order]
    
    # 5. Excel 파일 저장
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    filename = f'stock_data_fixed_minus_{timestamp}.xlsx'
    df.to_excel(filename, index=False)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n🎉 수집 완료!")
    print(f"총 {len(df)}개 종목의 데이터가 {filename}에 저장되었습니다.")
    print(f"소요시간: {duration}")
    
    # 6. 마이너스 값 검증
    print("\n🔍 마이너스 값 검증:")
    
    # 영업이익 마이너스 개수
    negative_operating = df[df['영업이익'].astype(str).str.startswith('-')].shape[0]
    print(f"영업손실 기업 수: {negative_operating}개")
    
    # 당기순이익 마이너스 개수  
    negative_net = df[df['당기순이익'].astype(str).str.startswith('-')].shape[0]
    print(f"순손실 기업 수: {negative_net}개")
    
    # 샘플 손실 기업 출력
    loss_companies = df[(df['영업이익'].astype(str).str.startswith('-')) | 
                       (df['당기순이익'].astype(str).str.startswith('-'))].head(5)
    if not loss_companies.empty:
        print(f"\n📋 손실 기업 샘플:")
        for _, row in loss_companies.iterrows():
            print(f"  - {row['종목명']}: 영업이익={row['영업이익']}, 당기순이익={row['당기순이익']}")
    
    # 7. 데이터 품질 체크
    print("\n📊 데이터 품질:")
    quality_fields = ['PER', 'PBR', 'ROE', '시가총액', '매출액', '배당수익률']
    for field in quality_fields:
        non_empty = df[df[field] != ''].shape[0]
        print(f"{field} 데이터 있는 종목: {non_empty}/{len(df)}")
    
    return df

def main():
    """메인 실행 함수"""
    try:
        # 데이터 수집
        df = get_stock_data()
        
        if df is None or df.empty:
            print("❌ 데이터 수집에 실패했습니다.")
            return
        
        # 구글 시트 업로드
        print(f"\n📤 구글 시트 업로드 시작...")
        uploader = GoogleSheetsUploader()
        
        if uploader.gc:
            spreadsheet_name = "주식"
            current_date = datetime.now().strftime('%Y-%m-%d')
            sheet_name = f"💾_수정된데이터_{current_date}_{datetime.now().strftime('%H%M')}"
            
            success = uploader.upload_dataframe(df, spreadsheet_name, sheet_name)
            
            if success:
                uploader.format_headers(success, sheet_name)
                print(f"📊 구글 시트 링크: {success}")
                print("✅ 구글 시트 업로드 완료!")
                print(f"📋 시트명: {sheet_name}")
            else:
                print("❌ 구글 시트 업로드에 실패했습니다.")
        else:
            print("❌ 구글 시트 연결에 실패했습니다.")
            print("💡 Excel 파일로만 저장되었습니다.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()