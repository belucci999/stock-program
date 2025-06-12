import requests
from bs4 import BeautifulSoup

def analyze_page_structure(code):
    """네이버 금융 외국인 거래 페이지의 HTML 구조 분석"""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        # 외국인 거래 페이지 URL
        url = f'https://finance.naver.com/item/frgn.naver?code={code}'
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 페이지 인코딩 확인
        print(f"페이지 인코딩: {response.encoding}")
        
        # 테이블 찾기
        tables = soup.select('table')
        print(f"페이지 내 테이블 수: {len(tables)}")
        
        # 각 테이블의 클래스와 ID 출력
        for i, table in enumerate(tables):
            table_class = table.get('class', ['없음'])
            table_id = table.get('id', '없음')
            print(f"테이블 {i+1}: class={table_class}, id={table_id}")
            
            # 테이블의 행(tr) 수 확인
            rows = table.select('tr')
            print(f"  - 행(TR) 수: {len(rows)}")
            
            # 머리글(th) 확인
            headers = table.select('th')
            if headers:
                print(f"  - 머리글(TH): {', '.join([h.text.strip() for h in headers[:5]])}")
            
            # 첫 번째 행 구조 확인
            if rows:
                first_row = rows[0]
                cells = first_row.select('td')
                print(f"  - 첫 번째 행 셀(TD) 수: {len(cells)}")
                if cells:
                    print(f"  - 첫 번째 행 내용: {' | '.join([c.text.strip()[:10] for c in cells[:5]])}")
        
        # 특별히 거래량 데이터가 있을 것 같은 테이블 확인
        volume_tables = []
        for i, table in enumerate(tables):
            headers = [h.text.strip() for h in table.select('th')]
            if any('거래량' in h for h in headers) or any('기관' in h for h in headers) or any('외국인' in h for h in headers):
                volume_tables.append(i)
        
        print(f"\n거래량 관련 테이블 인덱스: {volume_tables}")
        
        # 거래량 관련 테이블의 상세 분석
        if volume_tables:
            for idx in volume_tables:
                table = tables[idx]
                print(f"\n=== 거래량 관련 테이블 {idx+1} 상세 분석 ===")
                
                # 모든 헤더 텍스트 출력
                headers = table.select('th')
                header_texts = [h.text.strip() for h in headers]
                print(f"헤더 텍스트: {header_texts}")
                
                # 데이터 행 분석
                rows = table.select('tr:not(:first-child)')  # 헤더 행 제외
                print(f"데이터 행 수: {len(rows)}")
                
                # 첫 5개 행 데이터 출력
                for i, row in enumerate(rows[:5]):
                    cells = row.select('td')
                    cell_texts = [c.text.strip() for c in cells]
                    print(f"행 {i+1}: {cell_texts[:10]}")
        
        # HTML 파일로 저장하여 분석용
        with open(f'page_structure_{code}.html', 'w', encoding='utf-8') as f:
            f.write(soup.prettify())
        print(f"\nHTML 구조를 page_structure_{code}.html 파일로 저장했습니다.")
        
    except Exception as e:
        print(f"오류 발생: {e}")

# 테스트
analyze_page_structure('005930')  # 삼성전자 