import os
import sys
import glob
from datetime import datetime
import pandas as pd
from quick_stock_check import get_stock_data, is_regular_stock  # 기존 코드 활용
from rebound_strategies_analyzer import ReboundAnalyzer
from google_sheets_uploader import GoogleSheetsUploader

def get_latest_stock_data_file():
    """가장 최근에 생성된 주식 데이터 파일 찾기"""
    # 파일 패턴 설정 (stock_data_*.xlsx 또는 full_stock_data_*.xlsx)
    patterns = ['stock_data_*.xlsx', 'full_stock_data_*.xlsx']
    all_files = []
    
    for pattern in patterns:
        all_files.extend(glob.glob(pattern))
    
    if not all_files:
        return None
    
    # 파일 수정 시간 기준으로 정렬
    latest_file = max(all_files, key=os.path.getmtime)
    
    # 파일이 오늘 생성된 것인지 확인 (24시간 이내)
    file_mtime = os.path.getmtime(latest_file)
    current_time = datetime.now().timestamp()
    
    # 24시간(86400초) 이내에 생성된 파일이면 사용
    if current_time - file_mtime < 86400:
        return latest_file
    
    return None

def load_or_collect_stock_data():
    """최신 주식 데이터 파일을 로드하거나 새로 수집"""
    # 최신 파일 찾기
    latest_file = get_latest_stock_data_file()
    
    if latest_file:
        print(f"🔄 최신 데이터 파일 발견: {latest_file}")
        try:
            df = pd.read_excel(latest_file)
            print(f"✅ {len(df)}개 종목 데이터 로드 완료")
            
            # DataFrame을 딕셔너리 리스트로 변환
            stock_data = df.to_dict('records')
            
            # 필요한 필드가 있는지 확인
            if stock_data and '종목코드' in stock_data[0] and '종목명' in stock_data[0]:
                return stock_data
            else:
                print("⚠️ 파일 형식이 올바르지 않습니다. 새로 데이터를 수집합니다.")
        except Exception as e:
            print(f"⚠️ 파일 로드 중 오류 발생: {str(e)}. 새로 데이터를 수집합니다.")
    
    # 파일이 없거나 로드 실패시 새로 수집
    print("🔍 전체 종목 데이터 수집 중...")
    return get_stock_data()

def save_to_excel(results, strategy_name=None):
    """분석 결과를 엑셀 파일로 저장"""
    try:
        # 저장할 디렉토리 생성
        output_dir = 'results'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 현재 날짜로 파일명 생성
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        # 전략명이 지정된 경우 파일명에 포함
        if strategy_name:
            excel_file = os.path.join(output_dir, f'리바운드분석_{strategy_name}_{date_str}.xlsx')
        else:
            excel_file = os.path.join(output_dir, f'리바운드분석_{date_str}.xlsx')
        
        # 데이터 유효성 검사
        has_data = False
        for key in results.keys():
            if results[key] and len(results[key]) > 0:
                has_data = True
                break
        
        if not has_data:
            print("⚠️ 저장할 데이터가 없습니다.")
            return
        
        # 엑셀 파일 생성
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # 전략별 시트 생성
            strategies = {
                'volume_drop': '거래량급감',
                'ma45': '45일선',
                'ma360': '360일선'
            }
            
            for key, name in strategies.items():
                if results[key] and len(results[key]) > 0:
                    df = pd.DataFrame(results[key])
                    df.to_excel(writer, sheet_name=name, index=False)
            
            # 통합 시트 생성
            all_signals = []
            for key in strategies.keys():
                if results[key] and len(results[key]) > 0:
                    all_signals.extend(results[key])
            
            if all_signals:
                df_all = pd.DataFrame(all_signals)
                df_all.to_excel(writer, sheet_name='전체', index=False)
        
        print(f"✅ 엑셀 파일 저장 완료: {excel_file}")
        
    except Exception as e:
        print(f"❌ 엑셀 파일 저장 중 오류 발생: {str(e)}")

def run_volume_drop_strategy():
    """거래량 급감 전략만 실행"""
    print(f"🚀 거래량 급감 전략 분석 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    try:
        # 데이터 수집 또는 로드
        print("1. 주가 데이터 준비 중...")
        stock_data = load_or_collect_stock_data()
        
        # 리바운드 분석
        print("2. 거래량 급감 신호 분석 중...")
        analyzer = ReboundAnalyzer()
        
        # 데이터 필드 확인
        sample_data = stock_data[0] if stock_data else {}
        print(f"   데이터 필드: {', '.join(sample_data.keys())}")
        
        # 거래량 필드 매핑
        volume_field = '거래량'
        prev_volume_field = '전일거래량'
        
        # 필드 존재 확인
        if volume_field not in sample_data or prev_volume_field not in sample_data:
            print(f"⚠️ 필요한 거래량 필드가 없습니다: {volume_field}, {prev_volume_field}")
            return
        
        # 거래량 급감 전략만 분석
        count = 0
        signals = 0
        total = len(stock_data)
        
        for data in stock_data:
            count += 1
            if count % 100 == 0:
                print(f"   진행 중... {count}/{total} 종목 분석")
                
            if is_regular_stock(data['종목명']):
                # 거래량 급감 전략 분석
                if analyzer.analyze_volume_drop(data):
                    signals += 1
                    if signals % 10 == 0:
                        print(f"   {signals}개 신호 발견...")
        
        results = analyzer.get_results()
        
        # 구글 시트 업로드
        print("3. 구글 시트 업로드 중...")
        uploader = GoogleSheetsUploader()
        if uploader.setup_connection():
            # 시트 이름 설정
            today = datetime.now().strftime('%Y%m%d')
            sheet_name = f'거래량급감_{today}'
            
            # 결과가 있는 경우에만 업로드
            if results['volume_drop'] and len(results['volume_drop']) > 0:
                df = pd.DataFrame(results['volume_drop'])
                uploader.upload_dataframe(df, '주식', sheet_name)
                print(f"✅ 거래량급감 신호 업로드 완료")
            else:
                print(f"신호 없음: 거래량급감")
        
        # 엑셀 파일 저장
        print("4. 엑셀 파일 저장 중...")
        save_to_excel(results, '거래량급감')
        
        print("✅ 분석 완료!")
        print(f"- 거래량 급감 전략: {len(results['volume_drop'])}개 종목")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()

def run_ma45_strategy():
    """45일선 전략만 실행"""
    print(f"🚀 45일선 전략 분석 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    try:
        # 데이터 수집 또는 로드
        print("1. 주가 데이터 준비 중...")
        stock_data = load_or_collect_stock_data()
        
        # 리바운드 분석
        print("2. 45일선 신호 분석 중...")
        analyzer = ReboundAnalyzer()
        
        # 45일선 전략만 분석
        count = 0
        total = len(stock_data)
        
        for data in stock_data:
            count += 1
            if count % 50 == 0:
                print(f"  진행 중... {count}/{total} 종목 분석")
                
            if is_regular_stock(data['종목명']):
                # 과거 데이터 조회
                historical_data = analyzer.get_historical_data(data['종목코드'])
                if historical_data is not None and not historical_data.empty:
                    # 45일선 전략 분석
                    if analyzer.analyze_ma45(historical_data):
                        analyzer.results['ma45'].append({
                            'code': data['종목코드'],
                            'name': data['종목명'],
                            'strategy': '45일선',
                            'current_price': data['현재가'],
                            'ma45_price': historical_data.iloc[0]['MA45']
                        })
        
        results = analyzer.get_results()
        
        # 구글 시트 업로드
        print("3. 구글 시트 업로드 중...")
        uploader = GoogleSheetsUploader()
        if uploader.setup_connection():
            # 시트 이름 설정
            today = datetime.now().strftime('%Y%m%d')
            sheet_name = f'45일선_{today}'
            
            # 결과가 있는 경우에만 업로드
            if results['ma45'] and len(results['ma45']) > 0:
                df = pd.DataFrame(results['ma45'])
                uploader.upload_dataframe(df, '주식', sheet_name)
                print(f"✅ 45일선 신호 업로드 완료")
            else:
                print(f"신호 없음: 45일선")
        
        # 엑셀 파일 저장
        print("4. 엑셀 파일 저장 중...")
        save_to_excel(results, '45일선')
        
        print("✅ 분석 완료!")
        print(f"- 45일선 전략: {len(results['ma45'])}개 종목")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")

def run_ma360_strategy():
    """360일선 전략만 실행"""
    print(f"🚀 360일선 전략 분석 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    try:
        # 데이터 수집 또는 로드
        print("1. 주가 데이터 준비 중...")
        stock_data = load_or_collect_stock_data()
        
        # 리바운드 분석
        print("2. 360일선 신호 분석 중...")
        analyzer = ReboundAnalyzer()
        
        # 360일선 전략만 분석
        count = 0
        total = len(stock_data)
        
        for data in stock_data:
            count += 1
            if count % 50 == 0:
                print(f"  진행 중... {count}/{total} 종목 분석")
                
            if is_regular_stock(data['종목명']):
                # 과거 데이터 조회
                historical_data = analyzer.get_historical_data(data['종목코드'])
                if historical_data is not None and not historical_data.empty and len(historical_data) >= 360:
                    # 360일선 전략 분석
                    if analyzer.analyze_ma360(historical_data):
                        analyzer.results['ma360'].append({
                            'code': data['종목코드'],
                            'name': data['종목명'],
                            'strategy': '360일선',
                            'current_price': data['현재가'],
                            'ma360_price': historical_data.iloc[0]['MA360']
                        })
        
        results = analyzer.get_results()
        
        # 구글 시트 업로드
        print("3. 구글 시트 업로드 중...")
        uploader = GoogleSheetsUploader()
        if uploader.setup_connection():
            # 시트 이름 설정
            today = datetime.now().strftime('%Y%m%d')
            sheet_name = f'360일선_{today}'
            
            # 결과가 있는 경우에만 업로드
            if results['ma360'] and len(results['ma360']) > 0:
                df = pd.DataFrame(results['ma360'])
                uploader.upload_dataframe(df, '주식', sheet_name)
                print(f"✅ 360일선 신호 업로드 완료")
            else:
                print(f"신호 없음: 360일선")
        
        # 엑셀 파일 저장
        print("4. 엑셀 파일 저장 중...")
        save_to_excel(results, '360일선')
        
        print("✅ 분석 완료!")
        print(f"- 360일선 전략: {len(results['ma360'])}개 종목")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")

def run_all_strategies():
    """모든 리바운드 전략 실행"""
    print(f"🚀 전체 리바운드 전략 분석 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    
    try:
        # 데이터 수집 또는 로드
        print("1. 주가 데이터 준비 중...")
        stock_data = load_or_collect_stock_data()
        
        # 리바운드 분석
        print("2. 리바운드 신호 분석 중...")
        analyzer = ReboundAnalyzer()
        for data in stock_data:
            if is_regular_stock(data['종목명']):
                analyzer.analyze_stock(data)
        results = analyzer.get_results()
        
        # 구글 시트 업로드
        print("3. 구글 시트 업로드 중...")
        uploader = GoogleSheetsUploader()
        uploader.upload_rebound_signals(results)
        
        # 엑셀 파일 저장
        print("4. 엑셀 파일 저장 중...")
        save_to_excel(results)
        
        print("✅ 분석 완료!")
        
        # 결과 요약 출력
        print("\n📊 분석 결과 요약:")
        print(f"- 거래량 급감 전략: {len(results['volume_drop'])}개 종목")
        print(f"- 45일선 전략: {len(results['ma45'])}개 종목")
        print(f"- 360일선 전략: {len(results['ma360'])}개 종목")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")

def main():
    """명령줄 인수에 따라 특정 전략 실행"""
    # 명령줄 인수 확인
    if len(sys.argv) > 1:
        strategy = sys.argv[1].lower()
        
        if strategy == "1" or strategy == "volume" or strategy == "volume_drop":
            run_volume_drop_strategy()
        elif strategy == "2" or strategy == "ma45":
            run_ma45_strategy()
        elif strategy == "3" or strategy == "ma360":
            run_ma360_strategy()
        else:
            print("❌ 잘못된 전략 이름입니다.")
            print("사용법: python daily_rebound_analysis.py [전략번호 또는 이름]")
            print("  - 전략 1: 거래량급감 (volume_drop 또는 1)")
            print("  - 전략 2: 45일선 (ma45 또는 2)")
            print("  - 전략 3: 360일선 (ma360 또는 3)")
            print("  - 전체 전략: 인수 없이 실행")
    else:
        # 인수가 없으면 모든 전략 실행
        run_all_strategies()

if __name__ == "__main__":
    main() 