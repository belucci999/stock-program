import os
from datetime import datetime
import pandas as pd
from quick_stock_check import get_stock_data, is_regular_stock, GoogleSheetsUploader  # GoogleSheetsUploader도 가져옵니다
from rebound_strategies_analyzer import ReboundAnalyzer

def save_to_excel(results):
    """분석 결과를 엑셀 파일로 저장"""
    try:
        # 저장할 디렉토리 생성
        output_dir = 'results'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # 현재 날짜로 파일명 생성
        date_str = datetime.now().strftime('%Y-%m-%d')
        excel_file = os.path.join(output_dir, f'리바운드분석_{date_str}.xlsx')
        
        # 엑셀 파일 생성
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # 전략별 시트 생성
            strategies = {
                'volume_drop': '거래량급감',
                'ma45': '45일선',
                'ma360': '360일선'
            }
            
            for key, name in strategies.items():
                if results[key]:
                    df = pd.DataFrame(results[key])
                    df.to_excel(writer, sheet_name=name, index=False)
            
            # 통합 시트 생성
            all_signals = []
            for key in strategies.keys():
                all_signals.extend(results[key])
            
            if all_signals:
                df_all = pd.DataFrame(all_signals)
                df_all.to_excel(writer, sheet_name='전체', index=False)
        
        print(f"✅ 엑셀 파일 저장 완료: {excel_file}")
        
    except Exception as e:
        print(f"❌ 엑셀 파일 저장 중 오류 발생: {str(e)}")

def save_to_google_sheets(results):
    """리바운드 분석 결과를 구글 시트에 저장"""
    try:
        print("📤 구글 시트 업로드 시작...")
        uploader = GoogleSheetsUploader()
        
        if not uploader.gc:
            print("❌ 구글 시트 연결에 실패했습니다.")
            return False
        
        # 동일한 구글 시트 사용 (quick_stock_check.py와 동일)
        spreadsheet_name = "주식"
        
        # 현재 날짜와 시간으로 시트명 생성
        current_date = datetime.now().strftime('%Y-%m-%d')
        sheet_name = f"🔍_리바운드_{current_date}_{datetime.now().strftime('%H%M')}"
        
        # 결과를 데이터프레임으로 변환
        strategies = {
            'volume_drop': '거래량급감',
            'ma45': '45일선',
            'ma360': '360일선'
        }
        
        # 모든 결과를 하나의 데이터프레임으로 통합
        all_signals = []
        for key, name in strategies.items():
            for signal in results[key]:
                # 전략 이름 추가
                signal['전략'] = name
                all_signals.append(signal)
        
        if not all_signals:
            print("📊 발견된 리바운드 신호가 없습니다.")
            return False
        
        # 데이터프레임 생성
        df = pd.DataFrame(all_signals)
        
        # 컬럼 순서 정리 (종목코드, 종목명, 전략을 앞으로)
        if '종목코드' in df.columns and '종목명' in df.columns and '전략' in df.columns:
            first_columns = ['종목코드', '종목명', '전략']
            other_columns = [col for col in df.columns if col not in first_columns]
            df = df[first_columns + other_columns]
        
        # 구글 시트에 업로드
        success = uploader.upload_dataframe(df, spreadsheet_name, sheet_name)
        
        if success:
            uploader.format_headers(success, sheet_name)
            print(f"📊 구글 시트 링크: {success}")
            print("✅ 구글 시트 업로드 완료!")
            print(f"📋 시트명: {sheet_name}")
            return True
        else:
            print("❌ 구글 시트 업로드에 실패했습니다.")
            return False
        
    except Exception as e:
        print(f"❌ 구글 시트 업로드 중 오류 발생: {str(e)}")
        return False

def main():
    print(f"🚀 일일 리바운드 분석 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print("📌 분석 전략: 거래량 급감, 45일선(세력선), 360일선(최후의 수비선)")
    print("=" * 70)
    
    try:
        # 데이터 수집 (quick_stock_check.py의 함수 사용)
        print("1. 주가 데이터 수집 중...")
        stock_data = get_stock_data()
        
        # 리바운드 분석
        print("2. 리바운드 신호 분석 중...")
        print("   - 거래량 급감 전략: 폭등(500%↑) 후 급감(85%↓), 음봉 발생")
        print("   - 45일선 전략: 급등 후 하락, 거래량 감소, 45일선 첫 터치")
        print("   - 360일선 전략: 우상향 추세, 이탈 후 회복, 지지 확인")
        
        analyzer = ReboundAnalyzer()
        for code, data in stock_data.items():
            if is_regular_stock(data['name']):  # 일반 주식만 분석
                analyzer.analyze_stock(data)
        results = analyzer.get_results()
        
        # 구글 시트 업로드 (quick_stock_check.py와 동일한 시트 사용)
        print("3. 구글 시트 업로드 중...")
        save_to_google_sheets(results)
        
        # 엑셀 파일 저장
        print("4. 엑셀 파일 저장 중...")
        save_to_excel(results)
        
        print("✅ 분석 완료!")
        
        # 결과 요약 출력
        print("\n📊 분석 결과 요약:")
        print(f"- 거래량 급감 전략: {len(results['volume_drop'])}개 종목")
        if results['volume_drop']:
            high_reliability = sum(1 for item in results['volume_drop'] if item.get('reliability') == 'HIGH')
            print(f"  └ 신뢰도 높음: {high_reliability}개, 신뢰도 중간: {len(results['volume_drop']) - high_reliability}개")
        
        print(f"- 45일선 전략: {len(results['ma45'])}개 종목")
        if results['ma45'] and len(results['ma45']) > 0:
            avg_surge = sum(item.get('surge_pct', 0) for item in results['ma45']) / len(results['ma45'])
            print(f"  └ 평균 급등률: {avg_surge:.2f}%")
        
        print(f"- 360일선 전략: {len(results['ma360'])}개 종목")
        if results['ma360'] and len(results['ma360']) > 0:
            avg_recovery = sum(item.get('recovery_strength', 0) for item in results['ma360']) / len(results['ma360'])
            print(f"  └ 평균 회복 강도: {avg_recovery:.2f}%")
        
        # 총 발견 종목 수
        total_stocks = len(set([item['code'] for strategy in results.values() for item in strategy]))
        print(f"\n총 {total_stocks}개 종목에서 리바운드 신호 발견")
        
        # 다중 전략 해당 종목 찾기
        multi_strategy_stocks = {}
        for strategy, items in results.items():
            for item in items:
                code = item['code']
                if code not in multi_strategy_stocks:
                    multi_strategy_stocks[code] = []
                multi_strategy_stocks[code].append(strategy)
        
        # 2개 이상 전략에 해당하는 종목 표시
        multi_stocks = {k: v for k, v in multi_strategy_stocks.items() if len(v) >= 2}
        if multi_stocks:
            print("\n🔥 다중 전략 해당 종목 (높은 신뢰도):")
            for code, strategies in multi_stocks.items():
                # 종목명 찾기
                name = ""
                for strategy in results.values():
                    for item in strategy:
                        if item['code'] == code:
                            name = item['name']
                            break
                    if name:
                        break
                
                strategies_str = ", ".join([{
                    'volume_drop': '거래량급감',
                    'ma45': '45일선',
                    'ma360': '360일선'
                }[s] for s in strategies])
                
                print(f"  - {name} ({code}): {strategies_str}")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    main() 