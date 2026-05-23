import argparse
import os
import sys
import glob
from datetime import datetime
import pandas as pd
from quick_stock_check import get_stock_data, is_regular_stock  # 기존 코드 활용
from rebound_strategies_analyzer import ReboundAnalyzer
from google_sheets_uploader import GoogleSheetsUploader
from market_calendar import resolve_sheet_tab

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

STRATEGY_LABELS = {
    'volume_drop': '거래량 급감',
    'ma45': '45일선',
    'ma360': '360일선',
}


def _print_rebound_summary(results):
    print("\n📊 분석 결과 요약:")
    print(f"- 거래량 급감 전략: {len(results['volume_drop'])}개 종목")
    print(f"- 45일선 전략: {len(results['ma45'])}개 종목")
    print(f"- 360일선 전략: {len(results['ma360'])}개 종목")


def _run_rebound_analysis(strategies, sheet_tab=None, excel_suffix=None, title="리바운드"):
    """지정 전략을 전 종목에 대해 실행 후 구글 시트·엑셀 저장."""
    tab = resolve_sheet_tab(sheet_tab)
    labels = [STRATEGY_LABELS[s] for s in strategies]
    print(f"🚀 {title} 분석 시작 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    print(f"   전략: {', '.join(labels)}")
    print(f"   구글 시트 탭 접두: {tab}")

    print("1. 주가 데이터 준비 중...")
    stock_data = load_or_collect_stock_data()

    print("2. 리바운드 신호 분석 중...")
    analyzer = ReboundAnalyzer()
    total = len(stock_data)
    for i, data in enumerate(stock_data, start=1):
        if i % 50 == 0:
            print(f"   ... 진행 {i}/{total}")
        if is_regular_stock(data.get('종목명', '')):
            analyzer.analyze_stock(data, strategies=strategies)

    results = analyzer.get_results()

    print("3. 구글 시트 업로드 중...")
    uploader = GoogleSheetsUploader()
    if getattr(uploader, 'gc', None):
        uploader.upload_rebound_signals(results, date_str=tab, strategy_keys=strategies)
    else:
        print("⚠️ 구글 시트 연결 실패 — 엑셀만 저장합니다.")

    print("4. 엑셀 파일 저장 중...")
    save_to_excel(results, excel_suffix)

    print("✅ 분석 완료!")
    _print_rebound_summary(results)
    return results


def run_volume_drop_strategy(sheet_tab: str | None = None):
    """거래량 급감 전략만 실행"""
    try:
        _run_rebound_analysis(
            ('volume_drop',),
            sheet_tab=sheet_tab,
            excel_suffix='거래량급감',
            title='거래량 급감',
        )
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()


def run_ma45_strategy(sheet_tab: str | None = None):
    """45일선 전략만 실행"""
    try:
        _run_rebound_analysis(
            ('ma45',),
            sheet_tab=sheet_tab,
            excel_suffix='45일선',
            title='45일선',
        )
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")


def run_ma360_strategy(sheet_tab: str | None = None):
    """360일선 전략만 실행"""
    try:
        _run_rebound_analysis(
            ('ma360',),
            sheet_tab=sheet_tab,
            excel_suffix='360일선',
            title='360일선',
        )
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")


def run_all_strategies(sheet_tab: str | None = None):
    """모든 리바운드 전략 실행 (거래량 급감·45일선·360일선)"""
    try:
        _run_rebound_analysis(
            ('volume_drop', 'ma45', 'ma360'),
            sheet_tab=sheet_tab,
            excel_suffix=None,
            title='전체 리바운드',
        )
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")

def main():
    """명령줄 인수에 따라 특정 전략 실행"""
    parser = argparse.ArgumentParser(description="리바운드 전략 분석")
    parser.add_argument(
        "strategy",
        nargs="?",
        default="",
        help="volume_drop(1), ma45(2), ma360(3). 생략 시 전체",
    )
    parser.add_argument(
        "--sheet-tab",
        default="",
        help="구글 시트 탭 접두 YYYY-MM-DD (미지정 시 오늘 또는 직전 거래일)",
    )
    args = parser.parse_args()
    tab = resolve_sheet_tab(args.sheet_tab or None)

    strategy = (args.strategy or "").lower()
    if not strategy:
        run_all_strategies(sheet_tab=tab)
    elif strategy in ("1", "volume", "volume_drop"):
        run_volume_drop_strategy(sheet_tab=tab)
    elif strategy in ("2", "ma45"):
        run_ma45_strategy(sheet_tab=tab)
    elif strategy in ("3", "ma360"):
        run_ma360_strategy(sheet_tab=tab)
    else:
        print("❌ 잘못된 전략 이름입니다.")
        print("사용법: python daily_rebound_analysis.py [전략] [--sheet-tab YYYY-MM-DD]")
        print("  - 전략 1: 거래량급감 (volume_drop 또는 1)")
        print("  - 전략 2: 45일선 (ma45 또는 2)")
        print("  - 전략 3: 360일선 (ma360 또는 3)")
        print("  - 전체 전략: 인수 없이 실행")

if __name__ == "__main__":
    main() 