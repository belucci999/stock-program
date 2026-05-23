#!/usr/bin/env python3
from console_utf8 import enable as enable_utf8_console

enable_utf8_console()

import argparse
import subprocess
import sys
import time
import os
import glob
import pandas as pd
from datetime import datetime
from google_sheets_uploader import GoogleSheetsUploader
from market_calendar import resolve_sheet_tab
from stock_data_utils import fill_trading_amounts_df


def run_python_script(script_name, description, extra_args=None):
    """Python 스크립트 실행"""
    print(f"\n{'='*60}")
    print(f"[시작] {description}")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    try:
        python_path = sys.executable
        script_path = os.path.join(os.getcwd(), script_name)

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"

        cmd = [python_path, script_path]
        if extra_args:
            cmd.extend(extra_args)

        result = subprocess.run(
            cmd,
            cwd=os.getcwd(),
            env=env,
        )

        if result.returncode == 0:
            print(f"[완료] {description}")
            return True
        print(f"[실패] {description} (종료 코드: {result.returncode})")
        return False

    except Exception as e:
        print(f"[오류] {description} 실행 중: {str(e)}")
        return False


def find_latest_files():
    """최신 생성된 파일들 찾기"""
    today = datetime.now().strftime('%Y%m%d')

    stock_data_files = glob.glob(f'full_stock_data*{today}*.xlsx')
    analysis_files = glob.glob(f'contrarian_stocks*{today}*.xlsx')

    if not stock_data_files:
        stock_data_files = glob.glob('full_stock_data*.xlsx')
    if not analysis_files:
        analysis_files = glob.glob('contrarian_stocks*.xlsx')

    latest_stock_data = max(stock_data_files, key=os.path.getctime) if stock_data_files else None
    latest_analysis = max(analysis_files, key=os.path.getctime) if analysis_files else None

    return latest_stock_data, latest_analysis


def upload_to_google_sheets(stock_data_file, analysis_file, sheet_tab: str):
    """
    구글 시트에 날짜별 탭(YYYY-MM-DD)으로 업로드.
    같은 날 다시 실행하면 해당 탭을 비운 뒤 덮어씀.
    """
    print("\n[구글 시트] 업로드 시작...")

    uploader = GoogleSheetsUploader()
    if not getattr(uploader, "gc", None):
        print("[오류] 구글 시트 연결 실패 (credentials/google-sa.json, .env SPREADSHEET_ID 확인)")
        return False

    date_tab = sheet_tab
    run_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    contrarian_count = 0
    stock_count = 0

    sections = []

    try:
        summary_rows = {
            '항목': [
                '분석 일시',
                '업로드 시간',
                '전체 종목 수',
                '역발상 후보 종목 수',
                '데이터 파일',
                '분석 파일',
            ],
            '값': [
                run_time,
                run_time,
                '수집 중',
                '분석 중',
                os.path.basename(stock_data_file) if stock_data_file else 'N/A',
                os.path.basename(analysis_file) if analysis_file else 'N/A',
            ],
        }

        if stock_data_file and os.path.exists(stock_data_file):
            print(f"[업로드] 전체 주식 데이터: {stock_data_file}")
            df_stock = fill_trading_amounts_df(pd.read_excel(stock_data_file))
            stock_count = len(df_stock)

            df_stock_sorted = df_stock.sort_values(
                ['ROE', '시가총액'], ascending=[False, False], na_position='last'
            )

            sections.append(('--- 분석 요약 ---', pd.DataFrame(summary_rows)))
            sections.append(('--- 전체주식 TOP500 (ROE·시총액) ---', df_stock_sorted.head(500)))

            if '시장구분' in df_stock.columns:
                kospi = df_stock_sorted[df_stock_sorted['시장구분'] == '코스피'].head(200)
                kosdaq = df_stock_sorted[df_stock_sorted['시장구분'] == '코스닥'].head(200)
                if len(kospi) > 0:
                    sections.append(('--- 코스피 TOP200 ---', kospi))
                if len(kosdaq) > 0:
                    sections.append(('--- 코스닥 TOP200 ---', kosdaq))
        else:
            sections.append(('--- 분석 요약 ---', pd.DataFrame(summary_rows)))

        if analysis_file and os.path.exists(analysis_file):
            print(f"[업로드] 역발상 분석: {analysis_file}")
            excel_file = pd.ExcelFile(analysis_file)

            sheet_titles = {
                '역발상투자후보': '--- 역발상 투자 후보 ---',
                '대형주': '--- 대형주 ---',
                '중형주': '--- 중형주 ---',
                '소형주': '--- 소형주 ---',
                '기본조건만족': '--- 기본 조건 만족 ---',
                '필터링통계': '--- 필터링 통계 ---',
            }

            for sheet_name in excel_file.sheet_names:
                df_sheet = pd.read_excel(analysis_file, sheet_name=sheet_name)
                if '현재가' in df_sheet.columns:
                    df_sheet = fill_trading_amounts_df(df_sheet)
                title = sheet_titles.get(sheet_name, f'--- {sheet_name} ---')
                sections.append((title, df_sheet))
                if sheet_name == '역발상투자후보':
                    contrarian_count = len(df_sheet)

        # 요약 섹션 값 갱신
        if sections and sections[0][0] == '--- 분석 요약 ---':
            summary_df = sections[0][1]
            summary_df.loc[summary_df['항목'] == '전체 종목 수', '값'] = f"{stock_count:,}개"
            summary_df.loc[summary_df['항목'] == '역발상 후보 종목 수', '값'] = f"{contrarian_count}개"

        success = uploader.upload_sections_to_daily_tab(date_tab, sections)

        if success:
            url = uploader.get_spreadsheet_url()
            print(f"\n[완료] 구글 시트 업로드 (탭: {date_tab})")
            if url:
                print(f"URL: {url}")
        return success

    except Exception as e:
        print(f"[오류] 구글 시트 업로드 중: {str(e)}")
        return False


def main(sheet_tab: str | None = None):
    """매일 주식 분석 + 구글 시트 업로드 자동화 (전체 전략)"""
    parser = argparse.ArgumentParser(description="전체 주식 전략 일괄 실행")
    parser.add_argument(
        "--sheet-tab",
        default="",
        help="구글 시트 탭 YYYY-MM-DD (미지정 시 오늘 또는 직전 거래일)",
    )
    args = parser.parse_args()
    tab = resolve_sheet_tab(sheet_tab or args.sheet_tab or None)

    print("=" * 60)
    print("매일 주식 분석 + 구글 시트 자동화 (전체 전략)")
    print(f"실행: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"구글 시트 탭: {tab}")
    print(f"경로: {os.getcwd()}")
    print("=" * 60)

    data_success = run_python_script(
        "quick_stock_check.py",
        "전체 종목 데이터 수집 (시가총액 포함)",
    )

    if not data_success:
        print("[중단] 데이터 수집 실패")
        sys.exit(1)

    print("\n10초 대기 (파일 저장 완료)...")
    time.sleep(10)

    analysis_success = run_python_script(
        "contrarian_stock_screener.py",
        "역발상 투자 종목 스크리닝",
    )

    upload_success = False
    if data_success:
        time.sleep(5)
        stock_data_file, analysis_file = find_latest_files()
        if stock_data_file or analysis_file:
            upload_success = upload_to_google_sheets(stock_data_file, analysis_file, tab)
        else:
            print("[오류] 업로드할 파일 없음")

    ma20_success = run_python_script(
        "ma20_breakout_screener.py",
        "20일선 상향 돌파 스크리닝",
        extra_args=["--sheet-tab", tab],
    )

    rebound_success = run_python_script(
        "daily_rebound_analysis.py",
        "리바운드 전략 (거래량급감·45일선·360일선)",
        extra_args=["--sheet-tab", tab],
    )

    print(f"\n{'='*60}")
    print("실행 결과")
    print(f"  데이터 수집:   {'성공' if data_success else '실패'}")
    print(f"  역발상 분석:   {'성공' if analysis_success else '실패'}")
    print(f"  구글 시트:     {'성공' if upload_success else '실패'}")
    print(f"  20일선 돌파:   {'성공' if ma20_success else '실패'}")
    print(f"  리바운드 전략: {'성공' if rebound_success else '실패'}")
    print(f"완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if not all([data_success, ma20_success, rebound_success]):
        sys.exit(1)


if __name__ == "__main__":
    main()
