#!/usr/bin/env python3
import subprocess
import sys
import time
import os
import glob
import pandas as pd
from datetime import datetime
from google_sheets_uploader import GoogleSheetsUploader

def run_python_script(script_name, description):
    """Python 스크립트 실행"""
    print(f"\n{'='*60}")
    print(f"🚀 {description} 시작...")
    print(f"📅 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    try:
        # Python 경로
        python_path = "/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
        script_path = os.path.join(os.getcwd(), script_name)
        
        result = subprocess.run(
            [python_path, script_path],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )
        
        # 출력 표시
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print(f"⚠️ 오류/경고: {result.stderr}")
        
        if result.returncode == 0:
            print(f"✅ {description} 완료!")
            return True
        else:
            print(f"❌ {description} 실패! (종료 코드: {result.returncode})")
            return False
            
    except Exception as e:
        print(f"❌ {description} 실행 중 오류: {str(e)}")
        return False

def find_latest_files():
    """최신 생성된 파일들 찾기"""
    today = datetime.now().strftime('%Y%m%d')
    
    # 오늘 생성된 파일들 찾기
    stock_data_files = glob.glob(f'full_stock_data*{today}*.xlsx')
    analysis_files = glob.glob(f'contrarian_stocks*{today}*.xlsx')
    
    # 가장 최신 파일 선택
    latest_stock_data = max(stock_data_files, key=os.path.getctime) if stock_data_files else None
    latest_analysis = max(analysis_files, key=os.path.getctime) if analysis_files else None
    
    return latest_stock_data, latest_analysis

def upload_to_google_sheets(stock_data_file, analysis_file):
    """구글 시트에 결과 업로드"""
    print(f"\n📊 구글 시트 업로드 시작...")
    
    uploader = GoogleSheetsUploader()
    if not uploader.gc:
        print("❌ 구글 시트 연결 실패")
        return False
    
    # 스프레드시트 이름 (날짜 포함)
    today = datetime.now().strftime('%Y%m%d')
    spreadsheet_name = f"주식분석결과_{today}"
    
    success_count = 0
    total_uploads = 0
    
    try:
        # 요약 정보 먼저 생성
        summary_data = {
            '항목': [
                '📅 분석 일시',
                '🏢 전체 종목 수',
                '🎯 역발상 후보 종목 수',
                '📊 데이터 파일',
                '📈 분석 파일',
                '⏰ 업로드 시간',
                '🔗 스프레드시트 상태'
            ],
            '값': [
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '수집 중...',
                '분석 중...',
                os.path.basename(stock_data_file) if stock_data_file else 'N/A',
                os.path.basename(analysis_file) if analysis_file else 'N/A',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '업로드 진행 중...'
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        if uploader.upload_dataframe(summary_df, spreadsheet_name, "📋_분석요약"):
            success_count += 1
        total_uploads += 1
        
        # 1. 전체 주식 데이터 업로드
        if stock_data_file and os.path.exists(stock_data_file):
            print(f"📈 전체 주식 데이터 업로드 중: {stock_data_file}")
            df_stock = pd.read_excel(stock_data_file)
            
            # 요약 정보 업데이트
            summary_data['값'][1] = f"{len(df_stock):,}개"
            summary_df = pd.DataFrame(summary_data)
            uploader.upload_dataframe(summary_df, spreadsheet_name, "📋_분석요약")
            
            # ROE, 시가총액 기준으로 정렬한 상위 500개만 업로드
            df_stock_sorted = df_stock.sort_values(['ROE', '시가총액'], ascending=[False, False], na_position='last')
            df_stock_top = df_stock_sorted.head(500)
            
            if uploader.upload_dataframe(df_stock_top, spreadsheet_name, "📊_전체주식_TOP500"):
                uploader.format_sheet_headers(spreadsheet_name, "📊_전체주식_TOP500")
                success_count += 1
            total_uploads += 1
            
            # 시장별로 분리 업로드
            if '시장구분' in df_stock.columns:
                kospi_data = df_stock_sorted[df_stock_sorted['시장구분'] == '코스피'].head(200)
                kosdaq_data = df_stock_sorted[df_stock_sorted['시장구분'] == '코스닥'].head(200)
                
                if len(kospi_data) > 0:
                    if uploader.upload_dataframe(kospi_data, spreadsheet_name, "🏢_코스피_TOP200"):
                        uploader.format_sheet_headers(spreadsheet_name, "🏢_코스피_TOP200")
                        success_count += 1
                    total_uploads += 1
                
                if len(kosdaq_data) > 0:
                    if uploader.upload_dataframe(kosdaq_data, spreadsheet_name, "🏪_코스닥_TOP200"):
                        uploader.format_sheet_headers(spreadsheet_name, "🏪_코스닥_TOP200")
                        success_count += 1
                    total_uploads += 1
        
        # 2. 역발상 투자 분석 결과 업로드
        contrarian_count = 0
        if analysis_file and os.path.exists(analysis_file):
            print(f"🎯 역발상 분석 결과 업로드 중: {analysis_file}")
            
            # Excel 파일의 모든 시트 읽기
            excel_file = pd.ExcelFile(analysis_file)
            
            for sheet_name in excel_file.sheet_names:
                df_sheet = pd.read_excel(analysis_file, sheet_name=sheet_name)
                
                # 아이콘 추가하여 시트명 변경
                if sheet_name == '역발상투자후보':
                    new_sheet_name = "🎯_역발상투자후보"
                    contrarian_count = len(df_sheet)
                elif sheet_name == '대형주':
                    new_sheet_name = "🏢_대형주"
                elif sheet_name == '중형주':
                    new_sheet_name = "🏬_중형주"
                elif sheet_name == '소형주':
                    new_sheet_name = "🏪_소형주"
                elif sheet_name == '기본조건만족':
                    new_sheet_name = "📋_기본조건만족"
                elif sheet_name == '필터링통계':
                    new_sheet_name = "📈_필터링통계"
                else:
                    new_sheet_name = f"📊_{sheet_name}"
                
                if uploader.upload_dataframe(df_sheet, spreadsheet_name, new_sheet_name):
                    uploader.format_sheet_headers(spreadsheet_name, new_sheet_name)
                    success_count += 1
                total_uploads += 1
        
        # 최종 요약 정보 업데이트
        summary_data['값'][2] = f"{contrarian_count}개" if contrarian_count > 0 else "0개"
        summary_data['값'][6] = "✅ 업로드 완료!"
        
        summary_df = pd.DataFrame(summary_data)
        uploader.upload_dataframe(summary_df, spreadsheet_name, "📋_분석요약")
        uploader.format_sheet_headers(spreadsheet_name, "📋_분석요약")
        
        # 결과 출력
        spreadsheet_url = uploader.get_spreadsheet_url(spreadsheet_name)
        
        print(f"\n📊 구글 시트 업로드 완료!")
        print(f"✅ 성공: {success_count}/{total_uploads}")
        
        if spreadsheet_url:
            print(f"🔗 구글 시트 URL: {spreadsheet_url}")
        
        return success_count >= (total_uploads * 0.8)  # 80% 이상 성공하면 성공으로 간주
        
    except Exception as e:
        print(f"❌ 구글 시트 업로드 중 오류: {str(e)}")
        return False

def main():
    """매일 주식 분석 + 구글 시트 업로드 자동화"""
    
    print(f"""
🔥 매일 주식 분석 + 구글 시트 자동화! 🔥
📅 실행 일시: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}
📂 작업 디렉토리: {os.getcwd()}
🔗 구글 시트 자동 업로드 포함
    """)
    
    # 1단계: 데이터 수집
    data_success = run_python_script(
        "quick_stock_check.py", 
        "전체 종목 데이터 수집 (시가총액 포함)"
    )
    
    if not data_success:
        print("❌ 데이터 수집 실패로 분석을 중단합니다.")
        sys.exit(1)
    
    # 2단계: 분석 수행
    print("\n⏳ 데이터 정리를 위해 10초 대기...")
    time.sleep(10)
    
    analysis_success = run_python_script(
        "contrarian_stock_screener.py",
        "역발상 투자 종목 스크리닝"
    )
    
    # 3단계: 구글 시트 업로드
    upload_success = False
    if data_success:  # 데이터 수집이 성공했으면 업로드 시도
        time.sleep(5)  # 파일 생성 완료 대기
        
        stock_data_file, analysis_file = find_latest_files()
        
        if stock_data_file or analysis_file:
            upload_success = upload_to_google_sheets(stock_data_file, analysis_file)
        else:
            print("❌ 업로드할 파일을 찾을 수 없습니다.")
    
    # 최종 결과
    print(f"\n{'='*80}")
    print(f"🎉 매일 주식 분석 자동화 완료! 🎉")
    print(f"📅 완료 시간: {datetime.now().strftime('%Y년 %m월 %d일 %H시 %M분')}")
    
    print(f"\n📊 실행 결과:")
    print(f"   📈 데이터 수집: {'✅ 성공' if data_success else '❌ 실패'}")
    print(f"   🎯 역발상 분석: {'✅ 성공' if analysis_success else '❌ 실패'}")
    print(f"   📊 구글 시트 업로드: {'✅ 성공' if upload_success else '❌ 실패'}")
    
    if data_success and upload_success:
        print(f"\n🎊 핵심 작업이 완료되었습니다!")
        print(f"📱 구글 시트에서 결과를 확인하세요!")
    else:
        print(f"\n⚠️ 일부 작업에서 문제가 발생했습니다.")
    
    print(f"{'='*80}")

if __name__ == "__main__":
    main()