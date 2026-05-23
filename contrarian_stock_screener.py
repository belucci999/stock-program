from console_utf8 import enable as enable_utf8_console

enable_utf8_console()

import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import re

def find_latest_stock_data_file():
    """가장 최신 주식 데이터 파일을 찾기"""
    patterns = [
        'full_stock_data_*.xlsx',
        'stock_data_*.xlsx', 
        '*stock_data*.xlsx'
    ]
    
    all_files = []
    for pattern in patterns:
        files = glob.glob(pattern)
        all_files.extend(files)
    
    if not all_files:
        print("❌ 주식 데이터 파일을 찾을 수 없습니다.")
        return None
    
    unique_files = list(set(all_files))
    file_info = []
    for file in unique_files:
        match = re.search(r'(\d{8}_\d{4})', file)
        if match:
            datetime_str = match.group(1)
            try:
                file_datetime = datetime.strptime(datetime_str, '%Y%m%d_%H%M')
                file_info.append((file, file_datetime))
            except:
                continue
    
    if not file_info:
        print(f"❌ 날짜 형식을 인식할 수 있는 파일이 없습니다.")
        return None
    
    file_info.sort(key=lambda x: x[1])
    latest_file = file_info[-1][0]
    
    print(f"📁 최신 데이터 파일: {latest_file}")
    print(f"📅 수집 시간: {file_info[-1][1].strftime('%Y년 %m월 %d일 %H시 %M분')}")
    return latest_file

def clean_numeric_data(value):
    """문자열 숫자 데이터를 float로 변환"""
    if pd.isna(value) or value == '' or value == 'N/A':
        return np.nan
    
    if isinstance(value, str):
        value = value.replace(',', '')
        if value.startswith('-'):
            try:
                return -float(value[1:])
            except:
                return np.nan
        try:
            return float(value)
        except:
            return np.nan
    
    return float(value) if not pd.isna(value) else np.nan

def contrarian_screening():
    """역발상 투자 종목 스크리닝 (PER, PBR 조건 제외)"""
    
    print("🔍 역발상 투자 종목 스크리닝 시작...")
    
    # 최신 파일 찾기
    latest_file = find_latest_stock_data_file()
    if not latest_file:
        return
    
    try:
        # 데이터 읽기
        df = pd.read_excel(latest_file)
        print(f"✅ 총 {len(df)}개 종목 데이터 로드")
        
        # 숫자 데이터 정리
        numeric_columns = [
            'PER', 'PBR', 'ROE', '시가총액', '현재가', '전일종가',
            '거래량', '전일거래량', '거래대금', '전일거래대금', '거래대금증감율',
        ]
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].apply(clean_numeric_data)
        
        # 계산된 지표 추가
        df['가격변화율'] = ((df['현재가'] - df['전일종가']) / df['전일종가'] * 100).round(2)
        df['거래량변화율'] = ((df['거래량'] - df['전일거래량']) / df['전일거래량'] * 100).round(2)
        
        print(f"\n🎯 역발상 투자 스크리닝 조건:")
        print("=" * 60)
        
        # 기본 필터링
        valid_data = df[
            (df['현재가'].notna()) & 
            (df['전일종가'].notna()) &
            (df['거래량'].notna()) &
            (df['전일거래량'].notna()) &
            (df['ROE'].notna()) &
            (df['시가총액'].notna())
        ].copy()
        
        print(f"📊 기본 데이터 있는 종목: {len(valid_data)}개")
        
        # 역발상 투자 조건 적용
        print(f"\n🔍 조건별 필터링:")
        
        # 1. 거래량 85% 이상 감소
        condition1 = valid_data['거래량변화율'] <= -85
        filtered1 = valid_data[condition1]
        print(f"1️⃣ 거래량 85% 이상 감소: {len(filtered1)}개")
        
        # 2. 당일 음봉 (하락)
        condition2 = condition1 & (valid_data['가격변화율'] < 0)
        filtered2 = valid_data[condition2]
        print(f"2️⃣ + 당일 음봉: {len(filtered2)}개")
        
        # 3. 시가총액 500억 이상
        condition3 = condition2 & (valid_data['시가총액'] >= 500)
        filtered3 = valid_data[condition3]
        print(f"3️⃣ + 시총 500억 이상: {len(filtered3)}개")
        
        # 4. ROE 양수
        condition4 = condition3 & (valid_data['ROE'] > 0)
        filtered4 = valid_data[condition4]
        print(f"4️⃣ + ROE 양수: {len(filtered4)}개")
        
        # 5. 전일 거래량 5만개 이상
        condition5 = condition4 & (valid_data['전일거래량'] >= 50000)
        filtered5 = valid_data[condition5]
        print(f"5️⃣ + 전일 거래량 5만개 이상: {len(filtered5)}개")
        
        # === 추가 조건 (PER, PBR 제외) ===
        print(f"\n🌟 추가 조건 적용:")
        
        # 6. 주가 하락폭 -7% ~ -1% (과도한 하락 제외)
        condition6 = condition5 & (valid_data['가격변화율'].between(-7, -1))
        filtered6 = valid_data[condition6]
        print(f"6️⃣ + 하락폭 -7%~-1%: {len(filtered6)}개")
        
        # 최종 결과
        final_result = filtered6.copy()
        
        # 점수 계산 (우선순위) - PER, PBR 제외
        def calculate_score(row):
            score = 100  # 기본 점수
            
            # 거래량 감소폭에 따른 가점
            if row['거래량변화율'] <= -95:
                score += 25
            elif row['거래량변화율'] <= -90:
                score += 20
            elif row['거래량변화율'] <= -85:
                score += 15
            
            # ROE 높을수록 가점
            if row['ROE'] >= 20:
                score += 20
            elif row['ROE'] >= 15:
                score += 15
            elif row['ROE'] >= 10:
                score += 10
            elif row['ROE'] >= 5:
                score += 5
            
            # 시가총액 안정성 가점
            if row['시가총액'] >= 10000:  # 1조 이상 대형주
                score += 15
            elif row['시가총액'] >= 5000:  # 5천억 이상
                score += 12
            elif row['시가총액'] >= 1000:  # 1천억 이상
                score += 8
            elif row['시가총액'] >= 500:   # 500억 이상
                score += 5
            
            # 적당한 하락폭 가점 (-3% 내외가 이상적)
            if -4 <= row['가격변화율'] <= -2:
                score += 10
            elif -5 <= row['가격변화율'] <= -1:
                score += 5
            
            # 전일 거래량이 많을수록 가점 (유동성)
            if row['전일거래량'] >= 500000:  # 50만개 이상
                score += 10
            elif row['전일거래량'] >= 200000:  # 20만개 이상
                score += 7
            elif row['전일거래량'] >= 100000:  # 10만개 이상
                score += 5
            elif row['전일거래량'] >= 50000:   # 5만개 이상
                score += 3
            
            return score
        
        if len(final_result) > 0:
            final_result['투자점수'] = final_result.apply(calculate_score, axis=1)
            final_result = final_result.sort_values('투자점수', ascending=False)
            
            print(f"\n🎯 최종 역발상 투자 후보: {len(final_result)}개")
            print("=" * 110)
            
            # 상위 결과 출력
            display_count = min(20, len(final_result))
            print(f"\n🏆 TOP {display_count} 역발상 투자 후보:")
            
            for idx, row in final_result.head(display_count).iterrows():
                market_cap_str = f"{row['시가총액']:,.0f}억"
                prev_volume_str = f"{row['전일거래량']:,.0f}개"
                print(f"{row['종목명']:12} | "
                      f"점수: {row['투자점수']:3.0f} | "
                      f"거래량변화: {row['거래량변화율']:6.1f}% | "
                      f"가격변화: {row['가격변화율']:5.1f}% | "
                      f"ROE: {row['ROE']:5.1f}% | "
                      f"시총: {market_cap_str:>12} | "
                      f"전일거래량: {prev_volume_str:>10}")
            
            # 시총별 분류
            print(f"\n📊 시가총액별 분포:")
            large_cap = final_result[final_result['시가총액'] >= 10000]  # 1조 이상
            mid_cap = final_result[(final_result['시가총액'] >= 1000) & (final_result['시가총액'] < 10000)]  # 1천억~1조
            small_cap = final_result[final_result['시가총액'] < 1000]  # 1천억 미만
            
            print(f"   대형주 (1조 이상): {len(large_cap)}개")
            print(f"   중형주 (1천억~1조): {len(mid_cap)}개")
            print(f"   소형주 (1천억 미만): {len(small_cap)}개")
            
            # Excel 저장
            output_filename = f"contrarian_stocks_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            
            with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
                # 최종 결과
                final_result.to_excel(writer, sheet_name='역발상투자후보', index=False)
                
                # 시총별 분류
                if len(large_cap) > 0:
                    large_cap.to_excel(writer, sheet_name='대형주', index=False)
                if len(mid_cap) > 0:
                    mid_cap.to_excel(writer, sheet_name='중형주', index=False)
                if len(small_cap) > 0:
                    small_cap.to_excel(writer, sheet_name='소형주', index=False)
                
                # 단계별 필터링 결과
                filtered5.to_excel(writer, sheet_name='기본조건만족', index=False)
                
                # 조건별 통계
                stats_df = pd.DataFrame({
                    '조건': [
                        '전체 종목',
                        '거래량 85% 감소',
                        '+ 음봉',
                        '+ 시총 500억 이상',
                        '+ ROE 양수',
                        '+ 전일거래량 5만개 이상',
                        '+ 하락폭 -7%~-1%'
                    ],
                    '종목수': [
                        len(valid_data),
                        len(filtered1),
                        len(filtered2),
                        len(filtered3),
                        len(filtered4),
                        len(filtered5),
                        len(filtered6)
                    ]
                })
                stats_df.to_excel(writer, sheet_name='필터링통계', index=False)
            
            print(f"\n💾 결과가 '{output_filename}' 파일로 저장되었습니다!")
            
            # 간단 통계
            if len(final_result) > 0:
                avg_roe = final_result['ROE'].mean()
                avg_volume_change = final_result['거래량변화율'].mean()
                avg_price_change = final_result['가격변화율'].mean()
                avg_market_cap = final_result['시가총액'].mean()
                
                print(f"\n📈 후보 종목 평균 지표:")
                print(f"   평균 ROE: {avg_roe:.1f}%")
                print(f"   평균 거래량 감소: {avg_volume_change:.1f}%")
                print(f"   평균 가격 변화: {avg_price_change:.1f}%")
                print(f"   평균 시가총액: {avg_market_cap:,.0f}억원")
        
        else:
            print(f"\n😅 모든 조건을 만족하는 종목이 없습니다.")
            print(f"💡 조건을 완화하여 다시 시도해보세요.")
            
            # 기본 조건만 만족하는 종목이라도 표시
            if len(filtered5) > 0:
                print(f"\n📋 기본 조건(1~5)만 만족하는 종목: {len(filtered5)}개")
                for idx, row in filtered5.head(10).iterrows():
                    market_cap_str = f"{row['시가총액']:,.0f}억"
                    print(f"  {row['종목명']:12} | "
                          f"거래량변화: {row['거래량변화율']:6.1f}% | "
                          f"가격변화: {row['가격변화율']:5.1f}% | "
                          f"ROE: {row['ROE']:5.1f}% | "
                          f"시총: {market_cap_str}")
        
    except Exception as e:
        print(f"❌ 오류 발생: {str(e)}")

if __name__ == "__main__":
    contrarian_screening()