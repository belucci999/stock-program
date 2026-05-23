#!/usr/bin/env python3
"""
ROE > 5, 20일선 상향 돌파, 돌파일 양봉, 돌파일 거래대금 50억원 이상 종목 스크리닝.

상향 돌파: 전일 종가 <= 전일 20일선 AND 당일 종가 > 당일 20일선
추가: 당일 양봉(종가 > 시가), 당일 거래대금(종가 x 거래량) >= 50억원
"""
from console_utf8 import enable as enable_utf8_console

enable_utf8_console()

import argparse
import glob
import re
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from stock_data_utils import fill_trading_amounts_df

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )
}

MIN_ROE = 5
MIN_TRADING_VALUE_KRW = 5_000_000_000  # 50억원


def find_latest_stock_data_file():
    """가장 최신 full_stock_data 엑셀 파일."""
    files = glob.glob('full_stock_data*.xlsx')
    if not files:
        return None

    file_info = []
    for path in files:
        match = re.search(r'(\d{8}_\d{4})', path)
        if match:
            try:
                dt = datetime.strptime(match.group(1), '%Y%m%d_%H%M')
                file_info.append((path, dt))
            except ValueError:
                continue

    if not file_info:
        return max(files, key=lambda p: __import__('os').path.getmtime(p))

    file_info.sort(key=lambda x: x[1])
    return file_info[-1][0]


def clean_numeric(value):
    if pd.isna(value) or value == '':
        return np.nan
    try:
        if isinstance(value, str):
            value = value.replace(',', '').strip()
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def fetch_daily_prices(code: str, pages: int = 4) -> pd.DataFrame:
    """네이버 금융 일별 시세: 시가·종가·거래량."""
    code = str(code).zfill(6)
    rows = []

    for page in range(1, pages + 1):
        url = f'https://finance.naver.com/item/sise_day.naver?code={code}&page={page}'
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')

            for tr in soup.select('table.type2 tr'):
                tds = tr.select('td')
                if len(tds) < 7:
                    continue
                date_txt = tds[0].get_text(strip=True)
                if not re.match(r'\d{4}\.\d{2}\.\d{2}', date_txt):
                    continue
                try:
                    close = float(tds[1].get_text(strip=True).replace(',', ''))
                    open_price = float(tds[2].get_text(strip=True).replace(',', ''))
                    volume = int(tds[5].get_text(strip=True).replace(',', ''))
                    rows.append({
                        'date': datetime.strptime(date_txt, '%Y.%m.%d'),
                        'open': open_price,
                        'close': close,
                        'volume': volume,
                    })
                except (ValueError, IndexError):
                    continue
        except requests.RequestException:
            continue
        time.sleep(0.08)

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .drop_duplicates(subset=['date'])
        .sort_values('date')
        .reset_index(drop=True)
    )


def detect_ma20_breakout(df: pd.DataFrame) -> tuple[bool, dict | None]:
    """
    20일선 상향 돌파 + 돌파일 양봉 + 돌파일 거래대금 50억 이상.
    """
    if df is None or len(df) < 21:
        return False, None

    work = df.copy()
    work['ma20'] = work['close'].rolling(window=20).mean()

    prev = work.iloc[-2]
    today = work.iloc[-1]

    prev_close, today_close = prev['close'], today['close']
    prev_ma20, today_ma20 = prev['ma20'], today['ma20']
    today_open = today['open']
    today_volume = today['volume']

    if any(
        pd.isna(x)
        for x in (prev_close, today_close, prev_ma20, today_ma20, today_open, today_volume)
    ):
        return False, None

    ma20_breakout = today_close > today_ma20 and prev_close <= prev_ma20
    if not ma20_breakout:
        return False, None

    is_bullish = today_close > today_open
    if not is_bullish:
        return False, None

    trading_value = today_close * today_volume
    if trading_value < MIN_TRADING_VALUE_KRW:
        return False, None

    return True, {
        '돌파일': today['date'].strftime('%Y-%m-%d'),
        '시가': round(today_open, 2),
        '종가': round(today_close, 2),
        '전일종가': round(prev_close, 2),
        'MA20': round(today_ma20, 2),
        '전일MA20': round(prev_ma20, 2),
        '종가대비MA20(%)': round((today_close / today_ma20 - 1) * 100, 2),
        '거래량': int(today_volume),
        '돌파일거래대금': int(trading_value),
        '돌파일거래대금(억)': round(trading_value / 1e8, 1),
        '양봉': 'Y',
    }


def screen_ma20_breakout(limit: int = 0, sleep_sec: float = 0.12):
    """ROE>5 종목 중 조건 충족 종목 추출."""
    data_file = find_latest_stock_data_file()
    if not data_file:
        print('[오류] full_stock_data*.xlsx 파일이 없습니다. 먼저 quick_stock_check.py를 실행하세요.')
        return None

    print(f'[데이터] {data_file}')
    base = fill_trading_amounts_df(pd.read_excel(data_file))
    print(f'[로드] 전체 {len(base)}개 종목')

    if 'ROE' not in base.columns:
        print('[오류] ROE 컬럼이 없습니다.')
        return None

    base['ROE'] = base['ROE'].apply(clean_numeric)
    candidates = base[base['ROE'] > MIN_ROE].copy()
    print(f'[필터] ROE > {MIN_ROE}: {len(candidates)}개')
    print(f'[조건] 20일선 상향 돌파 + 돌파일 양봉 + 돌파일 거래대금 >= 50억원')

    if limit > 0:
        candidates = candidates.head(limit)
        print(f'[테스트] 상위 {limit}개만 검사')

    hits = []
    total = len(candidates)

    for i, (_, row) in enumerate(candidates.iterrows(), start=1):
        code = str(row.get('종목코드', '')).replace('.0', '').zfill(6)
        name = row.get('종목명', '')

        hist = fetch_daily_prices(code)
        ok, metrics = detect_ma20_breakout(hist)

        if ok:
            record = row.to_dict()
            record.update(metrics)
            hits.append(record)
            print(
                f'  [돌파] {name} ({code}) | ROE {row["ROE"]:.1f}% | '
                f'거래대금 {metrics["돌파일거래대금(억)"]}억'
            )

        if i % 50 == 0:
            print(f'  ... 진행 {i}/{total} (충족 {len(hits)}개)')

        time.sleep(sleep_sec)

    result_df = pd.DataFrame(hits)
    if len(result_df) > 0 and 'ROE' in result_df.columns:
        result_df = result_df.sort_values(
            ['ROE', '돌파일거래대금'], ascending=[False, False]
        )

    out_name = f'ma20_breakout_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
    result_df.to_excel(out_name, index=False)

    print(f'\n[완료] 조건 충족 종목: {len(result_df)}개')
    print(f'[저장] {out_name}')
    return result_df


def main():
    parser = argparse.ArgumentParser(
        description='ROE>5, 20일선 상향 돌파, 양봉, 거래대금 50억+ 스크리닝'
    )
    parser.add_argument('--limit', type=int, default=0, help='테스트용 검사 종목 수 제한')
    parser.add_argument('--sleep', type=float, default=0.12, help='종목당 요청 간격(초)')
    args = parser.parse_args()
    screen_ma20_breakout(limit=args.limit, sleep_sec=args.sleep)


if __name__ == '__main__':
    main()
