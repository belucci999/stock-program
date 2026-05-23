"""주식 데이터 후처리 (거래대금·전일거래대금 계산 등)."""
import pandas as pd
import numpy as np

# 수집/저장 시 컬럼 순서 (전일거래대금은 거래대금 바로 다음)
STOCK_DATA_COLUMN_ORDER = [
    '종목명', '종목코드', '시장구분', '업종',
    '현재가', '전일종가', '거래량', '전일거래량', '거래량증감율',
    '거래대금', '전일거래대금', '거래대금증감율',
    'PER', 'PBR', 'ROE', '시가총액',
    '매출액', '영업이익', '당기순이익', '부채비율', '유보율',
    '배당수익률', '배당금', '52주최고', '52주최저',
    '외국인비율', '기관비율', '베타', '수집일자',
]


def _to_float(value):
    if value is None or value == '' or (isinstance(value, float) and np.isnan(value)):
        return None
    try:
        if isinstance(value, str):
            value = value.replace(',', '').strip()
            if not value:
                return None
        return float(value)
    except (TypeError, ValueError):
        return None


def calc_trading_amount(price, volume):
    """거래대금 = 종가(가격) * 거래량"""
    p = _to_float(price)
    v = _to_float(volume)
    if p is not None and v is not None and p > 0 and v > 0:
        return round(p * v)
    return None


def calc_change_rate_pct(current, previous):
    """증감율(%) = (당일 - 전일) / 전일 * 100"""
    cur = _to_float(current)
    prev = _to_float(previous)
    if cur is None or prev is None or prev == 0:
        return ''
    return round((cur - prev) / prev * 100, 2)


def _is_empty(value):
    if value is None:
        return True
    if isinstance(value, float) and np.isnan(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def reorder_stock_data_columns(df: pd.DataFrame) -> pd.DataFrame:
    """표준 컬럼 순서 적용 (전일거래대금은 거래대금 옆)."""
    ordered = [c for c in STOCK_DATA_COLUMN_ORDER if c in df.columns]
    extra = [c for c in df.columns if c not in ordered]
    return df[ordered + extra]


def ensure_trading_amount_columns(df: pd.DataFrame) -> pd.DataFrame:
    """거래대금·전일거래대금 열이 없으면 추가."""
    out = df.copy()
    if '거래대금' not in out.columns:
        out['거래대금'] = ''
    if '전일거래대금' not in out.columns:
        out['전일거래대금'] = ''
    if '거래대금증감율' not in out.columns:
        out['거래대금증감율'] = ''
    return reorder_stock_data_columns(out)


def fill_trading_amounts_df(df: pd.DataFrame) -> pd.DataFrame:
    """거래대금·전일거래대금이 비어 있으면 종가*거래량으로 채움."""
    if df is None or len(df) == 0:
        return df

    out = ensure_trading_amount_columns(df)

    if '거래대금' in out.columns:
        for idx in out.index:
            if _is_empty(out.at[idx, '거래대금']):
                val = calc_trading_amount(
                    out.at[idx, '현재가'] if '현재가' in out.columns else None,
                    out.at[idx, '거래량'] if '거래량' in out.columns else None,
                )
                if val is not None:
                    out.at[idx, '거래대금'] = val

    if '전일거래대금' in out.columns:
        for idx in out.index:
            if _is_empty(out.at[idx, '전일거래대금']):
                val = calc_trading_amount(
                    out.at[idx, '전일종가'] if '전일종가' in out.columns else None,
                    out.at[idx, '전일거래량'] if '전일거래량' in out.columns else None,
                )
                if val is not None:
                    out.at[idx, '전일거래대금'] = val

    rates = []
    for idx in out.index:
        rate = calc_change_rate_pct(
            out.at[idx, '거래대금'] if '거래대금' in out.columns else None,
            out.at[idx, '전일거래대금'] if '전일거래대금' in out.columns else None,
        )
        rates.append(np.nan if rate == '' else rate)
    out['거래대금증감율'] = rates

    return reorder_stock_data_columns(out)


def fill_trading_amounts_record(record: dict) -> dict:
    """단일 종목 dict에 거래대금 필드 보정."""
    if _is_empty(record.get('거래대금')):
        val = calc_trading_amount(record.get('현재가'), record.get('거래량'))
        if val is not None:
            record['거래대금'] = val

    if '전일거래대금' not in record:
        record['전일거래대금'] = ''

    if _is_empty(record.get('전일거래대금')):
        val = calc_trading_amount(record.get('전일종가'), record.get('전일거래량'))
        if val is not None:
            record['전일거래대금'] = val

    if '거래대금증감율' not in record:
        record['거래대금증감율'] = ''

    record['거래대금증감율'] = calc_change_rate_pct(
        record.get('거래대금'), record.get('전일거래대금')
    )

    return record
