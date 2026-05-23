"""한국거래소(KRX) 거래일 판별."""
from datetime import date, datetime

_calendar = None


def _get_calendar():
    global _calendar
    if _calendar is None:
        import exchange_calendars as xcals

        _calendar = xcals.get_calendar("XKRX")
    return _calendar


def is_krx_trading_day(day: date | None = None) -> bool:
    """장이 열리는 날이면 True (주말·공휴 휴장 제외)."""
    day = day or datetime.now().date()
    return bool(_get_calendar().is_session(day))
