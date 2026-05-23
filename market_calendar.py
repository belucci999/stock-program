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


def get_last_krx_trading_day(day: date | None = None) -> date:
    """기준일이 거래일이면 그날, 아니면 직전 거래일을 반환."""
    day = day or datetime.now().date()
    cal = _get_calendar()
    if cal.is_session(day):
        return day
    session = cal.date_to_session(day, direction="previous")
    return session.date() if hasattr(session, "date") else day


def resolve_sheet_tab(explicit: str | None = None, day: date | None = None) -> str:
    """
    구글 시트 탭 이름(YYYY-MM-DD) 결정.
    explicit이 있으면 그대로, 없으면 기준일의 마지막 거래일.
    """
    if explicit and explicit.strip():
        return explicit.strip()
    ref = get_last_krx_trading_day(day)
    return ref.strftime("%Y-%m-%d")
