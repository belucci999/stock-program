#!/usr/bin/env python3
"""거래일에만 일일 주식 분석을 실행합니다. (작업 스케줄러 / 스케줄러 공용 진입점)"""
import sys
from datetime import datetime
from pathlib import Path

from console_utf8 import enable as enable_utf8_console

enable_utf8_console()

ROOT = Path(__file__).resolve().parent


def main() -> int:
    sys.path.insert(0, str(ROOT))

    from market_calendar import is_krx_trading_day

    today = datetime.now().date()
    if not is_krx_trading_day(today):
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {today} - 휴장일, 분석을 건너뜁니다.")
        return 0

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 거래일 - 일일 분석을 시작합니다.")
    import daily_auto_stock_analysis

    daily_auto_stock_analysis.main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
