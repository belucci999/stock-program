#!/usr/bin/env python3
"""거래일에만 전체 주식 전략을 실행합니다. (작업 스케줄러 / 스케줄러 공용 진입점)"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

from console_utf8 import enable as enable_utf8_console

enable_utf8_console()

ROOT = Path(__file__).resolve().parent


def main() -> int:
    sys.path.insert(0, str(ROOT))

    from market_calendar import is_krx_trading_day, resolve_sheet_tab

    parser = argparse.ArgumentParser(
        description="거래일 전체 주식 전략 자동 실행 (휴장일은 --force 로 직전 거래일 기준)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="휴장일·주말에도 실행 (시세·시트 탭은 직전 거래일 기준)",
    )
    parser.add_argument(
        "--sheet-tab",
        default="",
        help="구글 시트 탭 이름 YYYY-MM-DD (미지정 시 오늘 또는 직전 거래일)",
    )
    args = parser.parse_args()

    today = datetime.now().date()
    sheet_tab = resolve_sheet_tab(args.sheet_tab or None, today)

    if not args.force and not is_krx_trading_day(today):
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {today} - 휴장일, 분석을 건너뜁니다.")
        print("  직전 거래일 기준 실행: python run_scheduled_analysis.py --force")
        return 0

    if not is_krx_trading_day(today):
        print(
            f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 휴장일 강제 실행 "
            f"(기준 거래일·시트 탭: {sheet_tab})"
        )
    else:
        print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 거래일 - 전체 전략 분석을 시작합니다.")

    import daily_auto_stock_analysis

    daily_auto_stock_analysis.main(sheet_tab=sheet_tab)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
