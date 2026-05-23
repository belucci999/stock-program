#!/usr/bin/env python3
"""
백그라운드 스케줄러: 매일 17:00에 거래일 여부를 확인한 뒤 전체 전략을 실행합니다.

Windows에서는 setup_windows_scheduler.ps1 로 작업 스케줄러를 등록하는 것을 권장합니다.
이 스크립트는 터미널을 켜 둔 채로 쓸 때 사용합니다.
"""
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import schedule

ROOT = Path(__file__).resolve().parent
RUN_SCRIPT = ROOT / "run_scheduled_analysis.py"


def run_daily_job():
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M:%S}] 예약 작업 트리거")
    subprocess.run(
        [sys.executable, str(RUN_SCRIPT)],
        cwd=str(ROOT),
        check=False,
    )


def main():
    schedule.every().day.at("17:00").do(run_daily_job)
    print("일일 주식 전략 스케줄러가 실행 중입니다.")
    print("- 실행 시각: 매일 17:00 (거래일만 실제 분석)")
    print("- 전략: 데이터수집, 역발상, 20일선돌파, 리바운드(거래량급감·45일선·360일선)")
    print("- 종료: Ctrl+C")
    print(f"- 작업 디렉토리: {ROOT}")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
