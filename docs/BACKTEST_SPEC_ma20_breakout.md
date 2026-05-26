# 20일선 상향 돌파 — BACKTEST_SPEC

**전략 ID:** `ma20_breakout_roe5`  
**모듈:** `ma20_breakout_screener.py`  
**명세 버전:** 1.0

---

## 0. 레이어 구분

| **SCREENER** | **BACKTEST** |
|--------------|--------------|
| `detect_ma20_breakout` | **TBD** |

**상수:** `MIN_ROE = 5`, `MIN_TRADING_VALUE_KRW = 5_000_000_000` (50억)

---

## 1. 모멘텀

### SCREENER

| 항목 | YES/NO | 값 |
|------|--------|-----|
| 20일선 돌파 | **YES** | 전일 종가 **≤** 전일 MA20 **AND** 당일 종가 **>** 당일 MA20 |
| ROE 필터 (유니버스) | **YES** | **> 5** (엑셀 `ROE` 컬럼) |

### BACKTEST

**TBD**

---

## 2. 유동성

### SCREENER

| 항목 | 값 |
|------|-----|
| 돌파일 거래대금 | **≥ 5,000,000,000** KRW (종가×거래량) |
| 최소 일봉 | **21**일 |

---

## 3. 품질

### SCREENER

| 항목 | 값 |
|------|-----|
| ROE | **> 5%** (하드) |
| PER/PBR | **NO** |

---

## 4. 진입

### SCREENER (모두 AND)

| # | 조건 | YES/NO |
|---|------|--------|
| 1 | MA20 상향 돌파 | YES |
| 2 | 돌파일 양봉 | YES (종가 **>** 시가) |
| 3 | 돌파일 거래대금 | YES (**≥ 50억**) |
| 4 | ROE | YES (**> 5**) |

| 항목 | 값 |
|------|-----|
| 순위 | ROE ↓, 돌파일 거래대금 ↓ |
| 수동 선택 | **NO** |

### BACKTEST

**TBD**

---

## 5. 매도 / 6. 포트

**TBD**

---

## 7. 실행

```bash
python ma20_breakout_screener.py
# --limit N  --no-upload  --sheet-tab YYYY-MM-DD
```

출력: `ma20_breakout_YYYYMMDD_HHMM.xlsx`, Google Sheets 섹션 `--- 20일선 상향 돌파 ---`
