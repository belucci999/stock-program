# 🚀 한국 주식 자동분석 시스템

이 프로그램은 네이버 금융에서 한국 주식 정보를 자동으로 수집하고 분석하여 Google Sheets에 저장하는 완전 자동화 도구입니다.

## ✨ 주요 기능

- 📊 **전체 상장 주식 데이터 자동 수집** (코스피/코스닥)
- 🎯 **역발상 투자 후보 종목 자동 스크리닝**
- 📱 **Google Sheets 자동 업로드** (실시간 대시보드)
- 🔄 **완전 자동화** (데이터 수집 → 분석 → 업로드)
- 📈 **상위 종목 TOP 랭킹** (ROE, 시가총액 기준)

## 🛠️ 설치 방법

### 1. 저장소 클론
```bash
git clone https://github.com/belucci999/stock-program.git
cd stock-program
```

### 2. 가상환경 생성 및 활성화
```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# 또는
.venv\Scripts\activate     # Windows
```

### 3. 패키지 설치
```bash
pip install -r requirements.txt
```
### 4. 프로그램 실행
python daily_auto_stock_analysis.py

### 4. Google Sheets API 설정

#### 4.1 Google Cloud Console 설정
1. [Google Cloud Console](https://console.cloud.google.com) 접속
2. 새 프로젝트 생성 또는 기존 프로젝트 선택
3. "API 및 서비스" → "라이브러리" 이동
4. "Google Sheets API" 검색 후 활성화
5. "Google Drive API"도 검색 후 활성화

#### 4.2 서비스 계정 생성
1. "API 및 서비스" → "사용자 인증 정보" 이동
2. "사용자 인증 정보 만들기" → "서비스 계정" 선택
3. 서비스 계정 이름 입력 후 생성
4. 생성된 서비스 계정 클릭 → "키" 탭 이동
5. "키 추가" → "새 키 만들기" → "JSON" 선택
6. 다운로드된 JSON 파일을 프로젝트 루트에 `credentials.json`으로 저장

### 5. Google Sheets 문서 준비
1. [Google Sheets](https://sheets.google.com)에서 새 문서 생성
2. 문서 이름을 `주식분석결과_YYYYMMDD` 형식으로 설정
3. 서비스 계정 이메일을 문서 편집자로 공유 추가

## 🚀 사용 방법

### 자동 분석 실행
```bash
python daily_auto_stock_analysis.py
```

### 개별 스크립트 실행
```bash
# 데이터 수집만
python quick_stock_check.py

# 역발상 분석만  
python contrarian_stock_screener.py
```

## 📁 파일 구조 
```

## ⚙️ 다른 PC에서 설정하기

### 1. 저장소 클론 및 설정
```bash
git clone https://github.com/belucci999/stock-program.git
cd stock-program
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. credentials.json 파일 추가
- Google Cloud Console에서 새로 다운로드하거나
- 기존 파일을 안전하게 복사

### 3. Python 버전 호환성
- **권장**: Python 3.11 또는 3.12
- **주의**: Python 3.13은 일부 패키지와 호환성 문제 가능

## 🐛 문제 해결

### pandas 설치 오류 (Python 3.13)
```bash
pip install "pandas>=2.2.0" --only-binary=all
```

### Google Sheets 업로드 오류
- `credentials.json` 파일 확인
- 서비스 계정이 시트에 편집 권한이 있는지 확인
- 인터넷 연결 상태 확인

### NaN 값 오류
- 이미 수정된 `google_sheets_uploader.py`에서 자동 처리됨

## 📊 결과 확인

프로그램 실행 후 Google Sheets에서 다음 시트들을 확인할 수 있습니다:

- 📋 **분석요약**: 전체 실행 결과 요약
- 📊 **전체주식_TOP500**: ROE 기준 상위 500개 종목
- 🏢 **코스피_TOP200**: 코스피 상위 200개 종목  
- 🏪 **코스닥_TOP200**: 코스닥 상위 200개 종목
- 🎯 **역발상투자후보**: 선별된 투자 후보 종목들

## 📝 라이선스

이 프로젝트는 개인적/교육적 목적으로만 사용해주세요.

## ⚠️ 주의사항

- 네이버 금융 사이트의 이용약관을 준수해주세요
- 과도한 요청으로 서버에 부하를 주지 않도록 주의해주세요
- 투자 결정은 본인 책임하에 이루어져야 합니다