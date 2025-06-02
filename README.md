# 네이버 주식 크롤러

이 프로그램은 네이버 금융에서 한국 주식 정보를 매일 크롤링하여 Google Sheets에 저장하는 자동화 도구입니다.

## 설치 방법

1. 필요한 패키지 설치:
```bash
pip3 install -r requirements.txt
```

2. Google Sheets API 설정:
   - [Google Cloud Console](https://console.cloud.google.com)에서 새 프로젝트 생성
   - Google Sheets API 활성화
   - 서비스 계정 생성 및 JSON 키 파일 다운로드
   - JSON 키 파일을 프로젝트 루트 디렉토리에 `credentials.json`으로 저장

3. 환경 변수 설정:
   - `.env` 파일을 생성하고 다음 정보 입력:
   ```
   SPREADSHEET_ID=your_spreadsheet_id
   ```

## 사용 방법

1. Google Sheets 문서 생성 및 공유:
   - 새로운 Google Sheets 문서 생성
   - 문서 ID를 `.env` 파일의 `SPREADSHEET_ID`에 입력
   - 서비스 계정 이메일을 시트 편집자로 추가

2. 프로그램 실행:
```bash
python3 stock_crawler.py
```

## 기능

- 매일 장 마감 후 전체 상장 주식 정보 크롤링
- 다음 정보 수집:
  - 종목명
  - 종목코드
  - 현재가
  - 전일비
  - 등락률
  - 거래량
  - 거래대금
  - 시가총액
- Google Sheets에 자동 업데이트 