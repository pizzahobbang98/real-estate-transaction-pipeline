# 🏠 부동산 실거래가 자동 수집 파이프라인

국토교통부 실거래가 공공 API를 활용하여 전국 아파트 매매·전월세 데이터를
자동으로 수집·변환·시각화하는 데이터 엔지니어링 파이프라인입니다.

---

## 📌 프로젝트 개요

부동산 실거래가 데이터를 매월 자동으로 수집하고, 지역별·평형별 분석이 가능한
대시보드를 제공합니다. 2023년 1월부터 현재까지의 전국 데이터를 다룹니다.

---

## 🏗️ 아키텍처
국토교통부 API
↓
Python (수집)
↓
PostgreSQL (저장)
↓
Airflow (자동화, 매월 28일)
↓
dbt (변환)
↓
Streamlit (시각화)

---

## 🛠️ 기술 스택

| 분류 | 기술 |
|------|------|
| 언어 | Python 3.11 |
| 수집 | requests, xml.etree |
| 저장 | PostgreSQL 16 |
| 오케스트레이션 | Apache Airflow 2.9.1 |
| 변환 | dbt-postgres 1.10.0 |
| 시각화 | Streamlit, Plotly |
| 인프라 | Docker, Docker Compose |

---

## 📂 프로젝트 구조
real-estate-transaction-pipeline/
├── dags/
│   └── realestate_monthly.py
├── plugins/operators/
│   ├── molit_fetcher.py
│   ├── db_loader.py
│   ├── fetch_history.py
│   └── sigungu_codes.py
├── realestate_dbt/
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── mart/
├── app.py
├── docker-compose.yml
└── .env.example

---

## 📊 데이터 파이프라인

### 1. 수집 (Extract)
- 국토교통부 실거래가 API (매매 + 전월세)
- 전국 227개 시군구 × 월별 수집
- XML 응답 파싱 → pandas DataFrame

### 2. 적재 (Load)
- PostgreSQL 16에 적재
- UNIQUE 제약조건으로 중복 방지
- `apt_trade` (매매), `apt_rent` (전월세) 테이블 분리

### 3. 변환 (Transform)
- dbt 3계층 구조
  - `staging`: 원본 정제, 취소거래 필터링, 파생 컬럼 생성
  - `intermediate`: 지역·평형별 집계
  - `mart`: 전세가율 계산, 대시보드용 최종 테이블

### 4. 자동화 (Orchestration)
- Airflow DAG: 매월 28일 06:00 자동 실행
- 태스크: `fetch_trade → load_to_db → run_dbt → notify`

### 5. 시각화 (Visualization)
- Streamlit 대시보드
  - 지역별 평균 매매가 비교 (서울/경기/부산 권역별)
  - 평형별 거래량 비중
  - 지역별 전세가율
  - 매매가 분포 히스토그램
  - 실거래 아파트 상세 테이블
- Metabase 대시보드 (별도 제공)

---

## 🚀 실행 방법

### 1. 환경변수 설정
```bash
cp .env.example .env
# .env 파일에 API 키 입력
```

### 2. Docker 컨테이너 실행
```bash
docker compose up -d
```

### 3. 과거 데이터 수집 (최초 1회)
```bash
conda activate de_env
python plugins/operators/fetch_history.py
```

### 4. DB 적재
```bash
python plugins/operators/db_loader.py
```

### 5. dbt 변환
```bash
cd realestate_dbt
set PYTHONUTF8=1
dbt run
```

### 6. Streamlit 실행
```bash
streamlit run app.py
```

---

## 🔍 주요 트러블슈팅

| 문제 | 원인 | 해결 |
|------|------|------|
| API JSON 파싱 오류 | 공공API가 XML로 응답 | XML 파싱으로 전환 |
| Docker DB 연결 실패 | 컨테이너 내부 host명 상이 | `DB_HOST` 환경변수로 분기 |
| dbt 모델 인식 불가 | `_sources.yml` 0 bytes | 파일 재작성 |
| 취소거래 데이터 포함 | `cancel_deal` NaN 값 필터링 누락 | WHERE 조건 추가 |
| 중복 데이터 적재 | UNIQUE 제약조건 미설정 | 제약조건 추가 + 중복 제거 |

---

## 📈 수집 데이터 규모

- 수집 기간: 2023년 1월 ~ 현재 (매월 자동 갱신)
- 대상 지역: 전국 227개 시군구
- 매매 데이터: 약 XX만 건
- 전월세 데이터: 약 XX만 건

---

## 🗓️ 향후 계획

- [ ] AWS S3 raw layer 추가
- [ ] PySpark 대용량 처리 도입
- [ ] 카카오 지도 API 연동 (지역별 가격 히트맵)
- [ ] GitHub Actions CI/CD 구축

---

## 👤 개발자

- **이름**: Kang Jaehwan
- **GitHub**: [pizzahobbang98](https://github.com/pizzahobbang98)