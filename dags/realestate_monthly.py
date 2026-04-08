from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# ── 기본 설정 ──
default_args = {
    "owner":            "realestate",
    "retries":          2,                        # 실패 시 2번 재시도
    "retry_delay":      timedelta(minutes=5),     # 재시도 간격 5분
    "email_on_failure": False,
}


# ── Task 1: API 수집 ──
def fetch_trade(**context):
    import sys
    sys.path.insert(0, "/opt/airflow/plugins")
    from operators.molit_fetcher import fetch_all

    # 실행 월의 전월 데이터 수집
    # 예: 2025-02-01 실행 → 202501 수집
    execution_date = context["execution_date"]
    prev_month = execution_date.strftime("%Y%m")

    SERVICE_KEY = "95ebf59ae83e2d42fb7ce7793694c25418da5c100a62261d290640999660094c"

    df = fetch_all(SERVICE_KEY, deal_ymd=prev_month)

    # XCom으로 다음 태스크에 건수 전달
    context["ti"].xcom_push(key="row_count", value=len(df))
    context["ti"].xcom_push(key="deal_ymd",  value=prev_month)

    # 임시 CSV 저장
    csv_path = f"/opt/airflow/logs/output_{prev_month}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    context["ti"].xcom_push(key="csv_path", value=csv_path)

    logger.info(f"수집 완료: {prev_month} → {len(df)}건")


# ── Task 2: DB 적재 ──
def load_to_db(**context):
    import os
    import sys
    os.environ["DB_HOST"] = "postgres"   # ← 직접 하드코딩
    os.environ["DB_PORT"] = "5432"       # ← 직접 하드코딩
    sys.path.insert(0, "/opt/airflow/plugins")
    from operators.db_loader import load_csv_to_db

    os.environ["DB_HOST"] = "postgres"
    csv_path = context["ti"].xcom_pull(key="csv_path")
    deal_ymd = context["ti"].xcom_pull(key="deal_ymd")

    load_csv_to_db(csv_path)

    logger.info(f"DB 적재 완료: {deal_ymd}")


# ── Task 3: 완료 로그 ──
def notify(**context):
    row_count = context["ti"].xcom_pull(key="row_count")
    deal_ymd  = context["ti"].xcom_pull(key="deal_ymd")
    logger.info(f"파이프라인 완료! {deal_ymd} → 총 {row_count}건 적재")


# ── DAG 정의 ──
with DAG(
    dag_id="realestate_monthly",
    default_args=default_args,
    description="국토교통부 실거래가 월별 수집 파이프라인",
    schedule="0 6 1 * *",   # 매월 1일 06:00 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,                   # 과거 누락분 자동실행 방지
    tags=["realestate", "batch"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_trade",
        python_callable=fetch_trade,
    )

    t2 = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_db,
    )

    t3 = PythonOperator(
        task_id="notify",
        python_callable=notify,
    )

    # 실행 순서 정의
    t1 >> t2 >> t3