from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner":            "realestate",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

def fetch_trade(**context):
    import sys
    sys.path.insert(0, "/opt/airflow/plugins")
    from operators.molit_fetcher import fetch_all

    # 28일 실행 → 당월 데이터 수집
    # 예: 2025-04-28 실행 → 202504 수집
    execution_date = context["execution_date"]
    deal_ymd = execution_date.strftime("%Y%m")

    SERVICE_KEY = "95ebf59ae83e2d42fb7ce7793694c25418da5c100a62261d290640999660094c"

    df = fetch_all(SERVICE_KEY, deal_ymd=deal_ymd)

    context["ti"].xcom_push(key="row_count", value=len(df))
    context["ti"].xcom_push(key="deal_ymd",  value=deal_ymd)

    csv_path = f"/opt/airflow/logs/output_{deal_ymd}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    context["ti"].xcom_push(key="csv_path", value=csv_path)

    logger.info(f"수집 완료: {deal_ymd} → {len(df)}건")


def load_to_db(**context):
    import os
    import sys
    os.environ["DB_HOST"] = "postgres"
    os.environ["DB_PORT"] = "5432"
    sys.path.insert(0, "/opt/airflow/plugins")
    from operators.db_loader import load_csv_to_db

    csv_path = context["ti"].xcom_pull(key="csv_path")
    deal_ymd = context["ti"].xcom_pull(key="deal_ymd")

    load_csv_to_db(csv_path)
    logger.info(f"DB 적재 완료: {deal_ymd}")


def run_dbt(**context):
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "/opt/airflow/dbt", "--profiles-dir", "/opt/airflow/dbt"],
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt 실패: {result.stderr}")
    logger.info("dbt run 완료")


def notify(**context):
    row_count = context["ti"].xcom_pull(key="row_count")
    deal_ymd  = context["ti"].xcom_pull(key="deal_ymd")
    logger.info(f"파이프라인 완료! {deal_ymd} → 총 {row_count}건 적재")


with DAG(
    dag_id="realestate_monthly",
    default_args=default_args,
    description="국토교통부 실거래가 월별 수집 파이프라인",
    schedule="0 6 28 * *",   # 매월 28일 06:00 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
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
        task_id="run_dbt",
        python_callable=run_dbt,
    )

    t4 = PythonOperator(
        task_id="notify",
        python_callable=notify,
    )

    t1 >> t2 >> t3 >> t4