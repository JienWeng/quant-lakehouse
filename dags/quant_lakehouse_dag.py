"""
Quant lakehouse DAG: OKX (public candles) + FRED (macro) -> MinIO -> Spark SQL (silver/gold).
Personal / portfolio use — set FRED_API_KEY in .env for real FRED pulls.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "personal",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="quant_lakehouse_okx_fred",
    default_args=default_args,
    description="OKX + FRED -> MinIO; Spark SQL silver/gold (BTC vs VIX)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quant", "okx", "fred", "spark", "minio"],
) as dag:
    ingest_okx = BashOperator(
        task_id="ingest_okx",
        bash_command="python /opt/airflow/scripts/ingest_okx.py",
    )

    ingest_fred = BashOperator(
        task_id="ingest_fred",
        bash_command="python /opt/airflow/scripts/ingest_fred.py",
    )

    spark_silver_okx = BashOperator(
        task_id="spark_silver_okx",
        bash_command="bash /opt/airflow/scripts/run_spark_sql.sh /opt/airflow/spark/sql/10_silver_okx.sql",
    )

    spark_silver_fred = BashOperator(
        task_id="spark_silver_fred",
        bash_command="bash /opt/airflow/scripts/run_spark_sql.sh /opt/airflow/spark/sql/15_silver_fred.sql",
    )

    spark_gold = BashOperator(
        task_id="spark_gold_btc_vix",
        bash_command="bash /opt/airflow/scripts/run_spark_sql.sh /opt/airflow/spark/sql/20_gold_btc_vix.sql",
    )

    qc = BashOperator(
        task_id="qc_gold_counts",
        bash_command="bash /opt/airflow/scripts/run_spark_sql.sh /opt/airflow/spark/sql/99_qc_counts.sql",
    )

    ingest_okx >> spark_silver_okx
    ingest_fred >> spark_silver_fred
    [spark_silver_okx, spark_silver_fred] >> spark_gold >> qc
