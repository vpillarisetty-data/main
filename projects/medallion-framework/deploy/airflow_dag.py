"""Airflow DAG that drives the same bundle job, for teams orchestrating from Airflow."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
    dag_id="card_transactions_medallion",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,  # never overlap: Silver MERGE is single-writer
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True},
    tags=["medallion", "payments"],
) as dag:
    DatabricksRunNowOperator(
        task_id="run_medallion_job",
        databricks_conn_id="databricks_default",
        job_name="card_transactions_medallion",
    )
