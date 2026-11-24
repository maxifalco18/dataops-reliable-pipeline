"""Airflow DAG that schedules the pipeline hourly.

The pipeline itself has no Airflow dependency: Airflow only schedules,
retries and surfaces failures. Business logic stays testable on its own.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from dataops_pipeline.config import Settings
from dataops_pipeline.pipeline import run

default_args = {
    "owner": "data-engineering",
    "retries": 2,                         # transient infra failures
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def _run_pipeline() -> None:
    result = run(Settings.from_env())
    print(f"run {result.run_id}: loaded={result.rows_loaded} quarantined={result.rows_quarantined}")


with DAG(
    dag_id="dataops_device_metrics",
    description="Contract-validated, idempotent load of device telemetry with DQ checks",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,                    # no overlapping runs on the same watermark
    default_args=default_args,
    tags=["dataops", "telemetry"],
) as dag:
    PythonOperator(task_id="run_pipeline", python_callable=_run_pipeline)
