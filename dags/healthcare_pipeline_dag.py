"""
End-to-end healthcare pipeline DAG (batch half only -- see
databricks/README.md for why the ADT stream runs independently,
outside this schedule).

    generate_synthetic_data
            |
            v
    trigger_databricks_bronze_job   <- calls the Databricks Jobs API
            |                          (api/2.1/jobs/run-now), which is
            v                          the same endpoint validated by
          dbt_run                     hand during Phase 0 of the v3
            |                          rework plan.
            v
          dbt_test

Each task is a thin BashOperator wrapping a command you could also run
by hand -- the DAG's job is sequencing, retries, and a scheduled,
monitored "run the whole pipeline" button instead of running four
commands in order yourself.

Requires DATABRICKS_HOST / DATABRICKS_TOKEN (see docker-compose.yml)
and a Databricks Job already created from
databricks/notebooks/01_bronze_ingest.py, with its numeric job_id set
below via the DATABRICKS_BRONZE_JOB_ID env var.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/healthcare_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"  # expects a real profiles.yml here, see .gitignore

BRONZE_JOB_ID = os.environ.get("DATABRICKS_BRONZE_JOB_ID", "<set-me>")

default_args = {
    "owner": "healthcare-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="healthcare_pipeline",
    description="Bronze -> Silver -> Gold healthcare pipeline (batch half), on Databricks",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["healthcare", "portfolio", "databricks"],
) as dag:

    generate_synthetic_data = BashOperator(
        task_id="generate_synthetic_data",
        bash_command="cd /opt/airflow/data && python3 generate_synthetic_data.py",
    )

    upload_csvs_to_databricks = BashOperator(
        task_id="upload_csvs_to_databricks",
        bash_command=(
            "cd /opt/airflow/data/synthetic_data && "
            "for f in *.csv; do "
            "databricks fs cp \"$f\" \"dbfs:/Volumes/workspace/default/adt_pipeline/source_csv/$f\" --overwrite; "
            "done"
        ),
    )

    # Triggers the Databricks Job wrapping 01_bronze_ingest.py via the
    # workspace-level Jobs API -- the same call ("run-now") validated
    # by hand against `/api/2.1/jobs/list` during Phase 0.
    trigger_databricks_bronze_job = BashOperator(
        task_id="trigger_databricks_bronze_job",
        bash_command=(
            'curl -sf -X POST "https://$DATABRICKS_HOST/api/2.1/jobs/run-now" '
            '-H "Authorization: Bearer $DATABRICKS_TOKEN" '
            f'-d \'{{"job_id": {BRONZE_JOB_ID}}}\''
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && /home/airflow/tools-venv/bin/dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && /home/airflow/tools-venv/bin/dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    generate_synthetic_data >> upload_csvs_to_databricks >> trigger_databricks_bronze_job >> dbt_run >> dbt_test
