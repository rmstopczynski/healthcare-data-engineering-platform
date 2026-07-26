#!/usr/bin/env bash
# Runs dbt build + test INSIDE the healthcare_airflow container against
# Databricks (dbt-databricks is installed there per Dockerfile.airflow).
set -euo pipefail

echo "==> Running dbt models against Databricks..."
docker exec healthcare_airflow bash -c "cd /opt/airflow/dbt/healthcare_dbt && /home/airflow/tools-venv/bin/dbt run --profiles-dir /opt/airflow/dbt"

echo "==> Running /home/airflow/tools-venv/bin/dbt tests..."
docker exec healthcare_airflow bash -c "cd /opt/airflow/dbt/healthcare_dbt && /home/airflow/tools-venv/bin/dbt test --profiles-dir /opt/airflow/dbt"

echo "==> /home/airflow/tools-venv/bin/dbt run + test complete."
