#!/usr/bin/env bash
# Runs dbt build + test INSIDE the healthcare_airflow container against
# Databricks (dbt-databricks is installed there per Dockerfile.airflow).
set -euo pipefail

echo "==> Running dbt models against Databricks..."
docker exec healthcare_airflow bash -c "cd /opt/airflow/dbt/healthcare_dbt && dbt run --profiles-dir /opt/airflow/dbt"

echo "==> Running dbt tests..."
docker exec healthcare_airflow bash -c "cd /opt/airflow/dbt/healthcare_dbt && dbt test --profiles-dir /opt/airflow/dbt"

echo "==> dbt run + test complete."
