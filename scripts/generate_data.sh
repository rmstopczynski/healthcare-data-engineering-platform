#!/usr/bin/env bash
# Generates synthetic CSVs only -- runs INSIDE the healthcare_airflow
# container, which already has python3 + faker.
set -euo pipefail

echo "==> Generating synthetic data (inside the airflow container)..."
docker exec healthcare_airflow bash -c "cd /opt/airflow/data && python3 generate_synthetic_data.py"
