#!/usr/bin/env bash
# Consumes ADT events from Kafka and writes them to a local batch file
# under data/adt_landing/. Runs INSIDE the healthcare_airflow container.
# Bounded: exits automatically after ~10s of no new messages.
set -euo pipefail

echo "==> Consuming ADT events from Kafka into a local batch file..."
docker exec healthcare_airflow bash -c "cd /opt/airflow/data && python3 kafka_consumer.py"
