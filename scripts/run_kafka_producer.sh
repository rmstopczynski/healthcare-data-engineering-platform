#!/usr/bin/env bash
# Publishes simulated ADT events to Kafka. Runs INSIDE the
# healthcare_airflow container (has kafka-python installed) -- no host
# dependencies.
set -euo pipefail

echo "==> Publishing simulated ADT events to Kafka..."
docker exec healthcare_airflow bash -c "python3 /opt/airflow/data/kafka_producer.py"
