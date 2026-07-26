#!/usr/bin/env bash
# Runs the full ADT streaming demo: publish simulated events, consume
# them into a local batch file, then upload that batch to the
# Databricks volume landing folder for Auto Loader to pick up. This is
# the complete "ADT Event -> Kafka Topic -> Landing File -> Auto
# Loader -> Silver Delta" pattern in one command.
set -euo pipefail
cd "$(dirname "$0")"

./run_kafka_producer.sh
./run_kafka_consumer.sh
./upload_adt_batch_to_databricks.sh

echo ""
echo "==> ADT streaming demo complete."
echo "==> Check the adt_silver_stream table in Databricks, or browse the"
echo "==> 'adt-events' topic directly at http://localhost:8084 (Kafka UI)."
