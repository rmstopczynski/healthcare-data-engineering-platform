#!/usr/bin/env bash
# Runs the batch half of the pipeline end to end: generate synthetic
# data, load it into Databricks Bronze (via the Databricks notebooks
# under databricks/notebooks/ -- run manually in the workspace or via
# a Databricks Job), then dbt run/test for Silver -> Gold.
#
# The streaming half (Kafka -> Auto Loader) is independent of this
# script and this batch schedule by design -- run ./run_kafka_demo.sh
# separately. See databricks/README.md for why these are split.
set -euo pipefail
cd "$(dirname "$0")"

./generate_data.sh
echo "==> Synthetic CSVs generated. Load them into Bronze via the"
echo "==> databricks/notebooks/01_bronze_ingest.py notebook (or the"
echo "==> equivalent Databricks Job), then continue with dbt:"
./run_dbt.sh

echo ""
echo "==> Batch pipeline run complete."
