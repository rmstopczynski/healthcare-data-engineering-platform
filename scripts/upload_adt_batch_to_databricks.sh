#!/usr/bin/env bash
# Uploads locally-landed ADT batch files (written by kafka_consumer.py)
# into the Databricks volume landing folder, where Auto Loader picks
# them up. Requires the Databricks CLI configured with a host + token
# (see databricks/README.md) -- this is the one manual credential setup
# step in the whole pipeline, since it's talking to your cloud account.
set -euo pipefail
cd "$(dirname "$0")/../data"

if [ -z "$(ls -A adt_landing 2>/dev/null)" ]; then
  echo "==> No batch files in ./adt_landing -- run run_kafka_demo.sh first."
  exit 0
fi

echo "==> Uploading ADT batch files to Databricks volume landing folder..."
for f in adt_landing/*.jsonl; do
  databricks fs cp "$f" "dbfs:/Volumes/workspace/default/adt_pipeline/landing/$(basename "$f")"
  echo "  ... uploaded $(basename "$f")"
done

echo "==> Upload complete. Auto Loader will pick these up on its next trigger."
