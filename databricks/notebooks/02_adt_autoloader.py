# Databricks notebook source
# MAGIC %md
# MAGIC # ADT Stream -> Silver, via Auto Loader
# MAGIC
# MAGIC Ingests ADT (admit/discharge/transfer) event batch files -- landed by
# MAGIC `data/kafka_consumer.py` and uploaded by
# MAGIC `scripts/upload_adt_batch_to_databricks.sh` -- into a Silver Delta
# MAGIC table, incrementally.
# MAGIC
# MAGIC **Why Auto Loader instead of a native Kafka consumer running inside
# MAGIC Databricks:** Databricks Free Edition is serverless-only, with
# MAGIC outbound network access restricted to a limited set of trusted
# MAGIC domains, and no support for the custom Spark connector libraries a
# MAGIC direct Kafka consumer would need. It has no network path to a broker
# MAGIC running in local Docker at all. Landing events as files and letting
# MAGIC Auto Loader do incremental, checkpointed ingestion from there is a
# MAGIC serverless-native answer to the same problem, and mirrors a common
# MAGIC real-world pattern (Kafka -> object storage sink -> lakehouse
# MAGIC ingestion).
# MAGIC
# MAGIC Run this with `.trigger(availableNow=True)` on a schedule (e.g. a
# MAGIC Databricks Job, or triggered by the same Airflow DAG that runs the
# MAGIC batch pipeline) rather than as an always-on stream, since Free
# MAGIC Edition serverless jobs have a maximum runtime and this workload
# MAGIC doesn't need sub-second latency -- frequent micro-batches are the
# MAGIC right granularity for "bed occupancy within minutes."

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "default"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/adt_pipeline"

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{VOLUME_ROOT}/schema")
    .load(f"{VOLUME_ROOT}/landing")
)

query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{VOLUME_ROOT}/checkpoint")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.silver.adt_events")
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC `_rescued_data` (added automatically by Auto Loader) catches any
# MAGIC fields that don't match the inferred schema -- a real, built-in
# MAGIC answer to "how do you handle schema drift in the stream," worth
# MAGIC having ready if asked.

# COMMAND ----------

display(spark.table(f"{CATALOG}.silver.adt_events").orderBy("event_time", ascending=False).limit(20))
