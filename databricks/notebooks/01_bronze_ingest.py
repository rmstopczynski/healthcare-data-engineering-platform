# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingest
# MAGIC Loads the 7 synthetic source CSVs (`generate_synthetic_data.py`, run
# MAGIC locally or via `scripts/generate_data.sh`) into Bronze Delta tables:
# MAGIC raw copy of the source, plus ingestion metadata (load timestamp,
# MAGIC source file). No cleaning here -- that's Silver's job.
# MAGIC
# MAGIC Upload the CSVs first: workspace UI -> Catalog -> your volume ->
# MAGIC Upload, or `databricks fs cp` from the CLI, into
# MAGIC `/Volumes/workspace/default/adt_pipeline/source_csv/`.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA_BRONZE = "bronze"
SOURCE_DIR = "/Volumes/workspace/default/adt_pipeline/source_csv"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")

SOURCE_TABLES = [
    "states", "cities", "addresses", "hospitals", "doctors",
    "medications", "procedures", "insurance_providers",
    "patients", "patient_insurance", "prescriptions", "hospital_visits",
]

# COMMAND ----------

for table_name in SOURCE_TABLES:
    csv_path = f"{SOURCE_DIR}/{table_name}.csv"
    df = (
        spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{table_name}.csv"))
    )
    target = f"{CATALOG}.{SCHEMA_BRONZE}.{table_name}"
    df.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"Loaded {df.count()} rows -> {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checkpoint
# MAGIC Confirm the deliberately-messy data (duplicate patient IDs, malformed
# MAGIC ICD-10-style codes, missing insurance links -- introduced by
# MAGIC `generate_synthetic_data.py`) is visible here, unaltered. Silver is
# MAGIC where that gets handled explicitly, not silently dropped.

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.patients").limit(20))
