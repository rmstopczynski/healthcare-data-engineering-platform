# Databricks Layer

The warehouse/compute engine for this project, replacing the Postgres
setup used in earlier versions of this repo. Runs on **Databricks Free
Edition** (serverless-only — see the constraint below, it shapes
everything in this folder).

## One-time setup
1. Sign up at https://www.databricks.com/try-databricks — **Free
   Edition**, not the 14-day trial (a different, time-limited product).
2. Create the landing volume (used by both the CSV uploads and the ADT
   stream):
   ```sql
   CREATE VOLUME IF NOT EXISTS workspace.default.adt_pipeline;
   ```
3. Create the schemas:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.bronze;
   CREATE SCHEMA IF NOT EXISTS workspace.silver;
   CREATE SCHEMA IF NOT EXISTS workspace.gold;
   ```
4. Generate a personal access token (Settings → Developer → Access
   tokens), scoped to `jobs` + `sql` only. Copy `dbt/profiles.yml.sample`
   to `~/.dbt/profiles.yml` (and to `dbt/profiles.yml` for the
   containerized path — see `.gitignore`), filling in `host`,
   `http_path` (from your SQL Warehouse's connection details), and the
   token.

## Catalog/schema conventions
- Catalog: `workspace` (the default, single metastore Free Edition gives you)
- `bronze` — raw ingested tables, one per source CSV, plus `_ingested_at`/`_source_file` (see `notebooks/01_bronze_ingest.py`)
- `silver` — dbt staging models (cleaned/deduped/typed) **plus** `adt_events` (the Auto Loader output, not a dbt model — dbt doesn't run streaming jobs)
- `gold` — dbt marts: `fact_hospital_visit`, `fact_prescription`, `dim_patient`, `dim_doctor`, `dim_medication`, `dim_procedure`, `dim_date`

## Division of labor: PySpark vs. dbt vs. Auto Loader
- **PySpark notebook** (`01_bronze_ingest.py`): CSV → Bronze. Minimal transformation, just typed ingestion + metadata.
- **dbt** (`../dbt/healthcare_dbt`): Bronze → Silver → Gold for all 7 batch source tables — dedup, typing, business joins, tests. This was already a mature dbt project before the Databricks migration; the only changes were schema names (`raw`→`bronze`, `staging`→`silver`, `analytics`→`gold`) and porting a handful of Postgres-only functions (`DISTINCT ON`, `to_char`, `age()`, `generate_series()`) to their Databricks/Spark SQL equivalents (`QUALIFY row_number()`, `date_format()`, `months_between()`, `sequence()`+`explode()`).
- **Auto Loader** (`02_adt_autoloader.py`): the ADT stream specifically. This is the one piece dbt can't do (it's not a streaming tool), and the one piece that can't talk to Kafka directly (see constraint below).

## The constraint that shapes the streaming design
Databricks Free Edition is **serverless-only**: no classic clusters, no
custom compute configs, and outbound network access restricted to a
limited set of trusted domains. Concretely, it cannot open a network
connection to a Kafka broker running in local Docker — there's no path
for a direct Kafka consumer (Spark Structured Streaming or otherwise)
to reach it.

The workaround, which is also just a legitimate real-world pattern:
`data/kafka_consumer.py` writes cleaned ADT events to local
newline-delimited JSON batch files; `scripts/upload_adt_batch_to_databricks.sh`
pushes those into the workspace volume's `landing/` folder; Auto Loader
picks them up incrementally from there. Same teaching material for
Kafka itself (topics, partitions, consumer groups, offsets), a
serverless-native answer for getting the data into the lakehouse.

## Still open / not yet built
- The ICD-10-style malformed-code validation and missing-insurance-link
  flagging described in the original plan haven't been added to
  `generate_synthetic_data.py` or the staging models yet — that's real
  next-session work, not something to claim is done.
- Delta `MERGE`-based incremental updates for dimension tables (vs. the
  current full-refresh `view`/`table` materializations) are a natural
  next step using dbt's own `incremental` materialization with a
  `merge` strategy — dbt-databricks supports this natively, no custom
  PySpark needed. Worth doing for at least `stg_patients` as a concrete
  example before an interview.
