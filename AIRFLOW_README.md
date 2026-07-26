# Airflow Orchestration Layer

Turns the manual command sequence (generate synthetic data → upload to
Databricks → trigger Bronze ingestion → `dbt run` → `dbt test`) into a
scheduled, monitored Airflow DAG.

## What changed from the Postgres version of this repo

Previously, Airflow's tasks ran `psql` against a local Postgres
warehouse in the same docker-compose network. Now the warehouse is
Databricks (Free Edition, a managed cloud workspace) — so tasks either
run locally (`generate_synthetic_data`, `dbt run`/`dbt test` via
`dbt-databricks`) or call the **Databricks Jobs API** over the network
(`upload_csvs_to_databricks`, `trigger_databricks_bronze_job`). There's
no local warehouse container to `depends_on` anymore.

```
├── docker-compose.yml          <- "airflow" service only calls out to kafka now
├── Dockerfile.airflow          <- dbt-databricks, databricks provider, databricks-cli
└── dags/
    └── healthcare_pipeline_dag.py
```

## Setup

1. Create a Databricks Job from `databricks/notebooks/01_bronze_ingest.py`
   (Workflows -> Create Job -> Notebook task), note its numeric Job ID.
2. Set the required environment variables (a local `.env` file,
   gitignored, or exported before `docker compose up`):
   ```
   DATABRICKS_HOST=https://your-workspace-url
   DATABRICKS_TOKEN=your-token
   DATABRICKS_HTTP_PATH=your-sql-warehouse-http-path
   DATABRICKS_BRONZE_JOB_ID=123456789
   ```
3. Copy `dbt/profiles.yml.sample` to `dbt/profiles.yml` (gitignored —
   this is the file mounted into the container at
   `/opt/airflow/dbt/profiles.yml`).

## Running it

```bash
docker compose up -d --build
```

`--build` matters — it triggers building the custom Airflow image
(dbt-databricks + the Databricks provider + CLI on top of the base
image). First run takes a few minutes; later runs reuse the built
image.

```bash
docker logs -f healthcare_airflow
```
Look for `Airflow is ready` and the auto-generated admin
username/password (standalone mode prints these on first boot).

## Using it

1. Open **http://localhost:8083**, log in with the logged admin credentials
2. Find `healthcare_pipeline` in the DAG list, un-pause it if needed
3. Trigger it manually (▶), or let it run on its `@daily` schedule

Graph view shows five tasks in a line:
`generate_synthetic_data → upload_csvs_to_databricks →
trigger_databricks_bronze_job → dbt_run → dbt_test`. Click any task box
→ **Logs** to see exactly what it did.

## Why this matters over running the commands yourself

- **Retries** — each task retries once automatically on failure
- **Scheduling** — `@daily` means this can run unattended
- **Observability** — task-level logs, run history, a visual graph of what succeeded/failed
- **Dependency enforcement** — Airflow won't run `dbt_test` if `dbt_run` failed

## The one architectural boundary worth being ready to explain

The ADT/Kafka streaming branch (`scripts/run_kafka_demo.sh` +
`databricks/notebooks/02_adt_autoloader.py`) runs **independently of
this DAG and its `@daily` schedule** — Airflow is fundamentally a batch
scheduler, not a continuous-stream runner. In a real deployment, the
Auto Loader notebook would run as its own always-on or frequently-
triggered Databricks Job (e.g. every few minutes via `trigger(availableNow=True)`
on a schedule), separate from the nightly batch DAG. Same boundary that
existed in the original architecture doc — worth naming directly if
asked "does Airflow orchestrate the streaming part too?"

## A note on "standalone" mode

This uses Airflow's `standalone` command — one container, SQLite
metadata database, `SequentialExecutor`. Deliberate simplification for
a local portfolio project instead of the ~5 services a `CeleryExecutor`
production setup needs. Tasks run one at a time, which is fine here
since the DAG's tasks are meant to run in strict order anyway —
`LocalExecutor` with a dedicated metadata database would be the answer
if parallel task execution needed demonstrating, worth naming as a
known simplification if asked.
