# Healthcare Data Engineering Pipeline — Databricks Lakehouse

A healthcare data pipeline built on a **medallion (Bronze → Silver →
Gold) lakehouse architecture** on Databricks — synthetic patient,
provider, and hospital-visit data cleaned and modeled through dbt, plus
a near-real-time ADT (Admit/Discharge/Transfer) event stream landed via
Kafka and ingested with Databricks Auto Loader.

```
Source (7 synthetic CSVs, 3NF)          ADT events (Kafka, local)
        │                                        │
        ▼                                        ▼
   BRONZE (Delta)                    landing files (.jsonl)
   raw copy + ingestion metadata      uploaded to a Databricks volume
        │                                        │
   PySpark cleaning                     Databricks Auto Loader
        │                                        │
        ▼                                        ▼
   SILVER (dbt staging models)        SILVER STREAM (adt_events, Delta)
   deduped, typed, conformed                     │
        │                                        │
        └───────────────────┬────────────────────┘
                             ▼
                  GOLD (dbt marts)
                  fact_hospital_visit, fact_prescription,
                  dim_patient/doctor/medication/procedure/date
                  + bed-occupancy query against the ADT stream
```

## Contents

- [Why this version exists](#why-this-version-exists)
- [Why Databricks over Postgres](#why-databricks-over-postgres)
- [The constraint that shapes the streaming design](#the-constraint-that-shapes-the-streaming-design)
- [Source schema](#source-schema)
- [Quickstart](#quickstart)
- [Data](#data)
- [dbt transformation layer](#dbt-transformation-layer)
- [Airflow orchestration](#airflow-orchestration)
- [Kafka + Auto Loader streaming](#kafka--auto-loader-streaming)
- [Sample queries](#sample-queries)
- [What's still open](#whats-still-open)
- [Challenges encountered](#challenges-encountered-and-how-they-were-resolved)
- [Repo structure](#repo-structure)
- [Resume & interview talking points](RESUME_TALKING_POINTS.md)

## Why this version exists

This repo went through three stages:

1. **Origin.** The star schema — the fact/dimension split, the Julian
   date dimension, the original ERD — came from a university course
   project (ISM 6208, Data Warehousing) proposing a healthcare data
   warehouse on Oracle/Snowflake. No public repo, just the paper and
   diagrams.
2. **v1 rebuild.** A real, runnable implementation on Postgres + Docker
   + dbt + Airflow + MinIO (S3-pattern staging) + local PySpark + Kafka
   — everything past the schema concept, original solo work.
3. **v2/v3 rework (this version).** Migrated the warehouse/compute
   layer to Databricks + Delta Lake specifically for resume/keyword
   alignment with current job postings, and **pruned** MinIO and the
   parallel Snowflake DDL folder in the process — they added defend-
   surface (three different "storage layer" stories in one repo)
   without adding a distinct lesson beyond what dbt + Delta already
   demonstrate. The streaming story also changed from generic
   vitals/lab-result events to ADT (Admit/Discharge/Transfer)
   specifically, a real, HL7-adjacent healthcare pattern.

Worth calling out directly: this is a **rework, not a rebuild**. The
dimensional schema, the dbt project (models, tests, docs), the Airflow
DAG shape, and the Kafka producer/consumer mechanics all carried over —
what changed was the warehouse target, the layer names
(`raw/staging/analytics` → `bronze/silver/gold`), a handful of
Postgres-only SQL functions ported to their Databricks equivalents, and
the streaming ingestion path.

## Why Databricks over Postgres

Postgres was the lower-effort choice — it was already fully built and
working. Databricks was chosen anyway, deliberately, because it's
specifically named in job postings this project is meant to speak to,
in a way "a Postgres warehouse" isn't. That tradeoff (interview
readiness / lower risk vs. resume keyword alignment) is itself
reasonable interview material if asked "why not just keep what already
worked?"

## The constraint that shapes the streaming design

**Databricks Free Edition is serverless-only**: no classic clusters, no
custom compute configurations, and outbound network access restricted
to a limited set of trusted domains. Concretely, it cannot open a
network connection to a Kafka broker running in local Docker — there's
no path for a direct Kafka consumer (native or Spark Structured
Streaming) to reach it, regardless of client library or JAR version.

The design that works within that: the Kafka consumer lands cleaned ADT
events as batch files; those get uploaded to a Databricks volume;
**Auto Loader** (serverless-native, incremental file ingestion) picks
them up from there into a Silver Delta table. This is also a
legitimate, common real-world pattern — Kafka landing to object storage
before lakehouse ingestion — not purely a free-tier workaround. See
`databricks/README.md` and `KAFKA_README.md` for the full reasoning.

This was verified directly against a live Free Edition workspace before
committing to the design: Auto Loader ingesting a test file into Delta,
and the workspace-level Jobs API (`/api/2.1/jobs/list`) reachable with
a scoped personal access token — both confirmed working before Phase 1
of the rework began.

## Source schema

Two source ER diagrams drove this project: a normalized OLTP schema
(patients, hospitals, doctors, prescriptions, hospital visits,
insurance) and a target dimensional model (two fact tables —
`fact_prescription` and `fact_hospital_visit` — surrounded by conformed
dimensions, including a Julian date dimension). See `docs/erd/` for both
diagrams (note: `docs/architecture_diagram.svg` predates this rework and
needs regenerating to reflect the Databricks/ADT architecture above).

## Quickstart

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/)
and a free [Databricks account](https://www.databricks.com/try-databricks)
(**Free Edition**, not the 14-day trial).

**1. One-time Databricks setup** — see `databricks/README.md` for the
full walkthrough: create the landing volume, the three schemas
(`bronze`/`silver`/`gold`), a personal access token, and a Job wrapping
`databricks/notebooks/01_bronze_ingest.py`.

**2. Local environment:**
```bash
cp dbt/profiles.yml.sample dbt/profiles.yml   # fill in host/http_path/token
docker compose up -d --build                  # first run builds the custom Airflow image
```

**3. Run the batch pipeline:**
```bash
./scripts/generate_data.sh
# upload the generated CSVs to the workspace volume (see databricks/README.md),
# then run the Bronze notebook, then:
./scripts/run_dbt.sh
```
Or trigger the whole thing via the Airflow DAG at `http://localhost:8083`
once `DATABRICKS_BRONZE_JOB_ID` is set — see `AIRFLOW_README.md`.

**4. Run the streaming demo:**
```bash
./scripts/run_kafka_demo.sh
```
Then run `databricks/notebooks/02_adt_autoloader.py` in the workspace
(or as its own Databricks Job) to ingest the landed batch.

## Data

No production/real dataset was available for this project (real PHI
can't be used for a portfolio project — see the HIPAA note in the
original plan). `data/generate_synthetic_data.py` produces a
deterministic (seeded) synthetic dataset: 250 patients, 350 hospital
visits, 600 prescriptions, plus supporting reference data (hospitals,
doctors, medications, procedures, insurance providers, geography).

## dbt transformation layer

12 staging models (one per Bronze table) and 7 mart models (`dim_date`,
`dim_patient`, `dim_doctor`, `dim_medication`, `dim_procedure`,
`fact_prescription`, `fact_hospital_visit`), connected via `ref()`/
`source()` so dbt builds the full dependency graph automatically —
unchanged in shape from the Postgres version. What changed in this
rework:

- Schema config: `staging`/`analytics` → `silver`/`gold`; source schema `raw` → `bronze`
- `DISTINCT ON (col) ... ORDER BY col` (Postgres-only) → `QUALIFY ROW_NUMBER() OVER (PARTITION BY col ORDER BY col) = 1` (ANSI-adjacent, works on Databricks) across all 11 staging models needing dedup
- `to_char(date, 'YYYYMMDD')::integer` → `CAST(date_format(date, 'yyyyMMdd') AS INT)`
- `extract(year from age(current_date, dob))` → `FLOOR(months_between(current_date(), dob) / 12)`
- `generate_series(...)::date` (date dimension) → `EXPLODE(SEQUENCE(...))`

60 tests still apply unchanged: `unique`/`not_null` on every primary
key, `relationships` on every fact-table foreign key, `accepted_values`
on categorical columns. See `dbt/README.md` for full setup and what
each test checks.

## Airflow orchestration

`dags/healthcare_pipeline_dag.py` chains
`generate_synthetic_data → upload_csvs_to_databricks →
trigger_databricks_bronze_job → dbt_run → dbt_test`. The Bronze trigger
calls the Databricks Jobs API directly (`run-now`) rather than a local
`psql` command, since the warehouse is now a cloud workspace, not a
container on the same docker-compose network. See `AIRFLOW_README.md`
for full setup and the note on why the ADT stream runs outside this
schedule entirely.

## Kafka + Auto Loader streaming

A producer simulates ADT events; a consumer lands them as batch files;
Auto Loader ingests those into `workspace.silver.adt_events`. See
`KAFKA_README.md` for the full design, including why this isn't a
direct Kafka-to-Databricks connection (serverless network
restrictions) and why it's still a plain `kafka-python` consumer rather
than Spark Structured Streaming locally (JAR version-pinning risk,
same reasoning as before).

```bash
./scripts/run_kafka_demo.sh
```

## Sample queries

See `databricks/sql/gold_sample_queries.sql` for the full set (11
queries — the original 10 analyses, ported to Databricks SQL and
re-pointed at the actual dbt model names, plus a new bed-occupancy
query against the ADT stream). Two examples:

**Average length of stay by hospital**
```sql
SELECT
    hospital,
    COUNT(*)                      AS total_visits,
    ROUND(AVG(length_of_stay), 1) AS avg_length_of_stay_days
FROM workspace.gold.fact_hospital_visit
GROUP BY hospital
ORDER BY avg_length_of_stay_days DESC;
```

**Current bed occupancy, from the ADT stream**
```sql
SELECT hospital_id, room_no, event_type, event_time,
       ROW_NUMBER() OVER (PARTITION BY hospital_id, room_no ORDER BY event_time DESC) AS rn
FROM workspace.silver.adt_events
QUALIFY rn = 1 AND event_type != 'discharge';
```

## What's still open

Being upfront about what this rework did and didn't finish, rather than
implying more is done than actually is:

- **ICD-10-style malformed-code validation and missing-insurance-link
  flagging**, described in the original plan as new work for this
  version, haven't been added to `generate_synthetic_data.py` or the
  staging models yet.
- **Delta `MERGE`-based incremental dimension updates.** Current
  staging models are `view`/`table` full-refresh materializations.
  dbt-databricks supports `incremental` materialization with a `merge`
  strategy natively — worth implementing for at least `stg_patients`
  as a concrete before-an-interview example.
- **`docs/architecture_diagram.svg`** predates this rework and shows the
  old Postgres/MinIO/local-Spark architecture — needs regenerating.

## Challenges encountered (and how they were resolved)

Carried over from the Postgres version — these generalize into actual
lessons and remain accurate regardless of warehouse target (port
conflicts, a column-width bug cascading into FK failures across
staging tables, a GUI SQL editor silently running only the highlighted
statement, Git Bash mangling container paths, and JAR/image version
guessing on the now-removed local Spark layer). Full writeups
preserved in git history from the pre-rework version of this README —
worth re-reading before an interview even though the Spark-specific one
no longer applies to the current architecture.

## Repo structure

```
├── docker-compose.yml          # airflow + kafka/kafka-init/kafka-ui only
├── Dockerfile.airflow          # dbt-databricks, databricks provider, databricks-cli
├── AIRFLOW_README.md
├── KAFKA_README.md
├── RESUME_TALKING_POINTS.md
├── scripts/                    # zero-host-dependency pipeline runners
│   ├── generate_data.sh
│   ├── run_kafka_producer.sh
│   ├── run_kafka_consumer.sh
│   ├── run_kafka_demo.sh
│   ├── upload_adt_batch_to_databricks.sh
│   └── run_dbt.sh
├── dags/
│   └── healthcare_pipeline_dag.py
├── data/
│   ├── generate_synthetic_data.py
│   ├── kafka_producer.py
│   └── kafka_consumer.py       # writes adt_landing/*.jsonl, gitignored
├── databricks/
│   ├── README.md
│   ├── notebooks/
│   │   ├── 01_bronze_ingest.py
│   │   └── 02_adt_autoloader.py
│   └── sql/
│       └── gold_sample_queries.sql
├── dbt/
│   ├── README.md
│   ├── profiles.yml.sample
│   └── healthcare_dbt/
│       ├── dbt_project.yml
│       ├── macros/
│       └── models/
│           ├── staging/        # 12 stg_ models + sources.yml + tests -> silver
│           └── marts/          # 7 dim_/fact_ models + tests -> gold
└── docs/
    ├── erd/                    # source ER diagrams
    ├── architecture_diagram.svg  # stale, needs regenerating
    └── dbt_lineage_graph.png
```
