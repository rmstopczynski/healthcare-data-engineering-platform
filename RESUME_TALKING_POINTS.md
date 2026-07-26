# Resume & Interview Talking Points

Maps specific pieces of this repo to specific things you can say —
resume bullets and the follow-up questions they invite. "I built a
Databricks pipeline" invites "okay, walk me through it," and you want
the next 90 seconds ready.

## Picking bullets for a specific job posting

This is one project, but it covers two genuinely different skill sets —
batch/lakehouse engineering and streaming/event-driven engineering. Use
whichever section matches what the posting actually asks for.

### If the job wants batch/lakehouse/analytics engineering

> Built a medallion-architecture (Bronze/Silver/Gold) lakehouse pipeline
> on Databricks and Delta Lake, migrating a normalized OLTP schema to a
> dimensional star schema via dbt, orchestrated end-to-end with Airflow
> with automatic retries and 60 automated data-quality tests.

> Built a dbt project with 19 models and 60 automated data-quality tests
> (uniqueness, null checks, referential integrity, accepted-value
> constraints) targeting Databricks via `dbt-databricks`, with
> auto-generated documentation and lineage graphs.

> Orchestrated a 5-task Airflow DAG (generate → upload → trigger Databricks
> Job → transform → test) via Airflow's Databricks provider and the
> Databricks Jobs API, with automatic retries and dependency enforcement.

### If the job wants streaming/event-driven engineering

> Built a Kafka producer/consumer pipeline simulating ADT (Admit/
> Discharge/Transfer) hospital events, using consumer groups, partitioned
> topics, and an incremental file-landing pattern ingested by Databricks
> Auto Loader into a Silver Delta table — a real HL7-adjacent healthcare
> streaming pattern, not a generic event simulator.

> Designed a serverless-compatible streaming ingestion path around a
> platform network constraint (Databricks Free Edition can't reach a
> local broker directly): Kafka → landing files → Auto Loader, instead
> of a native in-cluster Kafka consumer — the same pattern real Kafka-
> to-lakehouse pipelines use via an object-storage sink.

### If the job wants both (or you want to show system design judgment)

> Designed and built a hybrid batch/streaming healthcare lakehouse
> (Databricks, Delta Lake, dbt, Airflow, Kafka, Auto Loader) — a batch
> layer for historical records and a speed layer for near-real-time ADT
> events, unified in one Databricks workspace.

## If asked to elaborate

**"Walk me through the architecture."**
Say it out loud in one breath: synthetic CSVs get generated, uploaded
to a Databricks volume, loaded into Bronze Delta tables via a PySpark
notebook. dbt cleans them into Silver and models them into a dimensional
Gold star schema, with 60 tests. Airflow orchestrates all of that on a
schedule, calling the Databricks Jobs API. Separately, a Kafka
producer/consumer pair simulates ADT events, lands them as files, and
Databricks Auto Loader ingests them into their own Silver stream table
feeding a bed-occupancy query.

**"Why does one project have both batch AND streaming — isn't that
overkill?"**
A hospital system has two real data patterns — structured historical
records that arrive in batches, and operational events (bed
occupancy, discharge status) that need to be known within minutes, not
on the next day's load. This models both, landing in the same
workspace/catalog. The tell that this is a real answer and not a
rationalization: you can point to why ADT specifically (a named,
HL7-adjacent healthcare concept) rather than a generic "events" table,
and to the schema difference (the ADT stream is append-only/event-typed;
the batch star schema is strict typed dimensional columns).

**"Why Databricks and not just keep Postgres, since Postgres was
already built and working?"**
The honest answer is the strongest one here: *"Postgres was genuinely
the lower-effort choice — it already worked. I moved to Databricks
anyway because it's specifically named in the job postings I'm
targeting, in a way a generic Postgres warehouse isn't. That's a
deliberate resume-alignment tradeoff, not a technical necessity, and
I can defend it as exactly that."* This is a better answer than
pretending there was a purely technical reason, because it demonstrates
you understand the difference between "what's technically better" and
"what's strategically worth learning."

**"Why doesn't Databricks consume the Kafka topic directly?"**
This is the best question you can get, because the honest answer
demonstrates you understand your own platform's constraints rather than
just wiring tools together. *"Databricks Free Edition is serverless-
only, with outbound network access restricted to a limited set of
trusted domains — it has no network path to a broker running in my
local Docker setup, regardless of client library or connector version.
I landed cleaned events as files instead and used Auto Loader for
incremental ingestion from there, which is also just a standard
real-world pattern — Kafka landing to object storage before lakehouse
ingestion — not only a workaround."* Good follow-up if pressed further:
you verified this by hand (Auto Loader ingesting a test file, the Jobs
API reachable with a scoped token) *before* redesigning the streaming
branch around it, rather than discovering the constraint mid-build.

**"Why Postgres/MinIO/local Spark in earlier versions, and why cut
them?"**
Also a fair question, and the honest answer is a strength: *"The
original build used Postgres, MinIO (S3-pattern staging), and local
Spark because Snowflake's only free option is a time-limited trial. I
migrated the warehouse to Databricks later, and cut MinIO and a dormant
Snowflake DDL folder in the same pass — they were three different
'storage layer' stories in one repo, which added surface area to defend
without adding a distinct lesson beyond what dbt + Delta already cover."*
This shows judgment about complexity, not just tool familiarity.

**"What was the hardest bug you hit?"**
Ranked by how good a story they make:
1. **Auto Loader volume error during setup**: `CREATE VOLUME` targets a
   managed Unity Catalog object, not an arbitrary filesystem path —
   `dbutils.fs.mkdirs()` on a volume that doesn't exist yet fails with
   `UC_VOLUME_NOT_FOUND`, distinct from a normal "folder doesn't exist"
   error. Fixed by creating the volume explicitly first, then treating
   paths inside it as ordinary subfolders. Good story for "how do you
   debug a platform you're new to" — the error message itself pointed
   at the right fix once read carefully.
2. **The FK-cascade bug** (from the original Postgres build): three
   `VARCHAR(20)` phone columns were too narrow for generated data,
   causing three table loads to fail — but the error *surfaced* three
   tables downstream, on tables with no data problem at all, because
   their foreign keys pointed at the tables that failed. Good story
   about root-causing past a misleading symptom.
3. **Porting `DISTINCT ON` and `to_char()` off Postgres**: neither exists
   in Databricks/Spark SQL. `QUALIFY ROW_NUMBER() OVER (...) = 1`
   replaces the former; `date_format()`/`months_between()`/`sequence()`
   replace the latter three. Good story for "how do you handle a SQL
   dialect migration," since it's a concrete, checkable before/after.

**"How do you know the data is correct?"**
60 dbt tests running after every model build — unique/not-null on every
primary key, `relationships` tests on every fact-table foreign key,
`accepted_values` on categorical columns.

**"Is this in production anywhere / who uses it?"**
Be straightforward: it's a portfolio project using synthetic data, since
real healthcare data (PHI) can't be used for a portfolio project. What's
real is the architecture, the debugging, the working pipeline — not a
live user base.

**"Where did this project come from / whose idea was the schema?"**
*"The schema design — the star schema, the Julian date dimension — came
out of a group project for a data warehousing course. That paper
recommended migrating to a real cloud warehouse as a next step but never
did it. This project is that recommendation, executed solo, on
Databricks specifically, plus everything the original scope never
touched — dbt, Airflow, Kafka, Auto Loader."*

## What NOT to claim

- Don't say "production" or "real users" — it's a portfolio project, say so.
- Don't claim the ICD-10 malformed-code validation or missing-insurance
  flagging described in the plan docs are built — they aren't yet (see
  `databricks/README.md`, "Still open"). Say what's actually implemented.
- Don't claim Delta `MERGE`-based incremental dimension updates are in
  place — current staging models are full-refresh views/tables. Say
  it's a known, planned next step (dbt-databricks supports it natively),
  not something already done.
- Don't claim "Spark Structured Streaming" for the Kafka piece — it's a
  plain `kafka-python` consumer feeding Auto Loader, which covers the
  same core mechanics but is a different, specific thing. Say what you
  actually built.
- Don't claim you designed the schema from scratch — the star schema
  and date dimension came from group coursework; the pipeline
  implementation on top of it is yours. Say both halves.

## Quick reference: what's actually in each layer

| Layer | Real skill demonstrated | File to point to |
|---|---|---|
| Databricks/Delta Lake | Lakehouse architecture, medallion layers, serverless constraints | `databricks/README.md` |
| dbt-databricks | Data modeling, testing, documentation, SQL-dialect porting | `dbt/healthcare_dbt/models/` |
| Airflow | Orchestration, scheduling, retries, external API calls | `dags/healthcare_pipeline_dag.py` |
| PySpark (notebook) | Bronze ingestion, DataFrame API | `databricks/notebooks/01_bronze_ingest.py` |
| Kafka | Event streaming, producers/consumers, partitioning | `data/kafka_producer.py`, `data/kafka_consumer.py` |
| Auto Loader | Serverless incremental ingestion, schema drift handling | `databricks/notebooks/02_adt_autoloader.py` |
| Debugging | Root-causing past misleading symptoms, platform-constraint design | README's Challenges section, this doc |
