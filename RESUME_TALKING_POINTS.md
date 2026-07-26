# Resume & Interview Talking Points

Maps specific pieces of this repo to specific things you can say — resume bullets and the follow-up questions they invite. "I built a Databricks pipeline" invites "okay, walk me through it," and you want the next 90 seconds ready.

## Picking bullets for a specific job posting

This is one project, but it covers two genuinely different skill sets — batch/lakehouse engineering and streaming/event-driven engineering. Use whichever section matches what the posting actually asks for.

### If the job wants batch/lakehouse/analytics engineering

> Built a medallion-architecture (Bronze/Silver/Gold) lakehouse pipeline on Databricks and Delta Lake, migrating a normalized OLTP schema to a dimensional star schema via dbt, orchestrated end-to-end with Airflow and the Databricks Jobs API, with automatic retries and 60 automated data-quality tests.

> Built a dbt project with 19 models and 60 automated data-quality tests (uniqueness, null checks, referential integrity, accepted-value constraints) targeting Databricks via `dbt-databricks`, with auto-generated documentation and lineage graphs.

> Orchestrated a 5-task Airflow DAG (generate → upload → trigger Databricks Job → transform → test) via the Databricks Jobs API, with automatic retries and dependency enforcement — debugged and verified running end to end against a live Databricks workspace.

### If the job wants streaming/event-driven engineering

> Built a Kafka producer/consumer pipeline simulating ADT (Admit/Discharge/Transfer) hospital events, using consumer groups, partitioned topics, and an incremental file-landing pattern ingested by Databricks Auto Loader into a Silver Delta table — a real HL7-adjacent healthcare streaming pattern, not a generic event simulator.

> Designed a serverless-compatible streaming ingestion path around a platform network constraint (Databricks Free Edition can't reach a local broker directly): Kafka → landing files → Auto Loader, instead of a native in-cluster Kafka consumer — the same pattern real Kafka-to-lakehouse pipelines use via an object-storage sink. Verified end to end, including recovering from a real Auto Loader schema-evolution event.

### If the job wants both (or you want to show system design judgment)

> Designed and built a hybrid batch/streaming healthcare lakehouse (Databricks, Delta Lake, dbt, Airflow, Kafka, Auto Loader) — a batch layer for historical records and a speed layer for near-real-time ADT events, unified in one Databricks workspace, fully orchestrated and verified running end to end.

## If asked to elaborate

**"Walk me through the architecture."**
Say it out loud in one breath: synthetic CSVs get generated, uploaded to a Databricks volume, loaded into Bronze Delta tables via a PySpark notebook. dbt cleans them into Silver and models them into a dimensional Gold star schema, with 60 tests. Airflow orchestrates all of that on a schedule, calling the Databricks Jobs API. Separately, a Kafka producer/consumer pair simulates ADT events, lands them as files, and Databricks Auto Loader ingests them into their own Silver stream table feeding a bed-occupancy query.

**"Why does one project have both batch AND streaming — isn't that overkill?"**
A hospital system has two real data patterns — structured historical records that arrive in batches, and operational events (bed occupancy, discharge status) that need to be known within minutes, not on the next day's load. This models both, landing in the same workspace/catalog. The tell that this is a real answer and not a rationalization: you can point to why ADT specifically (a named, HL7-adjacent healthcare concept) rather than a generic "events" table, and to the schema difference (the ADT stream is append-only/event-typed; the batch star schema is strict typed dimensional columns).

**"Why Databricks and not just keep Postgres, since Postgres was already built and working?"**
The honest answer is the strongest one here: *"Postgres was genuinely the lower-effort choice — it already worked. I moved to Databricks anyway because it's specifically named in the job postings I'm targeting, in a way a generic Postgres warehouse isn't. That's a deliberate resume-alignment tradeoff, not a technical necessity, and I can defend it as exactly that."* This is a better answer than pretending there was a purely technical reason, because it demonstrates you understand the difference between "what's technically better" and "what's strategically worth learning."

**"Why doesn't Databricks consume the Kafka topic directly?"**
*"Databricks Free Edition is serverless-only, with outbound network access restricted to a limited set of trusted domains — it has no network path to a broker running in my local Docker setup, regardless of client library or connector version. I landed cleaned events as files instead and used Auto Loader for incremental ingestion from there, which is also just a standard real-world pattern — Kafka landing to object storage before lakehouse ingestion — not only a workaround."* Good follow-up if pressed further: you verified this by hand before redesigning the streaming branch around it, then ran the full producer → consumer → CLI upload → Auto Loader chain end to end with real data, including hitting and resolving an actual schema-evolution event mid-stream.

**"What's the hardest bug you hit during the Databricks/Airflow migration specifically?"**
Genuinely a tossup between two, both good because the fix was quick once the actual cause was found — the value was in the diagnosis:
1. **The same environment-path-mangling bug recurring a third time, in a new form.** Git Bash's MSYS layer had already broken a `docker exec` command and a `spark-submit` invocation earlier in the project's life by silently rewriting POSIX-style paths into Windows paths. During the Databricks migration it did it again — this time to an *environment variable* (`DATABRICKS_HTTP_PATH`), producing a 404 that looked like a credentials problem rather than a path problem. Recognizing the pattern immediately from having seen it twice before is itself the interview-worthy part.
2. **Databricks personal access token scopes are capability-specific, not one bundled toggle.** A token that worked fine for dbt (`sql` + `unity-catalog`) and file uploads (`files`) still returned a 403 from the Jobs API with a message that plainly named the missing scope (`"does not have required scopes: jobs"`). Understanding that `sql`/`unity-catalog`/`files`/`jobs` are genuinely separate permissions — not "API access, on or off" — was the actual unlock.

**"Tell me about a time you had to debug something you didn't build."**
The legacy-vs-modern Databricks CLI issue is a strong answer here: `pip install databricks-cli` silently installs an old client that talks to a different (classic DBFS) API than Unity Catalog Volumes expect, producing a `://None/...` error that looked exactly like a missing credential. The actual fix was recognizing it was the wrong *tool*, not a misconfigured one — and then discovering the same wrong version was independently installed inside a Docker container too, requiring the fix twice, in two different environments, for the same underlying reason.

**"How do you know the data is correct?"**
60 dbt tests running after every model build — unique/not-null on every primary key, `relationships` tests on every fact-table foreign key, `accepted_values` on categorical columns. All 60 verified passing against live Databricks data, not just designed to pass.

**"Is this in production anywhere / who uses it?"**
Be straightforward: it's a portfolio project using synthetic data, since real healthcare data (PHI) can't be used for a portfolio project. What's real is the architecture, the debugging, the working pipeline — not a live user base.

**"Where did this project come from / whose idea was the schema?"**
*"The schema design — the star schema, the Julian date dimension — came out of a group project for a data warehousing course. That paper recommended migrating to a real cloud warehouse as a next step but never did it. This project is that recommendation, executed solo, on Databricks specifically, plus everything the original scope never touched — dbt, Airflow, Kafka, Auto Loader — all wired together and actually running, not just designed."*

## What NOT to claim

- Don't say "production" or "real users" — it's a portfolio project, say so.
- Don't claim the ICD-10 malformed-code validation or missing-insurance flagging described in the plan docs are built — they aren't yet (see `databricks/README.md`, "Still open"). Say what's actually implemented.
- Don't claim Delta `MERGE`-based incremental dimension updates are in place — current staging models are full-refresh views/tables. Say it's a known, planned next step (dbt-databricks supports it natively), not something already done.
- Don't claim "Spark Structured Streaming" for the Kafka piece — it's a plain `kafka-python` consumer feeding Auto Loader, which covers the same core mechanics but is a different, specific thing. Say what you actually built.
- Don't claim the producer/consumer event counts reconcile exactly — a small discrepancy (150 sent vs. ~128 landed in one run) was observed and not fully root-caused; it's noted honestly in the README rather than hidden.
- Don't claim you designed the schema from scratch — the star schema and date dimension came from group coursework; the pipeline implementation on top of it is yours. Say both halves.

## Quick reference: what's actually in each layer

| Layer | Real skill demonstrated | File to point to |
|---|---|---|
| Databricks/Delta Lake | Lakehouse architecture, medallion layers, serverless constraints | `databricks/README.md` |
| dbt-databricks | Data modeling, testing, documentation, SQL-dialect porting | `dbt/healthcare_dbt/models/` |
| Airflow | Orchestration, scheduling, retries, external API calls, environment isolation debugging | `dags/healthcare_pipeline_dag.py`, README's rework debugging log |
| PySpark (notebook) | Bronze ingestion, DataFrame API | `databricks/notebooks/01_bronze_ingest.py` |
| Kafka | Event streaming, producers/consumers, partitioning | `data/kafka_producer.py`, `data/kafka_consumer.py` |
| Auto Loader | Serverless incremental ingestion, schema drift handling | `databricks/notebooks/02_adt_autoloader.py` |
| Debugging | Root-causing past misleading symptoms, platform-constraint design, environment/tooling troubleshooting | README's Challenges + rework debugging log sections, this doc |
