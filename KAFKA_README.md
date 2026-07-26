# Kafka Streaming Layer

Simulates near-real-time hospital event ingestion: a producer publishes
ADT (Admit/Discharge/Transfer) events to a Kafka topic; a consumer
reads them and lands them as batch files for Databricks Auto Loader to
pick up. This is the "ADT Event -> Kafka Topic -> Landing File -> Auto
Loader -> Silver Delta" pattern.

ADT is a real, HL7-adjacent healthcare concept, not a generic ops
event: hospitals need to know bed occupancy and discharge status within
minutes, not on the next batch load, which is the concrete
justification for a streaming branch existing in this pipeline at all.
(Earlier versions of this repo used a generic vitals/lab-results event
story — replaced with ADT specifically for that reason.)

## Why events land as files instead of writing directly into a warehouse

This changed for a specific, testable reason, not a style preference:
**Databricks Free Edition is serverless-only**, with outbound network
access restricted to a limited set of trusted domains. It cannot open a
connection to a Kafka broker running in local Docker — there's no
network path, regardless of client library. So neither a native Kafka
consumer nor Spark Structured Streaming can run *inside* Databricks
against this broker.

The fix: the consumer here writes cleaned events to local
newline-delimited JSON batch files (`data/adt_landing/`);
`scripts/upload_adt_batch_to_databricks.sh` pushes those into the
workspace volume; Databricks Auto Loader (`databricks/notebooks/02_adt_autoloader.py`)
picks them up incrementally from there. This is also just a legitimate,
common real-world pattern — Kafka landing to object storage before
lakehouse ingestion — not only a workaround for the free tier.

## Why a plain Kafka consumer instead of Spark Structured Streaming, even locally

Unchanged from the original reasoning: Spark's Kafka connector needs a
second, separately version-pinned JAR chain
(`spark-sql-kafka`/`kafka-clients`/`commons-pool2`) — one of the more
fragile Spark integrations to get exactly right, and irrelevant to
demonstrating the actual Kafka mechanics. A plain Kafka consumer
(`kafka-python`) shows the same core concepts — topics, partitions,
consumer groups, offset commits, at-least-once delivery — without that
specific risk. Under the hood, Spark's Kafka source *is* just a Kafka
consumer with checkpointing bolted on; this isn't a different set of
concepts, just a lighter-weight client.

## What's involved

```
docker-compose.yml     <- "kafka", "kafka-init", "kafka-ui" services
Dockerfile.airflow      <- kafka-python + databricks-cli
data/
├── kafka_producer.py           <- publishes ADT events
├── kafka_consumer.py           <- writes batched .jsonl files to data/adt_landing/
scripts/
├── run_kafka_producer.sh
├── run_kafka_consumer.sh
├── upload_adt_batch_to_databricks.sh   <- pushes the batch file to the workspace volume
└── run_kafka_demo.sh           <- producer + consumer + upload, in sequence
databricks/
├── notebooks/02_adt_autoloader.py      <- Auto Loader: landing files -> silver.adt_events
└── README.md
```

## Running it

```bash
docker compose up -d --build
./scripts/run_kafka_demo.sh
```

This publishes ~150 simulated ADT events (with small random delays
between sends, imitating real-time arrival) to the `adt-events` topic,
consumes them into a local batch file, and uploads that batch to the
Databricks volume. Then run `databricks/notebooks/02_adt_autoloader.py`
in the workspace (or trigger it as a Databricks Job) to ingest it into
`workspace.silver.adt_events`.

## Verifying it worked

```sql
-- in a Databricks SQL editor
SELECT event_type, COUNT(*) FROM workspace.silver.adt_events GROUP BY event_type;
```

Or browse the topic directly in **Kafka UI** at `http://localhost:8084`
— individual messages, partition assignment, and consumer group
offsets, worth a screenshot for a portfolio README.

## Design notes

- **Bounded, not infinite.** The consumer stops after ~10 seconds of no
  new messages rather than running forever, matching every other
  one-shot script in this project.
- **3 partitions on the topic** (set in `kafka-init`), even though a
  single consumer instance here reads all of them — this is there so
  the topic *could* support multiple parallel consumers in the same
  group later, which is the actual point of partitioning in Kafka.
- **`_rescued_data` for schema drift.** Auto Loader adds this column
  automatically for any fields that don't match the inferred schema —
  a real, built-in answer to "how do you handle a stream whose shape
  changes over time," worth having ready if asked.
