"""
Consumes ADT events from the "adt-events" Kafka topic and writes them
out as a batched newline-delimited JSON file, instead of inserting
directly into a warehouse.

Why a file instead of a direct DB write (this is the one real design
change from the previous version of this script): Databricks Free
Edition is serverless-only, with outbound network access restricted to
a limited set of trusted domains -- it cannot open a connection back
to a Kafka broker (or any warehouse) running in local Docker. Landing
cleaned events as files and letting Databricks Auto Loader pick them up
sidesteps that constraint entirely, and mirrors a common real-world
pattern (Kafka -> object storage sink -> lakehouse ingestion) rather
than a lakehouse engine consuming the topic natively.

Still a plain Kafka consumer, not Spark Structured Streaming -- see
KAFKA_README.md for why. The core mechanics (consumer group, offset
tracking, at-least-once delivery) are unchanged regardless of what
happens to the message after it's read.

Bounded, not infinite: stops after CONSUMER_TIMEOUT_MS of no new
messages, same one-shot behavior as the rest of this project's scripts.

Run via: docker exec healthcare_airflow python3 /opt/airflow/data/kafka_consumer.py
(or scripts/run_kafka_consumer.sh, which wraps that)

Output: newline-delimited JSON batch files under ./adt_landing/, ready
to be uploaded to the Databricks volume landing folder (see
scripts/upload_adt_batch_to_databricks.sh) for Auto Loader to ingest.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "adt-events")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "adt-events-consumer")
CONSUMER_TIMEOUT_MS = int(os.environ.get("CONSUMER_TIMEOUT_MS", "10000"))

OUT_DIR = Path(os.environ.get("ADT_LANDING_DIR", "adt_landing"))


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    batch_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = OUT_DIR / f"adt_batch_{batch_stamp}.jsonl"

    print(f"Consuming from '{TOPIC}' on {BOOTSTRAP_SERVERS} "
          f"(group={GROUP_ID}, stops after {CONSUMER_TIMEOUT_MS}ms idle)...")

    counts = {"admit": 0, "discharge": 0, "transfer": 0}
    total = 0

    with open(out_path, "w") as f:
        for message in consumer:
            event = message.value
            f.write(json.dumps(event) + "\n")
            counts[event["event_type"]] = counts.get(event["event_type"], 0) + 1
            total += 1

            if total % 50 == 0:
                print(f"  ... {total} events written")

    consumer.close()

    if total == 0:
        out_path.unlink(missing_ok=True)
        print("Done. No new events on the topic -- nothing written.")
        return

    print(f"Done. Wrote {total} events "
          f"({counts.get('admit', 0)} admits, "
          f"{counts.get('discharge', 0)} discharges, "
          f"{counts.get('transfer', 0)} transfers) "
          f"to {out_path}")


if __name__ == "__main__":
    main()
