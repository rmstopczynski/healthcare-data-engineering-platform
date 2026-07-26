"""
Simulates ADT (Admit/Discharge/Transfer) events -- the near-real-time
leg of the pipeline. This is a real, HL7-adjacent healthcare pattern:
hospitals need to know bed occupancy and discharge status within
minutes, not on the next day's batch load, which is the concrete
justification for a streaming branch existing at all.

Publishes to the "adt-events" Kafka topic as JSON.

Run via: docker exec healthcare_airflow python3 /opt/airflow/data/kafka_producer.py
(or scripts/run_kafka_producer.sh, which wraps that)
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "adt-events")
NUM_EVENTS = int(os.environ.get("NUM_EVENTS", "150"))
MIN_DELAY = 0.02
MAX_DELAY = 0.15

# Matches the patient_id / hospital_id ranges used by
# generate_synthetic_data.py, so these events plausibly reference
# "real" patients/hospitals from the rest of the pipeline, even though
# there's no enforced FK relationship (same design choice as before).
PATIENT_ID_RANGE = (1, 250)
HOSPITAL_ID_RANGE = (1, 10)

EVENT_TYPES = ["admit", "discharge", "transfer"]
# Admits happen most often, discharges follow admits, transfers are rarer.
EVENT_WEIGHTS = [0.45, 0.40, 0.15]

ROOM_NO_RANGE = (100, 499)


def make_adt_event():
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    event = {
        "event_type": event_type,
        "patient_id": random.randint(*PATIENT_ID_RANGE),
        "hospital_id": random.randint(*HOSPITAL_ID_RANGE),
        "room_no": str(random.randint(*ROOM_NO_RANGE)),
    }
    if event_type == "transfer":
        event["from_room_no"] = str(random.randint(*ROOM_NO_RANGE))
        event["to_room_no"] = str(random.randint(*ROOM_NO_RANGE))
    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Publishing {NUM_EVENTS} ADT events to '{TOPIC}' on {BOOTSTRAP_SERVERS} ...")
    counts = {"admit": 0, "discharge": 0, "transfer": 0}

    for i in range(NUM_EVENTS):
        event = make_adt_event()
        event["event_id"] = str(uuid.uuid4())
        event["event_time"] = datetime.now(timezone.utc).isoformat()

        producer.send(TOPIC, value=event)
        counts[event["event_type"]] += 1

        if (i + 1) % 25 == 0:
            print(f"  ... {i + 1}/{NUM_EVENTS} sent")

        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    producer.flush()
    producer.close()

    print(f"Done. Sent {counts['admit']} admits, "
          f"{counts['discharge']} discharges, {counts['transfer']} transfers.")


if __name__ == "__main__":
    main()
