import json
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

TOPIC_NAME="clean_events"
BOOTSTRAP_SERVERS="host.docker.internal:29092"
GROUP_ID="postgres_loader"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 2345,
    "user": "postgres",
    "password": "postgres",
    "dbname": "kafka_db",
}

BATCH_SIZE = 10

INSERT_SQL = """
INSERT INTO kafka_events_silver (
    event_id,
    customer_id,
    event_type,
    amount,
    currency,
    event_timestamp
)
VALUES %s
ON CONFLICT (event_id) DO NOTHING;
"""
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
pg_conn.autocommit = False


print("connected to postgres")
print("starting kafka -> postgres loader")

buffer = []

def flush_to_postgres(records):
    rows = []
    for r in records:
        tmp = (
            r["event_id"],
            r["customer_id"],
            r["event_type"],
            r["amount"],
            r["currency"],
            r["event_timestamp"]
        )

        rows.append(tmp)

    with pg_conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
    pg_conn.commit()
    print(f"inserted {len(rows)} rows into postgres")

for message in consumer:
    event = message.value
    buffer.append({
        "event_id": event["event_id"],
        "customer_id": event["customer_id"],
        "event_type": event["event_type"],
        "amount": event["amount"],
        "currency": event["currency"],
        "event_timestamp": event["event_timestamp"]
    })

    if len(buffer) >= BATCH_SIZE:
        try:
            flush_to_postgres(buffer)
            consumer.commit()
            buffer.clear()
        except Exception as e:
            pg_conn.rollback()
            print(f"ERROR inserting batch: {e}")