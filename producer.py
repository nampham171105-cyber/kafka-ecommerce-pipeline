import json
import time
from datetime import datetime, timedelta
import uuid
import random
from kafka import KafkaProducer

BOOTSTRAP_SERVERS="host.docker.internal:29092"
TOPIC_NAME="raw_events"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES=["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENT_TYPES=["CLICK", "VIEW", "PAY"]

def random_timestamp_last_6_days():
    now = datetime.utcnow()
    past = now - timedelta(days=6)

    random_seconds = random.uniform(0, (now - past).total_seconds())
    return past + timedelta(seconds=random_seconds)

def generate_event():
    event_id = str(uuid.uuid4())
    customer_id = f"CUST_{random.randint(1,5)}"
    event_type = random.choice(EVENT_TYPES)
    amount = round(random.uniform(10, 500),2)
    currency = "USD"

    is_valid = True
    invalid_field = None

    # ty le loi 25%
    if random.random() < 0.25:
        is_valid = False
        # chon ngau nhien 1 truong loi
        invalid_field = random.choice([
            "customer_id",
            "event_type",
            "amount",
            "currency"
        ])

        if invalid_field == "customer_id":
            customer_id = None
        elif invalid_field == "event_type":
            event_type = random.choice(INVALID_EVENT_TYPES)
        elif invalid_field == "amount":
            amount = round(random.uniform(-500, -10),2)
        elif invalid_field == "currency":
            currency = None
        
    event = {
        "event_id": event_id,
        "customer_id": customer_id,
        "event_type": event_type,
        "amount": amount,
        "currency": currency,
        # "event_timestamp": datetime.now().isoformat(),
        "event_timestamp": random_timestamp_last_6_days().isoformat(), # tao ra cac ngay random de chay
        "is_valid": is_valid,
        "invalid_field": invalid_field
    }

    return event["customer_id"], event

print("starting the producer......")

while True:
    key, event = generate_event()
    producer.send(
        topic=TOPIC_NAME,
        key=key,
        value=event
    )
    print(f"produced event | key={key} | value={event['is_valid']}")
    time.sleep(1)

