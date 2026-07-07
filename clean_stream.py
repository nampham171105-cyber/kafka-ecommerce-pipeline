import json
from kafka import KafkaProducer, KafkaConsumer

BOOTSTRAP_SERVERS="host.docker.internal:29092"
INPUT_TOPIC="raw_events"
OUTPUT_TOPIC="clean_events"
GROUP_ID="silver-stream-processor"

VALID_EVENT_TYPES=["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID, # dinh danh group consumer, co the bat 2-3 scripts python cung group id de xu ly
    auto_offset_reset="earliest", # yeu cau kafka doc tu tin nhat cu nhat chua doc neu co chay lai
    enable_auto_commit=False, 
    # tat auto commit, xu ly bang tay, tranh viec mat du lieu khi may co van de
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))  
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def is_valid_event(event):
    if not event.get("customer_id"):
        return False
    if event.get("event_type") not in VALID_EVENT_TYPES:
        return False
    if event.get("amount") is None or event.get("amount") <= 0:
        return False
    if not event.get("currency"):
        return False
    return True

print("starting clean stream.........")

for message in consumer:
    key = message.key
    event = message.value

    if is_valid_event(event):
        producer.send(
            topic=OUTPUT_TOPIC,
            key=key,
            value=event
        )
        print(f"FORWARDED | key={key} | event_type={event['event_type']}")
    else:
        print(f"DROPPED | key={key} | reason=invalid")
    
    consumer.commit() 
    # tu tay conmmit sau khi da xu ly de neu co loi truoc khi den buoc lam sach thi no se doc lai tu offset chua commit