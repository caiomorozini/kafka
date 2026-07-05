import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "triagem",
    bootstrap_servers="localhost:29092",
    value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    auto_offset_reset="earliest",
)

print("Consumer started. Listening for messages...")

for msg in consumer:
    print(f"Received id: {msg.value['id']}")
