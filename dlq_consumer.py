import json

from kafka import KafkaConsumer

DLQ_TOPIC = "books_dlq"

consumer = KafkaConsumer(
    DLQ_TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="books-dlq-group",
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)

print("Listening to Dead Letter Queue...\n")

for message in consumer:
    print("Rejected Record:")
    print(json.dumps(message.value, indent=4))
    print("-" * 50)