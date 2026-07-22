import json

from kafka import KafkaConsumer, KafkaProducer
from pydantic import ValidationError

from schema import Book


RAW_TOPIC = "books_raw"
DLQ_TOPIC = "books_dlq"


consumer = KafkaConsumer(
    RAW_TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="books-validation-group-v1",
    value_deserializer=lambda message: json.loads(
        message.decode("utf-8")
    )
)

dlq_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Consumer is validating books...")

for message in consumer:
    book_data = message.value

    try:
        valid_book = Book.model_validate(book_data)

        print(
            f"VALID: {valid_book.title} "
            f"by {valid_book.author}"
        )

    except ValidationError as error:
        rejected_record = {
            "original_record": book_data,
            "rejection_reason": error.errors(),
            "source_topic": message.topic,
            "partition": message.partition,
            "offset": message.offset
        }

        dlq_producer.send(DLQ_TOPIC, value=rejected_record)
        dlq_producer.flush()

        print(
            f"REJECTED → {DLQ_TOPIC}: "
            f"{error.errors()}"
        )