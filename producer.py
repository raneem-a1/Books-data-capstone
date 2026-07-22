import json

import pandas as pd
from kafka import KafkaProducer


RAW_TOPIC = "books_raw"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

books = pd.read_csv("data/Books.csv")

books = books.rename(
    columns={
        "Title": "title",
        "Author": "author",
        "Genre": "category",
        "Height": "pages",
        "Publisher": "publisher"
    }
)

# تعويض القيم النصية المفقودة فقط
text_columns = ["title", "author", "category", "publisher"]
books[text_columns] = books[text_columns].fillna("Unknown")

# تحويل الصفحات إلى أرقام، وأي قيمة غير صالحة تصبح صفرًا
# حتى يرفضها الـConsumer ويرسلها إلى DLQ
books["pages"] = pd.to_numeric(
    books["pages"],
    errors="coerce"
).fillna(0).astype(int)

for _, row in books.iterrows():
    book = {
        "title": str(row["title"]),
        "author": str(row["author"]),
        "category": str(row["category"]),
        "pages": int(row["pages"]),
        "publisher": str(row["publisher"])
    }

    producer.send(RAW_TOPIC, value=book)

# سجل خاطئ عمدًا لاختبار الـDLQ
bad_book = {
    "title": "",
    "author": None,
    "category": "Test",
    "pages": -5,
    "publisher": ""
}

producer.send(RAW_TOPIC, value=bad_book)

producer.flush()
producer.close()

print(
    f"Sent {len(books)} valid-source records "
    f"and 1 test record to {RAW_TOPIC}"
)