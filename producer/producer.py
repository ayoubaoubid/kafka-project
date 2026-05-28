import json
import os
import time
import zipfile

import pandas as pd
from kafka import KafkaProducer


TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DATASET_PATH = os.getenv("DATASET_PATH", "/app/data/ecommerce.csv")


def load_dataset(path):
    def read_csv_with_fallback(source):
        for encoding in ("utf-8", "latin-1", "cp1252"):
            try:
                return pd.read_csv(source, encoding=encoding)
            except UnicodeDecodeError:
                if hasattr(source, "seek"):
                    source.seek(0)
        raise UnicodeDecodeError("csv", b"", 0, 1, "Could not decode dataset with supported encodings")

    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
            if not csv_names:
                raise FileNotFoundError("No CSV file found inside the zip archive")
            with archive.open(csv_names[0]) as csv_file:
                return read_csv_with_fallback(csv_file)

    return read_csv_with_fallback(path)


def create_producer():
    for attempt in range(1, 11):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
        except Exception as exc:
            print(f"Kafka not ready yet ({attempt}/10): {exc}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka")


producer = create_producer()
print("Kafka Producer Connected Successfully")

df = load_dataset(DATASET_PATH)
print(f"Dataset Loaded : {len(df)} rows")

for _, row in df.iterrows():
    data = {
        "InvoiceNo": str(row.get("InvoiceNo", "")),
        "StockCode": str(row.get("StockCode", "")),
        "Description": str(row.get("Description", "")),
        "Quantity": int(row.get("Quantity", 0) or 0),
        "InvoiceDate": str(row.get("InvoiceDate", "")),
        "UnitPrice": float(row.get("UnitPrice", 0) or 0),
        "CustomerID": str(row.get("CustomerID", "")),
        "Country": str(row.get("Country", "")),
    }

    producer.send(TOPIC_NAME, value=data)
    print(f"Message Sent : {data}")
    time.sleep(1)

producer.flush()
print("All messages sent successfully")
