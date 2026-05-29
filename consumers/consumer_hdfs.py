import json
import os

from hdfs import InsecureClient
from kafka import KafkaConsumer


TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
HDFS_WEB_URL = os.getenv("HDFS_WEB_URL", "http://namenode:9870")
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH", "/ecommerce/orders.jsonl")


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumer-hdfs",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

hdfs_client = InsecureClient(HDFS_WEB_URL, user="root")
hdfs_client.makedirs(os.path.dirname(HDFS_OUTPUT_PATH))
file_exists = hdfs_client.status(HDFS_OUTPUT_PATH, strict=False) is not None

for message in consumer:
    with hdfs_client.write(
        HDFS_OUTPUT_PATH,
        append=file_exists,
        overwrite=not file_exists,
        encoding="utf-8",
    ) as writer:
        writer.write(json.dumps(message.value) + "\n")
    file_exists = True
    print(f"Saved order to HDFS: {message.value.get('InvoiceNo', '')}")
