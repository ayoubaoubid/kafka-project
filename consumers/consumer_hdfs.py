import json
import os
import time

from hdfs import InsecureClient
from kafka import KafkaConsumer


TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
HDFS_WEB_URL = os.getenv("HDFS_WEB_URL", "http://namenode:9870")
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH", "/ecommerce/orders.jsonl")
HDFS_CONNECT_RETRIES = int(os.getenv("HDFS_CONNECT_RETRIES", "10"))
HDFS_CONNECT_WAIT = int(os.getenv("HDFS_CONNECT_WAIT", "6"))


def create_hdfs_client():
    """Connect to HDFS, retrying until the namenode is ready."""
    for attempt in range(1, HDFS_CONNECT_RETRIES + 1):
        try:
            client = InsecureClient(HDFS_WEB_URL, user="root")
            # Probe the connection with a simple status call
            client.status("/")
            print(f"Connected to HDFS at {HDFS_WEB_URL}")
            return client
        except Exception as exc:
            print(f"HDFS not ready yet ({attempt}/{HDFS_CONNECT_RETRIES}): {exc}")
            time.sleep(HDFS_CONNECT_WAIT)
    raise RuntimeError(f"Could not connect to HDFS after {HDFS_CONNECT_RETRIES} attempts")


def ensure_hdfs_dir(client, path):
    """Create parent directory in HDFS if it doesn't exist."""
    parent = os.path.dirname(path)
    if parent and parent != "/":
        client.makedirs(parent)


# --- Setup ---
hdfs_client = create_hdfs_client()
ensure_hdfs_dir(hdfs_client, HDFS_OUTPUT_PATH)

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="consumer-hdfs",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"HDFS Consumer listening on topic '{TOPIC_NAME}' → writing to {HDFS_OUTPUT_PATH}")

# --- Main loop ---
# Open one persistent HDFS append stream instead of reopening on every message
BATCH_SIZE = int(os.getenv("HDFS_BATCH_SIZE", "50"))
buffer = []

for message in consumer:
    order = message.value
    buffer.append(json.dumps(order))

    if len(buffer) >= BATCH_SIZE:
        payload = "\n".join(buffer) + "\n"
        with hdfs_client.write(HDFS_OUTPUT_PATH, append=True, encoding="utf-8") as writer:
            writer.write(payload)
        print(f"Flushed {len(buffer)} orders to HDFS (last InvoiceNo: {order.get('InvoiceNo', '')})")
        buffer.clear()
