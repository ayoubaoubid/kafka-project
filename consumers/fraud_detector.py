import json
import os

from kafka import KafkaConsumer


TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fraud-detector",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

for message in consumer:
    order = message.value
    quantity = int(order.get("Quantity", 0))
    unit_price = float(order.get("UnitPrice", 0))
    total = quantity * unit_price

    if quantity > 100 or total > 1000:
        print(f"Potential fraud detected: {order}")
