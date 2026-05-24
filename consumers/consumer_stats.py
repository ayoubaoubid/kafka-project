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
    group_id="consumer-stats",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

total_orders = 0
total_amount = 0.0

for message in consumer:
    order = message.value
    total_orders += 1
    total_amount += float(order.get("Quantity", 0)) * float(order.get("UnitPrice", 0))
    print(f"Orders: {total_orders} | Revenue: {total_amount:.2f}")
