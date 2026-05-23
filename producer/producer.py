import json
import time
import pandas as pd
from kafka import KafkaProducer

# =========================
# Configuration Kafka
# =========================

TOPIC_NAME = "ecommerce-orders"
KAFKA_SERVER = "kafka:9092"

# =========================
# Create Kafka Producer
# =========================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Kafka Producer Connected Successfully")

# =========================
# Load Dataset
# =========================

df = pd.read_csv("/app/data/ecommerce.csv")

print(f"Dataset Loaded : {len(df)} rows")

# =========================
# Send Data to Kafka
# =========================

for index, row in df.iterrows():

    # Convert row to dictionary
    data = {
        "InvoiceNo": str(row.get("InvoiceNo", "")),
        "StockCode": str(row.get("StockCode", "")),
        "Description": str(row.get("Description", "")),
        "Quantity": int(row.get("Quantity", 0)),
        "InvoiceDate": str(row.get("InvoiceDate", "")),
        "UnitPrice": float(row.get("UnitPrice", 0)),
        "CustomerID": str(row.get("CustomerID", "")),
        "Country": str(row.get("Country", ""))
    }

    # Send message to Kafka topic
    producer.send(TOPIC_NAME, value=data)

    print(f"Message Sent : {data}")

    # Simulate Real-Time Streaming
    time.sleep(1)

# =========================
# Flush & Close
# =========================

producer.flush()

print("All messages sent successfully")