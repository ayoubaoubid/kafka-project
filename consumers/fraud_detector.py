import json
import math
import os
import time
from datetime import datetime

import joblib
import pandas as pd
from kafka import KafkaConsumer


TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce-orders")
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MODEL_OUTPUT_PATH = os.getenv("MODEL_OUTPUT_PATH", "/app/models/fraud_detection_pipeline.pkl")
MODEL_WAIT_TIMEOUT = int(os.getenv("MODEL_WAIT_TIMEOUT", "180"))
ALERT_SCORE_THRESHOLD = float(os.getenv("ALERT_SCORE_THRESHOLD", "0"))


def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="fraud-detector",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def wait_for_model_bundle():
    deadline = time.time() + MODEL_WAIT_TIMEOUT
    while time.time() < deadline:
        if os.path.exists(MODEL_OUTPUT_PATH):
            print(f"Loading model bundle from: {MODEL_OUTPUT_PATH}")
            return joblib.load(MODEL_OUTPUT_PATH)
        print(f"Waiting for trained model bundle at {MODEL_OUTPUT_PATH} ...")
        time.sleep(5)
    raise FileNotFoundError(f"Model bundle not found after {MODEL_WAIT_TIMEOUT}s: {MODEL_OUTPUT_PATH}")


def parse_invoice_datetime(invoice_date):
    if not invoice_date:
        return None

    for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(str(invoice_date), fmt)
        except ValueError:
            continue
    return None


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_clean_string(value, default="UNKNOWN"):
    text = "" if value is None else str(value).strip()
    return text if text else default


def running_average(current_avg, current_count, new_value):
    if current_count <= 0:
        return new_value
    return ((current_avg * current_count) + new_value) / (current_count + 1)


def get_customer_stats(customer_id, model_bundle):
    default = {
        "customer_order_count": model_bundle["default_customer_order_count"],
        "customer_cancel_count": model_bundle["default_customer_cancel_count"],
        "customer_avg_price": model_bundle["default_customer_avg_price"],
        "customer_cancel_ratio": model_bundle["default_customer_cancel_ratio"],
    }
    return model_bundle["customer_stats_map"].get(customer_id, default)


def get_stock_stats(stock_code, model_bundle):
    default = {
        "stock_frequency": model_bundle["default_stock_frequency"],
        "stock_avg_price": model_bundle["default_stock_avg_price"],
    }
    return model_bundle["stock_stats_map"].get(stock_code, default)


def get_invoice_state(invoice_no, quantity_abs, line_total_abs, stock_code, invoice_states):
    current = invoice_states.get(
        invoice_no,
        {"stock_codes": set(), "invoice_total_qty": 0.0, "invoice_total_amount": 0.0},
    )
    stock_codes = set(current["stock_codes"])
    stock_codes.add(stock_code)
    return {
        "invoice_distinct_items": len(stock_codes),
        "invoice_total_qty": current["invoice_total_qty"] + quantity_abs,
        "invoice_total_amount": current["invoice_total_amount"] + line_total_abs,
    }


def build_feature_row(order, model_bundle, invoice_states):
    invoice_no = to_clean_string(order.get("InvoiceNo"), "")
    stock_code = to_clean_string(order.get("StockCode"))
    description = to_clean_string(order.get("Description"), "NO_DESC")
    customer_id = to_clean_string(order.get("CustomerID"), "GUEST")
    country = to_clean_string(order.get("Country"))

    quantity = to_float(order.get("Quantity"))
    unit_price = max(to_float(order.get("UnitPrice")), 0.0)
    quantity_abs = abs(quantity)
    line_total = quantity * unit_price
    line_total_abs = abs(line_total)
    invoice_dt = parse_invoice_datetime(order.get("InvoiceDate"))

    invoice_hour = invoice_dt.hour if invoice_dt else model_bundle["numeric_defaults"]["invoice_hour"]
    invoice_dayofweek = (
        invoice_dt.isoweekday() % 7 + 1
        if invoice_dt
        else model_bundle["numeric_defaults"]["invoice_dayofweek"]
    )
    invoice_month = invoice_dt.month if invoice_dt else model_bundle["numeric_defaults"]["invoice_month"]
    is_weekend = 1 if invoice_dt and invoice_dt.weekday() >= 5 else 0
    is_unusual_hour = 1 if invoice_dt and (invoice_dt.hour >= 22 or invoice_dt.hour <= 6) else 0

    customer_stats = get_customer_stats(customer_id, model_bundle)
    stock_stats = get_stock_stats(stock_code, model_bundle)
    country_frequency = model_bundle["country_frequency_map"].get(country, model_bundle["default_country_frequency"])
    invoice_stats = get_invoice_state(invoice_no, quantity_abs, line_total_abs, stock_code, invoice_states)
    price_quantity_ratio = unit_price / quantity_abs if quantity_abs > 0 else 0.0

    feature_row = {
        "quantity_log": math.log1p(quantity_abs),
        "unit_price_log": math.log1p(unit_price),
        "line_total_log": math.log1p(line_total_abs),
        "price_quantity_ratio": price_quantity_ratio,
        "description_length": len(description),
        "invoice_hour": invoice_hour,
        "invoice_dayofweek": invoice_dayofweek,
        "invoice_month": invoice_month,
        "is_weekend": is_weekend,
        "is_unusual_hour": is_unusual_hour,
        "is_cancelled": 1 if invoice_no.startswith("C") else 0,
        "is_return": 1 if quantity < 0 else 0,
        "customer_order_count": customer_stats["customer_order_count"],
        "customer_cancel_ratio": customer_stats["customer_cancel_ratio"],
        "customer_avg_price": customer_stats["customer_avg_price"],
        "stock_frequency": stock_stats["stock_frequency"],
        "stock_avg_price": stock_stats["stock_avg_price"],
        "country_frequency": country_frequency,
        "invoice_distinct_items": invoice_stats["invoice_distinct_items"],
        "invoice_total_qty": invoice_stats["invoice_total_qty"],
        "invoice_total_amount": invoice_stats["invoice_total_amount"],
        "Country": country,
        "StockCode": stock_code,
    }

    for feature_name, default_value in model_bundle["numeric_defaults"].items():
        if feature_name not in feature_row or pd.isna(feature_row[feature_name]):
            feature_row[feature_name] = default_value

    return feature_row


def update_reference_maps(order, model_bundle, invoice_states):
    invoice_no = to_clean_string(order.get("InvoiceNo"), "")
    stock_code = to_clean_string(order.get("StockCode"))
    customer_id = to_clean_string(order.get("CustomerID"), "GUEST")
    country = to_clean_string(order.get("Country"))

    quantity = to_float(order.get("Quantity"))
    unit_price = max(to_float(order.get("UnitPrice")), 0.0)
    quantity_abs = abs(quantity)
    line_total_abs = abs(quantity * unit_price)
    is_cancelled = 1 if invoice_no.startswith("C") else 0

    customer_stats = get_customer_stats(customer_id, model_bundle)
    current_count = customer_stats["customer_order_count"]
    current_cancel_count = customer_stats.get("customer_cancel_count", 0)
    current_avg_price = customer_stats["customer_avg_price"]

    updated_customer_count = current_count + 1
    updated_cancel_count = current_cancel_count + is_cancelled
    updated_avg_price = running_average(current_avg_price, current_count, unit_price)
    model_bundle["customer_stats_map"][customer_id] = {
        "customer_order_count": updated_customer_count,
        "customer_cancel_count": updated_cancel_count,
        "customer_avg_price": updated_avg_price,
        "customer_cancel_ratio": updated_cancel_count / updated_customer_count if updated_customer_count else 0.0,
    }

    stock_stats = get_stock_stats(stock_code, model_bundle)
    stock_count = stock_stats["stock_frequency"]
    stock_avg_price = stock_stats["stock_avg_price"]
    model_bundle["stock_stats_map"][stock_code] = {
        "stock_frequency": stock_count + 1,
        "stock_avg_price": running_average(stock_avg_price, stock_count, unit_price),
    }

    model_bundle["country_frequency_map"][country] = model_bundle["country_frequency_map"].get(
        country,
        model_bundle["default_country_frequency"],
    ) + 1

    current_invoice = invoice_states.get(
        invoice_no,
        {"stock_codes": set(), "invoice_total_qty": 0.0, "invoice_total_amount": 0.0},
    )
    current_invoice["stock_codes"].add(stock_code)
    current_invoice["invoice_total_qty"] += quantity_abs
    current_invoice["invoice_total_amount"] += line_total_abs
    invoice_states[invoice_no] = current_invoice


def main():
    model_bundle = wait_for_model_bundle()
    pipeline = model_bundle["pipeline"]
    feature_columns = model_bundle["numeric_features"] + model_bundle["categorical_features"]
    consumer = create_consumer()
    invoice_states = {}

    print(f"Fraud detector listening on topic '{TOPIC_NAME}' via {KAFKA_SERVER}")

    for message in consumer:
        order = message.value
        feature_row = build_feature_row(order, model_bundle, invoice_states)
        inference_df = pd.DataFrame([feature_row], columns=feature_columns)

        prediction = int(pipeline.predict(inference_df)[0])
        anomaly_score = float(pipeline.score_samples(inference_df)[0])
        is_fraud = prediction == -1 and anomaly_score <= ALERT_SCORE_THRESHOLD

        result = {
            "InvoiceNo": order.get("InvoiceNo", ""),
            "CustomerID": order.get("CustomerID", ""),
            "Country": order.get("Country", ""),
            "Quantity": order.get("Quantity", 0),
            "UnitPrice": order.get("UnitPrice", 0),
            "prediction": prediction,
            "anomaly_score": round(anomaly_score, 6),
            "is_fraud": is_fraud,
        }

        if is_fraud:
            print(f"FRAUD ALERT: {json.dumps(result)}")
        else:
            print(f"Order scored: {json.dumps(result)}")

        update_reference_maps(order, model_bundle, invoice_states)


if __name__ == "__main__":
    main()
