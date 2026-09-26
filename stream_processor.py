import json
import hashlib
import time
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaConsumer
from sqlalchemy import create_engine

# Database Connection
POSTGRES_URL = "postgresql+psycopg2://neondb_owner:npg_0vicIyX4lPVe@ep-bitter-waterfall-b40sfr9w.c-6.us-east-2.aws.neon.tech/neondb?sslmode=require"
def get_kafka_consumer():
    return KafkaConsumer(
        "ecommerce-events",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000
    )

def anonymize_ip(ip_str):
    salt = "salt_key"
    return hashlib.sha256(f"{ip_str}||{salt}".encode()).hexdigest()

def process_stream_batch(batch_records):
    if not batch_records:
        return

    df = pd.DataFrame(batch_records)

    # 1. PII Masking
    df["anonymized_ip"] = df["ip_address"].apply(anonymize_ip)
    df.drop(columns=["ip_address"], inplace=True)

    # 2. Bronze Lakehouse Sink (Local Parquet)
    df.to_parquet("./data_lake_bronze.parquet", engine="auto", index=False)

    # 3. Gold Aggregations: Window Metrics by Category
    now = datetime.now(timezone.utc)
    summary = df.groupby("product_category").apply(
        lambda g: pd.Series({
            "window_start": now,
            "window_end": now,
            "total_gmv": g[g["event_type"] == "purchase"]["total_amount"].sum(),
            "cart_additions": (g["event_type"] == "add_to_cart").sum(),
            "completed_purchases": (g["event_type"] == "purchase").sum()
        })
    ).reset_index()

    # 4. Write to PostgreSQL (Serving Layer)
    summary.to_sql("hourly_category_metrics", con=engine, if_exists="append", index=False)
    print(f"[✓] Processed batch of {len(df)} events -> Aggregated into PostgreSQL")

def main():
    print("[*] Starting Stream Processing Engine...")
    consumer = get_kafka_consumer()
    buffer = []
    last_flush = time.time()

    while True:
        for message in consumer:
            buffer.append(message.value)
            if len(buffer) >= 15:
                process_stream_batch(buffer)
                buffer = []
                last_flush = time.time()

        if buffer and (time.time() - last_flush > 3.0):
            process_stream_batch(buffer)
            buffer = []
            last_flush = time.time()

        time.sleep(0.2)

if __name__ == "__main__":
    main()