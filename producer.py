import json
import time
import random
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

TOPIC_NAME = "ecommerce-events"
BOOTSTRAP_SERVERS = "localhost:9092"

# Catalog setup
CATEGORIES = ["Electronics", "Fashion", "Home & Kitchen", "Books", "Beauty"]
EVENT_TYPES = ["view", "view", "view", "add_to_cart", "checkout", "purchase"]

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def generate_ecommerce_event():
    event_type = random.choice(EVENT_TYPES)
    base_price = round(random.uniform(5.0, 850.0), 2)
    quantity = 1 if event_type in ["view", "add_to_cart"] else random.randint(1, 4)

    return {
        "event_id": fake.uuid4(),
        "user_id": f"usr_{random.randint(1000, 9999)}",
        "ip_address": fake.ipv4(),
        "device_type": random.choice(["iOS", "Android", "Desktop"]),
        "event_type": event_type,
        "product_id": f"prod_{random.randint(100, 500)}",
        "product_category": random.choice(CATEGORIES),
        "price": base_price,
        "quantity": quantity,
        "total_amount": round(base_price * quantity, 2) if event_type == "purchase" else 0.0,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

if __name__ == "__main__":
    producer = get_kafka_producer()
    print(f"[*] Starting telemetry generator to Kafka topic: {TOPIC_NAME}")
    
    try:
        while True:
            # Emit batch of 1 to 5 concurrent events
            for _ in range(random.randint(1, 5)):
                event = generate_ecommerce_event()
                producer.send(TOPIC_NAME, value=event)
                print(f"[>] Dispatched {event['event_type']} by {event['user_id']} | Category: {event['product_category']}")
            
            producer.flush()
            time.sleep(random.uniform(0.1, 0.6))
    except KeyboardInterrupt:
        print("[!] Stopping generator...")
    finally:
        producer.close()