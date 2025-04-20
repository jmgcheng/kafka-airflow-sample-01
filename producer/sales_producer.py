import os
import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import uuid
import random

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales-topic")

def create_kafka_producer(retries=10, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except NoBrokersAvailable:
            print(f"[Retry {i+1}/{retries}] Kafka broker not available, retrying in {delay}s...")
            time.sleep(delay)
    raise Exception("Kafka broker not available after retries.")

producer = create_kafka_producer()

products = [
    {"product": "Shoes", "category": "Footwear"},
    {"product": "T-Shirt", "category": "Clothing"},
    {"product": "Headphones", "category": "Electronics"},
    {"product": "Keyboard", "category": "Electronics"},
    {"product": "Backpack", "category": "Accessories"}
]
locations = ["Manila", "Cebu", "Davao", "Baguio"]
payment_methods = ["Credit Card", "Cash", "GCash", "Maya"]
devices = ["Mobile", "Desktop", "Tablet"]

while True:
    p = random.choice(products)
    sale = {
        "sale_id": str(uuid.uuid4()),
        "product": p["product"],
        "category": p["category"],
        "price": round(random.uniform(10.0, 200.0), 2),
        "quantity": random.randint(1, 5),
        "discount": round(random.uniform(0, 0.3), 2),
        "location": random.choice(locations),
        "payment_method": random.choice(payment_methods),
        "device_type": random.choice(devices),
        "customer_id": f"CUST{random.randint(1000,9999)}",
        "timestamp": time.time()
    }
    producer.send(KAFKA_TOPIC, value=sale)
    print(f"Produced: {sale}")
    time.sleep(2)
