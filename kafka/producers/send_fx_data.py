import os
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load .env for Kafka config
load_dotenv()
bootstrap_server = os.getenv("KAFKA_BOOTSTRAP")
if not bootstrap_server:
    raise ValueError("❌ KAFKA_BOOTSTRAP is missing from .env")

print(f"Connecting to Kafka @ {bootstrap_server}")

conf = {
    'bootstrap.servers': bootstrap_server
}

producer = Producer(conf)
topic = "market_data"

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent to {msg.topic()} [{msg.partition()}]")

# Simulate sending 5 FX ticks
for i in range(5):
    fx_data = {
        "symbol": "EURUSD",
        "bid": round(1.0720 + i * 0.0001, 5),
        "ask": round(1.0725 + i * 0.0001, 5),
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ')
    }

    producer.produce(topic, value=json.dumps(fx_data), callback=delivery_report)
    producer.poll(0)
    time.sleep(1)

producer.flush()
