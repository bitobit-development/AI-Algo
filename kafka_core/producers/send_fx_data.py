"""
Kafka Producer: FX Market Data
────────────────────────────────
Sends simulated EURUSD FX ticks to Kafka topic defined in config.
"""

import os
import sys
import json
import time
from confluent_kafka import Producer
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS

# Validate config
if not KAFKA_BOOTSTRAP:
    raise ValueError("❌ KAFKA_BOOTSTRAP is missing in config.")

print(f"📡 Connecting to Kafka @ {KAFKA_BOOTSTRAP}")

# Kafka topic for FX data
topic = KAFKA_TOPICS["market_data"]

# Kafka producer configuration
conf = {'bootstrap.servers': KAFKA_BOOTSTRAP}
producer = Producer(conf)

# Callback for delivery confirmation
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent to {msg.topic()} [{msg.partition()}]")

# Simulate 5 FX ticks
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

# Ensure delivery
producer.flush()
