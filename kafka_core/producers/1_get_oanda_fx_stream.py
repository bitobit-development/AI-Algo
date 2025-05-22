"""
FILE: kafka_core/producers/send_oanda_stream.py
Kafka Producer: Send OANDA FX Stream
─────────────────────────────────────
Consumes live FX tick data from OANDA and sends it to Kafka.
Depends on oanda_api.stream for streaming connection.
"""

import os
import sys
import json
from confluent_kafka import Producer

# 🔧 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS
from oanda_api.stream import stream_prices

# Kafka config
topic = KAFKA_TOPICS["market_data"]
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered: {msg.value().decode()} → {msg.topic()}")


# Start streaming and sending to Kafka
def start():
    print("📡 Starting OANDA stream producer...")
    print(f"📦 Sending to Kafka topic: {topic}")
    for tick in stream_prices("EUR_USD"):
        producer.produce(topic, value=json.dumps(tick), callback=delivery_report)
        producer.poll(0)

if __name__ == "__main__":
    try:
        start()
    except KeyboardInterrupt:
        print("🚑 Stream manually stopped.")
    finally:
        producer.flush()
        print("🧹 Kafka producer shut down.")
