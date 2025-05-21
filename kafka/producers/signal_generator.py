"""
Signal Generator - Kafka Consumer
----------------------------------

This script listens to the 'market_data' Kafka topic and processes incoming
forex price tick data. It's the core of your signal generation system and 
will eventually produce BUY / SELL / HOLD signals based on trading logic 
or AI models.

Author: Haim (Bitobit)
Usage: python kafka/signal_generator.py
Dependencies: confluent-kafka, python-dotenv

Environment Variables:
- KAFKA_BOOTSTRAP: Kafka broker URI (e.g. 52.90.93.152:9092)
"""

import os
import json
from confluent_kafka import Consumer
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()
bootstrap_server = os.getenv("KAFKA_BOOTSTRAP")

if not bootstrap_server:
    raise ValueError("❌ KAFKA_BOOTSTRAP is missing from .env")

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': bootstrap_server,
    'group.id': 'signal-generator-group',         # Unique consumer group ID
    'auto.offset.reset': 'earliest'               # Start from beginning of topic if no offset saved
}

# Initialize consumer and subscribe to the market_data topic
consumer = Consumer(conf)
topic = "market_data"
consumer.subscribe([topic])

print(f"📡 Listening for messages on topic: '{topic}'...")

try:
    while True:
        # Poll for new messages (non-blocking)
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Kafka Error: {msg.error()}")
            continue

        # Parse and display FX tick data
        fx_data = json.loads(msg.value().decode("utf-8"))
        print(f"📥 Received FX Tick: {fx_data}")

        # TODO: Add trading logic or call AI model here

except KeyboardInterrupt:
    print("🛑 Signal generator stopped by user.")

finally:
    consumer.close()
    print("🧹 Kafka connection closed cleanly.")
