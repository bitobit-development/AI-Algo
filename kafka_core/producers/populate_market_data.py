"""
Populate Market Data - Kafka Consumer
─────────────────────────────────────
Consumes FX ticks from Kafka topic `fx_market_data` and saves to Supabase.

Author: Bitobit
Run:    python kafka_client/producers/populate_market_data.py
"""

import os
import sys
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer

# 🔧 Allow relative import from project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from supabase_client.client import supabase
from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS, USER_ID

# ✅ Check Kafka connectivity
if not KAFKA_BOOTSTRAP:
    raise ValueError("❌ KAFKA_BOOTSTRAP is missing from config.")

topic = KAFKA_TOPICS["market_data"]

# Kafka Consumer config
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'market-data-saver',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic])

print(f"📡 Subscribed to '{topic}' — saving FX ticks to Supabase...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Kafka Error: {msg.error()}")
            continue

        fx_data = json.loads(msg.value().decode("utf-8"))
        print(f"📥 Inserting tick: {fx_data}")

        # Add user_id and created_at
        fx_data["user_id"] = USER_ID
        fx_data["created_at"] = datetime.now(timezone.utc).isoformat()

        # Save to Supabase
        supabase.table("market_data").insert(fx_data).execute()

except KeyboardInterrupt:
    print("🛑 Market data sync stopped by user.")

finally:
    consumer.close()
    print("🧹 Kafka consumer closed cleanly.")
