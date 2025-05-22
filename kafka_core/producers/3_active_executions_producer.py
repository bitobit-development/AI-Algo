# kafka_core/producers/active_executions_producer.py
# Kafka Producer: Active Executions Stream
# ───────────────────────────────────────────────
# Polls Supabase for all active executions and streams them to Kafka.

import os
import sys
import json
import signal
from time import sleep
from datetime import datetime
from confluent_kafka import Producer

# 🔧 Add project root to sys.path
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS
from supabase_client.client import supabase

# Kafka config
topic = KAFKA_TOPICS["executions"]
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

running = True


# 🧹 Graceful shutdown on Ctrl+C
def signal_handler(sig, frame):
    global running
    print("\n🛑 Gracefully stopping producer...")
    running = False
    producer.flush()
    print("🧹 Kafka producer shut down.")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


# 📨 Delivery report callback
def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Delivered execution ID → {json.loads(msg.value().decode()).get('id')} → {msg.topic()}"
        )


# 📡 Fetch open executions from Supabase
def fetch_active_executions():
    return supabase.table("executions") \
        .select("*") \
        .is_("closed_at", None) \
        .not_.is_("oanda_trade_id", None) \
        .execute() \
        .data


# 🚀 Send active executions to Kafka
def produce_executions():
    executions = fetch_active_executions()
    print(f"📦 Current {len(executions)} active executions")

    for exec_data in executions:
        message = json.dumps(exec_data)
        producer.produce(topic, value=message, callback=delivery_report)
        producer.poll(0)
        sleep(1)  # ⏱️ Wait 1 second per row to limit rate


if __name__ == "__main__":
    print("📡 Starting Active Executions Producer...")
    print(f"📦 Sending to Kafka topic: {topic}")
    while running:
        produce_executions()
