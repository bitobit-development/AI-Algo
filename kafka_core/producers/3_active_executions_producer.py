# kafka_core/producers/active_executions_producer.py
# Kafka Producer: Active Executions Stream
# ───────────────────────────────────────────────
# Polls Supabase for all active executions and streams them to Kafka.

import os
import sys
import json
import signal
from time import sleep
from datetime import datetime,timezone
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
message_counter = 0 # 🔢 Counter for messages sent


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
    global message_counter
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        message_counter += 1
        payload = json.loads(msg.value().decode())

        trade_id = payload.get("oanda_trade_id")
        exec_id = payload.get("id")
        topic = msg.topic()

        signal_type = payload.get("signal_type", "N/A").upper()
        entry_price = payload.get("price")
        current_price = payload.get("current_price")
        pl = payload.get("pl")
        symbol = payload.get("symbol", "N/A")

        # Fallback for P/L
        if pl is None:
            pl = payload.get("broker_response", {}).get("response", {}).get("orderFillTransaction", {}).get("pl")

        pl = float(pl) if pl is not None else 0.0

        # Color P/L
        if pl > 0:
            pl_str = f"\033[92m+{pl:.2f} USD\033[0m"
        elif pl < 0:
            pl_str = f"\033[91m{pl:.2f} USD\033[0m"
        else:
            pl_str = f"{pl:.2f} USD"

        # Duration
        executed_at = payload.get("executed_at")
        try:
            exec_time = datetime.fromisoformat(executed_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            duration_seconds = int((now - exec_time).total_seconds())
        except Exception:
            duration_seconds = payload.get("duration_sec", 0)

        # Timestamp of this report
        updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        print(
            f"{message_counter:02d}. ✅ OANDA-ID ({trade_id}) Delivered execution ID → {exec_id} → {topic}\n"
            f"    ├─ ⚖️  PAIR: {symbol} | 🟩 TYPE: {signal_type} | 📍 Entry: {entry_price} → 📈 Now: {current_price}\n"
            f"    ├─ 💰 P/L: {pl_str} | ⏱️ Duration: {duration_seconds} sec | 🕒 Updated: {updated_at}\n"
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
    global message_counter
    executions = fetch_active_executions()
    print(f"📦 Current {len(executions)} active executions")
    message_counter = 0      # 🔄 reset counter at the start of each batch

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
