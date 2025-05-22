# Kafka Consumer: Manage Executions
# ─────────────────────────────────────────────
# Listens to execution messages, fetches live trade data from OANDA,
# updates Supabase with runtime metrics, and closes trades after logic check.

import os
import sys
import json
import time
import signal
from datetime import datetime, timezone
from confluent_kafka import Consumer

# 🔧 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from supabase_client.client import supabase
from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS
from oanda_api.check_price import get_price
from oanda_api.execution import close_trade

# ANSI color codes
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"

running = True
summary_report = []

def signal_handler(sig, frame):
    global running
    print("\n🛑 Stopping consumer gracefully...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

# Kafka setup
topic = KAFKA_TOPICS["executions"]
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "manage-executions-group",
    "auto.offset.reset": "latest"
})
consumer.subscribe([topic])

print(f"""
🚀 Manage Executions Consumer
──────────────────────────────────────
Listening to topic: {topic}
""")

def update_trade_runtime_fields(exec_id, pl, current_price, age_sec):
    try:
        supabase.table("executions").update({
            "pl": round(pl, 4),
            "current_price": current_price,
            "duration_sec": age_sec,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", exec_id).execute()
        print(f"📝 Runtime updated in Supabase")
    except Exception as e:
        print(f"❌ Failed to update runtime: {e}")

def update_trade_status_to_closed(exec_id, oanda_trade_id):
    try:
        supabase.table("executions").update({
            "status": "closed",
            "exit_reason": "max_duration",
            "closed_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", exec_id).execute()
        print(f"📦 Execution marked closed in Supabase")
    except Exception as e:
        print(f"❌ Failed to update trade status: {e}")

def get_trade_summary(oanda_trade_id):
    # TODO: Replace with real OANDA summary logic
    return {
        "pl": 12.34,
        "close_price": 1.0902
    }

def extract_oanda_trade_id(exec_data):
    try:
        order_fill = exec_data.get("broker_response", {}).get("response", {}).get("orderFillTransaction", {})
        return (
            order_fill.get("tradeOpened", {}).get("tradeID") or  # ✅ Preferred
            order_fill.get("id")  # ✅ Fallback
        )
    except Exception:
        return None

while running:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue

    try:
        print(f"\n📩 {CYAN}Received new message from Kafka{RESET}")
        exec_data = json.loads(msg.value())
        exec_id = exec_data["id"]

        oanda_trade_id = extract_oanda_trade_id(exec_data)

        if not oanda_trade_id:
            print(f"{RED}⛔ Skipping execution {exec_id} - missing OANDA trade ID.{RESET}")
            continue

        created_at = datetime.fromisoformat(exec_data["created_at"])
        symbol = exec_data["symbol"]
        if "_" not in symbol:
            symbol = symbol[:3] + "_" + symbol[3:]
        symbol = symbol.upper()
        open_price = float(exec_data["price"])
        units = int(exec_data["amount"])

        print(f"🆔 Trade ID: {exec_id} | OANDA ID: {oanda_trade_id} | Symbol: {symbol} | Amount: {units}")
        print("🔁 Starting live trade management...")

        while running:
            now = datetime.now(timezone.utc)
            age_sec = int((now - created_at).total_seconds())
            current_price = get_price(symbol)
            if current_price is None:
                print(f"⚠️ Could not fetch current price for {symbol}. Retrying...")
                time.sleep(5)
                continue

            pl = (current_price - open_price) * units if units > 0 else (open_price - current_price) * abs(units)
            pl_color = GREEN if pl >= 0 else RED
            print(f"⏱️ {age_sec}s | 📈 Price: {current_price} | 💸 P/L: {pl_color}{round(pl, 4)}{RESET}")

            update_trade_runtime_fields(exec_id, pl, current_price, age_sec)

            if age_sec >= 600:
                print(f"⌛ {YELLOW}Max duration reached. Closing trade...{RESET}")
                summary = get_trade_summary(oanda_trade_id)
                close_result = close_trade(oanda_trade_id)
                update_trade_status_to_closed(exec_id, oanda_trade_id)
                summary_report.append((exec_id, symbol, round(float(summary.get("pl", 0)), 2)))
                print("──────────────────────────────────────")
                break

            time.sleep(5)

    except Exception as e:
        print(f"❗ Error processing message: {e}")

# Final summary report
print("\n📊 Execution Summary Report")
print("────────────────────────────")
for trade_id, symbol, final_pl in summary_report:
    pl_color = GREEN if final_pl >= 0 else RED
    print(f"{trade_id} | {symbol} | Final P/L: {pl_color}{final_pl} USD{RESET}")
print("────────────────────────────")
