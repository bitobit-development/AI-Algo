"""
signal_generator.py
───────────────────────────
Consumes FX ticks from Kafka and generates trading signals
based on recent market data + a trained AI model (if available),
or fallback rule-based logic if the model is not ready yet.
"""

import os
import sys
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer

# 🔧 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS, USER_ID
from supabase_client.client import supabase

# Kafka setup
topic = KAFKA_TOPICS["market_data"]
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'signal-generator-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe([topic])
print(f"📡 Subscribed to topic: {topic}")

# Model setup
MODEL_PATH = "ai/models/signal_rf.pkl"
predict_signal = None
model_loaded = False

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Kafka error: {msg.error()}")
            continue

        fx_data = json.loads(msg.value().decode("utf-8"))
        print(f"📥 Tick received: {fx_data}")

        fx_data["user_id"] = USER_ID
        fx_data["created_at"] = datetime.now(timezone.utc).isoformat()
        try:
            supabase.table("market_data").insert(fx_data).execute()
            print("📊 Tick saved to Supabase.")
        except Exception as e:
            print(f"❌ Failed to insert market data: {e}")

        # Check for model (reloadable at runtime)
        if not model_loaded and os.path.exists(MODEL_PATH):
            from ai.signal_model import predict_signal
            model_loaded = True
            print("🧠 Model loaded — switching to AI mode.")

            # Load strategy + model_version from active_model.json
            try:
                with open("ai/active_model.json") as f:
                    active_meta = json.load(f)
                    model_version_id = active_meta.get("model_version_id")
                    strategy_id = active_meta.get("strategy_id")
                    print(f"🔗 Using model_version ID: {model_version_id}")
                    print(f"🧠 Strategy ID: {strategy_id}")
            except Exception as e:
                print(f"⚠️ Failed to load active model file: {e}")
                model_version_id = None
                strategy_id = None

        # Get last 3 market ticks
        history = supabase.table("market_data")\
            .select("bid, ask")\
            .eq("symbol", fx_data["symbol"])\
            .eq("user_id", USER_ID)\
            .order("timestamp", desc=True)\
            .limit(3)\
            .execute().data

        history = list(reversed(history))
        if len(history) < 3:
            print("⚠️ Not enough history for signal.")
            continue

        # Predict signal
        if model_loaded:
            try:
                print("🤖 Predicting signal using AI model...")
                result = predict_signal(history)
            except Exception as e:
                print(f"❌ Prediction error: {e}")
                continue
        else:
            print("🔧 Using fallback rule-based logic.")
            bids = [h["bid"] for h in history]
            result = {"signal_type": "hold", "confidence": 0.0}
            if bids[0] < bids[1] < bids[2]:
                result = {"signal_type": "buy", "confidence": 75.0}
            elif bids[0] > bids[1] > bids[2]:
                result = {"signal_type": "sell", "confidence": 75.0}

        # Insert signal
        if result["signal_type"] != "hold":
            print(f"🧠 Strategy [{strategy_id}] generated signal:")
            print(f"   → {result['signal_type']} @ confidence {result['confidence']}%")
            print(f"   → model_version: {model_version_id}")

            signal = {
                "symbol": fx_data["symbol"],
                "signal_type": result["signal_type"],
                "confidence": float(result["confidence"]),
                "time_generated": datetime.now(timezone.utc).isoformat(),
                "market_snapshot": fx_data,
                "user_id": USER_ID
            }
            if model_loaded and model_version_id and strategy_id:
                signal["model_version"] = model_version_id
                signal["strategy_id"] = strategy_id
            try:
                supabase.table("signals").insert(signal).execute()
                print(f"✅ Signal inserted: {result}")
            except Exception as e:
                print(f"❌ Failed to insert signal: {e}")
        else:
            print("📭 No actionable signal (hold). Skipped.")

except KeyboardInterrupt:
    print("🛑 Signal generator manually stopped.")

finally:
    consumer.close()
    print("🧹 Kafka consumer shut down.")
