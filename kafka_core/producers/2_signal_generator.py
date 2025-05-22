# file: kafka_core/producers/signal_generator.py
# Generates trading signals from market data and inserts into Supabase

import os
import sys
import json
import joblib
from datetime import datetime, timezone, time
from confluent_kafka import Consumer

# 🔧 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS, ADMIN_USER_ID, USER_ID
from supabase_client.client import supabase
from oanda_api.execution import place_order as execute_trade

# Kafka setup
topic = KAFKA_TOPICS["market_data"]
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': 'signal-generator-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe([topic])
print(f"📱 Subscribed to topic: {topic}")

try:
    strategy_pool = (
        supabase.table("model_versions")
        .select("id, strategy_id, accuracy, created_at")
        .eq("user_id", ADMIN_USER_ID)
        .order("created_at", desc=True)
        .execute()
        .data
    )
    print(f"📊 Loaded {len(strategy_pool)} strategy models.")
except Exception as e:
    print(f"⚠️ Supabase query failed: {e}")
    strategy_pool = []

try:
    from ai.signal_model import predict_signal
    model_loaded = True
except Exception as e:
    print(f"❌ Failed to load model predict function: {e}")
    model_loaded = False

print(f"✅ Model loaded: {model_loaded}")


def insert_execution_record(signal: dict, fx_data: dict, oanda_trade_id: str = None, trade_result: dict = None):
    try:
        now = datetime.now(timezone.utc).isoformat()
        current_price = (fx_data.get("bid", 0.0) + fx_data.get("ask", 0.0)) / 2

        execution = {
            "signal_id": signal.get("id"),
            "executed_at": now,
            "price": current_price,
            "amount": 1000,
            "status": "filled",
            "broker_response": trade_result,
            "symbol": fx_data.get("symbol"),
            "signal_type": signal.get("signal_type"),
            "current_price": current_price,
            "model_version": signal.get("model_version"),
            "strategy_id": signal.get("strategy_id"),
            "duration_sec": 0,
            "updated_at": now,
            "user_id": USER_ID,
        }

        if oanda_trade_id:
            execution["oanda_trade_id"] = oanda_trade_id

        supabase.table("executions").insert(execution).execute()
        print("📌 Execution record inserted.")
    except Exception as e:
        print(f"❌ Failed to insert execution: {e}")

skipped_ticks = 0

try:
    while True:
        print("🔁 Loop started")
        now = datetime.now(timezone.utc).astimezone().time()
        print(f"🕒 Time check: {now}")
        if now < time(7, 0) or now > time(22, 30):
            skipped_ticks += 1
            print(f"⏱️ Outside trading window (07:00–15:30). Tick skipped. Total skipped: {skipped_ticks}")
            continue

        open_trades = supabase.table("executions")\
            .select("id")\
            .eq("user_id", USER_ID)\
            .is_("closed_at", None)\
            .execute().data

        if len(open_trades) >= 100:
            print(f"⚠️ Max open trades (100) reached. Skipping tick.")
            continue

        print("⏳ Waiting for message...")
        msg = consumer.poll(1.0)
        if msg is None:
            print("⚠️ No message received from Kafka.")
            continue
        if msg.error():
            print(f"❌ Kafka error: {msg.error()}")
            continue

        fx_data = json.loads(msg.value().decode("utf-8"))
        print(f"✅ Tick received: {fx_data}")

        fx_data["user_id"] = USER_ID
        fx_data["created_at"] = datetime.now(timezone.utc).isoformat()

        try:
            supabase.table("market_data").insert(fx_data).execute()
            print("📊 Tick saved to Supabase.")
        except Exception as e:
            print(f"❌ Failed to insert market data: {e}")

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

        best_result = {"signal_type": "hold", "confidence": 0.0}
        best_meta = {}
        strategy_evaluations = []

        if model_loaded and strategy_pool:
            for model_meta in strategy_pool:
                try:
                    prediction = predict_signal(history, strategy_id=model_meta["strategy_id"])
                    reason = None
                    if prediction["signal_type"] == "hold":
                        reason = "Predicted 'hold' signal"
                    elif prediction["confidence"] < 60:
                        reason = f"Low confidence: {prediction['confidence']}%"

                    print(f"🧪 Tested strategy {model_meta['strategy_id']} → {prediction['signal_type']} ({prediction['confidence']}%)")

                    strategy_evaluations.append({
                        "strategy_id": model_meta["strategy_id"],
                        "model_version": model_meta["id"],
                        "confidence": prediction["confidence"],
                        "signal_type": prediction["signal_type"],
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "user_id": USER_ID,
                        "symbol": fx_data["symbol"],
                        "reason_skipped": reason
                    })

                    if prediction["confidence"] > best_result["confidence"] and prediction["signal_type"] != "hold":
                        best_result = prediction
                        best_meta = {
                            "model_version": model_meta["id"],
                            "strategy_id": model_meta["strategy_id"],
                            "model_created_at": model_meta["created_at"]
                        }

                except Exception as e:
                    print(f"⚠️ Error using model {model_meta['id']}: {e}")
                    continue

            if strategy_evaluations:
                try:
                    supabase.table("strategy_evaluations").insert(strategy_evaluations).execute()
                    print("🔏 Strategy evaluations logged.")
                except Exception as e:
                    print(f"❌ Failed to log strategy evaluations: {e}")

            if best_result["signal_type"] == "hold" or best_result["confidence"] < 60:
                print("🔍 Signal decision summary:")
                for ev in strategy_evaluations:
                    reason = ev.get("reason_skipped")
                    reason_msg = f" — Skipped: {reason}" if reason else ""
                    print(f" • Strategy {ev['strategy_id']}: {ev['signal_type']} ({ev['confidence']}%)" + reason_msg)

                print(f"🚫 Final decision: No signal inserted. Best signal was '{best_result['signal_type']}' with {best_result['confidence']}% confidence.")
                continue
        else:
            print("📬 No actionable model. Skipping.")
            continue

        print("🧠 Best Signal Summary:")
        print(f"   • Strategy ID: {best_meta.get('strategy_id')}")
        print(f"   • Model Version: {best_meta.get('model_version')}")
        print(f"   • Model Created At: {best_meta.get('model_created_at')}")
        print(f"   • Signal: {best_result['signal_type']} @ {best_result['confidence']}%")

        signal = {
            "symbol": fx_data["symbol"],
            "signal_type": best_result["signal_type"],
            "confidence": float(best_result["confidence"]),
            "time_generated": datetime.now(timezone.utc).isoformat(),
            "market_snapshot": fx_data,
            "user_id": USER_ID,
            "model_created_at": best_meta.get("model_created_at"),
            "prediction_summary": f"{best_result['signal_type']} with confidence {best_result['confidence']}% using strategy {best_meta.get('strategy_id')} and model trained at {best_meta.get('model_created_at')}",
            "stop_loss": -5.0
        }

        if best_meta.get("model_version") and best_meta.get("strategy_id"):
            signal["model_version"] = best_meta["model_version"]
            signal["strategy_id"] = best_meta["strategy_id"]

        # Step 1: Insert signal and get ID
        try:
            response = supabase.table("signals").insert(signal).execute()
            signal["id"] = response.data[0]["id"]
            print(f"✅ Signal inserted: {signal['id']}")
        except Exception as e:
            print(f"❌ Failed to insert signal: {e}")
            continue

        # Step 2: Execute trade
        try:
            symbol = fx_data["symbol"].upper()
            if "_" not in symbol and len(symbol) == 6:
                symbol = symbol[:3] + "_" + symbol[3:]
            trade_result = execute_trade(symbol, best_result["signal_type"], 1000)
            print("🚀 Executed trade on OANDA:\n")
            # {json.dumps(trade_result, indent=2)}
            # ✅ Extract OANDA trade ID from nested response
            oanda_trade_id = (
                trade_result.get("response", {})
                .get("orderFillTransaction", {})
                .get("id")
            )

            print(f"📌 OANDA Trade ID: {oanda_trade_id}")

            # Step 3: Log execution
            insert_execution_record(signal, fx_data, oanda_trade_id=oanda_trade_id, trade_result=trade_result)

        except Exception as e:
            print(f"❌ Trade execution error: {e}")

except KeyboardInterrupt:
    print("🚩 Signal generator manually stopped.")

finally:
    consumer.close()
    print("🧹 Kafka consumer shut down.")
