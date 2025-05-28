# file: kafka_core/producers/signal_generator.py
# Generates trading signals from market data and inserts into Supabase

import os
import sys
import json
import time
from datetime import datetime, timezone
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

# Spinner for idle periods
def idle_spinner(duration=15):
    spinner = ['|', '/', '-', '\\']
    end_time = time.time() + duration
    i = 0
    while time.time() < end_time:
        print(f"\r🧠 Thinking... {spinner[i % len(spinner)]}", end='', flush=True)
        time.sleep(0.25)
        i += 1
    print("\r", end='', flush=True)

# Helper: pip unit based on FX pair
def pip_unit(symbol: str) -> float:
    return 0.01 if symbol.endswith('JPY') else 0.0001

# Calculate stop-loss and take-profit levels
def calculate_exit_levels(signal: dict, fx_data: dict):
    entry_price = (fx_data.get('bid', 0) + fx_data.get('ask', 0)) / 2
    sl_pips = abs(signal.get('stop_loss', 5.0))
    tp_pips = sl_pips * 2
    unit = pip_unit(fx_data.get('symbol', '').upper())
    if signal.get('signal_type') == 'buy':
        sl_price = entry_price - sl_pips * unit
        tp_price = entry_price + tp_pips * unit
    else:
        sl_price = entry_price + sl_pips * unit
        tp_price = entry_price - tp_pips * unit
    return sl_price, tp_price

# Prompt user to confirm trade details
def confirm_trade(symbol: str, entry: float, signal_type: str, confidence: float, sl: float, tp: float) -> bool:
    print("────────────────────────────────────────────")
    print(f"🎯 Signal: {signal_type.upper()} @ {entry:.5f} ({confidence:.1f}%)")
    print(f"⚠️ Stop-Loss: {sl:.5f}")
    print(f"💰 Take-Profit: {tp:.5f}")
    print("────────────────────────────────────────────")
    choice = input("Place this trade? [y/N]: ").strip().lower()
    return choice == 'y'

# Insert execution record into Supabase
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

# Main signal loop

def main():
    # Load strategy pool
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

    # Load prediction function
    try:
        from ai.signal_model import predict_signal
        model_loaded = True
    except Exception as e:
        print(f"❌ Failed to load model predict function: {e}")
        model_loaded = False

    print(f"✅ Model loaded: {model_loaded}")

    print("🔁 Signal generation loop started...")
    from datetime import time as dt_time
    try:
        while True:
            now = datetime.now(timezone.utc).astimezone().time()
            if now < dt_time(7, 0) or now > dt_time(22, 30):
                continue

            # Check open trades
            open_trades = supabase.table("executions") \
                .select("id") \
                .eq("user_id", USER_ID) \
                .is_("closed_at", None) \
                .execute().data
            if open_trades:
                print("⛔ Limit reached: open trade active. Idling.")
                time_now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                supabase.table("signal_logs").insert({
                    "timestamp": time_now,
                    "status": "idle",
                    "message": "Signal generation idle: open trade active.",
                    "user_id": USER_ID
                }).execute()
                idle_spinner()
                continue

            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            fx_data = json.loads(msg.value().decode("utf-8"))
            fx_data.update({"user_id": USER_ID, "created_at": datetime.now(timezone.utc).isoformat()})
            try:
                supabase.table("market_data").insert(fx_data).execute()
            except Exception as e:
                print(f"❌ Failed to insert market data: {e}")

            # Fetch history for prediction
            history = supabase.table("market_data") \
                .select("bid, ask") \
                .eq("symbol", fx_data["symbol"]) \
                .eq("user_id", USER_ID) \
                .order("timestamp", desc=True) \
                .limit(3) \
                .execute().data
            history = list(reversed(history))
            if len(history) < 3:
                continue

            best_result = {"signal_type": "hold", "confidence": 0.0}
            best_meta = {}
            strategy_evaluations = []

            if model_loaded and strategy_pool:
                for meta in strategy_pool:
                    try:
                        pred = predict_signal(history, strategy_id=meta["strategy_id"])
                        reason = None
                        if pred["signal_type"] == "hold":
                            reason = "Predicted 'hold'"
                        elif pred["confidence"] < 60:
                            reason = f"Low confidence {pred['confidence']}%"
                        strategy_evaluations.append({
                            "strategy_id": meta["strategy_id"],
                            "model_version": meta["id"],
                            "confidence": pred["confidence"],
                            "signal_type": pred["signal_type"],
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "user_id": USER_ID,
                            "symbol": fx_data["symbol"],
                            "reason_skipped": reason
                        })
                        if pred["confidence"] > best_result["confidence"] and pred["signal_type"] != "hold":
                            best_result = pred
                            best_meta = {"model_version": meta["id"], "strategy_id": meta["strategy_id"]}
                    except Exception as e:
                        print(f"⚠️ Model error: {e}")
                        continue
                # Log evaluations
                if strategy_evaluations:
                    try:
                        supabase.table("strategy_evaluations").insert(strategy_evaluations).execute()
                    except:
                        pass
                if best_result["signal_type"] == "hold" or best_result["confidence"] < 60:
                    continue
            else:
                continue

            # Build signal payload
            signal = {
                "symbol": fx_data["symbol"],
                "signal_type": best_result["signal_type"],
                "confidence": float(best_result["confidence"]),
                "time_generated": datetime.now(timezone.utc).isoformat(),
                "market_snapshot": fx_data,
                "user_id": USER_ID,
                "model_created_at": best_meta.get("model_created_at"),
                "prediction_summary": f"{best_result['signal_type']} at {best_result['confidence']}%",
                "stop_loss": -5.0,
                "model_version": best_meta["model_version"],
                "strategy_id": best_meta["strategy_id"]
            }

            # Suggest exit levels & confirm
            sl_price, tp_price = calculate_exit_levels(signal, fx_data)
            entry = (fx_data['bid'] + fx_data['ask']) / 2
            if not confirm_trade(signal['symbol'], entry, signal['signal_type'], signal['confidence'], sl_price, tp_price):
                print("💤 Trade cancelled by user.")
                continue

            # Insert signal & execute
            try:
                res = supabase.table("signals").insert(signal).execute()
                signal["id"] = res.data[0]["id"]
            except Exception as e:
                print(f"❌ Insert signal failed: {e}")
                continue

            try:
                sym = signal['symbol'].upper()
                if '_' not in sym and len(sym) == 6:
                    sym = sym[:3] + '_' + sym[3:]
                trade_res = execute_trade(sym, signal['signal_type'], 1000)
                oid = trade_res.get('response', {}).get('orderFillTransaction', {}).get('id')
                insert_execution_record(signal, fx_data, oanda_trade_id=oid, trade_result=trade_res)
            except Exception as e:
                print(f"❌ Trade error: {e}")

    except KeyboardInterrupt:
        print("🚩 Signal generator stopped.")
    finally:
        consumer.close()
        print("🧹 Kafka consumer shut down.")

if __name__ == '__main__':
    main()
