"""
file: ai/train_model.py
───────────────────────────
Fetches historical FX data + signals, simulates TP/SL outcome,
trains a classifier, saves the model, and registers metadata.
"""

import os
import sys
from datetime import datetime
import json
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# 🔧 Ensure project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka_core.config import ADMIN_USER_ID , USER_ID
from supabase_client.client import supabase

# Constants
MODEL_PATH = "ai/models/signal_rf.pkl"
PIP = 0.0001

# 1. Load latest model_version for strategy/params
latest_model = supabase.table("model_versions")\
    .select("id, strategy_id, parameters")\
    .eq("user_id", ADMIN_USER_ID)\
    .order("created_at",desc=False)\
    .limit(1).execute().data

if not latest_model:
    raise Exception("❌ No model_versions found. Please create a strategy first.")

meta = latest_model[0]
strategy_id = meta["strategy_id"]
model_version_id = meta["id"]
params = meta["parameters"]

TP = params.get("TP", 10)
SL = params.get("SL", 5)
LOOKAHEAD_SEC = params.get("lookahead_sec", 600)
features = params.get("features", ["bid", "delta_ask"])

# ✅ Fetch strategy details for visibility
strategy = supabase.table("strategies")\
    .select("name, description, type, hyperparameters")\
    .eq("id", strategy_id)\
    .limit(1)\
    .execute().data

if not strategy:
    raise Exception(f"❌ Strategy not found for ID: {strategy_id}")

print(f"🧠 Loaded Strategy: {strategy[0]['name']} ({strategy[0]['type']})")
print(f"📋 Description: {strategy[0].get('description', 'No description')}")
print(f"⚙️  Hyperparameters: {strategy[0].get('hyperparameters')}")

# ✅ Save active model info immediately
active_model_path = "ai/active_model.json"
os.makedirs(os.path.dirname(active_model_path), exist_ok=True)

with open(active_model_path, "w") as f:
    json.dump({
        "strategy_id": strategy_id,
        "model_version_id": model_version_id
    }, f)

print(f"📁 Active model version preloaded from existing: {active_model_path}")

# 2. Fetch signals for this strategy
signals = supabase.table("signals")\
    .select("symbol, signal_type, market_snapshot, time_generated")\
    .eq("user_id", USER_ID)\
    .eq("strategy_id", strategy_id)\
    .execute().data

labeled = []
dates = []

# 3. Simulate TP/SL outcomes
for s in signals:
    try:
        snap = s["market_snapshot"]
        symbol = s["symbol"]
        entry_time = datetime.fromisoformat(s["time_generated"].replace("Z", "+00:00"))
        entry_price = snap["bid"] if s["signal_type"] == "sell" else snap["ask"]
        dates.append(entry_time)

        md_resp = supabase.table("market_data")\
            .select("bid, ask, timestamp")\
            .eq("symbol", symbol)\
            .eq("user_id", USER_ID)\
            .gt("timestamp", entry_time.isoformat())\
            .order("timestamp",desc=False)\
            .limit(100)\
            .execute().data

        outcome = "flat"
        for md in md_resp:
            dt = datetime.fromisoformat(md["timestamp"].replace("Z", "+00:00"))
            if (dt - entry_time).total_seconds() > LOOKAHEAD_SEC:
                break
            price = md["bid"] if s["signal_type"] == "sell" else md["ask"]
            if s["signal_type"] == "buy":
                if price >= entry_price + TP * PIP:
                    outcome = "win"
                    break
                if price <= entry_price - SL * PIP:
                    outcome = "loss"
                    break
            else:
                if price <= entry_price - TP * PIP:
                    outcome = "win"
                    break
                if price >= entry_price + SL * PIP:
                    outcome = "loss"
                    break

        labeled.append({
            "bid": snap["bid"],
            "delta_ask": snap["ask"] - snap["bid"],
            "label": outcome
        })

    except Exception as e:
        print(f"⚠️ Failed to label signal: {e}")
        continue

# 4. Train model
df = pd.DataFrame(labeled)
if df.empty:
    raise Exception("❌ Not enough data to train.")

X = df.drop(columns=["label"])
y = df["label"]

model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X, y)
accuracy = model.score(X, y)

# 5. Save model
os.makedirs("ai/models", exist_ok=True)
joblib.dump(model, MODEL_PATH)
print("✅ Model trained and saved.")

# 6. Save dataset entry
time_range = f"[{min(dates).isoformat()}, {max(dates).isoformat()}]"
dataset_resp = supabase.table("datasets").insert({
    "name": f"Dataset {datetime.now().strftime('%Y-%m-%d %H:%M')}",
    "time_range": time_range,
    "notes": f"TP={TP}, SL={SL}, Lookahead={LOOKAHEAD_SEC}s",
    "user_id": ADMIN_USER_ID
}).execute()

if not dataset_resp.data:
    raise Exception("❌ Dataset insert succeeded but returned no ID.")
dataset_id = dataset_resp.data[0]["id"]

# 7. Save new model_version
model_version_resp = supabase.table("model_versions").insert({
    "strategy_id": strategy_id,
    "version": f"v{datetime.now().strftime('%Y%m%d%H%M')}",
    "accuracy": accuracy,
    "trained_on": dataset_id,
    "parameters": {
        "TP": TP,
        "SL": SL,
        "lookahead_sec": LOOKAHEAD_SEC,
        "features": features
    },
    "user_id": ADMIN_USER_ID
}).execute()
if not model_version_resp.data:
    raise Exception("❌ Model version insert succeeded but returned no ID.")
model_version_id = model_version_resp.data[0]["id"]
active_model_path = "ai/active_model.json"
os.makedirs(os.path.dirname(active_model_path), exist_ok=True)
with open(active_model_path, "w") as f:
    json.dump({
        "strategy_id": strategy_id,
        "model_version_id": model_version_id
    }, f)

print(f"📁 Active model version written to {active_model_path}")
