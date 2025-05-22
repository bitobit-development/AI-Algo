import os
import sys
import json
import joblib
import pandas as pd
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Optional model imports
try:
    from xgboost import XGBClassifier
except ImportError:
    XGBClassifier = None

try:
    from keras.models import Sequential
    from keras.layers import Dense, LSTM, Input
    from keras.callbacks import EarlyStopping
except ImportError:
    Sequential = LSTM = None

# 🔧 Ensure project root is accessible
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka_core.config import ADMIN_USER_ID
from supabase_client.client import supabase

# Constants
MODEL_PATH = "ai/models/signal_model.pkl"
ACTIVE_MODEL_PATH = "ai/active_model.json"
MODEL_TYPE = os.getenv("MODEL_TYPE", "rf")  # Options: rf, xgb, lstm
STRATEGY_ID = "af31b3cd-cdaa-49d9-af1b-78f1119a9548"  # UUID from strategies table

print("📁 Loading signals and executions from Supabase...")

# Fetch signal data
signals = (
    supabase.table("signals")
    .select("id, symbol, signal_type, confidence, market_snapshot, time_generated")
    .eq("user_id", ADMIN_USER_ID)
    .execute()
    .data
)

# Fetch executions joined with signals
executions = (
    supabase.table("executions")
    .select("id, signal_id, closed_at, pl, signals(symbol, time_generated)")
    .execute()
    .data
)

# Merge and build DataFrame
rows = []
for ex in executions:
    signal = ex.get("signals")
    closed_at = ex.get("closed_at")
    time_generated = signal.get("time_generated") if signal else None

    if not signal or not time_generated or not closed_at:
        continue

    rows.append({
        "symbol": signal["symbol"],
        "time_generated": time_generated,
        "closed_at": closed_at,
        "pnl": ex["pl"]
    })

df = pd.DataFrame(rows)

# 🥹 Validate data
if df.empty:
    print("⚠️ No valid executions after filtering. Dumping raw data:")
    print(executions[:5])
    raise ValueError("No valid executions found with timestamps.")

# 🧬 Feature engineering
df["hour_of_day"] = pd.to_datetime(df["time_generated"]).dt.hour
df = df.sort_values(by="time_generated")
df["rolling_mean"] = df["pnl"].rolling(window=5, min_periods=1).mean()
df["volatility"] = df["pnl"].rolling(window=5, min_periods=1).std().fillna(0)

# 🎯 Target label: profit/loss threshold
TARGET_THRESHOLD = 0.5
df["target"] = (df["pnl"] >= TARGET_THRESHOLD).astype(int)

# Split features/target
features = ["hour_of_day", "rolling_mean", "volatility"]
X = df[features]
y = df["target"]

if len(df) < 2:
    raise ValueError(f"🚫 Not enough samples to train. Only {len(df)} found.")

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 🔬 Train model
print(f"🔬 Training model type: {MODEL_TYPE.upper()}...")

if MODEL_TYPE == "rf":
    model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
    model.fit(X_train, y_train)

elif MODEL_TYPE == "xgb" and XGBClassifier:
    model = XGBClassifier(n_estimators=100, max_depth=5, learning_rate=0.1, use_label_encoder=False, eval_metric='logloss')
    model.fit(X_train, y_train)

elif MODEL_TYPE == "lstm" and Sequential:
    X_lstm = X.values.reshape((X.shape[0], 1, X.shape[1]))
    y_lstm = y.values
    model = Sequential([
        Input(shape=(1, X.shape[1])),
        LSTM(32, activation='relu'),
        Dense(1, activation='sigmoid')
    ])
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    model.fit(X_lstm, y_lstm, epochs=10, verbose=1, callbacks=[EarlyStopping(patience=2)])
else:
    raise ValueError(f"Unsupported or unavailable model type: {MODEL_TYPE}")

# 📂 Save model
os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
if MODEL_TYPE in ["rf", "xgb"]:
    joblib.dump(model, MODEL_PATH)
    print("📂 Model saved to:", MODEL_PATH)
else:
    model.save(MODEL_PATH.replace(".pkl", ".h5"))
    print("📂 LSTM model saved to:", MODEL_PATH.replace(".pkl", ".h5"))

# 📝 Log model version
model_version_response = supabase.table("model_versions").insert({
    "user_id": ADMIN_USER_ID,
    "strategy_id": STRATEGY_ID,
    "model_type": MODEL_TYPE,
    "features_used": features,
    "created_at": datetime.utcnow().isoformat(),
    "version": f"v{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
}).execute()

model_version_id = model_version_response.data[0]["id"]

# 📋 Save metadata
model_metadata = {
    "strategy_id": STRATEGY_ID,
    "model_version_id": model_version_id,
    "description": f"Trained on signals using TP=10, SL=5, Lookahead=600s | Model: {MODEL_TYPE.upper()}",
    "parameters": {"TP": 10, "SL": 5, "lookahead_sec": 600},
    "trained_at": datetime.utcnow().isoformat(),
    "features_used": features
}

with open(ACTIVE_MODEL_PATH, "w") as f:
    json.dump(model_metadata, f, indent=2)

print("📋 Metadata saved to:", ACTIVE_MODEL_PATH)
print("✅ Training complete.")
