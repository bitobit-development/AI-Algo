"""
ai/signal_model.py
─────────────────────────
Loads the trained signal classification model and returns a prediction
(buy/sell/hold) from recent market_data.
"""

import os
import sys
import joblib
import pandas as pd

# 🔧 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ✅ Load trained model
model_path = "ai/models/signal_rf.pkl"
if not os.path.exists(model_path):
    print("⚠️ Model not found. Attempting to run train_model.py automatically...")

    import subprocess
    result = subprocess.run(["python", "ai/train_model.py"], capture_output=True, text=True)

    if result.returncode != 0:
        print("❌ Failed to train model:\n", result.stderr)
        raise FileNotFoundError("❌ Could not generate model. Check training errors.")

    print("✅ Model trained successfully.")

model = joblib.load(model_path)


def extract_features(history):
    """
    Takes recent history (list of dicts with 'bid' and 'ask') and
    returns a pandas DataFrame with model input features.
    """
    latest = history[-1]
    return pd.DataFrame([{
        "bid": latest["bid"],
        "delta_ask": latest["ask"] - latest["bid"]
    }])


def predict_signal(history):
    """
    Returns {'signal_type': 'buy'|'sell'|'hold', 'confidence': float}
    given recent market_data history.
    """
    if len(history) < 1:
        return {"signal_type": "hold", "confidence": 0.0}

    features = extract_features(history)
    prediction = model.predict(features)[0]
    proba = model.predict_proba(features)[0]
    confidence = max(proba) * 100

    return {
        "signal_type": prediction,
        "confidence": round(confidence, 2)
    }
