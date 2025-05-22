"""
ai/signal_model.py
────────────────────────────
Loads the trained signal classification model and returns a prediction
(buy/sell/hold) based on recent market data.
"""

import os
import sys
import joblib
import pandas as pd
import subprocess

# 🔧 Add project root to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ✅ Load or train model
MODEL_PATH = "ai/models/signal_rf.pkl"

def ensure_model():
    if not os.path.exists(MODEL_PATH):
        print("⚠️ Model not found. Attempting to run train_model.py automatically...")
        result = subprocess.run(["python", "ai/train_model.py"], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ Failed to train model:\n", result.stderr)
            raise FileNotFoundError("❌ Could not generate model. Check training errors.")
        print("✅ Model trained successfully.")
    return joblib.load(MODEL_PATH)

model = ensure_model()

def extract_features(history):
    """
    Converts recent market history into model features.
    Assumes history is a list of dicts with 'bid' and 'ask'.
    """
    if not history:
        raise ValueError("❌ Empty history provided to extract_features.")

    latest = history[-1]
    return pd.DataFrame([{
        "bid": latest["bid"],
        "delta_ask": latest["ask"] - latest["bid"]
    }])

def predict_signal(history, strategy_id=None):
    """
    Predicts a trading signal based on historical bid values and strategy.

    Args:
        history (list): List of dicts with 'bid' (and optionally 'ask')
        strategy_id (str): Optional strategy key, defaults to heuristic fallback

    Returns:
        dict: { signal_type: "buy"/"sell"/"hold", confidence: float }
    """
    if not history or len(history) < 3:
        return {"signal_type": "hold", "confidence": 0.0}

    bids = [row["bid"] for row in history]
    last, mid, first = bids[-1], bids[-2], bids[-3]

    # Strategy: Momentum
    if strategy_id == "momentum_v1":
        if last > mid > first:
            return {"signal_type": "buy", "confidence": 85.0}
        elif last < mid < first:
            return {"signal_type": "sell", "confidence": 85.0}

    # Strategy: Mean Reversion
    elif strategy_id == "mean_reversion_v1":
        avg = sum(bids) / len(bids)
        if last < avg * 0.998:
            return {"signal_type": "buy", "confidence": 75.0}
        elif last > avg * 1.002:
            return {"signal_type": "sell", "confidence": 75.0}

    # Strategy: Volatility Breakout
    elif strategy_id == "volatility_breakout_v1":
        volatility = max(bids) - min(bids)
        if volatility > 0.001:
            if last > max(bids[:-1]):
                return {"signal_type": "buy", "confidence": 80.0}
            elif last < min(bids[:-1]):
                return {"signal_type": "sell", "confidence": 80.0}

    # Default Fallback Logic
    if last > mid > first:
        return {"signal_type": "buy", "confidence": 70.0}
    elif last < mid < first:
        return {"signal_type": "sell", "confidence": 70.0}
    else:
        return {"signal_type": "hold", "confidence": 0.0}
