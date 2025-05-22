# kafka_core/consumers/execute_signals.py
# Executes high-confidence signals from Supabase using OANDA API

import os, sys
from datetime import datetime, timezone

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from kafka_core.config import USER_ID
from supabase_client.client import supabase
from oanda_api.execution import place_order  # centralized order executor

# Constants
MIN_CONFIDENCE       = 80
EXECUTION_AMOUNT     = 1000

print(f"🔍 Fetching signals for user {USER_ID} with confidence ≥ {MIN_CONFIDENCE}% and no reason set…")
signals = (
    supabase
    .table("signals")
    .select("id, symbol, signal_type, confidence, time_generated, market_snapshot")
    .eq("user_id", USER_ID)
    .gte("confidence", MIN_CONFIDENCE)
    .is_("executed", False)
    .is_("reason", None)
    .order("time_generated", desc=True)
    .limit(500)
    .execute()
    .data
)
print(f"📡 Fetched {len(signals)} signals for execution…")

for signal in signals:
    signal_id = signal["id"]
    try:
        raw_symbol = signal["symbol"]
        instrument = f"{raw_symbol[:3]}_{raw_symbol[3:]}"
        side       = "buy" if signal["signal_type"] == "buy" else "sell"
        snapshot   = signal["market_snapshot"]

        # Convert EXECUTION_AMOUNT to int and ensure it's valid
        amount = int(EXECUTION_AMOUNT)

        print(f"📈 Preparing to execute {side.upper()} {instrument} for {amount} units (signal ID: {signal_id})")

        # Step 2: Place order via OANDA API
        result = place_order(instrument, side, amount)
        if result.get("status") != "success":
            resp = result.get("response")
            if isinstance(resp, dict):
                reason = resp.get("errorMessage") or resp.get("errorCode") or str(resp)
            else:
                reason = str(resp)
            supabase.table("signals").update({"reason": reason}).eq("id", signal_id).execute()
            print(f"❌ {signal_id}: execution failed for {instrument}, reason '{reason}'")
            continue

        # Step 3: Record execution
        price = float(snapshot["ask"] if side == "buy" else snapshot["bid"])
        execution = {
            "signal_id":       signal_id,
            "executed_at":     datetime.now(timezone.utc).isoformat(),
            "price":           price,
            "amount":          amount,
            "status":          "filled",
            "broker_response": result.get("response"),
            "user_id":         USER_ID
        }
        supabase.table("executions").insert(execution).execute()
        print(f"✅ {signal_id}: executed {side.upper()} {instrument} @ {price:.5f}")

        # Step 4: Mark signal as executed
        supabase.table("signals").update({"executed": True}).eq("id", signal_id).execute()
        print(f"🔒 {signal_id}: marked as executed")

    except Exception as e:
        err = str(e)
        supabase.table("signals").update({"reason": err}).eq("id", signal_id).execute()
        print(f"⚠️ {signal_id}: error processing - {err}")
