# kafka_core/consumers/manage_executions.py
# Monitors active executions and closes trades to enforce 100 max open rule

import os, sys
from datetime import datetime, timezone
from time import sleep

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from supabase_client.client import supabase
from kafka_core.config import USER_ID
from oanda_api.openTrade import get_open_trades
from oanda_api.check_price import get_price
from oanda_api.execution import close_trade

MAX_ACTIVE_EXECUTIONS = 100
LOG_DIR = "logs"
LOG_PATH = os.path.join(LOG_DIR, "manage-exec.log")

os.makedirs(LOG_DIR, exist_ok=True)
log_file = open(LOG_PATH, "a")

def log(message):
    timestamp = datetime.now(timezone.utc).isoformat()
    full_msg = f"[{timestamp}] {message}"
    print(full_msg)
    log_file.write(full_msg + "\n")

log("🔄 Checking active executions...")

# Step 1: Get all open trades from OANDA
try:
    open_trades = get_open_trades()
except Exception as e:
    log(f"❌ Failed to fetch open trades: {e}")
    sys.exit(1)

trade_metrics = []
total_pl = 0
closed_count = 0
updated_count = 0
skipped_fifo = 0
valid_trades = []
total_age_sec = 0

now_utc = datetime.now(timezone.utc)

for trade in open_trades:
    try:
        instrument = trade["instrument"]
        units = int(trade["currentUnits"])
        trade_id = trade["id"]

        if units == 0:
            continue

        open_price = float(trade["price"])
        current_price = get_price(instrument)
        trade_open_time = datetime.fromisoformat(trade["openTime"])
        age_sec = (now_utc - trade_open_time).total_seconds()
        total_age_sec += age_sec

        pl = (current_price - open_price) * units if units > 0 else (open_price - current_price) * abs(units)
        total_pl += pl

        trade_info = {
            "trade_id": trade_id,
            "instrument": instrument,
            "units": units,
            "open_price": open_price,
            "current_price": current_price,
            "pl": round(pl, 5),
            "duration_sec": int(age_sec)
        }
        trade_metrics.append(trade_info)
        valid_trades.append(trade_info)

        log(f"🔍 Trade {trade_id}: {instrument} | Units: {units} | Open: {open_price} | Now: {current_price} | PL: {pl:.5f} | Duration: {int(age_sec)}s")

        supabase.table("executions").update({
            "current_price": current_price,
            "pl": round(pl, 5),
            "duration_sec": int(age_sec),
            "updated_at": now_utc.isoformat()
        }).eq("broker_response->>id", str(trade_id)).execute()
        updated_count += 1

    except Exception as ex:
        log(f"⚠️ Failed to process trade {trade.get('id')}: {ex}")

open_count = len(valid_trades)
if open_count <= MAX_ACTIVE_EXECUTIONS:
    log(f"✅ Within execution limit ({open_count}/{MAX_ACTIVE_EXECUTIONS})")
    log_file.close()
    sys.exit(0)

log(f"⚠️ Over limit: {open_count} valid active trades. Evaluating losers to close...")

open_execs = (
    supabase.table("executions")
    .select("id, signal_id, price, amount, user_id, broker_response")
    .eq("user_id", USER_ID)
    .is_("closed_at", None)
    .execute()
    .data
)

trade_metrics.sort(key=lambda t: t["pl"])
excess = open_count - MAX_ACTIVE_EXECUTIONS
closed_this_run = 0

for t in trade_metrics:
    if closed_this_run >= excess:
        break
    try:
        log(f"❌ Closing trade {t['trade_id']} ({t['instrument']}) | P/L: {t['pl']}")
        result = close_trade(t["trade_id"])
        log(f"🔁 Close response: {result['response']}")

        if result["status"] != "success":
            reason = result["response"].get("errorCode") or result["response"].get("errorMessage")
            if reason == "FIFO_VIOLATION":
                log(f"↪️ Skipped closing trade {t['trade_id']} due to FIFO_VIOLATION.")
                skipped_fifo += 1
                continue
            else:
                raise Exception(reason)

        log(f"✅ Trade {t['trade_id']} closed successfully.")

        supabase.table("executions").update({
            "closed_at": now_utc.isoformat(),
            "close_price": t["current_price"],
            "pl": t["pl"],
            "updated_at": now_utc.isoformat()
        }).eq("broker_response->>id", str(t["trade_id"])).execute()
        closed_count += 1
        updated_count += 1
        closed_this_run += 1

    except Exception as e:
        log(f"❌ Failed to close trade {t['trade_id']}: {e}")

avg_duration = total_age_sec / open_count if open_count else 0
log("\n📊 Summary Report")
log(f"🔓 Open trades from OANDA: {len(open_trades)}")
log(f"✅ Trades closed this run: {closed_count}")
log(f"↪️ Skipped due to FIFO_VIOLATION: {skipped_fifo}")
log(f"📝 Executions updated in Supabase: {updated_count}")
log(f"💰 Total running P/L (approx.): {round(total_pl, 2)}")
log(f"📉 Remaining open trades after cleanup: {open_count - closed_count}")
log(f"📆 Avg. Trade Duration (sec): {int(avg_duration)}")
log_file.close()
