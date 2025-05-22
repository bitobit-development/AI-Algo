# close_all_active_trades.py
# Forcefully closes all open OANDA trades

import os
import sys
from datetime import datetime, timezone

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from oanda_api.openTrade import get_open_trades
from oanda_api.execution import close_trade

# Log setup
LOG_DIR = "logs"
LOG_PATH = os.path.join(LOG_DIR, "close-all-trades.log")
os.makedirs(LOG_DIR, exist_ok=True)
log_file = open(LOG_PATH, "a")

def log(message):
    timestamp = datetime.now(timezone.utc).isoformat()
    full_msg = f"[{timestamp}] {message}"
    print(full_msg)
    log_file.write(full_msg + "\n")

log("🚨 Starting forced trade closure script...")

try:
    open_trades = get_open_trades()
except Exception as e:
    log(f"❌ Failed to fetch open trades: {e}")
    log_file.close()
    sys.exit(1)

log(f"🔍 Found {len(open_trades)} open trades...")

closed_count = 0
skipped_fifo = 0
failed_count = 0

for trade in open_trades:
    try:
        trade_id = trade["id"]
        instrument = trade["instrument"]
        units = int(trade["currentUnits"])

        if units == 0:
            continue

        log(f"❌ Attempting to close trade {trade_id} ({instrument})...")
        result = close_trade(trade_id)

        if result["status"] != "success":
            reason = result["response"].get("errorCode") or result["response"].get("errorMessage")
            if reason == "FIFO_VIOLATION":
                log(f"↪️ Skipped trade {trade_id} due to FIFO_VIOLATION.")
                skipped_fifo += 1
                continue
            else:
                raise Exception(reason)

        log(f"✅ Trade {trade_id} closed successfully.")
        closed_count += 1

    except Exception as ex:
        log(f"❌ Failed to close trade {trade.get('id')}: {ex}")
        failed_count += 1

# Final summary
log("\n📊 Final Summary")
log(f"✅ Trades closed: {closed_count}")
log(f"↪️ Skipped due to FIFO_VIOLATION: {skipped_fifo}")
log(f"❌ Failed to close: {failed_count}")
log(f"🧾 Script complete.")
log_file.close()