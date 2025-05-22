# file: scripts/close_all_executions.py

import os
import sys
from datetime import datetime, timezone

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from supabase_client.client import supabase
from kafka_core.config import USER_ID

now = datetime.now(timezone.utc).isoformat()

try:
    open_executions = supabase.table("executions")\
        .select("id")\
        .eq("user_id", USER_ID)\
        .is_("closed_at", None)\
        .execute().data

    print(f"🔍 Found {len(open_executions)} open executions")

    for row in open_executions:
        supabase.table("executions").update({
            "closed_at": now,
            "updated_at": now,
            "status": "closed",
            "close_price": None,          # or use current_price if available
            "pl": 0.0,                    # placeholder unless known
            "current_price": None,        # optional if unknown
            "duration_sec": 0,            # or calculate if timestamps available
            "exit_reason": "manual",
            "exit_confidence": 1.0,
            "model_exit_decision": True
        }).eq("id", row["id"]).execute()
        print(f"✅ Closed execution {row['id']}")

except Exception as e:
    print(f"❌ Failed to close executions: {e}")
