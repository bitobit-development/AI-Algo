# scripts/clear_supabase.py

import os
import sys
from dotenv import load_dotenv

# Add project root to import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from supabase_client.client import supabase

load_dotenv()

ENV_MODE = os.getenv("ENV_MODE", "dev")
DEV_USER_ID = os.getenv("DEV_USER_ID")
PROD_USER_ID = os.getenv("PROD_USER_ID")

USER_ID = DEV_USER_ID if ENV_MODE == "dev" else PROD_USER_ID

def delete_table_by_user_id(table_name):
    print(f"🧨 Clearing {table_name} for user_id = {USER_ID} and NULL entries...")
    supabase.table(table_name) \
        .delete() \
        .or_(f"user_id.eq.{USER_ID},user_id.is.null") \
        .execute()

def clear_supabase_data():
    if not USER_ID:
        raise Exception("❌ USER_ID not found for current ENV_MODE.")

    print(f"\n🧹 Deleting all data for user_id: {USER_ID} (and NULLs) in {ENV_MODE.upper()} mode...\n")

    # In dependency-safe order
    delete_table_by_user_id("executions")
    delete_table_by_user_id("signals")
    delete_table_by_user_id("model_versions")
    delete_table_by_user_id("datasets")
    delete_table_by_user_id("strategies")
    delete_table_by_user_id("market_data")

    print("\n✅ All user-specific and orphaned (NULL) data deleted.\n")

if __name__ == "__main__":
    clear_supabase_data()
