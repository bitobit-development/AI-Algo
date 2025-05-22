"""
file location: kafka/config.py
Centralized config loader for Kafka, Supabase, and Elasticsearch.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ────── Kafka Config ──────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")  # e.g. 52.90.93.152:9093
KAFKA_TOPICS = {
    "market_data": "market_data",
    "signals": "ai_trade_signals",
    "executions": "executions",
    "alerts": "alerts",
    "elastic": "elastic_indexing"
}

# ────── Supabase Config ──────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")
ENV_MODE = os.getenv("ENV_MODE", "dev").lower()
DEV_USER_ID = os.getenv("DEV_USER_ID")
PROD_USER_ID = os.getenv("PROD_USER_ID")
USER_ID = DEV_USER_ID if ENV_MODE == "dev" else PROD_USER_ID
ADMIN_USER_ID = "0346e0c7-fed8-4d33-bb9a-89f67976b22b"

# ────── Elasticsearch Config ──────
ELASTIC_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")

# ────── Utility Functions ──────
def print_config_summary():
    print("🧠 Active Configuration:")
    print(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"Supabase URL:   {SUPABASE_URL}")
    print(f"Elastic URL:    {ELASTIC_URL}")
    print(f"ENV Mode:       {ENV_MODE}")
    print(f"User ID:        {USER_ID}")
