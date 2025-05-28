# file: kafka/config.py
# Centralized config loader for Kafka, Supabase, and Elasticsearch.

import os
from datetime import time
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

# Define major currency pairs for 24/7 data collection
MAJOR_CURRENCY_PAIRS = [
    'EUR_USD', 'GBP_USD', 'USD_JPY', 'USD_CHF',
    'AUD_USD', 'USD_CAD', 'NZD_USD'
]

# ────── Market Hours (UTC) ──────
# Only collect data when each market is active to avoid stagnated updates.
# Times are in UTC. Use datetime.time objects.
# Windows widened to ensure full 24h coverage by adjusting Sydney session start.
MARKET_HOURS = {
    'EUR_USD': (time(7, 0), time(16, 0)),   # London session
    'GBP_USD': (time(7, 0), time(16, 0)),   # London session
    'USD_JPY': (time(0, 0), time(8, 0)),    # Tokyo/London overlap
    'USD_CHF': (time(7, 0), time(16, 0)),   # London session
    'AUD_USD': (time(21, 0), time(7, 0)),   # Sydney session extended to start at 21:00 UTC
    'USD_CAD': (time(13, 30), time(21, 0)), # New York session
    'NZD_USD': (time(21, 0), time(7, 0)),   # Sydney session extended to start at 21:00 UTC
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
    print(f"Major Pairs:    {MAJOR_CURRENCY_PAIRS}")
    print("Market Hours (UTC):")
    for inst, (start, end) in MARKET_HOURS.items():
        print(f"  - {inst}: {start.strftime('%H:%M')} to {end.strftime('%H:%M')}")
