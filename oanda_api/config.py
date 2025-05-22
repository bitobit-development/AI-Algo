"""
OANDA API Config
────────────────────────
Loads API keys and fetches account ID for fxTrade or fxPractice.
"""

import os
import requests
from dotenv import load_dotenv

# Load env vars
load_dotenv()

OANDA_API_KEY = os.getenv("OANDA_API_KEY")
OANDA_USER_ID = os.getenv("OANDA_USER_ID")
OANDA_ENV = os.getenv("OANDA_ENV", "practice")  # 'trade' or 'practice'

# Determine API base based on mode
if OANDA_ENV == "trade":
    BASE_URL = "https://api-fxtrade.oanda.com"
    STREAM_URL = "https://stream-fxtrade.oanda.com"
else:
    BASE_URL = "https://api-fxpractice.oanda.com"
    STREAM_URL = "https://stream-fxpractice.oanda.com"

def get_account_id():
    """Fetch list of account IDs for the authorized user"""
    url = f"{BASE_URL}/v3/accounts"
    headers = {
        "Authorization": f"Bearer {OANDA_API_KEY}"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data["accounts"][0]["id"]

