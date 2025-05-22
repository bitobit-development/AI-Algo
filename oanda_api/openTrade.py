# oanda_api/openTrade.py
# Fetches all open trades from OANDA

import requests
from oanda_api.config import OANDA_API_KEY, BASE_URL, OANDA_USER_ID

HEADERS = {
    "Authorization": f"Bearer {OANDA_API_KEY}",
    "Content-Type": "application/json"
}

def get_open_trades():
    """
    Retrieve all open trades for the OANDA account.

    Returns:
        list[dict]: List of open trade dictionaries.
    """
    url = f"{BASE_URL}/v3/accounts/{OANDA_USER_ID}/openTrades"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch open trades: {response.text}")

    return response.json().get("trades", [])


if __name__ == "__main__":
    try:
        trades = get_open_trades()
        print(f"🔎 Retrieved {len(trades)} open trades")
        for trade in trades:
            print(f"➡️  Trade ID {trade['id']} | {trade['instrument']} | Units: {trade['currentUnits']} | Price: {trade['price']}")
    except Exception as e:
        print(f"❌ Error: {e}")
