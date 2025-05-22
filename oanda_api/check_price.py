# oanda_api/check_price.py
# Utility to fetch current prices for instruments from OANDA

import requests
from oanda_api.config import OANDA_API_KEY, BASE_URL, OANDA_USER_ID

HEADERS = {
    "Authorization": f"Bearer {OANDA_API_KEY}",
    "Content-Type": "application/json"
}

def get_price(instrument: str) -> float:
    """
    Get the current mid-price (average of bid/ask) for an instrument.

    Args:
        instrument (str): OANDA instrument like "EUR_USD"

    Returns:
        float: mid price or raises exception if not found
    """
    url = f"{BASE_URL}/v3/accounts/{OANDA_USER_ID}/pricing?instruments={instrument}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch price for {instrument}: {response.text}")

    data = response.json()
    prices = data.get("prices", [])
    if not prices:
        raise Exception(f"No prices returned for {instrument}")

    bid = float(prices[0]["bids"][0]["price"])
    ask = float(prices[0]["asks"][0]["price"])
    return round((bid + ask) / 2, 5)


if __name__ == "__main__":
    test_instruments = ["EUR_USD", "USD_JPY"]
    for instrument in test_instruments:
        try:
            price = get_price(instrument)
            print(f"{instrument} current mid price: {price}")
        except Exception as e:
            print(f"Error: {e}")
