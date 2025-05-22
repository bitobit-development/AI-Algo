# oanda_api/execution.py
# Handles live trade execution via OANDA REST API

import os
import requests
from dotenv import load_dotenv
from oanda_api.config import OANDA_API_KEY, BASE_URL, OANDA_USER_ID

load_dotenv()

headers = {
    "Authorization": f"Bearer {OANDA_API_KEY}",
    "Content-Type": "application/json"
}

def place_order(instrument: str, side: str, amount: int) -> dict:
    """
    Places a live market order with OANDA.

    Args:
        instrument (str): e.g. "EUR_USD"
        side (str): "buy" or "sell"
        amount (int): units (positive for buy, negative for sell)

    Returns:
        dict: OANDA broker response (parsed JSON)
    """
    units = str(amount if side == "buy" else -amount)

    order_payload = {
        "order": {
            "units": units,
            "instrument": instrument,
            "timeInForce": "FOK",
            "type": "MARKET",
            "positionFill": "DEFAULT"
        }
    }

    response = requests.post(
        f"{BASE_URL}/v3/accounts/{OANDA_USER_ID}/orders",
        headers=headers,
        json=order_payload
    )

    try:
        return {
            "status": "success" if response.status_code == 201 else "error",
            "status_code": response.status_code,
            "response": response.json()
        }
    except Exception as e:
        return {
            "status": "error",
            "status_code": response.status_code,
            "response": {"error": str(e)}
        }

def close_trade(trade_id: str) -> dict:
    """
    Closes an open trade by ID.

    Args:
        trade_id (str): OANDA trade ID to close

    Returns:
        dict: API response JSON
    """
    url = f"{BASE_URL}/v3/accounts/{OANDA_USER_ID}/trades/{trade_id}/close"
    payload = {"units": "ALL"}

    response = requests.put(url, headers=headers, json=payload)

    try:
        return {
            "status": "success" if response.status_code in [200, 201] else "error",
            "status_code": response.status_code,
            "response": response.json()
        }
    except Exception as e:
        return {
            "status": "error",
            "status_code": response.status_code,
            "response": {"error": str(e)}
        }
