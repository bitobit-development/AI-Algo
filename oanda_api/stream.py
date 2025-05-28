"""
oanda_api/stream.py
───────────────────────
Connects to OANDA's streaming price API and yields real-time tick data.
Usage example:
    from oanda_api.stream import stream_prices
    for tick in stream_prices(["EUR_USD", "USD_CAD"]):
        print(tick)
"""

import os
import json
import requests
from dotenv import load_dotenv
from oanda_api.config import OANDA_API_KEY, STREAM_URL, get_account_id

load_dotenv()

account_id = get_account_id()

headers = {"Authorization": f"Bearer {OANDA_API_KEY}"}


def stream_prices(symbols=None):
    """
    Generator that yields real-time price updates for a list of FX symbols.
    """
    if isinstance(symbols, str):
        symbols = [symbols]
    if symbols is None:
        symbols = ["EUR_USD"]

    instruments_param = ",".join(symbols)
    url = f"{STREAM_URL}/v3/accounts/{account_id}/pricing/stream"
    params = {"instruments": instruments_param}

    print(f"📡 Streaming prices for {instruments_param} from OANDA...")

    try:
        with requests.get(url, headers=headers, params=params,
                          stream=True) as response:
            for line in response.iter_lines():
                if line:
                    decoded = line.decode("utf-8")
                    #print(f"📨 Raw line: {decoded}")

                    try:
                        data = json.loads(decoded)
                    except json.JSONDecodeError:
                        #print("⚠️ Failed to parse JSON")
                        continue

                    if data.get("type") == "HEARTBEAT":
                        continue

                    if "bids" in data and "asks" in data:
                        # print(f"📥 Tick received: {data}")
                        yield {
                            "symbol": data["instrument"].replace("_", ""),
                            "bid": float(data["bids"][0]["price"]),
                            "ask": float(data["asks"][0]["price"]),
                            "timestamp": data["time"]
                        }

    except KeyboardInterrupt:
        print("🛑 Stopped OANDA stream manually.")
    except Exception as e:
        print(f"❌ Stream error: {e}")
        raise
