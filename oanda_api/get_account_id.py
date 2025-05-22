#file location: oanda_api/get_account_id.py
#get_account_id.py

import os
import sys

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from oanda_api.config import get_account_id

print("🔑 Fetching OANDA Account ID...")
account_id = get_account_id()
print("✅ Your OANDA Account ID is:", account_id)
