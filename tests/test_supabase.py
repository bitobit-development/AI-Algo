# import pytest
# from supabase_client.client import supabase  # adjust if you renamed the folder

# def test_supabase_connection():
#     """Ensure we can connect and fetch data from Supabase."""
#     try:
#         response = supabase.table("strategies").select("*").limit(1).execute()
#         assert isinstance(response.data, list)
#     except Exception as e:
#         pytest.fail(f"Supabase query failed: {e}")
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from supabase_client.client import supabase
from kafka_client.config import USER_ID

res = supabase.table("strategies").select("*").eq("user_id", USER_ID).execute()
print(res.data)
