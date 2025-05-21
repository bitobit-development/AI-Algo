import pytest
from supabase_client.client import supabase  # adjust if you renamed the folder

def test_supabase_connection():
    """Ensure we can connect and fetch data from Supabase."""
    try:
        response = supabase.table("strategies").select("*").limit(1).execute()
        assert isinstance(response.data, list)
    except Exception as e:
        pytest.fail(f"Supabase query failed: {e}")

