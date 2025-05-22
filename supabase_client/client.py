"""
file: supabase_client/client.py
Supabase Client (using centralized config)
────────────────────────────────────────────
Initializes the Supabase client using credentials from config.py.
"""

from supabase import create_client, Client
from kafka_core.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE

# Validate presence of keys
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
    raise Exception("Missing Supabase URL or Service Role Key in config.")

# Create Supabase client using typed annotation
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)
