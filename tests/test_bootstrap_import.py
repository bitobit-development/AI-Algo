from core.bootstrap import *
from kafka_core.config import KAFKA_BOOTSTRAP
from supabase_client.client import supabase

print("✅ Bootstrap loaded successfully.")
print("KAFKA_BOOTSTRAP =", KAFKA_BOOTSTRAP)
print("✅ Supabase client:", type(supabase))
