"""
Kafka Topic Creation Script (Centralized with config.py)
───────────────────────────────────────────────────────────
Ensures all required topics exist in your Kafka cluster using
`confluent_kafka`. Topic names are loaded from `kafka/config.py`.

Run:
    python kafka_client/topics.py
"""

import os
import sys
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
# 🔧 Ensure root path is available so config can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS


# Load .env values (only needed if config.py doesn't do it)
load_dotenv()

# Confirm bootstrap server is loaded
if not KAFKA_BOOTSTRAP:
    raise ValueError("❌ KAFKA_BOOTSTRAP is missing in .env")

print(f"📡 Connecting to Kafka @ {KAFKA_BOOTSTRAP}")

# Extract topic names from centralized config
TOPICS = list(KAFKA_TOPICS.values())

# Initialize Kafka admin client
admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})

# Fetch current topic metadata from Kafka broker
metadata = admin.list_topics(timeout=10)
existing = set(metadata.topics.keys())

# Build list of new topics (topics not already present)
new_topics = [
    NewTopic(topic, num_partitions=1, replication_factor=1)
    for topic in TOPICS if topic not in existing
]

# Create only the topics that are missing
if new_topics:
    print(f"🛠 Creating topics: {[t.topic for t in new_topics]}")
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()  # Will raise exception if creation failed
            print(f"✅ Created topic: {topic}")
        except Exception as e:
            print(f"❌ Failed to create topic {topic}: {e}")
else:
    print("ℹ️ All topics already exist. No changes made.")
