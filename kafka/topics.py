import os
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

bootstrap_server = os.getenv("KAFKA_BOOTSTRAP")
if not bootstrap_server:
    raise ValueError("❌ KAFKA_BOOTSTRAP is missing in .env")

print(f"📡 Connecting to Kafka @ {bootstrap_server}")

TOPICS = [
    "fx_market_data",
    "ai_trade_signals",
    "executions",
    "alerts",
    "elastic_indexing"
]

admin = AdminClient({"bootstrap.servers": bootstrap_server})

# Get existing topics
metadata = admin.list_topics(timeout=10)
existing = set(metadata.topics.keys())

# Create only missing ones
new_topics = [
    NewTopic(topic, num_partitions=1, replication_factor=1)
    for topic in TOPICS if topic not in existing
]

if new_topics:
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
            print(f"✅ Created topic: {topic}")
        except Exception as e:
            print(f"❌ Failed to create topic {topic}: {e}")
else:
    print("ℹ️ All topics already exist.")
