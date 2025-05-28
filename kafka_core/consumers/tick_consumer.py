# file: kafka_core/consumers/tick_consumer.py
# Kafka Consumer: Read raw FX ticks and insert into Supabase tick_data table

import os
import sys
import json
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer

# 🔧 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS, USER_ID
from supabase_client.client import supabase

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    force=True
)
logger = logging.getLogger(__name__)

# Kafka Consumer configuration
def create_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'tick-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPICS['market_data']])
    logger.info('🕸️ Tick consumer started, subscribing to: %s', KAFKA_TOPICS['market_data'])
    return consumer

# Safely determine required timestamp fields
def get_timestamp(data: dict, key: str) -> str:
    value = data.get(key)
    if value:
        return value
    return datetime.now(timezone.utc).isoformat()

# Insert a single tick record into Supabase
def insert_tick(data: dict):
    bid = data.get('bid')
    ask = data.get('ask')
    if bid is None or ask is None:
        return  # skip non-price messages

    tick_time = get_timestamp(data, 'time')
    received_at = get_timestamp(data, 'received_at')

    record = {
        'symbol': data.get('instrument', data.get('symbol')),
        'tick_time': tick_time,
        'bid': bid,
        'ask': ask,
        'latency_ms': data.get('latency_ms'),
        'received_at': received_at,
        'user_id': USER_ID
    }
    try:
        supabase.table('tick_data').insert(record).execute()
        logger.info('✅ Inserted tick: %s → bid=%.5f, ask=%.5f, time=%s, latency=%sms, user=%s', record['symbol'], record['bid'], record['ask'], record['tick_time'], record.get('latency_ms', 'N/A'), record['user_id'])
    except Exception as e:
        logger.error('❌ Failed to insert tick: %s', e)

# Main loop reading from Kafka
def main(status_flags=None):
    if status_flags is None:
        status_flags = {"tick_consumer": True}
    consumer = create_consumer()
    try:
        while True:
            if not status_flags.get("tick_consumer", True):
                logger.info("⏸️ tick_consumer paused")
                time.sleep(1)
                continue
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error('❌ Consumer error: %s', msg.error())
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                insert_tick(data)
            except json.JSONDecodeError as e:
                logger.warning('⚠️ Failed to decode JSON: %s', e)
    except KeyboardInterrupt:
        logger.info('🛑 Tick consumer interrupted by user')
    finally:
        consumer.close()
        logger.info('🧹 Kafka consumer shut down')

if __name__ == '__main__':
    main()
