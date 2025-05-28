# file: kafka_core/consumers/candle_aggregator.py
# Kafka Consumer: Aggregate FX ticks into OHLC candles and insert into Supabase

import os
import sys
import json
import logging
from datetime import datetime, timezone, timedelta
from confluent_kafka import Consumer

# 🔧 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS, USER_ID
from supabase_client.client import supabase

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Candle intervals
INTERVALS = {
    '1m': timedelta(minutes=1),
    '5m': timedelta(minutes=5),
    '15m': timedelta(minutes=15)
}

# In-memory candle state: { symbol: { interval_label: state_dict } }
windows = {}

# Floor timestamp to nearest candle open
def floor_time(dt: datetime, delta: timedelta) -> datetime:
    seconds = dt.timestamp()
    period = delta.total_seconds()
    floored = seconds - (seconds % period)
    return datetime.fromtimestamp(floored, tz=timezone.utc)

# Supabase insert logic
def flush_candle(symbol: str, interval: str, state: dict):
    candle = {
        'symbol': symbol,
        'interval': interval,
        'start_time': state['start'].isoformat(),
        'open': state['open'],
        'high': state['high'],
        'low': state['low'],
        'close': state['close'],
        'volume': state['volume'],
        'user_id': USER_ID
    }
    try:
        supabase.table('ohlc_data').insert(candle).execute()
        logger.info(f"✅ Candle {symbol} [{interval}] @ {state['start']}")
    except Exception as e:
        logger.error(f"❌ Failed to insert candle {symbol} [{interval}]: {e}")

# Tick → in-memory update logic
def process_tick(tick: dict):
    symbol = tick.get('instrument') or tick.get('symbol')
    bid = tick.get('bid')
    ask = tick.get('ask')
    if not symbol or bid is None or ask is None:
        return

    price = (bid + ask) / 2
    ts_str = tick.get('received_at')
    ts = datetime.fromisoformat(ts_str) if ts_str else datetime.now(timezone.utc)

    sym_windows = windows.setdefault(symbol, {})
    for label, delta in INTERVALS.items():
        start = floor_time(ts, delta)
        state = sym_windows.get(label)

        if not state or state['start'] != start:
            if state:
                flush_candle(symbol, label, state)
            sym_windows[label] = {
                'start': start,
                'open': price,
                'high': price,
                'low': price,
                'close': price,
                'volume': 1
            }
        else:
            state['high'] = max(state['high'], price)
            state['low'] = min(state['low'], price)
            state['close'] = price
            state['volume'] += 1

# Kafka consumer setup
def create_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'candle-aggregator-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPICS['market_data']])
    logger.info('🕸️ Candle aggregator subscribed to: %s', KAFKA_TOPICS['market_data'])
    return consumer

# Main loop
def main():
    consumer = create_consumer()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error('❌ Consumer error: %s', msg.error())
                continue
            try:
                data = json.loads(msg.value().decode('utf-8'))
                process_tick(data)
            except Exception as e:
                logger.warning(f"⚠️ Skipping malformed message: {e}")
    except KeyboardInterrupt:
        logger.info('🛑 Candle aggregator interrupted by user')
    finally:
        for symbol, intervals in windows.items():
            for label, state in intervals.items():
                flush_candle(symbol, label, state)
        consumer.close()
        logger.info('🧹 Kafka consumer shut down')

if __name__ == '__main__':
    main()