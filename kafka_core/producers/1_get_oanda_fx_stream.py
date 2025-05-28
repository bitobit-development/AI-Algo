# file: kafka_core/producers/get_oanda_fx_stream.py
# Kafka Producer: Send OANDA FX Stream for multiple instruments,
# only starting streams during active market hours, capturing latency,
# graceful shutdown, and single error notification for startup failures

import os
import sys
import json
import threading
import time
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

# 🔧 Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from kafka_core.config import (
    KAFKA_BOOTSTRAP,
    KAFKA_TOPICS,
    MAJOR_CURRENCY_PAIRS,
    MARKET_HOURS
)
from oanda_api.stream import stream_prices

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Kafka setup
topic = KAFKA_TOPICS['market_data']
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

# Shutdown event for graceful exit
shutdown_event = threading.Event()

# Track instruments that failed startup
startup_failures = set()

# Delivery callback
def delivery_report(err, msg):
    key = msg.key().decode() if msg.key() else None
    if err:
        logger.error(f"❌ Delivery failed for {key}: {err}")
    else:
        logger.info(f"✅ Delivered tick for {key}")

# Check if market is open for the given instrument (UTC times)
def is_market_open(instrument: str) -> bool:
    hours = MARKET_HOURS.get(instrument)
    if not hours:
        return True
    start, end = hours
    now = datetime.now(timezone.utc).time()
    if start <= end:
        return start <= now <= end
    return now >= start or now <= end

# Stream ticks for one instrument to Kafka
def stream_to_kafka(instrument: str):
    logger.info(f"📡 Stream thread started for {instrument}")
    try:
        for tick in stream_prices(instrument):
            if shutdown_event.is_set():
                break
            # Process only price ticks (with bid/ask)
            if 'bid' not in tick or 'ask' not in tick:
                continue
            # Skip if market closed now
            if not is_market_open(instrument):
                continue
            # Compute latency if timestamp present
            time_str = tick.get('time')
            if time_str:
                tick_ts = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                now = datetime.now(timezone.utc)
                latency_ms = (now - tick_ts).total_seconds() * 1000
            else:
                latency_ms = None
            # Produce enriched payload
            enriched = {
                'bid': tick['bid'],
                'ask': tick['ask'],
                'instrument': instrument,
                'received_at': datetime.now(timezone.utc).isoformat(),
                'latency_ms': latency_ms
            }
            producer.produce(
                topic,
                key=instrument,
                value=json.dumps(enriched),
                callback=delivery_report
            )
            producer.poll(0.01)
    except Exception as e:
        # Only log startup failure once per instrument
        if instrument not in startup_failures:
            logger.error(f"💥 Failed to start stream for {instrument}: {e}")
            startup_failures.add(instrument)
    finally:
        logger.info(f"🛑 Stream thread exiting for {instrument}")

# Launch streams for all active pairs only once
def start():
    logger.info("🚀 Launching OANDA stream producer...")
    threads = []
    for inst in MAJOR_CURRENCY_PAIRS:
        if not is_market_open(inst):
            logger.info(f"⏰ Skipping {inst}: market closed at startup.")
            continue
        t = threading.Thread(
            target=stream_to_kafka,
            args=(inst,),
            name=f"Stream-{inst}",
            daemon=True
        )
        t.start()
        threads.append(t)
        time.sleep(0.1)
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("🚑 Shutdown signal received. Stopping streams...")
        shutdown_event.set()
    finally:
        for t in threads:
            t.join(timeout=1)
        producer.flush(timeout=10)
        logger.info("🧹 Kafka producer flushed and shut down.")

if __name__ == '__main__':
    start()
