# file: main.py
# AI Trading Entry Point using template dashboard.html for UI rendering

import threading
import time
import logging
import os
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, render_template, redirect, url_for, request, make_response
import csv
import io
import json
from kafka_core.producers.get_oanda_fx_stream import start as start_producer_wrapper
from kafka_core.consumers.tick_consumer import main as tick_consumer_main_wrapper
from kafka_core.config import KAFKA_BOOTSTRAP, KAFKA_TOPICS, USER_ID
from confluent_kafka import Consumer
from supabase_client.client import supabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
app = Flask(__name__)

INTERVALS = {'1m': timedelta(minutes=1), '5m': timedelta(minutes=5), '15m': timedelta(minutes=15)}
windows = {}
last_seen = {}

started_flags = {"producer": False, "tick_consumer": False, "candle_aggregator": False}
threads = {}
status_flags = {}
thread_targets = {
    "producer": lambda: start_producer_wrapper(status_flags),
    "tick_consumer": lambda: tick_consumer_main_wrapper(status_flags),
    "candle_aggregator": lambda: kafka_consumer_thread(status_flags)
}

# Log to Supabase action_log table
def log_action(service, action, source='system'):
    timestamp = datetime.now(timezone.utc).isoformat()
    supabase.table("action_log").insert({
        "service": service,
        "action": action,
        "timestamp": timestamp,
        "source": source
    }).execute()
    logger.info(f"📥 Logged: {service} → {action} ({source})")

# Candle logic

def floor_time(dt, delta):
    return datetime.fromtimestamp(dt.timestamp() - (dt.timestamp() % delta.total_seconds()), tz=timezone.utc)

def flush_candle(symbol, interval, state):
    record = {
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
        supabase.table('ohlc_data').insert(record).execute()
    except Exception as e:
        logger.error(f"❌ Failed to insert candle: {e}")

def process_tick(tick):
    symbol = tick.get('instrument') or tick.get('symbol')
    bid = tick.get('bid')
    ask = tick.get('ask')
    if not symbol or bid is None or ask is None:
        return
    ts = datetime.fromisoformat(tick.get('received_at')) if tick.get('received_at') else datetime.now(timezone.utc)
    price = (bid + ask) / 2
    sym_windows = windows.setdefault(symbol, {})
    for label, delta in INTERVALS.items():
        start = floor_time(ts, delta)
        state = sym_windows.get(label)
        if not state or state['start'] != start:
            if state:
                flush_candle(symbol, label, state)
            sym_windows[label] = {'start': start, 'open': price, 'high': price, 'low': price, 'close': price, 'volume': 1}
        else:
            state['high'] = max(state['high'], price)
            state['low'] = min(state['low'], price)
            state['close'] = price
            state['volume'] += 1

def kafka_consumer_thread(status_flags):
    if started_flags['candle_aggregator']:
        return
    logger.info("🧭 Starting candle_aggregator loop")
    started_flags['candle_aggregator'] = True
    started_flags['candle_aggregator'] = True
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'candle-aggregator-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([KAFKA_TOPICS['market_data']])
    try:
        while True:
            if not status_flags.get('candle_aggregator', True):
                logger.info("⏸️ candle_aggregator paused")
                time.sleep(1)
                continue
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    process_tick(data)
                except Exception:
                    continue
    finally:
        for sym, ivals in windows.items():
            for label, state in ivals.items():
                flush_candle(sym, label, state)
        consumer.close()

# Safe thread starter
def start_thread(name, target):
    if not started_flags[name] or not status_flags.get(name, True):
        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        threads[name] = thread
        status_flags[name] = True
        started_flags[name] = True
        log_action(name, "Startup")

@app.route('/stop/<service>')
def stop_service(service):
    if service in threads and threads[service].is_alive():
        status_flags[service] = False
        started_flags[service] = False
        log_action(service, "Stopped", source="manual")
    return redirect(url_for('index'))

@app.route('/restart/<service>')
def restart_service(service):
    if service in thread_targets:
        status_flags[service] = True
        started_flags[service] = False
        log_action(service, "Manual Restart", source="manual")
        start_thread(service, thread_targets[service])
    return redirect(url_for('index'))

@app.route('/')
def index():
    statuses = {
    name: threads[name].is_alive() and status_flags.get(name, False)
    if name in threads else False
    for name in thread_targets
}
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    for name in statuses:
        last_seen[name] = now if statuses[name] else last_seen.get(name, '—')
    q = request.args.get("q")
    query = supabase.table("action_log").select("service, action, timestamp, source")
    if q:
        query = query.ilike("action", f"%{q}%")
    rows = query.order("timestamp", desc=True).limit(150).execute().data
    grouped = {}
    for row in rows:
        grouped.setdefault(row['service'], []).append(row)
    log_by_service = {k: v[:50] for k, v in grouped.items()}
    return render_template("dashboard.html", statuses=statuses, now=now, last_seen=last_seen, log=rows, log_by_service=log_by_service, request=request, debug_status={name: f"alive={threads[name].is_alive()}, status={status_flags.get(name)}, started={started_flags.get(name)}" for name in thread_targets})

@app.route('/download')
def download():
    query = supabase.table("action_log").select("service, action, timestamp").order("timestamp", desc=True).limit(100).execute().data
    si = io.StringIO()
    cw = csv.DictWriter(si, fieldnames=["timestamp", "service", "action"])
    cw.writeheader()
    cw.writerows(query)
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=action_log.csv"
    output.headers["Content-type"] = "text/csv"
    return output

@app.route('/health')
def health():
    statuses = {name: t.is_alive() for name in threads.items()}
    return jsonify({"status": "ok" if all(statuses.values()) else "error", "details": statuses})

def monitor_threads():
    while True:
        time.sleep(10)
        logger.info("🔍 Monitoring thread statuses...")
        for name, thread in threads.items():
            logger.debug(f"🧠 {name} thread: alive={thread.is_alive()}, status_flag={status_flags.get(name)}, started_flag={started_flags.get(name)}")
        for name, thread in threads.items():
            if not thread.is_alive() and status_flags.get(name, True):
                logger.warning(f"🛑 {name} is not alive and flagged active. Restarting...")
                log_action(name, "Auto-Restart")
                new_thread = threading.Thread(target=thread_targets[name], daemon=True)
                threads[name] = new_thread
                status_flags[name] = True
                new_thread.start()

if __name__ == '__main__':
    logger.info("🚀 Launching Trading System")
    for name, target in thread_targets.items():
        start_thread(name, target)
    threading.Thread(target=monitor_threads, daemon=True).start()
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
