# Entry point for the AI trading system
# file: main.py
import subprocess
import time
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)

PROCESS_COMMANDS = [
    ("producer", ["python", "kafka_core/producers/1_get_oanda_fx_stream.py"]),
    ("tick_consumer", ["python", "kafka_core/consumers/tick_consumer.py"]),
    ("candle_aggregator", ["python", "kafka_core/consumers/candle_aggregator.py"]),
]

def start_process(name, cmd):
    logging.info(f"🚀 Starting {name}...")
    return subprocess.Popen(cmd)

def main():
    processes = {}
    try:
        for name, cmd in PROCESS_COMMANDS:
            processes[name] = start_process(name, cmd)
            time.sleep(1)

        # Monitor loop
        while True:
            for name, proc in list(processes.items()):
                if proc.poll() is not None:
                    logging.error(f"💥 {name} exited. Restarting...")
                    processes[name] = start_process(name, PROCESS_COMMANDS[[n for n, _ in PROCESS_COMMANDS].index(name)][1])
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("🛑 Shutting down all processes...")
        for proc in processes.values():
            proc.terminate()
        for proc in processes.values():
            proc.wait()
        logging.info("✅ All processes stopped cleanly.")

if __name__ == "__main__":
    main()
