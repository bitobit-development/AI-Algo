import os

# Define folder structure
folders = [
    "kafka",
    "ai/models",
    "brokers",
    "supabase",
    "monitoring",
    "tests"
]

# Define starter files (optional)
starter_files = {
    "main.py": "# Entry point for the AI trading system\n",
    ".env": "# KAFKA_BOOTSTRAP=\n",
    "requirements.txt": "confluent-kafka\n",
    "kafka/send_fx_data.py": "# Producer script for FX data\n",
    "ai/train_model.py": "# Model training script\n",
    "brokers/broker_interface.py": "# Unified trade interface\n",
    "supabase/client.py": "# Supabase DB client\n",
    "monitoring/logger.py": "# Logging setup\n",
}

# Create folders and __init__.py
for folder in folders:
    os.makedirs(folder, exist_ok=True)
    init_path = os.path.join(folder, "__init__.py")
    open(init_path, "w").close()

# Create starter files
for path, content in starter_files.items():
    with open(path, "w") as f:
        f.write(content)

print("✅ Project structure created.")
