from datetime import datetime, timezone
from supabase_client.client import supabase

def test_insert_all_tables():
    # ─────────────── Setup Phase ───────────────
    # 1. Insert strategy
    strategy = {
        "name": "Test AI Strategy",
        "description": "A test strategy for unit tests",
        "type": "ML",
        "hyperparameters": {"lr": 0.01, "layers": [64, 32]}
    }
    strategy_resp = supabase.table("strategies").insert(strategy).execute()
    strategy_id = strategy_resp.data[0]["id"]

    # 2. Insert dataset
    dataset = {
        "name": "Test Dataset",
        "time_range": "[\"2023-01-01T00:00:00Z\", \"2023-12-31T00:00:00Z\"]",
        "notes": "Used for unit test model training"
    }
    dataset_resp = supabase.table("datasets").insert(dataset).execute()
    dataset_id = dataset_resp.data[0]["id"]

    # 3. Insert model version
    model = {
        "strategy_id": strategy_id,
        "version": "v1.0.0-test",
        "accuracy": 0.95,
        "trained_on": dataset_id,
        "parameters": {"depth": 5, "dropout": 0.3}
    }
    model_resp = supabase.table("model_versions").insert(model).execute()
    model_id = model_resp.data[0]["id"]

    # 4. Insert signal
    signal = {
        "strategy_id": strategy_id,
        "symbol": "EURUSD",
        "signal_type": "buy",
        "confidence": 87.5,
        "time_generated": datetime.now(timezone.utc).isoformat(),
        "market_snapshot": {"open": 1.1, "high": 1.12},
        "model_version": model_id
    }
    signal_resp = supabase.table("signals").insert(signal).execute()
    signal_id = signal_resp.data[0]["id"]

    # 5. Insert execution
    execution = {
        "signal_id": signal_id,
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "price": 1.115,
        "amount": 1000,
        "status": "executed",
        "broker_response": {"order_id": "ABC123"}
    }
    execution_resp = supabase.table("executions").insert(execution).execute()
    execution_id = execution_resp.data[0]["id"]

    # 6. Insert market data
    market_data = {
        "symbol": "EURUSD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "open": 1.1,
        "high": 1.12,
        "low": 1.09,
        "close": 1.115,
        "volume": 500000
    }
    market_resp = supabase.table("market_data").insert(market_data).execute()
    market_id = market_resp.data[0]["id"]

    # ─────────────── Teardown Phase ───────────────
    supabase.table("executions").delete().eq("id", execution_id).execute()
    supabase.table("signals").delete().eq("id", signal_id).execute()
    supabase.table("model_versions").delete().eq("id", model_id).execute()
    supabase.table("datasets").delete().eq("id", dataset_id).execute()
    supabase.table("strategies").delete().eq("id", strategy_id).execute()
    supabase.table("market_data").delete().eq("id", market_id).execute()
