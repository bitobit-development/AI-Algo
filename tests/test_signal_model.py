import os
import sys

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ai.signal_model import predict_signal

def test_predict_signal():
    mock_history = [
        {"bid": 1.1010, "ask": 1.1015},
        {"bid": 1.1011, "ask": 1.1016},
        {"bid": 1.1012, "ask": 1.1017},
    ]

    result = predict_signal(mock_history)

    assert "signal_type" in result
    assert "confidence" in result
    assert result["signal_type"] in ["buy", "sell", "hold"]
    assert isinstance(result["confidence"], float)
    assert 0 <= result["confidence"] <= 100

    print("✅ Test passed:", result)
