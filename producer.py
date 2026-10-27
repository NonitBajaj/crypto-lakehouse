import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# 1. Connect to Redpanda Broker with retry logic
producer = None
print("Connecting to Redpanda broker at localhost:9092...")

while not producer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        print(" Connected to Redpanda broker successfully.")
    except NoBrokersAvailable:
        print("⏳ Waiting for Redpanda broker to become available...")
        time.sleep(2)

# 2. State tracking for realistic continuous price action (Random Walk)
market_state = {
    'BTC/USD': 64250.00,
    'ETH/USD': 3450.00,
    'SOL/USD': 148.50
}

# Volatility configuration per asset
volatility = {
    'BTC/USD': 0.0015,  # 0.15% fluctuation per tick
    'ETH/USD': 0.0020,  # 0.20% fluctuation per tick
    'SOL/USD': 0.0035   # 0.35% fluctuation per tick
}

print(" Streaming live simulated trade events to topic: 'crypto_raw' (Press Ctrl+C to stop)")

try:
    while True:
        for symbol, current_price in market_state.items():
            # Apply percentage drift
            pct_change = random.gauss(0, volatility[symbol])
            new_price = round(max(current_price * (1 + pct_change), 1.0), 2)
            market_state[symbol] = new_price

            # Generate trade payload
            payload = {
                "symbol": symbol,
                "price": new_price,
                "volume": round(random.uniform(0.05, 3.5), 4),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            producer.send('crypto_raw', value=payload)
            print(f"[{payload['timestamp']}] {symbol:<8} | Price: ${payload['price']:>9.2f} | Vol: {payload['volume']:>6.4f}")

        producer.flush()
        time.sleep(1)

except KeyboardInterrupt:
    print("\n Gracefully shutting down producer...")
    producer.close()
    print("Producer closed.")