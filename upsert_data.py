import os
import csv
import json
import redis
from datetime import datetime

def upsert_to_redis():
    # Connect to local Redis
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    data_dir = "/Users/leoinv/Documents/CODE/data"
    
    # Mapping for symbols to match dashboard expected keys
    SYMBOL_MAP = {
        "VN30F1M": "VN301!",
        "VN30": "VN30",
        "VNINDEX": "VNINDEX"
    }

    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    for filename in files:
        # Expected filename format: SYMBOL_INTERVAL.csv
        parts = filename.replace('.csv', '').split('_')
        if len(parts) < 2:
            continue
            
        raw_symbol = parts[0]
        interval = parts[1]
        
        # Map symbol if needed
        symbol = SYMBOL_MAP.get(raw_symbol, raw_symbol)
        
        filepath = os.path.join(data_dir, filename)
        candles = []
        
        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # Parse datetime to Unix timestamp
                    # Format: 2026-04-06 10:04:00
                    dt = datetime.strptime(row['datetime'], '%Y-%m-%d %H:%M:%S')
                    timestamp = int(dt.timestamp())
                    
                    candles.append({
                        "time": timestamp,
                        "open": float(row['open']),
                        "high": float(row['high']),
                        "low": float(row['low']),
                        "close": float(row['close']),
                        "volume": float(row['volume']) if row.get('volume') else 0
                    })
                except Exception as e:
                    print(f"Error parsing row in {filename}: {e}")
                    continue
        
        # Sort candles by time
        candles.sort(key=lambda x: x['time'])
        
        # Redis key format: candles:SYMBOL:INTERVAL
        redis_key = f"candles:{symbol}:{interval}"
        
        # Store in Redis as JSON string
        r.set(redis_key, json.dumps(candles))
        print(f"Upserted {len(candles)} bars for {redis_key}")

        # Also update the latest tick for this symbol if it's the 1m interval
        if interval == "1m" and candles:
            last = candles[-1]
            tick_key = f"tick:{symbol}"
            tick_data = {
                "symbol": symbol,
                "price": last['close'],
                "timestamp": last['time'] * 1000
            }
            r.set(tick_key, json.dumps(tick_data))
            print(f"Updated latest tick for {symbol}")

    print("Data upsert complete.")

if __name__ == "__main__":
    upsert_to_redis()
