import os
import json
import time
import pandas as pd
import urllib3
from pathlib import Path
from datetime import datetime, timezone, timedelta

# Configuration (Mirrors main.py)
SYMBOLS = ["VN301!", "VNINDEX", "VN30"]
API_KEY = os.getenv("DNSE_API_KEY", "eyJvcmciOiJkbnNlIiwiaWQiOiJiNDcxYTBhNjE4MTI0ZWNjYTI0YjI2YzcyMGExNzdkZiIsImgiOiJtdXJtdXIxMjgifQ==")
DATA_DIR = Path("data")

def sync_data():
    """Fetch recent data via REST API and append to Parquet files"""
    print(f"[{datetime.now()}] Starting hourly sync...")
    
    # Sync the last 2 hours to be safe
    now = int(time.time())
    start_time = int((datetime.now() - timedelta(hours=2)).timestamp())
    
    http = urllib3.PoolManager()
    DATA_DIR.mkdir(exist_ok=True)
    (DATA_DIR / "ohlcv").mkdir(exist_ok=True)

    for symbol in SYMBOLS:
        try:
            url = f"https://openapi.dnse.com.vn/v1/market/ohlc?symbol={symbol}&resolution=1&from={start_time}&to={now}"
            resp = http.request("GET", url, headers={"Authorization": f"Bearer {API_KEY}"})
            
            if resp.status == 200:
                data = json.loads(resp.data.decode('utf-8'))
                if data.get("s") == "ok" and "t" in data:
                    df_new = pd.DataFrame({
                        "timestamp": [datetime.fromtimestamp(t, tz=timezone.utc) for t in data["t"]],
                        "open": [float(x) for x in data["o"]],
                        "high": [float(x) for x in data["h"]],
                        "low": [float(x) for x in data["l"]],
                        "close": [float(x) for x in data["c"]],
                        "volume": [int(x) for x in data["v"]]
                    })
                    
                    if df_new.empty:
                        continue

                    # Group by date and append to respective files
                    df_new['date'] = df_new['timestamp'].dt.strftime('%Y-%m-%d')
                    for date_str, group in df_new.groupby('date'):
                        symbol_dir = DATA_DIR / "ohlcv" / symbol.replace("!", "F")
                        symbol_dir.mkdir(parents=True, exist_ok=True)
                        file_path = symbol_dir / f"{date_str}.parquet"
                        
                        final_df = group.drop(columns=['date'])
                        if file_path.exists():
                            existing_df = pd.read_parquet(file_path)
                            final_df = pd.concat([existing_df, final_df]).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
                        
                        final_df.to_parquet(file_path, compression="snappy")
                        print(f"  - Synchronized {len(group)} bars for {symbol} on {date_str}")
            else:
                print(f"  - Error syncing {symbol}: HTTP {resp.status}")
        except Exception as e:
            print(f"  - Sync error for {symbol}: {e}")

if __name__ == "__main__":
    sync_data()
