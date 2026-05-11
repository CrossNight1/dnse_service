import asyncio
import json
import os
import time
import pandas as pd
from pathlib import Path
import redis.asyncio as redis
from datetime import datetime, timezone
from dnse_ws.client import TradingClient
from dnse_ws.models import Trade, Ohlc
from dnse_ws.common import build_signature, get_date_header_name
import urllib3

# Configuration
SYMBOLS = ["VN301!", "VNINDEX", "VN30"]
API_KEY = os.getenv("DNSE_API_KEY", "eyJvcmciOiJkbnNlIiwiaWQiOiJiNDcxYTBhNjE4MTI0ZWNjYTI0YjI2YzcyMGExNzdkZiIsImgiOiJtdXJtdXIxMjgifQ==")
API_SECRET = os.getenv("DNSE_API_SECRET", "510ksymQU949Se_NphYe3_LXT1O8zclFx1lam3MPRuIMOQhdOvokSQPE7YmhHEUTS4pCq9ZqaWnpbaui34AJVw")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATA_DIR = Path("data")

r = redis.from_url(REDIS_URL, decode_responses=True)

class ParquetStore:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.buffers = {s: [] for s in SYMBOLS}
        self.base_dir.mkdir(exist_ok=True)
        (self.base_dir / "ticks").mkdir(exist_ok=True)

    def add_tick(self, symbol, price):
        self.buffers[symbol].append({
            "timestamp": datetime.now(timezone.utc),
            "price": float(price)
        })

    async def flush_loop(self):
        """Periodically save buffers to Parquet"""
        while True:
            await asyncio.sleep(60)  # Flush every 60 seconds
            for symbol, ticks in self.buffers.items():
                if not ticks:
                    continue
                
                # Copy and clear buffer
                to_save = ticks[:]
                self.buffers[symbol] = []
                
                try:
                    df = pd.DataFrame(to_save)
                    date_str = datetime.now().strftime("%Y-%m-%d")
                    symbol_dir = self.base_dir / "ticks" / symbol.replace("!", "F")
                    symbol_dir.mkdir(parents=True, exist_ok=True)
                    file_path = symbol_dir / f"{date_str}.parquet"
                    
                    if file_path.exists():
                        # Append to existing parquet
                        existing_df = pd.read_parquet(file_path)
                        df = pd.concat([existing_df, df]).drop_duplicates().sort_values("timestamp")
                    
                    df.to_parquet(file_path, compression="snappy")
                    print(f"[Data Store] Saved {len(to_save)} ticks for {symbol} to {file_path}")
                except Exception as e:
                    print(f"[Data Store] Error saving Parquet for {symbol}: {e}")

class DataService:
    def __init__(self):
        self.ws_client = TradingClient(
            api_key=API_KEY,
            api_secret=API_SECRET,
            auto_reconnect=True
        )
        self.store = ParquetStore(DATA_DIR)

    async def on_trade(self, trade: Trade):
        """Handle live trades (Ticks)"""
        data = {
            "symbol": trade.symbol,
            "price": float(trade.price),
            "timestamp": int(time.time() * 1000)
        }
        
        # 1. Update Redis for live chart
        await r.set(f"tick:{trade.symbol}", json.dumps(data))
        await r.publish("market_data", json.dumps(data))
        
        # 2. Store in local Parquet buffer
        self.store.add_tick(trade.symbol, trade.price)

    async def run(self):
        print("[Data Service] Starting Standalone Data Service with Parquet storage...")
        
        # 1. Register handlers
        self.ws_client.on("trade", self.on_trade)
        
        # 2. Start flush loop
        asyncio.create_task(self.store.flush_loop())
        
        # 3. Connect
        try:
            await self.ws_client.connect()
            print(f"[Data Service] Subscribing to: {SYMBOLS}")
            await self.ws_client.subscribe_trades(SYMBOLS)
            
            while True:
                await asyncio.sleep(10)
        except Exception as e:
            print(f"[Data Service] Error: {e}")
        finally:
            await self.ws_client.disconnect()

if __name__ == "__main__":
    service = DataService()
    asyncio.run(service.run())
