import asyncio
import json
import os
import time
import signal
import pandas as pd
from pathlib import Path
import redis.asyncio as redis
from datetime import datetime, timezone
from dnse_ws.client import TradingClient
from dnse_ws.models import Trade, Ohlc, Quote
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
        self.ohlcv_buffers = {s: [] for s in SYMBOLS}
        self.base_dir.mkdir(exist_ok=True)
        (self.base_dir / "ohlcv").mkdir(exist_ok=True)

    def add_ohlcv(self, ohlc: Ohlc):
        self.ohlcv_buffers[ohlc.symbol].append({
            "timestamp": datetime.fromtimestamp(ohlc.time, tz=timezone.utc),
            "open": float(ohlc.open),
            "high": float(ohlc.high),
            "low": float(ohlc.low),
            "close": float(ohlc.close),
            "volume": int(ohlc.volume)
        })

    def flush(self):
        """Save buffers to Parquet"""
        for symbol, data in self.ohlcv_buffers.items():
            if not data:
                continue
            
            # Copy and clear buffer
            to_save = data[:]
            self.ohlcv_buffers[symbol] = []
            
            try:
                df = pd.DataFrame(to_save)
                date_str = datetime.now().strftime("%Y-%m-%d")
                symbol_dir = self.base_dir / "ohlcv" / symbol.replace("!", "F")
                symbol_dir.mkdir(parents=True, exist_ok=True)
                file_path = symbol_dir / f"{date_str}.parquet"
                
                if file_path.exists():
                    existing_df = pd.read_parquet(file_path)
                    df = pd.concat([existing_df, df]).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
                
                df.to_parquet(file_path, compression="snappy")
                print(f"[Data Store] Saved {len(to_save)} bars for {symbol}")
            except Exception as e:
                print(f"[Data Store] Error saving OHLCV for {symbol}: {e}")

    async def flush_loop(self):
        """Periodically save buffers to Parquet"""
        while True:
            await asyncio.sleep(60)  # Flush every 60 seconds
            self.flush()

class DataService:
    def __init__(self):
        self.ws_client = TradingClient(
            api_key=API_KEY,
            api_secret=API_SECRET,
            auto_reconnect=True
        )
        self.store = ParquetStore(DATA_DIR)
        self._stop_event = asyncio.Event()

    async def on_trade(self, trade: Trade):
        """Handle live trades (Ticks) - Redis Only"""
        data = {
            "symbol": trade.symbol,
            "price": float(trade.price),
            "timestamp": int(time.time() * 1000)
        }
        await r.set(f"tick:{trade.symbol}", json.dumps(data))
        await r.publish("market_data", json.dumps(data))

    async def on_quote(self, quote: Quote):
        """Handle live quotes (BBO) - Redis Only"""
        if not quote.bid or not quote.offer:
            return
        data = {
            "symbol": quote.symbol,
            "bid": float(quote.bid[0].price),
            "ask": float(quote.offer[0].price),
            "timestamp": int(time.time() * 1000)
        }
        await r.set(f"quote:{quote.symbol}", json.dumps(data))

    async def on_ohlc(self, ohlc: Ohlc):
        """Handle live OHLC bars - Store in Parquet"""
        self.store.add_ohlcv(ohlc)

    def stop(self):
        print("\n[Data Service] Shutdown signal received...")
        self._stop_event.set()

    async def run(self):
        print("[Data Service] Starting Standalone Data Service (Live Ticks + OHLCV Storage)...")
        
        # 1. Register handlers
        self.ws_client.on("trade", self.on_trade)
        self.ws_client.on("quote", self.on_quote)
        self.ws_client.on("ohlc", self.on_ohlc)
        
        # 2. Start flush loop
        flush_task = asyncio.create_task(self.store.flush_loop())
        
        # 3. Connect
        try:
            await self.ws_client.connect()
            print(f"[Data Service] Subscribing to trades, quotes & 1m OHLC for: {SYMBOLS}")
            await self.ws_client.subscribe_trades(SYMBOLS)
            await self.ws_client.subscribe_quotes(SYMBOLS)
            await self.ws_client.subscribe_ohlc(SYMBOLS, "1") # Passed as string, not list
            
            # Wait for stop event instead of while True
            await self._stop_event.wait()
            
        except Exception as e:
            print(f"[Data Service] Error: {e}")
        finally:
            print("[Data Service] Cleaning up...")
            flush_task.cancel()
            self.store.flush() # Final flush before exit
            await self.ws_client.disconnect()
            print("[Data Service] Disconnected. Goodbye.")

if __name__ == "__main__":
    service = DataService()
    
    # Setup signal handling for Ctrl+C
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, service.stop)
        
    try:
        loop.run_until_complete(service.run())
    except KeyboardInterrupt:
        pass
