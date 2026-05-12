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

    async def bootstrap(self):
        """Fetch historical candles from REST API to fill gaps and update Redis"""
        print("[Data Service] Bootstrapping historical data...")
        
        # Calculate timestamps for today (from midnight to now)
        now = int(time.time())
        today_start = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        
        http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=5.0, read=10.0))
        
        for symbol in SYMBOLS:
            try:
                url = f"https://openapi.dnse.com.vn/v1/market/ohlc?symbol={symbol}&resolution=1&from={today_start}&to={now}"
                print(f"[Data Service] Fetching history for {symbol}...")
                
                # Run sync request in a thread to avoid blocking the event loop
                resp = await asyncio.to_thread(
                    http.request, "GET", url, 
                    headers={"Authorization": f"Bearer {API_KEY}"}
                )
                
                if resp.status == 200:
                    data = json.loads(resp.data.decode('utf-8'))
                    if data.get("s") == "ok" and "t" in data:
                        timestamps = data["t"]
                        opens = data["o"]
                        highs = data["h"]
                        lows = data["l"]
                        closes = data["c"]
                        volumes = data["v"]
                        
                        count = len(timestamps)
                        if count > 0:
                            print(f"[Data Service] Fetched {count} historical bars for {symbol}")
                            for i in range(count):
                                ohlc = Ohlc(
                                    symbol=symbol,
                                    resolution=1,
                                    open=opens[i],
                                    high=highs[i],
                                    low=lows[i],
                                    close=closes[i],
                                    volume=volumes[i],
                                    time=timestamps[i],
                                    lastUpdated=timestamps[i],
                                    type="ohlc"
                                )
                                self.store.add_ohlcv(ohlc)
                            
                            # Update Redis with the latest candle
                            last_idx = count - 1
                            redis_data = {
                                "symbol": symbol,
                                "open": float(opens[last_idx]),
                                "high": float(highs[last_idx]),
                                "low": float(lows[last_idx]),
                                "close": float(closes[last_idx]),
                                "volume": int(volumes[last_idx]),
                                "timestamp": timestamps[last_idx] * 1000
                            }
                            await r.set(f"ohlc:{symbol}:1m", json.dumps(redis_data))
                        else:
                            print(f"[Data Service] No historical data found for {symbol} today.")
                else:
                    print(f"[Data Service] Failed to bootstrap {symbol}: HTTP {resp.status}")
            except Exception as e:
                print(f"[Data Service] Bootstrap error for {symbol}: {e}")
        
        self.store.flush()
        print("[Data Service] Bootstrap complete.")

    async def run(self):
        print("[Data Service] Starting Standalone Data Service (Live Ticks + OHLCV Storage)...")
        
        # 1. Bootstrap historical data
        await self.bootstrap()
        
        # 2. Register handlers
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

async def main():
    service = DataService()
    
    # Setup signal handling for Ctrl+C
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, service.stop)
    except NotImplementedError:
        # Signal handlers are not implemented on Windows (not relevant here but good practice)
        pass
        
    await service.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
