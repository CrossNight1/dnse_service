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
SYMBOLS = ["VN30F1M", "VNINDEX", "VN30"]
TYPE_MAP = {
    "VN30F1M": "DERIVATIVE",
    "VNINDEX": "INDEX",
    "VN30": "INDEX"
}
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
        """Handle live trades - Push to Redis for live chart"""
        data = {
            "symbol": trade.symbol,
            "price": float(trade.price),
            "timestamp": trade.time,
            "side": trade.side,
            "volume": int(trade.volume),
            "type": TYPE_MAP.get(trade.symbol, "INDEX")
        }
        await r.publish("market_data", json.dumps(data))
        await r.set(f"tick:{trade.symbol}", json.dumps(data))

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
        """Handle live OHLC bars - Store in Parquet & Update Redis List"""
        # 1. Store for backtesting
        self.store.add_ohlcv(ohlc)
        
        # 2. Update Redis candles list (Dashboard expectation)
        try:
            key = f"candles:{ohlc.symbol}:1m"
            raw = await r.get(key)
            candles = json.loads(raw) if raw else []
            
            # Append new candle
            new_candle = {
                "time": ohlc.time,
                "open": float(ohlc.open),
                "high": float(ohlc.high),
                "low": float(ohlc.low),
                "close": float(ohlc.close),
                "volume": int(ohlc.volume),
                "type": TYPE_MAP.get(ohlc.symbol, "INDEX")
            }
            candles.append(new_candle)
            
            # Keep only last 1000 bars in Redis to prevent memory bloat
            if len(candles) > 1000:
                candles = candles[-1000:]
                
            await r.set(key, json.dumps(candles))
            
            # Also update higher timeframes for simple visualization (Optional)
            for tf in ["5m", "15m", "1h", "4h", "1D"]:
                await r.set(f"candles:{ohlc.symbol}:{tf}", json.dumps(candles))
                
        except Exception as e:
            print(f"[Data Service] Error updating Redis candles: {e}")

    def stop(self):
        print("\n[Data Service] Shutdown signal received...")
        self._stop_event.set()

    async def bootstrap(self):
        """Fetch historical candles from REST API to fill gaps and update Redis"""
        print(f"[Data Service] Bootstrapping historical data from today start...")
        
        # Calculate timestamps for today (from midnight to now)
        now = int(time.time())
        today_start = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        
        http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=5.0, read=10.0))

        async def sync_symbol(symbol):
            try:
                url = f"https://openapi.dnse.com.vn/v1/market/ohlc?symbol={symbol}&resolution=1&from={today_start}&to={now}"
                print(f"[Data Service] Fetching history for {symbol}...")
                
                resp = await asyncio.to_thread(
                    http.request, 
                    "GET", 
                    url, 
                    headers={"Authorization": f"Bearer {API_KEY}"},
                    timeout=5.0
                )
                
                all_bars = []
                if resp.status == 200:
                    data = json.loads(resp.data.decode('utf-8'))
                    if data.get("s") == "ok" and "t" in data:
                        for i in range(len(data["t"])):
                            bar = {
                                "time": int(data["t"][i]),
                                "open": float(data["o"][i]),
                                "high": float(data["h"][i]),
                                "low": float(data["l"][i]),
                                "close": float(data["c"][i]),
                                "volume": int(data["v"][i]),
                                "type": TYPE_MAP.get(symbol, "INDEX")
                            }
                            all_bars.append(bar)
                            
                            # Store in Parquet buffer
                            ohlc_obj = Ohlc(
                                symbol=symbol, resolution="1",
                                open=bar["open"], high=bar["high"],
                                low=bar["low"], close=bar["close"],
                                volume=bar["volume"], time=bar["time"]
                            )
                            self.store.add_ohlcv(ohlc_obj)
                
                if all_bars:
                    print(f"[Data Service] Bootstrapped {len(all_bars)} bars for {symbol}")
                    await r.set(f"candles:{symbol}:1m", json.dumps(all_bars))
                    for tf in ["5m", "15m", "1h", "4h", "1D"]:
                        await r.set(f"candles:{symbol}:{tf}", json.dumps(all_bars))
                    return

                # FALLBACK: Try local Parquet
                print(f"[Data Service] API failed or no data for {symbol}, trying local fallback...")
                date_str = datetime.now().strftime("%Y-%m-%d")
                symbol_path = symbol.replace("!", "F")
                file_path = DATA_DIR / "ohlcv" / symbol_path / f"{date_str}.parquet"
                
                if file_path.exists():
                    df = pd.read_parquet(file_path)
                    if not df.empty:
                        fallback_bars = []
                        for _, row in df.iterrows():
                            fallback_bars.append({
                                "time": int(row['timestamp'].timestamp()),
                                "open": float(row['open']), "high": float(row['high']),
                                "low": float(row['low']), "close": float(row['close']),
                                "volume": int(row['volume']), "type": TYPE_MAP.get(symbol, "INDEX")
                            })
                        print(f"[Data Service] Fallback: Loaded {len(fallback_bars)} bars from Parquet for {symbol}")
                        await r.set(f"candles:{symbol}:1m", json.dumps(fallback_bars))
                        for tf in ["5m", "15m", "1h", "4h", "1D"]:
                            await r.set(f"candles:{symbol}:{tf}", json.dumps(fallback_bars))
                else:
                    print(f"[Data Service] No local fallback data for {symbol}")

            except Exception as e:
                print(f"[Data Service] Error bootstrapping {symbol}: {str(e)}")

        # Run all syncs in parallel
        await asyncio.gather(*(sync_symbol(s) for s in SYMBOLS))
        
        self.store.flush()
        print("[Data Service] Bootstrap complete.")

    async def run(self):
        print("[Data Service] Starting Standalone Data Service (Live Ticks + OHLCV Storage)...")
        
        # 1. Start bootstrap in the background (Non-blocking)
        # This prevents the service from hanging if the REST API is slow
        asyncio.create_task(self.bootstrap())
        
        # 2. Register handlers
        self.ws_client.on("trade", self.on_trade)
        self.ws_client.on("quote", self.on_quote)
        self.ws_client.on("ohlc", self.on_ohlc)
        
        # 3. Start flush loop
        flush_task = asyncio.create_task(self.store.flush_loop())
        
        # 4. Connect
        try:
            await self.ws_client.connect()
            print(f"[Data Service] Subscribing to trades, quotes & 1m OHLC for: {SYMBOLS}")
            await self.ws_client.subscribe_trades(SYMBOLS)
            await self.ws_client.subscribe_quotes(SYMBOLS)
            await self.ws_client.subscribe_ohlc(SYMBOLS, "1")
            
            # Wait for stop event
            await self._stop_event.wait()
            
        except Exception as e:
            print(f"[Data Service] Error: {e}")
        finally:
            print("[Data Service] Cleaning up...")
            flush_task.cancel()
            self.store.flush()
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
