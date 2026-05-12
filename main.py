import asyncio
import json
import os
import time
import signal
import pandas as pd
from pathlib import Path
import redis.asyncio as redis
import hmac
import hashlib
import base64
import urllib.parse
from typing import Optional
from datetime import datetime, timezone
import urllib3
import uuid
from dnse_ws.client import TradingClient
from dnse_ws.models import Trade, Ohlc, Quote
from dnse_ws.common import build_signature, get_date_header_name

# Load Configuration
SCRIPT_DIR = Path(__file__).parent.absolute()
CONFIG_PATH = SCRIPT_DIR / "config.json"
DATA_DIR = SCRIPT_DIR / "data"

if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r") as f:
        CONFIG = json.load(f)
else:
    # Fallback default config
    CONFIG = {
        "SYMBOLS": ["VN30F1M", "VNINDEX", "VN30"],
        "SYMBOL_TYPES": {"VN30F1M": "DERIVATIVE", "VNINDEX": "INDEX", "VN30": "INDEX"},
        "API": {"BASE_URL": "https://openapi.dnse.com.vn", "WS_URL": "wss://ws-openapi.dnse.com.vn/v1/stream?encoding=json"},
        "BOOTSTRAP": {"DELAY_SECONDS": 10, "BACKOFF_SECONDS": 30, "RETRIES": 3},
        "REDIS": {"URL": "redis://localhost:6379", "CANDLE_LIMIT": 1000}
    }

SYMBOLS = CONFIG["SYMBOLS"]
TYPE_MAP = CONFIG["SYMBOL_TYPES"]
API_KEY = os.getenv("DNSE_API_KEY", "eyJvcmciOiJkbnNlIiwiaWQiOiJiNDcxYTBhNjE4MTI0ZWNjYTI0YjI2YzcyMGExNzdkZiIsImgiOiJtdXJtdXIxMjgifQ==")
API_SECRET = os.getenv("DNSE_API_SECRET", "510ksymQU949Se_NphYe3_LXT1O8zclFx1lam3MPRuIMOQhdOvokSQPE7YmhHEUTS4pCq9ZqaWnpbaui34AJVw")
REDIS_URL = os.getenv("REDIS_URL", CONFIG["REDIS"]["URL"])

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

    def get_last_timestamp(self, symbol: str) -> Optional[int]:
        """Detect the last stored timestamp for a symbol across all parquet files"""
        symbol_path = symbol.replace("!", "F")
        symbol_dir = self.base_dir / "ohlcv" / symbol_path
        if not symbol_dir.exists():
            return None
        
        # Get latest parquet file by filename (date)
        files = sorted(list(symbol_dir.glob("*.parquet")), reverse=True)
        if not files:
            return None
        
        try:
            latest_file = files[0]
            df = pd.read_parquet(latest_file)
            if not df.empty:
                last_ts = df["timestamp"].max()
                if isinstance(last_ts, datetime):
                    return int(last_ts.timestamp())
                return int(pd.to_datetime(last_ts).timestamp())
        except Exception as e:
            print(f"[Data Store] Error reading last timestamp for {symbol}: {e}")
        return None

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
            
            # Check if we should update last candle or append new one
            new_candle = {
                "time": ohlc.time,
                "open": float(ohlc.open), "high": float(ohlc.high),
                "low": float(ohlc.low), "close": float(ohlc.close),
                "volume": int(ohlc.volume), "type": TYPE_MAP.get(ohlc.symbol, "INDEX")
            }
            
            if candles and candles[-1]["time"] == ohlc.time:
                candles[-1] = new_candle
            else:
                candles.append(new_candle)
            
            # Keep only last 1000 bars
            if len(candles) > 1000:
                candles = candles[-1000:]
                
            await r.set(key, json.dumps(candles))
            
            # 3. Publish update for real-time UI
            pub_data = {
                "symbol": ohlc.symbol,
                "price": float(ohlc.close),
                "timestamp": int(time.time() * 1000),
                "bar": new_candle
            }
            await r.publish("market_data", json.dumps(pub_data))

            # Also update higher timeframes for simple visualization
            for tf in ["5m", "15m", "1h", "4h", "1D"]:
                await r.set(f"candles:{ohlc.symbol}:{tf}", json.dumps(candles))
                
        except Exception as e:
            print(f"[Data Service] Error updating Redis candles: {e}")

    def stop(self):
        print("\n[Data Service] Shutdown signal received...")
        self._stop_event.set()

    async def try_fallback(self, symbol: str):
        """Load from local Parquet or CSV if API is unavailable"""
        try:
            print(f"[Data Service] Attempting local fallback for {symbol}...")
            
            # 1. Try Parquet (New format)
            symbol_path = symbol.replace("!", "F")
            dir_path = DATA_DIR / "ohlcv" / symbol_path
            
            if dir_path.exists():
                files = sorted(list(dir_path.glob("*.parquet")), reverse=True)
                if files:
                    latest_file = files[0]
                    print(f"[Data Service] Fallback: Loading latest Parquet {latest_file.name}")
                    df = pd.read_parquet(latest_file)
                    await self._populate_redis_from_df(symbol, df)
                    return

            # 2. Try CSV (Legacy format in DATA_DIR)
            csv_path = DATA_DIR / f"{symbol}_1m.csv"
            if csv_path.exists():
                print(f"[Data Service] Fallback: Loading CSV {csv_path}")
                df = pd.read_csv(csv_path)
                col_map = {
                    'datetime': 'timestamp',
                    'time': 'timestamp', 'timestamp': 'timestamp',
                    'open': 'open', 'o': 'open',
                    'high': 'high', 'h': 'high',
                    'low': 'low', 'l': 'low',
                    'close': 'close', 'c': 'close',
                    'volume': 'volume', 'v': 'volume'
                }
                df = df.rename(columns={c: col_map[c.lower()] for c in df.columns if c.lower() in col_map})
                
                if 'timestamp' in df.columns:
                    if df['timestamp'].dtype == 'object':
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                    elif df['timestamp'].dtype in ['int64', 'float64']:
                        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                        
                    await self._populate_redis_from_df(symbol, df)
                    return
            
            print(f"[Data Service] Fallback: No data found for {symbol} in Parquet or CSV")
        except Exception as e:
            print(f"[Data Service] Fallback failed for {symbol}: {e}")

    async def _populate_redis_from_df(self, symbol: str, df: pd.DataFrame):
        """Helper to fill Redis from a DataFrame"""
        if df.empty: return
        bars = []
        df_display = df.tail(1000)
        for _, row in df_display.iterrows():
            ts = row['timestamp']
            try:
                if isinstance(ts, datetime):
                    ts_unix = int(ts.timestamp())
                elif isinstance(ts, str):
                    ts_unix = int(pd.to_datetime(ts).timestamp())
                else:
                    ts_unix = int(ts)
                
                bars.append({
                    "time": ts_unix,
                    "open": float(row['open']), "high": float(row['high']),
                    "low": float(row['low']), "close": float(row['close']),
                    "volume": int(row['volume']), "type": TYPE_MAP.get(symbol, "INDEX")
                })
            except Exception as e:
                continue
                
        if bars:
            print(f"[Data Service] Fallback: Populated {len(bars)} bars into Redis for {symbol}")
            await r.set(f"candles:{symbol}:1m", json.dumps(bars))
            for tf in ["5m", "15m", "1h", "4h", "1D"]:
                await r.set(f"candles:{symbol}:{tf}", json.dumps(bars))

    async def bootstrap(self):
        """Fetch historical candles from REST API to fill gaps and update Redis"""
        print(f"[Data Service] Bootstrapping historical data. Filling gaps...")
        
        now = int(time.time())
        today_start = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        
        http = urllib3.PoolManager(timeout=urllib3.Timeout(connect=15.0, read=30.0))

        async def sync_symbol(symbol):
            try:
                # 1. Determine gap start
                last_ts = self.store.get_last_timestamp(symbol)
                # Max gap to fetch: 3 days (to stay within API limits)
                start_from = max((last_ts + 60) if last_ts else (today_start - 86400 * 2), now - 86400 * 3)
                
                # 2. Correct API Path and Query (Strictly following Official SDK)
                if not CONFIG["BOOTSTRAP"].get("ENABLE_FETCH_API", True):
                    print(f"[Data Service] REST Fetch disabled for {symbol}. Using local fallback.")
                    await self.try_fallback(symbol)
                    return False

                path = "/price/ohlc"
                symbol_type = TYPE_MAP.get(symbol, "STOCK")
                params = {
                    "symbol": symbol,
                    "resolution": "1",
                    "from": start_from,
                    "to": now,
                    "type": symbol_type
                }
                
                # Build final URL with query params
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                url = f"{CONFIG['API']['BASE_URL']}{path}?{query_string}"
                
                # 3. Generate HMAC Signature Headers (SDK Parity)
                # JS format: "Tue, 12 May 2026 15:00:00 +0000"
                now_utc = datetime.now(timezone.utc)
                DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
                MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                day_name = DAY_NAMES[now_utc.weekday()]
                day = str(now_utc.day).zfill(2)
                month = MONTH_NAMES[now_utc.month - 1]
                year = now_utc.year
                hours = str(now_utc.hour).zfill(2)
                minutes = str(now_utc.minute).zfill(2)
                seconds = str(now_utc.second).zfill(2)
                date_value = f"{day_name}, {day} {month} {year} {hours}:{minutes}:{seconds} +0000"
                
                nonce = uuid.uuid4().hex.replace("-", "")
                
                # JS signature string: "(request-target): get /price/ohlc\ndate: DateValue\nnonce: Nonce"
                sig_string = f"(request-target): get {path}\ndate: {date_value}\nnonce: {nonce}"
                
                mac = hmac.new(API_SECRET.encode("utf-8"), sig_string.encode("utf-8"), hashlib.sha256)
                signature = urllib.parse.quote(base64.b64encode(mac.digest()).decode("utf-8"), safe="")
                
                sig_header_val = (
                    f'Signature keyId="{API_KEY}",algorithm="hmac-sha256",'
                    f'headers="(request-target) date",signature="{signature}",nonce="{nonce}"'
                )
                
                headers = {
                    "Date": date_value,
                    "X-Signature": sig_header_val,
                    "x-api-key": API_KEY,
                    "Accept": "application/json",
                    "User-Agent": "DNSE-Python-SDK/1.0"
                }

                last_date_str = datetime.fromtimestamp(start_from).strftime('%Y-%m-%d %H:%M')
                print(f"[Data Service] Fetching {symbol} ({symbol_type}) via /price/ohlc...")
                
                resp = await asyncio.to_thread(
                    http.request, "GET", url, 
                    headers=headers,
                    timeout=20.0
                )
                
                all_bars = []
                if resp.status == 200:
                    data = json.loads(resp.data.decode('utf-8'))
                    if isinstance(data, dict) and "t" in data:
                        for i in range(len(data["t"])):
                            bar = {
                                "time": int(data["t"][i]),
                                "open": float(data["o"][i]), "high": float(data["h"][i]),
                                "low": float(data["l"][i]), "close": float(data["c"][i]),
                                "volume": int(data["v"][i]) if "v" in data else 0,
                                "type": symbol_type
                            }
                            all_bars.append(bar)
                            
                            ohlc_obj = Ohlc(
                                symbol=symbol, resolution="1",
                                open=bar["open"], high=bar["high"],
                                low=bar["low"], close=bar["close"],
                                volume=bar["volume"], time=bar["time"]
                            )
                            self.store.add_ohlcv(ohlc_obj)
                    
                    if all_bars:
                        print(f"[Data Service] Recovered {len(all_bars)} bars for {symbol}")
                        await r.set(f"candles:{symbol}:1m", json.dumps(all_bars[-CONFIG["REDIS"]["CANDLE_LIMIT"]:]))
                        for tf in ["5m", "15m", "1h", "4h", "1D"]:
                            await r.set(f"candles:{symbol}:{tf}", json.dumps(all_bars[-CONFIG["REDIS"]["CANDLE_LIMIT"]:]))
                        return True
                    return False
                
                elif resp.status == 429:
                    print(f"[Data Service] Rate Limit hit for {symbol}. Need to back off.")
                    return "RATE_LIMIT"
                
                else:
                    body = resp.data.decode('utf-8')[:200]
                    print(f"[Data Service] API Error {resp.status} for {symbol}: {body}")
                    if resp.status == 404:
                        print(f"[Data Service] Retrying legacy endpoint for {symbol}...")
                        legacy_url = f"https://openapi.dnse.com.vn/v1/market/ohlc?symbol={symbol}&resolution=1&from={start_from}&to={now}"
                        resp = await asyncio.to_thread(
                            http.request, "GET", legacy_url,
                            headers={"Authorization": f"Bearer {API_KEY}"},
                            timeout=10.0
                        )
                        if resp.status == 200:
                            data = json.loads(resp.data.decode('utf-8'))
                            if data.get("s") == "ok" and "t" in data:
                                for i in range(len(data["t"])):
                                    bar = {"time": int(data["t"][i]), "open": float(data["o"][i]), "high": float(data["h"][i]), "low": float(data["l"][i]), "close": float(data["c"][i]), "volume": int(data["v"][i]), "type": symbol_type}
                                    all_bars.append(bar)
                                    self.store.add_ohlcv(Ohlc(symbol=symbol, resolution="1", open=bar["open"], high=bar["high"], low=bar["low"], close=bar["close"], volume=bar["volume"], time=bar["time"]))
                                
                                if all_bars:
                                    print(f"[Data Service] Recovered {len(all_bars)} bars for {symbol} (Legacy)")
                                    await r.set(f"candles:{symbol}:1m", json.dumps(all_bars[-CONFIG["REDIS"]["CANDLE_LIMIT"]:]))
                                    for tf in ["5m", "15m", "1h", "4h", "1D"]:
                                        await r.set(f"candles:{symbol}:{tf}", json.dumps(all_bars[-CONFIG["REDIS"]["CANDLE_LIMIT"]:]))
                                    return True

                await self.try_fallback(symbol)
                return False

            except Exception as e:
                print(f"[Data Service] Bootstrap error for {symbol}: {str(e)}")
                await self.try_fallback(symbol)
                return False

        # Run syncs sequentially with delays and backoff
        for s in SYMBOLS:
            for attempt in range(CONFIG["BOOTSTRAP"]["RETRIES"]):
                result = await sync_symbol(s)
                if result == "RATE_LIMIT":
                    wait_time = CONFIG["BOOTSTRAP"]["BACKOFF_SECONDS"] * (attempt + 1)
                    print(f"[Data Service] Rate limit encountered. Waiting {wait_time}s before retry...")
                    await asyncio.sleep(wait_time)
                    continue
                
                # Success or normal error, wait configurable delay before next symbol
                await asyncio.sleep(CONFIG["BOOTSTRAP"]["DELAY_SECONDS"])
                break 
        
        self.store.flush()
        print("[Data Service] Bootstrap sequence complete.")

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
