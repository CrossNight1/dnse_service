import asyncio
import json
import os
import time
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

r = redis.from_url(REDIS_URL, decode_responses=True)

class DataService:
    def __init__(self):
        self.ws_client = TradingClient(
            api_key=API_KEY,
            api_secret=API_SECRET,
            auto_reconnect=True
        )
        self.http = urllib3.PoolManager()

    async def fetch_history_rest(self, symbol):
        """Fetch history via REST API (Initial Sync)"""
        print(f"[Data Service] Fetching history for {symbol} via REST...")
        now = int(time.time())
        # Mapping symbol to DNSE API type/symbol if needed (Assuming direct for now)
        api_symbol = symbol.replace("!", "") # Handle VN301! -> VN301
        
        # Build REST request
        path = "/price/ohlc"
        query = {
            "symbol": api_symbol,
            "resolution": "1",
            "from": now - 86400 * 2,
            "to": now,
            "type": "INDEX" if "INDEX" in symbol or "VN30" == symbol else "DERIVATIVE" if "1!" in symbol else "STOCK"
        }
        
        # Simplified request logic for history
        # (In a real app, we'd use the full DNSEClient, but we'll stick to a simple fetch for the script)
        # For now, let's just log that we are ready to receive WS data
        print(f"[Data Service] History sync for {symbol} would happen here.")

    async def on_trade(self, trade: Trade):
        """Handle live trades (Ticks)"""
        # print(f"[Data Service] Tick: {trade.symbol} @ {trade.price}")
        
        data = {
            "symbol": trade.symbol,
            "price": float(trade.price),
            "timestamp": int(time.time() * 1000)
        }
        
        # 1. Update latest tick in Redis
        await r.set(f"tick:{trade.symbol}", json.dumps(data))
        
        # 2. Publish to market_data channel
        await r.publish("market_data", json.dumps(data))

    async def on_ohlc(self, ohlc: Ohlc):
        """Handle live OHLC bars"""
        # print(f"[Data Service] Bar: {ohlc.symbol} {ohlc.close}")
        # Note: WebSocket OHLC might need transformation to match dashboard expectations
        pass

    async def run(self):
        print("[Data Service] Starting Standalone Data Service...")
        
        # 1. Register handlers
        self.ws_client.on("trade", self.on_trade)
        self.ws_client.on("ohlc", self.on_ohlc)
        
        # 2. Connect
        try:
            await self.ws_client.connect()
            
            # 3. Subscribe
            print(f"[Data Service] Subscribing to trades for: {SYMBOLS}")
            await self.ws_client.subscribe_trades(SYMBOLS)
            
            # 4. Keep running
            while True:
                await asyncio.sleep(10)
                if not self.ws_client.is_healthy:
                    print("[Data Service] Client unhealthy, reconnecting...")
                    # Reconnection is handled by the client automatically if enabled
                    
        except Exception as e:
            print(f"[Data Service] Error: {e}")
        finally:
            await self.ws_client.disconnect()

if __name__ == "__main__":
    service = DataService()
    asyncio.run(service.run())
