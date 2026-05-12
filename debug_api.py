import urllib3
import json
import time

API_KEY = "eyJvcmciOiJkbnNlIiwiaWQiOiJiNDcxYTBhNjE4MTI0ZWNjYTI0YjI2YzcyMGExNzdkZiIsImgiOiJtdXJtdXIxMjgifQ=="
symbol = "VNINDEX"
now = int(time.time())
today_start = now - 3600 * 2 # Last 2 hours

http = urllib3.PoolManager()
url = f"https://openapi.dnse.com.vn/v1/market/ohlc?symbol={symbol}&resolution=1&from={today_start}&to={now}"

print(f"Requesting: {url}")
resp = http.request("GET", url, headers={"Authorization": f"Bearer {API_KEY}"})
print(f"Status: {resp.status}")
print(f"Data: {resp.data.decode('utf-8')[:500]}")
