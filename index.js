'use strict';

const Redis = require('ioredis');
const { DNSEClient } = require('./dnse-datafeed-sdk/javascript/dnse');

const API_KEY = process.env.DNSE_API_KEY || 'eyJvcmciOiJkbnNlIiwiaWQiOiJiNDcxYTBhNjE4MTI0ZWNjYTI0YjI2YzcyMGExNzdkZiIsImgiOiJtdXJtdXIxMjgifQ==';
const API_SECRET = process.env.DNSE_API_SECRET || '510ksymQU949Se_NphYe3_LXT1O8zclFx1lam3MPRuIMOQhdOvokSQPE7YmhHEUTS4pCq9ZqaWnpbaui34AJVw';
const REDIS_ADDR = process.env.REDIS_ADDR || 'localhost:6379';

const redis = new Redis(REDIS_ADDR);
const client = new DNSEClient({
    apiKey: API_KEY,
    apiSecret: API_SECRET,
    baseUrl: 'https://openapi.dnse.com.vn',
});

const SYMBOLS = ['VN301!', 'VNINDEX', 'VN30'];

// Intervals: base is 1m, aggregated from that
const INTERVALS = ['1', '5', '15', '60', '240', 'D']; // DNSE resolution codes

// ─── Aggregate 1m candles into higher timeframes ──────────────────────────────
function aggregateCandles(candles1m, minutes) {
    const buckets = {};
    for (const c of candles1m) {
        const key = Math.floor(c.time / (minutes * 60)) * (minutes * 60);
        if (!buckets[key]) {
            buckets[key] = { time: key, open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume || 0 };
        } else {
            const b = buckets[key];
            b.high = Math.max(b.high, c.high);
            b.low = Math.min(b.low, c.low);
            b.close = c.close;
            b.volume = (b.volume || 0) + (c.volume || 0);
        }
    }
    return Object.values(buckets).sort((a, b) => a.time - b.time);
}

// ─── Store candles into Redis for all intervals ───────────────────────────────
async function storeAllIntervals(symbol, candles1m) {
    const pipeline = redis.pipeline();

    // Store 1m
    pipeline.set(`candles:${symbol}:1m`, JSON.stringify(candles1m));

    // Aggregate & store higher timeframes
    const TF_MAP = [
        { key: '5m',  min: 5   },
        { key: '15m', min: 15  },
        { key: '1h',  min: 60  },
        { key: '4h',  min: 240 },
        { key: '1D',  min: 1440 },
    ];
    for (const tf of TF_MAP) {
        const agg = aggregateCandles(candles1m, tf.min);
        pipeline.set(`candles:${symbol}:${tf.key}`, JSON.stringify(agg));
    }

    // Also store the latest tick price
    if (candles1m.length > 0) {
        const last = candles1m[candles1m.length - 1];
        pipeline.set(`tick:${symbol}`, JSON.stringify({
            symbol, price: last.close, timestamp: last.time * 1000
        }));
    }

    await pipeline.exec();
    console.log(`[STORE] ${symbol}: Stored candles in Redis for all intervals`);
}

// ─── Fetch full OHLCV history from DNSE ──────────────────────────────────────
async function fetchHistory(symbol) {
    try {
        let apiType = 'STOCK';
        let apiSymbol = symbol;
        if (symbol === 'VNINDEX' || symbol === 'VN30') apiType = 'INDEX';
        else if (symbol === 'VN301!') { apiType = 'DERIVATIVE'; apiSymbol = 'VN30F1M'; }

        const now = Math.floor(Date.now() / 1000);
        const { status, body } = await client.getOhlc(apiType, {
            query: { symbol: apiSymbol, resolution: '1', from: now - 86400 * 2, to: now }
        });

        if (status === 200) {
            const data = JSON.parse(body);
            if (data && data.t && data.t.length > 0) {
                const candles1m = data.t.map((t, i) => ({
                    time: t,
                    open: data.o[i],
                    high: data.h[i],
                    low: data.l[i],
                    close: data.c[i],
                    volume: data.v ? data.v[i] : 0,
                }));

                await storeAllIntervals(symbol, candles1m);

                // Notify all connected WS clients that history is ready
                await redis.publish('history_ready', JSON.stringify({ symbol }));
                console.log(`[DNSE] Fetched ${candles1m.length} bars for ${symbol}`);
                return true;
            }
        }
    } catch (err) {
        console.error(`[DNSE] History fetch failed for ${symbol}:`, err.message);
    }
    return false;
}

async function generateMockHistory(symbol) {
    console.log(`[DNSE] Generating MOCK history for ${symbol}...`);
    const mockCandles = [];
    const now = Math.floor(Date.now() / 1000);
    let basePrice = symbol === 'VN301!' ? 1275 : (symbol === 'VNINDEX' ? 1250 : 1265);
    for (let i = 0; i < 200; i++) {
        const time = now - (200 - i) * 60;
        const change = (Math.random() - 0.5) * 2;
        const open = basePrice;
        const close = basePrice + change;
        mockCandles.push({
            time,
            open,
            high: Math.max(open, close) + Math.random(),
            low: Math.min(open, close) - Math.random(),
            close,
            volume: Math.floor(Math.random() * 10000)
        });
        basePrice = close;
    }
    await storeAllIntervals(symbol, mockCandles);
    await redis.publish('history_ready', JSON.stringify({ symbol }));
}

// ─── Fetch latest tick and update live candle ─────────────────────────────────
async function fetchRealData() {
    for (const symbol of SYMBOLS) {
        try {
            let apiType = 'STOCK';
            let apiSymbol = symbol;
            if (symbol === 'VNINDEX' || symbol === 'VN30') apiType = 'INDEX';
            else if (symbol === 'VN301!') { apiType = 'DERIVATIVE'; apiSymbol = 'VN30F1M'; }

            const now = Math.floor(Date.now() / 1000);
            const { status, body } = await client.getOhlc(apiType, {
                query: { symbol: apiSymbol, resolution: '1', from: now - 120, to: now }
            });

            if (status === 200) {
                const data = JSON.parse(body);
                if (data && data.c && data.c.length > 0) {
                    const latestBar = {
                        time: data.t[data.t.length - 1],
                        open: data.o[data.o.length - 1],
                        high: data.h[data.h.length - 1],
                        low: data.l[data.l.length - 1],
                        close: data.c[data.c.length - 1],
                        volume: data.v ? data.v[data.v.length - 1] : 0,
                    };

                    // Publish live tick for all clients
                    await redis.publish('market_data', JSON.stringify({
                        symbol,
                        price: latestBar.close,
                        timestamp: latestBar.time * 1000,
                        bar: latestBar, // Include full OHLC bar
                    }));

                    // Update live candle cache
                    await redis.set(`tick:${symbol}`, JSON.stringify({ symbol, price: latestBar.close, timestamp: latestBar.time * 1000 }));
                }
            }
        } catch (err) {
            // Market closed or error — skip silently
        }
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
    console.log('[DNSE Adapter] Starting...');

    for (const s of SYMBOLS) {
        // Always attempt to fetch fresh history on startup to ensure we have the latest data for the new day
        console.log(`[DNSE Adapter] Fetching fresh history for ${s} from DNSE API...`);
        const ok = await fetchHistory(s);
        
        if (!ok) {
            // Check if we at least have stale data in Redis
            const data = await redis.get(`candles:${s}:1m`);
            if (data) {
                console.log(`[DNSE Adapter] API fetch failed for ${s}, using existing data from Redis.`);
                await redis.publish('history_ready', JSON.stringify({ symbol: s }));
            } else {
                console.log(`[DNSE Adapter] No data found for ${s} in Redis or API.`);
            }
        }
    }

    // Always start polling for real data so we pick it up when available
    setInterval(async () => {
        for (const s of SYMBOLS) await fetchHistory(s);
    }, 600000); // Poll history less frequently (10 mins)
    setInterval(fetchRealData, 2000);
}

main().catch(console.error);
