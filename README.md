# DNSE Market Data Service

A high-performance market data ingestion and storage service for the Vietnam Stock Market (DNSE). This service provides real-time data streaming via WebSockets and persistent storage in Parquet format for backtesting and analysis.

## 🚀 Overview

The DNSE Market Data Service acts as the backbone of the Alpha Engine ecosystem, bridging the DNSE Open API with our internal real-time dashboard and backtesting engines. For a deep dive into the system design, see [architecture.md](./architecture.md).

### Key Features
- **Real-time Streaming**: Connects to DNSE WebSockets for low-latency Ticks, Quotes, and 1m OHLC bars.
- **Persistence**: Automatically flushes OHLCV data to **Snappy-compressed Parquet** files for high-speed historical analysis.
- **Redis Integration**: Publishes live market data to Redis Pub/Sub for immediate consumption by the `vps_dashboard`.
- **Intelligent Bootstrap**: Automatically fetches missing historical data from the REST API on startup.
- **Multi-layer Fallback**: Robust data recovery path: REST API ➔ Local Parquet ➔ Legacy CSV.

## 🛠️ Architecture

The service is designed to be standalone and resilient:

1.  **WebSocket Client**: Maintains a persistent connection to DNSE for live updates.
2.  **Redis Pub/Sub**: Acts as the real-time data bus for the frontend dashboard.
3.  **Parquet Store**: Efficiently stores massive amounts of intraday data with deduplication.
4.  **REST Fallback**: Handles gap filling and historical data fetching when WebSockets are insufficient.

## 📂 Project Structure

```text
dnse_service/
├── data/               # Local storage for Parquet and CSV files
│   └── ohlcv/          # Organized by symbol (e.g., VN30F1M, VNINDEX)
├── dnse_ws/            # Custom Python SDK for DNSE WebSockets
├── main.py             # Primary Python entry point (Recommended)
├── index.js            # Node.js fallback entry point (Polling-based)
├── run.sh              # Unified startup script
└── requirements.txt    # Python dependencies
```

## ⚡ Quick Start

### Prerequisites
- Python 3.10+
- Redis Server (default: `localhost:6379`)

### Installation
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Service
Use the provided `run.sh` script to start the service:

```bash
# Start the Python WebSocket client (Recommended)
./run.sh

# Start in the background
./run.sh --background

# Run the Node.js Polling client (Alternative)
./run.sh --node
```

## ⚙️ Configuration

Environment variables can be set in a `.env` file or exported to your shell:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DNSE_API_KEY` | Your DNSE OpenAPI Key | (Default Provided) |
| `DNSE_API_SECRET` | Your DNSE OpenAPI Secret | (Default Provided) |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |

## 📊 Data Format

Stored Parquet files use the following schema:
- `timestamp`: UTC Datetime
- `open`, `high`, `low`, `close`: Float
- `volume`: Integer

Files are stored daily: `data/ohlcv/{SYMBOL}/{YYYY-MM-DD}.parquet`
