#!/bin/bash

# Standalone Data Service Runner
# This script runs the DNSE data adapter independently of the dashboard.

# Get the absolute path of this script's directory
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "Starting Standalone Data Service..."

# Ensure dependencies are installed (optional, assuming they are there)
# npm install

# Run the service
# You can choose between the Python WebSocket client or the Node.js Polling client
# Python is recommended for real-time WebSocket data

if [ "$1" == "--node" ]; then
    echo "Running Node.js Polling client..."
    if [ "$2" == "--background" ]; then
        nohup node index.js > data_service.log 2>&1 &
    else
        node index.js
    fi
else
    echo "Running Python WebSocket client..."
    # Check if virtualenv exists
    if [ -d "venv" ]; then
        source venv/bin/bin/activate
    fi
    
    if [ "$1" == "--background" ]; then
        nohup python3 main.py > data_service.log 2>&1 &
    else
        python3 main.py
    fi
fi
