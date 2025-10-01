#!/bin/bash

# Stop script for ML Service

echo "🛑 Stopping ML Service..."

# Find and kill the service process
PID=$(pgrep -f "python.*run.py" | head -1)

if [ -n "$PID" ]; then
    kill $PID
    echo "✅ Service stopped (PID: $PID)"
else
    echo "ℹ️  Service is not running"
fi