#!/bin/bash

# Start script for ML Service in production

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "🚀 Starting ML Service..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run ./scripts/setup.sh first"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check if environment file exists
if [ ! -f "ml_service/.env" ]; then
    echo "❌ Environment file not found. Please copy ml_service/.env.example to ml_service/.env and configure it"
    exit 1
fi

# Change to ml_service directory
cd ml_service

# Start the service
echo "🌐 Starting ML Service on 0.0.0.0:8000..."
echo "📚 API documentation: http://localhost:8000/docs"
echo "💾 Logs directory: $(pwd)/../logs"
echo ""
echo "Press Ctrl+C to stop the service"
echo ""

python run.py --host 0.0.0.0 --port 8000