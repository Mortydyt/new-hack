#!/bin/bash

# Production setup script for ML Service
# Run this script after cloning from GitHub

set -e

echo "🚀 Setting up ML Service for production..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Python 3.9+ is installed
python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
echo "📋 Found Python version: $python_version"

if python3 -c "import sys; exit(0 if sys.version_info >= (3, 9) else 1)"; then
    echo -e "${GREEN}✅ Python version is compatible${NC}"
else
    echo -e "${RED}❌ Python 3.9+ is required${NC}"
    exit 1
fi

# Create virtual environment
echo "📦 Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✅ Virtual environment created${NC}"
else
    echo -e "${YELLOW}⚠️  Virtual environment already exists${NC}"
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📚 Installing dependencies..."
cd ml_service
pip install -r requirements.txt
cd ..

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p uploads cache logs

# Copy environment file if it doesn't exist
if [ ! -f "ml_service/.env" ]; then
    echo "⚙️  Creating environment file..."
    cp ml_service/.env.example ml_service/.env
    echo -e "${YELLOW}⚠️  Please edit ml_service/.env with your configuration${NC}"
    echo "   Required: OPENAI_API_KEY"
else
    echo -e "${GREEN}✅ Environment file already exists${NC}"
fi

# Set proper permissions
chmod +x scripts/*.sh

echo ""
echo -e "${GREEN}🎉 Setup completed successfully!${NC}"
echo ""
echo "📋 Next steps:"
echo "1. Edit ml_service/.env with your configuration"
echo "2. Activate virtual environment: source venv/bin/activate"
echo "3. Run the service: cd ml_service && python run.py --host 0.0.0.0 --port 8000"
echo "4. Check health: curl http://localhost:8000/health"
echo ""
echo "📚 API documentation will be available at: http://localhost:8000/docs"