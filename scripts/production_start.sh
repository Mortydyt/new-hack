#!/bin/bash

# 🚀 Production Start Script for ML Service
# Использование: ./scripts/production_start.sh

set -e

echo "🚀 STARTING ML SERVICE IN PRODUCTION MODE"
echo "=" * 60

# Проверка зависимостей
echo "🔍 Checking dependencies..."
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found. Please install Python 3.8+"
    exit 1
fi

if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 not found. Please install pip3"
    exit 1
fi

# Проверка наличия необходимых файлов
if [ ! -f "ml_service/main.py" ]; then
    echo "❌ ml_service/main.py not found"
    exit 1
fi

# Установка зависимостей если нужно
echo "📦 Installing dependencies..."
cd ml_service
if [ -f "requirements.txt" ]; then
    pip3 install -r requirements.txt
else
    echo "⚠️  No requirements.txt found, installing basic dependencies..."
    pip3 install fastapi uvicorn pandas requests numpy python-multipart
fi
cd ..

# Создание необходимых директорий
echo "📁 Creating directories..."
mkdir -p cache uploads logs

# Настройка переменных окружения
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export ML_SERVICE_ENV="production"
export ML_SERVICE_HOST="0.0.0.0"
export ML_SERVICE_PORT="8001"
export ML_SERVICE_WORKERS="1"
export ML_SERVICE_LOG_LEVEL="INFO"

# Очистка старых процессов
echo "🧹 Cleaning up old processes..."
pkill -f "python.*main.py" || true
sleep 2

# Запуск сервиса
echo "🚀 Starting ML Service..."
echo "   🌐 URL: http://localhost:${ML_SERVICE_PORT}"
echo "   📚 Docs: http://localhost:${ML_SERVICE_PORT}/docs"
echo "   ❤️  Health: http://localhost:${ML_SERVICE_PORT}/health"
echo "=" * 60

# Запуск в фоне с логированием
nohup python3 -m ml_service.main > logs/service.log 2>&1 &
SERVICE_PID=$!

echo "✅ Service started with PID: ${SERVICE_PID}"
echo "📝 Logs: logs/service.log"
echo "🔍 Use './scripts/health_check.sh' to monitor service"

# Сохранение PID для последующего управления
echo ${SERVICE_PID} > logs/service.pid

# Проверка запуска
echo "⏳ Waiting for service to start..."
sleep 5

if curl -s http://localhost:${ML_SERVICE_PORT}/health > /dev/null; then
    echo "✅ Service is running and healthy!"
    echo "🎯 Ready for production testing!"
else
    echo "⚠️  Service might have issues. Check logs:"
    echo "   tail -f logs/service.log"
fi

echo "=" * 60