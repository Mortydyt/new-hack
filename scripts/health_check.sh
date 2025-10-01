#!/bin/bash

# 🔍 Health Check Script for ML Service
# Использование: ./scripts/health_check.sh

echo "🔍 ML SERVICE HEALTH CHECK"
echo "=" * 50

BASE_URL="http://localhost:8001"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Функция для проверки статуса
check_status() {
    local url=$1
    local name=$2

    if curl -s -f "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✅${NC} $name: OK"
        return 0
    else
        echo -e "${RED}❌${NC} $name: FAILED"
        return 1
    fi
}

# Функция для проверки времени ответа
check_response_time() {
    local url=$1
    local name=$2

    local response_time=$(curl -o /dev/null -s -w '%{time_total}' "$url")
    local response_time_ms=$(echo "$response_time * 1000" | bc | cut -d. -f1)

    if (( response_time_ms < 1000 )); then
        echo -e "${GREEN}⚡${NC} $name: ${response_time_ms}ms"
    elif (( response_time_ms < 3000 )); then
        echo -e "${YELLOW}⏱️${NC} $name: ${response_time_ms}ms"
    else
        echo -e "${RED}🐌${NC} $name: ${response_time_ms}ms"
    fi
}

# Проверка базовых endpoint
echo "🌐 Checking API Endpoints:"
check_status "${BASE_URL}/" "Root"
check_status "${BASE_URL}/health" "Health"
check_status "${BASE_URL}/docs" "Documentation"

# Проверка времени ответа
echo -e "\n⏱️  Response Times:"
check_response_time "${BASE_URL}/health" "Health Check"

# Проверка системных ресурсов
echo -e "\n💻 System Resources:"
if command -v ps &> /dev/null; then
    if pgrep -f "python.*main.py" > /dev/null; then
        echo -e "${GREEN}✅${NC} ML Service Process: Running"

        # Проверка использования памяти процессом
        if command -v ps &> /dev/null; then
            local memory_usage=$(ps -o pid,ppid,cmd,%mem,%cpu --sort=-%mem -C python3 | grep "main.py" | head -1 | awk '{print $4}')
            local cpu_usage=$(ps -o pid,ppid,cmd,%mem,%cpu --sort=-%mem -C python3 | grep "main.py" | head -1 | awk '{print $5}')
            echo -e "${GREEN}💾${NC} Memory Usage: ${memory_usage}%"
            echo -e "${GREEN}📊${NC} CPU Usage: ${cpu_usage}%"
        fi
    else
        echo -e "${RED}❌${NC} ML Service Process: Not Running"
    fi
fi

# Проверка дискового пространства
echo -e "\n💿 Disk Space:"
if command -v df &> /dev/null; then
    local disk_usage=$(df -h . | tail -1 | awk '{print $5}' | sed 's/%//')
    if (( disk_usage < 80 )); then
        echo -e "${GREEN}✅${NC} Disk Usage: ${disk_usage}%"
    elif (( disk_usage < 90 )); then
        echo -e "${YELLOW}⚠️${NC} Disk Usage: ${disk_usage}%"
    else
        echo -e "${RED}🚨${NC} Disk Usage: ${disk_usage}%"
    fi
fi

# Проверка доступности порта
echo -e "\n🔌 Port Check:"
if command -v netstat &> /dev/null; then
    if netstat -an | grep ":8001" | grep -i listen > /dev/null; then
        echo -e "${GREEN}✅${NC} Port 8001: Listening"
    else
        echo -e "${RED}❌${NC} Port 8001: Not Listening"
    fi
fi

# Проверка логов на ошибки
echo -e "\n📝 Log Analysis:"
if [ -f "logs/service.log" ]; then
    local error_count=$(tail -100 logs/service.log | grep -i "error\|exception\|traceback" | wc -l)
    if (( error_count == 0 )); then
        echo -e "${GREEN}✅${NC} No errors in last 100 lines"
    else
        echo -e "${YELLOW}⚠️${NC} ${error_count} errors in last 100 lines"
    fi
else
    echo -e "${YELLOW}⚠️${NC} No log file found"
fi

# Детальная проверка health endpoint
echo -e "\n🏥 Detailed Health Check:"
if curl -s "${BASE_URL}/health" | grep -q "healthy"; then
    echo -e "${GREEN}✅${NC} Service Health: Good"
else
    echo -e "${RED}❌${NC} Service Health: Issues detected"
fi

# Рекомендации
echo -e "\n💡 Recommendations:"
if ! pgrep -f "python.*main.py" > /dev/null; then
    echo -e "${YELLOW}⚠️${NC} Start ML Service: ./scripts/production_start.sh"
fi

if [ -f "logs/service.log" ] && tail -100 logs/service.log | grep -i "error\|exception\|traceback" > /dev/null; then
    echo -e "${YELLOW}⚠️${NC} Check logs: tail -f logs/service.log"
fi

echo -e "${GREEN}✅${NC} Run full test: python3 test_health_check.py"
echo -e "${GREEN}✅${NC} Load testing: python3 test_load_testing.py"

echo -e "\n📅 $TIMESTAMP"
echo "=" * 50