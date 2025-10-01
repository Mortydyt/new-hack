# 🚀 РУКОВОДСТВО ПО ТЕСТИРОВАНИЮ В РЕАЛЬНЫХ УСЛОВИЯХ

## 📋 Подготовка окружения

### 1. Проверка состояния сервиса
```bash
# Проверка доступности сервиса
curl http://localhost:8001/

# Проверка здоровья
curl http://localhost:8001/health

# Документация API
open http://localhost:8001/docs
```

### 2. Мониторинг ресурсов
```bash
# Мониторинг CPU и памяти
top -p $(pgrep -f "python.*main.py")

# Проверка использования диска
df -h

# Мониторинг сети
netstat -an | grep 8001
```

## 🎯 Production тестирование

### Базовое тестирование
```bash
# Запуск базовых тестов
python3 test_real_data_simple.py
```

### Нагрузочное тестирование
```bash
# Запуск нагрузочного тестирования
python3 test_load_testing.py
```

### Полное тестирование на реальных данных
```bash
# Комплексное тестирование
python3 test_full_production.py
```

## 📊 Метрики для мониторинга

### 1. Производительность
- Время ответа API
- Использование памяти
- CPU нагрузка
- Скорость обработки файлов

### 2. Качество рекомендаций
- Уверенность в рекомендациях
- Соответствие рекомендаций типам данных
- Покрытие разных форматов данных

### 3. Надежность
- Стабильность работы под нагрузкой
- Обработка ошибок
- Восстановление после сбоев

## 🔧 Настройки для production

### Конфигурация ML сервиса
```bash
# Увеличение лимитов для больших файлов
export ML_SERVICE_MAX_FILE_SIZE=104857600  # 100MB
export ML_SERVICE_TIMEOUT=300              # 5 минут
export ML_SERVICE_CACHE_TTL=3600           # 1 час
```

### Оптимизация базы данных
```sql
-- Оптимизация PostgreSQL для больших данных
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET work_mem = '16MB';
```

## 🚨 Тревожные ситуации

### 1. Сервис не отвечает
```bash
# Проверка статуса процесса
ps aux | grep python

# Перезапуск сервиса
cd ml_service && pkill -f "python.*main.py" && nohup python3 main.py > service.log 2>&1 &
```

### 2. Ошибки памяти
```bash
# Проверка использования памяти
free -h

# Очистка кэша
rm -rf cache/* ml_service/cache/* uploads/*

# Перезапуск с увеличенной памятью
export PYTHON_MEMORY_LIMIT=2G
cd ml_service && python3 main.py
```

### 3. Проблемы с большими файлами
```bash
# Проверка размера файлов
ls -lh datasets/

# Тестирование с меньшим сэмплом
python3 test_large_files.py --sample-size 1000
```

## 📈 Отчетность

### Создание отчета о тестировании
```bash
# Генерация отчета
python3 generate_test_report.py > test_report.html

# Просмотр отчета
open test_report.html
```

### Ключевые метрики для отчета
- Общее время тестирования
- Количество успешных/неуспешных запросов
- Средняя уверенность рекомендаций
- Рекомендации по оптимизации

## 🔄 Регулярное тестирование

### Cron задачи для мониторинга
```bash
# Добавить в crontab -e
0 */6 * * * cd /path/to/project && python3 test_health_check.py
0 2 * * 0 cd /path/to/project && python3 test_full_production.py | mail -s "Weekly Test Report" admin@example.com
```

### Автоматические проверки
```bash
# Скрипт для автоматической проверки
./scripts/health_check.sh
```

## 🌐 Внешнее тестирование

### Тестирование из других сетей
```bash
# Если сервис доступен извне
curl http://your-server:8001/

# Или через туннель
ssh -L 8001:localhost:8001 user@server
```

### Интеграция с CI/CD
```yaml
# GitHub Actions пример
name: Production Test
on: [push]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run production tests
        run: |
          python3 test_full_production.py
```

## 📞 Поддержка

### Логи и отладка
```bash
# Просмотр логов
tail -f service.log

# Логи с детальной информацией
export PYTHONPATH=. python3 -m ml_service.main --debug
```

### Контакт для проблем
- Проверить документацию: `open docs/`
- Создать issue в репозитории
- Проверить статус: `curl http://localhost:8001/health`

---

**Следуйте этому руководству для комплексного тестирования вашего ML сервиса в реальных условиях!** 🚀