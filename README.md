# ML Service - Интеллектуальный анализатор данных

FastAPI сервис для автоматического анализа данных из различных источников (CSV, JSON, XML) и рекомендаций оптимального хранилища (PostgreSQL, ClickHouse, HDFS) с генерацией DDL-скриптов.

## 🏗️ Архитектура
```
ml_service/
├── main.py              # FastAPI приложение
├── models/              # Модели данных (Pydantic)
├── parsers/             # Парсеры форматов (CSV, JSON, XML)
├── analyzers/           # Анализ данных и система рекомендаций
├── generators/          # Генераторы DDL для разных СУБД
├── utils/               # Утилиты (кэш, валидаторы)
├── uploads/             # Временные файлы
├── requirements.txt     # Зависимости Python
└── .env.example         # Пример конфигурации
```

## ✨ Функциональность

### 1. Парсеры данных
- **CSV-парсер**: анализ структуры, типов данных, статистики
- **JSON-парсер**: обработка вложенных структур, массивов
- **XML-парсер**: извлечение данных из сложной иерархии

### 2. Система рекомендаций
- **Rule-based**: базовые правила для очевидных случаев
- **LLM-улучшения**: использование OpenAI API для генерации DDL
- **Поддерживаемые правила**:
  - Временные данные + большой объем → ClickHouse
  - Сложные связи + вложенные структуры → PostgreSQL
  - Большие объемы + архивация → HDFS

### 3. Генераторы DDL
- **PostgreSQL**: CREATE TABLE с оптимальными типами, индексами, JSONB
- **ClickHouse**: ENGINE = MergeTree, партицирование по дате
- **HDFS**: структура папок, рекомендации по формату (Parquet)

### 4. API Эндпоинты
- `GET /` - информация о сервисе
- `POST /analyze` - анализ данных, возвращает профиль
- `POST /recommend` - полная рекомендация по хранилищу
- `GET /health` - проверка состояния
- `GET /cache/stats` - статистика кэша
- `DELETE /cache/clear` - очистка кэша

## 🚀 Быстрый старт (Production)

### Требования
- Python 3.9+
- pip

### 1. Клонирование и установка
```bash
git clone <repository-url>
cd Project
./scripts/setup.sh
```

### 2. Конфигурация
```bash
# Отредактируйте файл с вашими настройками
nano ml_service/.env
```
Обязательно установите:
- `OPENAI_API_KEY` - ваш API ключ OpenAI

### 3. Запуск сервиса
```bash
./scripts/start.sh
```

### 4. Проверка работы
```bash
curl http://localhost:8000/health
```

API документация: http://localhost:8000/docs

## 🛠️ Ручная установка

1. **Создание виртуального окружения:**
```bash
python3 -m venv venv
source venv/bin/activate
```

2. **Установка зависимостей:**
```bash
cd ml_service
pip install -r requirements.txt
```

3. **Настройка окружения:**
```bash
cp .env.example .env
# Отредактируйте .env с вашими настройками
```

4. **Запуск:**
```bash
python run.py --host 0.0.0.0 --port 8000
```

## 📚 Примеры использования

### Анализ данных
```bash
curl -X POST "http://localhost:8000/analyze" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@data.csv" \
  -F "format=csv"
```

### Получение рекомендации
```bash
curl -X POST "http://localhost:8000/recommend" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@data.json" \
  -F "format=json" \
  -F "table_name=users"
```

## 📊 Формат ответа API

```json
{
  "target": "clickhouse|postgresql|hdfs",
  "confidence": 0.0-1.0,
  "rationale": "текстовое обоснование",
  "schedule_hint": "realtime|hourly|daily|weekly|monthly",
  "ddl_hints": ["hint1", "hint2"],
  "ddl_script": "CREATE TABLE ...",
  "data_profile": {
    "format": "csv|json|xml",
    "record_count": int,
    "field_count": int,
    "has_temporal": bool,
    "has_numeric": bool,
    "has_text": bool,
    "has_categorical": bool,
    "has_spatial": bool,
    "has_nested": bool,
    "unique_ids": ["field1", "field2"],
    "temporal_range": ["min_date", "max_date"],
    "estimated_size_mb": float
  }
}
```

## 🔧 Управление сервисом

- **Запуск:** `./scripts/start.sh`
- **Остановка:** `./scripts/stop.sh`
- **Проверка здоровья:** `curl http://localhost:8000/health`
- **Логи:** `tail -f logs/app.log`

## 📝 Переменные окружения

- `OPENAI_API_KEY` - API ключ OpenAI (обязательно)
- `ENABLE_LLM` - включение LLM улучшений (default: true)
- `MAX_TOKENS` - максимальное количество токенов (default: 2000)
- `CACHE_TTL` - время жизни кэша в секундах (default: 7200)
- `UPLOAD_MAX_SIZE_MB` - максимальный размер файла (default: 10000)
- `DEBUG` - режим отладки (default: false)

## 🔒 Безопасность

- Все чувствительные данные хранятся в переменных окружения
- Файлы загружаются во временную директорию и автоматически удаляются
- Валидация типов данных и размеров файлов
- Настройки CORS для production-окружения

## 🐛 Тестирование

Для запуска тестов:
```bash
cd ml_service
python -m pytest tests/
```

Для нагрузочного тестирования:
```bash
python test_load_testing.py
```