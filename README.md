# ML-компонент интеллектуального инженера данных

## Описание
FastAPI сервис для автоматического анализа данных из различных источников (CSV, JSON, XML) и рекомендаций оптимального хранилища (PostgreSQL, ClickHouse, HDFS) с генерацией DDL-скриптов.

## Архитектура
```
ml_service/
├── main.py              # FastAPI приложение
├── models/              # Модели данных (Pydantic)
├── parsers/             # Парсеры форматов (CSV, JSON, XML)
├── analyzers/           # Анализ данных и система рекомендаций
├── generators/          # Генераторы DDL для разных СУБД
├── uploads/             # Временные файлы
└── requirements.txt     # Зависимости
```

## Функциональность

### 1. Парсеры данных
- **CSV-парсер**: анализ структуры, типов данных, статистики
- **JSON-парсер**: обработка вложенных структур, массивов
- **XML-парсер**: извлечение данных из сложной иерархии

### 2. Система рекомендаций
- **Rule-based**: базовые правила для очевидных случаев
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

## Установка и запуск

1. Установка зависимостей:
```bash
cd ml_service
pip install -r requirements.txt
```

2. Запуск сервиса:
```bash
python main.py
# или с параметрами
python run.py --host 0.0.0.0 --port 8000 --reload
```

3. API документация доступна по адресу: http://localhost:8000/docs

## Пример использования

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

## Формат ответа API

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