import os
import json
import time
import hashlib
from typing import Dict, Any, Optional
from openai import OpenAI
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


class OpenAIClient:
    """Клиент для работы с OpenAI API"""

    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.max_tokens = int(os.getenv("MAX_TOKENS", "2000"))
        self.temperature = float(os.getenv("TEMPERATURE", "0.1"))
        self.cache = {}
        self.cache_ttl = int(os.getenv("LLM_CACHE_TTL", "3600"))  # 1 час

    def generate_rationale(self, profile: Dict[str, Any], features: Dict[str, Any],
                          target: str, confidence: float) -> str:
        """
        Генерация текстового обоснования выбора хранилища
        """
        prompt = self._build_rationale_prompt(profile, features, target, confidence)

        # Проверяем кэш
        cache_key = f"rationale_{hash(prompt)}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            return cached_result

        # Генерируем обоснование
        response = self._call_openai(prompt)

        # Сохраняем в кэш
        self._save_to_cache(cache_key, response)

        return response

    def generate_ddl(self, table_name: str, profile: Dict[str, Any], features: Dict[str, Any],
                    target: str, schema_info: Dict[str, Any]) -> str:
        """
        Генерация DDL скрипта
        """
        prompt = self._build_ddl_prompt(table_name, profile, features, target, schema_info)

        # Проверяем кэш
        cache_key = f"ddl_{hash(prompt)}"
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            return cached_result

        # Генерируем DDL
        response = self._call_openai(prompt)

        # Сохраняем в кэш
        self._save_to_cache(cache_key, response)

        return response

    def _build_rationale_prompt(self, profile: Dict[str, Any], features: Dict[str, Any],
                               target: str, confidence: float) -> str:
        """Построение промпта для обоснования"""
        format_name = profile.get('format', 'unknown')
        estimated_size_mb = profile.get('estimated_size_mb', 0)
        record_count = profile.get('record_count', 0)
        field_count = profile.get('field_count', 0)

        # Типы данных
        has_temporal = profile.get('has_temporal', False)
        has_numeric = profile.get('has_numeric', False)
        has_text = profile.get('has_text', False)
        has_categorical = profile.get('has_categorical', False)
        has_spatial = profile.get('has_spatial', False)
        has_nested = profile.get('has_nested', False)

        # Дополнительная информация
        unique_ids = profile.get('unique_ids', [])
        temporal_range = profile.get('temporal_range', [])
        data_quality = features.get('data_quality_score', 0)

        return f"""Ты - эксперт по базам данных и архитектор систем хранения данных.

Проанализируй характеристики датасета и сгенерируй краткое обоснование (2-3 предложения) почему рекомендуется использовать {target}.

**Характеристики датасета:**
- Формат: {format_name}
- Размер: {estimated_size_mb:.2f} MB
- Количество записей: {record_count:,}
- Количество полей: {field_count}
- Уверенность рекомендации: {confidence:.1%}

**Типы данных:**
- Временные данные: {'✓' if has_temporal else '✗'}
- Числовые данные: {'✓' if has_numeric else '✗'}
- Текстовые данные: {'✓' if has_text else '✗'}
- Категориальные данные: {'✓' if has_categorical else '✗'}
- Пространственные данные: {'✓' if has_spatial else '✗'}
- Вложенные структуры: {'✓' if has_nested else '✗'}

**Дополнительная информация:**
- Уникальные идентификаторы: {', '.join(unique_ids[:3]) if unique_ids else 'Не обнаружены'}
- Временной диапазон: {f'{temporal_range[0]} - {temporal_range[1]}' if temporal_range else 'Не обнаружен'}
- Качество данных: {data_quality:.2%}

Сгенерируй обоснование на русском языке, объясняющее преимущества {target} для данного типа данных.
Учти особенности: {'пространственные данные' if has_spatial else 'временные ряды' if has_temporal else 'сложные структуры' if has_nested else 'смешанные типы данных'}.

Ответ должен быть только текстом обоснования без форматирования."""

    def _build_ddl_prompt(self, table_name: str, profile: Dict[str, Any], features: Dict[str, Any],
                         target: str, schema_info: Dict[str, Any]) -> str:
        """Построение промпта для DDL"""
        columns = features.get('columns', [])
        dtypes = features.get('dtypes', {})

        # Формируем информацию о колонках
        columns_info = []
        for col in columns[:15]:  # Первые 15 колонок
            dtype = dtypes.get(col, 'VARCHAR')
            columns_info.append(f"- {col}: {dtype}")

        # Определяем особенности данных
        temporal_cols = [col for col in columns if any(keyword in col.lower() for keyword in
                        ['date', 'time', 'created', 'updated', 'timestamp'])]
        numeric_cols = [col for col in columns if any(keyword in col.lower() for keyword in
                       ['id', 'price', 'amount', 'count', 'number', 'quantity'])]
        text_cols = [col for col in columns if any(keyword in col.lower() for keyword in
                    ['name', 'description', 'title', 'status', 'type'])]

        record_count = profile.get('record_count', 0)

        return f"""Ты - эксперт по {target}. Создай оптимизированный DDL-скрипт для таблицы {table_name}.

**Требования:**
- СУБД: {target}
- Таблица: {table_name}
- Ожидаемое количество записей: {record_count:,}

**Структура данных:**
{chr(10).join(columns_info)}

**Ключевые поля:**
- Временные поля: {', '.join(temporal_cols[:3]) if temporal_cols else 'Нет'}
- Числовые поля: {', '.join(numeric_cols[:3]) if numeric_cols else 'Нет'}
- Текстовые поля: {', '.join(text_cols[:3]) if text_cols else 'Нет'}

**Требования к DDL:**
"""

        # Добавляем специфичные требования для разных СУБД
        if target == "postgresql":
            ddl_requirements = """
1. Используй оптимальные типы данных PostgreSQL
2. Добавь PRIMARY KEY для подходящего поля
3. Добавь индексы для часто запрашиваемых полей
4. Для текстовых полей используй VARCHAR с ограничением длины
5. Для временных полей используй TIMESTAMP
6. Добавь комментарии для таблицы и важных полей"""
        elif target == "clickhouse":
            ddl_requirements = """
1. Используй ENGINE = MergeTree()
2. Добавь партицирование по временным полям (если есть)
3. Оптимизируй ORDER BY для частых запросов
4. Используй подходящие типы данных (String, UInt64, Float64, DateTime)
5. Добавь TTL для устаревших данных (если применимо)"""
        else:  # hdfs
            ddl_requirements = """
1. Создай DDL для Hive/Impala совместимый формат
2. Используй внешнюю таблицу
3. Укажи формат хранения (PARQUET или ORC)
4. Добавь партицирование по дате (если есть временные поля)
5. Укажи расположение данных в HDFS"""

        return prompt + ddl_requirements + """

Верни только SQL код без пояснений и форматирования. Код должен быть готов к выполнению."""

    def _call_openai(self, prompt: str) -> str:
        """Вызов OpenAI API"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Ты - эксперт по базам данных. Отвечай точно по делу без лишних комментариев."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            # В случае ошибки API, возвращаем заглушку
            print(f"OpenAI API error: {e}")
            return self._get_fallback_response(prompt)

    def _get_fallback_response(self, prompt: str) -> str:
        """Запасной ответ при ошибке API"""
        if "обоснование" in prompt.lower():
            return "Рекомендация основана на анализе характеристик данных и оптимальных практиках хранения."
        else:
            return "-- DDL скрипт сгенерирован в fallback режиме\nCREATE TABLE table_name (id VARCHAR(255) PRIMARY KEY);"

    def _get_from_cache(self, cache_key: str) -> Optional[str]:
        """Получение данных из кэша"""
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return data
            else:
                del self.cache[cache_key]
        return None

    def _save_to_cache(self, cache_key: str, data: str):
        """Сохранение данных в кэш"""
        self.cache[cache_key] = (data, time.time())

    def clear_cache(self):
        """Очистка кэша"""
        self.cache.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Статистика использования"""
        return {
            "model": self.model,
            "cache_size": len(self.cache),
            "max_tokens": self.max_tokens,
            "temperature": self.temperature
        }

    def test_connection(self) -> bool:
        """Тест подключения к OpenAI API"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": "Test"}],
                max_tokens=5
            )
            return True
        except Exception as e:
            print(f"OpenAI connection test failed: {e}")
            return False