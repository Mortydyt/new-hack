import os
from typing import Dict, Any, List, Optional
from ..models.schemas import StorageType, ScheduleHint, DataProfile
from .openai_client import OpenAIClient


class RuleEngine:
    def __init__(self):
        self.rules = self._define_rules()
        self.openai_client = None
        self.enable_llm = os.getenv("ENABLE_LLM", "false").lower() == "true"

        # Инициализируем OpenAI клиент если включен LLM
        if self.enable_llm:
            try:
                self.openai_client = OpenAIClient()
                if not self.openai_client.test_connection():
                    print("OpenAI connection failed, falling back to rule-based only")
                    self.openai_client = None
            except Exception as e:
                print(f"Failed to initialize OpenAI client: {e}")
                self.openai_client = None

    def _define_rules(self) -> List[Dict[str, Any]]:
        return [
            {
                'name': 'very_large_temporal_data',
                'conditions': {
                    'has_temporal': True,
                    'estimated_size_mb': {'operator': '>', 'value': 500},
                    'record_count': {'operator': '>', 'value': 1000000}
                },
                'recommendation': {
                    'target': StorageType.CLICKHOUSE,
                    'confidence': 0.95,
                    'schedule_hint': ScheduleHint.HOURLY,
                    'rationale': 'Очень большие объемы временных данных (более 500MB) требуют высокой производительности аналитических запросов. ClickHouse оптимизирован для временных рядов и обеспечивает быструю агрегацию.',
                    'ddl_hints': [
                        'Использовать MergeTree с партицированием по дате/месяцу',
                        'Оптимизировать ORDER BY по временным полям и ID',
                        'Использовать сжатие данных',
                        'Настроить TTL для автоматической очистки устаревших данных'
                    ]
                }
            },
            {
                'name': 'large_temporal_data',
                'conditions': {
                    'has_temporal': True,
                    'estimated_size_mb': {'operator': '>', 'value': 100},
                    'record_count': {'operator': '>', 'value': 100000}
                },
                'recommendation': {
                    'target': StorageType.CLICKHOUSE,
                    'confidence': 0.9,
                    'schedule_hint': ScheduleHint.DAILY,
                    'rationale': 'Большие объемы временных данных идеально подходят для ClickHouse с его оптимизацией для аналитических запросов и сжатия данных.',
                    'ddl_hints': [
                        'Использовать партицирование по дате',
                        'Оптимизировать ORDER BY для частых запросов',
                        'Использовать MergeTree engine'
                    ]
                }
            },
            {
                'name': 'cadastral_spatial_data',
                'conditions': {
                    'has_spatial': True,
                    'has_nested': True,
                    'field_count': {'operator': '>', 'value': 15}
                },
                'recommendation': {
                    'target': StorageType.POSTGRESQL,
                    'confidence': 0.92,
                    'schedule_hint': ScheduleHint.REALTIME,
                    'rationale': 'Кадастровые данные со сложной пространственной структурой требуют PostgreSQL с PostGIS для геопространственных запросов и поддержки вложенных атрибутов.',
                    'ddl_hints': [
                        'Установить PostGIS расширение',
                        'Создать пространственные индексы (GiST)',
                        'Использовать JSONB для вложенных атрибутов объекта',
                        'Оптимизировать запросы по кадастровым номерам'
                    ]
                }
            },
            {
                'name': 'complex_nested_structures',
                'conditions': {
                    'has_nested': True,
                    'has_text': True,
                    'field_count': {'operator': '>', 'value': 20}
                },
                'recommendation': {
                    'target': StorageType.POSTGRESQL,
                    'confidence': 0.85,
                    'schedule_hint': ScheduleHint.HOURLY,
                    'rationale': 'Сложные вложенные структуры и текстовые данные требуют реляционной базы данных с поддержкой JSONB и полнотекстового поиска.',
                    'ddl_hints': [
                        'Использовать JSONB для вложенных структур',
                        'Добавить GIN индексы для полнотекстового поиска',
                        'Оптимизировать связи между таблицами'
                    ]
                }
            },
            {
                'name': 'business_entities_data',
                'conditions': {
                    'has_categorical': True,
                    'has_text': True,
                    'estimated_size_mb': {'operator': '>', 'value': 50},
                    'field_count': {'operator': '>', 'value': 10}
                },
                'recommendation': {
                    'target': StorageType.POSTGRESQL,
                    'confidence': 0.8,
                    'schedule_hint': ScheduleHint.DAILY,
                    'rationale': 'Данные бизнес-сущностей (ИП, организации) с текстовыми полями и категориями лучше хранить в PostgreSQL для обеспечения целостности данных и поддержки сложных запросов.',
                    'ddl_hints': [
                        'Использовать ограничения целостности (constraints)',
                        'Добавить уникальные индексы для ИНН/ОГРН',
                        'Оптимизировать текстовые поля для поиска',
                        'Использовать внешние ключи для связей'
                    ]
                }
            },
            {
                'name': 'massive_archive_data',
                'conditions': {
                    'estimated_size_mb': {'operator': '>', 'value': 5000},
                    'record_count': {'operator': '>', 'value': 10000000}
                },
                'recommendation': {
                    'target': StorageType.HDFS,
                    'confidence': 0.9,
                    'schedule_hint': ScheduleHint.WEEKLY,
                    'rationale': 'Массивные наборы данных (более 5GB) для долгосрочного архивирования требуют распределенной файловой системы HDFS с ее масштабируемостью и отказоустойчивостью.',
                    'ddl_hints': [
                        'Использовать Parquet или ORC формат',
                        'Организовать партицирование по году/месяцу',
                        'Настроить сжатие Snappy или ZSTD',
                        'Реализовать разделение на hot/cold данные'
                    ]
                }
            },
            {
                'name': 'large_archive_data',
                'conditions': {
                    'estimated_size_mb': {'operator': '>', 'value': 1000},
                    'record_count': {'operator': '>', 'value': 1000000}
                },
                'recommendation': {
                    'target': StorageType.HDFS,
                    'confidence': 0.8,
                    'schedule_hint': ScheduleHint.WEEKLY,
                    'rationale': 'Большие объемы данных для долгосрочного хранения лучше всего подходят для HDFS с его масштабируемостью и отказоустойчивостью.',
                    'ddl_hints': [
                        'Использовать Parquet формат',
                        'Организовать партицирование по дате',
                        'Настроить сжатие данных'
                    ]
                }
            },
            {
                'name': 'small_dataset',
                'conditions': {
                    'estimated_size_mb': {'operator': '<', 'value': 10},
                    'record_count': {'operator': '<', 'value': 10000}
                },
                'recommendation': {
                    'target': StorageType.POSTGRESQL,
                    'confidence': 0.7,
                    'schedule_hint': ScheduleHint.DAILY,
                    'rationale': 'Небольшие наборы данных эффективнее всего хранить в PostgreSQL из-за простоты управления и универсальности.',
                    'ddl_hints': [
                        'Оптимизировать типы данных',
                        'Добавить необходимые индексы',
                        'Использовать схему по умолчанию'
                    ]
                }
            },
            {
                'name': 'medium_mixed_data',
                'conditions': {
                    'estimated_size_mb': {'operator': '>=', 'value': 10},
                    'estimated_size_mb': {'operator': '<=', 'value': 100},
                    'record_count': {'operator': '>=', 'value': 10000},
                    'record_count': {'operator': '<=', 'value': 100000}
                },
                'recommendation': {
                    'target': StorageType.POSTGRESQL,
                    'confidence': 0.75,
                    'schedule_hint': ScheduleHint.DAILY,
                    'rationale': 'Данные среднего размера со смешанными типами хорошо подходят для PostgreSQL, который обеспечивает баланс между производительностью и функциональностью.',
                    'ddl_hints': [
                        'Оптимизировать типы данных для экономии места',
                        'Добавить составные индексы для частых запросов',
                        'Рассмотреть материализованные представления'
                    ]
                }
            },
            {
                'name': 'default_recommendation',
                'conditions': {},
                'recommendation': {
                    'target': StorageType.POSTGRESQL,
                    'confidence': 0.6,
                    'schedule_hint': ScheduleHint.DAILY,
                    'rationale': 'PostgreSQL является универсальным решением для большинства типов данных с хорошим балансом производительности и функциональности.',
                    'ddl_hints': [
                        'Проанализировать структуру данных',
                        'Оптимизировать типы данных',
                        'Добавить базовые индексы'
                    ]
                }
            }
        ]

    def evaluate_conditions(self, conditions: Dict[str, Any], profile: DataProfile) -> bool:
        for key, condition in conditions.items():
            if key == 'has_temporal':
                if profile.has_temporal != condition:
                    return False
            elif key == 'has_numeric':
                if profile.has_numeric != condition:
                    return False
            elif key == 'has_text':
                if profile.has_text != condition:
                    return False
            elif key == 'has_categorical':
                if profile.has_categorical != condition:
                    return False
            elif key == 'has_spatial':
                if profile.has_spatial != condition:
                    return False
            elif key == 'has_nested':
                if profile.has_nested != condition:
                    return False
            elif key == 'estimated_size_mb':
                operator = condition['operator']
                value = condition['value']
                if operator == '>' and profile.estimated_size_mb <= value:
                    return False
                elif operator == '<' and profile.estimated_size_mb >= value:
                    return False
            elif key == 'record_count':
                operator = condition['operator']
                value = condition['value']
                if operator == '>' and profile.record_count <= value:
                    return False
                elif operator == '<' and profile.record_count >= value:
                    return False
            elif key == 'field_count':
                operator = condition['operator']
                value = condition['value']
                if operator == '>' and profile.field_count <= value:
                    return False
                elif operator == '<' and profile.field_count >= value:
                    return False
                elif operator == '>=' and profile.field_count < value:
                    return False
                elif operator == '<=' and profile.field_count > value:
                    return False

        return True

    def get_recommendation(self, profile: DataProfile, features: Dict[str, Any]) -> Dict[str, Any]:
        best_match = None
        highest_priority = 0

        for rule in self.rules:
            if self.evaluate_conditions(rule['conditions'], profile):
                # Приоритет основан на количестве выполненных условий
                priority = len(rule['conditions'])
                if priority > highest_priority:
                    highest_priority = priority
                    best_match = rule

        if best_match:
            recommendation = best_match['recommendation'].copy()
        else:
            # Возвращаем рекомендацию по умолчанию
            recommendation = self.rules[-1]['recommendation'].copy()

        # Улучшаем обоснование с помощью LLM если доступно
        if self.openai_client and recommendation.get('confidence', 0) > 0.5:
            try:
                # Конвертируем профиль в словарь для LLM
                profile_dict = profile.dict()
                target = recommendation['target'].value
                confidence = recommendation['confidence']

                # Генерируем улучшенное обоснование
                enhanced_rationale = self.openai_client.generate_rationale(
                    profile_dict, features, target, confidence
                )
                recommendation['rationale'] = enhanced_rationale
                recommendation['llm_enhanced'] = True
            except Exception as e:
                print(f"LLM rationale generation failed: {e}")
                # Оставляем оригинальное обоснование
                recommendation['llm_enhanced'] = False

        return recommendation

    def generate_enhanced_ddl(self, table_name: str, profile: DataProfile, features: Dict[str, Any],
                              target: StorageType, schema_info: Dict[str, Any]) -> Optional[str]:
        """
        Генерация улучшенного DDL с помощью LLM
        """
        if not self.openai_client:
            return None

        try:
            profile_dict = profile.dict()
            return self.openai_client.generate_ddl(
                table_name, profile_dict, features, target.value, schema_info
            )
        except Exception as e:
            print(f"LLM DDL generation failed: {e}")
            return None