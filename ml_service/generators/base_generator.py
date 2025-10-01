from abc import ABC, abstractmethod
from typing import Dict, Any, List
import pandas as pd


class BaseDDLGenerator(ABC):
    def __init__(self):
        self.type_mappings = {}
        self.index_suggestions = []

    @abstractmethod
    def generate_ddl(self, table_name: str, df: pd.DataFrame, features: Dict[str, Any]) -> str:
        pass

    def _map_pandas_to_db_type(self, dtype: str, col_name: str) -> str:
        """Преобразование pandas dtype в тип данных базы данных"""
        if 'int' in str(dtype):
            return 'BIGINT'
        elif 'float' in str(dtype):
            return 'DOUBLE PRECISION'
        elif 'datetime' in str(dtype):
            return 'TIMESTAMP'
        elif 'bool' in str(dtype):
            return 'BOOLEAN'
        else:
            return 'TEXT'

    def _get_primary_keys(self, df: pd.DataFrame, features: Dict[str, Any]) -> List[str]:
        """Определение первичных ключей"""
        primary_keys = []

        # Используем поля, определенные как уникальные идентификаторы
        if 'unique_ids' in features and features['unique_ids']:
            primary_keys.extend(features['unique_ids'])

        # Если нет уникальных идентификаторов, ищем поля с уникальными значениями
        if not primary_keys:
            for col in df.columns:
                if df[col].nunique() == len(df):
                    primary_keys.append(col)

        return primary_keys

    def _suggest_indexes(self, df: pd.DataFrame, features: Dict[str, Any]) -> List[str]:
        """Генерация рекомендаций по индексам"""
        indexes = []

        # Индексы для временных полей
        if features.get('has_temporal', False):
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    indexes.append(col)

        # Индексы для частых фильтров (кардинальность 10-90%)
        for col in df.columns:
            unique_ratio = df[col].nunique() / len(df)
            if 0.1 <= unique_ratio <= 0.9:
                indexes.append(col)

        return indexes

    def _clean_column_name(self, col_name: str) -> str:
        """Очистка имени колонки для SQL"""
        # Заменяем спецсимволы и пробелы
        cleaned = col_name.replace(' ', '_').replace('-', '_').replace('.', '_')
        # Удаляем множественные подчеркивания
        cleaned = '_'.join([part for part in cleaned.split('_') if part])
        return cleaned.lower()