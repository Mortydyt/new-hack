from typing import Dict, Any, List
import pandas as pd
from .base_generator import BaseDDLGenerator


class PostgreSQLDDLGenerator(BaseDDLGenerator):
    def __init__(self):
        super().__init__()
        self.type_mappings = {
            'int64': 'BIGINT',
            'int32': 'INTEGER',
            'float64': 'DOUBLE PRECISION',
            'float32': 'REAL',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN',
            'object': 'TEXT'
        }

    def generate_ddl(self, table_name: str, df: pd.DataFrame, features: Dict[str, Any]) -> str:
        ddl_parts = [f"CREATE TABLE {table_name} ("]
        column_definitions = []
        constraints = []

        # Генерация определений колонок
        for col in df.columns:
            clean_name = self._clean_column_name(col)
            pandas_type = str(df[col].dtype)

            # Определение типа данных
            if features.get('has_nested', False) and ('json' in col.lower() or 'dict' in col.lower()):
                db_type = 'JSONB'
            elif features.get('has_spatial', False) and any(keyword in col.lower() for keyword in ['lat', 'lon', 'coord']):
                db_type = 'GEOMETRY(POINT, 4326)'
            else:
                db_type = self._map_pandas_type(pandas_type, col, features)

            nullable = 'NOT NULL' if df[col].isnull().sum() == 0 else ''
            column_definitions.append(f"    {clean_name} {db_type} {nullable}".strip())

        # Первичные ключи
        primary_keys = self._get_primary_keys(df, features)
        if primary_keys:
            clean_keys = [self._clean_column_name(pk) for pk in primary_keys]
            constraints.append(f"    PRIMARY KEY ({', '.join(clean_keys)})")

        # Сборка DDL
        ddl_parts.extend(column_definitions)
        if constraints:
            ddl_parts.extend(constraints)

        ddl_parts.append(");")

        # Добавление индексов
        indexes = self._generate_indexes(table_name, df, features)
        if indexes:
            ddl_parts.append("\n-- Индексы")
            ddl_parts.extend(indexes)

        # Добавление расширений для особых случаев
        extensions = self._generate_extensions(features)
        if extensions:
            ddl_parts.insert(0, extensions)

        return "\n".join(ddl_parts)

    def _map_pandas_type(self, pandas_type: str, col_name: str, features: Dict[str, Any]) -> str:
        """Преобразование pandas типа в PostgreSQL тип"""
        if 'int' in pandas_type:
            return 'BIGINT'
        elif 'float' in pandas_type:
            return 'DOUBLE PRECISION'
        elif 'datetime' in pandas_type:
            return 'TIMESTAMP'
        elif 'bool' in pandas_type:
            return 'BOOLEAN'
        elif features.get('has_categorical', False) and self._is_categorical(col_name):
            return 'VARCHAR(255)'
        else:
            return 'TEXT'

    def _is_categorical(self, col_name: str) -> bool:
        """Проверка, является ли колонка категориальной"""
        categorical_keywords = ['type', 'category', 'status', 'state', 'gender', 'country']
        return any(keyword in col_name.lower() for keyword in categorical_keywords)

    def _generate_indexes(self, table_name: str, df: pd.DataFrame, features: Dict[str, Any]) -> List[str]:
        """Генерация индексов"""
        indexes = []
        index_columns = self._suggest_indexes(df, features)

        for col in index_columns:
            clean_name = self._clean_column_name(col)
            if features.get('has_spatial', False) and any(keyword in col.lower() for keyword in ['lat', 'lon', 'coord']):
                indexes.append(f"CREATE INDEX idx_{table_name}_{clean_name} ON {table_name} USING GIST ({clean_name});")
            else:
                indexes.append(f"CREATE INDEX idx_{table_name}_{clean_name} ON {table_name} ({clean_name});")

        # GIN индекс для JSONB полей
        if features.get('has_nested', False):
            for col in df.columns:
                if 'json' in col.lower() or 'dict' in col.lower():
                    clean_name = self._clean_column_name(col)
                    indexes.append(f"CREATE INDEX idx_{table_name}_{clean_name}_gin ON {table_name} USING GIN ({clean_name});")

        return indexes

    def _generate_extensions(self, features: Dict[str, Any]) -> str:
        """Генерация расширений PostgreSQL"""
        extensions = []

        if features.get('has_spatial', False):
            extensions.append("CREATE EXTENSION IF NOT EXISTS postgis;")

        if features.get('has_nested', False):
            extensions.append("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

        if extensions:
            return "-- Расширения\n" + "\n".join(extensions) + "\n"
        return ""