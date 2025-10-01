from typing import Dict, Any, List
import pandas as pd
from .base_generator import BaseDDLGenerator


class ClickHouseDDLGenerator(BaseDDLGenerator):
    def __init__(self):
        super().__init__()
        self.type_mappings = {
            'int64': 'Int64',
            'int32': 'Int32',
            'float64': 'Float64',
            'float32': 'Float32',
            'datetime64[ns]': 'DateTime',
            'bool': 'UInt8',
            'object': 'String'
        }

    def generate_ddl(self, table_name: str, df: pd.DataFrame, features: Dict[str, Any]) -> str:
        ddl_parts = [f"CREATE TABLE {table_name} ("]
        column_definitions = []
        engine_settings = []

        # Генерация определений колонок
        for col in df.columns:
            clean_name = self._clean_column_name(col)
            pandas_type = str(df[col].dtype)
            db_type = self._map_pandas_type(pandas_type, col)

            # Добавление кодировок для строковых полей
            if db_type == 'String':
                if features.get('has_categorical', False) and self._is_low_cardinality(df[col]):
                    db_type = f'LowCardinality({db_type})'
                else:
                    db_type = f'{db_type}'

            column_definitions.append(f"    {clean_name} {db_type}")

        # Определение партиционирования и сортировки
        temporal_col = self._find_temporal_column(df, features)
        if temporal_col:
            clean_temporal = self._clean_column_name(temporal_col)
            engine_settings.append(f"PARTITION BY toYYYYMM({clean_temporal})")
            engine_settings.append(f"ORDER BY ({clean_temporal}")

            # Добавление ключей для уникальных идентификаторов
            unique_ids = features.get('unique_ids', [])
            if unique_ids:
                clean_ids = [self._clean_column_name(uid) for uid in unique_ids[:3]]  # Максимум 3
                engine_settings[1] += f", {', '.join(clean_ids)})"
            else:
                engine_settings[1] += ")"

        # Сборка DDL
        ddl_parts.extend(column_definitions)
        ddl_parts.append(") ENGINE = MergeTree()")

        if engine_settings:
            ddl_parts.extend(engine_settings)

        ddl_parts.append(";")

        # Добавление материализованных представлений для оптимизации
        materialized_views = self._generate_materialized_views(table_name, df, features)
        if materialized_views:
            ddl_parts.append("\n-- Материализованные представления")
            ddl_parts.extend(materialized_views)

        return "\n".join(ddl_parts)

    def _map_pandas_type(self, pandas_type: str, col_name: str) -> str:
        """Преобразование pandas типа в ClickHouse тип"""
        if 'int' in pandas_type:
            return 'Int64'
        elif 'float' in pandas_type:
            return 'Float64'
        elif 'datetime' in pandas_type:
            return 'DateTime'
        elif 'bool' in pandas_type:
            return 'UInt8'
        elif features.get('has_nested', False) and ('json' in col_name.lower() or 'dict' in col_name.lower()):
            return 'String'  # JSON как строка или использовать специальный тип
        else:
            return 'String'

    def _find_temporal_column(self, df: pd.DataFrame, features: Dict[str, Any]) -> str:
        """Поиск временной колонки для партиционирования"""
        if not features.get('has_temporal', False):
            return None

        temporal_keywords = ['date', 'time', 'created', 'updated', 'timestamp']
        for col in df.columns:
            if any(keyword in col.lower() for keyword in temporal_keywords):
                return col

        # Если не найдено по ключевым словам, ищем по типу
        for col in df.columns:
            if 'datetime' in str(df[col].dtype):
                return col

        return None

    def _is_low_cardinality(self, series: pd.Series) -> bool:
        """Проверка, является ли колонка низкой кардинальности"""
        unique_ratio = series.nunique() / len(series)
        return unique_ratio < 0.1  # Менее 10% уникальных значений

    def _generate_materialized_views(self, table_name: str, df: pd.DataFrame, features: Dict[str, Any]) -> List[str]:
        """Генерация материализованных представлений"""
        views = []

        # Агрегация по временным периодам
        if features.get('has_temporal', False):
            temporal_col = self._find_temporal_column(df, features)
            if temporal_col:
                clean_temporal = self._clean_column_name(temporal_col)
                numeric_cols = [col for col in df.columns if 'int' in str(df[col].dtype) or 'float' in str(df[col].dtype)]

                if numeric_cols:
                    view_name = f"{table_name}_daily_stats"
                    view_ddl = f"""CREATE MATERIALIZED VIEW {view_name}
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date)
AS SELECT
    toDate({clean_temporal}) as date,
    {', '.join([f'sum({self._clean_column_name(col)}) as {self._clean_column_name(col)}_sum' for col in numeric_cols[:3]])}
FROM {table_name}
GROUP BY toDate({clean_temporal});"""
                    views.append(view_ddl)

        return views