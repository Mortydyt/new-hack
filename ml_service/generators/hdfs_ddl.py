from typing import Dict, Any, List
import pandas as pd
from .base_generator import BaseDDLGenerator


class HDFSDDLGenerator(BaseDDLGenerator):
    def __init__(self):
        super().__init__()

    def generate_ddl(self, table_name: str, df: pd.DataFrame, features: Dict[str, Any]) -> str:
        """Генерация рекомендаций по структуре HDFS"""
        ddl_parts = [f"-- Рекомендации по структуре HDFS для таблицы {table_name}"]
        ddl_parts.append("--")

        # Рекомендации по формату
        format_recommendations = self._generate_format_recommendations(features)
        ddl_parts.extend(format_recommendations)

        # Рекомендации по структуре папок
        structure_recommendations = self._generate_structure_recommendations(table_name, features)
        ddl_parts.extend(structure_recommendations)

        # Рекомендации по партицированию
        partitioning_recommendations = self._generate_partitioning_recommendations(df, features)
        ddl_parts.extend(partitioning_recommendations)

        # Рекомендации по сжатию
        compression_recommendations = self._generate_compression_recommendations(features)
        ddl_parts.extend(compression_recommendations)

        # Hive DDL для совместимости
        hive_ddl = self._generate_hive_ddl(table_name, df, features)
        if hive_ddl:
            ddl_parts.append("\n-- Hive DDL для метаданных")
            ddl_parts.append(hive_ddl)

        return "\n".join(ddl_parts)

    def _generate_format_recommendations(self, features: Dict[str, Any]) -> List[str]:
        """Генерация рекомендаций по формату файлов"""
        recommendations = ["-- Формат данных:"]

        if features.get('has_nested', False):
            recommendations.append("-- Рекомендуемый формат: Parquet (поддержка вложенных структур)")
        elif features.get('has_temporal', False):
            recommendations.append("-- Рекомендуемый формат: ORC (оптимизация для временных данных)")
        else:
            recommendations.append("-- Рекомендуемый формат: Parquet (хорошее сжатие и производительность)")

        return recommendations

    def _generate_structure_recommendations(self, table_name: str, features: Dict[str, Any]) -> List[str]:
        """Генерация рекомендаций по структуре папок"""
        recommendations = ["-- Структура папок HDFS:"]

        base_path = f"/data/{table_name}"
        recommendations.append(f"-- Базовый путь: {base_path}")

        if features.get('has_temporal', False):
            recommendations.append("-- Структура с партицированием по дате:")
            recommendations.append(f"-- {base_path}/year={{YYYY}}/month={{MM}}/day={{DD}}/")
        else:
            recommendations.append("-- Простая структура:")
            recommendations.append(f"-- {base_path}/data/")

        return recommendations

    def _generate_partitioning_recommendations(self, df: pd.DataFrame, features: Dict[str, Any]) -> List[str]:
        """Генерация рекомендаций по партицированию"""
        recommendations = ["-- Партицирование:"]

        if features.get('has_temporal', False):
            temporal_col = self._find_temporal_column(df)
            if temporal_col:
                recommendations.append(f"-- Партицирование по: {temporal_col}")
                recommendations.append("-- Размер партиции: 128-512 MB")
                recommendations.append("-- Частота обновления: ежедневно")
        elif features.get('estimated_size_mb', 0) > 1000:
            recommendations.append("-- Партицирование по хешу ключевых полей")
            recommendations.append("-- Количество партиций: 10-20")

        return recommendations

    def _generate_compression_recommendations(self, features: Dict[str, Any]) -> List[str]:
        """Генерация рекомендаций по сжатию"""
        recommendations = ["-- Сжатие данных:"]

        if features.get('has_numeric', False) and not features.get('has_text', False):
            recommendations.append("-- Рекомендуемый алгоритм: Snappy (быстрое сжатие)")
        elif features.get('has_text', False):
            recommendations.append("-- Рекомендуемый алгоритм: GZIP (хорошее сжатие текста)")
        else:
            recommendations.append("-- Рекомендуемый алгоритм: ZSTD (баланс скорости и сжатия)")

        recommendations.append("-- Размер блока: 128 MB")
        recommendations.append("-- Включить сжатие на уровне HDFS")

        return recommendations

    def _find_temporal_column(self, df: pd.DataFrame) -> str:
        """Поиск временной колонки"""
        temporal_keywords = ['date', 'time', 'created', 'updated', 'timestamp']
        for col in df.columns:
            if any(keyword in col.lower() for keyword in temporal_keywords):
                return col
        return None

    def _generate_hive_ddl(self, table_name: str, df: pd.DataFrame, features: Dict[str, Any]) -> str:
        """Генерация Hive DDL"""
        hive_ddl = [f"CREATE EXTERNAL TABLE {table_name} ("]

        # Определение колонок
        column_definitions = []
        for col in df.columns:
            clean_name = self._clean_column_name(col)
            pandas_type = str(df[col].dtype)
            hive_type = self._map_to_hive_type(pandas_type)
            column_definitions.append(f"    {clean_name} {hive_type}")

        hive_ddl.extend(column_definitions)
        hive_ddl.append(")")

        # Формат и местоположение
        hive_ddl.append("STORED AS PARQUET")

        if features.get('has_temporal', False):
            temporal_col = self._find_temporal_column(df)
            if temporal_col:
                clean_temporal = self._clean_column_name(temporal_col)
                hive_ddl.append(f"PARTITIONED BY (dt STRING)")

        hive_ddl.append(f"LOCATION '/data/{table_name}/';")

        return "\n".join(hive_ddl)

    def _map_to_hive_type(self, pandas_type: str) -> str:
        """Преобразование pandas типа в Hive тип"""
        if 'int' in pandas_type:
            return 'BIGINT'
        elif 'float' in pandas_type:
            return 'DOUBLE'
        elif 'datetime' in pandas_type:
            return 'TIMESTAMP'
        elif 'bool' in pandas_type:
            return 'BOOLEAN'
        else:
            return 'STRING'