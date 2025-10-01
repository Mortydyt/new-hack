from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from ..models.schemas import DataProfile, DataFormat


class BaseParser(ABC):
    def __init__(self):
        self.data = None
        self.features = {}

    @abstractmethod
    def parse(self, file_path: str) -> pd.DataFrame:
        pass

    def extract_features(self, df: pd.DataFrame, sample_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Извлечение признаков из DataFrame с оптимизацией для больших данных
        """
        # Оптимизация памяти перед анализом
        df = self._optimize_dataframe(df)

        # Если размер данных большой, используем сэмплирование
        if sample_size and len(df) > sample_size:
            df_sample = df.sample(n=sample_size, random_state=42)
        else:
            df_sample = df

        features = {
            'record_count': len(df),
            'field_count': len(df.columns),
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'null_counts': df.isnull().sum().to_dict(),
            'unique_counts': self._calculate_unique_counts(df_sample, df),
            'sample_size': len(df_sample) if sample_size else None
        }

        features['has_temporal'] = self._has_temporal_data(df_sample)
        features['has_numeric'] = self._has_numeric_data(df_sample)
        features['has_text'] = self._has_text_data(df_sample)
        features['has_categorical'] = self._has_categorical_data(df_sample)
        features['has_spatial'] = self._has_spatial_data(df_sample)
        features['has_nested'] = self._has_nested_data(df_sample)
        features['unique_ids'] = self._find_unique_ids(df_sample)
        features['temporal_range'] = self._get_temporal_range(df_sample)
        features['estimated_size_mb'] = features['memory_usage'] / (1024 * 1024)

        # Дополнительные метрики для больших данных
        features['data_quality_score'] = self._calculate_data_quality(df)
        features['compression_ratio'] = self._estimate_compression_ratio(df)

        return features

    def _has_temporal_data(self, df: pd.DataFrame) -> bool:
        for col in df.columns:
            if df[col].dtype in ['datetime64[ns]', 'object']:
                try:
                    pd.to_datetime(df[col], errors='raise')
                    return True
                except:
                    continue
        return False

    def _has_numeric_data(self, df: pd.DataFrame) -> bool:
        # Проверяем встроенные числовые типы
        numeric_types = ['int64', 'float64', 'int32', 'float32', 'int16', 'float16']
        if any(dtype in numeric_types for dtype in df.dtypes):
            return True

        # Дополнительно проверяем object типы, которые могут содержать числа
        for col in df.select_dtypes(include=['object']).columns:
            try:
                # Пробуем преобразовать в числа
                pd.to_numeric(df[col], errors='coerce')
                # Если хотя бы одно значение успешно преобразовалось и не NaN
                if df[col].str.match(r'^-?\d*\.?\d+$').any():
                    return True
            except:
                continue
        return False

    def _has_text_data(self, df: pd.DataFrame) -> bool:
        text_cols = df.select_dtypes(include=['object']).columns
        return len(text_cols) > 0

    def _has_categorical_data(self, df: pd.DataFrame) -> bool:
        for col in df.columns:
            if df[col].dtype == 'object':
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio < 0.5:  # Если уникальных значений меньше 50%
                    return True
        return False

    def _has_spatial_data(self, df: pd.DataFrame) -> bool:
        spatial_keywords = ['lat', 'lon', 'latitude', 'longitude', 'coordinate', 'coords', 'geometry', 'point', 'polygon']

        for col in df.columns:
            col_lower = col.lower()
            # Проверяем точные совпадения с ключевыми словами
            if any(keyword in col_lower for keyword in spatial_keywords):
                return True
            # Проверяем комбинации (например, x_coord, y_coord)
            if col_lower in ['x', 'y'] and len(df.columns) >= 2:
                # Ищем пару координат
                other_cols = [c for c in df.columns if c != col]
                if any(other.lower() in ['x', 'y'] for other in other_cols):
                    return True
        return False

    def _has_nested_data(self, df: pd.DataFrame) -> bool:
        nested_keywords = ['json', 'dict', 'list', 'array', 'nested']
        return any(keyword.lower() in str(dtype).lower() for dtype in df.dtypes for keyword in nested_keywords)

    def _find_unique_ids(self, df: pd.DataFrame) -> List[str]:
        unique_ids = []
        id_keywords = ['id', 'uuid', 'guid', 'key', 'code', 'number']

        for col in df.columns:
            col_lower = col.lower()
            # Проверяем, что все значения уникальны
            if df[col].nunique() == len(df):
                # Дополнительно проверяем, что это похоже на ID по названию
                if any(keyword in col_lower for keyword in id_keywords):
                    unique_ids.append(col)
                # Или если это целочисленный тип (вероятно ID)
                elif df[col].dtype in ['int64', 'int32']:
                    unique_ids.append(col)
        return unique_ids

    def _get_temporal_range(self, df: pd.DataFrame) -> List[str]:
        temporal_cols = []
        for col in df.columns:
            try:
                temp_col = pd.to_datetime(df[col], errors='raise')
                temporal_cols.append(col)
            except:
                continue

        if temporal_cols:
            # Используем первую временную колонку
            col = temporal_cols[0]
            temp_data = pd.to_datetime(df[col])
            return [temp_data.min().isoformat(), temp_data.max().isoformat()]
        return []

    def create_profile(self, features: Dict[str, Any], format: DataFormat) -> DataProfile:
        return DataProfile(
            format=format,
            record_count=features['record_count'],
            field_count=features['field_count'],
            has_temporal=features['has_temporal'],
            has_numeric=features['has_numeric'],
            has_text=features['has_text'],
            has_categorical=features['has_categorical'],
            has_spatial=features['has_spatial'],
            has_nested=features['has_nested'],
            unique_ids=features['unique_ids'],
            temporal_range=features['temporal_range'] if features['temporal_range'] else None,
            estimated_size_mb=features['estimated_size_mb']
        )

    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Оптимизация использования памяти DataFrame"""
        # Оптимизация числовых типов
        for col in df.select_dtypes(include=['int64']).columns:
            col_min = df[col].min()
            col_max = df[col].max()
            if col_min > 0:
                if col_max < 255:
                    df[col] = df[col].astype('uint8')
                elif col_max < 65535:
                    df[col] = df[col].astype('uint16')
                elif col_max < 4294967295:
                    df[col] = df[col].astype('uint32')
            else:
                if col_min > -128 and col_max < 127:
                    df[col] = df[col].astype('int8')
                elif col_min > -32768 and col_max < 32767:
                    df[col] = df[col].astype('int16')
                elif col_min > -2147483648 and col_max < 2147483647:
                    df[col] = df[col].astype('int32')

        # Оптимизация float типов
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')

        # Оптимизация object типов
        for col in df.select_dtypes(include=['object']).columns:
            num_unique_values = len(df[col].unique())
            num_total_values = len(df[col])
            if num_unique_values / num_total_values < 0.5:
                df[col] = df[col].astype('category')

        return df

    def _calculate_unique_counts(self, df_sample: pd.DataFrame, df_full: pd.DataFrame) -> Dict[str, Any]:
        """Эффективный расчет уникальных значений"""
        unique_counts = {}
        for col in df_sample.columns:
            if len(df_full) > 100000:  # Для больших данных
                unique_counts[col] = min(df_sample[col].nunique(), 100000)  # Ограничение для безопасности
            else:
                unique_counts[col] = df_full[col].nunique()
        return unique_counts

    def _calculate_data_quality(self, df: pd.DataFrame) -> float:
        """Расчет качества данных (0-1)"""
        total_cells = df.shape[0] * df.shape[1]
        null_cells = df.isnull().sum().sum()
        null_ratio = null_cells / total_cells if total_cells > 0 else 0

        # Проверка на дубликаты
        duplicate_ratio = df.duplicated().sum() / len(df) if len(df) > 0 else 0

        # Комбинированный показатель качества
        quality_score = 1.0 - (null_ratio + duplicate_ratio)
        return max(0.0, min(1.0, quality_score))

    def _estimate_compression_ratio(self, df: pd.DataFrame) -> float:
        """Оценка коэффициента сжатия"""
        original_size = df.memory_usage(deep=True).sum()

        # Оценка размера после сжатия (упрощенная)
        estimated_compressed_size = 0
        for col in df.columns:
            if df[col].dtype == 'object':
                # Текстовые данные хорошо сжимаются
                estimated_compressed_size += df[col].memory_usage(deep=True) * 0.3
            elif df[col].dtype in ['int64', 'float64']:
                # Числовые данные сжимаются умеренно
                estimated_compressed_size += df[col].memory_usage(deep=True) * 0.6
            else:
                estimated_compressed_size += df[col].memory_usage(deep=True) * 0.8

        return estimated_compressed_size / original_size if original_size > 0 else 1.0