import pandas as pd
import json
from typing import Dict, Any
from .base_parser import BaseParser
from ..models.schemas import DataFormat


class JSONParser(BaseParser):
    def parse(self, file_path: str) -> pd.DataFrame:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Если это массив объектов
            if isinstance(data, list):
                df = pd.json_normalize(data)
            # Если это один объект
            elif isinstance(data, dict):
                df = pd.json_normalize([data])
            else:
                raise ValueError("Unsupported JSON structure")

            self.data = df
            return df

        except Exception as e:
            raise ValueError(f"Error parsing JSON file: {str(e)}")

    def _flatten_nested_structures(self, df: pd.DataFrame) -> pd.DataFrame:
        """Рекурсивное преобразование вложенных структур"""
        for col in df.columns:
            if df[col].dtype == 'object':
                # Проверяем, содержит ли колонка словари или списки
                sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                if isinstance(sample, (dict, list)):
                    # Нормализуем вложенную структуру
                    normalized = pd.json_normalize(df[col])
                    normalized.columns = [f"{col}_{subcol}" for subcol in normalized.columns]
                    df = df.drop(col, axis=1).join(normalized)

        return df

    def analyze(self, file_path: str) -> Dict[str, Any]:
        df = self.parse(file_path)
        df = self._flatten_nested_structures(df)
        features = self.extract_features(df)
        profile = self.create_profile(features, DataFormat.JSON)

        return {
            'data_profile': profile.dict(),
            'features': features
        }