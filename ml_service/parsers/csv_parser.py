import pandas as pd
from typing import Dict, Any, Optional
import os
from .base_parser import BaseParser
from ..models.schemas import DataFormat


class CSVParser(BaseParser):
    def __init__(self):
        super().__init__()
        self.chunk_size = 10000  # Размер чанка для больших файлов
        self.max_sample_size = 50000  # Максимальный размер сэмпла для анализа

    def parse(self, file_path: str) -> pd.DataFrame:
        try:
            # Определяем размер файла для выбора стратегии парсинга
            file_size = os.path.getsize(file_path)

            if file_size > 100 * 1024 * 1024:  # > 100MB - используем чанкование
                return self._parse_large_file(file_path)
            else:
                return self._parse_regular_file(file_path)

        except Exception as e:
            raise ValueError(f"Error parsing CSV file: {str(e)}")

    def _parse_regular_file(self, file_path: str) -> pd.DataFrame:
        """Парсинг обычных файлов с автоопределением разделителя"""
        encodings = ['utf-8', 'cp1251', 'latin1']
        separators = [',', ';', '\t', '|']  # Проверяем разделители по порядку

        for encoding in encodings:
            for sep in separators:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=sep, low_memory=False)
                    # Проверяем, что данные разбились на несколько колонок
                    if len(df.columns) > 1:
                        self.data = df
                        return df
                except (UnicodeDecodeError, pd.errors.ParserError, ValueError):
                    continue

        # Если стандартные разделители не сработали, пробуем автоматическое определение
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                if ';' in first_line:
                    sep = ';'
                elif ',' in first_line:
                    sep = ','
                elif '\t' in first_line:
                    sep = '\t'
                else:
                    sep = ','  # по умолчанию

            df = pd.read_csv(file_path, encoding='utf-8', sep=sep, low_memory=False)
            self.data = df
            return df
        except Exception as e:
            # Если ничего не помогло, используем запятую и игнорируем ошибки
            df = pd.read_csv(file_path, encoding='utf-8', errors='ignore', sep=',', low_memory=False)
            self.data = df
            return df

    def _parse_large_file(self, file_path: str) -> pd.DataFrame:
        """Парсинг больших файлов с чанкованием и автоопределением разделителя"""
        encodings = ['utf-8', 'cp1251', 'latin1']
        separators = [None, ';', ',', '\t', '|']

        for encoding in encodings:
            for sep in separators:
                try:
                    # Читаем первые несколько строк для определения структуры
                    sample_df = pd.read_csv(file_path, encoding=encoding, sep=sep,
                                           nrows=1000, low_memory=False)

                    # Проверяем, что данные разбились на несколько колонок
                    if len(sample_df.columns) > 1:
                        # Читаем весь файл по чанкам и объединяем
                        chunks = []
                        for chunk in pd.read_csv(file_path, encoding=encoding, sep=sep,
                                               chunksize=self.chunk_size, low_memory=False):
                            chunks.append(chunk)
                            # Ограничиваем общее количество записей для анализа
                            if sum(len(c) for c in chunks) > self.max_sample_size:
                                break

                        df = pd.concat(chunks, ignore_index=True)
                        self.data = df
                        return df

                except (UnicodeDecodeError, pd.errors.ParserError):
                    continue

        # Если не получилось, пробуем с автоматическим определением
        try:
            sample_df = pd.read_csv(file_path, encoding='utf-8', errors='ignore',
                                   sep=None, nrows=1000, engine='python')

            chunks = []
            for chunk in pd.read_csv(file_path, encoding='utf-8', errors='ignore',
                                   sep=None, chunksize=self.chunk_size, engine='python'):
                chunks.append(chunk)
                if sum(len(c) for c in chunks) > self.max_sample_size:
                    break

            df = pd.concat(chunks, ignore_index=True)
        except:
            # Если и это не работает, используем запятую как разделитель по умолчанию
            sample_df = pd.read_csv(file_path, encoding='utf-8', errors='ignore',
                                   sep=',', nrows=1000, low_memory=False)

            chunks = []
            for chunk in pd.read_csv(file_path, encoding='utf-8', errors='ignore',
                                   sep=',', chunksize=self.chunk_size, low_memory=False):
                chunks.append(chunk)
                if sum(len(c) for c in chunks) > self.max_sample_size:
                    break

            df = pd.concat(chunks, ignore_index=True)

        self.data = df
        return df

    def analyze(self, file_path: str) -> Dict[str, Any]:
        try:
            df = self.parse(file_path)

            # Определяем размер сэмпла для анализа
            sample_size = min(self.max_sample_size, len(df))

            features = self.extract_features(df, sample_size=sample_size)
            profile = self.create_profile(features, DataFormat.CSV)

            return {
                'data_profile': profile.dict(),
                'features': features,
                'file_size_mb': os.path.getsize(file_path) / (1024 * 1024),
                'parsing_strategy': 'chunked' if len(df) >= self.max_sample_size else 'full'
            }

        except Exception as e:
            raise ValueError(f"Error analyzing CSV file: {str(e)}")