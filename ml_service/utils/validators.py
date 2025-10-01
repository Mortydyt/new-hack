import os
import magic
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from fastapi import UploadFile, HTTPException
from ..models.schemas import DataFormat


class FileValidator:
    """Валидация загружаемых файлов"""

    ALLOWED_MIME_TYPES = {
        DataFormat.CSV: [
            'text/csv',
            'text/plain',
            'application/csv'
        ],
        DataFormat.JSON: [
            'application/json',
            'text/json'
        ],
        DataFormat.XML: [
            'application/xml',
            'text/xml',
            'application/xhtml+xml'
        ]
    }

    ALLOWED_EXTENSIONS = {
        DataFormat.CSV: ['.csv', '.txt'],
        DataFormat.JSON: ['.json', '.jsonl'],
        DataFormat.XML: ['.xml', '.xsd']
    }

    MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB
    MIN_FILE_SIZE = 1  # 1 byte

    @classmethod
    def validate_file(cls, file: UploadFile, format: Optional[DataFormat] = None) -> Tuple[DataFormat, Dict[str, Any]]:
        """
        Валидация загруженного файла
        :return: (определенный формат, информация о файле)
        """
        file_info = {
            'filename': file.filename,
            'content_type': file.content_type,
            'size': 0
        }

        # Проверка имени файла
        if not file.filename:
            raise HTTPException(status_code=400, detail="Filename is required")

        # Определение формата файла
        if format is None:
            detected_format = cls._detect_format(file.filename)
        else:
            detected_format = format

        file_info['detected_format'] = detected_format

        # Проверка расширения файла
        if not cls._validate_extension(file.filename, detected_format):
            raise HTTPException(
                status_code=400,
                detail=f"File extension not supported for format {detected_format.value}"
            )

        # Временное сохранение файла для проверки
        temp_path = None
        try:
            import tempfile
            import aiofiles

            # Создание временного файла
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as temp_file:
                temp_path = temp_file.name
                content = file.file.read()
                file_info['size'] = len(content)
                temp_file.write(content)

            # Проверка размера файла
            cls._validate_file_size(file_info['size'])

            # Проверка MIME типа
            cls._validate_mime_type(temp_path, detected_format)

            # Базовая валидация структуры
            cls._validate_file_structure(temp_path, detected_format)

            return detected_format, file_info

        except Exception as e:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)
            raise e
        finally:
            # Возвращаем указатель файла в начало
            file.file.seek(0)

    @classmethod
    def _detect_format(cls, filename: str) -> DataFormat:
        """Определение формата файла по расширению"""
        extension = Path(filename).suffix.lower()

        for format_type, extensions in cls.ALLOWED_EXTENSIONS.items():
            if extension in extensions:
                return format_type

        # Если расширение не распознано, пробуем по имени файла
        if any(keyword in filename.lower() for keyword in ['csv', 'data']):
            return DataFormat.CSV
        elif any(keyword in filename.lower() for keyword in ['json']):
            return DataFormat.JSON
        elif any(keyword in filename.lower() for keyword in ['xml']):
            return DataFormat.XML

        raise HTTPException(
            status_code=400,
            detail=f"Cannot detect file format for: {filename}"
        )

    @classmethod
    def _validate_extension(cls, filename: str, format: DataFormat) -> bool:
        """Валидация расширения файла"""
        extension = Path(filename).suffix.lower()
        return extension in cls.ALLOWED_EXTENSIONS[format]

    @classmethod
    def _validate_file_size(cls, size: int) -> None:
        """Валидация размера файла"""
        if size < cls.MIN_FILE_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"File too small: {size} bytes"
            )

        if size > cls.MAX_FILE_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"File too large: {size} bytes. Maximum size: {cls.MAX_FILE_SIZE} bytes"
            )

    @classmethod
    def _validate_mime_type(cls, file_path: str, format: DataFormat) -> None:
        """Валидация MIME типа файла"""
        try:
            mime = magic.Magic(mime=True)
            detected_mime = mime.from_file(file_path)

            allowed_mimes = cls.ALLOWED_MIME_TYPES[format]
            if detected_mime not in allowed_mimes:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid MIME type '{detected_mime}' for format {format.value}. "
                           f"Allowed types: {allowed_mimes}"
                )
        except Exception as e:
            # Если не удалось определить MIME тип, пропускаем эту проверку
            pass

    @classmethod
    def _validate_file_structure(cls, file_path: str, format: DataFormat) -> None:
        """Базовая валидация структуры файла"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if format == DataFormat.CSV:
                    cls._validate_csv_structure(f)
                elif format == DataFormat.JSON:
                    cls._validate_json_structure(f)
                elif format == DataFormat.XML:
                    cls._validate_xml_structure(f)
        except UnicodeDecodeError:
            # Пробуем другие кодировки
            try:
                with open(file_path, 'r', encoding='cp1251') as f:
                    if format == DataFormat.CSV:
                        cls._validate_csv_structure(f)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail="File encoding not supported. Please use UTF-8 or CP1251"
                )

    @classmethod
    def _validate_csv_structure(cls, file_handle) -> None:
        """Валидация структуры CSV файла"""
        first_line = file_handle.readline().strip()
        if not first_line:
            raise HTTPException(status_code=400, detail="CSV file is empty")

        # Проверяем, что есть разделители
        if ';' not in first_line and ',' not in first_line and '\t' not in first_line:
            raise HTTPException(
                status_code=400,
                detail="CSV file does not contain valid separators (;, comma, or tab)"
            )

    @classmethod
    def _validate_json_structure(cls, file_handle) -> None:
        """Валидация структуры JSON файла"""
        import json
        try:
            first_char = file_handle.read(1)
            if first_char not in ['[', '{']:
                raise HTTPException(
                    status_code=400,
                    detail="JSON file must start with '[' or '{'"
                )

            # Попытка парсинга первых нескольких символов
            file_handle.seek(0)
            json.loads(file_handle.read(1000))  # Читаем только начало для валидации
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON structure")

    @classmethod
    def _validate_xml_structure(cls, file_handle) -> None:
        """Валидация структуры XML файла"""
        first_line = file_handle.readline().strip()
        if not first_line.startswith('<?xml') and not first_line.startswith('<'):
            raise HTTPException(
                status_code=400,
                detail="XML file must start with '<?xml' or '<'"
            )


class DataValidator:
    """Валидация данных после парсинга"""

    @staticmethod
    def validate_dataframe(df, format: DataFormat) -> Dict[str, Any]:
        """
        Валидация DataFrame после парсинга
        :return: словарь с предупреждениями и ошибками
        """
        validation_result = {
            'warnings': [],
            'errors': [],
            'statistics': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'null_count': df.isnull().sum().sum(),
                'duplicate_rows': df.duplicated().sum()
            }
        }

        # Проверка на пустые данные
        if df.empty:
            validation_result['errors'].append("DataFrame is empty")
            return validation_result

        # Проверка на слишком много null значений
        null_percentage = (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
        if null_percentage > 80:
            validation_result['warnings'].append(f"High percentage of null values: {null_percentage:.1f}%")

        # Проверка на дубликаты
        if validation_result['statistics']['duplicate_rows'] > len(df) * 0.5:
            validation_result['warnings'].append(
                f"High number of duplicate rows: {validation_result['statistics']['duplicate_rows']}"
            )

        # Проверка на слишком много колонок
        if len(df.columns) > 100:
            validation_result['warnings'].append(f"High number of columns: {len(df.columns)}")

        # Проверка на слишком мало колонок
        if len(df.columns) < 2:
            validation_result['warnings'].append(f"Low number of columns: {len(df.columns)}")

        # Специфичные проверки для разных форматов
        if format == DataFormat.CSV:
            validation_result.update(DataValidator._validate_csv_specific(df))
        elif format == DataFormat.JSON:
            validation_result.update(DataValidator._validate_json_specific(df))
        elif format == DataFormat.XML:
            validation_result.update(DataValidator._validate_xml_specific(df))

        return validation_result

    @staticmethod
    def _validate_csv_specific(df) -> Dict[str, Any]:
        """Специфичные проверки для CSV"""
        result = {'warnings': [], 'errors': []}

        # Проверка на числовые колонки с низким разнообразием
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            unique_ratio = df[col].nunique() / len(df)
            if unique_ratio < 0.01 and df[col].nunique() > 1:
                result['warnings'].append(f"Column '{col}' has low cardinality for numeric data")

        return result

    @staticmethod
    def _validate_json_specific(df) -> Dict[str, Any]:
        """Специфичные проверки для JSON"""
        result = {'warnings': [], 'errors': []}

        # Проверка на вложенные структуры (колонки с JSON-подобными данными)
        for col in df.columns:
            if df[col].dtype == 'object':
                sample_values = df[col].dropna().head(10)
                for val in sample_values:
                    if isinstance(val, str) and ('{' in val or '[' in val):
                        result['warnings'].append(f"Column '{col}' may contain nested JSON structures")
                        break

        return result

    @staticmethod
    def _validate_xml_specific(df) -> Dict[str, Any]:
        """Специфичные проверки для XML"""
        result = {'warnings': [], 'errors': []}

        # Проверка на потенциальные пространственные данные
        spatial_keywords = ['coord', 'lat', 'lon', 'x', 'y', 'geometry']
        for col in df.columns:
            if any(keyword in col.lower() for keyword in spatial_keywords):
                result['warnings'].append(f"Column '{col}' may contain spatial data")

        # Проверка на кадастровые номера
        for col in df.columns:
            if 'cad' in col.lower() or 'кадастр' in col.lower():
                result['warnings'].append(f"Column '{col}' may contain cadastral information")

        return result