import pandas as pd
import xml.etree.ElementTree as ET
import xmltodict
import os
from typing import Dict, Any, List, Optional, Iterator
from .base_parser import BaseParser
from ..models.schemas import DataFormat


class XMLParser(BaseParser):
    def __init__(self):
        super().__init__()
        self.max_sample_size = 10000  # Ограничение для больших XML файлов
        self.chunk_size = 1000  # Размер чанка для потоковой обработки

    def parse(self, file_path: str) -> pd.DataFrame:
        try:
            file_size = os.path.getsize(file_path)

            if file_size > 50 * 1024 * 1024:  # > 50MB - потоковая обработка
                return self._parse_large_xml_streaming(file_path)
            else:
                return self._parse_regular_xml(file_path)

        except Exception as e:
            raise ValueError(f"Error parsing XML file: {str(e)}")

    def _parse_regular_xml(self, file_path: str) -> pd.DataFrame:
        """Парсинг обычных XML файлов"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()

            xml_dict = xmltodict.parse(xml_content)
            df = self._extract_cadastral_data(xml_dict)
            self.data = df
            return df

        except Exception as e:
            # Если не получилось с xmltodict, пробуем ElementTree
            return self._parse_with_elementtree(file_path)

    def _parse_large_xml_streaming(self, file_path: str) -> pd.DataFrame:
        """Потоковая обработка больших XML файлов"""
        return self._parse_with_elementtree_streaming(file_path)

    def _parse_with_elementtree(self, file_path: str) -> pd.DataFrame:
        """Парсинг с использованием ElementTree для сложных структур"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            records = []

            # Специфичная обработка для кадастровых данных
            if root.tag == 'root':
                for item in root.findall('.//item'):
                    record = self._extract_cadastral_item(item)
                    if record:
                        records.append(record)
                        if len(records) >= self.max_sample_size:
                            break

            if not records:
                # Универсальная обработка
                records = self._universal_xml_extract(root)

            df = pd.DataFrame(records)
            self.data = df
            return df

        except Exception as e:
            raise ValueError(f"ElementTree parsing failed: {str(e)}")

    def _parse_with_elementtree_streaming(self, file_path: str) -> pd.DataFrame:
        """Потоковый парсинг больших XML файлов"""
        records = []

        try:
            context = ET.iterparse(file_path, events=('start', 'end'))

            for event, elem in context:
                if event == 'end' and elem.tag == 'item':
                    record = self._extract_cadastral_item(elem)
                    if record:
                        records.append(record)
                        if len(records) >= self.max_sample_size:
                            break

                    # Очищаем память
                    elem.clear()

            df = pd.DataFrame(records)
            self.data = df
            return df

        except Exception as e:
            raise ValueError(f"Streaming XML parsing failed: {str(e)}")

    def _extract_cadastral_item(self, element: ET.Element) -> Optional[Dict[str, Any]]:
        """Извлечение данных из кадастрового элемента"""
        record = {}

        # Базовые поля
        cad_number = element.findtext('.//cad_number')
        if cad_number:
            record['cad_number'] = cad_number

        # Метаданные
        status = element.findtext('.//status')
        if status:
            record['status'] = status

        last_change = element.findtext('.//last_container_fixed_at')
        if last_change:
            record['last_container_fixed_at'] = last_change

        # Адресные данные
        address = element.findtext('.//address')
        if address:
            record['address'] = address

        # Координаты (пространственные данные)
        coords = self._extract_coordinates(element)
        if coords:
            record.update(coords)

        # Тип объекта
        object_type = element.findtext('.//object_type')
        if object_type:
            record['object_type'] = object_type

        # Площадь
        area = element.findtext('.//area')
        if area:
            try:
                record['area'] = float(area)
            except ValueError:
                record['area'] = area

        # Назначение
        purpose = element.findtext('.//purpose')
        if purpose:
            record['purpose'] = purpose

        return record if record else None

    def _extract_coordinates(self, element: ET.Element) -> Dict[str, Any]:
        """Извлечение координат из элемента"""
        coords = {}

        # Поиск координат в разных форматах
        for coord_tag in ['coordinates', 'coord', 'point', 'location']:
            coord_elem = element.find(f'.//{coord_tag}')
            if coord_elem is not None:
                coord_text = coord_elem.text
                if coord_text:
                    # Парсинг разных форматов координат
                    if ',' in coord_text:
                        parts = coord_text.split(',')
                        if len(parts) >= 2:
                            coords['latitude'] = parts[0].strip()
                            coords['longitude'] = parts[1].strip()
                    else:
                        coords['coordinates_raw'] = coord_text
                    break

        return coords

    def _extract_cadastral_data(self, xml_dict: Dict[str, Any]) -> pd.DataFrame:
        """Специфичное извлечение кадастровых данных"""
        records = []

        def extract_cadastral_recursive(data, prefix='', record=None):
            if record is None:
                record = {}

            if isinstance(data, dict):
                for key, value in data.items():
                    new_key = f"{prefix}_{key}" if prefix else key

                    if key in ['cad_number', 'status', 'address', 'purpose', 'object_type']:
                        # Прямые поля
                        record[new_key] = str(value) if value is not None else None
                    elif key in ['area', 'latitude', 'longitude']:
                        # Числовые поля
                        try:
                            record[new_key] = float(value) if value is not None else None
                        except (ValueError, TypeError):
                            record[new_key] = str(value) if value is not None else None
                    elif key in ['last_container_fixed_at', 'date_created']:
                        # Временные поля
                        record[new_key] = str(value) if value is not None else None
                    elif isinstance(value, (dict, list)):
                        # Рекурсивная обработка вложенных структур
                        extract_cadastral_recursive(value, new_key, record)
                    else:
                        record[new_key] = str(value) if value is not None else None

            elif isinstance(data, list):
                # Если это список элементов, создаем отдельные записи
                if len(data) > 1 and any(isinstance(item, dict) for item in data):
                    for i, item in enumerate(data):
                        new_record = record.copy()
                        extract_cadastral_recursive(item, f"{prefix}_{i}", new_record)
                        if new_record:
                            records.append(new_record)
                else:
                    record[prefix] = str(data)

        extract_cadastral_recursive(xml_dict)

        if not records:
            # Если не нашли специфичную структуру, используем универсальный метод
            return self._extract_tabular_data(xml_dict)

        return pd.DataFrame(records)

    def _universal_xml_extract(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Универсальное извлечение данных из XML"""
        records = []

        def element_to_dict(element):
            result = {}
            # Атрибуты
            result.update(element.attrib)

            # Текст элемента
            if element.text and element.text.strip():
                result['text'] = element.text.strip()

            # Дочерние элементы
            for child in element:
                child_dict = element_to_dict(child)
                for key, value in child_dict.items():
                    if key in result:
                        # Если ключ уже существует, создаем список
                        if not isinstance(result[key], list):
                            result[key] = [result[key]]
                        result[key].append(value)
                    else:
                        result[key] = value

            return result

        # Находим повторяющиеся элементы как потенциальные записи
        all_elements = list(root.iter())
        element_counts = {}
        for elem in all_elements:
            tag = elem.tag
            element_counts[tag] = element_counts.get(tag, 0) + 1

        # Ищем элементы, которые повторяются много раз
        for tag, count in element_counts.items():
            if count > 10 and tag != root.tag:  # Потенциальные записи
                for elem in root.findall(f'.//{tag}'):
                    record = element_to_dict(elem)
                    if record:
                        records.append(record)
                        if len(records) >= self.max_sample_size:
                            break
                if len(records) >= self.max_sample_size:
                    break

        return records

    def _extract_tabular_data(self, xml_dict: Dict[str, Any]) -> pd.DataFrame:
        """Базовое извлечение табличных данных (резервный метод)"""
        records = []

        def extract_recursive(data, parent_key='', path=''):
            if isinstance(data, dict):
                for key, value in data.items():
                    new_path = f"{path}.{key}" if path else key
                    extract_recursive(value, key, new_path)
            elif isinstance(data, list):
                if all(isinstance(item, dict) for item in data):
                    for item in data:
                        flat_record = self._flatten_dict(item)
                        records.append(flat_record)
                else:
                    records.append({path: data})
            else:
                records.append({path: data})

        extract_recursive(xml_dict)

        if not records:
            raise ValueError("No tabular data found in XML structure")

        return pd.DataFrame(records)

    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Преобразование вложенного словаря в плоский"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if all(isinstance(item, (str, int, float)) for item in v):
                    items.append((new_key, str(v)))
                else:
                    for i, item in enumerate(v):
                        items.append((f"{new_key}_{i}", str(item)))
            else:
                items.append((new_key, v))
        return dict(items)

    def analyze(self, file_path: str) -> Dict[str, Any]:
        try:
            df = self.parse(file_path)
            sample_size = min(self.max_sample_size, len(df))

            features = self.extract_features(df, sample_size=sample_size)
            profile = self.create_profile(features, DataFormat.XML)

            return {
                'data_profile': profile.dict(),
                'features': features,
                'file_size_mb': os.path.getsize(file_path) / (1024 * 1024),
                'parsing_strategy': 'streaming' if len(df) >= self.max_sample_size else 'full',
                'xml_structure': 'cadastral' if 'cad_number' in df.columns else 'generic'
            }

        except Exception as e:
            raise ValueError(f"Error analyzing XML file: {str(e)}")