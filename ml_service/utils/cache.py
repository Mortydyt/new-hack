import hashlib
import json
import time
import os
from typing import Dict, Any, Optional
from pathlib import Path


class AnalysisCache:
    def __init__(self, cache_dir: str = "cache", ttl_seconds: int = 3600):
        """
        Кэш для результатов анализа данных
        :param cache_dir: директория для хранения кэша
        :param ttl_seconds: время жизни кэша в секундах (по умолчанию 1 час)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.ttl_seconds = ttl_seconds

    def _get_file_hash(self, file_path: str) -> str:
        """Вычисление хэша файла"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_cache_key(self, file_path: str, analysis_type: str = "analysis") -> str:
        """Генерация ключа кэша"""
        file_hash = self._get_file_hash(file_path)
        file_size = os.path.getsize(file_path)
        return f"{analysis_type}_{file_hash}_{file_size}"

    def _get_cache_path(self, cache_key: str) -> Path:
        """Получение пути к файлу кэша"""
        return self.cache_dir / f"{cache_key}.json"

    def is_valid(self, cache_path: Path) -> bool:
        """Проверка валидности кэша"""
        if not cache_path.exists():
            return False

        # Проверка времени жизни
        file_time = cache_path.stat().st_mtime
        current_time = time.time()
        return (current_time - file_time) < self.ttl_seconds

    def get(self, file_path: str, analysis_type: str = "analysis") -> Optional[Dict[str, Any]]:
        """
        Получение данных из кэша
        :param file_path: путь к файлу
        :param analysis_type: тип анализа ("analysis" или "recommendation")
        :return: данные из кэша или None
        """
        cache_key = self._get_cache_key(file_path, analysis_type)
        cache_path = self._get_cache_path(cache_key)

        if not self.is_valid(cache_path):
            return None

        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)

            # Проверяем, что файл не изменился
            if cached_data.get('file_hash') == self._get_file_hash(file_path):
                return cached_data.get('data')
            else:
                # Файл изменился, удаляем устаревший кэш
                cache_path.unlink()
                return None

        except (json.JSONDecodeError, IOError):
            # Поврежденный кэш, удаляем
            if cache_path.exists():
                cache_path.unlink()
            return None

    def set(self, file_path: str, data: Dict[str, Any], analysis_type: str = "analysis") -> None:
        """
        Сохранение данных в кэш
        :param file_path: путь к файлу
        :param data: данные для кэширования
        :param analysis_type: тип анализа
        """
        cache_key = self._get_cache_key(file_path, analysis_type)
        cache_path = self._get_cache_path(cache_key)

        cache_data = {
            'file_hash': self._get_file_hash(file_path),
            'file_path': file_path,
            'timestamp': time.time(),
            'data': data
        }

        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
        except IOError:
            # Не удалось сохранить кэш, игнорируем ошибку
            pass

    def clear(self) -> None:
        """Очистка всего кэша"""
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                cache_file.unlink()
            except IOError:
                pass

    def clear_expired(self) -> None:
        """Очистка просроченного кэша"""
        current_time = time.time()
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                file_time = cache_file.stat().st_mtime
                if (current_time - file_time) >= self.ttl_seconds:
                    cache_file.unlink()
            except IOError:
                pass

    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики кэша"""
        cache_files = list(self.cache_dir.glob("*.json"))
        total_size = sum(f.stat().st_size for f in cache_files)

        current_time = time.time()
        valid_files = 0
        expired_files = 0

        for cache_file in cache_files:
            file_time = cache_file.stat().st_mtime
            if (current_time - file_time) < self.ttl_seconds:
                valid_files += 1
            else:
                expired_files += 1

        return {
            'total_files': len(cache_files),
            'valid_files': valid_files,
            'expired_files': expired_files,
            'total_size_mb': total_size / (1024 * 1024),
            'cache_dir': str(self.cache_dir)
        }