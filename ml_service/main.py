import os
import uuid
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, Optional
import pandas as pd
import aiofiles
import tempfile

from .models.schemas import (
    AnalysisRequest, AnalysisResponse, RecommendationResponse,
    DataFormat, StorageType, ScheduleHint
)
from .parsers import get_parser
from .analyzers import RuleEngine
from .generators import get_ddl_generator
from .utils import AnalysisCache, FileValidator, DataValidator


app = FastAPI(
    title="ML Service for Data Storage Recommendation",
    description="ML-сервис для анализа данных и рекомендаций по оптимальному хранилищу",
    version="1.0.0"
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальные переменные
rule_engine = RuleEngine()
cache = AnalysisCache(cache_dir="cache", ttl_seconds=7200)  # 2 часа кэширования
uploads_dir = "uploads"
os.makedirs(uploads_dir, exist_ok=True)
os.makedirs("cache", exist_ok=True)


@app.get("/")
async def root():
    """Корневой эндпоинт для проверки работы сервиса"""
    return {
        "service": "ML Data Storage Recommendation Service",
        "version": "1.0.0",
        "status": "running",
        "endpoints": [
            "POST /analyze - анализ данных",
            "POST /recommend - рекомендация хранилища"
        ]
    }


@app.post("/analyze", response_model=AnalysisResponse)
async def analyze_data(file: UploadFile = File(...), format: DataFormat = None, use_cache: bool = True, validate: bool = True):
    """
    Анализирует загруженный файл и возвращает профиль данных
    """
    try:
        # Валидация файла
        if validate:
            detected_format, file_info = FileValidator.validate_file(file, format)
        else:
            detected_format = format or _detect_file_format(file.filename)
            file_info = {'filename': file.filename, 'detected_format': detected_format}

        # Сохранение временного файла
        temp_file_path = await _save_upload_file(file)

        # Проверка кэша
        if use_cache:
            cached_result = cache.get(temp_file_path, "analysis")
            if cached_result:
                os.unlink(temp_file_path)
                response = AnalysisResponse(**cached_result)
                response.file_info = file_info  # Добавляем информацию о файле
                return response

        # Анализ данных
        parser = get_parser(detected_format)
        analysis_result = parser.analyze(temp_file_path)

        # Валидация данных после парсинга
        if validate:
            data_validation = DataValidator.validate_dataframe(parser.data, detected_format)
            analysis_result['data_validation'] = data_validation

        # Добавляем информацию о файле
        analysis_result['file_info'] = file_info

        # Сохранение в кэш
        if use_cache:
            cache.set(temp_file_path, analysis_result, "analysis")

        # Удаление временного файла
        os.unlink(temp_file_path)

        return AnalysisResponse(**analysis_result)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Analysis failed: {str(e)}")


@app.post("/recommend", response_model=RecommendationResponse)
async def recommend_storage(file: UploadFile = File(...), format: DataFormat = None, table_name: str = "recommended_table", use_cache: bool = True, validate: bool = True):
    """
    Анализирует данные и возвращает рекомендацию по оптимальному хранилищу
    """
    try:
        # Валидация файла
        if validate:
            detected_format, file_info = FileValidator.validate_file(file, format)
        else:
            detected_format = format or _detect_file_format(file.filename)
            file_info = {'filename': file.filename, 'detected_format': detected_format}

        # Сохранение временного файла
        temp_file_path = await _save_upload_file(file)

        # Проверка кэша
        if use_cache:
            cached_result = cache.get(temp_file_path, "recommendation")
            if cached_result:
                os.unlink(temp_file_path)
                response = RecommendationResponse(**cached_result)
                response.file_info = file_info
                return response

        # Анализ данных
        parser = get_parser(detected_format)
        analysis_result = parser.analyze(temp_file_path)

        # Валидация данных после парсинга
        validation_warnings = []
        if validate:
            data_validation = DataValidator.validate_dataframe(parser.data, detected_format)
            validation_warnings = data_validation.get('warnings', [])
            validation_errors = data_validation.get('errors', [])

            if validation_errors:
                # Если есть критические ошибки, возвращаем их
                return RecommendationResponse(
                    target=StorageType.POSTGRESQL,
                    confidence=0.1,
                    rationale=f"Data validation failed: {'; '.join(validation_errors)}",
                    schedule_hint=ScheduleHint.DAILY,
                    ddl_hints=["Fix data quality issues before proceeding"],
                    ddl_script="-- Data validation failed. Please fix the following issues:\n" +
                              "\n".join([f"-- {error}" for error in validation_errors]),
                    data_profile=analysis_result['data_profile'],
                    file_info=file_info,
                    validation_errors=validation_errors,
                    validation_warnings=validation_warnings
                )

        # Получение данных
        features = analysis_result['features']
        data_profile_dict = analysis_result['data_profile']

        # Преобразование в объект DataProfile
        from .models.schemas import DataProfile
        data_profile = DataProfile(**data_profile_dict)

        # Получение рекомендации
        recommendation = rule_engine.get_recommendation(data_profile, features)

        # Генерация DDL
        ddl_generator = get_ddl_generator(recommendation['target'])
        df = parser.data  # Получаем DataFrame из парсера

        # Пробуем сгенерировать улучшенный DDL с помощью LLM
        enhanced_ddl = rule_engine.generate_enhanced_ddl(
            table_name, data_profile, features, recommendation['target'],
            {'columns': list(df.columns), 'dtypes': df.dtypes.to_dict()}
        )

        if enhanced_ddl:
            ddl_script = enhanced_ddl
        else:
            ddl_script = ddl_generator.generate_ddl(table_name, df, features)

        # Формирование результата
        result = RecommendationResponse(
            target=recommendation['target'],
            confidence=recommendation['confidence'],
            rationale=recommendation['rationale'],
            schedule_hint=recommendation['schedule_hint'],
            ddl_hints=recommendation['ddl_hints'],
            ddl_script=ddl_script,
            data_profile=data_profile,
            file_info=file_info,
            validation_warnings=validation_warnings
        )

        # Сохранение в кэш
        if use_cache:
            cache.set(temp_file_path, result.dict(), "recommendation")

        # Удаление временного файла
        os.unlink(temp_file_path)

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Recommendation failed: {str(e)}")


@app.get("/health")
async def health_check():
    """Проверка состояния сервиса"""
    return {"status": "healthy"}


@app.get("/cache/stats")
async def cache_stats():
    """Статистика кэша"""
    return cache.get_stats()


@app.delete("/cache/clear")
async def clear_cache():
    """Очистка кэша"""
    cache.clear()
    return {"message": "Cache cleared successfully"}


@app.delete("/cache/expired")
async def clear_expired_cache():
    """Очистка просроченного кэша"""
    cache.clear_expired()
    return {"message": "Expired cache cleared successfully"}


def _detect_file_format(filename: str) -> DataFormat:
    """Определение формата файла по расширению"""
    if not filename:
        raise ValueError("Filename is required")

    extension = filename.split('.')[-1].lower()
    format_map = {
        'csv': DataFormat.CSV,
        'json': DataFormat.JSON,
        'xml': DataFormat.XML,
        'txt': DataFormat.CSV  # По умолчанию считаем как CSV
    }

    if extension not in format_map:
        raise ValueError(f"Unsupported file format: {extension}")

    return format_map[extension]


async def _save_upload_file(file: UploadFile) -> str:
    """Сохранение загруженного файла во временную директорию"""
    try:
        # Создание уникального имени файла
        file_extension = file.filename.split('.')[-1] if '.' in file.filename else 'tmp'
        unique_filename = f"{uuid.uuid4()}.{file_extension}"
        file_path = os.path.join(uploads_dir, unique_filename)

        # Сохранение файла
        async with aiofiles.open(file_path, 'wb') as f:
            content = await file.read()
            await f.write(content)

        return file_path

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving file: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)