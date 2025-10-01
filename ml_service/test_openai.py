#!/usr/bin/env python3
"""
Тестовый скрипт для проверки OpenAI интеграции
"""
import os
import sys

# Добавляем родительскую директорию в Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from dotenv import load_dotenv
from ml_service.analyzers.openai_client import OpenAIClient

# Загружаем переменные окружения
load_dotenv()

def test_openai_connection():
    """Тест подключения к OpenAI API"""
    print("🚀 Testing OpenAI integration...")

    # Проверяем переменные окружения
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("❌ OPENAI_API_KEY not found in environment variables")
        return False

    # Инициализируем клиент
    try:
        client = OpenAIClient()
        print(f"✅ OpenAI client initialized")
        print(f"   Model: {client.model}")
        print(f"   Max tokens: {client.max_tokens}")
        print(f"   Temperature: {client.temperature}")
    except Exception as e:
        print(f"❌ Failed to initialize OpenAI client: {e}")
        return False

    # Тестируем подключение
    print("\n🔌 Testing API connection...")
    if client.test_connection():
        print("✅ OpenAI API connection successful")
    else:
        print("❌ OpenAI API connection failed")
        return False

    # Тестируем генерацию обоснования
    print("\n📝 Testing rationale generation...")
    test_profile = {
        'format': 'csv',
        'estimated_size_mb': 150.5,
        'record_count': 1000000,
        'field_count': 25,
        'has_temporal': True,
        'has_numeric': True,
        'has_text': True,
        'has_categorical': True,
        'has_spatial': False,
        'has_nested': False,
        'unique_ids': ['id'],
        'temporal_range': ['2021-01-01', '2021-12-31']
    }

    test_features = {
        'columns': ['id', 'created_at', 'price', 'name', 'category'],
        'data_quality_score': 0.95
    }

    try:
        rationale = client.generate_rationale(test_profile, test_features, "clickhouse", 0.9)
        print(f"✅ Rationale generated successfully:")
        print(f"   {rationale}")
    except Exception as e:
        print(f"❌ Rationale generation failed: {e}")
        return False

    # Тестируем генерацию DDL
    print("\n🗄️ Testing DDL generation...")
    try:
        ddl = client.generate_ddl(
            "test_table",
            test_profile,
            test_features,
            "clickhouse",
            {'columns': test_features['columns'], 'dtypes': {col: 'String' for col in test_features['columns']}}
        )
        print(f"✅ DDL generated successfully:")
        print(f"   {ddl[:200]}...")
    except Exception as e:
        print(f"❌ DDL generation failed: {e}")
        return False

    # Показываем статистику
    print("\n📊 Client stats:")
    stats = client.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

    print("\n✅ All tests passed! OpenAI integration is working correctly.")
    return True

if __name__ == "__main__":
    success = test_openai_connection()
    sys.exit(0 if success else 1)