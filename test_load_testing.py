#!/usr/bin/env python3
"""
Нагрузочное тестирование ML сервиса
Использование: python3 test_load_testing.py
"""
import requests
import json
import time
import threading
import queue
import statistics
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import os
import tempfile
import random

class LoadTester:
    def __init__(self, base_url="http://localhost:8001"):
        self.base_url = base_url
        self.results = queue.Queue()
        self.errors = queue.Queue()

    def create_test_data(self, data_type="csv", size="small"):
        """Создание тестовых данных разного размера"""
        if data_type == "csv":
            if size == "small":
                df = pd.DataFrame({
                    'id': range(1, 101),
                    'name': [f'Item_{i}' for i in range(1, 101)],
                    'value': [random.randint(1, 1000) for _ in range(100)],
                    'category': [random.choice(['A', 'B', 'C']) for _ in range(100)]
                })
            else:  # large
                df = pd.DataFrame({
                    'id': range(1, 10001),
                    'name': [f'Item_{i}' for i in range(1, 10001)],
                    'value': [random.randint(1, 10000) for _ in range(10000)],
                    'category': [random.choice(['A', 'B', 'C', 'D', 'E']) for _ in range(10000)],
                    'timestamp': [pd.Timestamp.now() - pd.Timedelta(days=random.randint(0, 365)) for _ in range(10000)]
                })

            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
                df.to_csv(f.name, index=False)
                return f.name, 'csv'

        elif data_type == "json":
            if size == "small":
                data = [
                    {"id": i, "name": f"User_{i}", "age": random.randint(18, 80)}
                    for i in range(1, 51)
                ]
            else:  # large
                data = [
                    {
                        "id": i,
                        "name": f"User_{i}",
                        "age": random.randint(18, 80),
                        "email": f"user{i}@example.com",
                        "active": random.choice([True, False]),
                        "score": random.randint(1, 100),
                        "created_at": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
                    }
                    for i in range(1, 5001)
                ]

            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
                return f.name, 'json'

    def send_request(self, file_path, format_type, request_id):
        """Отправка одного запроса"""
        start_time = time.time()

        try:
            with open(file_path, 'rb') as f:
                files = {'file': f}
                data = {
                    'format': format_type,
                    'table_name': f'test_table_{request_id}',
                    'use_cache': 'false'
                }

                response = requests.post(
                    f"{self.base_url}/recommend",
                    files=files,
                    data=data,
                    timeout=30
                )

            end_time = time.time()
            response_time = end_time - start_time

            result = {
                'request_id': request_id,
                'response_time': response_time,
                'status_code': response.status_code,
                'success': response.status_code == 200,
                'file_size': os.path.getsize(file_path),
                'format': format_type
            }

            if response.status_code == 200:
                result_data = response.json()
                result['confidence'] = result_data.get('confidence', 0)
                result['target'] = result_data.get('target', 'unknown')
            else:
                result['error'] = response.text[:200]

            self.results.put(result)

        except Exception as e:
            end_time = time.time()
            self.errors.put({
                'request_id': request_id,
                'error': str(e),
                'response_time': end_time - start_time
            })

        finally:
            try:
                os.unlink(file_path)
            except:
                pass

    def run_load_test(self, concurrent_users=10, requests_per_user=5, data_types=None):
        """Запуск нагрузочного тестирования"""
        print(f"🚀 НАГРУЗОЧНОЕ ТЕСТИРОВАНИЕ")
        print(f"👥 Пользователей: {concurrent_users}")
        print(f"📨 Запросов на пользователя: {requests_per_user}")
        print(f"🎯 Всего запросов: {concurrent_users * requests_per_user}")
        print("=" * 60)

        if data_types is None:
            data_types = [('csv', 'small'), ('json', 'small'), ('csv', 'large')]

        start_time = time.time()
        total_requests = concurrent_users * requests_per_user

        def worker():
            for i in range(requests_per_user):
                data_type, size = random.choice(data_types)
                file_path, format_type = self.create_test_data(data_type, size)
                request_id = threading.current_thread().ident * 1000 + i
                self.send_request(file_path, format_type, request_id)

        # Запуск потоков
        with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
            futures = [executor.submit(worker) for _ in range(concurrent_users)]
            for future in futures:
                future.result()

        end_time = time.time()
        total_time = end_time - start_time

        # Анализ результатов
        self.analyze_results(total_requests, total_time, concurrent_users)

    def analyze_results(self, total_requests, total_time, concurrent_users):
        """Анализ результатов тестирования"""
        results_list = []
        errors_list = []

        while not self.results.empty():
            results_list.append(self.results.get())

        while not self.errors.empty():
            errors_list.append(self.errors.get())

        # Статистика
        successful_requests = [r for r in results_list if r['success']]
        failed_requests = [r for r in results_list if not r['success']] + errors_list

        print(f"\n📊 РЕЗУЛЬТАТЫ НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ")
        print("=" * 60)
        print(f"⏱️ Общее время: {total_time:.2f} секунд")
        print(f"📈 Всего запросов: {total_requests}")
        print(f"✅ Успешных: {len(successful_requests)} ({len(successful_requests)/total_requests*100:.1f}%)")
        print(f"❌ Ошибок: {len(failed_requests)} ({len(failed_requests)/total_requests*100:.1f}%)")
        print(f"📊 Requests/sec: {total_requests/total_time:.2f}")

        if successful_requests:
            response_times = [r['response_time'] for r in successful_requests]
            confidences = [r['confidence'] for r in successful_requests]

            print(f"\n⚡ ПРОИЗВОДИТЕЛЬНОСТЬ (успешные запросы):")
            print(f"   Среднее время ответа: {statistics.mean(response_times):.3f} секунд")
            print(f"   Минимальное время: {min(response_times):.3f} секунд")
            print(f"   Максимальное время: {max(response_times):.3f} секунд")
            print(f"   95-й перцентиль: {statistics.quantiles(response_times, n=20)[18]:.3f} секунд")
            print(f"   Средняя уверенность: {statistics.mean(confidences):.1%}")

        if failed_requests:
            print(f"\n❌ ОШИБКИ:")
            error_types = {}
            for error in failed_requests:
                error_msg = error.get('error', 'Unknown error')[:100]
                error_types[error_msg] = error_types.get(error_msg, 0) + 1

            for error_msg, count in error_types.items():
                print(f"   {error_msg}: {count} раз")

        # Рекомендации
        print(f"\n💡 РЕКОМЕНДАЦИИ:")
        if len(successful_requests) / total_requests < 0.95:
            print("   ⚠️  Высокий процент ошибок - проверьте стабильность сервиса")
        if statistics.mean(response_times) > 5.0:
            print("   ⚠️  Медленное время ответа - оптимизируйте обработку")
        if total_requests/total_time < 1.0:
            print("   ⚠️  Низкая пропускная способность - увеличьте ресурсы")

        print(f"\n🎯 НАГРУЗКА ({concurrent_users} пользователей):")
        print(f"   Сервис {'стабилен' if len(successful_requests)/total_requests > 0.9 else 'нестабилен'} под нагрузкой")
        print(f"   Производительность {'хорошая' if statistics.mean(response_times) < 2.0 else 'требует оптимизации'}")

def health_check():
    """Проверка здоровья сервиса"""
    try:
        response = requests.get("http://localhost:8001/health", timeout=10)
        if response.status_code == 200:
            health = response.json()
            print(f"✅ Сервис здоров: {health}")
            return True
        else:
            print(f"⚠️  Сервис отвечает с ошибкой: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Сервис не доступен: {str(e)}")
        return False

def main():
    print("🔬 НАЧАЛО НАГРУЗОЧНОГО ТЕСТИРОВАНИЯ ML СЕРВИСА")
    print("=" * 80)

    # Проверка здоровья
    if not health_check():
        print("\n❌ Пожалуйста, запустите ML сервис перед тестированием")
        print("   cd ml_service && python3 main.py")
        return

    # Разные сценарии тестирования
    tester = LoadTester()

    print("\n📋 СЦЕНАРИИ ТЕСТИРОВАНИЯ:")
    print("-" * 40)

    # Сценарий 1: Легкая нагрузка
    print("\n1️⃣ Сценарий: Легкая нагрузка")
    tester.run_load_test(concurrent_users=5, requests_per_user=3)

    # Сценарий 2: Средняя нагрузка
    print("\n2️⃣ Сценарий: Средняя нагрузка")
    tester.run_load_test(concurrent_users=10, requests_per_user=5)

    # Сценарий 3: Высокая нагрузка
    print("\n3️⃣ Сценарий: Высокая нагрузка")
    tester.run_load_test(concurrent_users=20, requests_per_user=3)

    print(f"\n🎉 НАГРУЗОЧНОЕ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 80)

if __name__ == "__main__":
    main()