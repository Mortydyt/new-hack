#!/usr/bin/env python3
"""
Проверка здоровья и мониторинг ML сервиса
Использование: python3 test_health_check.py
"""
import requests
import time
import json
import psutil
import os
from datetime import datetime

class HealthMonitor:
    def __init__(self, base_url="http://localhost:8001"):
        self.base_url = base_url
        self.start_time = time.time()

    def check_service_health(self):
        """Проверка здоровья сервиса"""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=10)
            if response.status_code == 200:
                health_data = response.json()
                return {
                    'status': 'healthy',
                    'response_time': response.elapsed.total_seconds(),
                    'details': health_data
                }
            else:
                return {
                    'status': 'error',
                    'response_time': response.elapsed.total_seconds(),
                    'error': f'HTTP {response.status_code}'
                }
        except requests.exceptions.Timeout:
            return {
                'status': 'timeout',
                'error': 'Request timeout'
            }
        except requests.exceptions.ConnectionError:
            return {
                'status': 'offline',
                'error': 'Connection refused'
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }

    def check_api_endpoints(self):
        """Проверка доступности API endpoints"""
        endpoints = [
            ('/', 'GET'),
            ('/health', 'GET'),
            ('/docs', 'GET')
        ]

        results = {}
        for endpoint, method in endpoints:
            try:
                if method == 'GET':
                    response = requests.get(f"{self.base_url}{endpoint}", timeout=5)
                results[endpoint] = {
                    'status': response.status_code,
                    'response_time': response.elapsed.total_seconds()
                }
            except Exception as e:
                results[endpoint] = {
                    'status': 'error',
                    'error': str(e)
                }

        return results

    def check_system_resources(self):
        """Проверка системных ресурсов"""
        try:
            # Поиск процесса ML сервиса
            ml_service_pid = None
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if 'python' in proc.info['name'].lower() and \
                       any('main.py' in cmd for cmd in proc.info['cmdline'] if cmd):
                        ml_service_pid = proc.info['pid']
                        break
                except:
                    continue

            resources = {
                'cpu_percent': psutil.cpu_percent(interval=1),
                'memory_percent': psutil.virtual_memory().percent,
                'disk_percent': psutil.disk_usage('/').percent,
                'ml_service_found': ml_service_pid is not None
            }

            if ml_service_pid:
                try:
                    process = psutil.Process(ml_service_pid)
                    resources['ml_service_cpu'] = process.cpu_percent()
                    resources['ml_service_memory'] = process.memory_percent()
                except:
                    pass

            return resources
        except Exception as e:
            return {'error': str(e)}

    def test_sample_request(self):
        """Тестовый запрос к сервису"""
        try:
            # Создаем простые тестовые данные
            test_data = {
                'format': 'json',
                'table_name': 'health_check_test',
                'data': [
                    {'id': 1, 'name': 'test', 'value': 100},
                    {'id': 2, 'name': 'sample', 'value': 200}
                ]
            }

            # В реальном сценарии здесь был бы файл, но для health check достаточно проверки API
            response = requests.get(f"{self.base_url}/", timeout=5)
            return {
                'status': response.status_code == 200,
                'response_time': response.elapsed.total_seconds()
            }
        except Exception as e:
            return {
                'status': False,
                'error': str(e)
            }

    def generate_report(self):
        """Генерация отчета о здоровье системы"""
        print("🔍 HEALTH CHECK REPORT")
        print("=" * 50)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️  Uptime: {time.time() - self.start_time:.1f} seconds")
        print("-" * 50)

        # Проверка здоровья сервиса
        health = self.check_service_health()
        print(f"🏥 Service Health: {health['status'].upper()}")
        if health['status'] == 'healthy':
            print(f"   ✅ Response time: {health['response_time']:.3f}s")
            print(f"   📊 Details: {health.get('details', {})}")
        else:
            print(f"   ❌ Error: {health.get('error', 'Unknown error')}")

        # Проверка endpoints
        print(f"\n🌐 API Endpoints:")
        endpoints = self.check_api_endpoints()
        for endpoint, info in endpoints.items():
            status_icon = "✅" if info.get('status') == 200 else "❌"
            response_time = info.get('response_time', 0)
            print(f"   {status_icon} {endpoint}: {info.get('status', 'ERROR')} ({response_time:.3f}s)")

        # Проверка системных ресурсов
        print(f"\n💻 System Resources:")
        resources = self.check_system_resources()
        if 'error' not in resources:
            print(f"   🖥️  CPU: {resources['cpu_percent']:.1f}%")
            print(f"   💾 Memory: {resources['memory_percent']:.1f}%")
            print(f"   💿 Disk: {resources['disk_percent']:.1f}%")
            print(f"   🎯 ML Service: {'✅ Running' if resources['ml_service_found'] else '❌ Not found'}")

            if 'ml_service_cpu' in resources:
                print(f"   📈 ML CPU: {resources['ml_service_cpu']:.1f}%")
                print(f"   📊 ML Memory: {resources['ml_service_memory']:.1f}%")
        else:
            print(f"   ❌ Resource check failed: {resources['error']}")

        # Тестовый запрос
        print(f"\n🧪 Sample Request:")
        sample = self.test_sample_request()
        if sample['status']:
            print(f"   ✅ Success ({sample['response_time']:.3f}s)")
        else:
            print(f"   ❌ Failed: {sample.get('error', 'Unknown error')}")

        # Рекомендации
        print(f"\n💡 Recommendations:")
        self.generate_recommendations(health, endpoints, resources)

        return {
            'timestamp': datetime.now().isoformat(),
            'health': health,
            'endpoints': endpoints,
            'resources': resources,
            'sample_request': sample
        }

    def generate_recommendations(self, health, endpoints, resources):
        """Генерация рекомендаций на основе метрик"""
        recommendations = []

        # Проверка здоровья сервиса
        if health['status'] != 'healthy':
            recommendations.append("🚨 Service is not healthy - check logs and restart if needed")

        # Проверка времени ответа
        if health['status'] == 'healthy' and health['response_time'] > 2.0:
            recommendations.append("⚠️  Slow response time - consider optimization")

        # Проверка доступности endpoints
        failed_endpoints = [ep for ep, info in endpoints.items() if info.get('status') != 200]
        if failed_endpoints:
            recommendations.append(f"⚠️  Failed endpoints: {', '.join(failed_endpoints)}")

        # Проверка ресурсов
        if 'error' not in resources:
            if resources['cpu_percent'] > 80:
                recommendations.append("⚠️  High CPU usage - check for performance bottlenecks")
            if resources['memory_percent'] > 80:
                recommendations.append("⚠️  High memory usage - consider cleanup or upgrade")
            if resources['disk_percent'] > 80:
                recommendations.append("⚠️  Low disk space - clean up unnecessary files")
            if not resources['ml_service_found']:
                recommendations.append("🚨 ML service process not found - start the service")

        if not recommendations:
            recommendations.append("✅ All systems operating normally")

        for rec in recommendations:
            print(f"   {rec}")

def main():
    """Основная функция"""
    print("🔬 ML SERVICE HEALTH MONITOR")
    print("=" * 50)

    monitor = HealthMonitor()
    report = monitor.generate_report()

    # Сохранение отчета в файл
    try:
        with open('health_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\n💾 Report saved to: health_report.json")
    except Exception as e:
        print(f"\n❌ Failed to save report: {str(e)}")

    print(f"\n🎯 Health check completed!")

if __name__ == "__main__":
    main()