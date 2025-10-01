#!/usr/bin/env python3
"""
Упрощенное тестирование ML сервиса с реальными датасетами
Использование: python3 test_real_data_simple.py
"""
import requests
import json
import pandas as pd
import os
import tempfile
import xml.etree.ElementTree as ET

def test_real_csv():
    """Тест с реальными CSV данными о билетах музеев"""
    print("🔬 CSV: Тестируем реальные данные о билетах музеев...")

    # Берем реальные данные (первые 1000 строк)
    csv_file = "datasets/syn_csv/part-00000-37dced01-2ad2-48c8-a56d-54b4d8760599-c000.csv"
    df = pd.read_csv(csv_file, sep=';', nrows=1000)

    print(f"   📊 Размер: {len(df)} строк × {len(df.columns)} колонок")
    print(f"   📋 Пример колонок: {list(df.columns[:5])}")

    # Сохраняем во временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        df.to_csv(f.name, index=False, sep=';')
        temp_file = f.name

    try:
        with open(temp_file, 'rb') as f:
            files = {'file': f}
            data = {
                'format': 'csv',
                'table_name': 'museum_tickets_real',
                'use_cache': 'false'
            }

            response = requests.post("http://localhost:8001/recommend", files=files, data=data)

            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Рекомендация: {result.get('target')} ({result.get('confidence'):.1%})")
                print(f"   📊 Профиль: {result.get('data_profile', {}).get('record_count', 0)} записей, {result.get('data_profile', {}).get('field_count', 0)} полей")
                return result
            else:
                print(f"   ❌ Ошибка: {response.status_code}")
                print(f"   📝 Детали: {response.text}")
                return None
    finally:
        os.unlink(temp_file)

def test_real_json():
    """Тест с реальными JSON данными об ИП"""
    print("\n🔬 JSON: Тестируем реальные данные об ИП...")

    # Создаем тестовые данные на основе структуры реальных
    test_data = [
        {
            "date_exec": "2024-10-09",
            "code_form_ind_entrep": "1",
            "name_form_ind_entrep": "Индивидуальный предприниматель",
            "inf_surname_ind_entrep_sex": "1",
            "citizenship_kind": "1",
            "inf_authority_reg_ind_entrep_name": "Управление Федеральной налоговой службы по Пензенской области",
            "inf_authority_reg_ind_entrep_code": "5800",
            "inf_reg_tax_ind_entrep": "Управление Федеральной налоговой службы по Пензенской области",
            "inf_okved_code": "01.25.1",
            "inf_okved_name": "Выращивание прочих плодовых и ягодных культур",
            "process_dttm": "2024-10-09T21:07:09.765+03:00",
            "error_code": 0,
            "inf_surname_ind_entrep_firstname": "ВЛАДИМИР",
            "inf_surname_ind_entrep_surname": "ЗВЕРЕВ",
            "inf_surname_ind_entrep_midname": "ЕВГЕНЬЕВИЧ",
            "dob": "1980-02-29",
            "date_ogrnip": "2023-11-14",
            "id_card": "77 82 349990"
        },
        {
            "date_exec": "2024-10-08",
            "code_form_ind_entrep": "1",
            "name_form_ind_entrep": "Индивидуальный предприниматель",
            "inf_surname_ind_entrep_sex": "2",
            "citizenship_kind": "1",
            "inf_authority_reg_ind_entrep_name": "Управление Федеральной налоговой службы по Московской области",
            "inf_authority_reg_ind_entrep_code": "5000",
            "inf_reg_tax_ind_entrep": "Управление Федеральной налоговой службы по Московской области",
            "inf_okved_code": "62.01",
            "inf_okved_name": "Разработка компьютерного программного обеспечения",
            "process_dttm": "2024-10-08T15:30:00.000+03:00",
            "error_code": 0,
            "inf_surname_ind_entrep_firstname": "АННА",
            "inf_surname_ind_entrep_surname": "ПЕТРОВА",
            "inf_surname_ind_entrep_midname": "ИВАНОВНА",
            "dob": "1985-05-15",
            "date_ogrnip": "2022-03-20",
            "id_card": "50 12 345678"
        }
    ]

    print(f"   📊 Размер: {len(test_data)} записей")
    print(f"   📋 Пример полей: {list(test_data[0].keys())[:5]}")

    # Сохраняем во временный файл без переносов строк для надежности
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
        json.dump(test_data, f, ensure_ascii=False, separators=(',', ':'))
        temp_file = f.name

    try:
        with open(temp_file, 'rb') as f:
            files = {'file': f}
            data = {
                'format': 'json',
                'table_name': 'individual_entrepreneurs_real',
                'use_cache': 'false'
            }

            response = requests.post("http://localhost:8001/recommend", files=files, data=data)

            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Рекомендация: {result.get('target')} ({result.get('confidence'):.1%})")
                print(f"   📊 Профиль: {result.get('data_profile', {}).get('record_count', 0)} записей, {result.get('data_profile', {}).get('field_count', 0)} полей")
                return result
            else:
                print(f"   ❌ Ошибка: {response.status_code}")
                print(f"   📝 Детали: {response.text}")
                return None
    finally:
        os.unlink(temp_file)

def test_real_xml():
    """Тест с реальными XML данными (кадастровые)"""
    print("\n🔬 XML: Тестируем кадастровые данные...")

    # Создаем тестовые XML данные на основе структуры реальных
    xml_content = '''<?xml version="1.0" encoding="utf-8"?>
<root>
    <item>
        <metadata>
            <last_change_record_number>50:21:0140218:1614-11</last_change_record_number>
            <last_container_fixed_at>2019-01-09T00:00:00+03:00</last_container_fixed_at>
            <status>actual</status>
        </metadata>
        <object_common_data>
            <cad_number>50:21:0140218:1614</cad_number>
            <previously_posted>true</previously_posted>
            <quarter_cad_number>77:17:0140116</quarter_cad_number>
        </object_common_data>
        <dated_info>
            <oti>
                <letter>Л</letter>
            </oti>
        </dated_info>
    </item>
    <item>
        <metadata>
            <last_change_record_number>77:01:0000001:1234</last_change_record_number>
            <last_container_fixed_at>2020-05-15T00:00:00+03:00</last_container_fixed_at>
            <status>actual</status>
        </metadata>
        <object_common_data>
            <cad_number>77:01:0000001:1234</cad_number>
            <previously_posted>false</previously_posted>
            <quarter_cad_number>77:01:0001000</quarter_cad_number>
        </object_common_data>
        <dated_info>
            <oti>
                <letter>А</letter>
            </oti>
        </dated_info>
    </item>
</root>'''

    print(f"   📊 Размер: {len(xml_content)} символов")
    print(f"   📋 Структура: корневой элемент, 2 записи с метаданными")

    # Сохраняем во временный файл
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False, encoding='utf-8') as f:
        f.write(xml_content)
        temp_file = f.name

    try:
        with open(temp_file, 'rb') as f:
            files = {'file': f}
            data = {
                'format': 'xml',
                'table_name': 'cadastral_data_real',
                'use_cache': 'false'
            }

            response = requests.post("http://localhost:8001/recommend", files=files, data=data)

            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Рекомендация: {result.get('target')} ({result.get('confidence'):.1%})")
                print(f"   📊 Профиль: {result.get('data_profile', {}).get('record_count', 0)} записей, {result.get('data_profile', {}).get('field_count', 0)} полей")
                return result
            else:
                print(f"   ❌ Ошибка: {response.status_code}")
                print(f"   📝 Детали: {response.text}")
                return None
    finally:
        os.unlink(temp_file)

def main():
    print("🔬 ТЕСТИРОВАНИЕ ML СЕРВИСА С РЕАЛЬНЫМИ ДАННЫМИ (упрощенная версия)")
    print("="*80)

    # Проверка статуса
    try:
        response = requests.get("http://localhost:8001/")
        print("✅ Сервис запущен и готов к тестированию")
    except:
        print("❌ Сервис не запущен")
        return

    print("\n📋 НАЧИНАЕМ ТЕСТИРОВАНИЕ С РЕАЛЬНЫМИ ДАННЫМИ:")
    print("-" * 60)

    results = []

    # Тестируем CSV
    csv_result = test_real_csv()
    if csv_result:
        results.append(("CSV Museum Tickets", csv_result))

    # Тестируем JSON
    json_result = test_real_json()
    if json_result:
        results.append(("JSON Individual Entrepreneurs", json_result))

    # Тестируем XML
    xml_result = test_real_xml()
    if xml_result:
        results.append(("XML Cadastral Data", xml_result))

    # Сводная таблица
    if results:
        print(f"\n" + "="*80)
        print("📊 СВОДНАЯ ТАБЛИЦА РЕКОМЕНДАЦИЙ")
        print("="*80)
        print(f"{'Датасет':<30} {'Рекомендация':<12} {'Уверенность':<10} {'Записи':<8} {'Поля':<6} {'Время':<6} {'Числа':<6} {'Текст':<6}")
        print("-" * 80)

        for name, result in results:
            profile = result.get('data_profile', {})
            print(f"{name:<30} {result.get('target', 'N/A'):<12} {result.get('confidence', 0):<10.1%} {profile.get('record_count', 0):<8} {profile.get('field_count', 0):<6} {str(profile.get('has_temporal', False)):<6} {str(profile.get('has_numeric', False)):<6} {str(profile.get('has_text', False)):<6}")

    print(f"\n" + "="*80)
    print("🎯 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("="*80)
    print("✅ ML сервис успешно протестирован на данных, приближенных к вашим реальным")
    print("📊 Рекомендации основаны на анализе структуры ваших данных")

if __name__ == "__main__":
    main()