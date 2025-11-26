import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_relationships import test_table_relationships
from tests.test_data_types import test_data_types_and_constraints
from tests.test_reports_correctness import test_weekly_report_correctness, test_report_data_consistency

def run_all_tests():
    """Запускает все тесты"""
    print("🧪 ЗАПУСК ТЕСТОВ ПРОЕКТА АНАЛИТИКИ")
    print("=" * 50)
    
    tests = [
        ("Связи между таблицами", test_table_relationships),
        ("Типы данных и ограничения", test_data_types_and_constraints),
        ("Корректность недельного отчета", test_weekly_report_correctness),
        ("Согласованность данных отчетов", test_report_data_consistency),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🔍 Запуск теста: {test_name}")
        if test_func():
            passed += 1
            print(f"✅ Тест '{test_name}' пройден")
        else:
            print(f"❌ Тест '{test_name}' не пройден")
    
    print(f"\n📊 ИТОГ: {passed}/{total} тестов пройдено")
    
    if passed == total:
        print("🎉 Все тесты пройдены успешно! Проект работает корректно.")
    else:
        print("💥 Некоторые тесты не пройдены. Требуется исправление.")
    
    return passed == total

if __name__ == "__main__":
    run_all_tests()