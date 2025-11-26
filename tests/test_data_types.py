import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from database.config import get_connection_string

def test_data_types_and_constraints():
    """Проверяем типы данных и ограничения"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("✅ ТЕСТ ТИПОВ ДАННЫХ И ОГРАНИЧЕНИЙ:")
        
        # 1. Проверка отрицательных цен
        cursor.execute("SELECT COUNT(*) FROM products WHERE price < 0")
        negative_prices = cursor.fetchone()[0]
        assert negative_prices == 0, f"Найдены товары с отрицательной ценой: {negative_prices}"
        print("   ✅ Нет товаров с отрицательной ценой")
        
        # 2. Проверка отрицательных количеств
        cursor.execute("SELECT COUNT(*) FROM order_items WHERE quantity < 0")
        negative_quantities = cursor.fetchone()[0]
        assert negative_quantities == 0, f"Найдены отрицательные количества: {negative_quantities}"
        print("   ✅ Нет отрицательных количеств товаров")
        
        # 3. Проверка корректности calculated subtotal
        cursor.execute("""
        SELECT COUNT(*) FROM order_items 
        WHERE ABS(subtotal - (quantity * unit_price)) > 0.01
        """)
        incorrect_subtotals = cursor.fetchone()[0]
        assert incorrect_subtotals == 0, f"Найдены некорректные subtotal: {incorrect_subtotals}"
        print("   ✅ Все subtotal рассчитаны корректно")
        
        # 4. Проверка типов данных в weekly_sales_report
        cursor.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN week_start IS NULL THEN 1 END) as null_weeks,
            COUNT(CASE WHEN top_category IS NULL THEN 1 END) as null_categories,
            COUNT(CASE WHEN revenue_in_category < 0 THEN 1 END) as negative_revenue
        FROM weekly_sales_report
        """)
        stats = cursor.fetchone()
        
        assert stats[0] > 0, "Нет данных в weekly_sales_report"
        assert stats[1] == 0, "Есть записи с NULL week_start"
        assert stats[2] == 0, "Есть записи с NULL категориями"
        assert stats[3] == 0, "Есть записи с отрицательной выручкой"
        print("   ✅ Типы данных в weekly_sales_report корректны")
        
        cursor.close()
        conn.close()
        print("✅ Все типы данных и ограничения корректны")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте типов данных: {e}")
        return False

if __name__ == "__main__":
    test_data_types_and_constraints()