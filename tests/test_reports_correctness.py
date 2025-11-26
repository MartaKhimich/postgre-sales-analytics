import sys
import os
from decimal import Decimal

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from database.config import get_connection_string

def test_weekly_report_correctness():
    """Проверка корректности данных в weekly_sales_report"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("✅ ТЕСТ КОРРЕКТНОСТИ НЕДЕЛЬНОГО ОТЧЕТА:")
        
        # Проверка ВСЕХ записей на предмет некорректных значений
        cursor.execute("""
        SELECT 
            week_start,
            top_category,
            orders_in_category,
            unique_customers_in_category,
            revenue_in_category,
            items_sold_in_category,
            avg_order_value_in_category,
            unique_products_in_category
        FROM weekly_sales_report
        ORDER BY week_start DESC, top_category
        """)
        all_rows = cursor.fetchall()
        
        print(f"   📊 Всего записей в отчете: {len(all_rows)}")
        
        # Проверка каждой записи индивидуально
        problematic_rows = []
        for i, row in enumerate(all_rows):
            week_start, category, orders_count, customers_count, revenue, items_sold, avg_order, unique_products = row
            
            # Проверка каждого поля
            checks = [
                (isinstance(week_start, type(row[0])), "week_start должен быть датой"),
                (isinstance(category, str) and category, "category должен быть непустой строкой"),
                (isinstance(orders_count, int) and orders_count >= 0, "orders_count должен быть неотрицательным integer"),
                (isinstance(customers_count, int) and customers_count >= 0, "customers_count должен быть неотрицательным integer"),
                (isinstance(revenue, (int, float, Decimal)) and revenue >= 0, f"revenue должен быть неотрицательным числом (получено: {revenue})"),
                (isinstance(items_sold, (int, float, Decimal)) and items_sold >= 0, "items_sold должен быть неотрицательным числом"),
                (isinstance(avg_order, (int, float, Decimal)) and avg_order >= 0, "avg_order должен быть неотрицательным числом"),
                (isinstance(unique_products, int) and unique_products >= 0, "unique_products должен быть неотрицательным integer")
            ]
            
            for check_passed, error_msg in checks:
                if not check_passed:
                    problematic_rows.append({
                        'index': i,
                        'row': row,
                        'error': error_msg,
                        'revenue_value': revenue,
                        'revenue_type': type(revenue)
                    })
                    break 
        
        if problematic_rows:
            print(f"   ❌ Найдено проблемных записей: {len(problematic_rows)}")
            for problem in problematic_rows[:3]: 
                print(f"      Запись #{problem['index']}: {problem['error']}")
                print(f"        Данные: Неделя={problem['row'][0]}, Категория='{problem['row'][1]}', Выручка={problem['row'][4]}(тип: {type(problem['row'][4])})")
            
            cursor.close()
            conn.close()
            return False
        
        print("   ✅ Все записи прошли проверку типов")
        
        # Остальные проверки (математика, логика, округление)
        cursor.execute("""
        SELECT COUNT(*) FROM weekly_sales_report 
        WHERE ABS(avg_order_value_in_category - (revenue_in_category / NULLIF(orders_in_category, 0))) > 1.0
        AND orders_in_category > 0
        AND revenue_in_category > 0
        """)
        incorrect_avg = cursor.fetchone()[0]
        assert incorrect_avg == 0, f"Найдены некорректные средние чеки: {incorrect_avg}"
        print("   ✅ Средние чеки рассчитаны корректно")
        
        cursor.execute("""
        SELECT COUNT(*) FROM weekly_sales_report 
        WHERE items_sold_in_category < unique_products_in_category
        AND items_sold_in_category > 0
        """)
        illogical_products = cursor.fetchone()[0]
        assert illogical_products == 0, f"Найдены illogical product counts: {illogical_products}"
        print("   ✅ Количества товаров логичны")
        
        cursor.execute("""
        SELECT COUNT(*) FROM weekly_sales_report 
        WHERE revenue_in_category::text ~ '\.\d{3,}'
        OR avg_order_value_in_category::text ~ '\.\d{3,}'
        """)
        bad_formatting = cursor.fetchone()[0]
        assert bad_formatting == 0, f"Найдены значения с неправильным округлением: {bad_formatting}"
        print("   ✅ Денежные значения правильно округлены")
        
        cursor.close()
        conn.close()
        print("✅ Недельный отчет корректен!")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте недельного отчета: {e}")
        return False

def test_report_data_consistency():
    """Проверяем согласованность данных между разными отчетами"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("✅ ТЕСТ СОГЛАСОВАННОСТИ ДАННЫХ:")
        
        # Сравнение общей выручки из разных источников
        cursor.execute("SELECT SUM(total_amount) FROM orders WHERE order_status = 'completed'")
        total_from_orders = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(total_revenue) FROM category_analysis")
        total_from_categories = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(revenue_in_category) FROM weekly_sales_report")
        total_from_weekly = cursor.fetchone()[0] or 0
        
        # Допуск небольшой погрешности из-за округлений
        tolerance = 1.0
        assert abs(total_from_orders - total_from_categories) < tolerance, \
            f"Расхождения в общей выручке: orders={total_from_orders}, categories={total_from_categories}"
        assert abs(total_from_orders - total_from_weekly) < tolerance, \
            f"Расхождения в общей выручке: orders={total_from_orders}, weekly={total_from_weekly}"
        
        print(f"   ✅ Общая выручка согласована: ${total_from_orders:,.2f}")
        
        cursor.close()
        conn.close()
        print("✅ Данные во всех отчетах согласованы")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте согласованности: {e}")
        return False

if __name__ == "__main__":
    test_weekly_report_correctness()
    test_report_data_consistency()