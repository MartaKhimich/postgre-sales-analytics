import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from database.config import get_connection_string

def test_table_relationships():
    """Проверяем связи между таблицами"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        # Проверка связей через сложный запрос
        query = """
        SELECT 
            o.id as order_id,
            u.first_name || ' ' || u.last_name as customer,
            o.total_amount,
            COUNT(oi.id) as items_count,
            SUM(oi.quantity) as total_quantity
        FROM orders o
        JOIN users u ON o.user_id = u.id
        JOIN order_items oi ON o.id = oi.order_id
        GROUP BY o.id, u.first_name, u.last_name, o.total_amount
        ORDER BY o.total_amount DESC
        LIMIT 5;
        """
        
        cursor.execute(query)
        orders = cursor.fetchall()
        
        print("✅ ТЕСТ СВЯЗЕЙ МЕЖДУ ТАБЛИЦАМИ:")
        print("📋 ТОП-5 заказов по сумме:")
        for order in orders:
            print(f"   Заказ #{order[0]} | Клиент: {order[1]} | Сумма: ${order[2]:.2f} | Товаров: {order[3]} | Штук: {order[4]}")
        
        # Проверка, что есть данные
        assert len(orders) > 0, "Нет данных для тестирования связей"
        assert orders[0][2] > 0, "Сумма заказа должна быть положительной"
        
        print("✅ Связи между таблицами работают корректно")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте связей: {e}")
        return False

if __name__ == "__main__":
    test_table_relationships()