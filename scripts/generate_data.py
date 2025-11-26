import psycopg2
from faker import Faker
import random
import traceback
from datetime import datetime, timedelta
from database.config import get_connection_string

fake = Faker()

def generate_sample_data():
    """Генерация тестовых данных"""
    
    conn = psycopg2.connect(get_connection_string())
    cursor = conn.cursor()
    
    try:
        print("👥 Генерация пользователей...")
        users = []
        for _ in range(100):
            users.append((
                fake.first_name()[:45], 
                fake.last_name()[:45],
                fake.unique.email()[:95],  
                fake.country()[:45],
                fake.city()[:45]
            ))
        
        cursor.executemany(
            "INSERT INTO users (first_name, last_name, email, country, city) VALUES (%s, %s, %s, %s, %s)",
            users
        )

        print("📦 Генерация продуктов...")
        categories = ['Electronics', 'Books', 'Clothing', 'Home & Garden', 'Sports']
        products = []
        for _ in range(50):
            products.append((
                fake.catch_phrase()[:195],
                round(random.uniform(10, 1000), 2),
                random.choice(categories)
            ))
        
        cursor.executemany(
            "INSERT INTO products (title, price, category) VALUES (%s, %s, %s)",
            products
        )

        cursor.execute("SELECT id FROM users ORDER BY id")
        user_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT id FROM products ORDER BY id") 
        product_ids = [row[0] for row in cursor.fetchall()]
        
        print(f"📊 Сгенерировано пользователей: {len(user_ids)}")
        print(f"📊 Сгенерировано продуктов: {len(product_ids)}")

        print("🛒 Генерация заказов...")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=90)

        cursor.execute("SELECT id, price FROM products")
        product_prices = {row[0]: row[1] for row in cursor.fetchall()}
        
        for order_counter in range(1, 201):
            user_id = random.choice(user_ids)
            order_date = fake.date_time_between(start_date=start_date, end_date=end_date)
            
            cursor.execute(
                "INSERT INTO orders (user_id, order_date, total_amount, order_status) VALUES (%s, %s, %s, %s) RETURNING id",
                (user_id, order_date, 0, random.choice(['completed', 'completed', 'processing', 'cancelled']))
            )
            order_id = cursor.fetchone()[0]
            
            order_total = 0
            num_items = random.randint(1, 4)
            
            for _ in range(num_items):
                product_id = random.choice(product_ids)
                quantity = random.randint(1, 3)
                
                unit_price = product_prices[product_id]
                
                cursor.execute(
                    "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                    (order_id, product_id, quantity, unit_price)
                )
                
                order_total += quantity * unit_price
            
            cursor.execute(
                "UPDATE orders SET total_amount = %s WHERE id = %s",
                (round(order_total, 2), order_id)
            )
            
            if order_counter % 50 == 0:
                print(f"   Создано заказов: {order_counter}/200")
        
        print("🔍 Проверка целостности данных...")
        cursor.execute("""
            SELECT COUNT(*) FROM orders o 
            WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = o.user_id)
        """)
        orphaned_orders = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_items oi 
            WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.id = oi.order_id)
            OR NOT EXISTS (SELECT 1 FROM products p WHERE p.id = oi.product_id)
        """)
        orphaned_items = cursor.fetchone()[0]
        
        print(f"   Найдено orphaned orders: {orphaned_orders}")
        print(f"   Найдено orphaned order_items: {orphaned_items}")
        
        conn.commit()
        print("✅ Тестовые данные успешно сгенерированы!")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка при генерации данных: {e}")
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

def verify_data_integrity():
    """Дополнительная проверка целостности данных"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("\n🔍 ДЕТАЛЬНАЯ ПРОВЕРКА ЦЕЛОСТНОСТИ:")
        
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM products") 
        product_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM orders")
        order_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM order_items")
        order_items_count = cursor.fetchone()[0]
        
        print(f"   👥 Пользователей: {user_count}")
        print(f"   📦 Продуктов: {product_count}")
        print(f"   🛒 Заказов: {order_count}")
        print(f"   📋 Элементов заказов: {order_items_count}")
        
        cursor.execute("""
        SELECT COUNT(*) FROM order_items 
        WHERE subtotal != (quantity * unit_price)
        """)
        incorrect_subtotals = cursor.fetchone()[0]
        print(f"   ✅ Корректных subtotal: {order_items_count - incorrect_subtotals}")
        print(f"   ❌ Некорректных subtotal: {incorrect_subtotals}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")

if __name__ == "__main__":
    generate_sample_data()
    verify_data_integrity()