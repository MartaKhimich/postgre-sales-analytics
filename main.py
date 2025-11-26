import psycopg2
from database.init_database import init_database
from scripts.generate_data import generate_sample_data, verify_data_integrity
from scripts.create_views import create_analytical_views, refresh_materialized_views, show_view_info
from reports.weekly_sales_report import show_comprehensive_report
from database.config import get_connection_string


def check_existing_data():
    """Проверка на наличие данных в базе"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM users")
        users_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM orders")
        orders_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return users_count > 10 and orders_count > 10
        
    except Exception as e:
        print(f"❌ Ошибка при проверке данных: {e}")
        return False

def clear_existing_data():
    """Очищение всех тестовых данных"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        clear_sql = """
        DELETE FROM order_items;
        DELETE FROM orders;
        DELETE FROM products;
        DELETE FROM users;
        """
        
        cursor.execute(clear_sql)
        conn.commit()
        
        print("✅ Старые данные очищены")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при очистке данных: {e}")

def main():
    print("🚀 ЗАПУСК ПРОЕКТА АНАЛИТИКИ ПРОДАЖ")
    print("=" * 50)
    
    # 1. Создание таблиц
    init_database()
    
    # 2. Интерактивный запрос о генерации тестовых данных
    has_data = check_existing_data()

    if has_data:
        print("✅ В базе уже есть данные")
        regenerate = input("Сгенерировать новые данные? (старые будут удалены) [y/N]: ").strip().lower()
        if regenerate == 'y':
            print("🗑️  Очистка старых данных...")
            clear_existing_data()
            has_data = False

    if not has_data:
        print("📝 Генерация тестовых данных...")
        generate_sample_data()
        verify_data_integrity()
    else:
        print("✅ Используем существующие данные")
    
    # 3. Создание аналитические представления
    create_analytical_views()
    
    # 4. Обновление материализованных представлений
    refresh_materialized_views()
    
    # 5. Отображение информации о представлениях
    show_view_info()
    
    # 6. Комплексный отчет
    show_comprehensive_report()

if __name__ == "__main__":
    main()