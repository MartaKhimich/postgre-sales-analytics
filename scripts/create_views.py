import psycopg2
from database.config import get_connection_string

def create_analytical_views():
    """Создание всех аналитических представлений и материализованных представлений"""
    
    regular_views_sql = """
    -- Представление для ежедневных продаж
    DROP VIEW IF EXISTS daily_sales;
    CREATE VIEW daily_sales AS
    SELECT 
        DATE(o.order_date) as sale_date,
        COUNT(DISTINCT o.id) as orders_count,
        SUM(o.total_amount) as total_revenue,
        ROUND(AVG(o.total_amount)::numeric, 2) as avg_order_value,
        COUNT(DISTINCT o.user_id) as unique_customers
    FROM orders o
    WHERE o.order_status = 'completed'
    GROUP BY DATE(o.order_date)
    ORDER BY sale_date DESC;

    -- Представление для анализа по категориям
    DROP VIEW IF EXISTS category_analysis;
    CREATE VIEW category_analysis AS
    SELECT 
        p.category,
        COUNT(DISTINCT o.id) as orders_count,
        SUM(oi.quantity) as items_sold,
        SUM(oi.subtotal) as total_revenue,
        ROUND(AVG(p.price)::numeric, 2) as avg_product_price,
        COUNT(DISTINCT o.user_id) as unique_customers
    FROM products p
    JOIN order_items oi ON p.id = oi.product_id
    JOIN orders o ON oi.order_id = o.id
    WHERE o.order_status = 'completed'
    GROUP BY p.category
    ORDER BY total_revenue DESC;

    -- Представление для клиентской аналитики
    DROP VIEW IF EXISTS customer_analytics;
    CREATE VIEW customer_analytics AS
    SELECT 
        u.id as user_id,
        u.first_name || ' ' || u.last_name as customer_name,
        u.email,
        u.city,
        u.country,
        COUNT(o.id) as total_orders,
        SUM(o.total_amount) as total_spent,
        ROUND(AVG(o.total_amount)::numeric, 2) as avg_order_value,
        MAX(o.order_date) as last_order_date
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id AND o.order_status = 'completed'
    GROUP BY u.id, u.first_name, u.last_name, u.email, u.city, u.country
    ORDER BY total_spent DESC NULLS LAST;

    -- Представление для детальной информации о заказах
    DROP VIEW IF EXISTS order_details;
    CREATE VIEW order_details AS
    SELECT 
        o.id as order_id,
        u.first_name || ' ' || u.last_name as customer_name,
        o.order_date,
        o.total_amount,
        o.order_status,
        COUNT(oi.id) as items_count,
        STRING_AGG(p.title || ' (x' || oi.quantity || ')', ', ') as products
    FROM orders o
    JOIN users u ON o.user_id = u.id
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    GROUP BY o.id, u.first_name, u.last_name, o.order_date, o.total_amount, o.order_status
    ORDER BY o.order_date DESC;
    """

    materialized_views_sql = """
    -- Материализованное представление для недельных отчетов по продажам
    DROP MATERIALIZED VIEW IF EXISTS weekly_sales_report;
    CREATE MATERIALIZED VIEW weekly_sales_report AS
    SELECT 
        DATE_TRUNC('week', o.order_date) AS week_start,
        p.category AS top_category,
    
        -- Количество заказов в ЭТОЙ КАТЕГОРИИ на этой неделе
        COUNT(DISTINCT o.id) AS orders_in_category,
    
        -- Количество уникальных клиентов купивших товары ЭТОЙ КАТЕГОРИИ на этой неделе  
        COUNT(DISTINCT o.user_id) AS unique_customers_in_category,
    
        -- Выручка от товаров ЭТОЙ КАТЕГОРИИ на этой неделе
        SUM(oi.subtotal) AS revenue_in_category,
    
        -- Количество товаров ЭТОЙ КАТЕГОРИИ проданных на этой неделе
        SUM(oi.quantity) AS items_sold_in_category,
    
        -- Средняя стоимость заказа в ЭТОЙ КАТЕГОРИИ на этой неделе
        ROUND(
            CASE 
                WHEN COUNT(DISTINCT o.id) > 0 THEN SUM(oi.subtotal) / COUNT(DISTINCT o.id)
                ELSE 0 
            END::numeric, 2
        ) AS avg_order_value_in_category,
    
        -- Количество уникальных товаров в ЭТОЙ КАТЕГОРИИ на этой неделе
        COUNT(DISTINCT oi.product_id) AS unique_products_in_category
    FROM orders o
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    WHERE o.order_status = 'completed'
    GROUP BY DATE_TRUNC('week', o.order_date), p.category
    ORDER BY week_start DESC, revenue_in_category DESC;

    
    -- Материализованное представление для статистики за месяц
    DROP MATERIALIZED VIEW IF EXISTS monthly_sales_summary;
    CREATE MATERIALIZED VIEW monthly_sales_summary AS
    SELECT 
        DATE_TRUNC('month', o.order_date) AS month_start,
        EXTRACT(YEAR FROM o.order_date) AS year,
        EXTRACT(MONTH FROM o.order_date) AS month,
        COUNT(DISTINCT o.id) AS total_orders,
        COUNT(DISTINCT o.user_id) AS unique_customers,
        SUM(o.total_amount) AS total_revenue,
        SUM(oi.quantity) AS total_items_sold,
        ROUND(AVG(o.total_amount)::numeric, 2) AS avg_order_value
    FROM orders o
    JOIN order_items oi ON o.id = oi.order_id
    WHERE o.order_status = 'completed'
    GROUP BY DATE_TRUNC('month', o.order_date), year, month
    ORDER BY month_start DESC;
    """

    indexes_sql = """
    -- Индекс для weekly_sales_report
    CREATE UNIQUE INDEX IF NOT EXISTS idx_weekly_sales_week 
    ON weekly_sales_report (week_start, top_category);

    -- Индекс для monthly_sales_summary  
    CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_sales_month
    ON monthly_sales_summary (month_start);

    -- Дополнительные индексы для ускорения запросов
    CREATE INDEX IF NOT EXISTS idx_weekly_sales_revenue 
    ON weekly_sales_report (revenue_in_category DESC);

    CREATE INDEX IF NOT EXISTS idx_monthly_sales_revenue
    ON monthly_sales_summary (total_revenue DESC);
    """

    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("🔄 Создание аналитических представлений...")
        
        cursor.execute(regular_views_sql)
        print("   ✅ Представления созданы")
        
        cursor.execute(materialized_views_sql)
        print("   ✅ Материализованные представления созданы")
        
        cursor.execute(indexes_sql)
        print("   ✅ Индексы для материализованных представлений созданы")
        
        conn.commit()
        print("🎉 Все представления успешно созданы!")
        
    except Exception as e:
        print(f"❌ Ошибка при создании представлений: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def refresh_materialized_views():
    """Обновление всех материализованных представлений"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("🔄 Обновление материализованных представлений...")
        
        cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY weekly_sales_report")
        print("   ✅ weekly_sales_report обновлено")
        
        cursor.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY monthly_sales_summary") 
        print("   ✅ monthly_sales_summary обновлено")
        
        conn.commit()
        print("🎉 Все материализованные представления обновлены!")
        
    except Exception as e:
        print(f"❌ Ошибка при обновлении представлений: {e}")
        try:
            conn.rollback()
            cursor.execute("REFRESH MATERIALIZED VIEW weekly_sales_report")
            cursor.execute("REFRESH MATERIALIZED VIEW monthly_sales_summary")
            conn.commit()
            print("✅ Представления обновлены (без CONCURRENTLY)")
        except Exception as e2:
            print(f"❌ Критическая ошибка: {e2}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def drop_all_views():
    """Удаление всех представлений (для пересоздания)"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("🗑️  Удаление всех представлений...")
        
        drop_sql = """
        DROP VIEW IF EXISTS daily_sales CASCADE;
        DROP VIEW IF EXISTS category_analysis CASCADE;
        DROP VIEW IF EXISTS customer_analytics CASCADE;
        DROP VIEW IF EXISTS order_details CASCADE;
        DROP MATERIALIZED VIEW IF EXISTS weekly_sales_report CASCADE;
        DROP MATERIALIZED VIEW IF EXISTS monthly_sales_summary CASCADE;
        """
        
        cursor.execute(drop_sql)
        conn.commit()
        print("✅ Все представления удалены!")
        
    except Exception as e:
        print(f"❌ Ошибка при удалении представлений: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def show_view_info():
    """Отображение информации о созданных представлениях"""
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("📊 ИНФОРМАЦИЯ О ПРЕДСТАВЛЕНИЯХ:")
        
        cursor.execute("""
        SELECT table_name, table_type 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'VIEW'
        ORDER BY table_name;
        """)
        
        views = cursor.fetchall()
        print("\n👁️  ОБЫЧНЫЕ ПРЕДСТАВЛЕНИЯ:")
        for view_name, view_type in views:
            print(f"   📋 {view_name}")
        
        cursor.execute("""
        SELECT matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'public'
        ORDER BY matviewname;
        """)
        
        materialized_views = cursor.fetchall()
        print("\n💾 МАТЕРИАЛИЗОВАННЫЕ ПРЕДСТАВЛЕНИЯ:")
        for mv_name in materialized_views:
            print(f"   💽 {mv_name[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM weekly_sales_report")
        weekly_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM monthly_sales_summary")
        monthly_count = cursor.fetchone()[0]
        
        print(f"\n📈 СТАТИСТИКА:")
        print(f"   📅 Записей в weekly_sales_report: {weekly_count}")
        print(f"   📊 Записей в monthly_sales_summary: {monthly_count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка при получении информации: {e}")

if __name__ == "__main__":
    create_analytical_views()
    refresh_materialized_views()
    show_view_info()