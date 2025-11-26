import psycopg2
import traceback
from datetime import datetime, timedelta
from database.config import get_connection_string

def show_weekly_report(weeks_back=8):
    """Показывает отчет из материализованного представления weekly_sales_report"""
    
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        query = """
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
        WHERE week_start >= %s
        ORDER BY week_start DESC, revenue_in_category DESC;
        """
        
        cutoff_date = datetime.now() - timedelta(weeks=weeks_back)
        
        cursor.execute(query, [cutoff_date])
        results = cursor.fetchall()
        
        print("📊 НЕДЕЛЬНЫЙ ОТЧЕТ ПО ПРОДАЖАМ")
        print("=" * 90)
        
        if not results:
            print("❌ Нет данных для отображения")
            return
        
        for row in results:
            print(f"\n🗓️  Неделя с: {row[0].strftime('%Y-%m-%d')}")
            print(f"   🏷️  Категория: {row[1]}")
            print(f"   📦 Заказов в категории: {row[2]:>4} | 👥 Клиентов в категории: {row[3]:>4}")
            print(f"   💰 Выручка в категории: ${row[4]:>10,.2f} | 📊 Средний чек в категории: ${row[6]:>8.2f}")
            print(f"   📦 Товаров в категории: {int(row[5]):>4} | 🏷️  Уникальных товаров в категории: {row[7]:>3}")
        
        print("\n" + "=" * 90)
        print("📈 СВОДНАЯ СТАТИСТИКА:")
        
        summary_query = """
        SELECT 
            COUNT(DISTINCT week_start) as weeks_count,
            SUM(orders_in_category) as total_orders_in_categories,
            SUM(revenue_in_category) as total_revenue_in_categories,
            AVG(avg_order_value_in_category) as overall_avg_order_in_categories,
            MAX(revenue_in_category) as best_category_week_revenue,
            SUM(items_sold_in_category) as total_items_sold_in_categories
        FROM weekly_sales_report
        WHERE week_start >= %s;
        """
        
        cursor.execute(summary_query, [cutoff_date])
        summary = cursor.fetchone()
        
        print(f"   📅 Период: {weeks_back} недель | Недель в отчете: {summary[0]}")
        print(f"   📦 Всего заказов по категориям: {summary[1]:>6}")
        print(f"   💰 Общая выручка по категориям: ${summary[2]:>12,.2f}")
        print(f"   📊 Средний чек по категориям: ${summary[3]:>8.2f}")
        print(f"   🏆 Лучшая неделя для категории: ${summary[4]:>10,.2f}")
        print(f"   📦 Всего товаров продано по категориям: {int(summary[5]):>6}")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Ошибка при генерации отчета: {e}")
    finally:
        if conn:
            conn.close()

def show_monthly_report(months_back=6):
    """Показывает отчет из материализованного представления monthly_sales_summary"""
    
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        query = """
        SELECT 
            month_start,
            year,
            month,
            total_orders,
            unique_customers,
            total_revenue,
            total_items_sold,
            avg_order_value
        FROM monthly_sales_summary
        WHERE month_start >= %s
        ORDER BY month_start DESC;
        """
        
        cutoff_date = datetime.now() - timedelta(days=months_back*30)

        cursor.execute(query, [cutoff_date])
        results = cursor.fetchall()
        
        print("\n📅 МЕСЯЧНЫЙ ОТЧЕТ ПО ПРОДАЖАМ")
        print("=" * 80)
        
        if not results:
            print("❌ Нет данных для отображения")
            return
        
        for row in results:
            year = int(row[1]) if row[1] else datetime.now().year
            month = int(row[2]) if row[2] else datetime.now().month
            month_name = datetime(year, month, 1).strftime('%B %Y')

            print(f"\n📅 {month_name}:")
            print(f"   📦 Заказов: {row[3]:>4} | 👥 Уникальных клиентов: {row[4]:>4}")
            print(f"   💰 Выручка: ${row[5]:>12,.2f} | 📊 Средний чек: ${row[7]:>8.2f}")
            print(f"   📦 Товаров продано: {int(row[6]) if row[6] else 0:>6}")
        
        print("\n" + "=" * 80)
        print("📈 АНАЛИЗ РОСТА (месяц к месяцу):")
        
        growth_query = """
        SELECT 
            month_start,
            total_revenue,
            LAG(total_revenue) OVER (ORDER BY month_start) as prev_month_revenue,
            CASE 
                WHEN LAG(total_revenue) OVER (ORDER BY month_start) IS NOT NULL THEN
                    ROUND(
                        (total_revenue - LAG(total_revenue) OVER (ORDER BY month_start)) / 
                        LAG(total_revenue) OVER (ORDER BY month_start) * 100, 1
                    )
                ELSE NULL
            END as growth_percent
        FROM monthly_sales_summary
        WHERE month_start >= %s
        ORDER BY month_start DESC;
        """
        
        cursor.execute(growth_query, [cutoff_date])
        growth_data = cursor.fetchall()
        
        for row in growth_data:
            month_start, revenue, prev_revenue, growth = row
            if growth is not None:
                month_str = month_start.strftime('%Y-%m')
                trend = "📈" if growth > 0 else "📉" if growth < 0 else "➡️"
                print(f"   {month_str}: ${revenue:>10,.2f} {trend} {growth:>+5.1f}%")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Ошибка при генерации месячного отчета: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

def show_category_analysis():
    """Анализ продаж по категориям"""
    
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        query = """
        SELECT 
            category,
            orders_count,
            items_sold,
            total_revenue,
            avg_product_price,
            unique_customers,
            ROUND(total_revenue / SUM(total_revenue) OVER() * 100, 1) as revenue_share
        FROM category_analysis
        ORDER BY total_revenue DESC;
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\n🏷️  АНАЛИЗ ПРОДАЖ ПО КАТЕГОРИЯМ")
        print("=" * 90)
        
        if not results:
            print("❌ Нет данных для отображения")
            return
        
        total_revenue = sum(row[3] for row in results)
        
        for row in results:
            category, orders_count, items_sold, revenue, avg_price, unique_customers, revenue_share = row
            print(f"\n📁 {category:>15}:")
            print(f"   💰 Выручка: ${revenue:>10,.2f} ({revenue_share:>4}% от общей)")
            print(f"   📦 Заказов: {orders_count:>4} | 🛒 Товаров: {items_sold:>5}")
            print(f"   👥 Клиентов: {unique_customers:>4} | 💵 Средняя цена: ${avg_price:>7.2f}")
        
        print(f"\n💰 ОБЩАЯ ВЫРУЧКА ПО ВСЕМ КАТЕГОРИЯМ: ${total_revenue:,.2f}")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Ошибка при анализе категорий: {e}")
    finally:
        if conn:
            conn.close()

def show_top_customers(limit=10):
    """Показывает топ клиентов по объему покупок"""
    
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        query = """
        SELECT 
            customer_name,
            email,
            city,
            country,
            total_orders,
            total_spent,
            avg_order_value,
            last_order_date
        FROM customer_analytics
        WHERE total_orders > 0
        ORDER BY total_spent DESC
        LIMIT %s;
        """
        
        cursor.execute(query, [limit])
        results = cursor.fetchall()
        
        print(f"\n👑 ТОП-{limit} КЛИЕНТОВ ПО ОБЪЕМУ ПОКУПОК")
        print("=" * 100)
        
        if not results:
            print("❌ Нет данных для отображения")
            return
        
        for i, row in enumerate(results, 1):
            customer_name, email, city, country, total_orders, total_spent, avg_order_value, last_order_date = row
            print(f"\n#{i:>2} {customer_name:>20} ({city}, {country})")
            print(f"   📧 {email}")
            print(f"   💰 Всего потрачено: ${total_spent:>10,.2f} | 📦 Заказов: {total_orders:>3}")
            print(f"   📊 Средний чек: ${avg_order_value:>8.2f} | 📅 Последний заказ: {last_order_date.strftime('%Y-%m-%d')}")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Ошибка при получении данных о клиентах: {e}")
    finally:
        if conn:
            conn.close()

def show_daily_sales_trend(days_back=30):
    """Показывает тренд ежедневных продаж"""
    
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        query = """
        SELECT 
            sale_date,
            orders_count,
            total_revenue,
            avg_order_value,
            unique_customers
        FROM daily_sales
        WHERE sale_date >= %s
        ORDER BY sale_date DESC;
        """
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        cursor.execute(query, [cutoff_date])
        results = cursor.fetchall()
        
        print(f"\n📈 ТРЕНД ЕЖЕДНЕВНЫХ ПРОДАЖ (последние {days_back} дней)")
        print("=" * 80)
        
        if not results:
            print("❌ Нет данных для отображения")
            return
        
        recent_days = results[:10]
        
        for row in recent_days:
            sale_date, orders_count, total_revenue, avg_order_value, unique_customers = row
            print(f"   📅 {sale_date.strftime('%Y-%m-%d')}: "
                  f"${total_revenue:>8,.2f} | {orders_count:>2} заказов | "
                  f"{unique_customers:>2} клиентов | чек ${avg_order_value:>6.2f}")
        
        total_revenue = sum(row[2] for row in results) 
        avg_daily_revenue = total_revenue / len(results) if results else 0
        
        best_day = max(results, key=lambda x: x[2]) if results else None
        
        print(f"\n📊 СТАТИСТИКА ЗА {days_back} ДНЕЙ:")
        print(f"   💰 Общая выручка: ${total_revenue:,.2f}")
        print(f"   📊 Средняя дневная выручка: ${avg_daily_revenue:,.2f}")
        if best_day:
            print(f"   🏆 Лучший день: {best_day[0].strftime('%Y-%m-%d')} (${best_day[2]:,.2f})")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Ошибка при анализе ежедневных продаж: {e}")
    finally:
        if conn:
            conn.close()

def performance_comparison():
    """Сравнение производительности материализованных vs обычных представлений"""
    
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        print("\n⚡ СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
        print("=" * 60)
        
        # Тест материализованного представления
        print("🔄 Тестирование материализованного представления...")
        cursor.execute("EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM weekly_sales_report;")
        mv_result = cursor.fetchone()[0][0]
        mv_time = mv_result['Execution Time']
        
        # Тест аналогичного запроса к базовым таблицам
        print("🔄 Тестирование запроса к базовым таблицам...")
        complex_query = """
        EXPLAIN (ANALYZE, FORMAT JSON) 
        SELECT 
            DATE_TRUNC('week', o.order_date) AS week_start,
            COUNT(DISTINCT o.id) AS total_orders,
            COUNT(DISTINCT o.user_id) AS unique_customers,
            SUM(o.total_amount) AS total_revenue
        FROM orders o
        JOIN order_items oi ON o.id = oi.order_id
        WHERE o.order_status = 'completed'
        GROUP BY DATE_TRUNC('week', o.order_date)
        ORDER BY week_start DESC;
        """
        cursor.execute(complex_query)
        direct_result = cursor.fetchone()[0][0]
        direct_time = direct_result['Execution Time']
        
        print(f"\n📊 РЕЗУЛЬТАТЫ:")
        print(f"   💾 Материализованное представление: {mv_time:.2f} ms")
        print(f"   🗄️  Прямой запрос к таблицам: {direct_time:.2f} ms")
        
        speedup = direct_time / mv_time if mv_time > 0 else 0
        print(f"   🚀 Ускорение: {speedup:.1f}x")
        
        cursor.close()
        
    except Exception as e:
        print(f"❌ Ошибка при тестировании производительности: {e}")
    finally:
        if conn:
            conn.close()

def show_comprehensive_report():
    """Комплексный отчет со всей аналитикой"""
    
    print("🎯 КОМПЛЕКСНЫЙ АНАЛИТИЧЕСКИЙ ОТЧЕТ")
    print("=" * 100)
    
    show_weekly_report(weeks_back=12)
    show_monthly_report(months_back=6)
    show_category_analysis()
    show_top_customers(limit=8)
    show_daily_sales_trend(days_back=30)
    performance_comparison()

if __name__ == "__main__":
    show_comprehensive_report()