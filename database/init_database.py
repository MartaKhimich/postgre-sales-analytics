import psycopg2
from database.config import get_connection_string

def init_database():
    """Инициализация базовых таблиц"""
    
    schema_sql = """
    -- Таблица пользователей
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        country VARCHAR(50),
        city VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Таблица продуктов
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        title VARCHAR(200) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        category VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Таблица заказов (основная информация о заказе)
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
        total_amount DECIMAL(10,2) NOT NULL,
        order_status VARCHAR(20) CHECK (order_status IN ('processing', 'completed', 'cancelled')),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Таблица элементов заказа (товары в заказе)
    CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
        product_id INTEGER REFERENCES products(id),
        quantity INTEGER NOT NULL CHECK (quantity > 0),
        unit_price DECIMAL(10,2) NOT NULL,
        subtotal DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    indexes_sql = """
    -- Индексы для быстрого поиска
    CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
    CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
    CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status);
    
    -- Индексы для order_items
    CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
    CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
    
    -- Индекс для продуктов
    CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
    """

    triggers_sql = """
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    -- Триггер для users
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_users_updated_at'
        ) THEN
            CREATE TRIGGER trigger_users_updated_at
            BEFORE UPDATE ON users
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;
    END;
    $$;

    -- Триггер для products
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_products_updated_at'
        ) THEN
            CREATE TRIGGER trigger_products_updated_at
            BEFORE UPDATE ON products
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;
    END;
    $$;

    -- Триггер для orders
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_orders_updated_at'
        ) THEN
            CREATE TRIGGER trigger_orders_updated_at
            BEFORE UPDATE ON orders
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;
    END;
    $$;

    -- Триггер для order_items
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'trigger_order_items_updated_at'
        ) THEN
            CREATE TRIGGER trigger_order_items_updated_at
            BEFORE UPDATE ON order_items
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;
    END;
    $$;
    """

    
    try:
        conn = psycopg2.connect(get_connection_string())
        cursor = conn.cursor()
        
        cursor.execute(schema_sql)
        cursor.execute(indexes_sql)
        cursor.execute(triggers_sql)
        conn.commit()
        print("✅ Базовые таблицы созданы!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()