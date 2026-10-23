-- Заказы (M4, `erp-simulator/docs/dairy-plant-roadmap.md`, 6).
--
-- Заказ перестаёт быть `{ orderNum, volume }` в `useState` кабинета: список
-- живёт в БД движка, поэтому F5 и второй браузер видят одно и то же.
--
-- `order_num` — бизнес-ключ демо-контура и корреляционный ключ сообщений
-- (M3): по нему движок находит вторую сторону обмена, поэтому он же PRIMARY KEY.
--
-- `customer_instance_id` / `supplier_instance_id` — экземпляры процесса,
-- запущенные из заказа (их проставляет `POST /orders/:orderNum/instances`).
-- Текущий шаг и состояние контура API считает по legacy read-model
-- (`tokens`, `process_instances`), а не хранит копией: источник истины один.
CREATE TABLE IF NOT EXISTS orders (
    order_num            TEXT PRIMARY KEY,
    nomenclature         TEXT NOT NULL,
    volume_liters        DOUBLE PRECISION NOT NULL CHECK (volume_liters > 0),
    fat_content          DOUBLE PRECISION NOT NULL CHECK (fat_content >= 0 AND fat_content <= 100),
    packaging            TEXT NOT NULL,
    due_date             DATE NOT NULL,
    counterparty         TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'NEW',
    customer_instance_id TEXT,
    supplier_instance_id TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_created_at
    ON orders(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_orders_customer_instance
    ON orders(customer_instance_id);

CREATE INDEX IF NOT EXISTS idx_orders_supplier_instance
    ON orders(supplier_instance_id);
