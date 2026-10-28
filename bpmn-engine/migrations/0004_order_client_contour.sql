-- Третий контур заказа (M5, `erp-simulator/docs/dairy-plant-roadmap.md`).
--
-- У заказа появляется третья сторона — клиент завода (`Process_Client`).
-- Колонка аддитивная: у заказов M3/M4 её значение `NULL`, а API продолжает
-- отдавать те же `customer*`/`supplier*`-поля, поэтому старый кабинет и
-- `tests/orders-api.e2e.cjs` работают без правок.
--
-- Текущий шаг и состояние контура, как и у двух первых сторон, не хранятся
-- копией: их считает `SELECT_ORDER` по `tokens` и `process_instances`.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS client_instance_id TEXT;

CREATE INDEX IF NOT EXISTS idx_orders_client_instance
    ON orders(client_instance_id);
