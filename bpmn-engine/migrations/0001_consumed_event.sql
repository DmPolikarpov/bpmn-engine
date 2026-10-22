-- Идемпотентность доставки событий в рантайме M1 (дизайн `docs/runtime-m1-design.md`, 9.5).
--
-- `event_id` — то, что шлюз кладёт в сообщение (`event_id: correlation_key`).
-- Клиент генерирует его заново на каждый HTTP-запрос, поэтому таблица
-- защищает от повторной доставки JetStream (потеря ack, рестарт пода), а не
-- от двойного клика.
CREATE TABLE IF NOT EXISTS consumed_event (
    event_id    TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consumed_event_instance
    ON consumed_event(instance_id);
