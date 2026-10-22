-- Ретраи ответов ERP (M2, дизайн `docs/runtime-m1-design.md`, 10.5).
--
-- Ключ — `idempotency_key` команды `<instance>:<element>:<token>`: он стабилен
-- для одной активации `serviceTask`, поэтому повторный ответ на ту же команду
-- не заводит вторую строку, а продолжает счёт попыток.
--
-- `next_attempt_at` — когда переотправить команду (backoff), `dispatched_at` —
-- когда команда последний раз ушла воркеру. Пока `dispatched_at IS NULL`,
-- ответы считаются повтором уже обработанной ошибки: так дубль сообщения не
-- сжигает вторую попытку.
CREATE TABLE IF NOT EXISTS erp_retry (
    idempotency_key TEXT PRIMARY KEY,
    instance_id     TEXT NOT NULL,
    element_id      TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_status     TEXT,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_erp_retry_due
    ON erp_retry(next_attempt_at);

CREATE INDEX IF NOT EXISTS idx_erp_retry_instance
    ON erp_retry(instance_id);
