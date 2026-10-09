-- 1. Таблица шаблонов BPMN-процессов
CREATE TABLE IF NOT EXISTS process_definitions (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    bpmn_xml TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_def_version UNIQUE (name, version)
);

-- 2. Таблица экземпляров процессов
CREATE TABLE IF NOT EXISTS process_instances (
    id VARCHAR(64) PRIMARY KEY,
    definition_id VARCHAR(64) NOT NULL REFERENCES process_definitions(id),
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    business_key VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Таблица токенов (текущее состояние выполнения)
CREATE TABLE IF NOT EXISTS tokens (
    id VARCHAR(64) PRIMARY KEY,
    instance_id VARCHAR(64) NOT NULL REFERENCES process_instances(id) ON DELETE CASCADE,
    element_id VARCHAR(255) NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Таблица истории и аудита (для Exactly-Once)
CREATE TABLE IF NOT EXISTS execution_history (
    id BIGSERIAL PRIMARY KEY,
    instance_id VARCHAR(64) NOT NULL REFERENCES process_instances(id) ON DELETE CASCADE,
    event_type VARCHAR(64) NOT NULL,
    element_id VARCHAR(255),
    payload JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для ускорения работы
CREATE INDEX IF NOT EXISTS idx_pi_business_key ON process_instances(business_key);
CREATE INDEX IF NOT EXISTS idx_tokens_instance ON tokens(instance_id);
CREATE INDEX IF NOT EXISTS idx_history_instance ON execution_history(instance_id);

-- Тестовые данные для проверки
INSERT INTO process_definitions (id, name, version, bpmn_xml)
VALUES ('order_process_v1', 'Order Processing', 1, '<xml/>')
ON CONFLICT DO NOTHING;

INSERT INTO process_instances (id, definition_id, status, business_key)
VALUES ('pi_test_1001', 'order_process_v1', 'ACTIVE', 'ERP-ORDER-7789')
ON CONFLICT DO NOTHING;