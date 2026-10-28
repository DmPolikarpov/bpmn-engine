-- Аудит исполнителя шага (M6, `erp-simulator/docs/dairy-plant-roadmap.md`).
--
-- `execution_history` была таблицей «для Exactly-Once» и не заполнялась.
-- Теперь движок пишет сюда завершения и отказы человеческих шагов, поэтому
-- нужен исполнитель: роль сессии, от имени которой пришло событие.
--
-- Колонка аддитивная и nullable: события без роли (NATS-воркеры, E2E-скрипты)
-- продолжают писаться (`actor` = NULL), а старые строки остаются валидными.
--
-- Таблицу создаёт initdb кластера (`schema.sql` / `db-init-configmap.yaml`),
-- а в уже развёрнутой БД колонку добавляет эта миграция — как 0004 для `orders`.
ALTER TABLE execution_history
    ADD COLUMN IF NOT EXISTS actor TEXT;

CREATE INDEX IF NOT EXISTS idx_history_actor
    ON execution_history(actor);
