-- Активация шага для расчёта ожидания и SLA (ограничение 9 в README,
-- `erp-simulator/docs/dairy-plant-roadmap.md`, M8).
--
-- `tokens.created_at` пишется один раз — при первом появлении узла у
-- экземпляра. Повторная активация того же узла (переработка партии, возврат на
-- предыдущий шаг, цикл в схеме) обновляет только `state`, поэтому SLA считал
-- ожидание от первого раза: у шага, активированного неделю назад и заново
-- открытого минуту назад, панель показывала «просрочено».
--
-- `state_changed_at` — время последней записи состояния проекцией. Ожидание
-- открытого шага считается от него; у завершённых шагов начало берётся из
-- `created_at` (событие завершения приходит из `execution_history`).
--
-- Колонка аддитивная и nullable: старые строки остаются валидными, а бэкфилл
-- ниже делает их эквивалентными прежнему поведению.
--
-- Владение: `tokens` создаёт initdb от `postgres`, а `ALTER TABLE` требует
-- владения. На чистой БД его передаёт `db-init-configmap.yaml`; на уже
-- развёрнутой — разово:
--   kubectl exec -i bpmn-db-1 -c postgres -- \
--     psql -U postgres -d bpmn_db -c 'ALTER TABLE tokens OWNER TO bpmn_user;'
-- (симптом без этого — `CrashLoopBackOff` движка и
-- `must be owner of table tokens`, SQLSTATE 42501).
ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS state_changed_at TIMESTAMP WITH TIME ZONE;

UPDATE tokens
   SET state_changed_at = created_at
 WHERE state_changed_at IS NULL;

-- Панель и SLA-воркер выбирают открытые шаги: индекс по состоянию полезнее
-- полного сканирования таблицы с историей.
CREATE INDEX IF NOT EXISTS idx_tokens_state_changed
    ON tokens(state, state_changed_at);
