use sqlx::PgPool;

/// Сохранение определения BPMN-процесса в таблицу process_definitions
pub async fn save_process_definition(
    pool: &PgPool,
    process_id: &str,
    bpmn_xml: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO process_definitions (id, name, bpmn_xml)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) 
        DO UPDATE SET bpmn_xml = EXCLUDED.bpmn_xml
        "#,
    )
    .bind(process_id) // Совпадает с id из init.sql
    .bind(process_id) // Имя процесса
    .bind(bpmn_xml) // Совпадает с bpmn_xml из init.sql
    .execute(pool)
    .await?;

    Ok(())
}

/// Сохранение инстанса процесса в таблицу process_instances
pub async fn save_process_instance(
    pool: &PgPool,
    instance_id: &str,
    process_id: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO process_instances (id, definition_id, status)
        VALUES ($1, $2, 'ACTIVE')
        ON CONFLICT (id) 
        DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
        "#,
    )
    .bind(instance_id) // Совпадает с id из init.sql
    .bind(process_id) // Совпадает с definition_id из init.sql
    .execute(pool)
    .await?;

    Ok(())
}

/// Сохранение или обновление состояния токена
pub async fn save_token_state(
    pool: &PgPool,
    instance_id: &str,
    element_id: &str,
    state: &str,
) -> Result<(), sqlx::Error> {
    let token_id = format!("{}:{}", instance_id, element_id);

    // 1. Пробуем обновить статус существующего токена.
    //
    // `state_changed_at` обязателен: `created_at` — первое появление узла, и
    // повторная активация (переработка партии, возврат на шаг) иначе оставляла
    // ожидание от первого раза, а SLA показывал ложное «просрочено» (M8,
    // ограничение 9 в README; миграция `0006`).
    let rows_affected = sqlx::query(
        r#"
        UPDATE tokens 
        SET state = $1, state_changed_at = NOW() 
        WHERE instance_id = $2 AND element_id = $3;
        "#,
    )
    .bind(state)
    .bind(instance_id)
    .bind(element_id)
    .execute(pool)
    .await?
    .rows_affected();

    // 2. Если токена ещё нет в БД — создаем запись
    if rows_affected == 0 {
        sqlx::query(
            r#"
            INSERT INTO tokens (id, instance_id, element_id, state, created_at, state_changed_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW());
            "#,
        )
        .bind(&token_id)
        .bind(instance_id)
        .bind(element_id)
        .bind(state)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// Удаляет вендорные токены завершённых экземпляров (M8, ограничение 2).
///
/// Вендорная таблица `token` хранит и проходные токены (`startEvent`, шлюзы,
/// throw-события): они остаются `Waiting` навсегда и копятся — на демо-схеме
/// завода это 4 строки на каждый завершённый прогон. Рантайм читает токены
/// только по событиям живого экземпляра, поэтому строки завершённых
/// (`process_instance.state <> 'Running'`) ему не нужны: история остаётся в
/// `history_event`, а шаги — в legacy-проекции (`tokens`, `execution_history`).
///
/// Возвращает число удалённых строк.
pub async fn purge_finished_tokens(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"
        DELETE FROM token
         WHERE instance_id IN (
             SELECT id FROM process_instance WHERE state <> 'Running'
         )
        "#,
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Статус legacy-инстанса (`process_instances.status`) по проекции движка.
pub async fn update_process_instance_status(
    pool: &PgPool,
    instance_id: &str,
    status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE process_instances
        SET status = $1, updated_at = NOW()
        WHERE id = $2
        "#,
    )
    .bind(status)
    .bind(instance_id)
    .execute(pool)
    .await?;

    Ok(())
}

/// Есть ли экземпляр в legacy `process_instances`.
///
/// Нужен рантайму графа: экземпляр, начатый прежним legacy-путём, не имеет
/// строки в вендорных таблицах, и старт создал бы его заново с первого шага.
pub async fn instance_exists(pool: &PgPool, instance_id: &str) -> Result<bool, sqlx::Error> {
    let row: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM process_instances WHERE id = $1")
        .bind(instance_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

/// Запись аудита шага в `execution_history` (M6).
///
/// `event_type` — `USER_TASK_COMPLETED` или `USER_TASK_DENIED`; `actor` —
/// заявленная роль сессии (`None` — событие без роли: воркер, E2E-скрипт).
/// Таблица остаётся legacy-аудитом (вендорный `history_event` пишет движок),
/// но именно её читает кабинет и будущий API метрик (M7).
pub async fn record_step(
    pool: &PgPool,
    instance_id: &str,
    event_type: &str,
    element_id: &str,
    actor: Option<&str>,
    payload: &serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO execution_history (instance_id, event_type, element_id, actor, payload)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(instance_id)
    .bind(event_type)
    .bind(element_id)
    .bind(actor)
    .bind(payload)
    .execute(pool)
    .await?;

    Ok(())
}
