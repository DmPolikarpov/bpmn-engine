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
    .bind(process_id)      // Совпадает с id из init.sql
    .bind(process_id)      // Имя процесса
    .bind(bpmn_xml)        // Совпадает с bpmn_xml из init.sql
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
    .bind(instance_id)     // Совпадает с id из init.sql
    .bind(process_id)      // Совпадает с definition_id из init.sql
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

    // 1. Пробуем обновить статус существующего токена
    let rows_affected = sqlx::query(
        r#"
        UPDATE tokens 
        SET state = $1 
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
            INSERT INTO tokens (id, instance_id, element_id, state, created_at)
            VALUES ($1, $2, $3, $4, NOW());
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