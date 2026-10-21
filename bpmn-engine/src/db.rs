use sqlx::{PgPool, Row};

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

/// Закрытие всех прочих ACTIVE-токенов экземпляра при активации нового узла.
///
/// Инвариант «один активный узел на экземпляр»: пока движок только дописывал
/// новые ACTIVE-токены, клиент накапливал их и подсвечивал несколько узлов
/// одновременно. Здесь прежние ACTIVE-токены переводятся в COMPLETED, а их
/// `element_id` возвращаются вызывающему коду — по ним публикуются события
/// `token_updated COMPLETED`. Токены в других состояниях (COMPLETED,
/// TIMED_OUT, FAILED) не затрагиваются, поэтому вызов идемпотентен.
pub async fn complete_active_tokens(
    pool: &PgPool,
    instance_id: &str,
    keep_element_id: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        UPDATE tokens
        SET state = 'COMPLETED'
        WHERE instance_id = $1 AND state = 'ACTIVE' AND element_id <> $2
        RETURNING element_id;
        "#,
    )
    .bind(instance_id)
    .bind(keep_element_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|row| row.get("element_id")).collect())
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

/// Есть ли экземпляр в legacy `process_instances`.
///
/// Нужен режиму `graph`: экземпляр, начатый до включения графа, не имеет строки
/// в вендорных таблицах, и старт создал бы его заново с первого шага.
pub async fn instance_exists(pool: &PgPool, instance_id: &str) -> Result<bool, sqlx::Error> {
    let row: Option<(i32,)> = sqlx::query_as("SELECT 1 FROM process_instances WHERE id = $1")
        .bind(instance_id)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}