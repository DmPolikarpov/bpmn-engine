//! Персистентное слияние параллельных ветвей поверх таблицы `parallel_join`.
//!
//! Вендорный `PostgresParallelJoinRepo` несовместим со своей схемой
//! (`ensure_group` пишет колонку `id`, которой нет в `parallel_join`, а
//! `try_join` при отсутствии строки возвращает `true`), поэтому он не
//! подключается. Рантайм держал счётчик ветвей в памяти пода
//! (`TokenArrivedHandler::join_state`): при `replicas: 3` или рестарте между
//! двумя ветвями слияние не набирало `expected` и экземпляр зависал на
//! `parallelGateway`-слиянии (ограничение 13, `docs/runtime-m1-design.md`).
//!
//! Эта реализация хранит счётчик в Postgres и подключается к `EngineContext`
//! через [`GraphStores::context`](super::store::GraphStores::context), поэтому
//! «сколько ветвей уже пришло» переживает и рестарт пода, и обработку ветвей
//! разными подами.

use anyhow::anyhow;
use async_trait::async_trait;
use bpm_engine::bpm_engine_storage::ParallelJoinRepo;
use deadpool_postgres::Pool;

/// `ParallelJoinRepo` поверх вендорной таблицы `parallel_join`
/// (`parallel_group_id` PRIMARY KEY, `expected`, `joined`).
pub struct PgParallelJoinRepo {
    pool: Pool,
}

impl PgParallelJoinRepo {
    /// Создаёт репозиторий поверх пула движка (`deadpool`).
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ParallelJoinRepo for PgParallelJoinRepo {
    async fn ensure_group(&self, group_id: &str, expected: u32) -> anyhow::Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                r#"
                INSERT INTO parallel_join (parallel_group_id, expected, joined)
                VALUES ($1, $2, 0)
                ON CONFLICT (parallel_group_id) DO NOTHING
                "#,
                &[&group_id, &(expected as i32)],
            )
            .await?;
        Ok(())
    }

    async fn try_join(&self, group_id: &str) -> anyhow::Result<bool> {
        let client = self.pool.get().await?;
        // Атомарный инкремент, не превышающий `expected`: строка вернётся ровно
        // на том вызове, который набирает нужное число ветвей.
        let row = client
            .query_opt(
                r#"
                UPDATE parallel_join
                SET joined = joined + 1
                WHERE parallel_group_id = $1 AND joined < expected
                RETURNING joined, expected
                "#,
                &[&group_id],
            )
            .await?;
        if let Some(row) = row {
            let joined: i32 = row.get("joined");
            let expected: i32 = row.get("expected");
            return Ok(joined >= expected);
        }

        // UPDATE ничего не вернул: либо ветви уже слились (`joined >= expected`),
        // либо группа не зарегистрирована. В отличие от вендорного репозитория,
        // отсутствие строки — это ошибка, а не «ветка слилась».
        let row = client
            .query_opt(
                "SELECT joined, expected FROM parallel_join WHERE parallel_group_id = $1",
                &[&group_id],
            )
            .await?;
        match row {
            Some(row) => {
                let joined: i32 = row.get("joined");
                let expected: i32 = row.get("expected");
                Ok(joined >= expected)
            }
            None => Err(anyhow!("parallel join group not registered: {group_id}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PgParallelJoinRepo;
    use bpm_engine::bpm_engine_storage::ParallelJoinRepo;
    use bpm_engine_adapter_postgres::{create_pool, migrate};

    /// Проверяет SQL-контракт репозитория на живой БД.
    ///
    /// По умолчанию пропущен (`cargo test -- --ignored`), как и остальные
    /// проверки, которым нужен Postgres (дизайн, приложение B). Строка
    /// `parallel_join` создаётся вендорным `migrate`, которым пользуется и
    /// `GraphStores::ensure_schema`.
    #[tokio::test]
    #[ignore = "требует живой PostgreSQL (DATABASE_URL)"]
    async fn pg_join_repo_counts_branches() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL не задан");
        let pool = create_pool(&url).expect("пул создаётся");
        migrate(&pool).await.expect("миграции применяются");

        let group = format!("test-join-group-{}", std::process::id());
        let repo = PgParallelJoinRepo::new(pool.clone());
        pool.get()
            .await
            .expect("соединение")
            .execute(
                "DELETE FROM parallel_join WHERE parallel_group_id = $1",
                &[&group],
            )
            .await
            .expect("чистка строки группы");

        repo.ensure_group(&group, 2).await.expect("ensure_group");
        assert!(!repo.try_join(&group).await.expect("первая ветвь"));
        assert!(repo.try_join(&group).await.expect("вторая ветвь"));

        // Повторная регистрация группы не сбрасывает уже набранный счётчик.
        repo.ensure_group(&group, 2).await.expect("ensure_group повторно");
        assert!(
            repo.try_join(&group).await.expect("уже слитая группа"),
            "после достижения expected слияние остаётся завершённым"
        );

        // Незарегистрированная группа — ошибка, а не «слилась».
        assert!(
            repo.try_join("no-such-group").await.is_err(),
            "отсутствие группы обязано быть ошибкой"
        );
    }
}
