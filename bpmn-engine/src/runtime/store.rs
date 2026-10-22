//! Сборка контекста вендорного движка поверх PostgreSQL.
//!
//! Здесь же живёт [`CachedDefStore`]: вендорный `PostgresProcessDefStore`
//! компилирует XML на каждый `load`, а компиляция утекает (`Box::leak` в
//! `compiler.rs:1001-1008`), поэтому кэш по хешу XML обязателен
//! (дизайн `docs/runtime-m1-design.md`, 9.4).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bpm_engine::bpm_engine_bpmn::parse_and_compile;
use bpm_engine::bpm_engine_core::ProcessDefinition;
use bpm_engine::bpm_engine_runtime::EngineContext;
use bpm_engine::bpm_engine_storage::{ProcessDefinitionRecord, ProcessDefinitionStore};
use bpm_engine_adapter_postgres::{
    create_pool, migrate, PostgresHistoryRepo, PostgresProcessDefStore, PostgresProcessStore,
    PostgresTokenStore,
};
use deadpool_postgres::Pool;

/// Предел кэша определений — защита от неограниченного роста памяти пода.
pub const MAX_CACHED_DEFINITIONS: usize = 64;

/// Источник XML схемы для кэша (БД в бою, словарь в тестах).
#[async_trait]
pub trait XmlSource: Send + Sync {
    /// Читает `bpmn_xml` по идентификатору определения.
    async fn load_xml(&self, id: &str) -> anyhow::Result<Option<String>>;
}

/// `bpmn_xml` из вендорной таблицы `process_definition`, без компиляции.
pub struct PgXmlSource {
    pool: Pool,
}

impl PgXmlSource {
    /// Создаёт источник поверх пула движка.
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl XmlSource for PgXmlSource {
    async fn load_xml(&self, id: &str) -> anyhow::Result<Option<String>> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT bpmn_xml FROM process_definition WHERE id = $1",
                &[&id],
            )
            .await?;
        Ok(row.map(|row| row.get(0)))
    }
}

struct CacheEntry {
    hash: u64,
    definition: ProcessDefinition,
}

/// Кэш скомпилированных определений поверх [`XmlSource`].
///
/// Проверка хеша на каждом `load` делает кэш корректным и между подами: под,
/// который не делал деплой, увидит новую схему на следующем событии, а не
/// после рестарта.
pub struct CachedDefStore {
    source: Arc<dyn XmlSource>,
    fallback: Arc<dyn ProcessDefinitionStore>,
    cache: RwLock<HashMap<String, CacheEntry>>,
    compilations: AtomicU64,
}

impl CachedDefStore {
    /// Создаёт кэш; `fallback` обслуживает версионирование (`list_versions`,
    /// `get_active`, `activate`, `deprecate`), которое в M1 не используется.
    pub fn new(source: Arc<dyn XmlSource>, fallback: Arc<dyn ProcessDefinitionStore>) -> Self {
        Self {
            source,
            fallback,
            cache: RwLock::new(HashMap::new()),
            compilations: AtomicU64::new(0),
        }
    }

    /// Сколько раз схема реально компилировалась (для теста «нет утечки»).
    #[cfg(test)]
    pub fn compilations(&self) -> u64 {
        self.compilations.load(Ordering::Relaxed)
    }

    /// Сколько определений сейчас в кэше.
    #[cfg(test)]
    pub fn cached_len(&self) -> usize {
        self.cache.read().unwrap().len()
    }
}

fn hash_xml(xml: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    xml.hash(&mut hasher);
    hasher.finish()
}

#[async_trait]
impl ProcessDefinitionStore for CachedDefStore {
    async fn load(&self, id: &str) -> anyhow::Result<Option<ProcessDefinition>> {
        let Some(xml) = self.source.load_xml(id).await? else {
            return Ok(None);
        };
        let hash = hash_xml(&xml);
        if let Some(entry) = self.cache.read().unwrap().get(id) {
            if entry.hash == hash {
                return Ok(Some(entry.definition.clone()));
            }
        }

        let definition = parse_and_compile(&xml)
            .map_err(|error| anyhow::anyhow!("failed to compile definition {id}: {error:?}"))?;
        self.compilations.fetch_add(1, Ordering::Relaxed);

        let mut cache = self.cache.write().unwrap();
        if cache.len() >= MAX_CACHED_DEFINITIONS {
            cache.clear();
        }
        cache.insert(
            id.to_string(),
            CacheEntry {
                hash,
                definition: definition.clone(),
            },
        );
        Ok(Some(definition))
    }

    async fn list_versions(&self, key: &str) -> anyhow::Result<Vec<ProcessDefinitionRecord>> {
        self.fallback.list_versions(key).await
    }

    async fn get_active(&self, key: &str) -> anyhow::Result<Option<ProcessDefinitionRecord>> {
        self.fallback.get_active(key).await
    }

    async fn activate(&self, id: &str) -> anyhow::Result<()> {
        self.fallback.activate(id).await
    }

    async fn deprecate(&self, id: &str) -> anyhow::Result<()> {
        self.fallback.deprecate(id).await
    }
}

/// Хранилища и пул, собранные один раз на процесс.
pub struct GraphStores {
    /// Пул движка (`deadpool`): его используют вендорные сторы.
    pub pool: Pool,
    /// Инстансы (`process_instance` + `token`).
    pub process_store: Arc<PostgresProcessStore>,
    /// Токены с оптимистичной блокировкой (`claim_token`).
    pub token_store: Arc<PostgresTokenStore>,
    /// Определения (деплой — ингерентный метод, не часть трейта).
    pub def_store: Arc<PostgresProcessDefStore>,
    /// Кэш скомпилированных определений для `EngineContext`.
    pub cached_def_store: Arc<CachedDefStore>,
    /// Аудит в `history_event`.
    pub history_repo: Arc<PostgresHistoryRepo>,
}

impl GraphStores {
    /// Создаёт сторы поверх `DATABASE_URL`.
    ///
    /// `parallel_join_repo` намеренно не подключается: вендорный
    /// `PostgresParallelJoinRepo` вставляет колонку `id`, которой нет в
    /// `parallel_join` (`migrate` её не создаёт), поэтому слияние веток
    /// держится на in-memory состоянии хендлера (дизайн 9.2).
    pub fn new(database_url: &str) -> anyhow::Result<Self> {
        let pool = create_pool(database_url)?;
        let def_store = Arc::new(PostgresProcessDefStore::new(pool.clone()));
        let cached_def_store = Arc::new(CachedDefStore::new(
            Arc::new(PgXmlSource::new(pool.clone())),
            def_store.clone(),
        ));

        Ok(Self {
            process_store: Arc::new(PostgresProcessStore::new(pool.clone())),
            token_store: Arc::new(PostgresTokenStore::new(pool.clone())),
            history_repo: Arc::new(PostgresHistoryRepo::new(pool.clone())),
            def_store,
            cached_def_store,
            pool,
        })
    }

    /// Контекст движка для одного прохода пумпы.
    pub fn context(&self) -> EngineContext {
        EngineContext::builder(
            self.process_store.clone(),
            self.token_store.clone(),
            self.cached_def_store.clone(),
        )
        .history_repo(self.history_repo.clone())
        .build()
    }

    /// Создаёт вендорные таблицы (`process_definition`, `process_instance`,
    /// `token`, `history_event`, ...).
    ///
    /// `replicas: 3` поднимают `migrate` одновременно, а PostgreSQL не
    /// гарантирует, что параллельные `CREATE TABLE IF NOT EXISTS` не упадут
    /// на гонке системных каталогов. Поэтому DDL сериализуется
    /// advisory-блокировкой (`pg_advisory_xact_lock`): её держит транзакция
    /// этого пула, а сам `migrate` работает на другом соединении — таблицы
    /// ему не нужны, нужен только порядок.
    pub async fn ensure_schema(&self) -> anyhow::Result<()> {
        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;
        transaction
            .execute(
                "SELECT pg_advisory_xact_lock(hashtextextended('bpm-engine:migrate', 0))",
                &[],
            )
            .await?;

        match migrate(&self.pool).await {
            Ok(()) => {
                transaction.commit().await?;
                Ok(())
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    /// Деплой определения (upsert по `def.id`, дизайн 9.4).
    pub async fn deploy(&self, process_id: &str, bpmn_xml: &str) -> anyhow::Result<()> {
        self.def_store.deploy(process_id, bpmn_xml).await
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use bpm_engine::bpm_engine_adapter_memory::ProcessDefStore;

    use super::*;

    const PROCESS_ID: &str = "cache_test";

    fn xml() -> String {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="cache_test" name="Cache" isExecutable="true">
    <startEvent id="Start"/>
    <userTask id="Task_A" name="A"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="Task_A"/>
    <sequenceFlow id="Flow_A" sourceRef="Task_A" targetRef="End"/>
  </process>
</definitions>"#
            .to_string()
    }

    /// Источник XML без БД: словарь, который тест может менять.
    struct StaticXmlSource {
        xml: RwLock<HashMap<String, String>>,
    }

    impl StaticXmlSource {
        fn new(entries: &[(&str, String)]) -> Self {
            let xml = entries
                .iter()
                .map(|(id, body)| (id.to_string(), body.clone()))
                .collect();
            Self {
                xml: RwLock::new(xml),
            }
        }

        fn set(&self, id: &str, body: String) {
            self.xml.write().unwrap().insert(id.to_string(), body);
        }
    }

    #[async_trait]
    impl XmlSource for StaticXmlSource {
        async fn load_xml(&self, id: &str) -> anyhow::Result<Option<String>> {
            Ok(self.xml.read().unwrap().get(id).cloned())
        }
    }

    fn store(source: Arc<StaticXmlSource>) -> CachedDefStore {
        CachedDefStore::new(source, Arc::new(ProcessDefStore::new()))
    }

    #[tokio::test]
    async fn hundred_loads_compile_once() {
        let source = Arc::new(StaticXmlSource::new(&[(PROCESS_ID, xml())]));
        let cache = store(source);

        for _ in 0..100 {
            let definition = cache
                .load(PROCESS_ID)
                .await
                .expect("load без ошибок")
                .expect("определение найдено");
            assert_eq!(definition.id, PROCESS_ID);
        }

        assert_eq!(
            cache.compilations(),
            1,
            "компиляция утекает (`Box::leak`), поэтому схема должна собираться один раз"
        );
        assert_eq!(cache.cached_len(), 1);
    }

    #[tokio::test]
    async fn changed_xml_is_recompiled() {
        let source = Arc::new(StaticXmlSource::new(&[(PROCESS_ID, xml())]));
        let cache = store(source.clone());

        cache.load(PROCESS_ID).await.expect("первый load");
        cache.load(PROCESS_ID).await.expect("повторный load");
        assert_eq!(cache.compilations(), 1);

        let changed = xml().replace("Task_A", "Task_B");
        source.set(PROCESS_ID, changed);

        let definition = cache
            .load(PROCESS_ID)
            .await
            .expect("load после деплоя")
            .expect("определение найдено");
        assert!(
            definition.nodes.contains_key("Task_B"),
            "под, не делавший деплой, должен увидеть новую схему"
        );
        assert_eq!(cache.compilations(), 2);
    }

    #[tokio::test]
    async fn unknown_definition_is_none() {
        let source = Arc::new(StaticXmlSource::new(&[]));
        let cache = store(source);

        assert!(cache.load("missing").await.expect("load без ошибок").is_none());
        assert_eq!(cache.compilations(), 0);
    }

    #[tokio::test]
    async fn cache_is_capped() {
        let mut entries = Vec::new();
        for index in 0..(MAX_CACHED_DEFINITIONS + 1) {
            let id = format!("cache_{index}");
            let body = xml().replace("cache_test", &id);
            entries.push((id, body));
        }
        let refs: Vec<(&str, String)> = entries
            .iter()
            .map(|(id, body)| (id.as_str(), body.clone()))
            .collect();
        let cache = store(Arc::new(StaticXmlSource::new(&refs)));

        for (id, _) in &entries {
            cache.load(id).await.expect("load без ошибок");
        }

        assert!(
            cache.cached_len() <= MAX_CACHED_DEFINITIONS,
            "кэш не должен расти без предела, в нём {} записей",
            cache.cached_len()
        );
    }
}
