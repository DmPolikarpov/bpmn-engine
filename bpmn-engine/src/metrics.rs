//! API серверных метрик (M7).
//!
//! Инфографика запусков в кабинете считается в браузере, поэтому у руководителя
//! пусто, если процессы запускал не он. Здесь те же цифры считаются на сервере и
//! отдаются по NATS `request/reply` (`mes.in.v1.api.metrics`); HTTP-шлюз
//! перекладывает их в `GET /api/v1/metrics`. Два браузера и F5 видят одно и то
//! же, потому что источник один — Postgres.
//!
//! Источники:
//!
//! - **Запуск** — legacy-проекция `process_instances` (`created_at`, `status`,
//!   `updated_at`): её пишет рантайм графа (`runtime/project.rs`), её же читает
//!   кабинет.
//! - **Шаг** — активация `tokens.created_at`, завершение
//!   `execution_history.created_at` (аудит шага, M6). Если пары нет (сервисный
//!   шаг без записи аудита), берётся пара вендорной истории
//!   `TokenArrived`/`TokenCompleted`. Точность: legacy — миллисекунды, вендорная
//!   история — секунды (`occurred_at` хранится epoch-секундами).
//! - **Финиш** — `ProcessCompleted`/`ProcessTerminated` вендорной истории; если
//!   события нет, узел последнего `TokenArrived` сверяется с **конечными узлами
//!   определения** (`endEvent` в `bpmn_xml`), а не с «узлом без исходящих связей
//!   текущей схемы»: правка схемы в редакторе больше не объявляет финишем
//!   произвольный шаг.
//! - **Роль шага** — `execution_history.actor` (M6), иначе роль неизвестна.
//!
//! Доля брака: конечные узлы, помеченные как брак, берутся из
//! `METRICS_DEFECT_END_EVENTS` (id через запятую), из запроса (`defectNodes`) или
//! из имени `endEvent` (`брак`, `возврат`, `defect`, `reject`). Найденные id
//! возвращаются в ответе (`defectEndEvents`), чтобы панель показывала основание
//! расчёта, а не «магическое» число.
//!
//! Контракт запроса/ответа проверяется без живой БД: `aggregate` — чистая
//! функция над снимком таблиц (`Snapshot`), стор только загружает строки. Так же
//! устроен API заказов (`orders.rs`).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Row};

/// Предмет NATS-запроса метрик (отвечает движок, шлюз перекладывает в REST).
pub const API_SUBJECT: &str = "mes.in.v1.api.metrics";
/// Очередь нужна, чтобы на запрос ответила одна реплика: Postgres общий.
pub const API_QUEUE: &str = "metrics-api";

/// Сколько последних запусков отдавать по умолчанию.
pub const DEFAULT_LIMIT: usize = 20;
/// Верхняя граница `limit`: панель рисует таблицу, а не выгрузку.
pub const MAX_LIMIT: usize = 50;
/// Таймаут SLA по умолчанию — тот же, что у воркера (`SLA_TIMEOUT_MINUTES`).
pub const DEFAULT_SLA_MINUTES: i64 = 30;
/// Больше суток ждать шаг в демо нечего; защита от опечатки в `slaMinutes`.
pub const MAX_SLA_MINUTES: i64 = 1440;
/// Сколько строк каждого источника читать. Демо-масштаб с запасом.
pub const DEFAULT_ROW_LIMIT: i64 = 5000;
/// Точность вендорной истории: `occurred_at` — epoch-секунды.
pub const VENDOR_PRECISION_MS: i64 = 1000;

/// Пределы чтения таблиц (в тестах не нужны, в проде — защита от полного скана).
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub history: i64,
    pub rows: i64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            history: DEFAULT_ROW_LIMIT,
            rows: DEFAULT_ROW_LIMIT,
        }
    }
}

/// Запрос метрик. Все поля необязательны: `{}` — «всё по всем процессам».
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    /// Разрез по процессу (завод / поставщик / клиент).
    #[serde(default, alias = "process_id")]
    pub process_id: Option<String>,
    /// Сколько последних запусков вернуть (1..=[`MAX_LIMIT`]).
    #[serde(default)]
    pub limit: Option<i64>,
    /// Порог SLA в минутах; по умолчанию — `SLA_TIMEOUT_MINUTES` движка.
    #[serde(default, alias = "sla_minutes")]
    pub sla_minutes: Option<i64>,
    /// Явный список конечных узлов «брак» (перебивает env и эвристику по имени).
    #[serde(default, alias = "defect_nodes")]
    pub defect_nodes: Option<Vec<String>>,
}

/// Настройки расчёта из окружения движка.
#[derive(Debug, Clone)]
pub struct Options {
    pub sla_minutes: i64,
    pub defect_nodes: BTreeSet<String>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            sla_minutes: DEFAULT_SLA_MINUTES,
            defect_nodes: BTreeSet::new(),
        }
    }
}

impl Options {
    /// `SLA_TIMEOUT_MINUTES` и `METRICS_DEFECT_END_EVENTS` — по образцу воркера SLA.
    pub fn from_env() -> Self {
        let sla_minutes = std::env::var("SLA_TIMEOUT_MINUTES")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(DEFAULT_SLA_MINUTES);

        let defect_nodes = std::env::var("METRICS_DEFECT_END_EVENTS")
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();

        Self {
            sla_minutes,
            defect_nodes,
        }
    }
}


/// Ответ API. `status` повторяет HTTP-код для шлюза (как в `orders.rs`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reply {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Момент расчёта: два браузера в одну секунду увидят одно и то же.
    pub generated_at: String,
    pub sla_minutes: i64,
    /// Точность самого грубого источника времени в ответе (мс).
    pub precision_ms: i64,
    pub defect_end_events: Vec<String>,
    pub processes: Vec<ProcessMetrics>,
    pub steps: Vec<StepMetrics>,
    pub roles: Vec<RoleMetrics>,
    pub totals: Totals,
    pub warnings: Vec<String>,
}

impl Reply {
    pub fn failure(status: u16, error: impl Into<String>) -> Self {
        Self {
            ok: false,
            status: Some(status),
            error: Some(error.into()),
            generated_at: iso(Some(Utc::now().timestamp_millis())).unwrap_or_default(),
            ..Default::default()
        }
    }
}

/// Метрики одного процесса (определения).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessMetrics {
    pub process_id: String,
    #[serde(default)]
    pub process_name: Option<String>,
    /// Все запуски, попавшие в выборку (включая активные).
    pub runs: usize,
    pub completed: usize,
    pub active: usize,
    pub terminated: usize,
    /// Цикл запуска: от старта до финиша (только завершённые).
    #[serde(default)]
    pub average_cycle_ms: Option<i64>,
    #[serde(default)]
    pub median_cycle_ms: Option<i64>,
    #[serde(default)]
    pub min_cycle_ms: Option<i64>,
    #[serde(default)]
    pub max_cycle_ms: Option<i64>,
    /// Среднее время шага — «сколько ждёт одно действие».
    #[serde(default)]
    pub average_step_ms: Option<i64>,
    /// Запуски, где хотя бы один шаг превысил SLA.
    pub sla_runs: usize,
    /// Шаги, превысившие SLA (включая ещё открытые).
    pub sla_steps: usize,
    pub open_steps: usize,
    pub defect_runs: usize,
    /// Доля брака среди завершённых; `None` — конечные узлы брака не заданы.
    #[serde(default)]
    pub defect_share: Option<f64>,
    /// Финальный узел → сколько запусков на нём закончилось.
    pub outcomes: Vec<OutcomeCount>,
    pub last_runs: Vec<RunMetrics>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeCount {
    pub node_id: String,
    pub runs: usize,
}

/// Один запуск в таблице «последние».
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RunMetrics {
    pub instance_id: String,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub finished_at: Option<String>,
    #[serde(default)]
    pub duration_ms: Option<i64>,
    /// `COMPLETED`, `TERMINATED` или `ACTIVE`.
    pub status: String,
    #[serde(default)]
    pub last_node: Option<String>,
    /// Откуда взят финиш: `process` (событие движка), `graph` (узел — `endEvent`
    /// определения), `legacy` (только статус проекции), `None` — не завершён.
    #[serde(default)]
    pub finished_by: Option<String>,
    pub sla_breached: bool,
    /// Шаги этого запуска, превысившие SLA (что именно ждало).
    pub breached_steps: Vec<BreachedStep>,
    /// Номер заказа (`orders`, M4). `None` — экземпляр запущен не из заказа:
    /// панель подписывает столбик id экземпляра.
    #[serde(default)]
    pub order_num: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BreachedStep {
    pub element_id: String,
    #[serde(default)]
    pub role: Option<String>,
    pub waited_ms: i64,
    pub open: bool,
}

/// Разрез «шаг»: сколько ждёт конкретное действие и не нарушает ли SLA.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StepMetrics {
    pub process_id: String,
    pub element_id: String,
    #[serde(default)]
    pub role: Option<String>,
    pub samples: usize,
    /// Сколько раз шаг ещё ждёт исполнителя.
    pub open: usize,
    pub average_ms: i64,
    pub max_ms: i64,
    pub sla_breaches: usize,
}

/// Разрез «роль»: нагрузка и ожидание по ролям (M6 `actor`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoleMetrics {
    pub process_id: String,
    #[serde(default)]
    pub role: Option<String>,
    pub steps: usize,
    pub average_ms: i64,
    pub sla_breaches: usize,
}

/// Счётчики источников: сразу видно, читал ли API пустую БД.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub instances: usize,
    pub runs: usize,
    pub steps: usize,
    pub open_steps: usize,
    pub events: usize,
    pub definitions: usize,
}

/// Снимок таблиц-источников. Отдельный тип, чтобы `aggregate` не знал про sqlx.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// Вендорная история в порядке времени (старые сверху).
    pub history: Vec<HistoryRow>,
    pub definitions: Vec<DefinitionRow>,
    pub instances: Vec<LegacyInstance>,
    pub tokens: Vec<TokenRow>,
    pub step_log: Vec<StepLogRow>,
    /// Заказы: связь бизнес-ключа с экземплярами контуров (`orders`, M4).
    pub orders: Vec<OrderLink>,
    /// Чтение упало в лимит: цифры неполные, об этом предупреждаем в ответе.
    pub truncated: bool,
}

/// Связь заказа с экземплярами (`orders`).
///
/// Нужна ради подписи: `instance_id` — техническая строка (`inst-cust-…`), а
/// руководитель ищет заказ по номеру. Номер заказа и есть корреляционный ключ
/// сообщений M3 (`orderNum`), поэтому одного поля достаточно обоим контурам.
#[derive(Debug, Clone, Default)]
pub struct OrderLink {
    pub order_num: String,
    pub customer_instance_id: Option<String>,
    pub supplier_instance_id: Option<String>,
}

/// Строка вендорной истории (`history_event`).
#[derive(Debug, Clone)]
pub struct HistoryRow {
    pub instance_id: String,
    pub event_type: String,
    pub payload: Value,
    pub occurred_at: String,
}

/// Определение процесса: вендорная таблица (XML) и legacy (имя).
#[derive(Debug, Clone, Default)]
pub struct DefinitionRow {
    pub id: String,
    pub def_key: String,
    pub name: Option<String>,
    pub bpmn_xml: Option<String>,
}

/// Запуск в legacy-проекции (`process_instances`).
#[derive(Debug, Clone)]
pub struct LegacyInstance {
    pub id: String,
    pub definition_id: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Активация шага (`tokens`): время первого появления узла у экземпляра.
#[derive(Debug, Clone)]
pub struct TokenRow {
    pub instance_id: String,
    pub element_id: String,
    pub state: String,
    pub created_at: DateTime<Utc>,
    /// Время последней записи состояния (`state_changed_at`, миграция `0006`).
    /// `None` — строка старше миграции либо колонка не заполнена.
    pub state_changed_at: Option<DateTime<Utc>>,
}

impl TokenRow {
    /// С какого момента узел ждёт **сейчас** — последняя активация.
    ///
    /// `created_at` пишется один раз, поэтому у повторно активированного узла
    /// (переработка партии, возврат на предыдущий шаг) ожидание считалось бы от
    /// первого раза и SLA показывал ложное «просрочено» (M8, ограничение 9).
    /// У старых строк `state_changed_at` нет — тогда поведение прежнее.
    pub fn waiting_since(&self) -> DateTime<Utc> {
        self.state_changed_at.unwrap_or(self.created_at)
    }

    /// Начало шага для расчёта длительности.
    ///
    /// Открытый шаг ждёт с последней активации (`waiting_since`). У закрытого
    /// шага `state_changed_at` — это время завершения, поэтому началом остаётся
    /// `created_at`, а конец берётся из аудита (`execution_history`).
    pub fn started_at(&self) -> DateTime<Utc> {
        if self.state == "ACTIVE" {
            self.waiting_since()
        } else {
            self.created_at
        }
    }
}

/// Аудит шага (`execution_history`): кто и когда завершил узел.
#[derive(Debug, Clone)]
pub struct StepLogRow {
    pub instance_id: String,
    pub element_id: String,
    pub actor: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Чтение источников метрик. Трейт отделяет расчёт от Postgres: контракт
/// проверяется на `Snapshot`, собранном вручную.
#[async_trait::async_trait]
pub trait MetricsStore: Send + Sync {
    async fn load(&self, limits: Limits) -> anyhow::Result<Snapshot>;
}
/// Postgres-реализация: пять независимых SELECT, дальше всё считает `aggregate`.
pub struct PgMetricsStore {
    pool: PgPool,
}

impl PgMetricsStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl MetricsStore for PgMetricsStore {
    async fn load(&self, limits: Limits) -> anyhow::Result<Snapshot> {
        // История читается «сверху вниз» (новые первыми) и разворачивается:
        // при равных секундах порядок вставки сохраняется, а лимит отсекает
        // именно старые записи.
        let history_rows = sqlx::query(
            r#"
            SELECT instance_id, event_type, payload, occurred_at
            FROM history_event
            ORDER BY occurred_at DESC, id DESC
            LIMIT $1
            "#,
        )
        .bind(limits.history)
        .fetch_all(&self.pool)
        .await?;

        let truncated = history_rows.len() as i64 >= limits.history;

        let mut history = Vec::with_capacity(history_rows.len());
        for row in history_rows.iter().rev() {
            let payload: String = row.get("payload");
            history.push(HistoryRow {
                instance_id: row.get("instance_id"),
                event_type: row.get("event_type"),
                payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
                occurred_at: row.get("occurred_at"),
            });
        }

        // XML берём из вендорной таблицы (её пишет рантайм). Имя процесса
        // лежит только в legacy `process_definitions` (его заполняет деплой).
        let vendor_defs = sqlx::query(
            r#"
            SELECT id, def_key, bpmn_xml
            FROM process_definition
            WHERE status IS NULL OR status = 'active'
            ORDER BY def_key, version DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let legacy_defs = sqlx::query(
            r#"
            SELECT id, name
            FROM process_definitions
            ORDER BY id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut definitions: Vec<DefinitionRow> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for row in &vendor_defs {
            let def_key: String = row.get("def_key");
            if !seen.insert(def_key.clone()) {
                continue;
            }
            let name = legacy_defs
                .iter()
                .find(|legacy| legacy.get::<String, _>("id") == def_key)
                .map(|legacy| legacy.get::<String, _>("name"));
            definitions.push(DefinitionRow {
                id: row.get("id"),
                def_key,
                name,
                bpmn_xml: row.get("bpmn_xml"),
            });
        }
        for row in &legacy_defs {
            let id: String = row.get("id");
            if !seen.insert(id.clone()) {
                continue;
            }
            definitions.push(DefinitionRow {
                id: id.clone(),
                def_key: id,
                name: Some(row.get("name")),
                bpmn_xml: None,
            });
        }

        let instance_rows = sqlx::query(
            r#"
            SELECT id, definition_id, status, created_at, updated_at
            FROM process_instances
            ORDER BY created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limits.rows)
        .fetch_all(&self.pool)
        .await?;

        let instances = instance_rows
            .iter()
            .map(|row| LegacyInstance {
                id: row.get("id"),
                definition_id: row.get("definition_id"),
                status: row.get("status"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })
            .collect();

        let token_rows = sqlx::query(
            r#"
            SELECT instance_id, element_id, state, created_at, state_changed_at
            FROM tokens
            ORDER BY created_at
            LIMIT $1
            "#,
        )
        .bind(limits.rows)
        .fetch_all(&self.pool)
        .await?;

        let tokens = token_rows
            .iter()
            .map(|row| TokenRow {
                instance_id: row.get("instance_id"),
                element_id: row.get("element_id"),
                state: row.get("state"),
                created_at: row.get("created_at"),
                state_changed_at: row.try_get("state_changed_at").ok().flatten(),
            })
            .collect();

        let log_rows = sqlx::query(
            r#"
            SELECT instance_id, element_id, actor, created_at
            FROM execution_history
            ORDER BY created_at
            LIMIT $1
            "#,
        )
        .bind(limits.rows)
        .fetch_all(&self.pool)
        .await?;

        let step_log = log_rows
            .iter()
            .filter_map(|row| {
                let element_id: Option<String> = row.get("element_id");
                Some(StepLogRow {
                    instance_id: row.get("instance_id"),
                    element_id: element_id?,
                    actor: row.get("actor"),
                    created_at: row.get("created_at"),
                })
            })
            .collect();

        // Заказы нужны только для подписи запусков: если таблицы нет (старая
        // БД без M4), панель просто подпишет столбик id экземпляра.
        let orders = match sqlx::query(
            r#"
            SELECT order_num, customer_instance_id, supplier_instance_id
            FROM orders
            ORDER BY created_at DESC
            LIMIT $1
            "#,
        )
        .bind(limits.rows)
        .fetch_all(&self.pool)
        .await
        {
            Ok(rows) => rows
                .iter()
                .map(|row| OrderLink {
                    order_num: row.get("order_num"),
                    customer_instance_id: row.get("customer_instance_id"),
                    supplier_instance_id: row.get("supplier_instance_id"),
                })
                .collect(),
            Err(_) => Vec::new(),
        };

        Ok(Snapshot {
            history,
            definitions,
            instances,
            tokens,
            step_log,
            orders,
            truncated,
        })
    }
}

/// Разбор тела NATS-запроса и обработка. Битый JSON — это 400, а не 500.
pub async fn handle_bytes(store: &dyn MetricsStore, body: &[u8]) -> Reply {
    match serde_json::from_slice::<Request>(body) {
        Ok(request) => handle(store, request, &Options::from_env()).await,
        Err(error) => Reply::failure(400, format!("некорректный JSON запроса метрик: {error}")),
    }
}

pub async fn handle(store: &dyn MetricsStore, request: Request, options: &Options) -> Reply {
    match store.load(Limits::default()).await {
        Ok(snapshot) => aggregate(&snapshot, &request, options, Utc::now().timestamp_millis()),
        Err(error) => {
            eprintln!("❌ [Metrics] ошибка чтения источников: {error:#}");
            Reply::failure(500, "ошибка чтения метрик из БД")
        }
    }
}


// ---------------------------------------------------------------------------
// Разбор времени и определение процесса
// ---------------------------------------------------------------------------

/// Имена конечных узлов, по которым узел считается «браком», если явный список
/// не задан (`метрики считают по схеме, а не по константе в коде`).
const DEFECT_NAME_HINTS: [&str; 4] = ["брак", "возврат", "defect", "reject"];

/// Единый разбор времени источника.
///
/// Вендорные таблицы хранят время строкой: `occurred_at` — epoch-секунды,
/// `created_at` — тоже текст (поэтому формат пробуем, а не угадываем). Legacy
/// `timestamptz` приходит как `DateTime<Utc>` и сюда не попадает.
pub fn parse_timestamp_ms(raw: &str) -> Option<i64> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    if let Ok(number) = raw.parse::<i64>() {
        // 10 знаков — секунды, 13 — миллисекунды: обе формы встречались в
        // истории (`occurred_at` пишется секундами).
        return Some(if number.abs() >= 100_000_000_000 {
            number
        } else {
            number * 1000
        });
    }

    if let Ok(datetime) = DateTime::parse_from_rfc3339(raw) {
        return Some(datetime.timestamp_millis());
    }

    for format in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%.f%#z"] {
        if let Ok(datetime) = DateTime::parse_from_str(raw, format) {
            return Some(datetime.timestamp_millis());
        }
    }

    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(Utc.from_utc_datetime(&naive).timestamp_millis());
    }

    None
}

/// Миллисекунды → ISO-строка для кабинета (`null`, если времени нет).
fn iso(ms: Option<i64>) -> Option<String> {
    let ms = ms?;
    Utc.timestamp_millis_opt(ms)
        .single()
        .map(|datetime| datetime.to_rfc3339_opts(SecondsFormat::Millis, true))
}

/// Конечные узлы определения: `endEvent` (в т.ч. `<bpmn:endEvent>`).
///
/// Возвращает `id → имя`: имя нужно эвристике «брак». Именно этот набор, а не
/// «узлы без исходящих связей», отвечает на вопрос «запуск завершён?» (M7).
pub fn end_events(xml: &str) -> BTreeMap<String, Option<String>> {
    let Ok(document) = roxmltree::Document::parse(xml) else {
        return BTreeMap::new();
    };

    document
        .descendants()
        .filter(|node| node.is_element() && node.tag_name().name() == "endEvent")
        .filter_map(|node| {
            node.attribute("id").map(|id| {
                (
                    id.to_string(),
                    node.attribute("name").map(str::to_string),
                )
            })
        })
        .collect()
}

/// Конечные узлы «брак»: явный список (env или запрос) главнее имени узла.
fn defect_nodes(
    ends: &BTreeMap<String, Option<String>>,
    explicit: &BTreeSet<String>,
) -> BTreeSet<String> {
    if !explicit.is_empty() {
        return explicit.clone();
    }

    ends.iter()
        .filter(|(_, name)| {
            name.as_deref().is_some_and(|name| {
                let name = name.to_lowercase();
                DEFECT_NAME_HINTS
                    .iter()
                    .any(|hint| name.contains(hint))
            })
        })
        .map(|(id, _)| id.clone())
        .collect()
}


/// Индекс определений: `id` и `def_key` → канонический id процесса, имя, концы.
///
/// Нужен потому, что история двигает `process_id` из `ProcessStarted`, legacy
/// `process_instances` — `definition_id`, а вендорная таблица — `id`/`def_key`.
/// Все три указывают на один процесс, но строками могут отличаться.
#[derive(Debug, Default)]
struct DefinitionIndex {
    canonical: HashMap<String, String>,
    names: HashMap<String, String>,
    ends: HashMap<String, BTreeMap<String, Option<String>>>,
}

impl DefinitionIndex {
    fn new(rows: &[DefinitionRow]) -> Self {
        let mut index = Self::default();

        for row in rows {
            let process_id = if row.def_key.trim().is_empty() {
                row.id.clone()
            } else {
                row.def_key.clone()
            };

            index.canonical.insert(row.id.clone(), process_id.clone());
            index
                .canonical
                .insert(process_id.clone(), process_id.clone());

            if let Some(name) = row.name.as_deref().map(str::trim).filter(|n| !n.is_empty()) {
                index
                    .names
                    .entry(process_id.clone())
                    .or_insert_with(|| name.to_string());
            }

            if let Some(xml) = row.bpmn_xml.as_deref() {
                let ends = end_events(xml);
                if !ends.is_empty() {
                    index.ends.insert(process_id, ends);
                }
            }
        }

        index
    }

    fn process_id(&self, raw: &str) -> String {
        self.canonical
            .get(raw)
            .cloned()
            .unwrap_or_else(|| raw.to_string())
    }

    fn name(&self, process_id: &str) -> Option<String> {
        self.names.get(process_id).cloned()
    }

    fn end_events(&self, process_id: &str) -> Option<&BTreeMap<String, Option<String>>> {
        self.ends.get(process_id)
    }
}

/// Шаг запуска: интервал «активирован → завершён» и исполнитель.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepFact {
    pub element_id: String,
    pub role: Option<String>,
    pub started_ms: i64,
    /// `None` — шаг ещё ждёт исполнителя.
    pub finished_ms: Option<i64>,
}

impl StepFact {
    fn open(&self) -> bool {
        self.finished_ms.is_none()
    }

    /// Ожидание: для открытого шага — до `now_ms` (его и видит SLA-таймаут).
    fn waited_ms(&self, now_ms: i64) -> i64 {
        (self.finished_ms.unwrap_or(now_ms) - self.started_ms).max(0)
    }
}

/// Запуск целиком: сроки, финиш и шаги.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFact {
    pub instance_id: String,
    pub process_id: String,
    pub started_ms: Option<i64>,
    pub finished_ms: Option<i64>,
    pub status: String,
    pub finished_by: Option<String>,
    pub last_node: Option<String>,
    /// Номер заказа, если экземпляр запущен из заказа (`orders`, M4).
    pub order_num: Option<String>,
    pub steps: Vec<StepFact>,
}

impl RunFact {
    fn duration_ms(&self) -> Option<i64> {
        Some((self.finished_ms? - self.started_ms?).max(0))
    }

    /// Шаги, превысившие SLA. Список, а не флаг: панель показывает, **что**
    /// именно ждало, — это и есть «SLA виден без консоли».
    fn breached(&self, sla_ms: i64, now_ms: i64) -> Vec<BreachedStep> {
        self.steps
            .iter()
            .map(|step| (step, step.waited_ms(now_ms)))
            .filter(|(_, waited)| *waited > sla_ms)
            .map(|(step, waited)| BreachedStep {
                element_id: step.element_id.clone(),
                role: step.role.clone(),
                waited_ms: waited,
                open: step.open(),
            })
            .collect()
    }
}


// ---------------------------------------------------------------------------
// Сборка запусков из снимка
// ---------------------------------------------------------------------------

/// Собирает запуски из снимка: объединение «есть история» и «есть проекция».
///
/// Порядок — новые сверху (как в кабинете), при равном старте — по id: ответ
/// детерминирован, поэтому два браузера показывают одинаковые строки.
pub fn build_runs(snapshot: &Snapshot) -> Vec<RunFact> {
    let definitions = DefinitionIndex::new(&snapshot.definitions);

    let mut events: HashMap<&str, Vec<&HistoryRow>> = HashMap::new();
    for row in &snapshot.history {
        events.entry(row.instance_id.as_str()).or_default().push(row);
    }

    let mut legacy: HashMap<&str, &LegacyInstance> = HashMap::new();
    for instance in &snapshot.instances {
        legacy.insert(instance.id.as_str(), instance);
    }

    let mut tokens: HashMap<&str, Vec<&TokenRow>> = HashMap::new();
    for token in &snapshot.tokens {
        tokens.entry(token.instance_id.as_str()).or_default().push(token);
    }

    let mut step_log: HashMap<&str, Vec<&StepLogRow>> = HashMap::new();
    for row in &snapshot.step_log {
        step_log
            .entry(row.instance_id.as_str())
            .or_default()
            .push(row);
    }

    // Экземпляр → номер заказа: у заказа до двух контуров (M4).
    let mut order_of: HashMap<&str, &str> = HashMap::new();
    for link in &snapshot.orders {
        for instance_id in [
            link.customer_instance_id.as_deref(),
            link.supplier_instance_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            order_of.insert(instance_id, link.order_num.as_str());
        }
    }

    let mut ids: BTreeSet<&str> = BTreeSet::new();
    ids.extend(events.keys().copied());
    ids.extend(legacy.keys().copied());

    let mut runs: Vec<RunFact> = ids
        .into_iter()
        .map(|instance_id| {
            build_run(
                instance_id,
                events.get(instance_id).map(Vec::as_slice).unwrap_or(&[]),
                legacy.get(instance_id).copied(),
                tokens.get(instance_id).map(Vec::as_slice).unwrap_or(&[]),
                step_log.get(instance_id).map(Vec::as_slice).unwrap_or(&[]),
                &definitions,
                order_of.get(instance_id).copied(),
            )
        })
        .collect();

    runs.sort_by(|left, right| {
        right
            .started_ms
            .cmp(&left.started_ms)
            .then_with(|| right.instance_id.cmp(&left.instance_id))
    });

    runs
}


/// Один запуск: лента событий → шаги, роли и финиш.
fn build_run(
    instance_id: &str,
    events: &[&HistoryRow],
    legacy: Option<&LegacyInstance>,
    tokens: &[&TokenRow],
    step_log: &[&StepLogRow],
    definitions: &DefinitionIndex,
    order_num: Option<&str>,
) -> RunFact {
    let mut process_raw = legacy.map(|instance| instance.definition_id.clone());
    let mut started_ms: Option<i64> = None;
    let mut finish_event: Option<(i64, String)> = None;
    let mut last_node: Option<(i64, String)> = None;

    // Открытые шаги по токенам: (token_id, node_id, активация).
    let mut open: Vec<(String, String, i64)> = Vec::new();
    let mut steps: Vec<StepFact> = Vec::new();
    let mut has_vendor_tokens = false;

    for event in events {
        let at = parse_timestamp_ms(&event.occurred_at);

        match event.event_type.as_str() {
            "ProcessStarted" => {
                if process_raw.is_none() {
                    process_raw = event
                        .payload
                        .get("process_id")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                }
                if started_ms.is_none() {
                    started_ms = at;
                }
            }
            "TokenArrived" => {
                has_vendor_tokens = true;
                let (Some(node_id), Some(at)) = (
                    event.payload.get("node_id").and_then(Value::as_str),
                    at,
                ) else {
                    continue;
                };
                let token_id = event
                    .payload
                    .get("token_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();

                // Повторный приход на тот же токен — это цикл: предыдущая
                // итерация закрыта самим фактом нового прихода.
                if let Some(index) = open.iter().position(|(token, _, _)| *token == token_id) {
                    let (_, node, start) = open.remove(index);
                    steps.push(StepFact {
                        element_id: node,
                        role: None,
                        started_ms: start,
                        finished_ms: Some(at),
                    });
                }

                last_node = Some((at, node_id.to_string()));
                open.push((token_id, node_id.to_string(), at));
            }
            "TokenCompleted" => {
                let token_id = event
                    .payload
                    .get("token_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let Some(at) = at else { continue };

                if let Some(index) = open.iter().position(|(token, _, _)| token == token_id) {
                    let (_, node, start) = open.remove(index);
                    steps.push(StepFact {
                        element_id: node,
                        role: None,
                        started_ms: start,
                        finished_ms: Some(at),
                    });
                }
            }
            "ProcessCompleted" => {
                if let Some(at) = at {
                    finish_event = Some((at, "COMPLETED".to_string()));
                }
            }
            "ProcessTerminated" | "ProcessCancelled" | "InstanceTerminated" => {
                if let Some(at) = at {
                    finish_event = Some((at, "TERMINATED".to_string()));
                }
            }
            // `UserTaskCompleted` и прочие события финиш не двигают: роль и
            // время завершения берутся из `execution_history` (точнее, в мс).
            _ => {}
        }
    }

    for (_, node, start) in open {
        steps.push(StepFact {
            element_id: node,
            role: None,
            started_ms: start,
            finished_ms: None,
        });
    }

    // Если вендорных токенов нет (старые записи), шаги восстанавливаются из
    // legacy-проекции: `tokens.created_at` + `execution_history.created_at`.
    if !has_vendor_tokens {
        for token in tokens {
            // Открытый шаг ждёт с последней активации (`state_changed_at`).
            let start = token.started_at().timestamp_millis();
            let finished = step_log
                .iter()
                .filter(|row| row.element_id == token.element_id)
                .map(|row| row.created_at.timestamp_millis())
                .filter(|at| *at >= start)
                .min();

            steps.push(StepFact {
                element_id: token.element_id.clone(),
                role: None,
                started_ms: start,
                finished_ms: if token.state == "ACTIVE" { None } else { finished },
            });
        }
    }

    // Роль — последний непустой `actor` по узлу: переназначение задачи
    // перезаписывает исполнителя, панели нужен финальный.
    for step in steps.iter_mut() {
        step.role = step_log
            .iter()
            .filter(|row| row.element_id == step.element_id)
            .filter_map(|row| row.actor.as_deref())
            .map(str::trim)
            .filter(|actor| !actor.is_empty())
            .next_back()
            .map(str::to_string);
    }

    // Уточняем интервал по legacy-паре, когда она есть: вендорная история
    // пишет секунды, а `tokens`/`execution_history` — миллисекунды.
    for step in steps.iter_mut() {
        let pair = tokens
            .iter()
            .filter(|token| token.element_id == step.element_id)
            .find_map(|token| {
                let start = token.started_at().timestamp_millis();
                let finish = step_log
                    .iter()
                    .filter(|row| row.element_id == step.element_id)
                    .map(|row| row.created_at.timestamp_millis())
                    .filter(|at| *at >= start)
                    .min()?;
                Some((start, finish))
            });

        match pair {
            Some((start, finish)) => {
                step.started_ms = start;
                step.finished_ms = Some(finish);
            }
            None if step.open() => {
                if let Some(token) = tokens
                    .iter()
                    .find(|token| token.element_id == step.element_id && token.state == "ACTIVE")
                {
                    step.started_ms = token.started_at().timestamp_millis();
                }
            }
            None => {}
        }
    }
    // Финиш: событие движка → последний узел на `endEvent` схемы → legacy-статус.
    let process_id = process_raw
        .map(|raw| definitions.process_id(&raw))
        .unwrap_or_default();
    let ends = definitions.end_events(&process_id);
    let is_end = |node: &str| ends.is_some_and(|ends| ends.contains_key(node));

    let (finished_ms, status, finished_by) = match finish_event {
        Some((at, status)) => (Some(at), status, Some("event".to_string())),
        None => {
            let graph_finish = last_node
                .as_ref()
                .filter(|(_, node)| is_end(node))
                .map(|(at, _)| *at);

            match graph_finish {
                Some(at) => (Some(at), "COMPLETED".to_string(), Some("graph".to_string())),
                None => match legacy {
                    Some(instance)
                        if matches!(
                            instance.status.to_uppercase().as_str(),
                            "COMPLETED" | "TERMINATED"
                        ) =>
                    {
                        (
                            Some(instance.updated_at.timestamp_millis()),
                            instance.status.to_uppercase(),
                            Some("legacy".to_string()),
                        )
                    }
                    _ => (None, "ACTIVE".to_string(), None),
                },
            }
        }
    };

    let started_ms = started_ms
        .or_else(|| legacy.map(|instance| instance.created_at.timestamp_millis()))
        .or_else(|| steps.iter().map(|step| step.started_ms).min());

    RunFact {
        instance_id: instance_id.to_string(),
        process_id,
        started_ms,
        finished_ms,
        status,
        finished_by,
        last_node: last_node.map(|(_, node)| node),
        order_num: order_num
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        steps,
    }
}

// ---------------------------------------------------------------------------
// Расчёт
// ---------------------------------------------------------------------------

/// Чистый расчёт ответа. Ни sqlx, ни NATS: только снимок, запрос и момент
/// времени — поэтому контракт проверяется юнит-тестами без Postgres.
pub fn aggregate(snapshot: &Snapshot, request: &Request, options: &Options, now_ms: i64) -> Reply {
    let limit = request
        .limit
        .unwrap_or(DEFAULT_LIMIT as i64)
        .clamp(1, MAX_LIMIT as i64) as usize;
    let sla_minutes = request
        .sla_minutes
        .unwrap_or(options.sla_minutes)
        .clamp(1, MAX_SLA_MINUTES);
    let sla_ms = sla_minutes * 60_000;

    let definitions = DefinitionIndex::new(&snapshot.definitions);
    let wanted = request
        .process_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| definitions.process_id(value));

    let explicit_defects: BTreeSet<String> = request
        .defect_nodes
        .clone()
        .map(|nodes| {
            nodes
                .into_iter()
                .map(|node| node.trim().to_string())
                .filter(|node| !node.is_empty())
                .collect()
        })
        .unwrap_or_else(|| options.defect_nodes.clone());

    let mut warnings = Vec::new();
    if snapshot.truncated {
        warnings.push(format!(
            "история обрезана лимитом {DEFAULT_ROW_LIMIT} событий: старые запуски не учтены"
        ));
    }

    let runs: Vec<RunFact> = build_runs(snapshot)
        .into_iter()
        .filter(|run| {
            wanted
                .as_deref()
                .is_none_or(|wanted| run.process_id == wanted)
        })
        .collect();

    let mut by_process: BTreeMap<String, Vec<&RunFact>> = BTreeMap::new();
    for run in &runs {
        by_process
            .entry(run.process_id.clone())
            .or_default()
            .push(run);
    }

    if let Some(wanted) = wanted.as_deref() {
        if !by_process.contains_key(wanted) {
            warnings.push(format!("по процессу {wanted} запусков нет"));
        }
    }

    let mut reply = Reply {
        ok: true,
        status: Some(200),
        generated_at: iso(Some(now_ms)).unwrap_or_default(),
        sla_minutes,
        precision_ms: VENDOR_PRECISION_MS,
        totals: Totals {
            instances: snapshot.instances.len(),
            definitions: snapshot.definitions.len(),
            events: snapshot.history.len(),
            ..Default::default()
        },
        warnings,
        ..Default::default()
    };

    let mut defect_all: BTreeSet<String> = BTreeSet::new();
    for (process_id, process_runs) in &by_process {
        let ends = definitions
            .end_events(process_id)
            .cloned()
            .unwrap_or_default();

        if ends.is_empty() {
            reply.warnings.push(format!(
                "нет схемы {process_id}: финиш и доля брака считаются только по событиям движка"
            ));
        }

        let defects = defect_nodes(&ends, &explicit_defects);
        defect_all.extend(defects.iter().cloned());

        let (metrics, steps, roles) = process_metrics(
            process_id,
            process_runs,
            &definitions,
            &defects,
            limit,
            sla_ms,
            now_ms,
        );

        reply.totals.runs += metrics.runs;
        reply.totals.steps += steps.iter().map(|step| step.samples).sum::<usize>();
        reply.totals.open_steps += metrics.open_steps;
        reply.processes.push(metrics);
        reply.steps.extend(steps);
        reply.roles.extend(roles);
    }

    reply.defect_end_events = defect_all.into_iter().collect();

    reply.processes.sort_by(|left, right| {
        right
            .runs
            .cmp(&left.runs)
            .then_with(|| left.process_id.cmp(&right.process_id))
    });
    reply.steps.sort_by(|left, right| {
        right
            .samples
            .cmp(&left.samples)
            .then_with(|| left.element_id.cmp(&right.element_id))
            .then_with(|| left.role.cmp(&right.role))
    });
    reply.roles.sort_by(|left, right| {
        right
            .steps
            .cmp(&left.steps)
            .then_with(|| left.role.cmp(&right.role))
    });

    reply
}


/// Разрез одного процесса: запуски, цикл, SLA, брак, шаги и роли.
#[allow(clippy::too_many_arguments)]
fn process_metrics(
    process_id: &str,
    runs: &[&RunFact],
    definitions: &DefinitionIndex,
    defects: &BTreeSet<String>,
    limit: usize,
    sla_ms: i64,
    now_ms: i64,
) -> (ProcessMetrics, Vec<StepMetrics>, Vec<RoleMetrics>) {
    let mut metrics = ProcessMetrics {
        process_id: process_id.to_string(),
        process_name: definitions.name(process_id),
        runs: runs.len(),
        ..Default::default()
    };

    let mut cycles: Vec<i64> = Vec::new();
    let mut step_total: i64 = 0;
    let mut step_count: usize = 0;
    let mut outcomes: BTreeMap<String, usize> = BTreeMap::new();
    let mut step_acc: BTreeMap<(String, Option<String>), StepAcc> = BTreeMap::new();
    let mut role_acc: BTreeMap<Option<String>, RoleAcc> = BTreeMap::new();
    let mut last_runs: Vec<RunMetrics> = Vec::with_capacity(runs.len());

    for run in runs {
        match run.status.as_str() {
            "COMPLETED" => metrics.completed += 1,
            "TERMINATED" => metrics.terminated += 1,
            _ => metrics.active += 1,
        }

        if let Some(duration) = run.duration_ms() {
            cycles.push(duration);
        }

        if run.finished_ms.is_some() {
            if let Some(node) = run.last_node.clone() {
                *outcomes.entry(node.clone()).or_default() += 1;
                if defects.contains(&node) {
                    metrics.defect_runs += 1;
                }
            }
        }

        let breached = run.breached(sla_ms, now_ms);
        let sla_breached = !breached.is_empty();
        if sla_breached {
            metrics.sla_runs += 1;
        }
        metrics.sla_steps += breached.len();

        for step in &run.steps {
            let waited = step.waited_ms(now_ms);
            let open = step.open();

            if open {
                metrics.open_steps += 1;
            }
            step_total += waited;
            step_count += 1;

            let entry = step_acc
                .entry((step.element_id.clone(), step.role.clone()))
                .or_default();
            entry.samples += 1;
            entry.open += usize::from(open);
            entry.total_ms += waited;
            entry.max_ms = entry.max_ms.max(waited);
            if waited > sla_ms {
                entry.sla_breaches += 1;
            }

            let role = role_acc.entry(step.role.clone()).or_default();
            role.steps += 1;
            role.total_ms += waited;
            if waited > sla_ms {
                role.sla_breaches += 1;
            }
        }

        last_runs.push(RunMetrics {
            instance_id: run.instance_id.clone(),
            started_at: iso(run.started_ms),
            finished_at: iso(run.finished_ms),
            duration_ms: run.duration_ms(),
            status: run.status.clone(),
            last_node: run.last_node.clone(),
            finished_by: run.finished_by.clone(),
            sla_breached,
            breached_steps: breached,
            order_num: run.order_num.clone(),
        });
    }

    cycles.sort_unstable();
    if !cycles.is_empty() {
        metrics.average_cycle_ms = Some(cycles.iter().sum::<i64>() / cycles.len() as i64);
        // Медиана: верхняя при чётном числе — «типичный» запуск без усреднений.
        metrics.median_cycle_ms = Some(cycles[cycles.len() / 2]);
        metrics.min_cycle_ms = cycles.first().copied();
        metrics.max_cycle_ms = cycles.last().copied();
    }
    if step_count > 0 {
        metrics.average_step_ms = Some(step_total / step_count as i64);
    }
    metrics.defect_share = if metrics.completed == 0 || defects.is_empty() {
        None
    } else {
        // Доля от завершённых: активные ещё могут уйти в брак.
        Some(metrics.defect_runs as f64 / metrics.completed as f64)
    };

    let mut outcomes: Vec<OutcomeCount> = outcomes
        .into_iter()
        .map(|(node_id, runs)| OutcomeCount { node_id, runs })
        .collect();
    outcomes.sort_by(|left, right| {
        right
            .runs
            .cmp(&left.runs)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });
    metrics.outcomes = outcomes;

    last_runs.truncate(limit);
    metrics.last_runs = last_runs;

    let steps: Vec<StepMetrics> = step_acc
        .into_iter()
        .map(|((element_id, role), acc)| StepMetrics {
            process_id: process_id.to_string(),
            element_id,
            role,
            samples: acc.samples,
            open: acc.open,
            average_ms: acc.total_ms / acc.samples.max(1) as i64,
            max_ms: acc.max_ms,
            sla_breaches: acc.sla_breaches,
        })
        .collect();

    let roles: Vec<RoleMetrics> = role_acc
        .into_iter()
        .map(|(role, acc)| RoleMetrics {
            process_id: process_id.to_string(),
            role,
            steps: acc.steps,
            average_ms: acc.total_ms / acc.steps.max(1) as i64,
            sla_breaches: acc.sla_breaches,
        })
        .collect();

    (metrics, steps, roles)
}

#[derive(Debug, Default)]
struct StepAcc {
    samples: usize,
    open: usize,
    total_ms: i64,
    max_ms: i64,
    sla_breaches: usize,
}

#[derive(Debug, Default)]
struct RoleAcc {
    steps: usize,
    total_ms: i64,
    sla_breaches: usize,
}


#[cfg(test)]
mod tests {
    use super::*;

    /// Начало демо-прогонов: 2023-11-14T22:13:20Z.
    const T0: i64 = 1_700_000_000_000;
    const MINUTE: i64 = 60_000;

    fn xml() -> &'static str {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="zakaz" isExecutable="true">
    <bpmn:startEvent id="Start_1" />
    <bpmn:userTask id="Task_Accept" name="Приём заявки" />
    <bpmn:endEvent id="End_Ok" name="Готово" />
    <bpmn:endEvent id="End_Defect" name="Брак" />
  </bpmn:process>
</bpmn:definitions>"#
    }

    fn definitions() -> Vec<DefinitionRow> {
        vec![DefinitionRow {
            id: "zakaz:v1".to_string(),
            def_key: "zakaz".to_string(),
            name: Some("Заказ молока".to_string()),
            bpmn_xml: Some(xml().to_string()),
        }]
    }

    fn at(ms: i64) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(ms)
            .single()
            .expect("время существует")
    }

    fn event(instance: &str, event_type: &str, payload: Value, at_ms: i64) -> HistoryRow {
        HistoryRow {
            instance_id: instance.to_string(),
            event_type: event_type.to_string(),
            payload,
            // В вендорной таблице `occurred_at` — epoch-секунды строкой.
            occurred_at: (at_ms / 1000).to_string(),
        }
    }

    fn started(instance: &str, at_ms: i64) -> HistoryRow {
        event(
            instance,
            "ProcessStarted",
            serde_json::json!({ "process_id": "zakaz" }),
            at_ms,
        )
    }

    fn arrived(instance: &str, node: &str, at_ms: i64) -> HistoryRow {
        event(
            instance,
            "TokenArrived",
            serde_json::json!({ "token_id": node, "node_id": node }),
            at_ms,
        )
    }

    fn token_done(instance: &str, node: &str, at_ms: i64) -> HistoryRow {
        event(
            instance,
            "TokenCompleted",
            serde_json::json!({ "token_id": node }),
            at_ms,
        )
    }

    fn finished(instance: &str, at_ms: i64) -> HistoryRow {
        event(instance, "ProcessCompleted", serde_json::json!({}), at_ms)
    }

    fn legacy(instance: &str, status: &str, created_ms: i64, updated_ms: i64) -> LegacyInstance {
        LegacyInstance {
            id: instance.to_string(),
            definition_id: "zakaz".to_string(),
            status: status.to_string(),
            created_at: at(created_ms),
            updated_at: at(updated_ms),
        }
    }

    fn token(instance: &str, element: &str, state: &str, at_ms: i64) -> TokenRow {
        TokenRow {
            instance_id: instance.to_string(),
            element_id: element.to_string(),
            state: state.to_string(),
            created_at: at(at_ms),
            state_changed_at: None,
        }
    }

    /// Токен с историей состояний: `created_ms` — первое появление узла,
    /// `changed_ms` — последняя активация (колонка `state_changed_at`).
    fn token_reactivated(
        instance: &str,
        element: &str,
        state: &str,
        created_ms: i64,
        changed_ms: i64,
    ) -> TokenRow {
        TokenRow {
            instance_id: instance.to_string(),
            element_id: element.to_string(),
            state: state.to_string(),
            created_at: at(created_ms),
            state_changed_at: Some(at(changed_ms)),
        }
    }

    fn step_log(instance: &str, element: &str, actor: &str, at_ms: i64) -> StepLogRow {
        StepLogRow {
            instance_id: instance.to_string(),
            element_id: element.to_string(),
            actor: Some(actor.to_string()),
            created_at: at(at_ms),
        }
    }

    fn snapshot(
        history: Vec<HistoryRow>,
        instances: Vec<LegacyInstance>,
        tokens: Vec<TokenRow>,
        step_log: Vec<StepLogRow>,
    ) -> Snapshot {
        Snapshot {
            history,
            definitions: definitions(),
            instances,
            tokens,
            step_log,
            orders: Vec::new(),
            truncated: false,
        }
    }

    fn ask(body: Value) -> Request {
        serde_json::from_value(body).expect("запрос разбирается")
    }

    fn find_step<'a>(reply: &'a Reply, element: &str) -> &'a StepMetrics {
        reply
            .steps
            .iter()
            .find(|step| step.element_id == element)
            .expect("шаг есть в ответе")
    }


    #[test]
    fn aggregate_pairs_vendor_tokens_with_step_log() {
        let snapshot = snapshot(
            vec![
                started("A", T0),
                arrived("A", "Task_Accept", T0 + 10_000),
                token_done("A", "Task_Accept", T0 + 70_000),
                arrived("A", "End_Ok", T0 + 70_000),
                token_done("A", "End_Ok", T0 + 70_000),
                finished("A", T0 + 70_000),
            ],
            vec![legacy("A", "ACTIVE", T0, T0 + 70_000)],
            vec![token("A", "Task_Accept", "COMPLETED", T0 + 10_000)],
            vec![step_log("A", "Task_Accept", "operator", T0 + 65_000)],
        );

        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({})),
            &Options::default(),
            T0 + 120_000,
        );

        assert!(reply.ok);
        assert_eq!(reply.status, Some(200));
        assert_eq!(reply.sla_minutes, DEFAULT_SLA_MINUTES);
        assert_eq!(reply.generated_at, iso(Some(T0 + 120_000)).unwrap());
        assert!(reply.warnings.is_empty(), "{:?}", reply.warnings);
        assert_eq!(reply.totals.definitions, 1);
        assert_eq!(reply.totals.events, 6);

        let process = &reply.processes[0];
        assert_eq!(process.process_id, "zakaz");
        assert_eq!(process.process_name.as_deref(), Some("Заказ молока"));
        assert_eq!(process.runs, 1);
        assert_eq!(process.completed, 1);
        assert_eq!(process.active, 0);
        assert_eq!(process.average_cycle_ms, Some(70_000));
        assert_eq!(process.median_cycle_ms, Some(70_000));
        assert_eq!(process.min_cycle_ms, Some(70_000));
        assert_eq!(process.max_cycle_ms, Some(70_000));

        // Роль и время — из аудита шага (мс), а не из истории (секунды).
        let accept = find_step(&reply, "Task_Accept");
        assert_eq!(accept.role.as_deref(), Some("operator"));
        assert_eq!(accept.samples, 1);
        assert_eq!(accept.average_ms, 55_000);
        assert_eq!(accept.sla_breaches, 0);

        // Роли: `operator` у задачи и «роль неизвестна» у конечного узла.
        assert_eq!(reply.roles.len(), 2);
        let operator = reply
            .roles
            .iter()
            .find(|role| role.role.as_deref() == Some("operator"))
            .expect("роль исполнителя есть в ответе");
        assert_eq!(operator.steps, 1);
        assert_eq!(operator.average_ms, 55_000);
        assert_eq!(operator.sla_breaches, 0);

        let run = &process.last_runs[0];
        assert_eq!(run.status, "COMPLETED");
        assert_eq!(run.finished_by.as_deref(), Some("event"));
        assert_eq!(run.duration_ms, Some(70_000));
        assert_eq!(run.started_at, iso(Some(T0)));
        assert_eq!(run.last_node.as_deref(), Some("End_Ok"));
        assert!(!run.sla_breached);

        assert_eq!(
            process.outcomes,
            vec![OutcomeCount {
                node_id: "End_Ok".to_string(),
                runs: 1,
            }]
        );
        // «Брак» найден по имени `endEvent`, но ни один запуск туда не ушёл.
        assert_eq!(reply.defect_end_events, vec!["End_Defect".to_string()]);
        assert_eq!(process.defect_runs, 0);
        assert_eq!(process.defect_share, Some(0.0));
        assert_eq!(reply.totals.steps, 2);
        assert_eq!(reply.totals.open_steps, 0);
    }

    #[test]
    fn finish_without_event_is_recognised_by_end_event_of_schema() {
        let snapshot = snapshot(
            vec![
                started("B", T0),
                arrived("B", "Task_Accept", T0 + 5_000),
                arrived("B", "End_Ok", T0 + 50_000),
            ],
            vec![legacy("B", "ACTIVE", T0, T0 + 50_000)],
            Vec::new(),
            Vec::new(),
        );

        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({})),
            &Options::default(),
            T0 + 60_000,
        );

        let run = &reply.processes[0].last_runs[0];
        assert_eq!(run.status, "COMPLETED");
        assert_eq!(run.finished_by.as_deref(), Some("graph"));
        assert_eq!(run.duration_ms, Some(50_000));
        assert_eq!(run.finished_at, iso(Some(T0 + 50_000)));
    }

    #[test]
    fn run_stopped_on_user_task_is_not_finished() {
        let snapshot = snapshot(
            vec![started("C", T0), arrived("C", "Task_Accept", T0 + 3_000)],
            vec![legacy("C", "ACTIVE", T0, T0 + 3_000)],
            Vec::new(),
            Vec::new(),
        );

        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({})),
            &Options::default(),
            T0 + 4 * MINUTE,
        );

        let process = &reply.processes[0];
        assert_eq!(process.active, 1);
        assert_eq!(process.completed, 0);
        assert_eq!(process.average_cycle_ms, None);
        assert_eq!(process.average_step_ms, Some(4 * MINUTE - 3_000));

        let run = &process.last_runs[0];
        assert_eq!(run.status, "ACTIVE");
        assert_eq!(run.finished_by, None);
        assert_eq!(run.finished_at, None);
        assert_eq!(run.last_node.as_deref(), Some("Task_Accept"));
    }


    #[test]
    fn open_step_past_threshold_is_reported_as_sla_breach() {
        let snapshot = snapshot(
            vec![started("D", T0), arrived("D", "Task_Accept", T0)],
            vec![legacy("D", "ACTIVE", T0, T0)],
            Vec::new(),
            Vec::new(),
        );

        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({ "slaMinutes": 30 })),
            &Options::default(),
            T0 + 45 * MINUTE,
        );

        assert_eq!(reply.sla_minutes, 30);
        let process = &reply.processes[0];
        assert_eq!(process.sla_runs, 1);
        assert_eq!(process.sla_steps, 1);
        assert_eq!(process.open_steps, 1);

        let run = &process.last_runs[0];
        assert!(run.sla_breached);
        assert_eq!(run.breached_steps.len(), 1);
        assert_eq!(run.breached_steps[0].element_id, "Task_Accept");
        assert_eq!(run.breached_steps[0].waited_ms, 45 * MINUTE);
        assert!(run.breached_steps[0].open);

        assert_eq!(find_step(&reply, "Task_Accept").sla_breaches, 1);
        assert_eq!(reply.roles[0].role, None);
        assert_eq!(reply.roles[0].sla_breaches, 1);

        // Опечатка в пороге не отключает SLA: он прижимается к 1..=1440.
        let clamped = aggregate(
            &snapshot,
            &ask(serde_json::json!({ "slaMinutes": 0 })),
            &Options::default(),
            T0,
        );
        assert_eq!(clamped.sla_minutes, 1);
        let clamped = aggregate(
            &snapshot,
            &ask(serde_json::json!({ "slaMinutes": 99_999 })),
            &Options::default(),
            T0,
        );
        assert_eq!(clamped.sla_minutes, MAX_SLA_MINUTES);
    }

    #[test]
    fn defect_share_counts_runs_on_defect_end_events() {
        let snapshot = snapshot(
            vec![
                started("E1", T0),
                arrived("E1", "End_Defect", T0 + 30_000),
                started("E2", T0),
                arrived("E2", "End_Ok", T0 + 20_000),
            ],
            vec![
                legacy("E1", "ACTIVE", T0, T0),
                legacy("E2", "ACTIVE", T0, T0),
            ],
            Vec::new(),
            Vec::new(),
        );

        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({})),
            &Options::default(),
            T0 + MINUTE,
        );

        let process = &reply.processes[0];
        assert_eq!(process.completed, 2);
        assert_eq!(process.defect_runs, 1);
        assert_eq!(process.defect_share, Some(0.5));
        assert_eq!(reply.defect_end_events, vec!["End_Defect".to_string()]);
        assert_eq!(
            process.outcomes,
            vec![
                OutcomeCount {
                    node_id: "End_Defect".to_string(),
                    runs: 1,
                },
                OutcomeCount {
                    node_id: "End_Ok".to_string(),
                    runs: 1,
                },
            ]
        );

        // Явный список из запроса перебивает эвристику по имени узла.
        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({ "defectNodes": ["End_Ok"] })),
            &Options::default(),
            T0 + MINUTE,
        );
        assert_eq!(reply.defect_end_events, vec!["End_Ok".to_string()]);
        assert_eq!(reply.processes[0].defect_runs, 1);
        assert_eq!(reply.processes[0].defect_share, Some(0.5));
    }

    #[test]
    fn legacy_only_instance_uses_projection_times() {
        let snapshot = Snapshot {
            history: Vec::new(),
            definitions: vec![DefinitionRow {
                id: "legacy_proc".to_string(),
                def_key: "legacy_proc".to_string(),
                name: Some("Старый процесс".to_string()),
                bpmn_xml: None,
            }],
            instances: vec![LegacyInstance {
                id: "L".to_string(),
                definition_id: "legacy_proc".to_string(),
                status: "COMPLETED".to_string(),
                created_at: at(T0),
                updated_at: at(T0 + 90_000),
            }],
            tokens: vec![
                token("L", "Task_Start", "COMPLETED", T0 + 5_000),
                token("L", "Task_Check", "COMPLETED", T0 + 40_000),
            ],
            step_log: vec![
                step_log("L", "Task_Start", "operator", T0 + 30_000),
                step_log("L", "Task_Check", "technologist", T0 + 90_000),
            ],
            orders: Vec::new(),
            truncated: false,
        };

        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({})),
            &Options::default(),
            T0 + 120_000,
        );

        let process = &reply.processes[0];
        assert_eq!(process.process_id, "legacy_proc");
        assert_eq!(process.process_name.as_deref(), Some("Старый процесс"));
        assert_eq!(process.completed, 1);
        assert_eq!(process.average_cycle_ms, Some(90_000));
        // Схемы нет — «брак» не определить, и это сказано в предупреждении.
        assert_eq!(process.defect_share, None);
        assert!(
            reply
                .warnings
                .iter()
                .any(|warning| warning.contains("нет схемы")),
            "{:?}",
            reply.warnings
        );

        assert_eq!(find_step(&reply, "Task_Start").average_ms, 25_000);
        assert_eq!(
            find_step(&reply, "Task_Check").role.as_deref(),
            Some("technologist")
        );

        let run = &process.last_runs[0];
        assert_eq!(run.finished_by.as_deref(), Some("legacy"));
        assert_eq!(run.finished_at, iso(Some(T0 + 90_000)));
        assert_eq!(run.last_node, None);
    }

    #[test]
    fn last_runs_carry_order_number_from_orders_table() {
        // Заказ связывает два контура: у завода `inst-cust-1`, у поставщика
        // `inst-supp-1`. Панель подписывает оба столбика номером заказа.
        let mut linked = snapshot(
            vec![
                started("inst-cust-1", T0),
                arrived("inst-cust-1", "Task_Accept", T0 + 10_000),
            ],
            vec![
                legacy("inst-cust-1", "ACTIVE", T0, T0 + 10_000),
                legacy("inst-supp-1", "ACTIVE", T0, T0 + 10_000),
            ],
            Vec::new(),
            Vec::new(),
        );
        linked.orders = vec![OrderLink {
            order_num: "№1042".to_string(),
            customer_instance_id: Some("inst-cust-1".to_string()),
            supplier_instance_id: Some("inst-supp-1".to_string()),
        }];
        let reply = aggregate(
            &linked,
            &ask(serde_json::json!({})),
            &Options::default(),
            T0 + 60_000,
        );

        let order_of = |instance: &str| {
            reply
                .processes
                .iter()
                .flat_map(|process| process.last_runs.iter())
                .find(|run| run.instance_id == instance)
                .and_then(|run| run.order_num.clone())
        };

        assert_eq!(order_of("inst-cust-1").as_deref(), Some("№1042"));
        assert_eq!(order_of("inst-supp-1").as_deref(), Some("№1042"));

        // Экземпляр вне заказа: подписи нет, панель покажет id (поведение до M8).
        let mut without = snapshot(
            Vec::new(),
            vec![legacy("inst-cust-2", "ACTIVE", T0, T0)],
            Vec::new(),
            Vec::new(),
        );
        without.orders = Vec::new();
        let reply = aggregate(
            &without,
            &ask(serde_json::json!({})),
            &Options::default(),
            T0 + 60_000,
        );
        assert_eq!(reply.processes[0].last_runs[0].order_num, None);
    }

    #[test]
    fn sla_counts_waiting_from_last_activation_of_token() {
        // Узел впервые появился в T0 и был закрыт, а за 5 минут до «сейчас»
        // активирован снова — так выглядит возврат на шаг (переработка партии).
        let now = T0 + 200 * MINUTE;
        let reactivated = snapshot(
            Vec::new(),
            vec![legacy("R", "ACTIVE", T0, now)],
            vec![token_reactivated(
                "R",
                "Task_Accept",
                "ACTIVE",
                T0,
                now - 5 * MINUTE,
            )],
            Vec::new(),
        );

        let reply = aggregate(
            &reactivated,
            &ask(serde_json::json!({ "slaMinutes": 30 })),
            &Options::default(),
            now,
        );

        let process = &reply.processes[0];
        assert_eq!(process.open_steps, 1);
        assert_eq!(process.sla_runs, 0, "5 минут ожидания при SLA 30 — не просрочка");
        assert_eq!(process.sla_steps, 0);

        let run = &process.last_runs[0];
        assert!(!run.sla_breached);
        assert!(run.breached_steps.is_empty());
        assert_eq!(find_step(&reply, "Task_Accept").average_ms, 5 * MINUTE);

        // Строка без `state_changed_at` (до миграции 0006): поведение прежнее —
        // ожидание от первого появления узла, поэтому SLA видит просрочку.
        let stale = snapshot(
            Vec::new(),
            vec![legacy("R", "ACTIVE", T0, now)],
            vec![token("R", "Task_Accept", "ACTIVE", T0)],
            Vec::new(),
        );
        let stale_reply = aggregate(
            &stale,
            &ask(serde_json::json!({ "slaMinutes": 30 })),
            &Options::default(),
            now,
        );

        assert_eq!(stale_reply.processes[0].sla_runs, 1);
        assert_eq!(
            find_step(&stale_reply, "Task_Accept").average_ms,
            200 * MINUTE
        );
    }


    #[test]
    fn request_filters_process_and_clamps_limit() {
        let snapshot = snapshot(
            vec![
                started("F1", T0),
                arrived("F1", "End_Ok", T0 + 10_000),
                started("F2", T0 + 1_000),
                arrived("F2", "End_Ok", T0 + 20_000),
            ],
            vec![
                legacy("F1", "ACTIVE", T0, T0),
                legacy("F2", "ACTIVE", T0 + 1_000, T0 + 1_000),
            ],
            Vec::new(),
            Vec::new(),
        );

        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({ "limit": 1 })),
            &Options::default(),
            T0 + MINUTE,
        );
        assert_eq!(reply.processes[0].runs, 2);
        assert_eq!(reply.processes[0].last_runs.len(), 1);
        // Новые сверху: первым идёт запуск, стартовавший позже.
        assert_eq!(reply.processes[0].last_runs[0].instance_id, "F2");

        // Предел не растягивается больше `MAX_LIMIT`.
        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({ "limit": 99_999 })),
            &Options::default(),
            T0 + MINUTE,
        );
        assert_eq!(reply.processes[0].last_runs.len(), 2);

        // `id` вендорного определения приводится к `def_key`: кабинет знает
        // только бизнес-ключ процесса.
        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({ "processId": "zakaz:v1" })),
            &Options::default(),
            T0 + MINUTE,
        );
        assert_eq!(reply.processes.len(), 1);
        assert_eq!(reply.processes[0].process_id, "zakaz");

        let reply = aggregate(
            &snapshot,
            &ask(serde_json::json!({ "processId": "nope" })),
            &Options::default(),
            T0 + MINUTE,
        );
        assert!(reply.processes.is_empty());
        assert!(
            reply.warnings.iter().any(|warning| warning.contains("nope")),
            "{:?}",
            reply.warnings
        );
    }

    /// Стор для проверки `handle_bytes` без Postgres.
    struct Fixed(Snapshot);

    #[async_trait::async_trait]
    impl MetricsStore for Fixed {
        async fn load(&self, _limits: Limits) -> anyhow::Result<Snapshot> {
            Ok(self.0.clone())
        }
    }

    struct Broken;

    #[async_trait::async_trait]
    impl MetricsStore for Broken {
        async fn load(&self, _limits: Limits) -> anyhow::Result<Snapshot> {
            anyhow::bail!("нет соединения")
        }
    }

    #[tokio::test]
    async fn broken_json_is_400_and_store_error_is_500() {
        let reply = handle_bytes(&Fixed(Snapshot::default()), b"{").await;
        assert!(!reply.ok);
        assert_eq!(reply.status, Some(400));
        assert!(reply.error.unwrap().contains("JSON"));

        let reply = handle_bytes(&Broken, b"{}").await;
        assert!(!reply.ok);
        assert_eq!(reply.status, Some(500));
        assert_eq!(reply.error.as_deref(), Some("ошибка чтения метрик из БД"));
    }

    #[tokio::test]
    async fn empty_snapshot_returns_empty_metrics() {
        let reply = handle_bytes(&Fixed(Snapshot::default()), b"{}").await;

        assert!(reply.ok);
        assert_eq!(reply.status, Some(200));
        assert!(reply.processes.is_empty());
        assert!(reply.steps.is_empty());
        assert_eq!(reply.totals.runs, 0);
    }

    #[test]
    fn timestamps_parse_epoch_seconds_and_iso() {
        assert_eq!(parse_timestamp_ms("1700000000"), Some(T0));
        assert_eq!(parse_timestamp_ms("1700000000123"), Some(T0 + 123));
        assert_eq!(parse_timestamp_ms("2023-11-14T22:13:20.000Z"), Some(T0));
        assert_eq!(parse_timestamp_ms("2023-11-14 22:13:20.123456"), Some(T0 + 123));
        assert_eq!(parse_timestamp_ms("не время"), None);
        assert_eq!(iso(Some(T0 + 5)).as_deref(), Some("2023-11-14T22:13:20.005Z"));
        assert_eq!(iso(None), None);
    }

    #[test]
    fn end_events_are_read_from_schema() {
        let ends = end_events(xml());

        assert_eq!(ends.len(), 2, "только `endEvent`, не `userTask`");
        assert_eq!(
            ends.get("End_Defect").cloned().flatten().as_deref(),
            Some("Брак")
        );
        assert!(end_events("не xml").is_empty());
    }
}



