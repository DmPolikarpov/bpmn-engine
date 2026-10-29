//! Серверная схема проекта (M6, `erp-simulator/docs/dairy-plant-roadmap.md`, 5).
//!
//! Проект — одна BPMN-схема с процессами сторон (`bpmn:collaboration`). До этой
//! миграции он жил только в `localStorage` браузера: вторая сессия получала
//! демо-схему SDK, а номер версии знал только тот браузер, который схему правил.
//! Здесь проект переезжает в БД движка:
//!
//! - кабинет при входе спрашивает список (`list`) и берёт схему с сервера;
//! - «Задеплоить» поднимает версию (`deploy`) и пишет её в историю;
//! - второй браузер видит ту же схему и тот же номер версии, а `localStorage`
//!   остаётся кэшем, а не источником истины.
//!
//! Устройство повторяет API заказов (`orders.rs`): движок владеет таблицами и
//! отвечает на NATS-запрос [`API_SUBJECT`], HTTP-шлюз перекладывает REST в этот
//! запрос и обратно. Валидация — одна на оба входа, формат ошибок — тот же
//! (`details[].field` + `.message` из [`crate::orders::FieldError`]).
//!
//! Версия монотонна на проект: её считает `UPDATE … version + 1` в одной
//! транзакции с записью истории, поэтому две одновременные правки дают `v2` и
//! `v3`, а не две записи «v2».

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use crate::orders::FieldError;

/// Subject запроса кабинета (`nc.request` из HTTP-шлюза).
pub const API_SUBJECT: &str = "mes.in.v1.api.projects";

/// Очередь: на запрос отвечает одна реплика движка, а не все три.
pub const API_QUEUE: &str = "projects-api";

/// Идентификатор проекта: его выдаёт кабинет (`project-…`), он же ключ кэша.
const MAX_PROJECT_ID: usize = 64;
const MAX_NAME: usize = 160;

/// Предел схемы. Тело запроса шлюза ограничено `JSON_BODY_LIMIT` (10 МБ), но
/// 4 МБ уже подозрительны: демо-проект завода занимает ~90 КБ.
const MAX_XML_BYTES: usize = 4 * 1024 * 1024;

/// Сколько проектов отдавать в списке: кабинету нужно выбрать, а не выгрузить.
const LIST_LIMIT: i64 = 100;

/// Сколько версий показывать в истории (`versions`).
const VERSIONS_LIMIT: i64 = 20;

/// Проект в списке: без схемы, чтобы `list` не тянул мегабайты XML.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectSummary {
    pub project_id: String,
    pub name: String,
    /// Последняя версия; `1` — проект задеплоен один раз.
    pub version: i32,
    /// Размер схемы в байтах UTF-8: видно, что XML доехал целиком.
    pub xml_bytes: i32,
    /// Роль того, кто задеплоил (`SALES`, `DIRECTOR`, …); пусто — скрипт.
    pub updated_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Проект целиком — ответ на `get` и на `deploy`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployedProject {
    pub project_id: String,
    pub name: String,
    pub version: i32,
    pub bpmn_xml: String,
    pub updated_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Строка истории деплоев: кто и когда поднял версию.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectVersion {
    pub version: i32,
    pub xml_bytes: i32,
    pub updated_by: String,
    pub created_at: DateTime<Utc>,
}

/// Проект, который кабинет присылает на деплой.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewProject {
    pub project_id: String,
    pub name: String,
    pub bpmn_xml: String,
    /// Роль сессии — та же, что уходит в `X-Actor-Role` событий (M6).
    #[serde(default)]
    pub updated_by: String,
}

impl NewProject {
    /// Обрезает пробелы: их не видно в UI, но они ломают сравнение ключей.
    pub fn normalized(mut self) -> Self {
        self.project_id = self.project_id.trim().to_string();
        self.name = self.name.trim().to_string();
        self.updated_by = self.updated_by.trim().to_string();
        self
    }
}


/// Проверка проекта на входе: идентификатор, имя и «это вообще BPMN?».
pub fn validate(project: &NewProject) -> Vec<FieldError> {
    let mut errors = Vec::new();

    check_project_id(&mut errors, &project.project_id);
    check_text(&mut errors, "name", &project.name, MAX_NAME);
    check_xml(&mut errors, &project.bpmn_xml);

    errors
}

fn check_project_id(errors: &mut Vec<FieldError>, value: &str) {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        errors.push(FieldError::new("projectId", "поле обязательно"));
    } else if trimmed.chars().count() > MAX_PROJECT_ID {
        errors.push(FieldError::new(
            "projectId",
            &format!("не длиннее {MAX_PROJECT_ID} символов"),
        ));
    } else if !trimmed
        .chars()
        .all(|symbol| symbol.is_ascii_alphanumeric() || matches!(symbol, '-' | '_' | '.' | ':'))
    {
        // Идентификатор уезжает в URL (`PUT /api/v1/projects/:projectId`),
        // поэтому `?`, `#`, `/` и пробелы запрещены ещё на входе.
        errors.push(FieldError::new(
            "projectId",
            "только латиница, цифры и `-`, `_`, `.`, `:`",
        ));
    }
}

fn check_text(errors: &mut Vec<FieldError>, field: &str, value: &str, max_len: usize) {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        errors.push(FieldError::new(field, "поле обязательно"));
    } else if trimmed.chars().count() > max_len {
        errors.push(FieldError::new(
            field,
            &format!("не длиннее {max_len} символов"),
        ));
    }
}

fn check_xml(errors: &mut Vec<FieldError>, xml: &str) {
    if xml.trim().is_empty() {
        errors.push(FieldError::new("bpmnXml", "поле обязательно"));
        return;
    }

    if xml.len() > MAX_XML_BYTES {
        errors.push(FieldError::new(
            "bpmnXml",
            &format!("не больше {} КБ", MAX_XML_BYTES / 1024),
        ));
        return;
    }

    if !looks_like_bpmn(xml) {
        errors.push(FieldError::new(
            "bpmnXml",
            "нужна BPMN-схема: в тексте нет `definitions`/`process`",
        ));
    }
}

/// Грубая проверка «это BPMN».
///
/// Полный разбор делает кабинет (`validateEngineSchema` в SDK): серверу важно не
/// принять случайный текст, а не повторить правила BPMN. Префикс пространства
/// имён бывает любым (`bpmn:`, `bpmn2:` или без него), поэтому ищем подстроки.
fn looks_like_bpmn(xml: &str) -> bool {
    xml.contains("definitions") && xml.contains("process")
}


/// Запрос кабинета. Неизвестные поля игнорируются: контракт API проектов
/// расширяется аддитивно, как у заказов.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub action: String,
    #[serde(default)]
    pub project_id: Option<String>,
    /// Номер версии для `get`; без него отдаётся последняя.
    #[serde(default)]
    pub version: Option<i32>,
    #[serde(default)]
    pub project: Option<NewProject>,
}

/// Ответ API. `status` повторяет HTTP-код, чтобы шлюз не угадывал его по
/// тексту ошибки и отдавал 400/404 как есть.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<DeployedProject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projects: Option<Vec<ProjectSummary>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub versions: Option<Vec<ProjectVersion>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<FieldError>>,
}

impl Response {
    pub fn with_project(project: DeployedProject) -> Self {
        Self {
            ok: true,
            project: Some(project),
            ..Default::default()
        }
    }

    pub fn with_projects(projects: Vec<ProjectSummary>) -> Self {
        Self {
            ok: true,
            projects: Some(projects),
            ..Default::default()
        }
    }

    pub fn with_versions(versions: Vec<ProjectVersion>) -> Self {
        Self {
            ok: true,
            versions: Some(versions),
            ..Default::default()
        }
    }

    /// Отказ с HTTP-кодом и текстом; `details` — только у валидации.
    pub fn failure(status: u16, error: impl Into<String>) -> Self {
        Self {
            ok: false,
            status: Some(status),
            error: Some(error.into()),
            ..Default::default()
        }
    }

    pub fn invalid(details: Vec<FieldError>) -> Self {
        Self {
            ok: false,
            status: Some(400),
            error: Some("Проект не прошёл валидацию".to_string()),
            details: Some(details),
            ..Default::default()
        }
    }
}


/// Хранилище проектов. Трейт отделяет API от Postgres: контракт запроса и
/// ответа проверяется in-memory стором, без живой БД.
#[async_trait::async_trait]
pub trait ProjectStore: Send + Sync {
    async fn list(&self) -> anyhow::Result<Vec<ProjectSummary>>;
    /// `version = None` — последняя версия. `None` в результате — проекта нет.
    async fn get(
        &self,
        project_id: &str,
        version: Option<i32>,
    ) -> anyhow::Result<Option<DeployedProject>>;
    /// Вставка или новая версия; возвращает сохранённое состояние.
    async fn deploy(&self, project: NewProject) -> anyhow::Result<DeployedProject>;
    /// История версий; `None` — проекта нет.
    async fn versions(&self, project_id: &str) -> anyhow::Result<Option<Vec<ProjectVersion>>>;
}

/// Обработчик запроса: единственное место, где решается, что вернуть.
pub async fn handle(store: &dyn ProjectStore, request: Request) -> Response {
    let action = request.action.trim().to_ascii_lowercase();

    match action.as_str() {
        "list" => match store.list().await {
            Ok(projects) => Response::with_projects(projects),
            Err(error) => internal(error),
        },
        "get" => {
            let Some(project_id) = non_empty(request.project_id.as_deref()) else {
                return Response::failure(400, "не указан проект (projectId)");
            };

            if matches!(request.version, Some(version) if version <= 0) {
                return Response::failure(400, "версия должна быть положительным числом");
            }

            match store.get(project_id, request.version).await {
                Ok(Some(project)) => Response::with_project(project),
                Ok(None) => Response::failure(404, missing_message(project_id, request.version)),
                Err(error) => internal(error),
            }
        }
        "versions" => {
            let Some(project_id) = non_empty(request.project_id.as_deref()) else {
                return Response::failure(400, "не указан проект (projectId)");
            };

            match store.versions(project_id).await {
                Ok(Some(versions)) => Response::with_versions(versions),
                Ok(None) => Response::failure(404, format!("проект {project_id} не найден")),
                Err(error) => internal(error),
            }
        }
        "deploy" => {
            let Some(project) = request.project else {
                return Response::failure(400, "не передано тело проекта (project)");
            };
            let project = project.normalized();
            let errors = validate(&project);

            if !errors.is_empty() {
                return Response::invalid(errors);
            }

            match store.deploy(project).await {
                Ok(project) => Response::with_project(project),
                Err(error) => internal(error),
            }
        }
        other => Response::failure(400, format!("неизвестное действие: {other}")),
    }
}

/// Разбор тела NATS-запроса и обработка. Битый JSON — это 400, а не 500:
/// кабинет должен увидеть причину, а не «оркестратор упал».
pub async fn handle_bytes(store: &dyn ProjectStore, body: &[u8]) -> Response {
    match serde_json::from_slice::<Request>(body) {
        Ok(request) => handle(store, request).await,
        Err(error) => Response::failure(400, format!("некорректный JSON запроса: {error}")),
    }
}

fn missing_message(project_id: &str, version: Option<i32>) -> String {
    match version {
        Some(version) => format!("проект {project_id} версии {version} не найден"),
        None => format!("проект {project_id} не найден"),
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn internal(error: anyhow::Error) -> Response {
    eprintln!("❌ [Projects] ошибка хранилища: {error:#}");
    Response::failure(500, "ошибка хранилища проектов")
}


/// Postgres-хранилище проектов (миграция `0007_projects.sql`).
pub struct PgProjectStore {
    pool: PgPool,
}

/// Общий SELECT последней версии проекта.
const SELECT_PROJECT: &str = r#"
SELECT
    p.project_id,
    p.name,
    p.version,
    p.bpmn_xml,
    p.updated_by,
    p.created_at,
    p.updated_at
FROM projects p
"#;

fn map_project(row: &sqlx::postgres::PgRow) -> DeployedProject {
    DeployedProject {
        project_id: row.get("project_id"),
        name: row.get("name"),
        version: row.get("version"),
        bpmn_xml: row.get("bpmn_xml"),
        updated_by: row.get("updated_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

impl PgProjectStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Последняя версия либо версия из истории.
    async fn fetch(
        &self,
        project_id: &str,
        version: Option<i32>,
    ) -> anyhow::Result<Option<DeployedProject>> {
        let row = match version {
            None => {
                let sql = format!("{SELECT_PROJECT} WHERE p.project_id = $1");
                sqlx::query(&sql)
                    .bind(project_id)
                    .fetch_optional(&self.pool)
                    .await?
            }
            Some(version) => {
                // История: имя и дата создания проекта — из `projects`,
                // `updated_at` — время деплоя именно этой версии.
                let sql = r#"
                SELECT
                    p.project_id,
                    p.name,
                    v.version,
                    v.bpmn_xml,
                    v.updated_by,
                    p.created_at,
                    v.created_at AS updated_at
                FROM project_versions v
                JOIN projects p ON p.project_id = v.project_id
                WHERE v.project_id = $1 AND v.version = $2
                "#;
                sqlx::query(sql)
                    .bind(project_id)
                    .bind(version)
                    .fetch_optional(&self.pool)
                    .await?
            }
        };

        Ok(row.as_ref().map(map_project))
    }
}


#[async_trait::async_trait]
impl ProjectStore for PgProjectStore {
    async fn list(&self) -> anyhow::Result<Vec<ProjectSummary>> {
        // `octet_length` — байты UTF-8 (кириллица в схеме считается верно);
        // схему в список не тянем, иначе ответ на `list` весит мегабайты.
        let rows = sqlx::query(
            r#"
            SELECT
                project_id,
                name,
                version,
                octet_length(bpmn_xml)::INTEGER AS xml_bytes,
                updated_by,
                created_at,
                updated_at
            FROM projects
            ORDER BY updated_at DESC, project_id
            LIMIT $1
            "#,
        )
        .bind(LIST_LIMIT)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| ProjectSummary {
                project_id: row.get("project_id"),
                name: row.get("name"),
                version: row.get("version"),
                xml_bytes: row.get("xml_bytes"),
                updated_by: row.get("updated_by"),
                created_at: row.get("created_at"),
                updated_at: row.get("updated_at"),
            })
            .collect())
    }

    async fn get(
        &self,
        project_id: &str,
        version: Option<i32>,
    ) -> anyhow::Result<Option<DeployedProject>> {
        self.fetch(project_id, version).await
    }

    async fn deploy(&self, project: NewProject) -> anyhow::Result<DeployedProject> {
        // Версия и история — в одной транзакции: UPDATE блокирует строку, и
        // вторая одновременная правка увидит уже поднятую версию, а не ту же.
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            INSERT INTO projects (project_id, name, version, bpmn_xml, updated_by)
            VALUES ($1, $2, 1, $3, $4)
            ON CONFLICT (project_id) DO UPDATE
            SET name = EXCLUDED.name,
                version = projects.version + 1,
                bpmn_xml = EXCLUDED.bpmn_xml,
                updated_by = EXCLUDED.updated_by,
                updated_at = NOW()
            RETURNING
                project_id, name, version, bpmn_xml, updated_by, created_at, updated_at
            "#,
        )
        .bind(&project.project_id)
        .bind(&project.name)
        .bind(&project.bpmn_xml)
        .bind(&project.updated_by)
        .fetch_one(&mut *tx)
        .await?;

        let stored = map_project(&row);

        // История: та же тройка (проект, версия, схема). Конфликта быть не
        // может — версию выдал UPDATE выше; ошибка вставки откатит транзакцию,
        // чтобы «последняя версия» и история не разъехались.
        sqlx::query(
            r#"
            INSERT INTO project_versions (project_id, version, bpmn_xml, updated_by, created_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&stored.project_id)
        .bind(stored.version)
        .bind(&stored.bpmn_xml)
        .bind(&stored.updated_by)
        .bind(stored.updated_at)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(stored)
    }

    async fn versions(&self, project_id: &str) -> anyhow::Result<Option<Vec<ProjectVersion>>> {
        let exists = sqlx::query("SELECT 1 FROM projects WHERE project_id = $1")
            .bind(project_id)
            .fetch_optional(&self.pool)
            .await?;

        if exists.is_none() {
            return Ok(None);
        }

        let rows = sqlx::query(
            r#"
            SELECT
                version,
                octet_length(bpmn_xml)::INTEGER AS xml_bytes,
                updated_by,
                created_at
            FROM project_versions
            WHERE project_id = $1
            ORDER BY version DESC
            LIMIT $2
            "#,
        )
        .bind(project_id)
        .bind(VERSIONS_LIMIT)
        .fetch_all(&self.pool)
        .await?;

        Ok(Some(
            rows.iter()
                .map(|row| ProjectVersion {
                    version: row.get("version"),
                    xml_bytes: row.get("xml_bytes"),
                    updated_by: row.get("updated_by"),
                    created_at: row.get("created_at"),
                })
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use chrono::{Duration, TimeZone};
    use serde_json::json;

    use super::*;

    /// Метка времени, сдвинутая на `offset` секунд от начала прогона.
    fn at(offset: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 29, 9, 0, 0)
            .single()
            .expect("время существует")
            + Duration::seconds(offset)
    }

    /// Минимальная схема, проходящая проверку «это BPMN».
    fn bpmn(tag: &str) -> String {
        format!(
            concat!(
                r#"<?xml version="1.0" encoding="UTF-8"?>"#,
                r#"<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">"#,
                r#"<bpmn:process id="Process_{0}"/></bpmn:definitions>"#
            ),
            tag
        )
    }

    fn new_project(id: &str, xml: &str) -> NewProject {
        NewProject {
            project_id: id.to_string(),
            name: "Молочный завод".to_string(),
            bpmn_xml: xml.to_string(),
            updated_by: "DIRECTOR".to_string(),
        }
    }

    fn fields(errors: &[FieldError]) -> Vec<&str> {
        errors.iter().map(|error| error.field.as_str()).collect()
    }

    /// In-memory стор: контракт API проверяется без живой БД.
    ///
    /// Версии лежат по возрастанию — как их отдаёт история в Postgres,
    /// только в обратном порядке (`versions`).
    #[derive(Default)]
    struct MemoryStore {
        projects: Mutex<BTreeMap<String, Vec<DeployedProject>>>,
        clock: Mutex<i64>,
    }

    impl MemoryStore {
        /// Следующая метка: `updated_at` обязан расти, иначе порядок списка
        /// зависит от разрешения часов.
        fn tick(&self) -> DateTime<Utc> {
            let mut clock = self.clock.lock().expect("мьютекс");
            *clock += 1;
            at(*clock)
        }
    }


    #[async_trait::async_trait]
    impl ProjectStore for MemoryStore {
        async fn list(&self) -> anyhow::Result<Vec<ProjectSummary>> {
            let projects = self.projects.lock().expect("мьютекс");
            let mut summaries: Vec<ProjectSummary> = projects
                .values()
                .filter_map(|versions| versions.last())
                .map(|project| ProjectSummary {
                    project_id: project.project_id.clone(),
                    name: project.name.clone(),
                    version: project.version,
                    xml_bytes: project.bpmn_xml.len() as i32,
                    updated_by: project.updated_by.clone(),
                    created_at: project.created_at,
                    updated_at: project.updated_at,
                })
                .collect();

            summaries.sort_by(|left, right| {
                right
                    .updated_at
                    .cmp(&left.updated_at)
                    .then(left.project_id.cmp(&right.project_id))
            });

            Ok(summaries)
        }

        async fn get(
            &self,
            project_id: &str,
            version: Option<i32>,
        ) -> anyhow::Result<Option<DeployedProject>> {
            let projects = self.projects.lock().expect("мьютекс");

            Ok(projects.get(project_id).and_then(|versions| match version {
                None => versions.last().cloned(),
                Some(version) => versions
                    .iter()
                    .find(|project| project.version == version)
                    .cloned(),
            }))
        }

        async fn deploy(&self, project: NewProject) -> anyhow::Result<DeployedProject> {
            let mut projects = self.projects.lock().expect("мьютекс");
            let updated_at = self.tick();
            let versions = projects.entry(project.project_id.clone()).or_default();
            let version = versions.last().map(|last| last.version + 1).unwrap_or(1);
            let created_at = versions
                .first()
                .map(|first| first.created_at)
                .unwrap_or(updated_at);

            let stored = DeployedProject {
                project_id: project.project_id,
                name: project.name,
                version,
                bpmn_xml: project.bpmn_xml,
                updated_by: project.updated_by,
                created_at,
                updated_at,
            };

            versions.push(stored.clone());

            Ok(stored)
        }

        async fn versions(&self, project_id: &str) -> anyhow::Result<Option<Vec<ProjectVersion>>> {
            let projects = self.projects.lock().expect("мьютекс");

            Ok(projects.get(project_id).map(|versions| {
                versions
                    .iter()
                    .rev()
                    .map(|project| ProjectVersion {
                        version: project.version,
                        xml_bytes: project.bpmn_xml.len() as i32,
                        updated_by: project.updated_by.clone(),
                        created_at: project.updated_at,
                    })
                    .collect()
            }))
        }
    }

    async fn request(store: &MemoryStore, body: serde_json::Value) -> Response {
        handle_bytes(store, body.to_string().as_bytes()).await
    }

    fn deploy_body(xml: &str) -> serde_json::Value {
        json!({
            "action": "deploy",
            "project": {
                "projectId": "  project-dairy  ",
                "name": "  Молочный завод  ",
                "bpmnXml": xml,
                "updatedBy": " DIRECTOR "
            }
        })
    }

    #[test]
    fn validate_accepts_complete_project() {
        assert!(validate(&new_project("project-dairy", &bpmn("Dairy"))).is_empty());
    }

    #[test]
    fn validate_requires_id_name_and_xml() {
        let project = NewProject {
            project_id: "   ".to_string(),
            name: String::new(),
            bpmn_xml: "  ".to_string(),
            updated_by: String::new(),
        };

        assert_eq!(
            fields(&validate(&project)),
            vec!["projectId", "name", "bpmnXml"]
        );
    }

    #[test]
    fn validate_rejects_unsafe_project_id() {
        let project = new_project("project 1/2?", &bpmn("Dairy"));

        assert_eq!(fields(&validate(&project)), vec!["projectId"]);
    }

    #[test]
    fn validate_limits_project_id_and_name() {
        let project = NewProject {
            project_id: "p".repeat(MAX_PROJECT_ID + 1),
            name: "м".repeat(MAX_NAME + 1),
            bpmn_xml: bpmn("Dairy"),
            updated_by: String::new(),
        };

        assert_eq!(fields(&validate(&project)), vec!["projectId", "name"]);
    }

    #[test]
    fn validate_limits_xml_size() {
        let mut xml = bpmn("Dairy");
        xml.push_str(&"x".repeat(MAX_XML_BYTES));
        let project = new_project("project-dairy", &xml);

        let errors = validate(&project);

        assert_eq!(fields(&errors), vec!["bpmnXml"]);
        assert!(
            errors[0].message.contains("4096"),
            "сообщение объясняет предел: {}",
            errors[0].message
        );
    }

    #[test]
    fn validate_rejects_text_that_is_not_bpmn() {
        let project = new_project("project-dairy", "<html><body>не схема</body></html>");

        assert_eq!(fields(&validate(&project)), vec!["bpmnXml"]);
    }

    #[tokio::test]
    async fn deploy_stores_normalized_project() {
        let store = MemoryStore::default();

        let response = request(&store, deploy_body(&bpmn("Dairy"))).await;

        assert!(response.ok);
        assert_eq!(response.status, None);
        let project = response.project.expect("проект");
        assert_eq!(project.project_id, "project-dairy", "пробелы обрезаны");
        assert_eq!(project.name, "Молочный завод");
        assert_eq!(project.updated_by, "DIRECTOR");
        assert_eq!(project.version, 1);
        assert_eq!(project.created_at, project.updated_at);
    }

    #[tokio::test]
    async fn deploy_bumps_version_and_keeps_history() {
        let store = MemoryStore::default();
        let first = bpmn("Dairy");
        let second = bpmn("DairyV2");

        let created = request(&store, deploy_body(&first)).await;
        let updated = request(&store, deploy_body(&second)).await;

        assert_eq!(created.project.expect("v1").version, 1);
        let updated = updated.project.expect("v2");
        assert_eq!(updated.version, 2);
        assert_eq!(updated.bpmn_xml, second, "последняя версия — новая схема");
        assert!(updated.updated_at > updated.created_at, "время деплоя выросло");

        let latest = request(
            &store,
            json!({"action": "get", "projectId": "project-dairy"}),
        )
        .await;
        assert_eq!(latest.project.expect("последняя").version, 2);

        let old = request(
            &store,
            json!({"action": "get", "projectId": "project-dairy", "version": 1}),
        )
        .await;
        let old = old.project.expect("v1 из истории");
        assert_eq!(old.version, 1);
        assert_eq!(old.bpmn_xml, first, "история не перезаписывается");

        let versions = request(
            &store,
            json!({"action": "versions", "projectId": "project-dairy"}),
        )
        .await;
        let versions = versions.versions.expect("история");
        assert_eq!(
            versions.iter().map(|item| item.version).collect::<Vec<_>>(),
            vec![2, 1],
            "новые версии сверху"
        );
        assert_eq!(versions[1].xml_bytes as usize, first.len());
    }

    #[tokio::test]
    async fn deploy_rejects_invalid_project_with_field_details() {
        let store = MemoryStore::default();
        let mut body = deploy_body(&bpmn("Dairy"));
        body["project"]["projectId"] = json!("project/1");
        body["project"]["bpmnXml"] = json!("не схема");

        let response = request(&store, body).await;

        assert!(!response.ok);
        assert_eq!(response.status, Some(400));
        assert_eq!(
            fields(response.details.as_deref().expect("детали")),
            vec!["projectId", "bpmnXml"]
        );
        assert!(response.project.is_none());
    }

    #[tokio::test]
    async fn deploy_requires_project_body() {
        let store = MemoryStore::default();

        let response = request(&store, json!({"action": "deploy"})).await;

        assert_eq!(response.status, Some(400));
    }

    #[tokio::test]
    async fn get_returns_project_or_not_found() {
        let store = MemoryStore::default();
        request(&store, deploy_body(&bpmn("Dairy"))).await;

        let missing = request(
            &store,
            json!({"action": "get", "projectId": "project-unknown"}),
        )
        .await;
        assert_eq!(missing.status, Some(404));

        let missing_version = request(
            &store,
            json!({"action": "get", "projectId": "project-dairy", "version": 7}),
        )
        .await;
        assert_eq!(missing_version.status, Some(404));
        assert!(missing_version
            .error
            .expect("текст ошибки")
            .contains("версии 7"));

        let without_key = request(&store, json!({"action": "get"})).await;
        assert_eq!(without_key.status, Some(400));

        let bad_version = request(
            &store,
            json!({"action": "get", "projectId": "project-dairy", "version": 0}),
        )
        .await;
        assert_eq!(bad_version.status, Some(400));
    }

    #[tokio::test]
    async fn versions_returns_not_found_for_unknown_project() {
        let store = MemoryStore::default();

        let response = request(
            &store,
            json!({"action": "versions", "projectId": "project-unknown"}),
        )
        .await;

        assert_eq!(response.status, Some(404));
    }

    #[tokio::test]
    async fn list_returns_latest_version_of_every_project_newest_first() {
        let store = MemoryStore::default();
        request(&store, deploy_body(&bpmn("Dairy"))).await;
        request(
            &store,
            json!({
                "action": "deploy",
                "project": {
                    "projectId": "project-supplier",
                    "name": "Поставщик",
                    "bpmnXml": bpmn("Supplier")
                }
            }),
        )
        .await;

        let response = request(&store, json!({"action": "list"})).await;

        let projects = response.projects.expect("список");
        assert_eq!(projects.len(), 2);
        assert_eq!(projects[0].project_id, "project-supplier", "свежий сверху");
        assert!(projects[0].xml_bytes > 0);
        assert_eq!(projects[0].updated_by, "", "скрипт не передал роль");
    }

    #[tokio::test]
    async fn bad_json_and_unknown_action_are_bad_requests() {
        let store = MemoryStore::default();

        let broken = handle_bytes(&store, "{не json".as_bytes()).await;
        assert_eq!(broken.status, Some(400));

        let unknown = request(&store, json!({"action": "publish"})).await;
        assert_eq!(unknown.status, Some(400));
        assert!(unknown.error.expect("текст").contains("publish"));
    }
}
