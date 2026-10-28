//! Проверка прав на `sendEvent` (M6).
//!
//! HTTP-шлюз не знает схемы, поэтому перед публикацией события спрашивает
//! движок: `nc.request(API_SUBJECT, { instanceId, elementId, actor, bpmnXml })`.
//! Ответ синхронный — только так кабинет получает **403** с причиной, а не
//! «ничего не произошло» (ограничение 8, `dairy-plant-roadmap.md`).
//!
//! Проверка не заменяет авторизацию в рантайме: `GraphRuntime::handle_inbound`
//! проверяет те же роли ещё раз, поэтому прямой `nats pub` в
//! `mes.in.v1.event.erp.>` минует шлюз, но не минует движок.
//!
//! Откуда берётся узел:
//!
//! - старт — из `bpmnXml` самого события (экземпляра ещё нет);
//! - завершение шага — из проекции движка: единственный `ACTIVE`-токен
//!   экземпляра (`tokens`). Параллельных веток в демо нет (ограничение 11),
//!   поэтому уточнять токен не требуется.
//!
//! `actor` — **заявленная** роль сессии: в демо нет IdP, кабинет передаёт её
//! заголовком `X-Actor-Role`. Это ограничение M6, а не недосмотр: проверка
//! отвечает на вопрос «можно ли этой роли», а не «кто ты» (см. README, M6).

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

use crate::runtime::mapper;
use crate::runtime::roles::{self, Decision};
use crate::runtime::store::GraphStores;

/// Subject NATS-API проверки прав (зовёт HTTP-шлюз).
pub const API_SUBJECT: &str = "mes.in.v1.api.auth";

/// Очередь API: отвечает одна реплика движка (как у API заказов).
pub const API_QUEUE: &str = "auth-api";

/// Запрос шлюза: кто, в каком экземпляре и какой шаг двигает.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthRequest {
    /// Экземпляр процесса (`inst-client-…`, `inst-deploy-customer`).
    #[serde(default, alias = "instance_id")]
    pub instance_id: String,
    /// Адрес кнопки из события (`element_id`).
    #[serde(default, alias = "element_id")]
    pub element_id: String,
    /// Роль сессии; пусто — событие без роли (NATS-воркер, E2E-скрипт).
    #[serde(default)]
    pub actor: String,
    /// Схема из события: есть только у деплоя и старта.
    #[serde(default, alias = "bpmn_xml")]
    pub bpmn_xml: Option<String>,
}

/// Ответ движка: можно ли двигать шаг и что сказать при отказе.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthReply {
    /// Разрешено ли событие.
    pub allowed: bool,
    /// Готовая причина отказа для кабинета (`""` — отказа нет).
    pub reason: String,
    /// Роли, закреплённые за шагом (пусто — общая очередь).
    pub required: Vec<String>,
    /// Узел, который проверялся (`""` — узел не определён).
    pub element_id: String,
}

impl AuthReply {
    /// Ответ «проверять нечего»: деплой, пустой экземпляр, нет ожидающего шага.
    pub fn allow(element_id: &str) -> Self {
        Self {
            allowed: true,
            reason: String::new(),
            required: Vec::new(),
            element_id: element_id.to_string(),
        }
    }

    fn from_decision(element_id: &str, decision: Decision) -> Self {
        Self {
            allowed: decision.allowed,
            reason: decision.reason,
            required: decision.required,
            element_id: element_id.to_string(),
        }
    }
}

/// Проверять ли роль сессии для этого события.
///
/// Пустая роль — это событие **без** исполнителя: так шлют NATS-воркеры,
/// `nats pub` и E2E-скрипты (`tests/*.e2e.cjs`), и до M6 такие шаги двигались.
/// Отклонять их — значит сломать обратную совместимость ради проверки, которая
/// всё равно не является аутентификацией (IdP в демо нет, роль заявленная).
///
/// Кабинет роль заполняет всегда (`App.jsx`: `client.actor = currentUser.role`),
/// поэтому из интерфейса шаг мимо проверки не провести.
pub fn needs_role_check(actor: &str) -> bool {
    !actor.trim().is_empty()
}

/// Решение по событию шлюза.
pub async fn decide(
    stores: &GraphStores,
    legacy: &PgPool,
    request: &AuthRequest,
) -> anyhow::Result<AuthReply> {
    let node = request.element_id.trim();
    let actor = request.actor.trim();

    // Деплой схемы роль не двигает: шага ещё нет, определение только пишется.
    if mapper::is_deploy_address(request.instance_id.trim(), node) {
        return Ok(AuthReply::allow(node));
    }

    // Событие без роли не проверяется (см. `needs_role_check`): иначе шлюз
    // отвечал бы 403 воркерам и скриптам, которые роль не передают.
    if !needs_role_check(actor) {
        return Ok(AuthReply::allow(node));
    }

    // Старт: экземпляра нет, схема пришла в самом событии.
    if let Some(xml) = request
        .bpmn_xml
        .as_deref()
        .map(str::trim)
        .filter(|xml| !xml.is_empty())
    {
        let roles = roles::start_roles(xml).map_err(anyhow::Error::msg)?;
        let process = roles::process_id(xml).unwrap_or_else(|| request.instance_id.clone());

        return Ok(AuthReply::from_decision(
            "",
            roles::decide_start(actor, &process, &roles),
        ));
    }

    // Завершение шага: узел — из проекции движка (legacy `tokens`).
    let Some((node_id, definition_id)) = waiting_step(legacy, request.instance_id.trim()).await?
    else {
        return Ok(AuthReply::allow(""));
    };

    let Some(xml) = stores.load_xml(&definition_id).await? else {
        eprintln!(
            "⚠️ [Auth] definition_not_deployed instance={} definition={definition_id}",
            request.instance_id
        );
        return Ok(AuthReply::allow(&node_id));
    };

    let roles = roles::node_roles(&xml, &node_id).map_err(anyhow::Error::msg)?;

    Ok(AuthReply::from_decision(
        &node_id,
        roles::decide(actor, &node_id, &roles),
    ))
}

/// Ожидающий шаг экземпляра и его определение: первый `ACTIVE`-токен.
///
/// Источник — legacy-проекция, а не вендорные таблицы: её же читает кабинет, и
/// отдельная схема токенов движка шлюзу не нужна.
async fn waiting_step(
    pool: &PgPool,
    instance_id: &str,
) -> Result<Option<(String, String)>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT t.element_id, i.definition_id
        FROM tokens t
        JOIN process_instances i ON i.id = t.instance_id
        WHERE t.instance_id = $1 AND t.state = 'ACTIVE'
        ORDER BY t.created_at
        LIMIT 1
        "#,
    )
    .bind(instance_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| (row.get("element_id"), row.get("definition_id"))))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_accepts_camel_and_snake_case() {
        let camel: AuthRequest = serde_json::from_str(
            r#"{"instanceId":"inst-client-1","elementId":"End_Client","actor":"CLIENT","bpmnXml":"<xml/>"}"#,
        )
        .expect("camelCase");
        assert_eq!(camel.instance_id, "inst-client-1");
        assert_eq!(camel.element_id, "End_Client");
        assert_eq!(camel.actor, "CLIENT");
        assert_eq!(camel.bpmn_xml.as_deref(), Some("<xml/>"));

        let snake: AuthRequest = serde_json::from_str(
            r#"{"instance_id":"inst-cust-1","element_id":"Task_Approve","actor":"SALES"}"#,
        )
        .expect("snake_case");
        assert_eq!(snake.instance_id, "inst-cust-1");
        assert_eq!(snake.element_id, "Task_Approve");
        assert!(snake.bpmn_xml.is_none());

        // Пустой запрос не падает: шлюз может прислать только роль.
        let empty: AuthRequest = serde_json::from_str(r#"{"actor":"SALES"}"#).expect("только роль");
        assert_eq!(empty.instance_id, "");
    }

    #[test]
    fn event_without_role_is_not_checked() {
        // Воркеры, `nats pub` и E2E-скрипты роль не передают — до M6 такие
        // события двигали шаг, и проверка не должна их отклонять.
        assert!(!needs_role_check(""));
        assert!(!needs_role_check("   "));
        assert!(needs_role_check("SALES"));
        assert!(needs_role_check("  SALES  "));
    }

    #[test]
    fn reply_serializes_for_the_gateway() {
        let allowed = serde_json::to_value(AuthReply::allow("Task_Approve")).expect("json");
        assert_eq!(
            allowed,
            serde_json::json!({
                "allowed": true,
                "reason": "",
                "required": [],
                "elementId": "Task_Approve"
            })
        );

        let denied = AuthReply::from_decision(
            "Task_Approve",
            roles::decide(
                "WAREHOUSE",
                "Task_Approve",
                &roles::Roles::Only(vec!["SALES".to_string()]),
            ),
        );
        let denied = serde_json::to_value(denied).expect("json");
        assert_eq!(denied["allowed"], serde_json::json!(false));
        assert_eq!(denied["required"], serde_json::json!(["SALES"]));
        assert_eq!(
            denied["reason"],
            serde_json::json!(
                "Роль WAREHOUSE не может завершить шаг Task_Approve: нужна роль SALES."
            )
        );
    }
}
