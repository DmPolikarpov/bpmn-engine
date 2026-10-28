use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundEvent {
    #[serde(default)]
    pub event_id: String,
    pub instance_id: String,
    #[serde(default)]
    pub element_id: String,
    #[serde(default)]
    pub payload: Value,
    /// Роль сессии, от имени которой пришло событие (M6).
    ///
    /// Необязательна: `nats pub` воркеров, E2E-скрипты и старые клиенты шлют
    /// событие без неё, и движок тогда не проверяет роль (см.
    /// `runtime/roles.rs`). Кабинет заполняет её всегда — заголовком
    /// `X-Actor-Role` через HTTP-шлюз.
    #[serde(default)]
    pub actor: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionRequiredEvent {
    pub process_instance_id: String,
    pub current_step: String,
    pub target_system: String,
    pub payload: Value,
}

/// Команда воркеру `serviceTask` (контракт M2).
///
/// Публикуется в `mes.out.v1.cmd.<target_system>` сразу после активации узла,
/// ответ приходит в `mes.in.v1.erp.response` ([`ErpResponseEvent`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceCommand {
    /// Экземпляр процесса.
    pub instance_id: String,
    /// Узел `serviceTask` — тот же `element_id`, что и в ответе.
    pub element_id: String,
    /// Тема воркера: `bpmn:serviceTask/@camunda:topic` (`erp`, `mes`, …).
    pub target_system: String,
    /// Переменные экземпляра на момент активации шага.
    pub payload: Value,
    /// Ключ идемпотентности: `<instance>:<element>:<token>`, стабилен для
    /// одной активации (токен движка создаётся заново на каждый вход в узел).
    pub idempotency_key: String,
    /// Крайний срок ответа (RFC 3339): `now + timeout_secs` узла.
    pub deadline: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ErpResponseEvent {
    pub process_instance_id: String,
    #[serde(default)]
    pub element_id: String,
    pub status: String,
    pub error_message: Option<String>,
    /// Можно ли переотправить команду: `true` — движок сделает ретрай с
    /// backoff (пока не кончились `camunda:retries` узла), `false` — сразу
    /// компенсация (`ActionRequiredEvent`). Необязателен в JSON.
    #[serde(default)]
    pub retryable: bool,
    pub payload: serde_json::Value,
    /// Ключ идемпотентности из [`ServiceCommand`]; повтор с тем же ключом
    /// не двигает токен дважды. Необязателен: ручной `nats pub` и E2E-тесты
    /// шлют ответ без него.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}
