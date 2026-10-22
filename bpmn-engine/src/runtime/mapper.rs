//! Перевод сообщений NATS в события вендорного движка.
//!
//! Модуль не ходит в БД: хост-узел для завершения шага выбирается по снимку
//! в [`super::pump::Snapshot`], а здесь остаются только чистые преобразования
//! (дизайн `docs/runtime-m1-design.md`, 5.1).

use bpm_engine::bpm_engine_core::{payloads, EngineEvent};
use serde_json::Value;

use super::pump::Step;
use super::vars;
use crate::events::{ErpResponseEvent, InboundEvent};

/// XML схемы из события: деплой и первый старт приходят с `bpmn_xml`.
///
/// Шлюз кладёт `output_data` в `payload` целиком
/// (`bpmn-gateways/http-cluster/index.js:128`), поэтому схема лежит на
/// верхнем уровне.
pub fn deploy_xml(event: &InboundEvent) -> Option<&str> {
    event
        .payload
        .get("bpmn_xml")
        .and_then(Value::as_str)
        .filter(|xml| !xml.trim().is_empty())
}

/// `element_id` служебного события деплоя (`ENGINE_ELEMENTS.PROCESS_DEPLOYMENT`).
pub const DEPLOY_ELEMENT_ID: &str = "Process_Deployment";

/// Префикс служебного экземпляра деплоя (`ENGINE_ELEMENTS.DEPLOY`).
pub const DEPLOY_INSTANCE_PREFIX: &str = "inst-deploy-";

/// Служебное событие деплоя: определение сохраняется, инстанс не создаётся
/// (дизайн 5.1).
pub fn is_deploy_only(event: &InboundEvent) -> bool {
    event.element_id.trim() == DEPLOY_ELEMENT_ID
        || event.instance_id.starts_with(DEPLOY_INSTANCE_PREFIX)
}

/// Необязательный `token_id` из `output_data` — путь для `parallelGateway`,
/// где одновременно ждут два `userTask` (дизайн 5.2, п. 2).
pub fn token_id_hint(output_data: &Value) -> Option<&str> {
    output_data
        .get("token_id")
        .and_then(Value::as_str)
        .filter(|token_id| !token_id.is_empty())
}

/// Старт процесса: `process_id` берётся из скомпилированной схемы,
/// начальные переменные — из `output_data` (дизайн 10.2, п. 1).
pub fn process_started(process_id: &str, instance_id: &str, output_data: &Value) -> EngineEvent {
    EngineEvent::ProcessStarted(payloads::ProcessStarted {
        process_id: process_id.to_string(),
        instance_id: instance_id.to_string(),
        initial_variables: Some(vars::flatten(output_data)),
    })
}

/// Завершение человеческого шага.
///
/// `task_id` — `id` `Waiting`-токена, `node_id` — **хост-узел** из токена,
/// а не `element_id` из сообщения: `element_id` адресует кнопку, а не шаг
/// (дизайн 5.2).
pub fn user_task_completed(instance_id: &str, step: &Step, output_data: &Value) -> EngineEvent {
    EngineEvent::UserTaskCompleted(payloads::UserTaskCompleted {
        task_id: step.token_id.clone(),
        instance_id: instance_id.to_string(),
        node_id: step.element_id.clone(),
        variables: vars::flatten(output_data),
    })
}

/// Завершение внешней задачи (`serviceTask`) по ответу ERP.
pub fn external_task_completed(instance_id: &str, step: &Step, payload: &Value) -> EngineEvent {
    EngineEvent::ExternalTaskCompleted(payloads::ExternalTaskCompleted {
        instance_id: instance_id.to_string(),
        token_id: step.token_id.clone(),
        node_id: step.element_id.clone(),
        variables: vars::flatten(payload),
    })
}

/// Успешный ли ответ ERP (остальные статусы в M1 не двигают токен).
pub fn erp_succeeded(event: &ErpResponseEvent) -> bool {
    event.status.eq_ignore_ascii_case("SUCCESS")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn inbound(payload: Value) -> InboundEvent {
        InboundEvent {
            event_id: "evt-1".to_string(),
            instance_id: "instance-1".to_string(),
            element_id: "Task_Next".to_string(),
            payload,
        }
    }

    #[test]
    fn deploy_xml_is_read_from_payload() {
        let event = inbound(json!({"bpmn_xml": "<definitions/>", "order_amount": 10}));
        assert_eq!(deploy_xml(&event), Some("<definitions/>"));

        assert_eq!(deploy_xml(&inbound(json!({"bpmn_xml": "   "}))), None);
        assert_eq!(deploy_xml(&inbound(json!({"order_amount": 10}))), None);
    }

    #[test]
    fn token_hint_is_optional() {
        assert_eq!(
            token_id_hint(&json!({"token_id": "token-1"})),
            Some("token-1")
        );
        assert_eq!(token_id_hint(&json!({"token_id": ""})), None);
        assert_eq!(token_id_hint(&json!({})), None);
    }

    #[test]
    fn start_sends_initial_variables() {
        let event = process_started(
            "Process_Dairy",
            "instance-1",
            &json!({"bpmn_xml": "<definitions/>", "order_amount": 150000}),
        );
        let EngineEvent::ProcessStarted(payload) = event else {
            panic!("ожидалось ProcessStarted");
        };
        assert_eq!(payload.process_id, "Process_Dairy");
        assert_eq!(payload.instance_id, "instance-1");
        let variables = payload.initial_variables.expect("переменные старта");
        assert_eq!(variables.get("order_amount"), Some(&"150000".to_string()));
        assert!(!variables.contains_key("bpmn_xml"), "схема — не переменная");
    }

    #[test]
    fn user_task_completion_uses_token_as_host() {
        let step = Step {
            token_id: "token-1".to_string(),
            element_id: "Task_CheckStock".to_string(),
        };
        let event = user_task_completed(
            "instance-1",
            &step,
            &json!({"stockSufficient": "true", "token_id": "token-1"}),
        );
        let EngineEvent::UserTaskCompleted(payload) = event else {
            panic!("ожидалось UserTaskCompleted");
        };
        assert_eq!(payload.task_id, "token-1");
        assert_eq!(payload.node_id, "Task_CheckStock");
        assert_eq!(
            payload.variables.get("stockSufficient"),
            Some(&"true".to_string())
        );
        assert!(!payload.variables.contains_key("token_id"));
    }

    #[test]
    fn erp_status_is_case_insensitive() {
        let response = |status: &str| ErpResponseEvent {
            process_instance_id: "instance-1".to_string(),
            element_id: "ServiceTask_ValidateOrder".to_string(),
            status: status.to_string(),
            error_message: None,
            retryable: false,
            payload: json!({}),
            idempotency_key: None,
        };

        assert!(erp_succeeded(&response("SUCCESS")));
        assert!(erp_succeeded(&response("success")));
        assert!(!erp_succeeded(&response("FAILED")));
    }

    #[test]
    fn deploy_events_do_not_start_instances() {
        let deploy = InboundEvent {
            event_id: "evt-deploy".to_string(),
            instance_id: "inst-deploy-DAIRY".to_string(),
            element_id: "Process_Deployment".to_string(),
            payload: json!({"bpmn_xml": "<definitions/>", "org_type": "DAIRY"}),
        };
        assert!(
            is_deploy_only(&deploy),
            "служебный деплой не создаёт инстанс"
        );

        let start = InboundEvent {
            event_id: "evt-start".to_string(),
            instance_id: "inst-cust-1".to_string(),
            element_id: "Task_CheckStock".to_string(),
            payload: json!({"bpmn_xml": "<definitions/>"}),
        };
        assert!(!is_deploy_only(&start), "старт процесса — не деплой");
    }
}
