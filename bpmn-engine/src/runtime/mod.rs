//! Рантайм маршрутизации по графу схемы (M1).
//!
//! Модуль — единственное место, где встречаются NATS-контракт сервиса и типы
//! вендорного `bpm-engine`: `main.rs` знает только [`GraphRuntime`],
//! [`Outcome`] и [`StepEvent`] (дизайн `docs/runtime-m1-design.md`, 9.1).
//!
//! Режим один — маршрут считает движок по графу схемы (дизайн 10.1).
//! Legacy-ветка, где следующий узел называл клиент, удалена шагом 3 (M2).

pub mod mapper;
pub mod project;
pub mod pump;
pub mod store;
pub mod vars;

use bpm_engine::bpm_engine_bpmn::parse_and_compile;
use bpm_engine::bpm_engine_core::{EngineEvent, InstanceState};
use bpm_engine::bpm_engine_runtime::BpmEngine;
use serde_json::Value;
use sqlx::PgPool;

use self::project::Projection;
use self::pump::{Snapshot, StepKind};
use self::store::GraphStores;
use crate::db;
use crate::events::{ErpResponseEvent, InboundEvent, ServiceCommand};

/// Подключает структурированные логи вендора (`tracing`).
///
/// Вызывается один раз при старте; `println!` остаются только для событий
/// самого сервиса (SLA, ответы NATS).
pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init();
}

/// Изменение одного шага для WebSocket (`mes.bpmn.v1.event.<instance>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepEvent {
    /// Токен движка (в контракте M1 необязателен, нужен для M1.1).
    pub token_id: String,
    /// Узел схемы — `element_id` в WS-сообщении.
    pub element_id: String,
    /// `ACTIVE` или `COMPLETED`.
    pub state: &'static str,
    /// `output_data` события — только для `ACTIVE`: `COMPLETED` не должен
    /// перезаписывать данные заказа на клиенте.
    pub payload: Option<Value>,
}

/// Что нужно опубликовать после успешной обработки события.
#[derive(Debug, Default)]
pub struct Outcome {
    /// Изменения шагов.
    pub steps: Vec<StepEvent>,
    /// Имя WS-события завершения процесса: `process_completed` /
    /// `process_terminated`.
    pub process_finished: Option<&'static str>,
    /// Команды воркерам по только что активированным `serviceTask` (M2).
    pub commands: Vec<ServiceCommand>,
}

/// Рантайм графа: сторы движка, пумпа и проекция в legacy read-model.
pub struct GraphRuntime {
    stores: GraphStores,
    engine: BpmEngine,
    legacy: PgPool,
}

impl GraphRuntime {
    /// Собирает сторы и создаёт вендорные таблицы.
    ///
    /// Единственный режим исполнения: вызывается при старте движка.
    pub async fn new(database_url: &str, legacy: PgPool) -> anyhow::Result<Self> {
        let stores = GraphStores::new(database_url)?;
        stores.ensure_schema().await?;
        Ok(Self {
            stores,
            engine: pump::engine(),
            legacy,
        })
    }

    /// Входящее событие клиента (`mes.in.v1.event.>`).
    ///
    /// Деплой (`bpmn_xml` в `output_data`) выполняется всегда, дальше событие
    /// либо стартует экземпляр, либо завершает человеческий шаг — оба пути
    /// идут под advisory-блокировкой экземпляра.
    ///
    /// `element_id` в событии — адрес кнопки, а не маршрут (дизайн 5.2):
    /// старт идёт от `startEvent` схемы и **не требует** `element_id`, а
    /// завершение шага выбирает хост-узел по `Waiting`-токену.
    pub async fn handle_inbound(&self, event: &InboundEvent) -> anyhow::Result<Outcome> {
        let mut deployed_process = None;
        if let Some(xml) = mapper::deploy_xml(event) {
            match parse_and_compile(xml) {
                Ok(definition) => {
                    let process_id = definition.id.to_string();
                    self.stores.deploy(&process_id, xml).await?;
                    // Legacy read-model: кабинет и E2E читают process_definitions.
                    db::save_process_definition(&self.legacy, &process_id, xml).await?;
                    deployed_process = Some(process_id);
                }
                Err(error) => {
                    eprintln!(
                        "❌ [Graph] compile_failed instance={} event_id={} error={:?}",
                        event.instance_id, event.event_id, error
                    );
                    // Повторная доставка не поможет: подтверждаем и выходим.
                    return Ok(Outcome::default());
                }
            }
        }

        if mapper::is_deploy_only(event) {
            println!(
                "📦 [Graph] deploy instance={} (инстанс и токены не создаются)",
                event.instance_id
            );
            return Ok(Outcome::default());
        }

        let lock = pump::lock_instance(&self.legacy, &event.instance_id).await?;
        let result = self
            .handle_inbound_locked(event, deployed_process.as_deref())
            .await;
        match &result {
            Ok(_) => lock.commit().await?,
            Err(_) => {
                let _ = lock.rollback().await;
            }
        }
        result
    }

    async fn handle_inbound_locked(
        &self,
        event: &InboundEvent,
        deployed_process: Option<&str>,
    ) -> anyhow::Result<Outcome> {
        let instance_id = event.instance_id.as_str();
        let event_id = event.event_id.trim();

        if !event_id.is_empty() && pump::is_consumed(&self.legacy, event_id).await? {
            println!("🔁 [Graph] duplicate_event event_id={}", event_id);
            return Ok(Outcome::default());
        }

        let before = pump::load(
            &self.stores.process_store,
            &self.stores.cached_def_store,
            instance_id,
        )
        .await?;

        let (engine_event, completes_step) = match before.as_ref() {
            None => {
                if db::instance_exists(&self.legacy, instance_id).await? {
                    println!(
                        "⚠️ [Graph] instance_not_migrated instance={} (начат в legacy, граф его не продолжает)",
                        instance_id
                    );
                    return Ok(Outcome::default());
                }
                let Some(process_id) = deployed_process else {
                    eprintln!(
                        "⚠️ [Graph] unknown_instance instance={} element={} (нет bpmn_xml для старта)",
                        instance_id, event.element_id
                    );
                    return Ok(Outcome::default());
                };
                (
                    mapper::process_started(process_id, instance_id, &event.payload),
                    false,
                )
            }
            Some(snapshot) => {
                if deployed_process.is_some() {
                    // Старт пришёл повторно (ретрай клиента): шаг завершать нельзя.
                    println!(
                        "ℹ️ [Graph] instance_already_started instance={}",
                        instance_id
                    );
                    return Ok(Outcome::default());
                }
                let token_id = mapper::token_id_hint(&event.payload);
                let Some(step) = snapshot.host_step(StepKind::UserTask, token_id, None) else {
                    eprintln!(
                        "⚠️ [Graph] no_waiting_token instance={} element={}",
                        instance_id, event.element_id
                    );
                    return Ok(Outcome::default());
                };
                (
                    mapper::user_task_completed(instance_id, &step, &event.payload),
                    true,
                )
            }
        };

        let outcome = self
            .pump_and_project(instance_id, before.as_ref(), engine_event, Some(&event.payload))
            .await?;

        if completes_step
            && !event.element_id.trim().is_empty()
            && !has_active_step(&outcome, &event.element_id)
        {
            println!(
                "ℹ️ [Graph] client_target_mismatch instance={} client={} graph={:?}",
                instance_id,
                event.element_id,
                active_elements(&outcome)
            );
        }

        if !event_id.is_empty() {
            pump::mark_consumed(&self.legacy, event_id, instance_id, "InboundEvent").await?;
        }
        Ok(outcome)
    }

    /// Ответ ERP (`mes.in.v1.erp.response`).
    ///
    /// Успешный ответ завершает внешнюю задачу `serviceTask`; остальные
    /// статусы только логируются (ретраи — следующий шаг M2).
    ///
    /// Идемпотентность: ответ с `idempotency_key` из команды обрабатывается
    /// один раз (`consumed_event`). Ответ без ключа, как и раньше, гасится
    /// отсутствием ожидающего токена (дизайн 9.5).
    pub async fn handle_erp(&self, event: &ErpResponseEvent) -> anyhow::Result<Outcome> {
        if !mapper::erp_succeeded(event) {
            eprintln!(
                "⚠️ [Graph] erp_not_success instance={} element={} status={}",
                event.process_instance_id, event.element_id, event.status
            );
            return Ok(Outcome::default());
        }

        let idempotency_key = event
            .idempotency_key
            .as_deref()
            .map(str::trim)
            .filter(|key| !key.is_empty());

        if let Some(key) = idempotency_key {
            if pump::is_consumed(&self.legacy, key).await? {
                println!("🔁 [Graph] duplicate_event event_id={} source=erp", key);
                return Ok(Outcome::default());
            }
        }

        let lock = pump::lock_instance(&self.legacy, &event.process_instance_id).await?;
        let result = self.handle_erp_locked(event).await;
        match &result {
            Ok(outcome) => {
                lock.commit().await?;
                // Ключ пишется после коммита: если процесс упадёт между
                // коммитом и записью, повтор всё равно не найдёт ожидающий
                // токен и не сдвинет граф (9.5).
                if let Some(key) = idempotency_key {
                    if !outcome.steps.is_empty() {
                        pump::mark_consumed(
                            &self.legacy,
                            key,
                            &event.process_instance_id,
                            "ErpResponse",
                        )
                        .await?;
                    }
                }
            }
            Err(_) => {
                let _ = lock.rollback().await;
            }
        }
        result
    }

    async fn handle_erp_locked(&self, event: &ErpResponseEvent) -> anyhow::Result<Outcome> {
        let instance_id = event.process_instance_id.as_str();
        let before = pump::load(
            &self.stores.process_store,
            &self.stores.cached_def_store,
            instance_id,
        )
        .await?;

        let Some(snapshot) = before.as_ref() else {
            eprintln!("⚠️ [Graph] unknown_instance instance={} source=erp", instance_id);
            return Ok(Outcome::default());
        };

        let token_id = mapper::token_id_hint(&event.payload);
        let node_id = Some(event.element_id.as_str()).filter(|id| !id.is_empty());
        let Some(step) = pump::erp_host_step(snapshot, node_id, token_id) else {
            eprintln!(
                "⚠️ [Graph] erp_target_mismatch instance={} element={} source=erp",
                instance_id, event.element_id
            );
            return Ok(Outcome::default());
        };

        let engine_event = mapper::external_task_completed(instance_id, &step, &event.payload);
        self.pump_and_project(instance_id, Some(snapshot), engine_event, Some(&event.payload))
            .await
    }

    /// Один проход пумпы: снимок «после», дифф, legacy-запись, WS-события.
    async fn pump_and_project(
        &self,
        instance_id: &str,
        before: Option<&Snapshot>,
        engine_event: EngineEvent,
        payload: Option<&Value>,
    ) -> anyhow::Result<Outcome> {
        let mut context = self.stores.context();
        self.engine.run_async(engine_event, &mut context).await;

        let after = pump::load(
            &self.stores.process_store,
            &self.stores.cached_def_store,
            instance_id,
        )
        .await?;
        let Some(after) = after else {
            eprintln!("⚠️ [Graph] instance_lost instance={}", instance_id);
            return Ok(Outcome::default());
        };

        let projection = project::diff(before, &after);
        project::apply_legacy(
            &self.legacy,
            instance_id,
            &after.instance.process_def_id,
            &projection,
        )
        .await?;
        let commands = service_commands(instance_id, &after, &projection);
        Ok(to_outcome(&projection, payload, commands))
    }
}

/// Команды воркерам по активированным `serviceTask` (контракт M2).
///
/// `payload` — переменные экземпляра: воркер видит те же данные, что ушли
/// клиенту с `ACTIVE`, но без транспортных полей события — их отсекает
/// [`super::vars::flatten`] ещё на входе.
///
/// `idempotency_key` детерминирован: `<instance>:<element>:<token>`. Движок
/// выдаёт новый токен на каждый вход в узел (`transition.rs`), поэтому петля
/// процесса даёт новый ключ, а повторная доставка того же ответа — тот же.
fn service_commands(
    instance_id: &str,
    after: &Snapshot,
    projection: &Projection,
) -> Vec<ServiceCommand> {
    let payload = serde_json::json!(&after.instance.variables);
    projection
        .activated
        .iter()
        .filter_map(|step| {
            let spec = pump::external_task_spec(&after.definition, &step.element_id)?;
            let deadline = chrono::Utc::now() + chrono::Duration::seconds(spec.timeout_secs as i64);
            Some(ServiceCommand {
                instance_id: instance_id.to_string(),
                element_id: step.element_id.clone(),
                target_system: spec.task_type,
                payload: payload.clone(),
                idempotency_key: format!("{}:{}:{}", instance_id, step.element_id, step.token_id),
                deadline: deadline.to_rfc3339(),
            })
        })
        .collect()
}

/// Проекция → сообщения для WebSocket (и legacy пишет [`project::apply_legacy`]).
fn to_outcome(
    projection: &Projection,
    payload: Option<&Value>,
    commands: Vec<ServiceCommand>,
) -> Outcome {
    let mut steps = Vec::with_capacity(projection.activated.len() + projection.closed.len());

    for step in &projection.closed {
        steps.push(StepEvent {
            token_id: step.token_id.clone(),
            element_id: step.element_id.clone(),
            state: "COMPLETED",
            payload: None,
        });
    }
    for step in &projection.activated {
        steps.push(StepEvent {
            token_id: step.token_id.clone(),
            element_id: step.element_id.clone(),
            state: "ACTIVE",
            payload: payload.cloned(),
        });
    }

    Outcome {
        steps,
        process_finished: projection.finished.and_then(ws_process_event),
        commands,
    }
}

fn ws_process_event(state: InstanceState) -> Option<&'static str> {
    match state {
        InstanceState::Completed => Some("process_completed"),
        InstanceState::Terminated => Some("process_terminated"),
        InstanceState::Running => None,
    }
}

fn has_active_step(outcome: &Outcome, element_id: &str) -> bool {
    outcome
        .steps
        .iter()
        .any(|step| step.state == "ACTIVE" && step.element_id == element_id)
}

fn active_elements(outcome: &Outcome) -> Vec<&str> {
    outcome
        .steps
        .iter()
        .filter(|step| step.state == "ACTIVE")
        .map(|step| step.element_id.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::project::Projection;
    use super::pump::Step;
    use super::*;
    use serde_json::json;

    #[test]
    fn outcome_closes_old_step_and_activates_new() {
        let projection = Projection {
            activated: vec![Step {
                token_id: "token-2".to_string(),
                element_id: "Task_CheckStock".to_string(),
            }],
            closed: vec![Step {
                token_id: "token-1".to_string(),
                element_id: "Task_Start".to_string(),
            }],
            finished: None,
        };

        let outcome = to_outcome(&projection, Some(&json!({"order_amount": 10})), Vec::new());

        assert_eq!(outcome.steps.len(), 2);
        assert_eq!(outcome.steps[0].state, "COMPLETED");
        assert_eq!(outcome.steps[0].payload, None);
        assert_eq!(outcome.steps[1].state, "ACTIVE");
        assert_eq!(
            outcome.steps[1].payload,
            Some(json!({"order_amount": 10})),
            "payload уходит только с ACTIVE"
        );
        assert_eq!(outcome.process_finished, None);
        assert!(has_active_step(&outcome, "Task_CheckStock"));
        assert!(!has_active_step(&outcome, "Task_Start"));
    }

    #[test]
    fn completed_instance_reports_process_completed() {
        let projection = Projection {
            activated: vec![],
            closed: vec![Step {
                token_id: "token-1".to_string(),
                element_id: "Task_Ship".to_string(),
            }],
            finished: Some(InstanceState::Completed),
        };

        let outcome = to_outcome(&projection, None, Vec::new());

        assert_eq!(outcome.process_finished, Some("process_completed"));
        assert_eq!(active_elements(&outcome), Vec::<&str>::new());
    }

    #[test]
    fn terminated_instance_reports_process_terminated() {
        let projection = Projection {
            activated: vec![],
            closed: vec![],
            finished: Some(InstanceState::Terminated),
        };

        let outcome = to_outcome(&projection, None, Vec::new());

        assert_eq!(outcome.process_finished, Some("process_terminated"));
    }

    /// Схема с `serviceTask`: тема воркера — `camunda:topic`.
    const SERVICE_TASK_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"
             xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
             targetNamespace="http://bpmn-engine.local">
  <process id="runtime_service_cmd" name="Service" isExecutable="true">
    <startEvent id="Start"/>
    <serviceTask id="ServiceTask_ERP" name="ERP" camunda:topic="erp"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="ServiceTask_ERP"/>
    <sequenceFlow id="Flow_End" sourceRef="ServiceTask_ERP" targetRef="End"/>
  </process>
</definitions>"#;

    fn service_snapshot() -> Snapshot {
        let definition = parse_and_compile(SERVICE_TASK_XML).expect("схема компилируется");
        let instance = bpm_engine::bpm_engine_core::ProcessInstance {
            id: "instance-1".to_string(),
            process_def_id: "runtime_service_cmd".to_string(),
            tenant_id: None,
            tokens: vec![],
            variables: std::collections::HashMap::from([
                ("orderNum".to_string(), "№101".to_string()),
                ("stockSufficient".to_string(), "false".to_string()),
            ]),
            state: InstanceState::Running,
            version: 0,
            parent_instance_id: None,
            parent_token_id: None,
        };
        Snapshot {
            instance,
            definition,
        }
    }

    #[test]
    fn activated_service_task_yields_worker_command() {
        let snapshot = service_snapshot();
        let projection = Projection {
            activated: vec![Step {
                token_id: "token-1".to_string(),
                element_id: "ServiceTask_ERP".to_string(),
            }],
            closed: vec![],
            finished: None,
        };

        let commands = service_commands("instance-1", &snapshot, &projection);

        assert_eq!(commands.len(), 1);
        let command = &commands[0];
        assert_eq!(command.instance_id, "instance-1");
        assert_eq!(command.element_id, "ServiceTask_ERP");
        assert_eq!(command.target_system, "erp", "subject — mes.out.v1.cmd.erp");
        assert_eq!(
            command.payload,
            json!({"orderNum": "№101", "stockSufficient": "false"}),
            "воркер получает переменные экземпляра"
        );
        assert_eq!(
            command.idempotency_key, "instance-1:ServiceTask_ERP:token-1",
            "ключ стабилен для одной активации"
        );
        assert!(
            chrono::DateTime::parse_from_rfc3339(&command.deadline).is_ok(),
            "deadline — RFC 3339, иначе воркер его не разберёт"
        );
    }

    #[test]
    fn user_task_activation_has_no_worker_command() {
        let mut snapshot = service_snapshot();
        // Узел userTask берём из той же схемы: Start — проходной узел,
        // команда не должна появиться ни для него, ни для неизвестного id.
        snapshot.definition = parse_and_compile(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://bpmn-engine.local">
  <process id="runtime_user_cmd" name="User" isExecutable="true">
    <startEvent id="Start"/>
    <userTask id="Task_A" name="A"/>
    <endEvent id="End"/>
    <sequenceFlow id="Flow_Start" sourceRef="Start" targetRef="Task_A"/>
    <sequenceFlow id="Flow_End" sourceRef="Task_A" targetRef="End"/>
  </process>
</definitions>"#,
        )
        .expect("схема компилируется");
        let projection = Projection {
            activated: vec![
                Step {
                    token_id: "token-1".to_string(),
                    element_id: "Task_A".to_string(),
                },
                Step {
                    token_id: "token-2".to_string(),
                    element_id: "Unknown_Node".to_string(),
                },
            ],
            closed: vec![],
            finished: None,
        };

        assert!(
            service_commands("instance-1", &snapshot, &projection).is_empty(),
            "команду получает только serviceTask"
        );
    }
}
