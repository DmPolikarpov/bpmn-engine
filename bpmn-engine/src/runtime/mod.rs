//! Рантайм маршрутизации по графу схемы (M1).
//!
//! Модуль — единственное место, где встречаются NATS-контракт сервиса и типы
//! вендорного `bpm-engine`: `main.rs` знает только [`Routing`],
//! [`GraphRuntime`], [`Outcome`] и [`StepEvent`] (дизайн
//! `docs/runtime-m1-design.md`, 9.1).
//!
//! Переключатель режима — переменная окружения `ENGINE_ROUTING`:
//! `legacy` (по умолчанию) оставляет маршрут за клиентом, `graph` включает
//! исполнение по графу (дизайн 10.1).

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
use crate::events::{ErpResponseEvent, InboundEvent};

/// Режим маршрутизации (`ENGINE_ROUTING`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Routing {
    /// Маршрут считает клиент: токен ставится по `element_id` (как до M1).
    Legacy,
    /// Маршрут считает движок по графу схемы.
    Graph,
}

impl Routing {
    /// Читает режим из окружения; неизвестное значение — `legacy`.
    pub fn from_env() -> Self {
        Self::parse(&std::env::var("ENGINE_ROUTING").unwrap_or_default())
    }

    /// Разбирает значение переменной (регистр не важен).
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "graph" => Routing::Graph,
            _ => Routing::Legacy,
        }
    }

    /// Строковое имя режима для логов.
    pub fn as_str(self) -> &'static str {
        match self {
            Routing::Legacy => "legacy",
            Routing::Graph => "graph",
        }
    }
}

/// Подключает структурированные логи вендора (`tracing`).
///
/// Вызывается только в режиме `graph`: в `legacy` остаются текущие
/// `println!`, чтобы шаг 0 не менял наблюдаемость.
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
    /// `output_data` события — только для `ACTIVE`, как и в legacy-пути.
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
    /// Вызывается только при `ENGINE_ROUTING=graph`.
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

        if event.element_id.trim().is_empty() {
            eprintln!(
                "⚠️ [Graph] event_without_element_id instance={} event_id={}",
                event.instance_id, event.event_id
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
                    eprintln!("⚠️ [Graph] unknown_instance instance={}", instance_id);
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

        if completes_step && !has_active_step(&outcome, &event.element_id) {
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
    /// Успешный ответ завершает внешнюю задачу `serviceTask` уже в M1;
    /// остальные статусы только логируются (M2 добавит ретраи).
    pub async fn handle_erp(&self, event: &ErpResponseEvent) -> anyhow::Result<Outcome> {
        if !mapper::erp_succeeded(event) {
            eprintln!(
                "⚠️ [Graph] erp_not_success instance={} element={} status={}",
                event.process_instance_id, event.element_id, event.status
            );
            return Ok(Outcome::default());
        }

        let lock = pump::lock_instance(&self.legacy, &event.process_instance_id).await?;
        let result = self.handle_erp_locked(event).await;
        match &result {
            Ok(_) => lock.commit().await?,
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
        let Some(step) = snapshot.host_step(StepKind::ExternalTask, token_id, node_id) else {
            eprintln!(
                "⚠️ [Graph] no_waiting_token instance={} element={} source=erp",
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
        Ok(to_outcome(&projection, payload))
    }
}

/// Проекция → сообщения для WebSocket (и legacy пишет [`project::apply_legacy`]).
fn to_outcome(projection: &Projection, payload: Option<&Value>) -> Outcome {
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
    fn routing_defaults_to_legacy() {
        assert_eq!(Routing::parse(""), Routing::Legacy);
        assert_eq!(Routing::parse("legacy"), Routing::Legacy);
        assert_eq!(Routing::parse("LEGACY"), Routing::Legacy);
        assert_eq!(Routing::parse("unknown"), Routing::Legacy);
        assert_eq!(Routing::parse("graph"), Routing::Graph);
        assert_eq!(Routing::parse(" Graph "), Routing::Graph);
        assert_eq!(Routing::Graph.as_str(), "graph");
        assert_eq!(Routing::Legacy.as_str(), "legacy");
    }

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

        let outcome = to_outcome(&projection, Some(&json!({"order_amount": 10})));

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

        let outcome = to_outcome(&projection, None);

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

        let outcome = to_outcome(&projection, None);

        assert_eq!(outcome.process_finished, Some("process_terminated"));
    }
}
