//! Рантайм маршрутизации по графу схемы (M1).
//!
//! Модуль — единственное место, где встречаются NATS-контракт сервиса и типы
//! вендорного `bpm-engine`: `main.rs` знает только [`GraphRuntime`],
//! [`Outcome`] и [`StepEvent`] (дизайн `docs/runtime-m1-design.md`, 9.1).
//!
//! Режим один — маршрут считает движок по графу схемы (дизайн 10.1).
//! Legacy-ветка, где следующий узел называл клиент, удалена шагом 3 (M2).

pub mod mapper;
pub mod messages;
pub mod project;
pub mod pump;
pub mod roles;
pub mod store;
pub mod vars;

use bpm_engine::bpm_engine_bpmn::parse_and_compile;
use bpm_engine::bpm_engine_core::{payloads, EngineEvent, InstanceState, TokenStatus};
use bpm_engine::bpm_engine_runtime::BpmEngine;
use bpm_engine::bpm_engine_storage::{ProcessDefinitionStore, ProcessInstanceStore};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;

use self::project::Projection;
use self::pump::{ExternalTaskSpec, MessageCapture, Snapshot, Step, StepKind};
use self::store::GraphStores;
use crate::auth;
use crate::db;
use crate::events::{ActionRequiredEvent, ErpResponseEvent, InboundEvent, ServiceCommand};

/// Подключает структурированные логи вендора (`tracing`).
///
/// Вызывается один раз при старте; `println!` остаются только для событий
/// самого сервиса (SLA, ответы NATS).
pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
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
    /// Компенсации, которые нужно опубликовать (`ActionRequiredEvent`).
    ///
    /// Появляются, когда ответ воркера не повторяемый или ретраи исчерпаны:
    /// токен остаётся на шаге, а внешняя система получает запрос человека.
    pub actions: Vec<ActionRequiredEvent>,
    /// Другие экземпляры, которые двинулись из-за сообщений этого события (M3).
    ///
    /// Экземпляр второй стороны мог быть создан триггером узла
    /// (`trg:processTrigger`) или разбужен сообщением на catch-событии.
    /// `main.rs` публикует шаги каждого из них в свой subject
    /// `mes.bpmn.v1.event.<instance>`.
    pub peers: Vec<PeerOutcome>,
}

/// Изменения в «соседнем» экземпляре, вызванные сообщением (M3).
#[derive(Debug, Clone, PartialEq)]
pub struct PeerOutcome {
    /// Экземпляр второй стороны.
    pub instance_id: String,
    /// Изменения его шагов.
    pub steps: Vec<StepEvent>,
    /// `process_completed` / `process_terminated`, если он завершился.
    pub process_finished: Option<&'static str>,
    /// Команды воркерам по его активированным `serviceTask`.
    pub commands: Vec<ServiceCommand>,
    /// Компенсации по его шагам.
    pub actions: Vec<ActionRequiredEvent>,
}

impl PeerOutcome {
    fn from_outcome(instance_id: &str, outcome: Outcome) -> Self {
        Self {
            instance_id: instance_id.to_string(),
            steps: outcome.steps,
            process_finished: outcome.process_finished,
            commands: outcome.commands,
            actions: outcome.actions,
        }
    }
}

/// Политика ретраев внешних задач (M2).
///
/// Значения читаются из окружения один раз при старте: `ERP_RETRY_BASE_SECS`
/// (первая задержка, по умолчанию 5 с), `ERP_RETRY_MAX_SECS` (потолок
/// экспоненциального backoff, 300 с) и `ERP_RETRY_LEASE_SECS` (аренда
/// выбранного ретрая, 30 с — чтобы поды не отправили команду дважды).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Первая задержка backoff: дальше она удваивается на каждой попытке.
    pub base_secs: u64,
    /// Потолок задержки.
    pub max_secs: u64,
    /// Аренда строки `erp_retry` на под, который её отправил.
    pub lease_secs: u64,
}

impl RetryPolicy {
    /// Политика из окружения (значения по умолчанию — для демо и тестов).
    pub fn from_env() -> Self {
        Self {
            base_secs: env_secs("ERP_RETRY_BASE_SECS", 5),
            max_secs: env_secs("ERP_RETRY_MAX_SECS", 300),
            lease_secs: env_secs("ERP_RETRY_LEASE_SECS", 30),
        }
    }
}

/// Секунды из окружения; пустое или битое значение — `default`.
fn env_secs(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Рантайм графа: сторы движка, пумпа и проекция в legacy read-model.
pub struct GraphRuntime {
    stores: Arc<GraphStores>,
    engine: BpmEngine,
    /// Движок доставки сообщений (только `MessageCatchHandler`, M3).
    message_engine: BpmEngine,
    /// Сообщения, отправленные движком за текущий проход.
    messages: MessageCapture,
    legacy: PgPool,
    retry: RetryPolicy,
}

impl GraphRuntime {
    /// Собирает сторы и создаёт вендорные таблицы.
    ///
    /// Единственный режим исполнения: вызывается при старте движка.
    pub async fn new(database_url: &str, legacy: PgPool) -> anyhow::Result<Self> {
        let stores = Arc::new(GraphStores::new(database_url)?);
        stores.ensure_schema().await?;
        let messages = MessageCapture::default();
        Ok(Self {
            stores,
            engine: pump::engine(messages.clone()),
            message_engine: pump::message_engine(),
            messages,
            legacy,
            retry: RetryPolicy::from_env(),
        })
    }

    /// Сторы движка для API проверки прав (M6): шлюзу нужен `bpmn_xml`
    /// определения, а компилировать его ради ролей не требуется.
    pub fn stores(&self) -> Arc<GraphStores> {
        self.stores.clone()
    }

    /// Роли узла из задеплоенной схемы (`None` — схему прочитать не удалось).
    ///
    /// Схема читается из определения (`process_definition.bpmn_xml`), а не из
    /// скомпилированного графа: вендорный компилятор роли не разбирает
    /// (`bpmn:potentialOwner` ему не нужен). Один запрос на событие — та же
    /// цена, что у `dispatch_messages` для сообщений.
    async fn node_roles(&self, definition_id: &str, node_id: &str) -> Option<roles::Roles> {
        let xml = match self.stores.load_xml(definition_id).await {
            Ok(Some(xml)) => xml,
            Ok(None) => {
                eprintln!("⚠️ [Graph] definition_xml_missing definition={definition_id}");
                return None;
            }
            Err(error) => {
                eprintln!(
                    "⚠️ [Graph] definition_xml_failed definition={definition_id} error={error:#}"
                );
                return None;
            }
        };

        match roles::node_roles(&xml, node_id) {
            Ok(required) => Some(required),
            Err(error) => {
                eprintln!(
                    "⚠️ [Graph] roles_parse_failed definition={definition_id} element={node_id} error={error}"
                );
                None
            }
        }
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

        // Шаг, который закрывает это событие: нужен аудиту исполнителя (M6).
        let mut completed: Option<Step> = None;

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

                // Роль кнопки старта (M6): у старта нет `potentialOwner`,
                // поэтому роль объявляет `act:userAction` самого `startEvent`.
                // Событие без роли (воркер, E2E-скрипт) не проверяется — так же,
                // как и завершение шага.
                if auth::needs_role_check(event.actor.trim()) {
                    if let Some(xml) = mapper::deploy_xml(event) {
                        match roles::start_roles(xml) {
                            Ok(required) => {
                                let decision =
                                    roles::decide_start(event.actor.trim(), process_id, &required);
                                if !decision.allowed {
                                    eprintln!(
                                        "⛔ [Graph] auth_denied instance={} actor={} reason={}",
                                        instance_id,
                                        event.actor.trim(),
                                        decision.reason
                                    );
                                    return Ok(Outcome::default());
                                }
                            }
                            Err(error) => eprintln!(
                                "⚠️ [Graph] roles_parse_failed instance={} error={}",
                                instance_id, error
                            ),
                        }
                    }
                }

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

                // Роль исполнителя шага (M6): событие с чужой ролью не двигает
                // граф. Событие без роли пропускается — так шлют воркеры и
                // E2E-скрипты, и это же поведение было до M6.
                let actor = event.actor.trim();
                let definition_id = snapshot.definition.id.to_string();
                let required = if auth::needs_role_check(actor) {
                    self.node_roles(&definition_id, &step.element_id).await
                } else {
                    None
                };

                if let Some(required) = required {
                    let decision = roles::decide(actor, &step.element_id, &required);
                    if !decision.allowed {
                        eprintln!(
                            "⛔ [Graph] auth_denied instance={} element={} actor={} reason={}",
                            instance_id, step.element_id, actor, decision.reason
                        );
                        record_actor(
                            &self.legacy,
                            instance_id,
                            "USER_TASK_DENIED",
                            &step.element_id,
                            event,
                            json!({
                                "reason": decision.reason,
                                "required": decision.required,
                                "eventId": event_id,
                            }),
                        )
                        .await;
                        if !event_id.is_empty() {
                            pump::mark_consumed(
                                &self.legacy,
                                event_id,
                                instance_id,
                                "InboundEvent",
                            )
                            .await?;
                        }
                        return Ok(Outcome::default());
                    }
                }

                completed = Some(step.clone());

                (
                    mapper::user_task_completed(instance_id, &step, &event.payload),
                    true,
                )
            }
        };

        let outcome = self
            .pump_and_project(
                instance_id,
                before.as_ref(),
                engine_event,
                Some(&event.payload),
            )
            .await?;

        // Аудит исполнителя (M6): пишется после проекции, потому что FK
        // `execution_history.instance_id` требует строки экземпляра.
        if let Some(step) = &completed {
            record_actor(
                &self.legacy,
                instance_id,
                "USER_TASK_COMPLETED",
                &step.element_id,
                event,
                json!({ "eventId": event_id }),
            )
            .await;
        }

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
    /// Успешный ответ завершает внешнюю задачу `serviceTask`. Неуспешный:
    ///
    /// - `retryable = true` и остались `camunda:retries` — команда будет
    ///   переотправлена с экспоненциальным backoff (`erp_retry`);
    /// - иначе — компенсация: `ActionRequiredEvent` в `mes.out.v1.cmd.<system>`,
    ///   токен остаётся на шаге (решение принимает человек).
    ///
    /// Идемпотентность: терминальный ответ (успех или компенсация) с
    /// `idempotency_key` из команды обрабатывается один раз (`consumed_event`).
    /// Повтор неуспешного ответа, пока ретрай ещё не отправлен, не сжигает
    /// вторую попытку (`erp_retry/dispatched_at`).
    pub async fn handle_erp(&self, event: &ErpResponseEvent) -> anyhow::Result<Outcome> {
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
                // токен и не сдвинет граф (9.5). Ретрай не терминален —
                // его ответ придёт с тем же ключом и должен быть обработан.
                if let Some(key) = idempotency_key {
                    if !outcome.steps.is_empty() || !outcome.actions.is_empty() {
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
            eprintln!(
                "⚠️ [Graph] unknown_instance instance={} source=erp",
                instance_id
            );
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

        let retry_key = erp_retry_key(event, &step);

        if !mapper::erp_succeeded(event) {
            return self
                .handle_erp_failure(event, snapshot, &step, &retry_key)
                .await;
        }

        // Шаг закрывается: ретраи этой команды больше не нужны.
        pump::clear_retry(&self.legacy, &retry_key).await?;

        let engine_event = mapper::external_task_completed(instance_id, &step, &event.payload);
        self.pump_and_project(
            instance_id,
            Some(snapshot),
            engine_event,
            Some(&event.payload),
        )
        .await
    }

    /// Неуспешный ответ воркера: переотправить команду или позвать человека.
    async fn handle_erp_failure(
        &self,
        event: &ErpResponseEvent,
        snapshot: &Snapshot,
        step: &Step,
        retry_key: &str,
    ) -> anyhow::Result<Outcome> {
        let state = pump::load_retry(&self.legacy, retry_key).await?;

        // Ретрай запланирован, но ещё не ушёл воркеру: это повтор того же
        // сообщения (или гонка с публикатором), новую попытку не тратим.
        if state.is_some_and(|state| state.pending) {
            println!(
                "🔁 [Graph] erp_retry_pending instance={} element={}",
                event.process_instance_id, step.element_id
            );
            return Ok(Outcome::default());
        }

        let attempts = state.map(|state| state.attempts).unwrap_or(0);
        let spec =
            pump::external_task_spec(&snapshot.definition, &step.element_id).unwrap_or_else(|| {
                ExternalTaskSpec {
                    task_type: "default".to_string(),
                    retries: 0,
                    timeout_secs: 0,
                }
            });

        match failure_plan(
            spec.retries,
            attempts,
            event.retryable,
            self.retry.base_secs,
            self.retry.max_secs,
        ) {
            FailurePlan::Retry {
                attempt,
                delay_secs,
            } => {
                pump::schedule_retry(
                    &self.legacy,
                    retry_key,
                    &event.process_instance_id,
                    &step.element_id,
                    &step.token_id,
                    attempt,
                    &event.status,
                    event.error_message.as_deref(),
                    delay_secs as i64,
                )
                .await?;
                println!(
                    "🔁 [Graph] erp_retry_scheduled instance={} element={} attempt={}/{} delay={}s status={}",
                    event.process_instance_id,
                    step.element_id,
                    attempt,
                    spec.retries,
                    delay_secs,
                    event.status
                );
                Ok(Outcome::default())
            }
            FailurePlan::Compensate { reason } => {
                pump::clear_retry(&self.legacy, retry_key).await?;
                eprintln!(
                    "⚠️ [Graph] erp_compensate instance={} element={} reason={} attempts={}",
                    event.process_instance_id, step.element_id, reason, attempts
                );
                Ok(Outcome {
                    actions: vec![ActionRequiredEvent {
                        process_instance_id: event.process_instance_id.clone(),
                        current_step: step.element_id.clone(),
                        target_system: spec.task_type.to_uppercase(),
                        payload: serde_json::json!({
                            "status": "FAILED",
                            "reason": reason,
                            "attempts": attempts,
                            "last_status": event.status,
                            "error": event.error_message,
                        }),
                    }],
                    ..Outcome::default()
                })
            }
        }
    }
    /// Команды, которые пора переотправить после backoff.
    ///
    /// Публикует их `main.rs`: рантайм не знает про NATS. Шаг, который уже не
    /// ждёт ответа (успели ответить, процесс ушёл дальше), просто забывается.
    pub async fn due_retries(&self) -> anyhow::Result<Vec<ServiceCommand>> {
        const BATCH: i64 = 32;

        let jobs =
            pump::claim_due_retries(&self.legacy, BATCH, self.retry.lease_secs as i64).await?;
        let mut commands = Vec::with_capacity(jobs.len());

        for job in jobs {
            let snapshot = pump::load(
                &self.stores.process_store,
                &self.stores.cached_def_store,
                &job.instance_id,
            )
            .await?;

            let step = snapshot.as_ref().and_then(|snapshot| {
                snapshot.waiting_steps().into_iter().find(|step| {
                    step.token_id == job.token_id
                        && step.element_id == job.element_id
                        && pump::step_kind(&snapshot.definition, &step.element_id)
                            == Some(StepKind::ExternalTask)
                })
            });

            let Some((snapshot, step)) = snapshot.as_ref().zip(step.as_ref()) else {
                println!(
                    "🧹 [Graph] erp_retry_dropped instance={} element={}",
                    job.instance_id, job.element_id
                );
                pump::clear_retry(&self.legacy, &job.idempotency_key).await?;
                continue;
            };

            if let Some(command) = worker_command(&job.idempotency_key, snapshot, step) {
                println!(
                    "🔁 [Graph] erp_retry_dispatch instance={} element={} attempt={}",
                    job.instance_id, job.element_id, job.attempts
                );
                commands.push(command);
            }
        }

        Ok(commands)
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
        self.messages.clear();
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
        let mut outcome = to_outcome(&projection, payload, commands);
        outcome.peers = self.dispatch_messages(&after).await?;
        Ok(outcome)
    }

    /// Разбирает сообщения, отправленные за проход, и двигает вторую сторону (M3).
    ///
    /// Для каждого `MessageSent`:
    ///
    /// - триггер узла (`trg:processTrigger`) запускает экземпляр целевого
    ///   процесса — с детерминированным id `<prefix><orderNum>` и только
    ///   перечисленными в `payloadKeys` переменными;
    /// - сообщение доставляется ожидающему catch-событию другого экземпляра,
    ///   найденного по бизнес-ключу (`orderNum`), — не всем сразу.
    ///
    /// Схема читается из `process_definition`: она уже задеплоена тем же
    /// событием или раньше, поэтому разбор не требует отдельного хранилища.
    async fn dispatch_messages(&self, sender: &Snapshot) -> anyhow::Result<Vec<PeerOutcome>> {
        let captured = self.messages.take();
        if captured.is_empty() {
            return Ok(Vec::new());
        }

        let Some(xml) = self
            .stores
            .load_xml(&sender.instance.process_def_id)
            .await?
        else {
            eprintln!(
                "⚠️ [Graph] message_schema_missing instance={}",
                sender.instance.id
            );
            return Ok(Vec::new());
        };
        let Some(catalog) = messages::parse(&xml) else {
            eprintln!(
                "⚠️ [Graph] message_schema_unparsed instance={}",
                sender.instance.id
            );
            return Ok(Vec::new());
        };

        let mut peers = Vec::new();

        for message in captured {
            println!(
                "📨 [Graph] message_sent instance={} node={} message={}",
                message.instance_id, message.node_id, message.message_name
            );

            // Сверка с разбором схемы: имя сообщения пришло из движка, а
            // `trg:processTrigger`/`messageFlow` — из XML. Расхождение значит,
            // что схему обновили между деплоем и проходом, — сообщение всё
            // равно доставляется по имени движка.
            if let Some(declared) = catalog.throw_message(&message.node_id) {
                if declared != message.message_name {
                    eprintln!(
                        "⚠️ [Graph] message_name_mismatch node={} engine={} schema={}",
                        message.node_id, message.message_name, declared
                    );
                }
            }

            if let Some(flow) = catalog.flow_for(&message.message_name) {
                println!(
                    "🔗 [Graph] message_flow id={} source={} target={}",
                    flow.id, flow.source, flow.target
                );
            }

            if let Some(trigger) = catalog.trigger_for(&message.node_id) {
                match self.start_counterpart(sender, trigger).await {
                    Ok(Some(peer)) => peers.push(peer),
                    Ok(None) => {}
                    Err(error) => eprintln!(
                        "❌ [Graph] message_trigger_failed instance={} target={}: {:#}",
                        sender.instance.id, trigger.target_process, error
                    ),
                }
            }

            match self.deliver_message(sender, &message).await {
                Ok(delivered) => peers.extend(delivered),
                Err(error) => eprintln!(
                    "❌ [Graph] message_delivery_failed instance={} message={}: {:#}",
                    sender.instance.id, message.message_name, error
                ),
            }
        }

        Ok(peers)
    }

    /// Запускает экземпляр второй стороны по `trg:processTrigger`.
    ///
    /// Идемпотентность: id экземпляра детерминирован (`<prefix><orderNum>`),
    /// поэтому повторная доставка сообщения (ретрай, второй под) не создаёт
    /// второй экземпляр. `firstElementId` совпадает со штатным первым шагом
    /// целевого процесса; расхождение логируется — вход в произвольный узел
    /// появится вместе с явной активацией узла (M3.2).
    async fn start_counterpart(
        &self,
        sender: &Snapshot,
        trigger: &messages::ProcessTrigger,
    ) -> anyhow::Result<Option<PeerOutcome>> {
        let Some(business_key) = messages::correlation_value(&sender.instance.variables) else {
            eprintln!(
                "⚠️ [Graph] message_trigger_without_correlation instance={} target={}",
                sender.instance.id, trigger.target_process
            );
            return Ok(None);
        };

        let Some(instance_id) = messages::instance_id(&trigger.instance_prefix, &business_key)
        else {
            eprintln!(
                "⚠️ [Graph] message_trigger_bad_instance_id instance={} key={}",
                sender.instance.id, business_key
            );
            return Ok(None);
        };

        if self
            .stores
            .process_store
            .load(&instance_id)
            .await?
            .is_some()
        {
            println!(
                "🔁 [Graph] message_trigger_duplicate instance={} target={}",
                instance_id, trigger.target_process
            );
            return Ok(None);
        }

        if self
            .stores
            .cached_def_store
            .load(&trigger.target_process)
            .await?
            .is_none()
        {
            eprintln!(
                "⚠️ [Graph] message_target_not_deployed target={} instance={}",
                trigger.target_process, instance_id
            );
            return Ok(None);
        }

        let variables = messages::select_payload(&sender.instance.variables, &trigger.payload_keys);
        let start = EngineEvent::ProcessStarted(payloads::ProcessStarted {
            process_id: trigger.target_process.clone(),
            instance_id: instance_id.clone(),
            initial_variables: Some(variables.clone()),
        });

        println!(
            "🚀 [Graph] message_trigger instance={} target={} key={} payload={:?}",
            instance_id, trigger.target_process, business_key, trigger.payload_keys
        );

        let mut context = self.stores.context();
        self.messages.clear();
        self.engine.run_async(start, &mut context).await;
        // Каскад сообщений внутри нового экземпляра — M3.2: сейчас триггер
        // запускает процесс, но не разбирает его собственные throw-события.
        let _ = self.messages.take();

        let payload = serde_json::json!(&variables);
        let peer = self
            .project_peer(&instance_id, None, Some(&payload))
            .await?;

        if let (Some(first_element), Some(peer)) = (&trigger.first_element, &peer) {
            let active = peer
                .steps
                .iter()
                .find(|step| step.state == "ACTIVE")
                .map(|step| step.element_id.as_str());
            if active != Some(first_element.as_str()) {
                eprintln!(
                    "⚠️ [Graph] message_entry_mismatch instance={} expected={} actual={:?}",
                    instance_id, first_element, active
                );
            }
        }

        Ok(peer)
    }

    /// Доставляет сообщение ожидающему catch-событию второй стороны.
    ///
    /// Кандидаты — запущенные экземпляры, у которых:
    ///
    /// - в схеме есть catch-событие с этим именем сообщения и `Waiting`-токен
    ///   стоит именно на нём;
    /// - переменные совпадают с картой корреляции (`orderNum`), то есть это
    ///   тот же заказ, а не соседний.
    ///
    /// Доставку выполняет отдельный движок с `MessageCatchHandler`: он поднимает
    /// токен и отдаёт `TokenArrived`, а сообщение приносит свои переменные.
    /// Без бизнес-ключа доставки нет — «во все экземпляры» сообщение не уходит.
    async fn deliver_message(
        &self,
        sender: &Snapshot,
        message: &pump::CapturedMessage,
    ) -> anyhow::Result<Vec<PeerOutcome>> {
        let correlation = messages::correlation(&sender.instance.variables);
        if correlation.is_empty() {
            eprintln!(
                "⚠️ [Graph] message_without_correlation instance={} message={} (ключ '{}' не найден)",
                sender.instance.id,
                message.message_name,
                messages::CORRELATION_KEY
            );
            return Ok(Vec::new());
        }

        let running = self.stores.process_store.list_running(None).await?;
        let mut targets = Vec::new();

        for instance_id in running {
            if instance_id == sender.instance.id {
                continue;
            }
            let Some(snapshot) = pump::load(
                &self.stores.process_store,
                &self.stores.cached_def_store,
                &instance_id,
            )
            .await?
            else {
                continue;
            };
            if !messages::matches_correlation(&snapshot.instance.variables, &correlation) {
                continue;
            }

            let Some(xml) = self
                .stores
                .load_xml(&snapshot.instance.process_def_id)
                .await?
            else {
                continue;
            };
            let Some(catalog) = messages::parse(&xml) else {
                continue;
            };
            if catalog.catch_nodes(&message.message_name).is_empty() {
                continue;
            }

            let waiting = snapshot.instance.tokens.iter().any(|token| {
                token.status == TokenStatus::Waiting
                    && catalog.catch_message(&token.node_id) == Some(message.message_name.as_str())
            });
            if !waiting {
                continue;
            }

            targets.push(snapshot);
        }

        if targets.is_empty() {
            println!(
                "ℹ️ [Graph] message_no_waiter message={} key={:?}",
                message.message_name,
                correlation.get(messages::CORRELATION_KEY)
            );
            return Ok(Vec::new());
        }

        // Сообщение несёт скалярные переменные отправителя: вендор хранит их
        // строками, вложенные объекты отсекаются ещё на входе (`vars::flatten`).
        let payload = sender.instance.variables.clone();
        let delivery = payloads::MessageSent {
            instance_id: sender.instance.id.clone(),
            message_name: message.message_name.clone(),
            node_id: message.node_id.clone(),
            correlation: Some(correlation),
            payload: Some(payload.clone()),
        };

        println!(
            "📬 [Graph] message_delivery message={} from={} targets={:?}",
            message.message_name,
            sender.instance.id,
            targets
                .iter()
                .map(|target| target.instance.id.as_str())
                .collect::<Vec<_>>()
        );

        let mut context = self.stores.context();
        self.message_engine
            .run_async(EngineEvent::MessageSent(delivery), &mut context)
            .await;

        let payload = serde_json::json!(&payload);
        let mut peers = Vec::new();
        for before in &targets {
            if let Some(peer) = self
                .project_peer(&before.instance.id, Some(before), Some(&payload))
                .await?
            {
                peers.push(peer);
            }
        }

        Ok(peers)
    }

    /// Снимок «после» для экземпляра второй стороны: проекция, legacy, команды.
    ///
    /// Пустая проекция — экземпляр не изменился (например, сообщение пришло
    /// повторно и ждать уже нечего): ничего не публикуется.
    async fn project_peer(
        &self,
        instance_id: &str,
        before: Option<&Snapshot>,
        payload: Option<&Value>,
    ) -> anyhow::Result<Option<PeerOutcome>> {
        let Some(after) = pump::load(
            &self.stores.process_store,
            &self.stores.cached_def_store,
            instance_id,
        )
        .await?
        else {
            eprintln!("⚠️ [Graph] peer_instance_lost instance={}", instance_id);
            return Ok(None);
        };

        let projection = project::diff(before, &after);
        if projection.is_empty() {
            return Ok(None);
        }

        project::apply_legacy(
            &self.legacy,
            instance_id,
            &after.instance.process_def_id,
            &projection,
        )
        .await?;
        let commands = service_commands(instance_id, &after, &projection);
        let outcome = to_outcome(&projection, payload, commands);
        Ok(Some(PeerOutcome::from_outcome(instance_id, outcome)))
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
    projection
        .activated
        .iter()
        .filter_map(|step| {
            let key = format!("{}:{}:{}", instance_id, step.element_id, step.token_id);
            worker_command(&key, after, step)
        })
        .collect()
}

/// Команда воркеру для одного ожидающего `serviceTask`.
///
/// Общая для первой отправки и для ретраев: `idempotency_key` и `payload`
/// берутся из текущего снимка, поэтому повтор несёт те же данные, что и
/// исходная команда (воркер может смело сравнивать ключи).
fn worker_command(key: &str, snapshot: &Snapshot, step: &Step) -> Option<ServiceCommand> {
    let spec = pump::external_task_spec(&snapshot.definition, &step.element_id)?;
    let deadline = chrono::Utc::now() + chrono::Duration::seconds(spec.timeout_secs as i64);

    Some(ServiceCommand {
        instance_id: snapshot.instance.id.clone(),
        element_id: step.element_id.clone(),
        target_system: spec.task_type,
        payload: serde_json::json!(&snapshot.instance.variables),
        idempotency_key: key.to_string(),
        deadline: deadline.to_rfc3339(),
    })
}

/// Ключ ретраев ответа: ключ из команды, а если воркер его не вернул —
/// тот же детерминированный `<instance>:<element>:<token>`.
fn erp_retry_key(event: &ErpResponseEvent, step: &Step) -> String {
    event
        .idempotency_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "{}:{}:{}",
                event.process_instance_id, step.element_id, step.token_id
            )
        })
}

/// Что делать с неуспешным ответом воркера.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailurePlan {
    /// Переотправить команду через `delay_secs` секунд.
    Retry { attempt: i32, delay_secs: u64 },
    /// Ретраи исчерпаны или ошибка не повторяемая.
    Compensate { reason: &'static str },
}

/// План по неуспешному ответу.
///
/// `retries` — `camunda:retries` узла (вендор по умолчанию 3), `attempts` —
/// сколько ретраев уже сделано. Ретрай разрешён только при `retryable`:
/// без флага воркер явно просит человека.
fn failure_plan(
    retries: i32,
    attempts: i32,
    retryable: bool,
    base_secs: u64,
    max_secs: u64,
) -> FailurePlan {
    if !retryable {
        return FailurePlan::Compensate {
            reason: "NOT_RETRYABLE",
        };
    }

    if attempts >= retries {
        return FailurePlan::Compensate {
            reason: "RETRIES_EXHAUSTED",
        };
    }

    FailurePlan::Retry {
        attempt: attempts + 1,
        delay_secs: backoff_secs(attempts, base_secs, max_secs),
    }
}

/// Экспоненциальный backoff: `base`, `2·base`, `4·base`, … с потолком `max`.
///
/// `attempt` — номер уже сделанных ретраев (0 для первой задержки). Сдвиг
/// ограничен 20 битами, чтобы `1u64 << attempt` не переполнялся.
fn backoff_secs(attempt: i32, base_secs: u64, max_secs: u64) -> u64 {
    let shift = attempt.clamp(0, 20) as u32;
    base_secs.saturating_mul(1u64 << shift).min(max_secs)
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
        actions: Vec::new(),
        peers: Vec::new(),
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

/// Аудит исполнителя шага (M6).
///
/// Ошибка записи не отменяет уже применённое состояние: журнал вторичен по
/// отношению к графу, поэтому здесь только лог.
async fn record_actor(
    pool: &PgPool,
    instance_id: &str,
    event_type: &str,
    element_id: &str,
    event: &InboundEvent,
    payload: Value,
) {
    let actor = event.actor.trim();
    let actor = if actor.is_empty() { None } else { Some(actor) };

    if let Err(error) =
        db::record_step(pool, instance_id, event_type, element_id, actor, &payload).await
    {
        eprintln!(
            "⚠️ [Graph] audit_failed instance={instance_id} element={element_id} type={event_type}: {error}"
        );
    }
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

    #[test]
    fn retryable_failure_goes_to_backoff() {
        assert_eq!(
            failure_plan(3, 0, true, 5, 300),
            FailurePlan::Retry {
                attempt: 1,
                delay_secs: 5
            },
            "первый ретрай — через base-задержку"
        );
        assert_eq!(
            failure_plan(3, 2, true, 5, 300),
            FailurePlan::Retry {
                attempt: 3,
                delay_secs: 20
            },
            "третий ретрай — 4·base, счётчик попыток растёт"
        );
    }

    #[test]
    fn exhausted_or_non_retryable_failure_compensates() {
        assert_eq!(
            failure_plan(3, 3, true, 5, 300),
            FailurePlan::Compensate {
                reason: "RETRIES_EXHAUSTED"
            },
            "camunda:retries исчерпаны — нужен человек"
        );
        assert_eq!(
            failure_plan(3, 0, false, 5, 300),
            FailurePlan::Compensate {
                reason: "NOT_RETRYABLE"
            },
            "без retryable попытки не тратятся"
        );
        assert_eq!(
            failure_plan(0, 0, true, 5, 300),
            FailurePlan::Compensate {
                reason: "RETRIES_EXHAUSTED"
            },
            "retries = 0 — компенсация сразу"
        );
    }

    #[test]
    fn backoff_doubles_and_is_capped() {
        assert_eq!(backoff_secs(0, 5, 300), 5);
        assert_eq!(backoff_secs(1, 5, 300), 10);
        assert_eq!(backoff_secs(6, 5, 300), 300, "потолок важнее степени");
        assert_eq!(backoff_secs(1000, 5, 300), 300, "сдвиг не переполняется");
    }

    #[test]
    fn erp_retry_key_falls_back_to_command_key() {
        let step = Step {
            token_id: "token-1".to_string(),
            element_id: "ServiceTask_ERP".to_string(),
        };
        let mut event = ErpResponseEvent {
            process_instance_id: "instance-1".to_string(),
            element_id: "ServiceTask_ERP".to_string(),
            status: "FAILED".to_string(),
            error_message: None,
            retryable: true,
            payload: json!({}),
            idempotency_key: None,
        };

        assert_eq!(
            erp_retry_key(&event, &step),
            "instance-1:ServiceTask_ERP:token-1",
            "без ключа ретраи адресуются тем же ключом, что и команда"
        );

        event.idempotency_key = Some("  custom-key  ".to_string());
        assert_eq!(
            erp_retry_key(&event, &step),
            "custom-key",
            "ключ воркера важнее вычисленного"
        );
    }
}
